import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from msi_kernel import (
    ALL_SECTORS,
    CacheCoherenceKernel,
    LineMode,
    Message,
    MsgType,
    Op,
    Request,
    State,
)
from msi_kernel.explore import check_invariants, explore


def sectors(mask):
    return tuple(i for i in range(4) if mask & (1 << i))


class IndependentSectorLedger:
    """按完成顺序记录每扇区最后一次已提交写入的独立串行账本。"""

    def __init__(self, initial):
        self.last = {s: initial for s in range(4)}
        self.seen = set()

    def apply(self, completion):
        if completion.txn_id in self.seen:
            raise AssertionError(f"事务 {completion.txn_id} 重复完成")
        self.seen.add(completion.txn_id)
        if completion.op is Op.STORE:
            for sector, value in zip(sectors(completion.sectors),
                                     completion.value):
                self.last[sector] = value
        else:
            for sector, value in zip(sectors(completion.sectors),
                                     completion.value):
                if value != self.last[sector]:
                    raise AssertionError(
                        f"扇区 {sector} 读到 {value}，串行账本为 "
                        f"{self.last[sector]}")


def make_shared_factory():
    kernel = CacheCoherenceKernel({0: 0}, mode=LineMode.FOUR_SECTOR)
    for core in range(3):
        kernel.request(core, 0, Op.LOAD, sectors=ALL_SECTORS)
    return kernel


class TestSectorMode(unittest.TestCase):
    def test_default_remains_whole_line_mode(self):
        kernel = CacheCoherenceKernel()
        self.assertIs(kernel.mode, LineMode.WHOLE_LINE)
        self.assertIsNone(kernel.snapshot().sector_caches)
        with self.assertRaises(Exception):
            kernel.request(0, 0, Op.LOAD, sectors=1)
        self.assertEqual(kernel.completed(), ())

    def test_independent_sector_writes_progress_together(self):
        kernel = make_shared_factory()
        first = kernel.request(0, 0, Op.STORE, value=(10,), sectors=1)
        second = kernel.request(1, 0, Op.STORE, value=(20,), sectors=2)
        self.assertFalse(first.queued)
        self.assertFalse(second.queued)
        active = {t.txn_id: t.sectors for t in kernel.snapshot().sector_txns[0]}
        self.assertEqual(active, {4: 1, 5: 2})

        while kernel.pending():
            self.assertIsNone(check_invariants(kernel))
            kernel.deliver(kernel.pending()[0].msg_id)
        self.assertIsNone(check_invariants(kernel))
        values = {c.txn_id: c.value for c in kernel.snapshot().completed}
        self.assertEqual(values[4], (10,))
        self.assertEqual(values[5], (20,))
        snap = kernel.snapshot()
        self.assertIs(snap.sector_caches[0][0][0].state, State.MODIFIED)
        self.assertEqual(snap.sector_caches[0][0][0].version, 1)
        self.assertIs(snap.sector_caches[1][0][1].state, State.MODIFIED)
        self.assertEqual(snap.sector_caches[1][0][1].version, 1)

    def test_overlapping_requests_serialize_while_disjoint_request_starts(self):
        kernel = make_shared_factory()
        kernel.request(0, 0, Op.STORE, value=(10,), sectors=1)
        overlap = kernel.request(1, 0, Op.STORE, value=(11,), sectors=1)
        disjoint = kernel.request(2, 0, Op.STORE, value=(30,), sectors=4)
        self.assertTrue(overlap.queued)
        self.assertFalse(disjoint.queued)
        active = {t.txn_id: t.sectors for t in kernel.snapshot().sector_txns[0]}
        self.assertEqual(active, {4: 1, 6: 4})
        self.assertEqual(kernel.snapshot().queues[0][0].txn_id, 5)
        while kernel.pending():
            kernel.deliver(kernel.pending()[0].msg_id)
        by_txn = {c.txn_id: c for c in kernel.completed()}
        self.assertEqual(by_txn[4].value, (10,))
        self.assertEqual(by_txn[5].value, (11,))
        self.assertEqual(by_txn[6].value, (30,))
        self.assertEqual(by_txn[5].sectors, 1)

    def test_cross_sector_read_grants_both_atomically(self):
        kernel = make_shared_factory()
        kernel.request(0, 0, Op.STORE, value=(10,), sectors=1)
        kernel.request(1, 0, Op.STORE, value=(20,), sectors=2)
        while kernel.pending():
            kernel.deliver(kernel.pending()[0].msg_id)
        kernel.request(2, 0, Op.LOAD, sectors=3)

        fetch_zero = next(m for m in kernel.pending() if m.dst == 0)
        fetch_one = next(m for m in kernel.pending() if m.dst == 1)
        flush_zero = kernel.deliver(fetch_zero.msg_id).emitted[0]
        partial = kernel.deliver(flush_zero.msg_id)
        self.assertEqual(partial.completions, ())
        self.assertTrue(kernel.snapshot().sector_txns[0])
        flush_one = kernel.deliver(fetch_one.msg_id).emitted[0]
        complete = kernel.deliver(flush_one.msg_id)
        self.assertEqual(len(complete.completions), 1)
        self.assertEqual(complete.completions[0].value, (10, 20))
        self.assertEqual(complete.completions[0].sectors, 3)
        reader = kernel.snapshot().sector_caches[2][0]
        self.assertEqual((reader[0].value, reader[1].value), (10, 20))
        self.assertEqual((reader[0].version, reader[1].version), (1, 1))

    def test_mixed_dirty_and_shared_store_flushes_before_invalidating(self):
        kernel = CacheCoherenceKernel({0: 0}, mode=LineMode.FOUR_SECTOR)
        kernel.request(0, 0, Op.STORE, value=(10,), sectors=1)
        kernel.request(1, 0, Op.LOAD, sectors=2)
        kernel.request(2, 0, Op.STORE, value=(30, 31), sectors=3)

        pending = kernel.pending()
        self.assertEqual([m.mtype for m in pending], [MsgType.FETCH])
        flush = kernel.deliver(pending[0].msg_id).emitted[0]
        report = kernel.deliver(flush.msg_id)
        self.assertEqual(
            sorted(m.mtype for m in report.emitted),
            [MsgType.INV, MsgType.INV],
        )
        while kernel.pending():
            kernel.deliver(kernel.pending()[0].msg_id)
        completion = kernel.completed()[-1]
        self.assertEqual(completion.value, (30, 31))
        self.assertEqual(completion.sectors, 3)

    def test_wrong_sector_and_late_acks_are_stale(self):
        kernel = make_shared_factory()
        kernel.request(0, 0, Op.STORE, value=(10,), sectors=1)
        wrong = kernel.deliver(Message(
            None, 4, MsgType.INV_ACK, src=1, dst=3, addr=0, sectors=2))
        self.assertEqual(wrong.status.value, "stale")
        waiting = kernel.snapshot().sector_txns[0][0].ack_waiters
        self.assertEqual(waiting, ((1, 1), (2, 1)))

        while kernel.pending():
            kernel.deliver(kernel.pending()[0].msg_id)
        kernel.request(1, 0, Op.STORE, value=(20,), sectors=2)
        while kernel.pending():
            kernel.deliver(kernel.pending()[0].msg_id)
        late = kernel.deliver(Message(
            None, 4, MsgType.INV_ACK, src=2, dst=3, addr=0, sectors=1))
        self.assertEqual(late.status.value, "stale")
        next_request = kernel.request(2, 0, Op.STORE, value=(40,), sectors=4)
        self.assertFalse(next_request.queued)
        self.assertTrue(kernel.snapshot().sector_txns[0])

    def test_synthetic_interleavings_checked_every_prefix(self):
        scenario = [
            Request(0, 0, Op.STORE, value=(10,), sectors=1),
            Request(1, 0, Op.STORE, value=(20,), sectors=2),
            Request(2, 0, Op.LOAD, sectors=3),
        ]
        result = explore(make_shared_factory, scenario, budget=100_000)
        self.assertFalse(result.exhausted)
        self.assertEqual(result.violations, ())
        self.assertGreater(len(result.schedules), 1)
        for completions in result.schedules.values():
            ledger = IndependentSectorLedger(0)
            for completion in completions:
                ledger.apply(completion)

    def test_invalid_sector_inputs_and_batch_atomicity(self):
        with self.assertRaises(Exception):
            CacheCoherenceKernel(mode=LineMode.FOUR_SECTOR).request(
                0, 0, Op.LOAD, sectors=0)
        with self.assertRaises(Exception):
            CacheCoherenceKernel(mode=LineMode.FOUR_SECTOR).request(
                0, 0, Op.LOAD, sectors=True)
        with self.assertRaises(Exception):
            CacheCoherenceKernel(mode=LineMode.FOUR_SECTOR).request(
                0, 0, Op.STORE, value=1, sectors="bad")
        with self.assertRaises(Exception):
            CacheCoherenceKernel(mode=LineMode.FOUR_SECTOR).request(
                0, 0, Op.STORE, value=(1,), sectors=3)
        kernel = CacheCoherenceKernel(mode=LineMode.FOUR_SECTOR)
        batch = [
            Request(0, 0, Op.STORE, value=(1,), sectors=1),
            Request(1, 0, Op.STORE, value=(2,), sectors=99),
        ]
        with self.assertRaises(Exception):
            kernel.request_batch(batch)
        self.assertEqual(kernel.completed(), ())
        self.assertEqual(kernel.pending(), ())


if __name__ == "__main__":
    unittest.main()
