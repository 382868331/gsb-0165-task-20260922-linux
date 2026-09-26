"""固定四扇区模式的协议、交错和独立账本测试。"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from msi_kernel import (
    ALL_SECTORS,
    DIR,
    CacheCoherenceKernel,
    DeliveryStatus,
    Message,
    MsgType,
    Op,
    Request,
    State,
    check_invariants,
    drain,
    explore,
)
from reference import check_sector_completion_sequence


def sector_flat(completions):
    return [(c.addr, c.op.value, c.value, c.sectors) for c in completions]


class TestSectorModeBasics(unittest.TestCase):
    def test_default_remains_whole_line_mode(self):
        k = CacheCoherenceKernel()
        self.assertFalse(k.sector_mode)
        rep = k.request(0, 0, Op.STORE, value=7)
        self.assertEqual(rep.completions[0].sectors, ALL_SECTORS)
        self.assertIsNone(k.snapshot().directory[0].sectors)

    def test_independent_sector_writes_complete_separately(self):
        k = CacheCoherenceKernel(
            sector_mode=True,
            sector_values={(0, 0): 10, (0, 1): 20},
        )
        r0 = k.request(0, 0, Op.STORE, value=11, sectors=0b0001)
        r1 = k.request(1, 0, Op.STORE, value=21, sectors=0b0010)
        self.assertEqual(r0.completions[0].value, 11)
        self.assertEqual(r1.completions[0].value, 21)
        snap = k.snapshot()
        self.assertIs(snap.caches[0][0].sectors[0].state, State.MODIFIED)
        self.assertIs(snap.caches[1][0].sectors[1].state, State.MODIFIED)
        self.assertEqual(
            [sector.version for sector in snap.directory[0].sectors[:2]],
            [1, 1])

    def test_overlapping_sectors_remain_serial_while_disjoint_sector_progresses(self):
        k = CacheCoherenceKernel(sector_mode=True)
        k.request(1, 0, Op.LOAD, sectors=0b0011)
        first = k.request(0, 0, Op.STORE, value=11, sectors=0b0001)
        overlap = k.request(1, 0, Op.STORE, value=12, sectors=0b0001)
        disjoint = k.request(2, 0, Op.STORE, value=33, sectors=0b0100)
        self.assertFalse(first.queued)
        self.assertTrue(overlap.queued)
        self.assertFalse(disjoint.queued)
        self.assertEqual([q.txn_id for q in k.snapshot().queues[0]], [3])
        drain(k)
        stores = [c for c in k.completed() if c.op is Op.STORE]
        self.assertEqual([(c.txn_id, c.value, c.sectors) for c in stores],
                         [(4, 33, 4), (2, 11, 1), (3, 12, 1)])

    def test_invalid_sector_mask_is_rejected(self):
        k = CacheCoherenceKernel(sector_mode=True)
        for bad_mask in (0, 16, -1):
            with self.assertRaises(Exception):
                k.request(0, 0, Op.LOAD, sectors=bad_mask)


class TestCrossSectorRead(unittest.TestCase):
    def setUp(self):
        self.init_values = {(0, 0): 10, (0, 1): 20}
        self.scenario = [
            Request(0, 0, Op.LOAD, sectors=0b0011),
            Request(1, 0, Op.LOAD, sectors=0b0011),
            Request(0, 0, Op.STORE, value=11, sectors=0b0001),
            Request(1, 0, Op.STORE, value=21, sectors=0b0010),
            Request(2, 0, Op.LOAD, sectors=0b0011),
        ]

    def factory(self):
        return CacheCoherenceKernel(
            sector_mode=True, sector_values=self.init_values)

    def test_two_sector_read_grants_and_completes_atomically(self):
        k = self.factory()
        k.request_batch(self.scenario)
        self.assertTrue(all(
            line.state is State.INVALID
            for line in k.snapshot().caches[2][0].sectors[:2]))
        pending = list(k.pending())
        self.assertEqual({(m.dst, m.sectors) for m in pending},
                         {(1, 1), (0, 2)})
        self.assertEqual({m.mtype for m in pending}, {MsgType.INV})

        for msg in pending:
            ack = k.deliver(msg.msg_id).emitted[0]
            k.deliver(ack.msg_id)

        fetches = list(k.pending())
        self.assertEqual({(m.mtype, m.dst, m.sectors) for m in fetches},
                         {(MsgType.FETCH, 0, 1), (MsgType.FETCH, 1, 2)})
        first_flush = k.deliver(fetches[0].msg_id).emitted[0]
        k.deliver(first_flush.msg_id)
        self.assertFalse(any(c.txn_id == 5 for c in k.completed()))
        self.assertTrue(all(
            line.state is State.INVALID
            for line in k.snapshot().caches[2][0].sectors[:2]))

        second_fetch = next(m for m in k.pending() if m.mtype is MsgType.FETCH)
        second_flush = k.deliver(second_fetch.msg_id).emitted[0]
        rep = k.deliver(second_flush.msg_id)
        self.assertEqual(rep.completions[0].value, (11, 21, 0, 0))
        self.assertEqual(rep.completions[0].sectors, 0b0011)
        self.assertEqual(
            [(line.state.value, line.value, line.version)
             for line in k.snapshot().caches[2][0].sectors[:2]],
            [("S", 11, 1), ("S", 21, 1)])

    def test_all_interleavings_match_independent_sector_ledger(self):
        res = explore(self.factory, self.scenario)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        self.assertGreaterEqual(res.steps, 4)
        for completions in res.schedules.values():
            loads = [c for c in completions if c.txn_id == 5]
            self.assertEqual(len(loads), 1)
            self.assertEqual(loads[0].value, (11, 21, 0, 0))
            check_sector_completion_sequence(
                sector_flat(completions), self.init_values)

    def test_multi_sector_store_to_foreign_m_invalidates_every_flushed_sector(self):
        k = CacheCoherenceKernel(
            sector_mode=True,
            sector_values={(0, 0): 10, (0, 1): 20},
        )
        k.request(0, 0, Op.STORE, value=42, sectors=0b0011)
        k.request(1, 0, Op.STORE, value=99, sectors=0b0011)
        drain(k)
        comp = k.completed()[-1]
        self.assertEqual((comp.value, comp.sectors), ((99, 99, 0, 0), 0b0011))
        for sector in range(2):
            self.assertIs(k.snapshot().caches[0][0].sectors[sector].state,
                          State.INVALID)
            self.assertIs(k.snapshot().caches[1][0].sectors[sector].state,
                          State.MODIFIED)
            self.assertEqual(k.snapshot().directory[0].sectors[sector].owner,
                             1)


class TestSectorMessageMasks(unittest.TestCase):
    def test_ack_wrong_mask_is_stale_and_cannot_release_txn(self):
        k = CacheCoherenceKernel(sector_mode=True)
        k.request(0, 0, Op.LOAD, sectors=0b0011)
        k.request(1, 0, Op.LOAD, sectors=0b0011)
        k.request(0, 0, Op.STORE, value=11, sectors=0b0001)
        wrong = Message(None, 3, MsgType.INV_ACK, src=1, dst=DIR, addr=0,
                        sectors=0b0010)
        rep = k.deliver(wrong)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().directory[0].sectors[0].awaiting_acks,
                         (1,))
        real = next(m for m in k.pending() if m.sectors == 0b0001)
        ack = k.deliver(real.msg_id).emitted[0]
        done = k.deliver(ack.msg_id)
        self.assertEqual(done.completions[0].value, 11)

    def test_late_ack_from_old_txn_cannot_release_new_txn(self):
        k = CacheCoherenceKernel(sector_mode=True)
        k.request(0, 0, Op.LOAD, sectors=1)
        k.request(1, 0, Op.LOAD, sectors=1)
        k.request(0, 0, Op.STORE, value=11, sectors=1)
        inv = k.pending()[0]
        late_ack = k.deliver(inv.msg_id).emitted[0]

        drain(k)
        k.request(1, 0, Op.LOAD, sectors=1)
        drain(k)
        k.request(0, 0, Op.STORE, value=12, sectors=1)
        drain(k)

        k.request(1, 0, Op.LOAD, sectors=1)
        drain(k)
        k.request(1, 0, Op.STORE, value=13, sectors=1)
        self.assertEqual(k.snapshot().directory[0].sectors[0].active_txn, 7)
        stale = Message(None, 3, MsgType.INV_ACK, src=0, dst=DIR, addr=0,
                        sectors=1)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().directory[0].sectors[0].awaiting_acks,
                         (0,))
        real = next(m for m in k.pending()
                    if m.txn_id == 7 and m.mtype is MsgType.INV)
        ack = k.deliver(real.msg_id).emitted[0]
        self.assertEqual(k.deliver(ack.msg_id).completions[0].value, 13)

    def test_single_writer_invariant_holds_on_every_reachable_prefix(self):
        scenario = [
            Request(0, 0, Op.LOAD, sectors=0b0111),
            Request(1, 0, Op.LOAD, sectors=0b0111),
            Request(2, 0, Op.LOAD, sectors=0b0111),
            Request(0, 0, Op.STORE, value=1, sectors=0b0001),
            Request(1, 0, Op.STORE, value=2, sectors=0b0010),
            Request(2, 0, Op.LOAD, sectors=0b0011),
        ]
        res = explore(lambda: CacheCoherenceKernel(sector_mode=True), scenario)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)

        k = CacheCoherenceKernel(sector_mode=True)
        k.request_batch(scenario)
        self.assertIsNone(check_invariants(k))
        while k.pending():
            k.deliver(k.pending()[0].msg_id)
            self.assertIsNone(check_invariants(k))


if __name__ == "__main__":
    unittest.main()
