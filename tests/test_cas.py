"""CAS protocol tests."""

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, TESTS)

from msi_kernel import (
    CacheCoherenceKernel,
    Completion,
    DeliveryStatus,
    InvalidRequestError,
    MsgType,
    Op,
    Request,
    State,
    drain,
)
from reference import (
    check_completion_sequence,
    final_memory_after_completions,
)


def _flat(completed):
    events = []
    for c in completed:
        if c.op is Op.CAS:
            events.append((c.addr, "cas", c.value, c.success,
                           c.expected_value, c.new_value))
        else:
            events.append((c.addr, c.op.value, c.value))
    return events


class TestCompareAndSwap(unittest.TestCase):
    def test_two_cores_cas_zero_only_first_succeeds(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.CAS, expected_value=0, new_value=10,
                    txn_id=101),
            Request(2, 0, Op.CAS, expected_value=0, new_value=20,
                    txn_id=102),
        ])
        self.assertEqual([q.txn_id for q in k.snapshot().queues[0]], [102])
        drain(k)
        cas = [c for c in k.completed() if c.op is Op.CAS]
        self.assertEqual(cas, [
            Completion(101, 1, 0, Op.CAS, 0, True, 0, 10),
            Completion(102, 2, 0, Op.CAS, 10, False, 0, 20),
        ])
        snap = k.snapshot()
        owner = snap.directory[0].owner
        self.assertEqual(owner, 2)
        self.assertEqual(snap.caches[owner][0].value, 10)
        self.assertIs(snap.caches[owner][0].state, State.MODIFIED)
        self.assertEqual(snap.caches[1][0].state, State.INVALID)
        flat = _flat(k.completed())
        self.assertEqual(final_memory_after_completions(flat)[0],
                         snap.caches[owner][0].value)

    def test_cas_linearized_between_two_ordinary_stores(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=1, txn_id=1)
        k.request(1, 0, Op.CAS, expected_value=0, new_value=9, txn_id=2)
        k.request(2, 0, Op.STORE, value=3, txn_id=3)
        self.assertEqual([q.txn_id for q in k.snapshot().queues[0]], [3])
        drain(k)
        cas = next(c for c in k.completed() if c.op is Op.CAS)
        self.assertEqual((cas.value, cas.success), (1, False))
        self.assertEqual(k.snapshot().caches[2][0].value, 3)
        events = _flat(k.completed())
        check_completion_sequence(events, {0: 0})
        self.assertEqual(final_memory_after_completions(events, {0: 0})[0], 3)

    def test_cas_to_foreign_modified_line_compares_after_flush(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=7, txn_id=1)
        k.request(1, 0, Op.CAS, expected_value=7, new_value=8, txn_id=2)
        drain(k)
        comp = next(c for c in k.completed() if c.op is Op.CAS)
        self.assertEqual((comp.value, comp.success), (7, True))
        snap = k.snapshot()
        self.assertEqual(snap.directory[0].owner, 1)
        self.assertEqual(snap.caches[1][0].value, 8)
        self.assertEqual(snap.caches[0][0].state, State.INVALID)
        check_completion_sequence(_flat(k.completed()), {0: 0})

    def test_delayed_invalidation_ack_does_not_publish_cas(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.CAS, expected_value=0, new_value=7, txn_id=7),
        ])
        inv0 = next(m for m in k.pending() if m.dst == 0)
        ack0 = k.deliver(inv0.msg_id).emitted[0]
        k.deliver(ack0.msg_id)
        self.assertFalse(any(c.op is Op.CAS for c in k.completed()))
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (2,))
        self.assertIs(k.snapshot().caches[1][0].state, State.SHARED)
        inv2 = next(m for m in k.pending() if m.dst == 2)
        ack2 = k.deliver(inv2.msg_id).emitted[0]
        rep = k.deliver(ack2.msg_id)
        self.assertEqual(rep.completions[0],
                         Completion(7, 1, 0, Op.CAS, 0, True, 0, 7))

    def test_cas_request_retry_returns_original_result_once(self):
        k = CacheCoherenceKernel(memory={0: 0})
        request = Request(0, 0, Op.CAS, expected_value=0, new_value=5,
                          txn_id=88)
        first = k.request_batch([request])[0]
        self.assertTrue(first.completions[0].success)
        retry = k.request_batch([request])[0]
        self.assertEqual(retry, first)
        self.assertEqual(len([c for c in k.completed() if c.op is Op.CAS]), 1)
        self.assertEqual(k.snapshot().caches[0][0].value, 5)

    def test_duplicate_network_ack_does_not_swap_twice(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(1, 0, Op.CAS, expected_value=0, new_value=5,
                    txn_id=3),
        ])
        inv = next(m for m in k.pending() if m.mtype is MsgType.INV)
        ack = k.deliver(inv.msg_id).emitted[0]
        first = k.deliver(ack.msg_id)
        duplicate = k.deliver(ack.msg_id)
        self.assertTrue(first.completions[0].success)
        self.assertIs(duplicate.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(duplicate.completions, ())
        self.assertEqual(
            [c for c in k.completed() if c.op is Op.CAS],
            [Completion(3, 1, 0, Op.CAS, 0, True, 0, 5)])

    def test_same_request_id_with_different_cas_arguments_rejected(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.CAS, expected_value=0, new_value=5, txn_id=9)
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.CAS, expected_value=1, new_value=5, txn_id=9)
        self.assertEqual(cm.exception.field, "txn_id")
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.STORE, value=5, txn_id=9)

    def test_cas_validation_shape(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected_value=0)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected_value=0, new_value=True)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, value=0,
                        expected_value=0, new_value=1)


if __name__ == "__main__":
    unittest.main()
