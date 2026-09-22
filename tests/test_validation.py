"""输入校验与错误语义测试：NaN/Infinity、bool、可定位错误、批量原子性。"""

import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from msi_kernel import (
    CacheCoherenceKernel,
    InvalidMessageError,
    InvalidRequestError,
    Message,
    MsgType,
    Op,
    Request,
)


class TestScalarValidation(unittest.TestCase):
    def test_bool_rejected_as_integer(self):
        with self.assertRaises(InvalidRequestError) as cm:
            CacheCoherenceKernel().request(True, 0, Op.LOAD)
        self.assertEqual(cm.exception.field, "core")
        with self.assertRaises(InvalidRequestError) as cm:
            CacheCoherenceKernel().request(0, False, Op.LOAD)
        self.assertEqual(cm.exception.field, "addr")
        with self.assertRaises(InvalidRequestError) as cm:
            CacheCoherenceKernel().request(0, 0, Op.STORE, value=True)
        self.assertEqual(cm.exception.field, "value")

    def test_nan_and_infinity_rejected_with_clear_reason(self):
        for bad, name in [(float("nan"), "NaN"), (float("inf"), "Infinity"),
                          (float("-inf"), "Infinity")]:
            with self.assertRaises(InvalidRequestError) as cm:
                CacheCoherenceKernel().request(0, 0, Op.STORE, value=bad)
            self.assertIn(name, str(cm.exception))
            self.assertEqual(cm.exception.field, "value")

    def test_plain_float_rejected(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().request(0, 0, Op.STORE, value=1.0)

    def test_non_numeric_rejected(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().request("0", 0, Op.LOAD)
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().request(0, 0, "load")

    def test_range_errors(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(3, 0, Op.LOAD)
        self.assertEqual(cm.exception.field, "core")
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(-1, 0, Op.LOAD)
        self.assertEqual(cm.exception.field, "core")
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 8, Op.LOAD)
        self.assertEqual(cm.exception.field, "addr")

    def test_op_value_shape(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.LOAD, value=3)          # load 带值
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.STORE)                   # store 缺值

    def test_memory_validation(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(memory={0: float("nan")})
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(memory={0: True})
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(memory={9: 1})

    def test_txn_id_rules(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.LOAD, txn_id=0)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.LOAD, txn_id=True)
        k.request(0, 0, Op.LOAD, txn_id=7)
        with self.assertRaises(InvalidRequestError):
            k.request(1, 0, Op.LOAD, txn_id=7)          # 历史重复


class TestBatchAtomicity(unittest.TestCase):
    def test_invalid_batch_leaves_no_partial_change(self):
        k = CacheCoherenceKernel(memory={0: 0})
        good = [Request(0, 0, Op.STORE, value=1),
                Request(1, 0, Op.LOAD)]
        # 第三批元素非法（addr 越界，且带 request_seq=2）。
        bad_batch = good + [Request(0, 99, Op.LOAD)]
        with self.assertRaises(InvalidRequestError) as cm:
            k.request_batch(bad_batch)
        self.assertEqual(cm.exception.request_seq, 2)
        self.assertEqual(cm.exception.field, "addr")
        # 内核完全没接纳任何请求。
        s = k.snapshot()
        self.assertEqual(s.completed, ())
        self.assertEqual(s.pending_messages, ())
        self.assertIs(s.directory[0].state,
                      s.directory[0].state.__class__.INVALID)
        self.assertEqual(k.completed(), ())

    def test_duplicate_txn_id_within_batch_aborts(self):
        k = CacheCoherenceKernel()
        batch = [Request(0, 0, Op.LOAD, txn_id=5),
                 Request(1, 0, Op.LOAD, txn_id=5)]
        with self.assertRaises(InvalidRequestError) as cm:
            k.request_batch(batch)
        self.assertEqual(cm.exception.request_seq, 1)
        self.assertEqual(k.completed(), ())

    def test_batch_then_single_request_still_works(self):
        k = CacheCoherenceKernel(memory={0: 0})
        reps = k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=4),
        ])
        self.assertEqual(reps[0].txn_id, 1)
        rep = k.request(2, 0, Op.LOAD)
        self.assertEqual(rep.txn_id, 3)

    def test_non_iterable_batch(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().request_batch(123)
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().request_batch("abc")


class TestInjectedMessageValidation(unittest.TestCase):
    def _busy_kernel(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)
        return k

    def test_unknown_txn_id_injected(self):
        k = self._busy_kernel()
        msg = Message(None, 999, MsgType.INV_ACK, src=0, dst=3, addr=0)
        with self.assertRaises(InvalidMessageError) as cm:
            k.deliver(msg)
        self.assertEqual(cm.exception.field, "txn_id")

    def test_wrong_endpoints(self):
        k = self._busy_kernel()
        bad = Message(None, 2, MsgType.INV_ACK, src=3, dst=3, addr=0)
        with self.assertRaises(InvalidMessageError):
            k.deliver(bad)
        bad2 = Message(None, 2, MsgType.INV, src=0, dst=3, addr=0)
        with self.assertRaises(InvalidMessageError):
            k.deliver(bad2)

    def test_flush_requires_int_value(self):
        k = CacheCoherenceKernel()
        k.request(0, 0, Op.STORE, value=5)
        k.request(1, 0, Op.LOAD)
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 2, MsgType.FLUSH, src=0, dst=3,
                              addr=0, value=float("nan")))

    def test_non_flush_carries_no_value(self):
        k = self._busy_kernel()
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 2, MsgType.INV_ACK, src=0, dst=3,
                              addr=0, value=1))

    def test_injected_msg_id_must_be_none(self):
        k = self._busy_kernel()
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(5, 2, MsgType.INV_ACK, src=0, dst=3, addr=0))

    def test_deliver_wrong_type(self):
        with self.assertRaises(InvalidMessageError):
            CacheCoherenceKernel().deliver("1")
        with self.assertRaises(InvalidMessageError):
            CacheCoherenceKernel().deliver(True)  # bool 不当整数


if __name__ == "__main__":
    unittest.main()
