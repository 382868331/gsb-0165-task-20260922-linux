"""内核协议行为与错误语义测试。"""

from __future__ import annotations

import math
import unittest

from msi_kernel import (
    CoherenceKernel,
    KernelError,
    Message,
    MsgType,
    NUM_ADDR,
    NUM_CORES,
)


def drain(kernel: CoherenceKernel, order=0, max_steps: int = 500) -> None:
    """投递所有待发消息直到静止。order 选择取队首/队尾制造不同投递顺序。"""
    steps = 0
    while not kernel.is_quiescent():
        pending = kernel.pending_messages()
        if not pending:
            break
        msg = pending[0] if order == 0 else pending[-1]
        kernel.deliver(msg)
        steps += 1
        if steps > max_steps:
            raise AssertionError("kernel did not quiesce")
    kernel.check_invariants()


def cache_line(k, core, addr):
    return k.snapshot().caches[core][addr]


def dir_line(k, addr):
    return k.snapshot().directory[addr]


class ProtocolFlowTests(unittest.TestCase):
    def test_first_store_then_state_m(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 10)
        drain(k)
        self.assertEqual(cache_line(k, 0, 0).state, "M")
        self.assertEqual(cache_line(k, 0, 0).value, 10)
        d = dir_line(k, 0)
        self.assertEqual((d.state, d.owner, set(d.sharers)), ("M", 0, set()))

    def test_read_m_flushes_owner_and_shares(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 10)
        drain(k)
        k.request("GETS", 2, 1, 0)
        drain(k)
        self.assertEqual(cache_line(k, 0, 0).state, "S")
        self.assertEqual(cache_line(k, 1, 0).state, "S")
        d = dir_line(k, 0)
        self.assertEqual((d.state, d.owner, set(d.sharers), d.value),
                         ("S", None, {0, 1}, 10))
        op = k.snapshot().completed[-1]
        self.assertEqual((op.kind, op.value), ("LOAD", 10))

    def test_upgrade_waits_for_all_invalidation_acks(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        for c in (1, 2):
            k.request("GETS", 10 + c, c, 0)
            drain(k)
        # c1 升级，目录必须向 c0、c2 发 INV；任一未 ACK 前升级不完成。
        k.request("UPGR", 20, 1, 0, 77)
        pending = k.pending_messages()
        invs = [m for m in pending if m.mtype is MsgType.INV]
        self.assertEqual({m.dst for m in invs}, {0, 2})
        self.assertNotIn(20, {op.xid for op in k.snapshot().completed})
        for inv in invs:
            k.deliver(inv)
        # ACK 全部在途，仍未完成
        self.assertEqual(cache_line(k, 1, 0).state, "S")
        acks = [m for m in k.pending_messages() if m.mtype is MsgType.INV_ACK]
        self.assertEqual(len(acks), 2)
        k.deliver(acks[0])
        self.assertEqual(cache_line(k, 1, 0).state, "S")  # 还差一个 ACK
        k.deliver(acks[1])
        drain(k)
        self.assertEqual(cache_line(k, 1, 0).state, "M")
        self.assertEqual(cache_line(k, 1, 0).value, 77)
        self.assertEqual(cache_line(k, 0, 0).state, "I")
        self.assertEqual(cache_line(k, 2, 0).state, "I")
        self.assertEqual(dir_line(k, 0).state, "M")
        self.assertEqual(dir_line(k, 0).owner, 1)

    def test_getx_from_m_fetches_and_invalidates(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 11)
        drain(k)
        k.local_store(0, 0, 22)  # 脏写
        k.request("GETX", 2, 2, 0, 33)
        drain(k)
        self.assertEqual(cache_line(k, 0, 0).state, "I")
        self.assertEqual(cache_line(k, 2, 0).state, "M")
        self.assertEqual(cache_line(k, 2, 0).value, 33)
        self.assertEqual(dir_line(k, 0).owner, 2)
        # owner 脏值已回写内存，随后读者看到 33（新 owner 持有的值）
        k.request("GETS", 3, 1, 0)
        drain(k)
        self.assertEqual(k.snapshot().completed[-1].value, 33)

    def test_per_address_queue_independent(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        # a0 的 c1 读需要 FETCH，过程中 a1 的事务独立完成。
        k.request("GETS", 2, 1, 0)
        k.request("GETX", 3, 2, 1, 9)
        fetch = [m for m in k.pending_messages() if m.mtype is MsgType.FETCH][0]
        # 先把 a1 全部办完（地址独立）
        for _ in range(3):
            for m in list(k.pending_messages()):
                if m.addr == 1:
                    k.deliver(m)
        k.check_invariants()
        self.assertEqual(dir_line(k, 1).state, "M")
        self.assertEqual(dir_line(k, 1).owner, 2)
        # a0 仍在等待 flush
        self.assertEqual(dir_line(k, 0).state, "M")
        k.deliver(fetch)
        drain(k)
        self.assertEqual(dir_line(k, 0).state, "S")

    def test_grant_ack_is_per_address_barrier(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 5)
        drain(k)
        # c1 的读与 c2 的写在同地址排队
        k.request("GETS", 2, 1, 0)
        k.request("GETX", 3, 2, 0, 6)
        # 把 c1 的 FETCH/FLUSH/DATA 都处理掉，但扣留 c1 的 GRANT_ACK
        while True:
            pend = [m for m in k.pending_messages()
                    if not (m.mtype is MsgType.GRANT_ACK and m.xid == 2)]
            if not pend:
                break
            k.deliver(pend[0])
        # c1 已收 DATA 为 S，但 GRANT_ACK 未到：c2 的事务绝不能启动
        self.assertEqual(cache_line(k, 1, 0).state, "S")
        self.assertFalse(
            any(m.xid == 3 for m in k.pending_messages()),
            "queued transaction must not emit before prior GRANT_ACK",
        )
        self.assertEqual(tuple(k.snapshot().queued_xids[0]), (2, 3))
        ack = [m for m in k.pending_messages()
               if m.mtype is MsgType.GRANT_ACK and m.xid == 2][0]
        k.deliver(ack)  # 屏障解除，c2 的 FETCHX 链开始
        self.assertTrue(any(m.xid == 3 for m in k.pending_messages()))
        drain(k)
        self.assertEqual(cache_line(k, 2, 0).state, "M")
        self.assertEqual(dir_line(k, 0).owner, 2)

    def test_same_core_different_addresses_concurrent(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 5)  # 不 drain
        # 同一核 c0 对另一地址的事务并行开放，消息交错推进
        k.request("GETS", 2, 0, 1)
        by_addr = {0: [], 1: []}
        steps = 0
        while not k.is_quiescent():
            pend = k.pending_messages()
            self.assertTrue(pend)
            k.deliver(pend[0])
            steps += 1
            self.assertLess(steps, 100)
        k.check_invariants()
        self.assertEqual(cache_line(k, 0, 0).state, "M")
        self.assertEqual(cache_line(k, 0, 1).state, "S")
        self.assertEqual(dir_line(k, 0).owner, 0)
        self.assertEqual(dir_line(k, 1).state, "S")

    def test_same_address_requests_queue_in_order(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 5)
        drain(k)
        k.request("GETS", 2, 1, 0)
        k.request("GETX", 3, 2, 0, 6)  # 同地址排队
        self.assertEqual(tuple(k.snapshot().queued_xids[0]), (2, 3))
        drain(k)
        # 队列顺序：c1 先共享，随后 c2 FETCHX c1，最终 c2 为 M
        self.assertEqual(cache_line(k, 1, 0).state, "I")
        self.assertEqual(cache_line(k, 2, 0).state, "M")
        self.assertEqual(dir_line(k, 0).owner, 2)
        xids = [op.xid for op in k.snapshot().completed]
        self.assertLess(xids.index(2), xids.index(3))

    def test_completed_load_value_recorded(self):
        k = CoherenceKernel([42] * NUM_ADDR)
        k.request("GETS", 1, 0, 3)
        drain(k)
        op = k.snapshot().completed[-1]
        self.assertEqual((op.kind, op.core, op.addr, op.value), ("LOAD", 0, 3, 42))

    def test_local_hits(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 5)
        drain(k)
        self.assertEqual(k.local_load(0, 0).value, 5)
        k.local_store(0, 0, 6)
        self.assertEqual(cache_line(k, 0, 0).value, 6)
        with self.assertRaises(KernelError):
            k.local_load(1, 0)  # I 不能命中读
        with self.assertRaises(KernelError):
            k.local_store(1, 0, 1)  # 非 M 不能命中写


class IdempotencyTests(unittest.TestCase):
    def test_duplicate_delivery_is_noop(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 10)
        drain(k)
        k.request("GETS", 2, 1, 0)
        flush = [m for m in k.pending_messages() if m.mtype is MsgType.FLUSH]
        # 触发 FETCH 后重放 FLUSH 两次：第二次必须为空操作
        fetch = [m for m in k.pending_messages() if m.mtype is MsgType.FETCH][0]
        k.deliver(fetch)
        flush_msg = [m for m in k.pending_messages() if m.mtype is MsgType.FLUSH][0]
        self.assertEqual(k.deliver(flush_msg)[0].mtype, MsgType.DATA)
        self.assertEqual(k.deliver(flush_msg), [])  # 幂等
        drain(k)
        self.assertEqual(dir_line(k, 0).state, "S")

    def test_duplicate_inv_ack_noop(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        k.request("GETS", 2, 1, 0)
        drain(k)
        k.request("UPGR", 3, 0, 0, 8)
        inv = [m for m in k.pending_messages() if m.mtype is MsgType.INV][0]
        k.deliver(inv)
        ack = [m for m in k.pending_messages() if m.mtype is MsgType.INV_ACK][0]
        k.deliver(ack)
        drain(k)
        self.assertEqual(cache_line(k, 0, 0).state, "M")
        # 同一条 ACK 任何时候重投都是幂等空操作（即使事务已结束）
        self.assertEqual(k.deliver(ack), [])
        self.assertEqual(dir_line(k, 0).owner, 0)
        # 但伪造一个“看起来属于旧事务”的不同消息：拒绝，且不推进状态
        forged_old = Message(MsgType.INV_ACK, 999, 1, 3, 0, aid=0)
        with self.assertRaises(KernelError):
            k.deliver(forged_old)

    def test_stale_ack_cannot_advance_new_transaction(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        k.request("GETS", 2, 1, 0)
        drain(k)
        # 第一个升级事务 xid=3 的 ACK（aid=0）
        k.request("UPGR", 3, 0, 0, 8)
        inv1 = [m for m in k.pending_messages() if m.mtype is MsgType.INV][0]
        k.deliver(inv1)
        stale = Message(MsgType.INV_ACK, 3, 1, 3, 0, aid=0)
        k.deliver(stale)
        drain(k)
        self.assertEqual(dir_line(k, 0).owner, 0)
        # 再来一轮失效（新事务 xid=4，新 aid=1），旧 ACK 必须被拒绝
        k.request("GETS", 5, 2, 0)
        drain(k)
        k.request("UPGR", 6, 0, 0, 9)
        inv2 = [m for m in k.pending_messages() if m.mtype is MsgType.INV][0]
        self.assertEqual(inv2.aid, 1)
        # 伪造旧事务 ACK 投给目录：未知事务，拒绝
        forged_old = Message(MsgType.INV_ACK, 999, 2, 3, 0, aid=0)
        with self.assertRaises(KernelError):
            k.deliver(forged_old)
        # 伪造当前事务错误 aid：拒绝且不推进
        forged_aid = Message(MsgType.INV_ACK, 6, 2, 3, 0, aid=999)
        with self.assertRaises(KernelError):
            k.deliver(forged_aid)
        self.assertEqual(cache_line(k, 0, 0).state, "S")
        # 真正的 ACK 才能推进
        k.deliver(inv2)
        good = [m for m in k.pending_messages() if m.mtype is MsgType.INV_ACK][0]
        k.deliver(good)
        drain(k)
        self.assertEqual(cache_line(k, 0, 0).state, "M")
        self.assertEqual(cache_line(k, 0, 0).value, 9)


class ValidationTests(unittest.TestCase):
    def test_reject_bool_integer_params(self):
        k = CoherenceKernel()
        with self.assertRaises(KernelError):
            k.request("GETX", 1, True, 0, 10)
        with self.assertRaises(KernelError):
            k.request("GETX", 1, 0, 0, False)
        with self.assertRaises(KernelError):
            CoherenceKernel(memory=[True] * NUM_ADDR)

    def test_reject_nan_infinity(self):
        k = CoherenceKernel()
        for bad in (math.nan, math.inf, -math.inf):
            with self.assertRaises(KernelError):
                k.request("GETX", 1, 0, 0, bad)
            with self.assertRaises(KernelError):
                Message(MsgType.DATA_M, 1, 3, 0, 0, value=bad)

    def test_reject_out_of_range(self):
        k = CoherenceKernel()
        with self.assertRaises(KernelError):
            k.request("GETX", 1, NUM_CORES, 0, 1)
        with self.assertRaises(KernelError):
            k.request("GETX", 1, 0, NUM_ADDR, 1)
        with self.assertRaises(KernelError):
            k.request("GETX", -1, 0, 0, 1)

    def test_reject_wrong_request_state(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        with self.assertRaises(KernelError):
            k.request("GETS", 2, 0, 0)       # M 上不能 GETS
        with self.assertRaises(KernelError):
            k.request("UPGR", 3, 0, 0, 2)    # M 上不能 UPGR
        k.request("GETS", 4, 1, 0)
        drain(k)
        with self.assertRaises(KernelError):
            k.request("GETX", 5, 1, 0, 3)    # S 上不能 GETX（应走 UPGR）
        # 写请求缺写值必须拒绝
        with self.assertRaises(KernelError):
            k.request("GETX", 6, 2, 1)
        # 读请求带值必须拒绝
        with self.assertRaises(KernelError):
            k.request("GETS", 7, 2, 2, 9)

    def test_reject_duplicate_xid_reuse(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        with self.assertRaises(KernelError):
            k.request("GETX", 1, 1, 0, 2)  # xid 已被占用/使用

    def test_reject_same_core_addr_busy(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)  # 不 drain，(c0,a0) 事务未完成
        with self.assertRaises(KernelError):
            k.request("GETS", 2, 0, 0)  # 同一（核, 地址）忙
        # 同一核对不同地址可以并发开放（地址独立）
        msgs = k.request("GETS", 3, 0, 1)
        self.assertTrue(any(m.addr == 1 for m in msgs))
        drain(k)
        self.assertTrue(k.is_quiescent())

    def test_failed_request_leaves_no_partial_change(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        drain(k)
        before = k.canonical()
        with self.assertRaises(KernelError):
            k.request("GETS", 2, 0, 0)  # M 状态非法 GETS
        self.assertEqual(k.canonical(), before)
        with self.assertRaises(KernelError):
            k.request("GETX", True, 1, 0, 1)
        self.assertEqual(k.canonical(), before)

    def test_bad_memory_length(self):
        with self.assertRaises(KernelError):
            CoherenceKernel(memory=[1, 2, 3])

    def test_message_construction_validation(self):
        with self.assertRaises(KernelError):
            Message(MsgType.DATA_M, 1, 3, 0, 0)  # DATA_M 缺 value
        with self.assertRaises(KernelError):
            Message(MsgType.INV, 1, 3, 0, 0)  # INV 缺 aid
        with self.assertRaises(KernelError):
            Message(MsgType.INV_ACK, 1, 0, 3, 0)  # INV_ACK 缺 aid
        with self.assertRaises(KernelError):
            Message(MsgType.FLUSH, 1, 0, 3, 0)  # FLUSH 缺 value
        with self.assertRaises(KernelError):
            Message(MsgType.GETS, 1, 3, 3, 0)  # 方向错误
        with self.assertRaises(KernelError):
            Message(MsgType.DATA, 1, 3, 0, 0, value=5, aid=2)  # DATA 带 aid
        with self.assertRaises(KernelError):
            Message("X", 1, 3, 0, 0)  # type: ignore[arg-type]

    def test_deliver_by_index_and_bad_index(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 10)
        n = len(k._outgoing)  # type: ignore[attr-defined]
        self.assertGreaterEqual(n, 1)
        k.deliver(0)  # 下标投递 DATA_M
        with self.assertRaises(KernelError):
            k.deliver(n + 5)  # 越界
        with self.assertRaises(KernelError):
            k.deliver(True)  # bool 不是下标
        drain(k)
        self.assertTrue(k.is_quiescent())


class SnapshotTests(unittest.TestCase):
    def test_snapshot_exposes_all_outputs(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 10)
        snap = k.snapshot()
        self.assertEqual(len(snap.caches), NUM_CORES)
        self.assertEqual(len(snap.caches[0]), NUM_ADDR)
        self.assertEqual(len(snap.directory), NUM_ADDR)
        self.assertTrue(any(m.mtype is MsgType.DATA_M for m in snap.pending_messages))
        drain(k)
        self.assertEqual(k.snapshot().pending_messages, ())
        self.assertTrue(k.is_quiescent())


if __name__ == "__main__":
    unittest.main()
