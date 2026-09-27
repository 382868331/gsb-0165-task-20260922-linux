"""丢失 / 超时重传 / 重复到达的幂等与顺序保证测试。"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import unittest

from msi_kernel import (
    DIR,
    CacheCoherenceKernel,
    Completion,
    DeliveryStatus,
    InvalidMessageError,
    InvalidRequestError,
    Message,
    MsgType,
    Op,
    Request,
    State,
    drain,
)


def _only_pending(k, mtype=None):
    msgs = [m for m in k.pending() if mtype is None or m.mtype is mtype]
    assert len(msgs) == 1, msgs
    return msgs[0]


class TestLossAndRetransmit(unittest.TestCase):
    def test_lost_inv_is_retransmitted_on_timeout(self):
        k = CacheCoherenceKernel(memory={0: 0}, timeout=5)
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)          # 等核0 的 ACK
        inv = _only_pending(k, MsgType.INV)
        k.lose(inv.msg_id)                           # 网络丢包
        self.assertEqual(k.pending(), ())
        self.assertEqual(k.completed()[-1].op, Op.LOAD)  # store 卡住了

        rep = k.tick(4)                              # 未到超时，不重传
        self.assertEqual(rep.retransmitted, ())
        self.assertEqual(rep.clock, 4)
        rep = k.tick(1)                              # 到达 timeout=5，重传
        self.assertEqual(len(rep.retransmitted), 1)
        copy_inv = rep.retransmitted[0]
        self.assertNotEqual(copy_inv.msg_id, inv.msg_id)
        self.assertEqual((copy_inv.mtype, copy_inv.src, copy_inv.dst,
                          copy_inv.addr, copy_inv.txn_id),
                         (MsgType.INV, DIR, 0, 0, inv.txn_id))

        ack = k.deliver(copy_inv.msg_id).emitted[0]  # 重传副本生效
        fin = k.deliver(ack.msg_id)
        self.assertEqual(fin.completions,
                         (Completion(2, 1, 0, Op.STORE, 5),))
        self.assertIs(k.snapshot().directory[0].state, State.MODIFIED)

    def test_lost_flush_is_retransmitted_and_writeback_happens_once(self):
        k = CacheCoherenceKernel(memory={0: 0}, timeout=3)
        k.request(0, 0, Op.STORE, value=77)
        k.request(1, 0, Op.LOAD)                     # FETCH/FLUSH 流程
        fetch = _only_pending(k, MsgType.FETCH)
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.lose(flush.msg_id)                         # 回写丢失
        self.assertEqual(k.snapshot().memory[0], 0)  # 内存仍是旧值

        # 目录超时重传 FETCH；owner 已降级 S，幂等补发一份 FLUSH。
        copy_fetch = k.tick(3).retransmitted[0]
        self.assertEqual(copy_fetch.mtype, MsgType.FETCH)
        flush2 = k.deliver(copy_fetch.msg_id).emitted[0]
        self.assertEqual((flush2.mtype, flush2.value), (MsgType.FLUSH, 77))
        rep = k.deliver(flush2.msg_id)
        self.assertEqual(rep.completions,
                         (Completion(2, 1, 0, Op.LOAD, 77),))
        self.assertEqual(k.snapshot().memory[0], 77)
        loads = [c for c in k.completed()
                 if c.op is Op.LOAD and c.txn_id == 2]
        self.assertEqual(len(loads), 1)              # 回写只完成一次

    def test_lost_ack_retransmit_via_duplicate_inv(self):
        # INV 到达但 ACK 丢失：目录超时重传 INV，核端幂等补发 ACK。
        k = CacheCoherenceKernel(memory={0: 0}, timeout=2)
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=9)
        inv = _only_pending(k, MsgType.INV)
        ack = k.deliver(inv.msg_id).emitted[0]
        k.lose(ack.msg_id)                           # ACK 丢失
        self.assertIs(k.snapshot().caches[0][0].state, State.INVALID)
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (0,))

        copy_inv = k.tick(2).retransmitted[0]        # 目录重传 INV
        self.assertEqual(copy_inv.mtype, MsgType.INV)
        rep = k.deliver(copy_inv.msg_id)             # 重复 INV：幂等补 ACK
        self.assertIs(rep.status, DeliveryStatus.APPLIED)
        self.assertEqual([m.mtype for m in rep.emitted], [MsgType.INV_ACK])
        fin = k.deliver(rep.emitted[0].msg_id)
        self.assertEqual(fin.completions,
                         (Completion(2, 1, 0, Op.STORE, 9),))

    def test_lose_validation(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=1)
        inv = _only_pending(k, MsgType.INV)
        with self.assertRaises(InvalidMessageError):
            k.lose(9999)                             # 不存在
        with self.assertRaises(InvalidMessageError):
            k.lose(True)                             # bool 拒绝
        k.deliver(inv.msg_id)
        with self.assertRaises(InvalidMessageError):
            k.lose(inv.msg_id)                       # 已投递，不能丢失

    def test_tick_validation(self):
        k = CacheCoherenceKernel()
        for bad in (0, -1, True, 1.5, float("nan")):
            with self.assertRaises(InvalidRequestError):
                k.tick(bad)
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(timeout=0)
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(timeout=True)


class TestDuplicateArrival(unittest.TestCase):
    def test_duplicate_inv_copy_does_not_invalidate_twice(self):
        # 原件与重传副本都到达：第二次只补 ACK，写入只提交一次。
        k = CacheCoherenceKernel(memory={0: 0}, timeout=2)
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)
        inv = _only_pending(k, MsgType.INV)
        copy_inv = k.tick(2).retransmitted[0]
        ack1 = k.deliver(inv.msg_id).emitted[0]      # 原件先生效
        rep = k.deliver(copy_inv.msg_id)             # 副本迟到：幂等
        self.assertIs(rep.status, DeliveryStatus.APPLIED)
        self.assertEqual([m.mtype for m in rep.emitted], [MsgType.INV_ACK])
        self.assertIs(k.snapshot().caches[0][0].state, State.INVALID)
        k.deliver(ack1.msg_id)                       # 第一个 ACK 提交 store
        self.assertEqual(k.completed()[-1],
                         Completion(2, 1, 0, Op.STORE, 5))
        dup_ack = _only_pending(k, MsgType.INV_ACK)
        rep = k.deliver(dup_ack.msg_id)              # 重复 ACK：陈旧忽略
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        stores = [c for c in k.completed() if c.op is Op.STORE]
        self.assertEqual(len(stores), 1)             # 写入只执行一遍

    def test_duplicate_fetch_and_flush_writeback_once(self):
        k = CacheCoherenceKernel(memory={0: 0}, timeout=2)
        k.request(0, 0, Op.STORE, value=42)
        k.request(1, 0, Op.LOAD)
        fetch = _only_pending(k, MsgType.FETCH)
        copy_fetch = k.tick(2).retransmitted[0]
        flush1 = k.deliver(fetch.msg_id).emitted[0]  # 原件：降级 S + FLUSH
        rep = k.deliver(copy_fetch.msg_id)           # 副本：幂等补 FLUSH
        self.assertIs(rep.status, DeliveryStatus.APPLIED)
        flush2 = rep.emitted[0]
        self.assertEqual((flush2.mtype, flush2.value), (MsgType.FLUSH, 42))
        self.assertIs(k.snapshot().caches[0][0].state, State.SHARED)

        k.deliver(flush1.msg_id)                     # 第一份回写生效
        self.assertEqual(k.snapshot().memory[0], 42)
        rep = k.deliver(flush2.msg_id)               # 重复回写：陈旧忽略
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().memory[0], 42)
        loads = [c for c in k.completed() if c.txn_id == 2]
        self.assertEqual(len(loads), 1)              # load 只完成一次

    def test_same_msg_id_redelivery_still_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)
        inv = _only_pending(k, MsgType.INV)
        k.deliver(inv.msg_id)
        rep = k.deliver(inv.msg_id)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_DUPLICATE)


class TestStaleAckAfterRetransmit(unittest.TestCase):
    def test_late_old_ack_cannot_complete_new_request(self):
        # 同地址前后两个升级事务；旧事务的迟到 ACK 不得完成新事务。
        k = CacheCoherenceKernel(memory={0: 0}, timeout=2)
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=11),       # txn4：等核0、核2 ACK
        ])
        inv0 = next(m for m in k.pending() if m.dst == 0)
        ack0 = k.deliver(inv0.msg_id).emitted[0]
        k.lose(ack0.msg_id)                          # 核0 的 ACK 丢失
        # 目录超时向所有未 ACK 的核重传 INV；核0 幂等补 ACK。
        copies = k.tick(2).retransmitted
        copy_inv0 = next(m for m in copies if m.dst == 0)
        ack0b = k.deliver(copy_inv0.msg_id).emitted[0]
        k.deliver(ack0b.msg_id)
        # 核2 的 INV 原件与重传副本都在网络中：都投递，幂等只生效一次。
        for m in [m for m in k.pending() if m.mtype is MsgType.INV]:
            k.deliver(m.msg_id)
        for m in [m for m in k.pending() if m.mtype is MsgType.INV_ACK]:
            k.deliver(m.msg_id)
        self.assertEqual(k.completed()[-1],
                         Completion(4, 1, 0, Op.STORE, 11))

        # 同地址新请求：核2 写核1 持有的 M（txn5，先 FETCH 回写）。
        k.request(2, 0, Op.STORE, value=22)
        self.assertEqual(k.snapshot().directory[0].active_txn, 5)
        # 迟到的旧 ACK（属于已结束的 txn4）：不得完成 txn5。
        late = Message(None, 4, MsgType.INV_ACK, src=1, dst=DIR, addr=0)
        rep = k.deliver(late)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertTrue(k.snapshot().directory[0].awaiting_flush)
        self.assertEqual(k.completed()[-1].value, 11)
        # 正常流程：FETCH -> FLUSH -> INV -> ACK 才完成新事务。
        fetch = _only_pending(k, MsgType.FETCH)
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.deliver(flush.msg_id)
        real_inv = _only_pending(k, MsgType.INV)
        real_ack = k.deliver(real_inv.msg_id).emitted[0]
        rep = k.deliver(real_ack.msg_id)
        self.assertEqual(rep.completions,
                         (Completion(5, 2, 0, Op.STORE, 22),))

    def test_late_old_flush_does_not_pollute_new_txn(self):
        k = CacheCoherenceKernel(memory={0: 0}, timeout=2)
        k.request(0, 0, Op.STORE, value=7)
        k.request(1, 0, Op.LOAD)                     # txn2：FETCH/FLUSH
        fetch = _only_pending(k, MsgType.FETCH)
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.lose(flush.msg_id)                         # FLUSH 丢失
        copy_fetch = k.tick(2).retransmitted[0]      # 超时重传 FETCH
        flush2 = k.deliver(copy_fetch.msg_id).emitted[0]  # owner 补发 FLUSH
        k.deliver(flush2.msg_id)                     # txn2 完成，S={0,1}
        k.request(1, 0, Op.STORE, value=8)           # txn3：失效核0
        # 迟到的旧 FLUSH（属于已结束的 txn2）：不得污染内存与新事务。
        late = Message(None, 2, MsgType.FLUSH, src=0, dst=DIR, addr=0,
                       value=7)
        rep = k.deliver(late)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().memory[0], 7)
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (0,))


class TestOrderingPreserved(unittest.TestCase):
    def test_fifo_order_with_retransmission(self):
        # 重传参与下，同地址 FIFO 完成顺序与读值不变。
        k = CacheCoherenceKernel(memory={0: 100}, timeout=2)
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=200),      # 等核0 ACK
            Request(2, 0, Op.LOAD),                  # 排队
        ])
        inv = _only_pending(k, MsgType.INV)
        k.lose(inv.msg_id)                           # 丢失 -> 重传
        copy_inv = k.tick(2).retransmitted[0]
        ack = k.deliver(copy_inv.msg_id).emitted[0]
        k.deliver(ack.msg_id)                        # store 完成，txn4 启动
        fetch = _only_pending(k, MsgType.FETCH)
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.deliver(flush.msg_id)
        done = [(c.op.value, c.value) for c in k.completed()]
        self.assertEqual(done, [("load", 100), ("load", 100),
                                ("store", 200), ("load", 200)])

    def test_drain_still_works_without_ticks(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)
        k.request(2, 0, Op.LOAD)
        drain(k)                                     # 不 tick 就不会重传
        self.assertEqual(k.completed()[-1].value, 5)
        self.assertEqual(k.clock, 0)


if __name__ == "__main__":
    unittest.main()
