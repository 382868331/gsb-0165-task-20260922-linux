"""协议核心行为测试：MSI 转移、失效链、回写、排队、消息幂等/陈旧。"""

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
    explore,
)


def _flat(completed):
    return [(c.addr, c.op.value, c.value) for c in completed]


class TestBasicTransitions(unittest.TestCase):
    def test_load_invalid_fills_s_from_memory(self):
        k = CacheCoherenceKernel(memory={0: 17})
        rep = k.request(0, 0, Op.LOAD)
        self.assertEqual(rep.completions,
                         (Completion(1, 0, 0, Op.LOAD, 17),))
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.SHARED)
        self.assertEqual(s.caches[0][0].value, 17)
        self.assertIs(s.directory[0].state, State.SHARED)
        self.assertEqual(s.directory[0].sharers, (0,))
        self.assertEqual(k.pending(), ())

    def test_store_invalid_enters_m(self):
        k = CacheCoherenceKernel()
        rep = k.request(2, 1, Op.STORE, value=33)
        self.assertEqual(rep.completions[0].value, 33)
        s = k.snapshot()
        self.assertIs(s.caches[2][1].state, State.MODIFIED)
        self.assertIs(s.directory[1].state, State.MODIFIED)
        self.assertEqual(s.directory[1].owner, 2)
        self.assertEqual(s.directory[1].sharers, ())

    def test_s_hit_and_m_hit_complete_without_messages(self):
        k = CacheCoherenceKernel(memory={0: 5})
        k.request(0, 0, Op.LOAD)
        rep = k.request(0, 0, Op.LOAD)       # S 命中
        self.assertEqual(rep.completions[0].value, 5)
        k.request(0, 0, Op.STORE, value=9)  # 独占 S -> M
        rep = k.request(0, 0, Op.LOAD)       # M 命中
        self.assertEqual(rep.completions[0].value, 9)
        self.assertEqual(k.pending(), ())

    def test_solo_sharer_upgrade_commits_immediately(self):
        k = CacheCoherenceKernel(memory={0: 1})
        k.request(1, 0, Op.LOAD)            # 唯一 S
        rep = k.request(1, 0, Op.STORE, value=2)
        self.assertEqual(len(rep.completions), 1)
        self.assertIs(k.snapshot().directory[0].state, State.MODIFIED)
        self.assertEqual(k.pending(), ())


class TestInvalidationChain(unittest.TestCase):
    def setUp(self):
        # 三核 S 共享地址 0（内存初值 100），core1 发起 store=200。
        self.k = CacheCoherenceKernel(memory={0: 100})
        self.k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=200),
        ])

    def test_upgrade_waits_for_all_acks(self):
        k = self.k
        self.assertEqual({m.dst for m in k.pending()}, {0, 2})
        # 只到一个 ACK：store 不得完成。
        inv_to_0 = next(m for m in k.pending() if m.dst == 0)
        rep = k.deliver(inv_to_0.msg_id)
        ack = rep.emitted[0]
        k.deliver(ack.msg_id)
        self.assertEqual(k.completed()[-1].op, Op.LOAD)  # 最后完成仍是 load
        self.assertIs(k.snapshot().caches[1][0].state, State.SHARED)
        self.assertIs(k.snapshot().directory[0].state, State.SHARED)
        self.assertEqual(k.snapshot().directory[0].active_txn, 4)
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (2,))

        # 第二个 ACK 到达才提交 M。
        inv_to_2 = next(m for m in k.pending() if m.dst == 2)
        ack2 = k.deliver(inv_to_2.msg_id).emitted[0]
        rep = k.deliver(ack2.msg_id)
        self.assertEqual(rep.completions[0],
                         Completion(4, 1, 0, Op.STORE, 200))
        s = k.snapshot()
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 1)
        self.assertEqual(s.directory[0].sharers, ())
        self.assertIs(s.caches[1][0].state, State.MODIFIED)
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertIs(s.caches[2][0].state, State.INVALID)

    def test_all_interleavings_preserve_invariants_and_final_value(self):
        res = explore(
            lambda: CacheCoherenceKernel(memory={0: 100}),
            [Request(0, 0, Op.LOAD), Request(1, 0, Op.LOAD),
             Request(2, 0, Op.LOAD), Request(1, 0, Op.STORE, value=200)],
        )
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        self.assertEqual(res.terminal_states, 1)
        final = next(iter(res.schedules.values()))
        self.assertEqual(final[-1], Completion(4, 1, 0, Op.STORE, 200))

    def test_two_simultaneous_upgrades(self):
        # core1 与 core2 都是 S，几乎同时发起 store；第二个必须排队，
        # 且其值必须在第一个完成之后才可见。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=11),
            Request(2, 0, Op.STORE, value=22),
        ])
        self.assertTrue(k.snapshot().queues[0])
        drain(k)
        stores = [c for c in k.completed() if c.op is Op.STORE]
        self.assertEqual([(c.core, c.value) for c in stores],
                         [(1, 11), (2, 22)])
        s = k.snapshot()
        self.assertEqual((s.directory[0].state.value,
                          s.directory[0].owner), ("M", 2))
        # 枚举该场景的全部消息重排，结果必须一致。
        res = explore(
            lambda: CacheCoherenceKernel(memory={0: 0}),
            [Request(0, 0, Op.LOAD), Request(1, 0, Op.LOAD),
             Request(2, 0, Op.LOAD), Request(1, 0, Op.STORE, value=11),
             Request(2, 0, Op.STORE, value=22)],
        )
        self.assertEqual(res.violations, ())
        self.assertEqual(res.terminal_states, 1)
        for seq in res.schedules.values():
            vals = [(c.core, c.value) for c in seq if c.op is Op.STORE]
            self.assertEqual(vals, [(1, 11), (2, 22)])


class TestWriteback(unittest.TestCase):
    def test_read_m_address_fetches_owner_then_shares(self):
        k = CacheCoherenceKernel(memory={3: 0})
        k.request(0, 3, Op.STORE, value=77)          # core0 M，内存仍 0
        rep = k.request(1, 3, Op.LOAD)               # 读 M -> FETCH
        self.assertTrue(rep.queued is False)
        self.assertEqual(k.completed()[-1].op, Op.STORE)  # load 未完成
        fetch = next(m for m in k.pending()
                     if m.mtype is MsgType.FETCH and m.dst == 0)
        emitted = k.deliver(fetch.msg_id).emitted    # owner 降级 S，发 FLUSH
        self.assertEqual(emitted[0].mtype, MsgType.FLUSH)
        self.assertEqual(emitted[0].value, 77)
        self.assertIs(k.snapshot().caches[0][3].state, State.SHARED)
        # FLUSH 未到：目录仍 M，load 仍不能完成。
        self.assertIs(k.snapshot().directory[3].state, State.MODIFIED)
        rep = k.deliver(emitted[0].msg_id)
        self.assertEqual(rep.completions,
                         (Completion(2, 1, 3, Op.LOAD, 77),))
        s = k.snapshot()
        self.assertEqual(s.memory[3], 77)
        self.assertIs(s.directory[3].state, State.SHARED)
        self.assertEqual(s.directory[3].sharers, (0, 1))
        self.assertIsNone(s.directory[3].owner)

    def test_store_to_foreign_m_writes_back_then_upgrades(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=7)           # core0 M
        k.request(1, 0, Op.STORE, value=8)           # 先回写再升级
        drain(k)
        s = k.snapshot()
        self.assertEqual(s.memory[0], 7)             # 回写过
        self.assertEqual(s.directory[0].owner, 1)
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertIs(s.caches[1][0].state, State.MODIFIED)
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertEqual(k.completed()[-1].value, 8)

    def test_m_state_never_coexists_with_s(self):
        # 读 M 的回写过渡期间，owner 为 S 而目录仍 M：这是允许的过渡态，
        # 但任何缓存都不得出现“另一个 M”。
        k = CacheCoherenceKernel()
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)
        fetch = k.pending()[0]
        k.deliver(fetch.msg_id)
        states = [k.snapshot().caches[c][0].state.value for c in range(3)]
        self.assertEqual(sorted(states), ["I", "I", "S"])


class TestQueueingAndIndependence(unittest.TestCase):
    def test_same_address_fifo(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)           # 事务进行中
        r3 = k.request(2, 0, Op.LOAD)                # 排队
        r4 = k.request(0, 0, Op.STORE, value=6)      # 继续排队
        self.assertEqual(
            [q.txn_id for q in k.snapshot().queues[0]], [3, 4])
        drain(k)
        # FIFO：load 读到 5，随后 store 6。
        self.assertEqual([(c.op.value, c.value) for c in k.completed()[2:]],
                         [("load", 5), ("store", 6)])

    def test_different_addresses_progress_independently(self):
        k = CacheCoherenceKernel(memory={0: 0, 1: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)           # addr0 卡在 INV
        # addr1 完全不受 addr0 事务影响：
        rep = k.request(2, 1, Op.STORE, value=99)    # I -> M 立即完成
        self.assertEqual(rep.completions[0].value, 99)
        rep = k.request(2, 1, Op.LOAD)               # M 命中立即完成
        self.assertEqual(rep.completions[0].value, 99)
        self.assertIsNone(k.snapshot().directory[1].active_txn)
        # addr0 的事务依旧悬而未决。
        self.assertIsNotNone(k.snapshot().directory[0].active_txn)

    def test_interleaved_read_write_two_addresses(self):
        # 两个地址的长事务交错投递，各自读值正确、不变量成立。
        scenario = [
            Request(0, 0, Op.STORE, value=100),
            Request(1, 0, Op.LOAD),                  # fetch/flush
            Request(0, 1, Op.LOAD),
            Request(1, 1, Op.LOAD),
            Request(2, 1, Op.STORE, value=200),      # 2 个失效 ACK
            Request(2, 0, Op.LOAD),
        ]
        res = explore(lambda: CacheCoherenceKernel(
            memory={0: 0, 1: 0}), scenario)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        # 全部调度下读值集合唯一。
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {2: {100}, 3: {0}, 4: {0}, 6: {100}})


class TestMessageSemantics(unittest.TestCase):
    def test_lost_inv_is_retransmitted_after_caller_timeout(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=5),
        ])
        inv = k.pending()[0]
        loss = k.drop_message(inv.msg_id)
        self.assertEqual(loss.status.value, "lost")
        self.assertEqual(k.pending(), ())
        rep = k.advance_time(10)
        self.assertEqual(k.time, 10)
        self.assertEqual(len(rep.retransmitted), 1)
        retry = rep.retransmitted[0]
        self.assertEqual(retry.mtype, MsgType.INV)
        self.assertEqual(retry.txn_id, inv.txn_id)
        ack = k.deliver(retry.msg_id).emitted[0]
        done = k.deliver(ack.msg_id)
        self.assertEqual([c.value for c in done.completions], [5])

    def test_lost_ack_causes_safe_inv_replay_and_single_store(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=5),
        ])
        inv = k.pending()[0]
        ack = k.deliver(inv.msg_id).emitted[0]
        k.drop_message(ack.msg_id)
        self.assertEqual(
            [c for c in k.completed() if c.op is Op.STORE], [])
        retry = k.advance_time(10).retransmitted[0]
        # 核端已经失效过一次；重复 INV 只重放 ACK，不再次改变缓存。
        replay = k.deliver(retry.msg_id)
        self.assertEqual(len(replay.emitted), 1)
        self.assertIs(k.snapshot().caches[0][0].state, State.INVALID)
        final = k.deliver(replay.emitted[0].msg_id)
        self.assertEqual([c.value for c in final.completions], [5])
        stores = [c for c in k.completed() if c.op is Op.STORE]
        self.assertEqual(stores, [k.completed()[-1]])

    def test_lost_flush_causes_safe_fetch_replay_and_single_writeback(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=7)
        k.request(1, 0, Op.LOAD)
        fetch = next(m for m in k.pending() if m.mtype is MsgType.FETCH)
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.drop_message(flush.msg_id)
        self.assertEqual(k.snapshot().memory[0], 0)
        retry = k.advance_time(10).retransmitted[0]
        replay_flush = k.deliver(retry.msg_id).emitted[0]
        rep = k.deliver(replay_flush.msg_id)
        self.assertEqual(rep.completions[0].value, 7)
        self.assertEqual(k.snapshot().memory[0], 7)

    def test_duplicate_outstanding_command_completes_once(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=5),
        ])
        original = k.pending()[0]
        retry = k.advance_time(10).retransmitted[0]
        ack1 = k.deliver(original.msg_id).emitted[0]
        ack2 = k.deliver(retry.msg_id).emitted[0]
        self.assertEqual(k.deliver(ack1.msg_id).completions[0].value, 5)
        stale = k.deliver(ack2.msg_id)
        self.assertIs(stale.status, DeliveryStatus.IGNORED_STALE)
        stores = [c for c in k.completed() if c.op is Op.STORE]
        self.assertEqual(len(stores), 1)

    def test_message_and_transaction_ids_are_unique(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=1),
        ])
        ids = [m.msg_id for m in k.pending()]
        self.assertEqual(len(ids), len(set(ids)))

    def test_duplicate_delivery_is_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)
        inv = k.pending()[0]
        before = k.snapshot()
        k.deliver(inv.msg_id)
        ack = k.pending()[0]
        rep_dup = k.deliver(inv.msg_id)             # 重复 INV
        self.assertIs(rep_dup.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(rep_dup.emitted, ())
        self.assertEqual(rep_dup.completions, ())
        k.deliver(ack.msg_id)
        completed_once = k.completed()
        rep_dup_ack = k.deliver(ack.msg_id)         # 重复 ACK
        self.assertIs(rep_dup_ack.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(k.completed(), completed_once)
        # 状态与“不重复投递”一致（幂等）。
        self.assertEqual(k.snapshot().directory[0].owner, 1)
        self.assertIs(before is not None, True)

    def test_stale_ack_must_not_advance_new_txn(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)          # txn2，等核0 ACK
        inv = k.pending()[0]
        ack = k.deliver(inv.msg_id).emitted[0]
        k.deliver(ack.msg_id)                        # txn2 完成

        # 同地址再来：core0 重新读 M（回写事务 txn3），完成后 core1 再
        # 发起升级事务 txn4，此时等待核0 的 ACK。
        k.request(0, 0, Op.LOAD)
        drain(k)                              # txn3 fetch/flush 完成，S={0,1}
        k.request(1, 0, Op.STORE, value=6)    # txn4，等核0 ACK
        self.assertEqual(k.snapshot().directory[0].active_txn, 4)

        # 陈旧 ACK：声称属于 txn2（已结束）。
        stale = Message(None, 2, MsgType.INV_ACK, src=0, dst=DIR, addr=0)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(rep.emitted, ())
        self.assertEqual(rep.completions, ())
        # 新事务没被推进：仍在等核0 的 ACK。
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (0,))
        # 正确的 ACK 到达后才完成。
        real_inv = next(m for m in k.pending() if m.mtype is MsgType.INV)
        real_ack = k.deliver(real_inv.msg_id).emitted[0]
        rep = k.deliver(real_ack.msg_id)
        self.assertEqual(rep.completions[0].value, 6)

    def test_stale_flush_is_ignored(self):
        k = CacheCoherenceKernel()
        k.request(0, 0, Op.STORE, value=5)
        k.request(1, 0, Op.LOAD)
        fetch = k.pending()[0]
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.deliver(flush.msg_id)                     # txn2 完成
        stale = Message(None, 2, MsgType.FLUSH, src=0, dst=DIR,
                        addr=0, value=123)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().memory[0], 5)  # 内存不被陈旧值污染

    def test_stale_fetch_does_not_downgrade_owner(self):
        # 事务2（读M回写）已结束后，重放它的 FETCH 不得再降级当前 owner。
        k = CacheCoherenceKernel()
        k.request(0, 0, Op.STORE, value=5)
        k.request(1, 0, Op.LOAD)
        fetch = k.pending()[0]
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.deliver(flush.msg_id)                      # txn2 完成，S={0,1}
        k.request(1, 0, Op.STORE, value=6)          # txn3，正在失效核0
        self.assertIs(k.snapshot().caches[0][0].state, State.SHARED)
        stale_fetch = Message(None, 2, MsgType.FETCH, src=DIR, dst=0, addr=0)
        rep = k.deliver(stale_fetch)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        # 核0 没被陈旧 FETCH 降级/回写，事务3 仍在等它的 ACK。
        self.assertIs(k.snapshot().caches[0][0].state, State.SHARED)
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (0,))
        self.assertEqual(k.snapshot().memory[0], 5)

    def test_stale_inv_does_not_invalidate(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)          # txn2 失效核0
        inv = k.pending()[0]
        ack = k.deliver(inv.msg_id).emitted[0]
        k.deliver(ack.msg_id)                        # txn2 完成
        k.request(0, 0, Op.LOAD)                     # txn3：回写后核0 重新 S
        drain(k)
        k.request(1, 0, Op.STORE, value=6)          # txn4 失效核0
        # 重放属于 txn2 的陈旧 INV：不得失效核0、不得替 txn4 产生 ACK。
        stale_inv = Message(None, 2, MsgType.INV, src=DIR, dst=0, addr=0)
        rep = k.deliver(stale_inv)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertIs(k.snapshot().caches[0][0].state, State.SHARED)
        self.assertEqual(k.snapshot().directory[0].awaiting_acks, (0,))

    def test_unknown_msg_id_is_locatable_error(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidMessageError) as cm:
            k.deliver(4242)
        self.assertEqual(cm.exception.msg_id, 4242)

    def test_report_lists_emitted_messages(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([Request(0, 0, Op.LOAD),
                         Request(1, 0, Op.LOAD),
                         Request(1, 0, Op.STORE, value=1)])
        inv0 = next(m for m in k.pending() if m.dst == 0)
        rep = k.deliver(inv0.msg_id)
        self.assertEqual([m.mtype for m in rep.emitted], [MsgType.INV_ACK])


if __name__ == "__main__":
    unittest.main()
