"""evict(core, addr) 主动逐出测试：I/S/M 语义、排队交错、陈旧/重复报文。"""

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
from reference import check_completion_sequence


def _flat(completed):
    return [(c.addr, c.op.value, c.value) for c in completed]


class TestEvictBasicSemantics(unittest.TestCase):
    def test_evict_i_line_is_noop(self):
        k = CacheCoherenceKernel(memory={0: 7})
        rep = k.evict(1, 0)
        # I 行无操作：同步完成、无消息、状态不变。
        self.assertFalse(rep.queued)
        self.assertEqual(rep.completions, (Completion(1, 1, 0, Op.EVICT, 7),))
        self.assertEqual(k.pending(), ())
        s = k.snapshot()
        self.assertIs(s.caches[1][0].state, State.INVALID)
        self.assertIs(s.directory[0].state, State.INVALID)

    def test_evict_s_line_removes_sharer_only_after_notice(self):
        k = CacheCoherenceKernel(memory={0: 10})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)                      # S={0,1}
        rep = k.evict(0, 0)
        # 句柄待完成：通知未到目录前不得报告完成。
        self.assertEqual(rep.completions, ())
        self.assertNotIn(rep.txn_id,
                         [c.txn_id for c in k.completed()])
        # 本地行立即丢弃；目录仍登记核0 为共享者（通知在途）。
        self.assertIs(k.snapshot().caches[0][0].state, State.INVALID)
        self.assertEqual(k.snapshot().directory[0].sharers, (0, 1))
        notice = k.pending()[0]
        self.assertIs(notice.mtype, MsgType.EVICT_S)
        self.assertEqual((notice.src, notice.dst), (0, DIR))
        self.assertEqual(notice.txn_id, rep.txn_id)
        out = k.deliver(notice.msg_id)
        self.assertEqual(out.completions,
                         (Completion(rep.txn_id, 0, 0, Op.EVICT, 10),))
        s = k.snapshot()
        self.assertEqual(s.directory[0].sharers, (1,))
        self.assertIs(s.directory[0].state, State.SHARED)  # 核1 仍共享
        self.assertIs(s.caches[1][0].state, State.SHARED)

    def test_evict_last_sharer_turns_directory_invalid(self):
        k = CacheCoherenceKernel(memory={0: 3})
        k.request(2, 0, Op.LOAD)
        k.evict(2, 0)
        drain(k)
        s = k.snapshot()
        self.assertIs(s.directory[0].state, State.INVALID)
        self.assertEqual(s.directory[0].sharers, ())

    def test_evict_m_line_writes_back_before_completing(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=42)           # 核0 M，内存仍 0
        rep = k.evict(0, 0)
        # 待完成句柄：写回消息还没到目录，整项不得完成。
        self.assertEqual(rep.completions, ())
        self.assertEqual([c.txn_id for c in k.completed()], [1])
        # 本地行立即丢弃；目录仍 M/owner=0（写回在途，所有权未释放）。
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertTrue(s.directory[0].evicting)
        wb = k.pending()[0]
        self.assertIs(wb.mtype, MsgType.EVICT_WB)
        self.assertEqual(wb.value, 42)                # 最新数据随写回在途
        self.assertEqual(wb.txn_id, rep.txn_id)
        out = k.deliver(wb.msg_id)
        self.assertEqual(out.completions,
                         (Completion(rep.txn_id, 0, 0, Op.EVICT, 42),))
        s = k.snapshot()
        self.assertEqual(s.memory[0], 42)             # 最新数据已交回目录
        self.assertIs(s.directory[0].state, State.INVALID)
        self.assertIsNone(s.directory[0].owner)

    def test_evicted_m_data_visible_to_next_reader(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=55)
        k.evict(0, 0)
        drain(k)
        rep = k.request(1, 0, Op.LOAD)                # 目录 I：从内存填充
        self.assertEqual(rep.completions[0].value, 55)

    def test_evict_via_request_and_batch(self):
        k = CacheCoherenceKernel(memory={0: 1})
        reps = k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(0, 0, Op.EVICT),
        ])
        self.assertEqual(reps[1].completions, ())     # S 逐出需等通知
        drain(k)
        self.assertEqual([c.op for c in k.completed()],
                         [Op.LOAD, Op.EVICT])
        rep = k.request(1, 0, Op.EVICT)               # request() 也接受 EVICT
        self.assertEqual(rep.completions[0].op, Op.EVICT)  # I 行无操作


class TestEvictQueueing(unittest.TestCase):
    def test_evict_queues_behind_active_transaction(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)            # txn2：INV 在途
        rep = k.evict(0, 0)                           # 同地址：排队
        self.assertTrue(rep.queued)
        self.assertEqual([q.txn_id for q in k.snapshot().queues[0]], [3])
        drain(k)
        # 核0 的行已被 INV 失效，轮到逐出时是 I 行无操作。
        evict_comp = [c for c in k.completed() if c.op is Op.EVICT]
        self.assertEqual(len(evict_comp), 1)
        self.assertEqual(k.pending(), ())

    def test_evict_blocks_later_requests_same_address_fifo(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=9)            # 核0 M
        k.evict(0, 0)                                 # txn2：写回在途
        r3 = k.request(2, 0, Op.LOAD)                 # 排队等逐出完成
        self.assertTrue(r3.queued)
        drain(k)
        # FIFO：先逐出（写回 9），后 load 读到写回值。
        self.assertEqual([(c.op.value, c.value) for c in k.completed()[1:]],
                         [("evict", 9), ("load", 9)])

    def test_evict_does_not_block_other_addresses(self):
        k = CacheCoherenceKernel(memory={0: 0, 1: 0})
        k.request(0, 0, Op.STORE, value=1)
        k.evict(0, 0)                                 # addr0 写回在途
        rep = k.request(1, 1, Op.STORE, value=2)      # addr1 独立推进
        self.assertEqual(rep.completions[0].value, 2)
        self.assertIsNotNone(k.snapshot().directory[0].active_txn)


class TestEvictStaleAndDuplicate(unittest.TestCase):
    def test_stale_evict_wb_does_not_clear_reacquired_ownership(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=5)            # txn1：核0 M
        k.evict(0, 0)                                 # txn2：逐出写回
        drain(k)                                      # 目录 I，内存=5
        k.request(0, 0, Op.STORE, value=7)            # txn3：重新获得 M
        # 旧逐出的写回迟到（重放 txn2）：不得清掉后来重新获得的所有权。
        stale = Message(None, 2, MsgType.EVICT_WB, src=0, dst=DIR,
                        addr=0, value=5)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(rep.completions, ())
        s = k.snapshot()
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertIs(s.caches[0][0].state, State.MODIFIED)
        self.assertEqual(s.caches[0][0].value, 7)     # 新数据未被旧写回覆盖
        # 后续读仍读到 7。
        k.request(1, 0, Op.LOAD)
        drain(k)
        self.assertEqual(k.completed()[-1].value, 7)

    def test_stale_evict_s_does_not_remove_sharer(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.evict(0, 0)                                 # txn2：S 逐出
        drain(k)                                      # 目录 I
        k.request(0, 0, Op.LOAD)                      # txn3：重新 S
        k.request(1, 0, Op.LOAD)                      # txn4：S={0,1}
        k.request(1, 0, Op.STORE, value=6)            # txn5：等核0 ACK
        stale = Message(None, 2, MsgType.EVICT_S, src=0, dst=DIR, addr=0)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        s = k.snapshot()
        self.assertEqual(s.directory[0].sharers, (0, 1))  # 共享者未被误删
        self.assertEqual(s.directory[0].awaiting_acks, (0,))
        drain(k)
        self.assertEqual(k.completed()[-1].value, 6)

    def test_late_old_ack_does_not_complete_evict(self):
        # 旧 INV_ACK 迟到撞上 M 逐出事务：不得推进逐出。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=8)            # txn3：失效核0
        drain(k)                                      # 核1 M
        k.evict(1, 0)                                 # txn4：M 逐出，写回在途
        self.assertTrue(k.snapshot().directory[0].evicting)
        late_ack = Message(None, 3, MsgType.INV_ACK, src=0, dst=DIR, addr=0)
        rep = k.deliver(late_ack)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(rep.completions, ())
        # 逐出仍未完成：只能由它自己的 EVICT_WB 推进。
        self.assertNotIn(4, [c.txn_id for c in k.completed()])
        wb = k.pending()[0]
        out = k.deliver(wb.msg_id)
        self.assertEqual(out.completions, (Completion(4, 1, 0, Op.EVICT, 8),))

    def test_duplicate_evict_wb_is_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=5)
        k.evict(0, 0)
        wb = k.pending()[0]
        k.deliver(wb.msg_id)
        completed_once = k.completed()
        rep = k.deliver(wb.msg_id)                    # 重复投递同一写回
        self.assertIs(rep.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(rep.completions, ())
        self.assertEqual(k.completed(), completed_once)  # 不重复完成
        self.assertEqual(k.snapshot().memory[0], 5)

    def test_duplicate_evict_s_is_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.evict(0, 0)
        notice = k.pending()[0]
        k.deliver(notice.msg_id)
        rep = k.deliver(notice.msg_id)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(len([c for c in k.completed()
                              if c.op is Op.EVICT]), 1)

    def test_stale_flush_and_fetch_do_not_hijack_evict_txn(self):
        # 逐出事务等的是 EVICT_WB：伪造同事务 id 的 FLUSH/FETCH 不得生效。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=5)
        k.evict(0, 0)                                 # txn2
        forged_flush = Message(None, 2, MsgType.FLUSH, src=0, dst=DIR,
                               addr=0, value=999)
        rep = k.deliver(forged_flush)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().memory[0], 0)   # 内存未被污染
        forged_fetch = Message(None, 2, MsgType.FETCH, src=DIR, dst=0, addr=0)
        rep = k.deliver(forged_fetch)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertTrue(k.snapshot().directory[0].evicting)


class TestEvictValidation(unittest.TestCase):
    def test_evict_rejects_value_and_bad_operands(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.EVICT, value=1)
        self.assertEqual(cm.exception.field, "value")
        with self.assertRaises(InvalidRequestError):
            k.evict(True, 0)
        with self.assertRaises(InvalidRequestError):
            k.evict(0, 8)
        with self.assertRaises(InvalidRequestError):
            k.evict(0, 0, txn_id=-1)

    def test_injected_evict_message_validation(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.STORE, value=1)
        k.evict(0, 0)
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 2, MsgType.EVICT_WB, src=0, dst=DIR,
                              addr=0))                # 写回必须带数据
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 2, MsgType.EVICT_S, src=0, dst=DIR,
                              addr=0, value=3))       # 通知不得带数据
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 2, MsgType.EVICT_WB, src=DIR, dst=0,
                              addr=0, value=1))       # 方向必须 核→目录
        with self.assertRaises(InvalidMessageError):
            k.deliver(Message(None, 99, MsgType.EVICT_S, src=0, dst=DIR,
                              addr=0))                # 未注册事务


class TestEvictExploration(unittest.TestCase):
    """枚举短消息排列：逐步检查不变量，用串行完成顺序核对读值。"""

    def test_m_evict_with_concurrent_third_core_load(self):
        # 核0 写后逐出（M 写回在途），核2 同地址 load 排队等写回。
        scenario = [
            Request(0, 0, Op.STORE, value=70),
            Request(0, 0, Op.EVICT),
            Request(2, 0, Op.LOAD),
        ]
        res = explore(lambda: CacheCoherenceKernel(memory={0: 0}), scenario,
                      all_paths=True)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())          # 每步最多一个 M、owner 一致
        self.assertEqual(res.terminal_states, 1)
        # 所有排列下：load 只能读到写回值 70；完成序列过独立参考核对。
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {3: {70}})
        for comps in res.schedules.values():
            ops = [c.op for c in comps]
            self.assertEqual(ops, [Op.STORE, Op.EVICT, Op.LOAD])
            check_completion_sequence(_flat(comps), {0: 0})

    def test_s_evict_collides_with_upgrade_evict_first(self):
        # 核0 逐出 S 的通知在途时，核1 的升级排队；升级只能失效剩余共享者。
        scenario = [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(0, 0, Op.EVICT),                  # 核0 逐出 S
            Request(1, 0, Op.STORE, value=21),        # 撞升级的写
        ]
        res = explore(lambda: CacheCoherenceKernel(memory={0: 100}), scenario,
                      all_paths=True)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertEqual(res.terminal_states, 1)
        for comps in res.schedules.values():
            check_completion_sequence(_flat(comps), {0: 100})
        # 终态：核1 独占 M，核0/核2 均为 I。
        k = CacheCoherenceKernel(memory={0: 100})
        k.request_batch(scenario)
        drain(k)
        s = k.snapshot()
        self.assertEqual((s.directory[0].state.value, s.directory[0].owner),
                         ("M", 1))
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertIs(s.caches[2][0].state, State.INVALID)

    def test_s_evict_collides_with_upgrade_upgrade_first(self):
        # 升级事务在途（INV 未回齐）时核0 逐出排队；轮到逐出时行已被 INV
        # 失效，退化为 I 行无操作。
        scenario = [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=33),        # 升级先占事务
            Request(0, 0, Op.EVICT),                  # 逐出排队
            Request(2, 0, Op.LOAD),                   # 读写逐出交错
        ]
        res = explore(lambda: CacheCoherenceKernel(memory={0: 100}), scenario,
                      all_paths=True)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertEqual(res.terminal_states, 1)
        # 全部排列下读值唯一：核2 读到升级后的 33。
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {1: {100}, 2: {100}, 3: {100}, 6: {33}})
        for comps in res.schedules.values():
            check_completion_sequence(_flat(comps), {0: 100})
            # 逐出恰完成一次，且排在升级 store 之后（FIFO）。
            evict_pos = [i for i, c in enumerate(comps) if c.op is Op.EVICT]
            store_pos = [i for i, c in enumerate(comps) if c.op is Op.STORE]
            self.assertEqual(len(evict_pos), 1)
            self.assertLess(store_pos[0], evict_pos[0])

    def test_m_evict_while_other_core_reads_same_address(self):
        # 核0 M，核1 load 触发 FETCH/FLUSH 在途时核0 逐出排队：
        # 逐出启动时行已是 S（回写后降级），退化为 S 逐出。
        scenario = [
            Request(0, 0, Op.STORE, value=44),
            Request(1, 0, Op.LOAD),                   # FETCH/FLUSH 事务
            Request(0, 0, Op.EVICT),                  # 排队，轮到时是 S 逐出
            Request(2, 0, Op.LOAD),
        ]
        res = explore(lambda: CacheCoherenceKernel(memory={0: 0}), scenario,
                      all_paths=True)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertEqual(res.terminal_states, 1)
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {2: {44}, 4: {44}})
        for comps in res.schedules.values():
            check_completion_sequence(_flat(comps), {0: 0})


if __name__ == "__main__":
    unittest.main()
