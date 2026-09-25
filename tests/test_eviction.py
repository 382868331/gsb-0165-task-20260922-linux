"""evict(core, addr) 逐出协议测试：有限缓存容量的显式替换。

覆盖：
- I 行无操作、S 行移除共享者、M 行先回写最新数据才转 I；
- 逐出与 load/store 共用同地址 FIFO，API 返回待完成句柄（写回/ACK 未到
  之前不得报告完成）；
- 写回 FLUSH、失效/逐出 ACK 全部带原事务身份：重复投递幂等、旧事务的迟到
  ACK/FLUSH/EVICT 一律陈旧、旧逐出不能清掉后来重新获得的所有权；
- 三个重点交错的短消息排列枚举：M 逐出同时第三核 load、S 逐出撞升级、
  旧 ACK 迟到；逐步核对读值、至多一个 M 且目录 owner 一致。
"""

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
    InvalidRequestError,
    Message,
    MsgType,
    Op,
    Request,
    State,
)
from msi_kernel.explore import check_invariants, drain, explore


# ---------------------------------------------------------------------------
# 基础三态语义
# ---------------------------------------------------------------------------


class TestEvictBasics(unittest.TestCase):
    def test_evict_invalid_is_noop_without_messages(self):
        k = CacheCoherenceKernel(memory={0: 100})
        rep = k.evict(0, 0)
        self.assertFalse(rep.queued)
        self.assertEqual(rep.completions,
                         (Completion(1, 0, 0, Op.EVICT, 0),))
        self.assertEqual(k.pending(), ())
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertIs(s.directory[0].state, State.INVALID)
        # 内存不受影响。
        self.assertEqual(s.memory[0], 100)

    def test_evict_shared_self_invalidates_then_directory_removes_on_deliver(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)
        rep = k.evict(0, 0)
        # 句柄未完成：EVICT 通知还没投递。
        self.assertEqual(rep.completions, ())
        notice = next(m for m in k.pending() if m.mtype is MsgType.EVICT)
        s = k.snapshot()
        # 核端只改了自己：自己已 I，但目录此刻仍把它登记为共享者
        # （通知在途的协议过渡态）。
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertEqual(s.directory[0].sharers, (0, 1))
        self.assertEqual(s.directory[0].phase, "notice")

        rep = k.deliver(notice.msg_id)
        self.assertEqual(rep.completions, ())           # ACK 未到仍不完成
        ack = next(m for m in rep.emitted
                   if m.mtype is MsgType.EVICT_ACK)
        s = k.snapshot()
        # 目录在 EVICT 生效时才移除共享者；仍有核1，目录保持 S。
        self.assertEqual(s.directory[0].sharers, (1,))
        self.assertIs(s.directory[0].state, State.SHARED)
        self.assertEqual(s.directory[0].phase, "ack")

        rep = k.deliver(ack.msg_id)
        self.assertEqual(rep.completions,
                         (Completion(3, 0, 0, Op.EVICT, 0),))
        s = k.snapshot()
        self.assertIsNone(s.directory[0].active_txn)
        self.assertEqual(s.directory[0].sharers, (1,))
        self.assertIs(s.directory[0].state, State.SHARED)

    def test_evict_last_sharer_makes_directory_invalid(self):
        k = CacheCoherenceKernel(memory={2: 7})
        k.request(2, 2, Op.LOAD)
        k.evict(2, 2)
        notice = next(m for m in k.pending() if m.mtype is MsgType.EVICT)
        k.deliver(notice.msg_id)
        # 最后一个共享者移出：目录转 I（ACK 在途不影响目录状态）。
        self.assertIs(k.snapshot().directory[2].state, State.INVALID)
        self.assertEqual(k.snapshot().directory[2].sharers, ())
        drain(k)
        self.assertIs(k.snapshot().directory[2].state, State.INVALID)

    def test_evict_modified_must_write_back_before_invalid_complete(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=555)            # core0 M，内存仍 100
        rep = k.evict(0, 0)
        # 写回消息还没到：绝不能报告逐出完成。
        self.assertEqual(rep.completions, ())
        flush = next(m for m in k.pending() if m.mtype is MsgType.FLUSH)
        self.assertEqual(flush.src, 0)
        self.assertEqual(flush.dst, DIR)
        self.assertEqual(flush.value, 555)              # 最新数据随 FLUSH 交回

        s = k.snapshot()
        # 数据未被目录确认前，缓存行仍是 M、目录仍 M 且 owner 仍登记核0。
        self.assertIs(s.caches[0][0].state, State.MODIFIED)
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertEqual(s.memory[0], 100)              # 内存尚未入账
        self.assertEqual(s.directory[0].phase, "flush")

        rep = k.deliver(flush.msg_id)
        self.assertEqual(rep.completions, ())           # 还要等 EVICT_ACK
        self.assertEqual(k.snapshot().memory[0], 555)   # 写回此刻才入账
        s = k.snapshot()
        # ACK 未到：行与目录仍为 M（最新数据已落内存的协议过渡态）。
        self.assertIs(s.caches[0][0].state, State.MODIFIED)
        self.assertIs(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertEqual(s.directory[0].phase, "ack")
        ack = next(m for m in rep.emitted
                   if m.mtype is MsgType.EVICT_ACK)
        rep = k.deliver(ack.msg_id)
        self.assertEqual(rep.completions,
                         (Completion(2, 0, 0, Op.EVICT, 0),))
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.INVALID)
        self.assertIs(s.directory[0].state, State.INVALID)
        self.assertIsNone(s.directory[0].owner)
        self.assertEqual(s.memory[0], 555)

    def test_modified_eviction_then_load_reads_written_back_value(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=555)
        k.evict(0, 0)
        drain(k)
        # 第三核在逐出完成后读取：目录 I、内存已是最新值，直接填 S。
        rep = k.request(1, 0, Op.LOAD)
        self.assertEqual(rep.completions[0].value, 555)
        self.assertIs(k.snapshot().caches[1][0].state, State.SHARED)

    def test_evict_via_request_enum_and_batch_equivalent(self):
        k = CacheCoherenceKernel(memory={0: 0})
        rep = k.request(0, 0, Op.EVICT)                # I 行：同步完成
        self.assertEqual(rep.completions[0].op, Op.EVICT)
        reps = k.request_batch([Request(1, 0, Op.LOAD),
                                Request(2, 0, Op.EVICT)])  # 核2 从未持有：I
        self.assertEqual(reps[1].completions[0].op, Op.EVICT)


class TestEvictValidation(unittest.TestCase):
    def test_evict_rejects_value(self):
        with self.assertRaises(InvalidRequestError) as cm:
            CacheCoherenceKernel().request(0, 0, Op.EVICT, value=3)
        self.assertEqual(cm.exception.field, "value")

    def test_evict_rejects_bad_core_addr(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().evict(3, 0)
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel().evict(0, 8)

    def test_evict_txn_id_unique(self):
        k = CacheCoherenceKernel()
        k.evict(0, 0, txn_id=9)
        with self.assertRaises(InvalidRequestError):
            k.evict(1, 1, txn_id=9)


# ---------------------------------------------------------------------------
# 与同地址 FIFO 的串行关系
# ---------------------------------------------------------------------------


class TestEvictQueueing(unittest.TestCase):
    def test_evict_queued_behind_active_txn_starts_only_after_it(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=5)               # 失效核0 进行中
        rep = k.evict(0, 0)                               # 同地址排队
        self.assertTrue(rep.queued)
        self.assertEqual(rep.completions, ())
        # 逐出未开始：核0 还没自行置 I。
        self.assertIs(k.snapshot().caches[0][0].state, State.SHARED)
        drain(k)
        # 升级把核0 失效后，逐出启动时它已是 I：无操作完成，绝不重复发消息。
        comps = k.completed()
        self.assertEqual(comps[-1].op, Op.EVICT)
        self.assertIs(k.snapshot().directory[0].state, State.MODIFIED)
        self.assertEqual(k.snapshot().directory[0].owner, 1)

    def test_evict_then_chained_requests_pump_on_ack(self):
        # M 逐出进行中排队的 load 必须在 EVICT_ACK 到达（逐出完成）后才启动，
        # 且读到回写值。
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=555)
        k.evict(0, 0)                                     # txn2 等回写
        queued = k.request(2, 0, Op.LOAD)                 # 排队
        self.assertTrue(queued.queued)
        flush = next(m for m in k.pending() if m.mtype is MsgType.FLUSH)
        rep = k.deliver(flush.msg_id)
        # 回写已入账但逐出 ACK 未到：排队 load 仍不得启动。
        self.assertEqual(rep.completions, ())
        self.assertIsNotNone(k.snapshot().directory[0].active_txn)
        self.assertIs(k.snapshot().caches[2][0].state, State.INVALID)
        ack = next(m for m in rep.emitted
                   if m.mtype is MsgType.EVICT_ACK)
        rep = k.deliver(ack.msg_id)
        # 逐出完成链式启动 load：目录 I + 内存 555，同步完成读 555。
        ops = [(c.op, c.value) for c in rep.completions]
        self.assertEqual(ops, [(Op.EVICT, 0), (Op.LOAD, 555)])
        self.assertIs(k.snapshot().caches[2][0].state, State.SHARED)


# ---------------------------------------------------------------------------
# 幂等与陈旧身份
# ---------------------------------------------------------------------------


class TestEvictMessageIdentity(unittest.TestCase):
    def test_duplicate_evict_and_ack_are_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)
        k.evict(0, 0)
        notice = next(m for m in k.pending() if m.mtype is MsgType.EVICT)
        rep = k.deliver(notice.msg_id)
        ack = next(m for m in rep.emitted
                   if m.mtype is MsgType.EVICT_ACK)
        dup_notice = k.deliver(notice.msg_id)
        self.assertIs(dup_notice.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(dup_notice.emitted, ())
        self.assertEqual(k.snapshot().directory[0].sharers, (1,))
        k.deliver(ack.msg_id)
        done_once = k.completed()
        dup_ack = k.deliver(ack.msg_id)
        self.assertIs(dup_ack.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(k.completed(), done_once)

    def test_duplicate_flush_for_m_evict_is_idempotent(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=555)
        k.evict(0, 0)
        flush = next(m for m in k.pending() if m.mtype is MsgType.FLUSH)
        rep = k.deliver(flush.msg_id)
        ack = next(m for m in rep.emitted
                   if m.mtype is MsgType.EVICT_ACK)
        dup = k.deliver(flush.msg_id)
        self.assertIs(dup.status, DeliveryStatus.IGNORED_DUPLICATE)
        self.assertEqual(dup.emitted, ())
        # 只产生了一个 EVICT_ACK，逐出只完成一次。
        self.assertEqual(k.snapshot().memory[0], 555)
        k.deliver(ack.msg_id)
        self.assertEqual(
            [c for c in k.completed() if c.op is Op.EVICT][0].txn_id, 2)

    def test_old_evict_ack_cannot_clear_reacquired_shared_line(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)
        k.evict(0, 0)                                     # txn3
        drain(k)                                          # 逐出完成
        # 核0 后来重新获得该行的 S 所有权。
        k.request(0, 0, Op.LOAD)
        self.assertEqual(k.snapshot().directory[0].sharers, (0, 1))

        # 旧逐出事务（txn3）的 EVICT / EVICT_ACK 迟到：必须判陈旧。
        stale_ack = Message(None, 3, MsgType.EVICT_ACK,
                            src=DIR, dst=0, addr=0)
        rep = k.deliver(stale_ack)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(rep.completions, ())
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.SHARED)
        self.assertEqual(s.directory[0].sharers, (0, 1))

        stale_notice = Message(None, 3, MsgType.EVICT,
                               src=0, dst=DIR, addr=0)
        rep = k.deliver(stale_notice)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(k.snapshot().directory[0].sharers, (0, 1))

    def test_old_evict_flush_cannot_pollute_modified_reacquisition(self):
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=777)
        k.evict(0, 0)                                     # txn2：回写 777
        drain(k)                                          # 内存 777，目录 I
        # 核0 重新拿到 M 所有权并写入 888（内存仍停在 777）。
        k.request(0, 0, Op.STORE, value=888)              # txn3
        self.assertEqual(k.snapshot().directory[0].owner, 0)
        self.assertEqual(k.snapshot().memory[0], 777)

        # 旧逐出事务 txn2 的 FLUSH（带旧值 777）迟到：必须判陈旧，
        # 既不改内存也不动 owner。
        stale = Message(None, 2, MsgType.FLUSH, src=0, dst=DIR,
                        addr=0, value=777)
        rep = k.deliver(stale)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        s = k.snapshot()
        self.assertEqual(s.memory[0], 777)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertIs(s.caches[0][0].state, State.MODIFIED)
        # 新事务的正确回写读值仍然是 888。
        k.request(1, 0, Op.LOAD)
        drain(k)
        self.assertEqual(k.completed()[-1].value, 888)

    def test_ack_from_wrong_phase_is_stale(self):
        # M 逐出处于 flush 阶段时，注入其 ack 阶段才合法的 EVICT_ACK：陈旧。
        k = CacheCoherenceKernel(memory={0: 100})
        k.request(0, 0, Op.STORE, value=5)
        k.evict(0, 0)                                     # txn2，flush
        early = Message(None, 2, MsgType.EVICT_ACK, src=DIR, dst=0, addr=0)
        rep = k.deliver(early)
        self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        self.assertIsNotNone(k.snapshot().directory[0].active_txn)


# ---------------------------------------------------------------------------
# 重点交错枚举：逐步不变量 + 串行完成读值 + owner 一致
# ---------------------------------------------------------------------------


def _walk_schedule_asserting(factory, scenario, path):
    """按路径逐步重放，每一步都检查结构不变量与 owner 一致性；返回终态内核。"""
    k = factory()
    k.request_batch(list(scenario))
    for mid in path:
        k.deliver(mid)
        bad = check_invariants(k)
        assert bad is None, f"path={path} mid={mid}: {bad}"
        snap = k.snapshot()
        for a in range(8):
            m_cores = [c for c in range(3)
                       if snap.caches[c][a].state is State.MODIFIED]
            assert len(m_cores) <= 1, f"addr={a} 出现多个 M: {m_cores}"
            if m_cores:
                assert snap.directory[a].owner == m_cores[0], (
                    f"addr={a}: M 在核 {m_cores[0]} 但目录 owner="
                    f"{snap.directory[a].owner}")
    return k


class TestEvictInterleavingEnumeration(unittest.TestCase):
    def test_m_evict_concurrent_with_third_core_load(self):
        init = {0: 100}
        scenario = [
            Request(0, 0, Op.STORE, value=555),           # txn1 core0 M
            Request(0, 0, Op.EVICT),                      # txn2 FLUSH+ACK
            Request(2, 0, Op.LOAD),                       # txn3 排队等逐出
        ]
        res = explore(lambda: CacheCoherenceKernel(memory=dict(init)),
                      scenario, all_paths=True)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        # 每个调度、每一步：至多一个 M、owner 一致。
        for path in res.schedules:
            k = _walk_schedule_asserting(
                lambda: CacheCoherenceKernel(memory=dict(init)),
                scenario, path)
            comps = k.completed()
            # 串行完成顺序：store -> evict -> load；load 只能读到回写值 555。
            self.assertEqual([c.txn_id for c in comps], [1, 2, 3])
            self.assertEqual(comps[-1].op, Op.LOAD)
            self.assertEqual(comps[-1].value, 555)
            s = k.snapshot()
            self.assertEqual(s.memory[0], 555)
            self.assertEqual(s.directory[0].sharers, (2,))
        # 所有重排读值集合唯一。
        self.assertEqual(set(res.read_values[3]), {555})

    def test_s_evict_collides_with_upgrade(self):
        # 三核共享；核0 的 S 逐出与核1 的升级在同一地址 FIFO 上相撞。
        # 同地址严格串行：逐出事务的 EVICT/ACK 全部走完后升级才启动，
        # 因此升级只需失效剩下的核2（不会再向已逐出的核0 发 INV）。
        init = {0: 100}
        scenario = [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(0, 0, Op.EVICT),                      # txn4 先入
            Request(1, 0, Op.STORE, value=200),           # txn5 排队
        ]
        res = explore(lambda: CacheCoherenceKernel(memory=dict(init)),
                      scenario, all_paths=True)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        for path in res.schedules:
            k = _walk_schedule_asserting(
                lambda: CacheCoherenceKernel(memory=dict(init)),
                scenario, path)
            comps = k.completed()
            # 完成顺序严格串行：三个 load、逐出、升级。
            self.assertEqual([c.txn_id for c in comps], [1, 2, 3, 4, 5])
            s = k.snapshot()
            self.assertIs(s.directory[0].state, State.MODIFIED)
            self.assertEqual(s.directory[0].owner, 1)
            self.assertIs(s.caches[1][0].state, State.MODIFIED)
            self.assertEqual(s.caches[1][0].value, 200)
            self.assertIs(s.caches[0][0].state, State.INVALID)
            self.assertIs(s.caches[2][0].state, State.INVALID)
        # 升级阶段唯一等待的 ACK 来自核2（核0 的共享者身份已被逐出移除）：
        # 直接按消息链重放验证。
        k = CacheCoherenceKernel(memory=dict(init))
        k.request_batch(scenario)
        ev = next(m for m in k.pending() if m.mtype is MsgType.EVICT)
        ack = k.deliver(ev.msg_id).emitted[0]
        k.deliver(ack.msg_id)                            # 逐出完成，升级启动
        inv_dsts = sorted(m.dst for m in k.pending()
                          if m.mtype is MsgType.INV)
        self.assertEqual(inv_dsts, [2])

    def test_s_evict_and_upgrade_on_independent_addresses_interleave(self):
        # 真正的消息交错：地址0 上核0 的 S 逐出（EVICT/ACK）与地址1 上
        # 核2 对核1 的升级（INV/ACK）彼此独立，枚举应给出多种短消息排列，
        # 但每种排列终态与读值都一致。
        init = {0: 0, 1: 0}
        scenario = [
            Request(0, 0, Op.LOAD),                       # txn1
            Request(1, 1, Op.LOAD),                       # txn2
            Request(2, 1, Op.LOAD),                       # txn3
            Request(0, 0, Op.EVICT),                      # txn4 EVICT/ACK
            Request(2, 1, Op.STORE, value=8),             # txn5 INV 核1
        ]
        res = explore(lambda: CacheCoherenceKernel(memory=dict(init)),
                      scenario, all_paths=True)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        self.assertGreater(len(res.schedules), 1,
                           "两个独立地址的 EVICT/ACK 与 INV/ACK 应可交错")
        for path in res.schedules:
            k = _walk_schedule_asserting(
                lambda: CacheCoherenceKernel(memory=dict(init)),
                scenario, path)
            s = k.snapshot()
            self.assertIs(s.directory[0].state, State.INVALID)
            self.assertIs(s.directory[1].state, State.MODIFIED)
            self.assertEqual(s.directory[1].owner, 2)
            self.assertEqual(s.caches[2][1].value, 8)

    def test_upgrade_collides_with_s_evict_other_order(self):
        # 反方向：升级先占地址（INV 在途），S 逐出排队；升级 ACK 到齐后
        # 逐出启动时核0 已被失效，按 I 行无操作完成。全部重排结论一致。
        init = {0: 100}
        scenario = [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=200),
            Request(0, 0, Op.EVICT),
        ]
        res = explore(lambda: CacheCoherenceKernel(memory=dict(init)),
                      scenario)
        self.assertEqual(res.violations, ())
        self.assertFalse(res.exhausted)
        for path, comps in res.schedules.items():
            self.assertEqual([c.txn_id for c in comps], [1, 2, 3, 4])
            self.assertEqual(comps[-1].op, Op.EVICT)
            k = _walk_schedule_asserting(
                lambda: CacheCoherenceKernel(memory=dict(init)),
                scenario, path)
            self.assertEqual(k.snapshot().directory[0].owner, 1)

    def test_late_old_ack_in_enumerated_upgrade_then_evict_chain(self):
        # 旧 ACK 迟到发生在“升级 -> 逐出 -> 重新获得”的链上：逐消息驱动后
        # 注入旧事务身份的 ACK/通知，任何一步都不得被推进。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=9),              # txn3 失效核0
        ])
        drain(k)
        k.request(0, 0, Op.LOAD)                           # txn4 回写后重共享
        drain(k)
        k.evict(0, 0)                                      # txn5 S 逐出
        drain(k)
        k.request(0, 0, Op.LOAD)                           # txn6 再次获得 S
        # 三个旧事务的 ACK 类报文全部迟到：
        for tid, mtype, kwargs in [
            (3, MsgType.INV_ACK, {"src": 0, "dst": DIR}),
            (5, MsgType.EVICT_ACK, {"src": DIR, "dst": 0}),
            (5, MsgType.EVICT, {"src": 0, "dst": DIR}),
        ]:
            rep = k.deliver(Message(None, tid, mtype, addr=0, **kwargs))
            self.assertIs(rep.status, DeliveryStatus.IGNORED_STALE)
        s = k.snapshot()
        self.assertIs(s.caches[0][0].state, State.SHARED)
        self.assertEqual(s.directory[0].sharers, (0, 1))
        self.assertIsNone(check_invariants(k))


if __name__ == "__main__":
    unittest.main()
