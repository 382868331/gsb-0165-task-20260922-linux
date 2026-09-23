"""容量上限测试：目录等待队列 / 每核在途请求 / 网络通道三类容量。

覆盖：
- 正常：容量充足时行为与无容量模型一致；
- 边界：cap=0、恰好满载、满载后释放可重试；
- 失败：CapacityExceededError 可定位、零部分变更、不消耗事务 id；
       批量准入的容量原子性（深拷贝预演）；
- 网络背压：发送失败保留消息（blocked FIFO）与未完成状态，腾槽自动补发，
  最终读值与无容量模型逐事务一致；
- 2 核 2 地址下“满队列 + 回写 + 请求交错”，对多种紧容量组合做差分核对，
  期望值由独立参考（tests/reference.py）与无容量旧模型给出，不用被测实现
  自身生成期望值。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from msi_kernel import (
    CacheCoherenceKernel,
    CapacityConfig,
    CapacityExceededError,
    InfeasibleError,
    InvalidRequestError,
    MsgType,
    Op,
    Request,
    State,
    drain,
)
from msi_kernel.explore import check_invariants, explore
from reference import check_completion_sequence


def _lcg(seed):
    state = seed
    while True:
        state = (1103515245 * state + 12345) & 0x7FFFFFFF
        yield state


# ---------------------------------------------------------------------------
# 容量参数校验
# ---------------------------------------------------------------------------


class TestCapacityConfigValidation(unittest.TestCase):
    def test_bad_capacity_types_rejected(self):
        for kw in ("dir_queue_cap", "core_request_cap", "network_cap"):
            with self.assertRaises(InvalidRequestError):
                CacheCoherenceKernel(**{kw: -1})
            with self.assertRaises(InvalidRequestError):
                CacheCoherenceKernel(**{kw: 1.0})
            with self.assertRaises(InvalidRequestError):
                CacheCoherenceKernel(**{kw: True})       # bool 不当整数
            with self.assertRaises(InvalidRequestError):
                CacheCoherenceKernel(**{kw: "2"})

    def test_zero_and_none_accepted(self):
        CacheCoherenceKernel(dir_queue_cap=0,
                             core_request_cap=0, network_cap=0)
        CacheCoherenceKernel(dir_queue_cap=None,
                             core_request_cap=None, network_cap=None)

    def test_capacity_object_must_be_config(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(capacity={"network_cap": 1})
        cfg = CapacityConfig(network_cap=2)
        k = CacheCoherenceKernel(capacity=cfg)
        self.assertIs(k.capacity_config, cfg)

    def test_keyword_overrides_config(self):
        k = CacheCoherenceKernel(
            capacity=CapacityConfig(network_cap=5), network_cap=1)
        self.assertEqual(k.capacity_config.network_cap, 1)


# ---------------------------------------------------------------------------
# 目录等待队列容量
# ---------------------------------------------------------------------------


class TestDirQueueCapacity(unittest.TestCase):
    def test_queue_full_rejects_and_is_locatable(self):
        k = CacheCoherenceKernel(dir_queue_cap=1)
        k.request(0, 0, Op.STORE, value=1)     # I->M 同步
        k.request(1, 0, Op.LOAD)               # 读 M：活动 FETCH
        k.request(2, 0, Op.LOAD)               # 排队，队列恰好满 (1/1)
        with self.assertRaises(CapacityExceededError) as cm:
            k.request(0, 0, Op.LOAD)
        self.assertEqual(cm.exception.field, "dir_queue")
        self.assertEqual(cm.exception.limit, 1)
        self.assertEqual(cm.exception.used, 1)

    def test_rejection_leaves_no_partial_change(self):
        k = CacheCoherenceKernel(dir_queue_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)
        k.request(2, 0, Op.LOAD)
        before = k.snapshot()
        next_id = k._next_txn
        with self.assertRaises(CapacityExceededError):
            k.request(0, 0, Op.LOAD, txn_id=77)
        after = k.snapshot()
        # 不接纳、不排队、不消耗任何 id（自动/显式都不消耗）。
        self.assertEqual(after.queues, before.queues)
        self.assertEqual(after.pending_messages, before.pending_messages)
        self.assertEqual(after.completed, before.completed)
        self.assertEqual(k._next_txn, next_id)
        self.assertNotIn(77, k._used_txn)

    def test_retry_after_capacity_release_succeeds(self):
        k = CacheCoherenceKernel(dir_queue_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)               # 活动
        k.request(2, 0, Op.LOAD)               # 排队 (1/1)
        with self.assertRaises(CapacityExceededError):
            k.request(0, 0, Op.LOAD)
        drain(k)                               # 活动事务 + 队首完成，腾出队列
        rep = k.request(0, 0, Op.LOAD)         # 同参数重试成功
        drain(k)
        # 手算：store=1 -> M；两次回写后内存=1，所有 load 都读 1。
        self.assertTrue(all(c.value == 1 for c in k.completed()
                            if c.op is Op.LOAD))
        # 释放后地址处于 S，重试的 load 应同步完成（不再排队）。
        self.assertEqual(rep.queued, False)
        self.assertEqual(rep.completions[0].value, 1)

    def test_other_address_queue_independent(self):
        # addr0 队列打满不影响 addr1 准入。
        k = CacheCoherenceKernel(dir_queue_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)
        k.request(2, 0, Op.LOAD)               # addr0 队列满
        rep = k.request(0, 1, Op.STORE, value=9)  # addr1 空闲，同步完成
        self.assertEqual(rep.completions[0].value, 9)


# ---------------------------------------------------------------------------
# 每核在途请求容量
# ---------------------------------------------------------------------------


class TestCoreInflightCapacity(unittest.TestCase):
    def test_inflight_full_rejects(self):
        k = CacheCoherenceKernel(core_request_cap=1)
        k.request(0, 0, Op.STORE, value=1)      # 核0 在 addr0 为 M
        k.request(1, 0, Op.LOAD)                # 核1 读 M：活动事务 (1/1)
        k.request(0, 1, Op.STORE, value=5)      # 核0 在 addr1 也为 M
        # 核1 再来一个需要消息的请求（读核0 占 M 的 addr1）-> 在途将达 2，拒绝。
        with self.assertRaises(CapacityExceededError) as cm:
            k.request(1, 1, Op.LOAD)
        self.assertEqual(cm.exception.field, "core_inflight")
        self.assertEqual(cm.exception.limit, 1)
        self.assertEqual(cm.exception.used, 1)
        self.assertIn("核 1", str(cm.exception))
        # 其它核不受影响：核2 在途=0，可正常发起需要消息的请求。
        rep = k.request(2, 1, Op.LOAD)
        self.assertTrue(rep.queued is False and rep.completions == ())
        self.assertEqual(k.snapshot().capacity.core_inflight[2], 1)

    def test_async_request_consumes_slot_sync_hit_does_not(self):
        k = CacheCoherenceKernel(core_request_cap=1)
        k.request(0, 0, Op.STORE, value=1)     # 核0 M
        k.request(1, 0, Op.LOAD)               # 核1 读 M：活动，在途=1
        cap = k.snapshot().capacity
        self.assertEqual(cap.core_inflight[1], 1)
        # cap=1 已满：核1 再来一个需要消息的请求（读核0 占 M 的 addr1）被拒。
        k.request(0, 1, Op.STORE, value=5)     # 核0 在 addr1 为 M
        with self.assertRaises(CapacityExceededError):
            k.request(1, 1, Op.LOAD)
        # 但纯同步完成的请求（S 命中 / I 上写）不占槽位，仍允许。
        rep = k.request(1, 2, Op.STORE, value=8)   # addr2 目录 I，I->M
        self.assertEqual(rep.completions[0].value, 8)
        rep = k.request(1, 2, Op.LOAD)             # 刚变 M，M 命中
        self.assertEqual(rep.completions[0].value, 8)
        self.assertEqual(k.snapshot().capacity.core_inflight[1], 1)

    def test_cap_zero_allows_only_synchronous(self):
        k = CacheCoherenceKernel(core_request_cap=0)
        rep = k.request(0, 0, Op.STORE, value=9)      # I->M 同步
        self.assertEqual(rep.completions[0].value, 9)
        rep = k.request(0, 0, Op.STORE, value=10)     # M 命中同步
        self.assertEqual(rep.completions[0].value, 10)
        with self.assertRaises(CapacityExceededError):
            k.request(1, 0, Op.LOAD)                  # 读 M 需 FETCH，拒
        # 释放后（无在途）活动事务仍被 cap=0 禁止：语义即“不允许未完成请求”。
        self.assertEqual(k.snapshot().capacity.core_inflight[1], 0)

    def test_slot_released_on_completion(self):
        k = CacheCoherenceKernel(core_request_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)                   # 核1 在途=1
        drain(k)                                   # 回写完成，释放
        self.assertEqual(k.snapshot().capacity.core_inflight[1], 0)
        # 回写后 S={0,1}：核1 升级仍需失效核0，是异步事务，重新占 1 槽。
        rep = k.request(1, 0, Op.STORE, value=2)
        self.assertEqual(rep.completions, ())
        self.assertEqual(k.snapshot().capacity.core_inflight[1], 1)
        drain(k)
        self.assertEqual(k.completed()[-1].value, 2)
        self.assertEqual(k.snapshot().capacity.core_inflight[1], 0)


# ---------------------------------------------------------------------------
# 批量准入的容量原子性
# ---------------------------------------------------------------------------


class TestBatchCapacityAtomicity(unittest.TestCase):
    def test_batch_exceeding_dir_cap_aborts_entirely(self):
        k = CacheCoherenceKernel(dir_queue_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)                  # 活动
        before = k.snapshot()
        with self.assertRaises(CapacityExceededError) as cm:
            k.request_batch([Request(2, 0, Op.LOAD),    # 排队 (1/1)
                             Request(0, 0, Op.LOAD)])   # 再排队 -> 满，seq=1
        self.assertEqual(cm.exception.request_seq, 1)
        self.assertEqual(cm.exception.field, "dir_queue")
        after = k.snapshot()
        self.assertEqual(after.queues, before.queues)
        self.assertEqual(after.completed, before.completed)
        self.assertEqual(after.pending_messages, before.pending_messages)

    def test_batch_exceeding_core_cap_aborts_entirely(self):
        k = CacheCoherenceKernel(core_request_cap=1)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)                  # 核1 在途=1
        before = k.snapshot()
        # 核1 的两条新请求：第一条可能同步完成，但第二条需要在途 -> 整批回滚。
        with self.assertRaises(CapacityExceededError):
            k.request_batch([Request(1, 1, Op.STORE, value=2),  # I->M 同步
                             Request(1, 0, Op.LOAD)])            # 需在途
        after = k.snapshot()
        # 第一条的同步完成也被回滚：completed 不变。
        self.assertEqual(after.completed, before.completed)

    def test_batch_within_capacity_then_individual_exceeds(self):
        k = CacheCoherenceKernel(dir_queue_cap=2)
        k.request(0, 0, Op.STORE, value=1)
        k.request(1, 0, Op.LOAD)                  # 活动
        reps = k.request_batch([Request(2, 0, Op.LOAD),
                                Request(0, 0, Op.LOAD)])  # 排 2 个，恰好 (2/2)
        self.assertEqual([r.queued for r in reps], [True, True])
        with self.assertRaises(CapacityExceededError):
            k.request(1, 0, Op.LOAD)


# ---------------------------------------------------------------------------
# 网络通道背压
# ---------------------------------------------------------------------------


class TestNetworkBackpressure(unittest.TestCase):
    def _three_sharers(self, cap):
        k = CacheCoherenceKernel(memory={0: 100}, network_cap=cap)
        for c in range(3):
            k.request(c, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=200)      # 失效核0、核2
        return k

    def test_send_failure_preserves_message_in_blocked(self):
        k = self._three_sharers(cap=1)
        s = k.snapshot()
        # cap=1：只放得下 1 条 INV，另一条进 blocked，事务未完成。
        self.assertEqual([m.dst for m in s.pending_messages], [0])
        self.assertEqual([m.dst for m in s.blocked_messages], [2])
        self.assertEqual(s.capacity.network_pending, 1)
        self.assertEqual(s.capacity.network_blocked, 1)
        # 被 blocked 的消息不能直接投递（还没进通道）。
        blocked_id = s.blocked_messages[0].msg_id
        from msi_kernel import InvalidMessageError
        with self.assertRaises(InvalidMessageError):
            k.deliver(blocked_id)
        # 事务确实保持未完成。
        self.assertEqual(k.completed()[-1].op, Op.LOAD)

    def test_blocked_fifo_release_on_slot_freed(self):
        k = self._three_sharers(cap=1)
        # 手算的精确消息轨迹（id 按产生顺序：INV0=1, INV2=2, ACK0=3, ACK2=4）。
        inv0 = next(m for m in k.pending() if m.dst == 0)
        self.assertEqual(inv0.msg_id, 1)

        r = k.deliver(1)                         # 投 INV0
        # INV0 生效时通道仍被 INV0 占着 -> ACK0(3) 进 blocked；删除 INV0 后
        # 腾出 1 槽，blocked 队首 INV2(2) 自动补发。
        self.assertEqual([m.msg_id for m in r.blocked], [3])
        self.assertEqual([m.msg_id for m in r.released], [2])
        self.assertEqual([m.msg_id for m in k.pending()], [2])
        self.assertEqual([m.msg_id for m in k.blocked()], [3])

        r = k.deliver(2)                         # 投补发的 INV2
        self.assertEqual([m.msg_id for m in r.blocked], [4])
        self.assertEqual([m.msg_id for m in r.released], [3])
        self.assertEqual([m.msg_id for m in k.pending()], [3])
        self.assertEqual([m.msg_id for m in k.blocked()], [4])

        r = k.deliver(3)                         # 投补发的 ACK0，还差 ACK2
        self.assertEqual(r.completions, ())
        self.assertEqual([m.msg_id for m in r.released], [4])
        r = k.deliver(4)                         # ACK2 到齐，store 提交
        self.assertEqual(len(r.completions), 1)
        self.assertEqual(r.completions[0].value, 200)
        self.assertEqual(k.pending(), ())
        self.assertEqual(k.blocked(), ())
        self.assertIsNone(check_invariants(k))

    def test_blocked_progresses_under_fifo_drain(self):
        # 任何紧容量下 drain 都能把 blocked 链排空（无死锁）。
        for cap in (1, 2, 3):
            k = self._three_sharers(cap)
            drain(k)
            self.assertEqual(k.pending(), ())
            self.assertEqual(k.blocked(), ())
            self.assertEqual(k.completed()[-1].value, 200)

    def test_all_rearrangements_under_capacity_keep_invariants_and_values(self):
        # network_cap=2 下，a0 的 FETCH 先占 1 槽，a1 升级要发 2 条 INV：
        # 第 2 条 INV 发送时通道满 -> blocked。跨地址消息仍可交错，产生多条
        # 重排（blocked/pending 迁移已进入 descriptor）。每个可达状态都必须
        # 过容量不变量，且每个 load 在所有重排下读值唯一。
        scenario = [
            Request(0, 0, Op.STORE, value=50),       # 核0 M@0
            Request(1, 0, Op.LOAD),                  # FETCH a0 占 1 通道槽
            Request(0, 1, Op.LOAD),
            Request(2, 1, Op.LOAD),                  # S@1={0,2}
            Request(1, 1, Op.STORE, value=60),       # 2 INV：第 2 条 blocked
        ]

        def factory():
            return CacheCoherenceKernel(network_cap=2)

        res = explore(factory, scenario)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertGreater(len(res.schedules), 1, "紧容量下应存在多种重排")
        self.assertEqual(res.terminal_states, 1)
        # txn2 读 M 回写得 50；txn3/txn4 读 a1 内存初值 0。
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {2: {50}, 3: {0}, 4: {0}})
        # 任一终态都无残留 blocked，且完成取值序列与无容量模型一致。
        ku = CacheCoherenceKernel()
        ku.request_batch(scenario)
        drain(ku)
        capped_final = next(iter(res.schedules.values()))
        self.assertEqual([c.value for c in capped_final],
                         [c.value for c in ku.completed()])

    def test_retry_blocked_is_noop_when_channel_full(self):
        k = self._three_sharers(cap=1)
        # 通道满载（INV0 在 pending，INV2 在 blocked）：无空位，显式重试无效。
        self.assertEqual(k.retry_blocked(), ())
        self.assertEqual([m.msg_id for m in k.blocked()], [2])
        k.deliver(1)                             # 腾出槽位后已自动补发
        # 此刻 pending=INV2、blocked=ACK0；retry_blocked 仍因满载而无新增。
        self.assertEqual(k.retry_blocked(), ())

    def test_blocked_messages_are_not_lost_and_final_state_valid(self):
        k = CacheCoherenceKernel(memory={0: 100}, network_cap=1)
        for c in range(3):
            k.request(c, 0, Op.LOAD)
        k.request(1, 0, Op.STORE, value=200)
        # 再排队两个后续请求，制造“满队列 + 回写 + 升级”链。
        k.request(2, 0, Op.LOAD)
        k.request(0, 0, Op.STORE, value=300)
        drain(k)
        s = k.snapshot()
        self.assertEqual(k.blocked(), ())
        # 手算：200 升级 -> 核2 读回写的 200 -> 核0 再升级写 300，最终 M=核0。
        self.assertEqual(s.directory[0].state, State.MODIFIED)
        self.assertEqual(s.directory[0].owner, 0)
        self.assertEqual(s.memory[0], 200)       # 最后一次升级前回写过 200
        self.assertEqual(
            [c.value for c in k.completed() if c.op is Op.LOAD],
            [100, 100, 100, 200])
        self.assertEqual(k.completed()[-1].value, 300)
        self.assertIsNone(check_invariants(k))

    def test_zero_network_cap_deadends_with_clear_error(self):
        # network_cap=0：同步请求仍可完成；一旦事务必须发消息，消息全 blocked，
        # 没有任何可投递报文能腾出槽位 -> drain 明确报 InfeasibleError（不是
        # 静默返回未完成，也不是预算耗尽）。
        k = CacheCoherenceKernel(network_cap=0)
        k.request(0, 0, Op.STORE, value=1)            # I->M 同步
        rep = k.request(1, 0, Op.LOAD)                # 需 FETCH
        self.assertEqual(rep.completions, ())
        self.assertEqual(len(k.blocked()), 1)
        self.assertEqual(k.pending(), ())
        with self.assertRaises(InfeasibleError):
            drain(k)
        # blocked 消息被完整保留、未丢失。
        self.assertEqual([m.mtype for m in k.blocked()], [MsgType.FETCH])

    def test_capacity_invariant_catches_underfilled_blocked_state(self):
        # 人为制造“blocked 非空但通道未满”的不可能状态，检查器必须抓到。
        k = self._three_sharers(cap=1)
        # 正常状态：pending=INV0(满载), blocked=INV2。手动掏空 pending 而不
        # 触发补发，制造违例。
        k._pending.clear()
        bad = check_invariants(k)
        self.assertIsNotNone(bad)
        self.assertIn("blocked", bad)


# ---------------------------------------------------------------------------
# 2 核 2 地址：满队列 / 回写 / 请求交错，读值与“无容量旧模型”逐事务一致
# ---------------------------------------------------------------------------


def _admit_with_retry(k, req, steps, rng=None):
    """容量不足时投递一条最低 id 待发消息腾出空间，再重试同一请求。

    返回该请求最终的 RequestReport。所有容量拒绝都是暂时的。
    """
    while True:
        try:
            return k.request(req.core, req.addr, req.op, value=req.value,
                             txn_id=req.txn_id)
        except CapacityExceededError:
            pend = k.pending()
            assert pend, "容量满却没有可投递消息（不应发生）"
            k.deliver(pend[0].msg_id)
            steps.append(pend[0].msg_id)


def _drain_collect(k, steps):
    while True:
        pend = k.pending()
        if not pend:
            return
        k.deliver(pend[0].msg_id)
        steps.append(pend[0].msg_id)


class TestTwoCoreTwoAddrEquivalence(unittest.TestCase):
    def _scenarios(self):
        rng = _lcg(20260924)
        scenarios = []
        for _ in range(24):
            reqs = []
            for _ in range(6):
                core = next(rng) % 2            # 仅核 0/1
                addr = next(rng) % 2            # 仅地址 0/1
                if next(rng) % 2 == 0:
                    reqs.append(Request(core, addr, Op.STORE,
                                        value=10 + next(rng) % 90))
                else:
                    reqs.append(Request(core, addr, Op.LOAD))
            scenarios.append(reqs)
        return scenarios

    def test_capped_matches_uncapped_across_capacities(self):
        caps = [
            CapacityConfig(network_cap=1, dir_queue_cap=0, core_request_cap=1),
            CapacityConfig(network_cap=1, dir_queue_cap=1, core_request_cap=1),
            CapacityConfig(network_cap=2, dir_queue_cap=1, core_request_cap=2),
            CapacityConfig(network_cap=3, dir_queue_cap=2, core_request_cap=2),
            CapacityConfig(network_cap=1, dir_queue_cap=None,
                           core_request_cap=None),
        ]
        for ci, cap in enumerate(caps):
            for si, scenario in enumerate(self._scenarios()):
                with self.subTest(cap=ci, scenario=si):
                    # 受限内核：边推进边准入（请求与回写交错），每步查不变量。
                    kc = CacheCoherenceKernel(capacity=cap)
                    steps = []
                    for req in scenario:
                        _admit_with_retry(kc, req, steps)
                        self.assertIsNone(check_invariants(kc))
                    _drain_collect(kc, steps)
                    self.assertIsNone(check_invariants(kc))
                    self.assertEqual(kc.blocked(), ())
                    capped_vals = {c.txn_id: (c.op.value, c.addr, c.value)
                                   for c in kc.completed()}

                    # 无容量旧模型：一次准入、FIFO 排空。
                    ku = CacheCoherenceKernel()
                    ku.request_batch(scenario)
                    drain(ku)
                    uncapped_vals = {c.txn_id: (c.op.value, c.addr, c.value)
                                     for c in ku.completed()}

                    # 逐事务：每个 load/store 的地址与读/写值必须完全一致。
                    self.assertEqual(capped_vals, uncapped_vals)
                    self.assertEqual(len(capped_vals), len(scenario))
                    # 独立参考：完成序列中每个 load 读“该地址之前最后写入”。
                    flat = [(c.addr, c.op.value, c.value)
                            for c in kc.completed()]
                    check_completion_sequence(flat, {})

    def test_handcalculated_writeback_interleave_value(self):
        # 一个手算的 2 核 2 地址例子，显式断言每一步读值（不用内核算期望）。
        cap = CapacityConfig(network_cap=1, dir_queue_cap=1, core_request_cap=2)
        k = CacheCoherenceKernel(memory={0: 0, 1: 0}, capacity=cap)
        steps = []
        # 核0 在两个地址上分别写不同值；核1 交错读取触发两次回写。
        plan = [
            Request(0, 0, Op.STORE, value=11),   # M@0=核0
            Request(0, 1, Op.STORE, value=22),   # M@1=核0
            Request(1, 0, Op.LOAD),              # 回写 -> 读 11
            Request(1, 1, Op.LOAD),              # 回写 -> 读 22
            Request(1, 0, Op.STORE, value=33),   # S@0={0,1} 升级，失效核0
            Request(0, 1, Op.LOAD),              # 核0 读自己 M 的 22（M 命中）
        ]
        for req in plan:
            _admit_with_retry(k, req, steps)
        _drain_collect(k, steps)
        loads = [(c.addr, c.value) for c in k.completed() if c.op is Op.LOAD]
        # 手算顺序的读值：addr0 回写读 11；addr1 回写读 22；核0 M 命中读 22。
        self.assertEqual(loads, [(0, 11), (1, 22), (1, 22)])
        stores = {c.addr: c.value for c in k.completed()
                  if c.op is Op.STORE}
        self.assertEqual(stores, {0: 33, 1: 22})
        self.assertIsNone(check_invariants(k))
        # 与无容量模型逐事务对照。
        ku = CacheCoherenceKernel(memory={0: 0, 1: 0})
        ku.request_batch(plan)
        drain(ku)
        self.assertEqual(
            {c.txn_id: c.value for c in k.completed()},
            {c.txn_id: c.value for c in ku.completed()})


if __name__ == "__main__":
    unittest.main()
