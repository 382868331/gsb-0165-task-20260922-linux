"""容量上限与背压测试（本次迭代新增）。

覆盖：
- Limits 配置校验（非正整数 / bool / 非 int 拒绝）；
- 目录每地址等待队列上限：正常、恰好满边界、超限明确报错且零半更新；
- 每核未完成请求上限：同步完成不占容量、需往返才占、超限报错、完成后释放；
- 网络有向通道上限：满时消息保留在发送端（retained）、事务保持未完成、
  保留消息不可投递、腾出空间后按 FIFO 续传、续传后读值正确；
- 2 核 2 地址满队列 / 写回 / 请求交错下，紧容量内核在全部消息重排上的
  完成读值与“宽容量（等价旧无容量）模型”逐调度一致。

期望值不调用被测实现生成：跨模型一致性比较两个独立配置内核的产出，
读值允许值另由 tests/reference.py 的平凡抽象机独立给出。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from msi_kernel import (
    DIR,
    CacheCoherenceKernel,
    CapacityExceededError,
    DeliveryStatus,
    InvalidMessageError,
    InvalidRequestError,
    Limits,
    Message,
    MsgType,
    Op,
    Request,
    drain,
    explore,
)
from reference import check_completion_sequence


def _flat(completed):
    return [(c.addr, c.op.value, c.value) for c in completed]


class TestLimitsValidation(unittest.TestCase):
    def test_defaults_are_finite(self):
        lim = Limits()
        self.assertGreater(lim.queue_cap, 0)
        self.assertGreater(lim.core_cap, 0)
        self.assertGreater(lim.channel_cap, 0)
        k = CacheCoherenceKernel()
        self.assertEqual(k.limits, lim)
        self.assertIsNotNone(k.snapshot().limits)

    def test_non_positive_rejected(self):
        for kwargs in ({"queue_cap": 0}, {"core_cap": -1}, {"channel_cap": 0}):
            with self.assertRaises(InvalidRequestError):
                Limits(**kwargs)

    def test_bool_and_non_int_rejected(self):
        with self.assertRaises(InvalidRequestError):
            Limits(queue_cap=True)
        with self.assertRaises(InvalidRequestError):
            Limits(core_cap=1.0)
        with self.assertRaises(InvalidRequestError):
            Limits(channel_cap="2")

    def test_kernel_rejects_bad_limits_type(self):
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(limits={"queue_cap": 1})
        with self.assertRaises(InvalidRequestError):
            CacheCoherenceKernel(limits=4)


class TestQueueCapacity(unittest.TestCase):
    def _kernel(self, cap):
        # addr0：核0/1/2 已 S，核1 的 store 正在等两个 INV（活动事务）。
        k = CacheCoherenceKernel(memory={0: 0},
                                 limits=Limits(queue_cap=cap, core_cap=8,
                                               channel_cap=8))
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=5),
        ])
        return k

    def test_queue_boundary_exactly_full_then_reject(self):
        k = self._kernel(cap=1)
        rep = k.request(2, 0, Op.LOAD)          # 恰好占满唯一队列位
        self.assertTrue(rep.queued)
        self.assertEqual(len(k.snapshot().queues[0]), 1)
        with self.assertRaises(CapacityExceededError) as cm:
            k.request(0, 0, Op.STORE, value=6)  # 第 2 条：满
        self.assertEqual(cm.exception.field, "queue")
        self.assertEqual(cm.exception.used, 1)
        self.assertEqual(cm.exception.cap, 1)

    def test_rejection_leaves_no_partial_state(self):
        k = self._kernel(cap=1)
        k.request(2, 0, Op.LOAD)
        before_completed = k.completed()
        before_queue = list(k.snapshot().queues[0])
        before_pending = k.pending()
        with self.assertRaises(CapacityExceededError):
            k.request(0, 0, Op.STORE, value=6)
        self.assertEqual(k.completed(), before_completed)
        self.assertEqual(list(k.snapshot().queues[0]), before_queue)
        self.assertEqual(k.pending(), before_pending)
        self.assertEqual(k.retained_messages(), ())

    def test_drain_after_full_queue_gives_correct_values(self):
        # 队列满只是拒绝“新准入”，已排队请求在事务完成后照常推进。
        k = self._kernel(cap=2)
        k.request(2, 0, Op.LOAD)                 # 排队 1：读到 5
        k.request(0, 0, Op.STORE, value=6)       # 排队 2
        drain(k)
        self.assertEqual(
            [(c.op.value, c.value) for c in k.completed()[3:]],
            [("store", 5), ("load", 5), ("store", 6)])
        self.assertEqual(k.snapshot().queues[0], ())

    def test_batch_aborts_atomically_when_queue_fills_mid_batch(self):
        k = self._kernel(cap=1)
        batch = [Request(2, 0, Op.LOAD),         # 占满队列位
                 Request(0, 0, Op.LOAD)]         # 队列满 -> 整批中止
        with self.assertRaises(CapacityExceededError) as cm:
            k.request_batch(batch)
        self.assertEqual(cm.exception.request_seq, 1)
        self.assertEqual(cm.exception.field, "queue")
        # 第一条也不得留下：队列仍为空。
        self.assertEqual(k.snapshot().queues[0], ())
        self.assertEqual(len(k.completed()), 3)  # 只有最初三次 load


class TestCoreOutstandingCapacity(unittest.TestCase):
    def test_immediate_completion_does_not_consume_capacity(self):
        # core_cap=1：I 上读 / 唯一共享者升级等同步完成不占容量，可连续做。
        k = CacheCoherenceKernel(
            memory={0: 0, 2: 7},
            limits=Limits(queue_cap=8, core_cap=1, channel_cap=8))
        k.request(0, 2, Op.LOAD)                 # I->S 同步完成
        k.request(0, 2, Op.STORE, value=8)       # 唯一 S 升级，同步完成
        k.request(0, 0, Op.LOAD)                 # 另一地址 I->S 同步完成
        self.assertEqual(len(k.completed()), 3)

    def test_blocking_requests_are_counted_until_complete(self):
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0, 2: 0},
            limits=Limits(queue_cap=8, core_cap=2, channel_cap=8))
        k.request(1, 0, Op.STORE, value=1)       # 核1 拥有 addr0(M)
        k.request(1, 1, Op.STORE, value=2)       # 核1 拥有 addr1(M)
        k.request(1, 2, Op.STORE, value=3)       # 核1 拥有 addr2(M)
        k.request(0, 0, Op.LOAD)                 # 核0：FETCH 往返，占用 1
        k.request(0, 1, Op.LOAD)                 # 占用 2（满）
        with self.assertRaises(CapacityExceededError) as cm:
            k.request(0, 2, Op.LOAD)             # 核0 第 3 条往返：满
        self.assertEqual(cm.exception.field, "core_outstanding")
        self.assertEqual(cm.exception.used, 2)
        self.assertEqual(cm.exception.cap, 2)

    def test_capacity_freed_on_completion(self):
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0},
            limits=Limits(queue_cap=8, core_cap=1, channel_cap=8))
        k.request(1, 0, Op.STORE, value=1)       # 核1 M addr0
        k.request(0, 0, Op.LOAD)                 # 核0 唯一在途请求（占满）
        # 在另一地址的同步读不受影响（不占容量）：
        rep = k.request(0, 1, Op.LOAD)
        self.assertEqual(rep.completions[0].value, 0)
        # 排空在途请求后，核0 容量释放，可再发需往返请求。
        drain(k)
        k.request(1, 1, Op.STORE, value=9)       # 核1 占 addr1 M
        rep = k.request(0, 1, Op.LOAD)           # 读 M：FETCH 往返
        self.assertEqual(rep.completions, ())
        drain(k)
        self.assertEqual(k.completed()[-1].value, 9)


class TestChannelBackpressure(unittest.TestCase):
    def test_retained_when_channel_full_and_not_deliverable(self):
        k = CacheCoherenceKernel(
            memory={0: 10, 1: 20},
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=1))
        k.request(0, 0, Op.STORE, value=11)
        k.request(0, 1, Op.STORE, value=21)
        k.request(1, 0, Op.LOAD)                 # FETCH DIR->0 在途（满）
        k.request(2, 1, Op.LOAD)                 # FETCH DIR->0 被保留
        self.assertEqual(len(k.pending()), 1)
        retained = k.retained_messages()
        self.assertEqual(len(retained), 1)
        self.assertEqual((retained[0].src, retained[0].dst), (DIR, 0))
        self.assertEqual(k.channel_load(DIR, 0), 1)
        # 通道视图与保留视图都可见。
        snap = k.snapshot()
        self.assertEqual([(c.src, c.dst, len(c.in_flight))
                          for c in snap.channels], [(DIR, 0, 1)])
        self.assertEqual(len(snap.retained[0].retained), 1)
        # 事务保持未完成：还没有任何 load 完成事件。
        self.assertEqual(
            [c.value for c in k.completed() if c.op is Op.LOAD], [])
        with self.assertRaises(InvalidMessageError):
            k.deliver(retained[0].msg_id)

    def test_release_is_fifo_and_report_lists_released(self):
        # 同一通道 DIR->0 上压 3 条 FETCH（channel_cap=1）：1 在途 2 保留，
        # 续传必须严格按 msg_id 升序。
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0, 2: 0},
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=1))
        for a in range(3):
            k.request(0, a, Op.STORE, value=100 + a)
        k.request(1, 0, Op.LOAD)
        k.request(1, 1, Op.LOAD)
        k.request(1, 2, Op.LOAD)
        in_flight = k.pending()
        self.assertEqual(len(in_flight), 1)
        retained_ids = [m.msg_id for m in k.retained_messages()]
        self.assertEqual(len(retained_ids), 2)
        # 逐条投递在途 FETCH；每次腾出通道后恰续传 1 条，顺序为 FIFO。
        cur = in_flight[0].msg_id
        order = [cur]
        for _ in range(2):
            rep = k.deliver(cur)               # FETCH 到达，owner 发 FLUSH
            self.assertEqual(len(rep.released), 1)
            nxt = rep.released[0].msg_id
            order.append(nxt)
            self.assertEqual(nxt, retained_ids.pop(0))  # 续传最老的保留消息
            cur = nxt
        self.assertEqual(order, sorted(order))
        drain(k)
        self.assertEqual(
            [(c.addr, c.value) for c in k.completed() if c.op is Op.LOAD],
            [(0, 100), (1, 101), (2, 102)])

    def test_emitted_vs_released_are_distinct_in_report(self):
        # 满通道上投递在途消息：它处理时新产生的反向报文（FLUSH，走另一
        # 通道）在 emitted；同通道腾出后续传的保留报文在 released。
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0},
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=1))
        k.request(0, 0, Op.STORE, value=11)
        k.request(0, 1, Op.STORE, value=22)
        k.request(1, 0, Op.LOAD)
        k.request(2, 1, Op.LOAD)               # 第二条 DIR->0 FETCH 被保留
        rep = k.deliver(k.pending()[0].msg_id)
        self.assertEqual([m.mtype for m in rep.emitted], [MsgType.FLUSH])
        self.assertEqual([m.mtype for m in rep.released], [MsgType.FETCH])
        self.assertEqual(rep.released[0].addr, 1)

    def test_stale_injected_does_not_touch_channels_or_retained(self):
        # 外部注入的陈旧报文不占通道：投递它不得腾出通道、不得触发续传。
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0},
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=1))
        k.request(0, 0, Op.STORE, value=5)
        k.request(1, 0, Op.LOAD)
        fetch = k.pending()[0]
        flush = k.deliver(fetch.msg_id).emitted[0]
        k.deliver(flush.msg_id)                 # txn2 结束，S={0,1}
        k.request(0, 1, Op.STORE, value=7)
        k.request(1, 1, Op.LOAD)                # FETCH DIR->0 addr1 在途
        k.request(0, 2, Op.STORE, value=8)
        k.request(2, 2, Op.LOAD)                # FETCH DIR->0 addr2 保留
        self.assertEqual(k.channel_load(DIR, 0), 1)
        self.assertEqual(len(k.retained_messages()), 1)
        stale = k.deliver(Message(None, 2, MsgType.FETCH,
                                  src=DIR, dst=0, addr=0))
        self.assertIs(stale.status, DeliveryStatus.IGNORED_STALE)
        self.assertEqual(stale.released, ())
        self.assertEqual(k.channel_load(DIR, 0), 1)
        self.assertEqual(len(k.retained_messages()), 1)

    def test_backpressure_does_not_change_read_values(self):
        # 同一固定场景：channel_cap=1 的紧容量内核 drain 结果与
        # channel_cap=8 的宽容量内核逐完成事件一致。
        scenario = [
            Request(0, 0, Op.STORE, value=11),
            Request(0, 1, Op.STORE, value=22),
            Request(1, 0, Op.LOAD),
            Request(2, 1, Op.LOAD),
            Request(1, 1, Op.STORE, value=33),
            Request(2, 0, Op.LOAD),
        ]
        mem = {0: 0, 1: 0}

        def run(cap):
            k = CacheCoherenceKernel(
                memory=dict(mem),
                limits=Limits(queue_cap=8, core_cap=8, channel_cap=cap))
            k.request_batch(list(scenario))
            drain(k)
            return k.completed()

        tight = run(1)
        wide = run(8)
        self.assertEqual(_flat(tight), _flat(wide))
        check_completion_sequence(_flat(tight), mem)


class TestTwoCoreTwoAddrInterleaving(unittest.TestCase):
    """题面指定核对：2 核 2 地址、满队列 + 写回 + 请求交错。

    固定 3 核 8 地址是契约，故只使用核 {0,1}、地址 {0,1}，并把容量压到
    最小（channel_cap=1、queue_cap=2），枚举全部消息重排，逐调度与
    “宽容量（等价旧无容量）模型”比较完成读值。
    """

    SCENARIOS = [
        [  # 双地址各自 M owner 在不同核，交叉读触发两条回写
            Request(0, 0, Op.STORE, value=10),
            Request(1, 1, Op.STORE, value=20),
            Request(1, 0, Op.LOAD),
            Request(0, 1, Op.LOAD),
            Request(1, 0, Op.STORE, value=30),
        ],
        [  # 共享后两个升级排队 + 另一地址写回交错
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(0, 1, Op.STORE, value=40),
            Request(1, 1, Op.LOAD),
            Request(1, 0, Op.STORE, value=50),
            Request(0, 0, Op.STORE, value=60),
        ],
        [  # 同地址连续读写把队列压满，另一地址独立回写
            Request(0, 0, Op.STORE, value=70),
            Request(1, 0, Op.LOAD),
            Request(0, 0, Op.STORE, value=80),
            Request(1, 0, Op.LOAD),
            Request(0, 1, Op.LOAD),
            Request(1, 1, Op.STORE, value=90),
        ],
    ]

    TIGHT = Limits(queue_cap=2, core_cap=4, channel_cap=1)
    WIDE = Limits(queue_cap=8, core_cap=8, channel_cap=8)

    def test_all_rearrangements_match_wide_model(self):
        for idx, scenario in enumerate(self.SCENARIOS):
            res_tight = explore(
                lambda: CacheCoherenceKernel(limits=self.TIGHT),
                scenario, budget=200_000)
            res_wide = explore(
                lambda: CacheCoherenceKernel(limits=self.WIDE),
                scenario, budget=200_000)
            self.assertFalse(res_tight.exhausted, f"scenario {idx} 紧容量预算耗尽")
            self.assertFalse(res_wide.exhausted, f"scenario {idx} 宽容量预算耗尽")
            self.assertEqual(res_tight.violations, (), f"scenario {idx}")
            self.assertEqual(res_wide.violations, (), f"scenario {idx}")
            # 题面核心：每个 load 在紧容量全部重排下的读值集合，与旧的
            # 宽容量（等价无容量限制）模型完全一致。
            self.assertEqual(
                {t: set(v) for t, v in res_tight.read_values.items()},
                {t: set(v) for t, v in res_wide.read_values.items()},
                f"scenario {idx} 读值集合不一致")
            # 紧容量的每条完成序列都满足独立参考的允许读值。
            for seq in res_tight.schedules.values():
                check_completion_sequence(_flat(seq), {})
            # 容量只延迟不禁止：终态（队列/在途/保留全空）的可达集合相同。
            self.assertEqual(res_tight.terminal_states, res_wide.terminal_states,
                             f"scenario {idx}")
            # 紧容量是宽容量合法调度的子集：把紧容量枚举出的每条消息
            # 路径在宽容量内核上原样重放（两模型 msg_id 一一对应，保留
            # 消息也在发送时分配 id），必须逐消息可执行且完成序列相同。
            for path, tight_comp in res_tight.schedules.items():
                wide = CacheCoherenceKernel(limits=self.WIDE)
                wide.request_batch(list(scenario))
                for mid in path:
                    wide.deliver(mid)
                self.assertEqual(_flat(wide.completed()), _flat(tight_comp),
                                 f"scenario {idx} path {path}")
                # 路径走完后宽容量内核也应能排空（紧容量路径不制造死锁）。
                drain(wide)
                self.assertEqual(wide.pending(), ())
                self.assertEqual(wide.retained_messages(), ())

    def test_tight_queue_rejects_beyond_cap_but_completes_what_admitted(self):
        # queue_cap=1：同地址活动事务 + 1 条排队占满，第 2 条排队被拒；
        # 拒入后把已接纳部分排空，读值仍满足独立参考允许值。
        k = CacheCoherenceKernel(
            limits=Limits(queue_cap=1, core_cap=8, channel_cap=1))
        k.request_batch([
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=50),   # 活动事务
        ])
        k.request(0, 0, Op.LOAD)                 # 占满队列位
        with self.assertRaises(CapacityExceededError):
            k.request(2, 0, Op.LOAD)             # 被拒
        drain(k)
        flats = _flat(k.completed())
        self.assertEqual(flats,
                         [(0, "load", 0), (0, "load", 0),
                          (0, "store", 50), (0, "load", 50)])
        check_completion_sequence(flats, {})


class TestCapacityBatchAtomicity(unittest.TestCase):
    def test_core_cap_mid_batch_abort_leaves_zero_change(self):
        # core_cap=1：批次第 1 条让核0 占满（需往返），第 2 条再要核0 往返，
        # 整批必须零变更。
        k = CacheCoherenceKernel(
            memory={0: 0, 1: 0},
            limits=Limits(queue_cap=8, core_cap=1, channel_cap=8))
        k.request(1, 0, Op.STORE, value=1)       # 核1 占 addr0 M
        k.request(1, 1, Op.STORE, value=2)       # 核1 占 addr1 M
        snap_before = k.snapshot()
        batch = [Request(0, 0, Op.LOAD),         # 核0 FETCH，占满
                 Request(0, 1, Op.LOAD)]         # 核0 第 2 条 -> 满
        with self.assertRaises(CapacityExceededError) as cm:
            k.request_batch(batch)
        self.assertEqual(cm.exception.request_seq, 1)
        self.assertEqual(cm.exception.field, "core_outstanding")
        # 连第一条都不得留下。
        self.assertEqual(k.snapshot().directory[0].active_txn, None)
        self.assertEqual(k.pending(), ())
        self.assertEqual(k.retained_messages(), ())
        self.assertEqual(k.completed(), snap_before.completed)

    def test_invalid_batch_rolls_back_even_with_retained_sends(self):
        # 紧通道下，批次前半会触发背压保留；若批次后半非法，整批（含保留
        # 消息与活动事务）必须回滚干净。
        k = CacheCoherenceKernel(
            memory={a: 0 for a in range(4)},
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=1))
        for a in range(2):
            k.request(0, a, Op.STORE, value=10 + a)  # 核0 占两地址 M
        before = k.snapshot()
        # 第 1 条：addr0 FETCH DIR->0 在途；第 2 条：addr1 FETCH 保留；
        # 第 3 条非法（addr 越界）-> 整批回滚。
        bad_batch = [
            Request(1, 0, Op.LOAD),
            Request(2, 1, Op.LOAD),
            Request(0, 99, Op.LOAD),
        ]
        with self.assertRaises(InvalidRequestError) as cm:
            k.request_batch(bad_batch)
        self.assertEqual(cm.exception.request_seq, 2)
        self.assertEqual(k.pending(), before.pending_messages)
        self.assertEqual(k.retained_messages(), ())
        self.assertEqual(k.completed(), before.completed)
        for a in range(2):
            self.assertEqual(k.snapshot().directory[a].active_txn, None)


class TestDefaultLimitsUnchanged(unittest.TestCase):
    def test_default_flow_has_no_backpressure(self):
        # 默认容量在固定 3 核 8 地址正常并发下不应触发背压。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.LOAD), Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD), Request(1, 0, Op.STORE, value=5),
        ])
        self.assertEqual(k.retained_messages(), ())
        drain(k)
        self.assertEqual(k.retained_messages(), ())
        self.assertEqual(k.pending(), ())
        self.assertEqual(k.completed()[-1].value, 5)

    def test_max_concurrent_fetch_to_one_owner_fits_default(self):
        # 8 地址 owner 都是核0，8 个跨核读产生 8 条并发 FETCH DIR->0，
        # 默认 channel_cap=8 恰好容纳：无保留、全部顺利完成。
        k = CacheCoherenceKernel(memory={a: a for a in range(8)})
        for a in range(8):
            k.request(0, a, Op.STORE, value=100 + a)
        for a in range(8):
            k.request(1, a, Op.LOAD)
        self.assertEqual(k.retained_messages(), ())
        self.assertEqual(k.channel_load(DIR, 0), 8)
        drain(k)
        self.assertEqual(k.retained_messages(), ())
        for a in range(8):
            load_comp = [c for c in k.completed()
                         if c.op is Op.LOAD and c.addr == a]
            self.assertEqual(load_comp[0].value, 100 + a)


class TestTightChannelDeterministicScenarios(unittest.TestCase):
    """确定性 LCG 多场景：channel_cap=1 与宽容量逐完成事件一致。

    只压紧通道（queue/core 放宽），保证同一场景在两种配置下都能被完整
    接纳；期望值由独立参考机 check_completion_sequence 判定，不调用被测
    实现生成。
    """

    @staticmethod
    def _lcg(seed):
        state = seed
        while True:
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            yield state

    def test_many_scenarios_tight_vs_wide_drain(self):
        rng = self._lcg(20260924)
        for case in range(60):
            init = {a: (next(rng) % 40) for a in range(2)
                    if next(rng) % 2 == 0}
            scenario = []
            for _ in range(7):
                core = next(rng) % 3
                addr = next(rng) % 2
                if next(rng) % 2 == 0:
                    scenario.append(Request(core, addr, Op.STORE,
                                            value=100 + next(rng) % 50))
                else:
                    scenario.append(Request(core, addr, Op.LOAD))

            def run(channel_cap):
                k = CacheCoherenceKernel(
                    memory=dict(init),
                    limits=Limits(queue_cap=8, core_cap=8,
                                  channel_cap=channel_cap))
                k.request_batch(list(scenario))
                drain(k)
                self.assertEqual(k.pending(), (), f"case {case}")
                self.assertEqual(k.retained_messages(), (), f"case {case}")
                return _flat(k.completed())

            tight = run(1)
            wide = run(8)
            self.assertEqual(tight, wide, f"case {case} 紧/宽容量完成序列不同")
            check_completion_sequence(tight, init)


if __name__ == "__main__":
    unittest.main()
