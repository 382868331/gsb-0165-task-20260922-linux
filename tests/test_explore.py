"""explore：消息重排枚举、有界搜索、预算耗尽 vs 无解 的区分测试。"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from msi_kernel import (
    BudgetExhaustedError,
    CacheCoherenceKernel,
    InfeasibleError,
    Op,
    Request,
    State,
    apply_path,
)
from msi_kernel.explore import (
    check_invariants,
    descriptor,
    drain,
    explore,
    find_schedule,
)


class TestExploration(unittest.TestCase):
    def test_reads_are_value_deterministic_across_rearrangements(self):
        # 写后跨核读 + 多个共享者升级：消息重排方式很多，但读值唯一。
        scenario = [
            Request(0, 0, Op.STORE, value=50),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(0, 1, Op.LOAD),
            Request(2, 1, Op.LOAD),
            Request(0, 1, Op.STORE, value=60),
            Request(1, 1, Op.STORE, value=70),
        ]
        res = explore(lambda: CacheCoherenceKernel(), scenario)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertGreater(len(res.schedules), 1, "应存在多种消息重排")
        # 所有调度的终态结构相同。
        self.assertEqual(res.terminal_states, 1)
        self.assertEqual({t: set(v) for t, v in res.read_values.items()},
                         {2: {50}, 3: {50}, 4: {0}, 5: {0}})

    def test_violation_detector_can_catch_bad_state(self):
        # 直接篡改内核内部制造“两个 M”，检查器必须抓到。
        k = CacheCoherenceKernel()
        k.request(0, 0, Op.STORE, value=1)
        bad_line = k._caches[1][0]
        bad_line.state = State.MODIFIED
        bad_line.value = 2
        self.assertIn("2 个 M", check_invariants(k))

    def test_descriptor_independent_of_message_ids(self):
        def scenario_kernel():
            k = CacheCoherenceKernel(memory={0: 0})
            k.request_batch([Request(0, 0, Op.LOAD),
                             Request(1, 0, Op.LOAD),
                             Request(1, 0, Op.STORE, value=9)])
            return k

        k1 = scenario_kernel()
        d1 = descriptor(k1)
        # 投递一个 INV 后，状态指纹改变；待发多重集从 2 INV 变成 1 INV+1 ACK。
        inv = k1.pending()[0]
        k1.deliver(inv.msg_id)
        d2 = descriptor(k1)
        self.assertNotEqual(d1, d2)
        drain(k1)
        # 终态稳定可重复：不同投递过程到达同一终态指纹。
        k3 = scenario_kernel()
        drain(k3)
        self.assertEqual(descriptor(k1), descriptor(k3))

    def test_find_schedule_returns_viable_path(self):
        scenario = [Request(0, 0, Op.LOAD),
                    Request(1, 0, Op.STORE, value=5),
                    Request(2, 0, Op.LOAD)]
        path = find_schedule(lambda: CacheCoherenceKernel(memory={0: 0}),
                             scenario)
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch(scenario)
        apply_path(k, path)
        self.assertEqual(k.pending(), ())
        self.assertEqual(k.completed()[-1].value, 5)


class TestBudgetVsInfeasible(unittest.TestCase):
    def test_budget_exhausted_is_distinct_from_infeasible(self):
        # 目标恒假且状态空间有限：给足预算 -> 穷尽 -> Infeasible。
        with self.assertRaises(InfeasibleError):
            find_schedule(
                lambda: CacheCoherenceKernel(memory={0: 0}),
                [Request(0, 0, Op.LOAD), Request(1, 0, Op.STORE, value=1)],
                goal=lambda k: False,
                budget=1_000,
            )

    def test_budget_exhausted_raised_when_too_small(self):
        # 同一目标但预算只允许访问 1 个状态 -> BudgetExhausted（没证明无解）。
        with self.assertRaises(BudgetExhaustedError) as cm:
            find_schedule(
                lambda: CacheCoherenceKernel(memory={0: 0}),
                [Request(0, 0, Op.LOAD),
                 Request(1, 0, Op.LOAD),
                 Request(2, 0, Op.LOAD),
                 Request(1, 0, Op.STORE, value=1)],
                goal=lambda k: False,
                budget=1,
            )
        self.assertEqual(cm.exception.budget, 1)
        self.assertGreaterEqual(cm.exception.used, 1)

    def test_explore_flags_exhaustion_but_keeps_partial_results(self):
        scenario = [Request(0, 0, Op.LOAD), Request(1, 0, Op.LOAD),
                    Request(2, 0, Op.LOAD),
                    Request(1, 0, Op.STORE, value=1)]
        res = explore(lambda: CacheCoherenceKernel(memory={0: 0}), scenario,
                      budget=2)
        self.assertTrue(res.exhausted)
        # 预算耗尽不等于出错：已访问路径上仍做了不变量检查。
        self.assertEqual(res.violations, ())
        self.assertLessEqual(res.steps, res.budget + 3)

    def test_drain_budget_guard(self):
        # drain 的预算是防御上限；正常场景用极小预算会抛 BudgetExhausted。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([Request(0, 0, Op.LOAD),
                         Request(1, 0, Op.LOAD),
                         Request(2, 0, Op.LOAD),
                         Request(1, 0, Op.STORE, value=1)])
        with self.assertRaises(BudgetExhaustedError):
            drain(k, budget=1)


if __name__ == "__main__":
    unittest.main()
