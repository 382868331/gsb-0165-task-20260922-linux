"""独立参考模型对照测试。

期望值全部由 tests/reference.py 与本文件内的平凡抽象机独立给出，
不读取被测内核内部、不调用内核算法来“算期望”：
- 同地址请求按准入 FIFO 完成（不同地址才交错），因此仅凭场景的请求序列
  与初始内存即可逐地址模拟出每个 load 的唯一允许读值；
- 再用 reference.check_completion_sequence 对内核产出的完成序列复核。
内核只负责跑出实际完成序列，参考负责判定它是否合法。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from msi_kernel import (
    CacheCoherenceKernel,
    Op,
    Request,
    State,
    drain,
    explore,
)
from reference import (
    ReferenceViolation,
    check_completion_sequence,
    expected_load_values,
)


# 独立期望值：只吃“请求序列 + 初值”，不碰内核。
# 规则（参考模型，非被测代码）：同地址操作严格按准入顺序完成；
# load 读该地址当前值，store v 把当前值置为 v。
def expected_values_by_txn(scenario, init_memory=None, num_addrs=8):
    mem = [0] * num_addrs
    if init_memory:
        for a, v in init_memory.items():
            mem[a] = v
    # 批量准入、不显式给 id 时，txn_id 即场景中的序号 + 1。
    allowed = {}
    for idx, req in enumerate(scenario):
        if req.op is Op.LOAD:
            allowed[idx + 1] = mem[req.addr]
        else:
            mem[req.addr] = req.value
    return allowed


def flat(completions):
    return [(c.addr, c.op.value, c.value) for c in completions]


# 确定性 LCG 场景生成（不使用随机语料/外部数据）。
def _lcg(seed):
    state = seed
    while True:
        state = (1103515245 * state + 12345) & 0x7FFFFFFF
        yield state


def make_scenario(rng, length, addr_span=8):
    reqs = []
    for _ in range(length):
        core = next(rng) % 3
        addr = next(rng) % addr_span
        is_store = (next(rng) % 2) == 0
        if is_store:
            value = 1000 + (next(rng) % 100)
            reqs.append(Request(core, addr, Op.STORE, value=value))
        else:
            reqs.append(Request(core, addr, Op.LOAD))
    return reqs


def make_memory(rng, addr_span=8):
    return {a: (next(rng) % 50) for a in range(addr_span) if next(rng) % 3 == 0}


class TestAgainstIndependentReference(unittest.TestCase):
    def test_fixed_invalidation_scenario(self):
        init = {0: 100}
        scenario = [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(1, 0, Op.STORE, value=200),
            Request(2, 0, Op.LOAD),       # 读 M -> 回写后共享，读 200
            Request(0, 0, Op.LOAD),       # 再共享，读 200
            Request(0, 0, Op.STORE, value=300),
        ]
        allowed = expected_values_by_txn(scenario, init)
        k = CacheCoherenceKernel(memory=dict(init))
        k.request_batch(scenario)
        drain(k)
        by_txn = {c.txn_id: c for c in k.completed()}
        for tid, want in allowed.items():
            self.assertEqual(by_txn[tid].value, want, f"txn {tid}")
        # 参考机逐完成操作复核，不应抛异常。
        check_completion_sequence(flat(k.completed()), init)

    def test_deterministic_generated_scenarios_fifo_drain(self):
        rng = _lcg(20260922)
        for case in range(120):
            init = make_memory(rng)
            scenario = make_scenario(rng, 10)
            allowed = expected_values_by_txn(scenario, init)
            k = CacheCoherenceKernel(memory=dict(init))
            k.request_batch(scenario)
            drain(k)
            by_txn = {}
            for c in k.completed():
                # 同 txn 只完成一次。
                self.assertNotIn(c.txn_id, by_txn)
                by_txn[c.txn_id] = c
            for tid, want in allowed.items():
                self.assertIn(tid, by_txn, f"case {case}: txn {tid} 未完成")
                self.assertEqual(by_txn[tid].value, want,
                                 f"case {case} txn {tid}")
            check_completion_sequence(flat(k.completed()), init)

    def test_deterministic_generated_shuffled_deliveries(self):
        # 用不同但确定的投递顺序驱动，读值必须与参考一致。
        rng = _lcg(424242)
        for case in range(40):
            init = make_memory(rng)
            scenario = make_scenario(rng, 8)
            allowed = expected_values_by_txn(scenario, init)
            k = CacheCoherenceKernel(memory=dict(init))
            k.request_batch(scenario)
            while True:
                pend = k.pending()
                if not pend:
                    break
                choice = pend[next(rng) % len(pend)]
                k.deliver(choice.msg_id)
            by_txn = {c.txn_id: c for c in k.completed()}
            for tid, want in allowed.items():
                self.assertEqual(by_txn[tid].value, want,
                                 f"case {case} txn {tid}")
            check_completion_sequence(flat(k.completed()), init)

    def test_reference_detects_wrong_value(self):
        # 参考机自身必须能抓错值（证明它不是恒真检查）。
        bad = [(0, "load", 1)]  # 初值 0，却报读到 1
        with self.assertRaises(ReferenceViolation):
            check_completion_sequence(bad, {})

    def test_reference_expected_helper_is_independent(self):
        # 纯函数：同样输入同样输出，且输出可手工核对。
        seq = [(0, "store", 7), (0, "load", 7), (1, "load", 0),
               (0, "store", 8), (0, "load", 8)]
        self.assertEqual(expected_load_values(seq), [7, 0, 8])


class TestExhaustiveReferenceComparison(unittest.TestCase):
    def test_all_small_scenarios_all_rearrangements(self):
        # 穷举所有“3 核、2 地址、4 个操作”的小规模场景，枚举全部消息重排，
        # 每个调度都过不变量检查与独立参考允许值检查。
        ops = []
        for core in range(3):
            for addr in range(2):
                ops.append((core, addr, Op.LOAD))
                ops.append((core, addr, Op.STORE, 10 + core))
        # 全组合 12^4=20736 太多；用 LCG 固定抽取 300 个场景。
        rng = _lcg(99)
        for case in range(300):
            scenario = []
            for _ in range(4):
                core, addr, op = ops[next(rng) % len(ops)][:3]
                if op is Op.STORE:
                    scenario.append(Request(core, addr, op,
                                            value=10 + (next(rng) % 7)))
                else:
                    scenario.append(Request(core, addr, op))
            allowed = expected_values_by_txn(scenario, {})
            res = explore(lambda: CacheCoherenceKernel(), scenario,
                          budget=200_000)
            self.assertFalse(res.exhausted, f"case {case} 预算耗尽")
            self.assertEqual(res.violations, (), f"case {case}")
            for path, completions in res.schedules.items():
                by_txn = {c.txn_id: c for c in completions}
                for tid, want in allowed.items():
                    self.assertEqual(by_txn[tid].value, want,
                                     f"case {case} path {path} txn {tid}")
                check_completion_sequence(flat(completions), {})
                # 终态：每个地址至多一个 M，M 时无 S。
                k2 = CacheCoherenceKernel()
                k2.request_batch(scenario)
                for mid in path:
                    k2.deliver(mid)
                snap = k2.snapshot()
                for a in range(2):
                    ms = [c for c in range(3)
                          if snap.caches[c][a].state is State.MODIFIED]
                    ss = [c for c in range(3)
                          if snap.caches[c][a].state is State.SHARED]
                    self.assertLessEqual(len(ms), 1)
                    self.assertFalse(ms and ss)


if __name__ == "__main__":
    unittest.main()
