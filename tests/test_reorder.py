"""消息重排穷举、不变量与独立参考允许值对照测试。"""

from __future__ import annotations

import unittest

from msi_kernel import BudgetExhausted
from msi_kernel.reorder import RequestSpec, explore, replay
from msi_kernel.messages import MsgType
from msi_kernel.kernel import CoherenceKernel

from oracle_ref import allowed_load_values


def oid(core: int, idx: int) -> int:
    return core * 10_000 + idx


def per_core_indices(specs):
    """返回 (每个 spec 的 oid, 每核已见下标计数)。"""
    seen = [0, 0, 0]
    ids = []
    for s in specs:
        ids.append(oid(s.core, seen[s.core]))
        seen[s.core] += 1
    return ids


class ReorderSafetyTests(unittest.TestCase):
    def test_two_simultaneous_upgrades(self):
        prog = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("STORE", 2, 0, 30),
        ]
        r = explore(prog)
        self.assertTrue(r.complete)
        self.assertGreater(r.terminals, 1, "must genuinely explore multiple orders")
        r.assert_safe()
        # 终态 owner 恰为其中一个写者；两个升级两种发出顺序都应被覆盖，
        # 最终 owner 集合应同时出现 c1 与 c2。
        owners = set()
        for tr in r.sample_traces:
            k = replay(prog, tr)
            d = k.snapshot().directory[0]
            self.assertEqual(d.state, "M")
            self.assertIn(d.owner, (1, 2))
            self.assertEqual(set(d.sharers), set())
            owners.add(d.owner)
        self.assertEqual(owners, {1, 2})

    def test_dup_acks_and_rw_interleaving(self):
        prog = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 2, 0, 30),
            RequestSpec("LOAD", 0, 0),
        ]
        r = explore(prog, budget=500_000)
        self.assertTrue(r.complete)
        r.assert_safe()
        self.assertGreater(r.terminals, 1)

    def test_independent_addresses_interleave_freely(self):
        prog = [
            RequestSpec("STORE", 0, 0, 1),
            RequestSpec("STORE", 1, 1, 2),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("LOAD", 0, 1),
        ]
        r = explore(prog)
        r.assert_safe()
        self.assertGreater(r.terminals, 1)

    def test_single_store_single_load_orders(self):
        # 跨核无程序序：load 可先于 store 发出（读到初值 0）或在其后（读到 7）。
        prog = [RequestSpec("STORE", 0, 0, 7), RequestSpec("LOAD", 1, 0)]
        r = explore(prog)
        r.assert_safe()
        self.assertEqual(dict(r.load_values), {oid(1, 0): {0, 7}})

    def test_terminal_m_invariant_all_paths(self):
        prog = [
            RequestSpec("STORE", 0, 0, 1),
            RequestSpec("STORE", 1, 0, 2),
            RequestSpec("STORE", 2, 0, 3),
        ]
        r = explore(prog)
        r.assert_safe()
        for tr in r.sample_traces:
            k = replay(prog, tr)
            s = k.snapshot()
            mcores = [c for c in range(3) if s.caches[c][0].state == "M"]
            self.assertEqual(len(mcores), 1)
            self.assertEqual(s.directory[0].state, "M")
            self.assertEqual(s.directory[0].owner, mcores[0])
            self.assertEqual(set(s.directory[0].sharers), set())


class OracleComparisonTests(unittest.TestCase):
    """逐完成操作的允许值：内核观察集合必须等于独立参考的允许集合。"""

    def _check(self, specs, ops, init=(0,) * 8):
        ids = per_core_indices(specs)
        r = explore(specs, memory=list(init), budget=500_000)
        r.assert_safe()
        expected_flat = allowed_load_values(ops, init=init[0])
        expected = {ids[i]: vs for i, vs in expected_flat.items()}
        observed = {x: set(v) for x, v in r.load_values.items()}
        self.assertEqual(set(observed), set(expected))
        for x in expected:
            self.assertEqual(
                observed[x], expected[x],
                f"op oid={x}: observed {sorted(observed[x])} "
                f"!= allowed {sorted(expected[x])}",
            )
        # 每个操作在每个终态恰好完成一次
        n_term = r.terminals
        for x in ids:
            self.assertEqual(r.completion_count[x], n_term,
                             f"op {x} not completed in every terminal")

    def test_write_read_chain(self):
        specs = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("LOAD", 2, 0),
        ]
        ops = [(0, 0, "W", 10), (1, 0, "R", None),
               (1, 0, "W", 20), (2, 0, "R", None)]
        self._check(specs, ops)

    def test_concurrent_writes_read_allowed_values(self):
        # c0 先建共享；c1/c2 两个同时升级写 20/30；c0 读：
        # 允许读到 20 或 30（发出与消息重排共同决定线性化序）。
        specs = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("STORE", 2, 0, 30),
            RequestSpec("LOAD", 0, 0),
        ]
        ops = [(0, 0, "W", 10), (1, 0, "R", None), (2, 0, "R", None),
               (1, 0, "W", 20), (2, 0, "W", 30), (0, 0, "R", None)]
        self._check(specs, ops)

    def test_rw_interleave_allowed_values(self):
        specs = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("LOAD", 0, 0),
        ]
        ops = [(0, 0, "W", 10), (1, 0, "R", None), (2, 0, "R", None),
               (1, 0, "W", 20), (2, 0, "R", None), (0, 0, "R", None)]
        self._check(specs, ops)


class BudgetTests(unittest.TestCase):
    def test_budget_exhausted_distinct_from_proof(self):
        prog = [
            RequestSpec("STORE", 0, 0, 10),
            RequestSpec("LOAD", 1, 0),
            RequestSpec("LOAD", 2, 0),
            RequestSpec("STORE", 1, 0, 20),
            RequestSpec("STORE", 2, 0, 30),
            RequestSpec("LOAD", 0, 0),
        ]
        with self.assertRaises(BudgetExhausted) as cm:
            explore(prog, budget=3)
        self.assertEqual(cm.exception.budget, 3)
        self.assertGreaterEqual(cm.exception.visited, 1)
        # 预算充足时同一程序完整枚举且无违例——两种结论严格分开
        r = explore(prog, budget=500_000)
        r.assert_safe()
        self.assertTrue(r.complete)


class ExplicitDuplicateAckScenario(unittest.TestCase):
    def test_duplicate_inv_and_ack(self):
        k = CoherenceKernel()
        k.request("GETX", 1, 0, 0, 1)
        while k.pending_messages():
            k.deliver(k.pending_messages()[0])
        k.request("GETS", 2, 1, 0)
        while k.pending_messages():
            k.deliver(k.pending_messages()[0])
        k.request("UPGR", 3, 0, 0, 8)
        inv = [m for m in k.pending_messages() if m.mtype is MsgType.INV][0]
        k.deliver(inv)
        self.assertEqual(k.deliver(inv), [])  # 重复 INV 幂等
        ack = [m for m in k.pending_messages() if m.mtype is MsgType.INV_ACK][0]
        k.deliver(ack)
        self.assertEqual(k.deliver(ack), [])  # 重复 ACK 幂等
        while k.pending_messages():
            k.deliver(k.pending_messages()[0])
        k.check_invariants()
        self.assertEqual(k.snapshot().directory[0].owner, 0)
        self.assertEqual(k.snapshot().caches[0][0].value, 8)


if __name__ == "__main__":
    unittest.main()
