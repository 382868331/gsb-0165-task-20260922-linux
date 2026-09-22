"""小消息重排的穷举枚举（验收用，非生产路径）。

程序按核分成“每核有序操作链”；枚举三类交错的全部可能：

- 投递当前任意一条待发网络消息（模拟消息乱序到达）；
- 从任意核的链首发出下一个请求（跨核请求发出顺序本身也不固定，
  因此“两个同时升级”两种发出顺序都会被枚举到）；
- S/M 读命中、M 写命中在发出点立即完成（无消息）。

每个访问到的状态都调用 :meth:`CoherenceKernel.check_invariants`
（每地址至多一个 M、M 时无 S 等），并汇总每个 LOAD 操作在所有重排下的
完成读值集合。全程使用 :meth:`CoherenceKernel.clone` 做确定性分支，
没有线程、没有随机数、没有真实网络。

预算语义：
- 搜索完整结束返回 :class:`ExplorationResult`（``complete=True``），
  在该协议子集内“枚举完成、未发现违例”就是有界穷举证明；
- 访问状态数达到 ``budget`` 仍未结束时抛出 :class:`BudgetExhausted`，
  与“枚举完成且无违例”严格区分，不得当作性质成立。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .errors import BudgetExhausted
from .kernel import CoherenceKernel, _key

# 动作：('R', 操作 id) 发请求；('D', 消息键) 投递。
Action = tuple[str, Any]


@dataclass(frozen=True, slots=True)
class RequestSpec:
    kind: str       # 'LOAD'/'STORE'（按缓存状态决定 GETS/GETX/UPGR/命中）
    core: int
    addr: int
    value: int | None = None


@dataclass(slots=True)
class ExplorationResult:
    program: tuple[tuple[RequestSpec, ...], ...]  # 每核一条链
    visited: int
    terminals: int
    complete: bool = True
    violations: list[tuple[list[Action], str]] = field(default_factory=list)
    # LOAD 操作 id -> 在所有终态中观察到的完成读值集合
    load_values: dict[int, set[int]] = field(default_factory=lambda: defaultdict(set))
    # 每个操作 id 在多少个终态中完成（用于检查操作都能终止）
    completion_count: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    sample_traces: list[list[Action]] = field(default_factory=list)

    def assert_safe(self) -> None:
        if self.violations:
            trace, msg = self.violations[0]
            raise AssertionError(f"invariant violation: {msg}\ntrace={trace}")


def _plan_request(kernel: CoherenceKernel, spec: RequestSpec) -> str | None:
    """返回该操作在当前缓存状态下的动作；状态不允许发请求时返回 ``None``。"""
    from .kernel import I_STATE, M_STATE, S_STATE

    st = kernel._caches[spec.core][spec.addr][0]
    if spec.kind == "LOAD":
        return "LOCAL_LOAD" if st in (S_STATE, M_STATE) else "GETS"
    if spec.kind == "STORE":
        if st == M_STATE:
            return "LOCAL_STORE"
        if st == S_STATE:
            return "UPGR"
        return "GETX"
    raise ValueError(f"unsupported spec kind: {spec.kind!r}")


def _op_id(core: int, idx: int) -> int:
    return core * 10_000 + idx


def _act(
    kernel: CoherenceKernel, action: Action,
    chains: tuple[tuple[RequestSpec, ...], ...],
    heads: tuple[int, ...],
) -> None:
    tag, payload = action
    if tag == "R":
        core, idx = payload
        spec = chains[core][idx]
        oid = _op_id(core, idx)
        plan = _plan_request(kernel, spec)
        if plan == "LOCAL_LOAD":
            kernel.local_load(spec.core, spec.addr, xid=oid)
        elif plan == "LOCAL_STORE":
            kernel.local_store(spec.core, spec.addr, spec.value, xid=oid)
        else:
            kernel.request(plan, oid, spec.core, spec.addr, spec.value)
    else:
        target = None
        for m in kernel.pending_messages():
            if _key(m) == payload:
                target = m
                break
        if target is None:
            raise AssertionError(f"replay failed: pending message {payload} vanished")
        kernel.deliver(target)


def _group_by_core(
    specs: list[RequestSpec] | tuple[RequestSpec, ...],
) -> tuple[tuple[RequestSpec, ...], ...]:
    chains: list[list[RequestSpec]] = [[], [], []]
    for s in specs:
        chains[s.core].append(s)
    return tuple(tuple(c) for c in chains)


def replay(
    specs: list[RequestSpec] | tuple[RequestSpec, ...],
    trace: list[Action],
    memory: list[int] | None = None,
) -> CoherenceKernel:
    """按给定动作轨迹重放，返回终态内核（用于复现反例/demo）。"""
    chains = _group_by_core(specs)
    k = CoherenceKernel(memory)
    heads = [0, 0, 0]
    for a in trace:
        _act(k, a, chains, tuple(heads))
        if a[0] == "R":
            heads[a[1][0]] += 1
    return k


def explore(
    specs: list[RequestSpec] | tuple[RequestSpec, ...],
    *,
    memory: list[int] | None = None,
    budget: int = 100_000,
    keep_traces: int = 4,
) -> ExplorationResult:
    """对“每核有序、跨核无序”的请求程序穷举所有请求发出/消息投递交错。

    参数:
        specs: 操作序列；同核操作按出现顺序保持程序序，跨核顺序任意。
        memory: 初始内存（8 个整数），默认全 0。
        budget: 访问状态数上限；耗尽抛 :class:`BudgetExhausted`。
        keep_traces: 保留多少条到终态的样例轨迹。
    """
    chains = _group_by_core(specs)
    result = ExplorationResult(chains, 0, 0)
    seen: set[bytes] = set()
    total = sum(len(c) for c in chains)

    def visit(k: CoherenceKernel, heads: list[int], issued: int,
              trace: list[Action]) -> None:
        if result.visited >= budget:
            raise BudgetExhausted(result.visited, budget)
        fp = (k.canonical(), tuple(heads))
        if fp in seen:
            return
        seen.add(fp)
        result.visited += 1

        try:
            k.check_invariants()
        except AssertionError as e:
            result.violations.append((list(trace), str(e)))
            return

        actions: list[Action] = [("D", _key(m)) for m in k.pending_messages()]
        for core in range(3):
            idx = heads[core]
            if idx < len(chains[core]):
                trial = k.clone()
                action = ("R", (core, idx))
                try:
                    _act(trial, action, chains, tuple(heads))
                except Exception:
                    continue  # 当前状态该请求不合法（如同核有未完成事务）
                actions.append(action)

        if not actions:
            if issued < total:
                result.violations.append(
                    (list(trace), f"program stuck after issuing {issued}/{total} requests")
                )
                return
            if not k.is_quiescent():
                result.violations.append((list(trace), "deadlock: no actions but not quiescent"))
                return
            result.terminals += 1
            snap = k.snapshot()
            done_ids = set()
            for op in snap.completed:
                done_ids.add(op.xid)
                if op.kind == "LOAD":
                    result.load_values[op.xid].add(op.value)
            for oid in done_ids:
                result.completion_count[oid] += 1
            if len(result.sample_traces) < keep_traces:
                result.sample_traces.append(list(trace))
            return

        for action in actions:
            child = k.clone()
            new_heads = list(heads)
            _act(child, action, chains, tuple(heads))
            if action[0] == "R":
                new_heads[action[1][0]] += 1
            visit(child, new_heads, issued + (1 if action[0] == "R" else 0),
                  trace + [action])

    visit(CoherenceKernel(memory), [0, 0, 0], 0, [])
    result.complete = True
    return result
