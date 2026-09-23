"""消息重排枚举与有界搜索。

对外提供：
- explore(kernel_factory, scenario, ...): 穷举“当前待发消息”的所有投递重排，
  逐步检查缓存一致性结构不变量，并收集每个调度下的完成读值；
- find_schedule / apply_path / drain / descriptor: 调度搜索与辅助工具。

搜索是显式有界的：预算（已访问状态数）耗尽抛 BudgetExhaustedError；
在预算内穷尽所有可达调度仍找不到目标才抛 InfeasibleError —— 二者严格区分。

注入消息（陈旧/重复报文）属于“异常网络输入”，不参与 explore 的常规重排；
常规重排只投递内核实际产生的待发消息。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from .errors import BudgetExhaustedError, InfeasibleError
from .model import (
    NUM_ADDRS,
    NUM_CORES,
    CacheCoherenceKernel,
    Completion,
    Op,
    Request,
    State,
)

# scenario: 用同一个工厂构造的全新内核上，先注入的请求序列。
Scenario = Sequence[Request]
KernelFactory = Callable[[], CacheCoherenceKernel]


@dataclass(frozen=True)
class Violation:
    path: tuple[int, ...]
    detail: str


@dataclass(frozen=True)
class ExplorationResult:
    """枚举结果。

    schedules:        完整调度（消息 id 投递序列）→ 完成事件序列。
    terminal_states:  去重后的终止状态描述符数量。
    violations:       不变量违例（正常协议下应为空）。
    read_values:      每个 LOAD 事务 id → 它在所有调度下出现过的读值集合。
    steps:            实际访问的状态数。
    budget:           本次预算。
    exhausted:        是否在触达全部终态前用完预算。
    """

    schedules: dict[tuple[int, ...], tuple[Completion, ...]]
    terminal_states: int
    violations: tuple[Violation, ...]
    read_values: dict[int, frozenset[int]]
    steps: int
    budget: int
    exhausted: bool


def descriptor(kernel: CacheCoherenceKernel) -> tuple:
    """内核状态的结构指纹（只含结构与取值，不含消息 id），用于状态去重。"""
    snap = kernel.snapshot()
    caches = tuple(
        tuple((line.state.value, line.value) for line in cache.values())
        for cache in snap.caches
    )
    directory = tuple(
        (d.state.value, d.sharers, d.owner, d.active_txn,
         d.awaiting_flush, d.awaiting_acks)
        for d in snap.directory
    )
    # 待发消息只按“类型/源/目的/地址/事务”的多重集计，与具体 id 无关。
    pend = tuple(sorted(
        (m.mtype.value, m.src, m.dst, m.addr, m.txn_id, m.value)
        for m in snap.pending_messages
    ))
    # blocked（因网络通道满而保留）同样进入指纹：一条消息在 pending 还是
    # blocked 是不同的可达状态，不能被结构去重错误合并。
    blocked = tuple(sorted(
        (m.mtype.value, m.src, m.dst, m.addr, m.txn_id, m.value)
        for m in snap.blocked_messages
    ))
    queues = tuple(
        tuple((q.txn_id, q.core, q.op.value, q.value) for q in qq)
        for qq in snap.queues
    )
    completed = tuple(
        (c.txn_id, c.op.value, c.value) for c in snap.completed
    )
    return caches, directory, snap.memory, pend, blocked, queues, completed


def check_invariants(kernel: CacheCoherenceKernel) -> Optional[str]:
    """结构一致性不变量检查；违例返回说明，正常返回 None。

    口径（对任意外部投递之间的可观测状态成立；含报文在途过渡态）：
    1. 三个核缓存中 M 至多一个；
    2. 任一缓存为 M 时，其它缓存不得为 S；
    3. 目录 M：owner 存在，除 owner 外其它缓存必须 I
       （FETCH 已送达而 FLUSH 未到的过渡态里 owner 可为 S）；
    4. 目录 S：无 M 缓存，owner 为空，每个 S 缓存核都必须登记在共享者
       集合中（允许反向：INV 已在核端生效、ACK 在途时共享者暂时多出核）；
    5. 目录 I：所有缓存 I，owner 为空。
    """
    snap = kernel.snapshot()
    for a in range(NUM_ADDRS):
        states = {c: snap.caches[c][a].state for c in range(NUM_CORES)}
        ms = [c for c, s in states.items() if s is State.MODIFIED]
        ss = [c for c, s in states.items() if s is State.SHARED]
        if len(ms) > 1:
            return f"addr={a}: 同时存在 {len(ms)} 个 M 缓存: {ms}"
        if ms and ss:
            return f"addr={a}: M 缓存 {ms} 与 S 缓存 {ss} 共存"
        d = snap.directory[a]
        if d.state is State.MODIFIED:
            if d.owner is None:
                return f"addr={a}: 目录 M 但 owner 为空"
            for c, s in states.items():
                if c != d.owner and s is not State.INVALID:
                    return (f"addr={a}: 目录 M，非 owner 核 {c} 状态为 "
                            f"{s.value}（应为 I）")
            if states[d.owner] not in (State.MODIFIED, State.SHARED):
                return (f"addr={a}: 目录 M，owner 核 {d.owner} 状态为 "
                        f"{states[d.owner].value}（应为 M 或回写在途 S）")
        elif d.state is State.SHARED:
            if d.owner is not None:
                return f"addr={a}: 目录 S 但 owner={d.owner} 非空"
            if ms:
                return f"addr={a}: 目录 S 却存在 M 缓存 {ms}"
            extra = [c for c in ss if c not in d.sharers]
            if extra:
                return (f"addr={a}: 核 {extra} 持有 S 但不在目录共享者集合 "
                        f"{list(d.sharers)} 中")
        else:
            if d.owner is not None:
                return f"addr={a}: 目录 I 但 owner={d.owner} 非空"
            if any(s is not State.INVALID for s in states.values()):
                return f"addr={a}: 目录 I 却存在非 I 缓存"
    # 容量不变量：
    # 6. 待发报文数不得超过网络通道上限；
    # 7. blocked 非空时，待发队列必须恰好处于满载（否则 blocked 早该自动补发），
    #    二者总量不得超过网络上限 + blocked 自身（blocked 是“保留但未入队”）；
    # 8. 每地址等待队列长度不得超过目录队列上限；每核在途请求数不得超过其上限。
    cap = snap.capacity
    if cap is not None:
        n_pending = len(snap.pending_messages)
        n_blocked = len(snap.blocked_messages)
        if cap.config.network_cap is not None:
            if n_pending > cap.config.network_cap:
                return (f"待发报文 {n_pending} 超过网络通道上限 "
                        f"{cap.config.network_cap}")
            if n_blocked and n_pending != cap.config.network_cap:
                return (f"blocked 非空（{n_blocked}）但待发队列未满载"
                        f"（{n_pending}/{cap.config.network_cap}），"
                        f"blocked 消息应当已被自动补发")
        for a, qlen in enumerate(cap.dir_queues):
            c = cap.config.dir_queue_cap
            if c is not None and qlen > c:
                return f"addr={a}: 等待队列 {qlen} 超过上限 {c}"
        for core, n in enumerate(cap.core_inflight):
            c = cap.config.core_request_cap
            if c is not None and n > c:
                return f"核 {core}: 在途请求 {n} 超过上限 {c}"
    return None


def _build(factory: KernelFactory, scenario: Scenario) -> CacheCoherenceKernel:
    kernel = factory()
    if scenario:
        kernel.request_batch(list(scenario))
    return kernel


def _clone(kernel: CacheCoherenceKernel) -> CacheCoherenceKernel:
    return copy.deepcopy(kernel)


def drain(kernel: CacheCoherenceKernel, *, budget: int = 100_000) -> tuple[Completion, ...]:
    """按 msg_id 升序投递全部待发消息直到队列清空（确定性安全调度）。

    超过 budget 步抛 BudgetExhaustedError（死锁/活锁不应在本协议中发生，
    预算只是防御上限）。
    """
    done: list[Completion] = []
    steps = 0
    while True:
        pend = kernel.pending()
        if not pend:
            if kernel.blocked():
                # 待发队列空但仍有消息被背压：没有任何可投递报文能腾出槽位，
                # 协议无法继续（network_cap>=1 的正常配置不会出现；典型为
                # network_cap=0 且事务必须发消息）。这不是预算问题，明确报错。
                raise InfeasibleError(
                    f"drain 无法继续：待发队列为空但仍有 "
                    f"{len(kernel.blocked())} 条 blocked 消息（通道容量 "
                    f"{kernel.capacity_config.network_cap} 不足以容纳任何"
                    f"在途报文）")
            return tuple(kernel.completed())
        steps += 1
        if steps > budget:
            raise BudgetExhaustedError(
                "drain 投递步数超出预算", budget=budget, used=steps)
        rep = kernel.deliver(pend[0].msg_id)
        done.extend(rep.completions)


def apply_path(kernel: CacheCoherenceKernel, path: Sequence[int]) -> object:
    """在给定内核上按 msg_id 序列投递；返回最终 snapshot()。"""
    for mid in path:
        kernel.deliver(mid)
    return kernel.snapshot()


def explore(factory: KernelFactory, scenario: Scenario = (), *,
            budget: int = 100_000,
            all_paths: bool = False) -> ExplorationResult:
    """穷举 scenario 下所有合法消息重排。

    从每个可达状态出发，当前待发消息中的任一条都是下一步候选。
    默认按结构指纹去重（重排等价类只展开一次），每一步都检查不变量。

    all_paths=True 时不做剪枝，额外枚举出每一条完整的原始投递序列
    （消息 id 路径），适合消息总数很小的演示场景；路径深度有界
    （每条消息至多投递一次），不会循环，仍受 budget 保护。
    """
    kernel = _build(factory, scenario)
    initial = descriptor(kernel)
    visited: set[tuple] = {initial}
    schedules: dict[tuple[int, ...], tuple[Completion, ...]] = {}
    violations: list[Violation] = []
    read_values: dict[int, set[int]] = {}
    steps = 0
    exhausted = False

    # 每个 DFS 节点独立携带内核副本与已走路径，避免回溯时手动还原状态。
    terminal_fps: set[tuple] = set()

    def note_terminal(k: CacheCoherenceKernel, path: tuple[int, ...]) -> None:
        schedules[path] = k.completed()
        # 终态指纹取除 completed 外的全部结构分量（含 blocked）。
        terminal_fps.add(descriptor(k)[:-1])
        for comp in k.completed():
            if comp.op is Op.LOAD:
                read_values.setdefault(comp.txn_id, set()).add(comp.value)

    def recurse(k: CacheCoherenceKernel, path: tuple[int, ...]) -> None:
        nonlocal steps, exhausted
        steps += 1
        if steps > budget:
            exhausted = True
            return
        bad = check_invariants(k)
        if bad is not None:
            violations.append(Violation(path, bad))
        # 终态：待发队列与 blocked 暂存区都空（blocked 非空说明通道仍满、
        # 尚有消息未入队，事务不可能全部完成）。
        if not k.pending() and not k.blocked():
            note_terminal(k, path)
            return
        for m in k.pending():
            child = _clone(k)
            child.deliver(m.msg_id)
            new_path = path + (m.msg_id,)
            fp = descriptor(child)
            if all_paths:
                # 不按状态指纹剪枝：枚举每一条原始消息序列。
                recurse(child, new_path)
            elif fp not in visited:
                visited.add(fp)
                recurse(child, new_path)
            elif not child.pending() and not child.blocked():
                # 等价终态经由不同消息排列再次到达，仍记录其完成读值。
                note_terminal(child, new_path)

    recurse(kernel, ())
    return ExplorationResult(
        schedules=schedules,
        terminal_states=len(terminal_fps),
        violations=tuple(violations),
        read_values={t: frozenset(v) for t, v in read_values.items()},
        steps=steps,
        budget=budget,
        exhausted=exhausted,
    )


def find_schedule(factory: KernelFactory, scenario: Scenario = (), *,
                  goal: Optional[Callable[[CacheCoherenceKernel], bool]] = None,
                  want_complete: bool = True,
                  budget: int = 100_000) -> tuple[int, ...]:
    """搜索一条满足目标的消息投递序列（msg_id 元组）。

    goal:         终态判定，缺省为“所有请求事务均已完成”。
    want_complete: True 时还要求投递队列已排空。
    预算耗尽 → BudgetExhaustedError；穷尽无目标 → InfeasibleError。
    """
    root = _build(factory, scenario)
    if goal is None:
        goal = lambda k: True  # noqa: E731
    visited: set[tuple] = {descriptor(root)}
    stack: list[tuple[CacheCoherenceKernel, tuple[int, ...]]] = [(root, ())]
    used = 0
    while stack:
        k, path = stack.pop()
        used += 1
        if used > budget:
            raise BudgetExhaustedError(
                "find_schedule 状态预算耗尽", budget=budget, used=used)
        if goal(k) and (not want_complete
                        or (not k.pending() and not k.blocked())):
            return path
        for m in k.pending():
            child = _clone(k)
            child.deliver(m.msg_id)
            fp = descriptor(child)
            if fp not in visited:
                visited.add(fp)
                stack.append((child, path + (m.msg_id,)))
    raise InfeasibleError(
        "在全部可达调度中均不存在满足目标的终态（已穷尽，非预算问题）")
