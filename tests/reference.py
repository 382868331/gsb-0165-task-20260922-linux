"""独立参考模型：逐完成操作的允许读值判定。

本模块刻意 **不导入 msi_kernel**，不调用被测核心的任何代码来得到期望值。
它只吃原始数据（load/store 三元组或 CAS 六元组），用一个平凡的每地址
标量抽象机判定每个完成事件允许返回什么：

- 地址在完成序列上存在全序（目录对同地址请求 FIFO 准入、同地址协议串行）；
- 因此某个 load 完成时的允许值唯一确定：完成序列中该地址上、位于它之前
  的最后一个 store 的写入值；此前没有 store 则为初始内存值；
- 不同地址之间不做顺序约束。

往返一致（用核心自身快照反推）只能作为补充测试，期望值以本参考为准。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Sequence

# load/store: (addr, op, value)；
# CAS: (addr, "cas", old_value, success, expected_value, new_value)。
FlatCompletion = tuple[int, str, int] | tuple[
    int, str, int, bool, int, int]


class ReferenceViolation(AssertionError):
    pass


def expected_load_values(
    completions: Iterable[FlatCompletion],
    initial_memory: dict[int, int] | None = None,
    num_addrs: int = 8,
) -> list[int]:
    """按完成顺序给出每个 load 事件 **应当** 读到的值（与事件等序、仅 load）。

    纯函数：不依赖被测实现。
    """
    mem = [0] * num_addrs
    if initial_memory:
        for a, v in initial_memory.items():
            mem[a] = v
    expected: list[int] = []
    for event in completions:
        addr, op, value = event[0], event[1], event[2]
        if op == "store":
            mem[addr] = value
        elif op == "load":
            expected.append(mem[addr])
        elif op == "cas":
            _, _, old_value, success, expected_value, new_value = event
            if old_value != mem[addr]:
                raise ReferenceViolation(
                    f"CAS 地址 {addr} 返回旧值 {old_value}，标量状态为 "
                    f"{mem[addr]}")
            if success != (old_value == expected_value):
                raise ReferenceViolation(
                    f"CAS 地址 {addr} success={success}，但旧值 {old_value} "
                    f"与期望值 {expected_value} 的比较结果应为 "
                    f"{old_value == expected_value}")
            if success:
                mem[addr] = new_value
        else:
            raise ValueError(f"参考模型未知 op: {op!r}")
    return expected


def check_completion_sequence(
    completions: Sequence[FlatCompletion],
    initial_memory: dict[int, int] | None = None,
    num_addrs: int = 8,
) -> list[int]:
    """断言整条完成序列中每个 load 的实际读值等于参考允许值。

    违例抛 ReferenceViolation，指出第几个 load、实际/期望。
    """
    mem = [0] * num_addrs
    if initial_memory:
        for a, v in initial_memory.items():
            mem[a] = v
    load_index = 0
    for event in completions:
        addr, op, value = event[0], event[1], event[2]
        if op == "store":
            mem[addr] = value
        elif op == "load":
            want = mem[addr]
            if value != want:
                raise ReferenceViolation(
                    f"第 {load_index} 个 load（地址 {addr}）读到 {value}，"
                    f"按完成顺序它只允许读到 {want}")
            load_index += 1
        elif op == "cas":
            _, _, old_value, success, expected_value, new_value = event
            want_success = old_value == expected_value
            if old_value != mem[addr] or success != want_success:
                raise ReferenceViolation(
                    f"CAS（地址 {addr}）返回 old={old_value}, success="
                    f"{success}；标量状态为 {mem[addr]}，期望值="
                    f"{expected_value}")
            if success:
                mem[addr] = new_value
        else:
            raise ValueError(f"参考模型未知 op: {op!r}")
    return mem


def final_memory_after_completions(
    completions: Sequence[FlatCompletion],
    initial_memory: dict[int, int] | None = None,
    num_addrs: int = 8,
) -> list[int]:
    """按完成顺序执行 load/store/CAS，返回独立标量状态机的最终每地址值。"""
    check_completion_sequence(completions, initial_memory, num_addrs)
    mem = [0] * num_addrs
    if initial_memory:
        for a, v in initial_memory.items():
            mem[a] = v
    for event in completions:
        addr, op, value = event[0], event[1], event[2]
        if op == "store" or (op == "cas" and event[3]):
            mem[addr] = value if op == "store" else event[5]
    return mem


def per_address_store_order(completions: Iterable[FlatCompletion]
                            ) -> dict[int, list[int]]:
    """辅助：每地址 store 的完成写入序列（供测试做附加断言）。"""
    order: dict[int, list[int]] = defaultdict(list)
    for event in completions:
        addr, op, value = event[0], event[1], event[2]
        if op == "store":
            order[addr].append(value)
        elif op == "cas" and event[3]:
            order[addr].append(event[5])
    return dict(order)
