"""独立参考模型：按线性化记录执行的标量状态机。

本模块刻意 **不导入 msi_kernel**，不调用被测核心的任何代码来得到期望值。
它只吃原始数据（完成事件序列），用一个平凡的"每地址一个标量"抽象机
按完成顺序（即目录授予顺序的线性化记录）逐步执行：

- 地址在完成序列上存在全序（目录对同地址请求 FIFO 准入、同地址协议串行）；
- 因此某个 load 完成时的允许值唯一确定：完成序列中该地址上、位于它之前
  的最后一个 store 的写入值；此前没有 store 则为初始内存值；
- cas 完成事件携带 (旧值, success, 新值)：参考机要求返回的旧值逐位等于
  当前标量值，success 必须等于"当前值 == 期望值"，成功时标量变为新值；
- 不同地址之间不做顺序约束。

往返一致（用核心自身快照反推）只能作为补充测试，期望值以本参考为准。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Sequence

# 完成事件：
#   (addr, "load", value)  /  (addr, "store", value)
#   (addr, "cas", old, success, expected, new_value)
FlatCompletion = tuple[int, str, int]
FlatCas = tuple[int, str, int, bool, int, int]


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
            if event[3]:  # success：标量改写为新值
                mem[addr] = event[5]
        else:
            raise ValueError(f"参考模型未知 op: {op!r}")
    return expected


def check_completion_sequence(
    completions: Sequence,
    initial_memory: dict[int, int] | None = None,
    num_addrs: int = 8,
) -> None:
    """逐完成事件对照标量参考机。

    load：实际读值必须等于参考当前值；store：参考值被改写；
    cas：返回旧值必须等于参考当前值，success 必须与"参考值 == 期望值"
    一致，成功时参考值改写为新值。违例抛 ReferenceViolation。
    """
    mem = [0] * num_addrs
    if initial_memory:
        for a, v in initial_memory.items():
            mem[a] = v
    load_index = 0
    cas_index = 0
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
            success, expected, new_value = event[3], event[4], event[5]
            want_old = mem[addr]
            if value != want_old:
                raise ReferenceViolation(
                    f"第 {cas_index} 个 cas（地址 {addr}）返回旧值 {value}，"
                    f"按线性化记录它只允许是 {want_old}")
            want_success = want_old == expected
            if success != want_success:
                raise ReferenceViolation(
                    f"第 {cas_index} 个 cas（地址 {addr}）success={success}，"
                    f"按线性化记录（旧值 {want_old} vs 期望 {expected}）"
                    f"应为 {want_success}")
            if success:
                mem[addr] = new_value
            cas_index += 1
        else:
            raise ValueError(f"参考模型未知 op: {op!r}")


def per_address_store_order(completions: Iterable[FlatCompletion]
                            ) -> dict[int, list[int]]:
    """辅助：每地址 store 的完成写入序列（供测试做附加断言）。"""
    order: dict[int, list[int]] = defaultdict(list)
    for addr, op, value in completions:
        if op == "store":
            order[addr].append(value)
    return dict(order)
