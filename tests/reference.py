"""独立参考模型：逐完成操作的允许读值判定。

本模块刻意 **不导入 msi_kernel**，不调用被测核心的任何代码来得到期望值。
它只吃原始数据（完成事件的 (addr, op, value) 三元组序列），用一个平凡的
“每地址最后写入”抽象机判定每个 load 完成时允许读到什么：

- 地址在完成序列上存在全序（目录对同地址请求 FIFO 准入、同地址协议串行）；
- 因此某个 load 完成时的允许值唯一确定：完成序列中该地址上、位于它之前
  的最后一个 store 的写入值；此前没有 store 则为初始内存值；
- 不同地址之间不做顺序约束。

往返一致（用核心自身快照反推）只能作为补充测试，期望值以本参考为准。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Sequence

# 完成事件：(addr, op, value)，op 用字符串 "load"/"store"。
FlatCompletion = tuple[int, str, int]


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
    for addr, op, value in completions:
        if op == "store":
            mem[addr] = value
        elif op == "load":
            expected.append(mem[addr])
        else:
            raise ValueError(f"参考模型未知 op: {op!r}")
    return expected


def check_completion_sequence(
    completions: Sequence[FlatCompletion],
    initial_memory: dict[int, int] | None = None,
    num_addrs: int = 8,
) -> None:
    """断言整条完成序列中每个 load 的实际读值等于参考允许值。

    违例抛 ReferenceViolation，指出第几个 load、实际/期望。
    """
    mem = [0] * num_addrs
    if initial_memory:
        for a, v in initial_memory.items():
            mem[a] = v
    load_index = 0
    for addr, op, value in completions:
        if op == "store":
            mem[addr] = value
        else:
            want = mem[addr]
            if value != want:
                raise ReferenceViolation(
                    f"第 {load_index} 个 load（地址 {addr}）读到 {value}，"
                    f"按完成顺序它只允许读到 {want}")
            load_index += 1


def per_address_store_order(completions: Iterable[FlatCompletion]
                            ) -> dict[int, list[int]]:
    """辅助：每地址 store 的完成写入序列（供测试做附加断言）。"""
    order: dict[int, list[int]] = defaultdict(list)
    for addr, op, value in completions:
        if op == "store":
            order[addr].append(value)
    return dict(order)
