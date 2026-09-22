"""测试专用的独立参考模型（不导入、不调用被测内核核心）。

思路：一致性协议允许的读值集合，可以用一个与消息实现完全无关的抽象模型
刻画——每个地址有一个全局当前值，所有 load/store 在“保持每核程序序”的
前提下可以任意线性化；某条 load 的允许读值，就是它在所有合法线性化中
可能读到的“最近一次写值”。这里枚举 3 条每核操作链的全部拓扑序，
用最朴素的全局内存重放得到允许集合。

它与 :mod:`msi_kernel` 的消息状态机没有任何共享代码，只输出期望值集合。
"""

from __future__ import annotations

from itertools import permutations
from typing import Sequence


def allowed_load_values(
    ops: Sequence[tuple[int, int, str, int | None]],
    init: int = 0,
) -> dict[int, set[int]]:
    """枚举保持每核程序序的全部线性化，返回每条读操作下标 -> 允许读值集合。

    ``ops[i] = (core, addr, kind, value)``，kind 为 ``'R'`` 或 ``'W'``。
    线性化中读操作读到其位置之前该地址最近一次写的值（无写则为 ``init``）。
    """
    n = len(ops)
    per_core: list[list[int]] = [[], [], []]
    for i, (core, *_rest) in enumerate(ops):
        per_core[core].append(i)

    # 所有“每核链的交错”：给每个位置选一个核，再按链中顺序取操作。
    core_counts = [len(c) for c in per_core]
    patterns: set[tuple[int, ...]] = set()

    def gen(rem: list[int], acc: tuple[int, ...]) -> None:
        if len(acc) == n:
            patterns.add(acc)
            return
        for c in range(3):
            if rem[c]:
                rem[c] -= 1
                gen(rem, acc + (c,))
                rem[c] += 1

    gen(list(core_counts), ())

    result: dict[int, set[int]] = {i: set() for i, op in enumerate(ops) if op[2] == "R"}
    for pattern in patterns:
        cursors = [0, 0, 0]
        mem = [init] * 8
        for c in pattern:
            i = per_core[c][cursors[c]]
            cursors[c] += 1
            _, addr, kind, value = ops[i]
            if kind == "R":
                result[i].add(mem[addr])
            else:
                mem[addr] = value
    return result
