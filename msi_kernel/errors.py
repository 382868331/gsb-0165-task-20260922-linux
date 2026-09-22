"""错误类型与输入校验。

公开错误语义：
- 所有非法输入抛出 ``KernelError``（``ValueError`` 子类），错误信息以参数名开头，
  便于定位；不做静默纠正。
- 整数参数拒绝 ``bool``（``True``/``False`` 不是合法整数）。
- 数值输入若为浮点 ``NaN``/``Infinity`` 一律拒绝。
- 重排搜索预算耗尽抛出 ``BudgetExhausted``（``RuntimeError`` 子类），
  与“枚举完成且性质成立/发现反例”严格区分。
"""

from __future__ import annotations

import math
from typing import Any


class KernelError(ValueError):
    """内核接口收到非法输入时抛出。"""


class BudgetExhausted(RuntimeError):
    """消息重排搜索在证明完成前耗尽访问预算。

    这既不表示性质成立，也不表示无解（未发现违例），只表示搜索未完成。
    """

    def __init__(self, visited: int, budget: int) -> None:
        super().__init__(
            f"reorder exploration budget exhausted: visited={visited}, budget={budget}"
        )
        self.visited = visited
        self.budget = budget


def _describe(v: Any) -> str:
    if isinstance(v, bool):
        return f"bool {v!r}"
    if isinstance(v, float):
        return f"float {v!r}"
    return f"{type(v).__name__} {v!r}"


def require_int(name: str, v: Any, *, lo: int | None = None, hi: int | None = None) -> int:
    """要求严格的 Python ``int``（拒绝 bool），可加闭区间约束。

    浮点先按有限性判定，使 ``NaN``/``Infinity`` 给出明确错误信息。
    """
    if isinstance(v, bool):
        raise KernelError(f"{name}: integer parameter must not be bool, got {v!r}")
    if isinstance(v, float):
        if not math.isfinite(v):
            raise KernelError(f"{name}: NaN/Infinity is not allowed, got {v!r}")
        raise KernelError(f"{name}: integer required, got float {v!r}")
    if not isinstance(v, int):
        raise KernelError(f"{name}: integer required, got {_describe(v)}")
    if lo is not None and v < lo:
        raise KernelError(f"{name}: must be >= {lo}, got {v}")
    if hi is not None and v > hi:
        raise KernelError(f"{name}: must be <= {hi}, got {v}")
    return v


def require_optional_int(name: str, v: Any) -> int | None:
    """可选整数：``None`` 或严格 ``int``（拒绝 bool、NaN/Inf 浮点）。"""
    if v is None:
        return None
    return require_int(name, v)
