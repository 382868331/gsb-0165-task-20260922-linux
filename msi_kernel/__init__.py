"""目录式 MSI 缓存一致性消息内核（3 核 / 8 地址，纯消息驱动，离线）。

公开接口：
- :class:`CoherenceKernel`：内核本体（request/deliver/snapshot/clone）。
- :class:`Message` / :class:`MsgType`：消息与消息类型。
- 视图：``CacheLineView``、``DirLineView``、``CompletedOp``、``KernelSnapshot``。
- 错误：``KernelError``、``BudgetExhausted``。
- 重排枚举：:mod:`msi_kernel.reorder`（见该模块）。
"""

from __future__ import annotations

from .errors import BudgetExhausted, KernelError
from .kernel import (
    CacheLineView,
    CoherenceKernel,
    CompletedOp,
    DirLineView,
    KernelSnapshot,
)
from .messages import DIRECTORY, NUM_ADDR, NUM_CORES, Message, MsgType, make_request

__all__ = [
    "CoherenceKernel",
    "Message",
    "MsgType",
    "make_request",
    "CacheLineView",
    "DirLineView",
    "CompletedOp",
    "KernelSnapshot",
    "KernelError",
    "BudgetExhausted",
    "NUM_CORES",
    "NUM_ADDR",
    "DIRECTORY",
]
