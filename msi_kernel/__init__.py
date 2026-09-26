"""msi_kernel —— 离线、消息驱动的 3 核 8 地址 MSI 目录一致性内核。

公开模块：
- errors: 错误类型
- model:  协议内核与全部公开数据结构
- explore: 消息重排枚举 / 有界搜索
"""

from .errors import (
    KernelError,
    InvalidRequestError,
    InvalidMessageError,
    BudgetExhaustedError,
    InfeasibleError,
)
from .model import (
    NUM_CORES,
    NUM_ADDRS,
    NUM_SECTORS,
    ALL_SECTORS,
    DIR,
    State,
    Op,
    MsgType,
    DeliveryStatus,
    Message,
    Request,
    Completion,
    CacheSectorView,
    DirectorySectorView,
    RequestReport,
    DeliveryReport,
    CacheCoherenceKernel,
)
from .explore import (
    explore,
    find_schedule,
    apply_path,
    drain,
    descriptor,
    check_invariants,
    ExplorationResult,
)

__all__ = [
    "NUM_CORES",
    "NUM_ADDRS",
    "NUM_SECTORS",
    "ALL_SECTORS",
    "DIR",
    "State",
    "Op",
    "MsgType",
    "DeliveryStatus",
    "Message",
    "Request",
    "Completion",
    "CacheSectorView",
    "DirectorySectorView",
    "RequestReport",
    "DeliveryReport",
    "CacheCoherenceKernel",
    "explore",
    "find_schedule",
    "apply_path",
    "drain",
    "descriptor",
    "check_invariants",
    "ExplorationResult",
    "KernelError",
    "InvalidRequestError",
    "InvalidMessageError",
    "BudgetExhaustedError",
    "InfeasibleError",
]
