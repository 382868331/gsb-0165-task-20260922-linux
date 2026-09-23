"""错误类型。

所有内核错误继承 KernelError，非法输入异常带可定位字段
（core / addr / msg_id / 触发它的请求序号 request_seq 等），
绝不静默纠正输入。
"""

from __future__ import annotations


class KernelError(Exception):
    """msi_kernel 所有错误的基类。"""


class InvalidRequestError(KernelError):
    """load/store 请求非法（核号、地址、事务 id 等）。

    属性:
        request_seq: 该非法请求在批次中的序号（单请求为 0）。
        field:       出错字段名，如 "core" / "addr" / "value" / "txn_id"。
    """

    def __init__(self, message: str, *, request_seq: int | None = None,
                 field: str | None = None) -> None:
        self.request_seq = request_seq
        self.field = field
        loc = []
        if request_seq is not None:
            loc.append(f"request_seq={request_seq}")
        if field is not None:
            loc.append(f"field={field!r}")
        suffix = f" [{', '.join(loc)}]" if loc else ""
        super().__init__(message + suffix)


class InvalidMessageError(KernelError):
    """deliver 的消息非法：类型未知、字段不合法或引用不存在的事务。

    陈旧 ACK（属于已结束事务）不是非法消息：它被内核静默忽略但计入
    ignored，见 DeliveryStatus。只有引用从未注册过的 txn_id 才报此错。

    属性:
        msg_id: 出错消息在内核待发队列中的 id（若适用）。
        field:  出错字段名。
    """

    def __init__(self, message: str, *, msg_id: int | None = None,
                 field: str | None = None) -> None:
        self.msg_id = msg_id
        self.field = field
        loc = []
        if msg_id is not None:
            loc.append(f"msg_id={msg_id}")
        if field is not None:
            loc.append(f"field={field!r}")
        suffix = f" [{', '.join(loc)}]" if loc else ""
        super().__init__(message + suffix)


class CapacityExceededError(KernelError):
    """请求接纳会超过容量上限（等待队列 / 每核未完成请求数）。

    与输入合法性错误区分：请求本身合法，但当前没有容量接纳它。
    报错在任何状态变更之前抛出，单请求不会部分准入；批量接纳时整批
    零变更（request_seq 定位批中位置）。

    属性:
        request_seq: 触发上限的请求在批次中的序号（单请求为 0）。
        field:       触及的上限名："queue" / "core_outstanding"。
        cap:         该上限的容量。
        used:        抛出时已占用的容量。
    """

    def __init__(self, message: str, *, field: str, cap: int, used: int,
                 request_seq: int | None = None) -> None:
        self.field = field
        self.cap = cap
        self.used = used
        self.request_seq = request_seq
        loc = [f"field={field!r}", f"used={used}", f"cap={cap}"]
        if request_seq is not None:
            loc.append(f"request_seq={request_seq}")
        super().__init__(message + f" [{', '.join(loc)}]")


class BudgetExhaustedError(KernelError):
    """有界搜索的交付步数预算耗尽（没证明任何东西，只是没搜完）。

    与 InfeasibleError 严格区分：预算耗尽不代表无解。
    """

    def __init__(self, message: str, *, budget: int, used: int) -> None:
        self.budget = budget
        self.used = used
        super().__init__(f"{message} (budget={budget}, used={used})")


class InfeasibleError(KernelError):
    """在完整搜索后证明目标不可达（与预算耗尽严格区分）。"""
