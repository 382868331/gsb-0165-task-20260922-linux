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
