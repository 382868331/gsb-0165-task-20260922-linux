"""消息定义。

内核是纯消息驱动的：所有状态迁移都由一条 :class:`Message` 触发，
没有真实线程或网络——消息只在“待发消息队列”与 ``deliver`` 调用之间流动。

方向
----
- 请求类（核 -> 目录）：``GETS``、``GETX``、``UPGR``
- 目录 -> 核：``INV``（作废）、``FETCH``（回写并降为 S）、
  ``FETCHX``（回写并作废）、``DATA``（以 S 送数据）、
  ``DATA_M``（以 M 送数据）、``UPGR_ACK``（升级许可）
- 核 -> 目录：``FLUSH``（owner 回写数据）、``INV_ACK``（作废完成）、
  ``GRANT_ACK``（授权送达确认，同地址队列的跨事务屏障）

每个开放事务的消息都携带该事务唯一的 ``xid``。幂等键
``(type, src, dst, xid, value)`` 相同的消息重复投递为空操作；
``INV_ACK`` 还带 ``aid`` 使同一目录事务下每个失效的 ACK 唯一；
``GRANT_ACK`` 携带 ``xid``，因此陈旧 ACK（来自旧事务）无法推进新事务。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import KernelError, require_int, require_optional_int

NUM_CORES = 3
NUM_ADDR = 8
DIRECTORY = NUM_CORES  # 目录的节点号：0..NUM_CORES-1 为核


class MsgType(str, Enum):
    GETS = "GETS"
    GETX = "GETX"
    UPGR = "UPGR"
    INV = "INV"
    FETCH = "FETCH"
    FETCHX = "FETCHX"
    DATA = "DATA"
    DATA_M = "DATA_M"
    UPGR_ACK = "UPGR_ACK"
    FLUSH = "FLUSH"
    INV_ACK = "INV_ACK"
    GRANT_ACK = "GRANT_ACK"


# 各类消息携带的字段：
_REQUEST_TYPES = frozenset({MsgType.GETS, MsgType.GETX, MsgType.UPGR})
_DIR_TO_CORE = frozenset(
    {
        MsgType.INV,
        MsgType.FETCH,
        MsgType.FETCHX,
        MsgType.DATA,
        MsgType.DATA_M,
        MsgType.UPGR_ACK,
    }
)
_CORE_TO_DIR = frozenset(
    {MsgType.FLUSH, MsgType.INV_ACK, MsgType.GRANT_ACK}
)


@dataclass(frozen=True, slots=True)
class Message:
    """一条不可变消息。

    属性:
        mtype: 消息类型。
        xid: 所属事务 ID（请求 xid 在整个会话中唯一）。
        src/dst: 发送/接收节点；核为 0..2，目录为 3。
        addr: 缓存地址 0..7。
        value: ``DATA*``/``FLUSH`` 携带的整数数据，其余为 ``None``。
        aid: ``INV_ACK`` 对应的失效序号（目录每发一条 INV 递增），
            其余消息为 ``None``。
    """

    mtype: MsgType
    xid: int
    src: int
    dst: int
    addr: int
    value: int | None = None
    aid: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mtype, MsgType):
            raise KernelError(f"mtype: MsgType required, got {type(self.mtype).__name__}")
        require_int("xid", self.xid, lo=0)
        require_int("src", self.src, lo=0, hi=DIRECTORY)
        require_int("dst", self.dst, lo=0, hi=DIRECTORY)
        if self.src == self.dst:
            raise KernelError(f"src/dst: message must cross nodes, both are {self.src}")
        require_int("addr", self.addr, lo=0, hi=NUM_ADDR - 1)
        require_optional_int("value", self.value)
        require_optional_int("aid", self.aid)

        t = self.mtype
        if t in _REQUEST_TYPES:
            if self.src >= NUM_CORES or self.dst != DIRECTORY:
                raise KernelError(f"{t.value}: request must be core -> directory")
            if self.aid is not None:
                raise KernelError(f"{t.value}: request must not carry aid")
            if t is MsgType.GETS:
                if self.value is not None:
                    raise KernelError("GETS: load request must not carry a value")
            else:
                # GETX/UPGR 是写事务，必须携带整数写入值；授权后写入缓存，
                # owner 被回写时 FLUSH 携带的就是该值。
                if self.value is None:
                    raise KernelError(f"{t.value}: write request requires an integer value")
        elif t in _DIR_TO_CORE:
            if self.src != DIRECTORY or self.dst >= NUM_CORES:
                raise KernelError(f"{t.value}: must be directory -> core")
            if t in (MsgType.DATA, MsgType.DATA_M):
                if self.value is None:
                    raise KernelError(f"{t.value}: data message requires an integer value")
            elif self.value is not None:
                raise KernelError(f"{t.value}: must not carry a value")
            if t is MsgType.INV:
                if self.aid is None:
                    raise KernelError("INV: requires an aid (invalidation id)")
            elif self.aid is not None:
                raise KernelError(f"{t.value}: must not carry an aid")
        else:  # 核 -> 目录
            if self.src >= NUM_CORES or self.dst != DIRECTORY:
                raise KernelError(f"{t.value}: must be core -> directory")
            if t is MsgType.FLUSH:
                if self.value is None:
                    raise KernelError("FLUSH: requires an integer value")
                if self.aid is not None:
                    raise KernelError("FLUSH: must not carry aid")
            elif t is MsgType.INV_ACK:
                if self.aid is None:
                    raise KernelError("INV_ACK: requires an aid matching the INV")
                if self.value is not None:
                    raise KernelError("INV_ACK: must not carry a value")
            else:  # GRANT_ACK
                if self.value is not None or self.aid is not None:
                    raise KernelError("GRANT_ACK: carries neither value nor aid")

    def __str__(self) -> str:
        s = f"{self.mtype.value}(xid={self.xid},c{self.src}->{'D' if self.dst == DIRECTORY else 'c' + str(self.dst)},a{self.addr}"
        if self.value is not None:
            s += f",v={self.value}"
        if self.aid is not None:
            s += f",aid={self.aid}"
        return s + ")"


def make_request(
    kind: str, xid: int, core: int, addr: int, value: int | None = None
) -> Message:
    """构造核 -> 目录请求。

    ``kind`` 为 ``'GETS'|'GETX'|'UPGR'``；GETX/UPGR 必须带整数 ``value``
    （写入值），GETS 必须不带。
    """
    try:
        mt = MsgType(kind)
    except ValueError:
        raise KernelError(
            f"kind: expected GETS/GETX/UPGR, got {kind!r}"
        ) from None
    if mt not in _REQUEST_TYPES:
        raise KernelError(f"kind: {kind} is not a request type")
    return Message(mt, xid, core, DIRECTORY, addr, value=value)
