"""消息驱动的 MSI 目录一致性协议内核（3 核 / 8 地址）。

本模块只做离散事件模拟，不含任何真实线程或网络：
- load/store 通过 CacheCoherenceKernel.request() / request_batch() 显式输入；
- 网络投递完全由调用方用 deliver(...) 显式输入，投递顺序即外部输入；
  * deliver(msg_id: int)  投递内核待发队列中的消息（正常驱动路径）；
  * deliver(Message(...)) 显式注入一条网络消息（msg_id 传 None），
    用于表达乱序 / 重复 / 陈旧报文等外部网络输入；
- 内核不自行丢消息、不自行乱序，pending 消息队列对调用方可见可枚举。

协议子集（固定规模 NUM_CORES=3, NUM_ADDRS=8）：
- 每核每地址缓存行为 I / S / M；
- 目录每地址记录状态、共享者集合、M 的 owner、内存值、活动事务与 FIFO 等待队列；
- 升级（S→M，或写他核 M）必须等齐全部失效 ACK 才提交；
- 读处于 M 的地址：目录先 FETCH 令 owner 回写（FLUSH），再以 S 共享；
- 同地址新请求 FIFO 排队，不同地址事务彼此独立、可并存交错。

本文件只实现题面契约的有限协议子集，不宣称兼容任何工业标准缓存一致性协议。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Optional, Union

from .errors import CapacityExceededError, InvalidMessageError, InvalidRequestError

NUM_CORES = 3
NUM_ADDRS = 8
# 目录在 src/dst 中的编号（核编号为 0..NUM_CORES-1）。
DIR = NUM_CORES


@dataclass(frozen=True)
class Limits:
    """三类资源容量上限（均为正整数）。

    queue_cap:    目录 **每个地址** 的 FIFO 等待队列长度上限（活动事务之外
                  还能排队多少条同地址请求）；
    core_cap:     **每个核** 已接纳但未完成的请求数上限（活动事务中的请求
                  与在目录队列中等待的请求都计入）；
    channel_cap:  **每条有向网络通道**（DIR→核 c 与 核 c→DIR 各算一条）上
                  在途（已发送、未投递）消息数上限。

    任一字段为非正整数、bool 或非 int 时构造即拒绝。
    """

    queue_cap: int = 8
    core_cap: int = 8
    channel_cap: int = 8

    def __post_init__(self) -> None:
        for name in ("queue_cap", "core_cap", "channel_cap"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise InvalidRequestError(
                    f"{name} 必须是正整数 int，收到 {type(value).__name__}: "
                    f"{value!r}", field=name)
            if value <= 0:
                raise InvalidRequestError(
                    f"{name} 必须是正整数，收到 {value}", field=name)


# 不显式给 limits 时的默认有限容量（足以容纳固定 3 核 8 地址规模下的正常
# 并发：同一通道同一时刻至多 2 条报文、每地址等待队列至多 5 条）。
DEFAULT_LIMITS = Limits()


class State(str, Enum):
    """缓存行 / 目录项状态。"""

    INVALID = "I"
    SHARED = "S"
    MODIFIED = "M"


class Op(str, Enum):
    """请求操作类型。"""

    LOAD = "load"
    STORE = "store"


class MsgType(str, Enum):
    """网络消息类型（目录↔核）。

    FETCH:   目录 → M 的 owner，要求回写最新数据并降级为 S；
    FLUSH:   owner → 目录，携带最新数据完成回写；
    INV:     目录 → 某个 S 共享者，要求失效该行；
    INV_ACK: 共享者 → 目录，失效完成确认。
    """

    FETCH = "FETCH"
    FLUSH = "FLUSH"
    INV = "INV"
    INV_ACK = "INV_ACK"


class DeliveryStatus(str, Enum):
    """单次 deliver 的结果状态。"""

    APPLIED = "applied"             # 正常生效
    IGNORED_DUPLICATE = "duplicate"  # 同一 msg_id 已投递过，幂等忽略
    IGNORED_STALE = "stale"          # 报文与当前活动事务不符（如陈旧 ACK），不推进新事务


# ---------------------------------------------------------------------------
# 公开数据结构（全部 frozen，外部持有的视图不会被内核后续变更污染）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Request:
    """一条 load/store 请求。

    LOAD 的 value 必须为 None；STORE 必须给 int 类型 value（bool 拒绝，
    float 拒绝，NaN/Infinity 给出明确报错）。
    txn_id 为 None 时内核自动分配；显式给出时必须为正整数且全生命周期唯一。
    """

    core: int
    addr: int
    op: Op
    value: Optional[int] = None
    txn_id: Optional[int] = None


@dataclass(frozen=True)
class Message:
    """一条网络消息。

    内核生成的消息 msg_id 为正整数；调用方显式注入网络输入时 msg_id 传 None。
    value 仅 FLUSH 使用（回写的数据，int）。
    """

    msg_id: Optional[int]
    txn_id: int
    mtype: MsgType
    src: int
    dst: int
    addr: int
    value: Optional[int] = None


@dataclass(frozen=True)
class Completion:
    """一个事务的完成事件。

    LOAD 的 value 为实际完成读值；STORE 的 value 为写入值。
    """

    txn_id: int
    core: int
    addr: int
    op: Op
    value: int


@dataclass(frozen=True)
class CacheView:
    state: State
    value: Optional[int]


@dataclass(frozen=True)
class DirectoryView:
    state: State
    sharers: tuple[int, ...]
    owner: Optional[int]
    active_txn: Optional[int]
    awaiting_flush: bool
    awaiting_acks: tuple[int, ...]


@dataclass(frozen=True)
class QueuedRequestView:
    txn_id: int
    core: int
    addr: int
    op: Op
    value: Optional[int]


@dataclass(frozen=True)
class RequestReport:
    txn_id: int
    queued: bool
    completions: tuple[Completion, ...]


@dataclass(frozen=True)
class DeliveryReport:
    """deliver 的结果报告。

    completions 为本次投递直接完成、或经 FIFO 链锁反应完成的全部事务。
    emitted 为本次投递新产生的待发（在途）消息。
    released 为本次投递腾出通道后，从发送端保留区自动续传进入在途通道
    的消息；背压解除前它们不占据通道、也不可被投递。
    """

    msg_id: Optional[int]
    txn_id: Optional[int]
    status: DeliveryStatus
    reason: Optional[str]
    emitted: tuple[Message, ...]
    completions: tuple[Completion, ...]
    released: tuple[Message, ...] = ()

    @property
    def applied(self) -> bool:
        return self.status is DeliveryStatus.APPLIED


@dataclass(frozen=True)
class ChannelView:
    """一条有向网络通道（src->dst）上的在途消息视图。"""

    src: int
    dst: int
    in_flight: tuple[Message, ...]


@dataclass(frozen=True)
class RetainedView:
    """发送端背压保留：通道满时消息先留在发送端，未进入在途通道。

    src/dst 为该消息最终要走的通道；in_flight 为按 msg_id 升序排列的
    保留消息（它们不属于任何通道的在途计数）。
    """

    src: int
    dst: int
    retained: tuple[Message, ...]


@dataclass(frozen=True)
class Snapshot:
    caches: tuple[dict[int, CacheView], ...]
    directory: tuple[DirectoryView, ...]
    memory: tuple[int, ...]
    pending_messages: tuple[Message, ...]
    queues: tuple[tuple[QueuedRequestView, ...], ...]
    completed: tuple[Completion, ...]
    channels: tuple[ChannelView, ...] = ()
    retained: tuple[RetainedView, ...] = ()
    limits: Optional[Limits] = None


# ---------------------------------------------------------------------------
# 内部可变结构
# ---------------------------------------------------------------------------


@dataclass
class _CacheLine:
    state: State = State.INVALID
    value: Optional[int] = None


@dataclass
class _Txn:
    """一个地址上的活动事务。"""

    txn_id: int
    core: int
    op: Op
    value: Optional[int]
    awaiting_flush: bool          # 正在等 owner 的 FLUSH
    awaiting_acks: set[int]       # 尚待 INV_ACK 的核集合
    flush_owner: Optional[int]    # FLUSH 阶段前的 owner


@dataclass
class _DirEntry:
    state: State = State.INVALID
    sharers: set[int] = field(default_factory=set)
    owner: Optional[int] = None
    txn: Optional[_Txn] = None
    queue: list[Request] = field(default_factory=list)


def _channel_key(src: int, dst: int) -> tuple[int, int]:
    return (src, dst)


# ---------------------------------------------------------------------------
# 输入校验（非法输入给可定位错误，绝不静默纠正）
# ---------------------------------------------------------------------------


def _check_int(field_name: str, x, *, request_seq: Optional[int] = None,
               msg: bool = False) -> None:
    import math

    # bool 是 int 的子类，按契约必须显式拒绝。
    if isinstance(x, bool):
        err = (InvalidMessageError if msg else InvalidRequestError)
        raise err(
            f"{field_name} 必须是整数 int，收到 bool: {x!r}", field=field_name)
    if isinstance(x, float):
        if math.isnan(x):
            reason = f"{field_name} 不能是 NaN"
        elif math.isinf(x):
            reason = f"{field_name} 不能是 Infinity"
        else:
            reason = f"{field_name} 必须是整数 int，收到 float: {x!r}"
        raise (InvalidMessageError if msg else InvalidRequestError)(
            reason, field=field_name)
    if not isinstance(x, int):
        raise (InvalidMessageError if msg else InvalidRequestError)(
            f"{field_name} 必须是整数 int，收到 {type(x).__name__}: {x!r}",
            field=field_name)


def _check_core(core, *, request_seq: Optional[int] = None) -> None:
    _check_int("core", core, request_seq=request_seq)
    if not 0 <= core < NUM_CORES:
        raise InvalidRequestError(
            f"core 必须在 [0,{NUM_CORES})，收到 {core}",
            request_seq=request_seq, field="core")


def _check_addr(addr, *, request_seq: Optional[int] = None) -> None:
    _check_int("addr", addr, request_seq=request_seq)
    if not 0 <= addr < NUM_ADDRS:
        raise InvalidRequestError(
            f"addr 必须在 [0,{NUM_ADDRS})，收到 {addr}",
            request_seq=request_seq, field="addr")


def _validate_request(req: object, *, request_seq: Optional[int]) -> Request:
    if not isinstance(req, Request):
        raise InvalidRequestError(
            f"请求必须是 Request 实例，收到 {type(req).__name__}",
            request_seq=request_seq, field="request")
    _check_core(req.core, request_seq=request_seq)
    _check_addr(req.addr, request_seq=request_seq)
    if not isinstance(req.op, Op):
        raise InvalidRequestError(
            f"op 必须是 Op.LOAD/Op.STORE，收到 {req.op!r}",
            request_seq=request_seq, field="op")
    if req.op is Op.LOAD:
        if req.value is not None:
            raise InvalidRequestError(
                "load 请求不能带 value（完成读值由 Completion 给出）",
                request_seq=request_seq, field="value")
    else:
        if req.value is None:
            raise InvalidRequestError(
                "store 请求必须提供 int 类型 value",
                request_seq=request_seq, field="value")
        _check_int("value", req.value, request_seq=request_seq)
    if req.txn_id is not None:
        _check_int("txn_id", req.txn_id, request_seq=request_seq)
        if req.txn_id <= 0:
            raise InvalidRequestError(
                f"txn_id 必须为正整数，收到 {req.txn_id}",
                request_seq=request_seq, field="txn_id")
    return req


# ---------------------------------------------------------------------------
# 内核
# ---------------------------------------------------------------------------


class CacheCoherenceKernel:
    """消息驱动 MSI 目录内核。

    构造:
        memory: 可选 {addr: int} 初始内存映像；缺省所有地址为 0。
                值必须是 int（bool / float / NaN / Infinity 一律拒绝）。
        limits: 可选 Limits 容量配置；缺省为有限的 DEFAULT_LIMITS。
                三类上限：每地址等待队列、每核未完成请求、每条有向网络
                通道在途消息。通道满时内核发送的报文保留在发送端
                （retained），事务保持未完成，待通道腾出后自动续传；
                请求接纳会超过队列 / 每核上限时抛 CapacityExceededError，
                且任何状态变更之前抛出（批次整体零变更）。
    """

    def __init__(self, memory: Optional[dict[int, int]] = None,
                 limits: Optional[Limits] = None) -> None:
        if memory is not None and not isinstance(memory, dict):
            raise InvalidRequestError("memory 必须是 dict[int, int]",
                                      field="memory")
        if limits is None:
            limits = DEFAULT_LIMITS
        elif not isinstance(limits, Limits):
            raise InvalidRequestError(
                f"limits 必须是 Limits 实例，收到 {type(limits).__name__}",
                field="limits")
        mem = [0] * NUM_ADDRS
        if memory:
            for addr, value in memory.items():
                _check_addr(addr)
                _check_int("memory value", value)
                mem[addr] = value
        self._memory: list[int] = mem
        self._limits: Limits = limits
        self._dir: list[_DirEntry] = [_DirEntry() for _ in range(NUM_ADDRS)]
        self._caches: list[dict[int, _CacheLine]] = [
            {a: _CacheLine() for a in range(NUM_ADDRS)} for _ in range(NUM_CORES)
        ]
        # 在途（已发送、待投递）消息：msg_id -> Message。
        self._pending: dict[int, Message] = {}
        # 通道在途计数：(src,dst) -> 条数；与 _pending 严格同步。
        self._in_flight: dict[tuple[int, int], int] = {}
        # 背压保留（通道满时尚未进入在途通道）：(src,dst) -> [msg_id...]（FIFO）。
        self._retained: dict[tuple[int, int], list[int]] = {}
        # 保留消息体：msg_id -> Message（保留消息不占 msg_id 之外的通道资源）。
        self._retained_msgs: dict[int, Message] = {}
        self._history: dict[int, Message] = {}
        self._completed: list[Completion] = []
        self._used_txn: set[int] = set()
        self._next_txn = 1
        self._next_msg = 1

    # -- 观测 ---------------------------------------------------------------

    @property
    def num_cores(self) -> int:
        return NUM_CORES

    @property
    def num_addrs(self) -> int:
        return NUM_ADDRS

    def pending(self) -> tuple[Message, ...]:
        """当前在途（待投递）消息，按 msg_id 升序。

        背压保留在发送端的消息不计入（见 retained_messages()），它们在
        通道腾出之前不可投递。
        """
        return tuple(self._pending[i] for i in sorted(self._pending))

    def retained_messages(self) -> tuple[Message, ...]:
        """因目标通道满而保留在发送端、尚未进入在途通道的消息。"""
        return tuple(self._retained_msgs[i]
                     for i in sorted(self._retained_msgs))

    @property
    def limits(self) -> Limits:
        return self._limits

    def _channel_in_flight(self, src: int, dst: int) -> int:
        return self._in_flight.get((src, dst), 0)

    def channel_load(self, src: int, dst: int) -> int:
        """某条有向通道当前在途消息数（观测用，含校验）。"""
        _check_int("src", src, msg=True)
        _check_int("dst", dst, msg=True)
        return self._channel_in_flight(src, dst)

    def _outstanding_for_core(self, core: int) -> int:
        """该核已接纳但未完成的请求数：活动事务 + 各地址队列中的请求。"""
        count = 0
        for e in self._dir:
            if e.txn is not None and e.txn.core == core:
                count += 1
            count += sum(1 for r in e.queue if r.core == core)
        return count

    def completed(self) -> tuple[Completion, ...]:
        return tuple(self._completed)

    def snapshot(self) -> Snapshot:
        """返回当前状态的只读视图（内容已拷贝，调用方修改不影响内核）。"""
        caches = tuple(
            {a: CacheView(line.state, line.value) for a, line in cache.items()}
            for cache in self._caches
        )
        directory = tuple(
            DirectoryView(
                e.state,
                tuple(sorted(e.sharers)),
                e.owner,
                e.txn.txn_id if e.txn is not None else None,
                bool(e.txn.awaiting_flush) if e.txn is not None else False,
                tuple(sorted(e.txn.awaiting_acks)) if e.txn is not None else (),
            )
            for e in self._dir
        )
        queues = tuple(
            tuple(
                QueuedRequestView(r.txn_id, r.core, r.addr, r.op, r.value)
                for r in e.queue
            )
            for e in self._dir
        )
        # 只在快照里暴露实际非空的通道 / 非空保留区，避免噪声。
        by_channel: dict[tuple[int, int], list[Message]] = {}
        for m in self._pending.values():
            by_channel.setdefault((m.src, m.dst), []).append(m)
        channels = tuple(
            ChannelView(src, dst,
                        tuple(sorted(ms, key=lambda m: m.msg_id)))
            for (src, dst), ms in sorted(by_channel.items())
        )
        retained = tuple(
            RetainedView(src, dst,
                         tuple(self._retained_msgs[i]
                               for i in self._retained[(src, dst)]))
            for (src, dst) in sorted(self._retained)
            if self._retained[(src, dst)]
        )
        return Snapshot(
            caches=caches,
            directory=directory,
            memory=tuple(self._memory),
            pending_messages=self.pending(),
            queues=queues,
            completed=tuple(self._completed),
            channels=channels,
            retained=retained,
            limits=self._limits,
        )

    # -- 请求输入 -----------------------------------------------------------

    def request(self, core: int, addr: int, op: Op, *,
                value: Optional[int] = None,
                txn_id: Optional[int] = None) -> RequestReport:
        """输入一条 load/store 请求。

        该地址有活动事务或已有排队请求时进入 FIFO 队列（queued=True）；
        无消息即可满足的请求（I/S 命中、I 上写、独占 S 升级）在本次调用内完成。
        """
        req = _validate_request(
            Request(core, addr, op, value, txn_id), request_seq=0)
        return self._admit(req)

    def request_batch(self, requests: Iterable[Request]) -> list[RequestReport]:
        """原子地批量接纳请求。

        先完整校验整批（含批内 txn_id 去重）；任一非法则内核状态零变更，
        不留下部分接纳。校验通过后按输入顺序逐条准入。
        """
        if isinstance(requests, (str, bytes)):
            raise InvalidRequestError("requests 必须是 Request 的可迭代对象",
                                      field="requests")
        try:
            original = list(requests)
        except TypeError as exc:
            raise InvalidRequestError(f"requests 不可迭代: {exc}",
                                      field="requests") from exc
        validated: list[Request] = []
        seen_ids: set[int] = set()
        for seq, raw in enumerate(original):
            req = _validate_request(raw, request_seq=seq)
            if req.txn_id is not None:
                if req.txn_id in seen_ids or req.txn_id in self._used_txn:
                    raise InvalidRequestError(
                        f"txn_id={req.txn_id} 重复（批内或历史中已存在）",
                        request_seq=seq, field="txn_id")
                seen_ids.add(req.txn_id)
            validated.append(req)
        # 容量判定可能在接纳到一半时才触发（前一条改变了队列/每核占用）。
        # 在深副本上试接纳整批：任一 CapacityExceededError 都丢弃副本，
        # 原内核零变更，绝不留下部分接纳。
        trial = copy.deepcopy(self)
        reports: list[RequestReport] = []
        for seq, req in enumerate(validated):
            try:
                reports.append(trial._admit(req))
            except CapacityExceededError as exc:
                exc.request_seq = seq
                raise
        # 全部接纳成功：副本状态整体替换进来（trial 随即丢弃，可直接移交）。
        self.__dict__.update(trial.__dict__)
        return reports

    def _alloc_txn(self) -> int:
        while self._next_txn in self._used_txn:
            self._next_txn += 1
        tid = self._next_txn
        self._used_txn.add(tid)
        self._next_txn += 1
        return tid

    def _admit(self, req: Request) -> RequestReport:
        # 容量判定先于 txn id 分配等任何状态变更，超容量不留部分准入。
        entry = self._dir[req.addr]
        will_queue = entry.txn is not None or bool(entry.queue)
        if will_queue:
            used = len(entry.queue)
            if used >= self._limits.queue_cap:
                raise CapacityExceededError(
                    f"地址 {req.addr} 的目录等待队列已满（{used} 条请求"
                    f"正在活动事务之外排队），新请求不能被接纳",
                    field="queue", cap=self._limits.queue_cap, used=used)
        stays_outstanding = will_queue or self._would_block(req, entry)
        if stays_outstanding:
            used_core = self._outstanding_for_core(req.core)
            if used_core >= self._limits.core_cap:
                raise CapacityExceededError(
                    f"核 {req.core} 已有 {used_core} 条接纳但未完成的请求，"
                    f"新请求不能被接纳",
                    field="core_outstanding", cap=self._limits.core_cap,
                    used=used_core)
        if req.txn_id is None:
            req = Request(req.core, req.addr, req.op, req.value,
                          self._alloc_txn())
        elif req.txn_id in self._used_txn:
            raise InvalidRequestError(
                f"txn_id={req.txn_id} 已被使用", field="txn_id")
        else:
            self._used_txn.add(req.txn_id)
            if req.txn_id >= self._next_txn:
                self._next_txn = req.txn_id + 1
        if will_queue:
            entry.queue.append(req)
            return RequestReport(req.txn_id, True, ())
        completions = self._start(req, entry)
        return RequestReport(req.txn_id, False, tuple(completions))

    def _would_block(self, req: Request, entry: _DirEntry) -> bool:
        """地址空闲时该请求是否需要消息往返才能完成（因而会占每核容量）。

        判定与 _start 的分支严格一致：纯函数，不修改状态。
        """
        if req.op is Op.LOAD:
            # 仅“读别的核拥有的 M”需要 FETCH/FLUSH。
            return entry.state is State.MODIFIED and entry.owner != req.core
        if entry.state is State.MODIFIED:
            return entry.owner != req.core
        if entry.state is State.INVALID:
            return False
        # 目录 S：除请求者外还有别的共享者时必须发 INV。
        return any(c != req.core for c in entry.sharers)

    # -- 协议推进 -----------------------------------------------------------

    def _start(self, req: Request, entry: _DirEntry) -> list[Completion]:
        """启动地址上的新事务；同步完成时经 _finish 返回完成事件。"""
        core, addr = req.core, req.addr
        line = self._caches[core][addr]
        done: list[Completion] = []

        if req.op is Op.LOAD:
            if entry.state is State.MODIFIED and entry.owner == core:
                self._finish(req, line.value, done)
                return done
            if entry.state is State.SHARED and core in entry.sharers:
                self._finish(req, line.value, done)
                return done
            if entry.state in (State.INVALID, State.SHARED):
                # 目录内存有效（I：无缓存者；S：内存干净），直接给数共享。
                line.state = State.SHARED
                line.value = self._memory[addr]
                entry.state = State.SHARED
                entry.sharers.add(core)
                self._finish(req, line.value, done)
                return done
            # 目录 M 且 owner 为别的核：先回写再共享。
            owner = entry.owner
            assert owner is not None
            entry.txn = _Txn(req.txn_id, core, Op.LOAD, None,
                             awaiting_flush=True, awaiting_acks=set(),
                             flush_owner=owner)
            self._send(MsgType.FETCH, DIR, owner, addr, req.txn_id)
            return done

        # STORE
        if entry.state is State.MODIFIED and entry.owner == core:
            line.value = req.value
            self._finish(req, req.value, done)
            return done
        if entry.state is State.INVALID:
            line.state = State.MODIFIED
            line.value = req.value
            entry.state = State.MODIFIED
            entry.owner = core
            self._finish(req, req.value, done)
            return done
        if entry.state is State.SHARED:
            targets = set(entry.sharers)
            targets.discard(core)
            if not targets:
                # requester 是唯一共享者：独占式升级，无失效对象，立即提交。
                line.state = State.MODIFIED
                line.value = req.value
                entry.state = State.MODIFIED
                entry.owner = core
                entry.sharers.clear()
                self._finish(req, req.value, done)
                return done
            entry.txn = _Txn(req.txn_id, core, Op.STORE, req.value,
                             awaiting_flush=False, awaiting_acks=set(targets),
                             flush_owner=None)
            for c in sorted(targets):
                self._send(MsgType.INV, DIR, c, addr, req.txn_id)
            return done
        # 目录 M、owner 为别的核：先回写，再失效旧 owner，最后升级。
        owner = entry.owner
        assert owner is not None
        entry.txn = _Txn(req.txn_id, core, Op.STORE, req.value,
                         awaiting_flush=True, awaiting_acks=set(),
                         flush_owner=owner)
        self._send(MsgType.FETCH, DIR, owner, addr, req.txn_id)
        return done

    def _finish(self, req: Request, value: Optional[int],
                done: list[Completion]) -> None:
        assert value is not None
        comp = Completion(req.txn_id, req.core, req.addr, req.op, value)
        self._completed.append(comp)
        done.append(comp)
        self._pump_queue(req.addr, done)

    def _pump_queue(self, addr: int, done: list[Completion]) -> None:
        entry = self._dir[addr]
        if entry.txn is not None or not entry.queue:
            return
        nxt = entry.queue.pop(0)
        done.extend(self._start(nxt, entry))

    # -- 消息 ---------------------------------------------------------------

    def _send(self, mtype: MsgType, src: int, dst: int, addr: int,
              txn_id: int, value: Optional[int] = None) -> int:
        """发送一条内核产生的报文。

        目标有向通道在途数已达 channel_cap 时，报文保留在发送端保留区
        （分配 msg_id、保留消息体，但不进入在途通道、不计通道占用、
        不可投递）；事务因此保持未完成。待该通道有报文被投递腾出空位后，
        _release_retained 按 msg_id 升序 FIFO 自动续传。
        """
        msg_id = self._next_msg
        self._next_msg += 1
        msg = Message(msg_id, txn_id, mtype, src, dst, addr, value)
        key = (src, dst)
        if self._channel_in_flight(src, dst) >= self._limits.channel_cap:
            self._retained.setdefault(key, []).append(msg_id)
            self._retained_msgs[msg_id] = msg
        else:
            self._pending[msg_id] = msg
            self._in_flight[key] = self._channel_in_flight(src, dst) + 1
        return msg_id

    def _release_retained(self, freed_key: tuple[int, int],
                          released: list[Message]) -> None:
        """通道 freed_key 腾出空位后，把该通道保留消息按 FIFO 续传。"""
        queue = self._retained.get(freed_key)
        while queue and self._channel_in_flight(*freed_key) < self._limits.channel_cap:
            msg_id = queue.pop(0)
            msg = self._retained_msgs.pop(msg_id)
            self._pending[msg_id] = msg
            self._in_flight[freed_key] = self._channel_in_flight(*freed_key) + 1
            released.append(msg)

    def deliver(self, what: Union[int, Message]) -> DeliveryReport:
        """显式网络投递。

        参数:
            what: 待发消息的 msg_id（int），或调用方显式构造的 Message
                  （msg_id 字段传 None，表示外部网络输入）。

        语义:
        - 正常消息生效并从在途通道移除（腾出 1 个通道位置）；
        - 同一 msg_id 重复投递：IGNORED_DUPLICATE，幂等无副作用；
        - 报文属于已结束/其他事务（典型：陈旧 INV_ACK/FLUSH）：
          IGNORED_STALE，不推进当前新事务，但报文本身已到达、仍腾出通道；
        - msg_id 从不存在：InvalidMessageError；msg_id 仍是背压保留消息
          （尚未进入在途通道）：InvalidMessageError 明确提示不可投递；
        - 注入消息引用从未注册的 txn_id、或字段非法：InvalidMessageError。

        容量与背压：本次处理新产生的报文在 emitted 中（目标通道满的那些
        同时仍在发送端保留）；腾出通道后自动续传的保留消息在 released 中。
        失败不留部分变更：所有有效性判定在任何状态修改之前完成。
        """
        if isinstance(what, Message):
            msg, injected = self._validate_injected(what), True
        else:
            _check_int("msg_id", what, msg=True)
            if what in self._history:
                return DeliveryReport(
                    what, self._history[what].txn_id,
                    DeliveryStatus.IGNORED_DUPLICATE,
                    "该消息已投递过，重复投递不产生任何效果", (), ())
            msg = self._pending.get(what)
            if msg is None:
                if what in self._retained_msgs:
                    raise InvalidMessageError(
                        f"msg_id={what} 仍因目标通道满而保留在发送端，"
                        f"尚未进入在途通道，当前不能投递", msg_id=what)
                raise InvalidMessageError(
                    f"msg_id={what} 既不在待发队列也无投递历史", msg_id=what)
            injected = False

        # 先在“通道仍占满”的状态下生效：本次新产生、走同一通道的报文会
        # 排在已有保留消息之后，保证每通道严格按 msg_id 序 FIFO 续传。
        start_next_msg = self._next_msg
        done: list[Completion] = []
        stale_reason = self._apply(msg, done)
        emitted = tuple(
            self._pending[i] if i in self._pending else self._retained_msgs[i]
            for i in range(start_next_msg, self._next_msg)
        )

        released: list[Message] = []
        if not injected:
            key = (msg.src, msg.dst)
            del self._pending[msg.msg_id]
            self._in_flight[key] = self._channel_in_flight(*key) - 1
            if self._in_flight[key] <= 0:
                self._in_flight.pop(key, None)
            self._history[msg.msg_id] = msg
            # 报文已到达（即便陈旧）：腾出通道并续传保留消息。
            self._release_retained(key, released)

        if stale_reason is not None:
            return DeliveryReport(
                msg.msg_id, msg.txn_id, DeliveryStatus.IGNORED_STALE,
                stale_reason, (), (), tuple(released))
        return DeliveryReport(
            msg.msg_id, msg.txn_id, DeliveryStatus.APPLIED, None,
            emitted, tuple(done), tuple(released))

    # -- 注入消息校验 -------------------------------------------------------

    def _validate_injected(self, msg: object) -> Message:
        if not isinstance(msg, Message):
            raise InvalidMessageError(
                f"deliver 的参数必须是 int(msg_id) 或 Message，收到 "
                f"{type(msg).__name__}", field="message")
        if msg.msg_id is not None:
            raise InvalidMessageError(
                "注入的网络消息 msg_id 必须为 None（msg_id 由内核分配）；"
                "投递内核消息请直接 deliver(msg_id)", field="msg_id")
        _check_int("txn_id", msg.txn_id, msg=True)
        if msg.txn_id <= 0:
            raise InvalidMessageError(
                f"txn_id 必须为正整数，收到 {msg.txn_id}", field="txn_id")
        if not isinstance(msg.mtype, MsgType):
            raise InvalidMessageError(
                f"mtype 必须是 MsgType，收到 {msg.mtype!r}", field="mtype")
        _check_int("src", msg.src, msg=True)
        _check_int("dst", msg.dst, msg=True)
        _check_int("addr", msg.addr, msg=True)
        if not 0 <= msg.addr < NUM_ADDRS:
            raise InvalidMessageError(
                f"addr 必须在 [0,{NUM_ADDRS})，收到 {msg.addr}", field="addr")
        mtype = msg.mtype
        if mtype in (MsgType.FETCH, MsgType.INV):
            if msg.src != DIR or not 0 <= msg.dst < NUM_CORES:
                raise InvalidMessageError(
                    f"{mtype.value} 必须 src=DIR({DIR}) 且 dst 为核号",
                    field="src/dst")
        else:
            if not 0 <= msg.src < NUM_CORES or msg.dst != DIR:
                raise InvalidMessageError(
                    f"{mtype.value} 必须 src 为核号且 dst=DIR({DIR})",
                    field="src/dst")
        if mtype is MsgType.FLUSH:
            _check_int("value", msg.value, msg=True)
        elif msg.value is not None:
            raise InvalidMessageError(
                f"{mtype.value} 消息不能带 value", field="value")
        if msg.txn_id not in self._used_txn:
            raise InvalidMessageError(
                f"txn_id={msg.txn_id} 从未在本内核注册过", field="txn_id")
        return msg

    # -- 消息生效（返回 None 表示生效，返回 str 表示陈旧忽略原因） -----------

    def _apply(self, msg: Message, done: list[Completion]) -> Optional[str]:
        entry = self._dir[msg.addr]
        txn = entry.txn

        if msg.mtype is MsgType.FETCH:
            # 陈旧性判定先于任何状态修改：FETCH 必须对应当前“等回写”事务，
            # 且接收方正是该事务记录的旧 owner。
            if (txn is None or txn.txn_id != msg.txn_id
                    or not txn.awaiting_flush or txn.flush_owner != msg.dst):
                return (f"陈旧 FETCH：属于事务 {msg.txn_id}、指向核 {msg.dst}，"
                        f"地址 {msg.addr} 当前活动事务为 "
                        f"{txn.txn_id if txn else None}，不推进新事务")
            owner_line = self._caches[msg.dst][msg.addr]
            if owner_line.state is not State.MODIFIED:
                raise InvalidMessageError(
                    f"FETCH 到达时核 {msg.dst} 对地址 {msg.addr} 不是 M",
                    msg_id=msg.msg_id)
            owner_line.state = State.SHARED
            self._send(MsgType.FLUSH, msg.dst, DIR, msg.addr,
                       msg.txn_id, owner_line.value)
            return None

        if msg.mtype is MsgType.INV:
            # INV 必须对应处于“等失效 ACK”阶段的当前事务且接收方在等待集合中。
            if (txn is None or txn.txn_id != msg.txn_id
                    or txn.awaiting_flush
                    or msg.dst not in txn.awaiting_acks):
                return (f"陈旧 INV：属于事务 {msg.txn_id}、指向核 {msg.dst}，"
                        f"地址 {msg.addr} 当前活动事务为 "
                        f"{txn.txn_id if txn else None}，不推进新事务")
            target_line = self._caches[msg.dst][msg.addr]
            if target_line.state is not State.SHARED:
                raise InvalidMessageError(
                    f"INV 到达时核 {msg.dst} 对地址 {msg.addr} 不是 S",
                    msg_id=msg.msg_id)
            target_line.state = State.INVALID
            target_line.value = None
            self._send(MsgType.INV_ACK, msg.dst, DIR, msg.addr, msg.txn_id)
            return None

        if msg.mtype is MsgType.FLUSH:
            if (txn is None or txn.txn_id != msg.txn_id
                    or not txn.awaiting_flush):
                return (f"FLUSH 属于事务 {msg.txn_id}，与地址 {msg.addr} "
                        f"当前活动事务"
                        f"（{txn.txn_id if txn else None}）不符，不推进新事务")
            self._memory[msg.addr] = msg.value
            old_owner = txn.flush_owner
            entry.owner = None
            entry.state = State.SHARED
            entry.sharers = {old_owner}
            txn.awaiting_flush = False
            if txn.op is Op.LOAD:
                entry.txn = None
                line = self._caches[txn.core][msg.addr]
                line.state = State.SHARED
                line.value = msg.value
                entry.sharers.add(txn.core)
                self._finish(Request(txn.core, msg.addr, Op.LOAD, None,
                                     txn.txn_id), msg.value, done)
                return None
            # STORE：回写完成后失效旧 owner（若 requester 就是旧 owner，
            # 该路径在准入时已被 M 命中覆盖，故 targets 必含旧 owner）。
            targets = {old_owner}
            targets.discard(txn.core)
            txn.awaiting_acks = targets
            if not targets:
                self._finalize_store(msg.addr, txn, done)
            else:
                for c in sorted(targets):
                    self._send(MsgType.INV, DIR, c, msg.addr, txn.txn_id)
            return None

        # INV_ACK
        if (txn is None or txn.txn_id != msg.txn_id
                or txn.awaiting_flush or msg.src not in txn.awaiting_acks):
            return (f"陈旧 INV_ACK：来自核 {msg.src}、属于事务 {msg.txn_id}，"
                    f"地址 {msg.addr} 当前活动事务为 "
                    f"{txn.txn_id if txn else None}，不推进新事务")
        txn.awaiting_acks.discard(msg.src)
        entry.sharers.discard(msg.src)
        if not txn.awaiting_acks:
            self._finalize_store(msg.addr, txn, done)
        return None

    def _finalize_store(self, addr: int, txn: _Txn,
                        done: list[Completion]) -> None:
        entry = self._dir[addr]
        line = self._caches[txn.core][addr]
        line.state = State.MODIFIED
        line.value = txn.value
        entry.state = State.MODIFIED
        entry.owner = txn.core
        entry.sharers.clear()
        entry.txn = None
        self._finish(Request(txn.core, addr, Op.STORE, txn.value, txn.txn_id),
                     txn.value, done)
