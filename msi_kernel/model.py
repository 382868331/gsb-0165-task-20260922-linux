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

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Optional, Union

from .errors import InvalidMessageError, InvalidRequestError

NUM_CORES = 3
NUM_ADDRS = 8
NUM_SECTORS = 4
ALL_SECTORS = (1 << NUM_SECTORS) - 1
# 目录在 src/dst 中的编号（核编号为 0..NUM_CORES-1）。
DIR = NUM_CORES


class LineMode(str, Enum):
    """缓存行寻址模式。"""

    WHOLE_LINE = "whole_line"
    FOUR_SECTOR = "four_sector"


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
    value: Optional[Union[int, tuple[int, ...]]] = None
    txn_id: Optional[int] = None
    sectors: Optional[int] = None


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
    value: Optional[Union[int, tuple[int, ...]]] = None
    sectors: Optional[int] = None


@dataclass(frozen=True)
class Completion:
    """一个事务的完成事件。

    LOAD 的 value 为实际完成读值；STORE 的 value 为写入值。
    """

    txn_id: int
    core: int
    addr: int
    op: Op
    value: Union[int, tuple[int, ...]]
    sectors: Optional[int] = None


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
    sectors: Optional[int] = None


@dataclass(frozen=True)
class SectorCacheView:
    sector: int
    state: State
    value: int
    version: int


@dataclass(frozen=True)
class SectorDirectoryView:
    sector: int
    state: State
    sharers: tuple[int, ...]
    owner: Optional[int]
    value: int
    version: int


@dataclass(frozen=True)
class SectorTxnView:
    txn_id: int
    core: int
    op: Op
    sectors: int
    flush_waiters: tuple[tuple[int, int], ...]
    ack_waiters: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class RequestReport:
    txn_id: int
    queued: bool
    completions: tuple[Completion, ...]


@dataclass(frozen=True)
class DeliveryReport:
    """deliver 的结果报告。

    completions 为本次投递直接完成、或经 FIFO 链锁反应完成的全部事务。
    emitted 为本次投递新产生的待发消息。
    """

    msg_id: Optional[int]
    txn_id: Optional[int]
    status: DeliveryStatus
    reason: Optional[str]
    emitted: tuple[Message, ...]
    completions: tuple[Completion, ...]

    @property
    def applied(self) -> bool:
        return self.status is DeliveryStatus.APPLIED


@dataclass(frozen=True)
class Snapshot:
    caches: tuple[dict[int, CacheView], ...]
    directory: tuple[DirectoryView, ...]
    memory: tuple[int, ...]
    pending_messages: tuple[Message, ...]
    queues: tuple[tuple[QueuedRequestView, ...], ...]
    completed: tuple[Completion, ...]
    sector_caches: Optional[
        tuple[dict[int, tuple[SectorCacheView, ...]], ...]
    ] = None
    sector_directory: Optional[tuple[tuple[SectorDirectoryView, ...], ...]] = None
    sector_txns: Optional[tuple[tuple[SectorTxnView, ...], ...]] = None


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


@dataclass
class _SectorSlot:
    state: State = State.INVALID
    value: int = 0
    version: int = 0


@dataclass
class _SectorTxn:
    addr: int
    txn_id: int
    core: int
    op: Op
    sectors: int
    values: tuple[int, ...]
    flush_waiters: dict[int, int] = field(default_factory=dict)
    ack_waiters: dict[int, int] = field(default_factory=dict)


@dataclass
class _SectorDirEntry:
    states: list[State] = field(
        default_factory=lambda: [State.INVALID for _ in range(NUM_SECTORS)])
    sharers: list[set[int]] = field(
        default_factory=lambda: [set() for _ in range(NUM_SECTORS)])
    owners: list[Optional[int]] = field(
        default_factory=lambda: [None for _ in range(NUM_SECTORS)])
    versions: list[int] = field(
        default_factory=lambda: [0 for _ in range(NUM_SECTORS)])
    txns: list[_SectorTxn] = field(default_factory=list)
    queue: list[Request] = field(default_factory=list)


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


def _check_sector_mask(mask, *, request_seq: Optional[int] = None,
                       msg: bool = False) -> int:
    error_type = InvalidMessageError if msg else InvalidRequestError
    _check_int("sectors", mask, request_seq=request_seq, msg=msg)
    if not 1 <= mask <= ALL_SECTORS:
        raise error_type(
            f"sectors 必须是 [1,{ALL_SECTORS}] 内的扇区掩码，收到 {mask}",
            request_seq=request_seq, field="sectors")
    return int(mask)


def _mask_sectors(mask: int):
    for sector in range(NUM_SECTORS):
        if mask & (1 << sector):
            yield sector


def _validate_sector_store_value(value, mask: int,
                                 request_seq: Optional[int]):
    count = mask.bit_count()
    if not isinstance(value, tuple):
        raise InvalidRequestError(
            f"四扇区模式 store 的 value 必须是长度 {count} 的 int 元组",
            request_seq=request_seq, field="value")
    if len(value) != count:
        raise InvalidRequestError(
            f"store 覆盖 {count} 个扇区，但 value 长度为 {len(value)}",
            request_seq=request_seq, field="value")
    for index, item in enumerate(value):
        _check_int(f"value[{index}]", item, request_seq=request_seq)


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
        if req.sectors is None:
            _check_int("value", req.value, request_seq=request_seq)
        else:
            mask = _check_sector_mask(req.sectors, request_seq=request_seq)
            _validate_sector_store_value(req.value, mask, request_seq)
    if req.sectors is not None:
        _check_sector_mask(req.sectors, request_seq=request_seq)
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
        mode:   默认 LineMode.WHOLE_LINE 保持原整行 MSI；
                LineMode.FOUR_SECTOR 启用固定 4 扇区模式，初始 memory
                标量会广播到该地址的 4 个扇区。
    """

    def __init__(self, memory: Optional[dict[int, int]] = None,
                 *,
                 mode: LineMode = LineMode.WHOLE_LINE) -> None:
        if memory is not None and not isinstance(memory, dict):
            raise InvalidRequestError("memory 必须是 dict[int, int]",
                                      field="memory")
        if not isinstance(mode, LineMode):
            raise InvalidRequestError(
                f"mode 必须是 LineMode，收到 {mode!r}", field="mode")
        self._mode = mode
        mem = [0] * NUM_ADDRS
        sector_mem = [[0] * NUM_SECTORS for _ in range(NUM_ADDRS)]
        if memory:
            for addr, value in memory.items():
                _check_addr(addr)
                _check_int("memory value", value)
                if mode is LineMode.FOUR_SECTOR:
                    sector_mem[addr] = [value] * NUM_SECTORS
                else:
                    mem[addr] = value
        self._memory: list[int] = mem
        self._sector_memory = sector_mem
        self._dir: list[_DirEntry] = [_DirEntry() for _ in range(NUM_ADDRS)]
        self._sector_dir: list[_SectorDirEntry] = [
            _SectorDirEntry() for _ in range(NUM_ADDRS)
        ]
        self._caches: list[dict[int, _CacheLine]] = [
            {a: _CacheLine() for a in range(NUM_ADDRS)} for _ in range(NUM_CORES)
        ]
        self._sector_caches: list[dict[int, list[_SectorSlot]]] = [
            {
                a: [_SectorSlot() for _ in range(NUM_SECTORS)]
                for a in range(NUM_ADDRS)
            }
            for _ in range(NUM_CORES)
        ]
        self._pending: dict[int, Message] = {}
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

    @property
    def mode(self) -> LineMode:
        return self._mode

    def pending(self) -> tuple[Message, ...]:
        """当前待发（待投递）消息，按 msg_id 升序。"""
        return tuple(self._pending[i] for i in sorted(self._pending))

    def completed(self) -> tuple[Completion, ...]:
        return tuple(self._completed)

    def snapshot(self) -> Snapshot:
        """返回当前状态的只读视图（内容已拷贝，调用方修改不影响内核）。"""
        if self._mode is LineMode.FOUR_SECTOR:
            return self._snapshot_sector()
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
        return Snapshot(
            caches=caches,
            directory=directory,
            memory=tuple(self._memory),
            pending_messages=self.pending(),
            queues=queues,
            completed=tuple(self._completed),
        )

    def _snapshot_sector(self) -> Snapshot:
        line_caches = tuple(
            {a: CacheView(State.INVALID, None) for a in range(NUM_ADDRS)}
            for _ in range(NUM_CORES)
        )
        sector_caches = tuple(
            {
                a: tuple(
                    SectorCacheView(
                        sector,
                        self._sector_caches[core][a][sector].state,
                        self._sector_caches[core][a][sector].value,
                        self._sector_caches[core][a][sector].version,
                    )
                    for sector in range(NUM_SECTORS)
                )
                for a in range(NUM_ADDRS)
            }
            for core in range(NUM_CORES)
        )
        sector_directory = tuple(
            tuple(
                SectorDirectoryView(
                    sector,
                    entry.states[sector],
                    tuple(sorted(entry.sharers[sector])),
                    entry.owners[sector],
                    self._sector_memory[a][sector],
                    entry.versions[sector],
                )
                for sector in range(NUM_SECTORS)
            )
            for a, entry in enumerate(self._sector_dir)
        )
        sector_txns = tuple(
            tuple(
                SectorTxnView(
                    txn.txn_id,
                    txn.core,
                    txn.op,
                    txn.sectors,
                    tuple(sorted(txn.flush_waiters.items())),
                    tuple(sorted(txn.ack_waiters.items())),
                )
                for txn in entry.txns
            )
            for entry in self._sector_dir
        )
        queues = tuple(
            tuple(
                QueuedRequestView(
                    req.txn_id, req.core, req.addr, req.op, req.value,
                    req.sectors)
                for req in entry.queue
            )
            for entry in self._sector_dir
        )
        return Snapshot(
            caches=line_caches,
            directory=(),
            memory=(),
            pending_messages=self.pending(),
            queues=queues,
            completed=tuple(self._completed),
            sector_caches=sector_caches,
            sector_directory=sector_directory,
            sector_txns=sector_txns,
        )

    # -- 请求输入 -----------------------------------------------------------

    def request(self, core: int, addr: int, op: Op, *,
                value: Optional[int] = None,
                txn_id: Optional[int] = None,
                sectors: Optional[int] = None) -> RequestReport:
        """输入一条 load/store 请求。

        该地址有活动事务或已有排队请求时进入 FIFO 队列（queued=True）；
        无消息即可满足的请求（I/S 命中、I 上写、独占 S 升级）在本次调用内完成。
        """
        if self._mode is LineMode.FOUR_SECTOR:
            mask = (ALL_SECTORS if sectors is None
                    else _check_sector_mask(sectors))
            if op is Op.STORE and isinstance(value, int) and not isinstance(value, bool):
                value = tuple(value for _ in range(mask.bit_count()))
            req = _validate_request(
                Request(core, addr, op, value, txn_id, mask), request_seq=0)
        else:
            req = _validate_request(
                Request(core, addr, op, value, txn_id, sectors), request_seq=0)
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
            if self._mode is LineMode.FOUR_SECTOR and isinstance(raw, Request):
                if raw.sectors is None:
                    value = raw.value
                    if (raw.op is Op.STORE and isinstance(value, int)
                            and not isinstance(value, bool)):
                        value = tuple(value for _ in range(NUM_SECTORS))
                    raw = Request(raw.core, raw.addr, raw.op, value,
                                  raw.txn_id, ALL_SECTORS)
            req = _validate_request(raw, request_seq=seq)
            if (self._mode is LineMode.WHOLE_LINE
                    and req.sectors is not None):
                raise InvalidRequestError(
                    "sectors 仅允许在 LineMode.FOUR_SECTOR 模式下使用",
                    request_seq=seq, field="sectors")
            if req.txn_id is not None:
                if req.txn_id in seen_ids or req.txn_id in self._used_txn:
                    raise InvalidRequestError(
                        f"txn_id={req.txn_id} 重复（批内或历史中已存在）",
                        request_seq=seq, field="txn_id")
                seen_ids.add(req.txn_id)
            validated.append(req)
        reports: list[RequestReport] = []
        for req in validated:
            # _admit 内部负责自动 id 的分配与显式 id 的登记；
            # 能走到这里的请求已通过全部校验，接纳过程不会再失败。
            reports.append(self._admit(req))
        return reports

    def _alloc_txn(self) -> int:
        while self._next_txn in self._used_txn:
            self._next_txn += 1
        tid = self._next_txn
        self._used_txn.add(tid)
        self._next_txn += 1
        return tid

    def _admit(self, req: Request) -> RequestReport:
        if self._mode is LineMode.FOUR_SECTOR:
            if req.sectors is None:
                req = Request(req.core, req.addr, req.op, req.value,
                              req.txn_id, ALL_SECTORS)
        elif req.sectors is not None:
            raise InvalidRequestError(
                "sectors 仅允许在 LineMode.FOUR_SECTOR 模式下使用",
                field="sectors")
        if req.txn_id is None:
            req = Request(req.core, req.addr, req.op, req.value,
                          self._alloc_txn(), req.sectors)
        elif req.txn_id in self._used_txn:
            raise InvalidRequestError(
                f"txn_id={req.txn_id} 已被使用", field="txn_id")
        else:
            self._used_txn.add(req.txn_id)
            if req.txn_id >= self._next_txn:
                self._next_txn = req.txn_id + 1
        if self._mode is LineMode.FOUR_SECTOR:
            return self._admit_sector(req)
        entry = self._dir[req.addr]
        if entry.txn is not None or entry.queue:
            entry.queue.append(req)
            return RequestReport(req.txn_id, True, ())
        completions = self._start(req, entry)
        return RequestReport(req.txn_id, False, tuple(completions))

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
              txn_id: int, value=None,
              sectors: Optional[int] = None) -> int:
        msg_id = self._next_msg
        self._next_msg += 1
        self._pending[msg_id] = Message(
            msg_id, txn_id, mtype, src, dst, addr, value, sectors)
        return msg_id

    def _find_sector_txn(self, addr: int, txn_id: int) -> Optional[_SectorTxn]:
        for txn in self._sector_dir[addr].txns:
            if txn.txn_id == txn_id:
                return txn
        return None

    def _sector_active_mask(self, entry: _SectorDirEntry) -> int:
        mask = 0
        for txn in entry.txns:
            mask |= txn.sectors
        return mask

    def _sector_queued_mask(self, entry: _SectorDirEntry, before: int = 0) -> int:
        mask = 0
        for req in entry.queue[:before]:
            mask |= req.sectors
        return mask

    def _admit_sector(self, req: Request) -> RequestReport:
        entry = self._sector_dir[req.addr]
        position = len(entry.queue)
        blocking = self._sector_active_mask(entry)
        blocking |= self._sector_queued_mask(entry, position)
        if blocking & req.sectors:
            entry.queue.append(req)
            return RequestReport(req.txn_id, True, ())
        done = self._start_sector(req, entry)
        return RequestReport(req.txn_id, False, tuple(done))

    def _start_sector(self, req: Request, entry: _SectorDirEntry,
                      done: Optional[list[Completion]] = None) -> list[Completion]:
        if done is None:
            done = []
        mask = req.sectors
        sectors = list(_mask_sectors(mask))
        if req.op is Op.LOAD:
            flush_waiters: dict[int, int] = {}
            for sector in sectors:
                owner = entry.owners[sector]
                slot = self._sector_caches[req.core][req.addr][sector]
                readable = (
                    (entry.states[sector] is State.SHARED
                     and req.core in entry.sharers[sector])
                    or (entry.states[sector] is State.MODIFIED
                        and owner == req.core
                        and slot.state is State.MODIFIED)
                )
                if not readable:
                    owner = entry.owners[sector]
                    if entry.states[sector] is not State.MODIFIED or owner is None:
                        continue
                    flush_waiters[owner] = flush_waiters.get(owner, 0) | (1 << sector)
            if not flush_waiters:
                self._grant_sector_load(req, entry, done)
            else:
                txn = _SectorTxn(req.addr, req.txn_id, req.core, Op.LOAD, mask, (),
                                 flush_waiters, {})
                entry.txns.append(txn)
                for owner, owner_mask in sorted(flush_waiters.items()):
                    self._send(MsgType.FETCH, DIR, owner, req.addr,
                               req.txn_id, sectors=owner_mask)
            return done

        flush_waiters = {}
        ack_waiters: dict[int, int] = {}
        all_owner = True
        for sector in sectors:
            owner = entry.owners[sector]
            if entry.states[sector] is State.MODIFIED and owner == req.core:
                slot = self._sector_caches[req.core][req.addr][sector]
                if slot.state is not State.MODIFIED:
                    raise InvalidMessageError(
                        f"目录记录核 {req.core} 为扇区 {sector} owner，但缓存不是 M")
                continue
            all_owner = False
            if entry.states[sector] is State.MODIFIED and owner is not None:
                flush_waiters[owner] = flush_waiters.get(owner, 0) | (1 << sector)
            else:
                targets = set(entry.sharers[sector])
                targets.discard(req.core)
                for target in targets:
                    ack_waiters[target] = ack_waiters.get(target, 0) | (1 << sector)
        if all_owner:
            self._finalize_sector_store(req, entry, done)
            return done
        if not flush_waiters and not ack_waiters:
            self._finalize_sector_store(req, entry, done)
            return done
        txn = _SectorTxn(req.addr, req.txn_id, req.core, Op.STORE, mask,
                         tuple(req.value), flush_waiters, ack_waiters)
        entry.txns.append(txn)
        if flush_waiters:
            for owner, owner_mask in sorted(flush_waiters.items()):
                self._send(MsgType.FETCH, DIR, owner, req.addr, req.txn_id,
                           sectors=owner_mask)
            return done
        if ack_waiters:
            for target, target_mask in sorted(ack_waiters.items()):
                self._send(MsgType.INV, DIR, target, req.addr, req.txn_id,
                           sectors=target_mask)
            return done
        entry.txns.remove(txn)
        self._finalize_sector_store(req, entry, done)
        return done

    def _grant_sector_load(self, req: Request, entry: _SectorDirEntry,
                           done: list[Completion]) -> None:
        values = []
        for sector in _mask_sectors(req.sectors):
            owner = entry.owners[sector]
            if entry.states[sector] is State.MODIFIED and owner == req.core:
                source = self._sector_caches[req.core][req.addr][sector]
            else:
                source_value = self._sector_memory[req.addr][sector]
                source_version = entry.versions[sector]
                slot = self._sector_caches[req.core][req.addr][sector]
                slot.state = State.SHARED
                slot.value = source_value
                slot.version = source_version
                entry.states[sector] = State.SHARED
                entry.owners[sector] = None
                entry.sharers[sector].add(req.core)
                source = slot
            values.append(source.value)
        self._complete_sector(req, tuple(values), done)

    def _finalize_sector_store(self, req: Request, entry: _SectorDirEntry,
                               done: list[Completion]) -> None:
        for index, sector in enumerate(_mask_sectors(req.sectors)):
            slot = self._sector_caches[req.core][req.addr][sector]
            slot.state = State.MODIFIED
            slot.value = req.value[index]
            slot.version = entry.versions[sector] + 1
            entry.states[sector] = State.MODIFIED
            entry.owners[sector] = req.core
            entry.sharers[sector].clear()
            entry.versions[sector] = slot.version
        self._complete_sector(req, tuple(req.value), done)

    def _complete_sector(self, req: Request, values: tuple[int, ...],
                         done: list[Completion]) -> None:
        comp = Completion(req.txn_id, req.core, req.addr, req.op, values,
                          req.sectors)
        self._completed.append(comp)
        done.append(comp)
        entry = self._sector_dir[req.addr]
        entry.txns = [txn for txn in entry.txns if txn.txn_id != req.txn_id]
        self._pump_sector_queue(req.addr, done)

    def _pump_sector_queue(self, addr: int, done: list[Completion]) -> None:
        entry = self._sector_dir[addr]
        while entry.queue:
            active_mask = self._sector_active_mask(entry)
            if active_mask & entry.queue[0].sectors:
                return
            nxt = entry.queue.pop(0)
            self._start_sector(nxt, entry, done)

    def deliver(self, what: Union[int, Message]) -> DeliveryReport:
        """显式网络投递。

        参数:
            what: 待发消息的 msg_id（int），或调用方显式构造的 Message
                  （msg_id 字段传 None，表示外部网络输入）。

        语义:
        - 正常消息生效并从待发队列移除；
        - 同一 msg_id 重复投递：IGNORED_DUPLICATE，幂等无副作用；
        - 报文属于已结束/其他事务（典型：陈旧 INV_ACK/FLUSH）：
          IGNORED_STALE，不推进当前新事务；
        - msg_id 从不存在：InvalidMessageError；
        - 注入消息引用从未注册的 txn_id、或字段非法：InvalidMessageError。

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
                raise InvalidMessageError(
                    f"msg_id={what} 既不在待发队列也无投递历史", msg_id=what)
            injected = False

        before = set(self._pending)
        done: list[Completion] = []
        if self._mode is LineMode.FOUR_SECTOR:
            stale_reason = self._apply_sector(msg, done)
        else:
            stale_reason = self._apply(msg, done)
        new_msgs = tuple(m for i, m in self._pending.items() if i not in before)
        if not injected:
            del self._pending[msg.msg_id]
            self._history[msg.msg_id] = msg
        if stale_reason is not None:
            return DeliveryReport(
                msg.msg_id, msg.txn_id, DeliveryStatus.IGNORED_STALE,
                stale_reason, (), ())
        return DeliveryReport(
            msg.msg_id, msg.txn_id, DeliveryStatus.APPLIED, None,
            new_msgs, tuple(done))

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
        if self._mode is LineMode.FOUR_SECTOR:
            _check_sector_mask(msg.sectors, msg=True)
        elif msg.sectors is not None:
            raise InvalidMessageError(
                "sectors 仅允许在 LineMode.FOUR_SECTOR 模式下使用",
                field="sectors")
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
            if self._mode is LineMode.FOUR_SECTOR:
                if not isinstance(msg.value, tuple):
                    raise InvalidMessageError(
                        "四扇区 FLUSH 的 value 必须是 (value, version) 元组",
                        field="value")
                if len(msg.value) != 2:
                    raise InvalidMessageError(
                        "四扇区 FLUSH 的 value 必须包含 value 与 version",
                        field="value")
                _check_int("flush value", msg.value[0], msg=True)
                _check_int("flush version", msg.value[1], msg=True)
            else:
                _check_int("value", msg.value, msg=True)
        elif msg.value is not None:
            raise InvalidMessageError(
                f"{mtype.value} 消息不能带 value", field="value")
        if msg.txn_id not in self._used_txn:
            raise InvalidMessageError(
                f"txn_id={msg.txn_id} 从未在本内核注册过", field="txn_id")
        return msg

    # -- 消息生效（返回 None 表示生效，返回 str 表示陈旧忽略原因） -----------

    def _apply_sector(self, msg: Message,
                      done: list[Completion]) -> Optional[str]:
        entry = self._sector_dir[msg.addr]
        txn = self._find_sector_txn(msg.addr, msg.txn_id)
        mask = msg.sectors
        if txn is None:
            return (f"扇区消息属于事务 {msg.txn_id}，但地址 {msg.addr} "
                    f"当前没有该活动事务，不推进新事务")

        if msg.mtype is MsgType.FETCH:
            wanted = txn.flush_waiters.get(msg.dst, 0)
            if not wanted or mask & wanted != mask:
                return (f"陈旧 FETCH：事务 {msg.txn_id} 未等待核 {msg.dst} "
                        f"扇区掩码 {mask:0{NUM_SECTORS}b}")
            for sector in _mask_sectors(mask):
                slot = self._sector_caches[msg.dst][msg.addr][sector]
                if slot.state is not State.MODIFIED:
                    raise InvalidMessageError(
                        f"FETCH 到达时核 {msg.dst} 扇区 {sector} 不是 M")
            values = {}
            for sector in _mask_sectors(mask):
                slot = self._sector_caches[msg.dst][msg.addr][sector]
                slot.state = State.SHARED
                values[sector] = (slot.value, slot.version)
            for sector, data in sorted(values.items()):
                self._send(MsgType.FLUSH, msg.dst, DIR, msg.addr,
                           msg.txn_id, data, 1 << sector)
            return None

        if msg.mtype is MsgType.INV:
            wanted = txn.ack_waiters.get(msg.dst, 0)
            if not wanted or mask & wanted != mask or bool(txn.flush_waiters):
                return (f"陈旧/过早 INV：事务 {msg.txn_id} 尚未在等核 "
                        f"{msg.dst} 扇区掩码 {mask:0{NUM_SECTORS}b}")
            for sector in _mask_sectors(mask):
                slot = self._sector_caches[msg.dst][msg.addr][sector]
                if slot.state is not State.SHARED:
                    raise InvalidMessageError(
                        f"INV 到达时核 {msg.dst} 扇区 {sector} 不是 S")
            for sector in _mask_sectors(mask):
                slot = self._sector_caches[msg.dst][msg.addr][sector]
                slot.state = State.INVALID
                slot.value = 0
                slot.version = 0
                entry.sharers[sector].discard(msg.dst)
            self._send(MsgType.INV_ACK, msg.dst, DIR, msg.addr,
                       msg.txn_id, sectors=mask)
            return None

        if msg.mtype is MsgType.FLUSH:
            if msg.src not in txn.flush_waiters:
                return (f"陈旧/迟到 FLUSH：事务 {msg.txn_id} 未等待核 "
                        f"{msg.src} 的回写")
            wanted = txn.flush_waiters[msg.src]
            if mask.bit_count() != 1 or mask & wanted != mask:
                return (f"FLUSH 扇区掩码与事务等待不符：{mask:0{NUM_SECTORS}b}")
            sector = next(_mask_sectors(mask))
            value, version = msg.value
            slot = self._sector_caches[msg.src][msg.addr][sector]
            if slot.version != version or slot.state not in (
                    State.MODIFIED, State.SHARED):
                raise InvalidMessageError(
                    "FLUSH 的版本与 owner 缓存版本不一致")
            self._sector_memory[msg.addr][sector] = value
            entry.versions[sector] = version
            entry.states[sector] = State.SHARED
            entry.owners[sector] = None
            entry.sharers[sector] = {msg.src}
            slot.state = State.SHARED
            txn.flush_waiters[msg.src] = wanted & ~mask
            if not txn.flush_waiters[msg.src]:
                del txn.flush_waiters[msg.src]
            if not txn.flush_waiters:
                self._complete_flush_sectors(txn, entry, done)
            return None

        wanted = txn.ack_waiters.get(msg.src, 0)
        if txn.flush_waiters or not wanted or mask & wanted != mask:
            return (f"陈旧/迟到 ACK：事务 {msg.txn_id} 未等待核 {msg.src} "
                    f"扇区掩码 {mask:0{NUM_SECTORS}b}，不放行新事务")
        txn.ack_waiters[msg.src] = wanted & ~mask
        if not txn.ack_waiters[msg.src]:
            del txn.ack_waiters[msg.src]
        if not txn.flush_waiters and not txn.ack_waiters:
            req = Request(txn.core, msg.addr, Op.STORE,
                          tuple(txn.values), txn.txn_id, txn.sectors)
            entry.txns.remove(txn)
            self._finalize_sector_store(req, entry, done)
        return None

    def _complete_flush_sectors(self, txn: _SectorTxn, entry: _SectorDirEntry,
                                done: list[Completion]) -> None:
        if txn.op is Op.LOAD:
            fake_req = Request(txn.core, txn.addr, Op.LOAD, None,
                               txn.txn_id, txn.sectors)
            entry.txns.remove(txn)
            for sector in _mask_sectors(txn.sectors):
                slot = self._sector_caches[txn.core][txn.addr][sector]
                slot.state = State.SHARED
                slot.value = self._sector_memory[txn.addr][sector]
                slot.version = entry.versions[sector]
                entry.sharers[sector].add(txn.core)
            values = tuple(
                self._sector_caches[txn.core][txn.addr][sector].value
                for sector in _mask_sectors(txn.sectors)
            )
            self._complete_sector(fake_req, values, done)
            return
        targets: dict[int, int] = {}
        for sector in _mask_sectors(txn.sectors):
            old_sharers = set(entry.sharers[sector])
            old_sharers.discard(txn.core)
            for target in old_sharers:
                targets[target] = targets.get(target, 0) | (1 << sector)
        txn.ack_waiters = targets
        if targets:
            for target, target_mask in sorted(targets.items()):
                self._send(MsgType.INV, DIR, target, txn.addr, txn.txn_id,
                           sectors=target_mask)
            return
        req = Request(txn.core, txn.addr, Op.STORE, tuple(txn.values),
                      txn.txn_id, txn.sectors)
        entry.txns.remove(txn)
        self._finalize_sector_store(req, entry, done)

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
