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
- evict(core, addr) 显式逐出（模拟有限缓存容量的替换决策），与 load/store
  共用同一地址 FIFO：I 行无操作立即完成；S 行先发 EVICT 通知、目录移除共享者
  后回 EVICT_ACK；M 行必须先经 FLUSH 把最新数据交回目录、收到 EVICT_ACK 才
  转 I。逐出的全部推进仍只来自显式 deliver，内核不直接改其他核的状态；
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
# 目录在 src/dst 中的编号（核编号为 0..NUM_CORES-1）。
DIR = NUM_CORES


class State(str, Enum):
    """缓存行 / 目录项状态。"""

    INVALID = "I"
    SHARED = "S"
    MODIFIED = "M"


class Op(str, Enum):
    """请求操作类型。"""

    LOAD = "load"
    STORE = "store"
    EVICT = "evict"


class MsgType(str, Enum):
    """网络消息类型（目录↔核）。

    FETCH:    目录 → M 的 owner，要求回写最新数据并降级为 S；
    FLUSH:    owner → 目录，携带最新数据完成回写（读 M / 写 M / M 逐出共用）；
    INV:      目录 → 某个 S 共享者，要求失效该行；
    INV_ACK:  共享者 → 目录，失效完成确认；
    EVICT:    核 → 目录，S 行替换通知（该行干净，不携带数据）；
    EVICT_ACK: 目录 → 核，逐出请求获准完成（共享者已移除 / 回写已入账）。
    """

    FETCH = "FETCH"
    FLUSH = "FLUSH"
    INV = "INV"
    INV_ACK = "INV_ACK"
    EVICT = "EVICT"
    EVICT_ACK = "EVICT_ACK"


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
    """一条 load/store/evict 请求。

    LOAD 的 value 必须为 None；STORE 必须给 int 类型 value（bool 拒绝，
    float 拒绝，NaN/Infinity 给出明确报错）；EVICT 的 value 必须为 None，
    也可直接用 kernel.evict(core, addr)。
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

    LOAD 的 value 为实际完成读值；STORE 的 value 为写入值；
    EVICT 的 value 恒为 0（逐出没有读值；M 行的最新数据随 FLUSH 先行交回
    目录，不出现在完成事件里）。
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
    phase: str = ""


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


# ---------------------------------------------------------------------------
# 内部可变结构
# ---------------------------------------------------------------------------


@dataclass
class _CacheLine:
    state: State = State.INVALID
    value: Optional[int] = None


@dataclass
class _Txn:
    """一个地址上的活动事务。

    phase 取值（消息驱动的各等待阶段）：
    - "flush":  已向旧 owner 发 FETCH（load/store），或 M 行逐出已由 owner
                发出 FLUSH；正在等带回数据的 FLUSH；
    - "inv":    回写已入账（若有），已发 INV，正在等 awaiting_acks 中的 ACK；
    - "notice": S 行逐出：核端已自行置 I，EVICT 通知正在送往目录；
    - "ack":    逐出已被目录受理（共享者已移除 / 回写已入账），EVICT_ACK
                正在送回逐出核，收到后逐出事务才完成。
    """

    txn_id: int
    core: int
    op: Op
    value: Optional[int]
    phase: str = ""
    awaiting_acks: set[int] = field(default_factory=set)
    flush_owner: Optional[int] = None

    @property
    def awaiting_flush(self) -> bool:
        return self.phase == "flush"


@dataclass
class _DirEntry:
    state: State = State.INVALID
    sharers: set[int] = field(default_factory=set)
    owner: Optional[int] = None
    txn: Optional[_Txn] = None
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
    elif req.op is Op.EVICT:
        if req.value is not None:
            raise InvalidRequestError(
                "evict 请求不能带 value（M 行的最新数据由 FLUSH 自动回写）",
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
    """

    def __init__(self, memory: Optional[dict[int, int]] = None) -> None:
        if memory is not None and not isinstance(memory, dict):
            raise InvalidRequestError("memory 必须是 dict[int, int]",
                                      field="memory")
        mem = [0] * NUM_ADDRS
        if memory:
            for addr, value in memory.items():
                _check_addr(addr)
                _check_int("memory value", value)
                mem[addr] = value
        self._memory: list[int] = mem
        self._dir: list[_DirEntry] = [_DirEntry() for _ in range(NUM_ADDRS)]
        self._caches: list[dict[int, _CacheLine]] = [
            {a: _CacheLine() for a in range(NUM_ADDRS)} for _ in range(NUM_CORES)
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

    def pending(self) -> tuple[Message, ...]:
        """当前待发（待投递）消息，按 msg_id 升序。"""
        return tuple(self._pending[i] for i in sorted(self._pending))

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
                bool(e.txn.phase == "flush") if e.txn is not None else False,
                tuple(sorted(e.txn.awaiting_acks)) if e.txn is not None else (),
                e.txn.phase if e.txn is not None else "",
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

    def evict(self, core: int, addr: int, *,
              txn_id: Optional[int] = None) -> RequestReport:
        """显式逐出该核该地址的缓存行（上层模拟有限缓存容量的替换）。

        与 load/store 走同一套准入与同地址 FIFO，返回的也是待完成句柄：
        - 行在 I：无操作，调用内同步完成，不产生任何消息；
        - 行在 S：核端立即自行置 I（只改自己的缓存），随后必须显式投递
          EVICT 与 EVICT_ACK：目录在 EVICT 生效时才把该核移出共享者集合，
          EVICT_ACK 到达后事务才完成；
        - 行在 M：缓存行与目录都保持 M，owner 先发携带最新数据的 FLUSH；
          FLUSH 入账后目录回 EVICT_ACK，ACK 到达时缓存行与目录才一起转 I。
          写回消息未到时不会报告完成，数据未交回目录前该行不会转 I。
        """
        req = _validate_request(
            Request(core, addr, Op.EVICT, None, txn_id), request_seq=0)
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

        if req.op is Op.EVICT:
            if line.state is State.INVALID:
                # I 行逐出：无操作，不产生任何消息，调用内同步完成。
                self._finish(req, 0, done)
                return done
            if line.state is State.SHARED:
                # S 行（干净）逐出：核端此刻只把自己置 I，不触碰任何他核；
                # 目录共享者身份的移除要等 EVICT 通知被显式投递。
                line.state = State.INVALID
                line.value = None
                entry.txn = _Txn(req.txn_id, core, Op.EVICT, None,
                                 phase="notice")
                self._send(MsgType.EVICT, core, DIR, addr, req.txn_id)
                return done
            # M 行（脏）逐出：owner 先把最新数据随 FLUSH 交回目录，但在
            # EVICT_ACK 回来之前该行仍是 M（目录也仍登记 owner）——数据没被
            # 目录确认之前不允许转 I；写回消息在途期间绝不报告逐出完成。
            assert entry.state is State.MODIFIED and entry.owner == core
            dirty = line.value
            assert dirty is not None
            entry.txn = _Txn(req.txn_id, core, Op.EVICT, None, phase="flush")
            self._send(MsgType.FLUSH, core, DIR, addr, req.txn_id, dirty)
            return done

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
                             phase="flush", flush_owner=owner)
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
                             phase="inv", awaiting_acks=set(targets))
            for c in sorted(targets):
                self._send(MsgType.INV, DIR, c, addr, req.txn_id)
            return done
        # 目录 M、owner 为别的核：先回写，再失效旧 owner，最后升级。
        owner = entry.owner
        assert owner is not None
        entry.txn = _Txn(req.txn_id, core, Op.STORE, req.value,
                         phase="flush", flush_owner=owner)
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
        msg_id = self._next_msg
        self._next_msg += 1
        self._pending[msg_id] = Message(
            msg_id, txn_id, mtype, src, dst, addr, value)
        return msg_id

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
        mtype = msg.mtype
        if mtype in (MsgType.FETCH, MsgType.INV, MsgType.EVICT_ACK):
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
            # 陈旧性判定先于任何状态修改：FETCH 必须对应处于 flush 阶段的
            # 当前事务（读 M / 写 M 的回写请求），且接收方正是记录的旧 owner。
            # M 行逐出不经 FETCH（owner 主动发 FLUSH），故阶段不符即陈旧。
            if (txn is None or txn.txn_id != msg.txn_id
                    or txn.phase != "flush" or txn.op is Op.EVICT
                    or txn.flush_owner != msg.dst):
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
            # INV 必须对应处于 inv 阶段的当前事务且接收方在等待集合中。
            if (txn is None or txn.txn_id != msg.txn_id
                    or txn.phase != "inv"
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
            # FLUSH 只在 flush 阶段被接受，且必须来自该阶段记录的回写核：
            # 读/写 M 时是旧 owner（flush_owner）；M 行逐出时是逐出核本身。
            if txn is None or txn.txn_id != msg.txn_id or txn.phase != "flush":
                return (f"FLUSH 属于事务 {msg.txn_id}，与地址 {msg.addr} "
                        f"当前活动事务"
                        f"（{txn.txn_id if txn else None}）不符，不推进新事务")
            writer = txn.flush_owner if txn.op is not Op.EVICT else txn.core
            if msg.src != writer:
                return (f"FLUSH 来自核 {msg.src}，事务 {msg.txn_id} 的回写核 "
                        f"应为核 {writer}，不推进事务")
            if txn.op is Op.EVICT:
                # M 行逐出的回写此刻入账：内存更新，但在 EVICT_ACK 送达前
                # 目录保持 M、owner 仍登记该核，缓存行也仍为 M——该行尚未
                # 被允许转 I（dir M + 内存已新 是协议允许的过渡状态）。
                self._memory[msg.addr] = msg.value
                txn.phase = "ack"
                self._send(MsgType.EVICT_ACK, DIR, txn.core, msg.addr,
                           msg.txn_id)
                return None
            self._memory[msg.addr] = msg.value
            old_owner = txn.flush_owner
            entry.owner = None
            entry.state = State.SHARED
            entry.sharers = {old_owner}
            txn.phase = "inv"
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

        if msg.mtype is MsgType.EVICT:
            # S 行逐出通知：只在 notice 阶段、且来自逐出核本人时生效。
            # 目录在此刻才把它移出共享者集合——这是本事务中目录的唯一变更，
            # 不触碰任何其他核。
            if (txn is None or txn.txn_id != msg.txn_id
                    or txn.phase != "notice" or txn.core != msg.src):
                return (f"陈旧 EVICT：来自核 {msg.src}、属于事务 {msg.txn_id}，"
                        f"地址 {msg.addr} 当前活动事务为 "
                        f"{txn.txn_id if txn else None}，不推进新事务")
            if entry.state is State.SHARED:
                entry.sharers.discard(msg.src)
                if not entry.sharers:
                    entry.state = State.INVALID
            txn.phase = "ack"
            self._send(MsgType.EVICT_ACK, DIR, msg.src, msg.addr, msg.txn_id)
            return None

        if msg.mtype is MsgType.EVICT_ACK:
            # 逐出事务的最后一步：目录受理凭证送回逐出核，旧 ACK（事务已结束、
            # 或阶段已越过 ack）一律判陈旧，不能完成后来重新获得的所有权。
            if (txn is None or txn.txn_id != msg.txn_id
                    or txn.phase != "ack" or txn.op is not Op.EVICT
                    or txn.core != msg.dst):
                return (f"陈旧 EVICT_ACK：指向核 {msg.dst}、属于事务 "
                        f"{msg.txn_id}，地址 {msg.addr} 当前活动事务为 "
                        f"{txn.txn_id if txn else None}，不推进新事务")
            evicted_line = self._caches[msg.dst][msg.addr]
            if evicted_line.state is State.MODIFIED:
                # M 行逐出：数据已在 ack 阶段前由 FLUSH 入账，此刻才允许
                # 把缓存行与目录一起转 I（owner 清空）。
                if entry.state is not State.MODIFIED or entry.owner != msg.dst:
                    raise InvalidMessageError(
                        f"EVICT_ACK 到达时核 {msg.dst} 的 M 行与目录状态 "
                        f"（{entry.state.value}/owner={entry.owner}）不一致",
                        msg_id=msg.msg_id)
                evicted_line.state = State.INVALID
                evicted_line.value = None
                entry.state = State.INVALID
                entry.owner = None
                entry.sharers.clear()
            elif evicted_line.state is not State.INVALID:
                raise InvalidMessageError(
                    f"EVICT_ACK 到达时核 {msg.dst} 对地址 {msg.addr} "
                    f"既不是 I 也不是待逐出的 M（状态 "
                    f"{evicted_line.state.value}）", msg_id=msg.msg_id)
            entry.txn = None
            self._finish(Request(txn.core, msg.addr, Op.EVICT, None,
                                 txn.txn_id), 0, done)
            return None

        # INV_ACK
        if (txn is None or txn.txn_id != msg.txn_id
                or txn.phase != "inv" or msg.src not in txn.awaiting_acks):
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
