"""消息驱动的目录式 MSI 缓存一致性内核（3 核 / 8 地址）。

这是纯函数式步进的模拟：没有线程、没有网络、没有时钟。输入只有两类：

- :meth:`CoherenceKernel.request`：某核发起 load/store（产生 GETS/GETX/UPGR）；
- :meth:`CoherenceKernel.deliver`：显式投递一条网络消息（可按任意顺序）。

每次处理把新产生的消息追加到“待发消息”队列并返回，调用方决定下一条投递谁，
因此所有消息重排都可以从外部精确驱动。

目录按地址维护 ``I/S/M``、共享者集合与 owner；同地址请求严格排队，
不同地址的事务彼此独立。升级必须收齐所有失效 ACK；读 M 地址必须先让
owner 回写（FLUSH）再共享。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterable

from .errors import KernelError, require_int
from .messages import (
    DIRECTORY,
    NUM_ADDR,
    NUM_CORES,
    Message,
    MsgType,
    make_request,
)

I_STATE, S_STATE, M_STATE = "I", "S", "M"


def _key(m: Message) -> tuple:
    return (m.mtype, m.xid, m.src, m.dst, m.addr, m.value, m.aid)


@dataclass(frozen=True, slots=True)
class CacheLineView:
    state: str
    value: int | None


@dataclass(frozen=True, slots=True)
class DirLineView:
    state: str
    sharers: frozenset[int]
    owner: int | None
    value: int | None


@dataclass(frozen=True, slots=True)
class CompletedOp:
    kind: str          # 'LOAD' / 'STORE'
    core: int
    addr: int
    xid: int
    value: int | None  # LOAD 的完成读值；STORE 为 None


@dataclass(frozen=True, slots=True)
class KernelSnapshot:
    caches: tuple[tuple[CacheLineView, ...], ...]      # [core][addr]
    directory: tuple[DirLineView, ...]                 # [addr]
    pending_messages: tuple[Message, ...]
    completed: tuple[CompletedOp, ...]
    queued_xids: tuple[tuple[int, ...], ...]           # [addr]


class _Txn:
    __slots__ = (
        "xid", "kind", "requester", "addr", "write_value",
        "waiting_flush", "flush_seen", "flush_value",
        "inv", "acked_aids", "waiting_grant", "grant_value",
    )

    def __init__(
        self, xid: int, kind: MsgType, requester: int, addr: int,
        write_value: int | None,
    ) -> None:
        self.xid = xid
        self.kind = kind
        self.requester = requester
        self.addr = addr
        self.write_value = write_value  # GETX/UPGR 的写入值；GETS 为 None
        self.waiting_flush = False
        self.flush_seen = False
        self.flush_value: int | None = None
        self.inv: dict[int, int] = {}        # aid -> 被失效核
        self.acked_aids: set[int] = set()
        self.waiting_grant = False
        self.grant_value: int | None = None

    def acks_complete(self) -> bool:
        return len(self.acked_aids) >= len(self.inv)


class CoherenceKernel:
    """目录式 MSI 一致性内核。

    构造参数:
        memory: 长度 8 的初始内存（整数）。默认全 0。
    """

    def __init__(self, memory: Iterable[int] | None = None) -> None:
        if memory is None:
            mem = [0] * NUM_ADDR
        else:
            mem = list(memory)
            if len(mem) != NUM_ADDR:
                raise KernelError(
                    f"memory: must have exactly {NUM_ADDR} integers, got {len(mem)}"
                )
        for i, v in enumerate(mem):
            if isinstance(v, bool):
                raise KernelError(f"memory[{i}]: integer value must not be bool")
            if not isinstance(v, int):
                raise KernelError(f"memory[{i}]: integer required, got {type(v).__name__}")

        # [core][addr] -> [state, value]
        self._caches: list[list[list]] = [
            [[I_STATE, None] for _ in range(NUM_ADDR)] for _ in range(NUM_CORES)
        ]
        self._dir_state: list[str] = [I_STATE] * NUM_ADDR
        self._sharers: list[set[int]] = [set() for _ in range(NUM_ADDR)]
        self._owner: list[int | None] = [None] * NUM_ADDR
        self._mem: list[int] = list(mem)

        self._aid_next: list[int] = [0] * NUM_ADDR
        self._queues: list[deque[int]] = [deque() for _ in range(NUM_ADDR)]
        self._txns: dict[int, _Txn] = {}
        # 同一（核, 地址）同时只允许一个未完成事务；不同地址独立，
        # 因此同一核可对不同地址各有一个开放事务。
        self._busy: dict[tuple[int, int], int] = {}

        self._outgoing: list[Message] = []
        self._delivered: set[tuple] = set()
        self._used_xids: set[int] = set()
        self._completed: list[CompletedOp] = []

    # ------------------------------------------------------------------ 输入

    def request(
        self, kind: str, xid: int, core: int, addr: int, value: int | None = None
    ) -> list[Message]:
        """核 ``core`` 发起一个 load/store 请求。

        - ``GETS``：load 缺失（缓存必须为 I），``value`` 必须省略；
        - ``GETX``：写缺失（缓存必须为 I），``value`` 为整数写入值；
        - ``UPGR``：S -> M 升级（缓存必须为 S），``value`` 为整数写入值。

        也可使用便捷方法 :meth:`load` / :meth:`store`。
        成功后返回本次新产生的待发消息。同一（核, 地址）同时只允许一个
        未完成事务；同一核对不同地址可以各有一个（地址独立）。
        """
        msg = make_request(kind, xid, core, addr, value)
        return self._handle_request(msg)

    def load(self, xid: int, core: int, addr: int) -> list[Message]:
        """发起 GETS（仅在缓存为 I 时；S/M 命中请用 :meth:`local_load`）。"""
        return self.request("GETS", xid, core, addr)

    def store(self, xid: int, core: int, addr: int, value: int) -> list[Message]:
        """缓存为 S 时发 UPGR、为 I 时发 GETX；M 命中请用 :meth:`local_store`。"""
        st = self._caches[core][addr][0]
        kind = "UPGR" if st == S_STATE else "GETX"
        return self.request(kind, xid, core, addr, value)

    def local_load(self, core: int, addr: int, xid: int = -1) -> CompletedOp:
        """S/M 本地读命中，记录并返回完成的 LOAD（不产生任何消息）。"""
        require_int("core", core, lo=0, hi=NUM_CORES - 1)
        require_int("addr", addr, lo=0, hi=NUM_ADDR - 1)
        st, val = self._caches[core][addr]
        if st == I_STATE:
            raise KernelError(f"local_load: core {core} a{addr} is I; issue a GETS request")
        op = CompletedOp("LOAD", core, addr, xid, val)
        self._completed.append(op)
        return op

    def local_store(self, core: int, addr: int, value: int, xid: int = -1) -> CompletedOp:
        """M 本地写命中：直接更新缓存值，记录并返回完成的 STORE（不产生消息）。"""
        require_int("core", core, lo=0, hi=NUM_CORES - 1)
        require_int("addr", addr, lo=0, hi=NUM_ADDR - 1)
        require_int("value", value)
        if self._caches[core][addr][0] != M_STATE:
            raise KernelError(f"local_store: core {core} a{addr} is not M")
        self._caches[core][addr][1] = value
        op = CompletedOp("STORE", core, addr, xid, None)
        self._completed.append(op)
        return op

    def deliver(self, message: Message | int) -> list[Message]:
        """显式投递一条网络消息。

        ``message`` 可以是 :class:`~msi_kernel.messages.Message`，也可以是
        只增的“已发出消息日志” ``_outgoing`` 中的下标（``int``；已投递的下标
        再次使用同样按幂等空操作处理）。返回本次投递新触发的消息列表。

        幂等：``(type, src, dst, xid, value, aid)`` 完全相同的消息重复投递为空
        操作（返回 ``[]``）。携带陈旧 ``xid``/``aid`` 的消息不属于幂等重复，
        抛出 :class:`~msi_kernel.errors.KernelError`，且不推进任何事务。
        """
        if isinstance(message, int) and not isinstance(message, bool):
            idx = require_int("message_index", message, lo=0)
            if idx >= len(self._outgoing):
                raise KernelError(
                    f"message_index: {idx} out of range (have {len(self._outgoing)})"
                )
            msg = self._outgoing[idx]
        elif isinstance(message, Message):
            msg = message
        else:
            raise KernelError(
                f"message: Message or pending index required, got {type(message).__name__}"
            )
        return self._deliver_one(msg)

    # ------------------------------------------------------------- 快照/克隆

    def snapshot(self) -> KernelSnapshot:
        """返回不可变状态快照（缓存、目录、待发消息、完成记录、队列）。"""
        caches = tuple(
            tuple(CacheLineView(s, v) for s, v in core) for core in self._caches
        )
        directory = tuple(
            DirLineView(
                self._dir_state[a],
                frozenset(self._sharers[a]),
                self._owner[a],
                self._mem[a],
            )
            for a in range(NUM_ADDR)
        )
        pending = tuple(m for m in self._outgoing if _key(m) not in self._delivered)
        completed = tuple(self._completed)
        queues = tuple(tuple(q) for q in self._queues)
        return KernelSnapshot(caches, directory, pending, completed, queues)

    def clone(self) -> "CoherenceKernel":
        """深拷贝（消息重排枚举用它做分支，不做任何真实并发）。"""
        k = object.__new__(CoherenceKernel)
        k._caches = [[list(line) for line in core] for core in self._caches]
        k._dir_state = list(self._dir_state)
        k._sharers = [set(s) for s in self._sharers]
        k._owner = list(self._owner)
        k._mem = list(self._mem)
        k._aid_next = list(self._aid_next)
        k._queues = [deque(q) for q in self._queues]
        k._busy = dict(self._busy)
        k._txns = {}
        for xid, t in self._txns.items():
            nt = object.__new__(_Txn)
            nt.xid = t.xid
            nt.kind = t.kind
            nt.requester = t.requester
            nt.addr = t.addr
            nt.write_value = t.write_value
            nt.waiting_flush = t.waiting_flush
            nt.flush_seen = t.flush_seen
            nt.flush_value = t.flush_value
            nt.inv = dict(t.inv)
            nt.acked_aids = set(t.acked_aids)
            nt.waiting_grant = t.waiting_grant
            nt.grant_value = t.grant_value
            k._txns[xid] = nt
        # Message 不可变，列表与集合浅拷贝即可。
        k._outgoing = list(self._outgoing)
        k._delivered = set(self._delivered)
        k._used_xids = set(self._used_xids)
        k._completed = list(self._completed)
        return k

    def pending_messages(self) -> list[Message]:
        return [m for m in self._outgoing if _key(m) not in self._delivered]

    def is_quiescent(self) -> bool:
        """所有请求都已完成且网络中没有待投递消息。"""
        return not self._txns and not self.pending_messages()

    # ------------------------------------------------------------ 一致性断言

    def check_invariants(self) -> None:
        """检查全局一致性不变量；违例抛 ``AssertionError``。

        缓存级性质（任何消息在途窗口都成立）：每个地址至多一个核为 M，
        且 M 不与任何 S 共存。目录级性质按队首事务的具体阶段精确判定，
        允许两类单消息在途瞬态：FETCH 已送达而 FLUSH 未到（owner 已降级、
        目录仍记 M），授权已送达而 GRANT_ACK 未到（请求核可领先目录一拍）。
        """
        for a in range(NUM_ADDR):
            dstate = self._dir_state[a]
            m_cores = {c for c in range(NUM_CORES) if self._caches[c][a][0] == M_STATE}
            s_cores = {c for c in range(NUM_CORES) if self._caches[c][a][0] == S_STATE}
            assert len(m_cores) <= 1, f"a{a}: multiple M cores {m_cores}"
            assert not (m_cores & s_cores), f"a{a}: M {m_cores} coexists with S {s_cores}"

            q = self._queues[a]
            if not q:
                self._check_idle(a, dstate, m_cores, s_cores)
            else:
                self._check_head(a, self._txns[q[0]], m_cores, s_cores)
                head = self._txns[q[0]]
                for xid in list(q)[1:]:
                    tq = self._txns[xid]
                    st = self._caches[tq.requester][a][0]
                    if tq.kind is MsgType.UPGR:
                        # 入队时为 S；等待期间可能已被（含已结束的）前序写事务
                        # 失效而为 I——轮到它时 _start 会改写为 GETX。
                        assert st in (S_STATE, I_STATE), (
                            f"a{a}: queued UPGR c{tq.requester} is {st}, expected S/I"
                        )
                    else:
                        # GETX/GETS 请求者起始为 I；GETX 可能由排队 UPGR 在
                        # 队首发出 INV 时转写而来，此时失效可能尚未处理（仍 S）。
                        if st == S_STATE:
                            assert tq.kind is MsgType.GETX
                            assert tq.requester in set(head.inv.values()), (
                                f"a{a}: queued GETX c{tq.requester} S "
                                f"without being an invalidation target"
                            )
                        else:
                            assert st == I_STATE, (
                                f"a{a}: queued {tq.kind.value} c{tq.requester} "
                                f"is {st}, expected I"
                            )

            for open_xid in self._txns:
                tt = self._txns[open_xid]
                assert self._busy[(tt.requester, tt.addr)] == open_xid

    def _check_idle(self, a: int, dstate: str, m_cores: set[int], s_cores: set[int]) -> None:
        if dstate == I_STATE:
            assert self._owner[a] is None and not self._sharers[a]
            assert not m_cores and not s_cores, f"a{a}: dir I with caches M={m_cores} S={s_cores}"
        elif dstate == S_STATE:
            assert self._owner[a] is None
            assert self._sharers[a] == s_cores, f"a{a}: dir S sharers {self._sharers[a]} != {s_cores}"
            for c in s_cores:
                assert self._caches[c][a][1] == self._mem[a], f"a{a}: stale S value at c{c}"
        else:
            o = self._owner[a]
            assert o is not None, f"a{a}: dir M without owner"
            assert m_cores == {o} and not s_cores and not self._sharers[a], (
                f"a{a}: dir M owner c{o}, caches M={m_cores} S={s_cores}"
            )

    def _check_head(self, a: int, t: "_Txn", m_cores: set[int], s_cores: set[int]) -> None:
        r = t.requester
        rs, rv = self._caches[r][a]
        others = {c for c in range(NUM_CORES) if c != r}

        if t.waiting_flush and not t.flush_seen:
            # FETCH/FETCHX 已发、FLUSH 未到目录。owner 可能尚未收到（仍 M），
            # 也可能已收到并降级（GETS: S；GETX: I），FLUSH 在途。
            assert self._dir_state[a] == M_STATE
            o = self._owner[a]
            assert o is not None and o != r
            ost = self._caches[o][a][0]
            if t.kind is MsgType.GETS:
                assert ost in (M_STATE, S_STATE), f"a{a}: owner c{o} state {ost}"
            else:
                assert ost in (M_STATE, I_STATE), f"a{a}: owner c{o} state {ost}"
            assert rs == I_STATE and m_cores <= {o}
            assert s_cores <= ({o} if t.kind is MsgType.GETS else set())
            for c in others - {o}:
                assert self._caches[c][a][0] == I_STATE, f"a{a}: c{c} non-I during fetch"
            return

        unacked = {c for aid, c in t.inv.items() if aid not in t.acked_aids}
        acked = {c for aid, c in t.inv.items() if aid in t.acked_aids}

        if t.waiting_grant:
            # 请求核可能尚未收到授权（原状态），也可能已收到并领先目录一拍。
            if t.kind is MsgType.GETS:
                assert rs in (I_STATE, S_STATE)
                assert self._dir_state[a] in (I_STATE, S_STATE)
                expected_sharers = set(self._sharers[a])
                assert s_cores == expected_sharers | ({r} if rs == S_STATE else set())
                assert m_cores == set()
                for c in s_cores:
                    assert self._caches[c][a][1] == t.grant_value, (
                        f"a{a}: c{c} S value != granted value"
                    )
            else:
                assert t.kind in (MsgType.GETX, MsgType.UPGR)
                if t.kind is MsgType.GETX:
                    assert rs in (I_STATE, M_STATE)
                else:
                    assert rs in (S_STATE, M_STATE)
                assert m_cores <= {r}
                if t.flush_seen or self._dir_state[a] == I_STATE:
                    # flush 后或目录原本为 I 的直授权路径
                    assert self._dir_state[a] == I_STATE
                    assert not self._sharers[a] and self._owner[a] is None
                    assert s_cores == set()
                else:
                    assert self._dir_state[a] == S_STATE
                    # sharers 随 ACK 逐步移除；GETX 请求者起始为 I 不在 sharers，
                    # UPGR 请求者仍在 sharers，授权后可瞬态领先目录为 M。
                    expect_sharers = unacked | ({r} if t.kind is MsgType.UPGR else set())
                    assert self._sharers[a] == expect_sharers
                    assert s_cores == unacked | (({r} if rs == S_STATE else set()))
                    assert self._owner[a] is None
            # 未 ACK 的核可能尚未收到 INV（仍 S），也可能已作废而 ACK 在途（I）。
            for c in unacked:
                assert self._caches[c][a][0] in (S_STATE, I_STATE), (
                    f"a{a}: unacked c{c} is {self._caches[c][a][0]}"
                )
            for c in acked:
                assert self._caches[c][a][0] == I_STATE
            return

        # 失效收集阶段（GETX-from-S / UPGR）：目录仍为 S。
        assert t.inv and not t.waiting_grant
        assert self._dir_state[a] == S_STATE and self._owner[a] is None
        expect_sharers = unacked | ({r} if t.kind is MsgType.UPGR else set())
        assert self._sharers[a] == expect_sharers
        # 未 ACK 的核可能尚未收到 INV（仍 S），也可能已作废而 ACK 在途（I）。
        expect_s = unacked | ({r} if t.kind is MsgType.UPGR else set())
        assert s_cores <= expect_s
        if t.kind is MsgType.UPGR:
            assert rs == S_STATE and rv == self._mem[a]
            assert r in s_cores
        else:
            assert rs == I_STATE
        assert not m_cores
        for c in acked:
            assert self._caches[c][a][0] == I_STATE
        for c in others - unacked - acked - {r}:
            assert self._caches[c][a][0] == I_STATE
        for c in unacked:
            assert self._caches[c][a][0] in (S_STATE, I_STATE)

    # ------------------------------------------------------------------ 内部

    def _emit(self, m: Message) -> None:
        self._outgoing.append(m)

    def _handle_request(self, msg: Message) -> list[Message]:
        key = _key(msg)
        if key in self._delivered:
            return []  # 完全相同的请求重投：幂等空操作
        xid, r, a = msg.xid, msg.src, msg.addr
        if xid in self._used_xids:
            raise KernelError(
                f"xid {xid}: already used by another transaction; "
                f"identical redelivery is idempotent, but this message differs"
            )
        if (r, a) in self._busy:
            raise KernelError(
                f"core {r} addr {a}: has outstanding transaction "
                f"xid={self._busy[(r, a)]}"
            )
        cs = self._caches[r][a][0]
        if msg.mtype is MsgType.GETS and cs != I_STATE:
            raise KernelError(f"GETS xid={xid}: core {r} a{a} state is {cs}, expected I")
        if msg.mtype is MsgType.GETX and cs != I_STATE:
            raise KernelError(f"GETX xid={xid}: core {r} a{a} state is {cs}, expected I")
        if msg.mtype is MsgType.UPGR and cs != S_STATE:
            raise KernelError(f"UPGR xid={xid}: core {r} a{a} state is {cs}, expected S")

        # 以下检查全部通过后才允许改变状态（失败不留部分变更）。
        self._used_xids.add(xid)
        self._delivered.add(key)
        self._busy[(r, a)] = xid
        t = _Txn(xid, msg.mtype, r, a, msg.value)
        self._txns[xid] = t
        self._queues[a].append(xid)

        before = len(self._outgoing)
        if len(self._queues[a]) == 1:
            self._start(t)
        return self._outgoing[before:]

    def _start(self, t: _Txn) -> None:
        a, r = t.addr, t.requester
        # 排队期间请求者可能已被队首写事务失效：UPGR 自动改写为 GETX。
        if t.kind is MsgType.UPGR and self._caches[r][a][0] == I_STATE:
            t.kind = MsgType.GETX
        dstate = self._dir_state[a]
        if t.kind is MsgType.GETS:
            if dstate in (I_STATE, S_STATE):
                self._grant_data(t, MsgType.DATA, self._mem[a])
            else:
                assert dstate == M_STATE
                o = self._owner[a]
                assert o is not None and o != r
                t.waiting_flush = True
                self._emit(Message(MsgType.FETCH, t.xid, DIRECTORY, o, a))
        elif t.kind is MsgType.GETX:
            if dstate == I_STATE:
                self._grant_data(t, MsgType.DATA_M, t.write_value)
            elif dstate == S_STATE:
                self._send_invs(t, set(self._sharers[a]))
                if t.acks_complete():
                    self._grant_data(t, MsgType.DATA_M, t.write_value)
            else:
                o = self._owner[a]
                assert o is not None and o != r
                t.waiting_flush = True
                self._emit(Message(MsgType.FETCHX, t.xid, DIRECTORY, o, a))
        else:  # UPGR
            assert dstate == S_STATE and r in self._sharers[a]
            self._send_invs(t, set(self._sharers[a]) - {r})
            if t.acks_complete():
                self._grant_upgrade(t)

    def _send_invs(self, t: _Txn, targets: set[int]) -> None:
        a = t.addr
        for c in sorted(targets):
            aid = self._aid_next[a]
            self._aid_next[a] += 1
            t.inv[aid] = c
            self._emit(Message(MsgType.INV, t.xid, DIRECTORY, c, a, aid=aid))
        # 被失效核若在同地址排队了 UPGR，失效 ACK 后它必为 I：
        # 目录在失效发出的同时把该排队升级改写为 GETX（真实目录常见做法）。
        for qxid in list(self._queues[a])[1:]:
            q = self._txns[qxid]
            if q.kind is MsgType.UPGR and q.requester in targets:
                q.kind = MsgType.GETX

    def _grant_data(self, t: _Txn, mtype: MsgType, value: int) -> None:
        t.waiting_grant = True
        t.grant_value = value
        self._emit(Message(mtype, t.xid, DIRECTORY, t.requester, t.addr, value=value))

    def _grant_upgrade(self, t: _Txn) -> None:
        t.waiting_grant = True
        self._emit(Message(MsgType.UPGR_ACK, t.xid, DIRECTORY, t.requester, t.addr))

    def _deliver_one(self, msg: Message) -> list[Message]:
        key = _key(msg)
        if key in self._delivered:
            return []
        before = len(self._outgoing)

        if msg.mtype in (MsgType.GETS, MsgType.GETX, MsgType.UPGR):
            self._handle_request(msg)
            return self._outgoing[before:]

        if msg.dst != DIRECTORY:
            self._deliver_at_core(msg)
        else:
            self._deliver_at_directory(msg)

        self._delivered.add(key)
        return self._outgoing[before:]

    # ----------------------------------------------------------- 核侧反应

    def _deliver_at_core(self, msg: Message) -> None:
        c, a = msg.dst, msg.addr
        st, val = self._caches[c][a]
        t = self._txns.get(msg.xid)
        if msg.mtype is MsgType.INV:
            if t is None or msg.aid not in t.inv or t.inv[msg.aid] != c:
                raise KernelError(
                    f"stale INV xid={msg.xid} aid={msg.aid} at c{c}: no matching invalidation"
                )
            if msg.aid in t.acked_aids:
                raise KernelError(f"INV aid={msg.aid} already acked (non-duplicate message)")
            if st != S_STATE:
                raise KernelError(
                    f"INV xid={msg.xid} aid={msg.aid}: c{c} a{a} is {st}, expected S"
                )
            self._caches[c][a] = [I_STATE, None]
            self._emit(Message(MsgType.INV_ACK, msg.xid, c, DIRECTORY, a, aid=msg.aid))
        elif msg.mtype is MsgType.FETCH:
            self._expect_owner_flush(msg, c, M_STATE)
            self._emit(Message(MsgType.FLUSH, msg.xid, c, DIRECTORY, a, value=val))
            self._caches[c][a] = [S_STATE, val]
        elif msg.mtype is MsgType.FETCHX:
            self._expect_owner_flush(msg, c, M_STATE)
            self._emit(Message(MsgType.FLUSH, msg.xid, c, DIRECTORY, a, value=val))
            self._caches[c][a] = [I_STATE, None]
        elif msg.mtype is MsgType.DATA:
            self._expect_grant(msg, c)
            if st != I_STATE:
                raise KernelError(f"DATA xid={msg.xid}: c{c} a{a} is {st}, expected I")
            self._caches[c][a] = [S_STATE, msg.value]
            self._emit(Message(MsgType.GRANT_ACK, msg.xid, c, DIRECTORY, a))
        elif msg.mtype is MsgType.DATA_M:
            self._expect_grant(msg, c)
            if st != I_STATE:
                raise KernelError(f"DATA_M xid={msg.xid}: c{c} a{a} is {st}, expected I")
            self._caches[c][a] = [M_STATE, msg.value]
            self._emit(Message(MsgType.GRANT_ACK, msg.xid, c, DIRECTORY, a))
        elif msg.mtype is MsgType.UPGR_ACK:
            self._expect_grant(msg, c)
            if st != S_STATE:
                raise KernelError(f"UPGR_ACK xid={msg.xid}: c{c} a{a} is {st}, expected S")
            # 升级许可到达：以自己的写入值进入 M。
            self._caches[c][a] = [M_STATE, t.write_value]
            self._emit(Message(MsgType.GRANT_ACK, msg.xid, c, DIRECTORY, a))
        else:
            raise KernelError(f"{msg.mtype.value}: not deliverable to a core")

    def _expect_owner_flush(self, msg: Message, c: int, need: str) -> None:
        t = self._txns.get(msg.xid)
        if t is None or not t.waiting_flush:
            raise KernelError(f"stale {msg.mtype.value} xid={msg.xid}: no flush transaction")
        a = msg.addr
        if self._owner[a] != c:
            raise KernelError(f"{msg.mtype.value} xid={msg.xid}: c{c} is not owner of a{a}")
        st = self._caches[c][a][0]
        if st != need:
            raise KernelError(f"{msg.mtype.value} xid={msg.xid}: c{c} a{a} is {st}, expected {need}")

    def _expect_grant(self, msg: Message, c: int) -> None:
        t = self._txns.get(msg.xid)
        if t is None or not t.waiting_grant or t.requester != c:
            raise KernelError(f"stale {msg.mtype.value} xid={msg.xid} at c{c}: no open grant")
        if t.addr != msg.addr:
            raise KernelError(f"{msg.mtype.value} xid={msg.xid}: addr mismatch")
        if msg.mtype in (MsgType.DATA, MsgType.DATA_M) and msg.value != t.grant_value:
            raise KernelError(
                f"{msg.mtype.value} xid={msg.xid}: value {msg.value} != granted {t.grant_value}"
            )

    # ----------------------------------------------------------- 目录侧反应

    def _deliver_at_directory(self, msg: Message) -> None:
        t = self._txns.get(msg.xid)
        if t is None:
            raise KernelError(
                f"stale {msg.mtype.value} xid={msg.xid} at directory: unknown transaction"
            )
        if self._queues[t.addr][0] != t.xid:
            raise KernelError(
                f"{msg.mtype.value} xid={msg.xid}: transaction queued behind "
                f"xid={self._queues[t.addr][0]} on a{t.addr}"
            )
        if msg.mtype is MsgType.FLUSH:
            self._on_flush(t, msg)
        elif msg.mtype is MsgType.INV_ACK:
            self._on_inv_ack(t, msg)
        elif msg.mtype is MsgType.GRANT_ACK:
            self._on_grant_ack(t, msg)
        else:
            raise KernelError(f"{msg.mtype.value}: not deliverable to the directory")

    def _on_flush(self, t: _Txn, msg: Message) -> None:
        a = msg.addr
        if not t.waiting_flush or t.flush_seen:
            raise KernelError(f"FLUSH xid={t.xid}: unexpected duplicate/non-matching flush")
        if self._owner[a] != msg.src:
            raise KernelError(f"FLUSH xid={t.xid}: src c{msg.src} is not owner")
        t.flush_seen = True
        t.flush_value = msg.value
        self._mem[a] = msg.value
        if t.kind is MsgType.GETS:
            # owner 回写后降 S，与目录共享；数据再发给请求者。
            self._dir_state[a] = S_STATE
            self._owner[a] = None
            self._sharers[a] = {msg.src}
            self._grant_data(t, MsgType.DATA, msg.value)
        else:
            assert t.kind is MsgType.GETX
            self._dir_state[a] = I_STATE
            self._owner[a] = None
            self._sharers[a] = set()
            self._grant_data(t, MsgType.DATA_M, t.write_value)

    def _on_inv_ack(self, t: _Txn, msg: Message) -> None:
        aid = msg.aid
        if aid not in t.inv:
            raise KernelError(
                f"stale INV_ACK xid={t.xid} aid={aid}: no such invalidation in this transaction"
            )
        if t.inv[aid] != msg.src:
            raise KernelError(f"INV_ACK xid={t.xid} aid={aid}: ack from c{msg.src}, expected c{t.inv[aid]}")
        if aid in t.acked_aids:
            raise KernelError(f"INV_ACK xid={t.xid} aid={aid}: conflicting re-ack (duplicate delivery is idempotent)")
        a = t.addr
        self._sharers[a].discard(msg.src)
        t.acked_aids.add(aid)
        if t.acks_complete() and not t.waiting_grant:
            if t.kind is MsgType.GETX:
                self._grant_data(t, MsgType.DATA_M, t.write_value)
            else:
                self._grant_upgrade(t)

    def _on_grant_ack(self, t: _Txn, msg: Message) -> None:
        a = t.addr
        r = t.requester
        if not t.waiting_grant:
            raise KernelError(f"GRANT_ACK xid={t.xid}: no grant in flight")
        if msg.src != r:
            raise KernelError(f"GRANT_ACK xid={t.xid}: from c{msg.src}, expected c{r}")
        rstate, rval = self._caches[r][a]

        if t.kind is MsgType.GETS:
            assert rstate == S_STATE
            self._dir_state[a] = S_STATE
            self._sharers[a].add(r)
            self._owner[a] = None
            done_value = rval
            kind = "LOAD"
        elif t.kind is MsgType.GETX:
            assert rstate == M_STATE
            self._dir_state[a] = M_STATE
            self._sharers[a] = set()
            self._owner[a] = r
            done_value = None
            kind = "STORE"
        else:  # UPGR
            assert rstate == M_STATE
            self._dir_state[a] = M_STATE
            self._sharers[a] = set()
            self._owner[a] = r
            done_value = None
            kind = "STORE"

        self._completed.append(CompletedOp(kind, r, a, t.xid, done_value))
        self._close(t)

    def _close(self, t: _Txn) -> None:
        a = t.addr
        q = self._queues[a]
        assert q and q[0] == t.xid
        q.popleft()
        del self._txns[t.xid]
        self._busy.pop((t.requester, t.addr), None)
        if q:
            self._start(self._txns[q[0]])

    # ------------------------------------------------------------- 重放指纹

    def canonical(self) -> tuple:
        """与观察等价的结构化状态指纹（重排枚举去重用）。

        包含缓存、目录、开放事务阶段、已投递集合与完成记录；xid/aid 按本会话
        实际编码（枚举在同一内核实例的克隆上进行，编号确定）。
        """
        parts: list = []
        for core in self._caches:
            parts.append(tuple((s, v) for s, v in core))
        for a in range(NUM_ADDR):
            parts.append((
                self._dir_state[a],
                tuple(sorted(self._sharers[a])),
                self._owner[a],
                self._mem[a],
                self._aid_next[a],
                tuple(self._queues[a]),
            ))
        for xid in sorted(self._txns):
            t = self._txns[xid]
            parts.append((
                t.kind, t.requester, t.addr,
                t.waiting_flush, t.flush_seen, t.flush_value,
                tuple(sorted(t.inv.items())),
                tuple(sorted(t.acked_aids)),
                t.waiting_grant, t.grant_value, t.write_value,
            ))
        parts.append(tuple(sorted(self._busy.items())))
        parts.append(tuple(sorted(self._delivered)))
        parts.append(tuple(sorted(_key(m) for m in self.pending_messages())))
        parts.append(tuple((c.kind, c.core, c.addr, c.xid, c.value)
                           for c in self._completed))
        return tuple(parts)
