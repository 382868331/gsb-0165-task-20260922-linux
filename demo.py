"""固定输入演示：目录式 MSI 消息内核。

运行：python demo.py（约 1-2 秒，纯离线真实计算，无 sleep、无预录结果）。

内容：
1. 正常结果：手工驱动一条完整“失效链”（共享 -> 升级失效 -> owner 回写 ->
   再升级），逐步打印消息；随后对“两个同时升级 + 读写交错”的小程序穷举
   全部消息重排，打印状态数、终态数与每条读操作的允许值集合；
2. 异常边界：实际触发并捕获一个陈旧 ACK 拒绝和一个 NaN 输入拒绝。
"""

from __future__ import annotations

import time

from msi_kernel import CoherenceKernel, KernelError, Message, MsgType
from msi_kernel.reorder import RequestSpec, explore


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def drain_step(kernel: CoherenceKernel, trace: list[str], *, only: set | None = None) -> None:
    """投递所有可投递消息（按 FIFO），把消息文本记入 trace。"""
    while True:
        pending = [m for m in kernel.pending_messages() if only is None or m.mtype in only]
        if not pending:
            return
        m = pending[0]
        new = kernel.deliver(m)
        trace.append(f"  deliver {m}")
        for nm in new:
            trace.append(f"      -> new pending: {nm}")


def print_snapshot(kernel: CoherenceKernel) -> None:
    snap = kernel.snapshot()
    print("每核缓存（只列非 I 的行）:")
    for c, core in enumerate(snap.caches):
        for a, line in enumerate(core):
            if line.state != "I":
                print(f"  core{c} addr{a}: {line.state} value={line.value}")
    print("目录:")
    for a, d in enumerate(snap.directory):
        if d.state != "I":
            sh = "{" + ",".join(f"c{c}" for c in sorted(d.sharers)) + "}"
            own = f"c{d.owner}" if d.owner is not None else "-"
            print(f"  addr{a}: {d.state} sharers={sh} owner={own} mem={d.value}")
    print("待发消息:", len(snap.pending_messages))
    for m in snap.pending_messages:
        print(f"  {m}")
    print("完成记录:")
    for op in snap.completed:
        if op.kind == "LOAD":
            print(f"  LOAD  core{op.core} addr{op.addr} xid={op.xid} -> value={op.value}")
        else:
            print(f"  STORE core{op.core} addr{op.addr} xid={op.xid} (write committed)")


def normal_demo() -> None:
    banner("正常结果：失效链（3 核 / 地址 0，初值 mem=0）")
    k = CoherenceKernel()
    trace: list[str] = []

    print("\n[1] core0 GETX 写入 10")
    for m in k.request("GETX", 1, 0, 0, 10):
        trace.append(f"  request -> {m}")
    drain_step(k, trace)

    print("[2] core1、core2 分别 GETS（读 M 地址：owner 回写后三方共享）")
    k.request("GETS", 2, 1, 0)
    drain_step(k, trace)
    k.request("GETS", 3, 2, 0)
    drain_step(k, trace)

    print("[3] core1 UPGR 写入 20：必须等 c0、c2 的全部失效 ACK")
    k.request("UPGR", 4, 1, 0, 20)
    drain_step(k, trace)

    print("[4] core2 被失效后重新 GETS：owner(c1) FETCH 回写 20 后共享")
    k.request("GETS", 5, 2, 0)
    drain_step(k, trace)

    print("[5] core2 UPGR 写入 30：失效 c1 后成为新 owner")
    k.request("UPGR", 6, 2, 0, 30)
    drain_step(k, trace)

    print("\n失效链消息流水（请求发出 -> 目录 -> 核 -> ACK）:")
    for line in trace:
        print(line)

    print()
    print_snapshot(k)
    assert k.is_quiescent()

    banner("正常结果：穷举消息重排（两个同时升级 + 读写交错）")
    program = [
        RequestSpec("STORE", 0, 0, 10),
        RequestSpec("LOAD", 1, 0),
        RequestSpec("LOAD", 2, 0),
        RequestSpec("STORE", 1, 0, 20),
        RequestSpec("LOAD", 2, 0),
        RequestSpec("STORE", 2, 0, 30),
        RequestSpec("LOAD", 0, 0),
    ]
    t0 = time.perf_counter()
    result = explore(program, budget=500_000)
    dt = time.perf_counter() - t0
    result.assert_safe()
    print(f"枚举完成（complete={result.complete}）：访问状态 {result.visited} 个，"
          f"终态 {result.terminals} 个，用时 {dt:.2f}s")
    print("所有重排均满足：每地址至多一个 M；M 时无 S；无死锁。")
    print("逐完成读操作的允许值（跨全部消息重排）:")
    labels = {
        (1, 0): "core1 第1次读 addr0",
        (2, 0): "core2 第1次读 addr0",
        (2, 1): "core2 被失效后重读 addr0",
        (0, 1): "core0 最后读 addr0",
    }
    for (core, idx), label in labels.items():
        oid = core * 10_000 + idx
        vals = sorted(result.load_values[oid])
        print(f"  {label}: 允许值集合 = {vals}")


def rejection_demo() -> None:
    banner("异常边界：实际触发的拒绝（不静默纠正、不留部分变更）")
    k = CoherenceKernel()
    k.request("GETX", 1, 0, 0, 10)
    while k.pending_messages():
        k.deliver(k.pending_messages()[0])
    k.request("GETS", 2, 1, 0)
    while k.pending_messages():
        k.deliver(k.pending_messages()[0])

    print("\n[边界1] 伪造陈旧 ACK：xid=999（不存在的事务）的 INV_ACK")
    stale = Message(MsgType.INV_ACK, 999, 1, 3, 0, aid=0)
    before = k.canonical()
    try:
        k.deliver(stale)
    except KernelError as e:
        print(f"  捕获 KernelError: {e}")
    assert k.canonical() == before, "被拒绝后状态必须完全不变"
    print("  状态指纹前后一致：拒绝未留下任何部分变更；目录仍为 S，共享者不变。")

    print("\n[边界2] 写入值为 NaN：数值输入拒绝 NaN/Infinity，整数拒绝 bool")
    try:
        k.request("GETX", 3, 2, 0, float("nan"))
    except KernelError as e:
        print(f"  捕获 KernelError: {e}")
    try:
        k.request("GETX", True, 2, 0, 7)
    except KernelError as e:
        print(f"  捕获 KernelError: {e}")

    print("\n[边界3] core0 缓存为 S 时对同地址发 GETS（GETS 只允许在 I 时发出）")
    try:
        k.request("GETS", 4, 0, 0)
    except KernelError as e:
        print(f"  捕获 KernelError: {e}")


def main() -> None:
    t0 = time.perf_counter()
    normal_demo()
    rejection_demo()
    banner(f"演示结束，总用时 {time.perf_counter() - t0:.2f}s（无网络、无线程、无 sleep）")


if __name__ == "__main__":
    main()
