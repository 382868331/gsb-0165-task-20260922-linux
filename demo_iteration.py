"""四扇区缓存行增量演示：纯离线、纯离散事件，无 sleep / 网络 / 硬件。"""

import time

from msi_kernel import (
    ALL_SECTORS,
    DIR,
    CacheCoherenceKernel,
    LineMode,
    Message,
    MsgType,
    Op,
    Request,
)
from msi_kernel.explore import explore

ADDR = 0


def make_shared():
    kernel = CacheCoherenceKernel({ADDR: 0}, mode=LineMode.FOUR_SECTOR)
    for core in range(3):
        kernel.request(core, ADDR, Op.LOAD, sectors=ALL_SECTORS)
    return kernel


def drain_pending(kernel):
    while kernel.pending():
        kernel.deliver(kernel.pending()[0].msg_id)


def show_sectors(kernel):
    snap = kernel.snapshot()
    for core in range(3):
        vals = [
            (s.state.value, s.value, s.version)
            for s in snap.sector_caches[core][ADDR]
        ]
        print(f"      核{core}: {vals}")
    print(f"      活动事务: {snap.sector_txns[ADDR]}")


def independent_expected(completed):
    last = [0, 0, 0, 0]
    checked = []
    for comp in completed:
        covered = [i for i in range(4) if comp.sectors & (1 << i)]
        if comp.op is Op.STORE:
            for sector, value in zip(covered, comp.value):
                last[sector] = value
        else:
            for sector, value in zip(covered, comp.value):
                if last[sector] != value:
                    raise AssertionError("独立串行账本与读值不一致")
            checked.append((comp.txn_id, comp.value))
    return checked


def part_independent_writers_and_atomic_read():
    print("=" * 72)
    print("A. 两个核写不同扇区；第三核在同一授刻原子取得两个扇区")
    print("=" * 72)
    kernel = make_shared()
    first = kernel.request(0, ADDR, Op.STORE, value=(10,), sectors=0b0001)
    second = kernel.request(1, ADDR, Op.STORE, value=(20,), sectors=0b0010)
    print(f"  两个不重叠写均未排队: queued={first.queued},{second.queued}")
    show_sectors(kernel)

    messages = kernel.pending()
    inv_to_1 = next(m for m in messages if m.dst == 1 and m.sectors == 1)
    inv_to_0 = next(m for m in messages if m.dst == 0 and m.sectors == 2)
    ack_to_1 = kernel.deliver(inv_to_1.msg_id).emitted[0]
    ack_to_0 = kernel.deliver(inv_to_0.msg_id).emitted[0]
    kernel.deliver(ack_to_1.msg_id)
    kernel.deliver(ack_to_0.msg_id)
    drain_pending(kernel)
    print("  失效确认乱序投递后：两个扇区分别成为不同核的 dirty owner")
    show_sectors(kernel)

    reader = kernel.request(2, ADDR, Op.LOAD, sectors=0b0011)
    assert not reader.queued
    fetch_0 = next(m for m in kernel.pending() if m.dst == 0)
    fetch_1 = next(m for m in kernel.pending() if m.dst == 1)
    flush_0 = kernel.deliver(fetch_0.msg_id).emitted[0]
    first_half = kernel.deliver(flush_0.msg_id)
    print(f"  只完成扇区0 FLUSH 后跨区读完成数: "
          f"{len(first_half.completions)}（必须为0）")
    assert not first_half.completions
    flush_1 = kernel.deliver(fetch_1.msg_id).emitted[0]
    done = kernel.deliver(flush_1.msg_id)
    print(f"  扇区0/1 均授权后跨区读完成: {done.completions[0].value}，"
          f"掩码={done.completions[0].sectors:04b}")
    independent_expected(kernel.completed())
    print("  独立串行账本核对通过：读值=(10,20)，不存在半个旧值/半个新值")


def part_stale_confirmation():
    print("=" * 72)
    print("B. 异常路径：错误扇区和迟到 ACK 都不能放行新事务")
    print("=" * 72)
    kernel = make_shared()
    kernel.request(0, ADDR, Op.STORE, value=(10,), sectors=1)
    wrong = kernel.deliver(Message(
        None, 4, MsgType.INV_ACK, src=1, dst=DIR, addr=ADDR, sectors=2))
    print(f"  同事务但错误扇区 ACK: {wrong.status.value}；等待未减少")
    print(f"      {kernel.snapshot().sector_txns[ADDR][0].ack_waiters}")
    drain_pending(kernel)

    kernel.request(1, ADDR, Op.STORE, value=(20,), sectors=2)
    drain_pending(kernel)
    late = kernel.deliver(Message(
        None, 4, MsgType.INV_ACK, src=2, dst=DIR, addr=ADDR, sectors=1))
    print(f"  上一事务迟到 ACK: {late.status.value}")
    new_write = kernel.request(2, ADDR, Op.STORE, value=(40,), sectors=4)
    print(f"  新的不重叠事务 queued={new_write.queued}；迟到 ACK 未消费它的等待")
    drain_pending(kernel)
    independent_expected(kernel.completed())
    print("  新事务正常完成，独立串行账本核对通过")


def part_exploration():
    print("=" * 72)
    print("C. 合成消息交错枚举：逐前缀检查逐扇区单写者，并核对读账本")
    print("=" * 72)
    scenario = [
        Request(0, ADDR, Op.STORE, value=(10,), sectors=1),
        Request(1, ADDR, Op.STORE, value=(20,), sectors=2),
        Request(2, ADDR, Op.LOAD, sectors=3),
    ]
    result = explore(make_shared, scenario, budget=100_000)
    print(f"  访问状态数={result.steps}；终态路径数={len(result.schedules)}；"
          f"预算耗尽={result.exhausted}；不变量违例={result.violations}")
    for completions in result.schedules.values():
        independent_expected(completions)
    assert not result.violations
    assert not result.exhausted
    print(f"  跨区 load 事务6 在全部等价交错下读值集合: "
          f"{result.read_values[6]}")


def main():
    start = time.perf_counter()
    part_independent_writers_and_atomic_read()
    print()
    part_stale_confirmation()
    print()
    part_exploration()
    elapsed = time.perf_counter() - start
    print("=" * 72)
    print(f"演示结束，现场计算耗时 {elapsed:.2f} 秒（无 sleep、无预录答案）。")
    if elapsed > 90:
        raise SystemExit(f"演示耗时 {elapsed:.2f}s 超过 90 秒上限")


if __name__ == "__main__":
    main()
