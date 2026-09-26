"""固定四扇区缓存行模式的合成交错演示。

全程使用离线、离散消息；不等待、不联网、不使用预录结果。
"""

import time

from msi_kernel import (
    DIR,
    CacheCoherenceKernel,
    DeliveryStatus,
    Message,
    MsgType,
    Op,
    Request,
    State,
    check_invariants,
    drain,
    explore,
)

ADDR = 0
S0 = 0b0001
S1 = 0b0010
BOTH = S0 | S1
INIT = {(ADDR, 0): 100, (ADDR, 1): 200}


def describe_sectors(kernel):
    rows = []
    for core in range(3):
        parts = []
        for sector in range(2):
            line = kernel.snapshot().caches[core][ADDR].sectors[sector]
            parts.append(f"s{sector}:{line.state.value}/v{line.version}/v{line.value}")
        rows.append(f"核{core}[{', '.join(parts)}]")
    return " | ".join(rows)


def assert_prefix(kernel, label):
    problem = check_invariants(kernel)
    if problem is not None:
        raise AssertionError(f"{label}: {problem}")


def apply_message(kernel, msg_id, label):
    report = kernel.deliver(msg_id)
    assert_prefix(kernel, label)
    emitted = [
        (m.msg_id, m.mtype.value, f"{m.src}->{m.dst}", f"0b{m.sectors:04b}")
        for m in report.emitted
    ]
    done = [
        (c.txn_id, c.core, c.op.value, c.value, f"0b{c.sectors:04b}")
        for c in report.completions
    ]
    print(f"  {label}: msg={msg_id} -> {report.status.value}; "
          f"新消息={emitted}; 完成={done}")
    return report


def ledger_check(completions):
    memory = [[100, 200, 0, 0]]
    for comp in completions:
        sectors = [s for s in range(4) if comp.sectors & (1 << s)]
        if comp.op is Op.STORE:
            for sector in sectors:
                memory[comp.addr][sector] = comp.value
        else:
            actual = comp.value if len(sectors) == 1 else tuple(
                comp.value[s] for s in sectors)
            expected = tuple(memory[comp.addr][s] for s in sectors)
            actual_tuple = (actual,) if len(sectors) == 1 else actual
            if actual_tuple != expected:
                raise AssertionError(
                    f"txn={comp.txn_id} 账本不符: {actual_tuple} != {expected}")


def build_scenario_kernel():
    kernel = CacheCoherenceKernel(sector_mode=True, sector_values=INIT)
    kernel.request_batch([
        Request(0, ADDR, Op.LOAD, sectors=BOTH),
        Request(1, ADDR, Op.LOAD, sectors=BOTH),
        Request(0, ADDR, Op.STORE, value=101, sectors=S0),
        Request(1, ADDR, Op.STORE, value=202, sectors=S1),
        Request(2, ADDR, Op.LOAD, sectors=BOTH),
    ])
    return kernel


def main():
    started = time.perf_counter()
    print("=" * 76)
    print("固定四扇区：两核独立写不同扇区，第三核在同一获授时刻跨区读取")
    print("=" * 76)
    kernel = build_scenario_kernel()
    print("初始待发消息：")
    for msg in kernel.pending():
        print(f"  msg={msg.msg_id}: {msg.mtype.value} {msg.src}->{msg.dst} "
              f"txn={msg.txn_id} mask=0b{msg.sectors:04b}")
    print("前缀状态：", describe_sectors(kernel))
    assert_prefix(kernel, "准入后")

    inv_ids = [m.msg_id for m in kernel.pending() if m.mtype is MsgType.INV]
    for msg_id in inv_ids:
        ack = apply_message(kernel, msg_id, "投递 INV").emitted[0]
        apply_message(kernel, ack.msg_id, "返回 INV_ACK")

    fetches = list(kernel.pending())
    first_flush = apply_message(kernel, fetches[0].msg_id,
                                "先完成一侧 FETCH").emitted[0]
    apply_message(kernel, first_flush.msg_id, "先到一个 FLUSH")
    print("  仅一个扇区回写后，跨区 load 仍未完成：",
          [c.txn_id for c in kernel.completed() if c.txn_id == 5])
    assert not [c for c in kernel.completed() if c.txn_id == 5]

    remaining_fetch = next(m for m in kernel.pending()
                          if m.mtype is MsgType.FETCH)
    second_flush = apply_message(kernel, remaining_fetch.msg_id,
                                 "再完成另一侧 FETCH").emitted[0]
    completed = apply_message(kernel, second_flush.msg_id,
                              "第二个 FLUSH 到达，原子授权")
    print("  跨区读出：", completed.completions[0].value[:2])
    print("完成后状态：", describe_sectors(kernel))
    ledger_check(kernel.completed())

    print()
    print("=" * 76)
    print("乱序与掩码异常：错误扇区/旧事务 ACK 均不能放行当前事务")
    print("=" * 76)
    kernel2 = CacheCoherenceKernel(sector_mode=True)
    kernel2.request(0, ADDR, Op.LOAD, sectors=S0)
    kernel2.request(1, ADDR, Op.LOAD, sectors=S0)
    kernel2.request(0, ADDR, Op.STORE, value=7, sectors=S0)
    wrong_mask = Message(None, 3, MsgType.INV_ACK, src=1, dst=DIR,
                         addr=ADDR, sectors=S1)
    report = kernel2.deliver(wrong_mask)
    assert report.status is DeliveryStatus.IGNORED_STALE
    assert kernel2.snapshot().directory[ADDR].sectors[0].awaiting_acks == (1,)
    print(f"  错误掩码 ACK: {report.status.value}；扇区0仍等待核1")
    real_inv = next(m for m in kernel2.pending() if m.sectors == S0)
    real_ack = kernel2.deliver(real_inv.msg_id).emitted[0]
    report = kernel2.deliver(real_ack.msg_id)
    assert report.completions[0].value == 7
    print("  正确掩码 ACK 后写入完成：", report.completions[0].value)

    late = Message(None, 3, MsgType.INV_ACK, src=1, dst=DIR,
                   addr=ADDR, sectors=S0)
    report = kernel2.deliver(late)
    assert report.status is DeliveryStatus.IGNORED_STALE
    print(f"  迟到的旧事务 ACK: {report.status.value}；不影响新事务")

    print()
    print("=" * 76)
    print("有界状态探索：每个可达前缀检查单写者；结果对串行扇区账本核对")
    print("=" * 76)
    scenario = [
        Request(0, ADDR, Op.LOAD, sectors=BOTH),
        Request(1, ADDR, Op.LOAD, sectors=BOTH),
        Request(0, ADDR, Op.STORE, value=101, sectors=S0),
        Request(1, ADDR, Op.STORE, value=202, sectors=S1),
        Request(2, ADDR, Op.LOAD, sectors=BOTH),
    ]
    result = explore(build_scenario_kernel, scenario, budget=10000)
    assert result.violations == ()
    assert not result.exhausted
    for schedule_completions in result.schedules.values():
        ledger_check(schedule_completions)
    print(f"  访问状态数={result.steps}；终态重排={len(result.schedules)}；"
          f"不变量违例={len(result.violations)}；预算耗尽={result.exhausted}")
    print("  全部终态跨区读值均为 (101, 202, 0, 0)，且串行账本核对通过")

    drain(build_scenario_kernel())
    elapsed = time.perf_counter() - started
    print("=" * 76)
    print(f"演示完成，实际计算耗时 {elapsed:.3f} 秒；无 sleep、无网络、无预录结果")
    if elapsed > 90:
        raise SystemExit(f"演示超过 90 秒：{elapsed:.3f}s")


if __name__ == "__main__":
    main()
