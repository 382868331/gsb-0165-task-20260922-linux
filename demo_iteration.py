"""CAS 增量固定输入演示（纯离散事件、无真实原子指令、无等待）。"""

import os
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

from msi_kernel import (
    CacheCoherenceKernel,
    InvalidRequestError,
    Op,
    Request,
    drain,
)
from reference import final_memory_after_completions


def flatten(completions):
    events = []
    for comp in completions:
        if comp.op is Op.CAS:
            events.append((comp.addr, "cas", comp.value, comp.success,
                           comp.expected_value, comp.new_value))
        else:
            events.append((comp.addr, comp.op.value, comp.value))
    return events


def check_scalar(label, kernel, initial):
    events = flatten(kernel.completed())
    final = final_memory_after_completions(events, initial)
    actual = [kernel.snapshot().memory[a] for a in range(kernel.num_addrs)]
    authoritative = list(actual)
    for addr in range(kernel.num_addrs):
        owner = kernel.snapshot().directory[addr].owner
        if owner is not None:
            authoritative[addr] = kernel.snapshot().caches[owner][addr].value
    if final != authoritative:
        raise AssertionError(
            f"{label}: 标量状态机 {final} != 内核权威值 {authoritative}")
    print(f"  独立标量状态机核对通过: {final[:2]}；内核权威值: "
          f"{authoritative[:2]}")


def race_and_duplicate_response():
    print("=" * 72)
    print("A. 两个核同时 CAS(0) 到不同值；延迟 ACK 与重复响应")
    print("=" * 72)
    kernel = CacheCoherenceKernel(memory={0: 0})
    kernel.request_batch([
        Request(0, 0, Op.LOAD),
        Request(1, 0, Op.LOAD),
        Request(2, 0, Op.LOAD),
        Request(1, 0, Op.CAS, expected_value=0, new_value=10, txn_id=101),
        Request(2, 0, Op.CAS, expected_value=0, new_value=20, txn_id=102),
    ])
    print("  准入顺序: load0,load1,load2,CAS101,CAS102；CAS102 在 FIFO 中等待")
    print(f"  队列: {[q.txn_id for q in kernel.snapshot().queues[0]]}")

    inv0 = next(message for message in kernel.pending()
                if message.dst == 0)
    ack0 = kernel.deliver(inv0.msg_id).emitted[0]
    kernel.deliver(ack0.msg_id)
    print("  核0 已失效但核2 ACK 未到："
          f"CAS完成数={sum(c.op is Op.CAS for c in kernel.completed())}")

    remaining = kernel.pending()
    last_report = None
    while remaining:
        for message in remaining:
            report = kernel.deliver(message.msg_id)
            if report.completions:
                last_report = report
        remaining = kernel.pending()

    duplicated_ack = kernel.deliver(ack0.msg_id)
    print(f"  重复送达旧 ACK: status={duplicated_ack.status.value}，"
          f"新完成={len(duplicated_ack.completions)}")

    first = Request(1, 0, Op.CAS, expected_value=0, new_value=10, txn_id=101)
    second = Request(2, 0, Op.CAS, expected_value=0, new_value=20, txn_id=102)
    retry1 = kernel.request_batch([first])[0]
    retry2 = kernel.request_batch([second])[0]
    print(f"  重传 CAS101: old={retry1.completions[0].value}, "
          f"success={retry1.completions[0].success}")
    print(f"  重传 CAS102: old={retry2.completions[0].value}, "
          f"success={retry2.completions[0].success}")
    cas_count = sum(c.op is Op.CAS for c in kernel.completed())
    print(f"  CAS 完成事件仍只有 {cas_count} 个，说明重传未再次交换")

    try:
        kernel.request(1, 0, Op.CAS, expected_value=1,
                       new_value=10, txn_id=101)
    except InvalidRequestError as exc:
        print(f"  相同 ID 不同参数已拒绝: {exc.field}")

    for comp in (retry1.completions[0], retry2.completions[0]):
        print(f"  CAS{comp.txn_id}: old={comp.value}, success={comp.success}, "
              f"expected={comp.expected_value}, new={comp.new_value}")
    assert (retry1.completions[0].value,
            retry1.completions[0].success) == (0, True)
    assert (retry2.completions[0].value,
            retry2.completions[0].success) == (10, False)
    assert cas_count == 2
    assert duplicated_ack.completions == ()
    check_scalar("race", kernel, {0: 0})


def cas_between_stores():
    print()
    print("=" * 72)
    print("B. CAS 夹在两个普通写之间，按目录 FIFO 线性化")
    print("=" * 72)
    kernel = CacheCoherenceKernel(memory={0: 0})
    kernel.request(0, 0, Op.STORE, value=1, txn_id=1)
    cas_request = Request(1, 0, Op.CAS, expected_value=0,
                          new_value=9, txn_id=2)
    kernel.request_batch([cas_request])
    kernel.request(2, 0, Op.STORE, value=3, txn_id=3)
    print("  准入顺序: STORE=1, CAS(expected=0,new=9), STORE=3")
    print(f"  CAS 启动时队列: {[q.txn_id for q in kernel.snapshot().queues[0]]}")
    drain(kernel)
    cas = next(comp for comp in kernel.completed() if comp.op is Op.CAS)
    retry = kernel.request_batch([cas_request])[0].completions[0]
    print(f"  CAS 返回 old={cas.value}, success={cas.success}；"
          f"它看到的是前一个普通写 1，不是请求准入时的缓存初值")
    print(f"  重传返回同一结果 old={retry.value}, success={retry.success}")
    print("  最终普通写 STORE=3 生效；CAS 因期望值 0 != old=1 未改值")
    assert (cas.value, cas.success) == (1, False)
    assert retry == cas
    check_scalar("fifo", kernel, {0: 0})


def main():
    started = time.perf_counter()
    race_and_duplicate_response()
    cas_between_stores()
    elapsed = time.perf_counter() - started
    print()
    print("=" * 72)
    print(f"演示结束: 实际计算耗时 {elapsed:.3f} 秒；无 sleep、无网络、无线程。")
    if elapsed > 90:
        raise SystemExit(f"演示耗时 {elapsed:.3f}s，超过 90 秒限制")


if __name__ == "__main__":
    main()
