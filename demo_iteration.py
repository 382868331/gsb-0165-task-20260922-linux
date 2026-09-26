"""CAS 增量演示：固定输入、现场计算、确定性消息模拟。

三个题面验收场景 + 一个实际触发的异常边界：
1. 两个核同时把 0 改成不同值 —— 恰好一个成功；
2. CAS 夹在普通写之间 —— 按目录授予（FIFO）顺序线性化；
3. 响应重复送达 —— 同 txn_id 重传返回原结果，不再次交换；
4. 异常边界 —— 同 txn_id 携带不同参数被拒绝。

只用标准库，无真实线程/网络/原子指令：消息投递顺序是显式输入，
整个演示为确定性离散事件模拟，运行时间远小于 90 秒上限。
"""

from msi_kernel import (
    CacheCoherenceKernel,
    InvalidRequestError,
    Op,
    Request,
    drain,
    explore,
)


def show(completion):
    if completion.op is Op.CAS:
        return (f"txn={completion.txn_id} core={completion.core} CAS "
                f"旧值={completion.value} 期望={completion.expected} "
                f"新值={completion.new_value} 成功={completion.success}")
    return (f"txn={completion.txn_id} core={completion.core} "
            f"{completion.op.value} 值={completion.value}")


def scenario_concurrent_cas():
    print("== 场景 1：两个核同时把 0 改成不同值 ==")
    scenario = [
        Request(0, 0, Op.LOAD),
        Request(1, 0, Op.LOAD),
        Request(2, 0, Op.LOAD),
        Request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=10),
        Request(1, 0, Op.CAS, expected=0, new_value=200, txn_id=11),
    ]
    # 穷举全部消息重排（all_paths），验证任意投递顺序下都恰好一个成功。
    res = explore(CacheCoherenceKernel, scenario, all_paths=True)
    print(f"枚举 {len(res.schedules)} 条完整调度，不变量违例 "
          f"{len(res.violations)} 起")
    for path, completed in sorted(res.schedules.items()):
        cas = [c for c in completed if c.op is Op.CAS]
        wins = [c for c in cas if c.success]
        assert len(wins) == 1, "每次运行必须恰好一个 CAS 成功"
    print("全部调度均恰好一个成功；其中一条调度的完成序列：")
    k = CacheCoherenceKernel(memory={0: 0})
    k.request_batch(scenario)
    drain(k)
    for c in k.completed():
        if c.op is Op.CAS:
            print("  " + show(c))
    k.request(2, 0, Op.LOAD)
    drain(k)
    print(f"  最终值（核2 随后 load）= {k.completed()[-1].value}")
    print()


def scenario_cas_between_stores():
    print("== 场景 2：CAS 夹在普通写之间（按目录授予顺序线性化）==")
    k = CacheCoherenceKernel(memory={0: 0})
    k.request_batch([
        Request(0, 0, Op.STORE, value=5),                # txn1
        Request(1, 0, Op.CAS, expected=5, new_value=7),  # txn2
        Request(2, 0, Op.STORE, value=9),                # txn3
        Request(0, 0, Op.CAS, expected=5, new_value=99),  # txn4
    ])
    drain(k)
    for c in k.completed():
        print("  " + show(c))
    print("  第二个 CAS 排在 store=9 之后，看到 9 而不是发请求时的 5，"
          "因此失败。")
    print()


def scenario_replayed_response():
    print("== 场景 3：响应重复送达（同 txn_id 重传）==")
    k = CacheCoherenceKernel(memory={0: 0})
    rep1 = k.request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=7)
    print("  首次:   " + show(rep1.completions[0]))
    rep2 = k.request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=7)
    print("  重传:   " + show(rep2.completions[0]) + "  （原结果，未再次交换）")
    assert rep1.completions == rep2.completions
    assert len([c for c in k.completed() if c.txn_id == 7]) == 1
    # 证明值只被交换了一次：新 CAS 以 100 为期望值应当成功。
    k.request(1, 0, Op.CAS, expected=100, new_value=5)
    drain(k)
    print("  校验:   " + show(k.completed()[-1]))
    print()


def scenario_rejected_conflict():
    print("== 异常边界：同 txn_id 携带不同参数 ==")
    k = CacheCoherenceKernel(memory={0: 0})
    k.request(0, 0, Op.CAS, expected=0, new_value=1, txn_id=7)
    try:
        k.request(0, 0, Op.CAS, expected=0, new_value=2, txn_id=7)
    except InvalidRequestError as exc:
        print(f"  拒绝: {exc}")
    print()


def main():
    scenario_concurrent_cas()
    scenario_cas_between_stores()
    scenario_replayed_response()
    scenario_rejected_conflict()
    print("演示完成：全部为现场确定性计算，无预录结果。")


if __name__ == "__main__":
    main()

