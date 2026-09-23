"""固定输入演示：消息驱动 MSI 目录一致性内核。

运行：python demo.py
全程真实计算（无 sleep、无预录结果、无网络/线程），确定性输出。
内容：
  A. 失效链逐步演示：3 核共享 -> 升级等齐 ACK -> 读 M 触发回写后共享；
  B. 枚举小规模消息重排，逐状态检查“至多一个 M、M 时无 S”；
  C. 实际触发的拒绝/异常边界（NaN、bool、未知 msg_id、重复/陈旧投递）；
  D. 容量与背压：目录队列 / 每核请求 / 有向通道上限，满通道保留 + 腾出
     后续传，2 核 2 地址紧容量与宽容量（等价旧无容量模型）读值一致。
"""

import time

from msi_kernel import (
    DIR,
    CacheCoherenceKernel,
    CapacityExceededError,
    InvalidMessageError,
    InvalidRequestError,
    Limits,
    Message,
    MsgType,
    Op,
    Request,
    State,
    drain,
    explore,
)

ADDR = 0


def line_state(k, core, addr=ADDR):
    return k.snapshot().caches[core][addr].state.value


def print_status(k, title=""):
    s = k.snapshot()
    if title:
        print(f"    -- {title}")
    print(f"       caches 核0/核1/核2 @a{ADDR}: "
          f"{line_state(k,0)}/{line_state(k,1)}/{line_state(k,2)}")
    d = s.directory[ADDR]
    print(f"       dir a{ADDR}: {d.state.value} "
          f"sharers={list(d.sharers)} owner={d.owner} "
          f"active_txn={d.active_txn} waiting_acks={list(d.awaiting_acks)}")
    pend = [(m.msg_id, m.mtype.value, f"{m.src}->{m.dst}", m.txn_id)
            for m in s.pending_messages]
    print(f"       pending: {pend}")


def deliver_and_show(k, msg_id, tag):
    rep = k.deliver(msg_id)
    comp = [(c.core, c.op.value, c.value) for c in rep.completions]
    print(f"  [{tag}] deliver msg {msg_id} -> {rep.status.value}; "
          f"新发消息 {[m.msg_id for m in rep.emitted]}; 完成 {comp}")
    return rep


def part_a_invalidation_chain():
    print("=" * 72)
    print("A. 正常结果：失效链（invalidation / writeback chain）逐步演示")
    print("=" * 72)
    k = CacheCoherenceKernel(memory={ADDR: 100})

    r1 = k.request(0, ADDR, Op.LOAD)
    r2 = k.request(1, ADDR, Op.LOAD)
    r3 = k.request(2, ADDR, Op.LOAD)
    print(f"  三个核 load a{ADDR}（内存初值 100）即完成读值: "
          f"{r1.completions[0].value}, {r2.completions[0].value}, "
          f"{r3.completions[0].value}")

    print(f"  核1 store a{ADDR}=200：必须先失效核0、核2 并等齐 ACK")
    k.request(1, ADDR, Op.STORE, value=200)
    print_status(k, "store 已准入，目录发出两条 INV，事务挂起")

    inv0 = next(m for m in k.pending() if m.dst == 0)
    inv2 = next(m for m in k.pending() if m.dst == 2)
    inv0_rep = deliver_and_show(k, inv0.msg_id, "失效核0（INV 到核）")
    ack0 = next(m for m in inv0_rep.emitted if m.mtype is MsgType.INV_ACK)
    deliver_and_show(k, ack0.msg_id, "核0 的 INV_ACK 回目录")
    print_status(k, "只回来 1 个 ACK：store 仍不得提交（核2 尚未 ACK）")
    assert k.snapshot().directory[ADDR].awaiting_acks == (2,)

    deliver_and_show(k, inv2.msg_id, "失效核2（INV 到核）")
    ack2 = next(m for m in k.pending() if m.mtype is MsgType.INV_ACK)
    rep = deliver_and_show(k, ack2.msg_id, "第 2 个 ACK 到齐")
    print_status(k, "ACK 到齐：核1 升级为 M")
    assert rep.completions[0].value == 200

    print(f"  核2 load a{ADDR}：地址处于 M（owner=核1），目录必须先请 owner 回写")
    k.request(2, ADDR, Op.LOAD)
    print_status(k, "目录发 FETCH 给 owner")
    fetch = next(m for m in k.pending() if m.mtype is MsgType.FETCH)
    flush_rep = deliver_and_show(k, fetch.msg_id, "FETCH 到达 owner")
    print_status(k, "owner 降级 S 并回 FLUSH（最新数据 200 在途中）")
    flush = flush_rep.emitted[0]
    rep = deliver_and_show(k, flush.msg_id, "FLUSH 到达目录")
    print_status(k, "回写完成：核1、核2 以 S 共享")
    print(f"  => 核2 完成读值 = {rep.completions[0].value}（目录内存="
          f"{k.snapshot().memory[ADDR]}，陈旧内存已被回写刷新）")

    print("  最终每核缓存：")
    s = k.snapshot()
    for c in range(3):
        v = s.caches[c][ADDR]
        print(f"     核{c}: 状态={v.state.value} 值={v.value}")
    print(f"  全部完成事件: {[(c.txn_id, f'核{c.core}', c.op.value, c.value) for c in s.completed]}")


def part_b_enumeration():
    print("=" * 72)
    print("B. 枚举消息重排：最多一个 M；存在 M 时不得有 S")
    print("=" * 72)
    # 两个同时升级 + 读写交错（单地址，消息排列多）。
    scenario = [
        Request(0, ADDR, Op.LOAD),
        Request(1, ADDR, Op.LOAD),
        Request(2, ADDR, Op.LOAD),
        Request(1, ADDR, Op.STORE, value=200),   # 升级 1
        Request(2, ADDR, Op.STORE, value=300),   # 升级 2（排队）
        Request(0, ADDR, Op.LOAD),               # 读写交错的读
    ]
    res = explore(lambda: CacheCoherenceKernel(memory={ADDR: 100}), scenario,
                  all_paths=True)
    print(f"  穷举访问状态数: {res.steps}；完整消息重排路径数: "
          f"{len(res.schedules)}；去重终态数: {res.terminal_states}；"
          f"预算耗尽: {res.exhausted}")
    print(f"  不变量违例: {list(res.violations)}（每个可达状态都检查）")
    for path in sorted(res.schedules):
        print(f"     重排 {path}")
    print("  各 load 在全部重排下的实际读值集合:")
    for tid, vals in sorted(res.read_values.items()):
        print(f"     事务 {tid}: {sorted(vals)}（重排不影响允许读值）")
    print("  固定允许值（完成顺序参考口径）：三个 load=100，交错的 load=300")
    # 直接核对每个终态结构：至多一个 M，M 时无 S。
    ok = True
    for path in res.schedules:
        k = CacheCoherenceKernel(memory={ADDR: 100})
        k.request_batch(scenario)
        for mid in path:
            k.deliver(mid)
        s = k.snapshot()
        m_cores = [c for c in range(3)
                   if s.caches[c][ADDR].state is State.MODIFIED]
        s_cores = [c for c in range(3)
                   if s.caches[c][ADDR].state is State.SHARED]
        good = len(m_cores) <= 1 and not (m_cores and s_cores)
        ok &= good
    print(f"  全部 {len(res.schedules)} 条重排终态均满足: "
          f"{'最多一个 M 且 M 时无 S' if ok else '存在违例'}")


def part_c_boundaries():
    print("=" * 72)
    print("C. 实际触发的拒绝 / 异常边界（真实抛出，非预录文字）")
    print("=" * 72)

    try:
        CacheCoherenceKernel().request(0, 0, Op.STORE, value=float("nan"))
    except InvalidRequestError as e:
        print(f"  1) NaN 写入被拒绝: {type(e).__name__}: {e}")

    try:
        CacheCoherenceKernel().request(True, 0, Op.LOAD)
    except InvalidRequestError as e:
        print(f"  2) bool 被当作整数参数时拒绝: {type(e).__name__}: {e}")

    k = CacheCoherenceKernel(memory={0: 0})
    k.request_batch([Request(0, 0, Op.LOAD),
                     Request(1, 0, Op.STORE, value=5)])
    inv = k.pending()[0]
    ack = k.deliver(inv.msg_id).emitted[0]
    k.deliver(ack.msg_id)
    dup = k.deliver(ack.msg_id)
    print(f"  3) 重复投递 ACK: {dup.status.value}（幂等，无新完成）")

    # 陈旧 ACK：引用已结束事务，不得推进新事务。
    # 先让核0 读 M（回写后 S={0,1}），再让核1 升级，产生等核0 ACK 的新事务。
    k.request(0, 0, Op.LOAD)
    drain(k)
    k.request(1, 0, Op.STORE, value=6)
    stale = k.deliver(Message(None, 2, MsgType.INV_ACK, src=0, dst=DIR, addr=0))
    print(f"  4) 陈旧 ACK（属于已结束事务2）: {stale.status.value}；"
          f"新事务仍等待 {list(k.snapshot().directory[0].awaiting_acks)}")

    try:
        k.deliver(99999)
    except InvalidMessageError as e:
        print(f"  5) 投递从不存在的 msg_id: {type(e).__name__}: {e}")


def part_d_capacity_and_backpressure():
    print("=" * 72)
    print("D. 容量上限与背压（目录队列 / 每核请求 / 有向网络通道）")
    print("=" * 72)
    tight = Limits(queue_cap=2, core_cap=4, channel_cap=1)

    # --- D1. 满通道：消息保留在发送端，事务保持未完成；腾出后续传 --------
    print("  [D1] channel_cap=1：核0 拥有 addr0/addr1（M），核1、核2 跨核读")
    k = CacheCoherenceKernel(memory={0: 10, 1: 20}, limits=tight)
    k.request(0, 0, Op.STORE, value=11)
    k.request(0, 1, Op.STORE, value=22)
    k.request(1, 0, Op.LOAD)                    # FETCH DIR->0 在途
    k.request(2, 1, Op.LOAD)                    # 通道满 -> 保留
    print(f"       在途待发: {[(m.msg_id, f'{m.mtype.value} a{m.addr}') for m in k.pending()]}")
    print(f"       发送端保留: {[(m.msg_id, f'{m.mtype.value} a{m.addr}') for m in k.retained_messages()]}")
    print(f"       通道 DIR->0 在途数: {k.channel_load(DIR, 0)}/1；"
          f"已完成 load 数: {sum(1 for c in k.completed() if c.op is Op.LOAD)}")
    try:
        k.deliver(k.retained_messages()[0].msg_id)
    except InvalidMessageError as e:
        print(f"       保留消息不可投递（真实报错）: {type(e).__name__}")
    rep = k.deliver(k.pending()[0].msg_id)     # 第一条 FETCH 到达
    print(f"       投递 msg {rep.msg_id}：emitted="
          f"{[m.mtype.value for m in rep.emitted]}，腾出通道后 released="
          f"{[(m.msg_id, f'{m.mtype.value} a{m.addr}') for m in rep.released]}")
    drain(k)
    vals = [(c.core, c.addr, c.value) for c in k.completed() if c.op is Op.LOAD]
    print(f"       排空后两个跨核读完成: {vals}（手算：核1 读 a0=11，核2 读 a1=22）")

    # --- D2. 目录等待队列上限：恰好满边界 + 超限拒绝（零半更新） ----------
    print("  [D2] queue_cap=2：同地址活动事务之外最多排 2 条")
    k2 = CacheCoherenceKernel(memory={0: 0}, limits=tight)
    k2.request_batch([Request(0, 0, Op.LOAD), Request(1, 0, Op.LOAD),
                      Request(1, 0, Op.STORE, value=5)])  # 活动事务（等 INV）
    k2.request(2, 0, Op.LOAD)                    # 排队 1
    k2.request(0, 0, Op.STORE, value=6)          # 排队 2（恰好满）
    print(f"       队列已排满: {[q.txn_id for q in k2.snapshot().queues[0]]}")
    done_before = len(k2.completed())
    try:
        k2.request(2, 0, Op.LOAD)                # 第 3 条：超 queue_cap
    except CapacityExceededError as e:
        print(f"       超容量被拒: {type(e).__name__}: {e}")
    print(f"       拒绝后完成数不变: {len(k2.completed()) == done_before}，"
          f"队列仍为 {[q.txn_id for q in k2.snapshot().queues[0]]}")

    # --- D3. 每核未完成请求上限 ------------------------------------------
    print("  [D3] core_cap=1：同一核接纳但未完成的请求超限被拒"
          "（同步完成的请求不占容量）")
    k3 = CacheCoherenceKernel(
        memory={a: 0 for a in range(5)},
        limits=Limits(queue_cap=8, core_cap=1, channel_cap=8))
    k3.request(1, 0, Op.STORE, value=1)
    k3.request(0, 0, Op.LOAD)                   # 核0 唯一在途往返，占满
    immediate = k3.request(0, 4, Op.LOAD)       # 空闲地址 I->S，同步完成
    print(f"       核0 在 addr4 的空闲读立即完成（不占容量）: "
          f"{immediate.completions[0].value}")
    k3.request(1, 1, Op.STORE, value=2)
    try:
        k3.request(0, 1, Op.LOAD)               # 核0 第 2 条往返：超限
    except CapacityExceededError as e:
        print(f"       超每核容量被拒: {type(e).__name__}: {e}")

    # --- D4. 2 核 2 地址：紧容量全部重排读值 == 宽容量（旧无容量）模型 ----
    print("  [D4] 2 核 2 地址写回 + 升级 + 请求交错：紧容量 vs 宽容量枚举")
    scenario = [
        Request(0, 0, Op.STORE, value=10),
        Request(1, 1, Op.STORE, value=20),
        Request(1, 0, Op.LOAD),                  # 回写 a0
        Request(0, 1, Op.LOAD),                  # 回写 a1
        Request(1, 0, Op.STORE, value=30),       # 升级排队
    ]
    res_t = explore(lambda: CacheCoherenceKernel(limits=tight), scenario)
    res_w = explore(
        lambda: CacheCoherenceKernel(
            limits=Limits(queue_cap=8, core_cap=8, channel_cap=8)),
        scenario)
    tv = {t: sorted(v) for t, v in res_t.read_values.items()}
    wv = {t: sorted(v) for t, v in res_w.read_values.items()}
    print(f"       紧容量: 访问 {res_t.steps} 状态, 重排 "
          f"{len(res_t.schedules)} 条, 违例 {len(res_t.violations)}, "
          f"预算耗尽 {res_t.exhausted}")
    print(f"       宽容量: 访问 {res_w.steps} 状态, 重排 "
          f"{len(res_w.schedules)} 条, 违例 {len(res_w.violations)}")
    print(f"       紧容量各 load 读值集合: {tv}")
    print(f"       宽容量各 load 读值集合: {wv}")
    print(f"       两者读值完全一致: {tv == wv}（手算：a0 的 load=10，"
          f"a1 的 load=20，与消息顺序、通道容量无关）")


def main():
    t0 = time.perf_counter()
    part_a_invalidation_chain()
    print()
    part_b_enumeration()
    print()
    part_c_boundaries()
    print()
    part_d_capacity_and_backpressure()
    dt = time.perf_counter() - t0
    print("=" * 72)
    print(f"演示结束，实际计算耗时 {dt:.2f} 秒（无 sleep、无网络、无预录结果）。")
    if dt > 8.0:
        raise SystemExit(f"演示耗时 {dt:.2f}s 超过约定的约 8 秒")


if __name__ == "__main__":
    main()
