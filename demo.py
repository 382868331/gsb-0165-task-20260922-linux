"""固定输入演示：消息驱动 MSI 目录一致性内核。

运行：python demo.py
全程真实计算（无 sleep、无预录结果、无网络/线程），确定性输出。
内容：
  A. 失效链逐步演示：3 核共享 -> 升级等齐 ACK -> 读 M 触发回写后共享；
  B. 枚举小规模消息重排，逐状态检查“至多一个 M、M 时无 S”；
  C. 实际触发的拒绝/异常边界（NaN、bool、未知 msg_id、重复/陈旧投递）；
  D. evict 逐出演示：S 行移除共享者、M 行回写后才转 I、I 行无操作，
     与第三核 load/升级的 FIFO 关系，以及旧 ACK/FLUSH 迟到不推进新事务，
     并枚举重点交错的全部短消息排列。
"""

import time

from msi_kernel import (
    DIR,
    CacheCoherenceKernel,
    InvalidMessageError,
    InvalidRequestError,
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


def part_d_eviction():
    print("=" * 72)
    print("D. evict(core, addr) 显式逐出：模拟有限缓存容量的替换")
    print("=" * 72)

    # --- D1: I 行无操作；S 行通知目录移除共享者 -----------------------------
    print("  D1) S 行逐出：核端自行置 I，目录在 EVICT 投递时才移除共享者")
    k = CacheCoherenceKernel(memory={ADDR: 100})
    k.request_batch([Request(0, ADDR, Op.LOAD),
                     Request(1, ADDR, Op.LOAD),
                     Request(2, ADDR, Op.LOAD)])
    rep_i = k.evict(2, ADDR + 1)            # 该地址核2 从未持有：I 行
    print(f"     逐出一条 I 行：同步完成 {[(c.op.value) for c in rep_i.completions]}，"
          f"待发消息 {len(k.pending())} 条（无操作）")
    rep = k.evict(0, ADDR)
    print(f"     逐出核0 的 S 行：句柄立即返回但完成数={len(rep.completions)}"
          f"（EVICT/ACK 未投递，不能报告完成）")
    print_status(k, "核0 已自行置 I；目录共享者暂仍含核0（通知在途）")
    assert k.snapshot().caches[0][ADDR].state is State.INVALID
    ev = next(m for m in k.pending() if m.mtype is MsgType.EVICT)
    rep = deliver_and_show(k, ev.msg_id, "EVICT 到目录")
    print(f"     目录移除共享者 -> {list(k.snapshot().directory[ADDR].sharers)}")
    ack = next(m for m in rep.emitted if m.mtype is MsgType.EVICT_ACK)
    deliver_and_show(k, ack.msg_id, "EVICT_ACK 回核0，逐出完成")

    # --- D2: M 行必须先回写最新数据 ----------------------------------------
    print("  D2) M 行逐出：最新数据必须 FLUSH 交回目录，ACK 后才转 I")
    k = CacheCoherenceKernel(memory={ADDR: 100})
    k.request(0, ADDR, Op.STORE, value=555)
    rep = k.evict(0, ADDR)                  # 先准入逐出（txn2，FLUSH 在途）
    queued = k.request(2, ADDR, Op.LOAD)    # 与逐出同地址：排在逐出后面
    print(f"     store=555（内存仍 100）；evict 返回完成数={len(rep.completions)}；"
          f"随后第三核 load queued={queued.queued}")
    flush = next(m for m in k.pending() if m.mtype is MsgType.FLUSH)
    print(f"     待发 FLUSH 携带最新数据 {flush.value}；此刻内存="
          f"{k.snapshot().memory[ADDR]}，目录 owner={k.snapshot().directory[ADDR].owner}")
    rep = deliver_and_show(k, flush.msg_id, "FLUSH 到目录（回写入账）")
    s = k.snapshot()
    print(f"     入账后内存={s.memory[ADDR]}，目录={s.directory[ADDR].state.value}，"
          f"本次完成={[c.op.value for c in rep.completions]}（ACK 未到，逐出仍未完成）")
    ack = next(m for m in rep.emitted if m.mtype is MsgType.EVICT_ACK)
    rep = deliver_and_show(k, ack.msg_id, "EVICT_ACK 到核0")
    print(f"     链式完成: {[(c.op.value, c.value) for c in rep.completions]}"
          f"（逐出完成后排队的 load 才启动，读到回写值）")

    # --- D3: 旧逐出 FLUSH 迟到，不能清掉后来重新获得的 M 所有权 -------------
    print("  D3) 旧事务身份的迟到消息：不推进新所有权")
    k.request(0, ADDR, Op.STORE, value=888)   # 核0 想重新获得 M（先失效核2）
    drain(k)
    print_status(k, "核0 重新升级为 M=888（目录 owner=核0，内存停在 555）")
    assert k.snapshot().directory[ADDR].owner == 0
    stale = k.deliver(Message(None, 2, MsgType.FLUSH, src=0, dst=DIR,
                              addr=ADDR, value=555))
    print(f"     旧逐出事务(txn2) 的 FLUSH 迟到: status={stale.status.value}，"
          f"owner 仍为 {k.snapshot().directory[ADDR].owner}，"
          f"内存仍 {k.snapshot().memory[ADDR]}")
    stale_ack = k.deliver(Message(None, 2, MsgType.EVICT_ACK,
                                  src=DIR, dst=0, addr=ADDR))
    print(f"     旧 EVICT_ACK 迟到: status={stale_ack.status.value}，"
          f"核0 仍为 {k.snapshot().caches[0][ADDR].state.value}")

    # --- D4: 重点交错的短消息排列枚举（有明确规模上限） --------------------
    print("  D4) 枚举重点交错（固定小场景，all_paths 保留全部原始排列）")
    # 场景一：M 逐出同时第三核 load（同地址 FIFO：消息 2 条，排列极少）。
    scn1 = [Request(0, ADDR, Op.STORE, value=555),
            Request(0, ADDR, Op.EVICT),
            Request(2, ADDR, Op.LOAD)]
    r1 = explore(lambda: CacheCoherenceKernel(memory={ADDR: 100}), scn1,
                 all_paths=True)
    print(f"     [M逐出 撞 第三核load] 完整排列 {len(r1.schedules)} 条，"
          f"访问状态 {r1.steps}，违例 {len(r1.violations)}，"
          f"load 读值集合 {[sorted(v) for v in r1.read_values.values()]}")
    # 场景二：地址0 的 S 逐出 与 地址1 的升级（独立地址，消息 4 条可交错）。
    scn2 = [Request(0, 0, Op.LOAD),
            Request(1, 1, Op.LOAD), Request(2, 1, Op.LOAD),
            Request(0, 0, Op.EVICT),
            Request(2, 1, Op.STORE, value=8)]
    r2 = explore(lambda: CacheCoherenceKernel(memory={0: 0, 1: 0}), scn2,
                 all_paths=True)
    print(f"     [S逐出 撞 他址升级] 完整排列 {len(r2.schedules)} 条，"
          f"访问状态 {r2.steps}，违例 {len(r2.violations)}，"
          f"预算耗尽 {r2.exhausted}")
    # 逐步核对：任一排他调度的每一步至多一个 M 且目录 owner 一致。
    bad_step = 0
    for path in r2.schedules:
        kk = CacheCoherenceKernel(memory={0: 0, 1: 0})
        kk.request_batch(scn2)
        for mid in path:
            kk.deliver(mid)
            snap = kk.snapshot()
            for a in (0, 1):
                ms = [c for c in range(3)
                      if snap.caches[c][a].state is State.MODIFIED]
                if len(ms) > 1 or (ms and snap.directory[a].owner != ms[0]):
                    bad_step += 1
    print(f"     逐消息步核对（最多一个 M、M 核即目录 owner）违例步数: {bad_step}")


def main():
    t0 = time.perf_counter()
    part_a_invalidation_chain()
    print()
    part_b_enumeration()
    print()
    part_c_boundaries()
    print()
    part_d_eviction()
    dt = time.perf_counter() - t0
    print("=" * 72)
    print(f"演示结束，实际计算耗时 {dt:.2f} 秒（无 sleep、无网络、无预录结果）。")
    if dt > 8.0:
        raise SystemExit(f"演示耗时 {dt:.2f}s 超过约定的约 8 秒")


if __name__ == "__main__":
    main()
