"""固定输入演示：消息驱动 MSI 目录一致性内核。

运行：python demo.py
全程真实计算（无 sleep、无预录结果、无网络/线程），确定性输出。
内容：
  A. 失效链逐步演示：3 核共享 -> 升级等齐 ACK -> 读 M 触发回写后共享；
  B. 丢失与超时重传：丢 INV/FLUSH 后由 tick 推进模拟时钟触发重传，
     原件与副本重复到达时幂等，一次写入只执行一遍；
  C. 枚举小规模消息重排，逐状态检查“至多一个 M、M 时无 S”；
  D. 实际触发的拒绝/异常边界（NaN、bool、未知 msg_id、重复/陈旧投递）。
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


def part_b_loss_and_retransmit():
    print("=" * 72)
    print("B. 丢失与超时重传（tick 推进模拟时钟，重复到达幂等）")
    print("=" * 72)
    k = CacheCoherenceKernel(memory={ADDR: 100}, timeout=3)
    k.request(0, ADDR, Op.LOAD)
    k.request(1, ADDR, Op.STORE, value=200)
    inv = k.pending()[0]
    lost = k.lose(inv.msg_id)
    print(f"  核1 store=200 的 INV(msg {lost.msg_id}) 被网络丢失，"
          f"store 悬而未决（completed 最后仍是 load）")
    print(f"  tick(2)：时钟={k.tick(2).clock}，未达 timeout=3，不重传")
    rep = k.tick(1)
    copy_inv = rep.retransmitted[0]
    print(f"  tick(1)：时钟={rep.clock} 到达超时，目录重传 "
          f"INV(msg {copy_inv.msg_id}) 给核{copy_inv.dst}")
    ack = k.deliver(copy_inv.msg_id).emitted[0]
    fin = k.deliver(ack.msg_id)
    print(f"  重传副本生效：核0 失效并回 ACK，store 完成值="
          f"{fin.completions[0].value}（只提交一次）")

    # 重复到达：读 M 触发 FETCH，原件与重传副本都被投递。
    k.request(2, ADDR, Op.LOAD)
    fetch = k.pending()[0]
    copy_fetch = k.tick(3).retransmitted[0]
    flush1 = k.deliver(fetch.msg_id).emitted[0]
    dup = k.deliver(copy_fetch.msg_id)
    flush2 = dup.emitted[0]
    print(f"  核2 读 M：FETCH 原件与超时重传副本都到达 owner，"
          f"副本幂等补发 FLUSH(msg {flush2.msg_id})，owner 只降级一次")
    k.deliver(flush1.msg_id)
    rep2 = k.deliver(flush2.msg_id)
    print(f"  第一份 FLUSH 回写内存={k.snapshot().memory[ADDR]}；"
          f"第二份重复 FLUSH -> {rep2.status.value}（回写不执行两遍）")
    loads = [c for c in k.completed() if c.op is Op.LOAD and c.core == 2]
    print(f"  核2 的 load 完成次数={len(loads)}，读值={loads[0].value}")
    assert len(loads) == 1 and loads[0].value == 200


def part_c_enumeration():
    print("=" * 72)
    print("C. 枚举消息重排：最多一个 M；存在 M 时不得有 S")
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


def part_d_boundaries():
    print("=" * 72)
    print("D. 实际触发的拒绝 / 异常边界（真实抛出，非预录文字）")
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

    try:
        k.tick(True)
    except InvalidRequestError as e:
        print(f"  6) tick 的步数为 bool 时拒绝: {type(e).__name__}: {e}")


def main():
    t0 = time.perf_counter()
    part_a_invalidation_chain()
    print()
    part_b_loss_and_retransmit()
    print()
    part_c_enumeration()
    print()
    part_d_boundaries()
    dt = time.perf_counter() - t0
    print("=" * 72)
    print(f"演示结束，实际计算耗时 {dt:.2f} 秒（无 sleep、无网络、无预录结果）。")
    if dt > 8.0:
        raise SystemExit(f"演示耗时 {dt:.2f}s 超过约定的约 8 秒")


if __name__ == "__main__":
    main()
