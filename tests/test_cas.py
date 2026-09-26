"""CAS（compare-and-swap）增量测试。

覆盖题面验收点：
- 两个核同时把 0 改成不同值：恰好一个成功；
- CAS 夹在普通写之间：按目录授予（FIFO）顺序线性化；
- 响应重复送达：同 txn_id 重传返回原结果、不再次交换；
- 被延迟的失效确认不能把 CAS 完成提前发布；
- 独立标量参考机（tests/reference.py，不导入 msi_kernel）逐完成事件对照；
- 参数校验异常路径。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import unittest

from msi_kernel import (
    CacheCoherenceKernel,
    Completion,
    InvalidRequestError,
    Op,
    Request,
    State,
    drain,
    explore,
)

import reference


def _flat(completed):
    """完成事件 → 参考机输入（cas 带 success/expected/new_value）。"""
    out = []
    for c in completed:
        if c.op is Op.CAS:
            out.append((c.addr, "cas", c.value, c.success, c.expected,
                        c.new_value))
        else:
            out.append((c.addr, c.op.value, c.value))
    return out


class TestConcurrentCas(unittest.TestCase):
    """两个核同时把 0 改成不同值：恰好一个成功。"""

    def _scenario(self):
        # 先让三核都持有 S（内存值 0），再同时发起 CAS：
        # 赢家的失效集含两个核，消息存在真实重排空间。
        return [
            Request(0, 0, Op.LOAD),
            Request(1, 0, Op.LOAD),
            Request(2, 0, Op.LOAD),
            Request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=10),
            Request(1, 0, Op.CAS, expected=0, new_value=200, txn_id=11),
        ]

    def test_exactly_one_success(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch(self._scenario())
        drain(k)
        cas = [c for c in k.completed() if c.op is Op.CAS]
        self.assertEqual(len(cas), 2)
        wins = [c for c in cas if c.success]
        self.assertEqual(len(wins), 1)
        # 赢家看到旧值 0，输家看到赢家写入的值。
        self.assertEqual(wins[0].value, 0)
        loser = [c for c in cas if not c.success][0]
        self.assertEqual(loser.value, wins[0].new_value)
        # 最终值 = 赢家新值（通过后续 load 验证）。
        k.request(2, 0, Op.LOAD)
        drain(k)
        self.assertEqual(k.completed()[-1].value, wins[0].new_value)

    def test_all_reorderings_match_reference(self):
        # 枚举全部消息重排：每个调度都恰好一个成功，且逐完成事件
        # 与独立标量参考机一致（返回旧值 + 最终值）。
        res = explore(CacheCoherenceKernel, self._scenario(), all_paths=True)
        self.assertFalse(res.exhausted)
        self.assertEqual(res.violations, ())
        self.assertGreater(len(res.schedules), 1)
        for path, completed in res.schedules.items():
            cas = [c for c in completed if c.op is Op.CAS]
            self.assertEqual(sum(1 for c in cas if c.success), 1,
                             f"调度 {path} 成功数不为 1")
            reference.check_completion_sequence(
                _flat(completed), initial_memory={0: 0})


class TestCasBetweenStores(unittest.TestCase):
    """CAS 夹在普通写之间：同地址按目录授予顺序线性化。"""

    def test_store_cas_store_fifo(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request_batch([
            Request(0, 0, Op.STORE, value=5),                  # txn1
            Request(1, 0, Op.CAS, expected=5, new_value=7),    # txn2
            Request(2, 0, Op.STORE, value=9),                  # txn3
            Request(0, 0, Op.CAS, expected=5, new_value=99),   # txn4
        ])
        drain(k)
        comps = k.completed()
        self.assertEqual([c.txn_id for c in comps], [1, 2, 3, 4])
        cas1, cas2 = comps[1], comps[3]
        # 第一个 CAS 看到 store=5，成功改写为 7。
        self.assertEqual((cas1.value, cas1.success), (5, True))
        # 第二个 CAS 排在 store=9 之后，看到 9 而非 5，失败。
        self.assertEqual((cas2.value, cas2.success), (9, False))
        reference.check_completion_sequence(_flat(comps),
                                            initial_memory={0: 0})
        # 最终值为 9。
        self.assertEqual(comps[-1].op, Op.CAS)
        k.request(1, 0, Op.LOAD)
        drain(k)
        self.assertEqual(k.completed()[-1].value, 9)

    def test_cas_reads_value_written_by_queued_store_not_stale_cache(self):
        # 核1 持有旧值 S 缓存；核0 先 store（失效核1），核1 的 CAS 排在
        # 后面：CAS 必须看到 store 的新值，而不是发请求时缓存里的旧值。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(1, 0, Op.LOAD)                       # 核1 S，值 0
        k.request_batch([
            Request(0, 0, Op.STORE, value=42),         # 失效核1
            Request(1, 0, Op.CAS, expected=42, new_value=43),
        ])
        drain(k)
        cas = [c for c in k.completed() if c.op is Op.CAS][0]
        self.assertEqual((cas.value, cas.success), (42, True))


class TestCasReplay(unittest.TestCase):
    """响应重复送达：同 txn_id 重传返回原结果，不得再次交换。"""

    def test_replay_returns_original_without_second_swap(self):
        k = CacheCoherenceKernel(memory={0: 0})
        rep1 = k.request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=7)
        self.assertEqual(rep1.completions[0].value, 0)
        self.assertTrue(rep1.completions[0].success)
        # 重传：参数完全一致 → 返回原完成事件，不再次执行。
        rep2 = k.request(0, 0, Op.CAS, expected=0, new_value=100, txn_id=7)
        self.assertFalse(rep2.queued)
        self.assertEqual(rep2.completions, rep1.completions)
        self.assertEqual(len([c for c in k.completed() if c.txn_id == 7]), 1)
        # 若被再次交换，当前值仍是 100、expected=0 会失败；
        # 用一个新 CAS 证明值只被交换了一次。
        k.request(1, 0, Op.CAS, expected=100, new_value=5)
        drain(k)
        self.assertTrue(k.completed()[-1].success)

    def test_replay_while_inflight_reports_queued(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(1, 0, Op.LOAD)                        # 核1 S
        k.request(2, 0, Op.LOAD)                        # 核2 S
        rep1 = k.request(1, 0, Op.CAS, expected=0, new_value=8, txn_id=9)
        self.assertFalse(rep1.queued)                   # 已启动（等 ACK）
        self.assertEqual(rep1.completions, ())
        rep2 = k.request(1, 0, Op.CAS, expected=0, new_value=8, txn_id=9)
        self.assertTrue(rep2.queued)                    # 在途重传：不重复入队
        self.assertEqual(rep2.completions, ())
        drain(k)
        self.assertEqual(len([c for c in k.completed() if c.txn_id == 9]), 1)

    def test_same_id_different_params_rejected(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.CAS, expected=0, new_value=1, txn_id=7)
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.CAS, expected=0, new_value=2, txn_id=7)
        self.assertEqual(cm.exception.field, "txn_id")
        with self.assertRaises(InvalidRequestError):
            k.request(1, 0, Op.CAS, expected=0, new_value=1, txn_id=7)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.STORE, value=1, txn_id=7)

    def test_batch_replay_and_conflict(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.CAS, expected=0, new_value=1, txn_id=3)
        # 批内重放（参数一致）返回原结果。
        reps = k.request_batch([
            Request(0, 0, Op.CAS, expected=0, new_value=1, txn_id=3)])
        self.assertEqual(reps[0].completions[0].value, 0)
        # 批内同 id 不同参数：整批拒绝，内核零变更。
        before = k.snapshot()
        with self.assertRaises(InvalidRequestError):
            k.request_batch([
                Request(0, 0, Op.CAS, expected=0, new_value=9, txn_id=3)])
        self.assertEqual(k.completed(), before.completed)


class TestCasDelayedAck(unittest.TestCase):
    """被延迟的失效确认不能把 CAS 完成提前发布。"""

    def test_no_completion_before_all_acks(self):
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(0, 0, Op.LOAD)
        k.request(1, 0, Op.LOAD)
        k.request(2, 0, Op.LOAD)                        # 三核共享
        k.request(0, 0, Op.CAS, expected=0, new_value=1)
        invs = list(k.pending())
        self.assertEqual(len(invs), 2)
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        # 投递一个 INV：核端已失效，但 ACK 未回 → 不得完成。
        ack1 = k.deliver(invs[0].msg_id).emitted[0]
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        # 第二个 INV 也送达、两个 ACK 只回了一个 → 仍不得完成。
        ack2 = k.deliver(invs[1].msg_id).emitted[0]
        k.deliver(ack1.msg_id)
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        # 全部 ACK 到齐才完成。
        rep = k.deliver(ack2.msg_id)
        cas = [c for c in rep.completions if c.op is Op.CAS]
        self.assertEqual(len(cas), 1)
        self.assertEqual((cas[0].value, cas[0].success), (0, True))

    def test_cas_waits_for_flush_then_inv(self):
        # 核2 持有 M（值 30）；核0 CAS 必须等 FETCH/FLUSH 回写并失效核2
        # 之后才比较，看到的一定是 30。
        k = CacheCoherenceKernel(memory={0: 0})
        k.request(2, 0, Op.STORE, value=30)
        k.request(0, 0, Op.CAS, expected=30, new_value=31)
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        fetch = k.pending()[0]
        flush = k.deliver(fetch.msg_id).emitted[0]
        inv = k.deliver(flush.msg_id).emitted[0]
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        ack = k.deliver(inv.msg_id).emitted[0]
        self.assertEqual([c for c in k.completed() if c.op is Op.CAS], [])
        rep = k.deliver(ack.msg_id)
        cas = [c for c in rep.completions if c.op is Op.CAS]
        self.assertEqual((cas[0].value, cas[0].success), (30, True))
        self.assertIs(k.snapshot().caches[2][0].state, State.INVALID)


class TestCasValidation(unittest.TestCase):
    """CAS 参数校验异常路径。"""

    def test_missing_params(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.CAS, new_value=1)
        self.assertEqual(cm.exception.field, "expected")
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.CAS, expected=0)
        self.assertEqual(cm.exception.field, "new_value")

    def test_bad_types(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected=True, new_value=1)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected=0, new_value=float("nan"))
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected=0, new_value=float("inf"))
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.CAS, expected=0.5, new_value=1)

    def test_misplaced_fields(self):
        k = CacheCoherenceKernel()
        with self.assertRaises(InvalidRequestError) as cm:
            k.request(0, 0, Op.CAS, value=1, expected=0, new_value=1)
        self.assertEqual(cm.exception.field, "value")
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.LOAD, expected=0, new_value=1)
        with self.assertRaises(InvalidRequestError):
            k.request(0, 0, Op.STORE, value=1, expected=0)
        # 全部拒绝后内核无变化。
        self.assertEqual(k.completed(), ())
        self.assertEqual(k.pending(), ())


if __name__ == "__main__":
    unittest.main()
