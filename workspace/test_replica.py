"""灾备复制器端到端测试（真实 HTTP 主站 + 真实复制器 + 真实持久化）。

覆盖：
  1. 持续复制：密封分段拉取、截面守恒、命令结果可查、只读语义
  2. 重复拉取不重复应用
  3. 进程重启：纪元 +1、从最后确认边界继续、截面摘要不变
  4. 旧纪元进程迟到更新（STALE_EPOCH，410）
  5. 缺失分段（gap）隔离且不污染截面，恢复后续传
  6. 错序分段隔离
  7. 事件哈希篡改 / 信封摘要篡改 / 摘要断链隔离
  8. 同序号正文不同（已确认重复 + 自洽分叉 alt_body）隔离
  9. 落后超限隔离
 10. 演练：锁定/验证/发任期/幂等再请求/取消失效/任期到期/并发演练拒绝
 11. 演练验证时又密封新分段 → ADVANCE_DURING_DRILL
 12. 全量审计（双链 + 守恒 + 重放等价 + 命令结果）
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from threading import Thread

from quota.http_server import create_server as create_primary
from replica import verifier
from replica.replicator import (
    ADVANCE_DURING_DRILL,
    BODY_CONFLICT,
    STALE_EPOCH,
    TERM_EXPIRED,
    Replicator,
)
from replica.http_server import create_server as create_replica_http
from replica.store import ReplicaStore

SEGMENT_SIZE = 4


# ----------------------------------------------------------------------
# HTTP 工具
# ----------------------------------------------------------------------

def http(method: str, url: str, body=None, headers=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            payload = {}
        return exc.code, payload


def wait_until(predicate, timeout=8.0, interval=0.05):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未成立（last={last!r}）")


# ----------------------------------------------------------------------
# 主站业务负载
# ----------------------------------------------------------------------

def seed_business(base: str, rounds: int = SEGMENT_SIZE) -> None:
    """注册三级节点 + 授约/授子约/消费循环，产生 >rounds*3 个业务事件。"""
    http("POST", f"{base}/api/bootstrap", {"capacity": 100_000_000})
    http("POST", f"{base}/api/nodes",
         {"type": "organization", "parent_id": "platform", "node_id": "O1"})
    http("POST", f"{base}/api/nodes",
         {"type": "organization", "parent_id": "platform", "node_id": "O2"})
    http("POST", f"{base}/api/nodes",
         {"type": "project", "parent_id": "O1", "node_id": "P1"})
    http("POST", f"{base}/api/nodes",
         {"type": "client", "parent_id": "P1", "node_id": "C1"})
    for i in range(rounds):
        # 平台 → 组织 → 项目 → 客户端逐级授约，保证每层有可用余额
        http("POST", f"{base}/api/leases",
             {"lessor_id": "platform", "holder_id": "O1", "amount": 1000,
              "lease_id": f"L-p-{i}", "command_id": f"pgrant-{i}"})
        http("POST", f"{base}/api/leases",
             {"lessor_id": "O1", "holder_id": "P1", "amount": 100,
              "lease_id": f"L-g-{i}", "command_id": f"grant-{i}"})
        http("POST", f"{base}/api/leases",
             {"lessor_id": "P1", "holder_id": "C1", "amount": 40,
              "lease_id": f"L-u-{i}", "command_id": f"sub-{i}"})
        http("POST", f"{base}/api/leases/usage",
             {"lease_id": f"L-u-{i}", "amount": 10,
              "usage_event_id": f"uid-{i}", "command_id": f"usage-{i}"})


def bump_capacity_n(base: str, n: int, tag: str, start: int = 90_000_000) -> None:
    """容量单调 +1 产生 n 个业务事件（1 亿容量足够大，不会触发缩容下限）。"""
    for i in range(n):
        http("POST", f"{base}/api/capacity",
             {"capacity": start + i + 1, "command_id": f"{tag}-{i}"})


# ----------------------------------------------------------------------
# 夹具：每个测试类一个真实主站，每个测试方法一个独立灾备库
# ----------------------------------------------------------------------

_PORT_LOCK = threading.Lock()
_NEXT_PORT = 8910


class ReplicaTestBase(unittest.TestCase):
    def setUp(self) -> None:
        global _NEXT_PORT
        with _PORT_LOCK:
            self.port = _NEXT_PORT
            _NEXT_PORT += 1
        self.tmp = tempfile.mkdtemp(prefix=f"dr-{self._testMethodName}-")
        self.primary_db = os.path.join(self.tmp, "primary.db")
        self.replica_db = os.path.join(self.tmp, "replica.db")
        self.primary: ThreadingHTTPServer = create_primary(
            "127.0.0.1", self.port, self.primary_db,
            segment_size=SEGMENT_SIZE, allow_feed_chaos=True,
        )
        self.server_thread = Thread(target=self.primary.serve_forever, daemon=True)
        self.server_thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.primary.shutdown()
        self.primary.service.shutdown()
        self.primary.server_close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_replicator(self, max_lag: int = 50, poll_interval: float = 0.03,
                        start: bool = True) -> Replicator:
        store = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep = Replicator(
            store, primary_url=self.base,
            max_lag_segments=max_lag, poll_interval=poll_interval,
        )
        if start:
            rep.start()
        return rep

    @staticmethod
    def stop(rep: Replicator | None) -> None:
        if rep is not None:
            try:
                rep.shutdown()
                rep.store.close()
            except Exception:  # noqa: BLE001
                pass

    def set_chaos(self, mode, index=None):
        return http("POST", f"{self.base}/replica/chaos",
                    {"mode": mode, "segment_index": index})


# ----------------------------------------------------------------------
# 1) 持续复制 / 幂等 / 只读
# ----------------------------------------------------------------------

class TestReplication(ReplicaTestBase):
    def test_01_replicates_and_state_conserves(self):
        seed_business(self.base, rounds=SEGMENT_SIZE)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        # 等到追平主站当前全部密封分段，使截面包含消费事件
        sealed = rep._fetch_primary_status()["sealed_index"]  # noqa: SLF001
        wait_until(lambda: rep.confirmed_index >= sealed)

        state = rep.current_state()
        # 守恒恒等式在每个确认截面都成立
        self.assertEqual(
            state["total_available"] + state["total_consumed"],
            state["global_capacity"],
        )
        self.assertGreater(state["total_consumed"], 0)
        # 命令结果（含 usage 幂等键与命令名）
        wait_until(lambda: rep.receipt("usage-0") is not None)
        r = rep.receipt("usage-0")
        self.assertEqual(r["result"]["command"], "usage")
        self.assertEqual(r["result"]["usage_event_id"], "uid-0")
        # 链上确认的每个分段都能在 /api/segments 看到
        digests = rep.store.segment_digests()
        self.assertEqual(set(digests), set(range(1, rep.confirmed_index + 1)))

    def test_02_duplicate_pull_ignored(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        env = rep.store.get_segment(1)
        out = rep.ingest(env, source="dup")
        self.assertEqual(out["outcome"], "duplicate_ignored")
        self.assertFalse(rep.store.has_open_gap())

    def test_03_read_only_http(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        httpd = create_replica_http("127.0.0.1", self.port + 100, rep)
        t = Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        self.addCleanup(httpd.shutdown)
        code, body = http(
            "POST", f"http://127.0.0.1:{self.port + 100}/api/leases", {"amount": 1})
        self.assertEqual(code, 405)
        self.assertEqual(body["error"], "READ_ONLY")
        code, body = http("GET", f"http://127.0.0.1:{self.port + 100}/api/state")
        self.assertEqual(code, 200)
        self.assertIn("global_capacity", body)


# ----------------------------------------------------------------------
# 2) 缺失 / 错序 / 篡改 / 分叉
# ----------------------------------------------------------------------

class TestQuarantine(ReplicaTestBase):
    def test_10_gap_then_recover(self):
        seed_business(self.base, rounds=SEGMENT_SIZE * 2)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        # 先确认 1 段，然后暂停，在第 2 段上设 gap
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.set_paused(True)
        wait_until(lambda: rep.status()["replication"]["primary_sealed_index"] >= 2)
        self.set_chaos("gap", 2)
        rep.set_paused(False)
        wait_until(lambda: any(
            q["reason"] == verifier.SEGMENT_INDEX_MISMATCH
            for q in rep.store.list_quarantined()))
        self.assertEqual(rep.confirmed_index, 1, "缺口存在时边界不得前进")
        # 恢复：去掉故障，应越过隔离确认第 2 段并把旧隔离标记消除
        self.set_chaos(None)
        wait_until(lambda: rep.confirmed_index >= 2)
        resolved = [q for q in rep.store.list_quarantined(True)
                    if q["reason"] == verifier.SEGMENT_INDEX_MISMATCH and q["resolved_at"]]
        self.assertTrue(resolved)

    def test_11_out_of_order(self):
        seed_business(self.base)
        rep = self.make_replicator(start=False)
        self.addCleanup(self.stop, rep)
        env = rep._fetch_segment(1)  # noqa: SLF001
        # 先篡改 index 成 2：第 2 段在边界 0 之前/之外 → 错序
        env["segment_index"] = 2
        with self.assertRaises(Exception) as cm:
            rep.ingest(env, source="ooo")
        self.assertEqual(cm.exception.reason, verifier.SEGMENT_INDEX_MISMATCH)
        self.assertEqual(rep.confirmed_index, 0)

    def test_12_event_hash_tampered(self):
        seed_business(self.base)
        rep = self.make_replicator(start=False)
        self.addCleanup(self.stop, rep)
        # 生产路径：主站故障注入改了事件载荷却保留旧 event_hash，
        # 信封 digest 必然也对不上 —— 两种断裂都应被隔离，边界纹丝不动。
        self.set_chaos("corrupt_event", 1)
        env = rep._fetch_segment(1)  # noqa: SLF001
        self.set_chaos(None)
        with self.assertRaises(Exception) as cm:
            rep.ingest(env, source="corrupt")
        self.assertIn(cm.exception.reason,
                      (verifier.EVENT_HASH_BROKEN, verifier.ENVELOPE_DIGEST_MISMATCH))
        self.assertEqual(rep.confirmed_index, 0)
        self.assertTrue(rep.store.list_quarantined())

    def test_13_envelope_digest_tampered(self):
        seed_business(self.base)
        rep = self.make_replicator(start=False)
        self.addCleanup(self.stop, rep)
        env = rep._fetch_segment(1)  # noqa: SLF001
        env["digest"] = "f" * 64
        with self.assertRaises(Exception) as cm:
            rep.ingest(env, source="digest")
        self.assertEqual(cm.exception.reason, verifier.ENVELOPE_DIGEST_MISMATCH)

    def test_14_prev_digest_broken(self):
        seed_business(self.base)
        rep = self.make_replicator(start=False)
        self.addCleanup(self.stop, rep)
        env = rep._fetch_segment(1)  # noqa: SLF001
        env["prev_digest"] = "a" * 64
        env["digest"] = verifier.envelope_digest(env)
        with self.assertRaises(Exception) as cm:
            rep.ingest(env, source="link")
        self.assertEqual(cm.exception.reason, verifier.CHAIN_DIGEST_BROKEN)

    def test_15_alt_body_fork_on_confirmed_index(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        confirmed_digest = rep.store.get_segment(1)["digest"]
        self.set_chaos("alt_body", 1)
        fork = rep._fetch_segment(1)  # noqa: SLF001
        self.set_chaos(None)
        self.assertNotEqual(fork["digest"], confirmed_digest)
        with self.assertRaises(Exception) as cm:
            rep.ingest(fork, source="alt")
        self.assertEqual(cm.exception.reason, BODY_CONFLICT)
        # 截面摘要纹丝不动
        self.assertEqual(rep.store.get_segment(1)["digest"], confirmed_digest)


# ----------------------------------------------------------------------
# 3) 重启 / 纪元围栏
# ----------------------------------------------------------------------

class TestRestartAndFencing(ReplicaTestBase):
    def test_20_restart_resumes_with_same_surface(self):
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        epoch1, boundary = rep.epoch, rep.confirmed_index
        digest1 = verifier.state_digest(rep.projection)
        self.stop(rep)

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        rep2.start()
        self.addCleanup(self.stop, rep2)
        self.assertEqual(rep2.epoch, epoch1 + 1)
        self.assertEqual(rep2.confirmed_index, boundary)
        self.assertEqual(verifier.state_digest(rep2.projection), digest1,
                         "重启重放的截面必须与重启前完全一致")

    def test_21_stale_epoch_rejected_410(self):
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        old_epoch = rep.epoch
        self.stop(rep)

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        rep2.start()
        self.addCleanup(self.stop, rep2)
        wait_until(lambda: rep2.epoch == old_epoch + 1)
        env = rep2.store.get_segment(1)
        with self.assertRaises(Exception) as cm:
            rep2.ingest(env, source="zombie", epoch=old_epoch)
        self.assertEqual(cm.exception.reason, STALE_EPOCH)
        self.assertEqual(cm.exception.status, 410)
        self.assertTrue(any(q["reason"] == STALE_EPOCH
                            for q in rep2.store.list_quarantined()))
        # 不带纪元的正常轮询不受影响
        wait_until(lambda: rep2.confirmed_index >= 1)


# ----------------------------------------------------------------------
# 4) 受控切换演练
# ----------------------------------------------------------------------

class TestDrill(ReplicaTestBase):
    def test_30_drill_issue_term_cancel_invalidates(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        drill = rep.create_drill("DR-OK", term_seconds=300)
        self.assertEqual(drill["state"], "ISSUED")
        locked = drill["locked_index"]

        surface = rep.serve_term(drill["term_token"])
        self.assertEqual(surface["locked_index"], locked)
        self.assertEqual(surface["frozen_digest"],
                         drill["report"]["frozen_digest"])
        self.assertIn("state", surface)
        self.assertGreater(len(surface["commands"]), 0)
        # 任期报告里全部检查通过
        self.assertTrue(all(c["ok"] for c in drill["report"]["checks"]))

        again = rep.create_drill("DR-OK")
        self.assertTrue(again.get("idempotent_replay"))
        self.assertEqual(again["term_token"], drill["term_token"])

        # 演练冻结期间写入不会被复制
        bump_capacity_n(self.base, 1, "during")
        time.sleep(0.25)
        self.assertEqual(rep.confirmed_index, locked)

        rep.cancel_drill("DR-OK")
        with self.assertRaises(Exception) as cm:
            rep.serve_term(drill["term_token"])
        self.assertEqual(cm.exception.reason, TERM_EXPIRED)
        wait_until(lambda: rep.confirmed_index > locked)

    def test_31_advance_during_validation_aborts(self):
        seed_business(self.base, rounds=SEGMENT_SIZE)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        boundary = rep.confirmed_index

        def disturb():
            time.sleep(0.3)
            # 写够密封一个新分段的事件数
            bump_capacity_n(self.base, SEGMENT_SIZE, f"adv{time.time():.0f}")

        Thread(target=disturb, daemon=True).start()
        with self.assertRaises(Exception) as cm:
            rep.create_drill("DR-ADV", validation_delay_ms=900)
        self.assertEqual(cm.exception.reason, ADVANCE_DURING_DRILL)
        self.assertEqual(rep.get_drill("DR-ADV")["state"], "ABORTED")
        # 失败演练释放冻结：之后可以追上
        wait_until(lambda: rep.confirmed_index > boundary)

    def test_32_term_expiry(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        drill = rep.create_drill("DR-TTL", term_seconds=1)
        token = drill["term_token"]
        self.assertIsNotNone(rep.serve_term(token))
        wait_until(lambda: rep.get_drill("DR-TTL")["state"] == "EXPIRED", timeout=4)
        with self.assertRaises(Exception) as cm:
            rep.serve_term(token)
        self.assertEqual(cm.exception.reason, TERM_EXPIRED)

    def test_33_only_one_active_drill(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.create_drill("DR-A", term_seconds=300)
        with self.assertRaises(Exception) as cm:
            rep.create_drill("DR-B")
        self.assertEqual(cm.exception.reason, "ALREADY_ACTIVE_DRILL")
        rep.cancel_drill("DR-A")

    def test_34_restart_orphan_validating_aborted(self):
        # 构造方式：先不启动轮询的 rep 手动无法进入 VALIDATING（需要确认截面），
        # 因此直接验证：已 ISSUED 的演练在重启后仍可服务（持久化任期）。
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        drill = rep.create_drill("DR-PERSIST", term_seconds=300)
        token = drill["term_token"]
        locked = drill["locked_index"]
        self.stop(rep)

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        rep2.start()
        self.addCleanup(self.stop, rep2)
        surface = rep2.serve_term(token)
        self.assertEqual(surface["locked_index"], locked)

    def test_35_full_audit_all_pass(self):
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        audit = rep.full_audit()
        self.assertTrue(audit["ok"], msg=json.dumps(audit, ensure_ascii=False)[:2000])
        names = {c["name"] for c in audit["checks"]}
        for required in ("segment_hash_chain", "non_negative_balances",
                         "global_conservation", "hierarchy_integrity",
                         "lease_bookkeeping", "replay_equivalence",
                         "command_receipts"):
            self.assertIn(required, names)


# ----------------------------------------------------------------------
# 5) 落后超限
# ----------------------------------------------------------------------

class TestLag(ReplicaTestBase):
    def test_40_lag_over_limit(self):
        seed_business(self.base)
        # 第一阶段：正常建站，先确认 1 段进入稳态
        rep = self.make_replicator(max_lag=50)
        wait_until(lambda: rep.confirmed_index >= 1)
        boundary = rep.confirmed_index
        # 第二阶段：把允许上限收紧到 0，主站再密封新分段即越限
        rep.max_lag_segments = 0
        bump_capacity_n(self.base, SEGMENT_SIZE, "lag")
        self.addCleanup(self.stop, rep)
        wait_until(lambda: any(
            q["reason"] == "LAG_OVER_LIMIT"
            for q in rep.store.list_quarantined()))
        self.assertTrue(rep.status()["replication"]["lag_over_limit"])
        # 越限期间边界停住，不暴露陈旧截面为“已确认”
        self.assertEqual(rep.confirmed_index, boundary)


# ----------------------------------------------------------------------
# 6) 任期到期恢复：主动收口 / 410 / 同步续传 / 单次终态 / 不跳异常批次
# ----------------------------------------------------------------------

class TestTermRecovery(ReplicaTestBase):
    def _finished_audits(self, store, drill_id):
        return [
            a for a in store.list_audit(500)
            if a["kind"] == "drill_finished" and a["detail"]["drill_id"] == drill_id
        ]

    def test_50_sweeper_closes_without_any_request_and_sync_resumes(self):
        """无人再请求旧凭证：截止时刻到达后 sweeper 仍主动收口，
        冻结解除，同步从停下的位置继续（不跳过任何分段）。"""
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        locked = rep.confirmed_index
        drill = rep.create_drill("DR-AUTO", term_seconds=1)
        token = drill["term_token"]
        # 冻结期间主站密封新分段
        bump_capacity_n(self.base, SEGMENT_SIZE, "after-lock")
        # 全程不调用 serve_term：记录必须自行收口
        wait_until(lambda: rep.get_drill("DR-AUTO")["state"] == "EXPIRED", timeout=4)
        d = rep.get_drill("DR-AUTO")
        self.assertIsNone(d["term_token"])
        self.assertIsNotNone(d["finished_at"])
        # 旧凭证稳定 410
        with self.assertRaises(Exception) as cm:
            rep.serve_term(token)
        self.assertEqual(cm.exception.status, 410)
        self.assertEqual(cm.exception.reason, TERM_EXPIRED)
        # 后台同步从冻结点续上，且恰好只确认被冻结的那一段（不跳号）
        wait_until(lambda: rep.confirmed_index >= locked + 1, timeout=6)
        self.assertEqual(rep.confirmed_index, locked + 1)
        digests = rep.store.segment_digests()
        self.assertEqual(set(digests), set(range(1, rep.confirmed_index + 1)))
        # 只有一次终态、一条收口审计
        self.assertEqual(len(self._finished_audits(rep.store, "DR-AUTO")), 1)

    def test_51_expiry_collides_with_cancel_and_concurrent_reads(self):
        """定时结束与人工撤销、并发读取同时相撞：只落一次终态、一条审计。"""
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        drill = rep.create_drill("DR-RACE", term_seconds=0.6)
        token = drill["term_token"]

        barrier = threading.Barrier(9)

        def reader():
            barrier.wait()
            for _ in range(40):
                try:
                    rep.serve_term(token)
                except Exception:
                    pass
                time.sleep(0.004)

        threads = [Thread(target=reader) for _ in range(8)]
        for t in threads:
            t.start()
        barrier.wait()
        time.sleep(0.62)  # 与 sweeper 的到期收口竞争
        try:
            rep.cancel_drill("DR-RACE")
        except Exception:
            pass
        for t in threads:
            t.join()
        time.sleep(0.3)

        d = rep.get_drill("DR-RACE")
        self.assertIn(d["state"], ("EXPIRED", "CANCELLED"))
        finals = self._finished_audits(rep.store, "DR-RACE")
        self.assertEqual(len(finals), 1, f"收口审计必须恰好一条: {finals}")
        self.assertEqual({finals[0]["detail"]["state"]}, {d["state"]})
        # 再读 / 再取消均为幂等，不再产生终态与审计
        with self.assertRaises(Exception):
            rep.serve_term(token)
        rep.cancel_drill("DR-RACE")
        self.assertEqual(len(self._finished_audits(rep.store, "DR-RACE")), 1)

    def test_52_expired_while_offline_finalized_on_restart(self):
        """截止时刻在停机期间到达：重启构造时立即收口（不依赖 sweeper），
        旧凭证即刻 410，同步恢复。"""
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        locked = rep.confirmed_index
        drill = rep.create_drill("DR-RESTART", term_seconds=300)
        token = drill["term_token"]
        bump_capacity_n(self.base, SEGMENT_SIZE, "offline")
        self.stop(rep)

        import sqlite3
        conn = sqlite3.connect(self.replica_db)
        conn.execute(
            "UPDATE drills SET term_expires_at = ? WHERE drill_id = 'DR-RESTART'",
            (time.time() - 10,),
        )
        conn.commit()
        conn.close()

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        self.addCleanup(self.stop, rep2)
        # 尚未 start()（sweeper/poller 都没跑）：构造阶段就应已收口
        self.assertEqual(rep2.get_drill("DR-RESTART")["state"], "EXPIRED")
        with self.assertRaises(Exception) as cm:
            rep2.serve_term(token)
        self.assertEqual(cm.exception.status, 410)
        self.assertIsNone(rep2.store.active_drill())
        self.assertEqual(len(self._finished_audits(store2, "DR-RESTART")), 1)

        rep2.start()
        wait_until(lambda: rep2.confirmed_index >= locked + 1, timeout=6)
        self.assertEqual(rep2.confirmed_index, locked + 1)

    def test_53_two_processes_finalize_only_once(self):
        """两个复制器进程共用同一 DB 同时收口同一到期演练：
        SQLite 条件更新保证只有一个赢家，终态/审计各一条。"""
        seed_business(self.base)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.create_drill("DR-2PROC", term_seconds=300)

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        self.addCleanup(self.stop, rep2)
        rep2.start()

        import sqlite3
        conn = sqlite3.connect(self.replica_db)
        conn.execute(
            "UPDATE drills SET term_expires_at = ? WHERE drill_id = 'DR-2PROC'",
            (time.time() - 1,),
        )
        conn.commit()
        conn.close()

        # 两边都立即扫一遍（竞争同一行）
        rep.sweep_due_terms()
        rep2.sweep_due_terms()
        time.sleep(0.3)

        d1 = rep.get_drill("DR-2PROC")
        d2 = rep2.get_drill("DR-2PROC")
        self.assertEqual(d1["state"], "EXPIRED")
        self.assertEqual(d2["state"], "EXPIRED")
        finals = self._finished_audits(rep.store, "DR-2PROC")
        self.assertEqual(len(finals), 1)

    def test_54_recovery_repairs_state_without_audit(self):
        """终态已落但收口审计缺失（两步提交间崩溃）：重启补一条审计，
        不改动终态。"""
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.create_drill("DR-HALF1", term_seconds=300)
        self.stop(rep)

        import sqlite3
        conn = sqlite3.connect(self.replica_db)
        conn.execute(
            "UPDATE drills SET state='EXPIRED', term_token=NULL, term_expires_at=NULL,"
            " finished_at=?, fail_reason='TERM_EXPIRED', updated_at=?"
            " WHERE drill_id='DR-HALF1'",
            (time.time() - 5, time.time() - 5),
        )
        conn.commit()
        conn.close()

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        self.addCleanup(self.stop, rep2)
        self.assertEqual(rep2.get_drill("DR-HALF1")["state"], "EXPIRED")
        finals = self._finished_audits(store2, "DR-HALF1")
        self.assertEqual(len(finals), 1)
        self.assertEqual(finals[0]["detail"]["state"], "EXPIRED")

    def test_55_recovery_repairs_audit_without_state(self):
        """收口审计已在但行残留 ISSUED（镜像半截）：重启把行补到既有终态，
        不重复写审计。"""
        seed_business(self.base)
        rep = self.make_replicator()
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.create_drill("DR-HALF2", term_seconds=300)
        self.stop(rep)

        import sqlite3
        conn = sqlite3.connect(self.replica_db)
        conn.execute(
            "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
            (time.time() - 5, "drill_finished",
             json.dumps({"drill_id": "DR-HALF2", "state": "EXPIRED",
                         "reason": "TERM_EXPIRED", "detail": "x"}, sort_keys=True)),
        )
        conn.execute(
            "UPDATE drills SET term_expires_at = ? WHERE drill_id = 'DR-HALF2'",
            (time.time() - 5,),
        )
        conn.commit()
        conn.close()

        store2 = ReplicaStore(self.replica_db, segment_size=SEGMENT_SIZE)
        rep2 = Replicator(store2, primary_url=self.base, poll_interval=0.03)
        self.addCleanup(self.stop, rep2)
        self.assertEqual(rep2.get_drill("DR-HALF2")["state"], "EXPIRED")
        finals = self._finished_audits(store2, "DR-HALF2")
        self.assertEqual(len(finals), 1)

    def test_56_recovery_does_not_skip_pending_quarantine(self):
        """异常批次（gap）尚未处置时演练到期：收口后续传必须先补上缺口
        分段、消除旧隔离，再继续前进，绝不跳过异常分段。"""
        seed_business(self.base, rounds=SEGMENT_SIZE * 2)
        rep = self.make_replicator()
        self.addCleanup(self.stop, rep)
        wait_until(lambda: rep.confirmed_index >= 1)
        rep.set_paused(True)
        wait_until(lambda: rep.status()["replication"]["primary_sealed_index"] >= 2)
        self.set_chaos("gap", 2)
        rep.set_paused(False)
        wait_until(lambda: any(
            q["reason"] == verifier.SEGMENT_INDEX_MISMATCH
            for q in rep.store.list_quarantined()))
        self.assertEqual(rep.confirmed_index, 1)

        drill = rep.create_drill("DR-GAP", term_seconds=1)
        self.assertEqual(drill["locked_index"], 1)
        wait_until(lambda: rep.get_drill("DR-GAP")["state"] == "EXPIRED", timeout=4)
        # 缺口未恢复前，边界仍停在 1：恢复过程不得跳过段 2
        time.sleep(0.4)
        self.assertEqual(rep.confirmed_index, 1)

        self.set_chaos(None)
        wait_until(lambda: rep.confirmed_index >= 2, timeout=8)
        self.assertEqual(rep.confirmed_index, 2)
        resolved = [
            q for q in rep.store.list_quarantined(True)
            if q["reason"] == verifier.SEGMENT_INDEX_MISMATCH and q["resolved_at"]
        ]
        self.assertTrue(resolved, "缺口恢复后旧隔离必须被消除")


if __name__ == "__main__":
    unittest.main(verbosity=2)
