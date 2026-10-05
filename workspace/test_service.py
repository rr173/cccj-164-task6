"""端到端测试：启动真实 HTTP 服务，覆盖功能/并发/重启/审计。

运行：python3 test_service.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Client:
    def __init__(self, base: str):
        self.base = base

    def call(self, method: str, path: str, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body, headers=None):
        return self.call("POST", path, body, headers)


class Server:
    def __init__(self):
        self.port = free_port()
        self.db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db.close()
        os.unlink(self.db.name)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "quota.http_server",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--db", self.db.name],
            cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.client = Client(f"http://127.0.0.1:{self.port}")
        self._wait_ready()

    def _wait_ready(self):
        deadline = time.time() + 15
        last = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise RuntimeError("服务提前退出:\n" + out)
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/state", timeout=1)
                return
            except Exception as e:
                last = e
                time.sleep(0.1)
        raise RuntimeError(f"服务未就绪: {last}")

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def restart(self):
        self.stop()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "quota.http_server",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--db", self.db.name],
            cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self._wait_ready()


# ----------------------------------------------------------------------

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}  {detail}")


def audit_ok(c: Client):
    code, a = c.get("/api/audit")
    failed = [x for x in a["checks"] if not x["ok"]]
    return code == 200 and a["ok"], failed


def setup_hierarchy(c: Client, cap=1000):
    c.post("/api/bootstrap", {"capacity": cap})
    _, o = c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-1"})
    _, p = c.post("/api/nodes", {"type": "project", "parent_id": "org-1", "node_id": "proj-1"})
    _, cl = c.post("/api/nodes", {"type": "client", "parent_id": "proj-1", "node_id": "cli-1"})
    return o["node_id"], p["node_id"], cl["node_id"]


# ----------------------------------------------------------------------

def test_basic_flow(srv: Server):
    print("\n[1] 基础分配 / 分层租约 / 记账守恒")
    c = srv.client
    o, p, cl = setup_hierarchy(c, 1000)

    code, _ = c.post("/api/leases", {"lessor_id": "platform", "holder_id": o, "amount": 400, "lease_id": "L-org"})
    check("平台 → 组织 400", code == 201, f"code={code}")
    code, _ = c.post("/api/leases", {"lessor_id": o, "holder_id": p, "amount": 300, "lease_id": "L-proj"})
    check("组织 → 项目 300", code == 201, f"code={code}")
    code, _ = c.post("/api/leases", {"lessor_id": p, "holder_id": cl, "amount": 200, "lease_id": "L-cli"})
    check("项目 → 客户端 200", code == 201, f"code={code}")

    st = c.get("/api/state")[1]
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("分层余额 platform=600", bal["platform"] == 600, str(bal))
    check("分层余额 org-1=100（400 入 - 300 出）", bal["org-1"] == 100, str(bal))
    check("分层余额 proj-1=100（300 入 - 200 出）", bal["proj-1"] == 100, str(bal))
    check("分层余额 cli-1=200", bal["cli-1"] == 200, str(bal))
    check("Σ可用+消费=容量", st["total_available"] + st["total_consumed"] == 1000)

    # 超父级可用额度 → 拒绝
    code, err = c.post("/api/leases", {"lessor_id": "proj-1", "holder_id": "cli-1", "amount": 150, "lease_id": "L-over"})
    check("超出父级可用 → 409", code == 409, f"code={code} {err}")

    # 非法层级
    code, _ = c.post("/api/leases", {"lessor_id": "platform", "holder_id": "proj-1", "amount": 1})
    check("跨级授约（平台→项目）→ 400", code == 400, f"code={code}")


def test_usage_release_consumed_kept(srv: Server):
    print("\n[2] 幂等扣减；归还只退未消费，已消费不消失")
    c = srv.client
    code, u1 = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1"})
    check("用量 70", code == 201, f"code={code}")
    code, r1b = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1"})
    check("相同请求重试 → 200 幂等重放，不重复扣减", code == 200 and r1b.get("replayed") is True,
          f"code={code}")
    st0 = c.get("/api/state")[1]
    check("重放后总消费仍为 70", st0["total_consumed"] == 70, str(st0["total_consumed"]))
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1",
                                           "command_id": "other-cmd"})
    check("同一回执换 command_id/不同语义 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 150, "usage_event_id": "evt-2"})
    check("超租约剩余的用量 → 409", code == 409, f"code={code}")

    st = c.get("/api/state")[1]
    l = next(x for x in st["leases"] if x["id"] == "L-cli")
    check("租约剩余 = 130", l["remaining"] == 130, str(l))
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("消费只扣持有者 cli=130", bal["cli-1"] == 130, str(bal))

    # 关闭必须自底向上：下层余量未回收时，先关上层会让上层余额为负
    code, _ = c.post("/api/leases/release", {"lease_id": "L-org"})
    check("下层仍占用时先归还组织租约 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/release", {"lease_id": "L-proj"})
    check("下层仍占用时先归还项目租约 → 409", code == 409, f"code={code}")

    code, r = c.post("/api/leases/release", {"lease_id": "L-cli"})
    check("归还客户端租约", code == 201, f"code={code}")
    check("释放未消费 130", r["released_amount"] == 130, str(r))
    check("已消费 70 保留", r["consumed_amount"] == 70, str(r))

    code, r = c.post("/api/leases/release", {"lease_id": "L-proj"})
    check("再归还项目租约：释放 230（300-70 消费沿链传播）", code == 201 and r["released_amount"] == 230, str(r))
    code, r = c.post("/api/leases/release", {"lease_id": "L-org"})
    check("归还组织租约：释放 330（400-70）", code == 201 and r["released_amount"] == 330, str(r))

    st = c.get("/api/state")[1]
    check("全部关闭后平台恢复 = 容量-消费 = 930", st["nodes"][0]["available"] == 930, str(st["nodes"][0]))
    check("总消费仍为 70（未因撤销/归还消失）", st["total_consumed"] == 70, str(st["total_consumed"]))
    check("Σ可用(930)+消费(70)=1000", st["total_available"] + st["total_consumed"] == 1000)

    # 已关闭租约拒绝任何操作
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 1, "usage_event_id": "evt-9"})
    check("已关闭租约不能再扣减 → 409", code == 409, f"code={code}")


def test_command_replay(srv: Server):
    print("\n[3] 命令幂等重放（同一 command_id 返回同一结果）")
    c = srv.client
    body = {"type": "organization", "parent_id": "platform", "node_id": "org-replay", "command_id": "cmd-rep-1"}
    code1, r1 = c.post("/api/nodes", body)
    code2, r2 = c.post("/api/nodes", body)
    check("首次 201、重放 200", code1 == 201 and code2 == 200, f"{code1}/{code2}")
    check("重放返回同一节点 ID", r1["node_id"] == r2["node_id"] == "org-replay")
    check("重放标记 replayed=true", r2.get("replayed") is True)

    code, _ = c.post("/api/nodes", {**body, "node_id": "org-other"})
    check("相同 command_id 不同请求体 → 409", code == 409, f"code={code}")

    # Idempotency-Key 头
    _, r3 = c.post("/api/nodes",
                   {"type": "project", "parent_id": "org-replay", "node_id": "p-hdr"},
                   headers={"Idempotency-Key": "hdr-key-1"})
    code4, r4 = c.post("/api/nodes",
                       {"type": "project", "parent_id": "org-replay", "node_id": "p-hdr"},
                       headers={"Idempotency-Key": "hdr-key-1"})
    check("Header 幂等键同样生效", code4 == 200 and r4["node_id"] == r3["node_id"])


def test_capacity_shrink(srv: Server):
    print("\n[4] 父级缩容保护（平台 & 中间层 revoke）")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-s"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-s", "node_id": "proj-s"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-s", "node_id": "cli-s"})
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-s", "amount": 200, "lease_id": "Ls-org"})
    c.post("/api/leases", {"lessor_id": "org-s", "holder_id": "proj-s", "amount": 120, "lease_id": "Ls-proj"})
    c.post("/api/leases", {"lessor_id": "proj-s", "holder_id": "cli-s", "amount": 80, "lease_id": "Ls-cli"})

    # 此时平台已承担：历史消费 70（前序）+ 新授出 200 = 270 不可缩
    code, _ = c.post("/api/capacity", {"capacity": 269})
    check("平台缩容到下限以下 → 409", code == 409, f"code={code}")

    # 消费 30 不改变平台余额（关闭时才退），下限仍 270；缩到 270 恰好安全
    c.post("/api/leases/usage", {"lease_id": "Ls-cli", "amount": 30, "usage_event_id": "evt-s1"})
    code, r = c.post("/api/capacity", {"capacity": 270})
    check("缩容到 270（=已承诺/消费下限）成功", code == 201, str(r))
    code, _ = c.post("/api/capacity", {"capacity": 269})
    check("继续低于下限 → 409", code == 409, f"code={code}")

    # 撤销必须自底向上；已消费部分不会退回
    code, _ = c.post("/api/leases/revoke", {"lease_id": "Ls-org"})
    check("子级仍有占用时撤销上层 → 409", code == 409, f"code={code}")
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-cli"})  # 退 50 给 proj
    check("撤销客户端租约释放 50（80-30）", code == 201 and r["released_amount"] == 50, str(r))
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-proj"})  # proj 余 90，退 90
    check("撤销项目租约释放 90（120-30）", code == 201 and r["released_amount"] == 90, str(r))
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-org"})
    check("撤销组织租约释放 170（200-30）", code == 201 and r["released_amount"] == 170, str(r))
    st = c.get("/api/state")[1]
    check("消费累计 = 70(前序) + 30 = 100，且永久保留", st["total_consumed"] == 100,
          str(st["total_consumed"]))


def test_expiry_and_offline(srv: Server):
    print("\n[5] 租约到期（含级联）与客户端离线")
    c = srv.client
    # 前序测试把全局容量缩到 180，先扩回 1000
    code, _ = c.post("/api/capacity", {"capacity": 1000})
    assert code == 201, f"恢复容量失败 {code}"
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-e"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-e", "node_id": "proj-e"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-e", "node_id": "cli-e"})

    # 项目租约 1 秒到期（早于客户端 3600 秒）：
    # 到期先挂起(pending_expired)，不抽走仍支撑下游的余量；
    # 待客户端租约释放后，项目租约才结算退款。
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-e", "amount": 100, "lease_id": "Le-org", "ttl_seconds": 3600})
    c.post("/api/leases", {"lessor_id": "org-e", "holder_id": "proj-e", "amount": 80, "lease_id": "Le-proj", "ttl_seconds": 1})
    c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 50, "lease_id": "Le-cli", "ttl_seconds": 3600})
    c.post("/api/leases/usage", {"lease_id": "Le-cli", "amount": 20, "usage_event_id": "evt-e1"})

    def lease_map():
        st = c.get("/api/state")[1]
        return {x["id"]: x for x in st["leases"] if x["id"].startswith("Le-")}

    # 等待项目租约到期挂起（客户端租约仍 active，项目不会被强退）
    deadline = time.time() + 5
    while time.time() < deadline:
        lm = lease_map()
        if lm.get("Le-proj", {}).get("status") == "pending_expired":
            break
        time.sleep(0.15)
    lm = lease_map()
    check("项目租约到期 → pending_expired（不抽走支撑下游的余量）",
          lm["Le-proj"]["status"] == "pending_expired", lm["Le-proj"]["status"])
    check("下游客户端租约仍然有效（未被级联强退）",
          lm["Le-cli"]["status"] == "active", lm["Le-cli"]["status"])
    check("挂起期间不能再扣减项目租约 → 409",
          c.post("/api/leases/usage", {"lease_id": "Le-proj", "amount": 1, "usage_event_id": "evt-pending"})[0] == 409)
    ok, failed = audit_ok(c)
    check("挂起期间审计通过（无负余额、不超卖）", ok, str(failed))

    # 客户端租约归还后，项目挂起租约自动结算，只退未消费 60
    c.post("/api/leases/release", {"lease_id": "Le-cli"})
    deadline = time.time() + 5
    while time.time() < deadline:
        lm = lease_map()
        if lm["Le-proj"]["status"] == "closed":
            break
        time.sleep(0.15)
    lm = lease_map()
    check("下游释放后挂起租约结算关闭，reason=expired",
          lm["Le-proj"]["status"] == "closed" and lm["Le-proj"]["reason"] == "expired",
          str(lm["Le-proj"]))
    check("结算只退未消费（proj 退 60）", lm["Le-proj"]["remaining"] == 60, str(lm["Le-proj"]))
    ok, failed = audit_ok(c)
    check("延迟结算后审计通过", ok, str(failed))

    # 离线场景：新租约 + 离线即关闭（客户端是叶子，恒安全）
    c.post("/api/leases", {"lessor_id": "org-e", "holder_id": "proj-e", "amount": 30, "lease_id": "Le-proj2"})
    c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 20, "lease_id": "Le-cli2"})
    code, r = c.post("/api/clients/presence", {"client_id": "cli-e", "online": False})
    check("客户端下线", code == 201, str(r))
    st = c.get("/api/state")[1]
    l2 = next(x for x in st["leases"] if x["id"] == "Le-cli2")
    check("离线关闭其活动租约，reason=offline", l2["status"] == "closed" and l2["reason"] == "offline")
    code, _ = c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 1, "lease_id": "Le-blocked"})
    check("离线客户端不能获得租约 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/usage", {"lease_id": "Le-cli2", "amount": 1, "usage_event_id": "evt-x"})
    check("离线客户端不能扣减 → 409", code == 409, f"code={code}")
    c.post("/api/clients/presence", {"client_id": "cli-e", "online": True})
    code, _ = c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 5, "lease_id": "Le-online"})
    check("重新上线后可再租约", code == 201, f"code={code}")


def test_concurrent_grants(srv: Server):
    print("\n[6] 跨层并发申请：高并发下绝不超卖")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-c"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-c", "node_id": "proj-c"})
    for i in range(30):
        c.post("/api/nodes", {"type": "client", "parent_id": "proj-c", "node_id": f"cli-c{i}"})
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-c", "amount": 100, "lease_id": "Lc-org"})
    c.post("/api/leases", {"lessor_id": "org-c", "holder_id": "proj-c", "amount": 100, "lease_id": "Lc-proj"})

    # 30 个客户端各抢 10（共需 300，只有 100）
    results = []
    lock = threading.Lock()

    def worker(i):
        code, body = srv.client.post(
            "/api/leases",
            {"lessor_id": "proj-c", "holder_id": f"cli-c{i}", "amount": 10,
             "lease_id": f"Lc-c{i}", "command_id": f"cmd-grant-{i}"},
        )
        with lock:
            results.append((code, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads: t.start()
    for t in threads: t.join()
    granted = sum(1 for code, _ in results if code == 201)
    rejected = sum(1 for code, _ in results if code == 409)
    check(f"恰好 10 个成功 / 20 个被拒（实际 {granted}/{rejected}）", granted == 10 and rejected == 20,
          str([b.get("message") for cd, b in results if cd not in (201, 409)][:3]))

    st = c.get("/api/state")[1]
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("并发后项目可用为 0（未超卖）", bal["proj-c"] == 0, str(bal))
    check("所有余额非负", all(n["available"] >= 0 for n in st["nodes"]))

    # 释放成功的 10 条租约，余量 100 回到项目，供下一步并发扣减
    for code, body in results:
        if code == 201:
            rc, _ = c.post("/api/leases/release", {"lease_id": body["lease_id"]})
            assert rc == 201, f"释放失败 {rc}"

    # 并发用量：同一租约 20 线程各扣 10，余量 100
    c.post("/api/leases", {"lessor_id": "proj-c", "holder_id": "cli-c0",
                           "amount": 100, "lease_id": "Lc-use"})
    usage_res = []

    def uworker(i):
        code, _ = srv.client.post("/api/leases/usage",
                                  {"lease_id": "Lc-use", "amount": 10, "usage_event_id": f"uce-{i}"})
        with lock:
            usage_res.append(code)

    threads = [threading.Thread(target=uworker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("并发扣减恰好 10 成功 / 10 拒绝",
          usage_res.count(201) == 10 and usage_res.count(409) == 10, str(usage_res))
    st = c.get("/api/state")[1]
    l = next(x for x in st["leases"] if x["id"] == "Lc-use")
    check("租约最终消费 100、剩余 0", l["consumed"] == 100 and l["remaining"] == 0)


def test_concurrent_replay_safe(srv: Server):
    print("\n[7] 同一命令的并发重复提交只生效一次")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-rp"})
    codes = []
    lock = threading.Lock()

    def worker():
        code, body = srv.client.post("/api/leases", {
            "lessor_id": "platform", "holder_id": "org-rp", "amount": 50,
            "lease_id": "L-rp-once", "command_id": "cmd-once-50",
        })
        with lock:
            codes.append(code)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("10 次相同并发提交：恰好 1 次 201，其余重放/去重",
          codes.count(201) == 1 and all(x in (200, 201) for x in codes), str(codes))
    st = c.get("/api/state")[1]
    check("组织余额只扣一次 = 50", next(n for n in st["nodes"] if n["id"] == "org-rp")["available"] == 50)


def test_restart_rebuild(srv: Server):
    print("\n[8] 服务重启：从事件账本重建当前余额")
    c = srv.client
    before = c.get("/api/state")[1]
    events_before = c.get("/api/events")[1]["events"]

    srv.restart()

    after = c.get("/api/state")[1]
    events_after = c.get("/api/events")[1]["events"]
    check("事件数重启前后一致", len(events_before) == len(events_after))
    check("全局容量重建一致", after["global_capacity"] == before["global_capacity"])
    check("节点集合重建一致",
          [n["id"] for n in after["nodes"]] == [n["id"] for n in before["nodes"]])
    check("各节点余额逐字段重建一致",
          [(n["id"], n["available"], n["consumed"]) for n in after["nodes"]]
          == [(n["id"], n["available"], n["consumed"]) for n in before["nodes"]])
    check("租约状态（含已关闭/消费/原因/修订号/截止时间）重建一致",
          [(l["id"], l["status"], l["amount"], l["consumed"], l["remaining"], l["reason"],
            l["revision"], l["expires_at"])
           for l in after["leases"]]
          == [(l["id"], l["status"], l["amount"], l["consumed"], l["remaining"], l["reason"],
               l["revision"], l["expires_at"])
              for l in before["leases"]])
    check("总消费重建一致", after["total_consumed"] == before["total_consumed"])
    ok, failed = audit_ok(c)
    check("重启后审计通过（含 replay_equivalence）", ok, str(failed))

    # 重启后旧幂等键仍然有效
    code, _ = c.post("/api/leases/usage",
                     {"lease_id": "L-rp-once", "amount": 1, "usage_event_id": "uce-0"})
    # uce-0 已在并发用量测试中用于 Lc-use（不同租约）；usage id 全局去重
    check("重启后已处理过的 usage_event_id 仍被拒绝", code == 409, f"code={code}")


def test_lease_amend(srv: Server):
    print("\n[10] 租约有效期修订（乐观修订号 / 幂等 / 冲突 / 并发互斥 / 重启）")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-a"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-a", "node_id": "proj-a"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-a", "node_id": "cli-a"})
    # 沿树注入额度：platform → org → proj（proj-a 方可向客户端授约）
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-a",
                           "amount": 100, "lease_id": "Lam-org"})
    c.post("/api/leases", {"lessor_id": "org-a", "holder_id": "proj-a",
                           "amount": 100, "lease_id": "Lam-proj"})

    # 长 TTL 租约（续约测试主对象）与永久租约（修订应被拒绝）
    code, _ = c.post("/api/leases", {"lessor_id": "proj-a", "holder_id": "cli-a",
                                     "amount": 40, "lease_id": "Lam-ttl", "ttl_seconds": 3600})
    check("带 TTL 租约授予（初始修订号 0）", code == 201, f"code={code}")
    c.post("/api/leases", {"lessor_id": "proj-a", "holder_id": "cli-a",
                           "amount": 10, "lease_id": "Lam-perm"})

    def state_of(lid):
        st = c.get("/api/state")[1]
        return next(x for x in st["leases"] if x["id"] == lid)

    before = state_of("Lam-ttl")
    check("授予后修订号 = 0", before["revision"] == 0, str(before.get("revision")))

    # 成功修订：expected_revision=0，延后 600 秒，修订号递增为 1
    code, r = c.post("/api/leases/amend",
                     {"lease_id": "Lam-ttl", "expected_revision": 0, "extend_seconds": 600,
                      "command_id": "cmd-amend-1"})
    check("修订成功（延后 600s）", code == 201 and r["revision"] == 1, f"code={code} {r}")
    after = state_of("Lam-ttl")
    check("截止时间被延长", after["expires_at"] >= before["expires_at"] + 590,
          f"{before['expires_at']} -> {after['expires_at']}")
    check("修订号递增为 1、修订历史计数 1",
          after["revision"] == 1 and after["amendment_count"] == 1, str(after))

    # 相同 command_id（连同相同请求体）重试 → 返回第一次的结果（幂等重放）
    t0 = time.time()
    code, r2 = c.post("/api/leases/amend",
                      {"lease_id": "Lam-ttl", "expected_revision": 0, "extend_seconds": 600,
                       "command_id": "cmd-amend-1"})
    check("同一请求标识再次提交 → 200 重放首次结果",
          code == 200 and r2.get("replayed") is True and r2["revision"] == 1
          and r2["new_expires_at"] == r["new_expires_at"], f"code={code} {r2}")
    check("幂等重放不改变时间/修订号（延长量不重新计算）",
          time.time() >= t0 and state_of("Lam-ttl")["revision"] == 1)

    # 修订号过旧（仍提交 0）→ 冲突，且回传最新状态
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-ttl", "expected_revision": 0, "extend_seconds": 900,
                        "command_id": "cmd-amend-stale"})
    check("修订号过旧 → 409", code == 409, f"code={code}")
    check("冲突回传当前修订号 1 与最新截止时间",
          err.get("details", {}).get("current_revision") == 1
          and err["details"].get("expires_at") == after["expires_at"], str(err.get("details")))

    # 修订号过新（跳跃）→ 冲突
    code, _ = c.post("/api/leases/amend",
                     {"lease_id": "Lam-ttl", "expected_revision": 5, "extend_seconds": 900})
    check("修订号过新（跳跃）→ 409", code == 409, f"code={code}")

    # 目标时间没有向后延长 → 冲突（绝对时间，恰好当前截止时间）
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-ttl", "expected_revision": 1,
                        "new_expires_at": after["expires_at"]})
    check("新截止时间未晚于当前 → 409", code == 409, f"code={code} {err}")

    # 绝对时间成功续到更远：修订号 1 → 2
    future = time.time() + 7200
    code, r = c.post("/api/leases/amend",
                     {"lease_id": "Lam-ttl", "expected_revision": 1,
                      "new_expires_at": future})
    check("绝对时间修订成功，修订号 → 2", code == 201 and r["revision"] == 2, f"code={code} {r}")
    check("缩短有效期（更早截止）→ 409",
          c.post("/api/leases/amend",
                 {"lease_id": "Lam-ttl", "expected_revision": 2,
                  "new_expires_at": state_of("Lam-ttl")["expires_at"] - 1})[0] == 409)
    check("同时提供 extend 与绝对时间 → 400",
          c.post("/api/leases/amend",
                 {"lease_id": "Lam-ttl", "expected_revision": 2,
                  "extend_seconds": 10, "new_expires_at": future + 10})[0] == 400)
    check("两者都不提供 → 400",
          c.post("/api/leases/amend",
                 {"lease_id": "Lam-ttl", "expected_revision": 2})[0] == 400)

    # 永久租约不能修订有效期
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-perm", "expected_revision": 0, "extend_seconds": 100})
    check("永久租约（无截止时间）修订 → 409", code == 409, f"code={code} {err}")

    # 已关闭的租约不能修订
    code, _ = c.post("/api/leases/revoke", {"lease_id": "Lam-perm"})
    check("先撤销永久租约", code == 201, f"code={code}")
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-perm", "expected_revision": 0, "extend_seconds": 100})
    check("已关闭租约修订 → 409，details 指明状态",
          code == 409 and err.get("details", {}).get("status") == "closed", f"{code} {err}")

    # 客户端已下线：其活动租约修订被拒。用独立客户端 cli-b，避免关闭主测租约
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-a", "node_id": "cli-b"})
    c.post("/api/leases", {"lessor_id": "proj-a", "holder_id": "cli-b",
                           "amount": 5, "lease_id": "Lam-off", "ttl_seconds": 3600})
    c.post("/api/clients/presence", {"client_id": "cli-b", "online": False})
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-off", "expected_revision": 0, "extend_seconds": 100})
    check("客户端已下线时修订其租约 → 409（离线即结算，租约已关闭）",
          code == 409 and err.get("details", {}).get("status") == "closed", f"{code} {err}")

    # 到期“挂起等待结算”只能发生在仍支撑下游的中间层租约：
    # 独立组织/项目，项目租约 1s 到期但仍挂着活动的客户端租约 → pending
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-m"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-m", "node_id": "proj-m"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-m", "node_id": "cli-m"})
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-m",
                           "amount": 50, "lease_id": "Lam-morg"})
    c.post("/api/leases", {"lessor_id": "org-m", "holder_id": "proj-m",
                           "amount": 40, "lease_id": "Lam-mproj", "ttl_seconds": 1})
    c.post("/api/leases", {"lessor_id": "proj-m", "holder_id": "cli-m",
                           "amount": 20, "lease_id": "Lam-mcli", "ttl_seconds": 3600})
    deadline = time.time() + 5
    while time.time() < deadline:
        if state_of("Lam-mproj")["status"] == "pending_expired":
            break
        time.sleep(0.1)
    code, err = c.post("/api/leases/amend",
                       {"lease_id": "Lam-mproj", "expected_revision": 0, "extend_seconds": 1000})
    check("到期挂起（等待结算）的中间层租约修订 → 409",
          code == 409 and err.get("details", {}).get("status") == "pending_expired",
          f"{code} {err}")

    # 不存在的租约 → 404
    code, _ = c.post("/api/leases/amend",
                     {"lease_id": "Lam-nope", "expected_revision": 0, "extend_seconds": 10})
    check("不存在的租约修订 → 404", code == 404, f"code={code}")

    # 并发：10 个请求都基于修订号 2 续约，恰好一个成功，其余修订号冲突
    results = []
    lock = threading.Lock()

    def aworker(i):
        code, body = srv.client.post("/api/leases/amend", {
            "lease_id": "Lam-ttl", "expected_revision": 2,
            "extend_seconds": 3600 + i, "command_id": f"cmd-amend-c{i}",
        })
        with lock:
            results.append((code, body))

    threads = [threading.Thread(target=aworker, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("并发同修订号续约：恰好 1 成功 / 9 冲突",
          sum(1 for cd, _ in results if cd == 201) == 1
          and sum(1 for cd, _ in results if cd == 409) == 9, str(sorted(cd for cd, _ in results)))
    check("成功后修订号恰好为 3", state_of("Lam-ttl")["revision"] == 3,
          str(state_of('Lam-ttl')))

    # 并发互斥：续约与归还同时进行，只能有一个状态变更落账
    c.post("/api/leases", {"lessor_id": "proj-a", "holder_id": "cli-a",
                           "amount": 5, "lease_id": "Lam-race", "ttl_seconds": 3600})
    race = []

    def amend_race():
        race.append(("amend",) + srv.client.post("/api/leases/amend", {
            "lease_id": "Lam-race", "expected_revision": 0, "extend_seconds": 999,
            "command_id": "cmd-race-amend"})[:1])

    def release_race():
        race.append(("release",) + srv.client.post("/api/leases/release",
                                                   {"lease_id": "Lam-race",
                                                    "expected_revision": 0,
                                                    "command_id": "cmd-race-rel"})[:1])

    t1 = threading.Thread(target=amend_race)
    t2 = threading.Thread(target=release_race)
    t1.start(); t2.start(); t1.join(); t2.join()
    winners = [name for name, cd in race if cd in (200, 201)]
    losers = [(name, cd) for name, cd in race if cd == 409]
    check("续约与归还竞争：恰有一方成功、一方看到最新状态(409)",
          len(winners) == 1 and len(losers) == 1, str(race))
    evts = c.get("/api/events")[1]["events"]
    amended = [e for e in evts if e["event_type"] == "LeaseAmended"
               and e["payload"].get("lease_id") == "Lam-race"]
    closed = [e for e in evts if e["event_type"] == "LeaseClosed"
              and e["payload"].get("lease_id") == "Lam-race"]
    check("事件账本中该租约只落入一个变更（不会既续约又关闭）",
          (len(amended) == 1) ^ (len(closed) == 1), f"amend={len(amended)} closed={len(closed)}")
    if amended:
        check("（本次为续约胜出）归还方随后看到租约仍活动但修订号已变",
              state_of("Lam-race")["status"] == "active"
              and state_of("Lam-race")["revision"] == 1)
    else:
        check("（本次为归还胜出）续约方随后看到租约已关闭",
              state_of("Lam-race")["status"] == "closed")

    # 同样的互斥适用于“续约 vs 撤销”：跑多轮保证两种先后都覆盖到
    saw_amend_win = saw_revoke_win = False
    for round_i in range(20):
        lid = f"Lam-rr{round_i}"
        c.post("/api/leases", {"lessor_id": "proj-a", "holder_id": "cli-a",
                               "amount": 1, "lease_id": lid, "ttl_seconds": 3600})
        outcomes = []

        def a2():
            outcomes.append(("amend",) + srv.client.post("/api/leases/amend", {
                "lease_id": lid, "expected_revision": 0, "extend_seconds": 777,
                "command_id": f"cmd-rr-a-{round_i}"})[:1])

        def r2():
            outcomes.append(("revoke",) + srv.client.post("/api/leases/revoke", {
                "lease_id": lid, "expected_revision": 0,
                "command_id": f"cmd-rr-r-{round_i}"})[:1])

        x = threading.Thread(target=a2); y = threading.Thread(target=r2)
        x.start(); y.start(); x.join(); y.join()
        codes = {name: cd for name, cd in outcomes}
        check(f"第{round_i}轮 续约/撤销恰一成一败",
              sorted(codes.values()) == [201, 409], str(outcomes))
        if codes.get("amend") == 201:
            saw_amend_win = True
            check(f"第{round_i}轮 续约胜出则租约仍 active/rev=1",
                  state_of(lid)["status"] == "active" and state_of(lid)["revision"] == 1)
            c.post("/api/leases/release", {"lease_id": lid})  # 清理
        else:
            saw_revoke_win = True
            check(f"第{round_i}轮 撤销胜出则租约 closed",
                  state_of(lid)["status"] == "closed")
    check("20 轮中两种胜出顺序都出现过（互斥不偏向任一方）",
          saw_amend_win and saw_revoke_win, f"amend_win={saw_amend_win} revoke_win={saw_revoke_win}")

    # 余额/守恒不受续约影响，审计仍通过
    st = c.get("/api/state")[1]
    check("续约不改变全局守恒",
          st["total_available"] + st["total_consumed"] == st["global_capacity"])
    ok, failed = audit_ok(c)
    check("续约场景全程审计通过（含 replay_equivalence）", ok, str(failed))

    # 重启后恢复修订号、截止时间与修订历史
    rev3_expires = state_of("Lam-ttl")["expires_at"]
    srv.restart()
    recovered = state_of("Lam-ttl")
    check("重启后修订号恢复为 3", recovered["revision"] == 3, str(recovered.get("revision")))
    check("重启后截止时间恢复", abs(recovered["expires_at"] - rev3_expires) < 1e-6,
          str(recovered.get("expires_at")))
    check("重启后修订历史恢复（3 次）", recovered["amendment_count"] == 3,
          str(recovered.get("amendment_count")))
    # 重启后旧的幂等命令标识仍返回第一次结果
    code, rr = c.post("/api/leases/amend",
                      {"lease_id": "Lam-ttl", "expected_revision": 0, "extend_seconds": 600,
                       "command_id": "cmd-amend-1"})
    check("重启后同一命令标识仍重放首次结果",
          code == 200 and rr.get("replayed") is True and rr["revision"] == 1, f"code={code} {rr}")
    ok, failed = audit_ok(c)
    check("重启后续约相关审计通过", ok, str(failed))

    # 修订与归还/撤销共享版本号：续约后用旧修订号归还/撤销 → 冲突
    code, err = c.post("/api/leases/release",
                       {"lease_id": "Lam-ttl", "expected_revision": 0})
    check("续约后用旧修订号归还 → 409（修订与归还互斥）", code == 409, f"{code} {err}")
    code, err = c.post("/api/leases/revoke",
                       {"lease_id": "Lam-ttl", "expected_revision": 2})
    check("用过旧修订号撤销 → 409", code == 409 and err["details"]["current_revision"] == 3,
          f"{code} {err}")
    code, _ = c.post("/api/leases/release", {"lease_id": "Lam-ttl"})
    check("不带修订号的归还仍成功（向后兼容）", code == 201, f"code={code}")


def test_capacity_plans(srv: Server):
    print("\n[12] 批量容量调整草案（预览/原子发布/陈旧拒绝/废弃/幂等/重启）")
    c = srv.client
    c.post("/api/capacity", {"capacity": 1000})
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-q"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-q", "node_id": "proj-q"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-q", "node_id": "cli-q"})

    # 自举隔离子树：把平台当前可用（动态读取，规避共享账本前序残留）
    # 以“节点容量扩容”整笔发给 org-q；此后平台可用 0，子树自成一池。
    plat_room = c.get("/api/state")[1]["nodes"][0]["available"]
    c.post("/api/capacity-plans", {"plan_id": "PL-boot",
                                   "changes": [{"node_id": "org-q", "capacity": plat_room}]})
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-boot"})
    rc, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-boot"})
    assert rc == 201, f"自举草案发布失败 {rc}"
    ORG_CAP = plat_room                      # org-q 容量与初始余额
    c.post("/api/leases", {"lessor_id": "org-q", "holder_id": "proj-q", "amount": 300, "lease_id": "Lq-proj"})
    c.post("/api/leases", {"lessor_id": "proj-q", "holder_id": "cli-q", "amount": 200, "lease_id": "Lq-cli"})
    # 余额：platform=0  org=ORG_CAP-300(>=200)  proj=100  cli=200
    st0 = c.get("/api/state")[1]
    bal0 = {n["id"]: n["available"] for n in st0["nodes"]}
    assert bal0["platform"] == 0 and bal0["proj-q"] == 100 and bal0["cli-q"] == 200, bal0
    assert bal0["org-q"] >= 200, bal0

    # ---- 结构校验 ----
    code, _ = c.post("/api/capacity-plans", {"changes": []})
    check("空变更清单 → 400", code == 400, f"code={code}")
    code, _ = c.post("/api/capacity-plans", {"changes": "not-a-list"})
    check("changes 非数组 → 400", code == 400, f"code={code}")
    code, _ = c.post("/api/capacity-plans", {"changes": [{"node_id": "nope", "capacity": 1}]})
    check("草案引用不存在节点 → 404", code == 404, f"code={code}")
    code, _ = c.post("/api/capacity-plans",
                     {"changes": [{"node_id": "org-q", "capacity": 10},
                                  {"node_id": "org-q", "capacity": 20}]})
    check("同一草案中重复节点 → 400", code == 400, f"code={code}")
    code, _ = c.post("/api/capacity-plans", {"plan_id": "PL-x", "changes": [{"node_id": "org-q", "capacity": 1}],
                                             "command_id": "cmd-plx-1"})
    code, _ = c.post("/api/capacity-plans", {"plan_id": "PL-x", "changes": [{"node_id": "org-q", "capacity": 1}],
                                             "command_id": "cmd-plx-2"})
    check("重复 plan_id → 409", code == 409, f"code={code}")
    # 创建命令的幂等：相同 command_id 重试只返回同一草案
    r1 = c.post("/api/capacity-plans", {"plan_id": "PL-idem", "changes": [{"node_id": "org-q", "capacity": ORG_CAP}],
                                        "command_id": "cmd-plan-idem"})
    r2 = c.post("/api/capacity-plans", {"plan_id": "PL-idem", "changes": [{"node_id": "org-q", "capacity": ORG_CAP}],
                                        "command_id": "cmd-plan-idem"})
    check("相同 command_id 的创建重试幂等（200 重放同一 plan_id）",
          r1[0] == 201 and r2[0] == 200 and r2[1].get("replayed") is True, f"{r1[0]}/{r2[0]}")

    # ---- 创建草案：proj 容量 0→50（org 出资 50）----
    code, pl = c.post("/api/capacity-plans", {
        "plan_id": "PL-happy",
        "changes": [{"node_id": "proj-q", "capacity": 50}],
        "command_id": "cmd-plan-happy",
    })
    check("创建草案", code == 201 and pl["status"] == "draft" and pl["base_seq"] > 0, f"{code} {pl}")
    base_seq = pl["base_seq"]

    code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-happy"})
    check("未预览就发布 → 409", code == 409, f"code={code}")

    # ---- 预览：逐层余额 + 受影响租约 ----
    code, pv = c.post("/api/capacity-plans/preview", {"plan_id": "PL-happy"})
    check("预览 200 且整批合法 fresh=True", code == 200 and pv["valid"] is True
          and pv["fresh"] is True and pv["drifted_from_base"] is False, f"{code} {pv.get('valid')}")
    lb = {x["node_id"]: x for x in pv["layer_balances"]}
    check("预览逐层余额 org-q −50",
          lb["org-q"]["before"] == bal0["org-q"] and lb["org-q"]["after"] == bal0["org-q"] - 50
          and lb["org-q"]["capacity_before"] == ORG_CAP and lb["org-q"]["capacity_after"] == ORG_CAP,
          str(lb["org-q"]))
    check("预览逐层余额 proj-q +50，容量 0→50",
          lb["proj-q"]["before"] == 100 and lb["proj-q"]["after"] == 150
          and lb["proj-q"]["capacity_after"] == 50, str(lb["proj-q"]))
    check("层级链只含变更节点的祖先（不包含无关的 cli-q）", "cli-q" not in lb)
    aff = {a["lease_id"]: a for a in pv["affected_leases"]}
    check("受影响租约 Lq-proj 角色=出租方/持有方",
          aff["Lq-proj"]["affected_role"] == "出租方/持有方", str(list(aff)))
    check("受影响租约 Lq-cli 角色=出租方（proj 是其出租方）",
          aff["Lq-cli"]["affected_role"] == "出租方", str(list(aff)))
    check("受影响租约带回余额前/后快照",
          aff["Lq-proj"]["holder_balance_before"] == 100 and aff["Lq-proj"]["holder_balance_after"] == 150)
    check("预览不改变实际余额", c.get("/api/state")[1]["nodes"][0]["available"] == 0)

    # ---- 发布：原子批次 ----
    code, pub = c.post("/api/capacity-plans/publish", {"plan_id": "PL-happy", "command_id": "cmd-plan-pub"})
    check("发布成功", code == 201 and pub["status"] == "published" and pub["applied_count"] == 1, f"{code} {pub}")
    st = c.get("/api/state")[1]
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    cap = {n["id"]: n["capacity"] for n in st["nodes"]}
    check("发布后余额与预览一致（org-50 / proj+50）",
          bal["org-q"] == bal0["org-q"] - 50 and bal["proj-q"] == 150, str(bal))
    check("节点容量已记录 proj=50 org=ORG_CAP",
          cap["proj-q"] == 50 and cap["org-q"] == ORG_CAP, str(cap))
    check("全局守恒仍成立", st["total_available"] + st["total_consumed"] == st["global_capacity"])
    evts = c.get("/api/events")[1]["events"]
    check("NodeCapacityAdjusted 业务事件入链（含自举 2 条）",
          len([e for e in evts if e["event_type"] == "NodeCapacityAdjusted"]) == 2)
    check("草案生命周期事件全部入哈希链（Created/Previewed/Published）",
          all(any(e["event_type"] == t for e in evts)
              for t in ("CapacityPlanCreated", "CapacityPlanPreviewed", "CapacityPlanPublished")))

    # 重复发布 → 只返回首次发布结果
    code, pub2 = c.post("/api/capacity-plans/publish", {"plan_id": "PL-happy", "command_id": "cmd-plan-pub"})
    check("同命令重复发布 → 200 重放首次结果", code == 200 and pub2.get("replayed") is True and pub2["applied_count"] == 1, f"{code}")
    code, pub3 = c.post("/api/capacity-plans/publish", {"plan_id": "PL-happy", "command_id": "cmd-plan-pub-other"})
    check("换新命令重复发布也只得到首次结果", code == 200 and pub3.get("replayed") is True, f"{code}")
    code, _ = c.post("/api/capacity-plans/discard", {"plan_id": "PL-happy"})
    check("已发布草案不能废弃 → 409", code == 409, f"code={code}")

    # ---- 任一项不合法：整批不生效 ----
    # org 当前余 = ORG_CAP-350（>=150）。合法项：org 小幅缩容 1（容量 ORG_CAP→ORG_CAP-1，
    # 只需退 1，余额充足）；非法项：proj 扩容到 99999（org 出不起）。
    snap = {n["id"]: (n["available"], n["capacity"]) for n in c.get("/api/state")[1]["nodes"]}
    code, _ = c.post("/api/capacity-plans", {"plan_id": "PL-bad", "changes": [
        {"node_id": "org-q", "capacity": ORG_CAP - 1},
        {"node_id": "proj-q", "capacity": 99999},
    ]})
    assert code == 201
    code, pvb = c.post("/api/capacity-plans/preview", {"plan_id": "PL-bad"})
    invalids = [i for i in pvb["items"] if not i["valid"]]
    check("预览逐项给出拒绝原因（org 合法、proj 非法）",
          pvb["valid"] is False and [i["node_id"] for i in invalids] == ["proj-q"]
          and "扩容被拒绝" in invalids[0]["reason"], str(pvb["items"]))
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-bad"})
    check("发布非法草案 → 409 且回传 rejected_items（整批不生效）",
          code == 409 and len(err["details"]["rejected_items"]) == 1, f"{code}")
    after = {n["id"]: (n["available"], n["capacity"]) for n in c.get("/api/state")[1]["nodes"]}
    check("整批回滚：合法项也未生效，余额/容量逐节点不变", after == snap, f"{snap} -> {after}")

    # ---- 合法缩容 + 非法扩容 混合批 ----
    code, _ = c.post("/api/capacity-plans", {"plan_id": "PL-shrink", "changes": [
        {"node_id": "proj-q", "capacity": 0},       # 退 50：proj 余 150 充足 → 合法
        {"node_id": "org-q", "capacity": 9_000_000},  # 平台可用 0，出不起 → 非法
    ]})
    code, pvs = c.post("/api/capacity-plans/preview", {"plan_id": "PL-shrink"})
    reasons = {i["node_id"]: (i["valid"], i["reason"]) for i in pvs["items"]}
    check("proj 缩容 50→0 合法（余 150 足以让出 50）", reasons["proj-q"][0] is True, str(reasons))
    check("org 扩容（父级平台出不起）非法", reasons["org-q"][0] is False and "扩容被拒绝" in reasons["org-q"][1], str(reasons))
    code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-shrink"})
    check("混合批发布 → 409，proj 未被缩容（容量仍 50）",
          code == 409 and next(n["capacity"] for n in c.get("/api/state")[1]["nodes"] if n["id"] == "proj-q") == 50,
          f"code={code}")

    # 纯合法缩容批：proj 50→0（退 50 给 org）
    code, _ = c.post("/api/capacity-plans", {"plan_id": "PL-shrink-ok",
                                             "changes": [{"node_id": "proj-q", "capacity": 0}]})
    code, pvsk = c.post("/api/capacity-plans/preview", {"plan_id": "PL-shrink-ok"})
    check("纯缩容批预览合法 delta=-50", pvsk["valid"] is True and pvsk["items"][0]["delta"] == -50, str(pvsk))
    code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-shrink-ok"})
    check("纯缩容批发布成功", code == 201, f"code={code}")
    st = c.get("/api/state")[1]
    pnode, onode = (next(n for n in st["nodes"] if n["id"] == x) for x in ("proj-q", "org-q"))
    check("proj 容量归零、余额退回 50（150→100）非负", pnode["capacity"] == 0 and pnode["available"] == 100, str(pnode))
    check("org 收回 50", onode["available"] == bal0["org-q"], str(onode))
    check("缩容后全局守恒", st["total_available"] + st["total_consumed"] == st["global_capacity"])

    # 容量无变化的条目拒绝
    c.post("/api/capacity-plans", {"plan_id": "PL-noop", "changes": [{"node_id": "cli-q", "capacity": 0}]})
    code, pvd = c.post("/api/capacity-plans/preview", {"plan_id": "PL-noop"})
    check("容量未变化的条目 → 非法", pvd["valid"] is False and "未变化" in pvd["items"][0]["reason"])
    code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-noop"})
    check("含无变化条目的批次不能发布 → 409", code == 409, f"code={code}")

    # ---- 陈旧版本 ----
    # 恢复 proj 容量 50
    c.post("/api/capacity-plans", {"plan_id": "PL-restore",
                                   "changes": [{"node_id": "proj-q", "capacity": 50}]})
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-restore"})
    c.post("/api/capacity-plans/publish", {"plan_id": "PL-restore"})
    # 此时 org 余 = bal0(org)，proj 余 = 150

    c.post("/api/capacity-plans", {"plan_id": "PL-stale",
                                   "changes": [{"node_id": "proj-q", "capacity": 20}]})
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-stale"})
    # 创建后又授约（org 余 >=200，必有 10 可用）
    code, _ = c.post("/api/leases", {"lessor_id": "org-q", "holder_id": "proj-q",
                                     "amount": 10, "lease_id": "Lq-new"})
    check("创建草案后又授约", code == 201, f"code={code}")
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-stale"})
    check("陈旧草案发布 → 409", code == 409, f"code={code}")
    check("拒绝详情指出需要重新预览，intervening 含授约",
          err["details"]["current_seq"] > err["details"]["preview_seq"]
          and any(x["event_type"] == "LeaseGranted" for x in err["details"]["intervening_changes"])
          and "重新预览" in err["details"]["hint"], str(err.get("details")))

    # 重新预览后又消费 → 再次陈旧
    code, pvy = c.post("/api/capacity-plans/preview", {"plan_id": "PL-stale"})
    check("重新预览成功（drifted_from_base=True）",
          code == 200 and pvy["fresh"] is True and pvy["drifted_from_base"] is True)
    c.post("/api/leases/usage", {"lease_id": "Lq-cli", "amount": 5, "usage_event_id": "evt-plan-1"})
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-stale"})
    check("预览后又消费 → 409（归因 UsageRecorded）",
          code == 409 and any(x["event_type"] == "UsageRecorded" for x in err["details"]["intervening_changes"]),
          f"{code}")

    # 重新预览后结算归还 → 又陈旧
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-stale"})
    c.post("/api/leases/release", {"lease_id": "Lq-new"})
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-stale"})
    check("预览后又结算归还 → 409（归因 LeaseClosed）",
          code == 409 and any(x["event_type"] == "LeaseClosed" for x in err["details"]["intervening_changes"]),
          f"{code}")

    # 其他草案的生命周期不推进业务水位：重新预览后即可发布
    c.post("/api/capacity-plans", {"plan_id": "PL-other", "changes": [{"node_id": "proj-q", "capacity": 10}]})
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-other"})
    c.post("/api/capacity-plans/discard", {"plan_id": "PL-other"})
    code, pvf = c.post("/api/capacity-plans/preview", {"plan_id": "PL-stale"})
    # 消费 5 沿链传播：proj 容量 50→20 要退 30，proj 余 = 100+10+50-5+10(release)-? 直接以预览结果为准
    check("对最新账本重新预览（合法与否以当前余额判定）", code == 200, str(pvf.get("items")))
    if pvf["valid"]:
        code, pubx = c.post("/api/capacity-plans/publish", {"plan_id": "PL-stale"})
        check("其他草案不推进水位：重新预览后发布成功", code == 201 and pubx["status"] == "published", f"{code}")
        stale_published = True
    else:
        # proj 余量不足以退 30 时改发布一个更小的缩容，验证陈旧闸门已解除
        c.post("/api/capacity-plans", {"plan_id": "PL-stale2",
                                       "changes": [{"node_id": "proj-q", "capacity": 45}]})
        c.post("/api/capacity-plans/preview", {"plan_id": "PL-stale2"})
        code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-stale2"})
        check("陈旧闸门解除后小步缩容可发布", code == 201, f"code={code} {pvf['items']}")
        stale_published = False

    # 续约（LeaseAmended）推进水位
    c.post("/api/leases", {"lessor_id": "proj-q", "holder_id": "cli-q",
                           "amount": 5, "lease_id": "Lq-ttl", "ttl_seconds": 3600})
    c.post("/api/capacity-plans", {"plan_id": "PL-amend",
                                   "changes": [{"node_id": "proj-q", "capacity": 50 if stale_published else 40}]})
    c.post("/api/capacity-plans/preview", {"plan_id": "PL-amend"})
    c.post("/api/leases/amend", {"lease_id": "Lq-ttl", "expected_revision": 0, "extend_seconds": 600})
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-amend"})
    check("预览后发生续约 → 409（归因 LeaseAmended）",
          code == 409 and any(x["event_type"] == "LeaseAmended" for x in err["details"]["intervening_changes"]),
          f"{code}")
    code, pva = c.post("/api/capacity-plans/preview", {"plan_id": "PL-amend"})
    code, puba = c.post("/api/capacity-plans/publish", {"plan_id": "PL-amend"})
    check("续约后重新预览可发布", code == 201 and puba["status"] == "published", f"{code} {pva.get('items')}")

    # 到期结算（后台 sweep 系统事件）也推进水位
    c.post("/api/leases", {"lessor_id": "proj-q", "holder_id": "cli-q",
                           "amount": 1, "lease_id": "Lq-soon", "ttl_seconds": 1})
    # 选一个对当前状态合法的 proj 容量目标（只扩 1，org 出资）
    c.post("/api/capacity-plans", {"plan_id": "PL-exp",
                                   "changes": [{"node_id": "proj-q",
                                                "capacity": next(n["capacity"] for n in c.get("/api/state")[1]["nodes"] if n["id"] == "proj-q") + 1}]})
    code, pve = c.post("/api/capacity-plans/preview", {"plan_id": "PL-exp"})
    check("到期测试草案预览合法", code == 200 and pve["valid"], str(pve.get("items")))
    deadline = time.time() + 5
    while time.time() < deadline:
        if {x["id"]: x for x in c.get("/api/state")[1]["leases"]}["Lq-soon"]["status"] == "closed":
            break
        time.sleep(0.15)
    check("短 TTL 租约已被 sweep 到期结算",
          {x["id"]: x for x in c.get("/api/state")[1]["leases"]}["Lq-soon"]["status"] == "closed")
    code, err = c.post("/api/capacity-plans/publish", {"plan_id": "PL-exp"})
    check("预览后发生到期结算 → 409（系统事件同样推进业务水位）",
          code == 409 and any(x["event_type"] in ("LeaseExpired", "LeaseClosed")
                              for x in err["details"]["intervening_changes"]), f"{code}")
    c.post("/api/capacity-plans/discard", {"plan_id": "PL-exp"})

    # 已发布草案再预览 → 409 回传发布结果
    code, err = c.post("/api/capacity-plans/preview", {"plan_id": "PL-happy"})
    check("已发布草案再预览 → 409 且回传发布结果",
          code == 409 and err["details"]["status"] == "published", f"{code}")

    # ---- 废弃 ----
    code, d = c.post("/api/capacity-plans/discard", {"plan_id": "PL-bad", "command_id": "cmd-disc-bad"})
    check("废弃草案", code == 201 and d["status"] == "discarded", f"{code} {d}")
    code, _ = c.post("/api/capacity-plans/publish", {"plan_id": "PL-bad"})
    check("废弃草案发布 → 409", code == 409, f"code={code}")
    code, d2 = c.post("/api/capacity-plans/discard", {"plan_id": "PL-bad", "command_id": "cmd-disc-bad"})
    check("重复废弃幂等 → 200", code == 200 and d2.get("replayed") is True, f"{code}")
    code, _ = c.post("/api/capacity-plans/discard", {"plan_id": "PL-ghost"})
    check("废弃不存在草案 → 404", code == 404, f"code={code}")

    # 列表 / 详情
    code, lst = c.get("/api/capacity-plans")
    statuses = {p["plan_id"]: p["status"] for p in lst["plans"]}
    check("草案列表状态正确",
          code == 200 and statuses.get("PL-happy") == "published"
          and statuses.get("PL-bad") == "discarded"
          and statuses.get("PL-other") == "discarded"
          and statuses.get("PL-exp") == "discarded", str(statuses))
    code, detail = c.get("/api/capacity-plans/PL-happy")
    check("草案详情含首次发布结果、变更清单、创建水位",
          code == 200 and detail["publish_result"]["applied_count"] == 1
          and detail["changes"] == [{"node_id": "proj-q", "capacity": 50}]
          and detail["base_seq"] == base_seq, f"{code}")
    code, _ = c.get("/api/capacity-plans/PL-ghost")
    check("查询不存在草案 → 404", code == 404, f"code={code}")
    ok, failed = audit_ok(c)
    check("草案场景审计通过（含 capacity_plan_consistency）", ok, str(failed))

    # ---- 重启后草案、发布结果、审计轨迹全部可查 ----
    srv.restart()
    code, lst2 = c.get("/api/capacity-plans")
    st2 = {p["plan_id"]: p["status"] for p in lst2["plans"]}
    check("重启后草案状态全部恢复",
          st2.get("PL-happy") == "published" and st2.get("PL-bad") == "discarded"
          and st2.get("PL-other") == "discarded" and st2.get("PL-amend") == "published",
          str(st2))
    happy = next(p for p in lst2["plans"] if p["plan_id"] == "PL-happy")
    check("重启后首次发布结果可查",
          happy["publish_result"]["status"] == "published"
          and happy["publish_result"]["layer_balances"] is not None)
    check("重启后最后预览快照可查（逐层余额/受影响租约）",
          happy["last_preview"] is not None
          and {x["node_id"] for x in happy["last_preview"]["layer_balances"]} == {"platform", "org-q", "proj-q"}
          and len(happy["last_preview"]["affected_leases"]) == 2,
          str(happy.get("last_preview", {}).keys()) if happy["last_preview"] else "None")
    check("重启后 base_seq 恢复", happy["base_seq"] == base_seq)
    code, rr = c.post("/api/capacity-plans/publish", {"plan_id": "PL-happy", "command_id": "cmd-plan-pub"})
    check("重启后重复发布 → 200 重放首次结果", code == 200 and rr.get("replayed") is True, f"{code}")
    evts_after = c.get("/api/events")[1]["events"]
    check("重启后草案生命周期审计轨迹仍在账本",
          all(any(e["event_type"] == t for e in evts_after)
              for t in ("CapacityPlanCreated", "CapacityPlanPreviewed",
                        "CapacityPlanPublished", "CapacityPlanDiscarded")))
    st = c.get("/api/state")[1]
    check("重启后节点容量与余额从事件重放恢复且非负",
          all(n["available"] >= 0 for n in st["nodes"])
          and next(n["capacity"] for n in st["nodes"] if n["id"] == "proj-q")
              == (50 if stale_published else 40))
    ok, failed = audit_ok(c)
    check("重启后审计通过（哈希链 + 重放等价 + 草案一致性）", ok, str(failed))

    # ---- 并发：两份草案争抢同一容量池，串行化保证至多一份生效 ----
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-cc"})
    # 平台可用为 0；容量池用 org-q 当前可用。两份草案都要把 proj 容量提到
    # “恰好抽干 org 池”，先到者成功、后到者因陈旧或超卖失败。
    st = c.get("/api/state")[1]
    proj_cur = next(n["capacity"] for n in st["nodes"] if n["id"] == "proj-q")
    org_pool = next(n["available"] for n in st["nodes"] if n["id"] == "org-q")
    target = proj_cur + org_pool
    for pid in ("PL-c1", "PL-c2"):
        rc = c.post("/api/capacity-plans", {"plan_id": pid,
                                            "changes": [{"node_id": "proj-q", "capacity": target}]})[0]
        rc, pv = c.post("/api/capacity-plans/preview", {"plan_id": pid})
        assert rc == 200 and pv["valid"], (pid, pv.get("items"))
    results = []
    lock = threading.Lock()

    def pub_worker(pid, cid):
        cd, body = srv.client.post("/api/capacity-plans/publish",
                                   {"plan_id": pid, "command_id": cid})
        with lock:
            results.append((pid, cd, body))

    threads = [
        threading.Thread(target=pub_worker, args=("PL-c1", "cmd-cc-1")),
        threading.Thread(target=pub_worker, args=("PL-c2", "cmd-cc-2")),
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    codes = sorted(cd for _, cd, _ in results)
    check("并发发布互斥：恰好一份 201、一份 409", codes == [201, 409], str(results))
    loser = next(pid for pid, cd, _ in results if cd == 409)
    st = c.get("/api/state")[1]
    check("并发后所有余额非负、全局守恒",
          all(n["available"] >= 0 for n in st["nodes"])
          and st["total_available"] + st["total_consumed"] == st["global_capacity"])
    pnode = next(n for n in st["nodes"] if n["id"] == "proj-q")
    check("赢方整笔生效（proj 容量到目标值）", pnode["capacity"] == target, str(pnode))
    code, pv = c.post("/api/capacity-plans/preview", {"plan_id": loser})
    check("输家重新预览看到当前现实（超卖或陈旧后的拒绝原因）",
          code == 200 and pv["valid"] is False and pv["items"][0]["reason"], str(pv.get("items")))
    ok, failed = audit_ok(c)
    check("并发场景审计通过", ok, str(failed))


def test_tamper_detection(srv: Server):
    print("\n[11] 账本篡改检测（哈希链 + 重放不等价）")
    c = srv.client
    ok, _ = audit_ok(c)
    check("正常状态审计通过", ok)
    # 直接改 SQLite 里的事件载荷
    import sqlite3
    conn = sqlite3.connect(srv.db.name)
    row = conn.execute("SELECT seq, payload FROM events ORDER BY seq LIMIT 1").fetchone()
    payload = json.loads(row[1])
    if "capacity" in payload:
        payload["capacity"] += 999
    conn.execute("UPDATE events SET payload=? WHERE seq=?", (json.dumps(payload, sort_keys=True), row[0]))
    conn.commit()
    conn.close()
    code, a = c.get("/api/audit")
    chain_check = next(x for x in a["checks"] if x["name"] == "event_hash_chain")
    check("篡改事件载荷后哈希链审计失败", code == 200 and not a["ok"] and not chain_check["ok"],
          str(chain_check))


def main():
    srv = Server()
    try:
        test_basic_flow(srv)
        test_usage_release_consumed_kept(srv)
        test_command_replay(srv)
        test_capacity_shrink(srv)
        test_expiry_and_offline(srv)
        test_concurrent_grants(srv)
        test_concurrent_replay_safe(srv)
        test_restart_rebuild(srv)
        test_lease_amend(srv)
        test_capacity_plans(srv)
        test_tamper_detection(srv)

        print("\n[终态审计]")
        code, a = srv.client.get("/api/audit")
        for chk in a["checks"]:
            print(f"  {'✓' if chk['ok'] else '✗'} {chk['name']}: {chk['detail']}")
    finally:
        srv.stop()

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
