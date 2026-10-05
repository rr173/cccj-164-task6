"""配额租约服务 —— 应用服务层。

职责：
  * 启动时从事件账本重放重建投影（账本是唯一事实来源）；
  * 串行执行命令：先到期扫描，再幂等去重，最后校验并原子提交；
  * 后台定时到期扫描；
  * 一致性审计：哈希链、非负余额、全局守恒、树/边完整性、
    重放重建与线上投影比对。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any

from .domain import (
    HANDLERS,
    PLATFORM_ID,
    RESULT_EXTRACTORS,
    ConflictError,
    NotFoundError,
    Projection,
    ValidationError,
    apply_event,
    expired_lease_events,
)
from .ledger import GENESIS_HASH, EventStore, canonical
from .plans import (
    PLAN_EVENT_TYPES,
    build_capacity_events,
    parse_changes,
    plan_created_event,
    plan_discarded_event,
    plan_previewed_event,
    plan_published_event,
    simulate_plan,
    summarize_intervening,
)


class StalePlanError(ConflictError):
    """草案基于的账本位置已过时（创建后发生了新的业务事件），必须重新预览。"""


class QuotaService:
    def __init__(self, db_path: str, sweep_interval: float = 1.0):
        self.store = EventStore(db_path)
        self.sweep_interval = sweep_interval
        self.projection = self._rebuild_from_log()
        self._stop_sweeper = threading.Event()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="lease-sweeper", daemon=True
        )
        self._sweeper.start()

    # ------------------------------------------------------------------
    # 重建
    # ------------------------------------------------------------------

    def _rebuild_from_log(self) -> Projection:
        """清空内存状态，完整重放事件（启动与审计共用）。"""
        p = Projection()
        events = self.store.load_events()
        for evt in events:
            apply_event(p, evt)
        return p

    def _apply_new(self, events: list[dict[str, Any]]) -> None:
        """把刚提交的事件（领域格式 {type,payload}）折叠进线上投影。"""
        base_seq = self.projection.seq
        for i, ev in enumerate(events, start=1):
            apply_event(
                self.projection,
                {"seq": base_seq + i, "type": ev["type"], **ev["payload"]},
            )

    def rebuild(self) -> Projection:
        with self.store.lock:
            return self._rebuild_from_log()
    # ------------------------------------------------------------------
    # 到期扫描
    # ------------------------------------------------------------------

    def _sweep_loop(self) -> None:
        while not self._stop_sweeper.wait(self.sweep_interval):
            try:
                self.run_expirations(time.time())
            except Exception:  # 扫描线程绝不能死
                pass

    def run_expirations(self, now: float | None = None) -> int:
        """关闭所有已到期租约（含级联）。返回关闭数量。"""
        now = time.time() if now is None else now
        with self.store.lock:
            events = expired_lease_events(self.projection, now)
            if not events:
                return 0
            # 幂等 command_id：同一秒内重复扫描不会产生重复登记
            lease_ids = sorted(ev["payload"]["lease_id"] for ev in events)
            command_id = "sys:expire:" + ",".join(
                lid[-8:] for lid in lease_ids
            )
            self.store.append_batch(
                events,
                command_id=command_id,
                fingerprint="system",
                result={"closed": len(events), "lease_ids": lease_ids},
                system=True,
            )
            self._apply_new(events)
            return len(events)

    # ------------------------------------------------------------------
    # 命令执行
    # ------------------------------------------------------------------

    def execute(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        if kind not in HANDLERS:
            raise ValidationError(f"未知命令: {kind}")
        command_id = body.get("command_id") or f"{kind}:auto:{canonical(body)}"
        fingerprint = canonical({"kind": kind, "body": body})

        with self.store.lock:
            # 任何命令前先确定性地结算到期租约
            self._apply_sweep_locked(time.time())

            existing = self.store.get_command(command_id)
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise ConflictError(
                        f"command_id {command_id} 已用于不同的请求体"
                    )
                return {**existing["result"], "replayed": True}

            events = HANDLERS[kind](self.projection, body)
            result = self._build_result(kind, events)
            self.store.append_batch(
                events, command_id=command_id, fingerprint=fingerprint, result=result
            )
            self._apply_new(events)
            return result

    def _apply_sweep_locked(self, now: float) -> None:
        events = expired_lease_events(self.projection, now)
        if not events:
            return
        lease_ids = sorted(ev["payload"]["lease_id"] for ev in events)
        self.store.append_batch(
            events,
            command_id="sys:expire:" + ",".join(lid[-8:] for lid in lease_ids),
            fingerprint="system",
            result={"closed": len(events), "lease_ids": lease_ids},
            system=True,
        )
        self._apply_new(events)

    @staticmethod
    def _build_result(kind: str, events: list[dict[str, Any]]) -> dict[str, Any]:
        items = []
        for ev in events:
            extractor = RESULT_EXTRACTORS.get(ev["type"])
            items.append(
                {"event_type": ev["type"], **(extractor(ev["payload"]) if extractor else {})}
            )
        result = {"command": kind, "event_count": len(events), "events": items}
        if len(items) == 1:
            result.update(items[0])
        return result

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def state(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            return self.projection.to_state()

    def events(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.store.lock:
            rows = self.store.load_hash_chain()
        out = []
        for r in rows[-limit:]:
            out.append(
                {
                    "seq": r["seq"],
                    "event_type": r["event_type"],
                    "command_id": r["command_id"],
                    "payload": json.loads(r["payload"]),
                }
            )
        return out

    # ------------------------------------------------------------------
    # 批量容量调整草案
    #
    # 生命周期： draft（创建）→ preview（可多次重新预览刷新基线）
    #                → published（原子写入整批容量事件）/ discarded（废弃）
    #
    # 陈旧判定用“业务水位” max seq(非草案生命周期事件)：创建草案后发生的
    # 任何授约/消费/续约/结算（含后台到期 sweep）都推进水位，发布时水位
    # 与最后一次预览不一致即拒绝并要求重新预览。
    # ------------------------------------------------------------------

    def _business_watermark(self) -> int:
        return self.store.max_business_seq(PLAN_EVENT_TYPES)

    def _require_plan(self, plan_id: Any) -> dict[str, Any]:
        if not plan_id or not isinstance(plan_id, str):
            raise ValidationError("plan_id 必填")
        row = self.store.get_plan_row(plan_id)
        if row is None:
            raise NotFoundError(f"容量调整草案不存在: {plan_id}")
        return row

    @staticmethod
    def _plan_view(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "plan_id": row["plan_id"],
            "status": row["status"],
            "changes": row["changes"],
            "base_seq": row["base_seq"],
            "last_preview_seq": row["last_preview_seq"],
            "last_preview": row["last_preview"],
            "publish_result": row["publish_result"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _dedupe_or_none(
        self, command_id: str, fingerprint: str
    ) -> dict[str, Any] | None:
        """相同 command_id 已存在：指纹一致则重放首次结果，否则冲突。"""
        existing = self.store.get_command(command_id)
        if existing is None:
            return None
        if existing["fingerprint"] != fingerprint:
            raise ConflictError(f"command_id {command_id} 已用于不同的请求体")
        return {**existing["result"], "replayed": True}

    def create_plan(self, body: dict[str, Any]) -> dict[str, Any]:
        # 创建是“每次都产生新草案”的命令：默认幂等键带随机唯一标识，
        # 避免两份内容相同的草案在命令回执表上互相碰撞；客户端可用显式
        # command_id 获得“同一草案创建请求重试只生效一次”的幂等性。
        command_id = body.get("command_id") or f"plan-create:{uuid.uuid4().hex}"
        fingerprint = canonical({"kind": "plan_create", "body": body})
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            replay = self._dedupe_or_none(command_id, fingerprint)
            if replay is not None:
                return replay

            changes = parse_changes(self.projection, body)
            plan_id = body.get("plan_id") or f"PL-{uuid.uuid4().hex[:12]}"
            if not isinstance(plan_id, str) or not plan_id:
                raise ValidationError("plan_id 非法")
            if self.store.get_plan_row(plan_id) is not None:
                raise ConflictError(f"草案 ID 已存在: {plan_id}")

            base_seq = self._business_watermark()
            now = time.time()
            event = plan_created_event(plan_id, changes, base_seq, now)
            result = {
                "command": "plan_create",
                "plan_id": plan_id,
                "status": "draft",
                "base_seq": base_seq,
                "changes": changes,
                "event_count": 1,
            }
            self.store.append_batch(
                [event],
                command_id=command_id,
                fingerprint=fingerprint,
                result=result,
                plan_change={
                    "mode": "insert",
                    "plan_id": plan_id,
                    "status": "draft",
                    "changes": changes,
                    "base_seq": base_seq,
                    "last_preview": None,
                },
            )
            self._apply_new([event])
            return result

    def preview_plan(self, body: dict[str, Any]) -> dict[str, Any]:
        plan_id = body.get("plan_id")
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            row = self._require_plan(plan_id)
            if row["status"] == "published":
                raise ConflictError(
                    f"草案 {plan_id} 已发布；重复发布/预览只会得到首次发布结果",
                    details={"plan_id": plan_id, "status": "published",
                             "publish_result": row["publish_result"]},
                )
            if row["status"] == "discarded":
                raise ConflictError(f"草案 {plan_id} 已废弃，不能再预览")

            plan = self.projection.plans[plan_id]
            preview = simulate_plan(self.projection, plan)
            watermark = self._business_watermark()
            # 预览本身针对的就是当前账本位置，因此对发布而言它是新鲜的；
            # drifted_from_base 仅提示“创建草案后账本已发生过业务变更”
            preview["fresh"] = True
            preview["drifted_from_base"] = watermark != row["base_seq"]
            preview["current_seq"] = watermark
            preview["base_seq"] = row["base_seq"]
            if preview["drifted_from_base"]:
                preview["intervening_changes"] = summarize_intervening(
                    self.store.events_after(row["base_seq"])
                )

            if row["last_preview_seq"] == watermark and row["last_preview"] is not None:
                # 水位未推进：该水位下投影确定，直接返回已存预览，
                # 不向哈希链重复追加 CapacityPlanPreviewed 事件
                return {"plan_id": plan_id, **row["last_preview"]}

            # 预览结果连同“最后预览基线”持久化：只有对最新水位做过预览，
            # 随后的发布才可能成功。预览事件是系统事件（不登记命令回执）。
            now = time.time()
            event = plan_previewed_event(plan_id, watermark, now)
            self.store.append_batch(
                [event],
                command_id=f"sys:plan-preview:{plan_id}:{watermark}",
                fingerprint="system",
                result={"plan_id": plan_id, "preview_seq": watermark},
                system=True,
                plan_change={
                    "mode": "update",
                    "plan_id": plan_id,
                    "status": "draft",
                    "last_preview_seq": watermark,
                    "last_preview": preview,
                },
            )
            self._apply_new([event])
            return {"plan_id": plan_id, **preview}

    def publish_plan(self, body: dict[str, Any]) -> dict[str, Any]:
        plan_id = body.get("plan_id")
        command_id = body.get("command_id") or f"plan-publish:{plan_id}"
        fingerprint = canonical({"kind": "plan_publish", "plan_id": plan_id})
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            replay = self._dedupe_or_none(command_id, fingerprint)
            if replay is not None:
                return replay

            row = self._require_plan(plan_id)
            if row["status"] == "published":
                # 重复发布只能得到首次发布结果
                return {**(row["publish_result"] or {}), "replayed": True}
            if row["status"] == "discarded":
                raise ConflictError(f"草案 {plan_id} 已废弃，不能发布")

            # 发布前提：必须先预览，且最后一次预览基于的账本位置仍是当前位置
            watermark = self._business_watermark()
            if row["last_preview_seq"] is None:
                raise ConflictError(
                    f"草案 {plan_id} 尚未预览，请先预览确认逐层余额与受影响租约后再发布"
                )
            if row["last_preview_seq"] != watermark:
                raise StalePlanError(
                    f"草案 {plan_id} 已陈旧：创建/最后预览后账本发生了新的授约、消费、"
                    "续约或结算，发布被拒绝，请重新预览后再发布",
                    details={
                        "plan_id": plan_id,
                        "preview_seq": row["last_preview_seq"],
                        "current_seq": watermark,
                        "intervening_changes": summarize_intervening(
                            self.store.events_after(row["last_preview_seq"])
                        ),
                        "hint": "调用 POST /api/capacity-plans/preview 重新预览",
                    },
                )

            # 针对*当前*线上投影重新模拟：任一条目不合法 → 整批不生效
            plan = self.projection.plans[plan_id]
            preview = simulate_plan(self.projection, plan)
            if not preview["valid"]:
                reasons = [
                    {"index": it["index"], "node_id": it["node_id"], "reason": it["reason"]}
                    for it in preview["items"]
                    if not it["valid"]
                ]
                raise ConflictError(
                    f"草案 {plan_id} 发布被拒绝：{len(reasons)} 项变更不合法，"
                    "整批未生效（原子批次，任一项不合法则全部不生效）",
                    details={"plan_id": plan_id, "rejected_items": reasons,
                             "preview": preview},
                )

            # 构造业务事件并在发布前做最后一次顺序校验（防御，正常不会触发）
            business_events = build_capacity_events(self.projection, plan["changes"])
            now = time.time()
            published_event = plan_published_event(
                plan_id, len(business_events), now
            )
            events = business_events + [published_event]
            result = {
                "command": "plan_publish",
                "plan_id": plan_id,
                "status": "published",
                "published_at": now,
                "applied_count": len(business_events),
                "items": preview["items"],
                "layer_balances": preview["layer_balances"],
                "affected_leases": preview["affected_leases"],
                "global_capacity_before": preview["global_capacity_before"],
                "global_capacity_after": preview["global_capacity_after"],
                "event_count": len(events),
            }
            self.store.append_batch(
                events,
                command_id=command_id,
                fingerprint=fingerprint,
                result=result,
                plan_change={
                    "mode": "update",
                    "plan_id": plan_id,
                    "status": "published",
                    "publish_result": result,
                },
            )
            self._apply_new(events)
            return result

    def discard_plan(self, body: dict[str, Any]) -> dict[str, Any]:
        plan_id = body.get("plan_id")
        command_id = body.get("command_id") or f"plan-discard:{plan_id}"
        fingerprint = canonical({"kind": "plan_discard", "plan_id": plan_id})
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            replay = self._dedupe_or_none(command_id, fingerprint)
            if replay is not None:
                return replay
            row = self._require_plan(plan_id)
            if row["status"] == "published":
                raise ConflictError(f"草案 {plan_id} 已发布，不能废弃")
            if row["status"] == "discarded":
                # 废弃天然幂等：重复废弃返回同一终态
                return {"command": "plan_discard", "plan_id": plan_id,
                        "status": "discarded", "replayed": True}

            now = time.time()
            event = plan_discarded_event(plan_id, now)
            result = {"command": "plan_discard", "plan_id": plan_id,
                      "status": "discarded", "event_count": 1}
            self.store.append_batch(
                [event],
                command_id=command_id,
                fingerprint=fingerprint,
                result=result,
                plan_change={
                    "mode": "update",
                    "plan_id": plan_id,
                    "status": "discarded",
                },
            )
            self._apply_new([event])
            return result

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            return self._plan_view(self._require_plan(plan_id))

    def list_plans(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            rows = self.store.list_plan_rows()
        return {"plans": [self._plan_view(r) for r in rows], "count": len(rows)}

    # ------------------------------------------------------------------
    # 一致性审计
    # ------------------------------------------------------------------

    def audit(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            checks: list[dict[str, Any]] = []

            # 1) 哈希链完整性 / 防篡改
            chain = self.store.load_hash_chain()
            prev_hash = GENESIS_HASH
            chain_ok = True
            broken_seq = None
            for r in chain:
                expect = EventStore._hash_event(
                    prev_hash,
                    r["event_type"],
                    __import__("json").loads(r["payload"]),
                    r["command_id"],
                    r["fingerprint"],
                )
                if expect != r["event_hash"] or r["prev_hash"] != prev_hash:
                    chain_ok = False
                    broken_seq = r["seq"]
                    break
                prev_hash = r["event_hash"]
            checks.append(
                {
                    "name": "event_hash_chain",
                    "ok": chain_ok,
                    "detail": "哈希链连续，事件载荷未被篡改"
                    if chain_ok
                    else f"链在 seq={broken_seq} 处断裂（疑似账本被修改）",
                }
            )

            # 2) 幂等命令表：每个非系统 command_id 只登记一次
            commands = self.store.load_commands()
            cmd_counts: dict[str, int] = {}
            for r in chain:
                if not r["command_id"].startswith("sys:"):
                    cmd_counts[r["command_id"]] = (
                        cmd_counts.get(r["command_id"], 0) + 1
                    )
            missing = [cid for cid in commands if cid not in cmd_counts]
            orphan = [cid for cid in cmd_counts if cid not in commands]
            idem_ok = not missing and not orphan
            checks.append(
                {
                    "name": "idempotent_command_registry",
                    "ok": idem_ok,
                    "detail": f"{len(commands)} 条命令均有唯一回执"
                    if idem_ok
                    else f"命令回执不一致 missing={missing} orphan={orphan}",
                }
            )

            # 3) 非负余额
            negatives = [
                (nid, bal)
                for nid, bal in self.projection.balance.items()
                if bal < 0
            ]
            checks.append(
                {
                    "name": "non_negative_balances",
                    "ok": not negatives,
                    "detail": "所有节点可用额度 >= 0（无超卖）"
                    if not negatives
                    else f"出现负余额: {negatives}",
                }
            )

            # 4) 全局守恒：Σavail + 总消费 = 全局容量
            total_avail = sum(self.projection.balance.values())
            conserved = total_avail + self.projection.total_consumed
            cap = self.projection.global_capacity
            cons_ok = conserved == cap
            checks.append(
                {
                    "name": "global_conservation",
                    "ok": cons_ok,
                    "detail": (
                        f"Σ可用({total_avail}) + 总消费({self.projection.total_consumed})"
                        f" = 全局容量({cap})"
                    ),
                }
            )

            # 5) 树与边的层级完整性
            structure_problems = []
            for nid, node in self.projection.nodes.items():
                if nid == PLATFORM_ID:
                    continue
                pid = node["parent_id"]
                if pid not in self.projection.nodes:
                    structure_problems.append(f"{nid} 的父节点 {pid} 缺失")
            for lid, lease in self.projection.leases.items():
                if (
                    lease["lessor_id"] not in self.projection.nodes
                    or lease["holder_id"] not in self.projection.nodes
                ):
                    structure_problems.append(f"租约 {lid} 引用了不存在的节点")
                elif (
                    self.projection.nodes[lease["holder_id"]]["parent_id"]
                    != lease["lessor_id"]
                ):
                    structure_problems.append(f"租约 {lid} 不是直接父子边")
            checks.append(
                {
                    "name": "hierarchy_integrity",
                    "ok": not structure_problems,
                    "detail": "所有租约均为合法父子边，节点树完整"
                    if not structure_problems
                    else "; ".join(structure_problems),
                }
            )

            # 6) 租约余量合法 + 已消费永不消失 + 状态机合法
            lease_problems = []
            for lid, lease in self.projection.leases.items():
                # 直接消费不能超过授予量；向上吸收分摊后每条边同样不越界
                if not (0 <= lease["own_consumed"] <= lease["amount"]):
                    lease_problems.append(f"{lid} 直接消费量越界")
                if not (0 <= lease["consumed"] <= lease["amount"]):
                    lease_problems.append(
                        f"{lid} 消费分摊越界 ({lease['consumed']}/{lease['amount']})"
                    )
                status = lease["status"]
                if status == "closed":
                    if lease["reason"] not in (
                        "released",
                        "revoked",
                        "expired",
                        "offline",
                    ):
                        lease_problems.append(f"{lid} 关闭原因非法")
                elif status not in ("active", "pending_expired"):
                    lease_problems.append(f"{lid} 状态非法: {status}")
                elif status == "active":
                    if lease["reason"] is not None:
                        lease_problems.append(f"{lid} 活动租约不应有关闭原因")
            checks.append(
                {
                    "name": "lease_bookkeeping",
                    "ok": not lease_problems,
                    "detail": "租约余量/消费/关闭状态均合法"
                    if not lease_problems
                    else "; ".join(lease_problems),
                }
            )

            # 7) 重放重建结果必须与线上投影逐字段一致
            rebuilt = self._rebuild_from_log()
            replay_ok = rebuilt.digest() == self.projection.digest()
            checks.append(
                {
                    "name": "replay_equivalence",
                    "ok": replay_ok,
                    "detail": f"从 {len(chain)} 条事件重放的状态与线上投影一致"
                    if replay_ok
                    else "重放状态与线上投影不一致（投影可能被外部破坏）",
                }
            )

            # 8) 容量调整草案：capacity_plans 表（权威行）与事件折叠出的
            #    草案状态逐字段一致；表状态机合法；发布结果与已落账事件吻合
            plan_problems = []
            table_rows = {r["plan_id"]: r for r in self.store.list_plan_rows()}
            log_plans = self.projection.plans
            for pid, row in table_rows.items():
                lp = log_plans.get(pid)
                if lp is None:
                    plan_problems.append(f"{pid} 有草案表行但账本无生命周期事件")
                    continue
                if row["status"] != lp["status"]:
                    plan_problems.append(
                        f"{pid} 表状态 {row['status']} 与账本状态 {lp['status']} 不一致"
                    )
                if row["changes"] != lp["changes"]:
                    plan_problems.append(f"{pid} 变更清单与账本不一致")
                if row["base_seq"] != lp["base_seq"]:
                    plan_problems.append(f"{pid} 创建时账本位置与账本不一致")
                if row["status"] not in ("draft", "published", "discarded"):
                    plan_problems.append(f"{pid} 状态非法: {row['status']}")
                if row["status"] == "published":
                    pr = row["publish_result"]
                    if pr is None or pr.get("applied_count") != lp["business_event_count"]:
                        plan_problems.append(f"{pid} 发布结果与落账事件数不一致")
            for pid in log_plans:
                if pid not in table_rows:
                    plan_problems.append(f"{pid} 账本有草案但 capacity_plans 表缺行")
            checks.append(
                {
                    "name": "capacity_plan_consistency",
                    "ok": not plan_problems,
                    "detail": (
                        f"{len(table_rows)} 份容量草案的表状态与账本折叠结果一致"
                        if not plan_problems
                        else "; ".join(plan_problems)
                    ),
                }
            )

            ok = all(c["ok"] for c in checks)
            return {
                "ok": ok,
                "checked_at": time.time(),
                "event_count": len(chain),
                "command_count": len(commands),
                "global_capacity": cap,
                "total_available": total_avail,
                "total_consumed": self.projection.total_consumed,
                "checks": checks,
            }

    def shutdown(self) -> None:
        self._stop_sweeper.set()
        self.store.close()
