"""配额租约服务 —— 领域层（事件折叠 + 命令处理）。

账户模型（显式逐节点记账）
--------------------------
节点构成 platform → organization → project → client 四层树，
每条租约是父子节点之间的一条边。所有节点有一个余额账户 avail：

  * Bootstrapped：       platform +capacity
  * CapacityAdjusted：   platform += (new - old)          （缩容检查见下）
  * LeaseGranted A：     lessor -A，holder +A
  * UsageRecorded C：    消费只发生在租约持有者，holder -C；
                         C 同时沿持有者的入边一路向上传播
                         （祖先租约的 consumed 增加、可退余量收缩）
  * LeaseClosed：        holder -(A-consumed)，lessor +(A-consumed)
                         —— 关闭只退“尚未消费”部分，消费永久保留

由上面的记账规则可推出：

  Σ avail(N) + 总消费 = 全局容量      （全局守恒，审计断言）
  avail(N) >= 0                        （任何时刻不得超卖）

子级只能在父级 *当前可用* 额度内申请：grant 校验 avail(lessor)。
父级缩容（revoke）若子级已把余量分配给孙级，会令子级余额为负 → 拒绝。
"""

from __future__ import annotations

import math
import time
import uuid
from typing import Any

# ----------------------------------------------------------------------
# 错误类型（映射到 HTTP 状态码）
# ----------------------------------------------------------------------


class DomainError(Exception):
    status = 400

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        # 冲突时回传最新状态（当前修订号/截止时间/租约状态），
        # 让失败方无需另查即可据此重试
        self.details = details


class ValidationError(DomainError):
    status = 400


class NotFoundError(DomainError):
    status = 404


class ConflictError(DomainError):
    status = 409


# ----------------------------------------------------------------------
# 常量
# ----------------------------------------------------------------------

PLATFORM_ID = "platform"

NODE_PREFIX = {"organization": "O", "project": "P", "client": "C"}
# 合法的父子层级
PARENT_TYPE = {
    "organization": "platform",
    "project": "organization",
    "client": "project",
}
CLOSE_REASONS = {"released", "revoked", "expired", "offline"}


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ----------------------------------------------------------------------
# 投影
# ----------------------------------------------------------------------


class Projection:
    def __init__(self) -> None:
        self.seq = 0
        self.bootstrapped = False
        self.global_capacity = 0
        self.total_consumed = 0
        self.nodes: dict[str, dict[str, Any]] = {}
        self.balance: dict[str, int] = {}
        # 每个节点的显式容量（platform 与 global_capacity 镜像）。
        # 非平台节点容量为 0 时，其额度完全来自父级租约。
        self.node_capacity: dict[str, int] = {}
        self.children: dict[str, list[str]] = {}
        self.leases: dict[str, dict[str, Any]] = {}
        self.usage_event_ids: set[str] = set()
        # 容量调整草案（从 CapacityPlan* 生命周期事件折叠，权威行在 capacity_plans 表）
        self.plans: dict[str, dict[str, Any]] = {}

    # ---- 派生读模型 --------------------------------------------------

    @staticmethod
    def edge_remaining(lease: dict[str, Any]) -> int:
        return lease["amount"] - lease["consumed"]

    def active_leases(self) -> list[dict[str, Any]]:
        """可接受新消费/新授约的活动租约。"""
        return [l for l in self.leases.values() if l["status"] == "active"]

    def live_leases(self) -> list[dict[str, Any]]:
        """记账意义上未结算的租约（active + pending_expired）。

        pending_expired 已到期但因仍支撑下游而暂时无法退款，余额照旧占用；
        下游陆续关闭后由 sweep 自底向上结算为 closed。
        """
        return [
            l
            for l in self.leases.values()
            if l["status"] in ("active", "pending_expired")
        ]

    # 凡涉及余额/吸收/占用统计，都应包含 pending
    def open_leases(self) -> list[dict[str, Any]]:
        return self.live_leases()

    def incoming(self, node_id: str, active_only: bool = True) -> list[dict[str, Any]]:
        leases = self.live_leases() if active_only else self.leases.values()
        return [l for l in leases if l["holder_id"] == node_id]

    def outgoing(self, node_id: str, active_only: bool = True) -> list[dict[str, Any]]:
        leases = self.live_leases() if active_only else self.leases.values()
        return [l for l in leases if l["lessor_id"] == node_id]

    def avail(self, node_id: str) -> int:
        if node_id not in self.nodes:
            raise NotFoundError(f"节点不存在: {node_id}")
        return self.balance.get(node_id, 0)

    def _node_consumed(self, node_id: str) -> int:
        return sum(l["consumed"] for l in self.incoming(node_id, active_only=False))

    def lease_view(self, lease: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        view = {
            **lease,
            "remaining": self.edge_remaining(lease),
            "amendment_count": len(lease["amendments"]),
            "lessor_type": self.nodes.get(lease["lessor_id"], {}).get("type"),
            "holder_type": self.nodes.get(lease["holder_id"], {}).get("type"),
        }
        if lease["status"] == "active" and lease["expires_at"] is not None:
            view["seconds_remaining"] = max(0, int(lease["expires_at"] - now))
        else:
            view["seconds_remaining"] = None
        return view

    def node_view(self, node_id: str) -> dict[str, Any]:
        n = self.nodes[node_id]
        in_open = self.incoming(node_id)
        out_open = self.outgoing(node_id)
        view = {
            **n,
            "available": self.avail(node_id),
            "capacity": self.node_capacity.get(node_id, 0),
            "incoming_remaining": sum(self.edge_remaining(l) for l in in_open),
            "outgoing_granted": sum(l["amount"] for l in out_open),
            "consumed": self._node_consumed(node_id),
            "children": list(self.children.get(node_id, [])),
        }
        if node_id == PLATFORM_ID:
            view["capacity"] = self.global_capacity
        return view

    def to_state(self) -> dict[str, Any]:
        order = [PLATFORM_ID] if self.bootstrapped else []

        def walk(nid: str) -> None:
            for cid in self.children.get(nid, []):
                order.append(cid)
                walk(cid)

        if self.bootstrapped:
            walk(PLATFORM_ID)
        nodes = [self.node_view(nid) for nid in order]
        leases = [
            self.lease_view(l)
            for l in sorted(self.leases.values(), key=lambda x: x["granted_at"])
        ]
        return {
            "bootstrapped": self.bootstrapped,
            "global_capacity": self.global_capacity,
            "total_consumed": self.total_consumed,
            "total_available": sum(n["available"] for n in nodes),
            "nodes": nodes,
            "leases": leases,
            "active_lease_count": len([l for l in leases if l["status"] == "active"]),
            "pending_lease_count": len(
                [l for l in leases if l["status"] == "pending_expired"]
            ),
            "server_time": time.time(),
        }

    def digest(self) -> dict[str, Any]:
        """规范化摘要：用于比对“重放重建”与“线上投影”。"""
        return {
            "global_capacity": self.global_capacity,
            "total_consumed": self.total_consumed,
            "balances": sorted(self.balance.items()),
            "node_capacity": sorted(self.node_capacity.items()),
            "leases": sorted(
                (
                    lid,
                    l["status"],
                    l["amount"],
                    l["consumed"],
                    l["own_consumed"],
                    l["reason"],
                    l["revision"],
                    l["expires_at"],
                )
                for lid, l in self.leases.items()
            ),
            "plans": sorted(
                (
                    pid,
                    pl["status"],
                    pl["base_seq"],
                    pl["last_preview_seq"],
                    len(pl["changes"]),
                )
                for pid, pl in self.plans.items()
            ),
            "presence": sorted(
                (nid, n["online"]) for nid, n in self.nodes.items()
            ),
        }


# ----------------------------------------------------------------------
# 事件折叠（重建与线上追加共用同一份代码）
# ----------------------------------------------------------------------


def apply_event(p: Projection, evt: dict[str, Any]) -> None:
    t = evt["type"]
    p.seq = evt["seq"]

    if t == "Bootstrapped":
        p.bootstrapped = True
        p.global_capacity = evt["capacity"]
        p.balance[PLATFORM_ID] = evt["capacity"]
        p.node_capacity[PLATFORM_ID] = evt["capacity"]
        if PLATFORM_ID not in p.nodes:
            p.nodes[PLATFORM_ID] = {
                "id": PLATFORM_ID,
                "type": "platform",
                "parent_id": None,
                "online": True,
            }
            p.children.setdefault(PLATFORM_ID, [])

    elif t == "NodeRegistered":
        p.nodes[evt["node_id"]] = {
            "id": evt["node_id"],
            "type": evt["node_type"],
            "parent_id": evt["parent_id"],
            # 注册即视为在线；需显式调用 presence 下线
            "online": True,
        }
        p.balance.setdefault(evt["node_id"], 0)
        p.node_capacity.setdefault(evt["node_id"], 0)
        p.children.setdefault(evt["parent_id"], []).append(evt["node_id"])
        p.children.setdefault(evt["node_id"], [])

    elif t == "CapacityAdjusted":
        delta = evt["new_capacity"] - p.global_capacity
        p.global_capacity = evt["new_capacity"]
        p.node_capacity[PLATFORM_ID] = evt["new_capacity"]
        p.balance[PLATFORM_ID] += delta

    elif t == "NodeCapacityAdjusted":
        # 显式节点容量调整：容量与余额沿父子边移动。
        # 缩容 Δ<0：节点把 −Δ 余量退回父级；扩容 Δ>0：从父级支取 Δ。
        # 平台节点永远走 CapacityAdjusted（全局容量入口），这里只处理子节点。
        node_id = evt["node_id"]
        delta = evt["new_capacity"] - p.node_capacity.get(node_id, 0)
        p.node_capacity[node_id] = evt["new_capacity"]
        p.balance[node_id] += delta
        p.balance[p.nodes[node_id]["parent_id"]] -= delta

    elif t == "LeaseGranted":
        p.leases[evt["lease_id"]] = {
            "id": evt["lease_id"],
            "holder_id": evt["holder_id"],
            "lessor_id": evt["lessor_id"],
            "amount": evt["amount"],
            "consumed": 0,          # 直接 + 沿层级向上吸收的消费
            "own_consumed": 0,      # 仅本租约持有者的直接消费
            "status": "active",
            "granted_at": evt["granted_at"],
            "expires_at": evt["expires_at"],
            "closed_at": None,
            "reason": None,
            # 乐观修订号：授予即为 0，每次有效期成功修订 +1
            "revision": 0,
            # 有效期修订历史（权威历史在事件账本；这里是折叠出的读模型）
            "amendments": [],
        }
        p.balance[evt["lessor_id"]] -= evt["amount"]
        p.balance[evt["holder_id"]] += evt["amount"]

    elif t == "UsageRecorded":
        lease = p.leases[evt["lease_id"]]
        lease["consumed"] += evt["amount"]
        lease["own_consumed"] += evt["amount"]
        p.balance[evt["holder_id"]] -= evt["amount"]
        p.total_consumed += evt["amount"]
        p.usage_event_ids.add(evt["usage_event_id"])
        # 直接持有边的 consumed 在上面已记。消费 C 还要在*每个祖先层*
        # 的活动入边上各吸收一次 C（按授予顺序贪心）：这代表该层可退
        # 给其上级的余量也收缩了 C。中间节点自身余额不变（入边与出边
        # 余量同步收缩），只有直接持有者余额 -C。
        # 某祖先层入边不足以覆盖 C（部分余额来自平台 base/历史留存）
        # 时，未覆盖部分无需记账——它不对应任何活动租约的退款。
        ancestor = p.nodes[evt["holder_id"]]["parent_id"]
        while ancestor is not None:
            still = evt["amount"]
            # pending 入边仍占用余额，同样要吸收（它们终将被结算退款）
            incoming = sorted(
                (l for l in p.live_leases() if l["holder_id"] == ancestor),
                key=lambda l: l["granted_at"],
            )
            for upper in incoming:
                if still <= 0:
                    break
                room = upper["amount"] - upper["consumed"]
                if room <= 0:
                    continue
                absorbed = min(still, room)
                upper["consumed"] += absorbed
                still -= absorbed
            ancestor = p.nodes[ancestor]["parent_id"]

    elif t == "LeaseExpired":
        # 到期：只标记挂起，不动余额。这样仍支撑下游活动租约的中间层
        # 租约不会把持有者余额抽成负数；待下游释放后再结算退款。
        lease = p.leases[evt["lease_id"]]
        lease["status"] = "pending_expired"
        lease["expired_marked_at"] = evt["marked_at"]

    elif t == "LeaseAmended":
        # 有效期修订：只更新截止时间与修订号，不动余额/消费量
        lease = p.leases[evt["lease_id"]]
        lease["revision"] = evt["revision"]
        lease["expires_at"] = evt["new_expires_at"]
        lease["amendments"].append(
            {
                "revision": evt["revision"],
                "old_expires_at": evt["old_expires_at"],
                "new_expires_at": evt["new_expires_at"],
                "amended_at": evt["amended_at"],
            }
        )

    elif t == "LeaseClosed":
        lease = p.leases[evt["lease_id"]]
        remaining = lease["amount"] - lease["consumed"]
        lease["status"] = "closed"
        lease["reason"] = evt["reason"]
        lease["closed_at"] = evt["closed_at"]
        # 关闭同样推进版本号（与续约互斥的依据）
        lease["revision"] = evt["revision"]
        # 只退尚未消费部分；已消费部分不复活
        p.balance[lease["holder_id"]] -= remaining
        p.balance[lease["lessor_id"]] += remaining

    elif t == "ClientPresenceChanged":
        p.nodes[evt["client_id"]]["online"] = evt["online"]

    # ---- 批量容量调整草案生命周期（不动余额；余额只随发布产生的
    #      NodeCapacityAdjusted 业务事件移动） ----

    elif t == "CapacityPlanCreated":
        p.plans[evt["plan_id"]] = {
            "id": evt["plan_id"],
            "status": "draft",
            "changes": [dict(c) for c in evt["changes"]],
            "base_seq": evt["base_seq"],
            "last_preview_seq": None,
            "last_preview_at": None,
            "created_at": evt["created_at"],
            "published_at": None,
            "discarded_at": None,
            "business_event_count": 0,
        }

    elif t == "CapacityPlanPreviewed":
        plan = p.plans[evt["plan_id"]]
        plan["last_preview_seq"] = evt["preview_seq"]
        plan["last_preview_at"] = evt["previewed_at"]

    elif t == "CapacityPlanPublished":
        plan = p.plans[evt["plan_id"]]
        plan["status"] = "published"
        plan["published_at"] = evt["published_at"]
        plan["business_event_count"] = evt["event_count"]

    elif t == "CapacityPlanDiscarded":
        plan = p.plans[evt["plan_id"]]
        plan["status"] = "discarded"
        plan["discarded_at"] = evt["discarded_at"]

    else:
        raise ValidationError(f"未知事件类型: {t}")


# ----------------------------------------------------------------------
# 命令校验
# ----------------------------------------------------------------------


def _require_bootstrap(p: Projection) -> None:
    if not p.bootstrapped:
        raise ConflictError("平台尚未初始化，请先设置全局容量 (bootstrap)")


def _require_int(value: Any, field: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field} 必须是整数")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{field} 必须 >= {minimum}")
    return value


def _lease_state(lease: dict[str, Any]) -> dict[str, Any]:
    """租约最新状态摘要（随冲突返回，供失败方据此重试）。"""
    return {
        "lease_id": lease["id"],
        "status": lease["status"],
        "current_revision": lease["revision"],
        "expires_at": lease["expires_at"],
        "reason": lease["reason"],
    }


def _get_active_lease(p: Projection, lease_id: Any, action: str = "操作") -> dict[str, Any]:
    if not lease_id or not isinstance(lease_id, str):
        raise ValidationError("lease_id 必填")
    lease = p.leases.get(lease_id)
    if lease is None:
        raise NotFoundError(f"租约不存在: {lease_id}")
    if lease["status"] == "pending_expired":
        raise ConflictError(
            f"租约 {lease_id} 已到期，等待余量结算，不能{action}",
            details=_lease_state(lease),
        )
    if lease["status"] != "active":
        raise ConflictError(
            f"租约 {lease_id} 已关闭（{lease['reason']}），不能{action}",
            details=_lease_state(lease),
        )
    return lease


def _close_event(lease: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "type": "LeaseClosed",
        "payload": {
            "lease_id": lease["id"],
            "reason": reason,
            # 关闭也是一次状态变更：修订号 +1，与续约共享同一版本
            "revision": lease["revision"] + 1,
            "released_amount": lease["amount"] - lease["consumed"],
            "consumed_amount": lease["consumed"],
            "closed_at": time.time(),
        },
    }


def _expire_event(lease: dict[str, Any], now: float) -> dict[str, Any]:
    return {
        "type": "LeaseExpired",
        "payload": {"lease_id": lease["id"], "marked_at": now},
    }


def handle_bootstrap(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    if p.bootstrapped:
        raise ConflictError("平台已初始化；调整容量请使用 /api/capacity")
    capacity = _require_int(body.get("capacity"), "capacity", 0)
    return [{"type": "Bootstrapped", "payload": {"capacity": capacity}}]


def handle_register(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    node_type = body.get("type")
    if node_type not in PARENT_TYPE:
        raise ValidationError(f"type 必须是 {sorted(PARENT_TYPE)} 之一")
    parent_id = body.get("parent_id")
    expected = PARENT_TYPE[node_type]
    if parent_id not in p.nodes:
        raise NotFoundError(f"父节点不存在: {parent_id}")
    if p.nodes[parent_id]["type"] != expected:
        raise ValidationError(
            f"{node_type} 的父级必须是 {expected}，实际为 {p.nodes[parent_id]['type']}"
        )
    node_id = body.get("node_id") or _new_id(NODE_PREFIX[node_type])
    if not isinstance(node_id, str) or not node_id:
        raise ValidationError("node_id 非法")
    if node_id in p.nodes or node_id == PLATFORM_ID:
        raise ConflictError(f"节点 ID 已存在: {node_id}")
    return [
        {
            "type": "NodeRegistered",
            "payload": {
                "node_id": node_id,
                "node_type": node_type,
                "parent_id": parent_id,
            },
        }
    ]


def handle_capacity(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    new_capacity = _require_int(body.get("capacity"), "capacity", 0)
    # 平台余额 = cap − Σ未结算出边授予量 − Σ已关闭平台边累计消费。
    # 缩容 Δ=(new-old) 会直接从平台余额扣减，因此下限是“使平台余额恰好为 0”
    # 的容量：当前容量 − 当前平台余额。低于它平台就会超卖为负。
    floor = p.global_capacity - p.avail(PLATFORM_ID)
    if new_capacity < floor:
        raise ConflictError(
            f"缩容被拒绝：已承诺/消费的额度要求容量 >= {floor}，"
            f"请求 {new_capacity} 会导致平台超卖"
        )
    if new_capacity == p.global_capacity:
        raise ConflictError("容量未变化")
    return [
        {
            "type": "CapacityAdjusted",
            "payload": {"old_capacity": p.global_capacity, "new_capacity": new_capacity},
        }
    ]


def handle_grant(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    lessor_id = body.get("lessor_id")
    holder_id = body.get("holder_id")
    amount = _require_int(body.get("amount"), "amount", 1)

    if lessor_id not in p.nodes:
        raise NotFoundError(f"父节点不存在: {lessor_id}")
    if holder_id not in p.nodes:
        raise NotFoundError(f"子节点不存在: {holder_id}")
    holder = p.nodes[holder_id]
    if holder["parent_id"] != lessor_id:
        raise ValidationError("租约只能授予直接下级（不能跨级/倒挂）")
    if lessor_id == PLATFORM_ID and holder["type"] != "organization":
        raise ValidationError("平台只能向组织授约")
    if holder["type"] == "client" and not holder["online"]:
        raise ConflictError(f"{holder_id} 已离线，不能向其授予租约")

    # 核心防超卖：只能在父级当前可用额度内申请（锁内读-校验-写）
    parent_available = p.avail(lessor_id)
    if amount > parent_available:
        raise ConflictError(
            f"超卖拒绝：申请 {amount}，父级 {lessor_id} 可用仅 {parent_available}"
        )

    ttl = body.get("ttl_seconds")
    if ttl is not None:
        ttl = _require_int(ttl, "ttl_seconds", 1)
    now = time.time()
    lease_id = body.get("lease_id") or _new_id("L")
    if not isinstance(lease_id, str) or not lease_id:
        raise ValidationError("lease_id 非法")
    if lease_id in p.leases:
        raise ConflictError(f"租约 ID 已存在: {lease_id}")

    return [
        {
            "type": "LeaseGranted",
            "payload": {
                "lease_id": lease_id,
                "lessor_id": lessor_id,
                "holder_id": holder_id,
                "amount": amount,
                "granted_at": now,
                "expires_at": now + ttl if ttl is not None else None,
            },
        }
    ]


def handle_usage(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    lease_id = body.get("lease_id")
    amount = _require_int(body.get("amount"), "amount", 1)
    usage_event_id = body.get("usage_event_id")
    if not usage_event_id or not isinstance(usage_event_id, str):
        raise ValidationError("usage_event_id 必填（用量回执的幂等键）")
    if usage_event_id in p.usage_event_ids:
        raise ConflictError(f"用量回执已处理: {usage_event_id}")

    lease = _get_active_lease(p, lease_id, "记录用量")
    holder = p.nodes[lease["holder_id"]]
    if holder["type"] == "client" and not holder["online"]:
        raise ConflictError(
            "客户端离线，不能记录用量", details=_lease_state(lease)
        )
    _check_expected_revision(p, lease, body)
    remaining = lease["amount"] - lease["consumed"]
    if amount > remaining:
        raise ConflictError(f"超卖拒绝：本次用量 {amount} 超过租约剩余 {remaining}")

    return [
        {
            "type": "UsageRecorded",
            "payload": {
                "lease_id": lease_id,
                "holder_id": lease["holder_id"],
                "amount": amount,
                "usage_event_id": usage_event_id,
                "recorded_at": time.time(),
            },
        }
    ]


def _check_expected_revision(p: Projection, lease: dict[str, Any], body: dict[str, Any]) -> None:
    """归还/撤销等写命令可携带 expected_revision 做乐观并发。

    与修订命令共享同一个租约版本号：任何成功的状态变更（修订或关闭）
    都会让修订号 +1，因此同一版本上竞争的两个写操作只有一个能落账，
    另一方在此被拒（409）并看到最新状态。不传则不做版本检查（向后兼容）。
    """
    if body.get("expected_revision") is None:
        return
    expected = _require_int(body.get("expected_revision"), "expected_revision", 0)
    if expected != lease["revision"]:
        raise ConflictError(
            f"修订号冲突：提交 {expected}，当前为 {lease['revision']}"
            "（租约刚被其他操作变更，请读取最新状态后重试）",
            details=_lease_state(lease),
        )


def _guard_close(p: Projection, lease: dict[str, Any], action: str) -> None:
    """关闭单条租约的通用防超卖守卫。

    关闭后持有者余额变为 avail − 剩余量；若其仍有活动下级租约
    占用着这份余量，余额会变负 —— 拒绝（显式 release/revoke 均适用）。
    到期路径走自底向上级联关闭、离线只关客户端叶子租约，不经此守卫。
    """
    remaining = lease["amount"] - lease["consumed"]
    if p.avail(lease["holder_id"]) - remaining < 0:
        raise ConflictError(
            f"{action}被拒绝：{lease['holder_id']} 仍有下级活动租约占用其余量，"
            "请先收回/等待下级租约释放后再操作"
        )


def handle_release(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    lease = _get_active_lease(p, body.get("lease_id"), "归还")
    _check_expected_revision(p, lease, body)
    _guard_close(p, lease, "归还")
    return [_close_event(lease, "released")]


def handle_revoke(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    lease = _get_active_lease(p, body.get("lease_id"), "撤销")
    _check_expected_revision(p, lease, body)
    _guard_close(p, lease, "撤销/缩容")
    return [_close_event(lease, "revoked")]


def handle_amend(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    """修订（延长）仍在生效租约的有效期。

    乐观并发：持有人提交它所看到的 ``expected_revision`` 与一个更晚的
    截止时间。成功后修订号 +1；编号过旧/过新、租约已关闭或等待结算、
    客户端已下线、目标时间没有向后延长都返回冲突（409），并回传租约
    最新状态。相同 command_id 的重试由命令回执表重放第一次的结果。

    目标截止时间二选一：相对 ``extend_seconds``（在*当前截止时间*
    之上顺延的增量）或绝对 Unix 时间 ``new_expires_at``。两者都必须让
    截止时间严格变晚。修订只改时间，不动任何余额。
    """
    _require_bootstrap(p)
    lease = _get_active_lease(p, body.get("lease_id"), "修订有效期")

    holder = p.nodes[lease["holder_id"]]
    if holder["type"] == "client" and not holder["online"]:
        raise ConflictError(
            f"客户端 {lease['holder_id']} 已下线，不能修订租约有效期",
            details=_lease_state(lease),
        )

    expected_revision = _require_int(
        body.get("expected_revision", 0), "expected_revision", 0
    )
    if expected_revision != lease["revision"]:
        raise ConflictError(
            f"修订号冲突：提交 {expected_revision}，当前为 {lease['revision']}"
            "（已有更新的修订或租约状态已变化），请读取最新状态后重试",
            details=_lease_state(lease),
        )

    if lease["expires_at"] is None:
        raise ConflictError(
            f"租约 {lease['id']} 是永久租约（无截止时间），无需修订有效期",
            details=_lease_state(lease),
        )

    has_ext = body.get("extend_seconds") is not None
    has_deadline = body.get("new_expires_at") is not None
    if has_ext == has_deadline:
        raise ValidationError(
            "必须且只能提供 extend_seconds 或 new_expires_at 之一"
        )
    if has_ext:
        # 延长量：在当前截止时间（而非“现在”）之上顺延，
        # 语义为明确的“向后延长”，与剩余多少无关
        extend = _require_int(body.get("extend_seconds"), "extend_seconds", 1)
        new_expires_at = lease["expires_at"] + extend
    else:
        raw = body.get("new_expires_at")
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValidationError("new_expires_at 必须是 Unix 时间戳（数字）")
        if not math.isfinite(raw) or raw <= 0:
            raise ValidationError("new_expires_at 必须是正数时间戳")
        new_expires_at = float(raw)

    if new_expires_at <= lease["expires_at"]:
        raise ConflictError(
            "有效期修订被拒绝：新截止时间必须晚于当前截止时间（不能缩短/不变）",
            details={
                **_lease_state(lease),
                "requested_expires_at": new_expires_at,
            },
        )

    return [
        {
            "type": "LeaseAmended",
            "payload": {
                "lease_id": lease["id"],
                "revision": lease["revision"] + 1,
                "old_expires_at": lease["expires_at"],
                "new_expires_at": new_expires_at,
                "amended_at": time.time(),
            },
        }
    ]


def handle_presence(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    _require_bootstrap(p)
    client_id = body.get("client_id")
    if client_id not in p.nodes or p.nodes[client_id]["type"] != "client":
        raise NotFoundError(f"客户端不存在: {client_id}")
    online = bool(body.get("online"))
    events: list[dict[str, Any]] = [
        {
            "type": "ClientPresenceChanged",
            "payload": {"client_id": client_id, "online": online},
        }
    ]
    if not online:
        # 离线：结算该客户端全部未关闭租约（active 或已挂起到期），
        # 仅退未消费余量。客户端是叶子节点（无出边），结算恒安全。
        for lease in p.live_leases():
            if lease["holder_id"] == client_id:
                events.append(_close_event(lease, "offline"))
    return events


def _depth(p: Projection, node_id: str) -> int:
    d = 0
    cur = p.nodes[node_id]["parent_id"]
    while cur is not None:
        d += 1
        cur = p.nodes[cur]["parent_id"]
    return d


def expired_lease_events(p: Projection, now: float) -> list[dict[str, Any]]:
    """确定性到期结算，在每条命令/查询前与后台定时器中运行。

    纯函数：只产出事件，不修改投影（投影由 apply_event 统一折叠）。
    内部在*副本标记*上做判定，以正确处理“同批先标记后结算”。

    两阶段：
      1) 凡到期且仍 active 的租约 -> LeaseExpired（标记 pending，不动余额）；
      2) 自底向上反复扫描 pending 租约，凡持有者已释放出足够余量
         （下游已先到期/被释放）的 -> LeaseClosed 退款，直到定点。

    这样中间层租约即便到期，也只会在其不再支撑活动下游时才收回余量，
    严格保证任何节点余额不为负、不超卖。
    """
    events: list[dict[str, Any]] = []
    # 副本状态：lease_id -> 本批结束后的状态（供结算可达性判定）
    status_after: dict[str, str] = {lid: l["status"] for lid, l in p.leases.items()}

    due = [
        l
        for l in p.active_leases()
        if l["expires_at"] is not None and l["expires_at"] <= now
    ]
    for lease in due:
        events.append(_expire_event(lease, now))
        status_after[lease["id"]] = "pending_expired"

    def virtual_available(node_id: str) -> int:
        """按本批事件*部分应用*后的余额估算：供 _settleable 判定。"""
        bal = p.avail(node_id)
        for ev in events:
            pl = ev["payload"]
            if ev["type"] == "LeaseClosed":
                lid = pl["lease_id"]
                lease = p.leases[lid]
                rem = lease["amount"] - lease["consumed"]
                if lease["holder_id"] == node_id:
                    bal -= rem
                if lease["lessor_id"] == node_id:
                    bal += rem
        return bal

    # 固定点：反复找“到期标记后、按本批部分应用仍可结算”的 pending
    settled: set[str] = set()
    progressed = True
    while progressed:
        progressed = False
        candidates = sorted(
            (
                p.leases[lid]
                for lid, st in status_after.items()
                if st == "pending_expired" and lid not in settled
            ),
            key=lambda l: -_depth(p, l["holder_id"]),
        )
        for lease in candidates:
            remaining = lease["amount"] - lease["consumed"]
            if virtual_available(lease["holder_id"]) >= remaining:
                events.append(_close_event(lease, "expired"))
                settled.add(lease["id"])
                progressed = True
    return events


HANDLERS = {
    "bootstrap": handle_bootstrap,
    "register": handle_register,
    "capacity": handle_capacity,
    "grant": handle_grant,
    "usage": handle_usage,
    "amend": handle_amend,
    "release": handle_release,
    "revoke": handle_revoke,
    "presence": handle_presence,
}

# 从事件载荷提取命令结果片段（重放时用）
RESULT_EXTRACTORS = {
    "Bootstrapped": lambda pl: {"capacity": pl["capacity"]},
    "NodeRegistered": lambda pl: {"node_id": pl["node_id"], "type": pl["node_type"]},
    "CapacityAdjusted": lambda pl: {"capacity": pl["new_capacity"]},
    "LeaseGranted": lambda pl: {
        "lease_id": pl["lease_id"],
        "amount": pl["amount"],
        "expires_at": pl["expires_at"],
        "lessor_id": pl["lessor_id"],
        "holder_id": pl["holder_id"],
    },
    "UsageRecorded": lambda pl: {
        "lease_id": pl["lease_id"],
        "amount": pl["amount"],
        "usage_event_id": pl["usage_event_id"],
    },
    "LeaseExpired": lambda pl: {"lease_id": pl["lease_id"], "marked_at": pl["marked_at"]},
    "LeaseAmended": lambda pl: {
        "lease_id": pl["lease_id"],
        "revision": pl["revision"],
        "old_expires_at": pl["old_expires_at"],
        "new_expires_at": pl["new_expires_at"],
        "amended_at": pl["amended_at"],
    },
    "LeaseClosed": lambda pl: {
        "lease_id": pl["lease_id"],
        "reason": pl["reason"],
        "revision": pl["revision"],
        "released_amount": pl["released_amount"],
        "consumed_amount": pl["consumed_amount"],
    },
    "ClientPresenceChanged": lambda pl: {
        "client_id": pl["client_id"],
        "online": pl["online"],
    },
    "NodeCapacityAdjusted": lambda pl: {
        "node_id": pl["node_id"],
        "old_capacity": pl["old_capacity"],
        "new_capacity": pl["new_capacity"],
    },
    "CapacityPlanCreated": lambda pl: {"plan_id": pl["plan_id"], "base_seq": pl["base_seq"]},
    "CapacityPlanPreviewed": lambda pl: {
        "plan_id": pl["plan_id"],
        "preview_seq": pl["preview_seq"],
    },
    "CapacityPlanPublished": lambda pl: {
        "plan_id": pl["plan_id"],
        "business_event_count": pl["event_count"],
    },
    "CapacityPlanDiscarded": lambda pl: {"plan_id": pl["plan_id"]},
}
