"""批量容量调整草案 —— 领域逻辑（草案生命周期事件 + 预览模拟）。

语义
----
每个节点有一个显式容量；调整节点容量等价于在*父子账户之间移动余额*：

  扩容 Δ>0：节点余额 +Δ，父级余额 −Δ（父级必须出得起，否则拒绝）
  缩容 Δ<0：节点余额 +Δ（即退回 −Δ），父级余额 −(Δ)（即收回）；
            节点当前可用余额必须足以让出，否则拒绝（防超卖）

平台节点的容量就是全局容量，平台项发出的业务事件沿用 ``CapacityAdjusted``，
因此「全局守恒 Σ可用+总消费=全局容量」对整批调整天然恒成立。

草案（plan）本身只是一份编辑中的变更清单，不动任何余额；只有发布成功时，
才把全部容量变更作为*一个原子批次*写入事件账本。预览在内存副本上模拟，
逐条目给出拒绝原因、执行后的逐层余额与受影响租约。
"""

from __future__ import annotations

import time
from typing import Any

from .domain import (
    PLATFORM_ID,
    ConflictError,
    NotFoundError,
    Projection,
    ValidationError,
    _require_bootstrap,
    _require_int,
)

# 草案生命周期事件：不移动余额，只在哈希链上留下可审计轨迹
PLAN_EVENT_TYPES = {
    "CapacityPlanCreated",
    "CapacityPlanPreviewed",
    "CapacityPlanPublished",
    "CapacityPlanDiscarded",
}

# 会推进“业务水位”、从而使旧草案变陈旧的事件类型标签
BUSINESS_EVENT_LABELS = {
    "Bootstrapped": "初始化容量",
    "NodeRegistered": "注册节点",
    "CapacityAdjusted": "全局容量调整",
    "NodeCapacityAdjusted": "节点容量调整",
    "LeaseGranted": "授约",
    "UsageRecorded": "消费",
    "LeaseExpired": "到期挂起",
    "LeaseAmended": "续约",
    "LeaseClosed": "结算/归还/撤销/离线",
    "ClientPresenceChanged": "上/下线",
}


# ----------------------------------------------------------------------
# 结构校验（创建草案时；预览/发布时再做语义模拟）
# ----------------------------------------------------------------------


def parse_changes(p: Projection, body: dict[str, Any]) -> list[dict[str, Any]]:
    """校验草案请求体，返回规范化变更清单 [{node_id, capacity}]。"""
    _require_bootstrap(p)
    raw = body.get("changes")
    if not isinstance(raw, list) or not raw:
        raise ValidationError("changes 必须是非空数组（批量至少包含一条节点容量变更）")
    if len(raw) > 500:
        raise ValidationError("单批变更不能超过 500 条")

    changes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, item in enumerate(raw):
        where = f"第 {i + 1} 条变更"
        if not isinstance(item, dict):
            raise ValidationError(f"{where} 必须是对象")
        node_id = item.get("node_id")
        if not node_id or not isinstance(node_id, str):
            raise ValidationError(f"{where} 缺少 node_id")
        if node_id not in p.nodes:
            raise NotFoundError(f"{where}: 节点不存在: {node_id}")
        if node_id in seen:
            raise ValidationError(f"{where}: 节点 {node_id} 在同一草案中出现多次")
        seen.add(node_id)
        capacity = _require_int(item.get("capacity"), f"{where} 的 capacity", 0)
        changes.append({"node_id": node_id, "capacity": capacity})
    return changes


# ----------------------------------------------------------------------
# 事件构造
# ----------------------------------------------------------------------


def plan_created_event(
    plan_id: str, changes: list[dict[str, Any]], base_seq: int, now: float
) -> dict[str, Any]:
    return {
        "type": "CapacityPlanCreated",
        "payload": {
            "plan_id": plan_id,
            "changes": changes,
            "base_seq": base_seq,
            "created_at": now,
        },
    }


def plan_previewed_event(plan_id: str, preview_seq: int, now: float) -> dict[str, Any]:
    return {
        "type": "CapacityPlanPreviewed",
        "payload": {
            "plan_id": plan_id,
            "preview_seq": preview_seq,
            "previewed_at": now,
        },
    }


def plan_published_event(plan_id: str, event_count: int, now: float) -> dict[str, Any]:
    return {
        "type": "CapacityPlanPublished",
        "payload": {
            "plan_id": plan_id,
            "event_count": event_count,
            "published_at": now,
        },
    }


def plan_discarded_event(plan_id: str, now: float) -> dict[str, Any]:
    return {
        "type": "CapacityPlanDiscarded",
        "payload": {"plan_id": plan_id, "discarded_at": now},
    }


def build_capacity_events(
    p: Projection, changes: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """发布时按*当前*容量构造业务事件（与预览顺序一致）。

    平台项 → CapacityAdjusted（全局容量入口），其余节点 → NodeCapacityAdjusted
    （父子账户间移动余额）。调用前必须已通过 simulate_plan 全量校验。
    """
    now = time.time()
    events: list[dict[str, Any]] = []
    for item in changes:
        node_id = item["node_id"]
        new_capacity = item["capacity"]
        if node_id == PLATFORM_ID:
            # 平台项走全局容量入口；每个节点在草案中至多出现一次
            events.append(
                {
                    "type": "CapacityAdjusted",
                    "payload": {
                        "old_capacity": p.global_capacity,
                        "new_capacity": new_capacity,
                    },
                }
            )
        else:
            old_capacity = p.node_capacity.get(node_id, 0)
            events.append(
                {
                    "type": "NodeCapacityAdjusted",
                    "payload": {
                        "node_id": node_id,
                        "old_capacity": old_capacity,
                        "new_capacity": new_capacity,
                        "adjusted_at": now,
                    },
                }
            )
    return events


# ----------------------------------------------------------------------
# 预览模拟（纯函数：在副本上推演，绝不修改线上投影）
# ----------------------------------------------------------------------


def _ancestor_chain(p: Projection, node_id: str) -> list[str]:
    """平台 → node_id 的层级链。"""
    chain = [node_id]
    cur = node_id
    while cur != PLATFORM_ID:
        cur = p.nodes[cur]["parent_id"]
        chain.append(cur)
    chain.reverse()
    return chain


def simulate_plan(p: Projection, plan: dict[str, Any]) -> dict[str, Any]:
    """在内存副本上按顺序推演整批变更。

    返回：
      valid            全部条目合法时为 true（任一项不合法整批不可发布）
      items            每条变更的逐项结果（含拒绝原因）
      layer_balances   执行后的逐层余额（受影响节点及其祖先链）
      affected_leases  余额被触及的活动/挂起租约
      global_capacity  模拟后的全局容量
    """
    changes = plan["changes"]
    cap = dict(p.node_capacity)
    bal = dict(p.balance)
    global_capacity = p.global_capacity

    items: list[dict[str, Any]] = []
    all_valid = True
    touched: set[str] = set()

    for index, item in enumerate(changes):
        node_id = item["node_id"]
        new_capacity = item["capacity"]
        old_capacity = cap.get(node_id, 0)
        delta = new_capacity - old_capacity
        result: dict[str, Any] = {
            "index": index,
            "node_id": node_id,
            "old_capacity": old_capacity,
            "new_capacity": new_capacity,
            "delta": delta,
            "valid": True,
            "reason": None,
        }

        node = p.nodes.get(node_id)
        if node is None:
            result.update(valid=False, reason="节点不存在")
        elif delta == 0:
            result.update(valid=False, reason="容量未变化（新旧容量相同）")
        elif delta < 0:
            # 缩容：节点要把 −Δ 余量退回父级，自身余额不得被抽成负数
            room = bal.get(node_id, 0)
            floor = old_capacity - room
            if room + delta < 0:
                result.update(
                    valid=False,
                    reason=(
                        f"缩容被拒绝：{node_id} 当前可用仅 {room}，"
                        f"容量最多降到 {floor}（降到 {new_capacity} 会超卖）"
                    ),
                )
                result["floor_capacity"] = floor
        elif node_id != PLATFORM_ID:
            # 扩容：从父级支取 Δ，父级当前可用必须出得起
            parent_id = node["parent_id"]
            parent_room = bal.get(parent_id, 0)
            if parent_room < delta:
                result.update(
                    valid=False,
                    reason=(
                        f"扩容被拒绝：父级 {parent_id} 当前可用仅 {parent_room}，"
                        f"不足以向 {node_id} 出资 {delta}"
                    ),
                )
                result["parent_id"] = parent_id
                result["parent_available"] = parent_room

        if result["valid"]:
            # 提交到模拟副本（顺序敏感：同批先到的变更影响后到的校验）
            cap[node_id] = new_capacity
            bal[node_id] = bal.get(node_id, 0) + delta
            if node_id == PLATFORM_ID:
                global_capacity = new_capacity
            else:
                parent_id = node["parent_id"]
                bal[parent_id] = bal.get(parent_id, 0) - delta
            touched.add(node_id)
            if node_id != PLATFORM_ID:
                touched.add(node["parent_id"])
        else:
            all_valid = False
        items.append(result)

    # 逐层余额：每条变更节点的祖先链（平台→该节点），去重保序；
    # 未直接触及的层也展示（before == after），便于对照层级传导
    chain_order: list[str] = []
    for item in changes:
        for nid in _ancestor_chain(p, item["node_id"]):
            if nid not in chain_order:
                chain_order.append(nid)
    layer_balances = [
        {
            "node_id": nid,
            "node_type": p.nodes[nid]["type"],
            "before": p.balance.get(nid, 0),
            "after": bal.get(nid, 0),
            "capacity_before": p.node_capacity.get(nid, 0)
            if nid != PLATFORM_ID
            else p.global_capacity,
            "capacity_after": cap.get(
                nid, p.global_capacity if nid == PLATFORM_ID else 0
            ),
            "touched": nid in touched,
        }
        for nid in chain_order
    ]

    # 受影响租约：出租方或持有方的余额会变化的未结算租约
    affected: list[dict[str, Any]] = []
    for lease in p.live_leases():
        roles = []
        if lease["lessor_id"] in touched:
            roles.append("出租方")
        if lease["holder_id"] in touched:
            roles.append("持有方")
        if not roles:
            continue
        remaining = lease["amount"] - lease["consumed"]
        affected.append(
            {
                "lease_id": lease["id"],
                "lessor_id": lease["lessor_id"],
                "holder_id": lease["holder_id"],
                "status": lease["status"],
                "amount": lease["amount"],
                "consumed": lease["consumed"],
                "remaining": remaining,
                "revision": lease["revision"],
                "affected_role": "/".join(roles),
                "lessor_balance_before": p.balance.get(lease["lessor_id"], 0),
                "lessor_balance_after": bal.get(lease["lessor_id"], 0),
                "holder_balance_before": p.balance.get(lease["holder_id"], 0),
                "holder_balance_after": bal.get(lease["holder_id"], 0),
            }
        )

    return {
        "valid": all_valid,
        "items": items,
        "layer_balances": layer_balances,
        "affected_leases": affected,
        "global_capacity_before": p.global_capacity,
        "global_capacity_after": global_capacity,
    }


def summarize_intervening(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把陈旧期间发生的业务事件归类为人类可读的变更摘要。"""
    counts: dict[str, int] = {}
    for ev in events:
        if ev["type"] in PLAN_EVENT_TYPES:
            continue
        counts[ev["type"]] = counts.get(ev["type"], 0) + 1
    return [
        {
            "event_type": t,
            "label": BUSINESS_EVENT_LABELS.get(t, t),
            "count": n,
        }
        for t, n in sorted(counts.items())
    ]
