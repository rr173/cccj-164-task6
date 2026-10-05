"""灾备侧校验器 —— 所有“能不能信”的判断都集中在这里。

四类校验：

1. **信封结构与分段链**：字段完整、seq 连续、``prev_digest`` 关联前段、
   ``digest`` 重算一致；
2. **主站事件哈希链**：分段内每条事件的 ``event_hash`` 按主站算法重算一致，
   且与前段最后一条事件首尾相接；
3. **投影语义**：事件能折叠、非负余额、全局守恒、层级与租约合法、
   从事件重放的结果与投影一致（复用主站领域代码，保证同一套规则）；
4. **命令结果**：非系统命令必须有回执、指纹与命令体一致、回执结果与
   “事件折叠 + 结果提取器”构造出的期望结果逐项一致。

校验器全部是纯函数/无状态，由管理器编排；任何一项失败都会带上机器可读
原因码（``VERIFY_*``），管理器据此隔离分段或终止演练。
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from quota.domain import (
    PLATFORM_ID,
    RESULT_EXTRACTORS,
    ConflictError,
    Projection,
    ValidationError,
    apply_event,
)
from quota.feed import GENESIS_DIGEST, envelope_digest
from quota.ledger import EventStore, canonical

# 校验原因码 → 对外隔离原因
SEGMENT_INDEX_MISMATCH = "SEGMENT_INDEX_MISMATCH"
SEGMENT_SIZE_MISMATCH = "SEGMENT_SIZE_MISMATCH"
SEQ_NOT_CONTIGUOUS = "SEQ_NOT_CONTIGUOUS"
ENVELOPE_DIGEST_MISMATCH = "ENVELOPE_DIGEST_MISMATCH"
CHAIN_DIGEST_BROKEN = "CHAIN_DIGEST_BROKEN"
EVENT_HASH_BROKEN = "EVENT_HASH_BROKEN"
EVENT_CROSS_CHAIN_BROKEN = "EVENT_CROSS_CHAIN_BROKEN"
EVENT_APPLY_FAILED = "EVENT_APPLY_FAILED"
RECEIPT_MALFORMED = "RECEIPT_MALFORMED"
PROJECTION_AUDIT_FAILED = "PROJECTION_AUDIT_FAILED"
RECEIPT_CHECK_FAILED = "RECEIPT_CHECK_FAILED"


class VerifyError(Exception):
    """校验失败：携带原因码与人类可读细节。"""

    def __init__(self, reason: str, detail: str, extra: dict[str, Any] | None = None):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.extra = extra or {}


# ----------------------------------------------------------------------
# 1) 信封结构与分段链
# ----------------------------------------------------------------------

REQUIRED_FIELDS = (
    "segment_index",
    "segment_size",
    "seq_start",
    "seq_end",
    "prev_digest",
    "digest",
    "events",
    "receipts",
)


def check_envelope(env: Any) -> None:
    """信封必须是包含全部字段的 JSON 对象。"""
    if not isinstance(env, dict):
        raise VerifyError(ENVELOPE_DIGEST_MISMATCH, "分段不是 JSON 对象")
    missing = [f for f in REQUIRED_FIELDS if f not in env]
    if missing:
        raise VerifyError(
            ENVELOPE_DIGEST_MISMATCH, f"分段缺少字段: {', '.join(missing)}"
        )
    if not isinstance(env["events"], list) or not isinstance(env["receipts"], list):
        raise VerifyError(ENVELOPE_DIGEST_MISMATCH, "events/receipts 必须是数组")
    if not all(isinstance(v, int) for v in (
        env["segment_index"], env["segment_size"], env["seq_start"], env["seq_end"]
    )):
        raise VerifyError(ENVELOPE_DIGEST_MISMATCH, "分段序号/seq 必须是整数")
    if not isinstance(env["prev_digest"], str) or not isinstance(env["digest"], str):
        raise VerifyError(ENVELOPE_DIGEST_MISMATCH, "摘要必须是十六进制字符串")


def check_index_and_size(env: dict[str, Any], expected_index: int, segment_size: int) -> None:
    if env["segment_index"] != expected_index:
        raise VerifyError(
            SEGMENT_INDEX_MISMATCH,
            f"分段序号错序：期望 {expected_index}，收到 {env['segment_index']}",
            {"expected": expected_index, "got": env["segment_index"]},
        )
    if env["segment_size"] != segment_size:
        raise VerifyError(
            SEGMENT_SIZE_MISMATCH,
            f"分段大小不一致：期望 {segment_size}，收到 {env['segment_size']}",
            {"expected": segment_size, "got": env["segment_size"]},
        )
    if len(env["events"]) != segment_size:
        raise VerifyError(
            SEGMENT_SIZE_MISMATCH,
            f"未密封的分段：含 {len(env['events'])} 条事件，期望 {segment_size} 条",
        )


def check_seq_contiguous(env: dict[str, Any], prev_seq_end: int) -> None:
    seqs = [e.get("seq") for e in env["events"]]
    if seqs != list(range(prev_seq_end + 1, prev_seq_end + 1 + len(seqs))):
        raise VerifyError(
            SEQ_NOT_CONTIGUOUS,
            f"分段内 seq 不连续：应为 {prev_seq_end + 1}..{prev_seq_end + len(seqs)}，"
            f"实际 {seqs[:3]}…",
            {"expected_start": prev_seq_end + 1, "actual_seqs": seqs[:10]},
        )
    if env["seq_start"] != seqs[0] or env["seq_end"] != seqs[-1]:
        raise VerifyError(
            SEQ_NOT_CONTIGUOUS,
            f"分段信封 seq_start/seq_end({env['seq_start']}..{env['seq_end']}) "
            f"与事件 seq({seqs[0]}..{seqs[-1]}) 不符",
        )


def check_chain_link(env: dict[str, Any], expected_prev_digest: str) -> None:
    if env["prev_digest"] != expected_prev_digest:
        raise VerifyError(
            CHAIN_DIGEST_BROKEN,
            f"分段 {env['segment_index']} 摘要断链：prev_digest 不匹配",
            {
                "expected": expected_prev_digest,
                "got": env["prev_digest"],
            },
        )
    actual = envelope_digest(env)
    if actual != env["digest"]:
        raise VerifyError(
            ENVELOPE_DIGEST_MISMATCH,
            f"分段 {env['segment_index']} 信封摘要重算不一致（载荷被篡改）",
            {"expected": actual, "got": env["digest"]},
        )


# ----------------------------------------------------------------------
# 2) 主站事件哈希链
# ----------------------------------------------------------------------

def check_event_hashes(env: dict[str, Any], prev_event_hash: str) -> str:
    """重算分段内事件哈希，返回本分段最后一条事件的哈希（供下段衔接）。"""
    prev = prev_event_hash
    for e in env["events"]:
        try:
            expect = EventStore._hash_event(
                prev,
                e["type"],
                e["payload"],
                e["command_id"],
                e["fingerprint"],
            )
        except KeyError as exc:
            raise VerifyError(
                EVENT_HASH_BROKEN,
                f"分段 {env['segment_index']} seq={e.get('seq')} 事件缺少字段: {exc}",
            )
        if e.get("prev_hash") != prev:
            raise VerifyError(
                EVENT_CROSS_CHAIN_BROKEN,
                f"seq={e['seq']} 的 prev_hash 与前段尾事件不衔接",
                {"seq": e["seq"], "expected": prev, "got": e.get("prev_hash")},
            )
        if e.get("event_hash") != expect:
            raise VerifyError(
                EVENT_HASH_BROKEN,
                f"seq={e['seq']} 事件哈希重算不一致（载荷/指纹被篡改）",
                {"seq": e["seq"], "expected": expect, "got": e.get("event_hash")},
            )
        prev = expect
    return prev


# ----------------------------------------------------------------------
# 3) 投影折叠 + 守恒审计
# ----------------------------------------------------------------------

def _domain_event(e: dict[str, Any]) -> dict[str, Any]:
    """馈源事件是 {seq,type,payload,...} 嵌套结构；主站 apply_event 消费
    的是账本读出的扁平事件（载荷字段展开到顶层）。折叠前在此转换。"""
    return {"seq": e["seq"], "type": e["type"], **e["payload"]}


def fold_segment(projection: Projection, env: dict[str, Any]) -> None:
    """把分段事件折叠进给定投影；任何领域异常都视为分段非法。"""
    for e in env["events"]:
        try:
            apply_event(projection, _domain_event(e))
        except (ValidationError, ConflictError, KeyError, TypeError) as exc:
            raise VerifyError(
                EVENT_APPLY_FAILED,
                f"seq={e.get('seq')} 事件无法折叠进投影: {exc}",
            )


def _projection_checks(p: Projection) -> list[dict[str, Any]]:
    """与主站同源的守恒/结构检查（不触碰 SQLite，只针对内存投影）。"""
    checks: list[dict[str, Any]] = []

    negatives = sorted((nid, b) for nid, b in p.balance.items() if b < 0)
    checks.append({
        "name": "non_negative_balances",
        "ok": not negatives,
        "detail": "所有节点可用额度 >= 0（无超卖）" if not negatives
        else f"出现负余额: {negatives}",
    })

    total_avail = sum(p.balance.values())
    cons_ok = total_avail + p.total_consumed == p.global_capacity
    checks.append({
        "name": "global_conservation",
        "ok": cons_ok,
        "detail": f"Σ可用({total_avail}) + 总消费({p.total_consumed}) "
                  f"= 全局容量({p.global_capacity})",
    })

    problems: list[str] = []
    for nid, node in p.nodes.items():
        if nid == PLATFORM_ID:
            continue
        if node["parent_id"] not in p.nodes:
            problems.append(f"{nid} 的父节点 {node['parent_id']} 缺失")
    for lid, lease in p.leases.items():
        if lease["lessor_id"] not in p.nodes or lease["holder_id"] not in p.nodes:
            problems.append(f"租约 {lid} 引用了不存在的节点")
        elif p.nodes[lease["holder_id"]]["parent_id"] != lease["lessor_id"]:
            problems.append(f"租约 {lid} 不是直接父子边")
    checks.append({
        "name": "hierarchy_integrity",
        "ok": not problems,
        "detail": "所有租约均为合法父子边，节点树完整" if not problems
        else "; ".join(problems),
    })

    lease_problems: list[str] = []
    for lid, lease in p.leases.items():
        if not (0 <= lease["own_consumed"] <= lease["amount"]):
            lease_problems.append(f"{lid} 直接消费量越界")
        if not (0 <= lease["consumed"] <= lease["amount"]):
            lease_problems.append(f"{lid} 消费分摊越界")
        status = lease["status"]
        if status == "closed":
            if lease["reason"] not in ("released", "revoked", "expired", "offline"):
                lease_problems.append(f"{lid} 关闭原因非法")
        elif status not in ("active", "pending_expired"):
            lease_problems.append(f"{lid} 状态非法: {status}")
        elif lease["reason"] is not None:
            lease_problems.append(f"{lid} 活动租约不应有关闭原因")
    checks.append({
        "name": "lease_bookkeeping",
        "ok": not lease_problems,
        "detail": "租约余量/消费/关闭状态均合法" if not lease_problems
        else "; ".join(lease_problems),
    })
    return checks


def audit_projection(p: Projection) -> dict[str, Any]:
    """针对内存投影执行守恒/结构审计（非负、守恒、层级、租约）。"""
    checks = _projection_checks(p)
    for c in checks:
        if not c["ok"]:
            raise VerifyError(
                PROJECTION_AUDIT_FAILED,
                f"投影守恒检查 {c['name']} 失败: {c['detail']}",
                {"failed_check": c},
            )
    return {
        "global_capacity": p.global_capacity,
        "total_available": sum(p.balance.values()),
        "total_consumed": p.total_consumed,
        "checks": checks,
    }


# ----------------------------------------------------------------------
# 4) 命令结果（回执）校验
# ----------------------------------------------------------------------

# 普通命令：command_id 可能是自动派生的，但指纹可解析回 {kind, body}
# 草案命令：指纹可解析回 {kind, body}（创建/预览）或 {kind, plan_id}
PLAN_KINDS = {
    "plan_create": "CapacityPlanCreated",
    "plan_publish": "CapacityPlanPublished",
    "plan_discard": "CapacityPlanDiscarded",
}


def _parse_fingerprint(fingerprint: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(fingerprint)
        return obj if isinstance(obj, dict) and "kind" in obj else None
    except (json.JSONDecodeError, TypeError):
        return None


def check_receipts_in_segment(env: dict[str, Any]) -> None:
    """增量校验：分段内回执必须指向本段事件，且指纹可解析。"""
    seg_seqs = {e["seq"] for e in env["events"]}
    seg_commands = {e["command_id"]: e for e in env["events"]}
    for r in env["receipts"]:
        for f in ("command_id", "fingerprint", "result", "seq"):
            if f not in r:
                raise VerifyError(
                    RECEIPT_MALFORMED,
                    f"分段 {env['segment_index']} 回执缺少字段 {f}",
                )
        if r["seq"] not in seg_seqs:
            raise VerifyError(
                RECEIPT_MALFORMED,
                f"回执 {r['command_id']} 指向 seq={r['seq']}，不在分段"
                f" {env['segment_index']} 范围内",
            )
        if r["command_id"] not in seg_commands:
            raise VerifyError(
                RECEIPT_MALFORMED,
                f"回执 {r['command_id']} 的命令在分段内找不到事件",
            )
        if _parse_fingerprint(r["fingerprint"]) is None:
            raise VerifyError(
                RECEIPT_MALFORMED,
                f"回执 {r['command_id']} 指纹不可解析",
            )


def expected_command_result(kind: str, body: dict[str, Any], events: list[dict[str, Any]]) \
        -> dict[str, Any] | None:
    """用与主站完全相同的逻辑，从事件构造期望命令结果。

    返回 None 表示该指纹声明的 kind 无法在灾备侧重放（不应发生）。
    """
    from quota.domain import HANDLERS

    if kind in HANDLERS:
        # 主站 QuotaService._build_result 的等价实现（events 已扁平）
        items = []
        for e in events:
            extractor = RESULT_EXTRACTORS.get(e["type"])
            items.append(
                {"event_type": e["type"],
                 **(extractor(e) if extractor else {})}
            )
        result = {"command": kind, "event_count": len(events), "events": items}
        if len(items) == 1:
            result.update(items[0])
        return result

    if kind == "plan_create":
        # 创建结果不依赖模拟，只核对结构字段
        ev = next((e for e in events if e["type"] == "CapacityPlanCreated"), None)
        if ev is None:
            return None
        return {
            "command": "plan_create",
            "plan_id": ev["plan_id"],
            "status": "draft",
            "base_seq": ev["base_seq"],
            "changes": ev["changes"],
            "event_count": 1,
        }
    if kind == "plan_publish":
        ev = next((e for e in events if e["type"] == "CapacityPlanPublished"), None)
        if ev is None:
            return None
        return {
            "command": "plan_publish",
            "plan_id": ev["plan_id"],
            "status": "published",
            "published_at": ev["published_at"],
            "applied_count": ev["event_count"],
            "event_count": len(events),
        }
    if kind == "plan_discard":
        ev = next((e for e in events if e["type"] == "CapacityPlanDiscarded"), None)
        if ev is None:
            return None
        return {
            "command": "plan_discard",
            "plan_id": ev["plan_id"],
            "status": "discarded",
            "event_count": 1,
        }
    return None


def full_receipt_check(all_segments: list[dict[str, Any]]) -> dict[str, Any]:
    """全量校验（演练/审计用）：

    * 每个非系统 command_id 的事件组都有且仅有一张回执；
    * 回执指纹与事件组吻合；
    * 回执结果与“从事件重建的期望结果”一致（忽略主站专有的预览快照
      等大字段：那些不参与事件链，灾备只锚定事件可推导部分）。
    """
    # 按 command_id 聚合事件（保持事件顺序）；折叠/结果推导统一用扁平事件
    groups: dict[str, list[dict[str, Any]]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    for env in all_segments:
        for e in env["events"]:
            groups.setdefault(e["command_id"], []).append(_domain_event(e))
        for r in env["receipts"]:
            if r["command_id"] in receipts:
                raise VerifyError(
                    RECEIPT_CHECK_FAILED,
                    f"命令 {r['command_id']} 有多张回执",
                )
            receipts[r["command_id"]] = r

    checked = 0
    problems: list[str] = []
    for cid, events in groups.items():
        if cid.startswith("sys:"):
            if cid in receipts:
                problems.append(f"系统命令 {cid} 不应有用户回执")
            continue
        r = receipts.get(cid)
        if r is None:
            problems.append(f"命令 {cid} 缺少回执")
            continue
        parsed = _parse_fingerprint(r["fingerprint"])
        if parsed is None:
            problems.append(f"命令 {cid} 指纹不可解析")
            continue

        # 指纹中的 kind 必须与事件类型族吻合
        kind = parsed["kind"]
        expect = expected_command_result(kind, parsed.get("body", {}), events)
        if expect is None:
            problems.append(f"命令 {cid} 无法从事件重建结果（kind={kind}）")
            continue

        # 结果核心字段必须逐项一致（回执可能额外带预览快照等，逐键核对）
        for key, value in expect.items():
            if key not in r["result"]:
                problems.append(f"命令 {cid} 回执结果缺少字段 {key}")
            elif r["result"][key] != value:
                problems.append(
                    f"命令 {cid} 回执结果字段 {key} 与事件推导不一致"
                )
        # 事件数量必须吻合
        if r["result"].get("event_count") not in (None, len(events)):
            problems.append(f"命令 {cid} 回执 event_count 与事件数不一致")
        checked += 1

    orphan = sorted(set(receipts) - set(groups))
    for cid in orphan:
        problems.append(f"回执 {cid} 在事件链中找不到对应事件")

    return {
        "name": "command_receipts",
        "ok": not problems,
        "checked_commands": checked,
        "missing_or_bad": problems[:20],
        "detail": f"{checked} 条命令回执与事件链推导结果一致"
        if not problems else "; ".join(problems[:5]),
    }


# ----------------------------------------------------------------------
# 全量重放（演练 / 启动审计用）
# ----------------------------------------------------------------------

def replay_all(all_segments: list[dict[str, Any]]) -> Projection:
    """从空投影折叠全部已确认分段。"""
    p = Projection()
    prev_digest = GENESIS_DIGEST
    prev_event_hash = "0" * 64
    prev_seq_end = 0
    for env in all_segments:
        check_chain_link(env, prev_digest)
        check_seq_contiguous(env, prev_seq_end)
        prev_event_hash = check_event_hashes(env, prev_event_hash)
        fold_segment(p, env)
        check_receipts_in_segment(env)
        prev_digest = env["digest"]
        prev_seq_end = env["seq_end"]
    return p


def state_digest(p: Projection) -> str:
    """投影规范化摘要：演练锁定截面与重启后重放必须得到同一个值。"""
    return hashlib.sha256(
        canonical(p.digest()).encode("utf-8")
    ).hexdigest()


def clone_projection(p: Projection) -> Projection:
    """深拷贝投影（分段先在副本上试折叠，通过后才原子发布）。"""
    return copy.deepcopy(p)
