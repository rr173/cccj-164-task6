"""主站只读复制馈源 —— 面向灾备复制器的事件分段接口。

把事件账本按固定大小切成**密封分段（sealed segment）**对外提供：

  * 分段内事件携带账本自身的 SHA-256 哈希链（event_hash/prev_hash）；
  * 每个分段信封带 ``prev_digest``（前段摘要）与自身 ``digest``，
    分段链同样不可篡改；
  * 命令幂等回执随其最后一条事件所在分段一起下发，灾备侧可独立验证
    “事件 → 命令结果”的一致性。

只读取 EventStore，绝不修改主站任何业务状态。

另提供仅在显式开启 ``--allow-feed-chaos`` 时可用的故障注入面，
用于演练缺失/篡改/分叉等异常（见 :meth:`Feed.set_chaos`）。
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from typing import Any

from .ledger import EventStore, canonical

GENESIS_DIGEST = "0" * 64

# 故障注入模式 → 灾备侧应观察到的隔离原因（仅演练用）
CHAOS_MODES = {
    # 该分段暂时 404，像在传输路径上丢失
    "gap",
    # 篡改事件载荷但保留旧哈希：事件哈希重算不符
    "corrupt_event",
    # 仅擦掉信封摘要：信封摘要不符
    "corrupt_digest",
    # 同一分段序号下发一份内部自洽但正文不同的分叉副本
    "alt_body",
}


def envelope_digest(env_without_digest: dict[str, Any]) -> str:
    """分段摘要：对规范化后的信封正文（不含 digest 字段）取 SHA-256。"""
    body = {
        "segment_index": env_without_digest["segment_index"],
        "segment_size": env_without_digest["segment_size"],
        "seq_start": env_without_digest["seq_start"],
        "seq_end": env_without_digest["seq_end"],
        "prev_digest": env_without_digest["prev_digest"],
        "events": [
            {
                "seq": e["seq"],
                "type": e["type"],
                "payload": e["payload"],
                "command_id": e["command_id"],
                "fingerprint": e["fingerprint"],
                "event_hash": e["event_hash"],
                "prev_hash": e["prev_hash"],
            }
            for e in env_without_digest["events"]
        ],
        "receipts": [
            {
                "command_id": r["command_id"],
                "fingerprint": r["fingerprint"],
                "result": r["result"],
                "seq": r["seq"],
            }
            for r in env_without_digest["receipts"]
        ],
    }
    return hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()


def _recompute_event_hashes(events: list[dict[str, Any]]) -> None:
    """对分段内事件按账本同样的算法重算哈希链（alt_body 分叉用）。"""
    prev = events[0]["prev_hash"]
    for e in events:
        e["prev_hash"] = prev
        e["event_hash"] = EventStore._hash_event(
            prev,
            e["type"],
            e["payload"],
            e["command_id"],
            e["fingerprint"],
        )
        prev = e["event_hash"]


def _bump_first_number(payload: dict[str, Any]) -> bool:
    """把载荷里第一个整数字段 +1（构造分叉正文），找不到返回 False。"""
    for key, value in payload.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            payload[key] = value + 1
            return True
    return False


class Feed:
    """从事件账本构造密封分段（只读，线程安全）。"""

    def __init__(self, store: EventStore, segment_size: int = 10) -> None:
        if segment_size < 1:
            raise ValueError("segment_size 必须 >= 1")
        self.store = store
        self.segment_size = segment_size
        # {"mode": str, "segment_index": int}
        self.chaos: dict[str, Any] | None = None
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 故障注入（仅测试 / 演练，需 --allow-feed-chaos 才挂路由）
    # ------------------------------------------------------------------

    def set_chaos(self, mode: str | None, segment_index: int | None = None) -> dict[str, Any]:
        with self._lock:
            if mode is not None:
                if mode not in CHAOS_MODES:
                    raise ValueError(f"未知 chaos 模式: {mode}")
                self.chaos = {"mode": mode, "segment_index": segment_index}
            else:
                self.chaos = None
            return copy.deepcopy(self.chaos)

    # ------------------------------------------------------------------
    # 分段构造
    # ------------------------------------------------------------------

    def _raw_segments_locked(self) -> list[dict[str, Any]]:
        """从账本读取全部事件并切成密封分段（最后一个不满的分段不对外）。"""
        rows = self.store.conn.execute(
            "SELECT seq, event_type, payload, command_id, fingerprint,"
            " event_hash, prev_hash FROM events ORDER BY seq"
        ).fetchall()
        commands = self.store.load_commands()
        # 回执按其最后一条事件序号归入对应分段
        receipts_by_chunk: dict[int, list[dict[str, Any]]] = {}
        for cid, rec in commands.items():
            chunk = (rec["seq"] - 1) // self.segment_size
            receipts_by_chunk.setdefault(chunk, []).append(
                {
                    "command_id": cid,
                    "fingerprint": rec["fingerprint"],
                    "result": rec["result"],
                    "seq": rec["seq"],
                }
            )

        chunks: dict[int, list[dict[str, Any]]] = {}
        for r in rows:
            chunk = (r["seq"] - 1) // self.segment_size
            chunks.setdefault(chunk, []).append(
                {
                    "seq": r["seq"],
                    "type": r["event_type"],
                    "payload": json.loads(r["payload"]),
                    "command_id": r["command_id"],
                    "fingerprint": r["fingerprint"],
                    "event_hash": r["event_hash"],
                    "prev_hash": r["prev_hash"],
                }
            )

        segments: list[dict[str, Any]] = []
        prev_digest = GENESIS_DIGEST
        for chunk_index in sorted(chunks):
            events = chunks[chunk_index]
            if len(events) < self.segment_size:
                break  # 尾部未密封：复制器只能拉取完整确认的截面
            index = chunk_index + 1  # 分段序号从 1 开始
            receipts = sorted(receipts_by_chunk.get(chunk_index, []), key=lambda x: x["command_id"])
            env = {
                "segment_index": index,
                "segment_size": self.segment_size,
                "seq_start": events[0]["seq"],
                "seq_end": events[-1]["seq"],
                "prev_digest": prev_digest,
                "events": events,
                "receipts": receipts,
            }
            env["digest"] = envelope_digest(env)
            segments.append(env)
            prev_digest = env["digest"]
        return segments

    def status(self) -> dict[str, Any]:
        with self.store.lock, self._lock:
            segments = self._raw_segments_locked()
            row = self.store.conn.execute("SELECT MAX(seq) AS m FROM events").fetchone()
            tip_seq = row["m"] or 0
            return {
                "tip_seq": tip_seq,
                "segment_size": self.segment_size,
                "sealed_index": len(segments),
                "segments": [
                    {
                        "segment_index": s["segment_index"],
                        "seq_start": s["seq_start"],
                        "seq_end": s["seq_end"],
                        "digest": s["digest"],
                    }
                    for s in segments
                ],
            }

    def get_segment(self, index: int) -> dict[str, Any] | None:
        """返回指定分段（可能经 chaos 变换）；None 表示缺失（404）。"""
        with self.store.lock, self._lock:
            segments = self._raw_segments_locked()
            if not (1 <= index <= len(segments)):
                return None
            env = segments[index - 1]
            chaos = self.chaos
            if chaos and chaos.get("segment_index") == index:
                env = self._apply_chaos(env, chaos["mode"])
                if env is None:
                    return None
            return env

    @staticmethod
    def _apply_chaos(env: dict[str, Any], mode: str) -> dict[str, Any] | None:
        if mode == "gap":
            return None
        env = copy.deepcopy(env)
        if mode == "corrupt_event":
            # 改载荷但不动 event_hash：灾备侧重算事件哈希必然不符
            if env["events"] and not _bump_first_number(env["events"][0]["payload"]):
                env["events"][0]["payload"]["_chaos_marker"] = True
            return env
        if mode == "corrupt_digest":
            env["digest"] = "f" * 64
            return env
        if mode == "alt_body":
            # 构造一份内部自洽（事件哈希链/信封摘要全部重算）但正文不同的副本
            if env["events"] and not _bump_first_number(env["events"][0]["payload"]):
                env["events"][0]["payload"]["_chaos_marker"] = True
            _recompute_event_hashes(env["events"])
            env["digest"] = envelope_digest(env)
            return env
        return env
