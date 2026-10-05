"""配额租约服务 —— 事件账本层。

只追加（append-only）的事件表 + 幂等命令表 + SHA-256 哈希链。
任何业务状态都不从外部写入，服务启动时重放 events 表重建。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    command_id   TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,
    event_hash   TEXT NOT NULL,
    prev_hash    TEXT NOT NULL,
    recorded_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_command ON events(command_id);

CREATE TABLE IF NOT EXISTS commands (
    command_id   TEXT PRIMARY KEY,
    fingerprint  TEXT NOT NULL,
    result       TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    recorded_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_plans (
    plan_id            TEXT PRIMARY KEY,
    status             TEXT NOT NULL,
    changes            TEXT NOT NULL,
    base_seq           INTEGER NOT NULL,
    last_preview_seq   INTEGER,
    last_preview       TEXT,
    publish_result     TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);
"""

GENESIS_HASH = "0" * 64


def canonical(obj: Any) -> str:
    """确定性 JSON：键排序、无空白、原样保留非 ASCII。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class CommandConflictError(Exception):
    """同一 command_id 携带了不同的请求体 —— 拒绝。"""


class EventStore:
    """串行化写入的 SQLite 事件存储。

    所有写操作在同一把进程内锁下完成“校验指纹 → 追加事件 → 提交投影”，
    配合 WAL 保证单进程内跨线程的线性一致（linearizable）。
    """

    def __init__(self, path: str):
        self.path = path
        first_time = not os.path.exists(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._first_time = first_time

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------
    # 读
    # ------------------------------------------------------------------

    def load_events(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT seq, event_type, payload FROM events ORDER BY seq"
        ).fetchall()
        return [
            {"seq": r["seq"], "type": r["event_type"], **json.loads(r["payload"])}
            for r in rows
        ]

    def load_commands(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT command_id, fingerprint, result, seq FROM commands"
        ).fetchall()
        return {
            r["command_id"]: {
                "fingerprint": r["fingerprint"],
                "result": json.loads(r["result"]),
                "seq": r["seq"],
            }
            for r in rows
        }

    def get_command(self, command_id: str) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT fingerprint, result, seq FROM commands WHERE command_id=?",
            (command_id,),
        ).fetchone()
        if r is None:
            return None
        return {
            "fingerprint": r["fingerprint"],
            "result": json.loads(r["result"]),
            "seq": r["seq"],
        }

    def load_hash_chain(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT seq, event_type, payload, command_id, fingerprint,"
            " event_hash, prev_hash FROM events ORDER BY seq"
        ).fetchall()
        return [dict(r) for r in rows]

    def max_business_seq(self, plan_event_types: set[str]) -> int:
        """最新一条*业务事件*的序号（草案生命周期事件不推进业务水位）。

        草案陈旧判定以它为准：创建/预览之后任何授约、消费、续约、结算、
        容量调整或系统到期结算都会推进该水位，而另一份草案的编辑不会。
        """
        if not plan_event_types:
            row = self.conn.execute("SELECT MAX(seq) AS m FROM events").fetchone()
            return row["m"] or 0
        placeholders = ",".join("?" for _ in plan_event_types)
        row = self.conn.execute(
            f"SELECT MAX(seq) AS m FROM events WHERE event_type NOT IN ({placeholders})",
            tuple(plan_event_types),
        ).fetchone()
        return row["m"] or 0

    def events_after(self, seq: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT seq, event_type, payload FROM events WHERE seq > ? ORDER BY seq",
            (seq,),
        ).fetchall()
        return [
            {"seq": r["seq"], "type": r["event_type"], **json.loads(r["payload"])}
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 容量调整草案行（与事件在同一 SQLite 事务内写入）
    # ------------------------------------------------------------------

    def get_plan_row(self, plan_id: str) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT plan_id, status, changes, base_seq, last_preview_seq,"
            " last_preview, publish_result, created_at, updated_at"
            " FROM capacity_plans WHERE plan_id=?",
            (plan_id,),
        ).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["changes"] = json.loads(d["changes"])
        d["last_preview"] = json.loads(d["last_preview"]) if d["last_preview"] else None
        d["publish_result"] = (
            json.loads(d["publish_result"]) if d["publish_result"] else None
        )
        return d

    def list_plan_rows(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT plan_id, status, changes, base_seq, last_preview_seq,"
            " last_preview, publish_result, created_at, updated_at"
            " FROM capacity_plans ORDER BY rowid"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["changes"] = json.loads(d["changes"])
            d["last_preview"] = (
                json.loads(d["last_preview"]) if d["last_preview"] else None
            )
            d["publish_result"] = (
                json.loads(d["publish_result"]) if d["publish_result"] else None
            )
            out.append(d)
        return out

    # ------------------------------------------------------------------
    # 写（调用方必须持有 self._lock 且已完成全部业务校验）
    # ------------------------------------------------------------------

    def append_batch(
        self,
        events: list[dict[str, Any]],
        command_id: str,
        fingerprint: str,
        result: dict[str, Any] | None = None,
        system: bool = False,
        plan_change: dict[str, Any] | None = None,
    ) -> list[int]:
        """原子地追加一批事件，可选登记命令回执与草案行，返回事件序号列表。

        一次命令可以产生多个事件（如客户端离线关闭多条租约、到期级联），
        因此去重约束在 commands.command_id 上（每个命令一行），而不是事件上。
        后台 sweep 产生的系统事件以 ``sys:`` 前缀的 command_id 标记，
        不登记 commands 表，只保留哈希链可审计性。

        ``plan_change`` 为 ``{"plan_id", "status", "changes"(list, 可省略),
        "base_seq"(可省略), "last_preview_seq"(可省略), "last_preview"(dict|None),
        "publish_result"(dict|None, 可省略), "mode": "insert"|"update"}`` 时，
        草案行与事件在同一事务提交：要么全部生效，要么全部回滚。
        """
        if system:
            result = result or {}
            command_id = (
                command_id
                if command_id.startswith("sys:")
                else f"sys:{command_id}"
            )
        seqs: list[int] = []
        with self.conn:  # 事务
            for event in events:
                prev = self.conn.execute(
                    "SELECT event_hash FROM events ORDER BY seq DESC LIMIT 1"
                ).fetchone()
                prev_hash = prev["event_hash"] if prev else GENESIS_HASH
                event_hash = self._hash_event(
                    prev_hash,
                    event["type"],
                    event["payload"],
                    command_id,
                    fingerprint,
                )
                cur = self.conn.execute(
                    "INSERT INTO events (event_type, payload, command_id, fingerprint,"
                    " event_hash, prev_hash, recorded_at)"
                    " VALUES (?,?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                    (
                        event["type"],
                        canonical(event["payload"]),
                        command_id,
                        fingerprint,
                        event_hash,
                        prev_hash,
                    ),
                )
                seqs.append(cur.lastrowid)
            if not system:
                self.conn.execute(
                    "INSERT INTO commands (command_id, fingerprint, result, seq, recorded_at)"
                    " VALUES (?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                    (command_id, fingerprint, canonical(result), seqs[-1]),
                )
            if plan_change is not None:
                self._upsert_plan_locked(plan_change)
        return seqs

    def _upsert_plan_locked(self, ch: dict[str, Any]) -> None:
        now_sql = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        if ch.get("mode") == "insert":
            self.conn.execute(
                "INSERT INTO capacity_plans (plan_id, status, changes, base_seq,"
                " last_preview_seq, last_preview, publish_result, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?," + now_sql + "," + now_sql + ")",
                (
                    ch["plan_id"],
                    ch["status"],
                    canonical(ch["changes"]),
                    ch["base_seq"],
                    ch.get("last_preview_seq"),
                    canonical(ch["last_preview"]) if ch.get("last_preview") is not None else None,
                    canonical(ch["publish_result"]) if ch.get("publish_result") is not None else None,
                ),
            )
            return
        sets = ["status = ?", "updated_at = " + now_sql]
        params: list[Any] = [ch["status"]]
        if "last_preview_seq" in ch:
            sets.append("last_preview_seq = ?")
            params.append(ch["last_preview_seq"])
        if "last_preview" in ch:
            sets.append("last_preview = ?")
            params.append(
                canonical(ch["last_preview"]) if ch["last_preview"] is not None else None
            )
        if "publish_result" in ch:
            sets.append("publish_result = ?")
            params.append(
                canonical(ch["publish_result"]) if ch["publish_result"] is not None else None
            )
        params.append(ch["plan_id"])
        self.conn.execute(
            f"UPDATE capacity_plans SET {', '.join(sets)} WHERE plan_id = ?", params
        )

    @staticmethod
    def _hash_event(
        prev_hash: str,
        event_type: str,
        payload: dict[str, Any],
        command_id: str,
        fingerprint: str,
    ) -> str:
        body = canonical(
            {
                "prev_hash": prev_hash,
                "event_type": event_type,
                "payload": payload,
                "command_id": command_id,
                "fingerprint": fingerprint,
            }
        )
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def close(self) -> None:
        self.conn.close()
