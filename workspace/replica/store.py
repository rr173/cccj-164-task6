"""灾备复制器持久化层（SQLite，仅标准库）。

存储内容：

========================== ==============================================
meta                       确认边界、分段大小、进程围栏纪元等单行状态
segments                   已确认分段原文（内容寻址：序号 + 摘要唯一）
quarantined                被拒绝/隔离的分段及原因（不污染确认截面）
drills                     受控切换演练及带任期地址（含锁定截面冻结）
audit_log                  只追加审计轨迹（拉取、隔离、演练、任期失效）
========================== ==============================================

确认边界（meta.confirmed_index）与分段写入在**同一事务**提交：
崩溃重启后要么整个分段已确认，要么完全不存在，绝无半截截面。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any

from quota.ledger import canonical

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    confirmed_index  INTEGER NOT NULL DEFAULT 0,
    confirmed_seq    INTEGER NOT NULL DEFAULT 0,
    last_digest      TEXT NOT NULL,
    last_event_hash  TEXT NOT NULL,
    segment_size     INTEGER NOT NULL,
    epoch            INTEGER NOT NULL DEFAULT 1,
    paused           INTEGER NOT NULL DEFAULT 0,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    segment_index  INTEGER PRIMARY KEY,
    seq_start      INTEGER NOT NULL,
    seq_end        INTEGER NOT NULL,
    prev_digest    TEXT NOT NULL,
    digest         TEXT NOT NULL UNIQUE,
    envelope_json  TEXT NOT NULL,
    epoch          INTEGER,
    applied_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantined (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_index    INTEGER,
    reason           TEXT NOT NULL,
    detail           TEXT NOT NULL,
    source           TEXT NOT NULL,
    envelope_digest  TEXT,
    epoch            INTEGER,
    detected_at      REAL NOT NULL,
    resolved_at      REAL,
    resolution       TEXT
);

CREATE TABLE IF NOT EXISTS drills (
    drill_id         TEXT PRIMARY KEY,
    state            TEXT NOT NULL,
    locked_index     INTEGER,
    locked_digest    TEXT,
    locked_event_hash TEXT,
    term_token       TEXT,
    term_expires_at  REAL,
    max_lag_segments INTEGER NOT NULL,
    report_json      TEXT NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    finished_at      REAL,
    fail_reason      TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    kind        TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""

GENESIS_HASH = "0" * 64


def new_token() -> str:
    return "drill-" + uuid.uuid4().hex


class ReplicaStore:
    def __init__(self, path: str, segment_size: int):
        self.path = path
        first_time = not os.path.exists(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        # 旧库迁移：分段表补“确认该批的接管纪元”列（历史行留 NULL）
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(segments)")}
        if "epoch" not in cols:
            self.conn.execute("ALTER TABLE segments ADD COLUMN epoch INTEGER")
        self.conn.commit()
        if first_time:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO meta (id, confirmed_index, confirmed_seq,"
                    " last_digest, last_event_hash, segment_size, epoch, paused, updated_at)"
                    " VALUES (1, 0, 0, ?, ?, ?, 1, 0, ?)",
                    (GENESIS_HASH, GENESIS_HASH, segment_size, time.time()),
                )

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------
    # meta
    # ------------------------------------------------------------------

    def meta(self) -> dict[str, Any]:
        r = self.conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        return dict(r)

    def get_epoch(self) -> int:
        return int(self.meta()["epoch"])

    def bump_epoch(self) -> int:
        """新进程接管：纪元 +1。旧纪元进程的迟到写入将被拒绝。"""
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE meta SET epoch = epoch + 1, updated_at = ? WHERE id = 1",
                (time.time(),),
            )
        return self.get_epoch()

    def set_paused(self, paused: bool) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE meta SET paused = ?, updated_at = ? WHERE id = 1",
                (1 if paused else 0, time.time()),
            )

    # ------------------------------------------------------------------
    # segments
    # ------------------------------------------------------------------

    # 所有权围栏提交结果
    COMMIT_CONFIRMED = "confirmed"
    COMMIT_DUPLICATE = "duplicate"
    COMMIT_BODY_CONFLICT = "body_conflict"
    COMMIT_ADVANCED = "advanced"
    COMMIT_OWNERSHIP_LOST = "ownership_lost"

    def commit_segment_if_owner(
        self, env: dict[str, Any], last_event_hash: str, owner_epoch: int,
    ) -> dict[str, Any]:
        """写入所有权围栏下的分段确认（单事务原子 CAS）。

        滚动升级期间可能有两个进程共用同一 DB：提交时在**同一事务**内核对
        ``meta.epoch`` 仍是调用方持有的纪元，且边界恰好停在前一段。只有仍是
        当前纪元主人的进程才能落盘；旧纪元进程的在途提交（哪怕校验全做完了）
        被稳定拒绝，绝不让“漏掉别人刚提交的中间批次”的内存结果污染磁盘。

        返回结果类别：

        * ``confirmed``      —— 本事务成功写入分段并推进边界；
        * ``duplicate``      —— 该序号已由当前纪元确认且正文一致（幂等原结果）；
        * ``body_conflict``  —— 该序号已被确认但正文不同（分叉）；
        * ``advanced``       —— 权威水位已越过本序号（缺口被别人补齐），
                                同时带回重算后的当前边界；
        * ``ownership_lost`` —— 纪元已被接替进程提升，调用方必须稳定退出。
        """
        now = time.time()
        index = env["segment_index"]
        digest = env["digest"]
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT epoch, confirmed_index, confirmed_seq, last_digest,"
                    " last_event_hash FROM meta WHERE id = 1"
                ).fetchone()
                current_epoch = int(row["epoch"])
                confirmed_index = int(row["confirmed_index"])
                if current_epoch != owner_epoch:
                    self.conn.rollback()
                    return {
                        "outcome": self.COMMIT_OWNERSHIP_LOST,
                        "segment_index": index,
                        "owner_epoch": owner_epoch,
                        "current_epoch": current_epoch,
                        "confirmed_index": confirmed_index,
                    }
                if index <= confirmed_index:
                    existing = self.conn.execute(
                        "SELECT digest FROM segments WHERE segment_index = ?",
                        (index,),
                    ).fetchone()
                    same = existing is not None and existing["digest"] == digest
                    self.conn.rollback()
                    return {
                        "outcome": (self.COMMIT_DUPLICATE if same
                                    else self.COMMIT_BODY_CONFLICT),
                        "segment_index": index,
                        "confirmed_index": confirmed_index,
                        "confirmed_digest": existing["digest"] if existing else None,
                    }
                if index != confirmed_index + 1:
                    self.conn.rollback()
                    return {
                        "outcome": self.COMMIT_ADVANCED,
                        "segment_index": index,
                        "confirmed_index": confirmed_index,
                        "confirmed_seq": int(row["confirmed_seq"]),
                        "last_digest": row["last_digest"],
                        "last_event_hash": row["last_event_hash"],
                    }
                self.conn.execute(
                    "INSERT INTO segments (segment_index, seq_start, seq_end,"
                    " prev_digest, digest, envelope_json, epoch, applied_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (
                        index,
                        env["seq_start"],
                        env["seq_end"],
                        env["prev_digest"],
                        digest,
                        canonical(env),
                        owner_epoch,
                        now,
                    ),
                )
                self.conn.execute(
                    "UPDATE meta SET confirmed_index = ?, confirmed_seq = ?,"
                    " last_digest = ?, last_event_hash = ?, updated_at = ? WHERE id = 1",
                    (index, env["seq_end"], digest, last_event_hash, now),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        return {"outcome": self.COMMIT_CONFIRMED, "segment_index": index,
                "seq_end": env["seq_end"], "digest": digest}

    def load_segments(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT envelope_json FROM segments ORDER BY segment_index"
        ).fetchall()
        return [json.loads(r["envelope_json"]) for r in rows]

    def get_segment(self, index: int) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT envelope_json FROM segments WHERE segment_index = ?", (index,)
        ).fetchone()
        return json.loads(r["envelope_json"]) if r else None

    def segment_digests(self) -> dict[int, str]:
        rows = self.conn.execute(
            "SELECT segment_index, digest FROM segments ORDER BY segment_index"
        ).fetchall()
        return {int(r["segment_index"]): r["digest"] for r in rows}

    def segment_epochs(self) -> dict[int, int | None]:
        """每个已确认分段是由哪个接管纪元提交的（连续水位的归属视图）。"""
        rows = self.conn.execute(
            "SELECT segment_index, epoch FROM segments ORDER BY segment_index"
        ).fetchall()
        return {int(r["segment_index"]): (int(r["epoch"]) if r["epoch"] is not None else None)
                for r in rows}

    # ------------------------------------------------------------------
    # quarantined
    # ------------------------------------------------------------------

    def quarantine(
        self,
        reason: str,
        detail: str,
        source: str,
        segment_index: int | None = None,
        envelope_digest_value: str | None = None,
        epoch: int | None = None,
    ) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "INSERT INTO quarantined (segment_index, reason, detail, source,"
                " envelope_digest, epoch, detected_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (
                    segment_index,
                    reason,
                    detail,
                    source,
                    envelope_digest_value,
                    epoch,
                    time.time(),
                ),
            )
            return int(cur.lastrowid)

    def list_quarantined(self, include_resolved: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM quarantined"
        if not include_resolved:
            sql += " WHERE resolved_at IS NULL"
        sql += " ORDER BY id DESC LIMIT 200"
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def resolve_open_for_index(self, segment_index: int, resolution: str) -> None:
        """该序号分段已被健康分段确认：此前对它的隔离记录标记为已消除。"""
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE quarantined SET resolved_at = ?, resolution = ?"
                " WHERE segment_index = ? AND resolved_at IS NULL",
                (time.time(), resolution, segment_index),
            )

    def has_open_gap(self) -> bool:
        r = self.conn.execute(
            "SELECT 1 FROM quarantined WHERE resolved_at IS NULL LIMIT 1"
        ).fetchone()
        return r is not None

    # ------------------------------------------------------------------
    # drills
    # ------------------------------------------------------------------

    def upsert_drill(self, drill: dict[str, Any]) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO drills (drill_id, state, locked_index, locked_digest,"
                " locked_event_hash, term_token, term_expires_at, max_lag_segments,"
                " report_json, created_at, updated_at, finished_at, fail_reason)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(drill_id) DO UPDATE SET state=excluded.state,"
                " locked_index=excluded.locked_index, locked_digest=excluded.locked_digest,"
                " locked_event_hash=excluded.locked_event_hash,"
                " term_token=excluded.term_token, term_expires_at=excluded.term_expires_at,"
                " report_json=excluded.report_json, updated_at=excluded.updated_at,"
                " finished_at=excluded.finished_at, fail_reason=excluded.fail_reason",
                (
                    drill["drill_id"],
                    drill["state"],
                    drill.get("locked_index"),
                    drill.get("locked_digest"),
                    drill.get("locked_event_hash"),
                    drill.get("term_token"),
                    drill.get("term_expires_at"),
                    drill["max_lag_segments"],
                    canonical(drill.get("report") or {}),
                    drill["created_at"],
                    drill["updated_at"],
                    drill.get("finished_at"),
                    drill.get("fail_reason"),
                ),
            )

    def get_drill(self, drill_id: str) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT * FROM drills WHERE drill_id = ?", (drill_id,)
        ).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
        del d["report_json"]
        return d

    def list_drills(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM drills ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
            del d["report_json"]
            out.append(d)
        return out

    def active_drill(self) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT * FROM drills WHERE state IN ('VALIDATING','ISSUED')"
            " ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
        del d["report_json"]
        return d

    def list_active_drills(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM drills WHERE state IN ('VALIDATING','ISSUED')"
            " ORDER BY created_at"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
            del d["report_json"]
            out.append(d)
        return out

    def finalize_drill_if_active(
        self,
        drill_id: str,
        state: str,
        reason: str,
        detail: str,
        report: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> bool:
        """把演练从**非终态**原子收口为终态，并在同一事务写且只写一条审计。

        条件更新（``state IN ('VALIDATING','ISSUED')``）是跨线程/跨进程的
        唯一赢家选举：定时到期与人工撤销（乃至第二个副本进程）同时收口时，
        只有一个事务能把行改写成功，且终态报告、状态变更与
        ``drill_finished`` 审计在同一事务提交——要么三者都落，要么都不落，
        绝不会出现“终态已落但审计缺失”或“审计已写但状态还停在 ISSUED”的
        半截状态。

        返回 True 表示本次调用是唯一赢家；False 表示演练已被别的路径收口，
        调用方必须保持原终态、不得再写审计。
        """
        now = now if now is not None else time.time()
        with self.lock, self.conn:
            if report is not None:
                cur = self.conn.execute(
                    "UPDATE drills SET state = ?, term_token = NULL, term_expires_at = NULL,"
                    " updated_at = ?, finished_at = ?, fail_reason = ?, report_json = ?"
                    " WHERE drill_id = ? AND state IN ('VALIDATING','ISSUED')",
                    (state, now, now, reason, canonical(report), drill_id),
                )
            else:
                # 不带报告：保留既有 report_json，仅在其中并入 termination 注记，
                # 避免把内存中没有完整报告的收口路径写成空报告。
                cur = self.conn.execute(
                    "UPDATE drills SET state = ?, term_token = NULL, term_expires_at = NULL,"
                    " updated_at = ?, finished_at = ?, fail_reason = ?,"
                    " report_json = json_set(COALESCE(report_json, '{}'),"
                    "                          '$.termination', json(?))"
                    " WHERE drill_id = ? AND state IN ('VALIDATING','ISSUED')",
                    (state, now, now, reason,
                     canonical({"reason": reason, "detail": detail, "at": now}), drill_id),
                )
            won = cur.rowcount == 1
            if not won:
                return False
            self.conn.execute(
                "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
                (now, "drill_finished",
                 canonical({"drill_id": drill_id, "state": state,
                            "reason": reason, "detail": detail})),
            )
            return True

    def issue_drill_if_validating(
        self, drill_id: str, token: str, term_expires_at: float,
        report: dict[str, Any], now: float | None = None,
    ) -> bool:
        """VALIDATING → ISSUED 的受保护推进。

        只允许行仍处于 VALIDATING 时发放任期；若演练已被并发收口（另一进程
        的启动作废 / ADVANCE_DURING_DRILL），条件更新影响 0 行，调用方必须
        放弃发放，绝不把终态覆盖回 ISSUED。
        """
        now = now if now is not None else time.time()
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE drills SET state = 'ISSUED', term_token = ?,"
                " term_expires_at = ?, report_json = ?, updated_at = ?"
                " WHERE drill_id = ? AND state = 'VALIDATING'",
                (token, term_expires_at, canonical(report), now, drill_id),
            )
            return cur.rowcount == 1

    def reconcile_drill_termination(self, drill_id: str) -> dict[str, Any] | None:
        """恢复时修复“终态行 / 收口审计”因崩溃留下的不一致（幂等）。

        - 行已终态但缺 ``drill_finished`` 审计：补一条，原因取行上的
          ``fail_reason``，不覆盖任何既有终态；
        - 审计已在但行仍停在非终态（审计先落盘、状态回滚的镜像半截）：
          不重复写审计，只把行补到该审计记录的终态。

        修复后返回演练行；两者一致或演练不存在时返回 None。
        """
        with self.lock, self.conn:
            r = self.conn.execute(
                "SELECT * FROM drills WHERE drill_id = ?", (drill_id,)
            ).fetchone()
            if r is None:
                return None
            row = dict(r)
            fin_rows = self.conn.execute(
                "SELECT detail_json FROM audit_log"
                " WHERE kind = 'drill_finished'"
                " AND json_extract(detail_json, '$.drill_id') = ?"
                " ORDER BY id",
                (drill_id,),
            ).fetchall()
            finals = [json.loads(x["detail_json"]) for x in fin_rows]
            terminal = row["state"] in ("CANCELLED", "ABORTED", "EXPIRED")

            if terminal and not finals:
                # 终态已落、审计缺失：补审计，绝不改动终态。
                reason = row["fail_reason"] or row["state"]
                self.conn.execute(
                    "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
                    (row["finished_at"] or time.time(), "drill_finished",
                     canonical({"drill_id": drill_id, "state": row["state"],
                                "reason": reason,
                                "detail": "崩溃恢复：补记缺失的收口审计"})),
                )
            elif not terminal and finals:
                # 审计已在、行残留非终态：把行补齐到既有审计终态，不再写审计。
                final_state = finals[-1]["state"]
                reason = finals[-1].get("reason") or final_state
                self.conn.execute(
                    "UPDATE drills SET state = ?, term_token = NULL, term_expires_at = NULL,"
                    " finished_at = COALESCE(finished_at, ?), fail_reason = ?,"
                    " updated_at = ? WHERE drill_id = ?",
                    (final_state, time.time(), reason, time.time(), drill_id),
                )
        return self.get_drill(drill_id)

    def drill_by_token(self, token: str) -> dict[str, Any] | None:
        r = self.conn.execute(
            "SELECT * FROM drills WHERE term_token = ?", (token,)
        ).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
        del d["report_json"]
        return d

    # ------------------------------------------------------------------
    # audit
    # ------------------------------------------------------------------

    def audit(self, kind: str, detail: dict[str, Any]) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
                (time.time(), kind, canonical(detail)),
            )

    def list_audit(self, limit: int = 100, kind: str | None = None) -> list[dict[str, Any]]:
        if kind:
            rows = self.conn.execute(
                "SELECT id, at, kind, detail_json FROM audit_log"
                " WHERE kind = ? ORDER BY id DESC LIMIT ?",
                (kind, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT id, at, kind, detail_json FROM audit_log"
                " ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"id": r["id"], "at": r["at"], "kind": r["kind"],
             "detail": json.loads(r["detail_json"])}
            for r in rows
        ]

    def close(self) -> None:
        self.conn.close()
