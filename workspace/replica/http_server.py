"""灾备复制器 HTTP 层（Python 标准库）。

对外只读查询面（任何写方法一律 405）：

  GET  /api/state                 最近完整确认截面（余额/租约/草案视图）
  GET  /api/plans                 确认截面内的容量草案
  GET  /api/commands              已确认命令回执（命令结果）
  GET  /api/commands/{command_id} 单条命令结果
  GET  /api/segments              已确认分段（序号/seq/摘要）
  GET  /term/{token}/state        带任期地址：锁定截面（演练发放）
  GET  /term/{token}/commands     带任期地址：锁定时的命令结果

运维 / 演练接口：

  GET  /ops/status                复制进度、延迟、隔离、当前演练
  GET  /ops/quarantine            隔离记录（含已消除）
  GET  /ops/audit                 全量一致性审计
  GET  /ops/audit-tail            审计轨迹尾部
  GET  /ops/drills                演练列表
  POST /ops/drills                创建受控切换演练（同 id 再请求返回原结果）
  GET  /ops/drills/{id}           演练报告
  POST /ops/drills/{id}/cancel    取消演练（任期地址立即失效）
  POST /ops/pause|resume          人工暂停 / 恢复复制
  POST /test/ingest               测试注入口（必须携带 X-Replica-Epoch）
  GET  /                          运维操作页面
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .replicator import ReplicatorError

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class ReplicaHandler(BaseHTTPRequestHandler):
    server_version = "QuotaReplica/1.0"

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        self.server.app_log(f"{self.command} {self.path}")  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    def _json(self, code: int, obj: dict | list) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ReplicatorError("BAD_JSON", f"请求体不是合法 JSON: {exc}", status=400)
        return data if isinstance(data, dict) else {}

    def _rep(self):
        return self.server.replicator  # type: ignore[attr-defined]

    def _error(self, exc: ReplicatorError) -> None:
        self._json(exc.status, {
            "error": exc.reason, "message": str(exc), "details": exc.details,
        })

    # ------------------------------------------------------------------
    # 只读查询面
    # ------------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        rep = self._rep()
        try:
            if path == "/api/state":
                return self._json(200, rep.current_state())
            if path == "/api/plans":
                state = rep.current_state()
                return self._json(200, {
                    "confirmed_index": state["replica"]["confirmed_index"],
                    "plans": [
                        {"id": pid, **pl}
                        for pid, pl in sorted(rep.projection.plans.items())
                    ],
                })
            if path == "/api/commands":
                return self._json(200, rep.commands_view())
            if path.startswith("/api/commands/"):
                cid = path[len("/api/commands/"):].strip("/")
                r = rep.receipt(cid)
                if r is None:
                    return self._json(404, {"error": "NOT_FOUND",
                                            "message": f"已确认截面内无此命令: {cid}"})
                return self._json(200, r)
            if path == "/api/segments":
                return self._json(200, {
                    "confirmed_index": rep.confirmed_index,
                    "segments": [
                        {"segment_index": s["segment_index"],
                         "seq_start": s["seq_start"], "seq_end": s["seq_end"],
                         "prev_digest": s["prev_digest"], "digest": s["digest"]}
                        for s in rep.segments
                    ],
                })
            # ---- 带任期地址 ----
            if path.startswith("/term/"):
                return self._serve_term(path)
            # ---- 运维面 ----
            if path == "/ops/status":
                return self._json(200, rep.status())
            if path == "/ops/quarantine":
                return self._json(200, {"records": rep.store.list_quarantined(True)})
            if path == "/ops/audit":
                return self._json(200, rep.full_audit())
            if path == "/ops/audit-tail":
                return self._json(200, {"entries": rep.store.list_audit(100)})
            if path == "/ops/drills":
                return self._json(200, {"drills": rep.list_drills()})
            if path.startswith("/ops/drills/"):
                did = path[len("/ops/drills/"):].strip("/")
                if did and "/" not in did:
                    return self._json(200, rep.get_drill(did))
            if path in ("/", "/index.html"):
                return self._serve_static("index.html", "text/html; charset=utf-8")
            if path.startswith("/static/"):
                return self._serve_static(path[len("/static/"):], self._guess_mime(path))
            self._json(404, {"error": "not_found", "message": f"未知路径: {path}"})
        except ReplicatorError as exc:
            self._error(exc)
        except Exception as exc:  # noqa: BLE001
            self.server.app_log(f"ERROR GET {path}: {exc!r}")  # type: ignore[attr-defined]
            self._json(500, {"error": "InternalError", "message": str(exc)})

    def _serve_term(self, path: str) -> None:
        rep = self._rep()
        parts = [p for p in path.split("/") if p]
        # /term/{token}/state | /term/{token}/commands | /term/{token}
        if len(parts) < 2 or parts[0] != "term":
            self._json(404, {"error": "not_found", "message": path})
            return
        token = parts[1]
        view = parts[2] if len(parts) > 2 else "state"
        surface = rep.serve_term(token)  # 无效/过期任期抛 410
        if view == "state":
            self._json(200, surface.get("surface_state", {}))
        elif view == "summary":
            self._json(200, {k: v for k, v in surface.items()
                            if k not in ("surface_state", "commands")})
        elif view == "commands":
            self._json(200, {
                "drill_id": surface["drill_id"],
                "state": surface["state"],
                "locked_index": surface["locked_index"],
                "term_expires_at": surface["term_expires_at"],
                "commands": surface.get("commands", []),
            })
        elif view == "report":
            self._json(200, {k: v for k, v in surface.items()
                            if k not in ("surface_state", "commands")})
        else:
            self._json(404, {"error": "not_found", "message": view})

    # ------------------------------------------------------------------
    # 写方法（灾备是只读的；业务写一律 405，只有运维面接受 POST）
    # ------------------------------------------------------------------

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        rep = self._rep()
        try:
            if path.startswith("/api/") or path.startswith("/term/"):
                return self._json(405, {
                    "error": "READ_ONLY",
                    "message": "灾备站点为只读复制副本，拒绝一切业务写入",
                })
            if path == "/ops/drills":
                body = self._read_body()
                drill = rep.create_drill(
                    body.get("drill_id"),
                    max_lag_segments=body.get("max_lag_segments"),
                    term_seconds=float(body.get("term_seconds", 300)),
                    validation_delay_ms=int(body.get("validation_delay_ms", 0)),
                )
                return self._json(201 if drill["state"] == "ISSUED" else 200, drill)
            if path.endswith("/cancel") and path.startswith("/ops/drills/"):
                did = path[len("/ops/drills/"): -len("/cancel")].strip("/")
                return self._json(200, rep.cancel_drill(did))
            if path == "/ops/pause":
                return self._json(200, rep.set_paused(True))
            if path == "/ops/resume":
                return self._json(200, rep.set_paused(False))
            if path == "/test/ingest":
                body = self._read_body()
                raw = self.headers.get("X-Replica-Epoch")
                if raw is None:
                    return self._json(400, {"error": "EPOCH_REQUIRED",
                                            "message": "测试注入必须携带 X-Replica-Epoch"})
                try:
                    epoch = int(raw)
                except ValueError:
                    return self._json(400, {"error": "BAD_EPOCH",
                                            "message": "X-Replica-Epoch 必须是整数"})
                return self._json(200, rep.ingest(body, source="test-inject", epoch=epoch))
            self._json(404, {"error": "not_found", "message": f"未知路径: {path}"})
        except ReplicatorError as exc:
            self._error(exc)
        except Exception as exc:  # noqa: BLE001
            self.server.app_log(f"ERROR POST {path}: {exc!r}")  # type: ignore[attr-defined]
            self._json(500, {"error": "InternalError", "message": str(exc)})

    def do_PUT(self) -> None:  # noqa: N802
        self._json(405, {"error": "READ_ONLY", "message": "灾备站点只读"})

    do_DELETE = do_PATCH = do_PUT  # type: ignore[assignment]

    # ------------------------------------------------------------------
    def _serve_static(self, rel_path: str, content_type: str) -> None:
        rel_path = rel_path.replace("\\", "/").lstrip("/")
        full = os.path.normpath(os.path.join(STATIC_DIR, rel_path))
        if not full.startswith(STATIC_DIR + os.sep) or not os.path.isfile(full):
            self._json(404, {"error": "not_found", "message": rel_path})
            return
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _guess_mime(path: str) -> str:
        if path.endswith(".css"):
            return "text/css; charset=utf-8"
        if path.endswith(".js"):
            return "application/javascript; charset=utf-8"
        return "application/octet-stream"


def create_server(host: str, port: int, replicator) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), ReplicaHandler)
    httpd.replicator = replicator  # type: ignore[attr-defined]
    httpd.app_log = lambda msg: print(f"[replica] {msg}", flush=True)  # type: ignore[attr-defined]
    return httpd
