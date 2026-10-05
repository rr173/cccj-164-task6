"""HTTP 接口层（Python 标准库，零外部依赖）。

路由：
  GET  /                       租约页面（SPA）
  GET  /api/state              当前全局/节点/租约状态
  GET  /api/audit              一致性审计
  GET  /api/events             事件账本（最近 200 条）
  POST /api/bootstrap          设置全局容量（一次性）
  POST /api/nodes              注册组织/项目/客户端
  POST /api/capacity           调整全局容量（缩容防超卖）
  POST /api/leases             申请租约（父级可用额度内、可带 ttl_seconds）
  POST /api/leases/usage       按幂等事件扣减（usage_event_id 去重）
  POST /api/leases/amend       修订有效期（expected_revision + extend_seconds/new_expires_at）
  POST /api/leases/release     归还租约（释放未消费部分）
  POST /api/leases/revoke      父级撤销/缩容（有下级占用时拒绝）
  POST /api/clients/presence   客户端上/下线（离线关闭其全部租约）
  POST /api/capacity-plans                 创建批量容量调整草案
  POST /api/capacity-plans/preview         预览草案（逐层余额/受影响租约/拒绝原因）
  POST /api/capacity-plans/publish         原子发布（陈旧版本拒绝，要求重新预览）
  POST /api/capacity-plans/discard         废弃草案
  GET  /api/capacity-plans                 草案列表
  GET  /api/capacity-plans/{id}            草案详情（含发布结果）
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .domain import DomainError, ValidationError
from .feed import CHAOS_MODES, Feed
from .service import QuotaService

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")

KIND_BY_PATH = {
    "/api/bootstrap": "bootstrap",
    "/api/nodes": "register",
    "/api/capacity": "capacity",
    "/api/leases": "grant",
    "/api/leases/usage": "usage",
    "/api/leases/amend": "amend",
    "/api/leases/release": "release",
    "/api/leases/revoke": "revoke",
    "/api/clients/presence": "presence",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "QuotaLease/1.0"

    # 静默标准库的噪声日志，改为结构化短日志
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        self.server.app_log(f"{self.command} {self.path} -> {args[1] if len(args) > 1 else ''}")

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------

    def _json(self, code: int, obj: dict) -> None:
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
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValidationError(f"请求体不是合法 JSON: {exc}")
        if not isinstance(data, dict):
            raise ValidationError("请求体必须是 JSON 对象")
        # header 中的幂等键也接收
        header_key = self.headers.get("Idempotency-Key")
        if header_key and not data.get("command_id"):
            data["command_id"] = header_key
        return data

    def _feed(self) -> Feed:
        return self.server.feed  # type: ignore[attr-defined]

    def _service(self) -> QuotaService:
        return self.server.service  # type: ignore[attr-defined]

    def _error(self, exc: DomainError) -> None:
        obj: dict = {"error": type(exc).__name__, "message": str(exc)}
        details = getattr(exc, "details", None)
        if details:
            obj["details"] = details
        self._json(exc.status, obj)

    # ------------------------------------------------------------------
    # 路由
    # ------------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/api/state":
                return self._json(200, self._service().state())
            if path == "/api/audit":
                return self._json(200, self._service().audit())
            if path == "/api/events":
                return self._json(200, {"events": self._service().events()})
            if path == "/api/capacity-plans":
                return self._json(200, self._service().list_plans())
            if path.startswith("/api/capacity-plans/"):
                plan_id = path[len("/api/capacity-plans/") :].strip("/")
                if plan_id:
                    return self._json(200, self._service().get_plan(plan_id))
            if path == "/replica/status":
                return self._json(200, self._feed().status())
            if path == "/replica/segments":
                return self._json(200, {"segments": self._feed().status()["segments"]})
            if path.startswith("/replica/segments/"):
                raw = path[len("/replica/segments/") :].strip("/")
                if raw.isdigit():
                    env = self._feed().get_segment(int(raw))
                    if env is None:
                        return self._json(
                            404,
                            {"error": "segment_unavailable",
                             "message": f"分段 {raw} 缺失或尚未密封"},
                        )
                    return self._json(200, env)
            if path in ("/", "/index.html"):
                return self._serve_static("index.html", "text/html; charset=utf-8")
            if path.startswith("/static/"):
                return self._serve_static(
                    path[len("/static/") :], self._guess_mime(path)
                )
            self._json(404, {"error": "not_found", "message": f"未知路径: {path}"})
        except DomainError as exc:
            self._error(exc)
        except Exception as exc:  # 防御：任何异常都返回结构化错误
            self.server.app_log(f"ERROR {path}: {exc!r}")
            self._json(500, {"error": "InternalError", "message": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/replica/chaos":
                # 故障注入：仅在服务以 --allow-feed-chaos 启动时可用
                if not getattr(self.server, "allow_feed_chaos", False):
                    return self._json(
                        403,
                        {"error": "chaos_disabled",
                         "message": "主站未开启 --allow-feed-chaos，故障注入被拒绝"},
                    )
                body = self._read_body()
                mode = body.get("mode")
                if mode is not None and mode not in CHAOS_MODES:
                    return self._json(
                        400,
                        {"error": "bad_mode",
                         "message": f"mode 必须是 {sorted(CHAOS_MODES)} 或 null"},
                    )
                idx = body.get("segment_index")
                if idx is not None and (not isinstance(idx, int) or idx < 1):
                    return self._json(400, {"error": "bad_index",
                                           "message": "segment_index 必须是 >= 1 的整数"})
                return self._json(200, {"chaos": self._feed().set_chaos(mode, idx)})
            if path.startswith("/api/capacity-plans/"):
                action = path[len("/api/capacity-plans/") :].strip("/")
                plan_methods = {
                    "preview": "preview_plan",
                    "publish": "publish_plan",
                    "discard": "discard_plan",
                }
                method_name = plan_methods.get(action)
                if method_name is None:
                    self._json(404, {"error": "not_found", "message": f"未知路径: {path}"})
                    return
                body = self._read_body()
                result = getattr(self._service(), method_name)(body)
                # 预览只是刷新预览基线（不产生用户可见的新资源），固定 200
                code = 200 if action == "preview" else (200 if result.get("replayed") else 201)
                return self._json(code, result)
            if path == "/api/capacity-plans":
                body = self._read_body()
                result = self._service().create_plan(body)
                code = 200 if result.get("replayed") else 201
                return self._json(code, result)
            kind = KIND_BY_PATH.get(path)
            if kind is None:
                self._json(404, {"error": "not_found", "message": f"未知路径: {path}"})
                return
            body = self._read_body()
            result = self._service().execute(kind, body)
            code = 200 if result.get("replayed") else 201
            self._json(code, result)
        except DomainError as exc:
            self._error(exc)
        except Exception as exc:
            self.server.app_log(f"ERROR POST {path}: {exc!r}")
            self._json(500, {"error": "InternalError", "message": str(exc)})

    # ------------------------------------------------------------------
    # 静态文件
    # ------------------------------------------------------------------

    def _serve_static(self, rel_path: str, content_type: str) -> None:
        # 防目录穿越
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


def create_server(
    host: str,
    port: int,
    db_path: str,
    segment_size: int = 10,
    allow_feed_chaos: bool = False,
) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.service = QuotaService(db_path)  # type: ignore[attr-defined]
    httpd.feed = Feed(httpd.service.store, segment_size=segment_size)  # type: ignore[attr-defined]
    httpd.allow_feed_chaos = allow_feed_chaos  # type: ignore[attr-defined]
    httpd.app_log = lambda msg: print(f"[quota] {msg}", flush=True)  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="多租户 API 配额租约服务")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")))
    parser.add_argument(
        "--db", default=os.environ.get("QUOTA_DB", os.path.join(os.getcwd(), "quota.db"))
    )
    parser.add_argument(
        "--segment-size", type=int, default=int(os.environ.get("SEGMENT_SIZE", "10")),
        help="灾备馈源的密封分段大小（事件数/分段）",
    )
    parser.add_argument(
        "--allow-feed-chaos", action="store_true",
        help="开启灾备馈源的故障注入面（仅测试/演练环境）",
    )
    args = parser.parse_args()

    httpd = create_server(
        args.host, args.port, args.db,
        segment_size=args.segment_size,
        allow_feed_chaos=args.allow_feed_chaos,
    )
    print(
        f"[quota] 服务已启动: http://{args.host}:{args.port}  "
        f"(账本 {args.db}，分段大小 {args.segment_size}，"
        f"故障注入 {'开' if args.allow_feed_chaos else '关'})",
        flush=True,
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.service.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
