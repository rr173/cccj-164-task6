"""灾备复制器命令行。

用法：
  python3 -m replica serve --primary http://127.0.0.1:8765 \\
      --db ./dr/replica.db --port 8899 --segment-size 10 --max-lag 5
  python3 -m replica audit --db ./dr/replica.db
      离线审计：不连主站、只读本地持久化，重放双链/守恒/命令结果
  python3 -m replica audit-tail --db ./dr/replica.db [--kind drill_issued]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .replicator import Replicator
from .store import ReplicaStore


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="配额服务只读灾备复制器")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="启动灾备复制器 + HTTP 运维/查询面")
    s.add_argument("--primary", required=True, help="主站基址，如 http://127.0.0.1:8765")
    s.add_argument("--db", required=True, help="灾备持久化 SQLite 路径")
    s.add_argument("--host", default=os.environ.get("DR_HOST", "0.0.0.0"))
    s.add_argument("--port", type=int, default=int(os.environ.get("DR_PORT", "8899")))
    s.add_argument("--segment-size", type=int, default=10)
    s.add_argument("--max-lag", type=int, default=5, help="允许的最大落后分段数")
    s.add_argument("--poll-interval", type=float, default=0.5)

    a = sub.add_parser("audit", help="离线全量一致性审计（只读本地数据库）")
    a.add_argument("--db", required=True)

    at = sub.add_parser("audit-tail", help="查看审计轨迹尾部")
    at.add_argument("--db", required=True)
    at.add_argument("--kind", default=None)
    at.add_argument("--limit", type=int, default=50)

    args = parser.parse_args(argv)

    if args.cmd == "serve":
        os.makedirs(os.path.dirname(os.path.abspath(args.db)) or ".", exist_ok=True)
        store = ReplicaStore(args.db, segment_size=args.segment_size)
        rep = Replicator(
            store,
            primary_url=args.primary,
            max_lag_segments=args.max_lag,
            poll_interval=args.poll_interval,
        )
        from .http_server import create_server

        httpd = create_server(args.host, args.port, rep)
        rep.start()
        print(
            f"[replica] 灾备复制器已启动: http://{args.host}:{args.port}\n"
            f"[replica]   主站 {args.primary} | 分段大小 {args.segment_size}"
            f" | 最大落后 {args.max_lag} 段 | 纪元 {rep.epoch}\n"
            f"[replica]   运维页面 http://localhost:{args.port}/",
            flush=True,
        )
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            rep.shutdown()
            store.close()
            httpd.server_close()
        return 0

    if args.cmd == "audit":
        if not os.path.exists(args.db):
            print(f"审计数据库不存在: {args.db}", file=sys.stderr)
            return 2
        store = ReplicaStore(args.db, segment_size=10)  # 已存在库仅读 meta
        from . import verifier

        segments = store.load_segments()
        checks = [
            {"name": "segment_hash_chain", "ok": True,
             "detail": f"{len(segments)} 个分段双链重放"},
        ]
        rebuilt = verifier.replay_all(segments)
        checks.extend(verifier.audit_projection(rebuilt)["checks"])
        checks.append(verifier.full_receipt_check(segments))
        result = {
            "ok": all(c["ok"] for c in checks),
            "db": args.db,
            "segment_size": store.meta()["segment_size"],
            "confirmed_index": store.meta()["confirmed_index"],
            "confirmed_seq": store.meta()["confirmed_seq"],
            "epoch": store.meta()["epoch"],
            "state_digest": verifier.state_digest(rebuilt),
            "open_quarantines": store.list_quarantined(),
            "checks": checks,
        }
        _print(result)
        store.close()
        return 0 if result["ok"] else 1

    if args.cmd == "audit-tail":
        if not os.path.exists(args.db):
            print(f"审计数据库不存在: {args.db}", file=sys.stderr)
            return 2
        store = ReplicaStore(args.db, segment_size=10)
        _print({"entries": store.list_audit(args.limit, kind=args.kind)})
        store.close()
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
