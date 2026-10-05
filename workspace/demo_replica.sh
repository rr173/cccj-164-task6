#!/usr/bin/env bash
# 灾备复制器一键演练：启动主站+灾备 → 制造业务 → 演示复制/隔离/演练/重启。
# 用法: bash demo_replica.sh
set -euo pipefail

DIR=$(mktemp -d /tmp/dr-demo-XXXX)
P=8765; R=8899
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT
PIDS=()

say() { printf "\n\033[1;36m== %s ==\033[0m\n" "$*"; }
jqf() { jq -c . ; }

say "1) 启动主站 :$P（分段大小 5，开启故障注入面）与灾备 :$R"
python3 -m quota.http_server --port $P --db "$DIR/primary.db" \
  --segment-size 5 --allow-feed-chaos >"$DIR/p.log" 2>&1 &
PIDS+=($!)
python3 -m replica serve --primary "http://127.0.0.1:$P" \
  --db "$DIR/replica.db" --port $R --segment-size 5 --max-lag 50 >"$DIR/r.log" 2>&1 &
PIDS+=($!)
sleep 1.5

say "2) 主站持续写入（三级树授约 + 消费）"
curl -s -X POST localhost:$P/api/bootstrap -H 'Content-Type: application/json' -d '{"capacity":100000000}' >/dev/null
for n in '{"type":"organization","parent_id":"platform","node_id":"O1"}' \
         '{"type":"project","parent_id":"O1","node_id":"P1"}' \
         '{"type":"client","parent_id":"P1","node_id":"C1"}'; do
  curl -s -X POST localhost:$P/api/nodes -H 'Content-Type: application/json' -d "$n" >/dev/null
done
for i in $(seq 0 14); do
  curl -s -X POST localhost:$P/api/leases -H 'Content-Type: application/json' \
    -d "{\"lessor_id\":\"platform\",\"holder_id\":\"O1\",\"amount\":10000,\"lease_id\":\"LP$i\"}" >/dev/null
  curl -s -X POST localhost:$P/api/leases -H 'Content-Type: application/json' \
    -d "{\"lessor_id\":\"O1\",\"holder_id\":\"P1\",\"amount\":1000,\"lease_id\":\"LO$i\"}" >/dev/null
  curl -s -X POST localhost:$P/api/leases -H 'Content-Type: application/json' \
    -d "{\"lessor_id\":\"P1\",\"holder_id\":\"C1\",\"amount\":100,\"lease_id\":\"LC$i\"}" >/dev/null
  curl -s -X POST localhost:$P/api/leases/usage -H 'Content-Type: application/json' \
    -d "{\"lease_id\":\"LC$i\",\"amount\":10,\"usage_event_id\":\"u$i\"}" >/dev/null
done
sleep 2
curl -s localhost:$R/ops/status | jq -c '.replication | {confirmed_index, confirmed_seq, lag_segments, state_digest: .state_digest[0:16]}'

say "3) 灾备只暴露完整确认截面（守恒：可用+消费=容量）"
curl -s localhost:$R/api/state | jq -c '{global_capacity, total_available, total_consumed, conserved: (.total_available+.total_consumed)}'

say "4) 受控切换演练（锁定→验证→发任期）"
DRILL=$(curl -s -X POST localhost:$R/ops/drills -H 'Content-Type: application/json' \
  -d '{"drill_id":"DR-DEMO","term_seconds":300}')
echo "$DRILL" | jq -c '{state, locked_index, term: .term_token[0:14], checks: [.report.checks[].name]}'
TOKEN=$(echo "$DRILL" | jq -r .term_token)
echo "-- 任期截面摘要:"; curl -s localhost:$R/term/$TOKEN/summary | jq -c '{state, locked_index, command_count, total_consumed}'

say "5) 演练冻结期主站继续写，灾备截面定格；取消后地址 410 并恢复"
curl -s -X POST localhost:$P/api/capacity -H 'Content-Type: application/json' -d '{"capacity":99999999}' >/dev/null
sleep 1
curl -s localhost:$R/ops/status | jq -c '.replication | {confirmed_index, frozen_by_drill}'
curl -s -X POST localhost:$R/ops/drills/DR-DEMO/cancel | jq -c '{state}'
curl -s -o /dev/null -w "旧任期地址 HTTP %{http_code}（410=已失效）\n" localhost:$R/term/$TOKEN/summary

say "6) 故障注入：主站 gap 缺失分段 → 灾备隔离且不污染截面"
for i in $(seq 0 6); do
  curl -s -X POST localhost:$P/api/capacity -H 'Content-Type: application/json' \
    -d "{\"capacity\":9000000$i,\"command_id\":\"gap-$i\"}" >/dev/null
done
curl -s -X POST localhost:$P/replica/chaos -H 'Content-Type: application/json' \
  -d '{"mode":"gap","segment_index":13}' >/dev/null
sleep 2
curl -s localhost:$R/ops/quarantine | jq -c '[.records[] | select(.resolved_at==null) | {reason, segment_index}][0]'
curl -s -X POST localhost:$P/replica/chaos -H 'Content-Type: application/json' -d '{"mode":null}' >/dev/null
say "7) 离线 CLI 审计（不连主站，重放双链/守恒/命令结果）"
sleep 2
python3 -m replica audit --db "$DIR/replica.db" | jq -c '{ok, confirmed_index, checks: [.checks[] | {name, ok}]}'

say "运维页面: http://localhost:$R/   （Ctrl-C 或脚本退出后清理 $DIR）"
echo "按回车结束演练…"; read -r || true
