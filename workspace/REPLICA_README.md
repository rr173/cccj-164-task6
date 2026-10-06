# 配额服务 · 只读灾备复制器

从主站（`quota/`）持续拉取**带序号与 SHA-256 哈希链的密封事件分段**，
在本地独立校验后更新**只读查询投影**；提供受控切换演练、持久化任务状态、
审计工具与运维页面。纯 Python 3.11 标准库，零外部依赖。

- **分段链**：每段带 `segment_index / seq_start..end / prev_digest / digest`，
  前段摘要关联后段，篡改任意字节都会断链；
- **事件链**：段内事件携带主站账本自身的 `prev_hash / event_hash`，跨段首尾相接；
- **原子截面**：分段在投影副本上通过全部校验后，才与确认边界在同一 SQLite
  事务提交并原子替换内存投影。任何时刻查询只看到**完整确认边界**；
- **幂等重放**：已确认序号的分段重复拉取正文一致即忽略；重启从最后确认边界恢复；
- **只暴露密封截面**：主站尾部未密封事件不会下发，杜绝“事件进一半”的截面。

```
主站 quota.http_server                 灾备 replica（只读）
  SQLite 事件账本 ── /replica/segments/{i} ──┐
  （event_hash 链）  /replica/status         │
                                             ▼
                         Replicator 拉取循环（轮询 + 长驻）
                           校验 → 投影副本试折叠 → 原子提交
                                             │
            SQLite(segments/meta/quarantined/drills/audit_log)
                                             │
        只读查询 /api/state|plans|commands   任期 /term/{token}/*
        运维 /ops/*（演练/暂停/审计）        运维页面 /
```

## 快速开始

```bash
# 1) 主站（开启馈源；--allow-feed-chaos 仅测试环境注入故障）
python3 -m quota.http_server --port 8765 --db ./dr/primary.db \
    --segment-size 10 --allow-feed-chaos

# 2) 灾备复制器
python3 -m replica serve --primary http://127.0.0.1:8765 \
    --db ./dr/replica.db --port 8899 --segment-size 10 --max-lag 8

# 3) 运维页面
open http://localhost:8899/

# 一键演示（复制/截面/演练/故障隔离/离线审计）
bash demo_replica.sh
```

测试：主站 220 项 + 灾备 32 项端到端（真实 HTTP 双进程 + 真实持久化）：

```bash
python3 test_service.py
python3 -m unittest test_replica
```

## 分段信封

```jsonc
{
  "segment_index": 3, "segment_size": 10,
  "seq_start": 21, "seq_end": 30,
  "prev_digest": "<第2段 digest，首段为 64 个 0>",
  "digest": "<对下面全部规范化字段的 SHA-256>",
  "events": [
    {"seq":21, "type":"LeaseGranted", "payload":{...},
     "command_id":"grant-1", "fingerprint":"{...canonical...}",
     "event_hash":"...", "prev_hash":"..."}
  ],
  "receipts": [
    {"command_id":"grant-1", "fingerprint":"...", "seq":21,
     "result":{"command":"grant","lease_id":"...","event_count":1}}
  ]
}
```

摘要规则（主备共用，`quota/feed.py::envelope_digest`）：对**不含 digest**
的规范化信封（键排序、无空白 JSON）取 SHA-256。主站只下发“满段”
（最后一个不满的尾部不密封），保证灾备截面始终落在完整边界。

## 校验与准入（`replica/verifier.py`）

每个新分段按顺序通过以下检查，任一失败即隔离（不触碰确认截面）：

| 顺序 | 检查 | 失败原因码 |
|---|---|---|
| 1 | 信封结构完整 | `ENVELOPE_DIGEST_MISMATCH` |
| 2 | 已确认同序号：正文一致→忽略；不同→分叉 | `BODY_CONFLICT` |
| 3 | 演练冻结期收到新分段 | `ADVANCE_DURING_DRILL` |
| 4 | 序号恰好 = 边界+1（错序/缺失） | `SEGMENT_INDEX_MISMATCH` |
| 5 | 段大小一致且已满段 | `SEGMENT_SIZE_MISMATCH` |
| 6 | 段内 seq 连续、与边界衔接 | `SEQ_NOT_CONTIGUOUS` |
| 7 | `prev_digest` 关联前段、`digest` 重算 | `CHAIN_DIGEST_BROKEN` / `ENVELOPE_DIGEST_MISMATCH` |
| 8 | 段内事件哈希重算、跨段首尾相接 | `EVENT_HASH_BROKEN` / `EVENT_CROSS_CHAIN_BROKEN` |
| 9 | 与主站当前权威摘要比对（识别自洽分叉） | `BODY_CONFLICT` |
| 10 | 投影副本折叠（复用主站领域代码） | `EVENT_APPLY_FAILED` |
| 11 | 非负余额 / 全局守恒 / 层级 / 租约 | `PROJECTION_AUDIT_FAILED` |
| 12 | 段内回执指向本段事件且指纹可解析 | `RECEIPT_MALFORMED` |

**落后隔离**：稳态运行中主站领先密封分段超过 `--max-lag`，复制器进入
`LAG_OVER_LIMIT` 隔离、停止前进并明确标记，不把陈旧截面当健康截面；
**初始冷启动追赶**不触发该隔离（首次建站允许批量追平）。缺口分段恢复后，
此前同序号的隔离记录自动标记“已消除”。

## 受控切换演练（状态机）

```
POST /ops/drills {drill_id?, term_seconds, max_lag_segments?, validation_delay_ms?}
   │  锁定前：查主站 sealed_index，落后超限 → 409 LAG_EXCEEDED_AT_LOCK
   ▼
VALIDATING ── 复制立即冻结；全量验证：
   │          ① 从磁盘分段重放双链 ② 守恒/层级/租约
   │          ③ 磁盘重放摘要 == 线上确认投影摘要（重放等价）
   │          ④ 命令回执完整、结果与事件推导一致
   │          ⑤ 验证前后主站 sealed_index 不得变化
   ├── 任一失败 ──▶ ABORTED（ADVANCE_DURING_DRILL / VALIDATION_FAILED …）
   │               冻结释放，复制自动恢复，最近可用截面不受污染
   ▼
ISSUED（发放不透明任期 token）
   /term/{token}/state    锁定截面：余额/租约/草案
   /term/{token}/commands 锁定时的命令结果
   /term/{token}/summary  截面摘要/任期倒计时/冻结摘要
   │
   ├── POST /ops/drills/{id}/cancel ──▶ CANCELLED（地址立即 410，恢复复制）
   └── 到达 term_expires_at（sweeper 0.5s 检查；无需任何请求触发）──▶ EXPIRED（410）
```

- **主动收口**：截止时刻到达后，即使调用方再也不发起请求，后台 sweeper
  也会**自行结束**该条演练记录（不依赖“下一次读任期地址”惰性触发），
  旧凭证随后**稳定返回 410**，后台复制立即从冻结点**继续**（不跳号、
  不漏掉仍待处置的异常批次——缺口分段仍须先补齐并消除旧隔离再前进）；
- **单次终态 / 单条审计**：终态落盘是一条带状态条件的 SQLite 原子更新
  （`state IN ('VALIDATING','ISSUED')`），终态行、终态报告与
  `drill_finished` 审计在**同一事务**提交。定时到期、人工撤销、并发任期
  读取乃至**两个副本进程共用同一 DB** 同时收口时，只有一个事务能成为赢家，
  因而全局只落**一次终态、一条审计**；输家按既有终态幂等返回；
- **重启恢复**：构造时先修复“终态行/收口审计”因崩溃留下的半截不一致
  （缺审计则补、行残留则按既有审计补齐，均不覆盖既有终态），随后立即
  作废 VALIDATING 演练、并主动结束在**停机期间到期**的 ISSUED 任期——
  不等待 sweeper 的首个 tick，重启后不存在“已过期却仍冻结”的窗口；

- **幂等**：同一 `drill_id` 再次请求返回**原结果**（同一 token、同一报告），
  不重复发任期；
- **任期只读**：`/term/*` 与 `/api/*` 的任何写方法一律 `405 READ_ONLY`；
- **持久化**：演练、token、锁定截面冻结与报告在 SQLite；进程重启后已发
  任期继续有效；验证中崩溃的演练启动时按 `PROCESS_RESTART` 作废；
- **并发演练**：存在 VALIDATING/ISSUED 演练时再创建 → `409 ALREADY_ACTIVE_DRILL`。

## 进程纪元围栏（滚动升级写入所有权）

复制器每次启动把 `meta.epoch` +1。纪元是**存储中**的写入所有权凭证，
每次**提交**（分段落盘 + 边界推进）都在**同一 SQLite 事务**内做条件
CAS（`meta.epoch` 必须仍等于本实例持有的纪元、且边界恰好停在前一段），
每次**发布**（内存投影原子替换）前再读一次权威纪元确认仍持所有权。
`POST /test/ingest` 必须携带 `X-Replica-Epoch`；纪元不匹配的迟到写入被
拒绝（`410 STALE_EPOCH`）并写入隔离表与审计轨迹。正常轮询自动携带当前纪元。

**滚动升级竞争修复**：上一实例尚未停稳、接替实例已提升纪元时，旧实例
在途拉到的批次可能与新实例竞争同一水位。围栏保证：

- **失主提交被稳定拒绝**：旧实例即使已完成全部链校验/试折叠，提交时
  CAS 发现纪元已变（`410 OWNERSHIP_LOST`），绝不落盘；随后停止后台
  拉取/sweep，演练/暂停等写操作同样 410，在途工作**稳定退出**；
- **重复批次只返回原结果**：同序号且正文一致的重复批次幂等返回
  `duplicate_ignored`，不重复落盘；返回前若内存落后于权威水位，先从
  存储完整重建，绝不让调用方停留在漏批截面；正文不同仍按 `BODY_CONFLICT`
  隔离；
- **异常缺口仍阻挡前进**：权威水位越过本序号却仍缺中段时，按
  `SEGMENT_INDEX_MISMATCH` 隔离，必须先补齐缺口并消除旧隔离才能前进；
- **接替实例从权威水位重建**：`start()` 顺序固定为「提升纪元 → 从存储
  已确认分段完整重放重建内存 → 启动后台循环」，杜绝“先发布陈旧内存、
  后接管”的窗口；
- **可观测**：`segments` 表记录每批的确认纪元；`/ops/status` 与运维页
  显示接管纪元（`takeover_epochs` / `segment_epochs`）、本实例纪元与
  存储权威纪元（`epoch` / `authoritative_epoch`）、失主状态
  （`ownership_lost`）与**连续水位**（`watermark_contiguous`：已确认
  序号必须恰好为 `1..confirmed_index`）；失主拒绝写入
  `ownership_lost` 审计与隔离表（含被拒旧纪元）。

由此「旧实例漏掉别人刚提交的中间批次、却继续发布后续批次内存结果，
磁盘重算值与查询结果不同」的窗口被彻底关闭：磁盘重算值始终等于查询
截面（`replay_equivalence`）。

## HTTP API

只读查询面：

| 方法/路径 | 说明 |
|---|---|
| `GET /api/state` | 最近确认截面（节点余额/租约/草案视图 + 截面摘要） |
| `GET /api/plans` | 确认截面内容量草案 |
| `GET /api/commands` | 已确认命令回执列表 |
| `GET /api/commands/{command_id}` | 单条命令结果 |
| `GET /api/segments` | 已确认分段（序号/seq/摘要） |
| `GET /term/{token}/state` | 任期：锁定截面 |
| `GET /term/{token}/commands` | 任期：锁定时命令结果 |
| `GET /term/{token}/summary` | 任期：截面元信息与倒计时 |

运维/演练：

| 方法/路径 | 说明 |
|---|---|
| `GET /ops/status` | 进度、主站 tip、延迟、是否落后超限、冻结演练、最后错误 |
| `GET /ops/quarantine` | 隔离记录（含原因、来源、是否已消除） |
| `GET /ops/audit` | 在线全量一致性审计 |
| `GET /ops/audit-tail` | 审计轨迹尾部 |
| `GET /ops/drills` / `POST /ops/drills` | 演练列表 / 创建演练（幂等） |
| `GET /ops/drills/{id}` | 演练报告（含全部检查项与锁定截面） |
| `POST /ops/drills/{id}/cancel` | 取消演练（任期失效） |
| `POST /ops/pause` / `POST /ops/resume` | 人工暂停/恢复复制 |
| `POST /test/ingest`（需 `X-Replica-Epoch`） | 测试/补拉：走同一套准入校验 |

主站新增只读馈源（`quota/feed.py`，挂载于既有服务，不改业务逻辑）：
`GET /replica/status`、`GET /replica/segments`、`GET /replica/segments/{i}`；
故障注入 `POST /replica/chaos`（模式 `gap / corrupt_event / corrupt_digest / alt_body`，
仅 `--allow-feed-chaos` 时可用）。

## 审计工具

```bash
# 离线：不连主站、只读本地数据库，从磁盘分段完整重放
python3 -m replica audit --db ./dr/replica.db
# 审计轨迹（可按 kind 过滤）
python3 -m replica audit-tail --db ./dr/replica.db --kind segment_quarantined
```

审计项：分段双链完整、非负余额、全局守恒（Σ可用+总消费=容量）、
层级父子边、租约记账、磁盘重放与线上截面等价、命令回执完整且结果与
事件推导一致。

## 持久化布局（单 SQLite）

| 表 | 内容 |
|---|---|
| `meta` | 确认边界（index/seq/尾摘要/尾事件哈希）、分段大小、纪元、暂停标志 |
| `segments` | 已确认分段原文（序号主键、digest 唯一）及确认它的接管纪元 —— 崩溃恢复的事实来源 |
| `quarantined` | 被拒/隔离记录、原因码、来源、检测/消除时间 |
| `drills` | 演练状态机、任期 token/到期时间、锁定摘要、冻结截面、报告 |
| `audit_log` | 只追加审计轨迹（启动、分段确认、隔离、演练全生命周期等） |

## 代码结构

| 文件 | 职责 |
|---|---|
| `quota/feed.py` | 主站只读馈源：密封分段构造、双层摘要、故障注入（新增） |
| `quota/http_server.py` | 挂载 `/replica/*` 路由与 CLI 参数（仅新增，未改既有行为） |
| `replica/verifier.py` | 纯函数校验：信封/双哈希链/投影守恒/命令结果/全量重放 |
| `replica/store.py` | SQLite 持久化：边界与分段同事务原子提交 |
| `replica/replicator.py` | 拉取循环、分段准入、原子截面、演练状态机、任期围栏 |
| `replica/http_server.py` | 只读查询面、任期地址、运维/演练接口、测试注入口 |
| `replica/__main__.py` | `serve` / `audit` / `audit-tail` 命令行 |
| `replica/static/index.html` | 运维页面（进度/延迟/隔离/截面/演练/审计） |
| `test_replica.py` | 32 项端到端测试（含到期主动收口、并发收口、重启恢复、异常批次不跳过与滚动升级写入所有权围栏） |
