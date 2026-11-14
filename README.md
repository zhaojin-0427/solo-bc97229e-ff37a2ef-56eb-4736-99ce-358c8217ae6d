# 自然博物馆标本登记库同步系统

把本地 SQLite(WAL) 标本登记库持续复制到一个**独立的** SQLite 副本，供分布研究
查询；鉴定员仍直接在源库登记、更正、撤回。提供 Flask 网页启动/停止同步、查看
进度与错误、按标本号查询副本，并附带可持续修改的示例源库。

## 需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 源库可并发提交增删改，首次复制期间也不停写 | 同步器只用只读短事务打开源库；WAL 允许读写并发，源写入不等待任何复制/同步完成 |
| 不得漏更 | 源库 `_change_log` 触发器记录全部行变更；首次复制 = 分页快照 + 重放快照窗口内全部日志；增量按序号续读 |
| 换库后不得把新库误判为“已同步”（两库序号可能相同） | 每个源库在 `_source_identity` 持久唯一 `source_uid`，副本 `sync_meta` 绑定来源 UID；启动前 `reconcile_source` 对账，UID 不同即判定换库，序号重叠也不会漏数 |
| 换库后首个查询前旧副本就不可查 | 对账发现换库立即在同一事务把副本置为 `REBUILDING` 并清空旧数据；Web `create_app` 在服务任何请求前完成对账，重建期间查询持续 503 |
| 新数据、来源标识、进度一致后才开放查询 | 重建最后一个事务同时提交数据、`source_uid`、`last_applied_seq`（提交前复核源 UID 未再变化），之后才置 RUNNING |
| 重建中断/失败后重启不放行旧数据 | 重建期进度始终为 0、阶段为 REBUILDING/ERROR（非 RUNNING 即 503）；重启重做完整重建，同次换库的重试不重复计数 |
| 同一源库普通重启按持久进度增量续跑 | UID 相同且 `last_applied_seq>0` 时直接续跑，不重做快照、不重建 |
| 运行中源文件被替换也能发现 | 增量循环每轮巡检源 UID，变化即主动失效并重建，无需人工重启 |
| 不得恢复已删标本 | 删除事件 `D` 按序号重放为 `DELETE`；删除后不会再被任何快照/重放路径插回 |
| 不得以旧鉴定覆盖新值 | 日志按 `seq` 升序应用，`INSERT OR REPLACE` 永远以更大序号携带的最新值为准 |
| 一次源事务的改动一起可见，回滚不传播 | 触发器与 DML 同事务：COMMIT 则行与日志原子可见，ROLLBACK 则一起消失，同步器永远看不到回滚内容 |
| 就绪前不供查询 | 副本 `phase != RUNNING`（含 BOOTSTRAP/REBUILDING/STOPPED/ERROR）时查询接口返回 HTTP 503 |
| 增量同步不反复扫描全表 | 只执行 `WHERE seq > ? AND seq <= ?` 的主键索引区间查询（`EXPL QUERY PLAN` 测试断言为 SEARCH 而非 SCAN） |
| 副本数据与进度共同提交 | 每批数据变更、审计流水、`sync_meta` 进度在副本的**同一个事务**中 COMMIT |
| 初次复制中断可重做 | 未完成时 `last_applied_seq=0`，重启清空副本重做 bootstrap（幂等） |
| 就绪后重启按持久进度接续 | 只要来源 UID 相同且 `last_applied_seq > 0`，无论上次 RUNNING/STOPPED/ERROR，都从该序号续跑，不重做快照 |
| 源为本地 SQLite WAL | 源库 `PRAGMA journal_mode=WAL`；副本为独立 SQLite 文件，独立挂载卷 |
| 允许增加辅助表及触发器 | 源库增加 `_change_log`、`_source_identity` 表与 3 个触发器；副本增加 `sync_meta`、`_applied_changes` |

## 架构

```
鉴定员/演示写入器 ──写──> 源库 specimens.db (SQLite WAL)
                            │ specimens 表
                            │ + _change_log (AFTER I/U/D 触发器, AUTOINCREMENT seq)
                            │ + _source_identity (持久唯一 source_uid)
                            │
              只读短事务 SELECT（不阻塞写入）
                            ▼
                    SyncEngine 后台线程
              ┌──────────────────────────────────────┐
              │ 启动对账 reconcile_source:             │
              │   UID 相同 → 按持久进度增量续跑         │
              │   UID 不同 → 置 REBUILDING 清空旧副本  │
              │ bootstrap: 主键分页快照                 │
              │   → 重放日志 [1, w1] → 绑定 UID → RUNNING│
              │ incremental: seq > 已应用序号 + UID 巡检 │
              └──────────────────────────────────────┘
                            │ 每批一个事务：数据 + 来源标识 + 进度一起 COMMIT
                            ▼
                    副本 specimens_replica.db (独立 SQLite)
              specimens / sync_meta(持久进度+source_uid+rebuild_count)
              / _applied_changes(审计)
                            ▲
              Flask 网页（仅 RUNNING 可查，BOOTSTRAP/REBUILDING 等均 503）
```

### 首次复制为什么正确（并发窗口）

1. 记录源端当前最大序号 `w0`；
2. 按主键分页把 `specimens` 快照写入副本（每页独立短事务，无长读锁）；
3. 快照结束再读一次最大序号 `w1`；
4. 重放日志区间 `[1, w1]`（从 1 开始对快照行是幂等覆盖）。

快照期间发生的任何并发提交，其日志序号都落在 `(w0, w1]`，必然被第 4 步覆盖：
插入被补进、删除被删掉（不复活）、更正以新值覆盖旧值。

## 目录结构

```
app/
  source_db.py     源库：WAL、specimens、CDC 表与触发器、持久来源标识、演示写入 API
  replica_db.py    副本：表结构、持久进度 meta（含 source_uid/rebuild_count）、就绪判定、查询
  sync.py          同步引擎：来源对账、bootstrap + 换库重建 + 增量循环，后台线程生命周期
  web.py           Flask 应用与 REST API（启动即对账，查询前旧副本已失效）
  templates/index.html  控制台页面
scripts/
  seed_source.py   播种示例源库（容器入口调用）
  demo_mutator.py  持续演示写入器（周期增/改/撤）
tests/             43 个 pytest 测试（CDC、并发、崩溃、续跑、同序号换库、重建中断、Web E2E）
Dockerfile, docker-compose.yml, entrypoint.sh, wsgi.py
```

## 用 Docker Compose 启动（推荐）

```bash
# 构建并启动 Web + 后台同步（源库自动播种 8 条示例标本）
docker compose up -d --build

# 浏览器打开
#   http://localhost:8000/
```

打开网页后可以：

* 查看同步阶段（INIT/BOOTSTRAP/RUNNING/STOPPED/ERROR）、已应用序号、源端水位、错误；
* 启动/停止同步；按标本号查询副本（未就绪时返回 503）；
* 在页面上直接对**源库**新增 / 更正 / 撤回，观察副本自动追上。

附带一个持续写入器，模拟鉴定员不停操作（可选 profile，不影响主服务）：

```bash
docker compose --profile mutator up -d
docker compose logs -f mutator
```

数据持久化在两个独立命名卷 `source_data`、`replica_data`。删除容器再
`docker compose up -d` 会自动按副本里的持久进度接续；清空 `replica_data` 卷
则会重新做首次复制。

### 换库恢复操作（数据工程师替换源 SQLite 文件）

当鉴定库需要用另一份 SQLite 库替换原路径时（两库变更日志序号可能相同）：

```bash
# 1. 在宿主机/卷上替换源库文件（保留同名路径），新库只需是标准 SQLite；
#    应用启动时会自动补齐 CDC 触发器与 _source_identity 来源标识
docker compose stop web
cp /path/to/restored-specimens.db /var/lib/docker/volumes/<src_volume>/_data/specimens.db
docker compose up -d web
```

系统行为（无需手工清理副本）：

1. Web 进程启动、服务任何查询前先做来源对账，发现 `source_uid` 不同立即把
   副本置为 `REBUILDING` 并清空——因此**首个查询就只能得到 503，绝不会返回
   旧库标本**；
2. 后台线程对新库整体重建（分页快照 + 日志重放），重建期间源库仍可正常
   接收增删改（WAL 只读短事务不阻塞写入）；
3. 重建最后一个事务把新数据、新 `source_uid`、`last_applied_seq` 一起
   持久化，随后副本回到 `RUNNING` 重新开放查询；
4. 若重建中途进程崩溃/被停，重启后阶段仍是 REBUILDING/ERROR（持续 503），
   完整重建成功前旧数据不会被放行。


## 本地（无 Docker）运行

```bash
pip install -r requirements.txt
export SOURCE_DB=./data/source/specimens.db
export REPLICA_DB=./data/replica/specimens_replica.db
python -m scripts.seed_source          # 播种示例源库（可选，Web 启动也会自动播种）
python -m flask --app wsgi:app run --port 8000
# 或：gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 wsgi:app
```

## REST API

| 方法 路径 | 说明 |
| --- | --- |
| `POST /api/sync/start` | 启动后台同步（已在运行则返回 `started:false`） |
| `POST /api/sync/stop` | 请求停止（当前批次完成后退出，phase=STOPPED） |
| `GET  /api/status` | 阶段、`last_applied_seq`、`source_uid`、`rebuild_count`、源端水位、已拷贝行数、错误、线程状态 |
| `GET  /api/specimen/<id>` | 按标本号查副本；未就绪 503，不存在/已撤回 404 |
| `POST /api/demo/insert` | 对源库新增 `{id,taxon,locality}` |
| `POST /api/demo/update` | 对源库更正 `{id,taxon,locality}` |
| `POST /api/demo/delete` | 撤回源记录 `{id}` |
| `GET  /api/source/specimens` | 源库当前标本（演示辅助） |
| `GET  /healthz` | 健康检查 |

## 测试

```bash
pip install pytest
pytest tests/ -v
```

覆盖要点：触发器捕获全部 DML、回滚不传播、已提交序号单调、单事务多行原子可见、
首次复制期间并发增删改的最终一致性、删除不复活、旧值不覆盖新值、中断重做、
重启按持久进度接续且不重做快照、提交点崩溃后数据与进度均不半生效、增量走索引
不全表扫描、同步期间源写入不被阻塞、就绪前 503、Web 端到端增改撤链路，以及：

* **同序号换库**：新库日志序号与旧库重叠（甚至小于旧进度）时，凭 `source_uid`
  识别换库并整体重建，旧库数据不残留、新库标本不漏；
* **换库即失效**：对账返回前副本已是 REBUILDING 且数据清空，Web 首个查询即 503；
  重建完成事务原子绑定新 UID 与进度后才重新开放；
* **重建中断/失败**：故障注入模拟重建崩溃，重启继续重建且全程不放行旧数据，
  同次换库重试不重复计数；
* **正常续跑**：同 UID 普通重启不重建、不重做快照，`rebuild_count` 保持 0；
* **运行中换库**：引擎不重启也能在增量轮询中发现 UID 变化并自动重建；
* **重建期间并发写**：重建过程中源库持续增改，最终副本与源库完全一致。

## 关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SOURCE_DB` | `/data/source/specimens.db` | 源库文件路径 |
| `REPLICA_DB` | `/data/replica/specimens_replica.db` | 副本文件路径 |
| `AUTO_START` | `1` | 容器/应用启动时是否自动开始同步 |
| `POLL_INTERVAL` | `1.0` | 无新变更时的轮询秒数（stop 可立即唤醒） |
| `SNAPSHOT_PAGE_SIZE` | `500` | 首次复制快照每页行数 |
| `CHANGE_BATCH_SIZE` | `500` | 增量每批应用的日志条数 |
| `MUTATOR_INTERVAL` | `3` | 演示写入器操作间隔秒数 |
