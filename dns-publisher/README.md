# 单域名 A 记录发布系统（FastAPI + SQLite + dnslib）

机房设备管理域名的 A 记录发布：管理服务生成**严格递增修订**，向两台**独立持久化**的
UDP 权威 DNS 节点整列表下发；运维以两端**实际 DNS 答复**核对一致后再安排切流。

## 架构

```
                 ┌──────────────────────────────┐
  Swagger /docs  │  mgmt (FastAPI + SQLite/WAL)  │
  POST /publishes│  修订表 / 节点状态 / 对账循环   │
                 └───────────────┬──────────────┘
                HTTP /publish    │     ▲ heartbeat / report
              ┌──────────────────┴─────┴──────────────────┐
              ▼                                             ▼
     ┌─────────────────┐                           ┌─────────────────┐
     │ node-a (dnslib) │◄────── UDP A 查询 ───────►│ node-b (dnslib) │
     │ 独立 SQLite 卷   │                           │ 独立 SQLite 卷   │
     └─────────────────┘                           └─────────────────┘
        10053/udp (宿主)                               10054/udp (宿主)
```

* **修订递增**：每次 `POST /publishes` 在 `BEGIN IMMEDIATE` 事务内写入，
  SQLite 保留锁串行化并发请求，修订号用 AUTOINCREMENT，绝不重号/乱序。
* **只接受较新修订**：节点 `revision <= 已服务修订` 一律 409 拒绝；
  管理端节点状态用 `MAX(served, 上报值)` 钳制，迟到旧上报无法回退版本。
* **地址列表整体生效**：节点在单个事务内替换整行 zone，UDP 应答要么全旧、要么全新。
* **失联不误报**：`/status` 区分 `synced / syncing / degraded-node-offline`；
  单节点失联时另一节点继续应答，待补节点显示 `waiting-node-recovery`，
  `synced` 恒为 false；恢复后后台对账循环自动补齐。
* **实答核对**：`GET /verify` 与 `scripts/verify_dns.py` 都直接发 UDP 查询，
  以两端真实答复比对最新修订，而不是只看控制面状态。

## 启动（Docker Compose）

```bash
docker compose up -d --build
# Swagger:
open http://localhost:8000/docs
```

发布与核对：

```bash
curl -X POST http://localhost:8000/publishes -H 'Content-Type: application/json' \
  -d '{"addresses":["10.10.1.11","10.10.1.12"]}'
curl http://localhost:8000/status
curl http://localhost:8000/verify

# 运维侧直接核对两端 UDP 实答（切流前必做）
python scripts/verify_dns.py device.example.internal 127.0.0.1:10053 127.0.0.1:10054
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/publishes` | 提交完整地址列表，生成新修订（201） |
| GET  | `/status` | 最新修订、各节点在线状态/已服务版本/待补状态、总体结论 |
| GET  | `/verify` | 用两端实际 UDP DNS 答复核对最新修订 |
| GET  | `/revisions` | 修订历史 |
| POST | `/reconcile` | 手动触发一次对账下发 |
| GET  | `/docs` | Swagger UI |

## 本地无 Docker 自测

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/selftest.py
```

自测覆盖：发布同步与实答一致、单节点失联（不误报）、恢复补齐、
迟到旧修订 409 拒绝、并发发布修订连续、节点重启持久化、输入校验。
