# 单域名 A 记录发布系统（FastAPI + SQLite + dnslib）

机房设备管理域名的 A 记录发布：管理服务生成**严格递增修订**，向两台**独立持久化**
权威 DNS 节点整列表下发；运维以两端**实际 DNS 答复**核对一致后再安排切流。
两台节点在**同一端口同时提供 UDP 与 TCP 查询**：地址组变大、UDP 载荷放不下时
按规定置 TC（不返回部分地址），由 TCP 取整组，核验链路自动回退。

## 架构

```
                 ┌──────────────────────────────┐
  Swagger /docs  │  mgmt (FastAPI + SQLite/WAL)  │
  POST /publishes│  修订表 / 节点状态 / 对账循环   │
                 └───────────────┬──────────────┘
                HTTP /publish    │     ▲ heartbeat / report
              ┌──────────────────┴─────┴──────────────────┐
              ▼                                             ▼
     ┌─────────────────┐      UDP/TCP A 查询       ┌─────────────────┐
     │ node-a (dnslib) │◄─────────────────────────►│ node-b (dnslib) │
     │ 独立 SQLite 卷   │   同端口 5353/udp+tcp     │ 独立 SQLite 卷   │
     └─────────────────┘                           └─────────────────┘
        10053/udp+tcp (宿主)                         10054/udp+tcp (宿主)
```

* **修订递增**：每次 `POST /publishes` 在 `BEGIN IMMEDIATE` 事务内写入，
  SQLite 保留锁串行化并发请求，修订号用 AUTOINCREMENT，绝不重号/乱序。
* **只接受较新修订**：节点 `revision <= 已服务修订` 一律 409 拒绝；
  管理端节点状态用 `MAX(served, 上报值)` 钳制，迟到旧上报无法回退版本。
* **地址列表整体生效**：节点在单个事务内替换整行 zone，应答要么全旧、要么全新。
* **UDP 截断策略（绝不给部分地址）**：
  * 无 EDNS 的 UDP 查询遵守经典 **512 字节**上限；
  * 带 EDNS(0) 时按客户端 OPT 通告的接收上限（小于 512 按 512 计）；
  * 完整地址组放不下时答复 **TC=1 且不带任何 A 记录**——截断包永远不会被
    当成"少了几条地址"的完整答复；
  * **TCP 查询始终返回节点当次服务的整组地址**，不受载荷上限影响。
* **核验遇 TC 自动走 TCP**：`GET /verify` 先发 EDNS UDP，任一节点 TC=1 即对该节点
  改用 TCP 取整组再比对；核验在数据库写事务之外做网络 I/O，不占用发布写锁；
  核验期间若出现新修订，旧修订只可能被标记为 `superseded_during_verification`，
  绝不会报告"旧修订已核实"。
* **失联不误报**：`/status` 区分 `synced / syncing / degraded-node-offline`；
  单节点失联时另一节点继续应答，待补节点显示 `waiting-node-recovery`，
  `synced` 恒为 false；恢复后后台对账循环自动补齐。

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

# 运维侧直接核对两端实答（切流前必做；UDP 遇 TC 自动回退 TCP）
python scripts/verify_dns.py device.example.internal 127.0.0.1:10053 127.0.0.1:10054
```

## 截断与 TCP 行为

| 查询方式 | 地址组放得下 | 地址组放不下 |
| --- | --- | --- |
| UDP 无 EDNS | 完整答复（≤512 字节，TC=0） | TC=1，**0 条 A 记录**（不给部分地址） |
| UDP 带 EDNS(0) | 完整答复（≤通告的 UDP 尺寸） | TC=1，**0 条 A 记录**，客户端应转 TCP |
| TCP | 完整整组地址 | 完整整组地址（65535 字节线长上限内） |

`/verify` 的每个节点结果包含 `transport`（`udp`/`tcp`）、`udp_truncated` 与
`tcp_fallback` 字段，可在 Swagger（`/docs`）上直接试用核对。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/publishes` | 提交完整地址列表，生成新修订（201） |
| GET  | `/status` | 最新修订、各节点在线状态/已服务版本/待补状态、总体结论 |
| GET  | `/verify` | 用两端实际 DNS 答复核对最新修订；TC 自动回退 TCP，核验期间被新修订取代时显式标记 |
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
迟到旧修订 409 拒绝、并发发布修订连续、节点重启持久化、输入校验、
大列表 512/EDNS 截断与 TCP 整组、`/verify` 的 TC 回退、核验与交错发布的竞态保护。
