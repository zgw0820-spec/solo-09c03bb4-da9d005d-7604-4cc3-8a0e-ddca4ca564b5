#!/usr/bin/env python3
"""端到端自测：在本机模拟 Compose 拓扑（管理服务 + 两个独立持久化节点）。

覆盖:
  1. 发布与双节点同步、整列表原子生效、实答一致（小列表 UDP 不截断、TCP 同端口可查）
  2. 单节点失联：另一节点继续应答；状态显示待补，绝不误报 synced
  3. 节点恢复后自动补齐到最新修订
  4. 迟到的旧修订推送被节点拒绝(409)，节点/同步结论不回退
  5. 并发发布：修订严格递增、无重号
  6. 节点重启：独立 SQLite 持久化，服务版本不丢
  7. 非法输入校验
  8. 大列表：无 EDNS 512 字节/EDNS 通告上限，超限 TC=1 且零部分答复；TCP 整组；
     /verify 遇 TC 自动 TCP 核对
  9. 核验与交错发布：旧修订不得误报已核实，核验不占用发布写锁

用法: python scripts/selftest.py [python可执行路径]
"""
from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from dnslib import DNSRecord, EDNS0

ROOT = Path(__file__).resolve().parent.parent
PY = sys.argv[1] if len(sys.argv) > 1 else sys.executable

DOMAIN = "device.example.internal"
MGMT_PORT = 18000
NODE_SPECS = [
    {"id": "node-a", "udp": 10053, "http": 18053},
    {"id": "node-b", "udp": 10054, "http": 18054},
]

PASS, FAIL = 0, 0
procs: dict[str, subprocess.Popen] = {}
tmpdir: Path | None = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, path: str, body: dict | None = None, port: int = MGMT_PORT,
         timeout: float = 8.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _question(domain: str, edns_size: int | None = None) -> bytes:
    q = DNSRecord.question(domain, qtype="A")
    if edns_size is not None:
        q.add_ar(EDNS0(udp_len=edns_size, version=0))
    return q.pack()


def dns_query(port: int, domain: str = DOMAIN, timeout: float = 3.0,
              edns_size: int | None = None, tcp: bool = False) -> dict:
    """Return parsed reply details over UDP (optionally EDNS) or TCP."""
    packet = _question(domain, edns_size)
    if not tcp:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(timeout)
        try:
            s.sendto(packet, ("127.0.0.1", port))
            data, _ = s.recvfrom(65535)
        finally:
            s.close()
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect(("127.0.0.1", port))
            s.sendall(struct.pack("!H", len(packet)) + packet)
            header = b""
            while len(header) < 2:
                part = s.recv(2 - len(header))
                if not part:
                    raise ConnectionError("TCP 连接提前关闭")
                header += part
            (msg_len,) = struct.unpack("!H", header)
            chunks, got = [], 0
            while got < msg_len:
                part = s.recv(min(65535, msg_len - got))
                if not part:
                    raise ConnectionError("TCP 应答不完整")
                chunks.append(part)
                got += len(part)
            data = b"".join(chunks)
        finally:
            s.close()
    reply = DNSRecord.parse(data)
    return {
        "tc": bool(reply.header.tc),
        "aa": bool(reply.header.aa),
        "answers": {str(rr.rdata) for rr in reply.rr},
        "answer_count": len(reply.rr),
        "raw_len": len(data),
    }


def dns_answers(port: int, domain: str = DOMAIN, timeout: float = 3.0) -> set[str]:
    return dns_query(port, domain, timeout)["answers"]


def wait_status(cond, timeout: float = 20.0, interval: float = 0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        _, last = http("GET", "/status")
        if cond(last):
            return last
        time.sleep(interval)
    return last


def start_node(spec: dict) -> subprocess.Popen:
    assert tmpdir is not None
    data_dir = tmpdir / spec["id"]
    data_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.update({
        "NODE_ID": spec["id"],
        "NODE_DB_PATH": str(data_dir / "node.db"),
        "MGMT_BASE_URL": f"http://127.0.0.1:{MGMT_PORT}",
        "DNS_PORT": str(spec["udp"]),
        "HTTP_PORT": str(spec["http"]),
        "HEARTBEAT_INTERVAL": "2",
    })
    log = open(tmpdir / f"{spec['id']}.log", "w")
    p = subprocess.Popen(
        [PY, "-m", "dnsnode.node"], cwd=ROOT, env=env,
        stdout=log, stderr=subprocess.STDOUT, text=True,
    )
    procs[spec["id"]] = p
    return p


def start_mgmt() -> subprocess.Popen:
    assert tmpdir is not None
    env = dict(os.environ)
    nodes_json = json.dumps([
        {"id": s["id"],
         "control_url": f"http://127.0.0.1:{s['http']}",
         "dns_host": "127.0.0.1",
         "dns_port": s["udp"]} for s in NODE_SPECS
    ])
    env.update({
        "MANAGED_DOMAIN": DOMAIN,
        "MGMT_DB_PATH": str(tmpdir / "management.db"),
        "OFFLINE_AFTER_SECONDS": "5",
        "RECONCILE_INTERVAL": "1",
        "NODES": nodes_json,
    })
    log = open(tmpdir / "mgmt.log", "w")
    p = subprocess.Popen(
        [PY, "-m", "uvicorn", "management.app:app",
         "--host", "127.0.0.1", "--port", str(MGMT_PORT)],
        cwd=ROOT, env=env,
        stdout=log, stderr=subprocess.STDOUT, text=True,
    )
    procs["mgmt"] = p
    return p


def wait_http(url_port: int, path: str, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{url_port}{path}", timeout=2
            ) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.4)
    return False


def stop(name: str) -> None:
    p = procs.pop(name, None)
    if p and p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            p.kill()


def teardown() -> None:
    for name in list(procs):
        stop(name)


# --------------------------------------------------------------------------- #
# Test scenarios
# --------------------------------------------------------------------------- #
def scenario_basic() -> None:
    print("\n=== 1. 发布/同步/整列表生效/实答一致 ===")
    code, body = http("POST", "/publishes", {"addresses": ["10.0.0.1", "10.0.0.2"]})
    check("首次发布返回201且修订号=1", code == 201 and body["revision"] == 1, str(body))

    st = wait_status(lambda s: s["overall_status"] == "synced")
    check("双节点同步完成 overall=synced", st["overall_status"] == "synced", json.dumps(st))
    check("两节点 served_revision 均为1",
          all(n["served_revision"] == 1 for n in st["nodes"]), json.dumps(st["nodes"]))

    a = dns_answers(NODE_SPECS[0]["udp"])
    b = dns_answers(NODE_SPECS[1]["udp"])
    check("节点A 实答=整列表", a == {"10.0.0.1", "10.0.0.2"}, str(a))
    check("节点B 实答=整列表", b == {"10.0.0.1", "10.0.0.2"}, str(b))
    check("两端实际答复一致", a == b)
    # 小列表在 512 字节内：UDP 不得置 TC，TCP 同端口同样可查且结果一致。
    small = dns_query(NODE_SPECS[0]["udp"])
    check("小列表 UDP 无截断(TC=0)且带完整答复",
          not small["tc"] and small["answers"] == a and small["raw_len"] <= 512,
          str(small))
    small_tcp = dns_query(NODE_SPECS[0]["udp"], tcp=True)
    check("同端口 TCP 查询返回整组小列表",
          not small_tcp["tc"] and small_tcp["answers"] == a, str(small_tcp))

    code, ver = http("GET", "/verify")
    check("/verify 全部匹配最新修订", code == 200 and ver["all_nodes_match"], json.dumps(ver))

    # 第二次发布：整列表替换，旧地址必须全部消失
    code, body = http("POST", "/publishes", {"addresses": ["10.0.0.3"]})
    check("第二次发布修订号=2(严格递增)", code == 201 and body["revision"] == 2, str(body))
    wait_status(lambda s: s["overall_status"] == "synced")
    a2 = dns_answers(NODE_SPECS[0]["udp"])
    b2 = dns_answers(NODE_SPECS[1]["udp"])
    check("整列表替换: 新地址生效", a2 == {"10.0.0.3"}, str(a2))
    check("整列表替换: 旧地址全部消失", "10.0.0.1" not in a2 and "10.0.0.2" not in a2, str(a2))
    check("替换后两端仍一致", a2 == b2)


def scenario_outage() -> None:
    print("\n=== 2. 单节点失联: 另一节点继续应答, 不误报 synced ===")
    stop("node-b")
    time.sleep(1)

    code, body = http("POST", "/publishes", {"addresses": ["10.0.1.10", "10.0.1.11"]})
    rev3 = body["revision"]
    check("失联期间发布成功, 修订号=3", code == 201 and rev3 == 3, str(body))

    # 存活节点应在数个对账周期内追上
    deadline = time.time() + 15
    st = None
    while time.time() < deadline:
        _, st = http("GET", "/status")
        na = next(n for n in st["nodes"] if n["node_id"] == "node-a")
        if na["served_revision"] == 3:
            break
        time.sleep(0.5)
    na = next(n for n in st["nodes"] if n["node_id"] == "node-a")
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("存活节点A 追上修订3", na["served_revision"] == 3, json.dumps(na))
    check("存活节点A 继续提供 DNS 实答",
          dns_answers(NODE_SPECS[0]["udp"]) == {"10.0.1.10", "10.0.1.11"})

    # 等待离线判定窗口
    time.sleep(6)
    _, st = http("GET", "/status")
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("失联节点B 标记 offline", st["overall_status"] == "degraded-node-offline"
          and not nb["online"], json.dumps(st))
    check("synced=false, 绝不误报同步完成", st["synced"] is False)
    check("待补状态: node-b waiting-node-recovery",
          nb["catch_up_status"] == "waiting-node-recovery", json.dumps(nb))
    check("pending_nodes 包含 node-b", "node-b" in st["pending_nodes"])

    code, ver = http("GET", "/verify")
    check("失联期间 /verify 明确不通过(不假装一致)",
          ver["all_nodes_match"] is False
          and any(not n["matches_latest_revision"] for n in ver["nodes"]),
          json.dumps(ver))


def scenario_recovery() -> None:
    print("\n=== 3. 节点恢复后自动补齐 ===")
    start_node(NODE_SPECS[1])
    check("节点B 控制面恢复", wait_http(NODE_SPECS[1]["http"], "/health"))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=25)
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("恢复后 node-b 自动补齐到修订3",
          st["overall_status"] == "synced" and nb["served_revision"] == 3, json.dumps(st))
    check("补齐后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"]))


def scenario_stale() -> None:
    print("\n=== 4. 迟到旧修订被拒(409), 不回退 ===")
    # 直接向节点重放一个旧 revision=1 的发布
    spec = NODE_SPECS[0]
    req = urllib.request.Request(
        f"http://127.0.0.1:{spec['http']}/publish",
        data=json.dumps({"revision": 1, "domain": DOMAIN,
                         "addresses": ["1.2.3.4"]}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        code, raw = 0, {}
    except urllib.error.HTTPError as e:
        code, raw = e.code, json.loads(e.read())
    check("旧修订推送被节点 409 拒绝", code == 409 and raw["status"] == "rejected", str(raw))
    check("节点仍服务当前修订(不回退)", raw.get("served_revision") == 3, str(raw))
    check("节点实答未被旧数据污染",
          dns_answers(spec["udp"]) == {"10.0.1.10", "10.0.1.11"})

    # 管理端状态同样不回退
    _, st = http("GET", "/status")
    check("管理端同步结论仍为 synced", st["overall_status"] == "synced", json.dumps(st))

    # 模拟迟到的旧上报
    code, _ = http("POST", "/internal/nodes/node-a/report", {"served_revision": 1})
    _, st = http("GET", "/status")
    na = next(n for n in st["nodes"] if n["node_id"] == "node-a")
    check("迟到旧上报被 MAX 钳制, served 不回退",
          code == 200 and na["served_revision"] == 3, json.dumps(na))


def scenario_interleave() -> None:
    print("\n=== 4b. 交错/乱序下发: 跳号接受、随后旧号拒绝 ===")
    spec = NODE_SPECS[0]
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]

    def direct_push(rev: int, addr: str):
        req = urllib.request.Request(
            f"http://127.0.0.1:{spec['http']}/publish",
            data=json.dumps({"revision": rev, "domain": DOMAIN,
                             "addresses": [addr]}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    # 乱序先到一个跳号的较新修订 -> 必须接受
    code, body = direct_push(base + 3, "192.0.2.20")
    check("跳号的较新修订(base+3)被接受", code == 200
          and body["served_revision"] == base + 3, str(body))
    check("节点实答立即切换为跳号修订数据",
          dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 迟到的中间修订(base+1) -> 必须 409, 数据不回退
    code, body = direct_push(base + 1, "192.0.2.19")
    check("迟到的中间修订被 409 拒绝", code == 409
          and body["status"] == "rejected", str(body))
    check("拒绝后实答不回退", dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 同号重放 -> 同样拒绝(幂等保护)
    code, _ = direct_push(base + 3, "192.0.2.99")
    check("同号重放被 409 拒绝", code == 409)
    check("重放不改变实答", dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 管理端连续发布: 落后的 node-b 逐版追上; node-a 对旧版持续 409,
    # 直到管理端修订号超过 base+3 后两节点重新收敛。
    last_rev = base
    for i in range(1, 5):
        code, body = http("POST", "/publishes",
                          {"addresses": [f"198.51.100.{i}"]})
        check(f"管理端第{i}次发布成功(修订 {body['revision']})", code == 201)
        last_rev = body["revision"]
    check("管理端修订号已超过注入的跳号修订", last_rev == base + 4)
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=30)
    check("交错后双节点最终收敛到管理端最新修订",
          all(n["served_revision"] == last_rev for n in st["nodes"]),
          json.dumps(st["nodes"]))
    check("收敛后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"])
          == {"198.51.100.4"})


def scenario_truncation() -> None:
    print("\n=== 8. 大列表: UDP 512/EDNS 上限与 TC, TCP 整组, /verify 自动回退 ===")
    # 40 个 A 记录在该域名下约 660 字节: 超过无 EDNS 的 512, 但 EDNS 4096 放得下。
    medium = [f"10.20.{i // 256}.{i % 256}" for i in range(40)]
    code, body = http("POST", "/publishes", {"addresses": medium})
    check("发布 40 地址中等列表成功", code == 201, str(body))
    wait_status(lambda s: s["overall_status"] == "synced", timeout=25)

    for spec in NODE_SPECS:
        port = spec["udp"]
        no_edns = dns_query(port, edns_size=None)
        check(f"{spec['id']} 无 EDNS: TC=1 且不返回部分地址(0 条答复)",
              no_edns["tc"] and no_edns["answer_count"] == 0
              and no_edns["raw_len"] <= 512, str(no_edns))
        with_edns = dns_query(port, edns_size=4096)
        check(f"{spec['id']} EDNS(4096): 不截断且返回整组 40 条",
              not with_edns["tc"] and with_edns["answers"] == set(medium)
              and with_edns["answer_count"] == 40, str(with_edns))
        tight = dns_query(port, edns_size=512)
        check(f"{spec['id']} EDNS(512) 同样放不下: TC=1、零答复",
              tight["tc"] and tight["answer_count"] == 0, str(tight))
        via_tcp = dns_query(port, tcp=True)
        check(f"{spec['id']} TCP 查询返回当次服务的整组地址",
              not via_tcp["tc"] and via_tcp["answers"] == set(medium)
              and via_tcp["answer_count"] == 40, str(via_tcp))

    code, ver = http("GET", "/verify")
    check("中等列表 /verify 经 UDP(EDNS) 直接核对通过",
          code == 200 and ver["all_nodes_match"]
          and all(n["transport"] == "udp" and not n["tcp_fallback"]
                  for n in ver["nodes"]), json.dumps(ver))

    # 300 个 A 记录约 4.8KB : 超过 EDNS 4096 通告上限, UDP 必 TC, 核验须走 TCP。
    large = [f"10.30.{i // 256}.{i % 256}" for i in range(300)]
    code, body = http("POST", "/publishes", {"addresses": large})
    check("发布 300 地址大列表成功", code == 201, str(body))
    wait_status(lambda s: s["overall_status"] == "synced", timeout=30)

    for spec in NODE_SPECS:
        port = spec["udp"]
        u = dns_query(port, edns_size=4096)
        check(f"{spec['id']} 大列表 EDNS(4096) 仍超限: TC=1、零部分答复",
              u["tc"] and u["answer_count"] == 0, str(u))
        t = dns_query(port, tcp=True)
        check(f"{spec['id']} 大列表 TCP 整组 300 条、AA=1",
              not t["tc"] and t["aa"] and t["answers"] == set(large)
              and t["answer_count"] == 300, str(t))

    code, ver = http("GET", "/verify")
    check("大列表 /verify 遇 TC 自动 TCP 核对且全部匹配",
          code == 200 and ver["all_nodes_match"]
          and all(n["udp_truncated"] and n["tcp_fallback"]
                  and n["transport"] == "tcp"
                  and n["answer_count"] == 300 for n in ver["nodes"]),
          json.dumps(ver))
    check("/verify 未把截断当成少地址",
          all(not n["matches_latest_revision"] or n["answers"] == sorted(large)
              for n in ver["nodes"]))

    # 运维脚本同样应在 TC 后回退 TCP 并判定一致。
    ops = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_dns.py"), DOMAIN,
         f"127.0.0.1:{NODE_SPECS[0]['udp']}",
         f"127.0.0.1:{NODE_SPECS[1]['udp']}"],
        cwd=ROOT, capture_output=True, text=True, timeout=30,
    )
    check("verify_dns.py 大列表 TC 后 TCP 核对退出码 0",
          ops.returncode == 0, ops.stdout + ops.stderr)


def scenario_verify_vs_publish() -> None:
    print("\n=== 9. 核验与交错发布: 旧修订不得误报已核实, 不阻塞发布写锁 ===")
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]

    # 9a. 确定性竞态: 探针进程内打补丁撑开 /verify 探测窗口, 强制交错发布落入。
    # 探针起独立端口的 uvicorn, 但复用同一管理库与 NODES 配置。
    probe_env = dict(os.environ)
    probe_env.update({
        "RACE_MGMT_PORT": "18099",
        "MANAGED_DOMAIN": DOMAIN,
        "MGMT_DB_PATH": str(tmpdir / "management.db"),
        "OFFLINE_AFTER_SECONDS": "5",
        "RECONCILE_INTERVAL": "1",
        "NODES": json.dumps([
            {"id": s["id"],
             "control_url": f"http://127.0.0.1:{s['http']}",
             "dns_host": "127.0.0.1",
             "dns_port": s["udp"]} for s in NODE_SPECS
        ]),
    })
    probe = subprocess.run(
        [PY, str(ROOT / "scripts" / "force_verify_race.py")],
        cwd=ROOT, env=probe_env, capture_output=True, text=True, timeout=60,
    )
    forced_superseded = probe.returncode == 0
    print(probe.stdout.strip())
    if probe.stderr.strip():
        print(probe.stderr.strip()[-1000:])
    check("确定性探针: 核验窗口内发布后旧修订被标记 superseded、不报已核实",
          forced_superseded, probe.stdout + probe.stderr)
    _, _post = http("GET", "/status")
    post_probe_rev = _post["latest_revision"]["revision"]

    observed_superseded = threading.Event()
    stop = threading.Event()
    errors: list[str] = []

    def verify_worker() -> None:
        # 大列表让单次核验(含可能的 TCP 回退)耗时更长, 提高交错窗口。
        while not stop.is_set():
            try:
                code, ver = http("GET", "/verify", timeout=15)
                if code != 200:
                    if len(errors) < 10:
                        errors.append(f"verify http {code}: {ver}")
                    time.sleep(0.1)
                    continue
                # 不变量 1: 一旦核验期间被新发布取代, 绝不能宣称旧修订已核实。
                # all_nodes_match 只表示两端答复与"期望修订"一致(节点可能还
                # 没切到新修订), 真正的核实结论是 verified_revision_current。
                if ver["superseded_during_verification"]:
                    observed_superseded.set()
                    if ver["verified_revision_current"]:
                        errors.append(
                            f"被取代的旧修订 {ver['expected_revision']} 仍被报告核实"
                        )
                    if ver["current_revision"] <= ver["expected_revision"]:
                        errors.append("superseded 标记与修订号矛盾")
                else:
                    # 未被取代时, 核实结论必须与逐节点匹配结果一致。
                    if ver["verified_revision_current"] != ver["all_nodes_match"]:
                        errors.append("未被取代但核实结论与节点匹配结果矛盾")
                # 不变量 2: 只要结论是"已核实", 期望修订必须仍是当前修订。
                if ver["verified_revision_current"] and (
                    ver["current_revision"] != ver["expected_revision"]
                    or ver["superseded_during_verification"]
                ):
                    errors.append("verified_revision_current 与取代状态矛盾")
            except Exception as exc:  # 网络抖动可接受, 记录即可
                if len(errors) < 10:
                    errors.append(f"verify exc: {exc!r}")
            time.sleep(0.1)  # 避免忙转打爆接口

    worker = threading.Thread(target=verify_worker, daemon=True)
    worker.start()

    # 主线程持续发布(小列表, UDP 即可核对), 每次发布计时以暴露写锁被长占。
    publish_latencies = []
    made = 0
    deadline = time.time() + 18
    while time.time() < deadline:
        t0 = time.time()
        code, body = http("POST", "/publishes",
                          {"addresses": [f"172.30.{made // 256}.{made % 256}"]})
        publish_latencies.append(time.time() - t0)
        if code != 201:
            errors.append(f"publish failed {code}: {body}")
            break
        made += 1
        time.sleep(0.35)
    stop.set()
    worker.join(timeout=5)

    check("交错期间所有发布均 201, 修订无丢失",
          made >= 5 and not any("publish failed" in e for e in errors),
          f"made={made} errors={errors}")
    _, st = http("GET", "/status")
    # 以探针结束后的真实修订号为基线, 不假设探针是否成功提交了发布。
    check("修订号恰好增加发布次数(写锁未被核验破坏)",
          st["latest_revision"]["revision"] == post_probe_rev + made,
          f"{st['latest_revision']['revision']} != {post_probe_rev + made}")
    expected_rev = post_probe_rev + made
    check("至少一次核验观察到修订被取代并正确标记",
          observed_superseded.is_set() or forced_superseded,
          str(errors))
    check("无旧修订误报核实/状态矛盾", not errors, str(errors[:5]))
    check("发布写锁未被核验长时间占用(单次发布 < 3s)",
          max(publish_latencies, default=0) < 3.0,
          f"max={max(publish_latencies, default=0):.2f}s")

    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=25)
    check("交错发布后双节点重新收敛",
          st["overall_status"] == "synced"
          and all(n["served_revision"] == expected_rev for n in st["nodes"]),
          json.dumps(st["nodes"]))
    code, ver = http("GET", "/verify")
    check("收敛后重新核验最新修订通过(小列表走 UDP)",
          code == 200 and ver["all_nodes_match"]
          and not ver["superseded_during_verification"]
          and ver["verified_revision_current"]
          and all(n["transport"] == "udp" for n in ver["nodes"]),
          json.dumps(ver))


def scenario_concurrent() -> None:
    print("\n=== 5. 并发发布: 修订严格递增无重号 ===")
    import concurrent.futures
    payloads = [[f"172.16.0.{i}"] for i in range(1, 9)]
    # 基线必须在并发发布之前取，否则拿到的是风暴后的最新修订号
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda p: http("POST", "/publishes", {"addresses": p}), payloads
        ))
    codes = [c for c, _ in results]
    revs = sorted(b["revision"] for _, b in results)
    check("8 个并发发布全部 201", all(c == 201 for c in codes), str(codes))
    check("修订号唯一且连续递增(base+1..base+8)",
          revs == list(range(base + 1, base + 9)), str(revs))

    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=30)
    check("并发风暴后最终双节点收敛到最新修订",
          st["overall_status"] == "synced"
          and all(n["served_revision"] == base + 8 for n in st["nodes"]), json.dumps(st))
    check("收敛后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"]))


def scenario_persistence() -> None:
    print("\n=== 6. 节点重启: 独立 SQLite 持久化 ===")
    for spec in NODE_SPECS:
        stop(spec["id"])
    for spec in NODE_SPECS:
        start_node(spec)
    for spec in NODE_SPECS:
        check(f"{spec['id']} 重启后健康", wait_http(spec["http"], "/health", timeout=15))
    # 节点未收到任何新发布也应凭本地库继续应答最新数据
    time.sleep(2)
    a = dns_answers(NODE_SPECS[0]["udp"])
    b = dns_answers(NODE_SPECS[1]["udp"])
    _, st0 = http("GET", "/status")
    expected = set(st0["latest_revision"]["addresses"])
    check("重启后节点A 凭持久化继续应答", a == expected, str(a))
    check("重启后节点B 凭持久化继续应答", b == expected, str(b))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=20)
    check("心跳恢复后管理端重新判定 synced",
          st["overall_status"] == "synced", json.dumps(st))


def scenario_validation() -> None:
    print("\n=== 7. 输入校验 ===")
    code, _ = http("POST", "/publishes", {"addresses": []})
    check("空地址列表拒绝", code == 422, str(code))
    code, _ = http("POST", "/publishes", {"addresses": ["10.0.0.999"]})
    check("非法 IPv4 拒绝", code == 422, str(code))
    code, _ = http("POST", "/publishes", {"addresses": ["10.0.0.1", "10.0.0.1"]})
    check("重复地址拒绝", code == 422, str(code))


def main() -> int:
    global tmpdir
    tmpdir = Path(tempfile.mkdtemp(prefix="dns-pub-test-"))
    print(f"临时数据目录: {tmpdir}")
    try:
        start_mgmt()
        if not wait_http(MGMT_PORT, "/status", timeout=20):
            print("管理服务启动失败")
            log_path = tmpdir / "mgmt.log" if tmpdir else None
            if log_path and log_path.exists():
                print(log_path.read_text()[-4000:])
            return 2
        for spec in NODE_SPECS:
            start_node(spec)
            if not wait_http(spec["http"], "/health", timeout=15):
                print(f"{spec['id']} 启动失败")
                return 2

        scenario_basic()
        scenario_outage()
        scenario_recovery()
        scenario_stale()
        scenario_interleave()
        scenario_concurrent()
        scenario_truncation()
        scenario_verify_vs_publish()
        scenario_persistence()
        scenario_validation()
    finally:
        teardown()

    print(f"\n==== 结果: {PASS} 通过, {FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
