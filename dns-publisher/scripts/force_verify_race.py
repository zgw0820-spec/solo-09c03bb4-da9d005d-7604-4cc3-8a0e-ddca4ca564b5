#!/usr/bin/env python3
"""确定性竞态探针：强制让 /verify 的节点探测窗口内出现一次新发布。

复用 selftest.py 启动的本地拓扑的同一套环境变量（MGMT_DB_PATH、NODES、
MANAGED_DOMAIN 等），在本进程内起一个独立端口的 uvicorn 服务，导入
management.app 后给 _verify_node 打补丁：首次调用阻塞 1.5 秒，期间另一个
线程对 /publishes 发起新修订。核验结束后必须报告
superseded_during_verification=true 且 verified_revision_current=false。

用法: RACE_MGMT_PORT=<端口> python scripts/force_verify_race.py
成功退出码 0，断言失败退出码 1。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PORT = int(os.environ.get("RACE_MGMT_PORT", "18099"))
BASE = f"http://127.0.0.1:{PORT}"

import uvicorn  # noqa: E402
from management import app as mgmt_app  # noqa: E402
from management import db  # noqa: E402

# --------------------------------------------------------------------------- #
# 确定性地撑开核验的网络探测窗口
# --------------------------------------------------------------------------- #
original_verify_node = mgmt_app._verify_node
gate = threading.Event()


def slow_verify_node(node, rev):  # noqa: ANN001
    if not gate.is_set():
        gate.set()
        time.sleep(1.5)  # 固定 expected 修订后、出结果前, 让交错发布落入
    return original_verify_node(node, rev)


mgmt_app._verify_node = slow_verify_node


def http(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.status, json.loads(r.read() or b"{}")


def wait_ready(timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(BASE + "/status", timeout=2):
                return True
        except Exception:
            time.sleep(0.3)
    return False


def main() -> int:
    db.init_db()
    config = uvicorn.Config(
        mgmt_app.app, host="127.0.0.1", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    if not wait_ready():
        print("FAIL: 探针内 uvicorn 未就绪")
        return 1

    try:
        _, before = http("GET", "/status")
        base_rev = before["latest_revision"]["revision"]

        result: dict = {}
        errors: list[str] = []

        def verify_call() -> None:
            try:
                _, body = http("GET", "/verify")
                result.update(body)
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))

        t = threading.Thread(target=verify_call)
        t.start()
        if not gate.wait(5):
            print("FAIL: 核验探针未在 5 秒内进入")
            return 1
        # 核验已固定 expected_revision 且正在探测节点——此刻发布新修订。
        code, pub = http("POST", "/publishes",
                         {"addresses": ["192.0.2.100", "192.0.2.101"]})
        if code != 201:
            print(f"FAIL: 交错发布失败 {code} {pub}")
            return 1
        t.join(timeout=20)

        if errors:
            print(f"FAIL: /verify 调用异常 {errors}")
            return 1
        checks = {
            "expected 修订是被取代的旧修订":
                result.get("expected_revision") == base_rev,
            "current 修订等于交错发布后的新修订":
                result.get("current_revision") == pub["revision"],
            "superseded_during_verification 为真":
                result.get("superseded_during_verification") is True,
            "旧修订不得报已核实(verified_revision_current=false)":
                result.get("verified_revision_current") is False,
            "返回了中文提示":
                bool(result.get("message")),
        }
        ok = True
        for name, cond in checks.items():
            print(f"[{'PASS' if cond else 'FAIL'}] {name}")
            ok = ok and cond
        print("响应字段:", json.dumps({k: result.get(k) for k in (
            "expected_revision", "current_revision",
            "superseded_during_verification", "verified_revision_current",
            "all_nodes_match", "message")}, ensure_ascii=False))
        return 0 if ok else 1
    finally:
        server.should_exit = True
        thread.join(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
