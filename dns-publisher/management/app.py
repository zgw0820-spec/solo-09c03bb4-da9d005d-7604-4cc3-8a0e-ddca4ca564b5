"""FastAPI management service for the single-domain A-record publisher.

Responsibilities
----------------
* Accept address-list submissions (Swagger / OpenAPI at ``/docs``) and turn
  each one into a new strictly increasing revision in SQLite.
* Push every revision to both independent DNS nodes and keep reconciling
  until each node serves the newest revision.
* Report per-node served revision, online/offline state and catch-up status,
  and an aggregate conclusion that can *never* read "synced" while a node is
  offline or behind.
* Verify the latest revision against the real DNS answers of both nodes.
  Verification speaks UDP first and, when a node sets TC (the answer group
  does not fit its UDP payload budget), retries over TCP so a truncated
  response is never mistaken for a missing-address mismatch. All network I/O
  happens outside SQLite write transactions, so verification cannot block a
  concurrent publish; if a newer revision appears while verification is in
  flight, the stale revision is explicitly reported as superseded rather
  than "verified".
"""
from __future__ import annotations

import asyncio
import json
import os
import struct
import time
from contextlib import asynccontextmanager
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from dnslib import EDNS0, QTYPE, RCODE, DNSRecord
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from . import db

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
DOMAIN = os.environ.get("MANAGED_DOMAIN", "device.example.internal").rstrip(".")

def _load_nodes() -> list[dict[str, Any]]:
    raw = os.environ.get(
        "NODES",
        '[{"id":"node-a","control_url":"http://127.0.0.1:8053",'
        '"dns_host":"127.0.0.1","dns_port":10053},'
        '{"id":"node-b","control_url":"http://127.0.0.1:8054",'
        '"dns_host":"127.0.0.1","dns_port":10054}]',
    )
    nodes = json.loads(raw)
    for n in nodes:
        n.setdefault("dns_port", 5353)
    return nodes


NODES: list[dict[str, Any]] = _load_nodes()
NODE_IDS = [n["id"] for n in NODES]
RECONCILE_INTERVAL = float(os.environ.get("RECONCILE_INTERVAL", "3"))
PUSH_TIMEOUT = float(os.environ.get("PUSH_TIMEOUT", "4"))


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class PublishRequest(BaseModel):
    addresses: list[str] = Field(
        ...,
        min_length=1,
        description="Complete A-record address list. The whole list replaces "
                    "the previous one atomically on every node.",
        examples=[["10.10.1.11", "10.10.1.12"]],
    )

    @field_validator("addresses")
    @classmethod
    def _valid_ips(cls, v: list[str]) -> list[str]:
        seen: set[str] = set()
        for ip in v:
            parts = ip.split(".")
            if len(parts) != 4 or not all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                raise ValueError(f"{ip!r} is not a valid IPv4 address")
            if ip in seen:
                raise ValueError(f"duplicate address {ip!r}")
            seen.add(ip)
        return v


class ReportRequest(BaseModel):
    served_revision: int = Field(..., ge=0)


# --------------------------------------------------------------------------- #
# Node interaction (blocking urllib, executed in worker threads)
# --------------------------------------------------------------------------- #
def _http_post(url: str, payload: dict, timeout: float = PUSH_TIMEOUT) -> tuple[int, dict]:
    req = urlrequest.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urlerror.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read() or b"{}")
        except Exception:
            return exc.code, {}
    except Exception as exc:
        raise ConnectionError(str(exc)) from exc


def _http_get(url: str, timeout: float = PUSH_TIMEOUT) -> dict:
    try:
        with urlrequest.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")
    except Exception as exc:
        raise ConnectionError(str(exc)) from exc


def push_to_node(node: dict, revision: int, domain: str, addresses: list[str]) -> dict:
    """Push one revision to one node. A 409 means it already has newer data."""
    try:
        code, body = _http_post(
            f"{node['control_url'].rstrip('/')}/publish",
            {"revision": revision, "domain": domain, "addresses": addresses},
        )
    except ConnectionError as exc:
        return {"ok": False, "status": "unreachable", "detail": str(exc)}

    if code == 200:
        return {"ok": True, "status": "applied",
                "served_revision": body.get("served_revision", revision)}
    if code == 409:
        # Stale push rejected — fetch the node's real served revision so the
        # management view reflects the node without ever going backwards.
        try:
            health = _http_get(f"{node['control_url'].rstrip('/')}/health")
            return {"ok": True, "status": "already-newer",
                    "served_revision": int(health.get("revision", revision))}
        except ConnectionError as exc:
            return {"ok": False, "status": "unreachable", "detail": str(exc)}
    return {"ok": False, "status": f"http-{code}", "detail": body}


def _record_served(node_id: str, served: int) -> None:
    with db.get_conn() as conn:
        db.report_served(conn, node_id, served)


async def reconcile_once() -> dict[str, Any]:
    """Push the latest revision to every node that is behind it."""
    with db.get_conn() as conn:
        latest = db.latest_revision(conn)
        if latest is None:
            return {"skipped": "no revision yet"}
        rev = db.row_to_revision_dict(latest)
        states = {r["node_id"]: r for r in db.all_node_state(conn)}

    results: dict[str, Any] = {}
    loop = asyncio.get_running_loop()
    tasks = []
    target_nodes = []
    for node in NODES:
        st = states.get(node["id"])
        served = st["served_revision"] if st else 0
        if served >= rev["revision"]:
            results[node["id"]] = {"ok": True, "status": "up-to-date",
                                   "served_revision": served}
            continue
        target_nodes.append(node)
        tasks.append(
            loop.run_in_executor(
                None, push_to_node, node,
                rev["revision"], rev["domain"], rev["addresses"],
            )
        )
    for node, outcome in zip(target_nodes, await asyncio.gather(*tasks, return_exceptions=True)):
        if isinstance(outcome, Exception):
            outcome = {"ok": False, "status": "error", "detail": str(outcome)}
        results[node["id"]] = outcome
        if outcome.get("ok") and outcome.get("served_revision") is not None:
            _record_served(node["id"], int(outcome["served_revision"]))
    return {"revision": rev["revision"], "results": results}


async def reconciler_loop() -> None:
    while True:
        try:
            await reconcile_once()
        except Exception as exc:  # never let the loop die
            print(f"[mgmt] reconcile error: {exc!r}")
        await asyncio.sleep(RECONCILE_INTERVAL)


# --------------------------------------------------------------------------- #
# Real DNS answer verification (UDP with EDNS(0), TCP fallback on TC)
# --------------------------------------------------------------------------- #
# EDNS UDP payload size advertised by the verifier. Large answer groups still
# do not fit and come back TC=1, which triggers the TCP retry below.
VERIFY_EDNS_UDP_SIZE = int(os.environ.get("VERIFY_EDNS_UDP_SIZE", "4096"))
DNS_TIMEOUT = float(os.environ.get("DNS_TIMEOUT", "3"))


def _dns_query_tcp(host: str, port: int, packet: bytes, timeout: float) -> bytes:
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))
        sock.sendall(struct.pack("!H", len(packet)) + packet)
        chunks = []
        # First read must yield the 2-octet length prefix.
        header = b""
        while len(header) < 2:
            part = sock.recv(2 - len(header))
            if not part:
                raise ConnectionError("TCP DNS connection closed before length prefix")
            header += part
        (msg_len,) = struct.unpack("!H", header)
        received = 0
        while received < msg_len:
            part = sock.recv(min(65535, msg_len - received))
            if not part:
                raise ConnectionError("TCP DNS connection closed mid-message")
            chunks.append(part)
            received += len(part)
        return b"".join(chunks)
    finally:
        sock.close()


def _dns_query(host: str, port: int, domain: str,
               timeout: float = DNS_TIMEOUT) -> dict:
    """Query one node over UDP, retrying over TCP when the reply sets TC.

    The UDP question carries an EDNS(0) OPT pseudo-record advertising
    ``VERIFY_EDNS_UDP_SIZE``. A TC=1 reply contains no usable answers, so the
    full answer group is always re-fetched over TCP instead of being treated
    as a (partial) match. Returns the wire details of the answer actually
    compared plus the transports used.
    """
    import socket

    question = DNSRecord.question(domain, qtype="A")
    question.add_ar(EDNS0(udp_len=VERIFY_EDNS_UDP_SIZE, version=0))
    wire_question = question.pack()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(wire_question, (host, int(port)))
        udp_data, _ = sock.recvfrom(65535)
    finally:
        sock.close()

    udp_reply = DNSRecord.parse(udp_data)
    used_tcp = False
    if udp_reply.header.tc:
        # Truncated: the node deliberately withheld the whole answer group.
        # Fetch the complete group over TCP before comparing anything.
        tcp_data = _dns_query_tcp(host, port, wire_question, timeout)
        reply = DNSRecord.parse(tcp_data)
        used_tcp = True
    else:
        reply = udp_reply

    answers = [str(rr.rdata) for rr in reply.rr if QTYPE[rr.rtype] == "A"]
    return {
        "rcode": RCODE[reply.header.rcode],
        "aa": bool(reply.header.aa),
        "answers": sorted(answers),
        "answer_count": len(answers),
        "udp_truncated": bool(udp_reply.header.tc),
        "tcp_fallback": used_tcp,
        "transport": "tcp" if used_tcp else "udp",
    }


def _verify_node(node: dict, rev: dict) -> dict:
    """Verify one node and compare its real answers to one fixed revision."""
    try:
        answer = _dns_query(node["dns_host"], node["dns_port"], rev["domain"])
        match = (
            answer["answers"] == sorted(rev["addresses"])
            and answer["aa"]
            and answer["rcode"] == "NOERROR"
        )
    except Exception as exc:
        answer = {"error": str(exc), "transport": None,
                  "udp_truncated": False, "tcp_fallback": False}
        match = False
    return {"node_id": node["id"], **answer,
            "matches_latest_revision": match}


# --------------------------------------------------------------------------- #
# Application
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    with db.get_conn() as conn:
        db.ensure_expected_nodes(conn, NODE_IDS)
    task = asyncio.create_task(reconciler_loop())
    yield
    task.cancel()


app = FastAPI(
    title="单域名 A 记录发布 API",
    description="机房设备管理域名 A 记录发布：递增修订、双权威节点同步、实答核对。",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/publishes", status_code=201, summary="提交新的地址列表（生成新修订）")
async def create_publish(req: PublishRequest, background: BackgroundTasks) -> dict:
    # BEGIN IMMEDIATE serialises concurrent submissions; revisions stay
    # strictly increasing even under parallel POSTs.
    with db.revision_tx() as conn:
        revision = db.insert_revision(conn, DOMAIN, req.addresses)
        row = db.latest_revision(conn)
        rev = db.row_to_revision_dict(row)

    # Immediate push attempt; the background reconciler guarantees retries.
    background.add_task(reconcile_once)
    return {
        "revision": revision,
        "domain": rev["domain"],
        "addresses": rev["addresses"],
        "message": "修订已创建，正在向各节点下发；可通过 /status 查看同步进度。",
    }


@app.get("/status", summary="查看最新修订与各节点服务版本/待补状态")
async def status() -> dict:
    now = time.time()
    with db.get_conn() as conn:
        latest = db.latest_revision(conn)
        rows = db.all_node_state(conn)
        states = {r["node_id"]: r for r in rows}

    latest_rev = db.row_to_revision_dict(latest) if latest else None
    target = latest_rev["revision"] if latest_rev else 0

    node_views: list[dict] = []
    offline_ids, pending_ids = [], []
    for node in NODES:
        st = states.get(node["id"])
        served = st["served_revision"] if st else 0
        last_seen = st["last_seen"] if st else 0
        online = (now - last_seen) <= db.OFFLINE_AFTER if last_seen > 0 else False
        caught_up = latest_rev is not None and served >= target
        if not online:
            offline_ids.append(node["id"])
        if not caught_up:
            pending_ids.append(node["id"])
        node_views.append({
            "node_id": node["id"],
            "online": online,
            "served_revision": served,
            "target_revision": target,
            "last_seen_seconds_ago": round(now - last_seen, 1) if last_seen else None,
            "caught_up": caught_up,
            "catch_up_status": (
                "served" if caught_up else
                ("pending-retry" if online else "waiting-node-recovery")
            ),
        })

    if latest_rev is None:
        overall = "no-revision-yet"
    elif offline_ids:
        # A node is missing: the other node keeps answering, but the pair is
        # NOT fully synced. Never report success on assumptions.
        overall = "degraded-node-offline"
    elif pending_ids:
        overall = "syncing"
    else:
        overall = "synced"

    return {
        "domain": DOMAIN,
        "latest_revision": latest_rev,
        "overall_status": overall,
        "synced": overall == "synced",
        "offline_nodes": offline_ids,
        "pending_nodes": pending_ids,
        "nodes": node_views,
    }


@app.get("/revisions", summary="修订历史")
async def revisions() -> list[dict]:
    with db.get_conn() as conn:
        return [db.row_to_revision_dict(r) for r in db.list_revisions(conn)]


@app.get("/verify", summary="以两端实际 DNS 答复核对最新修订（TC 自动走 TCP）")
async def verify() -> dict:
    # Snapshot the target revision first. This short read closes its
    # connection immediately: no DB transaction (and therefore no publish
    # write lock) is held while the network probes below run.
    with db.get_conn() as conn:
        latest = db.latest_revision(conn)
        if latest is None:
            raise HTTPException(409, "尚无修订，无法核对")
        rev = db.row_to_revision_dict(latest)
    expected_revision = rev["revision"]

    loop = asyncio.get_running_loop()
    findings = []
    all_match = True
    for node in NODES:
        finding = await loop.run_in_executor(None, _verify_node, node, rev)
        all_match &= finding["matches_latest_revision"]
        findings.append(finding)

    # A publish may have landed while the probes were in flight. Re-read the
    # newest revision (another lock-free short read) and refuse to certify
    # the old one: verification can only ever confirm the revision that is
    # still current when the answer comes back.
    with db.get_conn() as conn:
        current_row = db.latest_revision(conn)
    current_revision = current_row["revision"] if current_row else 0
    superseded = current_revision > expected_revision

    return {
        "domain": rev["domain"],
        "expected_revision": expected_revision,
        "current_revision": current_revision,
        "expected_addresses": sorted(rev["addresses"]),
        "superseded_during_verification": superseded,
        "verified_revision_current": all_match and not superseded,
        "all_nodes_match": all_match,
        "nodes": findings,
        "message": (
            "核验期间出现更新修订，旧修订不得视为已核实，请对最新修订重新发起核验。"
            if superseded else None
        ),
    }


@app.post("/reconcile", summary="手动触发一次对账下发")
async def manual_reconcile() -> dict:
    return await reconcile_once()


# --------------------------------------------------------------------------- #
# Internal endpoints used by the DNS nodes themselves
# --------------------------------------------------------------------------- #
@app.post("/internal/nodes/{node_id}/heartbeat")
async def heartbeat(node_id: str, req: ReportRequest) -> dict:
    if node_id not in NODE_IDS:
        raise HTTPException(404, "unknown node")
    with db.get_conn() as conn:
        db.upsert_node_heartbeat(conn, node_id)
        db.report_served(conn, node_id, req.served_revision)
    return {"ok": True}


@app.post("/internal/nodes/{node_id}/report")
async def report(node_id: str, req: ReportRequest) -> dict:
    if node_id not in NODE_IDS:
        raise HTTPException(404, "unknown node")
    _record_served(node_id, req.served_revision)
    return {"ok": True}
