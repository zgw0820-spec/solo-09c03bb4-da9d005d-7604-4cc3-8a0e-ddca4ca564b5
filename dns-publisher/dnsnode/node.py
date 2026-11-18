"""Persistent authoritative DNS node (UDP) with a push control API.

The node owns its own SQLite database (independent persistent volume in
Compose). It answers A queries for exactly one managed domain and applies the
full address list atomically. A publish carrying a revision that is not
strictly newer than the one already served is rejected with HTTP 409, so a
late retry can never roll a node back to an old revision.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request as urlrequest

from dnslib import A, QTYPE, RCODE, RR, DNSRecord

NODE_ID = os.environ.get("NODE_ID", "dns-node")
DB_PATH = os.environ.get("NODE_DB_PATH", "/data/node.db")
UDP_HOST = os.environ.get("UDP_HOST", "0.0.0.0")
UDP_PORT = int(os.environ.get("UDP_PORT", "5353"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8053"))
DNS_TTL = int(os.environ.get("DNS_TTL", "30"))
MGMT_BASE_URL = os.environ.get("MGMT_BASE_URL", "").rstrip("/")
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "5"))

_state_lock = threading.Lock()
# In-memory mirror of the durable SQLite row; loaded at startup.
_current: dict = {"revision": 0, "domain": "", "addresses": [], "applied_at": 0.0}


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS zone (
                id           INTEGER PRIMARY KEY CHECK (id = 1),
                revision     INTEGER NOT NULL,
                domain       TEXT NOT NULL,
                addresses    TEXT NOT NULL,
                applied_at   REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS applied_log (
                revision    INTEGER PRIMARY KEY,
                domain      TEXT NOT NULL,
                addresses   TEXT NOT NULL,
                applied_at  REAL NOT NULL
            );
            """
        )
        row = conn.execute(
            "SELECT revision, domain, addresses, applied_at FROM zone WHERE id = 1"
        ).fetchone()
        if row is not None:
            _current.update(
                revision=row["revision"],
                domain=row["domain"],
                addresses=json.loads(row["addresses"]),
                applied_at=row["applied_at"],
            )
            print(f"[node {NODE_ID}] restored revision {row['revision']} from {DB_PATH}")


def apply_publish(revision: int, domain: str, addresses: list[str]) -> tuple[bool, str]:
    """Atomically apply a whole new address list.

    Returns (applied, reason). Rejects any revision <= the served one.
    """
    with _state_lock:
        if revision <= _current["revision"]:
            return False, (
                f"stale revision {revision}: node already serves "
                f"{_current['revision']}"
            )
        now = time.time()
        payload = json.dumps(addresses)
        with _connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO zone(id, revision, domain, addresses, applied_at) "
                "VALUES (1, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "revision=excluded.revision, domain=excluded.domain, "
                "addresses=excluded.addresses, applied_at=excluded.applied_at",
                (revision, domain, payload, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO applied_log"
                "(revision, domain, addresses, applied_at) VALUES (?, ?, ?, ?)",
                (revision, domain, payload, now),
            )
            conn.execute("COMMIT")
        _current.update(
            revision=revision, domain=domain, addresses=list(addresses), applied_at=now
        )
    print(f"[node {NODE_ID}] applied revision {revision}: {domain} -> {addresses}")
    return True, "applied"


def snapshot() -> dict:
    with _state_lock:
        return dict(_current)


# --------------------------------------------------------------------------- #
# UDP authoritative DNS server (dnslib)
# --------------------------------------------------------------------------- #
class ManagedZoneResolver:
    """Answers A queries for the managed domain only; authoritative AA=1."""

    def resolve(self, request: DNSRecord, handler) -> DNSRecord:
        reply = request.reply()
        qname = str(request.q.qname).rstrip(".")
        qtype = QTYPE[request.q.qtype]
        snap = snapshot()

        if qtype != "A":
            # The service only publishes A records.
            reply.header.rcode = RCODE.REFUSED
            return reply
        if not snap["domain"] or qname != snap["domain"].rstrip("."):
            # Authoritative server, unknown name -> NXDOMAIN.
            reply.header.rcode = RCODE.NXDOMAIN
            return reply
        if snap["revision"] == 0:
            # No revision has ever been applied: do not fabricate data.
            reply.header.rcode = RCODE.SERVFAIL
            return reply

        for addr in snap["addresses"]:  # the whole list takes effect together
            reply.add_answer(
                RR(
                    request.q.qname,
                    QTYPE.A,
                    rdata=A(addr),
                    ttl=DNS_TTL,
                )
            )
        return reply


# --------------------------------------------------------------------------- #
# Control/health HTTP API
# --------------------------------------------------------------------------- #
class ControlHandler(BaseHTTPRequestHandler):
    server_version = f"dnsnode/{NODE_ID}"

    def log_message(self, fmt, *args):  # quieter logs
        print(f"[node {NODE_ID} http] {fmt % args}")

    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/") in ("/health", "/zones/current"):
            snap = snapshot()
            self._send_json(200, {"node_id": NODE_ID, **snap})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") != "/publish":
            self._send_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            data = json.loads(self.rfile.read(length) or b"{}")
            revision = int(data["revision"])
            domain = str(data["domain"])
            addresses = list(data["addresses"])
            assert revision > 0 and domain and addresses
            for a in addresses:
                parts = a.split(".")
                assert len(parts) == 4 and all(p.isdigit() for p in parts)
        except Exception:
            self._send_json(400, {"error": "invalid publish payload"})
            return

        applied, reason = apply_publish(revision, domain, addresses)
        if applied:
            _report_to_management(revision)
            self._send_json(200, {"status": "applied",
                                  "node_id": NODE_ID,
                                  "served_revision": revision})
        else:
            self._send_json(409, {"status": "rejected",
                                  "reason": reason,
                                  "node_id": NODE_ID,
                                  "served_revision": snapshot()["revision"]})


# --------------------------------------------------------------------------- #
# Best-effort reporting back to the management service
# --------------------------------------------------------------------------- #
def _post_json(url: str, payload: dict, timeout: float = 5.0) -> bool:
    req = urlrequest.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        print(f"[node {NODE_ID}] report to {url} failed: {exc}")
        return False


def _report_to_management(revision: int) -> None:
    if not MGMT_BASE_URL:
        return

    def _worker():
        url = f"{MGMT_BASE_URL}/internal/nodes/{NODE_ID}/report"
        for attempt in range(5):  # bounded retries; reconcile loop is the backstop
            if _post_json(url, {"served_revision": revision}):
                return
            time.sleep(min(2 ** attempt, 10))

    threading.Thread(target=_worker, daemon=True).start()


def _heartbeat_loop() -> None:
    if not MGMT_BASE_URL:
        return
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        _post_json(
            f"{MGMT_BASE_URL}/internal/nodes/{NODE_ID}/heartbeat",
            {"served_revision": snapshot()["revision"]},
        )


def main() -> None:
    init_db()

    from dnslib.server import DNSServer

    resolver = ManagedZoneResolver()
    dns_server = DNSServer(resolver, port=UDP_PORT, address=UDP_HOST, tcp=False)
    dns_server.start_thread()
    print(f"[node {NODE_ID}] UDP authoritative DNS on {UDP_HOST}:{UDP_PORT}")

    http_server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), ControlHandler)
    print(f"[node {NODE_ID}] control API on {HTTP_HOST}:{HTTP_PORT}")

    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        dns_server.stop()
        http_server.server_close()


if __name__ == "__main__":
    main()
