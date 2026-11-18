"""SQLite persistence for the management service.

Concurrency model
-----------------
Every revision insert runs inside a ``BEGIN IMMEDIATE`` transaction, so two
concurrent POST requests are serialised by SQLite's reserved lock and the
autoincrement-style revision number can never be duplicated or reordered.

Node state updates are clamped with ``MAX(served_revision, ?)`` so a late
retry carrying an old revision can never move a node (or the aggregate sync
conclusion) backwards.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable

DB_PATH = os.environ.get("MGMT_DB_PATH", "/data/management.db")
# A node that has not reported within this many seconds is considered offline.
OFFLINE_AFTER = float(os.environ.get("OFFLINE_AFTER_SECONDS", "15"))

_init_lock = threading.Lock()
_initialised = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_conn():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    global _initialised
    with _init_lock:
        if _initialised:
            return
        with get_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS revisions (
                    revision      INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain        TEXT NOT NULL,
                    addresses     TEXT NOT NULL,
                    created_at    REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_state (
                    node_id            TEXT PRIMARY KEY,
                    served_revision    INTEGER NOT NULL DEFAULT 0,
                    last_seen          REAL NOT NULL DEFAULT 0,
                    target_revision    INTEGER NOT NULL DEFAULT 0
                );
                """
            )
        _initialised = True


@contextmanager
def revision_tx():
    """Serialise writers with a reserved lock for the whole transaction."""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def insert_revision(conn: sqlite3.Connection, domain: str, addresses: list[str]) -> int:
    cur = conn.execute(
        "INSERT INTO revisions(domain, addresses, created_at) VALUES (?, ?, ?)",
        (domain, json.dumps(addresses), time.time()),
    )
    rev = int(cur.lastrowid)
    # Every known node now has this revision as its target.
    conn.execute("UPDATE node_state SET target_revision = ?", (rev,))
    return rev


def latest_revision(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT revision, domain, addresses, created_at "
        "FROM revisions ORDER BY revision DESC LIMIT 1"
    ).fetchone()


def get_revision(conn: sqlite3.Connection, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT revision, domain, addresses, created_at "
        "FROM revisions WHERE revision = ?",
        (revision,),
    ).fetchone()


def list_revisions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute("SELECT revision, domain, addresses, created_at "
                     "FROM revisions ORDER BY revision DESC")
    )


def upsert_node_heartbeat(conn: sqlite3.Connection, node_id: str) -> None:
    conn.execute(
        "INSERT INTO node_state(node_id, last_seen) VALUES (?, ?) "
        "ON CONFLICT(node_id) DO UPDATE SET last_seen = excluded.last_seen",
        (node_id, time.time()),
    )


def report_served(conn: sqlite3.Connection, node_id: str, served: int) -> None:
    """Record a node's served revision, monotonically non-decreasing.

    The ``MAX`` clamp is the guard against late/retried old reports: a stale
    POST arriving after a newer one cannot roll the node backwards, and the
    aggregate sync state derived from this table therefore cannot regress.
    """
    now = time.time()
    conn.execute(
        """
        INSERT INTO node_state(node_id, served_revision, last_seen, target_revision)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(node_id) DO UPDATE SET
            served_revision = MAX(node_state.served_revision, excluded.served_revision),
            last_seen       = excluded.last_seen
        """,
        (node_id, served, now),
    )


def all_node_state(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT node_id, served_revision, last_seen, target_revision "
        "FROM node_state ORDER BY node_id"
    ))


def ensure_expected_nodes(conn: sqlite3.Connection, node_ids: Iterable[str]) -> None:
    for node_id in node_ids:
        conn.execute(
            "INSERT OR IGNORE INTO node_state(node_id) VALUES (?)", (node_id,)
        )


def row_to_revision_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "revision": row["revision"],
        "domain": row["domain"],
        "addresses": json.loads(row["addresses"]),
        "created_at": row["created_at"],
    }
