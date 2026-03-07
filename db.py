"""
db.py — SQLite account registry for the Prospecting Toolkit.

Stores supplementary metadata (Salesforce URLs, Slack channel, notes) and
action items alongside the file-based report system.  The filesystem remains
authoritative for which accounts exist; this DB adds structured enrichment.

All functions accept db_path as their first argument so callers don't need to
import a global path — server.py owns the path constant.
"""

import sqlite3
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    slug               TEXT PRIMARY KEY,
    display_name       TEXT,
    domain             TEXT,
    sf_account_url     TEXT,
    sf_opportunity_url TEXT,
    slack_channel      TEXT,
    notes              TEXT,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS _migrations (
    name TEXT PRIMARY KEY
);



CREATE TABLE IF NOT EXISTS action_items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    slug          TEXT NOT NULL,
    source_report TEXT,
    text          TEXT NOT NULL,
    completed     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT DEFAULT (datetime('now')),
    completed_at  TEXT
);
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _connect(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _row_to_dict(row) -> dict:
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db(db_path: Path | str) -> None:
    """Create tables if they don't exist. Call once at server startup."""
    with _connect(db_path) as conn:
        conn.executescript(_SCHEMA)
        # Migrations for existing installs
        cols = {r[1] for r in conn.execute("PRAGMA table_info(accounts)")}
        if "domain" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN domain TEXT")


# ── Account metadata ─────────────────────────────────────────────────────

def get_account_meta(db_path: Path | str, slug: str) -> dict:
    """Return the accounts row for slug as a dict, or {} if none exists."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM accounts WHERE slug = ?", (slug,)
        ).fetchone()
    return _row_to_dict(row)


def upsert_account_meta(db_path: Path | str, slug: str, **fields) -> None:
    """
    Insert or update account metadata.  Only columns present in `fields` are
    written — omitted columns keep their existing values (or NULL for new rows).
    Always refreshes updated_at.
    """
    allowed = {
        "display_name", "domain", "sf_account_url", "sf_opportunity_url",
        "slack_channel", "notes",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return

    with _connect(db_path) as conn:
        # Ensure row exists
        conn.execute(
            "INSERT OR IGNORE INTO accounts (slug) VALUES (?)", (slug,)
        )
        # Update only supplied fields
        set_clause = ", ".join(f"{k} = ?" for k in safe)
        set_clause += ", updated_at = datetime('now')"
        conn.execute(
            f"UPDATE accounts SET {set_clause} WHERE slug = ?",
            [*safe.values(), slug],
        )


# ── Action items ─────────────────────────────────────────────────────────

def get_action_items(db_path: Path | str, slug: str) -> list[dict]:
    """Return all action items for an account, newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM action_items WHERE slug = ? ORDER BY id DESC",
            (slug,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_open_action_items(db_path: Path | str) -> list[dict]:
    """Return all incomplete action items across all accounts, newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM action_items
               WHERE completed = 0
               ORDER BY id DESC""",
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_action_items_all_accounts(db_path: Path | str) -> list[dict]:
    """Return ALL action items across all accounts (open + completed), newest first."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM action_items ORDER BY id DESC",
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_action_items_for_dedup(db_path: Path | str, slug: str) -> list[dict]:
    """Return ALL action items for an account (including completed) for deduplication."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM action_items WHERE slug = ? ORDER BY id",
            (slug,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def add_action_item(
    db_path: Path | str,
    slug: str,
    text: str,
    source_report: str | None = None,
) -> int:
    """Insert a new action item and return its id."""
    # Ensure the accounts row exists so foreign-key-style queries work later
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO accounts (slug) VALUES (?)", (slug,)
        )
        cur = conn.execute(
            "INSERT INTO action_items (slug, text, source_report) VALUES (?, ?, ?)",
            (slug, text.strip(), source_report),
        )
        return cur.lastrowid


def toggle_action_item(db_path: Path | str, item_id: int) -> dict:
    """Flip the completed flag; set/clear completed_at. Return updated row."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT completed FROM action_items WHERE id = ?", (item_id,)
        ).fetchone()
        if not row:
            return {}
        now_completed = 0 if row["completed"] else 1
        conn.execute(
            """UPDATE action_items
               SET completed = ?,
                   completed_at = CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END
               WHERE id = ?""",
            (now_completed, now_completed, item_id),
        )
        updated = conn.execute(
            "SELECT * FROM action_items WHERE id = ?", (item_id,)
        ).fetchone()
    return _row_to_dict(updated)


def delete_action_item(db_path: Path | str, item_id: int) -> None:
    """Delete an action item by id."""
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM action_items WHERE id = ?", (item_id,))
