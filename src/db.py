"""
SQLite layer for the mapping_tools API.

Both hf_map.db and ir_map.db share the same schema (produced by excel_sync_core.py):
  records  — current state, is_active flag
  history  — append-only log: ADDED / MODIFIED / REMOVED / RESTORED
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Shared generic queries (work for both DBs)
# ---------------------------------------------------------------------------

def get_summary(db_path: Path) -> dict:
    with _connect(db_path) as conn:
        total    = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        active   = conn.execute("SELECT COUNT(*) FROM records WHERE is_active = 1").fetchone()[0]
        inactive = conn.execute("SELECT COUNT(*) FROM records WHERE is_active = 0").fetchone()[0]
        changes  = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        last_sync = conn.execute("SELECT MAX(synced_at) FROM history").fetchone()[0]
    return {
        "total": total,
        "active": active,
        "inactive": inactive,
        "total_changes": changes,
        "last_sync": last_sync,
    }


def get_recent_changes(db_path: Path, limit: int = 50) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM history ORDER BY history_id DESC LIMIT ?", (limit,)
        ).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("changed_fields"):
            try:
                d["changed_fields"] = json.loads(d["changed_fields"])
            except (ValueError, TypeError):
                pass
        results.append(d)
    return results


def get_record_history(db_path: Path, record_id: str) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM history WHERE record_id = ? ORDER BY history_id DESC",
            (record_id,),
        ).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("changed_fields"):
            try:
                d["changed_fields"] = json.loads(d["changed_fields"])
            except (ValueError, TypeError):
                pass
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# HF Map (hf_map.db)
# Columns: id, firm, name, title, location, function, strategy, products, reports_to
# ---------------------------------------------------------------------------

def hf_get_all(db_path: Path, include_inactive: bool = False) -> list[dict]:
    where = "" if include_inactive else "WHERE is_active = 1"
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM records {where} ORDER BY firm, name"
        ).fetchall()
    return [dict(r) for r in rows]


def hf_get_one(db_path: Path, record_id: str) -> dict | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM records WHERE id = ?", (record_id,)
        ).fetchone()
    return dict(row) if row else None


def hf_get_firms(db_path: Path) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT firm, COUNT(*) AS headcount
            FROM records
            WHERE is_active = 1 AND firm IS NOT NULL AND firm != ''
            GROUP BY firm
            ORDER BY headcount DESC, firm
        """).fetchall()
    return [dict(r) for r in rows]


def hf_search(db_path: Path, q: str, limit: int = 100) -> list[dict]:
    pattern = f"%{q}%"
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT * FROM records
            WHERE is_active = 1
              AND (name LIKE ? OR firm LIKE ? OR title LIKE ? OR function LIKE ? OR strategy LIKE ?)
            ORDER BY firm, name
            LIMIT ?
        """, (pattern, pattern, pattern, pattern, pattern, limit)).fetchall()
    return [dict(r) for r in rows]


def hf_get_recent_moves(db_path: Path, limit: int = 50) -> list[dict]:
    """History rows where firm changed or a record was newly added."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT * FROM history
            WHERE (changed_fields LIKE '%"firm"%' OR change_type = 'ADDED')
            ORDER BY history_id DESC
            LIMIT ?
        """, (limit,)).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("changed_fields"):
            try:
                d["changed_fields"] = json.loads(d["changed_fields"])
            except (ValueError, TypeError):
                pass
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# IR Map (ir_map.db)
# Columns: id, name, group, function, current_firm, current_title, date_joined,
#          current_location, former_firm, former_title, date_left, former_location,
#          note, most_recent_date, hf_id
# ---------------------------------------------------------------------------

def ir_get_all(db_path: Path, include_inactive: bool = False) -> list[dict]:
    where = "" if include_inactive else "WHERE is_active = 1"
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM records {where} ORDER BY current_firm, name"
        ).fetchall()
    return [dict(r) for r in rows]


def ir_get_one(db_path: Path, record_id: str) -> dict | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM records WHERE id = ?", (record_id,)
        ).fetchone()
    return dict(row) if row else None


def ir_get_firms(db_path: Path) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT current_firm AS firm, COUNT(*) AS headcount
            FROM records
            WHERE is_active = 1 AND current_firm IS NOT NULL AND current_firm != ''
            GROUP BY current_firm
            ORDER BY headcount DESC, current_firm
        """).fetchall()
    return [dict(r) for r in rows]


def ir_search(db_path: Path, q: str, limit: int = 100) -> list[dict]:
    pattern = f"%{q}%"
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT * FROM records
            WHERE is_active = 1
              AND (name LIKE ? OR current_firm LIKE ? OR current_title LIKE ?
                   OR function LIKE ? OR "group" LIKE ?)
            ORDER BY current_firm, name
            LIMIT ?
        """, (pattern, pattern, pattern, pattern, pattern, limit)).fetchall()
    return [dict(r) for r in rows]


def ir_get_recent_moves(db_path: Path, limit: int = 50) -> list[dict]:
    """History rows where current_firm changed or a record was newly added."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT * FROM history
            WHERE (changed_fields LIKE '%"current_firm"%' OR change_type = 'ADDED')
            ORDER BY history_id DESC
            LIMIT ?
        """, (limit,)).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("changed_fields"):
            try:
                d["changed_fields"] = json.loads(d["changed_fields"])
            except (ValueError, TypeError):
                pass
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# Generic helpers — reused by all new maps (credit, commodities, equities, etc.)
# ---------------------------------------------------------------------------

def generic_get_one(db_path: Path, record_id: str, id_col: str = "id") -> dict | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            f'SELECT * FROM records WHERE "{id_col}" = ?', (record_id,)
        ).fetchone()
    return dict(row) if row else None


def generic_get_firms(db_path: Path, firm_col: str = "firm") -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(f"""
            SELECT "{firm_col}" AS firm, COUNT(*) AS headcount
            FROM records
            WHERE is_active = 1
              AND "{firm_col}" IS NOT NULL AND "{firm_col}" != ''
            GROUP BY "{firm_col}"
            ORDER BY headcount DESC, "{firm_col}"
        """).fetchall()
    return [dict(r) for r in rows]


def generic_search(
    db_path: Path,
    q: str,
    search_cols: list[str],
    order_cols: tuple[str, ...] = ("firm", "name"),
    limit: int = 100,
) -> list[dict]:
    pattern = f"%{q}%"
    conditions = " OR ".join(f'"{col}" LIKE ?' for col in search_cols)
    order = ", ".join(f'"{c}"' for c in order_cols)
    with _connect(db_path) as conn:
        rows = conn.execute(f"""
            SELECT * FROM records
            WHERE is_active = 1 AND ({conditions})
            ORDER BY {order}
            LIMIT ?
        """, (*[pattern] * len(search_cols), limit)).fetchall()
    return [dict(r) for r in rows]


def generic_get_recent_moves(
    db_path: Path,
    firm_col: str = "firm",
    limit: int = 50,
) -> list[dict]:
    moves_pattern = f'%"{firm_col}"%'
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT * FROM history
            WHERE (changed_fields LIKE ? OR change_type = 'ADDED')
            ORDER BY history_id DESC
            LIMIT ?
        """, (moves_pattern, limit)).fetchall()
    results = []
    for r in rows:
        d = dict(r)
        if d.get("changed_fields"):
            try:
                d["changed_fields"] = json.loads(d["changed_fields"])
            except (ValueError, TypeError):
                pass
        results.append(d)
    return results


def get_daily_change_counts(db_path: Path, days: int = 60) -> list[dict]:
    """One row per calendar day with a change count; gaps filled with zero."""
    from datetime import date, timedelta
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT date(synced_at) AS day, COUNT(*) AS count
            FROM history
            WHERE synced_at >= date('now', ?)
            GROUP BY day
            ORDER BY day ASC
        """, (f"-{days} days",)).fetchall()
    by_day   = {r["day"]: r["count"] for r in rows}
    today    = date.today()
    start    = today - timedelta(days=days - 1)
    result, current = [], start
    while current <= today:
        key = current.isoformat()
        result.append({"day": key, "count": by_day.get(key, 0)})
        current += timedelta(days=1)
    return result
