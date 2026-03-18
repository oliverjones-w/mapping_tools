"""
SQLite layer for bbg_results.db.

Schema:
  bbg_runs          — one row per CSV processed (full audit history per firm)
  bbg_confirmed     — people from BBG CSV confirmed in hf_map at expected firm
  bbg_discrepancies — people in hf_map but BBG shows them at a different firm
  bbg_additions     — people in BBG CSV not found in hf_map (new candidates)
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bbg_runs (
                run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id           TEXT NOT NULL,
                firm_name         TEXT,
                csv_filename      TEXT NOT NULL,
                source_type       TEXT NOT NULL,
                run_at            TEXT NOT NULL,
                rows_processed    INTEGER,
                confirmed_count   INTEGER,
                discrepancy_count INTEGER,
                addition_count    INTEGER
            );

            CREATE TABLE IF NOT EXISTS bbg_confirmed (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       INTEGER NOT NULL REFERENCES bbg_runs(run_id),
                firm_id      TEXT NOT NULL,
                hf_record_id TEXT,
                name         TEXT,
                firm         TEXT,
                title        TEXT,
                location     TEXT,
                function     TEXT,
                strategy     TEXT,
                products     TEXT,
                reports_to   TEXT
            );

            CREATE TABLE IF NOT EXISTS bbg_discrepancies (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id             INTEGER NOT NULL REFERENCES bbg_runs(run_id),
                firm_id            TEXT NOT NULL,
                name               TEXT,
                master_record_uids TEXT,
                discrepancy_field  TEXT,
                new_file_value     TEXT,
                master_file_values TEXT,
                alias_check_info   TEXT,
                source_file        TEXT,
                status             TEXT DEFAULT 'Active',
                first_seen         TEXT
            );

            CREATE TABLE IF NOT EXISTS bbg_additions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            INTEGER NOT NULL REFERENCES bbg_runs(run_id),
                firm_id           TEXT NOT NULL,
                name              TEXT,
                company           TEXT,
                canonical_company TEXT,
                title             TEXT,
                location          TEXT,
                focus             TEXT,
                source_file       TEXT,
                first_seen        TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_runs_firm   ON bbg_runs (firm_id);
            CREATE INDEX IF NOT EXISTS idx_runs_run_at ON bbg_runs (run_at);
            CREATE INDEX IF NOT EXISTS idx_conf_run    ON bbg_confirmed (run_id);
            CREATE INDEX IF NOT EXISTS idx_disc_run    ON bbg_discrepancies (run_id);
            CREATE INDEX IF NOT EXISTS idx_add_run     ON bbg_additions (run_id);
        """)


# ---------------------------------------------------------------------------
# Write helpers (called by the extraction script)
# ---------------------------------------------------------------------------

def create_run(
    db_path: Path,
    firm_id: str,
    firm_name: str,
    csv_filename: str,
    source_type: str,
    rows_processed: int,
    confirmed_count: int,
    discrepancy_count: int,
    addition_count: int,
) -> int:
    run_at = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO bbg_runs
               (firm_id, firm_name, csv_filename, source_type, run_at,
                rows_processed, confirmed_count, discrepancy_count, addition_count)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (firm_id, firm_name, csv_filename, source_type, run_at,
             rows_processed, confirmed_count, discrepancy_count, addition_count),
        )
        return cur.lastrowid


def insert_confirmed(db_path: Path, run_id: int, records: list[dict]) -> None:
    if not records:
        return
    with _connect(db_path) as conn:
        conn.executemany(
            """INSERT INTO bbg_confirmed
               (run_id, firm_id, hf_record_id, name, firm, title,
                location, function, strategy, products, reports_to)
               VALUES (:run_id, :firm_id, :hf_record_id, :name, :firm, :title,
                       :location, :function, :strategy, :products, :reports_to)""",
            records,
        )


def insert_discrepancies(db_path: Path, run_id: int, records: list[dict]) -> None:
    if not records:
        return
    with _connect(db_path) as conn:
        conn.executemany(
            """INSERT INTO bbg_discrepancies
               (run_id, firm_id, name, master_record_uids, discrepancy_field,
                new_file_value, master_file_values, alias_check_info,
                source_file, status, first_seen)
               VALUES (:run_id, :firm_id, :name, :master_record_uids,
                       :discrepancy_field, :new_file_value, :master_file_values,
                       :alias_check_info, :source_file, :status, :first_seen)""",
            records,
        )


def insert_additions(db_path: Path, run_id: int, records: list[dict]) -> None:
    if not records:
        return
    with _connect(db_path) as conn:
        conn.executemany(
            """INSERT INTO bbg_additions
               (run_id, firm_id, name, company, canonical_company,
                title, location, focus, source_file, first_seen)
               VALUES (:run_id, :firm_id, :name, :company, :canonical_company,
                       :title, :location, :focus, :source_file, :first_seen)""",
            records,
        )


# ---------------------------------------------------------------------------
# Read helpers (called by the API)
# ---------------------------------------------------------------------------

def get_firms_summary(db_path: Path) -> list[dict]:
    """Latest run stats per firm, with tracking %."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT r.firm_id, r.firm_name, r.run_at, r.rows_processed,
                   r.confirmed_count, r.discrepancy_count, r.addition_count,
                   r.run_id AS latest_run_id
            FROM bbg_runs r
            INNER JOIN (
                SELECT firm_id, MAX(run_id) AS max_run_id
                FROM bbg_runs
                GROUP BY firm_id
            ) latest ON r.run_id = latest.max_run_id
            ORDER BY r.firm_name, r.firm_id
        """).fetchall()

    results = []
    for row in rows:
        d = dict(row)
        c    = d.get("confirmed_count", 0) or 0
        disc = d.get("discrepancy_count", 0) or 0
        add  = d.get("addition_count", 0) or 0
        total = c + disc + add
        d["tracking_pct"] = round((c / total) * 100, 1) if total > 0 else 0.0
        results.append(d)
    return results


def get_runs_for_firm(db_path: Path, firm_id: str) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM bbg_runs WHERE firm_id = ? ORDER BY run_id DESC",
            (firm_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_run(db_path: Path, run_id: int) -> dict | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM bbg_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
    return dict(row) if row else None


def get_latest_run_id(db_path: Path, firm_id: str) -> int | None:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(run_id) FROM bbg_runs WHERE firm_id = ?", (firm_id,)
        ).fetchone()
    return row[0] if row else None


def get_confirmed_for_run(db_path: Path, run_id: int) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM bbg_confirmed WHERE run_id = ? ORDER BY firm, name",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_discrepancies_for_run(db_path: Path, run_id: int) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM bbg_discrepancies WHERE run_id = ? ORDER BY name",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_additions_for_run(db_path: Path, run_id: int) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM bbg_additions WHERE run_id = ? ORDER BY name",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]
