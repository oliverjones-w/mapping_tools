"""
excel_sync_core.py
------------------
Shared engine for syncing Excel maps (.xlsm) into SQLite with full version history.

Schema per database:
  records  — current state of every row (is_active=0 if removed from Excel)
  history  — append-only log of every change (ADDED / MODIFIED / REMOVED / RESTORED)
"""

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class ExcelSyncConfig:
    excel_path: Path
    db_path: Path
    sheet_name: str
    columns: list[str]          # exact column names as they appear in Excel
    id_column: str              # which column is the primary key
    header_row: int = 2         # pandas header= parameter (0-indexed row number)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    """Normalize an Excel column name to a safe SQLite column name."""
    return name.strip().lower().replace(" ", "_")


def _normalize_value(val) -> Optional[str]:
    """Convert any Excel cell value to a comparable string (or None)."""
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return None
    s = str(val).strip()
    # Excel often reads integer IDs as floats: "1.0" -> "1"
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            pass
    return s if s != "" else None


def _compute_hash(record: dict, columns: list[str]) -> str:
    """MD5 hash of all data column values — used for change detection."""
    payload = {col: record.get(col) for col in columns}
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()


# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------

def _q(col: str) -> str:
    """Double-quote a column name to avoid conflicts with SQL reserved words."""
    return f'"{col}"'


def _init_db(conn: sqlite3.Connection, norm_cols: list[str], id_col: str) -> None:
    """Create records and history tables if they don't already exist."""

    col_defs = ",\n    ".join(
        f"{_q(col)} TEXT PRIMARY KEY" if col == id_col else f"{_q(col)} TEXT"
        for col in norm_cols
    )

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS records (
            {col_defs},
            row_hash   TEXT,
            created_at TEXT,
            updated_at TEXT,
            is_active  INTEGER DEFAULT 1
        )
    """)

    hist_col_defs = ",\n    ".join(f"{_q(col)} TEXT" for col in norm_cols)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS history (
            history_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id      TEXT,
            change_type    TEXT,
            changed_fields TEXT,
            {hist_col_defs},
            row_hash       TEXT,
            synced_at      TEXT
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_history_record ON history (record_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_history_synced ON history (synced_at)")
    conn.commit()


# ---------------------------------------------------------------------------
# History writer
# ---------------------------------------------------------------------------

def _write_history(
    conn: sqlite3.Connection,
    record_id: str,
    change_type: str,
    changed_fields: Optional[list[str]],
    record: dict,
    norm_cols: list[str],
    row_hash: str,
    synced_at: str,
) -> None:
    cols_sql = ", ".join(_q(c) for c in norm_cols)
    placeholders = ", ".join("?" * len(norm_cols))
    conn.execute(
        f"""INSERT INTO history
               (record_id, change_type, changed_fields, {cols_sql}, row_hash, synced_at)
            VALUES (?, ?, ?, {placeholders}, ?, ?)""",
        [
            record_id,
            change_type,
            json.dumps(changed_fields) if changed_fields is not None else None,
            *[record.get(col) for col in norm_cols],
            row_hash,
            synced_at,
        ],
    )


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

def sync_dataframe_to_sqlite(df: pd.DataFrame, config: ExcelSyncConfig) -> None:
    """
    Sync a pre-loaded DataFrame into SQLite.

    Use this when you need to preprocess the data before syncing (e.g. add a
    synthetic ID column).  ``sync_excel_to_sqlite`` calls this internally after
    reading the Excel file.
    """
    now = datetime.now(timezone.utc).isoformat()

    df = df.copy()
    df.dropna(how="all", inplace=True)
    norm_col_map = {col: _norm(col) for col in config.columns}
    df.rename(columns=norm_col_map, inplace=True)

    norm_cols = [_norm(c) for c in config.columns]
    id_col = _norm(config.id_column)

    # Deduplicate on ID — keep last occurrence, warn if any dupes found
    dupes = df[id_col].dropna().duplicated(keep="last").sum()
    if dupes:
        print(f"WARNING: {dupes} duplicate ID(s) found — keeping last occurrence of each.")
    df.drop_duplicates(subset=[id_col], keep="last", inplace=True)

    print(f"Loaded {len(df)} rows.")

    # --- Open DB ---
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        _init_db(conn, norm_cols, id_col)

        active = {
            row[id_col]: dict(row)
            for row in conn.execute("SELECT * FROM records WHERE is_active = 1").fetchall()
        }
        inactive = {
            row[id_col]: dict(row)
            for row in conn.execute("SELECT * FROM records WHERE is_active = 0").fetchall()
        }

        stats = {"added": 0, "modified": 0, "removed": 0, "restored": 0, "unchanged": 0}
        source_ids: set[str] = set()

        for _, row in df.iterrows():
            record = {col: _normalize_value(row[col]) for col in norm_cols}
            record_id = record.get(id_col)

            if not record_id:
                continue

            source_ids.add(record_id)
            row_hash = _compute_hash(record, norm_cols)

            if record_id in active:
                if active[record_id]["row_hash"] == row_hash:
                    stats["unchanged"] += 1
                    continue

                changed = [
                    col for col in norm_cols
                    if _normalize_value(active[record_id].get(col)) != record.get(col)
                ]
                set_clause = ", ".join(f"{_q(col)} = ?" for col in norm_cols)
                conn.execute(
                    f"UPDATE records SET {set_clause}, row_hash = ?, updated_at = ? WHERE {_q(id_col)} = ?",
                    [*[record[col] for col in norm_cols], row_hash, now, record_id],
                )
                _write_history(conn, record_id, "MODIFIED", changed, record, norm_cols, row_hash, now)
                stats["modified"] += 1

            elif record_id in inactive:
                set_clause = ", ".join(f"{_q(col)} = ?" for col in norm_cols)
                conn.execute(
                    f"UPDATE records SET {set_clause}, row_hash = ?, updated_at = ?, is_active = 1 WHERE {_q(id_col)} = ?",
                    [*[record[col] for col in norm_cols], row_hash, now, record_id],
                )
                _write_history(conn, record_id, "RESTORED", None, record, norm_cols, row_hash, now)
                stats["restored"] += 1

            else:
                placeholders = ", ".join("?" * len(norm_cols))
                conn.execute(
                    f"INSERT INTO records ({', '.join(_q(c) for c in norm_cols)}, row_hash, created_at, updated_at, is_active) "
                    f"VALUES ({placeholders}, ?, ?, ?, 1)",
                    [*[record[col] for col in norm_cols], row_hash, now, now],
                )
                _write_history(conn, record_id, "ADDED", None, record, norm_cols, row_hash, now)
                stats["added"] += 1

        for record_id in set(active.keys()) - source_ids:
            conn.execute(
                f"UPDATE records SET is_active = 0, updated_at = ? WHERE {_q(id_col)} = ?",
                [now, record_id],
            )
            old = active[record_id]
            _write_history(
                conn, record_id, "REMOVED", None,
                {col: old.get(col) for col in norm_cols},
                norm_cols, old["row_hash"], now,
            )
            stats["removed"] += 1

        conn.commit()

    print(
        f"\nSync complete:\n"
        f"  Added:     {stats['added']}\n"
        f"  Modified:  {stats['modified']}\n"
        f"  Restored:  {stats['restored']}\n"
        f"  Removed:   {stats['removed']}\n"
        f"  Unchanged: {stats['unchanged']}\n"
        f"\nDatabase: {config.db_path}"
    )


def sync_excel_to_sqlite(config: ExcelSyncConfig) -> None:
    print(f"Reading '{config.excel_path.name}' (sheet='{config.sheet_name}', header row={config.header_row})...")
    try:
        df = pd.read_excel(
            config.excel_path,
            sheet_name=config.sheet_name,
            usecols=config.columns,
            header=config.header_row,
            engine="openpyxl",
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {config.excel_path}")
    except ValueError as e:
        raise ValueError(f"{e}. Check that column names and header_row are correct.") from e
    except Exception as e:
        raise RuntimeError(f"ERROR reading Excel: {e}") from e

    sync_dataframe_to_sqlite(df, config)
