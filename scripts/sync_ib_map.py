"""
sync_ib_map.py
--------------
Syncs the Investment Banking Map Excel into ib_map.db (project root).

Usage:
    python scripts/sync_ib_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Investment Banking Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "ib_map.db",
    sheet_name  = "Master",
    header_row  = 0,
    id_column   = "ID",
    columns     = [
        "ID",
        "Firm",
        "Name",
        "Title",
        "Location",
        "Vertical",
        "Group",
        "Focus / Coverage",
        "Former Firm",
        "Notes",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
