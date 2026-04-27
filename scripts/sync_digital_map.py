"""
sync_digital_map.py
-------------------
Syncs the Digital Assets Map Excel into digital_map.db (project root).

Usage:
    python scripts/sync_digital_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Digital Assets Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "digital_map.db",
    sheet_name  = "Master",
    header_row  = 2,
    id_column   = "ID",
    columns     = [
        "ID",
        "Firm",
        "Name",
        "Title",
        "Location",
        "Mandate",
        "Status",
        "Tag",
        "Notes",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
