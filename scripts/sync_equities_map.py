"""
sync_equities_map.py
--------------------
Syncs the Equities Map Excel into equities_map.db (project root).

Usage:
    python scripts/sync_equities_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Equities Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "equities_map.db",
    sheet_name  = "Master",
    header_row  = 0,
    id_column   = "ID",
    columns     = [
        "ID",
        "Firm",
        "Name",
        "Title",
        "Region",
        "Location",
        "Function",
        "Group",
        "Products",
        "Focus",
        "Sector Coverage",
        "Client Coverage",
        "Reports To",
        "Prior Firm",
        "Notes",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
