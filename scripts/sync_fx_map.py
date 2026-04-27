"""
sync_fx_map.py
--------------
Syncs the FX Map Excel into fx_map.db (project root).

Usage:
    python scripts/sync_fx_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\FX Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "fx_map.db",
    sheet_name  = "Master",
    header_row  = 2,
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
        "Focus",
        "Category",
        "Coverage",
        "Products",
        "Reports To",
        "Prior Firm",
        "Notes",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
