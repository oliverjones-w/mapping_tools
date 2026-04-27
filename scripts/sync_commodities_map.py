"""
sync_commodities_map.py
-----------------------
Syncs the Commodities Map Excel into commodities_map.db (project root).

Usage:
    python scripts/sync_commodities_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Commodites Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "commodities_map.db",
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
        "Coverage",
        "Sector",
        "Products",
        "Reports To",
        "Prior Firm",
        "Note",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
