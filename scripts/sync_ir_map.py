"""
sync_ir_map.py
--------------
Syncs the Interest Rates Map Excel into ir_map.db (project root).

Usage:
    python scripts/sync_ir_map.py

NOTE: header_row defaults to 2 (same layout as HF map). If the script
errors on column names, check the actual row index of the header in
the Excel sheet and adjust header_row below.
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

# ---------------------------------------------------------------------------
# Config — adjust header_row if the sheet layout ever changes
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Interest Rates Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "ir_map.db",
    sheet_name  = "People Moves",
    header_row  = 2,
    id_column   = "ID",
    columns     = [
        "Name",
        "Group",
        "Function",
        "Current Firm",
        "Current Title",
        "Date Joined",
        "Current Location",
        "Former Firm",
        "Former Title",
        "Date Left",
        "Former Location",
        "Note",
        "Most Recent Date",
        "HF ID",
        "ID",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
