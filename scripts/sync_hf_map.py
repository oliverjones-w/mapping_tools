"""
sync_hf_map.py
--------------
Syncs the Hedge Fund Map Excel into hf_map.db (project root).

Usage:
    python scripts/sync_hf_map.py
"""

from pathlib import Path
from excel_sync_core import ExcelSyncConfig, sync_excel_to_sqlite

# ---------------------------------------------------------------------------
# Config — adjust header_row if the sheet layout ever changes
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG = ExcelSyncConfig(
    excel_path  = Path(r"K:\Market Maps\Hedge Fund Map (K).xlsm"),
    db_path     = PROJECT_ROOT / "hf_map.db",
    sheet_name  = "Master",
    header_row  = 2,           # row index 2 = 3rd row is the header
    id_column   = "ID",
    columns     = [
        "ID",
        "Firm",
        "Name",
        "Title",
        "Location",
        "Function",
        "Strategy",
        "Products",
        "Reports To",
    ],
)

if __name__ == "__main__":
    sync_excel_to_sqlite(CONFIG)
