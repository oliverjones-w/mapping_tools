import pandas as pd
import json
from pathlib import Path

# === CONFIG ===

# === WSL Paths (Active) ===
MASTER_MAP_PATH = Path(r"K:\Market Maps\Hedge Fund Map (K).xlsm")
JSON_OUTPUT_PATH = Path(r"C:\obsidian-vault\config\master_names.json")

SHEET_NAME = "Master"
COLUMNS_TO_EXPORT = [
    "ID",
    "Firm",
    "Name",
    "Title",
    "Location",
    "Function",
    "Strategy",
    "Products",
    "Reports To"
]

def convert_excel_to_json():
    """Reads the Master Excel Map (.xlsm) and converts it to the master_names.json file."""
    print(f"Reading from {MASTER_MAP_PATH.name} (sheet: {SHEET_NAME})...")
    
    try:
        df = pd.read_excel(
            MASTER_MAP_PATH,
            sheet_name=SHEET_NAME,
            usecols=COLUMNS_TO_EXPORT,
            header=2,  # skip first 2 header rows if needed
            engine="openpyxl"  # important for .xlsx/.xlsm
        )
    except FileNotFoundError:
        print(f"ERROR: Cannot find file: {MASTER_MAP_PATH}")
        return
    except ValueError as e:
        print(f"ERROR: {e}")
        print("Check that 'COLUMNS_TO_EXPORT' and 'header' index are correct.")
        return
    except Exception as e:
        print(f"An unexpected error occurred reading the Excel file: {e}")
        return

    print(f"Loaded {len(df)} rows from Excel.")
    
    # --- Data Cleaning ---
    df = df.where(pd.notnull(df), None)

    # --- Conversion ---
    records = df.to_dict(orient="records")

    # --- Save Output ---
    print(f"Saving {len(records)} records to {JSON_OUTPUT_PATH.name}...")
    try:
        with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print("\nSuccessfully updated master_names.json!")
    except Exception as e:
        print(f"ERROR: Could not write JSON file: {e}")

if __name__ == "__main__":
    convert_excel_to_json()
