import json
import csv
import os
import glob
import shutil
from datetime import date, datetime
from typing import Dict, Any, List, Optional

# --- Configuration ---

import os  # Make sure os is imported at the top of your script

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Assume the parent directory is the root of the project (e.g., 'c:\obsidian-vault')
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

# --- Define Paths ---

# === WSL Paths (Active) ===
MASTER_PERSONS_FILE = r"/mnt/c/obsidian-vault/config/master_names.json"
FIRM_ALIASES_FILE = r"/mnt/c/obsidian-vault/config/firm_aliases.json"
BBG_EXTRACTION_ROOT = r"/mnt/c/data_extractions/bbg_extraction"

# === Windows Paths (Commented Out) ===
# MASTER_PERSONS_FILE = r"C:\obsidian-vault\config\master_names.json"
# FIRM_ALIASES_FILE = r"C:\obsidian-vault\config\firm_aliases.json"
# BBG_EXTRACTION_ROOT = r"C:\data_extractions\bbg_extraction"

# --- Dynamic Paths (These work for both OS) ---

# The folder where you will drop new .csv files
NEW_DATA_DIRECTORY = os.path.join(BBG_EXTRACTION_ROOT, "new")

# Output file for all *matches* (will be saved in ROOT_DIR)
ALL_MATCHES_OUTPUT_FILE = os.path.join(ROOT_DIR, 'all_bbg_master_records.json')

# --- Column Mapping ---

# Define the mapping between columns in the new file and the master file
# Format: { 'New_File_Column_Name': 'Master_File_Key_Name' }
COLUMN_MAPPING = {
    'Company': 'Firm',
    'Title': 'Title',
    'Location': 'Location',
    'Focus': 'Focus'
}

# --- End Configuration ---

def normalize_string(s: Optional[str]) -> str:
    """Helper function to clean strings for comparison."""
    if s is None:
        return ""
    return s.strip().lower()

def load_master_persons_map() -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """
    Loads the master persons JSON and builds a name-to-record map.
    The map's value is a LIST of person objects to handle duplicate names.
    """
    if not os.path.exists(MASTER_PERSONS_FILE):
        print(f"Error: Master file not found: {MASTER_PERSONS_FILE}")
        return None

    print(f"Loading master persons from {MASTER_PERSONS_FILE}...")
    try:
        with open(MASTER_PERSONS_FILE, 'r', encoding='utf-8') as f:
            persons_list = json.load(f)
        
        # This map now holds a list of persons for each name
        person_map: Dict[str, List[Dict[str, Any]]] = {}
        total_persons = 0
        for p in persons_list:
            name = p.get('Name')
            if name:
                total_persons += 1
                normalized_name = normalize_string(name)
                if normalized_name not in person_map:
                    person_map[normalized_name] = [] # Initialize an empty list
                person_map[normalized_name].append(p) # Add the person to the list
        
        print(f"Loaded {total_persons} total persons, grouped into {len(person_map)} unique names.")
        return person_map
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {MASTER_PERSONS_FILE}.")
        return None
    except Exception as e:
        print(f"An error occurred loading {MASTER_PERSONS_FILE}: {e}")
        return None

def load_firm_aliases_map() -> Optional[tuple[Dict[str, str], Dict[str, str]]]:
    """
    Loads the firm aliases JSON and builds two maps.
    1. alias_map: 'alias' -> 'canonical_name' (for data comparison)
    2. id_map: 'alias' -> 'id' (for folder organization)
    """
    if not os.path.exists(FIRM_ALIASES_FILE):
        print(f"Error: Firm aliases file not found: {FIRM_ALIASES_FILE}")
        return None, None
        
    print(f"Loading firm aliases from {FIRM_ALIASES_FILE}...")
    try:
        with open(FIRM_ALIASES_FILE, 'r', encoding='utf-8') as f:
            firm_list = json.load(f)
            
        if not isinstance(firm_list, list):
            print(f"Error: {FIRM_ALIASES_FILE} is not a JSON list as expected.")
            return None, None

        alias_map: Dict[str, str] = {}
        id_map: Dict[str, str] = {}
        
        for firm_obj in firm_list:
            canonical_name = firm_obj.get("canonical")
            firm_id = firm_obj.get("id")
            
            if not canonical_name or not firm_id:
                print(f"Warning: Skipping firm entry with missing 'canonical' or 'id'.")
                continue
                
            aliases = firm_obj.get("aliases", [])
            platforms = firm_obj.get("platforms", [])
            
            # Create a single list of all possible names for this firm
            all_names_to_map = [canonical_name] + aliases + platforms

            for name in all_names_to_map:
                if not name: continue
                
                normalized_name = normalize_string(name)
                
                # Populate the alias_map (for discrepancy checking)
                if normalized_name not in alias_map:
                    alias_map[normalized_name] = canonical_name
                elif alias_map[normalized_name] != canonical_name:
                    print(f"Warning (alias_map): '{name}' maps to multiple canonical names. Using '{alias_map[normalized_name]}'.")

                # Populate the id_map (for folder organization)
                if normalized_name not in id_map:
                    id_map[normalized_name] = firm_id
                elif id_map[normalized_name] != firm_id:
                    print(f"Warning (id_map): '{name}' maps to multiple IDs. Using '{id_map[normalized_name]}'.")

        print(f"Loaded {len(alias_map)} aliases into alias_map.")
        print(f"Loaded {len(id_map)} aliases into id_map.")
        return alias_map, id_map
        
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {FIRM_ALIASES_FILE}.")
        return None, None
    except Exception as e:
        print(f"An error occurred loading {FIRM_ALIASES_FILE}: {e}")
        return None, None

def get_discrepancy_key(row_dict: Dict[str, Any]) -> tuple:
    """
    Creates a unique, hashable key for a single discrepancy row.
    We use the 4 most specific fields to identify a unique problem.
    """
    return (
        row_dict.get('Name'),
        row_dict.get('Master_Record_UIDs'),
        row_dict.get('Discrepancy_Field'),
        row_dict.get('New_File_Value')
    )

def flatten_discrepancies(discrepancies_json: List[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
    """
    Turns the JSON output from process_one_file into a key-to-row_data map.
    This allows for O(1) lookups.
    """
    flat_map = {}
    for d in discrepancies_json:
        base_name = d['Name']
        base_uids = ", ".join(d['Master_Record_UIDs'])
        for field, details in d['Discrepancies'].items():
            flat_row = {
                'Name': base_name,
                'Master_Record_UIDs': base_uids,
                'Discrepancy_Field': field,
                'New_File_Value': details.get('new_file_value', 'N/A'),
                'Master_File_Values': ", ".join(details.get('master_file_values', [])),
                'Alias_Check_Info': details.get('alias_check', 'N/A'),
                'Source_File': details.get('source_file', 'N/A'),
                'Status': 'Active', # Set default status
                'First_Seen': str(date.today()), # Set default date
                'Last_Seen': str(date.today())  # Set default date
            }
            # Use the standardized key
            key = get_discrepancy_key(flat_row)
            if key not in flat_map:
                flat_map[key] = flat_row
                
    return flat_map


def process_one_file(filepath: str, person_map: Dict[str, List[Dict[str, Any]]], alias_map: Dict[str, str]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Processes a single new data file and compares it against the master map.
    Returns a tuple of (found_matches, found_discrepancies, found_additions).
    """
    if not os.path.exists(filepath):
        print(f"Error: New data file not found: {filepath}")
        return [], [], []

    print(f"Processing new data from {filepath}...")
    
    found_matches: List[Dict[str, Any]] = []
    found_discrepancies: List[Dict[str, Any]] = []
    found_additions: List[Dict[str, Any]] = [] # <-- NEW LIST
    processed_count = 0
    match_count = 0
    discrepancy_count = 0

    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:  # 'utf-8-sig' handles potential BOM
            reader = csv.DictReader(f)
            
            for row in reader:
                processed_count += 1
                
                # --- BEGIN REVISED NAME LOGIC ---
                new_name = None
                if 'First Name' in row and 'Last Name' in row:
                    first = row.get('First Name', '').strip()
                    last = row.get('Last Name', '').strip()
                    if first or last:
                       new_name = f"{first} {last}".strip()
                if not new_name and 'Name' in row:
                    new_name = row.get('Name', '').strip()
                if not new_name:
                    print(f"Warning: Skipping row {processed_count} in {filepath} (no Name or First/Last Name).")
                    continue
                # --- END REVISED NAME LOGIC ---
                
                normalized_name = normalize_string(new_name)
                master_records_list = person_map.get(normalized_name) 
                
                # Check if this person exists in the master list
                if master_records_list:
                    # --- (EXISTING DISCREPANCY LOGIC) ---
                    # (This block is unchanged)
                    match_count += 1
                    found_matches.extend(master_records_list)
                    person_discrepancies = {}
                    has_discrepancy = False 
                    
                    for new_col, master_key in COLUMN_MAPPING.items():
                        if new_col == 'Name': continue
                        new_value = row.get(new_col)
                        
                        if new_col == 'Company':
                            normalized_new_company = normalize_string(new_value)
                            canonical_new_company = alias_map.get(normalized_new_company)
                            found_a_firm_match = False 
                            
                            for master_record in master_records_list:
                                master_firm = master_record.get('Firm')
                                normalized_master_firm = normalize_string(master_firm)
                                firm_matches = False
                                if canonical_new_company:
                                    if normalize_string(canonical_new_company) == normalized_master_firm:
                                        firm_matches = True
                                else:
                                    if normalized_new_company == normalized_master_firm:
                                        firm_matches = True
                                if firm_matches:
                                    found_a_firm_match = True
                                    break 
                            
                            if not found_a_firm_match:
                                has_discrepancy = True
                                all_master_firms = list(set([mr.get('Firm', 'N/A') for mr in master_records_list]))
                                alias_check_message = ""
                                if canonical_new_company:
                                    alias_check_message = f"'{new_value}' (canonical: '{canonical_new_company}') matched none of: {all_master_firms}"
                                else:
                                    alias_check_message = f"'{new_value}' (no alias found) matched none of: {all_master_firms}"

                                person_discrepancies[master_key] = {
                                    'new_file_value': new_value,
                                    'master_file_values': all_master_firms,
                                    'alias_check': alias_check_message,
                                    'source_file': os.path.basename(filepath)
                                }
                    
                    if has_discrepancy:
                        discrepancy_count += 1
                        first_master_record = master_records_list[0]
                        all_master_ids = [mr.get('ID', 'N/A') for mr in master_records_list]
                        found_discrepancies.append({
                            'Name': first_master_record['Name'],
                            'Master_Record_UIDs': all_master_ids,
                            'Discrepancies': person_discrepancies
                        })
                    # --- (END OF EXISTING DISCREPANCY LOGIC) ---
                        
                else:
                    # --- BEGIN NEW ADDITIONS LOGIC ---
                    # This person is NOT in our master_map.
                    # Check if they are at a verified firm.
                    new_company_name = row.get('Company')
                    normalized_new_company = normalize_string(new_company_name)
                    canonical_new_company = alias_map.get(normalized_new_company)
                    
                    # Only add them if their firm is in our alias map
                    if canonical_new_company:
                        addition_record = {
                            'Name': new_name,
                            'Company': new_company_name,
                            'Canonical_Company': canonical_new_company,
                            'Title': row.get('Title'),
                            'Location': row.get('Location'),
                            'Focus': row.get('Focus'),
                            'Source_File': os.path.basename(filepath)
                        }
                        found_additions.append(addition_record)
                    # --- END NEW ADDITIONS LOGIC ---

    except FileNotFoundError:
        print(f"Error: File not found: {filepath}")
        return [], [], []
    except Exception as e:
        print(f"An error occurred while processing {filepath}: {e}")
        return [], [], []

    # --- Print summary for this file ---
    print(f"\n--- Summary for {os.path.basename(filepath)} ---")
    print(f"Processed rows:                 {processed_count}")
    print(f"Found matches in master list:   {match_count}")
    print(f"Persons with firm discrepancies: {discrepancy_count}")
    print(f"New persons found (at verified firms): {len(found_additions)}") # <-- NEW
    print("--------------------------------" + "-" * len(os.path.basename(filepath)))
    
    return found_matches, found_discrepancies, found_additions # <-- NEW


def main():
    # Load maps once
    alias_map, id_map = load_firm_aliases_map()
    master_map = load_master_persons_map()
    
    if not master_map or not alias_map or not id_map:
        print("Critical Error: Could not load master persons map or firm alias maps. Exiting.")
        return
        
    if not os.path.exists(NEW_DATA_DIRECTORY):
        print(f"Error: 'new' data directory not found: {NEW_DATA_DIRECTORY}")
        return
        
    csv_files_to_process = glob.glob(os.path.join(NEW_DATA_DIRECTORY, '*.csv'))
    
    if not csv_files_to_process:
        print(f"No .csv files found in {NEW_DATA_DIRECTORY}. Exiting.")
        return
        
    print(f"\nFound {len(csv_files_to_process)} .csv files to process...")

    all_found_matches: List[Dict[str, Any]] = []
    total_files_processed = 0
    total_new_discrepancies = 0
    total_resolved_discrepancies = 0
    total_new_additions = 0 # <-- NEW
    
    # Process each file
    for csv_file_path in csv_files_to_process:
        
        # 1. PROCESS NEW FILE
        matches, new_discrepancies_json, new_additions_json = process_one_file(csv_file_path, master_map, alias_map) # <-- NEW
        all_found_matches.extend(matches)
        
        # --- BEGIN RECONCILIATION LOGIC ---
        
        # 2. GET PATHS
        csv_filename = os.path.basename(csv_file_path)
        file_name_without_ext = os.path.splitext(csv_filename)[0]
        firm_identifier = file_name_without_ext.split('_')[0]
        normalized_base_name = normalize_string(firm_identifier)
        archive_folder_name = id_map.get(normalized_base_name, firm_identifier)
        
        firm_root_folder = os.path.join(BBG_EXTRACTION_ROOT, archive_folder_name)
        firm_discrepancy_folder = os.path.join(firm_root_folder, "discrepancies")
        firm_archive_folder = os.path.join(firm_root_folder, "archive")
        firm_additions_folder = os.path.join(firm_root_folder, "additions") # <-- NEW
        
        # Create all folders
        for folder in [firm_discrepancy_folder, firm_archive_folder, firm_additions_folder]: # <-- NEW
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        # This is the single, persistent log file for the firm
        master_log_path = os.path.join(firm_discrepancy_folder, f"{archive_folder_name}_discrepancies.csv")
        
        # This is a map of {key: row_dict} for all discrepancies found in the NEW file
        current_findings_map = flatten_discrepancies(new_discrepancies_json)
        
        # 3. LOAD OLD MASTER DISCREPANCY LOG
        updated_log_map = {}
        headers = [
            'Name', 'Master_Record_UIDs', 'Discrepancy_Field', 'New_File_Value', 
            'Master_File_Values', 'Alias_Check_Info', 'Source_File', 
            'Status', 'First_Seen', 'Last_Seen'
        ]
        
        if os.path.exists(master_log_path):
            print(f"Loading existing master log: {master_log_path}")
            try:
                with open(master_log_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        key = get_discrepancy_key(row)
                        updated_log_map[key] = row
            except Exception as e:
                print(f"Warning: Could not read {master_log_path}. Will create a new one. Error: {e}")

        # 4. RECONCILE DISCREPANCIES
        # (This block is unchanged)
        today_str = str(date.today())
        temp_new_discrepancies = 0
        temp_resolved_discrepancies = 0
        
        for key, new_row in current_findings_map.items():
            if key in updated_log_map:
                updated_log_map[key]['Status'] = 'Active'
                updated_log_map[key]['Last_Seen'] = today_str
                updated_log_map[key]['Master_File_Values'] = new_row['Master_File_Values']
                updated_log_map[key]['Alias_Check_Info'] = new_row['Alias_Check_Info']
                updated_log_map[key]['Source_File'] = new_row['Source_File']
            else:
                updated_log_map[key] = new_row
                temp_new_discrepancies += 1

        old_keys_to_check = list(updated_log_map.keys())
        for key in old_keys_to_check:
            if key not in current_findings_map:
                if updated_log_map[key]['Status'] == 'Active':
                    updated_log_map[key]['Status'] = 'Resolved'
                    temp_resolved_discrepancies += 1
        
        # 5. WRITE THE NEW MASTER DISCREPANCY LOG
        # (This block is unchanged)
        if updated_log_map:
            try:
                sorted_log_rows = sorted(
                    updated_log_map.values(), 
                    key=lambda r: (r['Status'] != 'Active', r['Name'])
                )
                with open(master_log_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(sorted_log_rows)
                
                print(f"Reconciliation complete for {archive_folder_name}:")
                print(f"  - {temp_new_discrepancies} new discrepancies logged.")
                print(f"  - {temp_resolved_discrepancies} discrepancies marked as resolved.")
                print(f"  - Master log saved: {master_log_path}")
                total_new_discrepancies += temp_new_discrepancies
                total_resolved_discrepancies += temp_resolved_discrepancies

            except Exception as e:
                print(f"Error writing master discrepancy log {master_log_path}: {e}")
        
        # --- BEGIN NEW SECTION: 6. PROCESS ADDITIONS ---
        
        # Define the master additions log path
        master_additions_log_path = os.path.join(firm_additions_folder, f"{archive_folder_name}_additions.csv")

        additions_headers = [
            'Name', 'Company', 'Canonical_Company', 'Title', 
            'Location', 'Focus', 'Source_File', 'First_Seen'
        ]
        
        # Load existing additions to prevent duplicates
        existing_additions = set()
        if os.path.exists(master_additions_log_path):
            try:
                with open(master_additions_log_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_additions.add(normalize_string(row.get('Name')))
            except Exception as e:
                print(f"Warning: Could not read {master_additions_log_path}. Error: {e}")

        # Filter for only new additions
        new_additions_to_write = []
        for addition in new_additions_json:
            norm_name = normalize_string(addition['Name'])
            if norm_name not in existing_additions:
                addition['First_Seen'] = today_str # Add the date
                new_additions_to_write.append(addition)
                existing_additions.add(norm_name) # Add to set to de-dupe from same file
        
        # Append new additions to the log
        if new_additions_to_write:
            file_exists = os.path.exists(master_additions_log_path)
            try:
                # Open in 'a' (append) mode
                with open(master_additions_log_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=additions_headers)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(new_additions_to_write)
                
                print(f"Successfully appended {len(new_additions_to_write)} new additions to {master_additions_log_path}")
                total_new_additions += len(new_additions_to_write)
            
            except Exception as e:
                print(f"Error appending to additions CSV {master_additions_log_path}: {e}")
        
        # --- END NEW SECTION ---

        # 7. ARCHIVE THE PROCESSED SOURCE FILE (was 6)
        try:
            now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            _ , ext = os.path.splitext(csv_filename)
            new_archive_filename = f"{now_str}{ext}"
            destination_path = os.path.join(firm_archive_folder, new_archive_filename)
            shutil.move(csv_file_path, destination_path)
            print(f"Successfully moved '{csv_filename}' to archive as '{new_archive_filename}'\n")
            total_files_processed += 1
        except Exception as e:
            print(f"Error moving file {csv_filename}: {e}\n")
            
        # --- END RECONCILIATION LOGIC ---

    # --- Save consolidated results (for matches only) ---
    unique_matches_map = {}
    try:
        unique_matches_map = {m.get('ID', m.get('Name')): m for m in all_found_matches}
        unique_matches_list = list(unique_matches_map.values())
        
        if unique_matches_list:
            with open(ALL_MATCHES_OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(unique_matches_list, f, indent=2)
            print(f"\nSuccessfully saved {len(unique_matches_list)} unique matching master records to {ALL_MATCHES_OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving all-matches file: {e}")
        
    # --- Print summary ---
    # --- Print summary ---
    print("\n--- Grand Total Summary ---")
    print(f"Processed {total_files_processed} files.")
    print(f"Total unique master records found:  {len(unique_matches_map)}")
    print(f"Total NEW discrepancies found:      {total_new_discrepancies} (appended to logs)")
    print(f"Total RESOLVED discrepancies found: {total_resolved_discrepancies} (updated in logs)")
    print(f"Total NEW additions found:          {total_new_additions} (appended to logs)")  # <-- UPDATED

    # Return a conventional exit code (0 = success)
    return 0


if __name__ == "__main__":
    import sys
    import traceback

    try:
        exit_code = main()
        # If main() didn't explicitly return, normalize to 0
        if exit_code is None:
            exit_code = 0
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)  # 128 + SIGINT
    except Exception as e:
        print(f"\nUnhandled error: {e}")
        traceback.print_exc()
        sys.exit(1)
