import json
import csv
import os
import glob
import shutil
from datetime import date, datetime
from typing import Dict, Any, List, Optional

# --- Configuration ---

# --- Define Paths ---

# === Central Config Directory ===
CONFIG_DIR = r"/mnt/c/obsidian-vault/config"

# === Data Extraction Directory ===
BBG_EXTRACTION_ROOT = r"/mnt/c/data_extractions/bbg_extraction"

# === Windows Paths (Commented Out) ===
# CONFIG_DIR = r"C:\obsidian-vault\config"
# BBG_EXTRACTION_ROOT = r"C:\data_extractions\bbg_extraction"

# --- Define File Paths ---
MASTER_PERSONS_FILE = os.path.join(CONFIG_DIR, 'master_names.json')
FIRM_ALIASES_FILE = os.path.join(CONFIG_DIR, 'firm_aliases.json')

# --- Dynamic Paths (These work for both OS) ---
# The folder where you will drop new .csv files
NEW_DATA_DIRECTORY = os.path.join(BBG_EXTRACTION_ROOT, "new")

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

def load_firm_aliases_map() -> Optional[tuple[Dict[str, str], Dict[str, str], set]]:
    """
    Loads the firm aliases JSON and builds three maps.
    1. alias_map: 'alias' -> 'canonical_name' (for data comparison)
    2. id_map: 'alias' -> 'id' (for folder organization)
    3. blacklist_set: a 'normalized_alias' -> True set (for fast skipping)
    """
    if not os.path.exists(FIRM_ALIASES_FILE):
        print(f"Error: Firm aliases file not found: {FIRM_ALIASES_FILE}")
        return None, None, None
        
    print(f"Loading firm aliases from {FIRM_ALIASES_FILE}...")
    try:
        with open(FIRM_ALIASES_FILE, 'r', encoding='utf-8') as f:
            firm_list = json.load(f)
            
        if not isinstance(firm_list, list):
            print(f"Error: {FIRM_ALIASES_FILE} is not a JSON list as expected.")
            return None, None, None

        alias_map: Dict[str, str] = {}
        id_map: Dict[str, str] = {}
        blacklist_set: set = set() # <-- NEW SET
        
        for firm_obj in firm_list:
            canonical_name = firm_obj.get("canonical")
            firm_id = firm_obj.get("id")
            
            if not canonical_name or not firm_id:
                print(f"Warning: Skipping firm entry with missing 'canonical' or 'id'.")
                continue
                
            aliases = firm_obj.get("aliases", [])
            platforms = firm_obj.get("platforms", [])
            
            # --- NEW BLACKLIST LOGIC ---
            # Add all blacklisted names from this firm to the global set
            blacklist_names = firm_obj.get("blacklist", [])
            for name in blacklist_names:
                if not name: continue
                blacklist_set.add(normalize_string(name))
            # --- END NEW BLACKLIST LOGIC ---
            
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
        print(f"Loaded {len(blacklist_set)} names into global blacklist_set.") # <-- NEW
        return alias_map, id_map, blacklist_set
        
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {FIRM_ALIASES_FILE}.")
        return None, None, None
    except Exception as e:
        print(f"An error occurred loading {FIRM_ALIASES_FILE}: {e}")
        return None, None, None


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


def process_one_file(filepath: str, person_map: Dict[str, List[Dict[str, Any]]], alias_map: Dict[str, str], blacklist_set: set) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
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
    found_additions: List[Dict[str, Any]] = []
    processed_count = 0
    match_count = 0
    discrepancy_count = 0

    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                processed_count += 1
                
                # --- NEW: BLACKLIST CHECK ---
                # Get the company name from the new file
                new_company_name = row.get('Company')
                
                # Check if the company is blacklisted
                if new_company_name and (normalize_string(new_company_name) in blacklist_set):
                    continue # Skip this row entirely
                # --- END BLACKLIST CHECK ---
                
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
                    # --- BEGIN UPDATED MATCH/DISCREPANCY LOGIC ---
                    person_discrepancies = {}
                    has_discrepancy = False 
                    
                    for new_col, master_key in COLUMN_MAPPING.items():
                        if new_col == 'Name': continue
                        new_value = row.get(new_col)
                        
                        if new_col == 'Company':
                            # We already know new_value (new_company_name) is not blacklisted
                            normalized_new_company = normalize_string(new_value)
                            canonical_new_company = alias_map.get(normalized_new_company)
                            
                            confirmed_firm_matches = [] 
                            
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
                                    confirmed_firm_matches.append(master_record)
                            
                            if confirmed_firm_matches:
                                found_matches.extend(confirmed_firm_matches)
                                match_count += 1
                            
                            else:
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
                        
                        # (You can add 'elif' blocks here for 'Title', 'Location' etc. if needed)

                    if has_discrepancy:
                        discrepancy_count += 1
                        first_master_record = master_records_list[0]
                        all_master_ids = [mr.get('ID', 'N/A') for mr in master_records_list]
                        found_discrepancies.append({
                            'Name': first_master_record['Name'],
                            'Master_Record_UIDs': all_master_ids,
                            'Discrepancies': person_discrepancies
                        })
                    # --- END OF UPDATED MATCH/DISCREPANCY LOGIC ---
                        
                else:
                    # --- BEGIN NEW ADDITIONS LOGIC ---
                    # This code is only reached if:
                    # 1. The company was NOT in the blacklist
                    # 2. The person's name was NOT in the master_map
                    
                    # We can re-use new_company_name from the blacklist check
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
    print(f"Processed rows:                      {processed_count}")
    print(f"Found matches (Name + Firm):         {match_count}")
    print(f"Persons with firm discrepancies:   {discrepancy_count}")
    print(f"New persons found (at verified firms): {len(found_additions)}")
    print("--------------------------------" + "-" * len(os.path.basename(filepath)))
    
    return found_matches, found_discrepancies, found_additions


def main():
    # Load maps once
    alias_map, id_map, blacklist_set = load_firm_aliases_map()
    master_map = load_master_persons_map()
    
    if not master_map or not alias_map or not id_map or blacklist_set is None:
        print("Critical Error: Could not load master persons map, firm alias maps, or blacklist. Exiting.")
        return 1
        
    if not os.path.exists(NEW_DATA_DIRECTORY):
        print(f"Error: 'new' data directory not found: {NEW_DATA_DIRECTORY}")
        return 1
        
    # --- NEW FILE DISCOVERY LOGIC ---
    
    files_to_process = []
    firms_with_new_files = set()
    
    # 1. Get all files from the "new" folder
    new_csvs = glob.glob(os.path.join(NEW_DATA_DIRECTORY, '*.csv'))
    
    for file_path in new_csvs:
        files_to_process.append(file_path)
        # Find the firm_id for this file to prevent re-running it
        file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
        firm_identifier = file_name_without_ext.split('_')[0]
        normalized_base_name = normalize_string(firm_identifier)
        archive_folder_name = id_map.get(normalized_base_name, firm_identifier)
        if archive_folder_name:
            firms_with_new_files.add(archive_folder_name)
    
    # 2. Get all existing firm IDs from the extraction root
    all_firm_folders = [
        d for d in os.listdir(BBG_EXTRACTION_ROOT)
        if os.path.isdir(os.path.join(BBG_EXTRACTION_ROOT, d)) and d != 'new'
    ]
    
    # 3. Find the latest archive file for firms *without* a new file
    for firm_id in all_firm_folders:
        if firm_id in firms_with_new_files:
            print(f"Skipping archive check for {firm_id} (new file found).")
            continue
            
        archive_folder = os.path.join(BBG_EXTRACTION_ROOT, firm_id, "archive")
        if not os.path.exists(archive_folder):
            continue
            
        archive_files = glob.glob(os.path.join(archive_folder, "*.csv"))
        if archive_files:
            latest_archive_file = max(archive_files, key=os.path.getmtime)
            files_to_process.append(latest_archive_file)
            print(f"Queueing latest archive file for {firm_id}: {os.path.basename(latest_archive_file)}")

    # --- END NEW FILE DISCOVERY LOGIC ---

    if not files_to_process:
        print(f"No .csv files found in {NEW_DATA_DIRECTORY} or archives. Exiting.")
        return 0
        
    print(f"\nFound {len(files_to_process)} total files to process (new and re-run)...")

    total_files_processed = 0
    total_new_discrepancies = 0
    total_new_additions = 0
    total_new_matches = 0
    
    # --- MAIN PROCESSING LOOP ---
    for csv_file_path in files_to_process:
        
        # 1. PROCESS NEW FILE
        print(f"\n--- Processing: {csv_file_path} ---")
        matches, new_discrepancies_json, new_additions_json = process_one_file(
            csv_file_path, master_map, alias_map, blacklist_set
        )
        
        # 2. GET PATHS
        csv_filename = os.path.basename(csv_file_path)
        file_name_without_ext = os.path.splitext(csv_filename)[0]
        
        is_new_file = os.path.normpath(os.path.dirname(csv_file_path)) == os.path.normpath(NEW_DATA_DIRECTORY)
        
        if is_new_file:
            firm_identifier = file_name_without_ext.split('_')[0]
        else:
            firm_identifier = os.path.basename(os.path.dirname(os.path.dirname(csv_file_path)))
        
        normalized_base_name = normalize_string(firm_identifier)
        archive_folder_name = id_map.get(normalized_base_name, firm_identifier)
        
        if not archive_folder_name:
            print(f"Warning: Could not find firm ID for '{firm_identifier}'. Skipping file '{csv_filename}'.")
            continue
            
        firm_root_folder = os.path.join(BBG_EXTRACTION_ROOT, archive_folder_name)
        firm_discrepancy_folder = os.path.join(firm_root_folder, "discrepancies")
        firm_archive_folder = os.path.join(firm_root_folder, "archive")
        firm_additions_folder = os.path.join(firm_root_folder, "additions")
        firm_confirmed_folder = os.path.join(firm_root_folder, "confirmed_matches")
        
        for folder in [firm_discrepancy_folder, firm_archive_folder, firm_additions_folder, firm_confirmed_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        # 3. WRITE DISCREPANCIES (OVERWRITE)
        master_log_path = os.path.join(firm_discrepancy_folder, f"{archive_folder_name}_discrepancies.csv")
        discrepancy_headers = [
            'Name', 'Master_Record_UIDs', 'Discrepancy_Field', 'New_File_Value', 
            'Master_File_Values', 'Alias_Check_Info', 'Source_File', 
            'Status', 'First_Seen', 'Last_Seen'
        ]
        current_findings_list = list(flatten_discrepancies(new_discrepancies_json).values())
        
        try:
            with open(master_log_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=discrepancy_headers)
                writer.writeheader()
                if current_findings_list:
                    writer.writerows(current_findings_list)
            print(f"Successfully saved {len(current_findings_list)} discrepancies to {master_log_path} (overwrite)")
            total_new_discrepancies += len(current_findings_list)
        except Exception as e:
            print(f"Error writing master discrepancy log {master_log_path}: {e}")

        
        # 4. WRITE ADDITIONS (OVERWRITE)
        master_additions_log_path = os.path.join(firm_additions_folder, f"{archive_folder_name}_additions.csv")
        additions_headers = [
            'Name', 'Company', 'Canonical_Company', 'Title', 
            'Location', 'Focus', 'Source_File', 'First_Seen'
        ]
        today_str = str(date.today())
        for addition_row in new_additions_json:
            addition_row['First_Seen'] = today_str

        try:
            with open(master_additions_log_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=additions_headers)
                writer.writeheader()
                if new_additions_json:
                    writer.writerows(new_additions_json)
            print(f"Successfully saved {len(new_additions_json)} additions to {master_additions_log_path} (overwrite)")
            total_new_additions += len(new_additions_json)
        except Exception as e:
            print(f"Error writing additions CSV {master_additions_log_path}: {e}")
        
        # 5. WRITE CONFIRMED MATCHES (OVERWRITE)
        if matches:
            master_matches_log_path = os.path.join(firm_confirmed_folder, f"{archive_folder_name}_matches.csv")
            matches_headers = list(matches[0].keys())
            
            try:
                with open(master_matches_log_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=matches_headers)
                    writer.writeheader()
                    writer.writerows(matches)
                print(f"Successfully saved {len(matches)} confirmed matches to {master_matches_log_path} (overwrite)")
                total_new_matches += len(matches)
            except Exception as e:
                print(f"Error writing confirmed matches CSV {master_matches_log_path}: {e}")
        
        # --- MODIFIED: 6. ARCHIVE THE PROCESSED SOURCE FILE (if it was new) ---
        if is_new_file:
            try:
                _ , ext = os.path.splitext(csv_filename)
                file_name_base = os.path.splitext(csv_filename)[0] # e.g., 'citadel_20251028'
                firm_part = file_name_base.split('_')[0]
                date_part = file_name_base.replace(f"{firm_part}_", "") # e.g., '20251028'
                
                if not date_part: # Handle case like 'citadel.csv'
                    raise ValueError("No date part in filename")
                
                # Try to parse YYYYMMDD and format as YYYY-MM-DD
                parsed_date = datetime.strptime(date_part, '%Y%m%d')
                new_archive_filename = f"{parsed_date.strftime('%Y-%m-%d')}{ext}" # e.g., '2025-10-28.csv'

            except ValueError:
                # Fallback: Can't parse date, just use the non-firm part
                # e.g., 'citadel_extra_info_file.csv' -> 'extra_info_file.csv'
                file_name_base = os.path.splitext(csv_filename)[0]
                firm_part = file_name_base.split('_')[0]
                archive_name_base = file_name_base.replace(f"{firm_part}_", "")
                _ , ext = os.path.splitext(csv_filename)
                
                if not archive_name_base: # Fallback for 'citadel.csv'
                    new_archive_filename = f"{datetime.now().strftime('%Y-%m-%d')}{ext}"
                else:
                    new_archive_filename = f"{archive_name_base}{ext}"

            # Now, move the file
            try:
                destination_path = os.path.join(firm_archive_folder, new_archive_filename)
                
                if os.path.exists(destination_path):
                    print(f"Warning: Archive file '{new_archive_filename}' already exists. Skipping move.")
                else:
                    shutil.move(csv_file_path, destination_path)
                    print(f"Successfully moved '{csv_filename}' to archive as '{new_archive_filename}'\n")
                
                total_files_processed += 1
            
            except Exception as e:
                print(f"Error moving file {csv_filename}: {e}\n")
        
        else:
            print(f"Successfully re-processed '{csv_filename}' from archive. No move needed.\n")
            total_files_processed += 1
            
    # --- Print summary ---
    print("\n--- Grand Total Summary ---")
    print(f"Processed {total_files_processed} files (new and re-runs).")
    print(f"Total Confirmed Matches found: {total_new_matches}")
    print(f"Total Discrepancies found:     {total_new_discrepancies}")
    print(f"Total Additions found:         {total_new_additions}")
    print("(All CSVs overwritten with these new totals)")

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
