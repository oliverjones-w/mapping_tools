import json
import csv
import os
import glob
import shutil
from datetime import date, datetime
from typing import Dict, Any, List, Optional
import warnings

# --- Configuration ---

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# --- Define Paths ---

# === Central Config Directory ===
CONFIG_DIR = r"C:\obsidian-vault\config"

# === Data Extraction Directory ===
BBG_EXTRACTION_ROOT = r"C:\data_extractions\bbg_extraction"

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

def load_master_persons_map() -> Optional[tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]]:
    """
    Loads the master persons JSON and builds a name-to-record map.
    The map's value is a LIST of person objects to handle duplicate names.
    It also returns the complete list of persons.
    """
    if not os.path.exists(MASTER_PERSONS_FILE):
        print(f"Error: Master file not found: {MASTER_PERSONS_FILE}")
        return None, None # <-- Returns 2 Nones

    print(f"Loading master persons from {MASTER_PERSONS_FILE}...")
    try:
        with open(MASTER_PERSONS_FILE, 'r', encoding='utf-8') as f:
            persons_list = json.load(f)
        
        person_map: Dict[str, List[Dict[str, Any]]] = {}
        total_persons = 0
        
        for p in persons_list:
            p['Source_Found'] = False 
            
            name = p.get('Name')
            if name:
                total_persons += 1
                normalized_name = normalize_string(name)
                if normalized_name not in person_map:
                    person_map[normalized_name] = []
                person_map[normalized_name].append(p)
        
        print(f"Loaded {total_persons} total persons, grouped into {len(person_map)} unique names.")
        return person_map, persons_list # <-- Returns 2 values here
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {MASTER_PERSONS_FILE}.")
        return None, None # <-- Returns 2 Nones
    except Exception as e:
        print(f"An error occurred loading {MASTER_PERSONS_FILE}: {e}")
        return None, None # <-- Returns 2 Nones
    
def load_firm_aliases_map() -> Optional[tuple[Dict[str, str], Dict[str, str], Dict[str, set]]]:
    """
    Loads the firm aliases JSON and builds maps, including a per-firm blacklist map.
    Returns: (alias_map, id_map, firm_blacklist_map)
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
        firm_blacklist_map: Dict[str, set] = {} # <-- NEW PER-FIRM BLACKLIST MAP
        total_blacklist_entries = 0 # For reporting

        for firm_obj in firm_list:
            canonical_name = firm_obj.get("canonical")
            firm_id = firm_obj.get("id")

            if not canonical_name or not firm_id:
                print(f"Warning: Skipping firm entry with missing 'canonical' or 'id'.")
                continue

            # --- NEW PER-FIRM BLACKLIST LOGIC ---
            firm_blacklist_map[firm_id] = set()
            blacklist_names = firm_obj.get("blacklist", [])
            for name in blacklist_names:
                if name:
                    firm_blacklist_map[firm_id].add(normalize_string(name))
                    total_blacklist_entries += 1
            # --- END NEW PER-FIRM BLACKLIST LOGIC ---

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
        print(f"Loaded {total_blacklist_entries} total blacklist entries across {len(firm_blacklist_map)} firms.") # <-- UPDATED REPORTING
        return alias_map, id_map, firm_blacklist_map # <-- UPDATED RETURN
        
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
        with open(filepath, 'r', encoding='utf-8-sig', errors='replace') as f:
            # Custom CSV Reader to clean headers (from previous fix)
            reader = csv.reader(f)
            headers = [h.strip() for h in next(reader)]
            reader = csv.DictReader(f, fieldnames=headers)
            
            for row in reader:
                processed_count += 1
                
                # --- NEW: Robustly extract and clean company name ---
                new_company_name = str(row.get('Company', '')).strip()

                # --- FIRM-SPECIFIC BLACKLIST CHECK ---
                # This uses the blacklist set provided by the main function (for this firm only)
                if new_company_name and (normalize_string(new_company_name) in blacklist_set):
                    continue
                # --- END BLACKLIST CHECK ---
                
                # --- BEGIN REVISED NAME LOGIC ---
                new_name = ""
                
                # 1. Try First Name + Last Name, ensuring safe string conversion
                first = str(row.get('First Name', '')).strip()
                last = str(row.get('Last Name', '')).strip()
                
                if first or last:
                    new_name = f"{first} {last}".strip()
                    
                # 2. Try combined 'Name' field (if it existed)
                if not new_name:
                    new_name = str(row.get('Name', '')).strip()
                    
                # 3. Skip if name is still empty (Data Quality Check)
                if not new_name:
                    print(f"Warning: Skipping row {processed_count} in {os.path.basename(filepath)} (name fields are empty).")
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
                            normalized_new_company = normalize_string(new_company_name)
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
                                for record in confirmed_firm_matches:
                                    record['Source_Found'] = True 
                            
                            else:
                                has_discrepancy = True
                                all_master_firms = list(set([mr.get('Firm', 'N/A') for mr in master_records_list]))
                                alias_check_message = ""
                                if canonical_new_company:
                                    alias_check_message = f"'{new_company_name}' (canonical: '{canonical_new_company}') matched none of: {all_master_firms}"
                                else:
                                    alias_check_message = f"'{new_company_name}' (no alias found) matched none of: {all_master_firms}"

                                person_discrepancies[master_key] = {
                                    'new_file_value': new_company_name,
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
                    # --- END OF UPDATED MATCH/DISCREPANCY LOGIC ---
                        
                else:
                    # --- BEGIN NEW ADDITIONS LOGIC ---
                    normalized_new_company = normalize_string(new_company_name)
                    canonical_new_company = alias_map.get(normalized_new_company)
                    
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
    
    return found_matches, found_discrepancies, found_additions

def main():
    # Load maps once
    # --- MODIFIED: Receives firm_blacklist_map ---
    alias_map, id_map, firm_blacklist_map = load_firm_aliases_map()
    master_map, all_persons_list = load_master_persons_map() 
    
    # --- MODIFIED: Check for firm_blacklist_map ---
    if not master_map or not alias_map or not id_map or firm_blacklist_map is None or all_persons_list is None:
        print("Critical Error: Could not load master data or alias maps. Exiting.")
        return 1
        
    if not os.path.exists(NEW_DATA_DIRECTORY):
        print(f"Error: 'new' data directory not found: {NEW_DATA_DIRECTORY}")
        return 1
        
    # --- FILE DISCOVERY remains the same ---
    files_to_process = []
    firms_with_new_files = set()
    
    new_csvs = glob.glob(os.path.join(NEW_DATA_DIRECTORY, '*.csv'))
    
    for file_path in new_csvs:
        files_to_process.append(file_path)
        file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
        firm_identifier = file_name_without_ext.split('_')[0]
        normalized_base_name = normalize_string(firm_identifier)
        archive_folder_name = id_map.get(normalized_base_name, firm_identifier)
        if archive_folder_name:
            firms_with_new_files.add(archive_folder_name)
    
    all_firm_folders = [
        d for d in os.listdir(BBG_EXTRACTION_ROOT)
        if os.path.isdir(os.path.join(BBG_EXTRACTION_ROOT, d)) and d != 'new'
    ]
    
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
        
        # 2. GET PATHS & FIRM ID
        csv_filename = os.path.basename(csv_file_path)
        file_name_without_ext = os.path.splitext(csv_filename)[0]
        is_new_file = os.path.normpath(os.path.dirname(csv_file_path)) == os.path.normpath(NEW_DATA_DIRECTORY)
        
        if is_new_file:
            firm_identifier = file_name_without_ext.split('_')[0]
        else:
            # We rely on the folder name for archive files
            firm_identifier = os.path.basename(os.path.dirname(os.path.dirname(csv_file_path)))
        
        normalized_base_name = normalize_string(firm_identifier)
        archive_folder_name = id_map.get(normalized_base_name, firm_identifier)
        
        if not archive_folder_name:
            print(f"Warning: Could not find firm ID for '{firm_identifier}'. Skipping file '{csv_filename}'.")
            continue
            
        # --- NEW: Get the specific blacklist for this firm ID ---
        current_blacklist_set = firm_blacklist_map.get(archive_folder_name, set())
        
        print(f"\n--- Processing: {csv_file_path} (ID: {archive_folder_name}) ---")
        
        # 1. PROCESS NEW FILE
        # --- MODIFIED: Pass the firm-specific blacklist set ---
        matches, new_discrepancies_json, new_additions_json = process_one_file(
            csv_file_path, master_map, alias_map, current_blacklist_set
        )

        firm_root_folder = os.path.join(BBG_EXTRACTION_ROOT, archive_folder_name)
        firm_discrepancy_folder = os.path.join(firm_root_folder, "discrepancies")
        firm_archive_folder = os.path.join(firm_root_folder, "archive")
        firm_additions_folder = os.path.join(firm_root_folder, "additions")
        firm_confirmed_folder = os.path.join(firm_root_folder, "confirmed_matches")
        firm_missing_folder = os.path.join(firm_root_folder, "missing_records")
        
        for folder in [firm_discrepancy_folder, firm_archive_folder, firm_additions_folder, firm_confirmed_folder, firm_missing_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        # 3, 4, 5, 6, 7, 8. WRITE LOGIC remains the same from the previous step.
        
        # ... (rest of the main function is identical to your previous version) ...
        # (Writing discrepancies, additions, matches, archiving)

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
                
                total_new_matches += len(matches)

            except Exception as e:
                print(f"Error writing confirmed matches CSV {master_matches_log_path}: {e}")
        """

        # 6. WRITE MISSING RECORDS (OVERWRITE)
        if not is_new_file:
            master_missing_log_path = os.path.join(firm_missing_folder, f"{archive_folder_name}_missing.csv")
            
            # The simplest way: look for ALL records NOT found, regardless of folder
            missing_records = [
                record for record in all_persons_list 
                if record['Source_Found'] is False and record.get('Firm') # Exclude unverified records
            ]

            if missing_records:
                headers = [k for k in missing_records[0].keys() if k != 'Source_Found']
                
                try:
                    with open(master_missing_log_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(missing_records)
                    print(f"Successfully saved {len(missing_records)} missing records to {master_missing_log_path} (overwrite)")
                except Exception as e:
                    print(f"Error writing missing records CSV {master_missing_log_path}: {e}")
            else:
                 print("No master records were found to be missing.")
            """

        # 6. WRITE MISSING RECORDS (OVERWRITE)
        if not is_new_file:
            master_missing_log_path = os.path.join(firm_missing_folder, f"{archive_folder_name}_missing.csv")
            
            missing_records = [
                record for record in all_persons_list 
                if record.get('Source_Found') is False and record.get('Firm')
            ]

            if missing_records:
                # Create a clean version of the data without the 'Source_Found' helper key
                clean_missing = [{k: v for k, v in r.items() if k != 'Source_Found'} for r in missing_records]
                headers = list(clean_missing[0].keys())
                
                try:
                    with open(master_missing_log_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(clean_missing)
                    # Removed the print statement here to save space
                except Exception as e:
                    print(f"Error writing missing records: {e}")



        # 7. ARCHIVE THE PROCESSED SOURCE FILE (if it was new)
        if is_new_file:
            try:
                _ , ext = os.path.splitext(csv_filename)
                file_name_base = os.path.splitext(csv_filename)[0]
                firm_part = file_name_base.split('_')[0]
                date_part = file_name_base.replace(f"{firm_part}_", "")

                if not date_part:
                    raise ValueError("No date part in filename")

                parsed_date = datetime.strptime(date_part, '%Y%m%d')
                new_archive_filename = f"{parsed_date.strftime('%Y-%m-%d')}{ext}"

            except ValueError:
                file_name_base = os.path.splitext(csv_filename)[0]
                firm_part = file_name_base.split('_')[0]
                archive_name_base = file_name_base.replace(f"{firm_part}_", "")
                _ , ext = os.path.splitext(csv_filename)
                
                if not archive_name_base:
                    new_archive_filename = f"{datetime.now().strftime('%Y-%m-%d')}{ext}"
                else:
                    new_archive_filename = f"{archive_name_base}{ext}"

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
