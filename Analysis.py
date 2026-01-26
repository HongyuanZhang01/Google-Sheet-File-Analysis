import os
import time
import csv
import json
import argparse
import sys
import shutil
import re 
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from thefuzz import process, fuzz 
from google import genai

# --- CONFIGURATION ---
# TODO: Users should replace these values with their own project details
GEMINI_API_KEY = "[Enter your Gemini API Key]"
SHEET_NAME = "[Enter Google Sheet Name]"
COLUMN_HEADER = "[Enter Column Header for Citations]" 
LOCAL_PDF_FOLDER = "[Enter path to local PDF folder]"

# Define the Analysis Prompt
ANALYSIS_PROMPT = """
Analyze this PDF research paper. The goal is to filter out all papers which do not draw on experimental studies.
1. Identify the overall research methodology used to produce the conclusion. Choose ONE from: [Experimental, Systematic Review, Survey, Qualitative (Case study, Ethnography, etc.), Other].
2. Provide a brief "Reason" explaining why this methodology fits (max 1 sentence).

Return the result as a valid JSON object with these keys:
- "methodology": "The chosen category",
- "reason": "The explanation"
"""

# --- CLIENT INITIALIZATION ---
if GEMINI_API_KEY == "[Enter your Gemini API Key]":
    print("❌ Error: Please update the GEMINI_API_KEY in the configuration section.")
    sys.exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

def safe_print(text):
    """Safely prints text, handling terminals that do not support specific unicode characters."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))

def get_files_from_sheet(sheet_name, column_header, mode, start_row=None, end_row=None, specific_rows=None):
    """
    Connects to Google Sheets and fetches the list of file citations to process.
    Supports filtering by a continuous range or a specific list of row numbers.
    """
    safe_print(f"Connecting to Google Sheet '{sheet_name}'...")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        gs_client = gspread.authorize(creds)
        sheet = gs_client.open(sheet_name).sheet1
        all_rows = sheet.get_all_values()
    except Exception as e:
        safe_print(f"Error connecting to Sheets: {e}")
        sys.exit(1)

    if not all_rows:
        safe_print("Sheet is empty.")
        sys.exit(1)

    headers = all_rows[0]
    try:
        file_col_index = headers.index(column_header)
    except ValueError:
        safe_print(f"❌ Error: Could not find column '{column_header}' in header row.")
        sys.exit(1)

    selected_files = []
    data_rows = all_rows[1:] 
    
    for index, row_data in enumerate(data_rows):
        actual_sheet_row = index + 2 
        
        if len(row_data) > file_col_index:
            file_name = row_data[file_col_index].strip()
        else:
            file_name = ""

        if not file_name:
            continue

        if mode == 'range':
            if start_row <= actual_sheet_row <= end_row:
                selected_files.append({"row": actual_sheet_row, "name": file_name})
        elif mode == 'list':
            if actual_sheet_row in specific_rows:
                selected_files.append({"row": actual_sheet_row, "name": file_name})

    if not selected_files:
        safe_print(f"No files found for the specified request.")
        sys.exit(0)

    safe_print(f"Found {len(selected_files)} entries to process.")
    return selected_files

# --- MATCHING LOGIC ---

def extract_year(text):
    """Extracts a 4-digit year (1900-2099) from text string."""
    match = re.search(r'\b(19|20)\d{2}\b', text)
    return match.group(0) if match else None

def normalize_text(text):
    """Normalizes text by lowercasing and removing non-alphanumeric characters."""
    text = text.lower()
    text = text.replace('.pdf', '')
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return " ".join(text.split())

def find_best_local_match(sheet_name_str, local_files):
    """
    Matches a citation string to the most likely local PDF filename.
    Uses fuzzy matching with year guardrails and title prioritization.
    """
    clean_sheet = normalize_text(sheet_name_str)
    sheet_year = extract_year(sheet_name_str) 
    
    best_file = None
    best_score = -1
    
    for file_name in local_files:
        clean_file = normalize_text(file_name)
        file_year = extract_year(file_name)
        
        # Guardrail: If years are present in both but do not match, skip.
        if sheet_year and file_year and sheet_year != file_year:
            continue 

        # Calculate Similarity Scores
        base_score = fuzz.token_set_ratio(clean_sheet, clean_file)
        partial_score = fuzz.partial_ratio(clean_sheet, clean_file)
        
        # Prioritize partial matches (titles) only if the filename is long enough
        if len(clean_file) > 15:
            final_score = max(base_score, partial_score)
        else:
            final_score = base_score

        # Tie-breaker: Favor longer filenames (more specific matches)
        final_score += (len(clean_file) / 1000.0)

        if final_score > best_score:
            best_score = final_score
            best_file = file_name
            
    display_score = min(int(best_score), 100)
    return best_file, display_score

# --- BATCH EXECUTION ---

def run_batch_job(file_list, local_folder):
    """Prepares files and submits a Batch Job to Gemini."""
    batch_inputs = []
    valid_files_count = 0

    if not os.path.exists(local_folder):
        safe_print(f"❌ Error: Local folder '{local_folder}' does not exist.")
        sys.exit(1)
        
    local_pdf_files = [f for f in os.listdir(local_folder) if f.lower().endswith('.pdf')]
    if not local_pdf_files:
        safe_print(f"❌ Error: No PDF files found in '{local_folder}'.")
        sys.exit(1)

    safe_print(f"Loaded {len(local_pdf_files)} local PDF filenames for matching.")
    safe_print("-" * 50)

    for entry in file_list:
        citation_text = entry['name']
        row_num = entry['row']
        
        matched_filename, score = find_best_local_match(citation_text, local_pdf_files)
        
        if score < 40: 
            safe_print(f"⚠️ Row {row_num}: Low match confidence ({score}%). Skipping.")
            continue
            
        safe_print(f"✅ Row {row_num} Match: '{matched_filename}' (Confidence: {score}%)")
        
        original_file_path = os.path.join(local_folder, matched_filename)
        
        # Create a temporary file with a safe ASCII name for upload
        temp_safe_name = f"temp_row_{row_num}.pdf"
        temp_file_path = os.path.join(local_folder, temp_safe_name)
        
        try:
            shutil.copy2(original_file_path, temp_file_path)
            
            gemini_file = client.files.upload(
                file=temp_file_path, 
                config={'mime_type': 'application/pdf'}
            )
        except Exception as e:
            safe_print(f"   ❌ Upload failed: {e}")
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            continue
            
        # Cleanup temp file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        # Wait for file processing
        while gemini_file.state.name == "PROCESSING":
            time.sleep(1)
            gemini_file = client.files.get(name=gemini_file.name)
            
        if gemini_file.state.name == "FAILED":
            safe_print(f"   ❌ Google failed to process PDF: {matched_filename}")
            continue

        # Prepare Batch Request Item
        safe_citation_snippet = citation_text[:50].encode('ascii', 'ignore').decode('ascii')
        safe_citation_id = f"{row_num}::{safe_citation_snippet}" 
        
        batch_inputs.append({
            "custom_id": safe_citation_id, 
            "request": {
                "contents": [
                    {"role": "user", "parts": [
                        {"text": ANALYSIS_PROMPT},
                        {"file_data": {"mime_type": gemini_file.mime_type, "file_uri": gemini_file.uri}}
                    ]}
                ]
            }
        })
        valid_files_count += 1

    if valid_files_count == 0:
        safe_print("No valid files were uploaded. Exiting.")
        return None

    # Write Batch JSONL file
    jsonl_filename = "batch_requests.jsonl"
    with open(jsonl_filename, "w", encoding='utf-8') as f:
        for item in batch_inputs:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    safe_print(f"\nSubmitting Batch Job for {valid_files_count} files...")
    
    batch_input_file = client.files.upload(
        file=jsonl_filename,
        config={'mime_type': 'application/json'}
    )
    
    # Priority list for models (Aliases first, then specific versions)
    model_candidates = [
        "models/gemini-flash-latest",           
        "models/gemini-pro-latest",             
        "models/gemini-2.0-flash-lite-preview-02-05", 
        "models/gemini-2.5-flash-lite"          
    ]
    
    batch_job = None
    for model_id in model_candidates:
        try:
            safe_print(f"Attempting batch job with: {model_id}")
            batch_job = client.batches.create(
                model=model_id,
                src=batch_input_file.name,
                config={'display_name': 'uncanny_meta_search_batch'} 
            )
            break 
        except Exception as e:
            safe_print(f"❌ Failed with {model_id}: {e}")
            
    if not batch_job:
        safe_print("❌ All model attempts failed. Check your API Quota.")
        return None
    
    safe_print(f"Job started! ID: {batch_job.name}")
    return batch_job

def save_results(batch_job):
    """Polls for job completion and saves results to CSV."""
    if not batch_job: return

    safe_print(f"Job started. ID: {batch_job.name}")

    # Polling Loop
    while True:
        time.sleep(30)
        batch_job = client.batches.get(name=batch_job.name)
        safe_print(f"Job State: {batch_job.state}") 
        
        state_str = str(batch_job.state)
        if "SUCCEEDED" in state_str:
            break
        elif "FAILED" in state_str:
            safe_print(f"Job failed: {batch_job.error}")
            return

    safe_print("Job done. Downloading results...")
    
    result_file_name = None
    
    # Try to determine the output filename from the job object
    try:
        if hasattr(batch_job, 'output_file'):
            result_file_name = batch_job.output_file.name
        elif hasattr(batch_job, 'dest') and hasattr(batch_job.dest, 'file_name'):
            result_file_name = batch_job.dest.file_name
        else:
            # Fallback for complex object structures
            pass
    except Exception as e:
        safe_print(f"⚠️  Warning: output_file attribute not found directly: {e}")

    # Fallback: Find the most recent JSON output file via the File List API
    if not result_file_name:
        safe_print("   Attempting to auto-detect output file from file list...")
        try:
            files = client.files.list(config={'page_size': 5}) 
            for f in files:
                if f.mime_type == 'text/x-json' or 'json' in f.mime_type:
                    result_file_name = f.name
                    break
        except Exception as e:
             safe_print(f"❌ Could not auto-detect output file: {e}")
             return

    if not result_file_name:
        safe_print("❌ Critical: Could not determine output file name.")
        return

    safe_print(f"   Downloading file: {result_file_name}")

    try:
        content_bytes = client.files.download(file=result_file_name)
        output_text = content_bytes.decode('utf-8')
    except Exception as e:
        safe_print(f"Error downloading results: {e}")
        return

    # Parse Results
    results_list = []
    for line in output_text.splitlines():
        try:
            entry = json.loads(line)
            custom_id = entry.get("custom_id")
            
            parts = custom_id.split("::", 1)
            row_num = parts[0]
            citation_snippet = parts[1] if len(parts) > 1 else ""

            if 'response' in entry and 'candidates' in entry['response']:
                candidate = entry['response']['candidates'][0]['content']['parts'][0]['text']
                # Clean Markdown formatting from JSON
                clean_json = candidate.replace("```json", "").replace("```", "").strip()
                data = json.loads(clean_json)
                
                results_list.append({
                    "Row": int(row_num),
                    "Sheet Citation (Snippet)": citation_snippet,
                    "Methodology": data.get("methodology"),
                    "Reason": data.get("reason")
                })
            else:
                safe_print(f"⚠️  Row {row_num} failed model generation (Blocked/Error)")
                
        except Exception as e:
            safe_print(f"Error parsing line: {e}")

    results_list.sort(key=lambda x: x["Row"])

    csv_filename = 'batch_results.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Row", "Sheet Citation (Snippet)", "Methodology", "Reason"])
        writer.writeheader()
        writer.writerows(results_list)
            
    safe_print(f"Done! Results saved to '{csv_filename}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze research papers via Google Gemini Batch API.")
    subparsers = parser.add_subparsers(dest='command')

    # Command: Range of rows
    parser_range = subparsers.add_parser('row-range', help="Process a continuous range of rows (e.g., 5 10)")
    parser_range.add_argument('start', type=int)
    parser_range.add_argument('end', type=int)

    # Command: Specific list of rows
    parser_row = subparsers.add_parser('row', help="Process specific rows (e.g., 5 8 12 20)")
    parser_row.add_argument('nums', nargs='+', type=int)

    args = parser.parse_args()
    files_to_process = []

    if args.command == 'row-range':
        files_to_process = get_files_from_sheet(SHEET_NAME, COLUMN_HEADER, 'range', start_row=args.start, end_row=args.end)
    elif args.command == 'row':
        files_to_process = get_files_from_sheet(SHEET_NAME, COLUMN_HEADER, 'list', specific_rows=args.nums)
    else:
        parser.print_help()
        sys.exit(1)

    job = run_batch_job(files_to_process, LOCAL_PDF_FOLDER)
    save_results(job)
