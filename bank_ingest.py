"""
bank_ingest.py
Job 1: Pull BAI2 from SFTP, parse, upload to Drive, append to Google Sheet,
and archive the CSV. Runs independently of NetSuite matching.

All config is driven by environment variables (set as GitHub Secrets).

Required env vars:
    SFTP_HOST                Bank SFTP hostname
    SFTP_PORT                Bank SFTP port (default 22)
    SFTP_USERNAME            SFTP username
    SFTP_PASSWORD            SFTP password
    SFTP_REMOTE_DIR          Remote directory containing BAI files
    GOOGLE_CLIENT_ID         Google OAuth2 client ID
    GOOGLE_CLIENT_SECRET     Google OAuth2 client secret
    GOOGLE_REFRESH_TOKEN     Google OAuth2 refresh token
    GOOGLE_DRIVE_FOLDER_ID   Drive folder to receive raw TXT + transactions CSV
    GOOGLE_ARCHIVE_FOLDER_ID Drive folder to move CSV into after sheet append
    GOOGLE_SHEET_ID          Google Spreadsheet ID
    GOOGLE_SHEET_TAB         (optional) Raw transactions tab, default: input
    LOCAL_WORK_DIR           (optional) local temp directory, default /tmp/bai_pipeline
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


from bai2_parser import parse_bai2, file_to_transaction_rows
from sftp_client import download_bai_file
from drive_uploader import upload_to_drive


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bank_ingest")


RUN_LOG_FILE = "ingest_log.json"
TOKEN_URI    = "https://oauth2.googleapis.com/token"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]



def get_config() -> dict:
    required = [
        "SFTP_HOST",
        "SFTP_USERNAME",
        "SFTP_PASSWORD",
        "SFTP_REMOTE_DIR",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REFRESH_TOKEN",
        "GOOGLE_DRIVE_FOLDER_ID",
        "GOOGLE_ARCHIVE_FOLDER_ID",
        "GOOGLE_SHEET_ID",
    ]
    config = {}
    missing = []
    for key in required:
        val = os.environ.get(key)
        if not val:
            missing.append(key)
        config[key] = val

    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    config["SFTP_PORT"]       = int(os.environ.get("SFTP_PORT") or "22")
    config["LOCAL_WORK_DIR"]  = os.environ.get("LOCAL_WORK_DIR") or "/tmp/bai_pipeline"
    config["GOOGLE_SHEET_TAB"] = os.environ.get("GOOGLE_SHEET_TAB") or "input"
    return config



def get_google_credentials(config: dict) -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=config["GOOGLE_REFRESH_TOKEN"],
        token_uri=TOKEN_URI,
        client_id=config["GOOGLE_CLIENT_ID"],
        client_secret=config["GOOGLE_CLIENT_SECRET"],
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds



def write_csv(rows: list, output_path: str) -> int:
    """Write list-of-dicts to CSV. Returns row count."""
    if not rows:
        logger.warning(f"No rows to write for {output_path}")
        Path(output_path).touch()
        return 0
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows to {output_path}")
    return len(rows)



def _get_sheet_id(sheets, spreadsheet_id: str, tab_name: str) -> int:
    """Look up the numeric sheetId for a tab by name (case-insensitive)."""
    meta = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties"
    ).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title", "").lower() == tab_name.lower():
            return props["sheetId"]
    raise ValueError(f"Worksheet '{tab_name}' not found in spreadsheet")


def _ensure_date_column_format(sheets, spreadsheet_id: str, sheet_id: int) -> None:
    """
    Set Column A to Date format (M/d/yy) so that USER_ENTERED dates
    are stored as real dates instead of text.  This overrides any
    'Plain Text' format that may have been set manually on the column.
    """
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {
                                    "type": "DATE",
                                    "pattern": "M/d/yy"
                                }
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat"
                    }
                }
            ]
        },
    ).execute()
    logger.info("Set Column A format to Date (M/d/yy)")


def _get_existing_keys(sheets, spreadsheet_id: str, tab: str) -> tuple[set[str], set[tuple]]:
    """
    Read the Input tab and return two sets for dedup:
      1. bank_refs: set of non-empty Bank Ref # values (column K)
      2. composite_keys: set of (Date, BAI Type Code, amount, Description[:50]) tuples
         for rows where Bank Ref # is empty
    """
    bank_refs: set[str] = set()
    composite_keys: set[tuple] = set()

    # Read columns A (Date), G (BAI Type Code), I (Credit Amount), J (Debit Amount),
    # K (Bank Ref #), O (Description)
    # We read all columns A through Q to keep it simple
    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab}!A:Q")
        .execute()
    )
    all_rows = result.get("values", [])
    if len(all_rows) <= 1:
        # Empty or header-only
        return bank_refs, composite_keys

    # Find column indices from the header row
    header = all_rows[0]
    col_map = {name: idx for idx, name in enumerate(header)}

    date_idx = col_map.get("Date", 0)
    bai_idx = col_map.get("BAI Type Code", 6)
    credit_idx = col_map.get("Credit Amount", 8)
    debit_idx = col_map.get("Debit Amount", 9)
    bankref_idx = col_map.get("Bank Ref #", 10)
    desc_idx = col_map.get("Description", 14)

    for row in all_rows[1:]:
        def _cell(r, idx):
            return r[idx].strip() if idx < len(r) else ""

        bank_ref = _cell(row, bankref_idx)
        if bank_ref:
            bank_refs.add(bank_ref)
        else:
            # composite key for rows without a Bank Ref #
            amount = _cell(row, credit_idx) or _cell(row, debit_idx)
            composite_keys.add((
                _cell(row, date_idx),
                _cell(row, bai_idx),
                amount,
                _cell(row, desc_idx)[:50],
            ))

    return bank_refs, composite_keys


def _dedup_rows(rows: list[dict], bank_refs: set[str], composite_keys: set[tuple]) -> list[dict]:
    """Filter out rows that already exist in the sheet."""
    new_rows = []
    skipped = 0
    for row in rows:
        ref = (row.get("Bank Ref #") or "").strip()
        if ref:
            if ref in bank_refs:
                skipped += 1
                continue
            # Add to set so intra-batch duplicates are also caught
            bank_refs.add(ref)
        else:
            amount = row.get("Credit Amount") or row.get("Debit Amount") or ""
            key = (
                (row.get("Date") or "").strip(),
                (row.get("BAI Type Code") or "").strip(),
                str(amount).strip(),
                (row.get("Description") or "")[:50].strip(),
            )
            if key in composite_keys:
                skipped += 1
                continue
            composite_keys.add(key)

        new_rows.append(row)

    logger.info(f"Dedup: {skipped} duplicate(s) skipped, {len(new_rows)} new row(s) to append")
    if skipped and not new_rows:
        logger.warning("All rows are duplicates — nothing to append")
    return new_rows


def append_to_sheet(sheets, spreadsheet_id: str, tab: str, rows: list[dict]) -> int:
    """
    Append rows (list of dicts) to a sheet tab.
    Writes header if the sheet is empty.
    Returns row count appended.
    """
    if not rows:
        logger.warning(f"No rows to append to '{tab}'")
        return 0

    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab}!A1:A1")
        .execute()
    )
    has_header = bool(result.get("values"))

    # Ensure Column A is formatted as Date so USER_ENTERED parses dates
    sheet_id = _get_sheet_id(sheets, spreadsheet_id, tab)
    _ensure_date_column_format(sheets, spreadsheet_id, sheet_id)

    values = []
    if not has_header:
        values.append(list(rows[0].keys()))
    for row in rows:
        values.append([str(v) if v is not None and str(v) != "nan" else "" for v in row.values()])

    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

    logger.info(f"Appended {len(rows)} rows to '{tab}' tab")
    return len(rows)



def move_file_in_drive(drive, file_id: str, src_folder_id: str, dst_folder_id: str) -> None:
    """Move a Drive file from src folder to dst folder."""
    drive.files().update(
        fileId=file_id,
        addParents=dst_folder_id,
        removeParents=src_folder_id,
        fields="id, parents",
    ).execute()
    logger.info(f"Moved file {file_id} to archive folder")



def append_run_log(work_dir: str, entry: dict):
    log_path = os.path.join(work_dir, RUN_LOG_FILE)
    history = []
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                history = json.load(f)
        except Exception:
            history = []
    history.insert(0, entry)
    history = history[:100]
    with open(log_path, "w") as f:
        json.dump(history, f, indent=2)



def run():
    started_at = datetime.utcnow().isoformat()
    config = get_config()
    work_dir = config["LOCAL_WORK_DIR"]
    os.makedirs(work_dir, exist_ok=True)

    log_entry = {
        "started_at":           started_at,
        "status":               "running",
        "bai_file":             None,
        "transaction_rows":     0,
        "raw_txt_drive_id":     None,
        "transactions_drive_id": None,
        "sheet_rows_appended":  0,
        "error":                None,
    }

    try:
        # ------------------------------------------------------------------
        # 1. Download raw TXT from SFTP (or use local file override)
        # ------------------------------------------------------------------
        local_bai_override = os.environ.get("LOCAL_BAI_FILE", "").strip()
        if local_bai_override and os.path.isfile(local_bai_override):
            logger.info(f"Step 1: Using local BAI file override: {local_bai_override}")
            local_bai_path = local_bai_override
        else:
            logger.info("Step 1: Downloading BAI file from SFTP...")
            local_bai_path = download_bai_file(
                host=config["SFTP_HOST"],
                port=config["SFTP_PORT"],
                username=config["SFTP_USERNAME"],
                password=config["SFTP_PASSWORD"],
                remote_dir=config["SFTP_REMOTE_DIR"],
                local_dir=work_dir,
            )
        log_entry["bai_file"] = os.path.basename(local_bai_path)
        logger.info(f"Downloaded: {local_bai_path}")

        # ------------------------------------------------------------------
        # 2. Upload raw TXT to Drive
        # ------------------------------------------------------------------
        logger.info("Step 2: Uploading raw TXT to Google Drive...")
        log_entry["raw_txt_drive_id"] = upload_to_drive(
            local_file_path=local_bai_path,
            drive_folder_id=config["GOOGLE_DRIVE_FOLDER_ID"],
            mime_type="text/plain",
        )

        # ------------------------------------------------------------------
        # 3. Parse BAI2 -> transactions
        # ------------------------------------------------------------------
        logger.info("Step 3: Parsing BAI2 file...")
        with open(local_bai_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        file_record = parse_bai2(content)
        transaction_rows = file_to_transaction_rows(file_record)
        logger.info(f"Parsed {len(transaction_rows)} transaction rows")

        # ------------------------------------------------------------------
        # 4. Write transactions CSV
        # ------------------------------------------------------------------
        logger.info("Step 4: Writing transactions CSV...")
        base_name = Path(local_bai_path).stem
        transactions_csv = os.path.join(work_dir, f"{base_name}_transactions.csv")
        log_entry["transaction_rows"] = write_csv(transaction_rows, transactions_csv)

        # ------------------------------------------------------------------
        # 5. Upload transactions CSV to Drive
        # ------------------------------------------------------------------
        logger.info("Step 5: Uploading transactions CSV to Google Drive...")
        transactions_drive_id = upload_to_drive(
            local_file_path=transactions_csv,
            drive_folder_id=config["GOOGLE_DRIVE_FOLDER_ID"],
        )
        log_entry["transactions_drive_id"] = transactions_drive_id

        # ------------------------------------------------------------------
        # 6. Append raw transactions to Google Sheet (input tab)
        # ------------------------------------------------------------------
        logger.info("Step 6: Appending raw transactions to Google Sheet...")
        creds  = get_google_credentials(config)
        drive  = build("drive",  "v3", credentials=creds)
        sheets = build("sheets", "v4", credentials=creds)

        # Dedup: read existing rows from sheet and filter out duplicates
        logger.info("Step 6a: Reading existing sheet data for dedup...")
        bank_refs, composite_keys = _get_existing_keys(
            sheets,
            spreadsheet_id=config["GOOGLE_SHEET_ID"],
            tab=config["GOOGLE_SHEET_TAB"],
        )
        logger.info(f"Found {len(bank_refs)} existing Bank Ref # values and {len(composite_keys)} composite keys")
        transaction_rows = _dedup_rows(transaction_rows, bank_refs, composite_keys)

        log_entry["sheet_rows_appended"] = append_to_sheet(
            sheets,
            spreadsheet_id=config["GOOGLE_SHEET_ID"],
            tab=config["GOOGLE_SHEET_TAB"],
            rows=transaction_rows,
        )

        # ------------------------------------------------------------------
        # 7. Move CSV to archive folder in Drive
        # ------------------------------------------------------------------
        logger.info("Step 7: Moving transactions CSV to archive folder...")
        move_file_in_drive(
            drive,
            file_id=transactions_drive_id,
            src_folder_id=config["GOOGLE_DRIVE_FOLDER_ID"],
            dst_folder_id=config["GOOGLE_ARCHIVE_FOLDER_ID"],
        )

        log_entry["status"] = "success"
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.info("Bank ingest completed successfully.")

    except Exception as e:
        log_entry["status"] = "error"
        log_entry["error"] = str(e)
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.exception(f"Bank ingest failed: {e}")
        sys.exit(1)

    finally:
        append_run_log(work_dir, log_entry)


if __name__ == "__main__":
    run()
