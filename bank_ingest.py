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

    values = []
    if not has_header:
        values.append(list(rows[0].keys()))
    for row in rows:
        values.append([str(v) if v is not None and str(v) != "nan" else "" for v in row.values()])

    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A1",
        valueInputOption="RAW",
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
        # 1. Download raw TXT from SFTP
        # ------------------------------------------------------------------
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
