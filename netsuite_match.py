"""
netsuite_match.py
Job 2: Read unmatched transactions from Google Sheet (input tab),
fetch open AR invoices from NetSuite, fuzzy-match, and write results
to the cash_application tab.

Runs independently of bank_ingest — works against whatever transactions
are already in the Sheet, so it can be re-run or triggered manually
even if the SFTP download failed.

All config is driven by environment variables (set as GitHub Secrets).

Required env vars:
    GOOGLE_CLIENT_ID         Google OAuth2 client ID
    GOOGLE_CLIENT_SECRET     Google OAuth2 client secret
    GOOGLE_REFRESH_TOKEN     Google OAuth2 refresh token
    GOOGLE_SHEET_ID          Google Spreadsheet ID
    GOOGLE_SHEET_TAB         (optional) Raw transactions tab, default: input
    GOOGLE_SHEET_CA_TAB      (optional) Cash application tab, default: netsuite_cash_app
    NETSUITE_ACCOUNT_ID      NetSuite account ID (default: 9060638)
    NETSUITE_CONSUMER_KEY    NetSuite TBA consumer key
    NETSUITE_CONSUMER_SECRET NetSuite TBA consumer secret
    NETSUITE_TOKEN_ID        NetSuite TBA token ID
    NETSUITE_TOKEN_SECRET    NetSuite TBA token secret
"""

import json
import logging
import os
import sys
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from netsuite_client import fetch_open_invoices
from matcher import match_transactions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("netsuite_match")

RUN_LOG_FILE = "match_log.json"
TOKEN_URI    = "https://oauth2.googleapis.com/token"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]


def get_config() -> dict:
    required = [
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REFRESH_TOKEN",
        "GOOGLE_SHEET_ID",
        "NETSUITE_CONSUMER_KEY",
        "NETSUITE_CONSUMER_SECRET",
        "NETSUITE_TOKEN_ID",
        "NETSUITE_TOKEN_SECRET",
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

    config["GOOGLE_SHEET_TAB"]    = os.environ.get("GOOGLE_SHEET_TAB") or "input"
    config["GOOGLE_SHEET_CA_TAB"] = os.environ.get("GOOGLE_SHEET_CA_TAB") or "netsuite_cash_app"
    config["NETSUITE_ACCOUNT_ID"] = os.environ.get("NETSUITE_ACCOUNT_ID") or "9060638"
    config["LOCAL_WORK_DIR"]      = os.environ.get("LOCAL_WORK_DIR") or "/tmp/bai_pipeline"
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


def read_sheet_rows(sheets, spreadsheet_id: str, tab: str) -> list[dict]:
    """
    Read all rows from a sheet tab. Returns list of dicts keyed by header row.
    """
    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab}!A:Z")
        .execute()
    )
    values = result.get("values", [])
    if len(values) < 2:
        return []

    headers = values[0]
    rows = []
    for row_data in values[1:]:
        # Pad short rows with empty strings
        padded = row_data + [""] * (len(headers) - len(row_data))
        rows.append(dict(zip(headers, padded)))
    return rows


# Column indices for the dedup key fields (based on standard column order).
# We use positional indices instead of header names because the cash_app tab
# has a duplicate "Date" column (col A = transaction date, col Q = run
# timestamp) and dict(zip(headers, ...)) would let col Q overwrite col A,
# breaking dedup and causing the entire input to be re-appended every run.
_COL_DATE         = 0   # A – Date
_COL_CREDIT_AMT   = 8   # I – Credit Amount
_COL_BANK_REF     = 10  # K – Bank Ref #
_COL_DESCRIPTION  = 13  # N – Description


def get_already_matched_keys(sheets, spreadsheet_id: str, ca_tab: str) -> set:
    """
    Read the cash_application tab and return a set of (Date, Credit Amount, Description, Bank Ref)
    tuples that have already been matched, so we don't duplicate them.
    """
    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{ca_tab}!A:Z")
        .execute()
    )
    values = result.get("values", [])
    if len(values) < 2:
        return set()

    def _cell(row, idx):
        return row[idx] if idx < len(row) else ""

    keys = set()
    for row_data in values[1:]:
        key = (
            _cell(row_data, _COL_DATE),
            _cell(row_data, _COL_CREDIT_AMT),
            _cell(row_data, _COL_DESCRIPTION),
            _cell(row_data, _COL_BANK_REF),
        )
        keys.add(key)
    return keys


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
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

    logger.info(f"Appended {len(rows)} rows to '{tab}' tab")
    return len(rows)


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
        "started_at":        started_at,
        "status":            "running",
        "input_rows_read":   0,
        "new_rows_to_match": 0,
        "invoices_fetched":  0,
        "matches_found":     0,
        "rows_appended":     0,
        "error":             None,
    }

    try:
        # ------------------------------------------------------------------
        # 1. Connect to Google Sheets
        # ------------------------------------------------------------------
        logger.info("Step 1: Connecting to Google Sheets...")
        creds  = get_google_credentials(config)
        sheets = build("sheets", "v4", credentials=creds)

        # ------------------------------------------------------------------
        # 2. Read all transactions from the input tab
        # ------------------------------------------------------------------
        logger.info("Step 2: Reading transactions from input tab...")
        all_transactions = read_sheet_rows(
            sheets,
            spreadsheet_id=config["GOOGLE_SHEET_ID"],
            tab=config["GOOGLE_SHEET_TAB"],
        )
        log_entry["input_rows_read"] = len(all_transactions)
        logger.info(f"Read {len(all_transactions)} rows from '{config['GOOGLE_SHEET_TAB']}' tab")

        if not all_transactions:
            logger.info("No transactions in input tab. Nothing to match.")
            log_entry["status"] = "success"
            log_entry["finished_at"] = datetime.utcnow().isoformat()
            return

        # ------------------------------------------------------------------
        # 3. Read already-matched rows from cash_application tab
        # ------------------------------------------------------------------
        logger.info("Step 3: Reading already-matched rows from cash_application tab...")
        already_matched = get_already_matched_keys(
            sheets,
            spreadsheet_id=config["GOOGLE_SHEET_ID"],
            ca_tab=config["GOOGLE_SHEET_CA_TAB"],
        )
        logger.info(f"Found {len(already_matched)} already-matched rows")

        # ------------------------------------------------------------------
        # 4. Filter to only unmatched transactions
        # ------------------------------------------------------------------
        new_transactions = []
        for txn in all_transactions:
            key = (
                txn.get("Date", ""),
                txn.get("Credit Amount", ""),
                txn.get("Description", ""),
                txn.get("Bank Ref #", ""),
            )
            if key not in already_matched:
                new_transactions.append(txn)

        log_entry["new_rows_to_match"] = len(new_transactions)
        logger.info(f"Found {len(new_transactions)} new/unmatched transactions to process")

        if not new_transactions:
            logger.info("All transactions already matched. Nothing new to process.")
            log_entry["status"] = "success"
            log_entry["finished_at"] = datetime.utcnow().isoformat()
            return

        # ------------------------------------------------------------------
        # 5. Fetch open AR invoices from NetSuite
        # ------------------------------------------------------------------
        logger.info("Step 5: Fetching open AR invoices from NetSuite...")
        invoices = fetch_open_invoices()
        log_entry["invoices_fetched"] = len(invoices)
        logger.info(f"Fetched {len(invoices)} open invoices")

        # ------------------------------------------------------------------
        # 6. Match transactions to invoices
        # ------------------------------------------------------------------
        logger.info("Step 6: Matching transactions to invoices...")
        matched_rows = match_transactions(new_transactions, invoices)
        matches_found = sum(1 for r in matched_rows if r.get("Invoice #"))
        log_entry["matches_found"] = matches_found
        logger.info(f"Matched {matches_found} of {len(matched_rows)} transactions")

        # ------------------------------------------------------------------
        # 7. Write matched results to cash_application tab
        # ------------------------------------------------------------------
        logger.info("Step 7: Writing results to cash_application tab...")
        log_entry["rows_appended"] = append_to_sheet(
            sheets,
            spreadsheet_id=config["GOOGLE_SHEET_ID"],
            tab=config["GOOGLE_SHEET_CA_TAB"],
            rows=matched_rows,
        )

        log_entry["status"] = "success"
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.info("NetSuite matching completed successfully.")

    except Exception as e:
        log_entry["status"] = "error"
        log_entry["error"] = str(e)
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.exception(f"NetSuite matching failed: {e}")
        sys.exit(1)

    finally:
        append_run_log(work_dir, log_entry)


if __name__ == "__main__":
    run()
