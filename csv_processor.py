"""
csv_processor.py

Watches the Drive inbox folder for _transactions.csv files that haven't been
processed yet, appends each one to the 'input' tab of the target Google Sheet,
then moves the CSV to the archive folder.

Auth: OAuth2 refresh token (same credentials used by drive_uploader.py)
"""

import os
import io
import json
import logging
from datetime import datetime

import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config from environment ──────────────────────────────────────────────────
CLIENT_ID       = os.environ["GOOGLE_CLIENT_ID"]
CLIENT_SECRET   = os.environ["GOOGLE_CLIENT_SECRET"]
REFRESH_TOKEN   = os.environ["GOOGLE_REFRESH_TOKEN"]

INBOX_FOLDER_ID   = os.environ.get("GOOGLE_DRIVE_FOLDER_ID",   "1EM8Fc87LGRoWPDk4Zn3FxBfeDJvQzzxp")
ARCHIVE_FOLDER_ID = os.environ.get("GOOGLE_ARCHIVE_FOLDER_ID", "1RwmYJR0j8F2Vk05V4zDCeJmDN3l_BYSK")
SPREADSHEET_ID    = os.environ.get("GOOGLE_SHEET_ID",           "1PDLXi7ZQxvDSeUbdf7_5ft1Npq7oIBad9PgTl0R2CpM")
SHEET_TAB         = os.environ.get("GOOGLE_SHEET_TAB",          "input")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

TOKEN_URI = "https://oauth2.googleapis.com/token"


def get_credentials() -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri=TOKEN_URI,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def list_transaction_csvs(drive, folder_id: str) -> list[dict]:
    """Return all *_transactions.csv files in the inbox folder, oldest first."""
    query = (
        f"'{folder_id}' in parents"
        " and trashed = false"
        " and mimeType = 'text/csv'"
        " and name contains '_transactions'"
    )
    results = (
        drive.files()
        .list(
            q=query,
            orderBy="createdTime",
            pageSize=50,
            fields="files(id, name, createdTime)",
        )
        .execute()
    )
    return results.get("files", [])


def download_csv(drive, file_id: str) -> pd.DataFrame:
    """Download a CSV file from Drive and return it as a DataFrame."""
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf)
    return df


def append_to_sheet(sheets, spreadsheet_id: str, tab: str, df: pd.DataFrame) -> int:
    """
    Append df rows to the sheet tab.
    If the sheet is empty, write the header row first.
    Returns the number of rows appended.
    """
    # Check if sheet already has data (to decide whether to include header)
    result = (
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab}!A1:A1")
        .execute()
    )
    has_header = bool(result.get("values"))

    rows = []
    if not has_header:
        rows.append(df.columns.tolist())

    for _, row in df.iterrows():
        rows.append([str(v) if pd.notna(v) else "" for v in row.tolist()])

    body = {"values": rows}
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

    return len(df)


def move_file(drive, file_id: str, src_folder_id: str, dst_folder_id: str) -> None:
    """Move a Drive file from src folder to dst folder."""
    drive.files().update(
        fileId=file_id,
        addParents=dst_folder_id,
        removeParents=src_folder_id,
        fields="id, parents",
    ).execute()


def run() -> None:
    log.info("csv_processor starting")
    creds  = get_credentials()
    drive  = build("drive",  "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)

    files = list_transaction_csvs(drive, INBOX_FOLDER_ID)
    if not files:
        log.info("No _transactions.csv files found in inbox — nothing to do.")
        return

    log.info(f"Found {len(files)} file(s) to process: {[f['name'] for f in files]}")

    total_rows = 0
    for f in files:
        log.info(f"Processing {f['name']} ...")
        df = download_csv(drive, f["id"])
        log.info(f"  → {len(df)} rows")

        rows_written = append_to_sheet(sheets, SPREADSHEET_ID, SHEET_TAB, df)
        log.info(f"  → Appended {rows_written} rows to '{SHEET_TAB}' tab")

        move_file(drive, f["id"], INBOX_FOLDER_ID, ARCHIVE_FOLDER_ID)
        log.info(f"  → Moved to archive folder")

        total_rows += rows_written

    log.info(f"Done. {len(files)} file(s) processed, {total_rows} total rows appended.")


if __name__ == "__main__":
    run()
