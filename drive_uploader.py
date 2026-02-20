"""
drive_uploader.py
Uploads files to a Google Drive folder using OAuth2 (user account).

Auth flow:
  - One-time: run `python drive_uploader.py --auth` locally to generate a
    refresh token. Copy the printed refresh token into GitHub Secrets.
  - Pipeline: reads GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
    from env vars and exchanges them for a short-lived access token at runtime.
    No browser, no interaction — fully headless.
"""

import argparse
import os
import logging
from typing import Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]


# ---------------------------------------------------------------------------
# Credential Helpers
# ---------------------------------------------------------------------------
def _creds_from_env() -> Credentials:
    """
    Build OAuth2 credentials from env vars at runtime (headless).
    Requires: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
    """
    client_id     = os.environ["GOOGLE_CLIENT_ID"]
    client_secret = os.environ["GOOGLE_CLIENT_SECRET"]
    refresh_token = os.environ["GOOGLE_REFRESH_TOKEN"]

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )
    # Force a refresh to get a valid access token
    creds.refresh(Request())
    return creds


def _get_service():
    """Return an authenticated Drive v3 service."""
    creds = _creds_from_env()
    return build("drive", "v3", credentials=creds)


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
def upload_to_drive(
    local_file_path: str,
    drive_folder_id: str,
    mime_type: str = "text/csv",
    overwrite: bool = True,
) -> str:
    """
    Upload a local file to a Google Drive folder.
    If overwrite is True and a file with the same name already exists,
    it will be replaced. Returns the Google Drive file ID.
    """
    service = _get_service()
    filename = os.path.basename(local_file_path)

    existing_id: Optional[str] = None
    if overwrite:
        query = (
            f"name='{filename}' and '{drive_folder_id}' in parents "
            f"and trashed=false"
        )
        results = (
            service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
        )
        files = results.get("files", [])
        if files:
            existing_id = files[0]["id"]
            logger.info(f"Found existing file {filename} (id={existing_id}), will overwrite.")

    media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)

    if existing_id:
        file_metadata = {"name": filename}
        updated = (
            service.files()
            .update(fileId=existing_id, body=file_metadata, media_body=media)
            .execute()
        )
        file_id = updated.get("id")
        logger.info(f"Updated file in Drive: {filename} (id={file_id})")
    else:
        file_metadata = {
            "name": filename,
            "parents": [drive_folder_id],
        }
        created = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        file_id = created.get("id")
        logger.info(f"Uploaded new file to Drive: {filename} (id={file_id})")

    return file_id


# ---------------------------------------------------------------------------
# One-time auth helper (run locally to generate refresh token)
# ---------------------------------------------------------------------------
def _run_auth_flow():
    """
    Interactive one-time OAuth flow. Run this locally:
        python drive_uploader.py --auth

    You will be prompted to log in with your Google account in a browser.
    The refresh token is printed to stdout — copy it into GitHub Secrets
    as GOOGLE_REFRESH_TOKEN.

    Requires GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to be set in your
    local environment (or passed inline).
    """
    client_id     = os.environ.get("GOOGLE_CLIENT_ID") or input("Client ID: ").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET") or input("Client Secret: ").strip()

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    creds = flow.run_local_server(port=0)

    print("\n" + "="*60)
    print("SUCCESS. Copy this refresh token into GitHub Secrets")
    print("Secret name: GOOGLE_REFRESH_TOKEN")
    print("="*60)
    print(creds.refresh_token)
    print("="*60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--auth",
        action="store_true",
        help="Run interactive OAuth flow to generate a refresh token",
    )
    args = parser.parse_args()
    if args.auth:
        _run_auth_flow()
    else:
        parser.print_help()
