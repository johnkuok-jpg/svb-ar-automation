"""
drive_uploader.py
Uploads files to a Google Drive folder using a service account.
"""

import os
import logging
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _get_service(service_account_file: str):
    """Build and return an authenticated Drive service."""
    creds = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def upload_to_drive(
    local_file_path: str,
    drive_folder_id: str,
    service_account_file: str,
    mime_type: str = "text/csv",
    overwrite: bool = True,
) -> str:
    """
    Upload a local file to a Google Drive folder.
    If overwrite is True and a file with the same name already exists,
    it will be replaced. Returns the Google Drive file ID.
    """
    service = _get_service(service_account_file)
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
