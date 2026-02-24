"""
sftp_client.py
Downloads the prior-day BAI2 file from SVB's SFTP server.
Targets only the _PD_ (Prior Day) file for account ending in 34669.
Auth: username + password.
"""

import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)

# Target account suffix -- only pull the PD file for this account
TARGET_ACCOUNT = "34669"


def get_pd_file_date_str() -> str:
    """Return today's date as YYYYMMDD string for the PD filename.

    SVB names prior-day files with the *processing date* (today),
    not the transaction date.  For example, a file generated on
    Tuesday 2026-02-24 containing Monday's transactions is named
    ``..._PD_20260224_34669.TXT``.

    The pipeline runs on weekdays only (via GitHub Actions cron),
    so no weekend/holiday logic is needed here.
    """
    today = datetime.now(timezone.utc)
    result = today.strftime("%Y%m%d")
    logger.info(f"PD file date string (today): {result}")
    return result


def connect_sftp(host: str, port: int, username: str, password: str) -> paramiko.SFTPClient:
    """Open an SFTP connection and return the client."""
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logger.info(f"Connected to SFTP {host}:{port} as {username}")
    return sftp


def find_bai_file(
    sftp: paramiko.SFTPClient,
    remote_dir: str,
    date_str: str,
    filename_pattern: Optional[str] = None,
) -> Optional[str]:
    """
    Find the prior-day BAI file for account 34669.
    SVB naming convention: ARR_IR_GWperp5594_PD_YYYYMMDD_34669.TXT
    """
    files = sftp.listdir(remote_dir)
    logger.info(f"Files in {remote_dir}: {len(files)} total")
    logger.info(f"Looking for pattern: *_PD_{date_str}_34669.TXT")

    pattern = re.compile(
        rf".*_PD_{re.escape(date_str)}_{re.escape(TARGET_ACCOUNT)}\.TXT$",
        re.IGNORECASE,
    )
    for f in files:
        if pattern.match(f):
            logger.info(f"Matched target file: {f}")
            return f"{remote_dir.rstrip('/')}/{f}"

    logger.warning(
        f"No PD file found for account {TARGET_ACCOUNT} on {date_str} in {remote_dir}"
    )
    return None


def download_bai_file(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    local_dir: str,
    **kwargs,
) -> str:
    """
    Full SFTP download flow. Returns local file path of the downloaded file.
    Raises FileNotFoundError if the target file cannot be located.
    """
    os.makedirs(local_dir, exist_ok=True)
    date_str = get_pd_file_date_str()

    sftp = connect_sftp(host, port, username, password)
    try:
        remote_path = find_bai_file(sftp, remote_dir, date_str)
        if not remote_path:
            raise FileNotFoundError(
                f"No prior-day file found for account {TARGET_ACCOUNT} "
                f"on {date_str} in {remote_dir}"
            )

        filename = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, filename)
        sftp.get(remote_path, local_path)
        logger.info(f"Downloaded {remote_path} -> {local_path}")
        return local_path
    finally:
        sftp.close()
