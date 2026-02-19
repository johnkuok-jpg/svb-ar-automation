"""
sftp_client.py
Downloads the prior-day BAI2 file from a bank SFTP server.
Auth: username + password.
"""

import os
import re
import logging
from datetime import datetime, timedelta
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)


def get_prior_day_str(fmt: str = "%Y%m%d") -> str:
    """Return yesterday's date as a string."""
    return (datetime.utcnow() - timedelta(days=1)).strftime(fmt)


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
    files = sftp.listdir(remote_dir)
    logger.info(f"Files in {remote_dir}: {files}")

    if filename_pattern:
        pattern = filename_pattern.replace("{date}", re.escape(date_str))
        for f in files:
            if re.fullmatch(pattern, f, re.IGNORECASE):
                return f"{remote_dir.rstrip('/')}/{f}"

    for f in files:
        if date_str in f and re.search(r"\.bai2?$", f, re.IGNORECASE):
            return f"{remote_dir.rstrip('/')}/{f}"

    for f in files:
        if re.search(r"\.bai2?$", f, re.IGNORECASE):
            logger.warning(f"Could not match date in filename; using first .bai file: {f}")
            return f"{remote_dir.rstrip('/')}/{f}"

    return None


def download_bai_file(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    local_dir: str,
    filename_pattern: Optional[str] = None,
    date_fmt: str = "%Y%m%d",
) -> str:
    """
    Full SFTP download flow. Returns local file path of the downloaded BAI file.
    Raises FileNotFoundError if the file cannot be located.
    """
    os.makedirs(local_dir, exist_ok=True)
    date_str = get_prior_day_str(date_fmt)

    sftp = connect_sftp(host, port, username, password)
    try:
        remote_path = find_bai_file(sftp, remote_dir, date_str, filename_pattern)
        if not remote_path:
            raise FileNotFoundError(
                f"No BAI file found for date {date_str} in {remote_dir}"
            )
        filename = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, filename)
        sftp.get(remote_path, local_path)
        logger.info(f"Downloaded {remote_path} -> {local_path}")
        return local_path
    finally:
        sftp.close()
