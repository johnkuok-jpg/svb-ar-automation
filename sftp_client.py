"""
sftp_client.py
Downloads the prior-day BAI2 file from SVB's SFTP server.
Targets only the _PD_ (Prior Day) file for account ending in 34669.
Auth: username + password.
"""

import os
import re
import logging
from datetime import datetime, timedelta
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)

# Target account suffix -- only pull the PD file for this account
TARGET_ACCOUNT = "34669"


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
    """
    Find the prior-day BAI file for account 34669.

    SVB naming convention:
        ARR_IR_GWperp5594_PD_YYYYMMDD_34669.TXT

    Matches: _PD_ + date + _ + TARGET_ACCOUNT + .TXT
    """
    files = sftp.listdir(remote_dir)
    logger.info(f"Files in {remote_dir}: {len(files)} total")

    # Primary: match exact SVB PD pattern for target account
    pattern = re.compile(
        rf".*_PD_{re.escape(date_str)}_{re.escape(TARGET_ACCOUNT)}\.TXT$",
        re.IGNORECASE,
    )
    for f in files:
        if pattern.match(f):
            logger.info(f"Matched target file: {f}")
            return f"{remote_dir.rstrip('/')}/{f}"

    # Fallback: if caller supplied a custom pattern
    if filename_pattern:
        custom = filename_pattern.replace("{date}", re.escape(date_str))
        for f in files:
            if re.fullmatch(custom, f, re.IGNORECASE):
                logger.info(f"Matched via custom pattern: {f}")
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
    filename_pattern: Optional[str] = None,
    date_fmt: str = "%Y%m%d",
) -> str:
    """
    Full SFTP download flow. Returns local file path of the downloaded file.
    Raises FileNotFoundError if the target file cannot be located.
    """
    os.makedirs(local_dir, exist_ok=True)
    date_str = get_prior_day_str(date_fmt)

    sftp = connect_sftp(host, port, username, password)
    try:
        remote_path = find_bai_file(sftp, remote_dir, date_str, filename_pattern)
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
