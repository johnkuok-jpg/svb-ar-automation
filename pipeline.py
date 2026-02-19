"""
pipeline.py
Main entry point. Pulls BAI2 from SFTP, converts to CSV, uploads to Drive.
All config is driven by environment variables (set as GitHub Secrets).

Required env vars:
    SFTP_HOST               Bank SFTP hostname
    SFTP_PORT               Bank SFTP port (default 22)
    SFTP_USERNAME           SFTP username
    SFTP_PASSWORD           SFTP password
    SFTP_REMOTE_DIR         Remote directory containing BAI files
    SFTP_FILENAME_PATTERN   (optional) regex pattern, use {date} as placeholder
    SFTP_DATE_FMT           (optional) strftime format for date in filename, default %Y%m%d
    GOOGLE_CLIENT_ID        Google OAuth2 client ID
    GOOGLE_CLIENT_SECRET    Google OAuth2 client secret
    GOOGLE_REFRESH_TOKEN    Google OAuth2 refresh token (from one-time auth flow)
    GOOGLE_DRIVE_FOLDER_ID  Target Google Drive folder ID
    LOCAL_WORK_DIR          (optional) local temp directory, default /tmp/bai_pipeline
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from bai2_parser import parse_bai2, file_to_balances_rows, file_to_transaction_rows
from sftp_client import download_bai_file
from drive_uploader import upload_to_drive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")

RUN_LOG_FILE = "run_log.json"


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

    config["SFTP_PORT"]              = int(os.environ.get("SFTP_PORT", "22"))
    config["SFTP_FILENAME_PATTERN"]  = os.environ.get("SFTP_FILENAME_PATTERN")
    config["SFTP_DATE_FMT"]          = os.environ.get("SFTP_DATE_FMT", "%Y%m%d")
    config["LOCAL_WORK_DIR"]         = os.environ.get("LOCAL_WORK_DIR", "/tmp/bai_pipeline")
    config["GOOGLE_DRIVE_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
    return config


def write_csv(rows: list, output_path: str) -> int:
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
        "started_at": started_at,
        "status": "running",
        "bai_file": None,
        "balances_rows": 0,
        "transaction_rows": 0,
        "balances_drive_id": None,
        "transactions_drive_id": None,
        "error": None,
    }

    try:
        logger.info("Step 1: Downloading BAI file from SFTP...")
        local_bai_path = download_bai_file(
            host=config["SFTP_HOST"],
            port=config["SFTP_PORT"],
            username=config["SFTP_USERNAME"],
            password=config["SFTP_PASSWORD"],
            remote_dir=config["SFTP_REMOTE_DIR"],
            local_dir=work_dir,
            filename_pattern=config["SFTP_FILENAME_PATTERN"],
            date_fmt=config["SFTP_DATE_FMT"],
        )
        log_entry["bai_file"] = os.path.basename(local_bai_path)
        logger.info(f"Downloaded: {local_bai_path}")

        logger.info("Step 2: Parsing BAI2 file...")
        with open(local_bai_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        file_record = parse_bai2(content)
        balance_rows = file_to_balances_rows(file_record)
        transaction_rows = file_to_transaction_rows(file_record)
        logger.info(
            f"Parsed {len(balance_rows)} balance rows, "
            f"{len(transaction_rows)} transaction rows"
        )

        logger.info("Step 3: Writing CSVs...")
        base_name = Path(local_bai_path).stem
        balances_csv     = os.path.join(work_dir, f"{base_name}_balances.csv")
        transactions_csv = os.path.join(work_dir, f"{base_name}_transactions.csv")

        log_entry["balances_rows"]    = write_csv(balance_rows, balances_csv)
        log_entry["transaction_rows"] = write_csv(transaction_rows, transactions_csv)

        logger.info("Step 4: Uploading to Google Drive...")
        log_entry["balances_drive_id"] = upload_to_drive(
            local_file_path=balances_csv,
            drive_folder_id=config["GOOGLE_DRIVE_FOLDER_ID"],
        )
        log_entry["transactions_drive_id"] = upload_to_drive(
            local_file_path=transactions_csv,
            drive_folder_id=config["GOOGLE_DRIVE_FOLDER_ID"],
        )

        log_entry["status"] = "success"
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.info("Pipeline completed successfully.")

    except Exception as e:
        log_entry["status"] = "error"
        log_entry["error"] = str(e)
        log_entry["finished_at"] = datetime.utcnow().isoformat()
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:
        append_run_log(work_dir, log_entry)


if __name__ == "__main__":
    run()
