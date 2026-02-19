# BAI -> CSV -> Google Drive Pipeline

Pulls the prior-day BAI2 balance file from a bank SFTP, converts it to two CSVs (balances + transactions), and uploads them to a Google Drive folder. Runs automatically every weekday at 8:00 AM PST via GitHub Actions.

---

## Repo Structure

```
├── bai2_parser.py          # Full-fidelity BAI2 parser (all record types)
├── sftp_client.py          # SFTP download (username + password)
├── drive_uploader.py       # Google Drive upload (service account)
├── pipeline.py             # Main pipeline -- orchestrates all three steps
├── streamlit_app.py        # Visibility dashboard (run history, CSV preview)
├── requirements.txt
└── .github/
    └── workflows/
        └── daily_run.yml   # GitHub Actions cron schedule
```

---

## One-Time Setup

### 1. Google Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/) -> **IAM & Admin -> Service Accounts**
2. Create a new service account (e.g., `bai-pipeline`)
3. Grant it no roles (it only needs Drive access)
4. Create a JSON key -> download it
5. In Google Drive, open the target folder -> **Share** -> paste the service account email -> **Editor**
6. Copy the **folder ID** from the Drive URL: `https://drive.google.com/drive/folders/<FOLDER_ID>`

### 2. GitHub Secrets

In your repo -> **Settings -> Secrets and variables -> Actions -> New repository secret**:

| Secret Name              | Value                                                   |
|--------------------------|---------------------------------------------------------|
| `SFTP_HOST`              | Bank SFTP hostname (e.g. `sftp.yourbank.com`)           |
| `SFTP_PORT`              | Port number (usually `22`)                              |
| `SFTP_USERNAME`          | SFTP username                                           |
| `SFTP_PASSWORD`          | SFTP password                                           |
| `SFTP_REMOTE_DIR`        | Remote path (e.g. `/outgoing/balance`)                  |
| `SFTP_FILENAME_PATTERN`  | *(optional)* regex, e.g. `balance_{date}\.bai2?`        |
| `SFTP_DATE_FMT`          | *(optional)* strftime format, default `%Y%m%d`          |
| `GOOGLE_SA_JSON`         | Full contents of your service account JSON key file     |
| `GOOGLE_DRIVE_FOLDER_ID` | Google Drive folder ID (from the folder URL)            |

### 3. Enable the Workflow

Go to **Actions** in your repo and enable workflows if prompted. The pipeline will then run automatically at 8:00 AM PST on weekdays. You can also trigger it manually from **Actions -> BAI -> CSV -> Google Drive (Daily) -> Run workflow**.

---

## Output Files

Each run produces two CSVs uploaded to your Drive folder:

### `<bai_filename>_balances.csv`
One row per balance entry per account:
`file_sender_id`, `file_receiver_id`, `file_creation_date`, `file_creation_time`, `resend_indicator`, `group_originator_id`, `group_receiver_id`, `group_status`, `as_of_date`, `as_of_time`, `as_of_date_modifier`, `currency_code`, `customer_account`, `balance_type_code`, `balance_amount`, `balance_item_count`, `balance_funds_type`, `account_control_total`, `account_record_count`, `group_control_total`, `group_record_count`, `file_control_total`, `file_record_count`

### `<bai_filename>_transactions.csv`
One row per transaction:
`file_sender_id`, `file_receiver_id`, `file_creation_date`, `file_creation_time`, `group_originator_id`, `group_receiver_id`, `as_of_date`, `as_of_time`, `as_of_date_modifier`, `customer_account`, `currency_code`, `type_code`, `amount`, `funds_type`, `bank_ref`, `customer_ref`, `text`

---

## Streamlit Dashboard

Deploy to [Streamlit Community Cloud](https://streamlit.io/cloud) for free:
1. Connect your GitHub repo
2. Set `streamlit_app.py` as the entry point
3. Add the same env vars as secrets in Streamlit settings

Or run locally:
```bash
pip install -r requirements.txt
export LOCAL_WORK_DIR=/tmp/bai_pipeline
streamlit run streamlit_app.py
```

---

## Schedule

| Setting        | Value                   |
|----------------|-------------------------|
| Frequency      | Weekdays (Mon-Fri)      |
| Time           | 8:00 AM PST (16:00 UTC) |
| Cron           | `0 16 * * 1-5`          |
| Manual trigger | Yes (GitHub Actions UI) |

To run 7 days a week, change `1-5` to `*` in `daily_run.yml`.

---

## Error Handling

- If the BAI file is not found on SFTP, the pipeline exits non-zero -> GitHub Actions marks the run **failed** and emails repo watchers
- CSV artifacts are retained in GitHub Actions for 7 days regardless of pass/fail
- Run history is written to `run_log.json` for the Streamlit dashboard
