"""
streamlit_app.py
Visibility dashboard for the BAI -> CSV -> Google Drive pipeline.

Shows:
  - Run history (status, file pulled, row counts, timestamps)
  - CSV previews (balances + transactions) from the latest successful run
  - Manual trigger button (runs pipeline.py as a subprocess)
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

WORK_DIR = os.environ.get("LOCAL_WORK_DIR", "/tmp/bai_pipeline")
RUN_LOG  = os.path.join(WORK_DIR, "run_log.json")

st.set_page_config(
    page_title="BAI Pipeline Dashboard",
    page_icon="🏦",
    layout="wide",
)

st.title("BAI -> CSV -> Google Drive Pipeline")
st.caption("Prior-day bank balance file automation")

with st.sidebar:
    st.header("Controls")
    if st.button("Run Pipeline Now", type="primary", use_container_width=True):
        with st.spinner("Running pipeline..."):
            result = subprocess.run(
                [sys.executable, "pipeline.py"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent,
            )
        if result.returncode == 0:
            st.success("Pipeline completed successfully.")
        else:
            st.error("Pipeline failed. See logs below.")
        with st.expander("stdout"):
            st.code(result.stdout or "(empty)")
        with st.expander("stderr"):
            st.code(result.stderr or "(empty)")
    st.divider()
    st.caption("Scheduled: daily at 8:00 AM PST via GitHub Actions")

st.subheader("Run History")

if not os.path.exists(RUN_LOG):
    st.info("No runs recorded yet. Trigger the pipeline manually or wait for the scheduled run.")
else:
    with open(RUN_LOG) as f:
        history = json.load(f)

    if not history:
        st.info("No runs recorded yet.")
    else:
        display_cols = [
            "started_at", "status", "bai_file",
            "balances_rows", "transaction_rows", "finished_at", "error",
        ]
        df_history = pd.DataFrame(history)[display_cols]
        df_history.columns = [
            "Started (UTC)", "Status", "BAI File",
            "Balance Rows", "Txn Rows", "Finished (UTC)", "Error",
        ]

        def _color_status(val):
            if val == "success":
                return "background-color: #d4edda; color: #155724"
            elif val == "error":
                return "background-color: #f8d7da; color: #721c24"
            return ""

        styled = df_history.style.applymap(_color_status, subset=["Status"])
        st.dataframe(styled, use_container_width=True, height=300)

        successful = [r for r in history if r.get("status") == "success"]
        if successful:
            latest = successful[0]
            base = Path(latest.get("bai_file", "")).stem

            st.divider()
            st.subheader(f"Latest Successful Run - {latest['started_at'][:10]}")

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("BAI File", latest.get("bai_file", "-"))
            col2.metric("Balance Rows", latest.get("balances_rows", 0))
            col3.metric("Transaction Rows", latest.get("transaction_rows", 0))
            col4.metric("Status", latest.get("status", "-").upper())

            balances_csv     = os.path.join(WORK_DIR, f"{base}_balances.csv")
            transactions_csv = os.path.join(WORK_DIR, f"{base}_transactions.csv")

            tab1, tab2 = st.tabs(["Balances CSV", "Transactions CSV"])

            with tab1:
                if os.path.exists(balances_csv):
                    df_bal = pd.read_csv(balances_csv)
                    st.dataframe(df_bal, use_container_width=True)
                    st.download_button(
                        "Download Balances CSV",
                        data=open(balances_csv, "rb").read(),
                        file_name=os.path.basename(balances_csv),
                        mime="text/csv",
                    )
                else:
                    st.warning("Balances CSV not found locally.")

            with tab2:
                if os.path.exists(transactions_csv):
                    df_txn = pd.read_csv(transactions_csv)
                    st.dataframe(df_txn, use_container_width=True)
                    st.download_button(
                        "Download Transactions CSV",
                        data=open(transactions_csv, "rb").read(),
                        file_name=os.path.basename(transactions_csv),
                        mime="text/csv",
                    )
                else:
                    st.warning("Transactions CSV not found locally.")
