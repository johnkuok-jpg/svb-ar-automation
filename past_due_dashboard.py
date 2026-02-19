"""
past_due_dashboard.py

Streamlit dashboard for past due AR invoices.
- Fetches past due invoices from NetSuite
- Shows table: Customer, Email, Invoice #, Amount Due, Due Date, Days Overdue
- Click a row to open an editable email draft
- Send triggers Gmail API (from john.kuok@perplexity.ai)
- Logs every sent email to the email_log tab in Google Sheet
- Shows full send history from email_log tab

Run locally:
    streamlit run past_due_dashboard.py

Required env vars / st.secrets:
    NETSUITE_ACCOUNT_ID, NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET
    NETSUITE_TOKEN_ID, NETSUITE_TOKEN_SECRET
    GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
    GOOGLE_SHEET_ID
    GMAIL_SENDER  (optional, defaults to john.kuok@perplexity.ai)
"""

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from netsuite_client import fetch_past_due_invoices
from gmail_sender import send_email

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Past Due AR Dashboard",
    page_icon="💰",
    layout="wide",
)

TOKEN_URI = "https://oauth2.googleapis.com/token"
SCOPES    = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
LOG_TAB = "email_log"


def _secret(key: str, default: str = None) -> str:
    """Read from st.secrets (Streamlit Cloud) or os.environ (GitHub Actions / local)."""
    if key in st.secrets:
        return st.secrets[key]
    val = os.environ.get(key, default)
    if val is None:
        raise KeyError(key)
    return val


SHEET_ID = _secret("GOOGLE_SHEET_ID", "1PDLXi7ZQxvDSeUbdf7_5ft1Npq7oIBad9PgTl0R2CpM")
SENDER   = _secret("GMAIL_SENDER", "john.kuok@perplexity.ai")

# ── Google Sheets helpers ──────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _sheets_service():
    creds = Credentials(
        token=None,
        refresh_token=_secret("GOOGLE_REFRESH_TOKEN"),
        token_uri=TOKEN_URI,
        client_id=_secret("GOOGLE_CLIENT_ID"),
        client_secret=_secret("GOOGLE_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


def _ensure_log_tab():
    sheets = _sheets_service()
    meta = sheets.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    existing = [s["properties"]["title"] for s in meta["sheets"]]
    if LOG_TAB not in existing:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": LOG_TAB}}}]}
        ).execute()
        # Write header
        sheets.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{LOG_TAB}!A1",
            valueInputOption="RAW",
            body={"values": [["Timestamp", "Sent By", "Invoice #", "Customer", "To Email", "Subject", "Body"]]}
        ).execute()


def _log_email(invoice_id: str, customer: str, to_email: str, subject: str, body: str):
    sheets = _sheets_service()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sheets.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{LOG_TAB}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[ts, SENDER, invoice_id, customer, to_email, subject, body]]}
    ).execute()


def _load_email_log() -> pd.DataFrame:
    sheets = _sheets_service()
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{LOG_TAB}!A:G"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return pd.DataFrame(columns=["Timestamp", "Sent By", "Invoice #", "Customer", "To Email", "Subject", "Body"])
        return pd.DataFrame(rows[1:], columns=rows[0])
    except Exception:
        return pd.DataFrame(columns=["Timestamp", "Sent By", "Invoice #", "Customer", "To Email", "Subject", "Body"])


# ── NetSuite data ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Fetching past due invoices from NetSuite...")
def load_invoices():
    return fetch_past_due_invoices()


# ── Email draft helper ─────────────────────────────────────────────────────────

def default_subject(inv: dict) -> str:
    return f"Past Due Invoice {inv['tranid']} \u2013 {inv['entity_name']}"


def default_body(inv: dict) -> str:
    amount = f"${inv['amount_due']:,.2f} {inv['currency']}"
    return f"""Hi,

I hope this message finds you well. I'm reaching out regarding invoice {inv['tranid']} for {amount}, which was due on {inv['due_date']} ({inv['days_overdue']} days ago).

Could you please let us know the status of this payment? If you have already sent it, please disregard this message.

If you have any questions or need a copy of the invoice, please don't hesitate to reach out.

Best regards,
Perplexity AI \u2014 Accounts Receivable
{SENDER}"""


# ── UI ─────────────────────────────────────────────────────────────────────────

st.title("💰 Past Due AR Dashboard")
st.caption(f"Data refreshes every 5 minutes  \u00b7  Sending from **{SENDER}**")

# Ensure log tab exists
_ensure_log_tab()

# Tabs
tab_invoices, tab_log = st.tabs(["📋 Past Due Invoices", "📨 Email Log"])

with tab_invoices:
    with st.spinner("Loading invoices..."):
        invoices = load_invoices()

    if not invoices:
        st.success("No past due invoices found.")
        st.stop()

    df = pd.DataFrame(invoices)

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Past Due Invoices", len(df))
    col2.metric("Total Amount Due", f"${df['amount_due'].sum():,.2f}")
    col3.metric("Avg Days Overdue", f"{df['days_overdue'].mean():.0f} days")

    st.divider()

    # Color-code days overdue
    def highlight_overdue(val):
        if isinstance(val, (int, float)):
            if val > 90:
                return "background-color: #ffd6d6"
            elif val > 30:
                return "background-color: #fff3cd"
        return ""

    # Display table
    display_df = df[[
        "tranid", "entity_name", "billing_email",
        "amount_due", "due_date", "days_overdue"
    ]].rename(columns={
        "tranid":        "Invoice #",
        "entity_name":   "Customer",
        "billing_email": "Billing Email",
        "amount_due":    "Amount Due",
        "due_date":      "Due Date",
        "days_overdue":  "Days Overdue",
    })

    styled = display_df.style.applymap(highlight_overdue, subset=["Days Overdue"]) \
        .format({"Amount Due": "${:,.2f}"})

    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("✉️ Send Follow-Up Email")

    # Invoice selector
    invoice_options = {
        f"{inv['tranid']} \u2014 {inv['entity_name']} (${inv['amount_due']:,.2f}, {inv['days_overdue']}d overdue)": inv
        for inv in invoices
    }
    selected_label = st.selectbox("Select invoice", list(invoice_options.keys()))
    selected_inv = invoice_options[selected_label]

    # Pre-fill editable fields
    to_email = st.text_input("To", value=selected_inv.get("billing_email", ""))
    subject  = st.text_input("Subject", value=default_subject(selected_inv))
    body     = st.text_area("Message", value=default_body(selected_inv), height=300)

    col_send, col_ns = st.columns([1, 4])

    with col_send:
        send_clicked = st.button("Send Email", type="primary", use_container_width=True)

    with col_ns:
        st.link_button(
            "Open in NetSuite \u2197",
            selected_inv["netsuite_url"],
            use_container_width=False
        )

    if send_clicked:
        if not to_email:
            st.error("No billing email on file for this customer. Please enter one manually.")
        else:
            with st.spinner("Sending..."):
                try:
                    send_email(to=to_email, subject=subject, body=body, sender=SENDER)
                    _log_email(
                        invoice_id=selected_inv["tranid"],
                        customer=selected_inv["entity_name"],
                        to_email=to_email,
                        subject=subject,
                        body=body,
                    )
                    st.success(f"Email sent to **{to_email}** and logged.")
                    # Clear cache so log tab refreshes
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Failed to send: {e}")

with tab_log:
    st.subheader("📨 Email Send History")
    with st.spinner("Loading log..."):
        log_df = _load_email_log()

    if log_df.empty:
        st.info("No emails sent yet.")
    else:
        st.dataframe(
            log_df.sort_values("Timestamp", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(log_df)} emails sent total")
