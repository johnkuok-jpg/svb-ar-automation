"""
past_due_dashboard.py

Streamlit dashboard for past due AR invoices.
- Password-protected login
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
    GMAIL_SENDER       (optional, defaults to john.kuok@perplexity.ai)
    DASHBOARD_PASSWORD (required — blocks access without password)
"""

import os
from datetime import datetime, timezone

import pandas as pd
import requests as _requests
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from netsuite_client import fetch_past_due_invoices, fetch_invoice_pdf
from gmail_sender import send_email

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Past Due AR Dashboard",
    page_icon="💰",
    layout="wide",
)

# ── Secrets helper ────────────────────────────────────────────────────────────
def _secret(key: str, default: str = None) -> str:
    """Read from st.secrets (Streamlit Cloud) or os.environ (GitHub Actions / local)."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    val = os.environ.get(key, default)
    if val is None:
        raise KeyError(key)
    return val

# ── Password gate ─────────────────────────────────────────────────────────────
def _check_password():
    correct = _secret("DASHBOARD_PASSWORD", "")
    if not correct:
        return
    if st.session_state.get("authenticated"):
        return
    st.title("💰 Past Due AR Dashboard")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == correct:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

_check_password()

# ── Constants ─────────────────────────────────────────────────────────────────
TOKEN_URI   = "https://oauth2.googleapis.com/token"
SCOPES      = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
LOG_TAB     = "email_log"
SHEET_ID    = _secret("GOOGLE_SHEET_ID", "1PDLXi7ZQxvDSeUbdf7_5ft1Npq7oIBad9PgTl0R2CpM")
SENDER      = _secret("GMAIL_SENDER", "john.kuok@perplexity.ai")
AR_CC       = "ar@perplexity.ai"
SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"

# ── Google credentials (requests-based, no httplib2) ─────────────────────────

@st.cache_resource(show_spinner=False)
def _get_creds() -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=_secret("GOOGLE_REFRESH_TOKEN"),
        token_uri=TOKEN_URI,
        client_id=_secret("GOOGLE_CLIENT_ID"),
        client_secret=_secret("GOOGLE_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds

def _auth_headers() -> dict:
    creds = _get_creds()
    if not creds.valid:
        creds.refresh(Request())
    return {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}

def _sheets_get(path: str, params: dict = None):
    r = _requests.get(f"{SHEETS_BASE}{path}", headers=_auth_headers(), params=params)
    r.raise_for_status()
    return r.json()

def _sheets_post(path: str, json: dict):
    r = _requests.post(f"{SHEETS_BASE}{path}", headers=_auth_headers(), json=json)
    r.raise_for_status()
    return r.json()

def _sheets_put(path: str, params: dict, json: dict):
    r = _requests.put(f"{SHEETS_BASE}{path}", headers=_auth_headers(), params=params, json=json)
    r.raise_for_status()
    return r.json()

# ── Google Sheets helpers ─────────────────────────────────────────────────────

def _ensure_log_tab():
    meta = _sheets_get(f"/{SHEET_ID}")
    existing = [s["properties"]["title"] for s in meta["sheets"]]
    if LOG_TAB not in existing:
        _sheets_post(f"/{SHEET_ID}:batchUpdate", {
            "requests": [{"addSheet": {"properties": {"title": LOG_TAB}}}]
        })
        _sheets_put(
            f"/{SHEET_ID}/values/{LOG_TAB}!A1",
            params={"valueInputOption": "RAW"},
            json={"values": [["Timestamp", "Sent By", "Invoice #", "Customer", "To Email", "CC", "Subject", "Body"]]},
        )

def _log_email(invoice_id: str, customer: str, to_email: str, cc_email: str, subject: str, body: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    r = _requests.post(
        f"{SHEETS_BASE}/{SHEET_ID}/values/{LOG_TAB}!A1:append",
        headers=_auth_headers(),
        params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
        json={"values": [[ts, SENDER, invoice_id, customer, to_email, cc_email, subject, body]]},
    )
    r.raise_for_status()

def _load_email_log() -> pd.DataFrame:
    empty = pd.DataFrame(columns=["Timestamp", "Sent By", "Invoice #", "Customer", "To Email", "CC", "Subject", "Body"])
    try:
        data = _sheets_get(f"/{SHEET_ID}/values/{LOG_TAB}!A:H")
        rows = data.get("values", [])
        if len(rows) <= 1:
            return empty
        return pd.DataFrame(rows[1:], columns=rows[0])
    except Exception:
        return empty

# ── NetSuite data ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Fetching past due invoices from NetSuite...")
def load_invoices():
    return fetch_past_due_invoices()

# ── Email draft helpers ───────────────────────────────────────────────────────

def default_subject(inv: dict) -> str:
    return f"Past Due Invoice {inv['tranid']} – {inv['entity_name']}"

def default_body(inv: dict) -> str:
    amount = f"${inv['amount_due']:,.2f} {inv['currency']}"
    return f"""Hi,

I hope this message finds you well. I'm reaching out regarding invoice {inv['tranid']} for {amount}, which was due on {inv['due_date']} ({inv['days_overdue']} days ago).

Could you please let us know the status of this payment? If you have already sent it, please disregard this message.

If you have any questions or need a copy of the invoice, please don't hesitate to reach out.

Best regards,
Perplexity AI — Accounts Receivable
{SENDER}"""

# ── UI ────────────────────────────────────────────────────────────────────────

st.title("💰 Past Due AR Dashboard")
st.caption(f"Data refreshes every 5 minutes  ·  Sending from **{SENDER}**")

_ensure_log_tab()

tab_invoices, tab_log = st.tabs(["📋 Past Due Invoices", "📨 Email Log"])

with tab_invoices:
    with st.spinner("Loading invoices..."):
        invoices = load_invoices()

    if not invoices:
        st.success("No past due invoices found.")
        st.stop()

    df = pd.DataFrame(invoices)

    col1, col2, col3 = st.columns(3)
    col1.metric("Past Due Invoices", len(df))
    col2.metric("Total Amount Due", f"${df['amount_due'].sum():,.2f}")
    col3.metric("Avg Days Overdue", f"{df['days_overdue'].mean():.0f} days")

    st.divider()

    def highlight_overdue(val):
        if isinstance(val, (int, float)):
            if val > 90:
                return "background-color: #ffd6d6"
            elif val > 30:
                return "background-color: #fff3cd"
        return ""

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

    styled = display_df.style.map(highlight_overdue, subset=["Days Overdue"]) \
        .format({"Amount Due": "${:,.2f}"})

    st.dataframe(styled, width="stretch", hide_index=True)

    st.divider()
    st.subheader("✉️ Send Follow-Up Email")

    invoice_options = {
        f"{inv['tranid']} — {inv['entity_name']} (${inv['amount_due']:,.2f}, {inv['days_overdue']}d overdue)": inv
        for inv in invoices
    }
    selected_label = st.selectbox("Select invoice", list(invoice_options.keys()))
    selected_inv = invoice_options[selected_label]

    to_email = st.text_input("To", value=selected_inv.get("billing_email", ""))
    cc_email = st.text_input("CC", value=AR_CC)
    subject  = st.text_input("Subject", value=default_subject(selected_inv))
    body     = st.text_area("Message", value=default_body(selected_inv), height=300)

    col_send, col_ns = st.columns([1, 4])

    with col_send:
        send_clicked = st.button("Send Email", type="primary", use_container_width=True)

    with col_ns:
        st.link_button(
            "Open in NetSuite ↗",
            selected_inv["netsuite_url"],
            use_container_width=False
        )

    # PDF preview / download
    with st.expander("📎 Attach Invoice PDF", expanded=True):
        attach_pdf = st.checkbox("Attach PDF to email", value=True)
        if st.button("Preview / Download PDF"):
            with st.spinner("Fetching PDF from NetSuite..."):
                try:
                    pdf_data = fetch_invoice_pdf(selected_inv["id"])
                    st.download_button(
                        label="⬇️ Download PDF",
                        data=pdf_data,
                        file_name=f"{selected_inv['tranid']}.pdf",
                        mime="application/pdf",
                    )
                except Exception as e:
                    st.error(f"Could not fetch PDF: {e}")

    if send_clicked:
        if not to_email:
            st.error("No billing email on file for this customer. Please enter one manually.")
        else:
            with st.spinner("Sending..."):
                try:
                    pdf_bytes = None
                    pdf_filename = None
                    if attach_pdf:
                        try:
                            pdf_bytes = fetch_invoice_pdf(selected_inv["id"])
                            pdf_filename = f"{selected_inv['tranid']}.pdf"
                        except Exception as e:
                            st.warning(f"Could not fetch PDF, sending without attachment: {e}")
                    send_email(
                        to=to_email, cc=cc_email, subject=subject, body=body, sender=SENDER,
                        pdf_bytes=pdf_bytes, pdf_filename=pdf_filename,
                    )
                    _log_email(
                        invoice_id=selected_inv["tranid"],
                        customer=selected_inv["entity_name"],
                        to_email=to_email,
                        cc_email=cc_email,
                        subject=subject,
                        body=body,
                    )
                    st.success(f"Email sent to **{to_email}**{', CC: ' + cc_email if cc_email else ''}{' with PDF attached' if pdf_bytes else ''} and logged.")
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
            width="stretch",
            hide_index=True,
        )
        st.caption(f"{len(log_df)} emails sent total")
