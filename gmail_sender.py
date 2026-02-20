"""
gmail_sender.py

Sends emails via the Gmail API using OAuth2.
Reads credentials from environment variables (same pattern as pipeline.py).

Required env vars / st.secrets:
    GOOGLE_CLIENT_ID
    GOOGLE_CLIENT_SECRET
    GOOGLE_REFRESH_TOKEN
    GMAIL_SENDER          (optional, defaults to authenticated user)
"""

import base64
import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TOKEN_URI = "https://oauth2.googleapis.com/token"

def _secret(key: str, default: str = None) -> str:
    """Read from st.secrets (Streamlit Cloud) or os.environ (GitHub Actions / local)."""
    try:
        import streamlit as st
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    val = os.environ.get(key, default)
    if val is None:
        raise KeyError(key)
    return val

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def _get_gmail_service():
    creds = Credentials(
        token=None,
        refresh_token=_secret("GOOGLE_REFRESH_TOKEN"),
        token_uri=TOKEN_URI,
        client_id=_secret("GOOGLE_CLIENT_ID"),
        client_secret=_secret("GOOGLE_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)

def send_email(to: str, subject: str, body: str, sender: str = None,
               cc: str = None,
               pdf_bytes: bytes = None, pdf_filename: str = None) -> dict:
    """
    Send a plain-text email via Gmail API, with optional CC and PDF attachment.

    Args:
        to:           Recipient email address(es), comma-separated
        cc:           CC email address(es), comma-separated (optional)
        subject:      Email subject line
        body:         Plain text email body
        sender:       From address (defaults to GMAIL_SENDER secret)
        pdf_bytes:    Raw PDF bytes to attach (optional)
        pdf_filename: Attachment filename, e.g. 'INV-1234.pdf' (optional)

    Returns:
        Gmail API message resource dict with 'id' and 'threadId'
    """
    from_addr = sender or _secret("GMAIL_SENDER", "me")

    msg = MIMEMultipart("mixed")
    msg["To"] = to
    msg["From"] = from_addr
    if cc:
        msg["Cc"] = cc
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if pdf_bytes:
        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header(
            "Content-Disposition",
            "attachment",
            filename=pdf_filename or "invoice.pdf"
        )
        msg.attach(part)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service = _get_gmail_service()
    result = service.users().messages().send(
        userId="me",
        body={"raw": raw}
    ).execute()
    return result
