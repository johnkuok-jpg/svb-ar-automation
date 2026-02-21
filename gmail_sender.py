"""
gmail_sender.py

Sends emails via the Gmail API using OAuth2.
Uses a *separate* refresh token (AR_GMAIL_REFRESH_TOKEN) that was
authorised as ar@perplexity.ai so that userId="me" sends from that
mailbox.  Falls back to GOOGLE_REFRESH_TOKEN if the AR token is not set.

Required env vars / st.secrets:
    GOOGLE_CLIENT_ID
    GOOGLE_CLIENT_SECRET
    AR_GMAIL_REFRESH_TOKEN   (authorised as ar@perplexity.ai)
    GMAIL_SENDER             (optional, defaults to ar@perplexity.ai)
"""

import base64
import html as _html
import os
import pathlib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TOKEN_URI = "https://oauth2.googleapis.com/token"
SENDER_NAME = "Perplexity AR"

# Logo for HTML email signature (base64-encoded PNG, loaded once)
_LOGO_PATH = pathlib.Path(__file__).with_name("perplexity_logo.png")
try:
    _LOGO_B64 = base64.b64encode(_LOGO_PATH.read_bytes()).decode()
except FileNotFoundError:
    _LOGO_B64 = None

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
    # Use the AR-specific token so userId="me" resolves to ar@perplexity.ai
    refresh_token = _secret("AR_GMAIL_REFRESH_TOKEN",
                            _secret("GOOGLE_REFRESH_TOKEN"))
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=TOKEN_URI,
        client_id=_secret("GOOGLE_CLIENT_ID"),
        client_secret=_secret("GOOGLE_CLIENT_SECRET"),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


def _signature_html(sender_addr: str) -> str:
    """Build an HTML email signature with the Perplexity logo."""
    logo_tag = ""
    if _LOGO_B64:
        logo_tag = (
            '<img src="data:image/png;base64,'
            f'{_LOGO_B64}"'
            ' alt="Perplexity" width="140" style="display:block;margin-bottom:8px" />'
        )
    return (
        '<table cellpadding="0" cellspacing="0" border="0" '
        'style="margin-top:24px;font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222">'
        '<tr><td style="padding-bottom:4px">'
        f'{logo_tag}'
        '</td></tr>'
        '<tr><td style="font-weight:bold;padding-bottom:2px">Perplexity AR</td></tr>'
        f'<tr><td><a href="mailto:{_html.escape(sender_addr)}" '
        f'style="color:#1a73e8;text-decoration:none">{_html.escape(sender_addr)}</a></td></tr>'
        '</table>'
    )


def _plain_to_html(text: str, sender_addr: str) -> str:
    """Convert plain text to simple HTML that preserves line breaks and looks
    natural in email clients (full-width, normal font size).
    Replaces the plain-text signature block with a branded HTML signature."""
    safe = _html.escape(text)

    # Split off signature (everything from "Best regards," onward)
    sig_marker = _html.escape("Best regards,")
    if sig_marker in safe:
        body_text, _ = safe.split(sig_marker, 1)
    else:
        body_text = safe

    paragraphs = body_text.strip().split("\n\n")
    body_parts = []
    for p in paragraphs:
        lines = p.strip().replace("\n", "<br>\n")
        if lines:
            body_parts.append(f'<p style="margin:0 0 16px 0">{lines}</p>')

    # Add "Best regards," back as text, then the branded signature
    body_parts.append('<p style="margin:0 0 4px 0">Best regards,</p>')
    body_parts.append(_signature_html(sender_addr))

    inner = "\n".join(body_parts)
    return (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;'
        'color:#222;line-height:1.6;max-width:600px">\n'
        f'{inner}\n'
        '</div>'
    )


def send_email(to: str, subject: str, body: str, sender: str = None,
               cc: str = None,
               pdf_bytes: bytes = None, pdf_filename: str = None) -> dict:
    """
    Send an HTML email via Gmail API, with optional CC and PDF attachment.

    The plain-text body is auto-converted to simple HTML so it renders at
    normal width in all email clients.  A plain-text fallback is included.

    Args:
        to:           Recipient email address(es), comma-separated
        cc:           CC email address(es), comma-separated (optional)
        subject:      Email subject line
        body:         Plain text email body (converted to HTML automatically)
        sender:       From address (defaults to GMAIL_SENDER secret)
        pdf_bytes:    Raw PDF bytes to attach (optional)
        pdf_filename: Attachment filename, e.g. 'INV-1234.pdf' (optional)

    Returns:
        Gmail API message resource dict with 'id' and 'threadId'
    """
    raw_addr = sender or _secret("GMAIL_SENDER", "ar@perplexity.ai")
    from_addr = f"{SENDER_NAME} <{raw_addr}>"

    msg = MIMEMultipart("mixed")
    msg["To"] = to
    msg["From"] = from_addr
    if cc:
        msg["Cc"] = cc
    msg["Subject"] = subject

    # Attach both plain text and HTML so every client gets a good render
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(body, "plain"))
    alt.attach(MIMEText(_plain_to_html(body, raw_addr), "html"))
    msg.attach(alt)

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
