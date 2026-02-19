"""
netsuite_client.py

Queries NetSuite for open and past due AR invoices via SuiteQL REST API.
Uses Token-Based Authentication (TBA) with HMAC-SHA256 OAuth 1.0a signing.

fetch_open_invoices()    → all invoices with foreignamountunpaid > 0
fetch_past_due_invoices() → invoices where duedate < today, with billing email
"""

import hashlib
import hmac
import os
import time
import random
import string
import urllib.parse
from base64 import b64encode

import requests


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


ACCOUNT_ID      = _secret("NETSUITE_ACCOUNT_ID", "9060638")
CONSUMER_KEY    = _secret("NETSUITE_CONSUMER_KEY")
CONSUMER_SECRET = _secret("NETSUITE_CONSUMER_SECRET")
TOKEN_ID        = _secret("NETSUITE_TOKEN_ID")
TOKEN_SECRET    = _secret("NETSUITE_TOKEN_SECRET")

# NetSuite REST endpoint
BASE_URL = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
INVOICE_URL_TEMPLATE = (
    f"https://{ACCOUNT_ID}.app.netsuite.com/app/accounting/transactions/custinvc.nl?id={{id}}&whence="
)


def _nonce(length: int = 11) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def _oauth_header(method: str, url: str) -> str:
    """Build OAuth 1.0a Authorization header with HMAC-SHA256."""
    ts = str(int(time.time()))
    nonce = _nonce()

    oauth_params = {
        "oauth_consumer_key":     CONSUMER_KEY,
        "oauth_nonce":            nonce,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp":        ts,
        "oauth_token":            TOKEN_ID,
        "oauth_version":          "1.0",
    }

    # Signature base string — MUST use base URL only (no query string)
    parsed = urllib.parse.urlparse(url)
    base_url = urllib.parse.urlunparse(parsed._replace(query="", fragment=""))

    # Include any URL query params in the signature params
    all_sig_params = dict(oauth_params)
    if parsed.query:
        for k, v in urllib.parse.parse_qsl(parsed.query):
            all_sig_params[k] = v

    sorted_params = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(all_sig_params.items())
    )
    base_string = "&".join([
        method.upper(),
        urllib.parse.quote(base_url, safe=""),
        urllib.parse.quote(sorted_params, safe=""),
    ])

    # Signing key
    signing_key = f"{urllib.parse.quote(CONSUMER_SECRET, safe='')}&{urllib.parse.quote(TOKEN_SECRET, safe='')}"

    # HMAC-SHA256
    signature = b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
    ).decode()

    oauth_params["oauth_signature"] = signature
    oauth_params["realm"] = ACCOUNT_ID.upper().replace("-", "_")

    header_parts = ", ".join(
        f'{k}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {header_parts}"


def fetch_open_invoices() -> list[dict]:
    """
    Return all open AR invoices from NetSuite.
    Filters: type = CustInvc, status = open (remainingamount > 0).
    """
    query = """
        SELECT
            t.id,
            t.tranid,
            t.trandate,
            t.duedate,
            t.foreigntotal,
            t.foreignamountunpaid,
            t.currency,
            e.entityid,
            e.altname
        FROM transaction t
        LEFT JOIN entity e ON t.entity = e.id
        WHERE t.type = 'CustInvc'
          AND t.foreignamountunpaid > 0
          AND t.voided = 'F'
        ORDER BY t.trandate DESC
    """

    url = BASE_URL
    headers = {
        "Authorization": _oauth_header("POST", url),
        "Content-Type":  "application/json",
        "Prefer":        "transient",
    }
    payload = {"q": query}

    invoices = []
    offset = 0
    limit = 1000

    while True:
        paginated_url = f"{url}?limit={limit}&offset={offset}"
        headers["Authorization"] = _oauth_header("POST", paginated_url)
        resp = requests.post(paginated_url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        for row in items:
            entity_name = row.get("altname") or row.get("entityid", "")
            invoices.append({
                "id":               str(row.get("id", "")),
                "tranid":           row.get("tranid", ""),
                "entity_name":      entity_name,
                "amount_remaining": float(row.get("foreignamountunpaid", 0)),
                "currency":         row.get("currency", "USD"),
                "trandate":         row.get("trandate", ""),
                "due_date":         row.get("duedate", ""),
                "netsuite_url":     INVOICE_URL_TEMPLATE.format(id=row.get("id", "")),
            })

        # Pagination
        has_more = data.get("hasMore", False)
        if not has_more:
            break
        offset += limit

    return invoices


def fetch_past_due_invoices() -> list[dict]:
    """
    Return all past due AR invoices from NetSuite.
    Filters: type = CustInvc, duedate < today, foreignamountunpaid > 0.
    Also fetches billing email from the customer record.
    """
    query = """
        SELECT
            t.id,
            t.tranid,
            t.trandate,
            t.duedate,
            t.foreigntotal,
            t.foreignamountunpaid,
            t.currency,
            e.entityid,
            e.altname,
            e.email
        FROM transaction t
        LEFT JOIN entity e ON t.entity = e.id
        WHERE t.type = 'CustInvc'
          AND t.foreignamountunpaid > 0
          AND t.voided = 'F'
          AND t.duedate < CURRENT_DATE
        ORDER BY t.duedate ASC
    """

    invoices = []
    offset = 0
    limit = 1000

    while True:
        paginated_url = f"{BASE_URL}?limit={limit}&offset={offset}"
        headers = {
            "Authorization": _oauth_header("POST", paginated_url),
            "Content-Type":  "application/json",
            "Prefer":        "transient",
        }
        resp = requests.post(paginated_url, json={"q": query}, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        from datetime import date
        today = date.today()

        for row in data.get("items", []):
            entity_name = row.get("altname") or row.get("entityid", "")

            # Calculate days overdue
            days_overdue = 0
            due_date_str = row.get("duedate", "")
            if due_date_str:
                try:
                    from datetime import datetime
                    due_dt = datetime.strptime(due_date_str, "%m/%d/%Y").date()
                    days_overdue = (today - due_dt).days
                except ValueError:
                    pass

            invoices.append({
                "id":               str(row.get("id", "")),
                "tranid":           row.get("tranid", ""),
                "entity_name":      entity_name,
                "billing_email":    row.get("email", ""),
                "amount_due":       float(row.get("foreignamountunpaid", 0)),
                "invoice_total":    float(row.get("foreigntotal", 0)),
                "currency":         row.get("currency", "USD"),
                "trandate":         row.get("trandate", ""),
                "due_date":         due_date_str,
                "days_overdue":     days_overdue,
                "netsuite_url":     INVOICE_URL_TEMPLATE.format(id=row.get("id", "")),
            })

        if not data.get("hasMore", False):
            break
        offset += limit

    return invoices
