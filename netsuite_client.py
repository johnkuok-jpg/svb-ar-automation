"""
netsuite_client.py

Queries NetSuite for open AR invoices via SuiteQL REST API.
Uses Token-Based Authentication (TBA) with HMAC-SHA256 OAuth 1.0a signing.

Returns a list of dicts with keys:
    id, tranid, entity_name, amount_remaining, currency, due_date, netsuite_url
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

ACCOUNT_ID = os.environ.get("NETSUITE_ACCOUNT_ID", "9060638")
CONSUMER_KEY    = os.environ["NETSUITE_CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["NETSUITE_CONSUMER_SECRET"]
TOKEN_ID        = os.environ["NETSUITE_TOKEN_ID"]
TOKEN_SECRET    = os.environ["NETSUITE_TOKEN_SECRET"]

# NetSuite REST endpoint
BASE_URL = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
INVOICE_URL_TEMPLATE = (
    f"https://{ACCOUNT_ID}.app.netsuite.com/app/accounting/transactions/custinvc.nl?id={{id}}"
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

    # Signature base string
    sorted_params = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(oauth_params.items())
    )
    base_string = "&".join([
        method.upper(),
        urllib.parse.quote(url, safe=""),
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
            t.amountremaining,
            t.currency,
            e.entityid,
            e.companyname,
            e.firstname,
            e.lastname
        FROM transaction t
        LEFT JOIN entity e ON t.entity = e.id
        WHERE t.type = 'CustInvc'
          AND t.amountremaining > 0
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
            # Build display name: prefer companyname, fall back to first+last
            entity_name = (
                row.get("companyname")
                or f"{row.get('firstname', '')} {row.get('lastname', '')}".strip()
                or row.get("entityid", "")
            )
            invoices.append({
                "id":               str(row.get("id", "")),
                "tranid":           row.get("tranid", ""),
                "entity_name":      entity_name,
                "amount_remaining": float(row.get("amountremaining", 0)),
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
