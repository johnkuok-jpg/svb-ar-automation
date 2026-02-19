"""
matcher.py

Matches bank transactions to open NetSuite AR invoices.

Matching strategy (scored out of 100):
  - Amount match (exact):         50 pts
  - Amount match (within 1%):     30 pts
  - Customer name fuzzy match:    up to 50 pts (token_set_ratio)

A match is accepted if total score >= 60.
If multiple invoices score the same, the one with the closest amount wins.

Returns a list of dicts mirroring the input transactions, with extra columns:
    matched_customer, invoice_number, match_confidence, netsuite_url
"""

from rapidfuzz import fuzz


AMOUNT_EXACT_PTS  = 50
AMOUNT_CLOSE_PTS  = 30   # within 1%
NAME_MAX_PTS      = 50
MIN_SCORE         = 60   # minimum to be considered a match


def _amount_score(txn_amount: float, inv_amount: float) -> int:
    if txn_amount <= 0 or inv_amount <= 0:
        return 0
    if abs(txn_amount - inv_amount) < 0.01:
        return AMOUNT_EXACT_PTS
    if abs(txn_amount - inv_amount) / max(txn_amount, inv_amount) <= 0.01:
        return AMOUNT_CLOSE_PTS
    return 0


def _name_score(description: str, entity_name: str) -> int:
    if not description or not entity_name:
        return 0
    return int(fuzz.token_set_ratio(description.upper(), entity_name.upper()) * NAME_MAX_PTS / 100)


def match_transactions(transactions: list[dict], invoices: list[dict]) -> list[dict]:
    """
    For each transaction, find the best-matching open invoice.
    Only credits (non-empty Credit Amount) are matched - debits are passed through unmatched.

    Args:
        transactions: list of dicts in SVB CSV column format
        invoices:     list of dicts from netsuite_client.fetch_open_invoices()

    Returns:
        list of dicts - same as transactions with 4 extra columns appended
    """
    results = []

    for txn in transactions:
        # Only try to match credits
        credit_str = str(txn.get("Credit Amount", "")).replace(",", "").strip()
        try:
            txn_amount = float(credit_str) if credit_str else 0.0
        except ValueError:
            txn_amount = 0.0

        description = str(txn.get("Description", ""))

        best_score   = 0
        best_invoice = None

        if txn_amount > 0:
            for inv in invoices:
                a_score = _amount_score(txn_amount, inv["amount_remaining"])
                n_score = _name_score(description, inv["entity_name"])
                total   = a_score + n_score

                if total > best_score:
                    best_score   = total
                    best_invoice = inv
                elif total == best_score and best_invoice is not None:
                    # Tiebreak: pick invoice with closer amount
                    if abs(txn_amount - inv["amount_remaining"]) < abs(txn_amount - best_invoice["amount_remaining"]):
                        best_invoice = inv

        if best_invoice and best_score >= MIN_SCORE:
            matched_customer = best_invoice["entity_name"]
            invoice_number   = best_invoice["tranid"]
            confidence       = f"{min(best_score, 100)}%"
            netsuite_url     = f'=HYPERLINK("{best_invoice["netsuite_url"]}","Open in NetSuite")'
        else:
            matched_customer = ""
            invoice_number   = ""
            confidence       = ""
            netsuite_url     = ""

        results.append({
            **txn,
            "Matched Customer": matched_customer,
            "Invoice #":        invoice_number,
            "Confidence":       confidence,
            "NetSuite Link":    netsuite_url,
        })

    return results
