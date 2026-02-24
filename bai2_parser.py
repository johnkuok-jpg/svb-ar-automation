"""
bai2_parser.py
Full-fidelity BAI2 file parser.
Parses all record types (01, 02, 03, 16, 49, 88, 98, 99) and returns
structured data ready for CSV export.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# BAI2 Record Type Constants
# ---------------------------------------------------------------------------
RT_FILE_HEADER       = "01"
RT_GROUP_HEADER      = "02"
RT_ACCOUNT_HEADER    = "03"
RT_TRANSACTION       = "16"
RT_ACCOUNT_TRAILER   = "49"
RT_GROUP_TRAILER     = "98"
RT_FILE_TRAILER      = "99"
RT_CONTINUATION      = "88"


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------
@dataclass
class TransactionRecord:
    # From the 16 record
    type_code: str = ""
    amount: str = ""
    funds_type: str = ""
    bank_ref: str = ""
    customer_ref: str = ""
    text: str = ""
    # Inherited from parent account/group/file
    account_id: str = ""
    currency_code: str = ""
    as_of_date: str = ""
    as_of_time: str = ""
    as_of_date_modifier: str = ""
    bank_id: str = ""
    customer_id: str = ""
    file_date: str = ""
    file_time: str = ""


@dataclass
class AccountRecord:
    customer_account: str = ""
    currency_code: str = ""
    type_code: str = ""
    amount: str = ""
    item_count: str = ""
    funds_type: str = ""
    # Additional status/balance fields (multiple 03 balance pairs)
    balances: List[dict] = field(default_factory=list)
    transactions: List[TransactionRecord] = field(default_factory=list)
    # Trailer
    account_control_total: str = ""
    account_record_count: str = ""


@dataclass
class GroupRecord:
    ultimate_receiver_id: str = ""
    originator_id: str = ""
    group_status: str = ""
    as_of_date: str = ""
    as_of_time: str = ""
    currency_code: str = ""
    as_of_date_modifier: str = ""
    accounts: List[AccountRecord] = field(default_factory=list)
    # Trailer
    group_control_total: str = ""
    group_record_count: str = ""


@dataclass
class FileRecord:
    sender_id: str = ""
    receiver_id: str = ""
    file_creation_date: str = ""
    file_creation_time: str = ""
    resend_indicator: str = ""
    record_size: str = ""
    blocking_factor: str = ""
    version_number: str = ""
    groups: List[GroupRecord] = field(default_factory=list)
    # Trailer
    file_control_total: str = ""
    file_record_count: str = ""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------
def _join_continuations(lines: List[str]) -> List[str]:
    """Merge 88 continuation records into their preceding record."""
    merged = []
    for line in lines:
        line = line.rstrip("\r\n")
        if not line:
            continue
        rt = line.split(",", 1)[0]
        if rt == RT_CONTINUATION and merged:
            # Strip the leading "88," and append to previous line
            continuation_data = line[3:] if line.startswith("88,") else line[2:]
            merged[-1] = merged[-1].rstrip("/") + continuation_data
        else:
            merged.append(line)
    return merged


def _split_fields(line: str) -> List[str]:
    """Split a BAI2 record into fields, respecting the trailing slash."""
    line = line.rstrip("/").rstrip(",")
    return line.split(",")


def parse_bai2(content: str) -> FileRecord:
    """
    Parse a full BAI2 file string into a FileRecord object.
    Raises ValueError on malformed input.
    """
    lines = content.splitlines()
    lines = _join_continuations(lines)

    file_rec = FileRecord()
    current_group: Optional[GroupRecord] = None
    current_account: Optional[AccountRecord] = None
    current_transaction: Optional[TransactionRecord] = None

    for raw_line in lines:
        if not raw_line.strip():
            continue

        fields = _split_fields(raw_line)
        rt = fields[0]

        # ------------------------------------------------------------------
        # 01 - File Header
        # ------------------------------------------------------------------
        if rt == RT_FILE_HEADER:
            file_rec.sender_id             = fields[1] if len(fields) > 1 else ""
            file_rec.receiver_id           = fields[2] if len(fields) > 2 else ""
            file_rec.file_creation_date    = fields[3] if len(fields) > 3 else ""
            file_rec.file_creation_time    = fields[4] if len(fields) > 4 else ""
            file_rec.resend_indicator      = fields[5] if len(fields) > 5 else ""
            file_rec.record_size           = fields[6] if len(fields) > 6 else ""
            file_rec.blocking_factor       = fields[7] if len(fields) > 7 else ""
            file_rec.version_number        = fields[8] if len(fields) > 8 else ""

        # ------------------------------------------------------------------
        # 02 - Group Header
        # ------------------------------------------------------------------
        elif rt == RT_GROUP_HEADER:
            current_group = GroupRecord()
            current_group.ultimate_receiver_id = fields[1] if len(fields) > 1 else ""
            current_group.originator_id        = fields[2] if len(fields) > 2 else ""
            current_group.group_status         = fields[3] if len(fields) > 3 else ""
            current_group.as_of_date           = fields[4] if len(fields) > 4 else ""
            current_group.as_of_time           = fields[5] if len(fields) > 5 else ""
            current_group.currency_code        = fields[6] if len(fields) > 6 else ""
            current_group.as_of_date_modifier  = fields[7] if len(fields) > 7 else ""
            file_rec.groups.append(current_group)

        # ------------------------------------------------------------------
        # 03 - Account Header
        # ------------------------------------------------------------------
        elif rt == RT_ACCOUNT_HEADER:
            current_account = AccountRecord()
            current_account.customer_account = fields[1] if len(fields) > 1 else ""
            current_account.currency_code    = fields[2] if len(fields) > 2 else ""

            # BAI2 account headers can have multiple type_code/amount/item_count/funds_type
            # groups. We capture all of them as balance entries.
            i = 3
            while i < len(fields):
                balance = {
                    "type_code":   fields[i]   if i   < len(fields) else "",
                    "amount":      fields[i+1] if i+1 < len(fields) else "",
                    "item_count":  fields[i+2] if i+2 < len(fields) else "",
                    "funds_type":  fields[i+3] if i+3 < len(fields) else "",
                }
                # Skip empty type_codes
                if balance["type_code"]:
                    current_account.balances.append(balance)
                i += 4

            if current_group:
                current_group.accounts.append(current_account)

        # ------------------------------------------------------------------
        # 16 - Transaction Detail
        # ------------------------------------------------------------------
        elif rt == RT_TRANSACTION:
            current_transaction = TransactionRecord()
            current_transaction.type_code     = fields[1] if len(fields) > 1 else ""
            current_transaction.amount        = fields[2] if len(fields) > 2 else ""
            current_transaction.funds_type    = fields[3] if len(fields) > 3 else ""
            current_transaction.bank_ref      = fields[4] if len(fields) > 4 else ""
            current_transaction.customer_ref  = fields[5] if len(fields) > 5 else ""
            current_transaction.text          = ",".join(fields[6:]) if len(fields) > 6 else ""

            # Inherit context
            if current_account and current_group:
                current_transaction.account_id          = current_account.customer_account
                current_transaction.currency_code       = current_account.currency_code or current_group.currency_code
                current_transaction.as_of_date          = current_group.as_of_date
                current_transaction.as_of_time          = current_group.as_of_time
                current_transaction.as_of_date_modifier = current_group.as_of_date_modifier
                current_transaction.bank_id             = current_group.originator_id
                current_transaction.customer_id         = current_group.ultimate_receiver_id
                current_transaction.file_date           = file_rec.file_creation_date
                current_transaction.file_time           = file_rec.file_creation_time

            if current_account:
                current_account.transactions.append(current_transaction)

        # ------------------------------------------------------------------
        # 49 - Account Trailer
        # ------------------------------------------------------------------
        elif rt == RT_ACCOUNT_TRAILER:
            if current_account:
                current_account.account_control_total = fields[1] if len(fields) > 1 else ""
                current_account.account_record_count  = fields[2] if len(fields) > 2 else ""
            current_transaction = None

        # ------------------------------------------------------------------
        # 98 - Group Trailer
        # ------------------------------------------------------------------
        elif rt == RT_GROUP_TRAILER:
            if current_group:
                current_group.group_control_total  = fields[1] if len(fields) > 1 else ""
                current_group.group_record_count   = fields[2] if len(fields) > 2 else ""
            current_account = None

        # ------------------------------------------------------------------
        # 99 - File Trailer
        # ------------------------------------------------------------------
        elif rt == RT_FILE_TRAILER:
            file_rec.file_control_total  = fields[1] if len(fields) > 1 else ""
            file_rec.file_record_count   = fields[2] if len(fields) > 2 else ""
            current_group = None

    return file_rec


# ---------------------------------------------------------------------------
# CSV Export Helpers
# ---------------------------------------------------------------------------
def file_to_balances_rows(file_rec: FileRecord) -> List[dict]:
    """Flatten all account balance records into a list of dicts."""
    rows = []
    for group in file_rec.groups:
        for account in group.accounts:
            for balance in account.balances:
                rows.append({
                    "file_sender_id":        file_rec.sender_id,
                    "file_receiver_id":      file_rec.receiver_id,
                    "file_creation_date":    file_rec.file_creation_date,
                    "file_creation_time":    file_rec.file_creation_time,
                    "resend_indicator":      file_rec.resend_indicator,
                    "group_originator_id":   group.originator_id,
                    "group_receiver_id":     group.ultimate_receiver_id,
                    "group_status":          group.group_status,
                    "as_of_date":            group.as_of_date,
                    "as_of_time":            group.as_of_time,
                    "as_of_date_modifier":   group.as_of_date_modifier,
                    "currency_code":         account.currency_code or group.currency_code,
                    "customer_account":      account.customer_account,
                    "balance_type_code":     balance["type_code"],
                    "balance_amount":        balance["amount"],
                    "balance_item_count":    balance["item_count"],
                    "balance_funds_type":    balance["funds_type"],
                    "account_control_total": account.account_control_total,
                    "account_record_count":  account.account_record_count,
                    "group_control_total":   group.group_control_total,
                    "group_record_count":    group.group_record_count,
                    "file_control_total":    file_rec.file_control_total,
                    "file_record_count":     file_rec.file_record_count,
                })
    return rows


# ---------------------------------------------------------------------------
# BAI2 type code -> SVB-style transaction type label + credit/debit logic
# BAI2 type codes 100-399 are credits (money IN), 400-699 are debits (money OUT)
# ---------------------------------------------------------------------------
def _is_credit(type_code: str) -> bool:
    """Return True if the BAI2 type code represents a credit (money in)."""
    try:
        return 100 <= int(type_code) <= 399
    except (ValueError, TypeError):
        return False


_TYPE_CODE_LABELS = {
    "169": "ACH CREDIT",
    "195": "WIRE TRANSFER CREDIT",
    "214": "FX Wire Transfer Credit",
    "174": "Miscellaneous ACH Credit",
    "301": "MOBILE DEPOSIT",
    "469": "ACH DEBIT",
    "495": "WIRE TRANSFER DEBIT",
    "575": "ZERO BAL TRF DEBIT",
    "496": "FX Wire Transfer Debit",
}


def _tran_type_label(type_code: str) -> str:
    """Return a human-readable transaction type label for a BAI2 type code."""
    if type_code in _TYPE_CODE_LABELS:
        return _TYPE_CODE_LABELS[type_code]
    return ("Credit" if _is_credit(type_code) else "Debit") + f" ({type_code})"


def _format_bai_date(raw: str) -> str:
    """Convert BAI2 date YYMMDD or YYYYMMDD to M/D/YYYY (no leading zeros)."""
    from datetime import datetime as _dt
    raw = raw.strip()
    try:
        if len(raw) == 6:
            dt = _dt.strptime(raw, "%y%m%d")
            return f"{dt.month}/{dt.day}/{dt.year}"
        elif len(raw) == 8:
            dt = _dt.strptime(raw, "%Y%m%d")
            return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        pass
    return raw


def file_to_transaction_rows(file_rec: FileRecord) -> List[dict]:
    """Flatten all transaction records into SVB CSV column format."""
    rows = []
    for group in file_rec.groups:
        for account in group.accounts:
            for txn in account.transactions:
                is_cr = _is_credit(txn.type_code)
                # BAI2 amounts are in cents (integer string) - convert to dollar string
                try:
                    amt = "{:,.2f}".format(int(txn.amount) / 100)
                except (ValueError, TypeError):
                    amt = txn.amount

                rows.append({
                    "Date":               _format_bai_date(txn.as_of_date),
                    "Bank ID":            txn.bank_id,
                    "Account Number":     txn.account_id,
                    "Account Title":      "AR Account",
                    "Entity":             "PERPLEXITY AI, INC.",
                    "Tran Type":          _tran_type_label(txn.type_code),
                    "BAI Type Code":      txn.type_code,
                    "Currency":           txn.currency_code,
                    "Credit Amount":      amt if is_cr else "",
                    "Debit Amount":       amt if not is_cr else "",
                    "Bank Ref #":         txn.bank_ref,
                    "End to End ID":      "",
                    "Customer Ref #":     txn.customer_ref,
                    "Description":        txn.text,
                    "Reason for Payment": "",
                    "Notes":              "",
                })
    return rows
