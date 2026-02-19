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

        if rt == RT_FILE_HEADER:
            file_rec.sender_id             = fields[1] if len(fields) > 1 else ""
            file_rec.receiver_id           = fields[2] if len(fields) > 2 else ""
            file_rec.file_creation_date    = fields[3] if len(fields) > 3 else ""
            file_rec.file_creation_time    = fields[4] if len(fields) > 4 else ""
            file_rec.resend_indicator      = fields[5] if len(fields) > 5 else ""
            file_rec.record_size           = fields[6] if len(fields) > 6 else ""
            file_rec.blocking_factor       = fields[7] if len(fields) > 7 else ""
            file_rec.version_number        = fields[8] if len(fields) > 8 else ""

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

        elif rt == RT_ACCOUNT_HEADER:
            current_account = AccountRecord()
            current_account.customer_account = fields[1] if len(fields) > 1 else ""
            current_account.currency_code    = fields[2] if len(fields) > 2 else ""
            i = 3
            while i < len(fields):
                balance = {
                    "type_code":   fields[i]   if i   < len(fields) else "",
                    "amount":      fields[i+1] if i+1 < len(fields) else "",
                    "item_count":  fields[i+2] if i+2 < len(fields) else "",
                    "funds_type":  fields[i+3] if i+3 < len(fields) else "",
                }
                if balance["type_code"]:
                    current_account.balances.append(balance)
                i += 4
            if current_group:
                current_group.accounts.append(current_account)

        elif rt == RT_TRANSACTION:
            current_transaction = TransactionRecord()
            current_transaction.type_code     = fields[1] if len(fields) > 1 else ""
            current_transaction.amount        = fields[2] if len(fields) > 2 else ""
            current_transaction.funds_type    = fields[3] if len(fields) > 3 else ""
            current_transaction.bank_ref      = fields[4] if len(fields) > 4 else ""
            current_transaction.customer_ref  = fields[5] if len(fields) > 5 else ""
            current_transaction.text          = ",".join(fields[6:]) if len(fields) > 6 else ""
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

        elif rt == RT_ACCOUNT_TRAILER:
            if current_account:
                current_account.account_control_total = fields[1] if len(fields) > 1 else ""
                current_account.account_record_count  = fields[2] if len(fields) > 2 else ""
            current_transaction = None

        elif rt == RT_GROUP_TRAILER:
            if current_group:
                current_group.group_control_total  = fields[1] if len(fields) > 1 else ""
                current_group.group_record_count   = fields[2] if len(fields) > 2 else ""
            current_account = None

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


def file_to_transaction_rows(file_rec: FileRecord) -> List[dict]:
    """Flatten all transaction records into a list of dicts."""
    rows = []
    for group in file_rec.groups:
        for account in group.accounts:
            for txn in account.transactions:
                rows.append({
                    "file_sender_id":        file_rec.sender_id,
                    "file_receiver_id":      file_rec.receiver_id,
                    "file_creation_date":    txn.file_date,
                    "file_creation_time":    txn.file_time,
                    "group_originator_id":   txn.bank_id,
                    "group_receiver_id":     txn.customer_id,
                    "as_of_date":            txn.as_of_date,
                    "as_of_time":            txn.as_of_time,
                    "as_of_date_modifier":   txn.as_of_date_modifier,
                    "customer_account":      txn.account_id,
                    "currency_code":         txn.currency_code,
                    "type_code":             txn.type_code,
                    "amount":                txn.amount,
                    "funds_type":            txn.funds_type,
                    "bank_ref":              txn.bank_ref,
                    "customer_ref":          txn.customer_ref,
                    "text":                  txn.text,
                })
    return rows
