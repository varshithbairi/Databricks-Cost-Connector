"""Redaction utilities for masking sensitive credentials and identifiers.

Used for generating public samples (samples/) and execution logs (execution_evidence.md)
without exposing secrets, private emails, or full workspace/account identifiers.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Union
from src.client import mask_email, mask_account_id, mask_host, mask_secret, mask_id


def redact_query_text(query_text: Any) -> Any:
    """Mask or truncate SQL query text to preserve privacy while showing statement type."""
    if not query_text or not isinstance(query_text, str):
        return query_text
    clean = " ".join(query_text.strip().split())
    first_word = clean.split()[0].upper() if clean.split() else "QUERY"
    return f"{first_word} [REDACTED_QUERY]"


def redact_value(key: str, val: Any) -> Any:
    """Targeted redaction based on exact key semantics."""
    if val is None or not isinstance(val, (str, dict, list)):
        return val

    key_lower = key.lower()

    if isinstance(val, dict):
        return {k: redact_value(k, v) for k, v in val.items()}

    if isinstance(val, list):
        return [redact_value(key, item) if isinstance(item, (dict, list, str)) else item for item in val]

    # Specific targeted key redactions
    if any(k in key_lower for k in ("token", "secret", "password")):
        return mask_secret(val)

    if any(k in key_lower for k in ("email", "creator_name", "creator_user_name", "run_as", "owned_by", "created_by", "user_or_service_principal_attribution")):
        return mask_email(val) if "@" in val else val

    if "user_name" in key_lower:
        return mask_email(val) if "@" in val else mask_id(val)

    if "user_id" in key_lower:
        return mask_id(val)

    if "account_id" in key_lower or "billing_account_identifier" in key_lower:
        return mask_account_id(val)

    if "workspace_id" in key_lower or "workspace_identifier" in key_lower:
        return mask_id(val)

    if "warehouse_id" in key_lower:
        return mask_id(val)

    if "host" in key_lower:
        return mask_host(val)

    if "query_text" in key_lower:
        return redact_query_text(val)

    if key_lower == "resource_or_workload_name" and isinstance(val, str) and any(
        val.strip().upper().startswith(kw) for kw in (
            "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "MERGE", "SHOW", "DESCRIBE"
        )
    ):
        return redact_query_text(val)

    return val


def redact_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively redact sensitive fields from a data dictionary."""
    return {k: redact_value(k, v) for k, v in record.items()}


def redact_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Redact a list of records."""
    return [redact_record(r) for r in records]
