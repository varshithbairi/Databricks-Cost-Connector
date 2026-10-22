"""Databricks Client initialization, connection validation, and SQL Execution Helper.

Provides secure credential handling, client instantiation, active warehouse resolution,
resilient SQL Statement Execution (with polling and multi-chunk pagination),
and structured error classification.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.sql import StatementParameterListItem, StatementState


def load_config() -> Dict[str, Optional[str]]:
    """Load configuration from environment variables or .env file."""
    load_dotenv(override=False)

    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID", "").strip() or None
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip() or None

    return {
        "host": host if host else None,
        "token": token if token else None,
        "account_id": account_id,
        "warehouse_id": warehouse_id,
    }


def mask_secret(secret: Optional[str], visible_chars: int = 4) -> str:
    """Safely mask a secret or token completely for logs and evidence."""
    if not secret:
        return "<none>"
    return "dapi[REDACTED_TOKEN]"


def mask_email(email: Optional[str]) -> str:
    """Mask email address for privacy in execution evidence (e.g. v***6@gmail.com)."""
    if not email:
        return "<unknown>"
    if "@" not in email:
        return mask_secret(email, 2)
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked_name = name[0] + "***"
    else:
        masked_name = name[0] + "***" + name[-1]
    return f"{masked_name}@{domain}"


def mask_account_id(account_id: Optional[str]) -> str:
    """Shorten account ID (first 4 + ... + last 4)."""
    if not account_id:
        return "<unknown>"
    clean = str(account_id).strip()
    if len(clean) <= 8:
        return clean
    return f"{clean[:4]}...{clean[-4:]}"


def mask_id(val: Optional[str]) -> str:
    """Shorten workspace ID, warehouse ID, or user ID for evidence privacy."""
    if not val:
        return "<unknown>"
    clean = str(val).strip()
    if len(clean) <= 6:
        return "***"
    return f"{clean[:3]}...{clean[-3:]}"


def mask_host(host: Optional[str]) -> str:
    """Shorten workspace host URL."""
    if not host:
        return "<unknown>"
    clean = host.replace("https://", "").replace("http://", "").strip()
    parts = clean.split(".")
    if len(parts) >= 3:
        subdomain = parts[0]
        if len(subdomain) > 8:
            subdomain = f"{subdomain[:4]}...{subdomain[-4:]}"
        return f"https://{subdomain}.{'.'.join(parts[1:])}"
    return host


def classify_error(err: Any) -> str:
    """Classify an exception or error string into the assessment's official statuses.
    
    Uses structured regex matching on status codes and known Databricks error keywords
    to avoid false positives on arbitrary numbers in request IDs or timestamps.
    """
    msg = str(err).upper()

    # Permission denied: 403 Forbidden, explicit permission denied errors
    if re.search(r"\b(PERMISSION_DENIED|FORBIDDEN)\b", msg) or "DOES NOT HAVE" in msg or "NOT PERMITTED" in msg:
        return "permission_denied"
    if re.search(r"\b(403|HTTP 403|STATUS 403)\b", msg):
        return "permission_denied"

    # Authentication failure / unauthorized: 401 Unauthorized
    if re.search(r"\b(401|HTTP 401|STATUS 401|UNAUTHORIZED|AUTHENTICATION_ERROR)\b", msg):
        return "permission_denied"

    # Unavailable in account / catalog not configured
    if (
        "TABLE_OR_VIEW_NOT_FOUND" in msg
        or "SCHEMA_NOT_FOUND" in msg
        or "CATALOG_NOT_FOUND" in msg
        or "NOT ENABLED" in msg
        or "NOT CONFIGURED" in msg
        or re.search(r"\bNOT FOUND\b", msg)
    ):
        return "unavailable_in_account"

    # Unsupported feature or API
    if "UNSUPPORTED" in msg or "NOT IMPLEMENTED" in msg:
        return "unsupported"

    return "failed"


def get_workspace_client(host: Optional[str] = None, token: Optional[str] = None) -> WorkspaceClient:
    """Instantiate and return an official Databricks WorkspaceClient."""
    config = load_config()
    final_host = host or config["host"]
    final_token = token or config["token"]

    if not final_host:
        raise ValueError(
            "DATABRICKS_HOST is not set. Please define it in your .env file or environment."
        )
    if not final_token:
        raise ValueError(
            "DATABRICKS_TOKEN is not set. Please define it in your .env file or environment."
        )

    return WorkspaceClient(host=final_host, token=final_token)


def find_sql_warehouse(client: WorkspaceClient) -> Tuple[Optional[str], str]:
    """Find a SQL warehouse to run queries on.
    
    Prefers DATABRICKS_WAREHOUSE_ID, then any RUNNING warehouse, then the first available warehouse.
    Returns (warehouse_id, selection_reason).
    """
    config = load_config()
    configured_id = config.get("warehouse_id")
    if configured_id:
        return configured_id, f"explicitly configured warehouse {configured_id}"

    try:
        warehouses = list(client.warehouses.list())
        if not warehouses:
            return None, "no warehouses available in workspace"

        # Prefer a RUNNING warehouse
        for wh in warehouses:
            state_val = str(getattr(wh, "state", "")).upper()
            if "RUNNING" in state_val:
                return wh.id, f"running warehouse '{wh.name}' ({wh.id})"

        # Otherwise pick the first available
        first = warehouses[0]
        return first.id, f"warehouse '{first.name}' ({first.id}, state: {getattr(first, 'state', 'UNKNOWN')})"
    except Exception as e:
        return None, f"error discovering warehouses: {str(e)}"


class SqlHelper:
    """Helper for executing queries with an active SQL warehouse, supporting chunking and polling."""

    @staticmethod
    def execute_query(
        client: WorkspaceClient,
        query: str,
        parameters: Optional[List[StatementParameterListItem]] = None,
        timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        """Execute a SQL statement via Databricks Statement Execution API.
        
        Handles:
        - Active warehouse resolution
        - Pending/Running status polling
        - Multi-chunk result pagination
        - Structured error mapping
        """
        wh_id, wh_reason = find_sql_warehouse(client)
        if not wh_id:
            return {
                "success": False,
                "error": f"No SQL warehouse available: {wh_reason}",
                "status": "unavailable_in_account",
                "rows": [],
                "columns": [],
                "warehouse_used": wh_reason,
            }

        try:
            kwargs: Dict[str, Any] = {
                "warehouse_id": wh_id,
                "statement": query,
                "wait_timeout": "30s",
            }
            if parameters:
                kwargs["parameters"] = parameters

            stmt = client.statement_execution.execute_statement(**kwargs)

            # Poll if statement is still PENDING or RUNNING
            start_poll = time.time()
            state = getattr(stmt.status, "state", None)
            while state in (StatementState.PENDING, StatementState.RUNNING):
                if time.time() - start_poll > timeout_seconds:
                    return {
                        "success": False,
                        "error": f"Statement timed out after {timeout_seconds}s in state {state}",
                        "status": "failed",
                        "rows": [],
                        "columns": [],
                        "warehouse_used": wh_reason,
                    }
                time.sleep(2)
                stmt = client.statement_execution.get_statement(stmt.statement_id)
                state = getattr(stmt.status, "state", None)

            state_str = str(state.value) if hasattr(state, "value") else str(state)
            if "SUCCEEDED" in state_str:
                cols = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest and stmt.manifest.schema else []
                raw_rows = list(stmt.result.data_array or [])

                # Exhaustive chunk pagination
                next_chunk_index = getattr(stmt.result, "next_chunk_index", None)
                while next_chunk_index is not None:
                    chunk = client.statement_execution.get_statement_result_chunk_n(
                        statement_id=stmt.statement_id,
                        chunk_index=next_chunk_index,
                    )
                    if chunk and chunk.data_array:
                        raw_rows.extend(chunk.data_array)
                    next_chunk_index = getattr(chunk, "next_chunk_index", None)

                row_dicts = [dict(zip(cols, row)) for row in raw_rows]
                return {
                    "success": True,
                    "rows": row_dicts,
                    "columns": cols,
                    "status": "collected" if row_dicts else "empty",
                    "warehouse_used": wh_reason,
                }
            else:
                err_msg = str(getattr(stmt.status, "error", "Statement execution did not succeed"))
                return {
                    "success": False,
                    "error": err_msg,
                    "status": classify_error(err_msg),
                    "rows": [],
                    "columns": [],
                    "warehouse_used": wh_reason,
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "status": classify_error(e),
                "rows": [],
                "columns": [],
                "warehouse_used": wh_reason,
            }


def verify_connection() -> Dict[str, Any]:
    """Test and verify live authentication with Databricks."""
    config = load_config()
    result: Dict[str, Any] = {
        "authenticated": False,
        "host": mask_host(config["host"]),
        "token_masked": mask_secret(config["token"]),
        "user_id": None,
        "user_name_masked": None,
        "workspace_id": None,
        "discovered_account_id": None,
        "can_read_clusters": False,
        "warehouse_info": None,
        "error": None,
    }

    try:
        w_client = get_workspace_client()
        me = w_client.current_user.me()

        result["authenticated"] = True
        result["user_id"] = me.id
        result["user_name_masked"] = mask_email(me.user_name)
        result["workspace_id"] = getattr(w_client.config, "workspace_id", None)
        result["discovered_account_id"] = getattr(w_client.config, "account_id", None)

        try:
            _ = list(w_client.clusters.list())
            result["can_read_clusters"] = True
        except Exception:
            result["can_read_clusters"] = False

        wh_id, wh_reason = find_sql_warehouse(w_client)
        result["warehouse_info"] = wh_reason

    except DatabricksError as e:
        error_msg = getattr(e, "message", None) or str(e)
        error_code = getattr(e, "error_code", "UNKNOWN")
        result["error"] = f"Databricks API Error: {error_msg} (code: {error_code})"
    except Exception as e:
        result["error"] = f"Connection Error: {str(e)}"

    return result
