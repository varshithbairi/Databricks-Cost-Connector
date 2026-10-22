"""Databricks Cost Connector Collectors Registry.

Modular, fault-isolated collectors for all 17 Databricks cost-management categories.
Includes decimal precision preservation, UTC timestamp handling, source-side time filtering,
and shared SQL querying with cloud-matched pricing joins.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
import requests

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from src.client import SqlHelper, classify_error, load_config


def to_utc_iso(val: Any) -> Optional[str]:
    """Convert various timestamp formats to UTC ISO 8601 string, preserving sub-seconds."""
    if val is None or val == "":
        return None
    try:
        dt: Optional[datetime] = None
        if isinstance(val, (int, float)):
            if val > 1e11:  # epoch milliseconds
                dt = datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
            else:  # epoch seconds
                dt = datetime.fromtimestamp(val, tz=timezone.utc)
        elif isinstance(val, datetime):
            dt = val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val.astimezone(timezone.utc)
        elif isinstance(val, str):
            val_clean = val.strip()
            if val_clean.isdigit():
                return to_utc_iso(int(val_clean))
            # Replace trailing Z with UTC offset for fromisoformat compatibility
            dt = datetime.fromisoformat(val_clean.replace("Z", "+00:00"))
            dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

        if dt is not None:
            if dt.microsecond > 0:
                iso = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
                return iso.rstrip("0") + "Z"
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None
    return None


def to_decimal_str(val: Any) -> Optional[str]:
    """Preserve exact decimal precision without floating point inaccuracies."""
    if val is None or val == "":
        return None
    if isinstance(val, Decimal):
        return str(val)
    try:
        d = Decimal(str(val))
        return format(d, "f") if "e" in str(d).lower() else str(d)
    except (InvalidOperation, ValueError, TypeError):
        return str(val)


@dataclass
class CollectionResult:
    """Standard container for output from every collector."""
    category: str
    purpose: str
    status: str  # collected, empty, permission_denied, unavailable_in_account, unsupported, failed
    records: List[Dict[str, Any]] = field(default_factory=list)
    record_count: int = 0
    earliest_timestamp: Optional[str] = None
    latest_timestamp: Optional[str] = None
    required_permission: str = ""
    explanation: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def finalize(self) -> CollectionResult:
        """Calculate counts and bounds without overriding terminal error states."""
        self.record_count = len(self.records)
        if self.status in ("permission_denied", "unavailable_in_account", "unsupported", "failed"):
            return self
        if self.record_count > 0:
            self.status = "collected"
        else:
            self.status = "empty"
        return self


class BaseCollector:
    """Base class for all category collectors."""
    category_name: str = "base"
    purpose: str = "Base collector"
    required_permission: str = "Workspace access"

    def __init__(self, client: Any = None):
        self.client = client

    def collect(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> CollectionResult:
        raise NotImplementedError


def query_billing_joined_prices(
    client: WorkspaceClient,
    category_name: str,
    purpose: str,
    required_permission: str,
    custom_where: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> CollectionResult:
    """Shared query helper for billing usage joined with list_prices.
    
    Includes:
    - Cloud dimension filter `(p.cloud IS NULL OR u.cloud = p.cloud)` to avoid row multiplication
    - Half-open time window `[start_time, end_time)`
    - Timestamp normalization and earliest/latest bounds tracking
    - Precise list_cost calculation via Decimal
    """
    query = (
        "SELECT "
        "  u.account_id, u.workspace_id, u.record_id, u.sku_name, u.cloud, "
        "  u.usage_start_time, u.usage_end_time, u.usage_date, u.custom_tags, "
        "  u.usage_unit, u.usage_quantity, u.usage_metadata, u.identity_metadata, "
        "  u.record_type, u.ingestion_date, u.billing_origin_product, "
        "  p.pricing, p.currency_code "
        "FROM system.billing.usage u "
        "LEFT JOIN system.billing.list_prices p "
        "  ON u.sku_name = p.sku_name "
        "  AND (p.cloud IS NULL OR u.cloud = p.cloud) "
        "  AND (p.currency_code IS NULL OR p.currency_code = 'USD') "
        "  AND u.usage_start_time >= p.price_start_time "
        "  AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)"
    )

    where_clauses = []
    params: List[StatementParameterListItem] = []

    if custom_where:
        where_clauses.append(f"({custom_where})")
    if start_time:
        where_clauses.append("u.usage_start_time >= :start_time")
        params.append(StatementParameterListItem(name="start_time", value=start_time.isoformat()))
    if end_time:
        where_clauses.append("u.usage_start_time < :end_time")
        params.append(StatementParameterListItem(name="end_time", value=end_time.isoformat()))

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    res = SqlHelper.execute_query(client, query, parameters=params if params else None)
    meta = dict(extra_metadata or {})
    meta["warehouse_used"] = res.get("warehouse_used")

    if not res["success"]:
        return CollectionResult(
            category=category_name,
            purpose=purpose,
            status=res["status"],
            required_permission=required_permission,
            explanation=f"Query failed: {res['error']}",
            metadata=meta,
        )

    records = res["rows"]
    earliest = None
    latest = None

    for r in records:
        r["usage_start_time"] = to_utc_iso(r.get("usage_start_time"))
        r["usage_end_time"] = to_utc_iso(r.get("usage_end_time"))
        r["usage_quantity"] = to_decimal_str(r.get("usage_quantity"))

        # Extract unit price from pricing JSON struct
        pricing_val = r.get("pricing")
        list_unit_price = None
        if isinstance(pricing_val, str):
            try:
                p_obj = json.loads(pricing_val)
                list_unit_price = p_obj.get("default")
            except Exception:
                pass
        elif isinstance(pricing_val, dict):
            list_unit_price = pricing_val.get("default")

        r["list_unit_price"] = to_decimal_str(list_unit_price)

        # list_cost = Decimal(qty) * Decimal(unit_price)
        if r["usage_quantity"] and r["list_unit_price"]:
            try:
                qty_d = Decimal(str(r["usage_quantity"]))
                price_d = Decimal(str(r["list_unit_price"]))
                r["list_cost"] = to_decimal_str(qty_d * price_d)
            except Exception:
                r["list_cost"] = None
        else:
            r["list_cost"] = None

        r["currency"] = r.get("currency_code")
        t = r.get("usage_start_time")
        if t:
            if not earliest or t < earliest:
                earliest = t
            if not latest or t > latest:
                latest = t

    expl = None
    if not records:
        expl = f"Query succeeded on {res.get('warehouse_used')} and returned 0 rows."

    return CollectionResult(
        category=category_name,
        purpose=purpose,
        status="collected" if records else "empty",
        records=records,
        record_count=len(records),
        earliest_timestamp=earliest,
        latest_timestamp=latest,
        required_permission=required_permission,
        explanation=expl,
        metadata=meta,
    )


# 1. Billing and Usage
class BillingUsageCollector(BaseCollector):
    category_name = "billing_and_usage"
    purpose = "System-level DBU consumption, SKU billing records, and list costs"
    required_permission = "USE CATALOG system, USE SCHEMA billing, SELECT on system.billing.usage"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            start_time=start_time,
            end_time=end_time,
        )


# 2. Account and Workspace Consumption
class AccountConsumptionCollector(BaseCollector):
    category_name = "account_and_workspace_consumption"
    purpose = "Account-wide aggregated billing and workspace usage export"
    required_permission = "Account Admin role on accounts.cloud.databricks.com"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        cfg = load_config()
        account_id = getattr(self.client.config, "account_id", None) or cfg.get("account_id")
        token = cfg.get("token")

        if not account_id or not token:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status="unavailable_in_account",
                required_permission=self.required_permission,
                explanation="No account ID configured or account API endpoint not enabled.",
            )

        try:
            url = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}/usage/download"
            resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)

            if resp.status_code == 200:
                records = []
                content_type = resp.headers.get("Content-Type", "")
                if "json" in content_type:
                    try:
                        parsed = resp.json()
                        records = parsed if isinstance(parsed, list) else [parsed]
                    except Exception as e:
                        return CollectionResult(
                            category=self.category_name,
                            purpose=self.purpose,
                            status="unsupported",
                            required_permission=self.required_permission,
                            explanation=f"Received HTTP 200 but failed to parse JSON: {str(e)}",
                        )
                else:
                    try:
                        reader = csv.DictReader(io.StringIO(resp.text))
                        records = list(reader)
                    except Exception as e:
                        return CollectionResult(
                            category=self.category_name,
                            purpose=self.purpose,
                            status="unsupported",
                            required_permission=self.required_permission,
                            explanation=f"Received HTTP 200 CSV but failed to parse: {str(e)}",
                        )

                return CollectionResult(
                    category=self.category_name,
                    purpose=self.purpose,
                    status="collected" if records else "empty",
                    records=records,
                    record_count=len(records),
                    required_permission=self.required_permission,
                    explanation=None if records else "Account usage export returned 0 records.",
                )
            elif resp.status_code in (401, 403):
                status = "permission_denied"
                return CollectionResult(
                    category=self.category_name,
                    purpose=self.purpose,
                    status=status,
                    required_permission=self.required_permission,
                    explanation=(
                        f"HTTP {resp.status_code}: Workspace Personal Access Token (PAT) cannot authenticate to Account Console APIs; "
                        "requires Account Admin OAuth credentials on accounts.cloud.databricks.com."
                    ),
                )
            else:
                return CollectionResult(
                    category=self.category_name,
                    purpose=self.purpose,
                    status="unavailable_in_account",
                    required_permission=self.required_permission,
                    explanation=f"Account API returned HTTP {resp.status_code}: {resp.text.strip()[:150]}",
                )
        except Exception as e:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=classify_error(e),
                required_permission=self.required_permission,
                explanation=f"Connection failure: {str(e)}",
            )


# 3. Cluster and Compute Consumption
class ClusterComputeCollector(BaseCollector):
    category_name = "cluster_and_compute_consumption"
    purpose = "All-purpose and automated compute clusters consumption and configurations"
    required_permission = "Workspace access, cluster read permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        where_cond = (
            "(u.sku_name LIKE '%ALL_PURPOSE%' OR u.sku_name LIKE '%JOBS_COMPUTE%' OR u.billing_origin_product = 'ALL_PURPOSE') "
            "AND u.sku_name NOT LIKE '%SQL%'"
        )
        res = query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )
        if res.record_count > 0:
            return res

        # Fallback to clusters API
        try:
            records = []
            earliest = None
            latest = None
            clusters = list(self.client.clusters.list())
            for c in clusters:
                start_iso = to_utc_iso(getattr(c, "start_time", None))
                rec = {
                    "cluster_id": getattr(c, "cluster_id", None),
                    "cluster_name": getattr(c, "cluster_name", None),
                    "spark_version": getattr(c, "spark_version", None),
                    "node_type_id": getattr(c, "node_type_id", None),
                    "driver_node_type_id": getattr(c, "driver_node_type_id", None),
                    "autotermination_minutes": getattr(c, "autotermination_minutes", None),
                    "state": str(getattr(c, "state", None)),
                    "creator_user_name": getattr(c, "creator_user_name", None),
                    "custom_tags": getattr(c, "custom_tags", None),
                    "start_time": start_iso,
                }
                records.append(rec)
                if start_iso:
                    if not earliest or start_iso < earliest:
                        earliest = start_iso
                    if not latest or start_iso > latest:
                        latest = start_iso

            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status="collected" if records else "empty",
                records=records,
                record_count=len(records),
                earliest_timestamp=earliest,
                latest_timestamp=latest,
                required_permission=self.required_permission,
                explanation=(
                    "0 cluster records in system.billing.usage; extracted cluster configurations from Clusters API."
                    if records else
                    "Query succeeded on running warehouse; 0 cluster records in system.billing.usage and 0 clusters configured in workspace."
                ),
            )
        except Exception as e:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=classify_error(e),
                required_permission=self.required_permission,
                explanation=f"Clusters API failed: {str(e)}",
            )


# 4. SQL Warehouse Consumption
class SqlWarehouseConsumptionCollector(BaseCollector):
    category_name = "sql_warehouse_consumption"
    purpose = "Consumption and DBU usage attributed to Databricks SQL warehouses"
    required_permission = "SELECT on system.billing.usage, workspace SQL view permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        wh_meta = []
        try:
            for wh in self.client.warehouses.list():
                wh_meta.append({"id": wh.id, "name": wh.name, "size": getattr(wh, "cluster_size", None), "state": str(getattr(wh, "state", None))})
        except Exception:
            pass

        where_cond = "u.billing_origin_product = 'SQL' OR u.sku_name LIKE '%SQL%' OR u.usage_type LIKE '%SQL%'"
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
            extra_metadata={"configured_warehouses": wh_meta},
        )


SqlWarehouseCollector = SqlWarehouseConsumptionCollector


# 5. Jobs and Pipeline Consumption
class JobsPipelineCollector(BaseCollector):
    category_name = "jobs_and_pipeline_consumption"
    purpose = "Workflows, automated job definitions, tasks, and task compute consumption"
    required_permission = "Workspace access, jobs view permissions, SELECT on system.billing.usage"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        where_cond = "u.billing_origin_product LIKE '%JOB%' OR u.billing_origin_product LIKE '%PIPELINE%' OR u.sku_name LIKE '%JOB%'"
        res = query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )
        if res.record_count > 0:
            return res

        # Fallback to jobs API
        try:
            records = []
            earliest = None
            latest = None
            for j in self.client.jobs.list(limit=50):
                created_iso = to_utc_iso(getattr(j, "created_time", None))
                settings = getattr(j, "settings", None)
                rec = {
                    "job_id": getattr(j, "job_id", None),
                    "name": getattr(settings, "name", None) if settings else None,
                    "creator_user_name": getattr(j, "creator_user_name", None),
                    "created_time": created_iso,
                    "format": str(getattr(settings, "format", None)) if settings else None,
                    "tags": getattr(settings, "tags", None) if settings else None,
                }
                records.append(rec)
                if created_iso:
                    if not earliest or created_iso < earliest:
                        earliest = created_iso
                    if not latest or created_iso > latest:
                        latest = created_iso

            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status="collected" if records else "empty",
                records=records,
                record_count=len(records),
                earliest_timestamp=earliest,
                latest_timestamp=latest,
                required_permission=self.required_permission,
                explanation="0 job consumption rows found in system.billing.usage and 0 jobs created in workspace.",
            )
        except Exception as e:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=classify_error(e),
                required_permission=self.required_permission,
                explanation=f"Jobs API query error: {str(e)}",
            )


# 6. Serverless Consumption
class ServerlessConsumptionCollector(BaseCollector):
    category_name = "serverless_consumption"
    purpose = "Consumption and DBU usage for serverless compute workloads and serverless SQL"
    required_permission = "SELECT on system.billing.usage"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        where_cond = "u.sku_name LIKE '%SERVERLESS%' OR u.billing_origin_product LIKE '%SERVERLESS%' OR u.usage_type LIKE '%SERVERLESS%'"
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )


# 7. Storage-Related Usage (queries billing storage SKUs AND table_metrics_history)
class StorageUsageCollector(BaseCollector):
    category_name = "storage_related_usage"
    purpose = "Managed Delta Lake table storage sizes, file counts, and DBFS storage billing"
    required_permission = "USE CATALOG system, USE SCHEMA storage, SELECT on system.billing.usage"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        where_cond = "u.billing_origin_product LIKE '%STORAGE%' OR u.sku_name LIKE '%STORAGE%'"
        billing_res = query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )
        if billing_res.record_count > 0:
            return billing_res

        query = "SELECT * FROM system.storage.table_metrics_history"
        res = SqlHelper.execute_query(self.client, query)
        if not res["success"]:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=res["status"],
                required_permission=self.required_permission,
                explanation=f"Query failed: {res['error']}",
                metadata={"warehouse_used": res.get("warehouse_used")},
            )

        records = res["rows"]
        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status="collected" if records else "empty",
            records=records,
            record_count=len(records),
            required_permission=self.required_permission,
            explanation=None if records else f"Query succeeded on {res.get('warehouse_used')} and returned 0 rows in table_metrics_history.",
            metadata={"warehouse_used": res.get("warehouse_used")},
        )


# 8. Data-Transfer and Network Usage
class NetworkDataTransferCollector(BaseCollector):
    category_name = "data_transfer_network_usage"
    purpose = "Cloud inter-region and internet egress data transfer consumption and rates"
    required_permission = "SELECT on system.billing.usage and system.billing.list_prices"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        where_cond = "u.sku_name LIKE '%EGRESS%' OR u.sku_name LIKE '%NETWORK%' OR u.billing_origin_product LIKE '%NETWORK%'"
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )


# 9. SKU and Pricing Information
class SkuPricingCollector(BaseCollector):
    category_name = "sku_and_pricing_information"
    purpose = "Official Databricks Unit (DBU) and cloud compute SKU rate catalog"
    required_permission = "SELECT on system.billing.list_prices"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        query = (
            "SELECT account_id, price_start_time, price_end_time, sku_name, "
            "cloud, currency_code, usage_unit, pricing "
            "FROM system.billing.list_prices"
        )
        params: List[StatementParameterListItem] = []
        where_clauses = []

        if start_time:
            where_clauses.append("price_start_time >= :start_time")
            params.append(StatementParameterListItem(name="start_time", value=start_time.isoformat()))
        if end_time:
            where_clauses.append("price_start_time < :end_time")
            params.append(StatementParameterListItem(name="end_time", value=end_time.isoformat()))

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        res = SqlHelper.execute_query(self.client, query, parameters=params if params else None)
        if not res["success"]:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=res["status"],
                required_permission=self.required_permission,
                explanation=f"Query failed: {res['error']}",
                metadata={"warehouse_used": res.get("warehouse_used")},
            )

        records = res["rows"]
        earliest = None
        latest = None

        for r in records:
            r["price_start_time"] = to_utc_iso(r.get("price_start_time"))
            r["price_end_time"] = to_utc_iso(r.get("price_end_time"))
            t = r.get("price_start_time")
            if t:
                if not earliest or t < earliest:
                    earliest = t
                if not latest or t > latest:
                    latest = t

        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status="collected" if records else "empty",
            records=records,
            record_count=len(records),
            earliest_timestamp=earliest,
            latest_timestamp=latest,
            required_permission=self.required_permission,
            explanation=None if records else f"Query succeeded on {res.get('warehouse_used')} and returned 0 rows.",
            metadata={"warehouse_used": res.get("warehouse_used")},
        )


# 10. List-Cost Calculations
class ListCostCalculationsCollector(BaseCollector):
    category_name = "list_cost_calculations"
    purpose = "Calculated gross list cost (consumed quantity * list unit price from rate catalog)"
    required_permission = "SELECT on system.billing.usage and system.billing.list_prices"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            start_time=start_time,
            end_time=end_time,
        )


# 11. Resource and Workload Attribution (Databricks SQL Query History API)
class QueryHistoryAttributionCollector(BaseCollector):
    category_name = "resource_workload_attribution"
    purpose = "Workload attribution mapping queries and tasks to specific warehouses/clusters"
    required_permission = "Workspace access, query history view permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        try:
            records: List[Dict[str, Any]] = []
            page_token = None
            earliest = None
            latest = None

            start_ms = int(start_time.timestamp() * 1000) if start_time else None
            end_ms = int(end_time.timestamp() * 1000) if end_time else None

            while True:
                kwargs: Dict[str, Any] = {"max_results": 100}
                if page_token:
                    kwargs["page_token"] = page_token

                resp = self.client.query_history.list(**kwargs)
                queries = getattr(resp, "res", []) or []

                for q in queries:
                    q_start_ms = getattr(q, "query_start_time_ms", None)
                    if q_start_ms is not None:
                        if start_ms is not None and q_start_ms < start_ms:
                            continue
                        if end_ms is not None and q_start_ms >= end_ms:
                            continue

                    q_start = to_utc_iso(q_start_ms)
                    q_end = to_utc_iso(getattr(q, "query_end_time_ms", None))

                    metrics = getattr(q, "metrics", None)
                    metrics_dict = metrics.as_dict() if hasattr(metrics, "as_dict") else {}

                    rec = {
                        "query_id": getattr(q, "query_id", None),
                        "query_text": getattr(q, "query_text", None),
                        "warehouse_id": getattr(q, "warehouse_id", None),
                        "user_name": getattr(q, "user_name", None),
                        "user_id": getattr(q, "user_id", None),
                        "status": str(getattr(q, "status", None)),
                        "duration_ms": getattr(q, "duration", None),
                        "query_start_time": q_start,
                        "query_end_time": q_end,
                        "read_bytes": metrics_dict.get("read_bytes"),
                        "rows_produced_count": metrics_dict.get("rows_produced_count"),
                        "compilation_time_ms": metrics_dict.get("compilation_time_ms"),
                        "execution_time_ms": metrics_dict.get("execution_time_ms"),
                    }
                    records.append(rec)

                    if q_start:
                        if not earliest or q_start < earliest:
                            earliest = q_start
                        if not latest or q_start > latest:
                            latest = q_start

                if not getattr(resp, "has_next_page", False) or not getattr(resp, "next_page_token", None):
                    break
                page_token = resp.next_page_token

            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status="collected" if records else "empty",
                records=records,
                record_count=len(records),
                earliest_timestamp=earliest,
                latest_timestamp=latest,
                required_permission=self.required_permission,
                explanation=None if records else "Query history API queried successfully and returned 0 executed queries.",
            )
        except Exception as e:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=classify_error(e),
                required_permission=self.required_permission,
                explanation=str(e),
            )


# 12. User and Service-Principal Attribution
class UserAttributionCollector(BaseCollector):
    category_name = "user_service_principal_attribution"
    purpose = "Identifies user email, service principal, or system identity initiating the cost"
    required_permission = "Workspace access, query history view permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        query_collector = QueryHistoryAttributionCollector(self.client)
        q_res = query_collector.collect(start_time, end_time)

        if q_res.status not in ("collected", "empty"):
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=q_res.status,
                required_permission=self.required_permission,
                explanation=q_res.explanation or "Unable to retrieve query history for user attribution.",
            )

        user_records: List[Dict[str, Any]] = []
        for q in q_res.records:
            if q.get("user_name") or q.get("user_id"):
                user_records.append({
                    "workload_id": q.get("query_id"),
                    "user_name": q.get("user_name"),
                    "user_id": q.get("user_id"),
                    "workload_type": "SQL_QUERY",
                    "resource_id": q.get("warehouse_id"),
                    "timestamp": q.get("query_start_time"),
                    "duration_ms": q.get("duration_ms"),
                })

        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status="collected" if user_records else "empty",
            records=user_records,
            record_count=len(user_records),
            earliest_timestamp=q_res.earliest_timestamp,
            latest_timestamp=q_res.latest_timestamp,
            required_permission=self.required_permission,
            explanation=None if user_records else "0 user-attributed query executions found.",
        )


# 13. Tags and Allocation Metadata
class TagsAllocationCollector(BaseCollector):
    category_name = "tags_and_allocation_metadata"
    purpose = "FinOps cost-center allocation tags, environment tags, and custom metadata"
    required_permission = "Workspace compute read permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        records: List[Dict[str, Any]] = []
        errors: List[str] = []

        try:
            warehouses = list(self.client.warehouses.list())
            for wh in warehouses:
                tags = getattr(wh, "tags", None)
                records.append({
                    "resource_id": getattr(wh, "id", None),
                    "resource_name": getattr(wh, "name", None),
                    "resource_type": "SQL_WAREHOUSE",
                    "tags": tags.as_dict() if hasattr(tags, "as_dict") else tags,
                })
        except Exception as e:
            errors.append(f"warehouses sub-source error: {str(e)}")

        try:
            clusters = list(self.client.clusters.list())
            for c in clusters:
                records.append({
                    "resource_id": getattr(c, "cluster_id", None),
                    "resource_name": getattr(c, "cluster_name", None),
                    "resource_type": "COMPUTE_CLUSTER",
                    "tags": getattr(c, "custom_tags", None),
                })
        except Exception as e:
            errors.append(f"clusters sub-source error: {str(e)}")

        if errors and not records:
            status = classify_error("; ".join(errors))
            explanation = "; ".join(errors)
        elif errors:
            status = "collected"
            explanation = f"Collected {len(records)} tag records, but some sub-sources had errors: {'; '.join(errors)}"
        else:
            status = "collected" if records else "empty"
            explanation = f"Extracted allocation tags from {len(records)} compute resources."

        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status=status,
            records=records,
            record_count=len(records),
            required_permission=self.required_permission,
            explanation=explanation,
        )


# 14. Budgets, Alerts, and Cost Controls
class BudgetsCostControlsCollector(BaseCollector):
    category_name = "budgets_alerts_cost_controls"
    purpose = "Governance guardrails, cluster sizing limits, budget policies, and automated alerts"
    required_permission = "Cluster policies and SQL alert view permissions"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        records: List[Dict[str, Any]] = []
        errors: List[str] = []

        try:
            policies = list(self.client.cluster_policies.list())
            for p in policies:
                records.append({
                    "control_type": "CLUSTER_POLICY",
                    "policy_id": getattr(p, "policy_id", None),
                    "name": getattr(p, "name", None),
                    "definition": getattr(p, "definition", None),
                    "description": getattr(p, "description", None),
                    "creator_user_name": getattr(p, "creator_user_name", None),
                })
        except Exception as e:
            errors.append(f"cluster_policies sub-source error: {str(e)}")

        try:
            alerts = list(self.client.alerts.list())
            for a in alerts:
                records.append({
                    "control_type": "SQL_ALERT",
                    "alert_id": getattr(a, "id", None),
                    "display_name": getattr(a, "display_name", None),
                    "query_id": getattr(a, "query_id", None),
                })
        except Exception as e:
            errors.append(f"alerts sub-source error: {str(e)}")

        if errors and not records:
            status = classify_error("; ".join(errors))
            explanation = "; ".join(errors)
        elif errors:
            status = "collected"
            explanation = f"Collected {len(records)} cost governance records, but some sub-sources had errors: {'; '.join(errors)}"
        else:
            status = "collected" if records else "empty"
            explanation = f"Collected {len(records)} cost governance policies and alerts."

        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status=status,
            records=records,
            record_count=len(records),
            required_permission=self.required_permission,
            explanation=explanation,
        )


# 15. Corrections, Retractions, and Restatements
class CorrectionsRetractionsCollector(BaseCollector):
    category_name = "corrections_retractions_restatements"
    purpose = "Adjustments, billing corrections, and credit retractions published by Databricks"
    required_permission = "SELECT on system.billing.usage"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        # Handles RETRACTION and RESTATEMENT
        where_cond = "u.record_type IN ('RETRACTION', 'RESTATEMENT')"
        return query_billing_joined_prices(
            self.client,
            self.category_name,
            self.purpose,
            self.required_permission,
            custom_where=where_cond,
            start_time=start_time,
            end_time=end_time,
        )


# 16. Optimization and Cost-Saving Information
class OptimizationCostSavingCollector(BaseCollector):
    category_name = "optimization_cost_saving_info"
    purpose = "Predictive optimization, automatic vacuum, and compute cost reduction operations"
    required_permission = "SELECT on system.storage.predictive_optimization_operations_history"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        query = "SELECT * FROM system.storage.predictive_optimization_operations_history"
        res = SqlHelper.execute_query(self.client, query)
        if not res["success"]:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=res["status"],
                required_permission=self.required_permission,
                explanation=f"Query failed: {res['error']}",
                metadata={"warehouse_used": res.get("warehouse_used")},
            )

        records = res["rows"]
        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status="collected" if records else "empty",
            records=records,
            record_count=len(records),
            required_permission=self.required_permission,
            explanation=None if records else f"Query succeeded on {res.get('warehouse_used')} and returned 0 rows in predictive optimization history.",
            metadata={"warehouse_used": res.get("warehouse_used")},
        )


# 17. Contract, Commitment, and Discounts
class ContractCommitmentCollector(BaseCollector):
    category_name = "contract_commitment_discounts"
    purpose = "Negotiated custom enterprise pricing, committed-use discounts, and contractual rates"
    required_permission = "SELECT on system.billing.account_prices"

    def collect(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> CollectionResult:
        query = "SELECT * FROM system.billing.account_prices"
        params: List[StatementParameterListItem] = []
        where_clauses = []

        if start_time:
            where_clauses.append("price_start_time >= :start_time")
            params.append(StatementParameterListItem(name="start_time", value=start_time.isoformat()))
        if end_time:
            where_clauses.append("price_start_time < :end_time")
            params.append(StatementParameterListItem(name="end_time", value=end_time.isoformat()))

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        res = SqlHelper.execute_query(self.client, query, parameters=params if params else None)
        if not res["success"]:
            return CollectionResult(
                category=self.category_name,
                purpose=self.purpose,
                status=res["status"],
                required_permission=self.required_permission,
                explanation=f"Query failed: {res['error']}",
                metadata={"warehouse_used": res.get("warehouse_used")},
            )

        records = res["rows"]
        return CollectionResult(
            category=self.category_name,
            purpose=self.purpose,
            status="collected" if records else "empty",
            records=records,
            record_count=len(records),
            required_permission=self.required_permission,
            explanation=None if records else f"Query succeeded on {res.get('warehouse_used')} and returned 0 rows in system.billing.account_prices.",
            metadata={"warehouse_used": res.get("warehouse_used")},
        )


ALL_COLLECTOR_CLASSES = [
    BillingUsageCollector,
    AccountConsumptionCollector,
    ClusterComputeCollector,
    SqlWarehouseConsumptionCollector,
    JobsPipelineCollector,
    ServerlessConsumptionCollector,
    StorageUsageCollector,
    NetworkDataTransferCollector,
    SkuPricingCollector,
    ListCostCalculationsCollector,
    QueryHistoryAttributionCollector,
    UserAttributionCollector,
    TagsAllocationCollector,
    BudgetsCostControlsCollector,
    CorrectionsRetractionsCollector,
    OptimizationCostSavingCollector,
    ContractCommitmentCollector,
]

__all__ = [
    "ALL_COLLECTOR_CLASSES",
    "BaseCollector",
    "CollectionResult",
    "to_utc_iso",
    "to_decimal_str",
    "BillingUsageCollector",
    "AccountConsumptionCollector",
    "ClusterComputeCollector",
    "SqlWarehouseConsumptionCollector",
    "SqlWarehouseCollector",
    "JobsPipelineCollector",
    "ServerlessConsumptionCollector",
    "StorageUsageCollector",
    "NetworkDataTransferCollector",
    "SkuPricingCollector",
    "ListCostCalculationsCollector",
    "QueryHistoryAttributionCollector",
    "UserAttributionCollector",
    "TagsAllocationCollector",
    "BudgetsCostControlsCollector",
    "CorrectionsRetractionsCollector",
    "OptimizationCostSavingCollector",
    "ContractCommitmentCollector",
    "query_billing_joined_prices",
]
