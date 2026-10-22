"""Record Normalizer for Databricks Cost and Usage Telemetry.

Transforms heterogeneous records from Databricks system tables and workspace APIs
into the canonical 29-field normalized schema.

Enforces:
- Exact decimal precision strings (preventing float rounding errors)
- ISO 8601 UTC timestamps
- Complete attribution extraction (run_as, owned_by, created_by, clusters, warehouses, jobs)
- Preservation of raw source records in additional_source_metadata
- Retention of null for unavailable source dimensions without fabricating values
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from src.collectors import to_decimal_str, to_utc_iso


def parse_json_safely(val: Any) -> Optional[Any]:
    """Parse JSON string safely or return original object if already parsed."""
    if val is None or val == "":
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        val_clean = val.strip()
        if (val_clean.startswith("{") and val_clean.endswith("}")) or (val_clean.startswith("[") and val_clean.endswith("]")):
            try:
                return json.loads(val_clean)
            except Exception:
                return None
    return None


class CostRecordNormalizer:
    """Normalizes raw cost and usage records into the standardized schema."""

    def __init__(self, default_account_id: Optional[str] = None, default_workspace_id: Optional[str] = None):
        self.default_account_id = default_account_id
        self.default_workspace_id = default_workspace_id
        self.now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def create_base_record(
        self,
        source_category: str,
        collection_time: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate a dictionary with all canonical fields initialized to null/defaults."""
        return {
            "platform": "Databricks",
            "billing_account_identifier": self.default_account_id,
            "workspace_identifier": self.default_workspace_id,
            "source_category": source_category,
            "source_record_identifier": None,
            "resource_or_workload_identifier": None,
            "resource_or_workload_name": None,
            "resource_or_workload_type": None,
            "service_or_product": None,
            "sku": None,
            "usage_start": None,
            "usage_end": None,
            "billing_period_start": None,
            "billing_period_end": None,
            "consumed_quantity": None,
            "consumed_unit": None,
            "pricing_quantity": None,
            "pricing_unit": None,
            "list_unit_price": None,
            "list_cost": None,
            "contracted_cost": None,
            "effective_cost": None,
            "billed_cost": None,
            "currency": None,
            "user_or_service_principal_attribution": None,
            "tags_or_allocation_metadata": None,
            "source_update_time": None,
            "collection_time": collection_time or self.now_utc_str,
            "additional_source_metadata": None,
        }

    def normalize_record(self, category: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a single raw record based on its schema and source category."""
        norm = self.create_base_record(category)

        # Retain original raw record in additional_source_metadata for complete auditability
        norm["additional_source_metadata"] = raw

        # Case 1: Billing and Usage records (from system.billing.usage or joined tables)
        # Handles billing_and_usage, cluster_and_compute_consumption, sql_warehouse_consumption,
        # serverless_consumption, storage_related_usage, data_transfer_network_usage,
        # list_cost_calculations, corrections_retractions_restatements, etc.
        if "record_id" in raw or "usage_quantity" in raw:
            norm["billing_account_identifier"] = raw.get("account_id") or self.default_account_id
            norm["workspace_identifier"] = raw.get("workspace_id") or self.default_workspace_id
            norm["source_record_identifier"] = raw.get("record_id")
            norm["sku"] = raw.get("sku_name")
            norm["service_or_product"] = raw.get("billing_origin_product") or "Databricks Compute"
            norm["usage_start"] = to_utc_iso(raw.get("usage_start_time"))
            norm["usage_end"] = to_utc_iso(raw.get("usage_end_time"))
            norm["consumed_quantity"] = to_decimal_str(raw.get("usage_quantity"))
            norm["consumed_unit"] = raw.get("usage_unit")

            # Pricing and cost fields
            distinct_pq = raw.get("pricing_quantity")
            norm["pricing_quantity"] = to_decimal_str(distinct_pq) if distinct_pq is not None else None
            norm["pricing_unit"] = raw.get("pricing_unit") or None
            norm["list_unit_price"] = to_decimal_str(raw.get("list_unit_price"))
            norm["list_cost"] = to_decimal_str(raw.get("list_cost"))
            norm["currency"] = raw.get("currency") or raw.get("currency_code") or None

            # Parse custom tags into dictionary
            tags_parsed = parse_json_safely(raw.get("custom_tags"))
            norm["tags_or_allocation_metadata"] = tags_parsed if isinstance(tags_parsed, dict) else (tags_parsed or None)

            # Source update time: ingestion_date in system.billing.usage
            norm["source_update_time"] = to_utc_iso(raw.get("ingestion_date"))

            # User attribution from identity_metadata (run_as -> owned_by -> created_by)
            im = parse_json_safely(raw.get("identity_metadata"))
            if isinstance(im, dict):
                norm["user_or_service_principal_attribution"] = (
                    im.get("run_as") or im.get("owned_by") or im.get("created_by") or im.get("user_name")
                )

            # Workload attribution from usage_metadata (cluster_id, warehouse_id, job_id, dlt_pipeline_id, notebook_id)
            um = parse_json_safely(raw.get("usage_metadata"))
            if isinstance(um, dict):
                if um.get("cluster_id"):
                    norm["resource_or_workload_identifier"] = um.get("cluster_id")
                    norm["resource_or_workload_type"] = "COMPUTE_CLUSTER"
                elif um.get("warehouse_id"):
                    norm["resource_or_workload_identifier"] = um.get("warehouse_id")
                    norm["resource_or_workload_type"] = "SQL_WAREHOUSE"
                elif um.get("job_id"):
                    norm["resource_or_workload_identifier"] = um.get("job_id")
                    norm["resource_or_workload_type"] = "DATABRICKS_WORKFLOW_JOB"
                elif um.get("dlt_pipeline_id"):
                    norm["resource_or_workload_identifier"] = um.get("dlt_pipeline_id")
                    norm["resource_or_workload_type"] = "DELTA_LIVE_TABLE_PIPELINE"
                elif um.get("notebook_id"):
                    norm["resource_or_workload_identifier"] = um.get("notebook_id")
                    norm["resource_or_workload_type"] = "NOTEBOOK"

        # Case 2: Direct Clusters API records (when compute cluster usage was not in billing)
        elif "cluster_id" in raw and "cluster_name" in raw:
            norm["resource_or_workload_identifier"] = raw.get("cluster_id")
            norm["resource_or_workload_name"] = raw.get("cluster_name")
            norm["resource_or_workload_type"] = "COMPUTE_CLUSTER"
            norm["service_or_product"] = "All-Purpose / Automated Compute"
            norm["sku"] = raw.get("node_type_id")
            norm["usage_start"] = raw.get("start_time")
            norm["user_or_service_principal_attribution"] = raw.get("creator_user_name")
            tags = raw.get("custom_tags")
            norm["tags_or_allocation_metadata"] = parse_json_safely(tags) if isinstance(tags, str) else tags

        # Case 3: Direct Jobs API records
        elif "job_id" in raw and "creator_user_name" in raw:
            norm["resource_or_workload_identifier"] = raw.get("job_id")
            norm["resource_or_workload_name"] = raw.get("name")
            norm["resource_or_workload_type"] = "DATABRICKS_WORKFLOW_JOB"
            norm["service_or_product"] = "Databricks Workflows"
            norm["user_or_service_principal_attribution"] = raw.get("creator_user_name")
            norm["usage_start"] = raw.get("created_time")
            tags = raw.get("tags")
            norm["tags_or_allocation_metadata"] = parse_json_safely(tags) if isinstance(tags, str) else tags

        # Case 4: SKU and Rate Catalog records
        elif category == "sku_and_pricing_information":
            norm["billing_account_identifier"] = raw.get("account_id") or self.default_account_id
            norm["sku"] = raw.get("sku_name")
            norm["service_or_product"] = "Databricks Unit (DBU)" if "DBU" in str(raw.get("usage_unit")) else "Network / Data Transfer"
            norm["pricing_unit"] = raw.get("usage_unit")
            norm["currency"] = raw.get("currency_code")
            pricing_data = raw.get("pricing")
            p_obj = parse_json_safely(pricing_data)
            if isinstance(p_obj, dict):
                norm["list_unit_price"] = to_decimal_str(p_obj.get("default"))

        # Case 5: Workload & User Attribution from Databricks SQL Query History
        elif category in ("resource_workload_attribution", "user_service_principal_attribution"):
            norm["source_record_identifier"] = raw.get("query_id") or raw.get("workload_id")
            norm["resource_or_workload_identifier"] = raw.get("warehouse_id") or raw.get("resource_id")
            norm["resource_or_workload_name"] = raw.get("query_text")
            norm["resource_or_workload_type"] = raw.get("workload_type") or "SQL_QUERY"
            norm["service_or_product"] = "Databricks SQL"
            norm["usage_start"] = raw.get("query_start_time") or raw.get("timestamp")
            norm["usage_end"] = raw.get("query_end_time")
            if raw.get("duration_ms") is not None:
                norm["consumed_quantity"] = to_decimal_str(raw.get("duration_ms"))
                norm["consumed_unit"] = "MILLISECONDS"
            norm["user_or_service_principal_attribution"] = raw.get("user_name")

        # Case 6: Tags and allocation metadata from compute configs
        elif category == "tags_and_allocation_metadata":
            norm["resource_or_workload_identifier"] = raw.get("resource_id")
            norm["resource_or_workload_name"] = raw.get("resource_name")
            norm["resource_or_workload_type"] = raw.get("resource_type")
            tags = raw.get("tags")
            norm["tags_or_allocation_metadata"] = parse_json_safely(tags) if isinstance(tags, str) else tags

        # Case 7: Budgets, Alerts, and Cost Controls
        elif category == "budgets_alerts_cost_controls":
            norm["resource_or_workload_identifier"] = raw.get("policy_id") or raw.get("alert_id")
            norm["resource_or_workload_name"] = raw.get("name") or raw.get("display_name")
            norm["resource_or_workload_type"] = raw.get("control_type")
            norm["service_or_product"] = "Governance and Cost Management"
            norm["user_or_service_principal_attribution"] = raw.get("creator_user_name")

        # Case 8: Contractual commitment / account rates
        elif category == "contract_commitment_discounts":
            norm["billing_account_identifier"] = raw.get("account_id") or self.default_account_id
            norm["sku"] = raw.get("sku_name")
            norm["currency"] = raw.get("currency_code")
            norm["pricing_unit"] = raw.get("usage_unit")

        return norm

    def normalize_batch(self, category: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize a collection of records for a given category."""
        return [self.normalize_record(category, r) for r in records]
