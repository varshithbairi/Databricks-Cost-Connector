"""System Catalog Table Discovery.

Queries system.information_schema.tables to catalog all accessible system tables,
and actively probes discovered cost tables (such as system.billing.attributed_usage).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from databricks.sdk import WorkspaceClient

from src.client import SqlHelper, classify_error
from src.collectors import CollectionResult


def discover_system_tables(client: WorkspaceClient) -> Dict[str, Any]:
    """Discover tables in system catalog schemas and probe discovered cost tables."""
    query = (
        "SELECT table_schema, table_name "
        "FROM system.information_schema.tables "
        "WHERE table_schema IN ('billing', 'compute', 'lakeflow', 'query', 'access', 'storage') "
        "ORDER BY table_schema, table_name"
    )
    res = SqlHelper.execute_query(client, query)
    if not res["success"]:
        return {
            "success": False,
            "status": res["status"],
            "error": res["error"],
            "tables_by_schema": {},
            "discovered_tables": [],
            "extra_cost_results": [],
        }

    tables_by_schema: Dict[str, List[str]] = {}
    discovered_tables: List[str] = []
    for r in res["rows"]:
        schema = r.get("table_schema")
        name = r.get("table_name")
        full_name = f"system.{schema}.{name}"
        discovered_tables.append(full_name)
        tables_by_schema.setdefault(schema, []).append(name)

    # Active probe of system.billing.attributed_usage if present in catalog
    extra_results: List[CollectionResult] = []
    if "attributed_usage" in tables_by_schema.get("billing", []):
        probe_query = "SELECT * FROM system.billing.attributed_usage LIMIT 10"
        probe_res = SqlHelper.execute_query(client, probe_query)
        if probe_res["success"]:
            records = probe_res["rows"]
            extra_results.append(
                CollectionResult(
                    category="attributed_usage_table",
                    purpose="Fine-grained resource and tag attribution from system.billing.attributed_usage",
                    status="collected" if records else "empty",
                    records=records,
                    record_count=len(records),
                    required_permission="SELECT on system.billing.attributed_usage",
                    explanation=None if records else f"Table probed on {probe_res.get('warehouse_used')} and returned 0 rows.",
                    metadata={"warehouse_used": probe_res.get("warehouse_used")},
                )
            )
        else:
            extra_results.append(
                CollectionResult(
                    category="attributed_usage_table",
                    purpose="Fine-grained resource and tag attribution from system.billing.attributed_usage",
                    status=probe_res["status"],
                    record_count=0,
                    required_permission="SELECT on system.billing.attributed_usage",
                    explanation=f"Probe query failed: {probe_res['error']}",
                    metadata={"warehouse_used": probe_res.get("warehouse_used")},
                )
            )

    return {
        "success": True,
        "status": "collected",
        "tables_by_schema": tables_by_schema,
        "discovered_tables": discovered_tables,
        "extra_cost_results": extra_results,
    }
