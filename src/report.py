"""Coverage Report and Execution Evidence Generator for Databricks Cost Connector.

Generates structured Markdown and JSON reports detailing the status, record count,
timestamps, permissions, and explanations for investigated cost-management categories.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional
from src.client import mask_account_id, mask_id
from src.collectors import CollectionResult


USAGE_CATEGORIES = {
    "billing_and_usage",
    "cluster_and_compute_consumption",
    "sql_warehouse_consumption",
    "serverless_consumption",
    "storage_related_usage",
    "data_transfer_network_usage",
    "list_cost_calculations",
    "resource_workload_attribution",
    "user_service_principal_attribution",
}


class CoverageReportGenerator:
    """Generates the official Coverage Report and Evidence for submitted deliverables."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate_markdown(
        self,
        results: List[CollectionResult],
        workspace_id: Optional[str] = None,
        account_id: Optional[str] = None,
        discovered_tables: Optional[List[str]] = None,
        collection_time_range: Optional[str] = None,
    ) -> str:
        """Create a GitHub-flavored Markdown coverage table."""
        status_counts: Dict[str, int] = {}
        total_records = 0
        usage_records = 0
        ref_records = 0

        for r in results:
            status_counts[r.status] = status_counts.get(r.status, 0) + 1
            total_records += r.record_count
            if r.category in USAGE_CATEGORIES:
                usage_records += r.record_count
            else:
                ref_records += r.record_count

        lines = [
            "# Databricks Cost Connector — Coverage Report",
            "",
            "## Summary",
            "",
            "- **Platform**: Databricks (AWS)",
            f"- **Workspace Identifier**: `{mask_id(workspace_id) if workspace_id else 'Discovered at runtime'}`",
            f"- **Billing Account Identifier**: `{mask_account_id(account_id) if account_id else 'Discovered at runtime'}`",
            f"- **Collection Time Range**: {collection_time_range or 'Full account history (all available intervals)'}",
            f"- **Categories Investigated**: {len(results)}",
            f"- **Total Records**: {total_records} ({usage_records} usage/workload rows, {ref_records} reference/config/rate rows)",
            "",
            "### Status Counts",
            "",
            "| Status | Count | Description |",
            "| :--- | :--- | :--- |",
            f"| `collected` | {status_counts.get('collected', 0)} | Telemetry extracted and normalized |",
            f"| `empty` | {status_counts.get('empty', 0)} | Accessible table or API; returned 0 rows for query criteria |",
            f"| `permission_denied` | {status_counts.get('permission_denied', 0)} | Access denied by role or permission boundary |",
            f"| `unavailable_in_account` | {status_counts.get('unavailable_in_account', 0)} | Feature or table not configured in account |",
            f"| `unsupported` | {status_counts.get('unsupported', 0)} | Feature not supported or not implemented |",
            f"| `failed` | {status_counts.get('failed', 0)} | API or query execution error |",
            "",
            "---",
            "",
            "## Category Investigation Matrix",
            "",
            "| Category / Dataset Name | Purpose | Status | Records | Earliest Timestamp | Latest Timestamp | Required Permission / Account Capability | Explanation when Records Not Collected |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]

        for r in results:
            earliest = f"`{r.earliest_timestamp}`" if r.earliest_timestamp else "-"
            latest = f"`{r.latest_timestamp}`" if r.latest_timestamp else "-"
            explanation = r.explanation.replace("|", "/") if r.explanation else "-"
            req_perm = r.required_permission.replace("|", "/") if r.required_permission else "-"
            purpose = r.purpose.replace("|", "/")
            status_badge = f"`{r.status}`"

            lines.append(
                f"| **{r.category}** | {purpose} | {status_badge} | {r.record_count} | {earliest} | {latest} | {req_perm} | {explanation} |"
            )

        if discovered_tables:
            lines.extend([
                "",
                "---",
                "",
                "## Discovered System Tables",
                "",
                "Tables discovered via `system.information_schema.tables`:",
                "",
            ])
            for t in sorted(discovered_tables):
                lines.append(f"- `{t}`")

        lines.append("")
        return "\n".join(lines)

    def generate_json(self, results: List[CollectionResult]) -> List[Dict[str, Any]]:
        """Create structured JSON dictionary for coverage analysis."""
        return [
            {
                "category": r.category,
                "purpose": r.purpose,
                "status": r.status,
                "record_count": r.record_count,
                "earliest_timestamp": r.earliest_timestamp,
                "latest_timestamp": r.latest_timestamp,
                "required_permission": r.required_permission,
                "explanation": r.explanation,
                "metadata": r.metadata,
            }
            for r in results
        ]

    def export_report(
        self,
        results: List[CollectionResult],
        workspace_id: Optional[str] = None,
        account_id: Optional[str] = None,
        discovered_tables: Optional[List[str]] = None,
        collection_time_range: Optional[str] = None,
    ) -> Dict[str, str]:
        """Write Markdown and JSON reports to disk."""
        md_content = self.generate_markdown(
            results, workspace_id, account_id, discovered_tables, collection_time_range
        )
        md_path = "coverage_report.md"
        with open(md_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(md_content)

        json_content = self.generate_json(results)
        json_path = os.path.join(self.output_dir, "coverage_report.json")
        with open(json_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(json_content, f, indent=2)

        return {"markdown_path": md_path, "json_path": json_path}
