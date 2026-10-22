"""Databricks Cost Connector CLI and Orchestration Pipeline.

Main entry point that coordinates:
1. Live authentication and identity verification
2. System catalog table discovery and active probing
3. Collection across 17 cost categories with pagination and fault isolation
4. Canonical schema normalization (29 attributes)
5. True raw and normalized artifact export
6. Sanitized sample export (redacted for public repository)
7. Coverage report and redacted execution evidence generation
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import List, Optional

from src.client import verify_connection, get_workspace_client, mask_account_id, mask_id
from src.collectors import ALL_COLLECTOR_CLASSES, CollectionResult
from src.discovery import discover_system_tables
from src.normalizer import CostRecordNormalizer
from src.exporter import CostDataExporter
from src.report import CoverageReportGenerator, USAGE_CATEGORIES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Databricks Cost Connector — Live Collection, Discovery, and Normalization"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory to save raw, normalized, and report outputs (default: output)",
    )
    parser.add_argument(
        "--start-time",
        help="Optional ISO 8601 UTC start time filter (e.g., 2026-10-01T00:00:00Z)",
    )
    parser.add_argument(
        "--end-time",
        help="Optional ISO 8601 UTC end time filter (e.g., 2026-10-06T23:59:59Z)",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify Databricks authentication and exit",
    )
    parser.add_argument(
        "--redact-output",
        action="store_true",
        help="Apply redaction to local output/ files (by default local files retain raw data)",
    )
    return parser.parse_args()


def run_pipeline(
    output_dir: str = "output",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    verify_only: bool = False,
    redact_output: bool = False,
) -> int:
    print("=" * 80)
    print("DATABRICKS COST CONNECTOR — EXECUTION PIPELINE")
    print("=" * 80)

    # Validate time range
    if start_time and end_time and start_time > end_time:
        print(f"\n[ERROR] Invalid time range: start_time ({start_time.isoformat()}) is after end_time ({end_time.isoformat()})")
        return 1

    time_range_desc = "Full available history (all available intervals)"
    if start_time or end_time:
        s_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ") if start_time else "Beginning"
        e_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ") if end_time else "Latest"
        time_range_desc = f"[{s_str} to {e_str})"

    # 1. Verify Authentication
    conn_info = verify_connection()
    if not conn_info["authenticated"]:
        print(f"\n[ERROR] Authentication failed: {conn_info['error']}")
        return 1

    print("\n[SUCCESS] Authenticated with Databricks:")
    print(f"  Host: {conn_info['host']}")
    print(f"  Token: {conn_info['token_masked']}")
    print(f"  User ID: {mask_id(conn_info['user_id'])}")
    print(f"  User: {conn_info['user_name_masked']}")
    print(f"  Discovered Workspace ID: {mask_id(conn_info['workspace_id'])}")
    print(f"  Discovered Account ID: {mask_account_id(conn_info['discovered_account_id'])}")
    print(f"  SQL Warehouse: {conn_info.get('warehouse_info')}")
    print(f"  Collection Time Range: {time_range_desc}")

    if verify_only:
        print("\nVerification successful. Exiting as requested by --verify-only.")
        return 0

    workspace_id = conn_info["workspace_id"]
    account_id = conn_info["discovered_account_id"]
    w_client = get_workspace_client()

    # 2. Discover System Tables & Active Probing
    print("\n" + "-" * 80)
    print("DISCOVERING SYSTEM CATALOG TABLES")
    print("-" * 80)
    discovery_res = discover_system_tables(w_client)
    discovered_tables = discovery_res.get("discovered_tables", [])
    if discovery_res["success"]:
        print(f"  Discovered {len(discovered_tables)} tables in system catalog:")
        for schema, tbls in discovery_res.get("tables_by_schema", {}).items():
            print(f"    - system.{schema}: {', '.join(tbls)}")
    else:
        print(f"  [NOTE] System table discovery returned: {discovery_res['error']}")

    # 3. Run Collectors
    print("\n" + "-" * 80)
    print("COLLECTING COST & USAGE TELEMETRY")
    print("-" * 80)

    collection_results: List[CollectionResult] = []
    normalizer = CostRecordNormalizer(default_account_id=account_id, default_workspace_id=workspace_id)
    exporter = CostDataExporter(output_dir, redact=redact_output)
    all_normalized_records = []

    for collector_cls in ALL_COLLECTOR_CLASSES:
        collector = collector_cls(w_client)
        try:
            res = collector.collect(start_time=start_time, end_time=end_time)
        except Exception as e:
            res = CollectionResult(
                category=collector.category_name,
                purpose=collector.purpose,
                status="failed",
                required_permission=collector.required_permission,
                explanation=f"Unhandled exception during collection: {str(e)}",
            )

        collection_results.append(res)
        exporter.export_raw_category(res)

        norm_records = normalizer.normalize_batch(res.category, res.records)
        exporter.export_normalized_category(res.category, norm_records)
        all_normalized_records.extend(norm_records)

        status_fmt = f"[{res.status.upper()}]"
        print(f"  {status_fmt:22} {res.category:36} Records: {res.record_count}")

    # Append any extra probed cost tables from discovery
    for extra in discovery_res.get("extra_cost_results", []):
        collection_results.append(extra)
        exporter.export_raw_category(extra)
        norm_extra = normalizer.normalize_batch(extra.category, extra.records)
        exporter.export_normalized_category(extra.category, norm_extra)
        all_normalized_records.extend(norm_extra)

        status_fmt = f"[{extra.status.upper()}]"
        print(f"  {status_fmt:22} {extra.category:36} Records: {extra.record_count} (Discovered & Probed)")

    # 4. Export Master Normalized File
    master_file = exporter.export_master_normalized(all_normalized_records)
    print(f"\n[OK] Master normalized dataset created: {master_file} ({len(all_normalized_records)} records)")

    # 5. Export Sanitized Samples for Public Submission
    raw_sample = None
    norm_sample = None
    for res in collection_results:
        if res.records and res.category == "billing_and_usage":
            raw_sample = res.records[0]
            norm_sample = normalizer.normalize_record(res.category, raw_sample)
            break
    if not raw_sample and all_normalized_records:
        norm_sample = all_normalized_records[0]
        raw_sample = norm_sample.get("additional_source_metadata") or norm_sample

    if raw_sample and norm_sample:
        exporter.export_sanitized_samples(raw_sample, norm_sample, samples_dir="samples")
        print("  Sanitized samples exported to samples/raw_sample.json and samples/normalized_sample.json")

    # 6. Generate Coverage Report
    print("\n" + "-" * 80)
    print("GENERATING COVERAGE REPORT")
    print("-" * 80)

    reporter = CoverageReportGenerator(output_dir)
    report_paths = reporter.export_report(
        collection_results,
        workspace_id=workspace_id,
        account_id=account_id,
        discovered_tables=discovered_tables,
        collection_time_range=time_range_desc,
    )
    print(f"  Markdown Report: {report_paths['markdown_path']}")
    print(f"  JSON Report:     {report_paths['json_path']}")

    # 7. Print Final Summary Table
    print("\n" + "=" * 80)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 80)
    collected_count = sum(1 for r in collection_results if r.status == "collected")
    empty_count = sum(1 for r in collection_results if r.status == "empty")
    denied_count = sum(1 for r in collection_results if r.status == "permission_denied")
    unavail_count = sum(1 for r in collection_results if r.status == "unavailable_in_account")
    unsupported_count = sum(1 for r in collection_results if r.status == "unsupported")
    failed_count = sum(1 for r in collection_results if r.status == "failed")

    usage_rows = sum(len(r.records) for r in collection_results if r.category in USAGE_CATEGORIES)
    ref_rows = sum(len(r.records) for r in collection_results if r.category not in USAGE_CATEGORIES)

    print(f"  Collection Time Range:   {time_range_desc}")
    print(f"  Categories Investigated: {len(collection_results)}")
    print(f"  Collected:               {collected_count}")
    print(f"  Empty (Verified):        {empty_count}")
    print(f"  Permission Denied:       {denied_count}")
    print(f"  Unavailable in Account:  {unavail_count}")
    print(f"  Unsupported:             {unsupported_count}")
    print(f"  Failed:                  {failed_count}")
    print(f"  Total Extracted Records: {len(all_normalized_records)} ({usage_rows} usage/workload rows, {ref_rows} rate/config rows)")
    print("=" * 80)

    return 0


def main():
    args = parse_args()
    start_dt = None
    end_dt = None
    if args.start_time:
        try:
            start_dt = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
        except Exception as e:
            print(f"Invalid --start-time format: {e}", file=sys.stderr)
            sys.exit(1)
    if args.end_time:
        try:
            end_dt = datetime.fromisoformat(args.end_time.replace("Z", "+00:00"))
        except Exception as e:
            print(f"Invalid --end-time format: {e}", file=sys.stderr)
            sys.exit(1)

    code = run_pipeline(
        output_dir=args.output_dir,
        start_time=start_dt,
        end_time=end_dt,
        verify_only=args.verify_only,
        redact_output=args.redact_output,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
