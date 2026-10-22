# Databricks Cost Connector — Redacted Live Execution Evidence

**Platform:** Databricks on AWS (Trial Account)

**Execution Status:** SUCCESS — 10 Collected, 7 Verified Empty, 1 Permission Denied, 0 Unavailable, 0 Unsupported, 0 Failed

**Collection Time Window:** Full available history (all available intervals)

---

## 1. Authentication and Identity Verification

```text
================================================================================
DATABRICKS COST CONNECTOR — EXECUTION PIPELINE
================================================================================

[SUCCESS] Authenticated with Databricks:
  Host: https://dbc-...5e4b.cloud.databricks.com
  Token: dapi[REDACTED_TOKEN]
  User ID: 791...087
  User: v***6@gmail.com
  Discovered Workspace ID: 747...520
  Discovered Account ID: 79df...d591
  SQL Warehouse: running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4)
  Collection Time Range: Full available history (all available intervals)
```

---

## 2. Pipeline Execution Summary

```text
--------------------------------------------------------------------------------
DISCOVERING SYSTEM CATALOG TABLES
--------------------------------------------------------------------------------
  Discovered 31 tables in system catalog:
    - system.access: assistant_events, audit, clean_room_events, column_lineage, inbound_network, outbound_network, table_lineage, workspaces_latest
    - system.billing: account_prices, attributed_usage, list_prices, usage
    - system.compute: clusters, instance_events, instance_pools, node_timeline, node_types, warehouse_events, warehouses
    - system.lakeflow: job_run_timeline, job_task_run_timeline, job_tasks, jobs, pipeline_update_timeline, pipelines, zerobus_ingest, zerobus_stream
    - system.query: history
    - system.storage: auto_upgrade_operations_history, predictive_optimization_operations_history, table_metrics_history

--------------------------------------------------------------------------------
COLLECTING COST & USAGE TELEMETRY
--------------------------------------------------------------------------------
  [COLLECTED]            billing_and_usage                    Records: 63
  [PERMISSION_DENIED]    account_and_workspace_consumption    Records: 0
  [EMPTY]                cluster_and_compute_consumption      Records: 0
  [COLLECTED]            sql_warehouse_consumption            Records: 8
  [EMPTY]                jobs_and_pipeline_consumption        Records: 0
  [COLLECTED]            serverless_consumption               Records: 8
  [COLLECTED]            storage_related_usage                Records: 52
  [EMPTY]                data_transfer_network_usage          Records: 0
  [COLLECTED]            sku_and_pricing_information          Records: 887
  [COLLECTED]            list_cost_calculations               Records: 63
  [COLLECTED]            resource_workload_attribution        Records: 213
  [COLLECTED]            user_service_principal_attribution   Records: 213
  [COLLECTED]            tags_and_allocation_metadata         Records: 1
  [COLLECTED]            budgets_alerts_cost_controls         Records: 5
  [EMPTY]                corrections_retractions_restatements Records: 0
  [EMPTY]                optimization_cost_saving_info        Records: 0
  [EMPTY]                contract_commitment_discounts        Records: 0
  [EMPTY]                attributed_usage_table               Records: 0 (Discovered & Probed)

[OK] Master normalized dataset created: output\normalized\all_normalized_costs.json (1513 records)
  Sanitized samples exported to samples/raw_sample.json and samples/normalized_sample.json

--------------------------------------------------------------------------------
GENERATING COVERAGE REPORT
--------------------------------------------------------------------------------
  Markdown Report: coverage_report.md
  JSON Report:     output\coverage_report.json

================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
  Collection Time Range:   Full available history (all available intervals)
  Categories Investigated: 18
  Collected:               10
  Empty (Verified):        7
  Permission Denied:       1
  Unavailable in Account:  0
  Unsupported:             0
  Failed:                  0
  Total Extracted Records: 1513 (620 usage/workload rows, 893 rate/config rows)
================================================================================
```

> **Record Count Note:** The reported total of 1,513 records comprises 620 usage and workload activity records (billing usage, serverless compute, storage records, and query history attribution) and 893 reference/catalog records (list prices, compute tags, and budget governance policies).

---

## 3. Automated Offline Unit Tests (`python -m pytest -v`)

```text
============================= test session starts =============================
platform win32 -- Python 3.14.0, pytest-8.4.2, pluggy-1.6.0 -- C:\Python314\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\Varsh\Documents\antigravity\Databricks-Cost-Connector
plugins: anyio-4.13.0
collecting ... collected 19 items

tests/test_collectors.py::test_classify_error PASSED                     [  5%]
tests/test_collectors.py::test_collection_result_finalize_preserves_errors PASSED [ 10%]
tests/test_collectors.py::test_empty_results PASSED                      [ 15%]
tests/test_collectors.py::test_sql_multi_chunk_pagination PASSED         [ 21%]
tests/test_collectors.py::test_sql_pending_polling_success PASSED        [ 26%]
tests/test_collectors.py::test_corrections_retractions_and_restatements PASSED [ 31%]
tests/test_collectors.py::test_permission_denied_classification PASSED   [ 36%]
tests/test_collectors.py::test_authentication_validation_failure PASSED  [ 42%]
tests/test_collectors.py::test_query_history_pagination_and_time_window PASSED [ 47%]
tests/test_collectors.py::test_tags_subsource_error_reported PASSED      [ 52%]
tests/test_collectors.py::test_raw_output_preserves_unredacted_records PASSED [ 57%]
tests/test_collectors.py::test_pricing_join_cloud_and_currency_filters PASSED [ 63%]
tests/test_normalizer.py::test_schema_completeness PASSED                [ 68%]
tests/test_normalizer.py::test_cluster_and_compute_normalization_not_null PASSED [ 73%]
tests/test_normalizer.py::test_decimal_precision_preservation PASSED     [ 78%]
tests/test_normalizer.py::test_usage_list_price_exact_multiplication PASSED [ 84%]
tests/test_normalizer.py::test_timestamp_handling PASSED                 [ 89%]
tests/test_normalizer.py::test_malformed_source_records PASSED           [ 94%]
tests/test_normalizer.py::test_redaction PASSED                          [100%]

============================= 19 passed in 2.40s ==============================
```

---

## 4. Sample Redacted Raw Record (`system.billing.usage` with `list_prices` join)

```json
{
  "account_id": "79df...d591",
  "workspace_id": "747...520",
  "record_id": "JjJ_k4hsTq5P17sHewjnksBi",
  "sku_name": "PREMIUM_DATABRICKS_STORAGE_US_EAST_OHIO",
  "cloud": "AWS",
  "usage_start_time": "2026-10-09T00:00:00Z",
  "usage_end_time": "2026-10-10T00:00:00Z",
  "usage_date": "2026-10-09",
  "custom_tags": "{}",
  "usage_unit": "DSU",
  "usage_quantity": "0.000201292621762785",
  "record_type": "ORIGINAL",
  "ingestion_date": "2026-10-10",
  "billing_origin_product": "DEFAULT_STORAGE",
  "pricing": "{\"default\":\"0.023000000000000000\",\"promotional\":{\"default\":null},\"effective_list\":{\"default\":\"0.023000000000000000\"}}",
  "currency_code": "USD",
  "list_unit_price": "0.023000000000000000",
  "list_cost": "0.000004629730300544055000000000000",
  "currency": "USD"
}
```

---

## 5. Sample Redacted Normalized Record (29 Canonical Attributes)

```json
{
  "platform": "Databricks",
  "billing_account_identifier": "79df...d591",
  "workspace_identifier": "747...520",
  "source_category": "billing_and_usage",
  "source_record_identifier": "JjJ_k4hsTq5P17sHewjnksBi",
  "resource_or_workload_identifier": null,
  "resource_or_workload_name": null,
  "resource_or_workload_type": null,
  "service_or_product": "DEFAULT_STORAGE",
  "sku": "PREMIUM_DATABRICKS_STORAGE_US_EAST_OHIO",
  "usage_start": "2026-10-09T00:00:00Z",
  "usage_end": "2026-10-10T00:00:00Z",
  "billing_period_start": null,
  "billing_period_end": null,
  "consumed_quantity": "0.000201292621762785",
  "consumed_unit": "DSU",
  "pricing_quantity": null,
  "pricing_unit": null,
  "list_unit_price": "0.023000000000000000",
  "list_cost": "0.000004629730300544055000000000000",
  "contracted_cost": null,
  "effective_cost": null,
  "billed_cost": null,
  "currency": "USD",
  "user_or_service_principal_attribution": null,
  "tags_or_allocation_metadata": {},
  "source_update_time": "2026-10-10T00:00:00Z",
  "collection_time": "2026-10-10T17:06:17Z",
  "additional_source_metadata": {
    "account_id": "79df...d591",
    "workspace_id": "747...520",
    "record_id": "JjJ_k4hsTq5P17sHewjnksBi",
    "sku_name": "PREMIUM_DATABRICKS_STORAGE_US_EAST_OHIO",
    "cloud": "AWS",
    "usage_start_time": "2026-10-09T00:00:00Z",
    "usage_end_time": "2026-10-10T00:00:00Z",
    "list_unit_price": "0.023000000000000000",
    "list_cost": "0.000004629730300544055000000000000",
    "currency": "USD"
  }
}
```