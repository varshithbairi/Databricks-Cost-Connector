# Databricks Cost Connector

A Python cost and usage connector for Databricks. Connects to a Databricks workspace, queries cost telemetry across Unity Catalog system tables and workspace APIs, preserves unmanipulated raw source records, and normalizes output into a unified 29-field schema.

---

## 1. Architecture

The connector interfaces directly with Unity Catalog System Tables and Databricks Workspace APIs:

```text
┌────────────────────────────────────────────────────────┐
│             Databricks Account / Workspace             │
│   • Unity Catalog: system.billing / system.storage     │
│   • Workspace APIs: SQL Warehouses, Clusters, Jobs     │
│   • Workload APIs: Query Execution History, Policies   │
└───────────────────────────┬────────────────────────────┘
                            │  (databricks-sdk)
                            ▼
┌────────────────────────────────────────────────────────┐
│                   Python Connector                     │
│                                                        │
│  1. Client Authentication (PAT / OAuth via SDK)        │
│  2. Table Discovery & Active Probe (system tables)     │
│  3. Modular Collectors (Isolated Fault Handling)       │
│  4. Resilient SQL Polling (Multi-chunk execution)      │
│  5. Schema Normalization (Exact Decimal & UTC Timestamps) │
└───────────────┬────────────────────────┬───────────────┘
                │                        │
                ▼                        ▼
┌───────────────────────────┐  ┌─────────────────────────┐
│     Raw Source Output     │  │    Normalized Output    │
│    (output/raw/*.json)    │  │ (output/normalized/*.json)│
└───────────────────────────┘  └─────────────────────────┘
                │
                ▼
┌────────────────────────────────────────────────────────┐
│             Coverage Report & Evidence                 │
│      (coverage_report.md & execution_evidence.md)      │
└────────────────────────────────────────────────────────┘
```

### Design Principles:
- **Fault-Isolated Execution**: Each category runs independently. If an endpoint fails or permissions are missing, the connector catches the error, classifies it, and continues remaining categories.
- **Null Preservation**: If a value is not present in the source dataset, it remains `null`. No placeholder data is fabricated. Specifically, `billing_period_start` and `billing_period_end` are `null` because Databricks system billing records track hourly usage timestamps (`usage_start_time`, `usage_end_time`) rather than fixed billing cycle dates.
- **Decimal Precision**: Financial rates and usage quantities are handled using Python `Decimal` strings to prevent binary floating-point inaccuracies.
- **UTC Timestamps**: Timestamps are converted to ISO-8601 UTC format (`YYYY-MM-DDTHH:MM:SSZ` or fractional seconds).
- **True Raw Preservation**: Local files under `output/raw/` retain exact source data and IDs. Redaction is applied strictly to public submission samples (`samples/`) and execution evidence (`execution_evidence.md`).

---

## 2. Investigated Cost Categories

All 17 required categories plus discovered tables are investigated:

| # | Category Name | Status | Records | Underlying Source |
| :- | :--- | :--- | :--- | :--- |
| 1 | `billing_and_usage` | `collected` | 63 | `system.billing.usage` joined with `list_prices` |
| 2 | `account_and_workspace_consumption` | `permission_denied` | 0 | Account Console usage download API (HTTP 401: workspace token requires account admin role) |
| 3 | `cluster_and_compute_consumption` | `empty` | 0 | All-purpose/jobs clusters in `system.billing.usage` & Clusters API (0 configured in workspace) |
| 4 | `sql_warehouse_consumption` | `collected` | 8 | `system.billing.usage` (SQL warehouse SKUs) |
| 5 | `jobs_and_pipeline_consumption` | `empty` | 0 | `system.billing.usage` (job usage metadata) / Jobs API |
| 6 | `serverless_consumption` | `collected` | 8 | `system.billing.usage` (Serverless SKUs) |
| 7 | `storage_related_usage` | `collected` | 52 | `system.billing.usage` (Storage SKUs) & `system.storage.table_metrics_history` |
| 8 | `data_transfer_network_usage` | `empty` | 0 | `system.billing.usage` (Network/Egress SKUs) |
| 9 | `sku_and_pricing_information` | `collected` | 887 | `system.billing.list_prices` |
| 10 | `list_cost_calculations` | `collected` | 63 | `usage` joined with `list_prices` (`list_cost` computed via Decimal) |
| 11 | `resource_workload_attribution` | `collected` | 213 | Databricks SQL Query History API |
| 12 | `user_service_principal_attribution` | `collected` | 213 | Databricks SQL Query History user attribution |
| 13 | `tags_and_allocation_metadata` | `collected` | 1 | Compute cluster and warehouse tags |
| 14 | `budgets_alerts_cost_controls` | `collected` | 5 | Cluster policies and SQL alert monitors |
| 15 | `corrections_retractions_restatements` | `empty` | 0 | `system.billing.usage` (`record_type IN ('RETRACTION', 'RESTATEMENT')`) |
| 16 | `optimization_cost_saving_info` | `empty` | 0 | `system.storage.predictive_optimization_operations_history` |
| 17 | `contract_commitment_discounts` | `empty` | 0 | `system.billing.account_prices` |
| 18 | `attributed_usage_table` | `empty` | 0 | Probed table `system.billing.attributed_usage` (verified empty on trial account) |

> Total Extracted Records: **1,513** (620 usage/workload rows, 893 rate/catalog/governance rows). See [`coverage_report.md`](coverage_report.md) for full matrix details.

---

## 3. Normalized Schema (29 Canonical Fields)

All collected records map to a fixed 29-field schema:

1. `platform`: Platform name (`"Databricks"`).
2. `billing_account_identifier`: Account ID.
3. `workspace_identifier`: Workspace ID.
4. `source_category`: Name of the source category.
5. `source_record_identifier`: Primary identifier of the source record.
6. `resource_or_workload_identifier`: Cluster ID, warehouse ID, or query ID.
7. `resource_or_workload_name`: Resource name or query statement summary.
8. `resource_or_workload_type`: Resource type (e.g., `SQL_WAREHOUSE`, `SQL_QUERY`, `COMPUTE_CLUSTER`).
9. `service_or_product`: Originating service (e.g., `SQL`, `DEFAULT_STORAGE`, `GENIE`).
10. `sku`: SKU name (e.g., `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO`).
11. `usage_start`: ISO-8601 UTC usage start timestamp.
12. `usage_end`: ISO-8601 UTC usage end timestamp.
13. `billing_period_start`: Billing period start if provided by source, else `null`.
14. `billing_period_end`: Billing period end if provided by source, else `null`.
15. `consumed_quantity`: Usage volume as exact decimal string.
16. `consumed_unit`: Consumed unit (`DBU`, `DSU`, `GB`, `MILLISECONDS`).
17. `pricing_quantity`: Billable pricing quantity if distinct from consumed quantity, else `null`.
18. `pricing_unit`: Pricing unit if distinct from consumed unit, else `null`.
19. `list_unit_price`: Rate per unit from `system.billing.list_prices`.
20. `list_cost`: Calculated list cost (`consumed_quantity * list_unit_price`).
21. `contracted_cost`: Contractual negotiated cost (`null` when no custom account rate applies).
22. `effective_cost`: Actual incurred cost (`null` when not provided by source).
23. `billed_cost`: Invoiced cost (`null` when not provided by source).
24. `currency`: Currency code (e.g., `"USD"`).
25. `user_or_service_principal_attribution`: Identity responsible for the workload (email or user ID).
26. `tags_or_allocation_metadata`: Parsed dictionary of resource allocation tags.
27. `source_update_time`: Ingestion/audit timestamp in Databricks system tables.
28. `collection_time`: Pipeline execution timestamp in UTC.
29. `additional_source_metadata`: Unaltered raw source record preserved for auditability.

---

## 4. Setup & Installation

### Prerequisites
- Python 3.10+
- A Databricks workspace (AWS, Azure, or GCP)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Configure `.env` with workspace credentials:
```bash
DATABRICKS_HOST=https://dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=optional_sql_warehouse_id
DATABRICKS_ACCOUNT_ID=optional_account_id
```

> **Security Note**: Never commit `.env` to version control or include it in ZIP archives. The repository `.gitignore` automatically excludes `.env` and Python cache directories.

---

## 5. Execution Guide

### Run Full Pipeline
Extracts all categories, calculates list cost, normalizes to 29 fields, preserves raw output, and produces the coverage report:
```bash
python main.py
```

### Command-Line Arguments
```bash
# Filter usage queries by half-open UTC interval [start, end)
python main.py --start-time 2026-10-01T00:00:00Z --end-time 2026-10-06T23:59:59Z

# Fast authentication and SQL warehouse probe without data extraction
python main.py --verify-only

# Apply redaction to local output/ files (by default local files retain raw data)
python main.py --redact-output

# Output to custom directory (default: output)
python main.py --output-dir custom_output
```

### Run Offline Tests
Execute unit tests covering schema normalization, decimal precision, pagination, polling, half-open date filtering, error classification, and data redaction:
```bash
python -m pytest -v
```

---

## 6. Project Layout

```text
.
├── main.py                     # CLI entry point and pipeline orchestrator
├── coverage_report.md          # Generated coverage matrix (LF line endings)
├── execution_evidence.md       # Redacted console output and verification log
├── requirements.txt            # Pinned dependency specifications
├── .env.example                # Template configuration file with placeholder values
├── .gitignore                  # Exclusion rules for secrets, caches, and outputs
├── samples/
│   ├── raw_sample.json         # Sanitized raw source record sample
│   └── normalized_sample.json  # Sanitized 29-field normalized schema sample
├── src/
│   ├── client.py               # Authentication, warehouse selection, SqlHelper & error classifier
│   ├── collectors/
│   │   └── __init__.py         # Unified collectors registry with shared query_billing_joined_prices
│   ├── discovery.py            # System table discovery and active probe engine
│   ├── normalizer.py           # 29-field schema transformation and Decimal handler
│   ├── exporter.py             # Raw, normalized, and sanitized sample JSON file output
│   ├── report.py               # Markdown and JSON coverage report generator
│   └── redact.py               # PII/host/query redaction utilities for public evidence
└── tests/
    ├── test_normalizer.py      # Normalizer, Decimal precision, cluster regression, schema tests
    └── test_collectors.py      # Pagination, polling, filtering, empty results, raw output tests
```

---

## 7. AI Usage Disclosure

- **Tool Used**: Gemini 3.8 Flash, GPT-5.6 Luna.
- **Scope of Assistance**:
  - Code review and refactoring of collectors, normalizer, and test suite.
  - Verification of Databricks SDK method signatures and SQL statement execution APIs.
  - Implementation of offline unit test cases and redaction utilities.
