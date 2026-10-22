# Databricks Cost Connector — Coverage Report

## Summary

- **Platform**: Databricks (AWS)
- **Workspace Identifier**: `747...520`
- **Billing Account Identifier**: `79df...d591`
- **Collection Time Range**: Full available history (all available intervals)
- **Categories Investigated**: 18
- **Total Records**: 1513 (620 usage/workload rows, 893 reference/config/rate rows)

### Status Counts

| Status | Count | Description |
| :--- | :--- | :--- |
| `collected` | 10 | Telemetry extracted and normalized |
| `empty` | 7 | Accessible table or API; returned 0 rows for query criteria |
| `permission_denied` | 1 | Access denied by role or permission boundary |
| `unavailable_in_account` | 0 | Feature or table not configured in account |
| `unsupported` | 0 | Feature not supported or not implemented |
| `failed` | 0 | API or query execution error |

---

## Category Investigation Matrix

| Category / Dataset Name | Purpose | Status | Records | Earliest Timestamp | Latest Timestamp | Required Permission / Account Capability | Explanation when Records Not Collected |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **billing_and_usage** | System-level DBU consumption, SKU billing records, and list costs | `collected` | 63 | `2026-10-03T23:00:00Z` | `2026-10-10T14:00:00Z` | USE CATALOG system, USE SCHEMA billing, SELECT on system.billing.usage | - |
| **account_and_workspace_consumption** | Account-wide aggregated billing and workspace usage export | `permission_denied` | 0 | - | - | Account Admin role on accounts.cloud.databricks.com | HTTP 401: Workspace Personal Access Token (PAT) cannot authenticate to Account Console APIs; requires Account Admin OAuth credentials on accounts.cloud.databricks.com. |
| **cluster_and_compute_consumption** | All-purpose and automated compute clusters consumption and configurations | `empty` | 0 | - | - | Workspace access, cluster read permissions | Query succeeded on running warehouse; 0 cluster records in system.billing.usage and 0 clusters configured in workspace. |
| **sql_warehouse_consumption** | Consumption and DBU usage attributed to Databricks SQL warehouses | `collected` | 8 | `2026-10-04T00:00:00Z` | `2026-10-10T14:00:00Z` | SELECT on system.billing.usage, workspace SQL view permissions | - |
| **jobs_and_pipeline_consumption** | Workflows, automated job definitions, tasks, and task compute consumption | `empty` | 0 | - | - | Workspace access, jobs view permissions, SELECT on system.billing.usage | 0 job consumption rows found in system.billing.usage and 0 jobs created in workspace. |
| **serverless_consumption** | Consumption and DBU usage for serverless compute workloads and serverless SQL | `collected` | 8 | `2026-10-04T00:00:00Z` | `2026-10-10T14:00:00Z` | SELECT on system.billing.usage | - |
| **storage_related_usage** | Managed Delta Lake table storage sizes, file counts, and DBFS storage billing | `collected` | 52 | `2026-10-04T00:00:00Z` | `2026-10-09T19:00:00Z` | USE CATALOG system, USE SCHEMA storage, SELECT on system.billing.usage | - |
| **data_transfer_network_usage** | Cloud inter-region and internet egress data transfer consumption and rates | `empty` | 0 | - | - | SELECT on system.billing.usage and system.billing.list_prices | Query succeeded on running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4) and returned 0 rows. |
| **sku_and_pricing_information** | Official Databricks Unit (DBU) and cloud compute SKU rate catalog | `collected` | 887 | `2013-01-01T00:00:00Z` | `2026-08-21T00:00:00Z` | SELECT on system.billing.list_prices | - |
| **list_cost_calculations** | Calculated gross list cost (consumed quantity * list unit price from rate catalog) | `collected` | 63 | `2026-10-03T23:00:00Z` | `2026-10-10T14:00:00Z` | SELECT on system.billing.usage and system.billing.list_prices | - |
| **resource_workload_attribution** | Workload attribution mapping queries and tasks to specific warehouses/clusters | `collected` | 213 | `2026-10-04T00:32:59.737Z` | `2026-10-10T17:20:37.326Z` | Workspace access, query history view permissions | - |
| **user_service_principal_attribution** | Identifies user email, service principal, or system identity initiating the cost | `collected` | 213 | `2026-10-04T00:32:59.737Z` | `2026-10-10T17:20:37.326Z` | Workspace access, query history view permissions | - |
| **tags_and_allocation_metadata** | FinOps cost-center allocation tags, environment tags, and custom metadata | `collected` | 1 | - | - | Workspace compute read permissions | Extracted allocation tags from 1 compute resources. |
| **budgets_alerts_cost_controls** | Governance guardrails, cluster sizing limits, budget policies, and automated alerts | `collected` | 5 | - | - | Cluster policies and SQL alert view permissions | Collected 5 cost governance policies and alerts. |
| **corrections_retractions_restatements** | Adjustments, billing corrections, and credit retractions published by Databricks | `empty` | 0 | - | - | SELECT on system.billing.usage | Query succeeded on running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4) and returned 0 rows. |
| **optimization_cost_saving_info** | Predictive optimization, automatic vacuum, and compute cost reduction operations | `empty` | 0 | - | - | SELECT on system.storage.predictive_optimization_operations_history | Query succeeded on running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4) and returned 0 rows in predictive optimization history. |
| **contract_commitment_discounts** | Negotiated custom enterprise pricing, committed-use discounts, and contractual rates | `empty` | 0 | - | - | SELECT on system.billing.account_prices | Query succeeded on running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4) and returned 0 rows in system.billing.account_prices. |
| **attributed_usage_table** | Fine-grained resource and tag attribution from system.billing.attributed_usage | `empty` | 0 | - | - | SELECT on system.billing.attributed_usage | Table probed on running warehouse 'Serverless Starter Warehouse' (79fe5b3af41045c4) and returned 0 rows. |

---

## Detailed Investigation Notes & Analysis

### cluster_and_compute_consumption — empty
The collection query returned zero matching cluster-related billing records for the available query range (`(u.sku_name LIKE '%ALL_PURPOSE%' OR u.sku_name LIKE '%JOBS_COMPUTE%' OR u.billing_origin_product = 'ALL_PURPOSE') AND u.sku_name NOT LIKE '%SQL%'`). The fallback to the workspace Clusters API also verified that zero compute clusters are currently configured in this workspace.

### jobs_and_pipeline_consumption — empty
The collection query returned zero matching job and pipeline consumption records in `system.billing.usage` for the available query range, and the Jobs API confirmed zero jobs or workflows have been created in this trial workspace.

### account_and_workspace_consumption — permission_denied
The account-level API request returned HTTP 401 because workspace Personal Access Tokens (PATs) cannot authenticate to Account Console APIs (`accounts.cloud.databricks.com`). Account-level consumption APIs require dedicated Account Admin OAuth service principal credentials.

### storage_related_usage — collected
Collected 52 storage DSU billing records from `system.billing.usage` (`PREMIUM_DATABRICKS_STORAGE_US_EAST_OHIO`). Note that storage billing records (`system.billing.usage`) and storage table metrics (`system.storage.table_metrics_history`) represent distinct telemetry sources and are reported accurately.

### attributed_usage_table — empty
The discovered table `system.billing.attributed_usage` was actively queried with a `LIMIT 10` probe. The query completed successfully on the running warehouse and returned zero rows, confirming that while the table exists and is accessible, no fine-grained attribution records have been generated in this workspace.

---

## Discovered System Tables

Tables discovered via `system.information_schema.tables`:

- `system.access.assistant_events`
- `system.access.audit`
- `system.access.clean_room_events`
- `system.access.column_lineage`
- `system.access.inbound_network`
- `system.access.outbound_network`
- `system.access.table_lineage`
- `system.access.workspaces_latest`
- `system.billing.account_prices`
- `system.billing.attributed_usage`
- `system.billing.list_prices`
- `system.billing.usage`
- `system.compute.clusters`
- `system.compute.instance_events`
- `system.compute.instance_pools`
- `system.compute.node_timeline`
- `system.compute.node_types`
- `system.compute.warehouse_events`
- `system.compute.warehouses`
- `system.lakeflow.job_run_timeline`
- `system.lakeflow.job_task_run_timeline`
- `system.lakeflow.job_tasks`
- `system.lakeflow.jobs`
- `system.lakeflow.pipeline_update_timeline`
- `system.lakeflow.pipelines`
- `system.lakeflow.zerobus_ingest`
- `system.lakeflow.zerobus_stream`
- `system.query.history`
- `system.storage.auto_upgrade_operations_history`
- `system.storage.predictive_optimization_operations_history`
- `system.storage.table_metrics_history`