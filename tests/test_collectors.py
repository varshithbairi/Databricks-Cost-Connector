"""Automated tests for Collector logic, SQL pagination, Polling, Empty results, and Error resilience.

Runs offline using synthetic mocks to test:
- Permission failures vs authentication failures
- Empty dataset handling
- Chunk pagination & pending polling
- Corrections handling (RETRACTION & RESTATEMENT)
- Unredacted raw output preservation
"""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import pytest

from databricks.sdk.service.sql import StatementState

from src.client import verify_connection, classify_error, SqlHelper
from src.collectors import (
    CollectionResult,
    TagsAllocationCollector,
    AccountConsumptionCollector,
    QueryHistoryAttributionCollector,
    CorrectionsRetractionsCollector,
    BillingUsageCollector,
    query_billing_joined_prices,
)
from src.exporter import CostDataExporter


def test_classify_error():
    """Verify error classification mapping across official statuses using structured boundaries."""
    assert classify_error("User does not have SELECT permission on table") == "permission_denied"
    assert classify_error("HTTP 403 Forbidden") == "permission_denied"
    assert classify_error("HTTP 401 Unauthorized") == "permission_denied"
    assert classify_error("[TABLE_OR_VIEW_NOT_FOUND] The table system.storage does not exist") == "unavailable_in_account"
    assert classify_error("Schema not found: system.nonexistent") == "unavailable_in_account"
    assert classify_error("Feature is not enabled on this workspace") == "unavailable_in_account"
    assert classify_error("Feature is unsupported on AWS") == "unsupported"
    assert classify_error("Unexpected network socket timeout") == "failed"
    # Ensure random numbers in request IDs don't trigger 401/403 false positives
    assert classify_error("Request ID req-40100 failed with internal database timeout") == "failed"


def test_collection_result_finalize_preserves_errors():
    """Verify that finalize() does NOT overwrite terminal error statuses based on record count."""
    res_failed = CollectionResult(category="test", purpose="test", status="failed", records=[])
    assert res_failed.finalize().status == "failed"

    res_denied = CollectionResult(category="test", purpose="test", status="permission_denied", records=[])
    assert res_denied.finalize().status == "permission_denied"

    res_unavail = CollectionResult(category="test", purpose="test", status="unavailable_in_account", records=[])
    assert res_unavail.finalize().status == "unavailable_in_account"

    res_unsupp = CollectionResult(category="test", purpose="test", status="unsupported", records=[])
    assert res_unsupp.finalize().status == "unsupported"

    res_empty = CollectionResult(category="test", purpose="test", status="", records=[])
    assert res_empty.finalize().status == "empty"

    res_col = CollectionResult(category="test", purpose="test", status="", records=[{"id": 1}])
    assert res_col.finalize().status == "collected"


def test_empty_results():
    """EXPLICIT TEST: Verify that a query returning zero rows yields status 'empty' and count 0."""
    mock_client = MagicMock()
    mock_wh = MagicMock()
    mock_wh.id = "wh-1"
    mock_wh.name = "Test WH"
    mock_wh.state = "RUNNING"
    mock_client.warehouses.list.return_value = [mock_wh]

    mock_stmt = MagicMock()
    mock_stmt.statement_id = "stmt-empty"
    mock_stmt.status.state = StatementState.SUCCEEDED
    mock_stmt.manifest.schema.columns = []
    mock_stmt.result.data_array = []
    mock_stmt.result.next_chunk_index = None
    mock_client.statement_execution.execute_statement.return_value = mock_stmt

    collector = BillingUsageCollector(mock_client)
    res = collector.collect()

    assert res.status == "empty"
    assert res.record_count == 0
    assert len(res.records) == 0
    assert res.earliest_timestamp is None
    assert res.latest_timestamp is None


def test_sql_multi_chunk_pagination():
    """Verify SqlHelper fetches all chunks via get_statement_result_chunk_n."""
    mock_client = MagicMock()
    mock_wh = MagicMock()
    mock_wh.id = "wh-1"
    mock_wh.name = "Test WH"
    mock_wh.state = "RUNNING"
    mock_client.warehouses.list.return_value = [mock_wh]

    mock_stmt = MagicMock()
    mock_stmt.statement_id = "stmt-123"
    mock_stmt.status.state = StatementState.SUCCEEDED
    col = MagicMock()
    col.name = "sku"
    mock_stmt.manifest.schema.columns = [col]
    mock_stmt.result.data_array = [["SKU_A"], ["SKU_B"]]
    mock_stmt.result.next_chunk_index = 1
    mock_client.statement_execution.execute_statement.return_value = mock_stmt

    mock_chunk = MagicMock()
    mock_chunk.data_array = [["SKU_C"]]
    mock_chunk.next_chunk_index = None
    mock_client.statement_execution.get_statement_result_chunk_n.return_value = mock_chunk

    res = SqlHelper.execute_query(mock_client, "SELECT sku FROM table")
    assert res["success"] is True
    assert len(res["rows"]) == 3
    assert res["rows"][0]["sku"] == "SKU_A"
    assert res["rows"][2]["sku"] == "SKU_C"
    mock_client.statement_execution.get_statement_result_chunk_n.assert_called_once_with(
        statement_id="stmt-123", chunk_index=1
    )


def test_sql_pending_polling_success():
    """Verify SqlHelper polls when statement is PENDING until SUCCEEDED."""
    mock_client = MagicMock()
    mock_wh = MagicMock()
    mock_wh.id = "wh-1"
    mock_wh.name = "Test WH"
    mock_wh.state = "RUNNING"
    mock_client.warehouses.list.return_value = [mock_wh]

    stmt_pending = MagicMock()
    stmt_pending.statement_id = "stmt-pending-1"
    stmt_pending.status.state = StatementState.PENDING
    mock_client.statement_execution.execute_statement.return_value = stmt_pending

    stmt_succeeded = MagicMock()
    stmt_succeeded.statement_id = "stmt-pending-1"
    stmt_succeeded.status.state = StatementState.SUCCEEDED
    col = MagicMock()
    col.name = "val"
    stmt_succeeded.manifest.schema.columns = [col]
    stmt_succeeded.result.data_array = [["42"]]
    stmt_succeeded.result.next_chunk_index = None
    mock_client.statement_execution.get_statement.return_value = stmt_succeeded

    with patch("time.sleep", return_value=None):
        res = SqlHelper.execute_query(mock_client, "SELECT 42")

    assert res["success"] is True
    assert len(res["rows"]) == 1
    assert res["rows"][0]["val"] == "42"


def test_corrections_retractions_and_restatements():
    """Verify that CorrectionsRetractionsCollector queries both RETRACTION and RESTATEMENT."""
    mock_client = MagicMock()
    mock_wh = MagicMock()
    mock_wh.id = "wh-1"
    mock_wh.name = "Test WH"
    mock_wh.state = "RUNNING"
    mock_client.warehouses.list.return_value = [mock_wh]

    mock_stmt = MagicMock()
    mock_stmt.statement_id = "stmt-corr"
    mock_stmt.status.state = StatementState.SUCCEEDED
    col1 = MagicMock(); col1.name = "record_id"
    col2 = MagicMock(); col2.name = "record_type"
    mock_stmt.manifest.schema.columns = [col1, col2]
    mock_stmt.result.data_array = [["rec-1", "RESTATEMENT"], ["rec-2", "RETRACTION"]]
    mock_stmt.result.next_chunk_index = None
    mock_client.statement_execution.execute_statement.return_value = mock_stmt

    collector = CorrectionsRetractionsCollector(mock_client)
    res = collector.collect()

    assert res.status == "collected"
    assert res.record_count == 2
    # Verify the executed SQL query contains IN ('RETRACTION', 'RESTATEMENT')
    call_kwargs = mock_client.statement_execution.execute_statement.call_args[1]
    assert "IN ('RETRACTION', 'RESTATEMENT')" in call_kwargs["statement"]


def test_permission_denied_classification(monkeypatch):
    """Verify that an endpoint returning HTTP 401/403 is classified as 'permission_denied'."""
    mock_client = MagicMock()
    mock_client.config.account_id = "test-account-id"

    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = '{"error_code": 401, "message": "Credential was not sent or was of an unsupported type"}'

    monkeypatch.setattr("requests.get", lambda *args, **kwargs: mock_resp)
    monkeypatch.setattr("src.collectors.load_config", lambda: {"token": "test-token", "account_id": "test-account"})

    collector = AccountConsumptionCollector(mock_client)
    result = collector.collect()

    assert result.status == "permission_denied"
    assert result.record_count == 0
    assert "401" in result.explanation


def test_authentication_validation_failure(monkeypatch):
    """Verify that missing host or token asserts authenticated False and never leaks secrets."""
    monkeypatch.setattr("src.client.load_config", lambda: {"host": None, "token": None, "account_id": None, "warehouse_id": None})
    res = verify_connection()
    assert res["authenticated"] is False
    assert "DATABRICKS_HOST is not set" in res["error"]
    assert res["token_masked"] == "<none>"

    monkeypatch.setattr("src.client.load_config", lambda: {"host": "https://dbc-test.cloud.databricks.com", "token": None, "account_id": None, "warehouse_id": None})
    res2 = verify_connection()
    assert res2["authenticated"] is False
    assert "DATABRICKS_TOKEN is not set" in res2["error"]


def test_query_history_pagination_and_time_window():
    """Verify query history pagination and half-open time boundary [start, end)."""
    mock_client = MagicMock()

    page1 = MagicMock()
    q1 = MagicMock()
    q1.query_id = "q-1"
    q1.query_start_time_ms = 1728039600000  # 2024-10-04T11:00:00Z (on start boundary)
    q1.query_end_time_ms = 1728039601000
    q1.metrics = None
    page1.res = [q1]
    page1.has_next_page = True
    page1.next_page_token = "token-page-2"

    page2 = MagicMock()
    q2 = MagicMock()
    q2.query_id = "q-2"
    q2.query_start_time_ms = 1728043200000  # 2024-10-04T12:00:00Z (on end boundary -> excluded)
    q2.query_end_time_ms = 1728043201000
    q2.metrics = None
    page2.res = [q2]
    page2.has_next_page = False
    page2.next_page_token = None

    def side_effect(**kwargs):
        if kwargs.get("page_token") == "token-page-2":
            return page2
        return page1

    mock_client.query_history.list.side_effect = side_effect

    start_dt = datetime(2024, 10, 4, 11, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2024, 10, 4, 12, 0, 0, tzinfo=timezone.utc)

    collector = QueryHistoryAttributionCollector(mock_client)
    result = collector.collect(start_time=start_dt, end_time=end_dt)

    assert result.status == "collected"
    assert result.record_count == 1
    assert result.records[0]["query_id"] == "q-1"


def test_tags_subsource_error_reported():
    """Verify that a failure in one sub-source (clusters) is reported in the explanation."""
    mock_client = MagicMock()
    wh = MagicMock()
    wh.id = "wh-1"
    wh.name = "WH 1"
    wh.tags = None
    mock_client.warehouses.list.return_value = [wh]
    mock_client.clusters.list.side_effect = Exception("Clusters API 500 internal error")

    collector = TagsAllocationCollector(mock_client)
    res = collector.collect()

    assert res.status == "collected"
    assert "clusters sub-source error" in res.explanation


def test_raw_output_preserves_unredacted_records(tmp_path):
    """Verify that export_raw_category preserves actual source identifiers on disk without masking."""
    exporter = CostDataExporter(output_dir=str(tmp_path), redact=False)
    res = CollectionResult(
        category="billing_and_usage",
        purpose="Test raw preservation",
        status="collected",
        records=[{
            "account_id": "79df8776-2d21-4d5f-a07b-62b31751d591",
            "workspace_id": "7474646336906520",
            "record_id": "rec-12345",
            "user_name": "developer@databricks.com",
        }],
    )
    res.finalize()
    raw_file = exporter.export_raw_category(res)

    import json
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    record = data["records"][0]
    assert record["account_id"] == "79df8776-2d21-4d5f-a07b-62b31751d591"
    assert record["workspace_id"] == "7474646336906520"
    assert record["user_name"] == "developer@databricks.com"


def test_pricing_join_cloud_and_currency_filters():
    """Verify that query_billing_joined_prices applies cloud and currency code safeguards in SQL."""
    mock_client = MagicMock()
    with patch("src.client.SqlHelper.execute_query") as mock_exec:
        mock_exec.return_value = {
            "success": True,
            "status": "collected",
            "rows": [{
                "account_id": "acc-1",
                "workspace_id": "ws-1",
                "record_id": "rec-1",
                "sku_name": "PREMIUM_ALL_PURPOSE_COMPUTE",
                "cloud": "AWS",
                "usage_start_time": "2024-10-04T10:00:00Z",
                "usage_end_time": "2024-10-04T11:00:00Z",
                "usage_date": "2024-10-04",
                "custom_tags": "{}",
                "usage_unit": "DBU",
                "usage_quantity": "2.5",
                "usage_metadata": "{}",
                "identity_metadata": "{}",
                "record_type": "ORIGINAL",
                "ingestion_date": "2024-10-04",
                "billing_origin_product": "ALL_PURPOSE",
                "pricing": '{"default": 0.55}',
                "currency_code": "USD",
            }],
            "warehouse_used": "Test WH",
            "error": None,
        }

        res = query_billing_joined_prices(
            client=mock_client,
            category_name="test_pricing_join",
            purpose="Test pricing join filters",
            required_permission="SELECT on system.billing.usage",
            custom_where="u.billing_origin_product = 'ALL_PURPOSE'",
        )

        # Verify SQL passed to execute_query includes both cloud and currency guards
        call_args = mock_exec.call_args
        sql_query = call_args[0][1]
        assert "(p.cloud IS NULL OR u.cloud = p.cloud)" in sql_query
        assert "(p.currency_code IS NULL OR p.currency_code = 'USD')" in sql_query
        assert "(u.billing_origin_product = 'ALL_PURPOSE')" in sql_query

        # Verify parsed results and cost calculation
        assert res.status == "collected"
        assert res.record_count == 1
        record = res.records[0]
        assert record["currency_code"] == "USD"
        assert record["list_unit_price"] == "0.55"
        assert record["list_cost"] == "1.375"  # 2.5 * 0.55 = 1.375

