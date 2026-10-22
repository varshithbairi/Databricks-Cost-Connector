"""Automated tests for Normalization, Decimal Precision, Timestamps, and Redaction.

These tests run completely offline without Databricks credentials using synthetic fixtures.
"""

from decimal import Decimal
import pytest

from src.collectors import to_decimal_str, to_utc_iso
from src.normalizer import CostRecordNormalizer
from src.redact import redact_record, redact_query_text


def test_schema_completeness():
    """Verify that every normalized record contains all canonical fields (29 total)."""
    normalizer = CostRecordNormalizer(default_account_id="acc-123", default_workspace_id="ws-456")
    raw_sample = {
        "account_id": "acc-123",
        "workspace_id": "ws-456",
        "record_id": "rec-001",
        "sku_name": "PREMIUM_ALL_PURPOSE_COMPUTE",
        "usage_quantity": "1.500000000000000000",
        "usage_unit": "DBU",
        "usage_start_time": "2026-10-04T10:00:00Z",
        "usage_end_time": "2026-10-04T11:00:00Z",
        "list_unit_price": "0.640000000000000000",
        "list_cost": "0.960000000000000000",
        "currency": "USD",
        "custom_tags": '{"cost_center": "ml-ops", "env": "prod"}',
        "identity_metadata": '{"run_as": "engineer@company.com"}',
        "usage_metadata": '{"cluster_id": "cluster-xyz-123"}',
        "ingestion_date": "2026-10-04T12:00:00Z",
    }
    norm = normalizer.normalize_record("billing_and_usage", raw_sample)

    expected_keys = [
        "platform",
        "billing_account_identifier",
        "workspace_identifier",
        "source_category",
        "source_record_identifier",
        "resource_or_workload_identifier",
        "resource_or_workload_name",
        "resource_or_workload_type",
        "service_or_product",
        "sku",
        "usage_start",
        "usage_end",
        "billing_period_start",
        "billing_period_end",
        "consumed_quantity",
        "consumed_unit",
        "pricing_quantity",
        "pricing_unit",
        "list_unit_price",
        "list_cost",
        "contracted_cost",
        "effective_cost",
        "billed_cost",
        "currency",
        "user_or_service_principal_attribution",
        "tags_or_allocation_metadata",
        "source_update_time",
        "collection_time",
        "additional_source_metadata",
    ]

    for key in expected_keys:
        assert key in norm, f"Missing required canonical field: {key}"

    assert norm["platform"] == "Databricks"
    assert norm["billing_account_identifier"] == "acc-123"
    assert norm["workspace_identifier"] == "ws-456"
    assert norm["sku"] == "PREMIUM_ALL_PURPOSE_COMPUTE"
    assert norm["consumed_quantity"] == "1.500000000000000000"
    assert norm["consumed_unit"] == "DBU"
    assert norm["list_unit_price"] == "0.640000000000000000"
    assert norm["list_cost"] == "0.960000000000000000"
    assert norm["currency"] == "USD"
    assert norm["user_or_service_principal_attribution"] == "engineer@company.com"
    assert norm["resource_or_workload_identifier"] == "cluster-xyz-123"
    assert norm["resource_or_workload_type"] == "COMPUTE_CLUSTER"
    assert norm["tags_or_allocation_metadata"] == {"cost_center": "ml-ops", "env": "prod"}
    assert norm["source_update_time"] == "2026-10-04T12:00:00Z"
    assert norm["pricing_quantity"] is None
    assert norm["billing_period_start"] is None


def test_cluster_and_compute_normalization_not_null():
    """REGRESSION TEST: Verify that cluster_and_compute_consumption records are NOT normalized to nulls."""
    normalizer = CostRecordNormalizer(default_account_id="acc-123", default_workspace_id="ws-456")
    raw_cluster_usage = {
        "account_id": "acc-123",
        "workspace_id": "ws-456",
        "record_id": "rec-cluster-001",
        "sku_name": "PREMIUM_ALL_PURPOSE_COMPUTE",
        "cloud": "AWS",
        "usage_start_time": "2026-10-04T00:00:00Z",
        "usage_end_time": "2026-10-04T01:00:00Z",
        "usage_quantity": "2.500000000000000000",
        "usage_unit": "DBU",
        "list_unit_price": "0.640000000000000000",
        "list_cost": "1.600000000000000000000000000",
        "currency": "USD",
        "usage_metadata": '{"cluster_id": "cluster-xyz-123", "node_type": "i3.xlarge"}',
        "identity_metadata": '{"owned_by": "data-team@company.com"}',
        "billing_origin_product": "ALL_PURPOSE",
    }
    norm = normalizer.normalize_record("cluster_and_compute_consumption", raw_cluster_usage)

    assert norm["source_record_identifier"] == "rec-cluster-001"
    assert norm["sku"] == "PREMIUM_ALL_PURPOSE_COMPUTE"
    assert norm["consumed_quantity"] == "2.500000000000000000"
    assert norm["consumed_unit"] == "DBU"
    assert norm["list_cost"] == "1.600000000000000000000000000"
    assert norm["usage_start"] == "2026-10-04T00:00:00Z"
    assert norm["resource_or_workload_identifier"] == "cluster-xyz-123"
    assert norm["resource_or_workload_type"] == "COMPUTE_CLUSTER"
    assert norm["user_or_service_principal_attribution"] == "data-team@company.com"


def test_decimal_precision_preservation():
    """Verify that arbitrary decimal precision is preserved without floating point truncation."""
    test_cases = [
        ("0.012391800000000000", "0.012391800000000000"),
        ("0.640000000000000000", "0.640000000000000000"),
        ("123456789.123456789012345678", "123456789.123456789012345678"),
        (Decimal("42.000000001"), "42.000000001"),
        (100, "100"),
        (None, None),
    ]

    for input_val, expected_out in test_cases:
        res = to_decimal_str(input_val)
        assert res == expected_out, f"Decimal precision failed for {input_val}: got {res}"


def test_usage_list_price_exact_multiplication():
    """Verify that multiplying consumed quantity and unit price preserves full precision."""
    qty_str = "0.012391800000000000"
    price_str = "0.023000000000000000"
    expected_cost = str(Decimal(qty_str) * Decimal(price_str))

    cost_str = to_decimal_str(Decimal(qty_str) * Decimal(price_str))
    assert cost_str == expected_cost
    assert cost_str == str(Decimal(qty_str) * Decimal(price_str))


def test_timestamp_handling():
    """Verify conversion of various timestamp formats to UTC ISO-8601 strings and sub-seconds."""
    ms_ts = 1728039600000  # 2024-10-04T11:00:00Z
    assert to_utc_iso(ms_ts) == "2024-10-04T11:00:00Z"

    s_ts = 1728039600
    assert to_utc_iso(s_ts) == "2024-10-04T11:00:00Z"

    iso_str = "2026-10-04T16:30:00+05:30"
    assert to_utc_iso(iso_str) == "2026-10-04T11:00:00Z"

    sub_sec = "2026-10-04T11:00:00.123456Z"
    assert to_utc_iso(sub_sec) == "2026-10-04T11:00:00.123456Z"

    z_str = "2026-10-04T11:00:00Z"
    assert to_utc_iso(z_str) == "2026-10-04T11:00:00Z"

    assert to_utc_iso(None) is None
    assert to_utc_iso("") is None
    assert to_utc_iso("not-a-timestamp") is None


def test_malformed_source_records():
    """Verify normalizer resilience when handling corrupted or incomplete source data."""
    normalizer = CostRecordNormalizer()

    malformed_metadata = {
        "record_id": "rec-bad-json",
        "usage_quantity": "5.0",
        "usage_metadata": "{invalid_json: true, missing_quotes}",
        "identity_metadata": "{broken-json",
        "custom_tags": "not json",
    }
    norm1 = normalizer.normalize_record("billing_and_usage", malformed_metadata)
    assert norm1["source_record_identifier"] == "rec-bad-json"
    assert norm1["consumed_quantity"] == "5.0"
    assert norm1["resource_or_workload_identifier"] is None
    assert norm1["user_or_service_principal_attribution"] is None
    assert norm1["tags_or_allocation_metadata"] is None

    norm2 = normalizer.normalize_record("billing_and_usage", {})
    assert norm2["platform"] == "Databricks"
    assert norm2["source_record_identifier"] is None
    assert norm2["consumed_quantity"] is None

    pricing_bad = {
        "sku_name": "TEST_SKU",
        "pricing": "not a json string",
    }
    norm3 = normalizer.normalize_record("sku_and_pricing_information", pricing_bad)
    assert norm3["sku"] == "TEST_SKU"
    assert norm3["list_unit_price"] is None


def test_redaction():
    """Verify masking of emails, account IDs, hostnames, query texts, and tokens."""
    sample = {
        "user_name": "alice.smith@databricks.com",
        "account_id": "79df8776-2d21-4d5f-a07b-62b31751d591",
        "host": "https://dbc-127103e0-5e4b.cloud.databricks.com",
        "token": "dapi1234567890abcdef1234567890abcdef",
        "query_text": "SELECT * FROM sensitive_financial_table WHERE credit_card = '1234'",
        "resource_or_workload_name": "SELECT secret_data FROM confidential_db",
    }
    redacted = redact_record(sample)

    assert "@" in redacted["user_name"]
    assert "alice.smith" not in redacted["user_name"]
    assert redacted["account_id"] == "79df...d591"
    assert "..." in redacted["host"] and "cloud.databricks.com" in redacted["host"]
    assert redacted["token"] == "dapi[REDACTED_TOKEN]"
    assert "sensitive_financial_table" not in redacted["query_text"]
    assert redacted["query_text"] == "SELECT [REDACTED_QUERY]"
    assert redacted["resource_or_workload_name"] == "SELECT [REDACTED_QUERY]"
