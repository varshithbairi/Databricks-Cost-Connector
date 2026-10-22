"""Artifact Exporter for Databricks Cost Connector.

Saves raw and normalized cost records to structured JSON files.
Preserves unmanipulated raw source records and identifiers in local output,
while providing sanitized sample export for public repository submission.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional
from src.collectors import CollectionResult
from src.redact import redact_record, redact_records


class CostDataExporter:
    """Exports raw, normalized, and sanitized sample datasets."""

    def __init__(self, output_dir: str = "output", redact: bool = False):
        self.output_dir = output_dir
        self.redact = redact
        self.raw_dir = os.path.join(output_dir, "raw")
        self.norm_dir = os.path.join(output_dir, "normalized")
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.norm_dir, exist_ok=True)

    def export_raw_category(self, result: CollectionResult) -> str:
        """Save raw records for a specific category.
        
        Preserves true unredacted source records by default.
        """
        filename = f"{result.category}.json"
        filepath = os.path.join(self.raw_dir, filename)

        raw_records = result.records
        if self.redact and raw_records:
            raw_records = redact_records(raw_records)

        payload = {
            "category": result.category,
            "purpose": result.purpose,
            "status": result.status,
            "record_count": result.record_count,
            "earliest_timestamp": result.earliest_timestamp,
            "latest_timestamp": result.latest_timestamp,
            "required_permission": result.required_permission,
            "explanation": result.explanation,
            "metadata": result.metadata,
            "records": raw_records,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)

        return filepath

    def export_normalized_category(self, category: str, records: List[Dict[str, Any]]) -> str:
        """Save normalized records for a specific category."""
        filename = f"{category}.json"
        filepath = os.path.join(self.norm_dir, filename)

        out_records = records
        if self.redact and out_records:
            out_records = redact_records(out_records)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(out_records, f, indent=2, default=str)

        return filepath

    def export_master_normalized(self, all_normalized_records: List[Dict[str, Any]]) -> str:
        """Save the unified normalized dataset combining all categories."""
        filepath = os.path.join(self.norm_dir, "all_normalized_costs.json")
        out_records = all_normalized_records
        if self.redact and out_records:
            out_records = redact_records(out_records)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(out_records, f, indent=2, default=str)
        return filepath

    def export_sanitized_samples(
        self,
        raw_sample: Dict[str, Any],
        norm_sample: Dict[str, Any],
        samples_dir: str = "samples",
    ) -> Dict[str, str]:
        """Export sanitized, redacted samples for public repository submission."""
        os.makedirs(samples_dir, exist_ok=True)
        raw_path = os.path.join(samples_dir, "raw_sample.json")
        norm_path = os.path.join(samples_dir, "normalized_sample.json")

        redacted_raw = redact_record(raw_sample)
        redacted_norm = redact_record(norm_sample)

        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(redacted_raw, f, indent=2, default=str)

        with open(norm_path, "w", encoding="utf-8") as f:
            json.dump(redacted_norm, f, indent=2, default=str)

        return {"raw_sample_path": raw_path, "normalized_sample_path": norm_path}
