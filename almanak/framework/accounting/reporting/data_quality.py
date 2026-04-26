"""Data quality section for accounting reports.

Surfaces any UNAVAILABLE confidence records so users can see where
accounting data is missing or incomplete.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .loader import AccountingData


@dataclass
class DataQualityIssue:
    event_type: str
    position_key: str
    timestamp: str
    reason: str
    protocol: str
    chain: str


@dataclass
class DataQualitySection:
    issues: list[DataQualityIssue] = field(default_factory=list)
    parse_errors: int = 0

    @property
    def is_empty(self) -> bool:
        return not self.issues and self.parse_errors == 0


def build_data_quality(data: AccountingData) -> DataQualitySection:
    """Extract UNAVAILABLE confidence records and deserialization errors."""
    issues: list[DataQualityIssue] = []
    for row in data.unavailable_records:
        issues.append(
            DataQualityIssue(
                event_type=row.get("event_type", ""),
                position_key=row.get("position_key", ""),
                timestamp=row.get("timestamp", ""),
                reason=_extract_reason(row.get("payload_json", "")),
                protocol=row.get("protocol", ""),
                chain=row.get("chain", ""),
            )
        )
    return DataQualitySection(issues=issues, parse_errors=data.parse_errors)


def _extract_reason(payload_json: str) -> str:
    try:
        d = json.loads(payload_json)
        return d.get("unavailable_reason", "") if isinstance(d, dict) else ""
    except (json.JSONDecodeError, TypeError):
        return ""
