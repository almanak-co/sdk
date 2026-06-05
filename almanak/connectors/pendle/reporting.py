"""Pendle accounting report provider."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._strategy_base.accounting_report_registry import (
    AccountingReportCapability,
    AccountingReportConnector,
    AccountingReportSectionCapability,
)
from almanak.framework.accounting.models import PendleAccountingEvent, PendleEventType
from almanak.framework.accounting.reporting.pendle_report import build_pendle_report
from almanak.framework.accounting.reporting.render_json import pendle_section_to_dict
from almanak.framework.accounting.reporting.render_text import render_pendle_section


class PendleAccountingReportConnector(
    AccountingReportConnector,
    AccountingReportCapability,
    AccountingReportSectionCapability,
):
    """Deserialize and render Pendle-specific accounting events for reporting."""

    key: ClassVar[str] = "pendle"
    strategy_class: ClassVar[str] = "pendle"
    event_types: ClassVar[frozenset[str]] = frozenset(event_type.value for event_type in PendleEventType)
    section_key: ClassVar[str] = "pendle"
    section_order: ClassVar[int] = 300

    def deserialize_event(self, identity: Any, payload_json: str) -> PendleAccountingEvent:
        return PendleAccountingEvent.from_payload_json(identity, payload_json)

    def build_section(self, data: Any) -> Any:
        return build_pendle_report(data)

    def render_text(self, section: Any, data: Any) -> str:
        return render_pendle_section(section)

    def to_json(self, section: Any) -> dict[str, Any]:
        return pendle_section_to_dict(section)


__all__ = ["PendleAccountingReportConnector"]
