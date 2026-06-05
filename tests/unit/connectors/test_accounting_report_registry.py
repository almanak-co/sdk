"""Tests for connector-owned accounting report providers."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from almanak.connectors._strategy_base.accounting_report_registry import (
    AccountingReportCapability,
    AccountingReportConnector,
    AccountingReportRegistry,
    AccountingReportRegistryError,
    AccountingReportSection,
    AccountingReportSectionCapability,
)


class _ReportConnector(AccountingReportConnector, AccountingReportCapability):
    key: ClassVar[str] = "report"
    strategy_class: ClassVar[str] = "report"
    event_types: ClassVar[frozenset[str]] = frozenset({"REPORT_EVENT"})

    def deserialize_event(self, identity: Any, payload_json: str) -> tuple[Any, str]:
        return identity, payload_json


class _DuplicateEventConnector(_ReportConnector):
    key: ClassVar[str] = "dup"


class _CaseDuplicateEventConnector(_ReportConnector):
    key: ClassVar[str] = "case_dup"
    event_types: ClassVar[frozenset[str]] = frozenset({"report_event"})


class _MixedCaseEventConnector(_ReportConnector):
    key: ClassVar[str] = "mixed"
    strategy_class: ClassVar[str] = "mixed"
    event_types: ClassVar[frozenset[str]] = frozenset({"Mixed_Event"})


class _ReservedLendingEventConnector(_ReportConnector):
    key: ClassVar[str] = "reserved"
    strategy_class: ClassVar[str] = "reserved"
    event_types: ClassVar[frozenset[str]] = frozenset({"SUPPLY"})


class _PaddedKeyConnector(_ReportConnector):
    key: ClassVar[str] = " report"


class _PaddedStrategyClassConnector(_ReportConnector):
    key: ClassVar[str] = "padded_strategy"
    strategy_class: ClassVar[str] = "report "


class _PaddedEventTypeConnector(_ReportConnector):
    key: ClassVar[str] = "padded_event"
    event_types: ClassVar[frozenset[str]] = frozenset({"REPORT_EVENT "})


class _NoCapabilityConnector(AccountingReportConnector):
    key: ClassVar[str] = "none"
    strategy_class: ClassVar[str] = "none"
    event_types: ClassVar[frozenset[str]] = frozenset({"NONE_EVENT"})


class _EarlySectionConnector(_ReportConnector, AccountingReportSectionCapability):
    key: ClassVar[str] = "early"
    event_types: ClassVar[frozenset[str]] = frozenset({"EARLY_EVENT"})
    section_key: ClassVar[str] = "early"
    section_order: ClassVar[int] = 10

    def build_section(self, data: Any) -> tuple[str, Any]:
        return self.section_key, data

    def render_text(self, section: Any, data: Any) -> str:
        return f"{section[0]}:{data}"

    def to_json(self, section: Any) -> dict[str, Any]:
        return {"section": section[0], "data": section[1]}


class _LateSectionConnector(_EarlySectionConnector):
    key: ClassVar[str] = "late"
    event_types: ClassVar[frozenset[str]] = frozenset({"LATE_EVENT"})
    section_key: ClassVar[str] = "late"
    section_order: ClassVar[int] = 20


class _DuplicateSectionConnector(_LateSectionConnector):
    key: ClassVar[str] = "duplicate_section"
    event_types: ClassVar[frozenset[str]] = frozenset({"DUPLICATE_SECTION_EVENT"})
    section_key: ClassVar[str] = "early"


class _PaddedSectionConnector(_EarlySectionConnector):
    key: ClassVar[str] = "padded_section"
    event_types: ClassVar[frozenset[str]] = frozenset({"PADDED_SECTION_EVENT"})
    section_key: ClassVar[str] = "early "


class _TypedSectionConnector(_EarlySectionConnector):
    key: ClassVar[str] = "typed"
    event_types: ClassVar[frozenset[str]] = frozenset({"TYPED_SECTION_EVENT"})
    section_key: ClassVar[str] = "typed"
    section_type: ClassVar[type[tuple]] = tuple


class _ParentSection:
    pass


class _ChildSection(_ParentSection):
    pass


class _ParentSectionConnector(_EarlySectionConnector):
    key: ClassVar[str] = "parent_section"
    event_types: ClassVar[frozenset[str]] = frozenset({"PARENT_SECTION_EVENT"})
    section_key: ClassVar[str] = "parent_section"
    section_type: ClassVar[type[_ParentSection]] = _ParentSection


class _ChildSectionConnector(_EarlySectionConnector):
    key: ClassVar[str] = "child_section"
    event_types: ClassVar[frozenset[str]] = frozenset({"CHILD_SECTION_EVENT"})
    section_key: ClassVar[str] = "child_section"
    section_type: ClassVar[type[_ChildSection]] = _ChildSection


class _DuplicateSectionTypeConnector(_TypedSectionConnector):
    key: ClassVar[str] = "duplicate_section_type"
    event_types: ClassVar[frozenset[str]] = frozenset({"DUPLICATE_SECTION_TYPE_EVENT"})
    section_key: ClassVar[str] = "duplicate_section_type"


class _MalformedSectionTypeConnector(_EarlySectionConnector):
    key: ClassVar[str] = "malformed_section_type"
    event_types: ClassVar[frozenset[str]] = frozenset({"MALFORMED_SECTION_TYPE_EVENT"})
    section_key: ClassVar[str] = "malformed_section_type"
    section_type: ClassVar[object] = "not-a-type"


def test_register_rejects_classes() -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match="did you forget to instantiate"):
        registry.register(_ReportConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match="AccountingReportCapability"):
        registry.register(_NoCapabilityConnector())


def test_register_rejects_duplicate_keys() -> None:
    registry = AccountingReportRegistry()
    registry.register(_ReportConnector())

    with pytest.raises(AccountingReportRegistryError, match="already registered"):
        registry.register(_ReportConnector())


def test_register_rejects_duplicate_event_types() -> None:
    registry = AccountingReportRegistry()
    registry.register(_ReportConnector())

    with pytest.raises(AccountingReportRegistryError, match="REPORT_EVENT"):
        registry.register(_DuplicateEventConnector())


def test_register_rejects_case_insensitive_duplicate_event_types() -> None:
    registry = AccountingReportRegistry()
    registry.register(_ReportConnector())

    with pytest.raises(AccountingReportRegistryError, match="REPORT_EVENT"):
        registry.register(_CaseDuplicateEventConnector())


def test_register_rejects_builtin_lending_event_type_claims() -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match="reserved for built-in lending"):
        registry.register(_ReservedLendingEventConnector())


@pytest.mark.parametrize(
    ("connector", "message"),
    [
        (_PaddedKeyConnector(), "connector key"),
        (_PaddedStrategyClassConnector(), "connector strategy_class"),
        (_PaddedEventTypeConnector(), "connector event_types"),
        (_PaddedSectionConnector(), "connector section_key"),
    ],
)
def test_register_rejects_padded_identifiers(connector: AccountingReportConnector, message: str) -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match=message):
        registry.register(connector)


def test_deserialize_event_routes_to_owner() -> None:
    registry = AccountingReportRegistry()
    connector = _ReportConnector()
    registry.register(connector)

    assert registry.deserialize_event("UNKNOWN", object(), "{}") is None
    assert registry.get("report") is connector
    assert registry.get("missing") is None
    key, event = registry.deserialize_event("REPORT_EVENT", "identity", '{"ok": true}')

    assert key == "report"
    assert event == ("identity", '{"ok": true}')
    assert registry.strategy_class_for_event_type("REPORT_EVENT") == "report"


def test_event_type_lookup_is_case_insensitive() -> None:
    registry = AccountingReportRegistry()
    registry.register(_MixedCaseEventConnector())

    assert registry.event_types() == frozenset({"MIXED_EVENT"})
    assert registry.deserialize_event("mixed_event", "identity", "{}") == ("mixed", ("identity", "{}"))
    assert registry.deserialize_event("MIXED_EVENT", "identity", "{}") == ("mixed", ("identity", "{}"))
    assert registry.strategy_class_for_event_type("mixed_event") == "mixed"


def test_register_rejects_duplicate_report_sections() -> None:
    registry = AccountingReportRegistry()
    registry.register(_EarlySectionConnector())

    with pytest.raises(AccountingReportRegistryError, match="section"):
        registry.register(_DuplicateSectionConnector())


def test_register_rejects_duplicate_report_section_types() -> None:
    registry = AccountingReportRegistry()
    registry.register(_TypedSectionConnector())

    with pytest.raises(AccountingReportRegistryError, match="section type"):
        registry.register(_DuplicateSectionTypeConnector())


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (_ParentSectionConnector(), _ChildSectionConnector()),
        (_ChildSectionConnector(), _ParentSectionConnector()),
    ],
)
def test_register_rejects_overlapping_report_section_types(
    first: AccountingReportConnector,
    second: AccountingReportConnector,
) -> None:
    registry = AccountingReportRegistry()
    registry.register(first)

    with pytest.raises(AccountingReportRegistryError, match="overlaps"):
        registry.register(second)


def test_register_rejects_malformed_report_section_type() -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match="section_type"):
        registry.register(_MalformedSectionTypeConnector())


def test_build_sections_routes_to_ordered_section_providers() -> None:
    registry = AccountingReportRegistry()
    registry.register(_LateSectionConnector())
    registry.register(_EarlySectionConnector())

    sections = registry.build_sections("payload")

    assert [section.key for section in sections] == ["early", "late"]
    assert [section.order for section in sections] == [10, 20]
    assert sections[0].section == ("early", "payload")
    assert sections[0].render_text("context") == "early:context"
    assert sections[0].to_json() == {"section": "early", "data": "payload"}


def test_section_renderer_lookup_routes_by_section_key() -> None:
    registry = AccountingReportRegistry()
    connector = _EarlySectionConnector()
    registry.register(connector)

    assert registry.section_provider("early") is connector
    assert registry.render_section_text("early", ("early", "payload"), "context") == "early:context"
    assert registry.section_to_json("early", ("early", "payload")) == {"section": "early", "data": "payload"}


def test_section_renderer_lookup_routes_by_section_type() -> None:
    registry = AccountingReportRegistry()
    connector = _TypedSectionConnector()
    registry.register(connector)

    assert registry.section_provider_for(("early", "payload")) is connector
    assert registry.render_section_text_for(("early", "payload"), "context") == "early:context"
    assert registry.section_to_json_for(("early", "payload")) == {"section": "early", "data": "payload"}


def test_section_renderer_lookup_rejects_unknown_section_key() -> None:
    registry = AccountingReportRegistry()

    with pytest.raises(AccountingReportRegistryError, match="no accounting report section provider"):
        registry.render_section_text("missing", object())
    with pytest.raises(AccountingReportRegistryError, match="no accounting report section provider"):
        registry.section_to_json("missing", object())


def test_section_renderer_lookup_rejects_unknown_section_type() -> None:
    registry = AccountingReportRegistry()
    registry.register(_TypedSectionConnector())

    with pytest.raises(AccountingReportRegistryError, match="no accounting report section provider"):
        registry.render_section_text_for(object())
    with pytest.raises(AccountingReportRegistryError, match="no accounting report section provider"):
        registry.section_to_json_for(object())


def test_accounting_report_section_treats_none_and_empty_collections_as_empty() -> None:
    provider = _EarlySectionConnector()

    assert AccountingReportSection(key="none", order=1, section=None, _provider=provider).is_empty
    assert AccountingReportSection(key="list", order=1, section=[], _provider=provider).is_empty
    assert AccountingReportSection(key="dict", order=1, section={}, _provider=provider).is_empty
    assert not AccountingReportSection(key="object", order=1, section=object(), _provider=provider).is_empty


def test_accounting_report_section_prefers_section_is_empty_attribute() -> None:
    provider = _EarlySectionConnector()

    assert AccountingReportSection(
        key="explicit",
        order=1,
        section=SimpleNamespace(is_empty=True),
        _provider=provider,
    ).is_empty
    assert not AccountingReportSection(
        key="explicit",
        order=1,
        section=SimpleNamespace(is_empty=False),
        _provider=provider,
    ).is_empty


def test_discovered_accounting_report_provider_failure_does_not_block_healthy_provider(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from almanak.connectors import _strategy_accounting_report_registry as registration

    registry = AccountingReportRegistry()
    healthy_connector = _ReportConnector()

    class _BrokenFactory:
        def instantiate(self) -> AccountingReportConnector:
            raise RuntimeError("boom")

    class _HealthyFactory:
        def instantiate(self) -> AccountingReportConnector:
            return healthy_connector

    connector_registry = SimpleNamespace(
        with_accounting_report=lambda: (
            SimpleNamespace(name="broken", accounting_report=_BrokenFactory()),
            SimpleNamespace(name="healthy", accounting_report=_HealthyFactory()),
        )
    )
    monkeypatch.setattr(registration, "CONNECTOR_REGISTRY", connector_registry)
    monkeypatch.setattr(registration, "ACCOUNTING_REPORT_REGISTRY", registry)

    with caplog.at_level(logging.ERROR, logger=registration.__name__):
        registration._register_discovered_accounting_report_providers()

    assert registry.get("report") is healthy_connector
    assert "Failed to register accounting report provider for connector broken" in caplog.text


def test_pendle_accounting_report_provider_is_boot_registered() -> None:
    from almanak.connectors._strategy_accounting_report_registry import ACCOUNTING_REPORT_REGISTRY
    from almanak.framework.accounting.models import PendleEventType

    connector = ACCOUNTING_REPORT_REGISTRY.get("pendle")

    assert connector is not None
    assert connector.event_types == frozenset(event_type.value for event_type in PendleEventType)


def test_boot_registered_pendle_section_type_routes_through_singleton_registry() -> None:
    from almanak.connectors._strategy_accounting_report_registry import ACCOUNTING_REPORT_REGISTRY
    from almanak.connectors.pendle.reporting import PendleSection

    provider = ACCOUNTING_REPORT_REGISTRY.get("pendle")
    section = PendleSection()

    assert provider is not None
    assert ACCOUNTING_REPORT_REGISTRY.section_provider_for(section) is provider
    assert ACCOUNTING_REPORT_REGISTRY.render_section_text_for(section) == ""
    assert ACCOUNTING_REPORT_REGISTRY.section_to_json_for(section) == {"positions": []}
