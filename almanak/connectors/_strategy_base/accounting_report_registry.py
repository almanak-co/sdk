"""Strategy-side accounting reporting provider registry.

Connectors can publish reporting providers for connector-specific accounting
events without making the framework reporting loader import concrete connector
modules. Providers claim event types for payload deserialization and may also
publish report sections that central CLI/report assembly discovers generically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

__all__ = [
    "ACCOUNTING_REPORT_REGISTRY",
    "AccountingReportCapability",
    "AccountingReportConnector",
    "AccountingReportRegistry",
    "AccountingReportRegistryError",
    "AccountingReportSection",
    "AccountingReportSectionCapability",
]


class AccountingReportRegistryError(Exception):
    """Registry contract violation."""


@runtime_checkable
class AccountingReportCapability(Protocol):
    """Connector-owned accounting report event deserializer."""

    def deserialize_event(self, identity: Any, payload_json: str) -> Any: ...


@runtime_checkable
class AccountingReportSectionCapability(Protocol):
    """Connector-owned accounting report section builder and renderer."""

    section_key: ClassVar[str]
    section_order: ClassVar[int]

    def build_section(self, data: Any) -> Any: ...

    def render_text(self, section: Any, data: Any) -> str: ...

    def to_json(self, section: Any) -> dict[str, Any]: ...


class AccountingReportConnector:
    """Base class for strategy-side connector accounting report providers."""

    key: ClassVar[str]
    strategy_class: ClassVar[str]
    event_types: ClassVar[frozenset[str]]


@dataclass(frozen=True)
class AccountingReportSection:
    """Built connector-owned accounting report section."""

    key: str
    order: int
    section: Any
    _provider: AccountingReportSectionCapability = field(repr=False)

    @property
    def is_empty(self) -> bool:
        if self.section is None:
            return True
        section_empty = getattr(self.section, "is_empty", None)
        if section_empty is not None:
            return bool(section_empty)
        try:
            return len(self.section) == 0
        except TypeError:
            return False

    def render_text(self, data: Any) -> str:
        return self._provider.render_text(self.section, data)

    def to_json(self) -> dict[str, Any]:
        return self._provider.to_json(self.section)


T = TypeVar("T")


class AccountingReportRegistry:
    """In-process registry of connector-owned accounting report providers."""

    def __init__(self) -> None:
        self._connectors: dict[str, AccountingReportConnector] = {}
        self._event_type_index: dict[str, AccountingReportConnector] = {}
        self._section_key_index: dict[str, AccountingReportConnector] = {}

    def get(self, key: str) -> AccountingReportConnector | None:
        """Return the registered provider for ``key``, if any."""
        return self._connectors.get(key)

    def register(self, connector: AccountingReportConnector) -> None:
        """Register a connector report provider instance."""
        if not isinstance(connector, AccountingReportConnector):
            raise AccountingReportRegistryError(
                "register() expects an AccountingReportConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, AccountingReportCapability):
            raise AccountingReportRegistryError(
                "register() expects a connector implementing AccountingReportCapability "
                f"in addition to AccountingReportConnector; {type(connector).__qualname__!s} "
                "is missing deserialize_event()."
            )

        key, event_types = self._validate_connector_metadata(connector)
        self._validate_event_type_claims(key, event_types)
        if isinstance(connector, AccountingReportSectionCapability):
            self._validate_section_claim(key, connector)

        self._connectors[key] = connector
        for event_type in event_types:
            self._event_type_index[event_type] = connector
        if isinstance(connector, AccountingReportSectionCapability):
            self._section_key_index[connector.section_key] = connector

    def _validate_connector_metadata(self, connector: AccountingReportConnector) -> tuple[str, frozenset[str]]:
        """Validate provider identity and event metadata."""
        key = connector.key
        if not isinstance(key, str) or not key.strip() or key != key.strip():
            raise AccountingReportRegistryError(f"connector key must be an unpadded non-empty string, got {key!r}")
        strategy_class = connector.strategy_class
        if (
            not isinstance(strategy_class, str)
            or not strategy_class.strip()
            or strategy_class != strategy_class.strip()
        ):
            raise AccountingReportRegistryError(
                f"connector strategy_class must be an unpadded non-empty string, got {strategy_class!r}"
            )
        event_types = connector.event_types
        if not isinstance(event_types, frozenset) or not event_types:
            raise AccountingReportRegistryError(
                f"connector event_types must be a non-empty frozenset[str], got {event_types!r}"
            )
        bad_event_types = [
            event_type
            for event_type in event_types
            if not isinstance(event_type, str) or not event_type.strip() or event_type != event_type.strip()
        ]
        if bad_event_types:
            raise AccountingReportRegistryError(
                f"connector event_types must contain unpadded non-empty strings, got {bad_event_types!r}"
            )

        existing = self._connectors.get(key)
        if existing is not None:
            raise AccountingReportRegistryError(
                f"accounting report provider {key!r} already registered by "
                f"{type(existing).__qualname__}; refusing {type(connector).__qualname__}"
            )
        return key, frozenset(event_type.upper() for event_type in event_types)

    def _validate_event_type_claims(self, key: str, event_types: frozenset[str]) -> None:
        """Validate that no existing provider owns these event types."""
        from almanak.framework.accounting.models import LendingEventType

        reserved_event_types = frozenset(event_type.value for event_type in LendingEventType)
        for event_type in event_types:
            if event_type in reserved_event_types:
                raise AccountingReportRegistryError(
                    f"accounting event_type {event_type!r} is reserved for built-in lending reporting; refusing {key!r}"
                )
            owner = self._event_type_index.get(event_type)
            if owner is not None:
                raise AccountingReportRegistryError(
                    f"accounting event_type {event_type!r} already claimed by {owner.key!r}; refusing {key!r}"
                )

    def _validate_section_claim(self, key: str, connector: AccountingReportSectionCapability) -> None:
        """Validate a provider's optional report-section claim."""
        section_key = connector.section_key
        if not isinstance(section_key, str) or not section_key.strip() or section_key != section_key.strip():
            raise AccountingReportRegistryError(
                f"connector section_key must be an unpadded non-empty string, got {section_key!r}"
            )
        section_order = connector.section_order
        if not isinstance(section_order, int):
            raise AccountingReportRegistryError(f"connector section_order must be an int, got {section_order!r}")
        section_owner = self._section_key_index.get(section_key)
        if section_owner is not None:
            raise AccountingReportRegistryError(
                f"accounting report section {section_key!r} already claimed by {section_owner.key!r}; refusing {key!r}"
            )

    def deserialize_event(self, event_type: str, identity: Any, payload_json: str) -> tuple[str, Any] | None:
        """Deserialize one row through the owning connector, if any."""
        connector = self._event_type_index.get(event_type.upper())
        if connector is None:
            return None
        if not isinstance(connector, AccountingReportCapability):
            raise AccountingReportRegistryError(
                f"accounting report provider {connector.key!r} no longer implements deserialize_event()."
            )
        return connector.key, connector.deserialize_event(identity, payload_json)

    def strategy_class_for_event_type(self, event_type: str) -> str | None:
        """Return the owning connector's strategy-class label for ``event_type``."""
        connector = self._event_type_index.get(event_type.upper())
        return connector.strategy_class if connector is not None else None

    def all(self) -> tuple[AccountingReportConnector, ...]:
        """Return every registered provider in registration order."""
        return tuple(self._connectors.values())

    def section_providers(self) -> tuple[AccountingReportSectionCapability, ...]:
        """Return registered providers that can build report sections."""
        providers = [c for c in self._connectors.values() if isinstance(c, AccountingReportSectionCapability)]
        return tuple(sorted(providers, key=lambda provider: provider.section_order))

    def build_sections(self, data: Any) -> tuple[AccountingReportSection, ...]:
        """Build every connector-owned accounting report section."""
        sections: list[AccountingReportSection] = []
        for provider in self.section_providers():
            sections.append(
                AccountingReportSection(
                    key=provider.section_key,
                    order=provider.section_order,
                    section=provider.build_section(data),
                    _provider=provider,
                )
            )
        return tuple(sections)

    def event_types(self) -> frozenset[str]:
        """Return every connector-owned accounting event type."""
        return frozenset(self._event_type_index)

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()
        self._event_type_index.clear()
        self._section_key_index.clear()


ACCOUNTING_REPORT_REGISTRY: AccountingReportRegistry = AccountingReportRegistry()
