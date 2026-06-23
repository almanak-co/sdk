"""Tests for the strategy-side principal-token market reader registry."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.principal_token_market_reader_registry import (
    PrincipalTokenMarketReadCapability,
    PrincipalTokenMarketReadConnector,
    PrincipalTokenMarketReader,
    PrincipalTokenMarketReadRegistry,
    PrincipalTokenMarketReadRegistryError,
)


class _Reader:
    def get_pt_to_asset_rate(self, market_address: str) -> Decimal:
        return Decimal("0.98")

    def get_implied_apy(self, market_address: str) -> Decimal:
        return Decimal("0.10")

    def is_market_expired(self, market_address: str) -> bool:
        return False

    def get_market_expiry_ts(self, market_address: str) -> int | None:
        return 1_782_777_600

    def get_days_to_maturity(self, market_address: str) -> int | None:
        return 30

    def get_market_tokens(self, market_address: str) -> dict[str, str]:
        return {"sy": "0xsy", "pt": "0xpt", "yt": "0xyt"}

    def estimate_pt_output(self, market_address: str, amount_in: int) -> int:
        return amount_in


class _MarketReadConnector(PrincipalTokenMarketReadConnector, PrincipalTokenMarketReadCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("pt_market")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def build_reader(
        self,
        *,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        cache_ttl_seconds: float = 30.0,
    ) -> PrincipalTokenMarketReader:
        return _Reader()


class _NoCapabilityConnector(PrincipalTokenMarketReadConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("none")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING


class _SecondMarketReadConnector(_MarketReadConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("second_pt_market")


class _ConflictingMarketReadConnector(_MarketReadConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("pt_market")


def test_register_rejects_classes() -> None:
    """Registry stores connector instances, not classes."""
    registry = PrincipalTokenMarketReadRegistry()

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="did you forget to instantiate"):
        registry.register(_MarketReadConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    """A principal-token reader connector must implement the reader capability."""
    registry = PrincipalTokenMarketReadRegistry()

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="PrincipalTokenMarketReadCapability"):
        registry.register(_NoCapabilityConnector())


def test_register_is_idempotent_for_same_connector_type() -> None:
    """Module reloads can safely re-register the same connector type."""
    registry = PrincipalTokenMarketReadRegistry()
    registry.register(_MarketReadConnector())
    registry.register(_MarketReadConnector())

    assert tuple(type(connector) for connector in registry.all()) == (_MarketReadConnector,)


def test_register_rejects_conflicting_protocol_implementations() -> None:
    """Protocol collisions from different connector types are hard errors."""
    registry = PrincipalTokenMarketReadRegistry()
    registry.register(_MarketReadConnector())

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="already registered"):
        registry.register(_ConflictingMarketReadConnector())


def test_lookup_returns_capability() -> None:
    """Lookup returns the principal-token reader capability view."""
    registry = PrincipalTokenMarketReadRegistry()
    connector = _MarketReadConnector()
    registry.register(connector)

    assert registry.lookup("pt_market") is connector
    assert registry.lookup("missing") is None


def test_build_reader_uses_registered_connector() -> None:
    """The registry builds readers through the connector-owned capability."""
    registry = PrincipalTokenMarketReadRegistry()
    registry.register(_MarketReadConnector())

    reader = registry.build_reader("pt_market", chain="ethereum", gateway_client=object())

    assert isinstance(reader, PrincipalTokenMarketReader)
    assert reader.get_pt_to_asset_rate("0xmarket") == Decimal("0.98")


def test_build_default_reader_uses_single_registered_connector() -> None:
    """The registry can build the sole reader without a framework protocol literal."""
    registry = PrincipalTokenMarketReadRegistry()
    registry.register(_MarketReadConnector())

    reader = registry.build_default_reader(chain="ethereum", gateway_client=object())

    assert isinstance(reader, PrincipalTokenMarketReader)
    assert reader.get_implied_apy("0xmarket") == Decimal("0.10")


def test_build_default_reader_rejects_missing_reader() -> None:
    """The default reader path fails loudly when no connector is registered."""
    registry = PrincipalTokenMarketReadRegistry()

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="no principal-token"):
        registry.build_default_reader(chain="ethereum")


def test_build_default_reader_rejects_ambiguous_readers() -> None:
    """The default reader path cannot choose between multiple protocols."""
    registry = PrincipalTokenMarketReadRegistry()
    registry.register(_MarketReadConnector())
    registry.register(_SecondMarketReadConnector())

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="multiple principal-token"):
        registry.build_default_reader(chain="ethereum")


def test_build_reader_rejects_missing_protocol() -> None:
    """Missing protocols fail with a registry error."""
    registry = PrincipalTokenMarketReadRegistry()

    with pytest.raises(PrincipalTokenMarketReadRegistryError, match="does not publish"):
        registry.build_reader("missing", chain="ethereum")


def test_with_capability_filters_by_protocol_class() -> None:
    """Capability filtering returns registered connectors matching the protocol."""
    registry = PrincipalTokenMarketReadRegistry()
    connector = _MarketReadConnector()
    registry.register(connector)

    assert registry.with_capability(PrincipalTokenMarketReadCapability) == (connector,)
