"""Unit tests for the registry-driven dispatch in ``FundingRateServiceServicer``.

VIB-4811 / Phase 3 replaced the hardcoded ``if venue == "hyperliquid": ... elif
venue == "gmx_v2": ...`` ladder with a registry lookup populated from
``GATEWAY_REGISTRY.capability_providers(GatewayFundingRateCapability)``. The
review-fix commit then layered case normalization + duplicate-provider
rejection on top.

This module exercises the three observable behaviors that emerged from those
fixes (CodeRabbit follow-up on PR #2436):

1. Case-insensitive venue dispatch — ``venue()`` returning ``"HyperLiquid"``
   must still resolve a request for ``"hyperliquid"`` (and vice-versa).
2. Duplicate venue across two different connectors must raise ``RuntimeError``
   at servicer construction time, before any request is served.
3. Unknown venue must raise the expected ``ValueError("Unknown venue")``
   inside ``_fetch_rate`` and return the historical zero-fallback inside
   ``_get_default_rate``.

Each test builds its own ``GatewayConnectorRegistry`` and patches the
module-level ``GATEWAY_REGISTRY`` in ``funding_rate_service`` so the global
registry stays untouched.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingRateCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.gateway_registry import GatewayConnectorRegistry
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.gateway.services.funding_rate_service import (
    FundingRateServiceServicer,
)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(network="mainnet")


class _FundingConnector(GatewayConnector):
    """Minimal connector implementing ``GatewayFundingRateCapability``.

    Subclasses override ``_venue`` / ``_default_rate`` to vary behavior. The
    Protocol's ``venue()`` is a method (not an attribute) so case
    normalization can short-circuit on whatever case the connector returns.
    """

    protocol: ClassVar[ProtocolName] = ProtocolName("test_perp")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    _venue_name: ClassVar[str] = "test_perp"
    _default_rate_value: ClassVar[Decimal] = Decimal("0.00005")

    def venue(self) -> str:
        return self._venue_name

    def default_funding_rate(self, market: str) -> Decimal:
        return self._default_rate_value

    async def fetch_funding_rate(self, service: Any, market: str, chain: str) -> Any:
        raise NotImplementedError


def _install_registry(monkeypatch: pytest.MonkeyPatch, registry: GatewayConnectorRegistry) -> None:
    """Swap the module-level ``GATEWAY_REGISTRY`` for the duration of the test."""
    monkeypatch.setattr(
        "almanak.gateway.services.funding_rate_service.GATEWAY_REGISTRY",
        registry,
    )


# ---------------------------------------------------------------------------
# 1. Case-insensitive venue dispatch
# ---------------------------------------------------------------------------


def test_dispatch_is_case_insensitive_at_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connector reporting ``"HyperLiquid"`` is stored under ``"hyperliquid"``."""

    class _MixedCase(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("mixedcase_perp")
        _venue_name: ClassVar[str] = "HyperLiquid"

    registry = GatewayConnectorRegistry()
    registry.register(_MixedCase())
    _install_registry(monkeypatch, registry)

    svc = FundingRateServiceServicer(_settings())  # type: ignore[arg-type]

    assert "hyperliquid" in svc._funding_rate_providers
    assert "HyperLiquid" not in svc._funding_rate_providers


def test_dispatch_resolves_uppercased_venue_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_get_default_rate("HYPERLIQUID", ...)`` reaches a lowercase-registered provider."""

    class _Lower(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("lower_perp")
        _venue_name: ClassVar[str] = "hyperliquid"
        _default_rate_value: ClassVar[Decimal] = Decimal("0.00007")

    registry = GatewayConnectorRegistry()
    registry.register(_Lower())
    _install_registry(monkeypatch, registry)

    svc = FundingRateServiceServicer(_settings())  # type: ignore[arg-type]

    assert svc._get_default_rate("HYPERLIQUID", "ETH-USD") == Decimal("0.00007")
    assert svc._get_default_rate("hyperliquid", "ETH-USD") == Decimal("0.00007")


# ---------------------------------------------------------------------------
# 2. Duplicate-venue rejection at construction
# ---------------------------------------------------------------------------


def test_duplicate_venue_across_connectors_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two different connector classes reporting the same venue must fail loudly."""

    class _Alpha(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("alpha_perp")
        _venue_name: ClassVar[str] = "hyperliquid"

    class _Beta(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("beta_perp")
        _venue_name: ClassVar[str] = "hyperliquid"

    registry = GatewayConnectorRegistry()
    registry.register(_Alpha())
    registry.register(_Beta())
    _install_registry(monkeypatch, registry)

    with pytest.raises(RuntimeError, match="Duplicate funding-rate provider for venue 'hyperliquid'"):
        FundingRateServiceServicer(_settings())  # type: ignore[arg-type]


def test_duplicate_venue_collision_is_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``"GMX_V2"`` and ``"gmx_v2"`` collide after lowercase normalization."""

    class _Upper(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("upper_perp")
        _venue_name: ClassVar[str] = "GMX_V2"

    class _Lower(_FundingConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("lower_perp")
        _venue_name: ClassVar[str] = "gmx_v2"

    registry = GatewayConnectorRegistry()
    registry.register(_Upper())
    registry.register(_Lower())
    _install_registry(monkeypatch, registry)

    with pytest.raises(RuntimeError, match="Duplicate funding-rate provider for venue 'gmx_v2'"):
        FundingRateServiceServicer(_settings())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 3. Unknown-venue paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_rate_unknown_venue_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_fetch_rate`` raises ``ValueError("Unknown venue")`` for unregistered venues."""
    registry = GatewayConnectorRegistry()
    # No provider registered for "polymarket"; service constructs cleanly.
    _install_registry(monkeypatch, registry)

    svc = FundingRateServiceServicer(_settings())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Unknown venue"):
        await svc._fetch_rate("polymarket", "ETH-USD", "ethereum")


def test_get_default_rate_unknown_venue_returns_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical contract: unknown ``(venue, market)`` yields ``Decimal("0.00001")``."""
    registry = GatewayConnectorRegistry()
    _install_registry(monkeypatch, registry)

    svc = FundingRateServiceServicer(_settings())  # type: ignore[arg-type]

    assert svc._get_default_rate("unknown_venue", "ETH-USD") == Decimal("0.00001")


# ---------------------------------------------------------------------------
# Capability runtime-check sanity — defends against silent dispatch failures
# ---------------------------------------------------------------------------


def test_funding_connector_satisfies_capability_protocol() -> None:
    """``isinstance(connector, GatewayFundingRateCapability)`` must hold.

    If the Protocol drops ``@runtime_checkable`` or grows a new method, the
    registry's ``capability_providers`` walk silently stops returning the
    connector and the service boots with an empty provider table. This
    sanity check fails the build instead.
    """
    assert isinstance(_FundingConnector(), GatewayFundingRateCapability)
