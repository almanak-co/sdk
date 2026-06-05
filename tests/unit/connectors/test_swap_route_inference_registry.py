"""Tests for connector-owned swap route inference."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from almanak import SwapIntent
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.swap_route_inference_registry import (
    SwapRouteInferenceCapability,
    SwapRouteInferenceConnector,
    SwapRouteInferenceRegistry,
    SwapRouteInferenceRegistryError,
)
from almanak.connectors.pendle.swap_route_inference import PendleSwapRouteInferenceConnector


class _RouteConnector(SwapRouteInferenceConnector, SwapRouteInferenceCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("route")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def claims_swap_route(self, intent: Any) -> bool:
        return getattr(intent, "claim", False) is True


class _SecondRouteConnector(_RouteConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("second_route")


class _ConflictingRouteConnector(_RouteConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("route")


class _NoCapabilityConnector(SwapRouteInferenceConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("none")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP


def test_register_rejects_classes() -> None:
    registry = SwapRouteInferenceRegistry()

    with pytest.raises(SwapRouteInferenceRegistryError, match="did you forget to instantiate"):
        registry.register(_RouteConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    registry = SwapRouteInferenceRegistry()

    with pytest.raises(SwapRouteInferenceRegistryError, match="SwapRouteInferenceCapability"):
        registry.register(_NoCapabilityConnector())


def test_register_is_idempotent_for_same_connector_type() -> None:
    registry = SwapRouteInferenceRegistry()
    registry.register(_RouteConnector())
    registry.register(_RouteConnector())

    assert tuple(type(connector) for connector in registry.all()) == (_RouteConnector,)


def test_register_rejects_conflicting_protocol_implementations() -> None:
    registry = SwapRouteInferenceRegistry()
    registry.register(_RouteConnector())

    with pytest.raises(SwapRouteInferenceRegistryError, match="already registered"):
        registry.register(_ConflictingRouteConnector())


def test_infer_protocol_returns_sole_claiming_connector() -> None:
    registry = SwapRouteInferenceRegistry()
    registry.register(_RouteConnector())

    assert registry.infer_protocol(SimpleNamespace(claim=True)) == "route"
    assert registry.infer_protocol(SimpleNamespace(claim=False)) is None


def test_infer_protocol_rejects_ambiguous_claims() -> None:
    registry = SwapRouteInferenceRegistry()
    registry.register(_RouteConnector())
    registry.register(_SecondRouteConnector())

    with pytest.raises(SwapRouteInferenceRegistryError, match="multiple connectors claim"):
        registry.infer_protocol(SimpleNamespace(claim=True))


@pytest.mark.parametrize(
    ("from_token", "to_token", "expected"),
    [
        ("USDC", "PT-wstETH-25JUN2026", True),
        ("YT-sUSDe-13AUG2026", "sUSDe", True),
        ("USDC", "WETH", False),
    ],
)
def test_pendle_claims_pt_and_yt_swap_symbols(from_token: str, to_token: str, expected: bool) -> None:
    connector = PendleSwapRouteInferenceConnector()

    intent = SimpleNamespace(from_token=from_token, to_token=to_token)

    assert connector.claims_swap_route(intent) is expected


def test_runtime_registry_infers_pendle_for_pt_and_yt_swaps() -> None:
    """The real boot registry keeps protocol-less Pendle swaps off default DEX routing."""
    from almanak.connectors._strategy_swap_route_inference_registry import SWAP_ROUTE_INFERENCE_REGISTRY

    pt_intent = SwapIntent(from_token="USDC", to_token="PT-wstETH-25JUN2026", amount_usd=100)
    yt_intent = SwapIntent(from_token="yt-sUSDe-13AUG2026", to_token="sUSDe", amount_usd=100)
    vanilla_intent = SwapIntent(from_token="USDC", to_token="WETH", amount_usd=100)

    assert SWAP_ROUTE_INFERENCE_REGISTRY.infer_protocol(pt_intent) == "pendle"
    assert SWAP_ROUTE_INFERENCE_REGISTRY.infer_protocol(yt_intent) == "pendle"
    assert SWAP_ROUTE_INFERENCE_REGISTRY.infer_protocol(vanilla_intent) is None
