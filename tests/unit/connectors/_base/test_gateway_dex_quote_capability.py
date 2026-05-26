"""``GatewayDexQuoteCapability`` contract tests (VIB-4811 / Phase 3).

``MultiDexPriceService`` now resolves the per-DEX quote function via
``GATEWAY_REGISTRY.capability_providers(GatewayDexQuoteCapability)``
instead of branching on ``if dex == "uniswap_v3" ... elif dex ==
"curve" ...``. Tests pin:

* ``isinstance(connector, GatewayDexQuoteCapability)`` is True iff
  the connector defines ``dex_name``, ``supported_chains``, and
  ``quote``.
* The registered ``uniswap_v3``, ``curve``, and ``enso`` connectors
  return their expected names + chain lists.
* The derived ``DEX_CHAINS`` matches the legacy hardcoded table.
* The derived ``SUPPORTED_DEXS`` matches the legacy list.
* The servicer's dispatch routes each DEX string to the matching
  ``_get_*_quote`` helper on the service.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar
from unittest.mock import AsyncMock

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexQuoteCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _DexQuoteImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("dex_quote_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def dex_name(self) -> str:
        return "demo_dex"

    def supported_chains(self) -> frozenset[str]:
        return frozenset({"ethereum"})

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any:
        return ("quoted", token_in, token_out, amount_in)


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_dex_quote_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP


def test_dex_quote_capability_runtime_isinstance() -> None:
    assert isinstance(_DexQuoteImpl(), GatewayDexQuoteCapability)
    assert not isinstance(_BareConnector(), GatewayDexQuoteCapability)


def test_registered_dex_quote_providers() -> None:
    """Uniswap V3, Curve, and Enso each expose ``GatewayDexQuoteCapability``."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayDexQuoteCapability)
    names = {p.dex_name() for p in providers}
    assert {"uniswap_v3", "curve", "enso"}.issubset(names)


def test_uniswap_v3_supported_chains_match_legacy() -> None:
    """Uniswap V3 connector supports Ethereum, Arbitrum, Optimism, Polygon, Base."""
    from almanak.connectors.uniswap_v3.gateway.provider import (
        UniswapV3GatewayConnector,
    )

    assert UniswapV3GatewayConnector().supported_chains() == frozenset(
        {"ethereum", "arbitrum", "optimism", "polygon", "base"}
    )


def test_curve_supported_chains_match_legacy() -> None:
    """Curve connector supports Ethereum and Arbitrum."""
    from almanak.connectors.curve.gateway.provider import CurveGatewayConnector

    assert CurveGatewayConnector().supported_chains() == frozenset({"ethereum", "arbitrum"})


def test_enso_supported_chains_match_legacy() -> None:
    """Enso connector supports Ethereum, Arbitrum, Optimism, Polygon, Base."""
    from almanak.connectors.enso.gateway.provider import EnsoGatewayConnector

    assert EnsoGatewayConnector().supported_chains() == frozenset(
        {"ethereum", "arbitrum", "optimism", "polygon", "base"}
    )


def test_dex_chains_match_legacy() -> None:
    """``DEX_CHAINS`` matches the pre-refactor hardcoded dispatch table."""
    from almanak.gateway.data.price.multi_dex import DEX_CHAINS

    legacy = {
        "ethereum": {"uniswap_v3", "curve", "enso"},
        "arbitrum": {"uniswap_v3", "curve", "enso"},
        "optimism": {"uniswap_v3", "enso"},
        "polygon": {"uniswap_v3", "enso"},
        "base": {"uniswap_v3", "enso"},
    }
    for chain, expected_dexs in legacy.items():
        actual = set(DEX_CHAINS.get(chain, []))
        assert actual == expected_dexs, (chain, actual, expected_dexs)


def test_supported_dexs_match_legacy() -> None:
    """``SUPPORTED_DEXS`` contains the historical three DEXs."""
    from almanak.gateway.data.price.multi_dex import SUPPORTED_DEXS

    assert set(SUPPORTED_DEXS) == {"uniswap_v3", "curve", "enso"}


def test_dex_enum_members_match_legacy() -> None:
    """The lazily-built ``Dex`` enum exposes UNISWAP_V3 / CURVE / ENSO."""
    from almanak.gateway.data.price.multi_dex import Dex

    # Trigger build + member access.
    assert Dex.UNISWAP_V3 == "uniswap_v3"
    assert Dex.CURVE == "curve"
    assert Dex.ENSO == "enso"


@pytest.mark.asyncio
async def test_capability_dispatch_routes_to_correct_helper() -> None:
    """``MultiDexPriceService.get_quote`` dispatches through the registry.

    Mocks ``_get_*_quote`` and confirms each DEX string routes to the
    matching helper on the service via the registered connector's
    ``quote`` method.
    """
    from almanak.gateway.data.price.multi_dex import DexQuote, MultiDexPriceService

    service = MultiDexPriceService(chain="ethereum")

    def _mk_quote(dex: str) -> DexQuote:
        return DexQuote(
            dex=dex,
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            amount_out=Decimal("0.04"),
            price=Decimal("0.0004"),
            chain="ethereum",
        )

    service._get_uniswap_v3_quote = AsyncMock(return_value=_mk_quote("uniswap_v3"))  # type: ignore[method-assign]
    service._get_curve_quote = AsyncMock(return_value=_mk_quote("curve"))  # type: ignore[method-assign]
    service._get_enso_quote = AsyncMock(return_value=_mk_quote("enso"))  # type: ignore[method-assign]

    uni_quote = await service.get_quote("uniswap_v3", "USDC", "WETH", Decimal("100"))
    assert uni_quote.dex == "uniswap_v3"
    service._get_uniswap_v3_quote.assert_awaited_once_with("USDC", "WETH", Decimal("100"))

    curve_quote = await service.get_quote("curve", "USDC", "WETH", Decimal("100"))
    assert curve_quote.dex == "curve"
    service._get_curve_quote.assert_awaited_once_with("USDC", "WETH", Decimal("100"))

    enso_quote = await service.get_quote("enso", "USDC", "WETH", Decimal("100"))
    assert enso_quote.dex == "enso"
    service._get_enso_quote.assert_awaited_once_with("USDC", "WETH", Decimal("100"))
