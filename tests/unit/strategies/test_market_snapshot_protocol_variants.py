"""Tests for protocol-aware balance resolution (VIB-3138).

Background: Polymarket on Polygon settles in USDC.e, not native USDC. A
strategy calling ``market.balance("USDC")`` without protocol context gets
native USDC and the CLOB later rejects the order with "insufficient balance".

Fix: ``market.balance("USDC", protocol="polymarket")`` now returns the
USDC.e balance via the ``PROTOCOL_TOKEN_VARIANTS`` registry.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.data.market_snapshot import PROTOCOL_TOKEN_VARIANTS
from almanak.framework.strategies.intent_strategy import (
    MarketSnapshot,
    TokenBalance,
)


def _make_market(chain: str, balances: dict[str, TokenBalance]) -> MarketSnapshot:
    market = MarketSnapshot(chain=chain, wallet_address="0xtest")
    for symbol, balance in balances.items():
        market._balances[symbol] = balance
    return market


def _tb(symbol: str, qty: str, usd: str) -> TokenBalance:
    return TokenBalance(symbol=symbol, balance=Decimal(qty), balance_usd=Decimal(usd))


class TestPolymarketUSDCVariant:
    """The ticket's motivating case: Polymarket on Polygon needs USDC.e."""

    def test_balance_with_polymarket_protocol_returns_usdc_e(self):
        """``balance("USDC", protocol="polymarket")`` resolves to USDC.e."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "USDC.e"
        assert result.balance == Decimal("1.21")

    def test_balance_without_protocol_returns_symbol_as_given(self):
        """No protocol -> no translation. USDC stays USDC."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC")
        assert result.symbol == "USDC"
        assert result.balance == Decimal("2.00")

    def test_balance_usd_honors_protocol_kwarg(self):
        """``balance_usd()`` should also route through the protocol variant."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        assert market.balance_usd("USDC", protocol="polymarket") == Decimal("1.21")
        assert market.balance_usd("USDC") == Decimal("2.00")


class TestUnknownProtocolPassthrough:
    """Unknown protocols leave the symbol unchanged -- no silent error."""

    def test_unknown_protocol_passthrough(self):
        market = _make_market(
            chain="polygon",
            balances={"USDC": _tb("USDC", "100", "100")},
        )
        result = market.balance("USDC", protocol="gmx_v2")  # not in registry
        assert result.symbol == "USDC"

    def test_unknown_chain_passthrough(self):
        market = _make_market(
            chain="ethereum",  # polymarket isn't deployed on ethereum
            balances={"USDC": _tb("USDC", "100", "100")},
        )
        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "USDC"

    def test_unknown_symbol_passthrough(self):
        """Symbol not in the protocol's variant map -> unchanged."""
        market = _make_market(
            chain="polygon",
            balances={"WMATIC": _tb("WMATIC", "50", "10")},
        )
        # polymarket's registry only has USDC -> USDC.e; WMATIC is not there.
        result = market.balance("WMATIC", protocol="polymarket")
        assert result.symbol == "WMATIC"


class TestRegistryShape:
    """Pin the registry entry the ticket requires so it doesn't regress."""

    def test_registry_has_polygon_polymarket_usdc_mapping(self):
        assert "polygon" in PROTOCOL_TOKEN_VARIANTS
        assert "polymarket" in PROTOCOL_TOKEN_VARIANTS["polygon"]
        assert PROTOCOL_TOKEN_VARIANTS["polygon"]["polymarket"]["USDC"] == "USDC.e"


class TestCaseInsensitiveChainAndProtocol:
    """Callers may pass chain/protocol in any case."""

    def test_uppercase_protocol_still_resolves(self):
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="POLYMARKET")
        assert result.symbol == "USDC.e"

    def test_uppercase_chain_still_resolves(self):
        market = _make_market(
            chain="POLYGON",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "USDC.e"

    def test_lowercase_symbol_still_resolves(self):
        """Registry stores canonical uppercase keys; callers may pass lowercase."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "USDC.e": _tb("USDC.e", "1.21", "1.21"),
            },
        )
        result = market.balance("usdc", protocol="polymarket")
        assert result.symbol == "USDC.e"
