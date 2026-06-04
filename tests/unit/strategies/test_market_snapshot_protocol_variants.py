"""Tests for protocol-aware balance resolution (VIB-3138).

Background: Polymarket V2 (April 2026 cutover) settles trades in PUSD, the
in-system collateral minted from USDC.e (or native USDC) via the
CollateralOnramp. A strategy calling ``market.balance("USDC")`` without
protocol context gets native USDC and the CLOB later rejects the order with
"insufficient balance".

Fix: ``market.balance("USDC", protocol="polymarket")`` returns the PUSD
balance — the spendable trading collateral. As of VIB-4989 the variant
mapping lives on the connector as a ``settlement_token_variants`` capability
(read via ``CapabilitiesRegistry``), not the former framework
``PROTOCOL_TOKEN_VARIANTS`` dict.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities
from almanak.framework.market import MarketSnapshot, TokenBalance


def _make_market(chain: str, balances: dict[str, TokenBalance]) -> MarketSnapshot:
    market = MarketSnapshot(chain=chain, wallet_address="0xtest")
    for symbol, balance in balances.items():
        market._balances[symbol] = balance
    return market


def _tb(symbol: str, qty: str, usd: str) -> TokenBalance:
    return TokenBalance(symbol=symbol, balance=Decimal(qty), balance_usd=Decimal(usd))


class TestPolymarketUSDCVariant:
    """The ticket's motivating case: Polymarket V2 on Polygon needs PUSD."""

    def test_balance_with_polymarket_protocol_returns_pusd(self):
        """``balance("USDC", protocol="polymarket")`` resolves to PUSD."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "PUSD": _tb("PUSD", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "PUSD"
        assert result.balance == Decimal("1.21")

    def test_balance_without_protocol_returns_symbol_as_given(self):
        """No protocol -> no translation. USDC stays USDC."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "PUSD": _tb("PUSD", "1.21", "1.21"),
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
                "PUSD": _tb("PUSD", "1.21", "1.21"),
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
        # polymarket's registry only has USDC -> PUSD; WMATIC is not there.
        result = market.balance("WMATIC", protocol="polymarket")
        assert result.symbol == "WMATIC"


class TestRegistryShape:
    """Pin the capability entry the ticket requires so it doesn't regress.

    VIB-4989: the mapping moved from the framework ``PROTOCOL_TOKEN_VARIANTS``
    dict onto the polymarket connector's ``settlement_token_variants``
    capability; assert the connector source of truth.
    """

    def test_registry_has_polygon_polymarket_usdc_mapping(self):
        variants = get_protocol_capabilities("polymarket").get("settlement_token_variants", {})
        assert "polygon" in variants
        assert variants["polygon"]["USDC"] == "PUSD"


class TestCaseInsensitiveChainAndProtocol:
    """Callers may pass chain/protocol in any case."""

    def test_uppercase_protocol_still_resolves(self):
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "PUSD": _tb("PUSD", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="POLYMARKET")
        assert result.symbol == "PUSD"

    def test_uppercase_chain_still_resolves(self):
        market = _make_market(
            chain="POLYGON",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "PUSD": _tb("PUSD", "1.21", "1.21"),
            },
        )
        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "PUSD"

    def test_lowercase_symbol_still_resolves(self):
        """Registry stores canonical uppercase keys; callers may pass lowercase."""
        market = _make_market(
            chain="polygon",
            balances={
                "USDC": _tb("USDC", "2.00", "2.00"),
                "PUSD": _tb("PUSD", "1.21", "1.21"),
            },
        )
        result = market.balance("usdc", protocol="polymarket")
        assert result.symbol == "PUSD"
