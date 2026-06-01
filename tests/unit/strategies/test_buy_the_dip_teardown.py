"""Teardown position discovery tests for BuyTheDipStrategy (VIB-4783).

Regression guard for the bug where ``get_open_positions`` reported the held
base-token position with a hardcoded ``value_usd=Decimal("0")`` instead of the
live on-chain balance. The fix mirrors the uniswap_rsi demo: query
``create_market_snapshot().balance(base_token)`` and gate the position on a
real holding (``balance > 0``).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from strategies.incubating.buy_the_dip.strategy import BuyTheDipStrategy


def _make_strategy() -> BuyTheDipStrategy:
    """Build a BTD strategy without running its gateway-coupled __init__."""
    with patch.object(BuyTheDipStrategy, "__init__", lambda self, *a, **k: None):
        strategy = BuyTheDipStrategy.__new__(BuyTheDipStrategy)

    strategy._chain = "arbitrum"
    strategy._deployment_id = "test-buy-the-dip"
    strategy.protocol = "uniswap_v3"
    strategy.base_token = "WETH"
    strategy.quote_token = "USDC"
    strategy._buy_count = 2
    strategy._sell_count = 1
    return strategy


def _market_with_balance(balance: Decimal, balance_usd: Decimal) -> MagicMock:
    market = MagicMock()
    market.balance.return_value = MagicMock(balance=balance, balance_usd=balance_usd)
    return market


def test_reports_live_balance_value() -> None:
    """A held base-token position reports the live USD value, not a hardcoded 0."""
    strategy = _make_strategy()
    market = _market_with_balance(Decimal("0.25"), Decimal("812.34"))

    with patch.object(strategy, "create_market_snapshot", return_value=market):
        summary = strategy.get_open_positions()

    assert len(summary.positions) == 1
    pos = summary.positions[0]
    assert pos.position_type.value == "TOKEN"
    assert pos.position_id == "buy_the_dip_base_holdings"
    # The headline regression: value must reflect the on-chain balance.
    assert pos.value_usd == Decimal("812.34")
    assert pos.value_usd != Decimal("0")
    # BTD-specific observability fields are preserved alongside the live read.
    assert pos.details["asset"] == "WETH"
    assert pos.details["quote_token"] == "USDC"
    assert pos.details["balance"] == "0.25"
    assert pos.details["buy_count"] == 2
    assert pos.details["sell_count"] == 1
    market.balance.assert_called_once_with("WETH")


def test_empty_when_no_balance() -> None:
    """A zero on-chain balance reports no positions (balance is authoritative)."""
    strategy = _make_strategy()
    market = _market_with_balance(Decimal("0"), Decimal("0"))

    with patch.object(strategy, "create_market_snapshot", return_value=market):
        summary = strategy.get_open_positions()

    assert summary.positions == []


def test_no_positions_when_balance_query_raises() -> None:
    """A failed balance query degrades to "no positions" rather than crashing."""
    strategy = _make_strategy()

    with patch.object(strategy, "create_market_snapshot", side_effect=ConnectionError("gateway down")):
        summary = strategy.get_open_positions()

    assert summary.positions == []
