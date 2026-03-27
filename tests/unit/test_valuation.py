"""Tests for the portfolio valuation module.

Covers:
- spot_valuer: pure math (value_tokens, total_value)
- portfolio_valuer: orchestration, confidence levels, failure contract
- TokenBalance.price_usd serialization round-trip
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from almanak.framework.valuation.spot_valuer import total_value, value_tokens


# ---------------------------------------------------------------------------
# spot_valuer tests
# ---------------------------------------------------------------------------


class TestValueTokens:
    """Pure math: balance * price."""

    def test_basic_valuation(self):
        balances = {"ETH": Decimal("1.5"), "USDC": Decimal("1000")}
        prices = {"ETH": Decimal("3500"), "USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 2
        eth = next(t for t in result if t.symbol == "ETH")
        usdc = next(t for t in result if t.symbol == "USDC")

        assert eth.value_usd == Decimal("5250")
        assert eth.price_usd == Decimal("3500")
        assert eth.balance == Decimal("1.5")

        assert usdc.value_usd == Decimal("1000")
        assert usdc.price_usd == Decimal("1")

    def test_zero_balance_excluded(self):
        balances = {"ETH": Decimal("0"), "USDC": Decimal("500")}
        prices = {"ETH": Decimal("3500"), "USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 1
        assert result[0].symbol == "USDC"

    def test_negative_balance_excluded(self):
        balances = {"ETH": Decimal("-1"), "USDC": Decimal("500")}
        prices = {"ETH": Decimal("3500"), "USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 1
        assert result[0].symbol == "USDC"

    def test_missing_price_excluded(self):
        balances = {"ETH": Decimal("1"), "USDC": Decimal("500")}
        prices = {"USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 1
        assert result[0].symbol == "USDC"

    def test_zero_price_excluded(self):
        """Zero price should not produce $0 valuations -- exclude the token."""
        balances = {"ETH": Decimal("1"), "USDC": Decimal("500")}
        prices = {"ETH": Decimal("0"), "USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 1
        assert result[0].symbol == "USDC"

    def test_negative_price_excluded(self):
        """Negative price (oracle corruption) should be excluded."""
        balances = {"ETH": Decimal("1"), "USDC": Decimal("500")}
        prices = {"ETH": Decimal("-3500"), "USDC": Decimal("1")}
        result = value_tokens(balances, prices)

        assert len(result) == 1
        assert result[0].symbol == "USDC"

    def test_empty_balances(self):
        result = value_tokens({}, {"ETH": Decimal("3500")})
        assert result == []

    def test_empty_prices(self):
        result = value_tokens({"ETH": Decimal("1")}, {})
        assert result == []

    def test_addresses_populated(self):
        balances = {"ETH": Decimal("1")}
        prices = {"ETH": Decimal("3500")}
        addresses = {"ETH": "0xabc"}
        result = value_tokens(balances, prices, addresses)

        assert result[0].address == "0xabc"

    def test_addresses_default_empty(self):
        balances = {"ETH": Decimal("1")}
        prices = {"ETH": Decimal("3500")}
        result = value_tokens(balances, prices)

        assert result[0].address == ""


class TestTotalValue:
    def test_sums_values(self):
        tokens = [
            TokenBalance(symbol="ETH", balance=Decimal("1"), value_usd=Decimal("3500")),
            TokenBalance(symbol="USDC", balance=Decimal("1000"), value_usd=Decimal("1000")),
        ]
        assert total_value(tokens) == Decimal("4500")

    def test_empty_list(self):
        assert total_value([]) == Decimal("0")


# ---------------------------------------------------------------------------
# TokenBalance price_usd tests
# ---------------------------------------------------------------------------


class TestTokenBalancePriceUsd:
    def test_price_usd_stored(self):
        tb = TokenBalance(
            symbol="ETH",
            balance=Decimal("1"),
            value_usd=Decimal("3500"),
            price_usd=Decimal("3500"),
        )
        assert tb.price_usd == Decimal("3500")

    def test_price_usd_defaults_none(self):
        tb = TokenBalance(symbol="ETH", balance=Decimal("1"), value_usd=Decimal("3500"))
        assert tb.price_usd is None

    def test_price_usd_coercion_from_float(self):
        tb = TokenBalance(
            symbol="ETH",
            balance=Decimal("1"),
            value_usd=Decimal("3500"),
            price_usd=3500.0,
        )
        assert tb.price_usd == Decimal("3500.0")
        assert isinstance(tb.price_usd, Decimal)

    def test_price_usd_coercion_from_string(self):
        tb = TokenBalance(
            symbol="ETH",
            balance=Decimal("1"),
            value_usd=Decimal("3500"),
            price_usd="3500",
        )
        assert isinstance(tb.price_usd, Decimal)

    def test_snapshot_serialization_roundtrip_with_price_usd(self):
        """price_usd survives to_dict/from_dict."""
        snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 3, 27, 12, 0, 0, tzinfo=UTC),
            strategy_id="test-strat",
            total_value_usd=Decimal("5000"),
            available_cash_usd=Decimal("5000"),
            value_confidence=ValueConfidence.HIGH,
            wallet_balances=[
                TokenBalance(
                    symbol="ETH",
                    balance=Decimal("1.5"),
                    value_usd=Decimal("5250"),
                    price_usd=Decimal("3500"),
                    address="0xabc",
                ),
            ],
        )
        data = snapshot.to_dict()
        assert data["wallet_balances"][0]["price_usd"] == "3500"
        assert data["wallet_balances"][0]["address"] == "0xabc"

        restored = PortfolioSnapshot.from_dict(data)
        assert restored.wallet_balances[0].price_usd == Decimal("3500")
        assert restored.wallet_balances[0].address == "0xabc"

    def test_snapshot_serialization_null_price_usd(self):
        """Null price_usd round-trips correctly."""
        snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 3, 27, 12, 0, 0, tzinfo=UTC),
            strategy_id="test-strat",
            total_value_usd=Decimal("1000"),
            available_cash_usd=Decimal("1000"),
            wallet_balances=[
                TokenBalance(symbol="USDC", balance=Decimal("1000"), value_usd=Decimal("1000")),
            ],
        )
        data = snapshot.to_dict()
        assert data["wallet_balances"][0]["price_usd"] is None

        restored = PortfolioSnapshot.from_dict(data)
        assert restored.wallet_balances[0].price_usd is None


# ---------------------------------------------------------------------------
# PortfolioValuer tests
# ---------------------------------------------------------------------------


def _make_strategy(
    strategy_id="test-strat",
    chain="arbitrum",
    tracked_tokens=None,
    positions=None,
):
    """Create a mock strategy with the StrategyLike protocol."""
    strategy = MagicMock()
    type(strategy).strategy_id = PropertyMock(return_value=strategy_id)
    type(strategy).chain = PropertyMock(return_value=chain)
    strategy._get_tracked_tokens.return_value = tracked_tokens if tracked_tokens is not None else ["ETH", "USDC"]

    if positions is not None:
        strategy.get_open_positions.return_value = TeardownPositionSummary(
            strategy_id=strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )
    elif hasattr(strategy, "get_open_positions"):
        strategy.get_open_positions.return_value = TeardownPositionSummary(
            strategy_id=strategy_id,
            timestamp=datetime.now(UTC),
            positions=[],
        )

    return strategy


def _make_market(prices=None, balances=None):
    """Create a mock MarketDataSource."""
    market = MagicMock()
    _prices = prices or {}
    _balances = balances or {}

    def mock_price(token, quote="USD"):
        if token in _prices:
            return _prices[token]
        raise ValueError(f"No price for {token}")

    def mock_balance(token):
        if token in _balances:
            result = MagicMock()
            result.balance = _balances[token]
            return result
        raise ValueError(f"No balance for {token}")

    market.price = mock_price
    market.balance = mock_balance
    return market


class TestPortfolioValuer:
    """Integration tests for the PortfolioValuer orchestrator."""

    def test_basic_spot_valuation(self):
        """Happy path: wallet with ETH and USDC gets valued correctly."""
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("2"), "USDC": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market, iteration_number=5)

        assert snapshot.total_value_usd == Decimal("12000")  # 2*3500 + 5000*1
        assert snapshot.available_cash_usd == Decimal("12000")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.strategy_id == "test-strat"
        assert snapshot.chain == "arbitrum"
        assert snapshot.iteration_number == 5
        assert len(snapshot.wallet_balances) == 2

        eth = next(t for t in snapshot.wallet_balances if t.symbol == "ETH")
        assert eth.price_usd == Decimal("3500")
        assert eth.balance == Decimal("2")
        assert eth.value_usd == Decimal("7000")

    def test_missing_price_partial_valuation(self):
        """Token with missing price is excluded, others still valued with ESTIMATED confidence."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC", "ARB"])
        market = _make_market(
            prices={"USDC": Decimal("1")},  # ETH and ARB prices missing
            balances={"ETH": Decimal("2"), "USDC": Decimal("5000"), "ARB": Decimal("100")},
        )

        snapshot = valuer.value(strategy, market)

        # Only USDC gets valued; ETH/ARB have balances but no prices -> ESTIMATED
        assert snapshot.total_value_usd == Decimal("5000")
        assert len(snapshot.wallet_balances) == 1
        assert snapshot.wallet_balances[0].symbol == "USDC"
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_empty_wallet_no_positions_high_confidence(self):
        """Empty wallet with no positions -> HIGH confidence $0 (legitimately empty)."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH"])
        # Balance of 0 -> excluded
        market = _make_market(
            prices={"ETH": Decimal("3500")},
            balances={"ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # No wallet balances, no positions -- but positions didn't fail
        # So this is HIGH confidence with $0 (legitimately empty wallet)
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.value_confidence == ValueConfidence.HIGH

    def test_positions_included_in_total(self):
        """Non-wallet positions (LP, lending) are added to total value."""
        valuer = PortfolioValuer()
        positions = [
            PositionInfo(
                position_type=PositionType.LP,
                position_id="lp-123",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("10000"),
                details={"tokens": ["WETH", "USDC"]},
            ),
        ]
        strategy = _make_strategy(positions=positions)
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("0"), "USDC": Decimal("5000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("15000")  # 5000 wallet + 10000 LP
        assert snapshot.available_cash_usd == Decimal("5000")  # wallet only
        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].value_usd == Decimal("10000")
        assert snapshot.value_confidence == ValueConfidence.HIGH

    def test_positions_failure_gives_estimated(self):
        """If get_open_positions raises, wallet values used with ESTIMATED confidence."""
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        strategy.get_open_positions.side_effect = RuntimeError("Position query failed")
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("1"), "USDC": Decimal("1000")},
        )

        snapshot = valuer.value(strategy, market)

        # Wallet values succeed, positions fail -> ESTIMATED
        assert snapshot.total_value_usd == Decimal("4500")
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_only_positions_no_wallet_gives_estimated(self):
        """Positions but no wallet balances -> ESTIMATED."""
        valuer = PortfolioValuer()
        positions = [
            PositionInfo(
                position_type=PositionType.LP,
                position_id="lp-123",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("10000"),
                details={"tokens": ["WETH", "USDC"]},
            ),
        ]
        strategy = _make_strategy(positions=positions, tracked_tokens=["ETH"])
        # Balance query fails for ETH
        market = MagicMock()
        market.balance.side_effect = RuntimeError("No balance")
        market.price.return_value = Decimal("3500")

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("10000")
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_failure_contract_never_raises(self):
        """Total failure returns UNAVAILABLE, never raises."""
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        strategy._get_tracked_tokens.side_effect = RuntimeError("Strategy broken")

        market = _make_market()

        # Should NOT raise
        snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.error is not None
        assert "Strategy broken" in snapshot.error

    def test_no_get_open_positions_still_works(self):
        """Strategy without get_open_positions uses wallet-only valuation."""
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        del strategy.get_open_positions  # Remove the method

        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("1"), "USDC": Decimal("1000")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("4500")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.positions == []

    def test_market_returns_decimal_balance(self):
        """Market.balance() returns plain Decimal (not object with .balance)."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH"])

        market = MagicMock()
        market.balance.return_value = Decimal("2.5")
        market.price.return_value = Decimal("3500")

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("8750")

    def test_iteration_number_passed_through(self):
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        market = _make_market(
            prices={"ETH": Decimal("3500")},
            balances={"ETH": Decimal("1")},
        )

        snapshot = valuer.value(strategy, market, iteration_number=42)
        assert snapshot.iteration_number == 42

    def test_snapshot_serialization_roundtrip(self):
        """Full valuation -> to_dict -> from_dict preserves all data."""
        valuer = PortfolioValuer()
        strategy = _make_strategy()
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("1.5"), "USDC": Decimal("2000")},
        )

        snapshot = valuer.value(strategy, market, iteration_number=10)
        data = snapshot.to_dict()
        restored = PortfolioSnapshot.from_dict(data)

        assert restored.total_value_usd == snapshot.total_value_usd
        assert restored.available_cash_usd == snapshot.available_cash_usd
        assert restored.strategy_id == snapshot.strategy_id
        assert restored.chain == snapshot.chain
        assert restored.iteration_number == 10
        assert len(restored.wallet_balances) == len(snapshot.wallet_balances)

        for orig, rest in zip(snapshot.wallet_balances, restored.wallet_balances):
            assert orig.symbol == rest.symbol
            assert orig.balance == rest.balance
            assert orig.value_usd == rest.value_usd
            assert orig.price_usd == rest.price_usd


class TestPortfolioValuerEdgeCases:
    def test_all_balance_queries_fail(self):
        """If all balance queries fail but positions work, partial result."""
        valuer = PortfolioValuer()
        positions = [
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="token-1",
                chain="arbitrum",
                protocol="wallet",
                value_usd=Decimal("5000"),
                details={},
            ),
        ]
        strategy = _make_strategy(positions=positions, tracked_tokens=["ETH", "USDC"])
        market = MagicMock()
        market.balance.side_effect = RuntimeError("Gateway unreachable")
        market.price.side_effect = RuntimeError("Gateway unreachable")

        snapshot = valuer.value(strategy, market)

        # Positions available, wallet failed
        assert snapshot.total_value_usd == Decimal("5000")
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_balance_returns_object_with_balance_attr(self):
        """Handle MarketSnapshot.balance() returning TokenBalance-like object."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH"])

        balance_obj = MagicMock()
        balance_obj.balance = Decimal("3")

        market = MagicMock()
        market.balance.return_value = balance_obj
        market.price.return_value = Decimal("3500")

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("10500")

    def test_empty_tracked_tokens(self):
        """Strategy with no tracked tokens produces empty but valid snapshot."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=[])
        market = _make_market()

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.wallet_balances == []

    def test_balance_failure_with_values_gives_estimated(self):
        """Some balance queries fail but we have partial values -> ESTIMATED."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC"])

        # ETH balance fails, USDC works
        def mock_balance(token):
            if token == "USDC":
                result = MagicMock()
                result.balance = Decimal("1000")
                return result
            raise RuntimeError("Gateway error")

        market = MagicMock()
        market.balance = mock_balance
        market.price.return_value = Decimal("1")

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("1000")
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED

    def test_strategy_accessor_failure_returns_unavailable(self):
        """If strategy.strategy_id raises, returns UNAVAILABLE (not exception)."""
        valuer = PortfolioValuer()
        strategy = MagicMock()
        type(strategy).strategy_id = PropertyMock(side_effect=RuntimeError("broken"))
        type(strategy).chain = PropertyMock(return_value="arbitrum")
        strategy._get_tracked_tokens.return_value = ["ETH"]
        market = _make_market(prices={"ETH": Decimal("3500")}, balances={"ETH": Decimal("1")})

        snapshot = valuer.value(strategy, market)
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
