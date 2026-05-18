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
from almanak.gateway.proto import gateway_pb2


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
    wallet_address="0x1234567890123456789012345678901234567890",
    tracked_tokens=None,
    positions=None,
):
    """Create a mock strategy with the StrategyLike protocol."""
    strategy = MagicMock()
    type(strategy).strategy_id = PropertyMock(return_value=strategy_id)
    type(strategy).chain = PropertyMock(return_value=chain)
    type(strategy).wallet_address = PropertyMock(return_value=wallet_address)
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

        # VIB-3614: total_value_usd is position-scoped; wallet-only → 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.available_cash_usd == Decimal("12000")
        assert snapshot.wallet_total_value_usd == Decimal("12000")  # 2*3500 + 5000*1
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
        # VIB-3614: wallet-only → total_value_usd == 0; full value in wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("5000")
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

        # VIB-3614: total_value_usd is position-scoped (LP value only)
        assert snapshot.total_value_usd == Decimal("10000")  # LP position only
        assert snapshot.available_cash_usd == Decimal("5000")  # wallet only
        assert snapshot.wallet_total_value_usd == Decimal("15000")  # 5000 wallet + 10000 LP
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
        # VIB-3614: positions failed so total_value_usd == 0; wallet in wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("4500")
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

        # VIB-3614: no positions method → wallet-only → total_value_usd == 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("4500")
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

        # VIB-3614: wallet-only → total_value_usd == 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("8750")

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

    def test_snapshot_positions_payload_roundtrip_with_metadata(self):
        """Persisted positions payload supports metadata envelope without breaking round-trip."""
        snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
            strategy_id="test-strat",
            total_value_usd=Decimal("4.70"),
            available_cash_usd=Decimal("0"),
            positions=[],
            snapshot_metadata={
                "valuation_source": "reconciled_external",
                "external_total_value_usd": "4.70",
            },
        )

        payload = snapshot.to_positions_payload()
        positions, metadata = PortfolioSnapshot.unpack_positions_payload(payload)

        assert positions == []
        assert metadata["valuation_source"] == "reconciled_external"
        assert metadata["external_total_value_usd"] == "4.70"

    def test_external_agreement_keeps_framework_total(self):
        """External wallet data within threshold preserves framework valuation."""
        client = MagicMock()
        client.integration.GetWalletPortfolio.return_value = gateway_pb2.WalletPortfolioResponse(
            success=True,
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            total_value_usd="4700",
            timestamp=int(datetime.now(UTC).timestamp()),
        )
        valuer = PortfolioValuer(gateway_client=client)
        strategy = _make_strategy(tracked_tokens=["ETH", "USDC"])
        market = _make_market(
            prices={"ETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("1"), "USDC": Decimal("1000")},
        )

        snapshot = valuer.value(strategy, market)

        # Verify the outbound request was sent with correct wallet/chain
        request = client.integration.GetWalletPortfolio.call_args.args[0]
        assert request.wallet_address == "0x1234567890123456789012345678901234567890"
        assert request.chain == "arbitrum"

        # VIB-3614: no positions → total_value_usd == 0; framework wallet in wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("4500")  # framework wallet (1 ETH + 1000 USDC)
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.snapshot_metadata["reconciliation_status"] == "framework_won_close_agreement"
        assert snapshot.snapshot_metadata["external_total_value_usd"] == "4700"

    def test_external_zero_framework_positive_wins_and_updates_positions(self):
        """External valuation replaces zero framework value and fills position coverage."""
        client = MagicMock()
        client.integration.GetWalletPortfolio.return_value = gateway_pb2.WalletPortfolioResponse(
            success=True,
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="avalanche",
            total_value_usd="4.70",
            timestamp=int(datetime.now(UTC).timestamp()),
            positions=[
                gateway_pb2.WalletPortfolioPosition(
                    position_id="tjv2-bin-1",
                    protocol="traderjoe_v2",
                    label="WAVAX/USDT LB",
                    position_type="liquidity_position",
                    value_usd="4.70",
                    pool_address="0xpool",
                    token_symbols=["WAVAX", "USDT"],
                    raw_details_json='{"vendor":"zerion"}',
                )
            ],
        )
        valuer = PortfolioValuer(gateway_client=client)
        positions = [
            PositionInfo(
                position_type=PositionType.LP,
                position_id="tj-strategy-pos",
                chain="avalanche",
                protocol="traderjoe_v2",
                value_usd=Decimal("0"),
                details={"pool_address": "0xpool", "strategy_note": "framework"},
            )
        ]
        strategy = _make_strategy(chain="avalanche", tracked_tokens=["WAVAX"], positions=positions)
        market = _make_market(prices={"WAVAX": Decimal("20")}, balances={"WAVAX": Decimal("0")})

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("4.70")
        # VIB-4584 / F3.1: the framework couldn't value this LP through any
        # registered protocol path (TraderJoe V2 LB has no on-chain repricer
        # yet), so the framework snapshot was stamped UNAVAILABLE. External
        # portfolio data (Zerion) supplied a value — it surfaces in
        # ``positions[0].value_usd`` and ``total_value_usd`` for operator
        # visibility — but UNAVAILABLE is sticky: an advisory external
        # reading cannot retroactively certify a value the framework never
        # verified.
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
        assert snapshot.snapshot_metadata["valuation_source"] == "reconciled_external"
        assert snapshot.snapshot_metadata["reconciliation_status"] == "external_won_zero_framework"
        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].protocol == "traderjoe_v2"
        assert snapshot.positions[0].value_usd == Decimal("4.70")
        assert snapshot.positions[0].details["strategy_note"] == "framework"
        assert snapshot.positions[0].details["vendor"] == "zerion"

    def test_external_large_divergence_framework_wins(self):
        """Large divergence logs a warning but framework total is authoritative."""
        client = MagicMock()
        client.integration.GetWalletPortfolio.return_value = gateway_pb2.WalletPortfolioResponse(
            success=True,
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            total_value_usd="15",
            timestamp=int(datetime.now(UTC).timestamp()),
        )
        valuer = PortfolioValuer(gateway_client=client)
        strategy = _make_strategy(tracked_tokens=["USDC"])
        # VIB-4225 ACC-02: include ETH=0 / price so the gas-native helper
        # stamps `gas_native_status="ok"`; otherwise the test trips the
        # `wallet_data_incomplete=True` path and confidence drops to ESTIMATED.
        market = _make_market(
            prices={"USDC": Decimal("1"), "ETH": Decimal("3500")},
            balances={"USDC": Decimal("10"), "ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # Framework on-chain value is authoritative; external is advisory
        # VIB-3614: wallet-only → total_value_usd == 0; framework wallet in wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("10")  # framework wallet (10 USDC + 0 ETH)
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.snapshot_metadata["reconciliation_status"] == "framework_won_large_divergence"
        assert snapshot.snapshot_metadata["external_total_value_usd"] == "15"

    def test_external_moderate_divergence_framework_wins(self):
        """Moderate divergence (10-20%) keeps framework total."""
        client = MagicMock()
        client.integration.GetWalletPortfolio.return_value = gateway_pb2.WalletPortfolioResponse(
            success=True,
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            total_value_usd="115",
            timestamp=int(datetime.now(UTC).timestamp()),
        )
        valuer = PortfolioValuer(gateway_client=client)
        strategy = _make_strategy(tracked_tokens=["USDC"])
        # VIB-4225 ACC-02: see large-divergence sibling — ETH=0 / price for
        # gas-native helper success.
        market = _make_market(
            prices={"USDC": Decimal("1"), "ETH": Decimal("3500")},
            balances={"USDC": Decimal("100"), "ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # VIB-3614: wallet-only → total_value_usd == 0; framework wallet in wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("100")  # framework wallet (100 USDC + 0 ETH)
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert snapshot.snapshot_metadata["reconciliation_status"] == "framework_won_moderate_divergence"

    def test_external_rpc_failure_falls_back_to_framework(self):
        """External RPC errors must not break framework valuation."""
        client = MagicMock()
        client.integration.GetWalletPortfolio.side_effect = RuntimeError("rpc failed")
        valuer = PortfolioValuer(gateway_client=client)
        strategy = _make_strategy(tracked_tokens=["USDC"])
        # VIB-4225 ACC-02: include ETH=0 / price so the gas-native helper
        # stamps `gas_native_status="ok"` (the test's purpose is RPC fallback,
        # not gas-native — keep the gas trail clean).
        market = _make_market(
            prices={"USDC": Decimal("1"), "ETH": Decimal("3500")},
            balances={"USDC": Decimal("10"), "ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # VIB-3614: RPC failure falls back to framework snapshot; wallet-only → total == 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("10")
        # VIB-4225 ACC-02: gas-native helper stamps `ok` on every framework
        # snapshot. Reconciliation keys are NOT set on the RPC-failure path
        # (no external snapshot to reconcile against).
        assert snapshot.snapshot_metadata == {"gas_native_status": "ok"}


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

        # VIB-3614: wallet-only → total_value_usd == 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("10500")

    def test_empty_tracked_tokens(self):
        """Strategy with no tracked tokens produces empty but valid snapshot.

        VIB-4225 ACC-02: ``_resolve_native_gas`` always appends the chain's
        native row (even at balance=0), so the wallet now has exactly one
        entry — the measured-zero native row — and confidence stays HIGH.
        """
        valuer = PortfolioValuer()
        strategy = _make_strategy(tracked_tokens=[])
        # Provide ETH=0 / price so the gas-native helper stamps "ok".
        market = _make_market(
            prices={"ETH": Decimal("3500")},
            balances={"ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.value_confidence == ValueConfidence.HIGH
        assert len(snapshot.wallet_balances) == 1
        assert snapshot.wallet_balances[0].symbol == "ETH"
        assert snapshot.wallet_balances[0].balance == Decimal("0")

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

        # VIB-3614: wallet-only (partial) → total_value_usd == 0
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.wallet_total_value_usd == Decimal("1000")
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


# ---------------------------------------------------------------------------
# VIB-3452: deployed_capital_usd — separate from full-wallet total_value_usd
# ---------------------------------------------------------------------------


class TestDeployedCapitalUsd:
    """Verify that deployed_capital_usd tracks per-position cost bases, not the
    full wallet total.  This is the root-cause fix for VIB-3452 where a strategy
    that deployed $1,000 into Aave V3 was reporting $33,089 as the PnL
    denominator because total_value_usd (all wallet funds) was being used.
    """

    def test_defaults_to_zero_when_no_accounting_context(self):
        """Without an accounting store wired in, deployed_capital_usd is 0."""
        valuer = PortfolioValuer()
        strategy = _make_strategy(
            tracked_tokens=["ETH", "USDC"],
            positions=[
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="aave-usdc-supply",
                    chain="arbitrum",
                    protocol="aave_v3",
                    value_usd=Decimal("1000"),
                    details={
                        "asset": "USDC",
                        "wallet": "0x1234567890123456789012345678901234567890",
                        "wallet_address": "0x1234567890123456789012345678901234567890",
                    },
                )
            ],
        )
        market = _make_market(
            prices={"ETH": Decimal("3300"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("10"), "USDC": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # deployed_capital_usd must be 0 — no accounting events means no cost basis
        assert snapshot.deployed_capital_usd == Decimal("0")
        # VIB-3614: total_value_usd is position-scoped ($1000 supply), not full wallet
        assert snapshot.total_value_usd == Decimal("1000")
        assert snapshot.wallet_total_value_usd == Decimal("34000")  # 10 ETH@$3300 + $1000 supply

    def test_deployed_capital_populated_from_position_cost_basis(self):
        """When positions have cost_basis_usd set, deployed_capital_usd sums them.

        Simulates the post-VIB-3424 flow: _enrich_position_pnl() writes
        cost_basis_usd onto each PositionValue; the valuer must then aggregate
        that into snapshot.deployed_capital_usd so callers never need to
        read total_value_usd as the deployment denominator.
        """
        from almanak.framework.portfolio.models import PositionValue

        valuer = PortfolioValuer()

        # Wire up a fake accounting store that returns one SUPPLY event for the position.
        # VIB-3503: PortfolioValuer now prefetches once per snapshot and groups events
        # by the row's position_key; the cache lookup uses the key derived by
        # _try_derive_lending_position_key, which for this test is
        # "lending:arbitrum:aave_v3:<wallet-lowercased>:usdc". The mock event must
        # carry that exact key so the cache-side filter finds it.
        mock_store = MagicMock()
        mock_store.get_accounting_events_sync.return_value = [
            {
                "timestamp": "2026-04-26T10:00:00",
                "event_type": "SUPPLY",
                "position_key": "lending:arbitrum:aave_v3:0x1234567890123456789012345678901234567890:usdc",
                "deployment_id": "test-deployment",
                "ledger_entry_id": "ledger-001",
                "payload_json": '{"principal_delta_usd": "1000", "interest_delta_usd": null}',
            }
        ]
        valuer.set_accounting_context(mock_store, "test-deployment")

        strategy = _make_strategy(
            tracked_tokens=["ETH", "USDC"],
            positions=[
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="aave-usdc-supply",
                    chain="arbitrum",
                    protocol="aave_v3",
                    value_usd=Decimal("1000"),
                    details={
                        "asset": "USDC",
                        "wallet": "0x1234567890123456789012345678901234567890",
                        "wallet_address": "0x1234567890123456789012345678901234567890",
                    },
                )
            ],
        )
        # Wallet holds 10 ETH @ $3300 + the $1000 USDC supply position
        # Without the fix total_value_usd ($34,000) would be wrongly used as the
        # PnL denominator.  With the fix deployed_capital_usd == $1,000.
        market = _make_market(
            prices={"ETH": Decimal("3300"), "USDC": Decimal("1")},
            balances={"ETH": Decimal("10"), "USDC": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # VIB-3614: total_value_usd is position-scoped (supply $1000 only).
        # Full wallet (10 ETH@$3300 + $1000 supply = $34,000) is in wallet_total_value_usd.
        assert snapshot.total_value_usd == Decimal("1000"), (
            f"expected $1000 total_value_usd (position only) but got ${snapshot.total_value_usd}"
        )
        assert snapshot.wallet_total_value_usd == Decimal("34000"), (
            f"expected $34000 wallet_total_value_usd but got ${snapshot.wallet_total_value_usd}"
        )

        # deployed_capital_usd must reflect only the $1,000 that was deployed.  VIB-3452.
        assert snapshot.deployed_capital_usd == Decimal("1000"), (
            f"expected $1000 deployed_capital_usd but got ${snapshot.deployed_capital_usd}; "
            "VIB-3452 regression: full wallet value is being used as PnL denominator"
        )

    def test_deployed_capital_sums_multiple_positions(self):
        """Multiple positions with different cost bases are summed correctly."""
        valuer = PortfolioValuer()

        # Inject pre-populated cost bases directly by replacing the instance method so
        # we test the summation logic in isolation without needing the full
        # accounting event pipeline.
        def patched_enrich(position_value, position_info, chain):
            # Assign fixed cost bases per position so we can assert the sum
            if position_info.position_id == "pos-a":
                position_value.cost_basis_usd = Decimal("500")
            elif position_info.position_id == "pos-b":
                position_value.cost_basis_usd = Decimal("750")

        valuer._enrich_position_pnl = patched_enrich

        strategy = _make_strategy(
            tracked_tokens=["ETH"],
            positions=[
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="pos-a",
                    chain="arbitrum",
                    protocol="aave_v3",
                    value_usd=Decimal("510"),
                    details={},
                ),
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="pos-b",
                    chain="arbitrum",
                    protocol="compound_v3",
                    value_usd=Decimal("760"),
                    details={},
                ),
            ],
        )
        market = _make_market(
            prices={"ETH": Decimal("3000")},
            balances={"ETH": Decimal("1")},
        )

        snapshot = valuer.value(strategy, market)

        # 500 + 750 = 1250 total deployed capital across both positions
        assert snapshot.deployed_capital_usd == Decimal("1250")

    def test_deployed_capital_zero_positions_no_cost_basis(self):
        """Positions present but no cost basis returns Decimal('0')."""
        valuer = PortfolioValuer()

        strategy = _make_strategy(
            tracked_tokens=["ETH"],
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="lp-xyz",
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("2000"),
                    details={"tokens": ["WETH", "USDC"]},
                )
            ],
        )
        market = _make_market(
            prices={"ETH": Decimal("3000")},
            balances={"ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        # LP has no accounting events -> cost_basis_usd stays 0 -> deployed == 0
        assert snapshot.deployed_capital_usd == Decimal("0")

    def test_deployed_capital_survives_serialization_roundtrip(self):
        """deployed_capital_usd is preserved through to_dict / from_dict."""
        snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC),
            strategy_id="test-strat",
            total_value_usd=Decimal("33000"),
            available_cash_usd=Decimal("33000"),
            deployed_capital_usd=Decimal("1000"),
        )
        data = snapshot.to_dict()
        assert data["deployed_capital_usd"] == "1000"

        restored = PortfolioSnapshot.from_dict(data)
        assert restored.deployed_capital_usd == Decimal("1000")

    def test_deployed_capital_defaults_to_zero_on_legacy_snapshots(self):
        """Snapshots without deployed_capital_usd key deserialize with 0."""
        data = {
            "timestamp": datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC).isoformat(),
            "strategy_id": "old-strat",
            "total_value_usd": "5000",
            "available_cash_usd": "5000",
            "value_confidence": "HIGH",
            "positions": [],
            "wallet_balances": [],
            "token_prices": {},
            # deliberately absent: deployed_capital_usd
        }
        restored = PortfolioSnapshot.from_dict(data)
        assert restored.deployed_capital_usd == Decimal("0")

    def test_deployed_capital_preserved_through_external_reconciliation(self):
        """deployed_capital_usd must survive _build_external_reconciled_snapshot.

        When framework_total is 0 and external wins, the reconciled snapshot is
        rebuilt from scratch. Without an explicit forward of deployed_capital_usd,
        the field silently resets to 0 — breaking the VIB-3452 fix in the
        zero-framework-value edge case (CodeRabbit MAJOR finding).
        """
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()

        # Build a framework snapshot with non-zero deployed_capital_usd but
        # zero total_value_usd (simulates a gateway-side balance query failure).
        framework_snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC),
            strategy_id="test-strat",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
            deployed_capital_usd=Decimal("1000"),
            value_confidence=ValueConfidence.UNAVAILABLE,
        )

        # Simulate an external portfolio with a positive total so it wins.
        external = {
            "total_value_usd": Decimal("1050"),
            "provider": "debank",
            "cache_hit": False,
            "timestamp": datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC),
            "positions": [],
        }

        reconciled = valuer._build_external_reconciled_snapshot(
            framework_snapshot, external, {"reconciliation_status": "external_won_zero_framework"}
        )

        # deployed_capital_usd must be forwarded from framework_snapshot, not reset to 0.
        assert reconciled.deployed_capital_usd == Decimal("1000"), (
            f"expected deployed_capital_usd=$1000 but got ${reconciled.deployed_capital_usd}; "
            "deployed_capital_usd was not forwarded through _build_external_reconciled_snapshot"
        )
        # VIB-3614: total_value_usd is position-scoped; no positions → 0
        # Full external total is in wallet_total_value_usd
        assert reconciled.total_value_usd == Decimal("0")
        assert reconciled.wallet_total_value_usd == Decimal("1050")

    def test_unavailable_confidence_preserved_through_external_reconciliation(self):
        """VIB-4584 / F3.1 — UNAVAILABLE must survive external reconciliation.

        When the framework can't value a position through a registered path
        (e.g. Aerodrome CL, Uniswap V4 LP), the snapshot is stamped
        UNAVAILABLE so a reader can distinguish "we have no idea" from
        "measured zero". External portfolio totals (Zerion / DeBank) are
        advisory metadata — they cannot retroactively certify a value the
        framework never verified. Reconciliation MUST keep the UNAVAILABLE
        verdict; otherwise the data-quality signal F3.1 was built to
        surface is silently downgraded to ESTIMATED.
        """
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()

        framework_snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC),
            strategy_id="future-dex-strat",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
            deployed_capital_usd=Decimal("0"),
            value_confidence=ValueConfidence.UNAVAILABLE,
        )

        external = {
            "total_value_usd": Decimal("12345"),
            "provider": "zerion",
            "cache_hit": False,
            "timestamp": datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC),
            "positions": [],
        }

        reconciled = valuer._build_external_reconciled_snapshot(
            framework_snapshot, external, {"reconciliation_status": "external_won_zero_framework"}
        )

        assert reconciled.value_confidence == ValueConfidence.UNAVAILABLE, (
            "external reconciliation must not downgrade UNAVAILABLE → ESTIMATED — "
            "external data is advisory metadata, not a value certification"
        )

    def test_borrow_with_negative_strategy_value_is_trusted_not_no_path(self):
        """VIB-4584 / F3.1 — strategies may report BORROW debt either as a
        positive gross amount (framework negates) or as an already-normalised
        negative value. The pre-fix `value_usd > 0` gate rejected the latter
        and flagged it as no_path, marking the snapshot UNAVAILABLE despite a
        perfectly fine fallback value being present. This test pins both:

        * BORROW with positive ``value_usd`` → framework returns the negated
          debt and marks it ``repriced=True`` (legacy behaviour).
        * BORROW with negative ``value_usd`` → already normalised; framework
          returns it unchanged with ``repriced=True``.
        * BORROW with zero ``value_usd`` → no signal anywhere → marked
          ``repriced=False`` so the snapshot drops to UNAVAILABLE.
        """
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()
        market = MagicMock()

        # Patch the on-chain repricer to None so the strategy-fallback branch
        # is the path under test.
        with patch.object(valuer, "_reprice_lending_on_chain_enriched", return_value=None):
            positive = PositionInfo(
                position_type=PositionType.BORROW,
                position_id="bor-pos",
                chain="ethereum",
                protocol="aave_v3",
                value_usd=Decimal("1000"),
            )
            value, _details, repriced = valuer._reprice_position_enriched(positive, "ethereum", market)
            assert (value, repriced) == (Decimal("-1000"), True), (
                "positive value_usd must be negated and trusted"
            )

            negative = PositionInfo(
                position_type=PositionType.BORROW,
                position_id="bor-neg",
                chain="ethereum",
                protocol="aave_v3",
                value_usd=Decimal("-2500"),
            )
            value, _details, repriced = valuer._reprice_position_enriched(negative, "ethereum", market)
            assert (value, repriced) == (Decimal("-2500"), True), (
                "negative value_usd is already normalised — must be trusted, not flagged no_path"
            )

            zero = PositionInfo(
                position_type=PositionType.BORROW,
                position_id="bor-zero",
                chain="ethereum",
                protocol="aave_v3",
                value_usd=Decimal("0"),
            )
            value, _details, repriced = valuer._reprice_position_enriched(zero, "ethereum", market)
            assert (value, repriced) == (Decimal("0"), False), (
                "zero value_usd with no on-chain path means we have no signal — repriced=False"
            )

    def test_non_unavailable_confidence_falls_through_to_estimated(self):
        """Companion to the no_path test above: when the framework snapshot
        has HIGH or ESTIMATED confidence going in, the reconciled snapshot
        falls back to ESTIMATED (the legacy behaviour) because the external
        provider's number "won" against framework_total <= 0. Only
        UNAVAILABLE is sticky — other confidence levels still pay the
        agreement-with-external cost.
        """
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()

        for input_confidence in (ValueConfidence.HIGH, ValueConfidence.ESTIMATED):
            framework_snapshot = PortfolioSnapshot(
                timestamp=datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC),
                strategy_id="strat",
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                deployed_capital_usd=Decimal("0"),
                value_confidence=input_confidence,
            )
            external = {
                "total_value_usd": Decimal("100"),
                "provider": "zerion",
                "cache_hit": False,
                "timestamp": datetime(2026, 5, 18, 12, 0, 0, tzinfo=UTC),
                "positions": [],
            }
            reconciled = valuer._build_external_reconciled_snapshot(
                framework_snapshot, external, {"reconciliation_status": "external_won_zero_framework"}
            )
            assert reconciled.value_confidence == ValueConfidence.ESTIMATED, (
                f"input={input_confidence} expected ESTIMATED but got {reconciled.value_confidence}"
            )


# ---------------------------------------------------------------------------
# VIB-3491: LP/perp/vault PositionValue enrichment from position_events
# ---------------------------------------------------------------------------


def _make_lp_position_info(position_id="12345", protocol="uniswap_v3", chain="arbitrum"):
    """Build a mock PositionInfo for an LP position."""
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("1100"),
        details={"tokens": ["WETH", "USDC"]},
    )


def _make_perp_position_info(position_id="perp-abc123", protocol="gmx_v2", chain="arbitrum"):
    """Build a mock PositionInfo for a PERP position."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("2200"),
        details={
            "market": "0xmarketaddress",
            "is_long": True,
            "wallet": "0x1234567890123456789012345678901234567890",
        },
    )


def _make_vault_position_info(
    position_id="vault-pos-1",
    protocol="morpho",
    chain="arbitrum",
    wallet="0x1234567890123456789012345678901234567890",
    vault_address="0xvaultaddress",
):
    """Build a mock PositionInfo for a VAULT position."""
    return PositionInfo(
        position_type=PositionType.VAULT,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("550"),
        details={
            "wallet": wallet,
            "wallet_address": wallet,
            "vault_address": vault_address,
            "asset": "USDC",
        },
    )


def _make_lp_open_event(position_id="12345", value_usd="1000", timestamp="2026-01-01T10:00:00", ledger_entry_id="ledger-lp-001"):
    """Build a synthetic LP OPEN event row as returned by get_position_events_sync."""
    return {
        "id": "evt-001",
        "deployment_id": "test-deployment",
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": timestamp,
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "value_usd": value_usd,
        "token0": "WETH",
        "token1": "USDC",
        "ledger_entry_id": ledger_entry_id,
        "attribution_json": "{}",
    }


def _make_perp_open_event(position_id="perp-abc123", value_usd="2000", timestamp="2026-01-02T10:00:00", ledger_entry_id="ledger-perp-001"):
    """Build a synthetic PERP OPEN event row."""
    return {
        "id": "evt-002",
        "deployment_id": "test-deployment",
        "position_id": position_id,
        "position_type": "PERP",
        "event_type": "OPEN",
        "timestamp": timestamp,
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "value_usd": value_usd,
        "ledger_entry_id": ledger_entry_id,
        "attribution_json": "{}",
    }


def _make_vault_deposit_event(value_usd="500", timestamp="2026-01-03T10:00:00", ledger_entry_id="ledger-vault-001"):
    """Build a synthetic VAULT_DEPOSIT accounting event row."""
    import json
    return {
        "id": "evt-003",
        "deployment_id": "test-deployment",
        "event_type": "VAULT_DEPOSIT",
        "position_key": "vault:arbitrum:morpho:0x1234567890123456789012345678901234567890:0xvaultaddress",
        "timestamp": timestamp,
        "protocol": "morpho",
        "chain": "arbitrum",
        "ledger_entry_id": ledger_entry_id,
        "payload_json": json.dumps({"cost_basis_usd": value_usd, "schema_version": 1}),
        "confidence": "HIGH",
        "schema_version": 1,
    }


class TestLpPerpVaultPositionEnrichment:
    """VIB-3491: LP/perp/vault PositionValue enrichment from position_events.

    Tests cover:
    - cost_basis_usd and entry_timestamp populated from LP_OPEN event
    - cost_basis_usd and entry_timestamp populated from PERP OPEN event
    - cost_basis_usd populated from VAULT_DEPOSIT accounting event
    - Position without OPEN event leaves cost_basis at 0
    - deployed_capital_usd reflects LP cost basis
    """

    def _make_mock_store(self, position_events=None, accounting_events=None):
        """Build a mock store that returns given events from the appropriate methods."""
        store = MagicMock()
        store.get_position_events_sync.return_value = position_events or []
        store.get_accounting_events_sync.return_value = accounting_events or []
        return store

    def test_lp_position_enriched_from_lp_open_event(self):
        """LP PositionValue gets cost_basis_usd and entry_timestamp from position_events OPEN row."""
        valuer = PortfolioValuer()
        open_event = _make_lp_open_event(value_usd="1000", timestamp="2026-01-01T10:00:00", ledger_entry_id="ledger-lp-001")
        store = self._make_mock_store(position_events=[open_event])
        valuer.set_accounting_context(store, "test-deployment")

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("1100"),
            label="uniswap_v3 lp",
        )
        pos_info = _make_lp_position_info(position_id="12345")

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        assert pos_val.cost_basis_usd == Decimal("1000"), (
            f"expected cost_basis_usd=1000 but got {pos_val.cost_basis_usd}"
        )
        assert pos_val.unrealized_pnl_usd == Decimal("100"), (
            f"expected unrealized_pnl_usd=100 (1100-1000) but got {pos_val.unrealized_pnl_usd}"
        )
        assert pos_val.entry_timestamp == "2026-01-01T10:00:00"
        assert pos_val.ledger_entry_id == "ledger-lp-001"

        # Verify the store was called with the correct position_id and event_type
        store.get_position_events_sync.assert_called_once_with(
            "test-deployment",
            position_id="12345",
            position_type="LP",
            event_type="OPEN",
        )

    def test_perp_position_enriched_from_perp_open_event(self):
        """PERP PositionValue gets cost_basis_usd and entry_timestamp from position_events OPEN row."""
        valuer = PortfolioValuer()
        open_event = _make_perp_open_event(value_usd="2000", timestamp="2026-01-02T10:00:00", ledger_entry_id="ledger-perp-001")
        store = self._make_mock_store(position_events=[open_event])
        valuer.set_accounting_context(store, "test-deployment")

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.PERP,
            protocol="gmx_v2",
            chain="arbitrum",
            value_usd=Decimal("2200"),
            label="gmx_v2 perp",
        )
        pos_info = _make_perp_position_info(position_id="perp-abc123")

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        assert pos_val.cost_basis_usd == Decimal("2000")
        assert pos_val.unrealized_pnl_usd == Decimal("200")  # 2200 - 2000
        assert pos_val.entry_timestamp == "2026-01-02T10:00:00"
        assert pos_val.ledger_entry_id == "ledger-perp-001"

        store.get_position_events_sync.assert_called_once_with(
            "test-deployment",
            position_id="perp-abc123",
            position_type="PERP",
            event_type="OPEN",
        )

    def test_vault_position_enriched_from_vault_deposit_event(self):
        """VAULT PositionValue gets cost_basis_usd from VAULT_DEPOSIT accounting event."""
        valuer = PortfolioValuer()
        vault_event = _make_vault_deposit_event(value_usd="500", timestamp="2026-01-03T10:00:00", ledger_entry_id="ledger-vault-001")
        store = self._make_mock_store(accounting_events=[vault_event])
        valuer.set_accounting_context(store, "test-deployment")

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.VAULT,
            protocol="morpho",
            chain="arbitrum",
            value_usd=Decimal("550"),
            label="morpho vault",
        )
        pos_info = _make_vault_position_info()

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        assert pos_val.cost_basis_usd == Decimal("500")
        assert pos_val.unrealized_pnl_usd == Decimal("50")  # 550 - 500
        assert pos_val.entry_timestamp == "2026-01-03T10:00:00"
        assert pos_val.ledger_entry_id == "ledger-vault-001"

        # get_accounting_events_sync is called with the derived vault position_key
        store.get_accounting_events_sync.assert_called_once_with(
            "test-deployment",
            position_key="vault:arbitrum:morpho:0x1234567890123456789012345678901234567890:0xvaultaddress",
        )

    def test_position_without_open_event_left_as_zero(self):
        """When no OPEN event exists for an LP position, cost_basis stays at 0."""
        valuer = PortfolioValuer()
        # Store returns empty list — no OPEN event
        store = self._make_mock_store(position_events=[])
        valuer.set_accounting_context(store, "test-deployment")

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("1100"),
            label="uniswap_v3 lp",
        )
        pos_info = _make_lp_position_info(position_id="99999")

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        assert pos_val.cost_basis_usd == Decimal("0"), (
            f"expected cost_basis_usd=0 when no OPEN event but got {pos_val.cost_basis_usd}"
        )
        assert pos_val.unrealized_pnl_usd == Decimal("0")
        assert pos_val.entry_timestamp == ""
        assert pos_val.ledger_entry_id == ""

    def test_deployed_capital_includes_lp_positions(self):
        """deployed_capital_usd reflects LP cost basis in addition to lending."""
        valuer = PortfolioValuer()
        open_event = _make_lp_open_event(position_id="12345", value_usd="3000")
        store = MagicMock()
        store.get_position_events_sync.return_value = [open_event]
        store.get_accounting_events_sync.return_value = []
        valuer.set_accounting_context(store, "test-deployment")

        strategy = _make_strategy(
            tracked_tokens=["ETH"],
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="12345",
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    value_usd=Decimal("3200"),
                    details={"tokens": ["WETH", "USDC"]},
                )
            ],
        )
        market = _make_market(
            prices={"ETH": Decimal("3000")},
            balances={"ETH": Decimal("0")},
        )

        snapshot = valuer.value(strategy, market)

        assert snapshot.deployed_capital_usd == Decimal("3000"), (
            f"expected deployed_capital_usd=3000 from LP cost basis, got {snapshot.deployed_capital_usd}"
        )

    def test_lp_position_without_accounting_context_stays_zero(self):
        """LP position with no accounting context set → cost_basis stays 0."""
        valuer = PortfolioValuer()
        # No set_accounting_context call

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("1000"),
            label="uniswap_v3 lp",
        )
        pos_info = _make_lp_position_info(position_id="12345")

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        assert pos_val.cost_basis_usd == Decimal("0")

    def test_lp_open_event_zero_value_usd_not_used(self):
        """An LP OPEN event with value_usd='0' does not set cost_basis (guard against bogus records)."""
        valuer = PortfolioValuer()
        open_event = _make_lp_open_event(value_usd="0")
        store = self._make_mock_store(position_events=[open_event])
        valuer.set_accounting_context(store, "test-deployment")

        from almanak.framework.portfolio.models import PositionValue

        pos_val = PositionValue(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("1000"),
            label="uniswap_v3 lp",
        )
        pos_info = _make_lp_position_info(position_id="12345")

        valuer._enrich_position_pnl(pos_val, pos_info, "arbitrum")

        # Zero value_usd is filtered out — cost_basis stays 0
        assert pos_val.cost_basis_usd == Decimal("0")

    def test_vault_position_key_derivation(self):
        """_try_derive_vault_position_key produces expected colon-delimited key."""
        pos_info = _make_vault_position_info(
            protocol="morpho",
            chain="arbitrum",
            wallet="0xABCD",
            vault_address="0xVAULT",
        )
        key = PortfolioValuer._try_derive_vault_position_key(pos_info, "arbitrum")
        assert key == "vault:arbitrum:morpho:0xabcd:0xvault"

    def test_vault_position_key_none_when_missing_wallet(self):
        """Returns None if vault PositionInfo has no wallet detail."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        pos_info = PositionInfo(
            position_type=PositionType.VAULT,
            position_id="vault-pos",
            chain="arbitrum",
            protocol="morpho",
            value_usd=Decimal("100"),
            details={"vault_address": "0xvault"},  # no wallet
        )
        key = PortfolioValuer._try_derive_vault_position_key(pos_info, "arbitrum")
        assert key is None

    def test_vault_position_key_none_when_missing_vault_address(self):
        """Returns None if vault PositionInfo has no vault_address detail."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        pos_info = PositionInfo(
            position_type=PositionType.VAULT,
            position_id="vault-pos",
            chain="arbitrum",
            protocol="morpho",
            value_usd=Decimal("100"),
            details={"wallet": "0xwallet"},  # no vault_address
        )
        key = PortfolioValuer._try_derive_vault_position_key(pos_info, "arbitrum")
        assert key is None


# ---------------------------------------------------------------------------
# VIB-4274 — pool-descriptor guard at the LP repricing entry points.
# ---------------------------------------------------------------------------


class TestLooksLikeEvmAddress:
    """Module-level helper that gates slot0 calls in the LP repricing path."""

    def test_accepts_42_char_hex(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        assert _looks_like_evm_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
        # Lower- and mixed-case both pass (EIP-55 checksums are mixed-case).
        assert _looks_like_evm_address("0x" + "a" * 40)
        assert _looks_like_evm_address("0x" + "A" * 40)

    def test_rejects_descriptor(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        # The actual VIB-4274 trigger.
        assert not _looks_like_evm_address("WETH/USDC/500")
        assert not _looks_like_evm_address("WAVAX/USDC/20")
        assert not _looks_like_evm_address("USDC/DAI")

    def test_rejects_wrong_length(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        assert not _looks_like_evm_address("0x" + "a" * 39)  # 41 chars total
        assert not _looks_like_evm_address("0x" + "a" * 41)  # 43 chars total
        assert not _looks_like_evm_address("0x")  # only prefix

    def test_rejects_non_hex_body(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        # 42 chars and 0x-prefixed but body contains 'Z'.
        assert not _looks_like_evm_address("0xZ" + "a" * 39)

    def test_rejects_missing_prefix(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        # 42 chars of hex but no 0x prefix.
        assert not _looks_like_evm_address("a" * 42)

    def test_rejects_non_string(self):
        from almanak.framework.valuation.portfolio_valuer import _looks_like_evm_address

        assert not _looks_like_evm_address(None)
        assert not _looks_like_evm_address(42)
        assert not _looks_like_evm_address(b"0x" + b"a" * 40)
        assert not _looks_like_evm_address({"address": "0x" + "a" * 40})


def _build_repriced_lp_inputs(pool_value):
    """Construct PortfolioValuer + position + market mocks for the LP repricing path.

    ``pool_value`` is what the producer stashed under ``position.details["pool"]``:
      - a 42-char hex address (valid),
      - a descriptor like ``"WETH/USDC/500"`` (the VIB-4274 trigger),
      - or any other shape we want the guard to reject.

    Returns ``(valuer, position, market)``.
    """
    from almanak.framework.valuation.lp_position_reader import LPPositionOnChain, PoolSlot0
    from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

    valuer = PortfolioValuer()

    # Mock LP reader: return a real-looking position so the repricing path
    # reaches the slot0 call. Active liquidity, no uncollected fees.
    valuer._lp_reader = MagicMock()
    valuer._lp_reader.read_position.return_value = LPPositionOnChain(
        token_id=12345,
        token0="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH (Arbitrum)
        token1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC (Arbitrum)
        fee=500,
        tick_lower=-100,
        tick_upper=100,
        liquidity=10_000_000_000,
        tokens_owed0=0,
        tokens_owed1=0,
    )
    # If the guard works, this MUST NOT be called with a descriptor.
    valuer._lp_reader.read_pool_slot0.return_value = PoolSlot0(
        sqrt_price_x96=2**96,
        tick=0,
    )

    position = PositionInfo(
        position_type=PositionType.LP,
        position_id="12345",
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={
            "pool": pool_value,
            "token0": "WETH",
            "token1": "USDC",
        },
    )
    market = _make_market(
        prices={"WETH": Decimal("3500"), "USDC": Decimal("1")},
    )
    return valuer, position, market


class TestLPRepricingPoolDescriptorGuard:
    """VIB-4274 — the LP repricing path must NEVER feed a non-hex value to
    ``read_pool_slot0``. Two sites in the valuer share the guard
    (``_reprice_lp_on_chain_enriched`` for snapshot persistence and
    ``_reprice_lp_on_chain`` for the legacy single-value path); test both."""

    def test_enriched_path_rejects_descriptor_under_pool_key(self):
        """``details["pool"] = "WETH/USDC/500"`` must NOT trip ``read_pool_slot0``.

        Pre-fix this was the production bug: every snapshot emitted
        ``-32602 odd number of digits`` and the resulting ``in_range`` flag was
        derived from the price-ratio fallback instead of the actual pool tick.
        """
        valuer, position, market = _build_repriced_lp_inputs("WETH/USDC/500")

        result = valuer._reprice_lp_on_chain_enriched(position, "arbitrum", market)

        # The repricing should still succeed (price-ratio fallback fills tick).
        assert result is not None
        # The guard's only job: slot0 must NEVER see the descriptor.
        valuer._lp_reader.read_pool_slot0.assert_not_called()

    def test_legacy_path_rejects_descriptor_under_pool_key(self):
        """Same guard, second call site (`_reprice_lp_on_chain`)."""
        valuer, position, market = _build_repriced_lp_inputs("WETH/USDC/500")

        result = valuer._reprice_lp_on_chain(position, "arbitrum", market)

        assert result is not None
        valuer._lp_reader.read_pool_slot0.assert_not_called()

    def test_enriched_path_accepts_pool_address_when_hex(self):
        """A real address under ``pool_address`` (or ``pool``) reaches slot0."""
        valid_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # WETH/USDC 500 Arbitrum
        valuer, position, market = _build_repriced_lp_inputs(valid_pool)

        result = valuer._reprice_lp_on_chain_enriched(position, "arbitrum", market)

        assert result is not None
        # slot0 IS reached with the valid hex address.
        valuer._lp_reader.read_pool_slot0.assert_called_once_with("arbitrum", valid_pool)

    def test_pool_address_key_preferred_over_pool(self):
        """When both keys are present, ``pool_address`` wins (matches VIB-3943 convention)."""
        valuer, position, market = _build_repriced_lp_inputs("WETH/USDC/500")
        valid_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        position.details["pool_address"] = valid_pool

        result = valuer._reprice_lp_on_chain_enriched(position, "arbitrum", market)

        assert result is not None
        # Address from ``pool_address`` reached slot0; descriptor under ``pool``
        # was ignored.
        valuer._lp_reader.read_pool_slot0.assert_called_once_with("arbitrum", valid_pool)
