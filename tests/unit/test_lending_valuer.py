"""Tests for lending valuation: pure math, hex parsing, and portfolio integration.

Covers:
- lending_valuer.py: value_lending_position, value_lending_portfolio
- lending_position_reader.py: hex parsing, reader with/without gateway
- portfolio_valuer.py: lending position repricing integration
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.valuation.lending_position_reader import (
    LendingPositionOnChain,
    LendingPositionReader,
    _decode_uint_hex,
    _pad_address,
    _parse_user_reserve_data_hex,
)
from almanak.framework.valuation.lending_valuer import (
    value_lending_portfolio,
    value_lending_position,
)

# =============================================================================
# TestValueLendingPosition — pure math
# =============================================================================


class TestValueLendingPosition:
    """Pure math tests for value_lending_position.

    USD outputs are MeasuredMoney (VIB-5216): a measured value's ``.value`` is the
    Decimal; a price-unavailable position is unmeasured, never a fabricated $0.
    """

    def test_supply_only(self):
        """Position with supply, no debt."""
        result = value_lending_position(
            atoken_balance=1_500_000_000,  # 1500 USDC (6 decimals)
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            collateral_enabled=True,
            asset="USDC",
        )
        assert result.supply_balance == Decimal("1500")
        assert result.supply_value_usd.value == Decimal("1500")
        assert result.debt_value_usd.value == Decimal("0")
        assert result.net_value_usd.value == Decimal("1500")
        assert result.collateral_enabled is True

    def test_borrow_only(self):
        """Position with only variable debt, no supply."""
        result = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH (18 decimals)
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        assert result.supply_balance == Decimal("0")
        assert result.supply_value_usd.value == Decimal("0")
        assert result.variable_debt_balance == Decimal("0.5")
        assert result.debt_value_usd.value == Decimal("1500")
        assert result.net_value_usd.value == Decimal("-1500")

    def test_supply_and_borrow(self):
        """Typical Aave position: supply collateral, borrow against it."""
        result = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH supplied
            stable_debt=0,
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH borrowed
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        assert result.supply_value_usd.value == Decimal("3000")
        assert result.debt_value_usd.value == Decimal("1500")
        assert result.net_value_usd.value == Decimal("1500")

    def test_stable_and_variable_debt(self):
        """Position with both stable and variable debt."""
        result = value_lending_position(
            atoken_balance=10_000_000_000,  # 10000 USDC supplied
            stable_debt=2_000_000_000,  # 2000 USDC stable debt
            variable_debt=3_000_000_000,  # 3000 USDC variable debt
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            asset="USDC",
        )
        assert result.stable_debt_balance == Decimal("2000")
        assert result.variable_debt_balance == Decimal("3000")
        assert result.debt_value_usd.value == Decimal("5000")
        assert result.net_value_usd.value == Decimal("5000")  # 10000 - 5000

    def test_zero_everything(self):
        """Empty position — a genuine measured zero (price WAS available)."""
        result = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
        )
        assert result.net_value_usd.is_measured
        assert result.net_value_usd.value == Decimal("0")
        assert result.supply_value_usd.value == Decimal("0")
        assert result.debt_value_usd.value == Decimal("0")

    def test_wbtc_8_decimals(self):
        """WBTC with 8 decimals."""
        result = value_lending_position(
            atoken_balance=50_000_000,  # 0.5 WBTC
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("60000"),
            token_decimals=8,
            asset="WBTC",
        )
        assert result.supply_balance == Decimal("0.5")
        assert result.supply_value_usd.value == Decimal("30000")

    def test_underwater_position(self):
        """Net value is negative when debt > supply (same asset borrow)."""
        result = value_lending_position(
            atoken_balance=100_000_000,  # 100 USDC
            stable_debt=0,
            variable_debt=200_000_000,  # 200 USDC debt
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
        )
        assert result.net_value_usd.value == Decimal("-100")

    def test_frozen_dataclass(self):
        """LendingPositionValue is immutable."""
        result = value_lending_position(
            atoken_balance=1_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
        )
        with pytest.raises(AttributeError):
            result.net_value_usd = Decimal("999")

    # -- VIB-5216: price-unavailable ⇒ unmeasured, never a fabricated $0 --------

    def test_price_unavailable_yields_unmeasured_not_zero(self):
        """A price-unavailable reserve is unmeasured (the #2866 placeholder class
        is closed) — the USD legs must NOT be a fabricated measured zero even
        though the position holds a real, non-zero balance."""
        result = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH supplied
            stable_debt=0,
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH borrowed
            token_price_usd=None,  # price could not be fetched
            token_decimals=18,
            asset="WETH",
        )
        # Balances are still measured (on-chain read succeeded) ...
        assert result.supply_balance == Decimal("1")
        assert result.variable_debt_balance == Decimal("0.5")
        # ... but the USD valuations are unmeasured, NOT Decimal("0").
        assert result.supply_value_usd.is_unmeasured
        assert result.debt_value_usd.is_unmeasured
        assert result.net_value_usd.is_unmeasured
        assert result.net_value_usd.value_or(Decimal("-1")) == Decimal("-1")

    def test_unmeasured_price_distinct_from_measured_zero_price(self):
        """Empty≠Zero at the valuation boundary: a measured $0 price is a measured
        value; an absent price is unmeasured."""
        zero_priced = value_lending_position(
            atoken_balance=1_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("0"),  # measured: the token is worth $0
            token_decimals=6,
        )
        assert zero_priced.supply_value_usd.is_measured
        assert zero_priced.supply_value_usd.value == Decimal("0")

        unpriced = value_lending_position(
            atoken_balance=1_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=None,  # unmeasured: price unavailable
            token_decimals=6,
        )
        assert unpriced.supply_value_usd.is_unmeasured


class TestValueLendingPortfolio:
    """Tests for aggregating multiple lending positions (MeasuredMoney totals)."""

    def test_single_position(self):
        """Portfolio with one position."""
        pos = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
        )
        total_supply, total_debt, total_net = value_lending_portfolio([pos])
        assert total_supply.value == Decimal("3000")
        assert total_debt.value == Decimal("0")
        assert total_net.value == Decimal("3000")

    def test_multi_asset_portfolio(self):
        """Portfolio: supply WETH, borrow USDC."""
        weth_supply = value_lending_position(
            atoken_balance=2_000_000_000_000_000_000,  # 2 WETH supplied
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        usdc_borrow = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=3_000_000_000,  # 3000 USDC borrowed
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            asset="USDC",
        )
        total_supply, total_debt, total_net = value_lending_portfolio([weth_supply, usdc_borrow])
        assert total_supply.value == Decimal("6000")  # 2 WETH * 3000
        assert total_debt.value == Decimal("3000")  # 3000 USDC
        assert total_net.value == Decimal("3000")

    def test_empty_portfolio(self):
        """Empty portfolio — measured zero (seeded with measured(0))."""
        total_supply, total_debt, total_net = value_lending_portfolio([])
        assert total_supply.is_measured
        assert total_supply.value == Decimal("0")
        assert total_debt.value == Decimal("0")
        assert total_net.value == Decimal("0")

    def test_one_unmeasured_leg_poisons_total(self):
        """Empty≠Zero propagation: a single unmeasured (price-unavailable) leg
        makes the portfolio totals unmeasured rather than silently summing a
        fabricated zero into a measured total."""
        priced = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH supplied
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        unpriced = value_lending_position(
            atoken_balance=1_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=None,
            token_decimals=6,
            asset="USDC",
        )
        total_supply, total_debt, total_net = value_lending_portfolio([priced, unpriced])
        assert total_supply.is_unmeasured
        assert total_net.is_unmeasured


# =============================================================================
# TestLendingPositionReader — hex parsing
# =============================================================================


class TestPadAddress:
    """Tests for _pad_address helper."""

    def test_with_0x_prefix(self):
        result = _pad_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert len(result) == 64
        assert result == "000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"

    def test_without_prefix(self):
        result = _pad_address("af88d065e77c8cC2239327C5EDb3A432268e5831")
        assert len(result) == 64


class TestDecodeUintHex:
    """Tests for _decode_uint_hex."""

    def test_zero(self):
        data = "0" * 64  # 1 word of zeros
        assert _decode_uint_hex(data, 0) == 0

    def test_one(self):
        data = "0" * 63 + "1"
        assert _decode_uint_hex(data, 0) == 1

    def test_second_word(self):
        data = "0" * 64 + "0" * 63 + "a"  # 2 words, second = 10
        assert _decode_uint_hex(data, 1) == 10


class TestParseUserReserveDataHex:
    """Tests for _parse_user_reserve_data_hex."""

    def _build_hex_response(
        self,
        atoken_balance: int = 0,
        stable_debt: int = 0,
        variable_debt: int = 0,
        principal_stable_debt: int = 0,
        scaled_variable_debt: int = 0,
        stable_borrow_rate: int = 0,
        liquidity_rate: int = 0,
        stable_rate_last_updated: int = 0,
        collateral_enabled: bool = True,
    ) -> str:
        """Build a mock hex response for getUserReserveData."""
        words = [
            atoken_balance,
            stable_debt,
            variable_debt,
            principal_stable_debt,
            scaled_variable_debt,
            stable_borrow_rate,
            liquidity_rate,
            stable_rate_last_updated,
            1 if collateral_enabled else 0,
        ]
        return "0x" + "".join(hex(w)[2:].zfill(64) for w in words)

    def test_supply_only_position(self):
        """Parse a position with only supply."""
        hex_data = self._build_hex_response(
            atoken_balance=1_500_000_000,  # 1500 USDC
            collateral_enabled=True,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert result is not None
        assert result.current_atoken_balance == 1_500_000_000
        assert result.current_stable_debt == 0
        assert result.current_variable_debt == 0
        assert result.usage_as_collateral_enabled is True
        assert result.is_active is True

    def test_supply_and_debt(self):
        """Parse a position with supply and variable debt."""
        hex_data = self._build_hex_response(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH
            liquidity_rate=30_000_000_000_000_000_000_000_000,  # 3% APY in ray
        )
        result = _parse_user_reserve_data_hex(hex_data, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
        assert result is not None
        assert result.current_atoken_balance == 1_000_000_000_000_000_000
        assert result.current_variable_debt == 500_000_000_000_000_000
        assert result.total_debt == 500_000_000_000_000_000
        assert result.liquidity_rate == 30_000_000_000_000_000_000_000_000

    def test_empty_position(self):
        """Parse a position with no supply or debt."""
        hex_data = self._build_hex_response()
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.is_active is False

    def test_collateral_disabled(self):
        """Parse a position with collateral disabled."""
        hex_data = self._build_hex_response(
            atoken_balance=1_000_000,
            collateral_enabled=False,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.usage_as_collateral_enabled is False

    def test_too_short_hex(self):
        """Hex response too short should return None."""
        result = _parse_user_reserve_data_hex("0x" + "00" * 10, "0xaddr")
        assert result is None

    def test_stable_and_variable_debt(self):
        """Parse a position with both stable and variable debt."""
        hex_data = self._build_hex_response(
            stable_debt=100_000_000,
            variable_debt=200_000_000,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.total_debt == 300_000_000


class TestLendingPositionReaderIntegration:
    """Tests for LendingPositionReader with mocked gateway."""

    def test_no_gateway_returns_none(self):
        """Without gateway client, read_position returns None."""
        reader = LendingPositionReader(gateway_client=None)
        result = reader.read_position("arbitrum", "0xaddr", "0xwallet")
        assert result is None

    def test_unknown_chain_returns_none(self):
        """Unknown chain returns None."""
        reader = LendingPositionReader(gateway_client=MagicMock())
        result = reader.read_position("solana", "0xaddr", "0xwallet")
        assert result is None

    def test_read_positions_filters_inactive(self):
        """read_positions only returns active positions."""
        reader = LendingPositionReader(gateway_client=None)
        result = reader.read_positions("arbitrum", ["0xa", "0xb"], "0xwallet")
        assert result == []


# =============================================================================
# TestPortfolioValuerLendingRepricing
# =============================================================================


class TestPortfolioValuerLendingRepricing:
    """Integration tests for lending repricing in PortfolioValuer."""

    def _make_position(self, position_type, **kwargs):
        """Create a PositionInfo for testing."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "test-position",
            "chain": "arbitrum",
            "protocol": "aave_v3",
            "value_usd": Decimal("999"),
            "details": {},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def test_supply_falls_back_without_gateway(self):
        """Without gateway, SUPPLY position uses strategy-reported value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("SUPPLY", value_usd=Decimal("5000"))
        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_borrow_falls_back_without_gateway(self):
        """Without gateway, BORROW fallback negates positive value_usd."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("BORROW", value_usd=Decimal("2000"))
        result = valuer._reprice_position(position, "arbitrum", market)
        # BORROW fallback normalizes to negative so debt reduces portfolio
        assert result == Decimal("-2000")

    def test_token_position_passes_through(self):
        """TOKEN position always uses strategy-reported value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("TOKEN", value_usd=Decimal("1234"))
        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("1234")

    def test_lending_repricing_with_mocked_on_chain_data(self):
        """Full lending re-pricing with mocked on-chain + market data."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        # Mock the lending reader to return on-chain data
        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=5_000_000_000,  # 5000 USDC
            current_stable_debt=0,
            current_variable_debt=1_000_000_000,  # 1000 USDC debt
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        # Mock market
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=6,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # Expected: supply 5000 - debt 1000 = 4000 (net, for SUPPLY position)
        assert result == Decimal("4000")

    def test_borrow_position_returns_debt_value(self):
        """BORROW position returns the debt value, not net."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=2_000_000_000,  # 2000 USDC debt
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "BORROW",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=6,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # BORROW returns negative so it reduces portfolio total when summed
        assert result == Decimal("-2000")

    def test_empty_position_returns_zero(self):
        """Inactive lending position returns 0."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
            },
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("0")

    def test_unknown_decimals_falls_back(self):
        """If decimals can't be resolved, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaddr",
            current_atoken_balance=1_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaddr_long_enough_to_be_valid_40chars",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "UNKNOWN_TOKEN",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=None,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # Falls back to strategy-reported value
        assert result == Decimal("999")

    def test_missing_asset_address_falls_back(self):
        """If no asset_address in details, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("5000"),
            details={"wallet": "0x1234567890abcdef1234567890abcdef12345678"},
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_missing_wallet_falls_back(self):
        """If no wallet in details, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("5000"),
            details={"asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_weth_supply_with_exact_values(self):
        """Deterministic: 2 WETH supplied at $3000 = $6000."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            current_atoken_balance=2_000_000_000_000_000_000,  # 2 WETH
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("3000")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "WETH",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=18,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        assert result == Decimal("6000")


# =============================================================================
# TestExtractAssetAddress
# =============================================================================


class TestExtractAssetAddress:
    """Tests for _extract_asset_address static method."""

    def test_asset_address_key(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )
        assert PortfolioValuer._extract_asset_address(pos) == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_underlying_key(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"underlying": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )
        assert PortfolioValuer._extract_asset_address(pos) == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_no_address_returns_none(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={},
        )
        assert PortfolioValuer._extract_asset_address(pos) is None

    def test_short_string_ignored(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset_address": "USDC"},  # Too short to be an address
        )
        assert PortfolioValuer._extract_asset_address(pos) is None


# =============================================================================
# TestDashboardMigration
# =============================================================================


class TestDashboardMigrationRemoval:
    """Verify the migration fallback was properly removed."""

    def test_no_extract_portfolio_value_from_state(self):
        """The old _extract_portfolio_value_from_state method should not exist."""
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer

        assert not hasattr(DashboardServiceServicer, "_extract_portfolio_value_from_state")


# =============================================================================
# TestLendingValuationProtocolRouting — Spark data-provider routing
# =============================================================================

# Ethereum single-reserve data providers, sourced from each connector's
# addresses.py. DISTINCT per protocol — repricing must query the SAME protocol
# the position belongs to, never silently default Spark to Aave V3.
_ETH_AAVE_DATA_PROVIDER = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"
_ETH_SPARK_DATA_PROVIDER = "0xFc21d6d146E6086B8359705C8b28512a983db0cb"


def _gateway_capturing_eth_call_target(captured: list[str], supply_wei: int = 1_000_000):
    """Fake gateway whose ``_rpc_stub.Call`` records each eth_call target.

    Records ``params[0]["to"]`` (the contract queried) and returns a valid
    9-word ``getUserReserveData`` response so the repricing path runs through
    the real ``LendingPositionReader``.
    """

    def _call(request, timeout=None):
        params = json.loads(request.params)
        captured.append(params[0]["to"])
        hex_payload = "0x" + f"{supply_wei:064x}" + "0" * (64 * 8)
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps(hex_payload)
        return resp

    stub = MagicMock()
    stub.Call.side_effect = _call
    gw = MagicMock()
    gw._rpc_stub = stub
    gw.config = SimpleNamespace(timeout=7)
    return gw


class TestLendingValuationProtocolRouting:
    """Regression (follow-up to PR #2533): both lending repricing paths must
    thread ``position.protocol`` to the on-chain read so a Spark
    position is priced against ITS data provider, not Aave V3's.

    Exercises the real ``PortfolioValuer`` -> ``LendingPositionReader`` ->
    ``LendingReadRegistry`` -> ``AddressRegistry`` chain for BOTH valuation
    call sites (``_reprice_lending_on_chain_enriched`` and
    ``_reprice_lending_on_chain``).
    """

    _WALLET = "0x" + "1" * 40
    _USDC = "0x" + "a" * 40
    _METHODS = ("_reprice_lending_on_chain_enriched", "_reprice_lending_on_chain")

    def _position(self, protocol):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        return PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="lending-routing-test",
            chain="ethereum",
            protocol=protocol,
            value_usd=Decimal("0"),
            details={"asset_address": self._USDC, "wallet": self._WALLET, "asset": "USDC"},
        )

    def _captured_target(self, method, protocol):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        captured: list[str] = []
        valuer = PortfolioValuer(gateway_client=_gateway_capturing_eth_call_target(captured))
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        getattr(valuer, method)(self._position(protocol), "ethereum", market)
        return captured

    @pytest.mark.parametrize("method", _METHODS)
    def test_spark_position_queries_spark_provider_not_aave(self, method):
        captured = self._captured_target(method, "spark")
        assert captured, f"{method} made no eth_call"
        assert captured[0].lower() == _ETH_SPARK_DATA_PROVIDER.lower()
        assert captured[0].lower() != _ETH_AAVE_DATA_PROVIDER.lower()

    @pytest.mark.parametrize("method", _METHODS)
    def test_aave_position_queries_aave_provider(self, method):
        captured = self._captured_target(method, "aave_v3")
        assert captured, f"{method} made no eth_call"
        assert captured[0].lower() == _ETH_AAVE_DATA_PROVIDER.lower()
        assert captured[0].lower() != _ETH_SPARK_DATA_PROVIDER.lower()


# =============================================================================
# TestVib5006LendingTrackCEnrichment — HF / supply_apy_pct / borrow_balance
# =============================================================================


class TestVib5006LendingTrackCEnrichment:
    """VIB-5006: the lending Track-C fields ``_materialise_lending`` reads but
    that were never populated — ``supply_apy_pct`` + ``borrow_balance`` (from the
    per-reserve read) and account-level ``health_factor`` (from the account-state
    read). Closes Accountant L2/L3/L5 for the Aave family."""

    def _make_position(self, position_type, **kwargs):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "test-position",
            "chain": "arbitrum",
            "protocol": "aave_v3",
            "value_usd": Decimal("999"),
            "details": {},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def _valuer_with_on_chain(self, on_chain):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = on_chain
        return valuer

    # --- Part 1: per-reserve enriched dict (supply_apy_pct + borrow_balance) ---

    def test_enriched_dict_stamps_supply_apy_and_borrow_balance(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        # liquidity_rate = 3e25 ray → 3e25 / 1e27 * 100 = 3.00% supply APY
        on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=5_000_000_000,  # 5000 USDC (6 dp)
            current_stable_debt=0,
            current_variable_debt=1_000_000_000,  # 1000 USDC debt
            liquidity_rate=30_000_000_000_000_000_000_000_000,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_on_chain(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "SUPPLY",
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )
        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=6):
            result = valuer._reprice_lending_on_chain_enriched(position, "arbitrum", market)

        assert result is not None
        _value, enriched = result
        assert Decimal(enriched["supply_apy_pct"]) == Decimal("3")
        assert Decimal(enriched["borrow_balance"]) == Decimal("1000")
        # health_factor is account-level — set in _get_positions, NOT here. The
        # old code stamped a perpetually-None HF via a bogus hasattr; assert the
        # fabricated key is gone.
        assert "health_factor" not in enriched

    def test_enriched_supply_apy_is_measured_zero_not_absent(self):
        """A genuine 0 liquidity_rate (read succeeded) ⇒ "0", never absent
        (Empty ≠ Zero)."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=5_000_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer_with_on_chain(on_chain)
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        position = self._make_position(
            "SUPPLY",
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )
        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=6):
            _value, enriched = valuer._reprice_lending_on_chain_enriched(position, "arbitrum", market)
        # Present + non-empty (the L5 gate) + parses to a measured 0 (not absent,
        # not None — Empty ≠ Zero). The raw string may render as "0E-27"; what
        # matters is _materialise_lending's _dec() reads it back as 0.
        assert enriched["supply_apy_pct"] not in (None, "")
        assert Decimal(enriched["supply_apy_pct"]) == Decimal("0")
        assert Decimal(enriched["borrow_balance"]) == Decimal("0")  # measured zero

    # --- Part 2: account-level health_factor enrichment ---

    def _account_state(self, hf):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        return LendingAccountState(
            collateral_usd=Decimal("6"),
            debt_usd=Decimal("1.8"),
            health_factor=hf,
            liquidation_threshold_bps=8500,
            e_mode_category=0,
        )

    def test_health_factor_stamped_for_aave_lending_leg(self, monkeypatch):
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("SUPPLY", details={"wallet": "0xWALLET"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **_kw: self._account_state(Decimal("2.6026")),
        )
        cache: dict = {}
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {"k": "v"}, cache, None)
        assert out["health_factor"] == "2.6026"
        assert out["k"] == "v"  # original details preserved

    def test_failed_read_stamps_explicit_none(self, monkeypatch):
        """Attempted-but-None read ⇒ explicit health_factor=None (measured-
        unmeasured), never a fabricated 0 — Empty ≠ Zero."""
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("BORROW", details={"wallet": "0xWALLET"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **_kw: None,
        )
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {}, {}, None)
        assert out["health_factor"] is None

    def test_none_hf_value_stamps_explicit_none(self, monkeypatch):
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("SUPPLY", details={"wallet": "0xWALLET"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **_kw: self._account_state(None),
        )
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {}, {}, None)
        assert out["health_factor"] is None

    def test_failed_read_overrides_stale_strategy_hf(self, monkeypatch):
        """A failed read must NOT let a stale strategy-reported HF survive —
        it stamps None so the merge can't pass a stale value off as live
        (VIB-5084 class)."""
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("BORROW", details={"wallet": "0xWALLET"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **_kw: None,
        )
        # enriched_details already carries a (stale) HF — it must be overridden.
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {"health_factor": "9.99"}, {}, None)
        assert out["health_factor"] is None

    def test_non_lending_position_skips_read(self, monkeypatch):
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("TOKEN", details={"wallet": "0xWALLET"})
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: calls.append(kw),
        )
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {}, {}, None)
        assert "health_factor" not in out
        assert calls == []  # no read for non-lending positions

    def test_morpho_per_market_reads_account_state_with_price_injection(self, monkeypatch):
        """VIB-4551: a Morpho leg (per-market — publishes a market table but no
        market-health reader) now reads the aggregate account state scoped by
        market_id (with the market's token prices injected), no longer skipped.
        HF is stamped.

        APY is None here because ``market_id="0xabc"`` is NOT a registered
        Morpho market, so the rate seam cannot establish the leg's role and
        fails closed (VIB-5729) — NOT because Morpho lacks a live rate (that
        blocker, VIB-5040, has shipped; see the market-scoped tests below)."""
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        valuer = self._valuer_with_on_chain(None)
        position = self._make_position(
            "BORROW", protocol="morpho_blue", details={"wallet": "0xWALLET", "market_id": "0xabc"}
        )
        captured: dict = {}

        def _as(**kw):
            captured.update(kw)
            return LendingAccountState(
                collateral_usd=Decimal("100"),
                debt_usd=Decimal("40"),
                health_factor=Decimal("1.95"),
                liquidation_threshold_bps=None,
                e_mode_category=None,
                lltv=Decimal("0.86"),
            )

        monkeypatch.setattr("almanak.framework.accounting.lending_reads.read_lending_account_state", _as)
        market = MagicMock()
        market.price.return_value = Decimal("2000")
        out = valuer._enrich_lending_trackc_fields(position, "ethereum", {}, {}, market)
        assert out["health_factor"] == "1.95"
        assert captured["market_id"] == "0xabc"  # per-market scoped read
        # Unknown market => role unresolvable => EXPLICIT None (key present) so a
        # stale strategy-reported APY can't survive the merge, never fabricated.
        assert "supply_apy_pct" in out and out["supply_apy_pct"] is None
        assert "borrow_apy_pct" in out and out["borrow_apy_pct"] is None

    def test_morpho_per_market_without_market_id_fails_closed(self, monkeypatch):
        """A per-market protocol with no resolvable market id cannot be scoped ⇒
        no read, AND an explicit health_factor=None stamped so a stale
        strategy-reported HF cannot survive the merge (Empty ≠ Zero / VIB-5084)."""
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("BORROW", protocol="morpho_blue", details={"wallet": "0xWALLET"})
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: calls.append(kw),
        )
        # A stale strategy-reported HF must be overridden, not preserved.
        out = valuer._enrich_lending_trackc_fields(position, "ethereum", {"health_factor": "9.99"}, {}, MagicMock())
        assert out["health_factor"] is None
        assert calls == []  # no read issued without a market to scope it

    def test_benqi_excluded_from_per_market_priced_read(self, monkeypatch):
        """BENQI publishes a market table but declares NO valuation roles (it needs
        a different collaterals-map injection), so it must NOT route through the
        priced per-market read — which would fail closed forever and only *look*
        wired. The dispatch gates on declares_valuation_roles, so a benqi leg's
        market id is forced None and it never issues a market-scoped read."""
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position(
            "BORROW", protocol="benqi", details={"wallet": "0xWALLET", "market_id": "0xbenqimkt"}
        )
        as_calls: list = []
        mh_calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: as_calls.append(kw),
        )
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **kw: mh_calls.append(kw),
        )
        valuer._enrich_lending_trackc_fields(position, "avalanche", {}, {}, MagicMock())
        assert mh_calls == []  # benqi has no market-health reader
        # Never scoped to the benqi market id — the per-market priced read is not taken.
        assert all(kw.get("market_id") is None for kw in as_calls)

    def test_account_state_read_cached_across_legs(self, monkeypatch):
        """Both legs of a loop (same protocol/chain/wallet) share ONE read."""
        valuer = self._valuer_with_on_chain(None)
        supply = self._make_position("SUPPLY", details={"wallet": "0xWALLET"})
        borrow = self._make_position("BORROW", details={"wallet": "0xWALLET"})
        read_count = {"n": 0}

        def _counting_read(**_kw):
            read_count["n"] += 1
            return self._account_state(Decimal("2.6"))

        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            _counting_read,
        )
        cache: dict = {}
        out_s = valuer._enrich_lending_trackc_fields(supply, "arbitrum", {}, cache, None)
        out_b = valuer._enrich_lending_trackc_fields(borrow, "arbitrum", {}, cache, None)
        assert out_s["health_factor"] == "2.6"
        assert out_b["health_factor"] == "2.6"
        assert read_count["n"] == 1  # cached per (protocol, chain, wallet)

    def test_account_state_cache_is_case_insensitive_on_wallet(self, monkeypatch):
        """A checksummed vs lowercase spelling of the same wallet shares ONE
        cached read — EVM addresses are case-insensitive (Gemini)."""
        valuer = self._valuer_with_on_chain(None)
        checksummed = self._make_position("SUPPLY", details={"wallet": "0xAbCdEf0000000000000000000000000000000001"})
        lowercased = self._make_position("BORROW", details={"wallet": "0xabcdef0000000000000000000000000000000001"})
        read_count = {"n": 0}

        def _counting_read(**_kw):
            read_count["n"] += 1
            return self._account_state(Decimal("2.6"))

        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            _counting_read,
        )
        cache: dict = {}
        valuer._enrich_lending_trackc_fields(checksummed, "arbitrum", {}, cache, None)
        valuer._enrich_lending_trackc_fields(lowercased, "arbitrum", {}, cache, None)
        assert read_count["n"] == 1  # case-normalised cache key

    def test_no_gateway_skips_read(self, monkeypatch):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        position = self._make_position("SUPPLY", details={"wallet": "0xWALLET"})
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: calls.append(kw),
        )
        out = valuer._enrich_lending_trackc_fields(position, "arbitrum", {}, {}, None)
        assert "health_factor" not in out
        assert calls == []


class TestCompoundV3LendingTrackCEnrichment:
    """VIB-5160: the shared lending Track-C seam stamps the same observability
    fields for Compound V3 (Comet) that VIB-5006 stamps for the Aave family —
    dispatching on connector capability, never a protocol-name if/elif. HF comes
    from the summed multi-collateral ``read_lending_market_health`` (not the
    single-leg account-state read), and supply/borrow APY from the gateway-routed
    ``market.lending_rate``. Closes Accountant L2/L3/L5 for Compound V3."""

    def _make_position(self, position_type, **kwargs):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "test-position",
            "chain": "base",
            "protocol": "compound_v3",
            "value_usd": Decimal("999"),
            "details": {"wallet": "0xWALLET", "market_id": "usdc", "asset": "USDC"},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def _valuer(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        return PortfolioValuer(gateway_client=MagicMock())

    def _market_state(self, hf):
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

        return LendingAccountState(
            collateral_usd=Decimal("100"),
            debt_usd=Decimal("40"),
            health_factor=hf,
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=Decimal("0.83"),
        )

    def _market(self, supply_apy="4.5", borrow_apy="6.1"):
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        def _rate(protocol, token, side, *, chain=None):
            pct = Decimal(supply_apy) if side == "supply" else Decimal(borrow_apy)
            return SimpleNamespace(apy_percent=pct)

        market.lending_rate.side_effect = _rate
        return market

    def test_compound_market_health_hf_and_apy_stamped(self, monkeypatch):
        """A Comet leg gets HF from the summed market-health read and supply +
        borrow APY from ``market.lending_rate`` — all measured, none fabricated.
        The APY token is the Comet's BASE SYMBOL ("USDC"), resolved from the
        connector market table — NOT the lowercase market key ("usdc"), which the
        case-sensitive rate provider would reject (the Anvil-proven failure mode)."""
        valuer = self._valuer()
        position = self._make_position("BORROW")  # market_id "usdc"
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **_kw: self._market_state(Decimal("2.5")),
        )
        rate_tokens: list[str] = []
        market = self._market()
        orig = market.lending_rate.side_effect

        def _capture(protocol, token, side, *, chain=None):
            rate_tokens.append(token)
            return orig(protocol, token, side, chain=chain)

        market.lending_rate.side_effect = _capture
        out = valuer._enrich_lending_trackc_fields(position, "base", {}, {}, market)
        assert out["health_factor"] == "2.5"
        assert Decimal(out["supply_apy_pct"]) == Decimal("4.5")
        assert Decimal(out["borrow_apy_pct"]) == Decimal("6.1")
        assert rate_tokens == ["USDC", "USDC"]  # base symbol, not the "usdc" market key

    def test_compound_dispatch_uses_market_health_not_account_state(self, monkeypatch):
        """Capability dispatch: Compound publishes a market-health reader, so HF
        MUST come from it — the single-leg account-state read (a wrong
        one-collateral HF) is NEVER taken for Compound."""
        valuer = self._valuer()
        position = self._make_position("SUPPLY")
        mh_calls: list = []
        as_calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **kw: (mh_calls.append(kw) or self._market_state(Decimal("3.0"))),
        )
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: as_calls.append(kw),
        )
        out = valuer._enrich_lending_trackc_fields(position, "base", {}, {}, self._market())
        assert out["health_factor"] == "3.0"
        assert len(mh_calls) == 1
        assert as_calls == []  # account-state path NOT taken for a market-health protocol

    def test_compound_failed_market_health_stamps_none_hf(self, monkeypatch):
        """Attempted-but-None market-health read ⇒ explicit health_factor=None
        (Empty ≠ Zero), never a fabricated healthy value."""
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"wallet": "0xWALLET", "market_id": "usdc", "asset": "USDC"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **_kw: None,
        )
        out = valuer._enrich_lending_trackc_fields(position, "base", {"health_factor": "9.99"}, {}, self._market())
        assert out["health_factor"] is None  # stale strategy HF overridden

    def test_compound_unavailable_rate_stamps_none_apy(self, monkeypatch):
        """An unavailable lending rate (gateway raises) ⇒ explicit None APY, never
        a fabricated rate. HF is still stamped from the (successful) health read."""
        valuer = self._valuer()
        position = self._make_position("SUPPLY")
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **_kw: self._market_state(Decimal("2.0")),
        )
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        market.lending_rate.side_effect = ValueError("RateHistoryUnavailable")
        out = valuer._enrich_lending_trackc_fields(position, "base", {}, {}, market)
        assert out["health_factor"] == "2.0"
        assert out["supply_apy_pct"] is None
        assert out["borrow_apy_pct"] is None

    def test_compound_strategy_reported_leg_resolves_market_and_wallet(self, monkeypatch):
        """A strategy-reported Comet leg carries ``market`` (not ``market_id``) and
        NO owner — Compound has no single-reserve discovery spec, so the leg never
        gets a wallet on-chain. The stamp must resolve the market key from
        ``market`` and the owner from the deployment wallet, then fire the read.
        (Regression for the Anvil finding: all-NULL Compound Track-C rows.)"""
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"asset": "WETH", "market": "usdc", "type": "collateral"})
        captured: dict = {}

        def _mh(**kw):
            captured.update(kw)
            return self._market_state(Decimal("2.7"))

        monkeypatch.setattr("almanak.framework.accounting.lending_reads.read_lending_market_health", _mh)
        out = valuer._enrich_lending_trackc_fields(
            position, "base", {}, {}, self._market(), strategy_wallet="0xDEPLOYMENTWALLET"
        )
        assert out["health_factor"] == "2.7"
        assert captured["market_id"] == "usdc"  # resolved from details["market"]
        assert captured["wallet_address"] == "0xdeploymentwallet"  # deployment-wallet fallback, lowercased

    def test_compound_no_wallet_and_no_strategy_wallet_skips(self, monkeypatch):
        """No owner anywhere ⇒ no read, details untouched (never a fabricated HF)."""
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"asset": "WETH", "market": "usdc"})
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            lambda **kw: calls.append(kw),
        )
        out = valuer._enrich_lending_trackc_fields(position, "base", {}, {}, self._market())
        assert "health_factor" not in out
        assert calls == []

    def test_compound_market_health_cached_across_legs(self, monkeypatch):
        """Both legs of a Comet loop (same protocol/chain/wallet/market_id) share
        ONE market-health read."""
        valuer = self._valuer()
        supply = self._make_position("SUPPLY")
        borrow = self._make_position("BORROW")
        read_count = {"n": 0}

        def _counting(**_kw):
            read_count["n"] += 1
            return self._market_state(Decimal("2.4"))

        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_market_health",
            _counting,
        )
        cache: dict = {}
        market = self._market()
        out_s = valuer._enrich_lending_trackc_fields(supply, "base", {}, cache, market)
        out_b = valuer._enrich_lending_trackc_fields(borrow, "base", {}, cache, market)
        assert out_s["health_factor"] == "2.4"
        assert out_b["health_factor"] == "2.4"
        assert read_count["n"] == 1  # cached per (protocol, chain, wallet, market_id)


# =============================================================================
# TestVib5417SparkRepricerUnmeasured — wallet plumbing + Empty≠Zero marker
# =============================================================================


class TestVib5417SparkRepricerUnmeasured:
    """VIB-5417: a lending leg that declares an on-chain read (e.g. ``spark``)
    must (1) reprice via the strategy wallet when ``details`` omits it, and
    (2) when it CANNOT be measured and the strategy reports no signal, mark the
    leg UNMEASURED (snapshot → UNAVAILABLE) instead of booking ``$0`` at HIGH.
    """

    def _make_position(self, position_type, *, protocol="spark", **kwargs):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "vib5417-pos",
            "chain": "ethereum",
            "protocol": protocol,
            "value_usd": Decimal("0"),
            "details": {},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def _valuer(self, on_chain):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = on_chain
        return valuer

    def test_spark_reprices_via_strategy_wallet_fallback(self):
        """(a) Details omit the wallet, but the valuer knows the strategy wallet:
        the read runs with that wallet and the leg reprices to a real NAV."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        on_chain = LendingPositionOnChain(
            asset_address="0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
            current_atoken_balance=4_000_000_000_000_000_000_000,  # 4000 DAI (18 dp)
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer = self._valuer(on_chain)
        valuer._strategy_wallet_address = "0x" + "9" * 40  # resolved at snapshot time
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        # No ``wallet`` key in details — only the asset (Spark's real shape).
        position = self._make_position(
            "SUPPLY",
            details={"asset_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "asset": "DAI"},
        )

        with patch.object(PortfolioValuer, "_get_token_decimals", return_value=18):
            value_usd, _details, repriced = valuer._reprice_position_enriched(position, "ethereum", market)

        assert repriced is True
        assert value_usd == Decimal("4000")  # measured on-chain, NOT the strategy's $0
        # The read used the strategy wallet supplied via the fallback.
        _args, kwargs = valuer._lending_reader.read_position.call_args
        assert kwargs["wallet_address"] == "0x" + "9" * 40

    def test_missing_inputs_no_signal_marks_unmeasured_not_zero_at_high(self):
        """(b) A read-declaring protocol whose inputs are missing AND whose
        strategy value carries no signal → unmeasured marker + repriced=False
        (snapshot drops to UNAVAILABLE), never a measured ``$0`` at HIGH."""
        valuer = self._valuer(on_chain=None)
        valuer._strategy_wallet_address = ""  # genuinely no wallet anywhere
        market = MagicMock()

        # details={} → no asset address resolvable; value_usd=0 → no signal.
        position = self._make_position("SUPPLY", value_usd=Decimal("0"), details={})

        value_usd, details, repriced = valuer._reprice_position_enriched(position, "ethereum", market)

        assert repriced is False, "no measurement + no signal must NOT be repriced=True"
        assert value_usd == Decimal("0")
        assert details.get("mark_unmeasured") is True
        assert details.get("valuation_status") == "no_path"
        assert details.get("unavailable_reason") == "unmeasured_on_chain_read"

    def test_borrow_missing_inputs_no_signal_marks_unmeasured(self):
        """BORROW companion to the above: value_usd==0 + no measurement → marker."""
        valuer = self._valuer(on_chain=None)
        valuer._strategy_wallet_address = ""
        market = MagicMock()

        position = self._make_position("BORROW", value_usd=Decimal("0"), details={})
        value_usd, details, repriced = valuer._reprice_position_enriched(position, "ethereum", market)

        assert (value_usd, repriced) == (Decimal("0"), False)
        assert details.get("mark_unmeasured") is True

    def test_nonzero_strategy_value_still_trusted_no_marker(self):
        """VIB-4584 preserved: a read-declaring lending leg with a signal-carrying
        strategy value (no on-chain measurement available) is STILL trusted at
        repriced=True — the VIB-5417 marker must not over-reach onto it."""
        valuer = self._valuer(on_chain=None)
        valuer._strategy_wallet_address = ""
        market = MagicMock()

        supply = self._make_position("SUPPLY", value_usd=Decimal("5000"), details={})
        v_s, d_s, r_s = valuer._reprice_position_enriched(supply, "ethereum", market)
        assert (v_s, r_s) == (Decimal("5000"), True)
        assert "mark_unmeasured" not in d_s

        borrow = self._make_position("BORROW", value_usd=Decimal("300"), details={})
        v_b, d_b, r_b = valuer._reprice_position_enriched(borrow, "ethereum", market)
        assert (v_b, r_b) == (Decimal("-300"), True)  # gross positive → negated
        assert "mark_unmeasured" not in d_b

    def test_none_protocol_does_not_crash_no_signal_marker(self):
        """Guard: a lending leg whose ``protocol`` is None must not raise from
        ``LendingReadRegistry.has`` (``_normalize`` -> ``.strip()``). It declares
        no on-chain read, so no marker is stamped — repriced=False carries the
        UNAVAILABLE signal."""
        valuer = self._valuer(on_chain=None)
        valuer._strategy_wallet_address = ""
        market = MagicMock()

        position = self._make_position("SUPPLY", protocol=None, value_usd=Decimal("0"), details={})
        value_usd, details, repriced = valuer._reprice_position_enriched(position, "ethereum", market)

        assert (value_usd, repriced) == (Decimal("0"), False)
        assert "mark_unmeasured" not in details  # no declared read -> no marker

    def test_value_snapshot_unavailable_for_unmeasured_spark_leg(self):
        """End-to-end: a value() snapshot over an unmeasurable Spark SUPPLY leg
        reports UNAVAILABLE confidence, not HIGH with a fabricated $0.

        Drives ``value()``'s REAL discovery path: positions arrive via
        ``get_open_positions()`` (a ``TeardownPositionSummary``) — NOT a bare
        ``get_positions`` the valuer never calls — and ``_get_tracked_tokens()``
        is present, so the snapshot does not bail early on an ``AttributeError``
        and actually reaches the Spark repricer. A spy on the repricer asserts the
        unmeasured leg was genuinely exercised (guards against the test passing
        UNAVAILABLE for the wrong reason)."""
        from datetime import UTC, datetime

        from almanak.framework.portfolio.models import ValueConfidence
        from almanak.framework.teardown.models import TeardownPositionSummary
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = None  # read produced nothing

        # Spy on the lending repricer to PROVE the Spark leg was actually reached.
        reprice_calls = {"n": 0}
        original_reprice = valuer._reprice_lending_on_chain_enriched

        def _spy_reprice(*args, **kwargs):
            reprice_calls["n"] += 1
            return original_reprice(*args, **kwargs)

        valuer._reprice_lending_on_chain_enriched = _spy_reprice

        position = self._make_position("SUPPLY", value_usd=Decimal("0"), details={})
        summary = TeardownPositionSummary(
            deployment_id="vib5417-strat",
            timestamp=datetime.now(UTC),
            positions=[position],
        )
        strategy = SimpleNamespace(
            chain="ethereum",
            wallet_address="",
            deployment_id="vib5417-strat",
            get_portfolio_snapshot=lambda *a, **k: None,
            get_open_positions=lambda *a, **k: summary,
            _get_tracked_tokens=lambda *a, **k: [],
        )
        # Balances/prices succeed (measured) so the ONLY driver of UNAVAILABLE is
        # the unmeasured Spark leg's ``no_path`` marker — not a failed wallet read.
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        market.balance.return_value = Decimal("0")

        snapshot = valuer.value(strategy, market)
        assert reprice_calls["n"] >= 1, "value() must actually reach the Spark repricer"
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE


class TestVib5729MorphoMarketScopedRates:
    """VIB-5729: isolated-market lending rates are read MARKET-SCOPED.

    Morpho Blue markets are isolated, so a rate belongs to a MARKET, not to a
    token. Several markets can lend the same loan token at very different rates
    (robinhood: USDG borrows at ~3.53% in USDe/USDG but ~2.77% in
    syrupUSDG/USDG). The seam must therefore ask for the position's OWN market,
    and must refuse any answer it cannot prove came from that market.

    These tests pin the three ways this can silently go wrong:
      1. Reading a token-keyed rate (wrong market's number, stamped as measured).
      2. Stamping the market's supply APY on a collateral leg (collateral earns 0).
      3. Trusting a rate from a gateway that ignored the market scoping.
    """

    # The real robinhood USDe/USDG market — collateral USDe, loan USDG.
    MARKET_ID = "0xc845da65a020ddca5f132efa8fea79676d8edfdea504226a4c01e7a9e34cddd6"
    OTHER_MARKET_ID = "0x919a9b6b94dae7c86620eaf7a08e597aae8a4c3a9e9c7671771fbaf62b6b61c7"

    def _valuer(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        return PortfolioValuer(gateway_client=MagicMock())

    def _make_position(self, position_type, **kwargs):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "morpho-test",
            "chain": "robinhood",
            "protocol": "morpho_blue",
            "value_usd": Decimal("20"),
            "details": {},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def _market(self, rate_by_market):
        """A market source whose lending_rate is market-scoped, like the real one.

        Raises if called WITHOUT a market_id — an isolated-market venue answering
        an unscoped query is the bug this suite exists to prevent.
        """
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        def _rate(protocol, token, side, chain=None, market_id=None):
            if not market_id:
                raise AssertionError(f"unscoped lending_rate for isolated market: {protocol}/{token}/{side}")
            return SimpleNamespace(apy_percent=rate_by_market[market_id][side])

        market.lending_rate.side_effect = _rate
        return market

    def _enrich(self, valuer, position, market):
        return valuer._enrich_lending_trackc_fields(position, "robinhood", {}, {}, market, strategy_wallet="0xW")

    def test_borrow_leg_reads_its_own_market_not_the_best_of_scan(self):
        """THE value assertion: the BORROW leg carries ITS market's rate.

        Both robinhood markets lend USDG. A token-keyed implementation returns
        the best-of answer (2.7744, the OTHER market) and would still satisfy
        Accountant L5 — which only checks the field is non-null. This test is
        what makes L5 green mean 'correct', not merely 'populated'.
        """
        rates = {
            self.MARKET_ID: {"borrow": Decimal("3.5325"), "supply": Decimal("3.1987")},
            self.OTHER_MARKET_ID: {"borrow": Decimal("2.7744"), "supply": Decimal("2.0026")},
        }
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"market_id": self.MARKET_ID, "asset": "USDG"})
        out = self._enrich(valuer, position, self._market(rates))

        assert out["borrow_apy_pct"] == "3.5325", "must be THIS market's borrow rate"
        assert out["borrow_apy_pct"] != "2.7744", "must NOT be the other USDG market's rate (best-of trap)"
        # A borrow leg carries no supply rate — the side it does not hold is unmeasured.
        assert out["supply_apy_pct"] is None

    def test_borrow_leg_requests_the_positions_market_id(self):
        """The scoping actually reaches the gateway call (not just the result)."""
        rates = {self.MARKET_ID: {"borrow": Decimal("3.5325"), "supply": Decimal("3.1987")}}
        market = self._market(rates)
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"market_id": self.MARKET_ID, "asset": "USDG"})
        self._enrich(valuer, position, market)

        assert market.lending_rate.call_args.kwargs["market_id"] == self.MARKET_ID
        assert market.lending_rate.call_args.args[2] == "borrow"

    def test_collateral_leg_is_measured_zero_not_the_markets_supply_apy(self):
        """Morpho collateral is not lent out: it earns exactly 0, by construction.

        The market's supply APY (3.1987%) is what USDG *loan-token* suppliers
        earn — stamping it on the USDe collateral leg would be fabrication with
        extra steps. Empty != Zero: this is a MEASURED zero (a known protocol
        invariant), so "0" is more honest than None.
        """
        rates = {self.MARKET_ID: {"borrow": Decimal("3.5325"), "supply": Decimal("3.1987")}}
        market = self._market(rates)
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"market_id": self.MARKET_ID, "asset": "USDe"})
        out = self._enrich(valuer, position, market)

        assert out["supply_apy_pct"] == "0", "collateral earns a measured zero"
        assert out["supply_apy_pct"] != "3.1987", "must NOT inherit the loan-token supply APY"
        assert out["borrow_apy_pct"] is None
        market.lending_rate.assert_not_called()  # no rate read is needed for collateral

    def test_loan_token_supply_leg_earns_the_markets_supply_apy(self):
        """The mirror of the collateral case: plain `supply()` of the LOAN token
        DOES earn the market's supply APY (morpho_blue declares
        supports_collateral_toggle=True, so this leg is reachable). Proves the
        Decimal("0") ruling is scoped to collateral, not applied blindly."""
        rates = {self.MARKET_ID: {"borrow": Decimal("3.5325"), "supply": Decimal("3.1987")}}
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"market_id": self.MARKET_ID, "asset": "USDG"})
        out = self._enrich(valuer, position, self._market(rates))

        assert out["supply_apy_pct"] == "3.1987", "loan-token supply earns the market supply APY"
        assert out["borrow_apy_pct"] is None

    def test_unknown_market_fails_closed(self):
        """A market that is not in the catalogue cannot have its role resolved."""
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"market_id": "0xdeadbeef", "asset": "USDG"})
        out = self._enrich(valuer, position, self._market({}))

        assert out["supply_apy_pct"] is None
        assert out["borrow_apy_pct"] is None

    def test_asset_matching_neither_token_fails_closed(self):
        """An asset that is neither the market's collateral nor its loan token is
        unattributable — refuse rather than guess a role."""
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"market_id": self.MARKET_ID, "asset": "WBTC"})
        out = self._enrich(valuer, position, self._market({}))

        assert out["supply_apy_pct"] is None
        assert out["borrow_apy_pct"] is None

    def test_missing_asset_fails_closed(self):
        """No asset => no role => unmeasured (never a fabricated zero)."""
        valuer = self._valuer()
        position = self._make_position("SUPPLY", details={"market_id": self.MARKET_ID})
        out = self._enrich(valuer, position, self._market({}))

        assert out["supply_apy_pct"] is None
        assert out["borrow_apy_pct"] is None

    def test_rate_source_failure_stamps_none_never_a_placeholder(self):
        """A raising rate source yields honest-unmeasured, not a stand-in number."""
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        market.lending_rate.side_effect = ValueError("RateHistoryUnavailable")
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"market_id": self.MARKET_ID, "asset": "USDG"})
        out = self._enrich(valuer, position, market)

        assert out["borrow_apy_pct"] is None
        assert out["supply_apy_pct"] is None

    def test_market_source_without_market_id_support_fails_closed(self):
        """A reduced/older market source whose lending_rate has no market_id kwarg
        must NOT be retried unscoped for an isolated market — dropping the scoping
        is exactly how another market's rate gets recorded as measured."""
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        def _legacy(protocol, token, side, chain=None):  # no market_id kwarg
            return SimpleNamespace(apy_percent=Decimal("2.7744"))  # the WRONG market

        market.lending_rate.side_effect = _legacy
        valuer = self._valuer()
        position = self._make_position("BORROW", details={"market_id": self.MARKET_ID, "asset": "USDG"})
        out = self._enrich(valuer, position, market)

        assert out["borrow_apy_pct"] is None, "must not fall back to an unscoped rate"


class TestVib5729PerMarketVenuesWithoutRateProviderStayUnmeasured:
    """Scope guard: only venues that CAN read a rate start reporting one.

    ``morpho_blue``, ``silo_v2``, ``euler_v2`` and ``fluid_vault`` all take the
    isolated-market branch, but only morpho_blue ships a gateway rate provider.
    The fix is capability-gated, not protocol-name-gated, so the other three must
    keep stamping an honest ``None`` — never a fabricated or borrowed number.

    This is the test that fails if someone later "fixes" L5 for those venues by
    widening the branch instead of writing their rate providers.
    """

    def test_only_morpho_blue_declares_a_gateway_rate_provider(self):
        """Pins the capability split the valuer branch relies on."""
        import importlib

        expected = {"morpho_blue": True, "silo_v2": False, "euler_v2": False, "fluid_vault": False}
        for protocol, has_provider in expected.items():
            try:
                mod = importlib.import_module(f"almanak.connectors.{protocol}.gateway.provider")
            except ModuleNotFoundError:
                assert not has_provider, f"{protocol} should have a gateway rate provider"
                continue
            found = any(
                hasattr(v, "fetch_lending_current") for v in vars(mod).values() if isinstance(v, type)
            )
            assert found is has_provider, f"{protocol}: gateway rate provider presence changed"

    @pytest.mark.parametrize("protocol", ["silo_v2", "euler_v2", "fluid_vault"])
    def test_venue_without_rate_provider_stamps_none(self, protocol):
        """No rate source => explicit None (key present), never a number."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        position = PositionInfo(
            position_type=PositionType.BORROW,
            position_id="p",
            chain="ethereum",
            protocol=protocol,
            value_usd=Decimal("10"),
            details={"market_id": "0xabc", "asset": "USDC", "wallet": "0xW"},
        )
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        out = valuer._enrich_lending_trackc_fields(position, "ethereum", {}, {}, market)

        assert "supply_apy_pct" in out and out["supply_apy_pct"] is None
        assert "borrow_apy_pct" in out and out["borrow_apy_pct"] is None

    def test_silo_collateral_leg_is_unmeasured_not_a_fabricated_zero(self):
        """A REAL Silo market — the measured-zero must NOT leak to non-Morpho venues.

        Regression for a bug Codex caught on PR #3287. Silo V2 publishes a market
        table naming both ``collateral_token`` and ``loan_token``, exactly like
        Morpho — so a role-discriminator gated on "is_per_market" stamped
        ``supply_apy_pct="0"`` on a Silo collateral leg. That is a FABRICATED
        measured zero: Silo lends its collateral out and it accrues. The zero is
        legal only where the connector declares ``collateral_earns_no_yield``.

        Uses a real registered market on purpose. The earlier version of this
        guard passed a FAKE market id, so ``market_params`` returned None and the
        test passed without ever reaching the branch under test.
        """
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        market_id, chain = "wavax/usdc", "avalanche"
        params = LendingReadRegistry.market_params("silo_v2", chain, market_id)
        # Pin the premise: if this stops holding, the regression is not exercised.
        assert params and params.get("collateral_token") and params.get("loan_token"), (
            "silo_v2 must still publish a collateral+loan market for this guard to bite"
        )
        assert not LendingReadRegistry.collateral_earns_no_yield("silo_v2")

        valuer = PortfolioValuer(gateway_client=MagicMock())
        position = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="silo-collateral",
            chain=chain,
            protocol="silo_v2",
            value_usd=Decimal("100"),
            details={"market_id": market_id, "asset": params["collateral_token"], "wallet": "0xW"},
        )
        market = MagicMock()
        market.price.return_value = Decimal("1.0")
        out = valuer._enrich_lending_trackc_fields(position, chain, {}, {}, market, strategy_wallet="0xW")

        assert out["supply_apy_pct"] is None, "Silo collateral accrues — a '0' here is fabricated"
        assert out["borrow_apy_pct"] is None

    def test_only_morpho_declares_collateral_earns_no_yield(self):
        """The measured-zero capability is opt-in and Morpho-only today."""
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry as R

        assert R.collateral_earns_no_yield("morpho_blue") is True
        for protocol in ("silo_v2", "euler_v2", "fluid_vault", "aave_v3", "compound_v3"):
            assert R.collateral_earns_no_yield(protocol) is False, protocol
        # Total on junk input — callers fail closed onto honest-unmeasured.
        assert R.collateral_earns_no_yield(None) is False
        assert R.collateral_earns_no_yield("nope") is False


class TestVib5729SignatureCallerContract:
    """Static guard: nobody may silently drop ``market_id`` on the accounting path.

    A behavioural test of the callee cannot catch this — the callee is correct;
    the bug would be a CALLER that omits the scoping and thereby restores the
    best-of answer. So this pins the callers themselves.

    Deliberately NOT pinned: ``best_lending_rate`` and the Compound branch.
    * ``best_lending_rate`` shops for a rate ACROSS protocols — a market id is
      meaningless there, and it never reaches accounting.
    * Compound V3 is multi-collateral against ONE base asset, so
      ``(protocol, base_symbol, side)`` already identifies the Comet. Passing a
      market_id would be actively harmful: its provider ignores the field, emits
      no echo, and the scope check would then (correctly) refuse the rate.
    """

    def test_every_rate_provider_accepts_market_id(self):
        """The dispatcher passes market_id uniformly — a provider missing the
        kwarg is a runtime TypeError, not a graceful degradation."""
        import importlib
        import inspect

        for slug in ("aave_v3", "compound_v3", "morpho_blue", "morpho_vault", "spark"):
            mod = importlib.import_module(f"almanak.connectors.{slug}.gateway.provider")
            impls = [
                v
                for v in vars(mod).values()
                if isinstance(v, type) and "fetch_lending_current" in vars(v)
            ]
            assert impls, f"{slug}: no fetch_lending_current implementation found"
            for cls in impls:
                params = inspect.signature(cls.fetch_lending_current).parameters
                assert "market_id" in params, f"{cls.__name__}.fetch_lending_current must accept market_id"
                assert params["market_id"].default is None, f"{cls.__name__}: market_id must default to None"

    def test_dispatcher_forwards_market_id_to_the_provider(self):
        """The gateway servicer must thread the request's market_id through."""
        import inspect

        from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer

        src = inspect.getsource(RateHistoryServiceServicer.GetLendingRateCurrent)
        assert "market_id=market_id or None" in src, "dispatcher must forward market_id to fetch_lending_current"
        assert "request.market_id" in src, "dispatcher must read market_id off the request"

    def test_monitor_forwards_market_id_down_to_the_wire(self):
        """RateMonitor -> _fetch_lending_rate_via_gateway -> RPC request."""
        import inspect

        from almanak.framework.data.rates import monitor as m

        assert "market_id=market_id" in inspect.getsource(m.RateMonitor._fetch_lending_rate_via_gateway)
        assert "market_id=market_id or " in inspect.getsource(m._monitor_call_lending_rate_current)

    def test_isolated_market_seam_always_scopes_its_rate_read(self):
        """The accounting seam must never call the rate read unscoped."""
        import inspect

        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        src = inspect.getsource(PortfolioValuer._isolated_market_rates)
        assert "market_id=market_id" in src, "isolated-market seam must pass market_id"

    def test_scoped_reads_never_fall_back_to_a_placeholder(self):
        """A market-scoped read is accounting-grade: a hardcoded placeholder
        constant is a fabrication, not a rate."""
        import inspect

        from almanak.framework.data.rates.monitor import RateMonitor

        src = inspect.getsource(RateMonitor.get_lending_rate)
        raise_idx = src.find("if market_id:")
        placeholder_idx = src.find("_placeholder_rate(")
        assert raise_idx != -1, "scoped-read guard missing"
        assert raise_idx < placeholder_idx, "the market_id guard must precede any placeholder fallback"
