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

from almanak.framework.valuation.lending_valuer import (
    LendingPositionValue,
    value_lending_portfolio,
    value_lending_position,
)
from almanak.framework.valuation.lending_position_reader import (
    LendingPositionOnChain,
    LendingPositionReader,
    _decode_uint_hex,
    _pad_address,
    _parse_user_reserve_data_hex,
)


# =============================================================================
# TestValueLendingPosition — pure math
# =============================================================================


class TestValueLendingPosition:
    """Pure math tests for value_lending_position."""

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
        assert result.supply_value_usd == Decimal("1500")
        assert result.debt_value_usd == Decimal("0")
        assert result.net_value_usd == Decimal("1500")
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
        assert result.supply_value_usd == Decimal("0")
        assert result.variable_debt_balance == Decimal("0.5")
        assert result.debt_value_usd == Decimal("1500")
        assert result.net_value_usd == Decimal("-1500")

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
        assert result.supply_value_usd == Decimal("3000")
        assert result.debt_value_usd == Decimal("1500")
        assert result.net_value_usd == Decimal("1500")

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
        assert result.debt_value_usd == Decimal("5000")
        assert result.net_value_usd == Decimal("5000")  # 10000 - 5000

    def test_zero_everything(self):
        """Empty position."""
        result = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
        )
        assert result.net_value_usd == Decimal("0")
        assert result.supply_value_usd == Decimal("0")
        assert result.debt_value_usd == Decimal("0")

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
        assert result.supply_value_usd == Decimal("30000")

    def test_underwater_position(self):
        """Net value is negative when debt > supply (same asset borrow)."""
        result = value_lending_position(
            atoken_balance=100_000_000,  # 100 USDC
            stable_debt=0,
            variable_debt=200_000_000,  # 200 USDC debt
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
        )
        assert result.net_value_usd == Decimal("-100")

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


class TestValueLendingPortfolio:
    """Tests for aggregating multiple lending positions."""

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
        assert total_supply == Decimal("3000")
        assert total_debt == Decimal("0")
        assert total_net == Decimal("3000")

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
        assert total_supply == Decimal("6000")  # 2 WETH * 3000
        assert total_debt == Decimal("3000")  # 3000 USDC
        assert total_net == Decimal("3000")

    def test_empty_portfolio(self):
        """Empty portfolio."""
        total_supply, total_debt, total_net = value_lending_portfolio([])
        assert total_supply == Decimal("0")
        assert total_debt == Decimal("0")
        assert total_net == Decimal("0")


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
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {"k": "v"}, cache)
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
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {}, {})
        assert out["health_factor"] is None

    def test_none_hf_value_stamps_explicit_none(self, monkeypatch):
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("SUPPLY", details={"wallet": "0xWALLET"})
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **_kw: self._account_state(None),
        )
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {}, {})
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
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {"health_factor": "9.99"}, {})
        assert out["health_factor"] is None

    def test_non_lending_position_skips_read(self, monkeypatch):
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position("TOKEN", details={"wallet": "0xWALLET"})
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: calls.append(kw),
        )
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {}, {})
        assert "health_factor" not in out
        assert calls == []  # no read for non-lending positions

    def test_per_market_protocol_skipped_no_read(self, monkeypatch):
        """A market_id ⇒ Morpho-class (per-market) protocol — skip rather than
        issue a read that would fail closed (VIB-4551 twin, separate ticket)."""
        valuer = self._valuer_with_on_chain(None)
        position = self._make_position(
            "BORROW", protocol="morpho_blue", details={"wallet": "0xWALLET", "market_id": "0xabc"}
        )
        calls: list = []
        monkeypatch.setattr(
            "almanak.framework.accounting.lending_reads.read_lending_account_state",
            lambda **kw: calls.append(kw),
        )
        out = valuer._enrich_lending_health_factor(position, "ethereum", {}, {})
        assert "health_factor" not in out
        assert calls == []

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
        out_s = valuer._enrich_lending_health_factor(supply, "arbitrum", {}, cache)
        out_b = valuer._enrich_lending_health_factor(borrow, "arbitrum", {}, cache)
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
        valuer._enrich_lending_health_factor(checksummed, "arbitrum", {}, cache)
        valuer._enrich_lending_health_factor(lowercased, "arbitrum", {}, cache)
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
        out = valuer._enrich_lending_health_factor(position, "arbitrum", {}, {})
        assert "health_factor" not in out
        assert calls == []
