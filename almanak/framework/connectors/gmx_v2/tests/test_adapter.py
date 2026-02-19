"""Tests for GMX v2 Adapter.

This test suite covers:
- Configuration and initialization
- Position management (open, close, increase, decrease)
- Order management (create, cancel)
- Market and token resolution
- Transaction building
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ..adapter import (
    DEFAULT_EXECUTION_FEE,
    GMX_V2_ADDRESSES,
    GMX_V2_MARKETS,
    GMX_V2_TOKENS,
    GMXv2Adapter,
    GMXv2Config,
    GMXv2Order,
    GMXv2OrderType,
    GMXv2Position,
    GMXv2PositionSide,
)

# =============================================================================
# Configuration Tests
# =============================================================================


class TestGMXv2Config:
    """Tests for GMXv2Config."""

    def test_config_creation_arbitrum(self) -> None:
        """Test config creation for Arbitrum."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        assert config.chain == "arbitrum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50
        assert config.execution_fee == DEFAULT_EXECUTION_FEE["arbitrum"]

    def test_config_creation_avalanche(self) -> None:
        """Test config creation for Avalanche."""
        config = GMXv2Config(
            chain="avalanche",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        assert config.chain == "avalanche"
        assert config.execution_fee == DEFAULT_EXECUTION_FEE["avalanche"]

    def test_config_invalid_chain(self) -> None:
        """Test config with invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            GMXv2Config(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_config_custom_slippage(self) -> None:
        """Test config with custom slippage."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,  # 1%
        )

        assert config.default_slippage_bps == 100

    def test_config_invalid_slippage(self) -> None:
        """Test config with invalid slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            GMXv2Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
            )

        with pytest.raises(ValueError, match="Slippage must be between"):
            GMXv2Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
            )

    def test_config_custom_execution_fee(self) -> None:
        """Test config with custom execution fee."""
        custom_fee = int(0.002 * 10**18)
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            execution_fee=custom_fee,
        )

        assert config.execution_fee == custom_fee

    def test_config_to_dict(self) -> None:
        """Test config serialization."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        config_dict = config.to_dict()

        assert config_dict["chain"] == "arbitrum"
        assert config_dict["wallet_address"] == "0x1234567890123456789012345678901234567890"
        assert config_dict["default_slippage_bps"] == 50


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestGMXv2AdapterInit:
    """Tests for GMXv2Adapter initialization."""

    def test_adapter_creation(self) -> None:
        """Test adapter creation."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = GMXv2Adapter(config)

        assert adapter.chain == "arbitrum"
        assert adapter.wallet_address == "0x1234567890123456789012345678901234567890"
        assert adapter.addresses == GMX_V2_ADDRESSES["arbitrum"]
        assert adapter.markets == GMX_V2_MARKETS["arbitrum"]
        assert adapter.tokens == GMX_V2_TOKENS["arbitrum"]

    def test_adapter_has_exchange_router(self) -> None:
        """Test adapter has exchange router address."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = GMXv2Adapter(config)

        assert "exchange_router" in adapter.addresses
        assert adapter.addresses["exchange_router"].startswith("0x")


# =============================================================================
# Position Data Tests
# =============================================================================


class TestGMXv2Position:
    """Tests for GMXv2Position dataclass."""

    def test_position_creation(self) -> None:
        """Test position creation."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )

        assert position.position_key == "0x1234"
        assert position.is_long is True
        assert position.side == GMXv2PositionSide.LONG

    def test_position_side_long(self) -> None:
        """Test position side property for long."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )

        assert position.side == GMXv2PositionSide.LONG

    def test_position_side_short(self) -> None:
        """Test position side property for short."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=False,
        )

        assert position.side == GMXv2PositionSide.SHORT

    def test_position_total_fees(self) -> None:
        """Test total fees calculation."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
            funding_fee_amount=Decimal("10"),
            borrowing_fee_amount=Decimal("5"),
        )

        assert position.total_fees == Decimal("15")

    def test_position_net_pnl(self) -> None:
        """Test net PnL calculation."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
            unrealized_pnl=Decimal("100"),
            funding_fee_amount=Decimal("10"),
            borrowing_fee_amount=Decimal("5"),
        )

        assert position.net_pnl == Decimal("85")  # 100 - 15

    def test_position_to_dict(self) -> None:
        """Test position serialization."""
        position = GMXv2Position(
            position_key="0x1234",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )

        position_dict = position.to_dict()

        assert position_dict["position_key"] == "0x1234"
        assert position_dict["is_long"] is True
        assert position_dict["side"] == "LONG"
        assert position_dict["size_in_usd"] == "5000"

    def test_position_from_dict(self) -> None:
        """Test position deserialization."""
        data = {
            "position_key": "0x1234",
            "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "size_in_usd": "5000",
            "size_in_tokens": "2.5",
            "collateral_amount": "1000",
            "entry_price": "2000",
            "is_long": True,
            "last_updated": datetime.now(UTC).isoformat(),
        }

        position = GMXv2Position.from_dict(data)

        assert position.position_key == "0x1234"
        assert position.size_in_usd == Decimal("5000")
        assert position.is_long is True


# =============================================================================
# Order Data Tests
# =============================================================================


class TestGMXv2Order:
    """Tests for GMXv2Order dataclass."""

    def test_order_creation(self) -> None:
        """Test order creation."""
        order = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        assert order.order_key == "0x5678"
        assert order.order_type == GMXv2OrderType.MARKET_INCREASE

    def test_order_is_increase(self) -> None:
        """Test is_increase property."""
        market_increase = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        limit_increase = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.LIMIT_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        market_decrease = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_DECREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("0"),
        )

        assert market_increase.is_increase is True
        assert limit_increase.is_increase is True
        assert market_decrease.is_increase is False

    def test_order_is_decrease(self) -> None:
        """Test is_decrease property."""
        market_decrease = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_DECREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("0"),
        )

        stop_loss = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.STOP_LOSS_DECREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("0"),
        )

        market_increase = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        assert market_decrease.is_decrease is True
        assert stop_loss.is_decrease is True
        assert market_increase.is_decrease is False

    def test_order_is_market_order(self) -> None:
        """Test is_market_order property."""
        market_order = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        limit_order = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.LIMIT_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        assert market_order.is_market_order is True
        assert limit_order.is_market_order is False

    def test_order_is_limit_order(self) -> None:
        """Test is_limit_order property."""
        limit_increase = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.LIMIT_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        stop_loss = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.STOP_LOSS_DECREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("0"),
        )

        market_order = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        assert limit_increase.is_limit_order is True
        assert stop_loss.is_limit_order is True
        assert market_order.is_limit_order is False

    def test_order_to_dict(self) -> None:
        """Test order serialization."""
        order = GMXv2Order(
            order_key="0x5678",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            initial_collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            order_type=GMXv2OrderType.MARKET_INCREASE,
            is_long=True,
            size_delta_usd=Decimal("5000"),
            initial_collateral_delta_amount=Decimal("1000"),
        )

        order_dict = order.to_dict()

        assert order_dict["order_key"] == "0x5678"
        assert order_dict["order_type"] == "MARKET_INCREASE"
        assert order_dict["is_increase"] is True
        assert order_dict["is_market_order"] is True

    def test_order_from_dict(self) -> None:
        """Test order deserialization."""
        data = {
            "order_key": "0x5678",
            "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            "initial_collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "order_type": "MARKET_INCREASE",
            "is_long": True,
            "size_delta_usd": "5000",
            "initial_collateral_delta_amount": "1000",
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        order = GMXv2Order.from_dict(data)

        assert order.order_key == "0x5678"
        assert order.order_type == GMXv2OrderType.MARKET_INCREASE
        assert order.size_delta_usd == Decimal("5000")

    def test_order_type_to_int(self) -> None:
        """Test order type integer conversion."""
        assert GMXv2OrderType.MARKET_INCREASE.to_int() == 0
        assert GMXv2OrderType.LIMIT_INCREASE.to_int() == 1
        assert GMXv2OrderType.MARKET_DECREASE.to_int() == 2
        assert GMXv2OrderType.LIMIT_DECREASE.to_int() == 3
        assert GMXv2OrderType.STOP_LOSS_DECREASE.to_int() == 4
        assert GMXv2OrderType.LIQUIDATION.to_int() == 5


# =============================================================================
# Position Management Tests
# =============================================================================


class TestGMXv2AdapterPositions:
    """Tests for position management methods."""

    @pytest.fixture
    def adapter(self) -> GMXv2Adapter:
        """Create adapter for testing."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return GMXv2Adapter(config)

    def test_open_position_long_market(self, adapter: GMXv2Adapter) -> None:
        """Test opening a long market position."""
        result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )

        assert result.success is True
        assert result.order_key is not None
        assert result.order is not None
        assert result.order.order_type == GMXv2OrderType.MARKET_INCREASE
        assert result.order.is_long is True
        assert result.order.size_delta_usd == Decimal("5000")

    def test_open_position_short_market(self, adapter: GMXv2Adapter) -> None:
        """Test opening a short market position."""
        result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=False,
        )

        assert result.success is True
        assert result.order is not None
        assert result.order.is_long is False

    def test_open_position_limit_order(self, adapter: GMXv2Adapter) -> None:
        """Test opening a position with limit order."""
        result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
            trigger_price=Decimal("1950"),
        )

        assert result.success is True
        assert result.order is not None
        assert result.order.order_type == GMXv2OrderType.LIMIT_INCREASE
        assert result.order.trigger_price == Decimal("1950")

    def test_open_position_unknown_market(self, adapter: GMXv2Adapter) -> None:
        """Test opening position with unknown market."""
        result = adapter.open_position(
            market="UNKNOWN/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )

        assert result.success is False
        assert "Unknown market" in (result.error or "")

    def test_open_position_unknown_token(self, adapter: GMXv2Adapter) -> None:
        """Test opening position with unknown collateral token."""
        result = adapter.open_position(
            market="ETH/USD",
            collateral_token="UNKNOWN",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )

        assert result.success is False
        assert "Unknown collateral token" in (result.error or "")

    def test_close_position(self, adapter: GMXv2Adapter) -> None:
        """Test closing a position."""
        # First open a position
        open_result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )
        assert open_result.success is True

        # Close the position
        close_result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=Decimal("5000"),
        )

        assert close_result.success is True
        assert close_result.order is not None
        assert close_result.order.order_type == GMXv2OrderType.MARKET_DECREASE
        assert close_result.order.is_decrease is True

    def test_close_position_limit_order(self, adapter: GMXv2Adapter) -> None:
        """Test closing a position with limit order."""
        close_result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=Decimal("5000"),
            trigger_price=Decimal("2100"),
        )

        assert close_result.success is True
        assert close_result.order is not None
        assert close_result.order.order_type == GMXv2OrderType.LIMIT_DECREASE

    def test_increase_position(self, adapter: GMXv2Adapter) -> None:
        """Test increasing an existing position."""
        result = adapter.increase_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            collateral_delta=Decimal("500"),
            size_delta_usd=Decimal("2500"),
        )

        assert result.success is True
        assert result.order is not None
        assert result.order.is_increase is True
        assert result.order.size_delta_usd == Decimal("2500")

    def test_decrease_position(self, adapter: GMXv2Adapter) -> None:
        """Test decreasing an existing position."""
        result = adapter.decrease_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=Decimal("2500"),
        )

        assert result.success is True
        assert result.order is not None
        assert result.order.is_decrease is True
        assert result.order.size_delta_usd == Decimal("2500")

    def test_get_position(self, adapter: GMXv2Adapter) -> None:
        """Test getting position details."""
        # Set up a test position
        position = GMXv2Position(
            position_key="0x1234",
            market=GMX_V2_MARKETS["arbitrum"]["ETH/USD"],
            collateral_token=GMX_V2_TOKENS["arbitrum"]["USDC"],
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )
        adapter.set_position(position)

        # Get the position
        retrieved = adapter.get_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
        )

        assert retrieved is not None
        assert retrieved.size_in_usd == Decimal("5000")
        assert retrieved.is_long is True

    def test_get_position_not_found(self, adapter: GMXv2Adapter) -> None:
        """Test getting non-existent position."""
        retrieved = adapter.get_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
        )

        assert retrieved is None

    def test_get_all_positions(self, adapter: GMXv2Adapter) -> None:
        """Test getting all positions."""
        # Set up test positions
        position1 = GMXv2Position(
            position_key="0x1234",
            market=GMX_V2_MARKETS["arbitrum"]["ETH/USD"],
            collateral_token=GMX_V2_TOKENS["arbitrum"]["USDC"],
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )
        position2 = GMXv2Position(
            position_key="0x5678",
            market=GMX_V2_MARKETS["arbitrum"]["BTC/USD"],
            collateral_token=GMX_V2_TOKENS["arbitrum"]["USDC"],
            size_in_usd=Decimal("10000"),
            size_in_tokens=Decimal("0.25"),
            collateral_amount=Decimal("2000"),
            entry_price=Decimal("40000"),
            is_long=False,
        )
        adapter.set_position(position1)
        adapter.set_position(position2)

        positions = adapter.get_all_positions()

        assert len(positions) == 2


# =============================================================================
# Order Management Tests
# =============================================================================


class TestGMXv2AdapterOrders:
    """Tests for order management methods."""

    @pytest.fixture
    def adapter(self) -> GMXv2Adapter:
        """Create adapter for testing."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return GMXv2Adapter(config)

    def test_cancel_order(self, adapter: GMXv2Adapter) -> None:
        """Test canceling an order."""
        # First create an order
        open_result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
            trigger_price=Decimal("1950"),
        )
        assert open_result.success is True
        assert open_result.order_key is not None

        # Cancel the order
        cancel_result = adapter.cancel_order(open_result.order_key)

        assert cancel_result.success is True
        assert adapter.get_order(open_result.order_key) is None

    def test_cancel_nonexistent_order(self, adapter: GMXv2Adapter) -> None:
        """Test canceling a non-existent order."""
        result = adapter.cancel_order("0xnonexistent")

        assert result.success is False
        assert "not found" in (result.error or "")

    def test_get_order(self, adapter: GMXv2Adapter) -> None:
        """Test getting order details."""
        # Create an order
        open_result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )
        assert open_result.success is True
        assert open_result.order_key is not None

        # Get the order
        order = adapter.get_order(open_result.order_key)

        assert order is not None
        assert order.size_delta_usd == Decimal("5000")

    def test_get_order_not_found(self, adapter: GMXv2Adapter) -> None:
        """Test getting non-existent order."""
        order = adapter.get_order("0xnonexistent")

        assert order is None

    def test_get_all_orders(self, adapter: GMXv2Adapter) -> None:
        """Test getting all orders."""
        # Create multiple orders
        adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )
        adapter.open_position(
            market="BTC/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("2000"),
            size_delta_usd=Decimal("10000"),
            is_long=False,
        )

        orders = adapter.get_all_orders()

        assert len(orders) == 2


# =============================================================================
# Helper Method Tests
# =============================================================================


class TestGMXv2AdapterHelpers:
    """Tests for adapter helper methods."""

    @pytest.fixture
    def adapter(self) -> GMXv2Adapter:
        """Create adapter for testing."""
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return GMXv2Adapter(config)

    def test_resolve_market_by_symbol(self, adapter: GMXv2Adapter) -> None:
        """Test market resolution by symbol."""
        market_address = adapter._resolve_market("ETH/USD")

        assert market_address == GMX_V2_MARKETS["arbitrum"]["ETH/USD"]

    def test_resolve_market_by_address(self, adapter: GMXv2Adapter) -> None:
        """Test market resolution by address."""
        address = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        market_address = adapter._resolve_market(address)

        assert market_address == address

    def test_resolve_market_unknown(self, adapter: GMXv2Adapter) -> None:
        """Test market resolution for unknown market."""
        market_address = adapter._resolve_market("UNKNOWN/USD")

        assert market_address is None

    def test_resolve_token_by_symbol(self, adapter: GMXv2Adapter) -> None:
        """Test token resolution by symbol."""
        token_address = adapter._resolve_token("USDC")

        assert token_address == GMX_V2_TOKENS["arbitrum"]["USDC"]

    def test_resolve_token_by_address(self, adapter: GMXv2Adapter) -> None:
        """Test token resolution by address."""
        address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        token_address = adapter._resolve_token(address)

        assert token_address == address

    def test_resolve_token_unknown(self, adapter: GMXv2Adapter) -> None:
        """Test token resolution for unknown token."""
        token_address = adapter._resolve_token("UNKNOWN")

        assert token_address is None

    def test_get_token_decimals(self, adapter: GMXv2Adapter) -> None:
        """Test getting token decimals."""
        assert adapter._get_token_decimals("USDC") == 6
        assert adapter._get_token_decimals("WETH") == 18
        assert adapter._get_token_decimals("WBTC") == 8
        assert adapter._get_token_decimals("UNKNOWN") == 18  # Default

    def test_clear_all(self, adapter: GMXv2Adapter) -> None:
        """Test clearing all state."""
        # Set up some state
        position = GMXv2Position(
            position_key="0x1234",
            market=GMX_V2_MARKETS["arbitrum"]["ETH/USD"],
            collateral_token=GMX_V2_TOKENS["arbitrum"]["USDC"],
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            entry_price=Decimal("2000"),
            is_long=True,
        )
        adapter.set_position(position)
        adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )

        # Clear all
        adapter.clear_all()

        assert len(adapter.get_all_positions()) == 0
        assert len(adapter.get_all_orders()) == 0
