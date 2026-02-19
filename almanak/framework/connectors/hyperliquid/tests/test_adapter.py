"""Tests for Hyperliquid Adapter.

This test suite covers:
- Configuration and initialization
- Order management (place, cancel, query)
- Position management
- Leverage settings
- L1 and L2 message signing
"""

from decimal import Decimal

import pytest

from ..adapter import (
    HYPERLIQUID_API_URLS,
    HYPERLIQUID_ASSETS,
    HYPERLIQUID_CHAIN_IDS,
    HYPERLIQUID_WS_URLS,
    EIP712Signer,
    ExternalSigner,
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidOrder,
    HyperliquidOrderSide,
    HyperliquidOrderStatus,
    HyperliquidOrderType,
    HyperliquidPosition,
    HyperliquidPositionSide,
    HyperliquidTimeInForce,
)

# =============================================================================
# Configuration Tests
# =============================================================================


class TestHyperliquidConfig:
    """Tests for HyperliquidConfig."""

    def test_config_creation_mainnet(self) -> None:
        """Test config creation for mainnet."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        assert config.network == "mainnet"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50
        assert config.api_url == HYPERLIQUID_API_URLS["mainnet"]
        assert config.ws_url == HYPERLIQUID_WS_URLS["mainnet"]
        assert config.chain_id == HYPERLIQUID_CHAIN_IDS["mainnet"]

    def test_config_creation_testnet(self) -> None:
        """Test config creation for testnet."""
        config = HyperliquidConfig(
            network="testnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        assert config.network == "testnet"
        assert config.api_url == HYPERLIQUID_API_URLS["testnet"]
        assert config.chain_id == HYPERLIQUID_CHAIN_IDS["testnet"]

    def test_config_invalid_network(self) -> None:
        """Test config with invalid network."""
        with pytest.raises(ValueError, match="Unsupported network"):
            HyperliquidConfig(
                network="invalid_network",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_config_custom_slippage(self) -> None:
        """Test config with custom slippage."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,  # 1%
        )

        assert config.default_slippage_bps == 100

    def test_config_invalid_slippage(self) -> None:
        """Test config with invalid slippage."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            HyperliquidConfig(
                network="mainnet",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
            )

        with pytest.raises(ValueError, match="Slippage must be between"):
            HyperliquidConfig(
                network="mainnet",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
            )

    def test_config_invalid_wallet_address(self) -> None:
        """Test config with invalid wallet address."""
        with pytest.raises(ValueError, match="Wallet address must start with 0x"):
            HyperliquidConfig(
                network="mainnet",
                wallet_address="1234567890123456789012345678901234567890",
            )

    def test_config_with_private_key(self) -> None:
        """Test config with private key."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            private_key="0x" + "a" * 64,
        )

        assert config.private_key == "0x" + "a" * 64

    def test_config_with_vault_address(self) -> None:
        """Test config with vault address."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            vault_address="0x0987654321098765432109876543210987654321",
        )

        assert config.vault_address == "0x0987654321098765432109876543210987654321"

    def test_config_to_dict(self) -> None:
        """Test config serialization."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        config_dict = config.to_dict()

        assert config_dict["network"] == "mainnet"
        assert config_dict["wallet_address"] == "0x1234567890123456789012345678901234567890"
        assert config_dict["default_slippage_bps"] == 50
        assert "api_url" in config_dict
        assert "chain_id" in config_dict

    def test_config_eip712_domain(self) -> None:
        """Test EIP-712 domain configuration."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        domain = config.eip712_domain
        assert domain["name"] == "Hyperliquid"
        assert domain["version"] == "1"
        assert domain["chainId"] == HYPERLIQUID_CHAIN_IDS["mainnet"]


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestHyperliquidAdapterInit:
    """Tests for HyperliquidAdapter initialization."""

    def test_adapter_creation(self) -> None:
        """Test adapter creation."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = HyperliquidAdapter(config)

        assert adapter.network == "mainnet"
        assert adapter.wallet_address == "0x1234567890123456789012345678901234567890"

    def test_adapter_with_private_key(self) -> None:
        """Test adapter with private key creates signer."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            private_key="0x" + "a" * 64,
        )
        adapter = HyperliquidAdapter(config)

        assert adapter._signer is not None

    def test_adapter_without_private_key(self) -> None:
        """Test adapter without private key has no signer."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = HyperliquidAdapter(config)

        assert adapter._signer is None

    def test_adapter_with_external_signer(self) -> None:
        """Test adapter with external signer."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )

        def mock_sign(action: dict, nonce: int, is_l1: bool) -> str:
            return "0x" + "ab" * 65

        signer = ExternalSigner(mock_sign)
        adapter = HyperliquidAdapter(config, signer=signer)

        assert adapter._signer is signer


# =============================================================================
# Order Placement Tests
# =============================================================================


class TestOrderPlacement:
    """Tests for order placement."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            private_key="0x" + "a" * 64,
        )
        return HyperliquidAdapter(config)

    def test_place_limit_buy_order(self, adapter: HyperliquidAdapter) -> None:
        """Test placing a limit buy order."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )

        assert result.success
        assert result.order_id is not None
        assert result.client_id is not None
        assert result.order is not None
        assert result.order.asset == "ETH"
        assert result.order.side == HyperliquidOrderSide.BUY
        assert result.order.size == Decimal("0.1")
        assert result.order.order_type == HyperliquidOrderType.LIMIT

    def test_place_limit_sell_order(self, adapter: HyperliquidAdapter) -> None:
        """Test placing a limit sell order."""
        result = adapter.place_order(
            asset="BTC",
            is_buy=False,
            size=Decimal("0.01"),
            price=Decimal("50000"),
        )

        assert result.success
        assert result.order is not None
        assert result.order.side == HyperliquidOrderSide.SELL
        assert result.order.asset == "BTC"

    def test_place_market_order(self, adapter: HyperliquidAdapter) -> None:
        """Test placing a market order."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.5"),
            price=Decimal("2000"),
            order_type=HyperliquidOrderType.MARKET,
        )

        assert result.success
        assert result.order is not None
        assert result.order.order_type == HyperliquidOrderType.MARKET
        # Market orders get slippage applied
        assert result.order.price > Decimal("2000")

    def test_place_order_with_client_id(self, adapter: HyperliquidAdapter) -> None:
        """Test placing order with custom client ID."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
            client_id="my_custom_id_123",
        )

        assert result.success
        assert result.client_id == "my_custom_id_123"
        assert result.order is not None
        assert result.order.client_id == "my_custom_id_123"

    def test_place_order_reduce_only(self, adapter: HyperliquidAdapter) -> None:
        """Test placing reduce-only order."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=False,
            size=Decimal("0.1"),
            price=Decimal("2100"),
            reduce_only=True,
        )

        assert result.success
        assert result.order is not None
        assert result.order.reduce_only is True

    def test_place_order_ioc(self, adapter: HyperliquidAdapter) -> None:
        """Test placing IOC order."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
            time_in_force=HyperliquidTimeInForce.IOC,
        )

        assert result.success
        assert result.order is not None
        assert result.order.time_in_force == HyperliquidTimeInForce.IOC

    def test_place_order_post_only(self, adapter: HyperliquidAdapter) -> None:
        """Test placing post-only (ALO) order."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
            time_in_force=HyperliquidTimeInForce.ALO,
        )

        assert result.success
        assert result.order is not None
        assert result.order.time_in_force == HyperliquidTimeInForce.ALO

    def test_place_order_unknown_asset(self, adapter: HyperliquidAdapter) -> None:
        """Test placing order with unknown asset."""
        result = adapter.place_order(
            asset="UNKNOWN",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("100"),
        )

        assert not result.success
        assert "Unknown asset" in (result.error or "")

    def test_place_order_invalid_size(self, adapter: HyperliquidAdapter) -> None:
        """Test placing order with invalid size."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("-0.1"),
            price=Decimal("2000"),
        )

        assert not result.success
        assert "size must be positive" in (result.error or "")

    def test_place_order_custom_slippage(self, adapter: HyperliquidAdapter) -> None:
        """Test market order with custom slippage."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
            order_type=HyperliquidOrderType.MARKET,
            slippage_bps=100,  # 1%
        )

        assert result.success
        assert result.order is not None
        # 1% slippage on buy should increase price by ~20
        expected_price = Decimal("2000") * Decimal("1.01")
        assert result.order.price == expected_price

    def test_place_order_stores_in_orders(self, adapter: HyperliquidAdapter) -> None:
        """Test that placed order is stored."""
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )

        assert result.success
        stored_order = adapter.get_order(result.order_id)  # type: ignore[arg-type]
        assert stored_order is not None
        assert stored_order.order_id == result.order_id


# =============================================================================
# Order Cancellation Tests
# =============================================================================


class TestOrderCancellation:
    """Tests for order cancellation."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            private_key="0x" + "a" * 64,
        )
        return HyperliquidAdapter(config)

    def test_cancel_order_by_id(self, adapter: HyperliquidAdapter) -> None:
        """Test canceling order by order ID."""
        # Place order first
        place_result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        assert place_result.success

        # Cancel it
        cancel_result = adapter.cancel_order(order_id=place_result.order_id)

        assert cancel_result.success
        assert place_result.order_id in cancel_result.cancelled_orders

    def test_cancel_order_by_client_id(self, adapter: HyperliquidAdapter) -> None:
        """Test canceling order by client ID."""
        # Place order with client ID
        place_result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
            client_id="test_cancel_cloid",
        )
        assert place_result.success

        # Cancel by client ID
        cancel_result = adapter.cancel_order(client_id="test_cancel_cloid")

        assert cancel_result.success
        assert len(cancel_result.cancelled_orders) == 1

    def test_cancel_nonexistent_order(self, adapter: HyperliquidAdapter) -> None:
        """Test canceling nonexistent order."""
        result = adapter.cancel_order(order_id="nonexistent_order_id")

        assert not result.success
        assert "not found" in (result.error or "").lower()

    def test_cancel_all_orders(self, adapter: HyperliquidAdapter) -> None:
        """Test canceling all orders."""
        # Place multiple orders
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        adapter.place_order(
            asset="BTC",
            is_buy=True,
            size=Decimal("0.01"),
            price=Decimal("50000"),
        )

        # Cancel all
        result = adapter.cancel_all_orders()

        assert result.success
        assert len(result.cancelled_orders) == 2
        assert adapter.get_open_orders() == []

    def test_cancel_all_orders_by_asset(self, adapter: HyperliquidAdapter) -> None:
        """Test canceling all orders for specific asset."""
        # Place orders for different assets
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        adapter.place_order(
            asset="ETH",
            is_buy=False,
            size=Decimal("0.1"),
            price=Decimal("2100"),
        )
        adapter.place_order(
            asset="BTC",
            is_buy=True,
            size=Decimal("0.01"),
            price=Decimal("50000"),
        )

        # Cancel only ETH orders
        result = adapter.cancel_all_orders(asset="ETH")

        assert result.success
        assert len(result.cancelled_orders) == 2

        # BTC order should still exist
        btc_orders = adapter.get_open_orders(asset="BTC")
        assert len(btc_orders) == 1

    def test_cancel_removes_from_orders(self, adapter: HyperliquidAdapter) -> None:
        """Test that cancelled order is removed from tracking."""
        place_result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        assert adapter.get_order(place_result.order_id) is not None  # type: ignore[arg-type]

        adapter.cancel_order(order_id=place_result.order_id)

        assert adapter.get_order(place_result.order_id) is None  # type: ignore[arg-type]


# =============================================================================
# Order Query Tests
# =============================================================================


class TestOrderQueries:
    """Tests for order queries."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
            private_key="0x" + "a" * 64,
        )
        return HyperliquidAdapter(config)

    def test_get_order(self, adapter: HyperliquidAdapter) -> None:
        """Test getting order by ID."""
        place_result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )

        order = adapter.get_order(place_result.order_id)  # type: ignore[arg-type]

        assert order is not None
        assert order.order_id == place_result.order_id
        assert order.asset == "ETH"

    def test_get_nonexistent_order(self, adapter: HyperliquidAdapter) -> None:
        """Test getting nonexistent order."""
        order = adapter.get_order("nonexistent")
        assert order is None

    def test_get_open_orders(self, adapter: HyperliquidAdapter) -> None:
        """Test getting all open orders."""
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        adapter.place_order(
            asset="BTC",
            is_buy=False,
            size=Decimal("0.01"),
            price=Decimal("55000"),
        )

        orders = adapter.get_open_orders()

        assert len(orders) == 2

    def test_get_open_orders_by_asset(self, adapter: HyperliquidAdapter) -> None:
        """Test getting open orders filtered by asset."""
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        adapter.place_order(
            asset="ETH",
            is_buy=False,
            size=Decimal("0.2"),
            price=Decimal("2100"),
        )
        adapter.place_order(
            asset="BTC",
            is_buy=True,
            size=Decimal("0.01"),
            price=Decimal("50000"),
        )

        eth_orders = adapter.get_open_orders(asset="ETH")

        assert len(eth_orders) == 2
        assert all(o.asset == "ETH" for o in eth_orders)


# =============================================================================
# Position Tests
# =============================================================================


class TestPositions:
    """Tests for position management."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return HyperliquidAdapter(config)

    def test_get_position_none(self, adapter: HyperliquidAdapter) -> None:
        """Test getting position when none exists."""
        position = adapter.get_position("ETH")
        assert position is None

    def test_set_and_get_position(self, adapter: HyperliquidAdapter) -> None:
        """Test setting and getting a position."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("1.5"),
            entry_price=Decimal("2000"),
            mark_price=Decimal("2100"),
            unrealized_pnl=Decimal("150"),
            leverage=Decimal("5"),
        )

        adapter.set_position(position)
        retrieved = adapter.get_position("ETH")

        assert retrieved is not None
        assert retrieved.asset == "ETH"
        assert retrieved.size == Decimal("1.5")
        assert retrieved.entry_price == Decimal("2000")

    def test_get_all_positions(self, adapter: HyperliquidAdapter) -> None:
        """Test getting all positions."""
        adapter.set_position(
            HyperliquidPosition(
                asset="ETH",
                size=Decimal("1.0"),
                entry_price=Decimal("2000"),
            )
        )
        adapter.set_position(
            HyperliquidPosition(
                asset="BTC",
                size=Decimal("-0.1"),
                entry_price=Decimal("50000"),
            )
        )
        adapter.set_position(
            HyperliquidPosition(
                asset="SOL",
                size=Decimal("0"),  # Zero position
                entry_price=Decimal("100"),
            )
        )

        positions = adapter.get_all_positions()

        # Should only return non-zero positions
        assert len(positions) == 2
        assets = {p.asset for p in positions}
        assert assets == {"ETH", "BTC"}

    def test_clear_positions(self, adapter: HyperliquidAdapter) -> None:
        """Test clearing all positions."""
        adapter.set_position(
            HyperliquidPosition(
                asset="ETH",
                size=Decimal("1.0"),
                entry_price=Decimal("2000"),
            )
        )

        adapter.clear_positions()

        assert adapter.get_position("ETH") is None
        assert adapter.get_all_positions() == []


# =============================================================================
# Position Properties Tests
# =============================================================================


class TestPositionProperties:
    """Tests for HyperliquidPosition properties."""

    def test_long_position_side(self) -> None:
        """Test long position side detection."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("1.0"),
            entry_price=Decimal("2000"),
        )

        assert position.side == HyperliquidPositionSide.LONG
        assert position.is_long
        assert not position.is_short

    def test_short_position_side(self) -> None:
        """Test short position side detection."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("-1.0"),
            entry_price=Decimal("2000"),
        )

        assert position.side == HyperliquidPositionSide.SHORT
        assert not position.is_long
        assert position.is_short

    def test_no_position_side(self) -> None:
        """Test no position side detection."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("0"),
            entry_price=Decimal("2000"),
        )

        assert position.side == HyperliquidPositionSide.NONE
        assert not position.is_long
        assert not position.is_short

    def test_notional_value(self) -> None:
        """Test notional value calculation."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("-2.0"),  # Short position
            entry_price=Decimal("2000"),
            mark_price=Decimal("2100"),
        )

        # Notional = abs(size) * mark_price = 2.0 * 2100 = 4200
        assert position.notional_value == Decimal("4200")

    def test_net_pnl(self) -> None:
        """Test net PnL calculation."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("1.0"),
            entry_price=Decimal("2000"),
            realized_pnl=Decimal("50"),
            unrealized_pnl=Decimal("100"),
        )

        assert position.net_pnl == Decimal("150")

    def test_position_to_dict(self) -> None:
        """Test position serialization."""
        position = HyperliquidPosition(
            asset="ETH",
            size=Decimal("1.0"),
            entry_price=Decimal("2000"),
            mark_price=Decimal("2100"),
            leverage=Decimal("5"),
        )

        data = position.to_dict()

        assert data["asset"] == "ETH"
        assert data["size"] == "1.0"
        assert data["entry_price"] == "2000"
        assert data["side"] == "long"
        assert data["is_long"] is True

    def test_position_from_dict(self) -> None:
        """Test position deserialization."""
        data = {
            "asset": "BTC",
            "size": "-0.5",
            "entry_price": "50000",
            "mark_price": "48000",
            "leverage": "10",
        }

        position = HyperliquidPosition.from_dict(data)

        assert position.asset == "BTC"
        assert position.size == Decimal("-0.5")
        assert position.is_short


# =============================================================================
# Order Properties Tests
# =============================================================================


class TestOrderProperties:
    """Tests for HyperliquidOrder properties."""

    def test_remaining_size(self) -> None:
        """Test remaining size calculation."""
        order = HyperliquidOrder(
            order_id="test",
            client_id=None,
            asset="ETH",
            side=HyperliquidOrderSide.BUY,
            size=Decimal("1.0"),
            price=Decimal("2000"),
            filled_size=Decimal("0.3"),
        )

        assert order.remaining_size == Decimal("0.7")

    def test_fill_percentage(self) -> None:
        """Test fill percentage calculation."""
        order = HyperliquidOrder(
            order_id="test",
            client_id=None,
            asset="ETH",
            side=HyperliquidOrderSide.BUY,
            size=Decimal("1.0"),
            price=Decimal("2000"),
            filled_size=Decimal("0.25"),
        )

        assert order.fill_percentage == Decimal("25")

    def test_order_is_open(self) -> None:
        """Test order open status."""
        open_order = HyperliquidOrder(
            order_id="test",
            client_id=None,
            asset="ETH",
            side=HyperliquidOrderSide.BUY,
            size=Decimal("1.0"),
            price=Decimal("2000"),
            status=HyperliquidOrderStatus.OPEN,
        )

        assert open_order.is_open
        assert not open_order.is_filled

    def test_order_is_filled(self) -> None:
        """Test order filled status."""
        filled_order = HyperliquidOrder(
            order_id="test",
            client_id=None,
            asset="ETH",
            side=HyperliquidOrderSide.BUY,
            size=Decimal("1.0"),
            price=Decimal("2000"),
            status=HyperliquidOrderStatus.FILLED,
        )

        assert not filled_order.is_open
        assert filled_order.is_filled

    def test_order_to_dict(self) -> None:
        """Test order serialization."""
        order = HyperliquidOrder(
            order_id="test123",
            client_id="my_cloid",
            asset="ETH",
            side=HyperliquidOrderSide.BUY,
            size=Decimal("0.5"),
            price=Decimal("2000"),
            order_type=HyperliquidOrderType.LIMIT,
            time_in_force=HyperliquidTimeInForce.GTC,
        )

        data = order.to_dict()

        assert data["order_id"] == "test123"
        assert data["client_id"] == "my_cloid"
        assert data["side"] == "B"
        assert data["order_type"] == "Limit"
        assert data["is_buy"] is True

    def test_order_from_dict(self) -> None:
        """Test order deserialization."""
        data = {
            "order_id": "test456",
            "asset": "BTC",
            "side": "A",
            "size": "0.1",
            "price": "50000",
            "status": "open",
        }

        order = HyperliquidOrder.from_dict(data)

        assert order.order_id == "test456"
        assert order.side == HyperliquidOrderSide.SELL
        assert order.is_sell


# =============================================================================
# Leverage Tests
# =============================================================================


class TestLeverage:
    """Tests for leverage management."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return HyperliquidAdapter(config)

    def test_set_leverage(self, adapter: HyperliquidAdapter) -> None:
        """Test setting leverage."""
        result = adapter.set_leverage("ETH", 10)
        assert result is True
        assert adapter.get_leverage("ETH") == 10

    def test_get_default_leverage(self, adapter: HyperliquidAdapter) -> None:
        """Test default leverage is 1."""
        assert adapter.get_leverage("BTC") == 1

    def test_set_leverage_invalid_low(self, adapter: HyperliquidAdapter) -> None:
        """Test setting leverage below minimum."""
        result = adapter.set_leverage("ETH", 0)
        assert result is False

    def test_set_leverage_invalid_high(self, adapter: HyperliquidAdapter) -> None:
        """Test setting leverage above maximum."""
        result = adapter.set_leverage("ETH", 51)
        assert result is False


# =============================================================================
# Message Signing Tests
# =============================================================================


class TestMessageSigning:
    """Tests for message signing."""

    def test_eip712_signer_creation(self) -> None:
        """Test EIP712Signer creation."""
        signer = EIP712Signer(
            private_key="0x" + "a" * 64,
            chain_id=1337,
            is_mainnet=True,
        )

        assert signer is not None
        assert signer._chain_id == 1337
        assert signer._is_mainnet is True

    def test_eip712_signer_l1_sign(self) -> None:
        """Test L1 action signing."""
        signer = EIP712Signer(
            private_key="0x" + "a" * 64,
            chain_id=1337,
            is_mainnet=True,
        )

        action = {"type": "order", "orders": []}
        signature = signer.sign_l1_action(action, nonce=1)

        assert signature.startswith("0x")
        assert len(signature) == 132  # 0x + 64 (r) + 64 (s) + 2 (v)

    def test_eip712_signer_l2_sign(self) -> None:
        """Test L2 action signing."""
        signer = EIP712Signer(
            private_key="0x" + "b" * 64,
            chain_id=421614,
            is_mainnet=False,
        )

        action = {"type": "cancel", "cancels": []}
        signature = signer.sign_l2_action(action, nonce=1)

        assert signature.startswith("0x")
        assert len(signature) == 132

    def test_external_signer(self) -> None:
        """Test external signer."""
        signed_actions: list[tuple[dict, int, bool]] = []

        def mock_sign(action: dict, nonce: int, is_l1: bool) -> str:
            signed_actions.append((action, nonce, is_l1))
            return "0x" + "cd" * 65

        signer = ExternalSigner(mock_sign)

        action = {"type": "order"}
        sig = signer.sign_l1_action(action, nonce=100)

        assert sig == "0x" + "cd" * 65
        assert len(signed_actions) == 1
        assert signed_actions[0] == (action, 100, True)


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Tests for adapter state management."""

    @pytest.fixture
    def adapter(self) -> HyperliquidAdapter:
        """Create adapter for tests."""
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        return HyperliquidAdapter(config)

    def test_clear_orders(self, adapter: HyperliquidAdapter) -> None:
        """Test clearing orders."""
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )

        adapter.clear_orders()

        assert adapter.get_open_orders() == []

    def test_clear_all(self, adapter: HyperliquidAdapter) -> None:
        """Test clearing all state."""
        adapter.set_position(
            HyperliquidPosition(
                asset="ETH",
                size=Decimal("1.0"),
                entry_price=Decimal("2000"),
            )
        )
        adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )
        adapter.set_leverage("ETH", 10)

        adapter.clear_all()

        assert adapter.get_all_positions() == []
        assert adapter.get_open_orders() == []
        assert adapter.get_leverage("ETH") == 1


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_api_urls_exist(self) -> None:
        """Test API URLs are defined."""
        assert "mainnet" in HYPERLIQUID_API_URLS
        assert "testnet" in HYPERLIQUID_API_URLS
        assert HYPERLIQUID_API_URLS["mainnet"].startswith("https://")

    def test_ws_urls_exist(self) -> None:
        """Test WebSocket URLs are defined."""
        assert "mainnet" in HYPERLIQUID_WS_URLS
        assert "testnet" in HYPERLIQUID_WS_URLS
        assert HYPERLIQUID_WS_URLS["mainnet"].startswith("wss://")

    def test_chain_ids_exist(self) -> None:
        """Test chain IDs are defined."""
        assert "mainnet" in HYPERLIQUID_CHAIN_IDS
        assert "testnet" in HYPERLIQUID_CHAIN_IDS

    def test_assets_exist(self) -> None:
        """Test common assets are defined."""
        assert "BTC" in HYPERLIQUID_ASSETS
        assert "ETH" in HYPERLIQUID_ASSETS
        assert "SOL" in HYPERLIQUID_ASSETS
        # Asset indices should be unique
        indices = list(HYPERLIQUID_ASSETS.values())
        assert len(indices) == len(set(indices))
