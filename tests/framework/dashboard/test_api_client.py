"""Tests for the DashboardAPIClient.

Tests cover:
- Client initialization and strategy scoping
- Strategy data access (state, timeline, config, position, summary)
- Market data access (price, balance, indicator)
- Operator actions (pause, resume)
- Error handling and graceful degradation
- Helper method conversions
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.custom.api_client import DashboardAPIClient, create_api_client


@pytest.fixture
def mock_gateway_client():
    """Create a mock gateway client."""
    client = MagicMock()
    return client


@pytest.fixture
def api_client(mock_gateway_client):
    """Create DashboardAPIClient with mock gateway."""
    return DashboardAPIClient(mock_gateway_client, "test-strategy")


class TestDashboardAPIClientInit:
    """Tests for DashboardAPIClient initialization."""

    def test_init_with_gateway_client(self, mock_gateway_client):
        """Test initialization stores gateway client and strategy ID."""
        client = DashboardAPIClient(mock_gateway_client, "my-strategy")

        assert client._client is mock_gateway_client
        assert client._strategy_id == "my-strategy"

    def test_strategy_id_property(self, api_client):
        """Test strategy_id property returns the scoped ID."""
        assert api_client.strategy_id == "test-strategy"

    def test_create_api_client_factory(self, mock_gateway_client):
        """Test factory function creates client correctly."""
        client = create_api_client(mock_gateway_client, "factory-strategy")

        assert isinstance(client, DashboardAPIClient)
        assert client.strategy_id == "factory-strategy"
        assert client._client is mock_gateway_client


class TestGetState:
    """Tests for get_state method."""

    def test_get_state_success(self, api_client, mock_gateway_client):
        """Test getting full strategy state."""
        mock_gateway_client.get_strategy_state.return_value = {
            "is_running": True,
            "position_count": 3,
            "last_trade": "2024-01-15T10:00:00Z",
        }

        state = api_client.get_state()

        assert state["is_running"] is True
        assert state["position_count"] == 3
        mock_gateway_client.get_strategy_state.assert_called_once_with("test-strategy", None)

    def test_get_state_with_fields(self, api_client, mock_gateway_client):
        """Test getting specific state fields."""
        mock_gateway_client.get_strategy_state.return_value = {"is_running": True}

        state = api_client.get_state(fields=["is_running"])

        mock_gateway_client.get_strategy_state.assert_called_once_with("test-strategy", ["is_running"])
        assert state == {"is_running": True}

    def test_get_state_error_returns_empty(self, api_client, mock_gateway_client):
        """Test error handling returns empty dict."""
        mock_gateway_client.get_strategy_state.side_effect = Exception("Connection failed")

        state = api_client.get_state()

        assert state == {}


class TestGetTimeline:
    """Tests for get_timeline method."""

    def test_get_timeline_success(self, api_client, mock_gateway_client):
        """Test getting timeline events."""
        mock_event = MagicMock()
        mock_event.timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
        mock_event.event_type = "TRADE"
        mock_event.description = "Swapped USDC for ETH"
        mock_event.tx_hash = "0xabc123"
        mock_event.chain = "arbitrum"
        mock_event.details = {"amount": "1000"}

        mock_gateway_client.get_timeline.return_value = [mock_event]

        events = api_client.get_timeline(limit=10)

        assert len(events) == 1
        assert events[0]["event_type"] == "TRADE"
        assert events[0]["description"] == "Swapped USDC for ETH"
        assert events[0]["tx_hash"] == "0xabc123"
        mock_gateway_client.get_timeline.assert_called_once_with(
            "test-strategy", limit=10, event_type_filter=None
        )

    def test_get_timeline_with_filter(self, api_client, mock_gateway_client):
        """Test filtering timeline by event type."""
        mock_gateway_client.get_timeline.return_value = []

        api_client.get_timeline(limit=20, event_type="ERROR")

        mock_gateway_client.get_timeline.assert_called_once_with(
            "test-strategy", limit=20, event_type_filter="ERROR"
        )

    def test_get_timeline_default_limit(self, api_client, mock_gateway_client):
        """Test default limit is 50."""
        mock_gateway_client.get_timeline.return_value = []

        api_client.get_timeline()

        mock_gateway_client.get_timeline.assert_called_once_with(
            "test-strategy", limit=50, event_type_filter=None
        )

    def test_get_timeline_error_returns_empty(self, api_client, mock_gateway_client):
        """Test error handling returns empty list."""
        mock_gateway_client.get_timeline.side_effect = Exception("Timeout")

        events = api_client.get_timeline()

        assert events == []


class TestGetConfig:
    """Tests for get_config method."""

    def test_get_config_success(self, api_client, mock_gateway_client):
        """Test getting strategy config."""
        mock_gateway_client.get_strategy_config.return_value = {
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "wallet_address": "0x1234",
        }

        config = api_client.get_config()

        assert config["chain"] == "arbitrum"
        assert config["protocol"] == "Uniswap V3"
        mock_gateway_client.get_strategy_config.assert_called_once_with("test-strategy")

    def test_get_config_error_returns_empty(self, api_client, mock_gateway_client):
        """Test error handling returns empty dict."""
        mock_gateway_client.get_strategy_config.side_effect = Exception("Not found")

        config = api_client.get_config()

        assert config == {}


class TestGetTradeTape:
    """Tests for get_trade_tape method."""

    def test_get_trade_tape_scopes_to_strategy(self, api_client, mock_gateway_client):
        """Test trade-tape fetch is scoped to the current strategy."""
        response = MagicMock()
        mock_gateway_client.get_trade_tape.return_value = response

        result = api_client.get_trade_tape(limit=25)

        assert result is response
        mock_gateway_client.get_trade_tape.assert_called_once_with("test-strategy", limit=25)

    def test_get_trade_tape_error_returns_empty_shape(self, api_client, mock_gateway_client):
        """Test error handling returns a typed empty ``TradeTapeResponse``.

        Returning a plain dict here would break attribute-style consumers
        (``response.rows`` / ``response.has_more``) that work against the
        success path. The fallback shape must match the success shape.
        """
        from almanak.framework.dashboard.gateway_client import TradeTapeResponse

        mock_gateway_client.get_trade_tape.side_effect = Exception("Unavailable")

        result = api_client.get_trade_tape()

        assert isinstance(result, TradeTapeResponse)
        assert result.rows == []
        assert result.has_more is False


class TestGetPosition:
    """Tests for get_position method."""

    def test_get_position_success(self, api_client, mock_gateway_client):
        """Test getting position with balances and LP positions."""
        # Mock position with token balances
        mock_balance = MagicMock()
        mock_balance.symbol = "USDC"
        mock_balance.balance = Decimal("1000.50")
        mock_balance.value_usd = Decimal("1000.50")

        # Mock LP position
        mock_lp = MagicMock()
        mock_lp.pool = "USDC/ETH"
        mock_lp.token0 = "USDC"
        mock_lp.token1 = "ETH"
        mock_lp.liquidity_usd = Decimal("5000")
        mock_lp.in_range = True

        mock_position = MagicMock()
        mock_position.token_balances = [mock_balance]
        mock_position.lp_positions = [mock_lp]
        mock_position.total_lp_value_usd = Decimal("5000")
        mock_position.health_factor = Decimal("1.8")
        mock_position.leverage = Decimal("2.0")

        mock_details = MagicMock()
        mock_details.position = mock_position

        mock_gateway_client.get_strategy_details.return_value = mock_details

        position = api_client.get_position()

        assert len(position["token_balances"]) == 1
        assert position["token_balances"][0]["symbol"] == "USDC"
        assert len(position["lp_positions"]) == 1
        assert position["lp_positions"][0]["pool"] == "USDC/ETH"
        assert position["total_lp_value_usd"] == "5000"
        assert position["health_factor"] == "1.8"
        assert position["leverage"] == "2.0"

    def test_get_position_empty(self, api_client, mock_gateway_client):
        """Test getting position when none exists."""
        mock_details = MagicMock()
        mock_details.position = None

        mock_gateway_client.get_strategy_details.return_value = mock_details

        position = api_client.get_position()

        assert position == {}

    def test_get_position_error_returns_empty(self, api_client, mock_gateway_client):
        """Test error handling returns empty dict."""
        mock_gateway_client.get_strategy_details.side_effect = Exception("Error")

        position = api_client.get_position()

        assert position == {}


class TestGetSummary:
    """Tests for get_summary method."""

    def test_get_summary_success(self, api_client, mock_gateway_client):
        """Test getting strategy summary."""
        mock_summary = MagicMock()
        mock_summary.strategy_id = "test-strategy"
        mock_summary.name = "My Strategy"
        mock_summary.status = "RUNNING"
        mock_summary.chain = "arbitrum"
        mock_summary.protocol = "Uniswap V3"
        mock_summary.total_value_usd = Decimal("10000")
        mock_summary.pnl_24h_usd = Decimal("250")
        mock_summary.attention_required = False
        mock_summary.attention_reason = ""

        mock_details = MagicMock()
        mock_details.summary = mock_summary

        mock_gateway_client.get_strategy_details.return_value = mock_details

        summary = api_client.get_summary()

        assert summary["strategy_id"] == "test-strategy"
        assert summary["name"] == "My Strategy"
        assert summary["status"] == "RUNNING"
        assert summary["total_value_usd"] == "10000"
        assert summary["pnl_24h_usd"] == "250"

    def test_get_summary_error_returns_empty(self, api_client, mock_gateway_client):
        """Test error handling returns empty dict."""
        mock_gateway_client.get_strategy_details.side_effect = Exception("Error")

        summary = api_client.get_summary()

        assert summary == {}


class TestGetPrice:
    """Tests for get_price method.

    Phase 2 of VIB-3259 threads chain context through ``get_price`` so the
    gateway can enforce multi-chain correctness. Tests pin that the explicit
    ``chain`` kwarg is honoured and that config fall-through still works
    for dashboards written against the old 2-arg signature.
    """

    def test_get_price_explicit_chain(self, api_client, mock_gateway_client):
        """Explicit ``chain`` kwarg must be forwarded to PriceRequest."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = "2500.50"
        mock_market.GetPrice.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            price = api_client.get_price("ETH", "USD", chain="arbitrum")

            assert price == 2500.50
            mock_pb2.PriceRequest.assert_called_once_with(token="ETH", quote="USD", chain="arbitrum")

    def test_get_price_falls_back_to_config_chain(self, api_client, mock_gateway_client):
        """When ``chain`` is omitted, the config's ``default_chain`` or
        ``chain`` must be forwarded — this keeps dashboards written against
        the old 2-arg signature correct on single-chain gateways and makes
        them work on multi-chain gateways too."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = "45000"
        mock_market.GetPrice.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market
        # Strategy config exposes default_chain (fall-through target).
        mock_gateway_client.get_strategy_config = MagicMock(return_value={"default_chain": "base"})

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            # Force get_config to surface the mocked chain.
            with patch.object(api_client, "get_config", return_value={"default_chain": "base"}):
                api_client.get_price("BTC")

            # Chain must be forwarded from config, not empty.
            mock_pb2.PriceRequest.assert_called_once_with(token="BTC", quote="USD", chain="base")

    def test_get_price_default_quote_sends_empty_chain_if_config_missing(self, api_client, mock_gateway_client):
        """If the config has no chain and none was passed explicitly, the
        request forwards an empty chain — the gateway decides whether to
        accept (single-chain) or reject (multi-chain, EVM address)."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = "45000"
        mock_market.GetPrice.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            with patch.object(api_client, "get_config", return_value={}):
                api_client.get_price("BTC")

            mock_pb2.PriceRequest.assert_called_once_with(token="BTC", quote="USD", chain="")

    def test_get_price_not_available(self, api_client, mock_gateway_client):
        """Test price not available returns None."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = None
        mock_market.GetPrice.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2"):
            with patch.object(api_client, "get_config", return_value={}):
                price = api_client.get_price("UNKNOWN")

            assert price is None

    def test_get_price_error_returns_none(self, api_client, mock_gateway_client):
        """Test error handling returns None."""
        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market.GetPrice.side_effect = Exception("Error")

        with patch("almanak.gateway.proto.gateway_pb2"):
            price = api_client.get_price("ETH")
            assert price is None

    def test_get_price_chain_fallback_caches_config(self, api_client, mock_gateway_client):
        """Strategy config chain is immutable for a session, so the fallback
        must cache it on the instance. Dashboards refresh charts on every
        tick; a gRPC ``get_config`` round-trip per price call multiplies
        gateway load for no benefit.
        """
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = "2500.50"
        mock_market.GetPrice.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        get_config_calls = 0

        def _counting_get_config():
            nonlocal get_config_calls
            get_config_calls += 1
            return {"default_chain": "base"}

        with patch("almanak.gateway.proto.gateway_pb2"):
            with patch.object(api_client, "get_config", side_effect=_counting_get_config):
                api_client.get_price("ETH")
                api_client.get_price("USDC")
                api_client.get_price("WBTC")

        # Despite three price calls with no explicit chain, config is
        # fetched exactly once. Second and third calls use the cached
        # value.
        assert get_config_calls == 1

    def test_get_price_explicit_chain_does_not_poison_cache(self, api_client, mock_gateway_client):
        """An explicit ``chain`` kwarg must NOT populate the fallback cache
        — otherwise a one-off call with an override chain would silently
        change the default for subsequent calls that omit the kwarg.
        """
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.price = "1"
        mock_market.GetPrice.return_value = mock_response
        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            # Explicit chain: must NOT hit get_config at all.
            with patch.object(api_client, "get_config") as mock_get_config:
                api_client.get_price("ETH", chain="arbitrum")
                mock_get_config.assert_not_called()

            # Subsequent call with no chain still falls back to config
            # (the explicit "arbitrum" did not leak into the cache).
            with patch.object(api_client, "get_config", return_value={"default_chain": "base"}) as mock_get_config:
                api_client.get_price("ETH")
                mock_get_config.assert_called_once()
                mock_pb2.PriceRequest.assert_called_with(token="ETH", quote="USD", chain="base")


class TestGetBalance:
    """Tests for get_balance method."""

    def test_get_balance_success(self, api_client, mock_gateway_client):
        """Test getting token balance."""
        # Mock get_config to return wallet and chain
        mock_gateway_client.get_strategy_config.return_value = {
            "wallet_address": "0x1234",
            "chain": "arbitrum",
        }

        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.balance = "1500.25"
        mock_market.GetBalance.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            balance = api_client.get_balance("USDC")

            assert balance == 1500.25
            mock_pb2.BalanceRequest.assert_called_once_with(
                token="USDC", chain="arbitrum", wallet_address="0x1234"
            )

    def test_get_balance_with_chain_override(self, api_client, mock_gateway_client):
        """Test getting balance with explicit chain."""
        mock_gateway_client.get_strategy_config.return_value = {
            "wallet_address": "0x1234",
            "chain": "arbitrum",
        }

        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.balance = "100"
        mock_market.GetBalance.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            api_client.get_balance("ETH", chain="base")

            mock_pb2.BalanceRequest.assert_called_once_with(
                token="ETH", chain="base", wallet_address="0x1234"
            )

    def test_get_balance_error_returns_none(self, api_client, mock_gateway_client):
        """Test error handling returns None when market call fails."""
        mock_gateway_client.get_strategy_config.return_value = {
            "wallet_address": "0x1234",
            "chain": "arbitrum",
        }

        # Make the market call fail
        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market.GetBalance.side_effect = Exception("RPC Error")

        with patch("almanak.gateway.proto.gateway_pb2"):
            balance = api_client.get_balance("USDC")

            assert balance is None


class TestGetIndicator:
    """Tests for get_indicator method."""

    def test_get_indicator_success(self, api_client, mock_gateway_client):
        """Test getting indicator value."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.value = "65.5"
        mock_market.GetIndicator.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            value = api_client.get_indicator("RSI", "ETH", params={"period": "14"})

            assert value == 65.5
            mock_pb2.IndicatorRequest.assert_called_once_with(
                indicator_type="RSI",
                token="ETH",
                quote="USD",
                params={"period": "14"},
            )

    def test_get_indicator_default_params(self, api_client, mock_gateway_client):
        """Test indicator with default parameters."""
        mock_market = MagicMock()
        mock_response = MagicMock()
        mock_response.value = "2000"
        mock_market.GetIndicator.return_value = mock_response

        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market = mock_market

        with patch("almanak.gateway.proto.gateway_pb2") as mock_pb2:
            api_client.get_indicator("SMA", "BTC")

            mock_pb2.IndicatorRequest.assert_called_once_with(
                indicator_type="SMA",
                token="BTC",
                quote="USD",
                params={},
            )

    def test_get_indicator_error_returns_none(self, api_client, mock_gateway_client):
        """Test error handling returns None."""
        mock_gateway_client._client = MagicMock()
        mock_gateway_client._client.market.GetIndicator.side_effect = Exception("Error")

        with patch("almanak.gateway.proto.gateway_pb2"):
            value = api_client.get_indicator("RSI", "ETH")

            assert value is None


class TestPauseStrategy:
    """Tests for pause_strategy method."""

    def test_pause_strategy_success(self, api_client, mock_gateway_client):
        """Test pausing strategy successfully."""
        mock_gateway_client.execute_action.return_value = True

        result = api_client.pause_strategy("Emergency maintenance")

        assert result is True
        mock_gateway_client.execute_action.assert_called_once_with(
            "test-strategy",
            action="PAUSE",
            reason="Emergency maintenance",
        )

    def test_pause_strategy_no_reason(self, api_client, mock_gateway_client):
        """Test pause requires reason."""
        result = api_client.pause_strategy("")

        assert result is False
        mock_gateway_client.execute_action.assert_not_called()

    def test_pause_strategy_error(self, api_client, mock_gateway_client):
        """Test pause error handling."""
        mock_gateway_client.execute_action.side_effect = Exception("Failed")

        result = api_client.pause_strategy("Test pause")

        assert result is False


class TestResumeStrategy:
    """Tests for resume_strategy method."""

    def test_resume_strategy_success(self, api_client, mock_gateway_client):
        """Test resuming strategy successfully."""
        mock_gateway_client.execute_action.return_value = True

        result = api_client.resume_strategy("Issue resolved")

        assert result is True
        mock_gateway_client.execute_action.assert_called_once_with(
            "test-strategy",
            action="RESUME",
            reason="Issue resolved",
        )

    def test_resume_strategy_default_reason(self, api_client, mock_gateway_client):
        """Test resume with default reason."""
        mock_gateway_client.execute_action.return_value = True

        api_client.resume_strategy()

        mock_gateway_client.execute_action.assert_called_once_with(
            "test-strategy",
            action="RESUME",
            reason="Resumed from dashboard",
        )

    def test_resume_strategy_error(self, api_client, mock_gateway_client):
        """Test resume error handling."""
        mock_gateway_client.execute_action.side_effect = Exception("Failed")

        result = api_client.resume_strategy()

        assert result is False


class TestEventToDict:
    """Tests for _event_to_dict helper method."""

    def test_event_to_dict_full(self, api_client):
        """Test converting event with all fields."""
        event = MagicMock()
        event.timestamp = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
        event.event_type = "TRADE"
        event.description = "Test trade"
        event.tx_hash = "0xabc"
        event.chain = "arbitrum"
        event.details = {"amount": "100"}

        result = api_client._event_to_dict(event)

        assert result["timestamp"] == "2024-01-15T10:00:00+00:00"
        assert result["event_type"] == "TRADE"
        assert result["description"] == "Test trade"
        assert result["tx_hash"] == "0xabc"
        assert result["chain"] == "arbitrum"
        assert result["details"] == {"amount": "100"}

    def test_event_to_dict_missing_fields(self, api_client):
        """Test converting event with missing optional fields."""
        event = MagicMock(spec=[])  # No attributes

        result = api_client._event_to_dict(event)

        assert result["timestamp"] is None
        assert result["description"] == ""
        assert result["tx_hash"] is None
        assert result["chain"] is None
        assert result["details"] == {}


class TestPositionToDict:
    """Tests for _position_to_dict helper method."""

    def test_position_to_dict_none(self, api_client):
        """Test converting None position."""
        result = api_client._position_to_dict(None)

        assert result == {}

    def test_position_to_dict_empty(self, api_client):
        """Test converting empty position."""
        position = MagicMock()
        position.token_balances = []
        position.lp_positions = []
        position.total_lp_value_usd = None
        position.health_factor = None
        position.leverage = None

        result = api_client._position_to_dict(position)

        assert result["token_balances"] == []
        assert result["lp_positions"] == []
        assert result["total_lp_value_usd"] == "0"
        assert result["health_factor"] is None
        assert result["leverage"] is None


class TestSummaryToDict:
    """Tests for _summary_to_dict helper method."""

    def test_summary_to_dict_none(self, api_client):
        """Test converting None summary."""
        result = api_client._summary_to_dict(None)

        assert result == {}

    def test_summary_to_dict_uses_strategy_id_fallback(self, api_client):
        """Test summary uses api client strategy_id as fallback."""
        summary = MagicMock(spec=[])  # No attributes

        result = api_client._summary_to_dict(summary)

        # Falls back to client's strategy_id
        assert result["strategy_id"] == "test-strategy"
        assert result["name"] == ""
        assert result["status"] == "UNKNOWN"
