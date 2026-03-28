"""Tests for Paper Trading + PortfolioValuer alignment (PnL Week 5).

Covers:
- DirectRpcAdapter: wraps JSON-RPC URL to gateway client interface
- PaperTrader._init_portfolio_valuer: creates valuer with fork RPC
- PaperTrader._value_portfolio_rich: rich valuation with fallback
- PaperTrader._record_equity_point: equity curve enrichment
- EquityPoint: new fields (spot_value_usd, position_value_usd, valuation_source)
- PaperTradingSummary: valuation_source field
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.models import EquityPoint
from almanak.framework.backtesting.paper.models import PaperTradingSummary
from almanak.framework.valuation.portfolio_valuer import StrategyLike
from almanak.framework.valuation.rpc_adapter import (
    DirectRpcAdapter,
    _AdapterConfig,
    _DirectRpcStub,
    _RpcResponse,
)


# =============================================================================
# DirectRpcAdapter Tests
# =============================================================================


class TestDirectRpcAdapter:
    def test_adapter_has_gateway_interface(self):
        adapter = DirectRpcAdapter("http://localhost:8545")
        assert hasattr(adapter, "_rpc_stub")
        assert hasattr(adapter, "config")
        assert hasattr(adapter.config, "timeout")

    def test_default_timeout(self):
        adapter = DirectRpcAdapter("http://localhost:8545")
        assert adapter.config.timeout == 10

    def test_custom_timeout(self):
        adapter = DirectRpcAdapter("http://localhost:8545", timeout=30)
        assert adapter.config.timeout == 30

    def test_rpc_stub_has_call_method(self):
        adapter = DirectRpcAdapter("http://localhost:8545")
        assert hasattr(adapter._rpc_stub, "Call")


class TestDirectRpcStub:
    def test_successful_call(self):
        stub = _DirectRpcStub("http://localhost:8545")

        @dataclass
        class FakeRequest:
            chain: str = "arbitrum"
            method: str = "eth_call"
            params: str = '[{"to": "0x1234", "data": "0x5678"}, "latest"]'

        with patch("almanak.framework.valuation.rpc_adapter.requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0xdeadbeef"}
            mock_resp.raise_for_status.return_value = None
            mock_post.return_value = mock_resp

            response = stub.Call(FakeRequest(), timeout=5)

            assert response.success is True
            assert json.loads(response.result) == "0xdeadbeef"
            mock_post.assert_called_once()

    def test_rpc_error_response(self):
        stub = _DirectRpcStub("http://localhost:8545")

        @dataclass
        class FakeRequest:
            chain: str = "arbitrum"
            method: str = "eth_call"
            params: str = "[]"

        with patch("almanak.framework.valuation.rpc_adapter.requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "fail"}}
            mock_resp.raise_for_status.return_value = None
            mock_post.return_value = mock_resp

            response = stub.Call(FakeRequest())
            assert response.success is False

    def test_connection_error(self):
        stub = _DirectRpcStub("http://localhost:99999")

        @dataclass
        class FakeRequest:
            chain: str = "arbitrum"
            method: str = "eth_call"
            params: str = "[]"

        with patch("almanak.framework.valuation.rpc_adapter.requests.post", side_effect=ConnectionError("refused")):
            response = stub.Call(FakeRequest())
            assert response.success is False
            assert "refused" in response.error

    def test_invalid_params_json(self):
        stub = _DirectRpcStub("http://localhost:8545")

        @dataclass
        class FakeRequest:
            chain: str = "arbitrum"
            method: str = "eth_call"
            params: str = "not valid json{{"

        response = stub.Call(FakeRequest())
        assert response.success is False


class TestRpcResponse:
    def test_success(self):
        r = _RpcResponse(success=True, result='"0xbeef"')
        assert r.success is True
        assert r.result == '"0xbeef"'

    def test_failure(self):
        r = _RpcResponse(success=False, error="timeout")
        assert r.success is False
        assert r.error == "timeout"


# =============================================================================
# EquityPoint enrichment tests
# =============================================================================


class TestEquityPointEnrichment:
    def test_new_fields_default_to_none(self):
        pt = EquityPoint(timestamp=datetime.now(UTC), value_usd=Decimal("1000"))
        assert pt.spot_value_usd is None
        assert pt.position_value_usd is None
        assert pt.valuation_source == "simple"

    def test_with_portfolio_valuer_data(self):
        pt = EquityPoint(
            timestamp=datetime.now(UTC),
            value_usd=Decimal("5000"),
            spot_value_usd=Decimal("3000"),
            position_value_usd=Decimal("2000"),
            valuation_source="portfolio_valuer",
        )
        assert pt.spot_value_usd == Decimal("3000")
        assert pt.position_value_usd == Decimal("2000")
        assert pt.valuation_source == "portfolio_valuer"

    def test_to_dict_simple(self):
        pt = EquityPoint(
            timestamp=datetime(2026, 3, 28, 12, 0, 0, tzinfo=UTC),
            value_usd=Decimal("1000"),
        )
        d = pt.to_dict()
        assert d["valuation_source"] == "simple"
        assert "spot_value_usd" not in d
        assert "position_value_usd" not in d

    def test_to_dict_rich(self):
        pt = EquityPoint(
            timestamp=datetime(2026, 3, 28, 12, 0, 0, tzinfo=UTC),
            value_usd=Decimal("5000"),
            eth_price_usd=Decimal("3000"),
            spot_value_usd=Decimal("3000"),
            position_value_usd=Decimal("2000"),
            valuation_source="portfolio_valuer",
        )
        d = pt.to_dict()
        assert d["valuation_source"] == "portfolio_valuer"
        assert d["spot_value_usd"] == "3000"
        assert d["position_value_usd"] == "2000"
        assert d["eth_price_usd"] == "3000"


# =============================================================================
# PaperTradingSummary valuation_source tests
# =============================================================================


class TestPaperTradingSummaryValuationSource:
    def test_default_valuation_source(self):
        summary = PaperTradingSummary(
            strategy_id="test",
            start_time=datetime.now(UTC),
            duration=timedelta(seconds=60),
            total_trades=0,
            successful_trades=0,
            failed_trades=0,
        )
        assert summary.valuation_source == "simple"

    def test_portfolio_valuer_source(self):
        summary = PaperTradingSummary(
            strategy_id="test",
            start_time=datetime.now(UTC),
            duration=timedelta(seconds=60),
            total_trades=1,
            successful_trades=1,
            failed_trades=0,
            valuation_source="portfolio_valuer",
        )
        assert summary.valuation_source == "portfolio_valuer"


# =============================================================================
# PaperTrader integration tests (mocked)
# =============================================================================


class _FakeStrategy:
    """Satisfies StrategyLike protocol for testing."""

    def __init__(
        self,
        strategy_id: str = "test",
        chain: str = "arbitrum",
        tokens: list[str] | None = None,
        wallet_address: str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    ):
        self._strategy_id = strategy_id
        self._chain = chain
        self._tokens = tokens or ["USDC", "ETH"]
        self._wallet_address = wallet_address

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def chain(self) -> str:
        return self._chain

    @property
    def wallet_address(self) -> str:
        return self._wallet_address

    def _get_tracked_tokens(self) -> list[str]:
        return self._tokens

    def decide(self, market):
        return None


class TestPaperTraderValuerIntegration:
    """Tests for PaperTrader's PortfolioValuer wiring."""

    def _make_trader(self):
        """Create a minimally mocked PaperTrader."""
        from almanak.framework.backtesting.paper.engine import PaperTrader
        from almanak.framework.backtesting.paper.config import PaperTraderConfig
        from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id="test_strategy",
        )
        fork_manager = MagicMock()
        fork_manager.get_rpc_url.return_value = "http://localhost:8545"
        fork_manager.is_running = True

        tracker = PaperPortfolioTracker(strategy_id="test_strategy")
        tracker.start_session({"USDC": Decimal("10000"), "ETH": Decimal("5")})

        trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=tracker,
            config=config,
        )
        return trader

    def test_init_portfolio_valuer_success(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()
        assert trader._valuer_available is True
        assert trader._portfolio_valuer is not None

    def test_init_portfolio_valuer_no_rpc_url(self):
        trader = self._make_trader()
        trader.fork_manager.get_rpc_url.return_value = ""
        trader._init_portfolio_valuer()
        assert trader._valuer_available is False

    def test_value_portfolio_rich_no_valuer(self):
        trader = self._make_trader()
        # Don't call _init_portfolio_valuer
        assert trader._value_portfolio_rich() is None

    def test_value_portfolio_rich_no_strategy(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()
        trader._current_strategy = None
        assert trader._value_portfolio_rich() is None

    def test_value_portfolio_rich_no_market_snapshot(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()
        trader._current_strategy = _FakeStrategy()
        trader._last_market_snapshot = None
        assert trader._value_portfolio_rich() is None

    def test_value_portfolio_rich_strategy_missing_chain(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()
        strategy = MagicMock(spec=["strategy_id", "decide"])  # No chain attr
        trader._current_strategy = strategy
        trader._last_market_snapshot = MagicMock()
        assert trader._value_portfolio_rich() is None

    def test_value_portfolio_rich_success(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()

        trader._current_strategy = _FakeStrategy()
        trader._last_market_snapshot = MagicMock()
        trader._tick_count = 5

        mock_snapshot = MagicMock()
        mock_snapshot.total_value_usd = Decimal("15000")
        mock_snapshot.available_cash_usd = Decimal("10000")
        mock_snapshot.position_value_usd = Decimal("5000")
        mock_snapshot.value_confidence.value = "HIGH"

        with patch.object(trader._portfolio_valuer, "value", return_value=mock_snapshot):
            result = trader._value_portfolio_rich()

        assert result is not None
        total, spot, positions = result
        assert total == Decimal("15000")
        assert spot == Decimal("10000")
        assert positions == Decimal("5000")

    def test_value_portfolio_rich_valuer_returns_unavailable(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()

        trader._current_strategy = _FakeStrategy()
        trader._last_market_snapshot = MagicMock()

        mock_snapshot = MagicMock()
        mock_snapshot.value_confidence.value = "UNAVAILABLE"

        with patch.object(trader._portfolio_valuer, "value", return_value=mock_snapshot):
            result = trader._value_portfolio_rich()

        assert result is None

    def test_value_portfolio_rich_valuer_exception_fallback(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()

        trader._current_strategy = _FakeStrategy()
        trader._last_market_snapshot = MagicMock()

        with patch.object(trader._portfolio_valuer, "value", side_effect=RuntimeError("boom")):
            result = trader._value_portfolio_rich()

        assert result is None

    @pytest.mark.asyncio
    async def test_cleanup_resets_valuer(self):
        trader = self._make_trader()
        trader._init_portfolio_valuer()
        assert trader._valuer_available is True

        await trader._cleanup()
        assert trader._valuer_available is False
        assert trader._portfolio_valuer is None
        assert trader._last_market_snapshot is None


# =============================================================================
# DirectRpcAdapter integration with position readers
# =============================================================================


class TestDirectRpcAdapterWithReaders:
    """Test that DirectRpcAdapter satisfies the interface LP/lending readers expect."""

    def test_lp_reader_accepts_adapter(self):
        from almanak.framework.valuation.lp_position_reader import LPPositionReader

        adapter = DirectRpcAdapter("http://localhost:8545")
        reader = LPPositionReader(adapter)
        assert reader._gateway is adapter

    def test_lending_reader_accepts_adapter(self):
        from almanak.framework.valuation.lending_position_reader import LendingPositionReader

        adapter = DirectRpcAdapter("http://localhost:8545")
        reader = LendingPositionReader(adapter)
        assert reader._gateway is adapter

    def test_portfolio_valuer_accepts_adapter(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        adapter = DirectRpcAdapter("http://localhost:8545")
        valuer = PortfolioValuer(gateway_client=adapter)
        assert valuer._lp_reader._gateway is adapter
        assert valuer._lending_reader._gateway is adapter
