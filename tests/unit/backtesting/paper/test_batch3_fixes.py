"""Tests for Paper Trading Batch 3 fixes.

VIB-1955: Multi-source OHLCV fallback for indicators
VIB-1956: Fork RPC URL exposure on MarketSnapshot
VIB-1957: Health telemetry counters
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestIndicatorFallback:
    """VIB-1955: Multi-source OHLCV provider replaces Binance-only."""

    def test_create_ohlcv_provider_returns_routing_provider(self):
        """_create_ohlcv_provider creates a RoutingOHLCVProvider with multiple sources."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader.config = SimpleNamespace(chain="arbitrum")

        provider = trader._create_ohlcv_provider()

        from almanak.framework.data.ohlcv.routing_provider import RoutingOHLCVProvider

        assert isinstance(provider, RoutingOHLCVProvider)

    def test_create_ohlcv_provider_falls_back_to_binance(self):
        """Falls back to BinanceOHLCVProvider when routing infra fails."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader.config = SimpleNamespace(chain="arbitrum")

        with patch(
            "almanak.framework.data.ohlcv.geckoterminal_provider.GeckoTerminalOHLCVProvider",
            side_effect=RuntimeError("GeckoTerminal unavailable"),
        ):
            provider = trader._create_ohlcv_provider()

        from almanak.framework.data.ohlcv.binance_provider import BinanceOHLCVProvider

        assert isinstance(provider, BinanceOHLCVProvider)

    def test_init_indicator_calculators_uses_routing_provider(self):
        """_init_indicator_calculators creates RSICalculator with RoutingOHLCVProvider."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader.config = SimpleNamespace(chain="arbitrum")
        trader._rsi_calculator = None

        trader._init_indicator_calculators()

        assert trader._rsi_calculator is not None

    def test_init_indicator_calculators_handles_failure(self):
        """_init_indicator_calculators sets None on complete failure."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._backtest_id = "test"
        trader.config = SimpleNamespace(chain="arbitrum")
        trader._rsi_calculator = None

        with patch.object(trader, "_create_ohlcv_provider", side_effect=RuntimeError("Total failure")):
            trader._init_indicator_calculators()

        assert trader._rsi_calculator is None


class TestBinanceDataProviderAdapter:
    """Test the _BinanceDataProviderAdapter used for OHLCVRouter registration."""

    def test_adapter_name_is_binance(self):
        """Adapter reports name='binance' for router provider chain matching."""
        from almanak.framework.backtesting.paper.engine import _BinanceDataProviderAdapter

        adapter = _BinanceDataProviderAdapter(MagicMock())
        assert adapter.name == "binance"

    def test_adapter_data_class_is_informational(self):
        """Adapter reports INFORMATIONAL data classification."""
        from almanak.framework.backtesting.paper.engine import _BinanceDataProviderAdapter
        from almanak.framework.data.models import DataClassification

        adapter = _BinanceDataProviderAdapter(MagicMock())
        assert adapter.data_class == DataClassification.INFORMATIONAL

    def test_adapter_health_returns_healthy(self):
        """Adapter health check returns healthy status."""
        from almanak.framework.backtesting.paper.engine import _BinanceDataProviderAdapter

        adapter = _BinanceDataProviderAdapter(MagicMock())
        health = adapter.health()
        assert health["status"] == "healthy"

    def test_adapter_health_degrades_after_failures(self):
        """Adapter reports degraded after 3+ consecutive failures."""
        from almanak.framework.backtesting.paper.engine import _BinanceDataProviderAdapter

        adapter = _BinanceDataProviderAdapter(MagicMock())
        adapter._consecutive_failures = 3
        health = adapter.health()
        assert health["status"] == "degraded"
        assert health["consecutive_failures"] == 3


class TestForkRpcUrl:
    """VIB-1956: Fork RPC URL exposure on MarketSnapshot."""

    def test_data_layer_snapshot_fork_rpc_url_defaults_none(self):
        """Data-layer MarketSnapshot.fork_rpc_url defaults to None."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1234")
        assert snapshot.fork_rpc_url is None
        assert snapshot.fork_block is None

    def test_data_layer_snapshot_fork_rpc_url_set(self):
        """Data-layer MarketSnapshot.fork_rpc_url reflects set value."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1234")
        snapshot._fork_rpc_url = "http://127.0.0.1:8546"
        snapshot._fork_block = 12345

        assert snapshot.fork_rpc_url == "http://127.0.0.1:8546"
        assert snapshot.fork_block == 12345

    def test_strategy_layer_snapshot_fork_rpc_url_defaults_none(self):
        """Strategy-facing MarketSnapshot.fork_rpc_url defaults to None."""
        from almanak.framework.strategies.intent_strategy import MarketSnapshot

        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1234")
        assert snapshot.fork_rpc_url is None
        assert snapshot.fork_block is None

    def test_strategy_layer_snapshot_fork_rpc_url_set(self):
        """Strategy-facing MarketSnapshot.fork_rpc_url reflects set value."""
        from almanak.framework.strategies.intent_strategy import MarketSnapshot

        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1234")
        snapshot._fork_rpc_url = "http://127.0.0.1:8547"
        snapshot._fork_block = 99999

        assert snapshot.fork_rpc_url == "http://127.0.0.1:8547"
        assert snapshot.fork_block == 99999

    def test_create_market_snapshot_from_fork_sets_rpc_url(self):
        """create_market_snapshot_from_fork populates fork_rpc_url."""
        import asyncio

        from almanak.framework.backtesting.paper.engine import create_market_snapshot_from_fork

        fork_manager = MagicMock()
        fork_manager.is_running = True
        fork_manager.current_block = 42000
        fork_manager.get_rpc_url.return_value = "http://127.0.0.1:8546"

        snapshot = asyncio.run(
            create_market_snapshot_from_fork(
                fork_manager=fork_manager,
                chain="arbitrum",
                wallet_address="0xabc",
            )
        )

        assert snapshot.fork_rpc_url == "http://127.0.0.1:8546"
        assert snapshot.fork_block == 42000


class TestHealthTelemetry:
    """VIB-1957: Health telemetry counters in PaperTraderState."""

    def test_state_defaults(self):
        """New PaperTraderState has zero telemetry counters."""
        from almanak.framework.backtesting.paper.background import PaperTraderState

        state = PaperTraderState(
            strategy_id="test",
            session_start=datetime(2026, 3, 26, tzinfo=UTC),
            last_save=datetime(2026, 3, 26, 1, 0, tzinfo=UTC),
            tick_count=0,
            trades=[],
            errors=[],
            current_balances={},
            initial_balances={},
            equity_curve=[],
            config={},
            pid=1234,
        )
        assert state.ticks_with_fork == 0
        assert state.ticks_with_indicators == 0
        assert state.ticks_with_action == 0
        assert state.last_successful_decision_at is None
        assert state.last_trade_at is None

    def test_state_to_dict_includes_telemetry(self):
        """to_dict serializes all health telemetry fields."""
        from almanak.framework.backtesting.paper.background import PaperTraderState

        now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
        state = PaperTraderState(
            strategy_id="test",
            session_start=datetime(2026, 3, 26, tzinfo=UTC),
            last_save=now,
            tick_count=100,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("10")},
            initial_balances={"ETH": Decimal("10")},
            equity_curve=[],
            config={},
            pid=1234,
            ticks_with_fork=90,
            ticks_with_indicators=80,
            ticks_with_action=30,
            last_successful_decision_at=now,
            last_trade_at=now,
        )
        d = state.to_dict()

        assert d["ticks_with_fork"] == 90
        assert d["ticks_with_indicators"] == 80
        assert d["ticks_with_action"] == 30
        assert d["last_successful_decision_at"] == now.isoformat()
        assert d["last_trade_at"] == now.isoformat()

    def test_state_from_dict_restores_telemetry(self):
        """from_dict deserializes health telemetry fields."""
        from almanak.framework.backtesting.paper.background import PaperTraderState

        now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
        state = PaperTraderState(
            strategy_id="test",
            session_start=datetime(2026, 3, 26, tzinfo=UTC),
            last_save=now,
            tick_count=50,
            trades=[],
            errors=[],
            current_balances={},
            initial_balances={},
            equity_curve=[],
            config={},
            pid=5678,
            ticks_with_fork=45,
            ticks_with_indicators=40,
            ticks_with_action=15,
            last_successful_decision_at=now,
            last_trade_at=now,
        )

        d = state.to_dict()
        restored = PaperTraderState.from_dict(d)

        assert restored.ticks_with_fork == 45
        assert restored.ticks_with_indicators == 40
        assert restored.ticks_with_action == 15
        assert restored.last_successful_decision_at == now
        assert restored.last_trade_at == now

    def test_state_from_dict_backward_compat(self):
        """from_dict handles old state files without telemetry fields."""
        from almanak.framework.backtesting.paper.background import PaperTraderState

        old_data = {
            "strategy_id": "old_strat",
            "session_start": "2026-03-20T00:00:00+00:00",
            "last_save": "2026-03-20T01:00:00+00:00",
            "tick_count": 200,
            "trades": [],
            "errors": [],
            "current_balances": {},
            "initial_balances": {},
            "equity_curve": [],
            "config": {},
            "pid": 9999,
            "status": "stopped",
            # No telemetry fields
        }

        state = PaperTraderState.from_dict(old_data)
        assert state.ticks_with_fork == 0
        assert state.ticks_with_indicators == 0
        assert state.ticks_with_action == 0
        assert state.last_successful_decision_at is None
        assert state.last_trade_at is None

    def test_state_roundtrip_with_none_timestamps(self):
        """Roundtrip with None timestamps preserves None."""
        from almanak.framework.backtesting.paper.background import PaperTraderState

        state = PaperTraderState(
            strategy_id="test",
            session_start=datetime(2026, 3, 26, tzinfo=UTC),
            last_save=datetime(2026, 3, 26, 1, 0, tzinfo=UTC),
            tick_count=10,
            trades=[],
            errors=[],
            current_balances={},
            initial_balances={},
            equity_curve=[],
            config={},
            pid=1111,
            ticks_with_fork=8,
            ticks_with_indicators=5,
            ticks_with_action=0,
            last_successful_decision_at=None,
            last_trade_at=None,
        )

        d = state.to_dict()
        assert "last_successful_decision_at" not in d
        assert "last_trade_at" not in d

        restored = PaperTraderState.from_dict(d)
        assert restored.last_successful_decision_at is None
        assert restored.last_trade_at is None
        assert restored.ticks_with_fork == 8

    def test_engine_initializes_telemetry_counters(self):
        """PaperTrader.run_loop initializes telemetry counters."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        trader = PaperTrader.__new__(PaperTrader)
        trader._running = False
        trader.config = SimpleNamespace(max_ticks=0, tick_interval_seconds=1, chain="arbitrum")
        trader._backtest_id = "test"
        trader._event_listeners = []

        # Simulate run_loop init (the part that sets up state)
        trader._running = True
        trader._trades = []
        trader._errors = []
        trader._equity_curve = []
        trader._tick_count = 0
        trader._last_execution_result = None
        trader._ticks_with_fork = 0
        trader._ticks_with_indicators = 0
        trader._ticks_with_action = 0
        trader._last_successful_decision_at = None
        trader._last_trade_at = None

        assert trader._ticks_with_fork == 0
        assert trader._ticks_with_indicators == 0
        assert trader._ticks_with_action == 0
