"""Tests for paper trading dashboard integration.

Covers:
- DASH-PT-02: Gateway paper session discovery
- DASH-PT-03: Dashboard model extension (PaperMetrics, StrategyStatus, data_source)
- DASH-PT-04/05: Overview + detail rendering (model-level, not Streamlit UI)
- DASH-PT-06: Promotion readiness heuristic
- DASH-PT-07: Test coverage

Also validates fixes for Codex review feedback:
- Fix 1: Proto fields execution_mode/paper_metrics_json exist in StrategySummary
- Fix 2: Status filtering applies uniformly to paper sessions
- Fix 3: Promotion heuristic requires trades (not all-holds)
- Fix 4: PnL from equity curve (portfolio value), not sum(trade.net_pnl_usd)
"""

import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.dashboard.models import (
    EquityCurvePoint,
    PaperMetrics,
    Strategy,
    StrategyStatus,
)


# ---------------------------------------------------------------------------
# DASH-PT-03: Model layer tests
# ---------------------------------------------------------------------------


class TestPaperMetrics:
    """Tests for the PaperMetrics dataclass."""

    def test_default_values(self):
        pm = PaperMetrics()
        assert pm.tick_count == 0
        assert pm.success_rate == Decimal("0")  # No decisions = 0%
        assert pm.error_rate == Decimal("0")
        assert pm.is_promotion_ready is False

    def test_success_rate_with_trades(self):
        pm = PaperMetrics(success_count=80, error_count=20)
        assert pm.success_rate == Decimal("0.8")

    def test_error_rate(self):
        pm = PaperMetrics(success_count=95, error_count=5)
        assert pm.error_rate == Decimal("0.05")

    def test_total_decisions_excludes_holds(self):
        """Fix 3: success_rate and error_rate ignore holds."""
        pm = PaperMetrics(success_count=10, error_count=2, hold_count=88)
        assert pm.total_decisions == 12  # Not 100
        assert pm.success_rate == Decimal(10) / Decimal(12)

    def test_all_holds_not_promotion_ready(self):
        """Fix 3: A session with only holds must NOT be promotion ready."""
        pm = PaperMetrics(
            tick_count=100,
            success_count=0,  # No actual trades
            error_count=0,
            hold_count=100,
            session_start=datetime.now(tz=UTC) - timedelta(hours=2),
        )
        assert pm.success_rate == Decimal("0")
        assert pm.is_promotion_ready is False

    def test_promotion_ready_all_criteria_met(self):
        pm = PaperMetrics(
            tick_count=100,
            success_count=95,
            error_count=3,
            hold_count=2,
            session_start=datetime.now(tz=UTC) - timedelta(hours=2),
        )
        assert pm.is_promotion_ready is True

    def test_promotion_not_ready_low_ticks(self):
        pm = PaperMetrics(
            tick_count=10,
            success_count=10,
            error_count=0,
            session_start=datetime.now(tz=UTC) - timedelta(hours=2),
        )
        assert pm.is_promotion_ready is False

    def test_promotion_not_ready_low_success_rate(self):
        pm = PaperMetrics(
            tick_count=100,
            success_count=50,
            error_count=50,
            session_start=datetime.now(tz=UTC) - timedelta(hours=2),
        )
        assert pm.is_promotion_ready is False

    def test_promotion_not_ready_high_error_rate(self):
        pm = PaperMetrics(
            tick_count=100,
            success_count=90,
            error_count=10,
            session_start=datetime.now(tz=UTC) - timedelta(hours=2),
        )
        assert pm.is_promotion_ready is False

    def test_promotion_not_ready_session_too_young(self):
        pm = PaperMetrics(
            tick_count=100,
            success_count=95,
            error_count=3,
            session_start=datetime.now(tz=UTC) - timedelta(minutes=30),
        )
        assert pm.is_promotion_ready is False

    def test_promotion_not_ready_no_session_start(self):
        pm = PaperMetrics(tick_count=100, success_count=95, error_count=3)
        assert pm.is_promotion_ready is False

    def test_session_age_hours(self):
        pm = PaperMetrics(session_start=datetime.now(tz=UTC) - timedelta(hours=3))
        assert pm.session_age_hours >= Decimal("2.9")

    def test_session_age_no_start(self):
        pm = PaperMetrics()
        assert pm.session_age_hours == Decimal("0")

    def test_equity_curve_points(self):
        points = [
            EquityCurvePoint(timestamp=datetime.now(tz=UTC), value_usd=Decimal("100")),
            EquityCurvePoint(timestamp=datetime.now(tz=UTC), value_usd=Decimal("105")),
        ]
        pm = PaperMetrics(equity_curve=points)
        assert len(pm.equity_curve) == 2
        assert pm.equity_curve[1].value_usd == Decimal("105")


class TestStrategyStatusPaperTrading:
    def test_paper_trading_status_exists(self):
        assert StrategyStatus.PAPER_TRADING == "PAPER_TRADING"

    def test_paper_trading_in_enum(self):
        assert "PAPER_TRADING" in [s.value for s in StrategyStatus]


class TestStrategyModelPaperFields:
    def test_default_execution_mode_is_live(self):
        strategy = Strategy(
            id="test", name="Test", status=StrategyStatus.RUNNING,
            pnl_24h_usd=Decimal("0"), total_value_usd=Decimal("0"),
            chain="arbitrum", protocol="Uniswap V3",
        )
        assert strategy.execution_mode == "live"
        assert strategy.paper_metrics is None

    def test_paper_execution_mode(self):
        pm = PaperMetrics(tick_count=50, success_count=40, error_count=2)
        strategy = Strategy(
            id="paper:test", name="Test (Paper)", status=StrategyStatus.PAPER_TRADING,
            pnl_24h_usd=Decimal("0"), total_value_usd=Decimal("0"),
            chain="arbitrum", protocol="Uniswap V3",
            execution_mode="paper", paper_metrics=pm,
        )
        assert strategy.execution_mode == "paper"
        assert strategy.paper_metrics is not None
        assert strategy.paper_metrics.tick_count == 50


# ---------------------------------------------------------------------------
# DASH-PT-03: data_source conversion tests
# ---------------------------------------------------------------------------


class TestDataSourceConversion:
    def test_convert_status_paper_trading(self):
        from almanak.framework.dashboard.data_source import _convert_status
        assert _convert_status("PAPER_TRADING") == StrategyStatus.PAPER_TRADING

    def test_build_paper_metrics_valid_json(self):
        from almanak.framework.dashboard.data_source import _build_paper_metrics
        from almanak.framework.dashboard.gateway_client import StrategySummary

        metrics_data = {
            "tick_count": 100, "success_count": 80, "hold_count": 15, "error_count": 5,
            "simulated_pnl_usd": "1.50", "total_gas_cost_usd": "0.25", "trades_per_hour": "3.5",
            "session_start": "2026-04-01T10:00:00+00:00",
            "equity_curve": [
                {"timestamp": "2026-04-01T10:00:00+00:00", "value": "100"},
                {"timestamp": "2026-04-01T11:00:00+00:00", "value": "101.50"},
            ],
            "error_breakdown": {"rpc_error": 3, "revert": 2},
            "ticks_with_fork": 90, "ticks_with_indicators": 85, "ticks_with_action": 30,
        }

        summary = StrategySummary(
            strategy_id="test", name="Test", status="PAPER_TRADING",
            chain="arbitrum", protocol="Uniswap V3",
            total_value_usd=Decimal("0"), pnl_24h_usd=Decimal("0"),
            last_action_at=None, attention_required=False, attention_reason="",
            is_multi_chain=False, paper_metrics_json=json.dumps(metrics_data),
        )

        pm = _build_paper_metrics(summary)
        assert pm is not None
        assert pm.tick_count == 100
        assert pm.success_count == 80
        assert pm.error_count == 5
        assert pm.simulated_pnl_usd == Decimal("1.50")
        assert len(pm.equity_curve) == 2
        assert pm.error_breakdown == {"rpc_error": 3, "revert": 2}
        assert pm.ticks_with_fork == 90

    def test_build_paper_metrics_empty_json(self):
        from almanak.framework.dashboard.data_source import _build_paper_metrics
        from almanak.framework.dashboard.gateway_client import StrategySummary

        summary = StrategySummary(
            strategy_id="test", name="Test", status="RUNNING",
            chain="arbitrum", protocol="",
            total_value_usd=Decimal("0"), pnl_24h_usd=Decimal("0"),
            last_action_at=None, attention_required=False, attention_reason="",
            is_multi_chain=False, paper_metrics_json="",
        )
        assert _build_paper_metrics(summary) is None

    def test_build_paper_metrics_invalid_json(self):
        from almanak.framework.dashboard.data_source import _build_paper_metrics
        from almanak.framework.dashboard.gateway_client import StrategySummary

        summary = StrategySummary(
            strategy_id="test", name="Test", status="PAPER_TRADING",
            chain="arbitrum", protocol="",
            total_value_usd=Decimal("0"), pnl_24h_usd=Decimal("0"),
            last_action_at=None, attention_required=False, attention_reason="",
            is_multi_chain=False, paper_metrics_json="{invalid json",
        )
        assert _build_paper_metrics(summary) is None

    def test_build_paper_metrics_zero_trades(self):
        from almanak.framework.dashboard.data_source import _build_paper_metrics
        from almanak.framework.dashboard.gateway_client import StrategySummary

        metrics_data = {
            "tick_count": 10, "success_count": 0, "hold_count": 10, "error_count": 0,
            "equity_curve": [], "error_breakdown": {},
        }

        summary = StrategySummary(
            strategy_id="test", name="Test", status="PAPER_TRADING",
            chain="arbitrum", protocol="",
            total_value_usd=Decimal("0"), pnl_24h_usd=Decimal("0"),
            last_action_at=None, attention_required=False, attention_reason="",
            is_multi_chain=False, paper_metrics_json=json.dumps(metrics_data),
        )

        pm = _build_paper_metrics(summary)
        assert pm is not None
        assert pm.tick_count == 10
        assert pm.success_count == 0
        assert pm.success_rate == Decimal("0")  # No decisions


# ---------------------------------------------------------------------------
# Fix 1: Proto fields exist in StrategySummary
# ---------------------------------------------------------------------------


class TestProtoFields:
    """Verify execution_mode and paper_metrics_json exist in the proto."""

    def test_proto_has_execution_mode_field(self):
        from almanak.gateway.proto import gateway_pb2
        msg = gateway_pb2.StrategySummary()
        msg.execution_mode = "paper"
        assert msg.execution_mode == "paper"

    def test_proto_has_paper_metrics_json_field(self):
        from almanak.gateway.proto import gateway_pb2
        msg = gateway_pb2.StrategySummary()
        msg.paper_metrics_json = '{"tick_count": 42}'
        assert msg.paper_metrics_json == '{"tick_count": 42}'

    def test_proto_roundtrip(self):
        """Verify data survives serialize -> deserialize."""
        from almanak.gateway.proto import gateway_pb2
        msg = gateway_pb2.StrategySummary(
            strategy_id="paper:test",
            name="Test (Paper)",
            status="PAPER_TRADING",
            execution_mode="paper",
            paper_metrics_json='{"tick_count": 100}',
        )
        serialized = msg.SerializeToString()
        msg2 = gateway_pb2.StrategySummary()
        msg2.ParseFromString(serialized)
        assert msg2.execution_mode == "paper"
        assert msg2.paper_metrics_json == '{"tick_count": 100}'


# ---------------------------------------------------------------------------
# DASH-PT-02: Gateway paper session discovery tests
# ---------------------------------------------------------------------------


class TestGatewayPaperSessionDiscovery:
    def _make_state_file(self, tmpdir: Path, strategy_id: str, **overrides) -> Path:
        data = {
            "strategy_id": strategy_id,
            "session_start": "2026-04-01T10:00:00+00:00",
            "last_save": datetime.now(tz=UTC).isoformat(),
            "tick_count": 50,
            "status": "running",
            "pid": os.getpid(),
            "config": {"chain": "arbitrum", "protocol": "Uniswap V3"},
            "trades": [
                {"timestamp": "2026-04-01T10:30:00+00:00", "gas_cost_usd": "0.10", "net_pnl_usd": "0.50"}
            ],
            "errors": [],
            "equity_curve": [
                {"timestamp": "2026-04-01T10:00:00+00:00", "value": "100"},
                {"timestamp": "2026-04-01T11:00:00+00:00", "value": "100.50"},
            ],
            "ticks_with_fork": 45, "ticks_with_indicators": 40, "ticks_with_action": 10,
        }
        data.update(overrides)
        state_file = tmpdir / f"{strategy_id}.state.json"
        state_file.write_text(json.dumps(data))
        return state_file

    def _make_servicer(self):
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer
        servicer = DashboardServiceServicer.__new__(DashboardServiceServicer)
        servicer._strategies_root = None
        return servicer

    def test_discovers_active_session(self, tmp_path):
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        self._make_state_file(paper_dir, "test_strategy")

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()

        assert len(sessions) == 1
        session = sessions[0]
        assert session["strategy_id"] == "paper:test_strategy"
        assert session["status"] == "PAPER_TRADING"
        assert session["execution_mode"] == "paper"
        assert session["chain"] == "arbitrum"
        assert session["paper_metrics_json"]

        metrics = json.loads(session["paper_metrics_json"])
        assert metrics["tick_count"] == 50
        assert metrics["ticks_with_fork"] == 45

    def test_pnl_from_equity_curve_not_trade_sum(self, tmp_path):
        """Fix 4: PnL should come from equity curve (portfolio value), not sum of trade PnL."""
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        self._make_state_file(
            paper_dir, "pnl_test",
            equity_curve=[
                {"timestamp": "2026-04-01T10:00:00+00:00", "value": "100.00"},
                {"timestamp": "2026-04-01T11:00:00+00:00", "value": "103.50"},
            ],
            trades=[
                {"timestamp": "2026-04-01T10:30:00+00:00", "gas_cost_usd": "0.10", "net_pnl_usd": "0.50"},
                {"timestamp": "2026-04-01T10:45:00+00:00", "gas_cost_usd": "0.10", "net_pnl_usd": "0.30"},
            ],
        )

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()

        metrics = json.loads(sessions[0]["paper_metrics_json"])
        # Should be 103.50 - 100.00 = 3.50 (equity-based), NOT 0.50 + 0.30 = 0.80 (trade-sum)
        assert Decimal(metrics["simulated_pnl_usd"]) == Decimal("3.50")

    def test_marks_stopped_as_inactive(self, tmp_path):
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        self._make_state_file(paper_dir, "stopped_strat", status="stopped")

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()

        assert len(sessions) == 1
        assert sessions[0]["status"] == "INACTIVE"

    def test_marks_stale_as_inactive(self, tmp_path):
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        self._make_state_file(
            paper_dir, "stale_strat",
            pid=999999999,
            last_save=(datetime.now(tz=UTC) - timedelta(minutes=10)).isoformat(),
        )

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()

        assert len(sessions) == 1
        assert sessions[0]["status"] == "INACTIVE"

    def test_handles_missing_directory(self, tmp_path):
        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()
        assert sessions == []

    def test_handles_corrupt_state_file(self, tmp_path):
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        (paper_dir / "corrupt.state.json").write_text("not valid json{{{")

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()
        assert sessions == []

    def test_equity_curve_downsampling(self, tmp_path):
        paper_dir = tmp_path / ".almanak" / "paper"
        paper_dir.mkdir(parents=True)
        big_curve = [
            {"timestamp": f"2026-04-01T{i // 60:02d}:{i % 60:02d}:00+00:00", "value": str(100 + i * 0.01)}
            for i in range(500)
        ]
        self._make_state_file(paper_dir, "big_strat", equity_curve=big_curve)

        with patch.object(Path, "home", return_value=tmp_path):
            sessions = self._make_servicer()._discover_paper_sessions()

        metrics = json.loads(sessions[0]["paper_metrics_json"])
        assert len(metrics["equity_curve"]) == 200


# ---------------------------------------------------------------------------
# Theme and utils tests
# ---------------------------------------------------------------------------


class TestThemeAndUtils:
    def test_status_color_for_paper_trading(self):
        from almanak.framework.dashboard.theme import get_status_color
        assert get_status_color(StrategyStatus.PAPER_TRADING) == "#2196f3"

    def test_status_icon_for_paper_trading(self):
        from almanak.framework.dashboard.utils import get_status_icon
        assert get_status_icon(StrategyStatus.PAPER_TRADING) == "\U0001f535"
