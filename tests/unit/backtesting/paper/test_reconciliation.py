"""Integration tests for position reconciliation.

This module tests the reconciliation functionality including:
- Position drift detection
- Discrepancy identification
- Auto-correction when enabled
- Alert emission for significant discrepancies
"""

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.models import ReconciliationEvent
from almanak.framework.backtesting.paper.position_reconciler import (
    ReconciliationAlert,
    auto_correct_positions,
    compare_positions,
    emit_reconciliation_alerts,
    reconcile_and_correct,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition


class TestPositionDriftDetection:
    """Tests for detecting position drift between tracked and actual positions."""

    def test_detects_amount_discrepancy(self):
        """Test that position drift in token amounts is detected."""
        now = datetime.now(UTC)

        # Create tracked position with 1.0 ETH
        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        # Set a unique position_id for matching
        tracked[0].position_id = "pos_1"

        # Create actual position with 0.9 ETH (10% less - simulates drift)
        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.9"),  # 10% drift
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"  # Same ID for matching

        # Compare positions
        events = compare_positions(tracked, actual, tolerance_pct=Decimal("0.01"))

        # Should detect the discrepancy
        assert len(events) >= 1
        amount_event = next(
            (e for e in events if e.field_name.startswith("amount_")), None
        )
        assert amount_event is not None
        assert amount_event.expected == Decimal("1.0")
        assert amount_event.actual == Decimal("0.9")
        assert amount_event.discrepancy == Decimal("0.1")
        assert amount_event.discrepancy_pct == Decimal("0.1")  # 10%

    def test_detects_missing_tracked_position(self):
        """Test detection of position that exists on-chain but not tracked."""
        now = datetime.now(UTC)

        # No tracked positions
        tracked: list[SimulatedPosition] = []

        # Actual has a position (e.g., discovered on-chain)
        actual = [
            SimulatedPosition.spot(
                token="USDC",
                amount=Decimal("5000"),
                entry_price=Decimal("1"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "untracked_pos"

        events = compare_positions(tracked, actual)

        # Should detect the untracked position
        assert len(events) == 1
        assert events[0].position_id == "untracked_pos"
        assert events[0].field_name == "existence"
        assert events[0].expected == Decimal("0")  # Not tracked
        assert events[0].actual == Decimal("5000")

    def test_detects_missing_actual_position(self):
        """Test detection of position tracked but not found on-chain."""
        now = datetime.now(UTC)

        # Tracked has a position
        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("2.0"),
                entry_price=Decimal("2500"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "lost_pos"

        # No actual positions (position was closed on-chain unexpectedly)
        actual: list[SimulatedPosition] = []

        events = compare_positions(tracked, actual)

        # Should detect the missing position
        assert len(events) == 1
        assert events[0].position_id == "lost_pos"
        assert events[0].field_name == "existence"
        assert events[0].expected == Decimal("2.0")
        assert events[0].actual == Decimal("0")

    def test_detects_lp_liquidity_mismatch(self):
        """Test detection of LP liquidity discrepancy."""
        now = datetime.now(UTC)

        # Create tracked LP position
        tracked_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1.0"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        tracked_lp.position_id = "lp_pos_1"

        # Create actual LP with different liquidity (15% less)
        actual_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0.85"),
            amount1=Decimal("1700"),
            liquidity=Decimal("850000"),  # 15% less
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        actual_lp.position_id = "lp_pos_1"

        events = compare_positions([tracked_lp], [actual_lp], tolerance_pct=Decimal("0.01"))

        # Should detect liquidity mismatch
        liq_event = next((e for e in events if e.field_name == "liquidity"), None)
        assert liq_event is not None
        assert liq_event.expected == Decimal("1000000")
        assert liq_event.actual == Decimal("850000")
        assert liq_event.discrepancy == Decimal("150000")

    def test_detects_perp_notional_mismatch(self):
        """Test detection of perp position notional USD discrepancy."""
        now = datetime.now(UTC)

        # Create tracked perp position (notional = collateral * leverage)
        # collateral=2000, leverage=5 -> notional=10000
        tracked_perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("2000"),
            entry_price=Decimal("2500"),
            entry_time=now,
            leverage=Decimal("5"),
        )
        tracked_perp.position_id = "perp_pos_1"

        # Create actual perp with different notional (partial close on-chain)
        # collateral=1400, leverage=5 -> notional=7000
        actual_perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1400"),
            entry_price=Decimal("2500"),
            entry_time=now,
            leverage=Decimal("5"),
        )
        actual_perp.position_id = "perp_pos_1"

        events = compare_positions([tracked_perp], [actual_perp], tolerance_pct=Decimal("0.01"))

        # Should detect notional mismatch (10000 vs 7000)
        notional_event = next((e for e in events if e.field_name == "notional_usd"), None)
        assert notional_event is not None
        assert notional_event.expected == Decimal("10000")
        assert notional_event.actual == Decimal("7000")

    def test_no_discrepancy_within_tolerance(self):
        """Test that small differences within tolerance don't trigger events."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        # 0.5% difference - within 1% tolerance
        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.995"),  # 0.5% less
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        events = compare_positions(tracked, actual, tolerance_pct=Decimal("0.01"))

        # Should not detect discrepancy (within tolerance)
        amount_events = [e for e in events if e.field_name.startswith("amount_")]
        assert len(amount_events) == 0


class TestAutoCorrectPositions:
    """Tests for auto-correction of position discrepancies."""

    def test_auto_correct_updates_tracked_amount(self):
        """Test that auto-correct updates tracked position to match actual."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.8"),  # 20% drift
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        # First compare to get events
        events = compare_positions(tracked, actual, tolerance_pct=Decimal("0.01"))

        # Apply auto-correction (threshold 5% - will correct 20% discrepancy)
        corrected_tracked, updated_events = auto_correct_positions(
            tracked, actual, events, alert_threshold_pct=Decimal("0.05")
        )

        # Verify tracked position was updated
        assert corrected_tracked[0].get_amount("ETH") == Decimal("0.8")

        # Verify events marked as auto-corrected
        corrected_events = [e for e in updated_events if e.auto_corrected]
        assert len(corrected_events) >= 1

    def test_auto_correct_adds_missing_position(self):
        """Test that auto-correct adds position missing from tracked."""
        now = datetime.now(UTC)

        tracked: list[SimulatedPosition] = []

        actual = [
            SimulatedPosition.spot(
                token="USDC",
                amount=Decimal("10000"),
                entry_price=Decimal("1"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "new_pos"

        events = compare_positions(tracked, actual)

        # Apply auto-correction
        corrected_tracked, updated_events = auto_correct_positions(
            tracked, actual, events, alert_threshold_pct=Decimal("0.05")
        )

        # Verify position was added
        assert len(corrected_tracked) == 1
        assert corrected_tracked[0].position_id == "new_pos"
        assert corrected_tracked[0].get_amount("USDC") == Decimal("10000")

    def test_auto_correct_removes_stale_position(self):
        """Test that auto-correct removes position not found in actual."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "stale_pos"

        actual: list[SimulatedPosition] = []

        events = compare_positions(tracked, actual)

        # Apply auto-correction
        corrected_tracked, updated_events = auto_correct_positions(
            tracked, actual, events, alert_threshold_pct=Decimal("0.05")
        )

        # Verify position was removed
        assert len(corrected_tracked) == 0

    def test_auto_correct_skips_below_threshold(self):
        """Test that auto-correct skips discrepancies below threshold."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        # 3% difference - below 5% threshold
        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.97"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        events = compare_positions(tracked, actual, tolerance_pct=Decimal("0.01"))

        # Apply auto-correction with 5% threshold
        corrected_tracked, updated_events = auto_correct_positions(
            tracked, actual, events, alert_threshold_pct=Decimal("0.05")
        )

        # Tracked should remain unchanged (below threshold)
        assert corrected_tracked[0].get_amount("ETH") == Decimal("1.0")

    def test_auto_correct_updates_lp_liquidity(self):
        """Test auto-correction of LP position liquidity."""
        now = datetime.now(UTC)

        tracked_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1.0"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        tracked_lp.position_id = "lp_1"

        actual_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0.8"),
            amount1=Decimal("1600"),
            liquidity=Decimal("800000"),  # 20% less
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        actual_lp.position_id = "lp_1"

        events = compare_positions([tracked_lp], [actual_lp], tolerance_pct=Decimal("0.01"))

        corrected_tracked, _ = auto_correct_positions(
            [tracked_lp], [actual_lp], events, alert_threshold_pct=Decimal("0.05")
        )

        # Verify liquidity was updated
        assert corrected_tracked[0].liquidity == Decimal("800000")


class TestReconciliationAlerts:
    """Tests for reconciliation alert emission."""

    def test_emits_warning_alert(self):
        """Test that warning alert is emitted for moderate discrepancy."""
        now = datetime.now(UTC)

        # Create event with 10% discrepancy (above 5% warning threshold)
        event = ReconciliationEvent(
            timestamp=now,
            position_id="pos_1",
            expected=Decimal("1000"),
            actual=Decimal("900"),
            discrepancy=Decimal("100"),
            discrepancy_pct=Decimal("0.10"),  # 10%
            field_name="amount_ETH",
            auto_corrected=False,
        )

        alerts = emit_reconciliation_alerts(
            [event],
            alert_threshold_pct=Decimal("0.05"),  # 5%
            critical_threshold_pct=Decimal("0.20"),  # 20%
            strategy_id="test_strategy",
        )

        assert len(alerts) == 1
        assert alerts[0].severity == "WARNING"
        assert alerts[0].position_id == "pos_1"
        assert "10.0%" in alerts[0].message

    def test_emits_critical_alert(self):
        """Test that critical alert is emitted for severe discrepancy."""
        now = datetime.now(UTC)

        # Create event with 30% discrepancy (above 20% critical threshold)
        event = ReconciliationEvent(
            timestamp=now,
            position_id="pos_1",
            expected=Decimal("1000"),
            actual=Decimal("700"),
            discrepancy=Decimal("300"),
            discrepancy_pct=Decimal("0.30"),  # 30%
            field_name="amount_ETH",
            auto_corrected=False,
        )

        alerts = emit_reconciliation_alerts(
            [event],
            alert_threshold_pct=Decimal("0.05"),
            critical_threshold_pct=Decimal("0.20"),
            strategy_id="test_strategy",
        )

        assert len(alerts) == 1
        assert alerts[0].severity == "CRITICAL"

    def test_no_alert_below_threshold(self):
        """Test that no alert is emitted for small discrepancy."""
        now = datetime.now(UTC)

        # Create event with 3% discrepancy (below 5% warning threshold)
        event = ReconciliationEvent(
            timestamp=now,
            position_id="pos_1",
            expected=Decimal("1000"),
            actual=Decimal("970"),
            discrepancy=Decimal("30"),
            discrepancy_pct=Decimal("0.03"),  # 3%
            field_name="amount_ETH",
            auto_corrected=False,
        )

        alerts = emit_reconciliation_alerts(
            [event],
            alert_threshold_pct=Decimal("0.05"),
            critical_threshold_pct=Decimal("0.20"),
            strategy_id="test_strategy",
        )

        assert len(alerts) == 0

    def test_alert_includes_auto_corrected_status(self):
        """Test that alert indicates if position was auto-corrected."""
        now = datetime.now(UTC)

        event = ReconciliationEvent(
            timestamp=now,
            position_id="pos_1",
            expected=Decimal("1000"),
            actual=Decimal("850"),
            discrepancy=Decimal("150"),
            discrepancy_pct=Decimal("0.15"),
            field_name="amount_ETH",
            auto_corrected=True,
        )

        alerts = emit_reconciliation_alerts(
            [event],
            alert_threshold_pct=Decimal("0.05"),
            critical_threshold_pct=Decimal("0.20"),
        )

        assert len(alerts) == 1
        assert alerts[0].auto_corrected is True


class TestReconcileAndCorrectWorkflow:
    """Tests for the full reconcile_and_correct workflow."""

    def test_full_workflow_with_auto_correct_enabled(self):
        """Test complete workflow: compare, correct, emit alerts."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.75"),  # 25% drift
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        events, alerts = reconcile_and_correct(
            tracked=tracked,
            actual=actual,
            auto_correct=True,
            alert_threshold_pct=Decimal("0.05"),
            critical_threshold_pct=Decimal("0.20"),
            tolerance_pct=Decimal("0.01"),
            strategy_id="test_strategy",
        )

        # Should have events and alerts
        assert len(events) >= 1
        assert len(alerts) >= 1

        # Events should be marked as auto-corrected
        corrected_events = [e for e in events if e.auto_corrected]
        assert len(corrected_events) >= 1

        # Alert should be CRITICAL (25% > 20% threshold)
        assert alerts[0].severity == "CRITICAL"

        # Tracked position should be updated
        assert tracked[0].get_amount("ETH") == Decimal("0.75")

    def test_full_workflow_without_auto_correct(self):
        """Test workflow with auto-correct disabled."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0.75"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        events, alerts = reconcile_and_correct(
            tracked=tracked,
            actual=actual,
            auto_correct=False,  # Disabled
            alert_threshold_pct=Decimal("0.05"),
            tolerance_pct=Decimal("0.01"),
            strategy_id="test_strategy",
        )

        # Events should NOT be marked as auto-corrected
        corrected_events = [e for e in events if e.auto_corrected]
        assert len(corrected_events) == 0

        # Tracked position should remain unchanged
        assert tracked[0].get_amount("ETH") == Decimal("1.0")

    def test_workflow_with_no_discrepancies(self):
        """Test workflow when positions match exactly."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "pos_1"

        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("1.0"),  # Exact match
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "pos_1"

        events, alerts = reconcile_and_correct(
            tracked=tracked,
            actual=actual,
            auto_correct=True,
            alert_threshold_pct=Decimal("0.05"),
            tolerance_pct=Decimal("0.01"),
            strategy_id="test_strategy",
        )

        # No discrepancies
        assert len(events) == 0
        assert len(alerts) == 0


class TestMultiPositionReconciliation:
    """Tests for reconciliation with multiple positions."""

    def test_reconcile_multiple_position_types(self):
        """Test reconciliation across spot, LP, and perp positions."""
        now = datetime.now(UTC)

        # Create tracked positions of different types
        spot_tracked = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("1.0"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        spot_tracked.position_id = "spot_1"

        lp_tracked = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0.5"),
            amount1=Decimal("1000"),
            liquidity=Decimal("500000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        lp_tracked.position_id = "lp_1"

        # collateral=1000, leverage=5 -> notional=5000
        perp_tracked = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1000"),
            entry_price=Decimal("2500"),
            entry_time=now,
            leverage=Decimal("5"),
        )
        perp_tracked.position_id = "perp_1"

        tracked = [spot_tracked, lp_tracked, perp_tracked]

        # Create actual positions with some drift
        spot_actual = SimulatedPosition.spot(
            token="ETH",
            amount=Decimal("0.85"),  # 15% drift
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        spot_actual.position_id = "spot_1"

        lp_actual = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0.5"),
            amount1=Decimal("1000"),
            liquidity=Decimal("500000"),  # No drift
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        lp_actual.position_id = "lp_1"

        # collateral=800, leverage=5 -> notional=4000 (20% drift from 5000)
        perp_actual = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("800"),
            entry_price=Decimal("2500"),
            entry_time=now,
            leverage=Decimal("5"),
        )
        perp_actual.position_id = "perp_1"

        actual = [spot_actual, lp_actual, perp_actual]

        events, alerts = reconcile_and_correct(
            tracked=tracked,
            actual=actual,
            auto_correct=True,
            alert_threshold_pct=Decimal("0.05"),
            critical_threshold_pct=Decimal("0.20"),
            tolerance_pct=Decimal("0.01"),
            strategy_id="multi_test",
        )

        # Should detect discrepancies in spot and perp, not LP
        spot_events = [e for e in events if e.position_id == "spot_1"]
        lp_events = [e for e in events if e.position_id == "lp_1"]
        perp_events = [e for e in events if e.position_id == "perp_1"]

        assert len(spot_events) >= 1  # 15% amount drift
        assert len(lp_events) == 0  # No drift
        assert len(perp_events) >= 1  # 20% notional drift

        # Should have alerts for spot (warning) and perp (critical)
        assert len(alerts) >= 2


class TestReconciliationEventSerialization:
    """Tests for ReconciliationEvent serialization."""

    def test_reconciliation_event_to_dict(self):
        """Test ReconciliationEvent serialization."""
        now = datetime.now(UTC)

        event = ReconciliationEvent(
            timestamp=now,
            position_id="pos_1",
            expected=Decimal("1000"),
            actual=Decimal("900"),
            discrepancy=Decimal("100"),
            discrepancy_pct=Decimal("0.10"),
            field_name="amount_ETH",
            auto_corrected=True,
        )

        data = event.to_dict()

        assert data["position_id"] == "pos_1"
        assert data["expected"] == "1000"
        assert data["actual"] == "900"
        assert data["discrepancy"] == "100"
        assert data["discrepancy_pct"] == "0.10"
        assert data["field_name"] == "amount_ETH"
        assert data["auto_corrected"] is True
        assert "timestamp" in data


class TestReconciliationAlertSerialization:
    """Tests for ReconciliationAlert serialization."""

    def test_reconciliation_alert_to_dict(self):
        """Test ReconciliationAlert serialization."""
        now = datetime.now(UTC)

        alert = ReconciliationAlert(
            timestamp=now,
            position_id="pos_1",
            field_name="amount_ETH",
            expected=Decimal("1000"),
            actual=Decimal("800"),
            discrepancy_pct=Decimal("0.20"),
            severity="CRITICAL",
            message="20% discrepancy detected",
            auto_corrected=False,
        )

        data = alert.to_dict()

        assert data["position_id"] == "pos_1"
        assert data["field_name"] == "amount_ETH"
        assert data["expected"] == "1000"
        assert data["actual"] == "800"
        assert data["discrepancy_pct"] == "0.20"
        assert data["severity"] == "CRITICAL"
        assert data["message"] == "20% discrepancy detected"
        assert data["auto_corrected"] is False


class TestEdgeCases:
    """Tests for edge cases in reconciliation."""

    def test_empty_positions(self):
        """Test reconciliation with both lists empty."""
        events, alerts = reconcile_and_correct(
            tracked=[],
            actual=[],
            auto_correct=True,
            alert_threshold_pct=Decimal("0.05"),
            strategy_id="empty_test",
        )

        assert len(events) == 0
        assert len(alerts) == 0

    def test_zero_amount_positions(self):
        """Test reconciliation with zero-amount positions."""
        now = datetime.now(UTC)

        tracked = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        tracked[0].position_id = "zero_pos"

        actual = [
            SimulatedPosition.spot(
                token="ETH",
                amount=Decimal("0"),
                entry_price=Decimal("2000"),
                entry_time=now,
            )
        ]
        actual[0].position_id = "zero_pos"

        events = compare_positions(tracked, actual)

        # No discrepancy between two zero amounts
        amount_events = [e for e in events if e.field_name.startswith("amount_")]
        assert len(amount_events) == 0

    def test_multiple_tokens_same_position(self):
        """Test position with multiple token amounts."""
        now = datetime.now(UTC)

        # LP position has two tokens
        tracked_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("1.0"),
            amount1=Decimal("2000"),
            liquidity=Decimal("1000000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        tracked_lp.position_id = "lp_multi"

        # Actual has drift in both tokens
        actual_lp = SimulatedPosition.lp(
            token0="ETH",
            token1="USDC",
            amount0=Decimal("0.8"),  # 20% less
            amount1=Decimal("1600"),  # 20% less
            liquidity=Decimal("800000"),
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=Decimal("0.003"),
            entry_price=Decimal("2000"),
            entry_time=now,
        )
        actual_lp.position_id = "lp_multi"

        events = compare_positions([tracked_lp], [actual_lp], tolerance_pct=Decimal("0.01"))

        # Should detect discrepancies for both tokens
        token0_events = [e for e in events if "ETH" in e.field_name]
        token1_events = [e for e in events if "USDC" in e.field_name]

        assert len(token0_events) >= 1
        assert len(token1_events) >= 1

    def test_lending_position_interest_discrepancy(self):
        """Test detection of lending position interest accrual discrepancy."""
        now = datetime.now(UTC)

        tracked_supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=now,
            apy=Decimal("0.05"),
        )
        tracked_supply.position_id = "supply_1"
        tracked_supply.interest_accrued = Decimal("100")  # Expected interest

        actual_supply = SimulatedPosition.supply(
            token="USDC",
            amount=Decimal("10000"),
            entry_price=Decimal("1"),
            entry_time=now,
            apy=Decimal("0.05"),
        )
        actual_supply.position_id = "supply_1"
        actual_supply.interest_accrued = Decimal("150")  # 50% more on-chain

        events = compare_positions([tracked_supply], [actual_supply], tolerance_pct=Decimal("0.01"))

        # Should detect interest discrepancy
        interest_events = [e for e in events if e.field_name == "interest_accrued"]
        assert len(interest_events) >= 1
        assert interest_events[0].expected == Decimal("100")
        assert interest_events[0].actual == Decimal("150")
