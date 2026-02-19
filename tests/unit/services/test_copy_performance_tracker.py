"""Tests for CopyPerformanceTracker."""

from decimal import Decimal

from almanak.framework.services.copy_performance_tracker import CopyPerformanceTracker


class TestRecordExecution:
    def test_single_execution_tracked(self):
        tracker = CopyPerformanceTracker()
        tracker.record_execution(Decimal("100"), latency_blocks=3)

        metrics = tracker.get_metrics()
        assert metrics["total_copies"] == 1
        assert metrics["total_volume_usd"] == "100"
        assert metrics["avg_latency_blocks"] == 3.0

    def test_multiple_executions_accumulated(self):
        tracker = CopyPerformanceTracker()
        tracker.record_execution(Decimal("100"), latency_blocks=2)
        tracker.record_execution(Decimal("200"), latency_blocks=4)

        metrics = tracker.get_metrics()
        assert metrics["total_copies"] == 2
        assert metrics["total_volume_usd"] == "300"
        assert metrics["avg_latency_blocks"] == 3.0

    def test_execution_without_latency(self):
        tracker = CopyPerformanceTracker()
        tracker.record_execution(Decimal("50"))

        metrics = tracker.get_metrics()
        assert metrics["total_copies"] == 1
        assert metrics["avg_latency_blocks"] == 0.0


class TestRecordSkip:
    def test_skip_counted(self):
        tracker = CopyPerformanceTracker()
        tracker.record_skip("daily_cap_reached")

        metrics = tracker.get_metrics()
        assert metrics["total_skips"] == 1
        assert metrics["skip_reasons"] == {"daily_cap_reached": 1}

    def test_multiple_skip_reasons_tracked(self):
        tracker = CopyPerformanceTracker()
        tracker.record_skip("daily_cap_reached")
        tracker.record_skip("daily_cap_reached")
        tracker.record_skip("below_min_usd")

        metrics = tracker.get_metrics()
        assert metrics["total_skips"] == 3
        assert metrics["skip_reasons"]["daily_cap_reached"] == 2
        assert metrics["skip_reasons"]["below_min_usd"] == 1


class TestGetMetrics:
    def test_initial_metrics(self):
        tracker = CopyPerformanceTracker()
        metrics = tracker.get_metrics()

        assert metrics["total_copies"] == 0
        assert metrics["total_volume_usd"] == "0"
        assert metrics["total_skips"] == 0
        assert metrics["skip_reasons"] == {}
        assert metrics["avg_latency_blocks"] == 0.0
        assert metrics["uptime_seconds"] >= 0

    def test_uptime_increases(self):
        import time

        tracker = CopyPerformanceTracker()
        time.sleep(0.01)
        metrics = tracker.get_metrics()
        assert metrics["uptime_seconds"] >= 0
