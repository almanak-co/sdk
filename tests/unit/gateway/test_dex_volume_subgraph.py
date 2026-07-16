

class TestCumulativeVolumeDifferencing:
    """Balancer swapVolume is cumulative; daily = consecutive-snapshot diff."""

    def test_differences_consecutive_and_drops_baseline(self):
        from decimal import Decimal

        from almanak.gateway.services._dex_volume_subgraph import _difference_cumulative_points
        from almanak.gateway.services.rate_history_service import DexVolumePoint

        day = 86400
        points = [
            DexVolumePoint(timestamp=0 * day, volume_usd=Decimal("1000")),  # baseline (pre-window)
            DexVolumePoint(timestamp=1 * day, volume_usd=Decimal("1500")),
            DexVolumePoint(timestamp=2 * day, volume_usd=Decimal("1500")),  # idle day
            DexVolumePoint(timestamp=3 * day, volume_usd=Decimal("1400")),  # restatement -> clamp 0
        ]
        daily = _difference_cumulative_points(points, start_ts=1 * day)
        assert [(p.timestamp, p.volume_usd) for p in daily] == [
            (1 * day, Decimal("500")),
            (2 * day, Decimal("0")),
            (3 * day, Decimal("0")),
        ]

    def test_point_without_baseline_is_dropped(self):
        # Snapshots are event-driven: a missing lookback row means an idle
        # pool, and emitting the raw cumulative would report the lifetime
        # total as one day's volume.
        from decimal import Decimal

        from almanak.gateway.services._dex_volume_subgraph import _difference_cumulative_points
        from almanak.gateway.services.rate_history_service import DexVolumePoint

        day = 86400
        points = [DexVolumePoint(timestamp=5 * day, volume_usd=Decimal("1370000000"))]
        daily = _difference_cumulative_points(points, start_ts=5 * day)
        assert daily == []

    def test_in_window_points_after_missing_baseline_still_difference(self):
        from decimal import Decimal

        from almanak.gateway.services._dex_volume_subgraph import _difference_cumulative_points
        from almanak.gateway.services.rate_history_service import DexVolumePoint

        day = 86400
        points = [
            DexVolumePoint(timestamp=5 * day, volume_usd=Decimal("1000")),
            DexVolumePoint(timestamp=6 * day, volume_usd=Decimal("1250")),
        ]
        daily = _difference_cumulative_points(points, start_ts=5 * day)
        assert [(p.timestamp, p.volume_usd) for p in daily] == [(6 * day, Decimal("250"))]


class TestCumulativePageBound:
    def test_lookback_days_count_against_the_page_limit(self):
        from almanak.gateway.services._dex_volume_subgraph import (
            _CUMULATIVE_BASELINE_LOOKBACK_SECONDS,
            _PAGE_FIRST,
            _SECONDS_PER_DAY,
        )

        lookback_days = _CUMULATIVE_BASELINE_LOOKBACK_SECONDS // _SECONDS_PER_DAY
        assert lookback_days == 7
        assert _PAGE_FIRST == 1000
