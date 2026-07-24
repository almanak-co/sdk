"""Branch coverage for InFlightExposureTracker limits and serialization.

Covers ``add_transfer`` limit enforcement (total, per-chain, per-bridge,
bypass, terminal exclusion), ``InFlightAsset.from_dict`` timestamp/default
handling, and ``cleanup_terminal_transfers`` age-based pruning. Pure
in-memory state — no chain access.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.state.in_flight import (
    InFlightAsset,
    InFlightExposureConfig,
    InFlightExposureTracker,
    InFlightLimitExceededError,
    InFlightStatus,
)


def _asset(
    transfer_id="t-1",
    *,
    amount_usd=Decimal("1000"),
    from_chain="arbitrum",
    bridge="across",
    status=InFlightStatus.BRIDGING,
    **overrides,
):
    fields = {
        "transfer_id": transfer_id,
        "token": "USDC",
        "amount": amount_usd,
        "amount_usd": amount_usd,
        "from_chain": from_chain,
        "to_chain": "optimism",
        "bridge": bridge,
        "status": status,
    }
    fields.update(overrides)
    return InFlightAsset(**fields)


def _tracker(**config_overrides):
    config = InFlightExposureConfig(**config_overrides) if config_overrides else None
    return InFlightExposureTracker(chains=["arbitrum", "optimism"], config=config)


class TestAddTransfer:
    def test_adds_and_tracks_transfer(self):
        tracker = _tracker()
        asset = _asset()

        tracker.add_transfer(asset)

        assert tracker.get_transfer("t-1") is asset
        assert tracker.total_in_flight_usd == Decimal("1000")

    def test_total_limit_exceeded_raises(self):
        tracker = _tracker(max_total_in_flight_usd=Decimal("1000"))
        tracker.add_transfer(_asset("t-1", amount_usd=Decimal("800")))

        with pytest.raises(InFlightLimitExceededError) as excinfo:
            tracker.add_transfer(_asset("t-2", amount_usd=Decimal("300")))

        assert excinfo.value.current_usd == Decimal("800")
        assert excinfo.value.new_usd == Decimal("300")
        assert excinfo.value.limit_usd == Decimal("1000")
        assert tracker.get_transfer("t-2") is None

    def test_per_chain_limit_exceeded_raises(self):
        tracker = _tracker(max_per_chain_in_flight_usd={"arbitrum": Decimal("500")})
        tracker.add_transfer(_asset("t-1", amount_usd=Decimal("400")))

        with pytest.raises(InFlightLimitExceededError) as excinfo:
            tracker.add_transfer(_asset("t-2", amount_usd=Decimal("200")))
        assert excinfo.value.limit_usd == Decimal("500")

        # The same amount from an unconstrained chain is accepted.
        tracker.add_transfer(_asset("t-3", amount_usd=Decimal("200"), from_chain="optimism"))
        assert tracker.get_transfer("t-3") is not None

    def test_per_bridge_limit_exceeded_raises(self):
        tracker = _tracker(max_per_bridge_in_flight_usd={"across": Decimal("500")})
        tracker.add_transfer(_asset("t-1", amount_usd=Decimal("400")))

        with pytest.raises(InFlightLimitExceededError) as excinfo:
            tracker.add_transfer(_asset("t-2", amount_usd=Decimal("200")))
        assert excinfo.value.limit_usd == Decimal("500")

        tracker.add_transfer(_asset("t-3", amount_usd=Decimal("200"), bridge="stargate"))
        assert tracker.get_transfer("t-3") is not None

    def test_enforce_limits_false_bypasses_checks(self):
        tracker = _tracker(max_total_in_flight_usd=Decimal("100"))

        tracker.add_transfer(_asset(amount_usd=Decimal("5000")), enforce_limits=False)

        assert tracker.total_in_flight_usd == Decimal("5000")

    def test_terminal_transfers_do_not_count_toward_limit(self):
        tracker = _tracker(max_total_in_flight_usd=Decimal("1000"))
        tracker.add_transfer(
            _asset("t-done", amount_usd=Decimal("900"), status=InFlightStatus.COMPLETED)
        )

        tracker.add_transfer(_asset("t-new", amount_usd=Decimal("900")))

        assert tracker.total_in_flight_usd == Decimal("900")


class TestInFlightAssetFromDict:
    def test_round_trips_through_to_dict(self):
        original = _asset(
            "t-rt",
            status=InFlightStatus.CONFIRMING,
            source_tx_hash="0x" + "11" * 32,
            destination_tx_hash="0x" + "22" * 32,
            expected_completion=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
            retry_count=2,
            metadata={"relayer": "fast"},
        )

        restored = InFlightAsset.from_dict(original.to_dict())

        assert restored == original

    def test_minimal_dict_uses_defaults(self):
        before = datetime.now(UTC)
        restored = InFlightAsset.from_dict(
            {
                "transfer_id": "t-min",
                "token": "USDC",
                "amount": "12.5",
                "amount_usd": "12.5",
                "from_chain": "arbitrum",
                "to_chain": "optimism",
                "bridge": "across",
            }
        )

        assert restored.amount == Decimal("12.5")
        assert restored.status == InFlightStatus.BRIDGING
        assert restored.expected_completion is None
        assert restored.retry_count == 0
        assert restored.metadata == {}
        assert restored.initiated_at >= before
        assert restored.updated_at >= before


class TestCleanupTerminalTransfers:
    def _aged(self, transfer_id, *, status, age_hours):
        asset = _asset(transfer_id, status=status)
        asset.updated_at = datetime.now(UTC) - timedelta(hours=age_hours)
        return asset

    def test_removes_only_old_terminal_transfers(self):
        tracker = _tracker()
        tracker.add_transfer(
            self._aged("old-done", status=InFlightStatus.COMPLETED, age_hours=200)
        )
        tracker.add_transfer(
            self._aged("fresh-done", status=InFlightStatus.FAILED, age_hours=1)
        )
        tracker.add_transfer(
            self._aged("old-active", status=InFlightStatus.BRIDGING, age_hours=200)
        )

        removed = tracker.cleanup_terminal_transfers(max_age_hours=168)

        assert removed == 1
        assert tracker.get_transfer("old-done") is None
        assert tracker.get_transfer("fresh-done") is not None
        assert tracker.get_transfer("old-active") is not None

    def test_empty_tracker_removes_nothing(self):
        assert _tracker().cleanup_terminal_transfers() == 0
