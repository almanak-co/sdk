"""Regression tests for measurement-derived archive gating (VIB-5869 / ALM-2695).

The bug: `fork_requires_archive` membership was folklore. Chains were added
one incident at a time, so the set ended up INVERTED relative to real
exposure — Ethereum (longest public-RPC window, ~19min) was flagged while
Arbitrum (~16s) and BSC (~48s) were not. A managed fork on BSC therefore
started happily and died minutes later on `missing trie node`.

These tests pin the flag set to the measured table so it cannot drift back
into folklore, and pin the gate to fail-fast so a doomed fork never starts.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    fork_archive_required_chains,
    fork_cold_start_slow_chains,
)
from almanak.core.chains._rpc_retention import (
    PUBLIC_RPC_RETENTION,
    SAFE_FORK_WINDOW_SECONDS,
    RetentionMeasurement,
    measured_fork_threatening_chains,
)


class TestMeasurementDrivesMembership:
    def test_measured_fork_threatening_chains_are_all_flagged(self) -> None:
        """THE core invariant: measurement is a lower bound on the flag set.

        Any chain measured to wedge a fork MUST refuse to start without an
        archive RPC. This is the assertion that fails if someone drops a flag
        (mutation-tested: removing `fork_requires_archive=True` from bsc.py
        fails exactly here).
        """
        flagged = fork_archive_required_chains()
        missing = measured_fork_threatening_chains() - flagged
        assert not missing, (
            f"Chains measured to wedge a fork but not flagged: {sorted(missing)}. "
            "Every chain in _rpc_retention.PUBLIC_RPC_RETENTION with a fork-threatening "
            "window must declare fork_requires_archive=True."
        )

    @pytest.mark.parametrize("chain", ["bsc", "arbitrum", "base", "optimism"])
    def test_alm_2695_chains_are_flagged(self, chain: str) -> None:
        """The four chains this ticket exists for. Explicit, not just derived —
        if the measurement table is ever gutted, these still hold the line."""
        assert chain in fork_archive_required_chains(), (
            f"{chain}'s public RPC serves seconds-to-minutes of state; a managed "
            f"fork wedges on the first cold read past that window (ALM-2695)."
        )

    def test_flag_set_is_not_looser_than_before_vib_5869(self) -> None:
        """Ratchet: VIB-646 / VIB-3971 / VIB-3973 flagged these after real
        production stalls. Measurement may TIGHTEN the guard, never loosen it."""
        legacy = {"polygon", "ethereum", "avalanche", "zerog", "xlayer"}
        assert legacy <= fork_archive_required_chains(), (
            f"Dropped a pre-existing archive flag: {sorted(legacy - fork_archive_required_chains())}"
        )

    def test_every_flagged_chain_is_in_the_measurement_table(self) -> None:
        """No flag without a recorded rationale — that is how the set became
        folklore the first time."""
        undocumented = fork_archive_required_chains() - set(PUBLIC_RPC_RETENTION)
        assert not undocumented, (
            f"Chains flagged with no entry in _rpc_retention.py: {sorted(undocumented)}. "
            "Measure the endpoint and record it rather than adding a bare flag."
        )

    def test_measurement_table_only_covers_registered_chains(self) -> None:
        unknown = set(PUBLIC_RPC_RETENTION) - set(ChainRegistry.names())
        assert not unknown, f"Measurements for unregistered chains: {sorted(unknown)}"

    def test_endpoint_measured_matches_the_descriptor(self) -> None:
        """A measurement is evidence about the endpoint probed. If a chain's
        public_rpc changes, the measurement is void and must be re-taken."""
        for name, m in PUBLIC_RPC_RETENTION.items():
            declared = ChainRegistry.get(name).rpc.public_rpc
            assert m.endpoint == declared, (
                f"{name}: measured {m.endpoint!r} but descriptor now declares {declared!r}. "
                "Re-run scripts/measure_rpc_state_retention.py."
            )


class TestThreatPredicate:
    def test_archive_endpoint_does_not_threaten(self) -> None:
        m = RetentionMeasurement("https://x", archive=True, retention_blocks=None, window_seconds=None, evidence="e")
        assert m.threatens_fork() is False

    def test_unmeasured_endpoint_does_not_hard_fail(self) -> None:
        """Empty != Zero. An unmeasured endpoint has no evidence to gate on and
        must not become a hard startup failure by default."""
        m = RetentionMeasurement("https://x", archive=None, retention_blocks=None, window_seconds=None, evidence="e")
        assert m.threatens_fork() is False

    def test_short_window_threatens(self) -> None:
        m = RetentionMeasurement("https://x", archive=False, retention_blocks=64, window_seconds=48.0, evidence="e")
        assert m.threatens_fork() is True

    def test_pruned_but_long_window_does_not_threaten(self) -> None:
        """Monad: pruned, but ~11.5 days of state outlives any run. 'Not
        archive' must not be conflated with 'unsafe' or the gate becomes noise."""
        m = RetentionMeasurement(
            "https://x", archive=False, retention_blocks=2_491_624, window_seconds=998_952.0, evidence="e"
        )
        assert m.threatens_fork() is False
        assert "monad" not in measured_fork_threatening_chains()

    def test_pruned_with_unknown_window_fails_safe(self) -> None:
        m = RetentionMeasurement("https://x", archive=False, retention_blocks=64, window_seconds=None, evidence="e")
        assert m.threatens_fork() is True

    def test_threshold_boundary(self) -> None:
        below = RetentionMeasurement(
            "https://x", archive=False, retention_blocks=1, window_seconds=SAFE_FORK_WINDOW_SECONDS - 1, evidence="e"
        )
        at = RetentionMeasurement(
            "https://x", archive=False, retention_blocks=1, window_seconds=SAFE_FORK_WINDOW_SECONDS, evidence="e"
        )
        assert below.threatens_fork() is True
        assert at.threatens_fork() is False


class TestAxesAreSeparate:
    def test_archive_and_slow_start_are_distinct_concerns(self) -> None:
        """VIB-5869 split one overloaded flag into two. Arbitrum proves they
        must stay split: it needs archive state but forks fast."""
        assert "arbitrum" in fork_archive_required_chains()
        assert "arbitrum" not in fork_cold_start_slow_chains()

    def test_cold_start_slow_set_preserves_pre_split_timeout_behaviour(self) -> None:
        """The split must not move any timeout. These are exactly the chains
        that got the 90s budget before VIB-5869."""
        assert fork_cold_start_slow_chains() == frozenset({"ethereum", "polygon", "avalanche", "zerog", "xlayer"})
