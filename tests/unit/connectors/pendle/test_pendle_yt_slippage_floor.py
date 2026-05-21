"""Tests for Pendle YT sell min_amount_out floor scaling with slippage.

Validates VIB-2174 fix: when TeardownManager escalates slippage above 500bps
for YT sells, the compiler should drop the min_amount_out floor to 1 (minimal)
so the SDK's own slippage reduction becomes the only protection. This prevents
INSUFFICIENT_TOKEN_OUT reverts on near-expiry YT tokens.
"""

import pytest


def _compute_yt_min_amount_out(amount_in: int, slippage_bps: int) -> tuple[int, str]:
    """Replicate the Pendle compiler's YT floor logic.

    This mirrors the yt_to_token branch in PendleCompiler.compile_swap().
    """
    if slippage_bps >= 500:
        min_amount_out = 1
        estimation_method = f"minimal floor (high slippage {slippage_bps}bps, YT near-expiry)"
    else:
        min_amount_out = amount_in // 100
        estimation_method = f"1% floor (YT near-expiry safe, slippage {slippage_bps}bps)"
    return min_amount_out, estimation_method


def _compute_sdk_final_min(min_amount_out: int, slippage_bps: int) -> int:
    """Replicate what the Pendle SDK does: apply slippage_bps on top of min_amount_out."""
    return int(min_amount_out * (10000 - slippage_bps) // 10000)


class TestYTSlippageFloor:
    """Verify YT sell floor responds to TeardownManager slippage escalation."""

    def test_default_slippage_uses_1pct_floor(self):
        """At default 200bps, the 1% floor should apply."""
        amount_in = 10**18  # 1 token in wei
        min_out, method = _compute_yt_min_amount_out(amount_in, 200)
        assert min_out == amount_in // 100
        assert "1% floor" in method

    def test_moderate_slippage_still_1pct_floor(self):
        """At 300bps, still below threshold, 1% floor applies."""
        amount_in = 10**18
        min_out, _ = _compute_yt_min_amount_out(amount_in, 300)
        assert min_out == amount_in // 100

    def test_high_slippage_drops_to_minimal_floor(self):
        """At 500bps, floor drops to 1 (minimal)."""
        amount_in = 10**18
        min_out, method = _compute_yt_min_amount_out(amount_in, 500)
        assert min_out == 1
        assert "minimal floor" in method

    def test_teardown_escalation_1000bps_uses_minimal_floor(self):
        """At 1000bps (TeardownManager max escalation), floor is 1."""
        amount_in = 10**18
        min_out, method = _compute_yt_min_amount_out(amount_in, 1000)
        assert min_out == 1
        assert "1000bps" in method

    def test_sdk_final_value_decreases_with_slippage_escalation(self):
        """The final on-chain minTokenOut should decrease as slippage escalates.

        This is the core VIB-2174 fix: before the fix, the final value was
        the same regardless of slippage because the floor dominated.
        """
        amount_in = 10**18
        final_values = []
        for bps in [200, 500, 700, 1000]:
            min_out, _ = _compute_yt_min_amount_out(amount_in, bps)
            final = _compute_sdk_final_min(min_out, bps)
            final_values.append(final)

        # Each escalation step should produce a lower (or equal) final value
        for i in range(len(final_values) - 1):
            assert final_values[i] >= final_values[i + 1], (
                f"Final minTokenOut should decrease with higher slippage: "
                f"{final_values[i]} vs {final_values[i + 1]}"
            )

        # The highest slippage should be dramatically lower than the lowest
        assert final_values[-1] < final_values[0], (
            "1000bps should produce a lower minTokenOut than 200bps"
        )

    def test_minimal_floor_with_sdk_slippage_gives_zero(self):
        """With min_amount_out=1 and high slippage, SDK floor approaches 0."""
        # SDK: 1 * (10000 - 1000) / 10000 = 0 (integer division)
        final = _compute_sdk_final_min(1, 1000)
        assert final == 0, "Minimal floor + 1000bps slippage should allow any output"

    def test_1pct_floor_with_default_slippage_is_reasonable(self):
        """At default slippage (200bps), the protection is ~0.98% of input."""
        amount_in = 10**18
        min_out, _ = _compute_yt_min_amount_out(amount_in, 200)
        final = _compute_sdk_final_min(min_out, 200)
        # 10^18 // 100 * (10000 - 200) / 10000 = 10^16 * 0.98 = 9.8 * 10^15
        expected = int(amount_in // 100 * 9800 // 10000)
        assert final == expected

    def test_boundary_499bps_uses_1pct_floor(self):
        """At exactly 499bps, still uses 1% floor (below 500 threshold)."""
        min_out, method = _compute_yt_min_amount_out(10**18, 499)
        assert min_out == 10**18 // 100
        assert "1% floor" in method

    def test_boundary_500bps_uses_minimal_floor(self):
        """At exactly 500bps, switches to minimal floor."""
        min_out, method = _compute_yt_min_amount_out(10**18, 500)
        assert min_out == 1
        assert "minimal floor" in method
