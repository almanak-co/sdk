"""Tests for Pendle YT sell min_amount_out floor scaling with slippage.

Validates VIB-2174: when TeardownManager escalates slippage above 500bps for YT
sells, the compiler drops the min_amount_out floor to 1 (minimal) so the SDK's own
slippage reduction becomes the only protection — preventing INSUFFICIENT_TOKEN_OUT
reverts on near-expiry YT tokens.

These tests call the REAL ``_compute_pendle_min_out`` (no local reimplementation) so
they cannot drift from production. Below 500bps the floor is VIB-5329's
50%-of-expected-output value (``amount_in × yt_to_asset_rate``), NOT the raw YT count.
"""

from decimal import Decimal

from almanak.connectors.pendle.compiler import _compute_pendle_min_out

# A representative near-expiry YT/asset rate (1 YT ≈ 0.005 underlying).
YT_RATE = Decimal("0.005")


def _yt_min_out(amount_in: int, slippage_bps: int) -> tuple[int, str]:
    """Production YT-sell floor for a given slippage, with the YT/asset rate supplied."""
    return _compute_pendle_min_out("yt_to_token", amount_in, slippage_bps, yt_to_asset_rate=YT_RATE)


def _expected_output_floor(amount_in: int) -> int:
    """The VIB-5329 floor: 50% of expected underlying output (mirrors production)."""
    return max(1, int(Decimal(str(amount_in)) * YT_RATE) // 2)


def _compute_sdk_final_min(min_amount_out: int, slippage_bps: int) -> int:
    """Replicate what the Pendle SDK does: apply slippage_bps on top of min_amount_out."""
    return int(min_amount_out * (10000 - slippage_bps) // 10000)


class TestYTSlippageFloor:
    """Verify YT sell floor responds to TeardownManager slippage escalation."""

    def test_default_slippage_uses_expected_output_floor(self):
        """At default 200bps, the 50%-of-expected-output floor applies (VIB-5329)."""
        amount_in = 10**18  # 1 YT in wei
        min_out, method = _yt_min_out(amount_in, 200)
        assert min_out == _expected_output_floor(amount_in)
        assert "expected output" in method
        # Must NOT be the old unit-wrong raw-count floor.
        assert min_out != amount_in // 100

    def test_moderate_slippage_still_expected_output_floor(self):
        """At 300bps, still below threshold, the expected-output floor applies."""
        amount_in = 10**18
        min_out, _ = _yt_min_out(amount_in, 300)
        assert min_out == _expected_output_floor(amount_in)

    def test_high_slippage_drops_to_minimal_floor(self):
        """At 500bps, floor drops to 1 (minimal)."""
        amount_in = 10**18
        min_out, method = _yt_min_out(amount_in, 500)
        assert min_out == 1
        assert "minimal floor" in method

    def test_teardown_escalation_1000bps_uses_minimal_floor(self):
        """At 1000bps (TeardownManager max escalation), floor is 1."""
        amount_in = 10**18
        min_out, method = _yt_min_out(amount_in, 1000)
        assert min_out == 1
        assert "1000bps" in method

    def test_sdk_final_value_decreases_with_slippage_escalation(self):
        """The final on-chain minTokenOut should decrease as slippage escalates.

        This is the core VIB-2174 fix: before it, the final value was the same
        regardless of slippage because the floor dominated.
        """
        amount_in = 10**18
        final_values = []
        for bps in [200, 500, 700, 1000]:
            min_out, _ = _yt_min_out(amount_in, bps)
            final = _compute_sdk_final_min(min_out, bps)
            final_values.append(final)

        # Each escalation step should produce a lower (or equal) final value.
        for i in range(len(final_values) - 1):
            assert final_values[i] >= final_values[i + 1], (
                f"Final minTokenOut should decrease with higher slippage: "
                f"{final_values[i]} vs {final_values[i + 1]}"
            )

        # The highest slippage should be dramatically lower than the lowest.
        assert final_values[-1] < final_values[0], "1000bps should produce a lower minTokenOut than 200bps"

    def test_minimal_floor_with_sdk_slippage_gives_zero(self):
        """With min_amount_out=1 and high slippage, SDK floor approaches 0."""
        # SDK: 1 * (10000 - 1000) / 10000 = 0 (integer division)
        final = _compute_sdk_final_min(1, 1000)
        assert final == 0, "Minimal floor + 1000bps slippage should allow any output"

    def test_expected_output_floor_with_default_slippage_is_reasonable(self):
        """At default slippage (200bps), the protection is ~0.98 × the expected-output floor."""
        amount_in = 10**18
        min_out, _ = _yt_min_out(amount_in, 200)
        final = _compute_sdk_final_min(min_out, 200)
        expected = int(_expected_output_floor(amount_in) * 9800 // 10000)
        assert final == expected

    def test_rate_unavailable_falls_back_to_minimal_floor(self):
        """VIB-5329: with no YT/asset rate, the sub-500bps floor degrades to 1 (not raw count)."""
        amount_in = 10**18
        min_out, method = _compute_pendle_min_out("yt_to_token", amount_in, 200, yt_to_asset_rate=None)
        assert min_out == 1
        assert "unavailable" in method
        assert min_out != amount_in // 100

    def test_boundary_499bps_uses_expected_output_floor(self):
        """At exactly 499bps, still uses the expected-output floor (below 500 threshold)."""
        amount_in = 10**18
        min_out, method = _yt_min_out(amount_in, 499)
        assert min_out == _expected_output_floor(amount_in)
        assert "expected output" in method

    def test_boundary_500bps_uses_minimal_floor(self):
        """At exactly 500bps, switches to minimal floor."""
        min_out, method = _yt_min_out(10**18, 500)
        assert min_out == 1
        assert "minimal floor" in method
