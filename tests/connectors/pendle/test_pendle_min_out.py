"""Characterization + regression tests for the Pendle ``min_amount_out`` floor.

VIB-5329: the near-expiry YT-sell floor previously computed
``min_amount_out = amount_in // 100`` where ``amount_in`` is the RAW YT token count.
But ``min_amount_out`` is denominated in the OUTPUT (underlying) token, and 1 YT is worth
only a small fraction of 1 underlying near expiry. The floor was therefore ~1/rate× too
high (~200× for a market where 1 YT ≈ 0.005 sUSDe), reverting every near-expiry YT sell
with ``Slippage: INSUFFICIENT_TOKEN_OUT``.

The fix derives the floor from the EXPECTED underlying output
(``amount_in × yt_to_asset_rate``), not the raw YT count.

Real-fork proof of this fix (YT entry → amount="all" exit executing on-chain):
``tests/reports/pendle_yt_roundtrip_executes_vib5329.md`` — selling ~9783 YT, the old
floor demanded ~97.83 sUSDe vs a true ~47.97 sUSDe output (1 YT ≈ 0.0062 sUSDe), while the
fixed 50%-of-expected floor was ~30.32 sUSDe and the exit landed (tx 0xe8639c…). The
scenario was first surfaced by the YT chaining round-trip in
``tests/reports/pendle_yt_chaining_roundtrip_vib5301.md``. The illustrative constants
below model the same class of near-expiry YT sell (1 YT worth a small fraction of 1
underlying); exact magnitudes differ per fork block/market.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.pendle.compiler import _compute_pendle_min_out

# Real-fork numbers (sUSDe and YT-sUSDe are both 18-decimal tokens).
YT_AMOUNT = 9_773_940_000_000_000_000_000  # ~9773.94 YT (wei, 18 dec)
REALISTIC_OUTPUT = 50_000_000_000_000_000_000  # ~50 sUSDe (wei, 18 dec) — the true sell return
# 1 YT ≈ 0.005116 sUSDe near expiry → ytToAsset rate.
YT_TO_ASSET_RATE = Decimal("0.005116")


class TestYtSellFloorCharacterization:
    """The core bug + its fix."""

    def test_old_raw_count_floor_overshoots_realistic_output(self):
        """Documents the bug: the OLD formula (raw YT count // 100) is far above realistic output.

        This is the value the pre-fix code produced. It is ~95% above the true ~50 sUSDe return,
        which is exactly why the router reverted with INSUFFICIENT_TOKEN_OUT.
        """
        old_floor = YT_AMOUNT // 100  # the pre-VIB-5329 expression
        assert old_floor == 97_739_400_000_000_000_000  # ~97.74 sUSDe
        # The defect, stated as an assertion: the old floor exceeds the realistic output.
        assert old_floor > REALISTIC_OUTPUT

    def test_yt_sell_floor_is_denominated_in_output_units(self):
        """After the fix: floor is derived from expected output and sits safely BELOW it."""
        floor, method = _compute_pendle_min_out(
            "yt_to_token", YT_AMOUNT, slippage_bps=50, yt_to_asset_rate=YT_TO_ASSET_RATE
        )
        expected_out = int(Decimal(str(YT_AMOUNT)) * YT_TO_ASSET_RATE)  # ~50 sUSDe
        # 50% of expected output, mirroring the PT-sell anti-MEV floor.
        assert floor == expected_out // 2
        # The invariant that FAILS on pre-fix code (which returned ~97.74 sUSDe):
        # a sane floor must never exceed the realistic expected output.
        assert floor <= REALISTIC_OUTPUT
        # And it must be far below the unit-wrong raw-count floor.
        assert floor < YT_AMOUNT // 100
        assert "expected output" in method

    def test_yt_sell_floor_blocks_gross_mev(self):
        """The floor still provides anti-MEV protection (≥ ~50% of expected output)."""
        floor, _ = _compute_pendle_min_out(
            "yt_to_token", YT_AMOUNT, slippage_bps=50, yt_to_asset_rate=YT_TO_ASSET_RATE
        )
        expected_out = int(Decimal(str(YT_AMOUNT)) * YT_TO_ASSET_RATE)
        assert floor >= expected_out // 2

    def test_yt_sell_rate_unavailable_falls_back_to_minimal_floor(self):
        """When the YT/asset rate cannot be read, fall back to a 1-wei floor (NOT raw count)."""
        floor, method = _compute_pendle_min_out(
            "yt_to_token", YT_AMOUNT, slippage_bps=50, yt_to_asset_rate=None
        )
        assert floor == 1
        assert "unavailable" in method
        # Crucially, the fallback is NOT the unit-wrong raw-count floor.
        assert floor != YT_AMOUNT // 100

    def test_yt_sell_high_slippage_minimal_floor_preserved(self):
        """VIB-2174 escalation: >=500bps still yields a 1-wei floor regardless of rate."""
        floor, method = _compute_pendle_min_out(
            "yt_to_token", YT_AMOUNT, slippage_bps=500, yt_to_asset_rate=YT_TO_ASSET_RATE
        )
        assert floor == 1
        assert "high slippage" in method

    def test_yt_sell_zero_rate_post_maturity_minimal_floor(self):
        """Post-maturity YT (rate clamped to 0) → minimal floor, no spurious revert."""
        floor, _ = _compute_pendle_min_out(
            "yt_to_token", YT_AMOUNT, slippage_bps=50, yt_to_asset_rate=Decimal("0")
        )
        assert floor == 1


class TestNonYtSellFloorsUnchanged:
    """No regression on PT sell, PT redeem, or BUY directions."""

    def test_pt_sell_floor_unchanged(self):
        floor, method = _compute_pendle_min_out("pt_to_token", 1_000_000_000_000_000_000, slippage_bps=50)
        assert floor == 500_000_000_000_000_000  # amount_in // 2
        assert "50% floor" in method

    def test_token_to_pt_buy_floor_unchanged(self):
        floor, method = _compute_pendle_min_out("token_to_pt", 1_000_000_000_000_000_000, slippage_bps=50)
        assert floor == 1_000_000_000_000_000_000  # 1:1
        assert "1:1 estimate" in method

    def test_token_to_yt_buy_floor_unchanged(self):
        floor, method = _compute_pendle_min_out("token_to_yt", 1_000_000_000_000_000_000, slippage_bps=50)
        assert floor == 1_000_000_000_000_000_000  # 1:1
        assert "1:1 estimate" in method

    def test_pt_sell_unaffected_by_yt_rate_kwarg(self):
        """Passing a YT/asset rate must not alter the PT-sell path."""
        floor, _ = _compute_pendle_min_out(
            "pt_to_token", 1_000_000_000_000_000_000, slippage_bps=50, yt_to_asset_rate=YT_TO_ASSET_RATE
        )
        assert floor == 500_000_000_000_000_000


class TestAdapterYtToAssetRate:
    """The adapter helper that supplies the rate (derived from ptToAsset, gateway-routed)."""

    def _adapter(self):
        from almanak.connectors.pendle import PendleAdapter

        return PendleAdapter.__new__(PendleAdapter)  # bypass __init__/network

    def test_yt_rate_is_one_minus_pt_rate(self):
        adapter = self._adapter()
        reader = MagicMock()
        reader.get_pt_to_asset_rate.return_value = Decimal("0.994884")
        adapter._on_chain_reader = reader
        adapter._get_on_chain_reader = lambda: reader

        rate = adapter.get_yt_to_asset_rate("0xMarket")
        assert rate == Decimal("1") - Decimal("0.994884")

    def test_yt_rate_clamped_to_zero_post_maturity(self):
        adapter = self._adapter()
        reader = MagicMock()
        reader.get_pt_to_asset_rate.return_value = Decimal("1.01")  # PT >= 1 post-maturity
        adapter._get_on_chain_reader = lambda: reader

        assert adapter.get_yt_to_asset_rate("0xMarket") == Decimal("0")

    def test_yt_rate_none_when_read_fails(self):
        adapter = self._adapter()

        def _boom():
            raise RuntimeError("rpc down")

        adapter._get_on_chain_reader = _boom
        assert adapter.get_yt_to_asset_rate("0xMarket") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
