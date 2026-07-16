"""Unit coverage for ``UniswapLPStrategy._pool_spot_price`` (VIB-exp19).

The reference ``uniswap_lp`` demo previously centered its LP range -- and
tested range-exit -- against ``market.price()``, a USD valuation oracle that
is hardcoded to 1.0 for stablecoins and can drift from the actual pool price
for volatile pairs. ``_pool_spot_price`` instead reads the pool's own price
via ``market.pool_price_by_pair()``, falling back to the oracle ratio only
when the pool can't be read (unsupported chain/protocol, RPC failure).

Exercised on the reference demo (``uniswap_lp``); ``traderjoe_lp`` (bin-based
AMM, no pool reader registered) and ``uniswap_v4_hooks`` (hooked pools, the
V4 pool reader only resolves vanilla no-hook PoolKeys) could not receive the
equivalent fix without either inventing new pool-reader support or silently
reading the price of the WRONG pool -- see the experiment writeup for that
gap.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.uniswap_lp.strategy import UniswapLPStrategy

    strat = UniswapLPStrategy.__new__(UniswapLPStrategy)
    strat.token0_symbol = "WETH"
    strat.token1_symbol = "USDC"
    strat._chain = "arbitrum"
    strat.fee_tier = 3000
    return strat


class TestPoolSpotPriceOrientation:
    def test_direct_orientation_used_as_is(self, strategy):
        """Pool price already oriented as token1-per-token0 (matches the
        oracle order) must pass through unchanged."""
        market = MagicMock()
        market.pool_price_by_pair.return_value.price = Decimal("3005")
        oracle_ratio = Decimal("3000")  # ~3000 USDC per WETH

        result = strategy._pool_spot_price(market, oracle_ratio)

        assert result == Decimal("3005")
        market.pool_price_by_pair.assert_called_once_with(
            "WETH", "USDC", chain="arbitrum", protocol="uniswap_v3", fee_tier=3000
        )

    def test_inverted_orientation_is_corrected(self, strategy):
        """When the pool's on-chain token0/token1 order is opposite the
        strategy's config order, pool_price_by_pair returns the reciprocal --
        the oracle-ratio hint must flip it back."""
        market = MagicMock()
        # Reciprocal of ~3000 (i.e. WETH-per-USDC, not USDC-per-WETH).
        market.pool_price_by_pair.return_value.price = Decimal("0.00033")
        oracle_ratio = Decimal("3000")

        result = strategy._pool_spot_price(market, oracle_ratio)

        assert result is not None
        assert abs(result - Decimal(1) / Decimal("0.00033")) < Decimal("0.01")
        # Corrected value must be in the same order of magnitude as the
        # oracle ratio, not the raw (wrongly-oriented) pool reading.
        assert result > Decimal("1000")

    def test_stable_pair_near_one_stays_near_one_either_orientation(self, strategy):
        """Stable/stable pools price near 1.0 in either orientation -- a
        wrong pick costs negligible precision exactly where orientation is
        hardest to disambiguate without an address lookup."""
        strategy.token0_symbol = "USDC"
        strategy.token1_symbol = "USDT"
        market = MagicMock()
        market.pool_price_by_pair.return_value.price = Decimal("1.0004")
        oracle_ratio = Decimal("1.0")  # market.price() stablecoin peg

        result = strategy._pool_spot_price(market, oracle_ratio)

        assert result is not None
        assert abs(result - Decimal("1.0")) < Decimal("0.001")

    def test_exact_peg_oracle_ratio_is_disambiguation_degenerate_and_warns(self, strategy, caplog):
        """Codex review finding (PR #3293): when oracle_ratio == 1.0 exactly
        (stablecoin_peg), ln(oracle_ratio) == 0 makes direct_distance ==
        inverse_distance ALWAYS, so the same-side heuristic can never
        actually disambiguate -- it always keeps pool_price_by_pair's raw
        reading unchanged, for every input. Document that (no silent
        correction happens) and require a loud warning instead of a silent
        guess, since the compiler-level guard is the only remaining
        safety net if the raw reading is actually reversed."""
        strategy.token0_symbol = "USDC"
        strategy.token1_symbol = "USDT"
        market = MagicMock()
        # A pool reading that -- if the caller's orientation guess were
        # wrong -- would actually be USDT-per-USDC's reciprocal, not the
        # value itself. The function has no way to tell from oracle_ratio
        # alone, so it must pass this through unchanged AND warn.
        market.pool_price_by_pair.return_value.price = Decimal("0.9996")
        oracle_ratio = Decimal("1.0")

        with caplog.at_level("WARNING"):
            result = strategy._pool_spot_price(market, oracle_ratio)

        assert result == Decimal("0.9996")  # passed through unchanged, not inverted
        assert any("cannot be disambiguated" in record.message for record in caplog.records)

    def test_non_peg_ratio_near_one_still_warns_but_may_correct(self, strategy, caplog):
        """A near-1.0 oracle_ratio that is NOT exactly 1.0 (e.g. a slightly
        drifted rebasing/wrapped pair) does not hit the exact-peg warning
        branch -- only market.price()'s hardcoded stablecoin_peg (exactly
        1.0) does."""
        strategy.token0_symbol = "USDC"
        strategy.token1_symbol = "USDT"
        market = MagicMock()
        market.pool_price_by_pair.return_value.price = Decimal("1.0004")
        oracle_ratio = Decimal("1.0001")  # close to, but not exactly, 1.0

        with caplog.at_level("WARNING"):
            strategy._pool_spot_price(market, oracle_ratio)

        assert not any("cannot be disambiguated" in record.message for record in caplog.records)


class TestPoolSpotPriceFallback:
    def test_unavailable_pool_reader_falls_back_to_none(self, strategy):
        """No pool reader registered for this chain/protocol -- caller must
        fall back to the oracle ratio (today's behavior), not crash."""
        market = MagicMock()
        market.pool_price_by_pair.side_effect = ValueError("No pool reader registry configured")

        assert strategy._pool_spot_price(market, Decimal("3000")) is None

    def test_pool_price_unavailable_error_falls_back_to_none(self, strategy):
        market = MagicMock()
        market.pool_price_by_pair.side_effect = RuntimeError("pool price unavailable")

        assert strategy._pool_spot_price(market, Decimal("3000")) is None

    def test_non_positive_pool_price_falls_back_to_none(self, strategy):
        market = MagicMock()
        market.pool_price_by_pair.return_value.price = Decimal("0")

        assert strategy._pool_spot_price(market, Decimal("3000")) is None

    def test_non_positive_oracle_ratio_falls_back_to_none(self, strategy):
        market = MagicMock()
        market.pool_price_by_pair.return_value.price = Decimal("3005")

        assert strategy._pool_spot_price(market, Decimal("0")) is None
