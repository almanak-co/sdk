"""VIB-6269 Q4 — price movement injected BETWEEN LP_OPEN compile and submit.

The decision document for the concentrated-liquidity protective minimum states
that CI cannot see this failure mode today and that the decision "does not
consider itself complete without" a test that injects price movement between
compile and submit. This is that test.

Why it matters, concretely. On 2026-08-03 the shipped ``uniswap_lp`` demo
reverted a real Arbitrum WETH/USDC mint with ``Error: Price slippage check``,
burning 205,684 gas, after the pool moved **three ticks (-0.030%)** during the
~26 s the mint spent queued behind three approval transactions. Three ticks is
not a market event; it is ordinary quote staleness. It reverted because the
tolerance was stated in the wrong domain: ``amount0Min``/``amount1Min`` on a
``mint`` constrain the deposit SPLIT, and the split is an amplified function of
price (measured 20.5x for that position), so a flat 0.5% per-leg haircut really
tolerated only 0.024% of price movement.

Every assertion below is a property of the EMITTED calldata evaluated against
the pool's own math (``recompute_lp_amounts`` is the same routine the
NonfungiblePositionManager's ``getLiquidityForAmounts`` +
``getAmountsForLiquidity`` implement), so a green run means the mint would not
have reverted -- not merely that some helper returned some number.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.cl_math import compute_lp_slippage_mins
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.lp_math import recompute_lp_amounts, tick_to_sqrt_ratio_x96
from almanak.framework.intents.vocabulary import Intent

LP_ADAPTER_CLS = "almanak.connectors.uniswap_v3.adapter.UniswapV3LPAdapter"
VALIDATE_POOL = "almanak.connectors.uniswap_v3.pool_validation.validate_v3_pool"
FETCH_SQRT = "almanak.connectors._strategy_base.v3_pool_validation.fetch_v3_pool_sqrt_price_x96"

# --- the 2026-08-03 Arbitrum incident, from the decoded on-chain calldata ----
# failed mint  0xf2a888a3e9d7d0a52241891a487d3429d73d2fb964148a457c73812814f5890c
# pool         0xC6962004f452bE9203591991D15f6b388e09E8D0  (WETH/USDC 0.05%)
INCIDENT_TICK_LOWER = -202100
INCIDENT_TICK_UPPER = -200090
# slot0() read from Arbitrum archive at the two block heights that bracket the
# incident. NOT tick approximations -- these are the exact on-chain values.
#   block 490558890/900 (ts 1785732390/2), tick -201038 -- the compile read
#   block 490558987      (ts 1785732414),  tick -201041 -- the reverted mint
INCIDENT_SQRT_COMPILE = 3416797190762740332110120
INCIDENT_SQRT_EXEC = 3416324775937034248257536
INCIDENT_STRATEGY_ASK = (760_000_000_000_000, 1_425_000)  # 0.000760 WETH, 1.425 USDC
# Emitted calldata, decoded from the reverted mint.
INCIDENT_AMOUNT0_DESIRED = 685_505_212_463_076  # 0.000685505 WETH
INCIDENT_AMOUNT1_DESIRED = 1_424_999  # 1.424999 USDC
INCIDENT_TOLERANCE = Decimal("0.005")  # the demo's explicit max_slippage


def _intent_with(tolerance: Decimal | None) -> SimpleNamespace:
    """Minimal stand-in for the fields ``compute_lp_slippage_mins`` reads."""
    return SimpleNamespace(protocol_params=None, max_slippage=tolerance)


def _price_move_sqrt(sqrt_price_x96: int, fraction: Decimal) -> int:
    """sqrtPriceX96 after moving PRICE by ``fraction`` (price = sqrtPrice**2)."""
    return int(Decimal(sqrt_price_x96) * (Decimal(1) + fraction).sqrt())


def _pool_would_consume(sqrt_price_x96: int, tick_lower: int, tick_upper: int, a0: int, a1: int) -> tuple[int, int]:
    """What NonfungiblePositionManager.mint actually pulls at this price."""
    return recompute_lp_amounts(sqrt_price_x96, tick_lower, tick_upper, a0, a1)


class TestIncidentReplay:
    """Replays the exact mainnet mint that reverted, from archive slot0 reads."""

    def test_compile_block_sqrt_price_reproduces_the_emitted_calldata(self):
        """Chain-verified provenance for the two constants above.

        Feeding the archive-read compile-block ``sqrtPriceX96`` and the
        strategy's ask into the SHIPPED alignment routine must reproduce the
        decoded on-chain calldata exactly. If it does not, the incident
        constants are wrong and every number derived from them is suspect.
        """
        a0, a1 = recompute_lp_amounts(
            INCIDENT_SQRT_COMPILE, INCIDENT_TICK_LOWER, INCIDENT_TICK_UPPER, *INCIDENT_STRATEGY_ASK
        )
        assert (a0, a1) == (INCIDENT_AMOUNT0_DESIRED, INCIDENT_AMOUNT1_DESIRED)

    def test_emitted_minimums_survive_the_three_tick_move_that_reverted_mainnet(self):
        sqrt_compile = INCIDENT_SQRT_COMPILE
        sqrt_exec = INCIDENT_SQRT_EXEC

        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=_intent_with(INCIDENT_TOLERANCE),
            amount0_desired=INCIDENT_AMOUNT0_DESIRED,
            amount1_desired=INCIDENT_AMOUNT1_DESIRED,
            default_lp_slippage=Decimal("0.99"),
            sqrt_price_x96=sqrt_compile,
            tick_lower=INCIDENT_TICK_LOWER,
            tick_upper=INCIDENT_TICK_UPPER,
        )

        consumed0, consumed1 = _pool_would_consume(
            sqrt_exec,
            INCIDENT_TICK_LOWER,
            INCIDENT_TICK_UPPER,
            INCIDENT_AMOUNT0_DESIRED,
            INCIDENT_AMOUNT1_DESIRED,
        )

        # This is the on-chain revert condition, verbatim:
        #   require(amount0 >= amount0Min && amount1 >= amount1Min, 'Price slippage check')
        assert consumed0 >= amount0_min, (
            f"token0 leg would revert: pool consumes {consumed0}, calldata demands {amount0_min}"
        )
        assert consumed1 >= amount1_min, (
            f"token1 leg would revert: pool consumes {consumed1}, calldata demands {amount1_min} "
            f"({(1 - consumed1 / amount1_min) * 100:.4f}% short) -- this is the mainnet revert"
        )

    def test_the_move_that_reverted_was_ordinary_staleness_not_a_market_event(self):
        """Guards the premise: the price barely moved. Fails if the fix is
        credited to a scenario that never contained the bug."""
        move = abs(Decimal(INCIDENT_SQRT_EXEC) ** 2 / Decimal(INCIDENT_SQRT_COMPILE) ** 2 - 1)
        assert move < Decimal("0.0005"), f"price move was {move * 100:.4f}%, expected well under 0.05%"

    def test_the_legacy_flat_haircut_would_still_revert_on_that_move(self):
        """Negative control. Without this, the test above could pass for a
        reason unrelated to the instrument change."""
        flat1 = int(Decimal(INCIDENT_AMOUNT1_DESIRED) * (Decimal(1) - INCIDENT_TOLERANCE))
        _, consumed1 = _pool_would_consume(
            INCIDENT_SQRT_EXEC,
            INCIDENT_TICK_LOWER,
            INCIDENT_TICK_UPPER,
            INCIDENT_AMOUNT0_DESIRED,
            INCIDENT_AMOUNT1_DESIRED,
        )
        assert consumed1 < flat1, (
            f"the flat {INCIDENT_TOLERANCE} haircut should REVERT here "
            f"(consumed {consumed1} vs min {flat1}); if it does not, this scenario "
            f"never contained the bug and proves nothing"
        )


class TestToleranceIsAPriceBand:
    """The user-facing tolerance must mean the same thing on every range."""

    # (tick_lower, tick_upper) around spot tick 0, roughly +/-0.5% .. +/-50%
    @pytest.mark.parametrize(
        "half_width_ticks",
        [50, 100, 200, 500, 1000, 2200, 4050],
        ids=["0.5pct", "1pct", "2pct", "5pct", "10pct", "25pct", "50pct"],
    )
    def test_a_price_move_inside_the_declared_tolerance_never_reverts(self, half_width_ticks):
        tolerance = Decimal("0.005")
        tick_lower, tick_upper = -half_width_ticks, half_width_ticks
        sqrt_compile = tick_to_sqrt_ratio_x96(0)

        # A pool-ratio-exact desired pair, as the compiler emits it.
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=_intent_with(tolerance),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

        # Sweep the whole declared band, both directions.
        for step in range(-10, 11):
            move = tolerance * Decimal(step) / 10
            consumed0, consumed1 = _pool_would_consume(
                _price_move_sqrt(sqrt_compile, move), tick_lower, tick_upper, a0d, a1d
            )
            assert consumed0 >= amount0_min and consumed1 >= amount1_min, (
                f"range [{tick_lower},{tick_upper}] reverts at a {float(move) * 100:+.4f}% price move, "
                f"inside the declared {float(tolerance) * 100}% tolerance: "
                f"consumed=({consumed0}, {consumed1}) mins=({amount0_min}, {amount1_min})"
            )


class TestBothZeroFallback:
    """A band that swamps the range must not ship an unprotected mint."""

    def test_permissive_default_falls_back_to_the_flat_haircut(self):
        tick_lower, tick_upper = -1000, 1000
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=_intent_with(None),  # -> default_lp_slippage
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

        # A +/-99% band leaves the range in both directions, so the band-aware
        # answer is (0, 0). We must not emit that; the flat 1%-of-desired
        # haircut (today's behaviour) stands in.
        assert amount0_min > 0 and amount1_min > 0
        assert amount0_min == int(Decimal(a0d) * Decimal("0.01"))
        assert amount1_min == int(Decimal(a1d) * Decimal("0.01"))

    @pytest.mark.parametrize("half_width_ticks", [50, 200, 1000, 4050])
    @pytest.mark.parametrize("tolerance", ["0", "0.001", "0.005", "0.02", "0.5", "0.99"])
    def test_never_tighter_than_the_legacy_flat_haircut(self, half_width_ticks, tolerance):
        """No mint that compiles today may get a TIGHTER floor tomorrow.

        Analytically the amplification is >= 1 so this always holds, but
        ``recompute_lp_amounts`` returns its inputs unchanged on its bail-out
        paths, which would otherwise emit ``min == desired``.
        """
        tick_lower, tick_upper = -half_width_ticks, half_width_ticks
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        band = compute_lp_slippage_mins(
            intent=_intent_with(Decimal(tolerance)),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        flat = compute_lp_slippage_mins(
            intent=_intent_with(Decimal(tolerance)),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
        )
        assert band[0] <= flat[0] and band[1] <= flat[1], (
            f"emitted min is TIGHTER than the legacy haircut: band={band} flat={flat}"
        )

    def test_a_band_reaching_one_range_bound_emits_a_zero_leg_AND_warns(self, caplog):
        """A narrow range with a real tolerance ships ``amount1Min = 0``.

        This is the honest answer -- the band reaches the range bound, so token1
        is not guaranteed at all -- but it is the shape VIB-6220 / VIB-6226 /
        VIB-6235 file as defects, so it must never be silent.
        """
        tick_lower, tick_upper = -50, 50  # ~+-0.5% range
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        with caplog.at_level("WARNING"):
            m0, m1 = compute_lp_slippage_mins(
                intent=_intent_with(Decimal("0.005")),  # band == range half-width
                amount0_desired=a0d,
                amount1_desired=a1d,
                default_lp_slippage=Decimal("0.99"),
                sqrt_price_x96=sqrt_compile,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
            )

        assert m1 == 0, "expected the down-band to reach the lower bound and zero token1"
        assert m0 > 0, "both-zero would have taken the flat fallback instead"
        warnings_seen = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any("amount1Min = 0" in m for m in warnings_seen), (
            f"a zero minimum shipped with no WARNING naming it. warnings={warnings_seen}"
        )

    @pytest.mark.parametrize(
        "half_width_ticks",
        [500, 1000, 5000, 10000, 20000, 50000, 100000],
        ids=lambda w: f"pm{w}ticks",
    )
    def test_the_permissive_default_NEVER_changes_calldata_on_any_range(self, half_width_ticks):
        """The default path must be byte-identical to the legacy haircut. ALWAYS.

        Keying the fallback on a ``(0, 0)`` band result was not equivalent: a 99%
        band does not cross BOTH range bounds on a wide range, so ticks like
        [-10000, 10000] produced one positive leg and one ZERO leg, silently
        dropping a default caller's token1 floor from 1%-of-desired to 0. This
        sweeps range widths either side of that boundary.
        """
        tick_lower, tick_upper = -half_width_ticks, half_width_ticks
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        with_slot0 = compute_lp_slippage_mins(
            intent=_intent_with(None),  # -> permissive default_lp_slippage
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        legacy = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
        )

        assert with_slot0 == legacy, (
            f"default-path calldata CHANGED on ticks [{tick_lower}, {tick_upper}]: "
            f"band-path emitted {with_slot0}, legacy emits {legacy}"
        )
        assert with_slot0[0] > 0 and with_slot0[1] > 0, (
            f"default path emitted a ZERO floor {with_slot0} — an unprotected leg"
        )

    def test_without_slot0_the_flat_haircut_is_unchanged(self):
        """Callers that cannot read slot0 keep the legacy instrument."""
        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=_intent_with(Decimal("0.005")),
            amount0_desired=1_000_000,
            amount1_desired=2_000_000,
            default_lp_slippage=Decimal("0.99"),
        )
        assert (amount0_min, amount1_min) == (995_000, 1_990_000)


class TestThroughTheCompiler:
    """End-to-end: compile a real LP_OPEN, then move the pool under it."""

    @staticmethod
    def _compile(compiler, mock_slot0, sqrt_price_x96: int | None):
        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
            max_slippage=Decimal("0.005"),
        )
        mock_slot0.return_value = None if sqrt_price_x96 is None else (sqrt_price_x96, 0)
        return compiler.compile(intent)

    @patch(FETCH_SQRT)
    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_emitted_minimums_bound_the_mint_after_an_injected_price_move(
        self, mock_adapter_cls, mock_validate, mock_slot0
    ):
        adapter = MagicMock(name="MockUniV3LPAdapter")
        adapter.get_position_manager_address.return_value = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
        adapter.get_mint_calldata.return_value = b"\xa1\xb2"
        adapter.estimate_mint_gas.return_value = 500_000
        mock_adapter_cls.return_value = adapter

        pool = MagicMock(name="PoolValidationResult")
        pool.exists, pool.is_skipped, pool.error, pool.warning = True, False, None, None
        pool.pool_address = "0x1111111111111111111111111111111111111111"
        mock_validate.return_value = pool

        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address="0x1111111111111111111111111111111111111111",
            config=IntentCompilerConfig(),
            price_oracle={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        )
        compiler.rpc_url = "http://localhost:8545"

        # Pass 1 discovers the tick range the compiler derives for this band,
        # so pass 2 can be stubbed with a slot0 that genuinely sits inside it.
        probe = self._compile(compiler, mock_slot0, None)
        assert probe.status == CompilationStatus.SUCCESS, probe.error
        tick_lower = probe.action_bundle.metadata["tick_lower"]
        tick_upper = probe.action_bundle.metadata["tick_upper"]

        sqrt_compile = tick_to_sqrt_ratio_x96((tick_lower + tick_upper) // 2)
        result = self._compile(compiler, mock_slot0, sqrt_compile)
        assert result.status == CompilationStatus.SUCCESS, result.error
        meta = result.action_bundle.metadata

        a0d, a1d = int(meta["amount0_desired"]), int(meta["amount1_desired"])
        a0_min, a1_min = int(meta["amount0_min"]), int(meta["amount1_min"])
        assert a0d > 0 and a1d > 0, "need a two-legged position for this to be meaningful"

        # ---- the injection: the pool moves after compile, before submit ----
        # -0.03%: the same three-tick drift that reverted mainnet, and far
        # inside the 0.5% the caller declared it would tolerate.
        sqrt_exec = _price_move_sqrt(sqrt_compile, Decimal("-0.0003"))
        consumed0, consumed1 = _pool_would_consume(sqrt_exec, tick_lower, tick_upper, a0d, a1d)

        assert consumed0 >= a0_min and consumed1 >= a1_min, (
            f"mint would revert 'Price slippage check' after a -0.03% move: "
            f"consumed=({consumed0}, {consumed1}) mins=({a0_min}, {a1_min})"
        )
