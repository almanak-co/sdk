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
    LP_SLIPPAGE_DEFAULT,
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.lp_math import recompute_lp_amounts, tick_to_sqrt_ratio_x96
from almanak.framework.intents.min_out_guard import UnprotectedTradeError
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


def _intent_with(tolerance: Decimal | None, *, require_two_sided_minimums: bool = False) -> SimpleNamespace:
    """Minimal stand-in for the fields ``compute_lp_slippage_mins`` reads."""
    return SimpleNamespace(
        protocol_params=None,
        max_slippage=tolerance,
        require_two_sided_minimums=require_two_sided_minimums,
    )


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

    def test_two_sided_policy_refuses_live_band_reaching_a_range_bound(self):
        """The scaffold opt-in is enforced at the live compiler seam, not only at boot."""
        tick_lower, tick_upper = -50, 50
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        with pytest.raises(UnprotectedTradeError, match="both token minimums remain positive"):
            compute_lp_slippage_mins(
                intent=_intent_with(Decimal("0.005"), require_two_sided_minimums=True),
                amount0_desired=a0d,
                amount1_desired=a1d,
                default_lp_slippage=Decimal("0.99"),
                sqrt_price_x96=sqrt_compile,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
            )

    @pytest.mark.parametrize(
        "half_width_ticks",
        [500, 1000, 5000, 10000, 20000, 50000, 100000],
        ids=lambda w: f"pm{w}ticks",
    )
    def test_the_shipped_default_reaches_the_price_band_on_every_range(self, half_width_ticks):
        """ALM-3186 / VIB-6225 — replaces ``..._NEVER_changes_calldata_on_any_range``.

        That test pinned VIB-6269's PROVENANCE GATE: an undeclared tolerance fell
        through to ``default_lp_slippage``, which was then the ``0.99``
        placeholder, and a placeholder must not reach a correct instrument. The
        gate was explicitly scoped — VIB-6269's decision doc says the class
        "closes when VIB-6225 sets a real default and the fallback stops firing".

        ``LP_SLIPPAGE_DEFAULT`` is now ``0.01``, a real price tolerance, so the
        contract INVERTS: the default must reach the band, and must be strictly
        more protective than the 1%-of-desired rubber stamp it replaced.

        Negative control: restore the provenance gate and every assertion below
        fails — the band-aware pair collapses back onto the flat pair, and
        ``band > flat`` (99% of desired vs 1% of desired) fails immediately.
        """
        tick_lower, tick_upper = -half_width_ticks, half_width_ticks
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        with_slot0 = compute_lp_slippage_mins(
            intent=_intent_with(None),  # -> LP_SLIPPAGE_DEFAULT
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        # What the OLD default (0.99) emitted on this same range: 1% of desired.
        old_default = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=Decimal("0.99"),
        )

        assert with_slot0[0] > 0 and with_slot0[1] > 0, (
            f"default path emitted a ZERO floor {with_slot0} on ticks "
            f"[{tick_lower}, {tick_upper}] — an unprotected leg"
        )
        assert with_slot0[0] > old_default[0] and with_slot0[1] > old_default[1], (
            f"the shipped default is no more protective than the 0.99 placeholder it "
            f"replaced on ticks [{tick_lower}, {tick_upper}]: now={with_slot0}, "
            f"was={old_default}"
        )

        # Still never TIGHTER than its own flat haircut (the VIB-6269 clamp).
        flat_same_tolerance = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
        )
        assert with_slot0[0] <= flat_same_tolerance[0] and with_slot0[1] <= flat_same_tolerance[1], (
            f"band-aware floor is TIGHTER than the flat haircut at the same tolerance: "
            f"band={with_slot0} flat={flat_same_tolerance}"
        )

    def test_the_shipped_default_refuses_a_99pct_adverse_split(self):
        """The headline behaviour change: 99% slippage is no longer accepted.

        The old default emitted ``amount*Min = 1% of desired``, i.e. it accepted
        a mint that consumed 1% of one leg — a 99% adverse split. The band-aware
        default refuses it.
        """
        tick_lower, tick_upper = -5000, 5000
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

        # A 99%-adverse outcome on either leg is now below the emitted floor,
        # i.e. the mint reverts on-chain instead of settling.
        assert int(Decimal(a0d) * Decimal("0.01")) < m0
        assert int(Decimal(a1d) * Decimal("0.01")) < m1

    def test_a_range_narrower_than_the_default_band_falls_back_and_will_revert(self):
        """The accepted tripwire, pinned so it is a decision rather than a surprise.

        A +-0.5% range cannot be floored by a 1% price band — the band reaches
        BOTH bounds, so ``cl_math`` falls back to the flat haircut rather than
        ship an unfloored mint, and a flat 1% haircut on a range this narrow
        reverts on ordinary drift. The remedy is the tolerance, not the
        instrument: declare ``max_slippage`` smaller than the range half-width.
        """
        tick_lower, tick_upper = -50, 50  # ~+-0.5%
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tick_lower, tick_upper, 10**18, 10**18)

        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

        # Flat haircut at the default tolerance — the both-zero fallback.
        assert (m0, m1) == (
            int(Decimal(a0d) * (Decimal(1) - LP_SLIPPAGE_DEFAULT)),
            int(Decimal(a1d) * (Decimal(1) - LP_SLIPPAGE_DEFAULT)),
        )
        # And a declared tolerance INSIDE the range half-width is the remedy.
        d0, d1 = compute_lp_slippage_mins(
            intent=_intent_with(Decimal("0.001")),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        assert d0 > 0 and d1 > 0

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
    def _compile(compiler, mock_slot0, sqrt_price_x96: int | None, *, require_two_sided_minimums: bool = False):
        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
            max_slippage=Decimal("0.005"),
            require_two_sided_minimums=require_two_sided_minimums,
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

    @patch(FETCH_SQRT)
    @patch(VALIDATE_POOL)
    @patch(LP_ADAPTER_CLS)
    def test_two_sided_policy_is_a_compiler_safety_refusal_at_live_range_edge(
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

        probe = self._compile(compiler, mock_slot0, None)
        assert probe.status == CompilationStatus.SUCCESS, probe.error
        tick_upper = int(probe.action_bundle.metadata["tick_upper"])

        result = self._compile(
            compiler,
            mock_slot0,
            tick_to_sqrt_ratio_x96(tick_upper - 1),
            require_two_sided_minimums=True,
        )
        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True
        assert "both token minimums remain positive" in str(result.error)


class TestInheritedToleranceMayNotZeroALeg:
    """ALM-3186 P1 — a single-zero band does not bound price to the advertised %.

    Found by review on PR #3643. Removing VIB-6269's provenance gate sent every
    undeclared LP_OPEN through the band branch, and on an asymmetric range the
    band can zero one leg. That is not "one leg unprotected" — it removes the
    floor in one whole DIRECTION:

    as price falls the position converts to token0, so the surviving token0
    minimum gets EASIER to satisfy while the zeroed token1 minimum can never
    bind. The mint therefore stays acceptable after an arbitrarily large adverse
    move. The old ``0.99`` default emitted a small but POSITIVE floor on both
    legs and so always kept a two-sided tripwire — this PR introduced the
    regression and owns it.

    Fix: an INHERITED tolerance refuses; a DECLARED one keeps the warning.
    """

    # The reviewer's exact case.
    TL, TU = -50, 1000

    def _desired(self):
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, self.TL, self.TU, 10**18, 10**18)
        return sqrt_compile, a0d, a1d

    def test_inherited_default_refuses_instead_of_shipping_a_zero_leg(self):
        """NEGATIVE CONTROL 1: revert the fix and this returns ``(…, 0)`` instead."""
        sqrt_compile, a0d, a1d = self._desired()

        with pytest.raises(UnprotectedTradeError) as exc:
            compute_lp_slippage_mins(
                intent=_intent_with(None),  # -> LP_SLIPPAGE_DEFAULT, nothing declared
                amount0_desired=a0d,
                amount1_desired=a1d,
                default_lp_slippage=LP_SLIPPAGE_DEFAULT,
                sqrt_price_x96=sqrt_compile,
                tick_lower=self.TL,
                tick_upper=self.TU,
            )

        msg = str(exc.value)
        assert "INHERITED" in msg
        assert "amount1Min" in msg, "the message must name the leg that would have been zeroed"
        assert f"[{self.TL}, {self.TU}]" in msg, "the message must name the range"
        assert "max_slippage" in msg, "the message must name the remedy"

    def test_the_refused_calldata_would_have_been_unbounded_downward(self):
        """Why the refusal is right, evaluated against the on-chain revert condition.

        Reproduces what the pre-fix code emitted, then asks ``recompute_lp_amounts``
        — the same routine the position manager implements — what the pool would
        consume at progressively worse prices. Every one of them clears both
        minimums, which is the defect in one assertion.
        """
        sqrt_compile, a0d, a1d = self._desired()

        # What a DECLARED tolerance still emits — byte-identical to what the
        # inherited path emitted before this fix.
        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(Decimal("0.01")),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=self.TL,
            tick_upper=self.TU,
        )
        assert m1 == 0, "expected the band to reach the lower bound and zero token1"

        # -9.5% and -39.3%: both still accepted by that calldata.
        for tick in (-1000, -5000):
            consumed0, consumed1 = recompute_lp_amounts(
                tick_to_sqrt_ratio_x96(tick), self.TL, self.TU, a0d, a1d
            )
            assert consumed0 >= m0 and consumed1 >= m1, (
                "sanity: this calldata is supposed to be unbounded downward"
            )

    def test_a_declared_tolerance_may_still_ship_one_zero_leg(self, caplog):
        """The caller stated the number; one-sided flooring is its honest reading.

        VIB-6269 Defect B / Q5 behaviour, deliberately unchanged.
        """
        sqrt_compile, a0d, a1d = self._desired()

        with caplog.at_level("WARNING"):
            m0, m1 = compute_lp_slippage_mins(
                intent=_intent_with(Decimal("0.01")),
                amount0_desired=a0d,
                amount1_desired=a1d,
                default_lp_slippage=LP_SLIPPAGE_DEFAULT,
                sqrt_price_x96=sqrt_compile,
                tick_lower=self.TL,
                tick_upper=self.TU,
            )

        assert m0 > 0 and m1 == 0
        assert any("amount1Min = 0" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")

    def test_protocol_params_tolerance_also_counts_as_declared(self):
        """``protocol_params["lp_slippage"]`` is a declaration too — same branch."""
        sqrt_compile, a0d, a1d = self._desired()
        declared = SimpleNamespace(
            protocol_params={"lp_slippage": Decimal("0.01")},
            max_slippage=None,
            require_two_sided_minimums=False,
        )

        m0, m1 = compute_lp_slippage_mins(
            intent=declared,
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=self.TL,
            tick_upper=self.TU,
        )
        assert m0 > 0 and m1 == 0

    @pytest.mark.parametrize("tick", [-200, -400, -1000, 100, 200, 400, 1000])
    def test_an_accepted_inherited_mint_binds_in_BOTH_directions(self, tick):
        """NEGATIVE CONTROL 2: a move beyond the band reverts, either way.

        The point of the refusal is that whatever the default DOES accept has a
        real two-sided bound. On a symmetric range the inherited 1% band floors
        both legs, and ``recompute_lp_amounts`` — the on-chain revert condition —
        confirms a >1% move in either direction fails the calldata.

        This is the companion that stops the refusal from being vacuous: it
        proves the default is not simply refusing everything.
        """
        tl, tu = -5000, 5000
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tl, tu, 10**18, 10**18)

        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),  # inherited
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tl,
            tick_upper=tu,
        )
        assert m0 > 0 and m1 > 0, "a symmetric range must keep BOTH legs floored"

        consumed0, consumed1 = recompute_lp_amounts(tick_to_sqrt_ratio_x96(tick), tl, tu, a0d, a1d)
        assert consumed0 < m0 or consumed1 < m1, (
            f"a {tick}-tick move ({(Decimal('1.0001') ** tick - 1) * 100:.3f}%) is outside the "
            f"1% band but the emitted minimums ({m0}, {m1}) still accept "
            f"({consumed0}, {consumed1}) — the floor does not bind"
        )

    def test_within_band_moves_are_still_accepted(self):
        """The bound is at the advertised tolerance, not tighter (liveness)."""
        tl, tu = -5000, 5000
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tl, tu, 10**18, 10**18)
        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tl,
            tick_upper=tu,
        )
        for tick in (-50, 0, 50):
            consumed0, consumed1 = recompute_lp_amounts(tick_to_sqrt_ratio_x96(tick), tl, tu, a0d, a1d)
            assert consumed0 >= m0 and consumed1 >= m1, (
                f"an honest {tick}-tick move inside the band was refused"
            )

    def test_intentionally_single_sided_mint_is_NOT_refused(self):
        """A zero DESIRED leg is not a protection gap — nothing to protect.

        Guards the refusal against over-reach: VIB-6269 explicitly supports
        intentionally single-sided LPs, and they must keep compiling on the
        default path.
        """
        tl, tu = -5000, 5000
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, _ = recompute_lp_amounts(sqrt_compile, tl, tu, 10**18, 10**18)

        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=0,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tl,
            tick_upper=tu,
        )
        assert m0 > 0
        assert m1 == 0

    def test_both_zero_fallback_semantics_are_preserved(self):
        """A band that swamps the range still falls back to the flat haircut.

        Unchanged by the P1 fix — the refusal is scoped to the SINGLE-zero shape.
        """
        tl, tu = -50, 50  # ~+-0.5%, narrower than the 1% band in both directions
        sqrt_compile = tick_to_sqrt_ratio_x96(0)
        a0d, a1d = recompute_lp_amounts(sqrt_compile, tl, tu, 10**18, 10**18)

        m0, m1 = compute_lp_slippage_mins(
            intent=_intent_with(None),
            amount0_desired=a0d,
            amount1_desired=a1d,
            default_lp_slippage=LP_SLIPPAGE_DEFAULT,
            sqrt_price_x96=sqrt_compile,
            tick_lower=tl,
            tick_upper=tu,
        )
        assert (m0, m1) == (
            int(Decimal(a0d) * (Decimal(1) - LP_SLIPPAGE_DEFAULT)),
            int(Decimal(a1d) * (Decimal(1) - LP_SLIPPAGE_DEFAULT)),
        )
