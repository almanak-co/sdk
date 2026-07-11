"""VIB-5740 regression: TraderJoe V2 SWAP oracle-aware price-impact guard.

Before this fix, ``amount_out_min`` was derived from the pool's own
``getSwapOut`` quote (``adapter.build_swap_transaction``), so a swap into a
one-sided / drained LB pair (where the quote is itself near-zero) passed its
own slippage floor and executed at ~99.85% loss (real-mainnet run
20260710-2222-noneth-traderjoe_lp). The guard now cross-checks the on-chain
quote against an INDEPENDENT oracle estimate and fails closed on excessive
impact.

These tests exercise ``_TraderJoeV2CompileImpl._guard_swap_price_impact``
directly — the single method that carries the fix — with a mock context, so
they need neither RPC nor the full compile harness.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.connectors.traderjoe_v2.compiler import _TraderJoeV2CompileImpl
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

# 0.0497 WAVAX ≈ $0.33 → 330_000 USDC units (6 dec): the honest oracle output.
ORACLE_OUT_WEI = 330_000
# What the drained LB pair actually quotes: 499 units ($0.000499) — the real
# on-chain value from the VIB-5740 run.
DRAINED_QUOTE_HUMAN = Decimal("0.000499")
# A healthy quote ~0.6% below oracle.
HEALTHY_QUOTE_HUMAN = Decimal("0.328")

USDC = SimpleNamespace(symbol="USDC", decimals=6)
WAVAX = SimpleNamespace(symbol="WAVAX", decimals=18)


def _make_impl(oracle_out_wei: int | Exception, *, rpc_url: str = "https://api.avax.network/ext/bc/C/rpc"):
    ctx = MagicMock()
    ctx.rpc_url = rpc_url
    ctx.max_price_impact_pct = Decimal("0.05")
    ctx.using_placeholders = False
    ctx.permission_discovery = False
    if isinstance(oracle_out_wei, Exception):
        ctx.services.calculate_expected_output.side_effect = oracle_out_wei
    else:
        ctx.services.calculate_expected_output.return_value = oracle_out_wei
    return _TraderJoeV2CompileImpl(ctx)


def _intent():
    return SimpleNamespace(
        max_price_impact=None,
        max_slippage=Decimal("0.01"),
        from_token="WAVAX",
        to_token="USDC",
        intent_id="tj-guard-test",
    )


def _guard(impl, quote_human: Decimal):
    return impl._guard_swap_price_impact(
        _intent(), WAVAX, USDC, amount_in_wei=10**16, quote=SimpleNamespace(amount_out=quote_human), rpc_url=impl._ctx.rpc_url
    )


def test_drained_pool_swap_fails_closed():
    """The VIB-5740 case: a nonzero quote ~99.85% below oracle → FAILED."""
    result = _guard(_make_impl(ORACLE_OUT_WEI), DRAINED_QUOTE_HUMAN)
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert "Price impact too high" in result.error
    # ~99.8% impact surfaced in the message.
    assert "99." in result.error


def test_healthy_pool_swap_passes_and_floors_off_guarded_baseline():
    """A quote within tolerance returns (amount_out_min, oracle, quoter)."""
    result = _guard(_make_impl(ORACLE_OUT_WEI), HEALTHY_QUOTE_HUMAN)
    assert isinstance(result, tuple)
    amount_out_min_wei, oracle_wei, quoter_wei = result
    assert oracle_wei == ORACLE_OUT_WEI
    assert quoter_wei == int(HEALTHY_QUOTE_HUMAN * Decimal(10**6))  # 328_000
    # Floor = safer(oracle, quoter) × (1 − 1% slippage) = 328_000 × 0.99.
    assert amount_out_min_wei == int(Decimal(328_000) * Decimal("0.99"))
    # And critically it is NOT the drained-pool-style near-zero floor.
    assert amount_out_min_wei > ORACLE_OUT_WEI // 2


def test_local_anvil_rpc_skips_guard():
    """On a local Anvil fork the guard is skipped (fork state ≠ live oracle)."""
    impl = _make_impl(ORACLE_OUT_WEI, rpc_url="http://127.0.0.1:8545")
    result = _guard(impl, DRAINED_QUOTE_HUMAN)
    # Skipped → returns a tuple even for the drained quote (no fail-closed).
    assert isinstance(result, tuple)
    assert result[1] == ORACLE_OUT_WEI


def test_local_anvil_skip_floors_off_quoter_not_stale_oracle():
    """When the guard is skipped, a stale/low oracle must NOT lower the floor.

    On a fork the oracle reflects live mainnet price while the quoter reflects
    fork pool state; the two are not time-aligned. If the oracle is LOWER than
    the fork quote, ``choose_safer_quote`` would have pulled ``amount_out_min``
    below the executable fork quote — the skip must use the quoter directly.
    """
    stale_low_oracle_wei = 100_000  # below the fork quote below
    fork_quote_human = Decimal("0.330")  # 330_000 units (6 dec)
    impl = _make_impl(stale_low_oracle_wei, rpc_url="http://127.0.0.1:8545")
    amount_out_min_wei, oracle_wei, quoter_wei = _guard(impl, fork_quote_human)
    assert oracle_wei == stale_low_oracle_wei
    assert quoter_wei == int(fork_quote_human * Decimal(10**6))  # 330_000
    # Floor is off the QUOTER (330_000 × 0.99), NOT the lower stale oracle.
    assert amount_out_min_wei == int(Decimal(330_000) * Decimal("0.99"))
    assert amount_out_min_wei > stale_low_oracle_wei


def test_missing_oracle_degrades_to_slippage_only():
    """A missing oracle price degrades to slippage-only, never a hard error."""
    impl = _make_impl(ValueError("price oracle has no price for WAVAX"))
    result = _guard(impl, DRAINED_QUOTE_HUMAN)
    assert isinstance(result, tuple)
    amount_out_min_wei, oracle_wei, quoter_wei = result
    assert oracle_wei == 0  # SKIPPED_NO_ORACLE
    assert quoter_wei == 499
    assert amount_out_min_wei == int(Decimal(499) * Decimal("0.99"))
