"""VIB-5924 — pin which price-impact threshold ACTUALLY applies to a swap compile.

`check_price_impact` runs on every DEX swap compile (uniswap_v3/v4, traderjoe_v2,
fluid, aerodrome, camelot, generic). Two candidate thresholds existed in the
source and only one was ever reachable:

* `IntentCompilerConfig.max_price_impact_pct` = 30% — the effective production cap.
* `SwapCompilerContext.max_price_impact_pct` = 5% — dead code. It read like a
  production default but `IntentCompiler._swap_compiler_context_kwargs()`
  unconditionally overwrites it, so it applied to exactly zero real compiles.

The 5% default is now removed (the field is required), so the trap cannot come
back. These tests pin the remaining contract so the effective number is stated
in one place a reader can find.

**These tests deliberately do NOT bless 30% as correct.** A 30% cap on a
money-path sanity guard is close to inert — `test_realistic_bad_fill_passes_the_effective_cap`
documents a real 9.5% venue-vs-oracle gap (Linea Uni V3 WETH/USDC, observed in
the VIB-5916 mainnet run) sailing through. Choosing the production default is a
separate design decision tracked on VIB-5924; when it lands, `EFFECTIVE_CAP`
below is the one line to change.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.base.compiler import SwapCompilerContext
from almanak.framework.intents._compiler_helpers import PriceImpactDecision, check_price_impact
from almanak.framework.intents.compiler_models import IntentCompilerConfig

# The cap that actually applies to a production swap compile today.
EFFECTIVE_CAP = Decimal("0.30")

# A realistic bad fill: quoter returns 9.5% less than the oracle estimate.
ORACLE_ESTIMATE = 1_000_000_000_000_000_000
QUOTER_9_5_PCT_WORSE = 905_000_000_000_000_000


def _config(**overrides) -> IntentCompilerConfig:
    return IntentCompilerConfig(allow_placeholder_prices=True, **overrides)


# ── which value applies ──────────────────────────────────────────────────


def test_effective_production_cap_is_the_config_default():
    """The single source of truth for the swap price-impact cap."""
    assert _config().max_price_impact_pct == EFFECTIVE_CAP


def test_swap_context_has_no_default_threshold():
    """VIB-5924: the dead 5% default is gone — the field must be supplied.

    A default here is a trap: it is unreachable in production (see
    `test_context_always_receives_the_config_value`) while advertising a cap
    that does not apply. Requiring it keeps `IntentCompilerConfig` the only
    place the number lives.
    """
    field = SwapCompilerContext.__dataclass_fields__["max_price_impact_pct"]
    assert field.default is dataclasses.MISSING
    assert field.default_factory is dataclasses.MISSING

    with pytest.raises(TypeError, match="max_price_impact_pct"):
        SwapCompilerContext(
            chain="arbitrum",
            wallet_address="0x" + "11" * 20,
            rpc_url=None,
            rpc_timeout=10.0,
            permission_discovery=False,
            allow_placeholder_prices=True,
            token_resolver=None,
            gateway_client=None,
            price_oracle={},
            cache={},
            services=object(),
        )


@pytest.mark.parametrize("configured", [Decimal("0.30"), Decimal("0.05"), Decimal("0.01")])
def test_context_always_receives_the_config_value(configured):
    """The context knob is a pass-through of the config, on every compile."""
    from almanak.framework.intents.compiler import IntentCompiler

    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "11" * 20,
        config=_config(max_price_impact_pct=configured),
    )
    kwargs = compiler._swap_compiler_context_kwargs()
    assert kwargs["max_price_impact_pct"] == configured


def test_config_rejects_out_of_range_thresholds():
    """Guard rails for whatever the design pass picks."""
    for bad in (Decimal("0"), Decimal("-0.1"), Decimal("1.5")):
        with pytest.raises(ValueError, match="max_price_impact_pct"):
            _config(max_price_impact_pct=bad)


# ── what the effective cap lets through ──────────────────────────────────


def test_realistic_bad_fill_passes_the_effective_cap():
    """A 9.5% venue-vs-oracle gap compiles clean under the 30% cap.

    Observed on Linea Uni V3 WETH/USDC in the VIB-5916 mainnet run (VIB-5922).
    This is the evidence for the threshold design pass — NOT an assertion that
    the current behaviour is desirable.
    """
    result = check_price_impact(
        oracle_estimate=ORACLE_ESTIMATE,
        quoter_amount=QUOTER_9_5_PCT_WORSE,
        intent_max_impact=None,
        config_max_impact=_config().max_price_impact_pct,
        offline_mode=False,
        using_placeholders=False,
    )
    assert result.decision is PriceImpactDecision.OK
    assert result.price_impact == Decimal("0.095")
    assert result.effective_max_impact == EFFECTIVE_CAP


def test_same_fill_is_blocked_at_a_low_single_digit_cap():
    """The same fill under a 5% cap is correctly rejected — the guard works;
    only the number is wrong."""
    result = check_price_impact(
        oracle_estimate=ORACLE_ESTIMATE,
        quoter_amount=QUOTER_9_5_PCT_WORSE,
        intent_max_impact=None,
        config_max_impact=Decimal("0.05"),
        offline_mode=False,
        using_placeholders=False,
    )
    assert result.decision is PriceImpactDecision.IMPACT_TOO_HIGH


def test_per_intent_override_beats_the_config_default():
    """The per-intent lever the design pass needs already exists and is wired
    (`SwapIntent.max_price_impact` → `compiler.py` `intent_max_impact=`), so
    tightening the default does not require new plumbing for deliberately thin
    venues (e.g. Pendle YT)."""
    result = check_price_impact(
        oracle_estimate=ORACLE_ESTIMATE,
        quoter_amount=QUOTER_9_5_PCT_WORSE,
        intent_max_impact=Decimal("0.05"),
        config_max_impact=EFFECTIVE_CAP,
        offline_mode=False,
        using_placeholders=False,
    )
    assert result.decision is PriceImpactDecision.IMPACT_TOO_HIGH
    assert result.effective_max_impact == Decimal("0.05")


def test_intent_override_can_also_loosen():
    """Symmetry check — the override replaces the default in both directions."""
    result = check_price_impact(
        oracle_estimate=ORACLE_ESTIMATE,
        quoter_amount=QUOTER_9_5_PCT_WORSE,
        intent_max_impact=Decimal("0.50"),
        config_max_impact=Decimal("0.05"),
        offline_mode=False,
        using_placeholders=False,
    )
    assert result.decision is PriceImpactDecision.OK
    assert result.effective_max_impact == Decimal("0.50")
