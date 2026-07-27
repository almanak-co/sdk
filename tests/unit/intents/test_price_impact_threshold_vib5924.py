"""VIB-5924 — pin which price-impact threshold ACTUALLY applies to a swap compile.

`check_price_impact` runs on every DEX swap compile (uniswap_v3/v4, traderjoe_v2,
fluid, aerodrome, camelot, generic). The production default lives only on
``IntentCompilerConfig.max_price_impact_pct`` (configurable at compiler
construction). Per-swap override: ``SwapIntent.max_price_impact``.

History:
* A dead 5% default on ``SwapCompilerContext`` was removed in #3427.
* Production default was 30% until this change; Codex/Claude second opinions
  recommended BLOCK ≈ 10% for early real-money users (not 5%, not 30%).

``EFFECTIVE_CAP`` below is the one line that must match the config default.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.base.compiler import SwapCompilerContext
from almanak.framework.intents._compiler_helpers import PriceImpactDecision, check_price_impact
from almanak.framework.intents.compiler_models import IntentCompilerConfig

# Production default — must match IntentCompilerConfig.max_price_impact_pct.
EFFECTIVE_CAP = Decimal("0.10")

# Realistic venue-vs-oracle gap observed on Linea Uni V3 WETH/USDC (VIB-5916).
ORACLE_ESTIMATE = 1_000_000_000_000_000_000
QUOTER_9_5_PCT_WORSE = 905_000_000_000_000_000


def _config(**overrides) -> IntentCompilerConfig:
    return IntentCompilerConfig(allow_placeholder_prices=True, **overrides)


# ── which value applies ──────────────────────────────────────────────────


def test_effective_production_cap_is_the_config_default():
    """The single source of truth for the swap price-impact cap."""
    assert _config().max_price_impact_pct == EFFECTIVE_CAP


def test_cap_is_configurable_not_hardcoded_into_the_guard():
    """Guard always takes config/intent; any Decimal in (0, 1] is valid."""
    for configured in (Decimal("0.05"), Decimal("0.10"), Decimal("0.30"), Decimal("0.50")):
        cfg = _config(max_price_impact_pct=configured)
        assert cfg.max_price_impact_pct == configured


def test_swap_context_has_no_default_threshold():
    """VIB-5924: the dead 5% default is gone — the field must be supplied."""
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


@pytest.mark.parametrize("configured", [Decimal("0.30"), Decimal("0.10"), Decimal("0.05"), Decimal("0.01")])
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
    for bad in (Decimal("0"), Decimal("-0.1"), Decimal("1.5")):
        with pytest.raises(ValueError, match="max_price_impact_pct"):
            _config(max_price_impact_pct=bad)


# ── what the effective cap lets through ──────────────────────────────────


def test_realistic_9_5_pct_gap_passes_at_10pct_default():
    """Linea Uni V3 ~9.5% venue-vs-oracle gap still compiles under BLOCK=10%."""
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


def test_15_pct_gap_is_blocked_at_10pct_default():
    """A 15% gap (inside the old inert 30% band) is rejected under 10%."""
    quoter_15 = int(ORACLE_ESTIMATE * 85 // 100)
    result = check_price_impact(
        oracle_estimate=ORACLE_ESTIMATE,
        quoter_amount=quoter_15,
        intent_max_impact=None,
        config_max_impact=EFFECTIVE_CAP,
        offline_mode=False,
        using_placeholders=False,
    )
    assert result.decision is PriceImpactDecision.IMPACT_TOO_HIGH
    assert result.effective_max_impact == EFFECTIVE_CAP


def test_same_fill_is_blocked_at_a_low_single_digit_cap():
    """5% still rejects the 9.5% Linea fill — use as thin-venue floor, not default."""
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
    """Thin venues can set SwapIntent.max_price_impact without changing global default."""
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
    """Override replaces the default in both directions (e.g. deliberately thin venue)."""
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
