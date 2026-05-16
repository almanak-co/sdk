"""VIB-4491: morpho_looping projected-HF guard + correct LLTV-based HF.

Tests follow the frozen UAT card at
docs/internal/uat-cards/VIB-4491.md (round 5, SHA 14fe2246).

We use ``MorphoLoopingStrategy.__new__`` to instantiate without running
__init__, matching the existing pattern in
test_looping_demo_teardown_sequences.py — this lets each test set exactly
the strategy fields it needs and isolates the projected-HF guard.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from almanak.demo_strategies.morpho_looping.strategy import MorphoLoopingStrategy
from almanak.framework.intents import IntentType


def _strategy(
    *,
    lltv: Decimal = Decimal("0.86"),
    target_ltv: Decimal = Decimal("0.50"),
    target_min_hf: Decimal = Decimal("1.10"),
    total_collateral: Decimal = Decimal("0.014"),
    total_borrowed: Decimal = Decimal("0"),
    chain: str = "ethereum",
    market_id: str = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
    collateral_token: str = "wstETH",
    borrow_token: str = "USDC",
) -> MorphoLoopingStrategy:
    """Build a bypass-__init__ MorphoLoopingStrategy with required fields set."""
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = chain  # `chain` is a read-only property reading from _chain
    strategy.market_id = market_id
    strategy.collateral_token = collateral_token
    strategy.borrow_token = borrow_token
    strategy.lltv = lltv
    strategy.target_ltv = target_ltv
    strategy.target_min_hf = target_min_hf
    strategy.min_health_factor = Decimal("1.5")
    strategy._total_collateral = total_collateral
    strategy._total_borrowed = total_borrowed
    strategy._pending_swap_amount = Decimal("0")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._current_health_factor = Decimal("0")
    strategy._loop_state = "supplied"
    strategy._current_loop = 0
    strategy._loops_completed = 0
    return strategy


# ─── D1.S1 — projected-HF guard refuses BORROW when projection < target_min_hf ──


def test_borrow_refused_when_projected_hf_below_threshold() -> None:
    """With LLTV=0.86 and target_min_hf=1.10, a tight-LTV BORROW that would push
    HF to ~1.06 (< 1.10) must be refused via HOLD intent."""
    strategy = _strategy(
        lltv=Decimal("0.86"),
        target_ltv=Decimal("0.81"),   # very aggressive — collateral*0.81/borrow → HF ≈ 1.062
        target_min_hf=Decimal("1.10"),
        total_collateral=Decimal("0.014"),
        total_borrowed=Decimal("0"),
    )
    intent = strategy._create_borrow_intent(
        collateral_price=Decimal("3400"),  # wstETH ≈ $3400
        borrow_price=Decimal("1"),         # USDC ≈ $1
    )
    assert intent.intent_type == IntentType.HOLD, (
        f"expected HOLD, got {intent.intent_type}; reason={getattr(intent, 'reason', None)}"
    )
    assert "projected_hf" in (getattr(intent, "reason", None) or ""), (
        f"HOLD reason must contain `projected_hf`: {intent.reason!r}"
    )


# ─── D1.S2 — active HF uses LLTV, not target_ltv ─────────────────────────


def test_active_hf_uses_lltv_not_target_ltv() -> None:
    """The HF formula inside ``_handle_complete_state`` uses ``self.lltv`` (post-fix);
    pre-fix it used ``self.target_ltv``. With LLTV=0.86 and target_ltv=0.50 and a
    deliberately distinguishable input, the two formulas yield different values
    (1.72 vs 1.00), so we can prove the right one is selected — by driving the
    method and inspecting the stamped ``_current_health_factor``.

    Setup: collateral=$100, borrow=$50.
    - Wrong (target_ltv=0.50): HF = (100 * 0.50) / 50 = 1.00.
    - Right (lltv=0.86):       HF = (100 * 0.86) / 50 = 1.72.
    """
    strategy = _strategy(
        lltv=Decimal("0.86"),
        target_ltv=Decimal("0.50"),
        total_collateral=Decimal("1"),       # collateral_price * 1 = $100
        total_borrowed=Decimal("50"),        # borrow_price * 50 = $50
    )
    strategy._loop_state = "complete"

    # Drive the actual strategy method — this is the runtime contract.
    strategy._handle_complete_state(
        collateral_price=Decimal("100"),
        borrow_price=Decimal("1"),
    )
    assert strategy._current_health_factor == Decimal("1.72"), (
        f"_handle_complete_state must compute HF via LLTV (0.86), expected 1.72, "
        f"got {strategy._current_health_factor}"
    )

    # Verify the wrong formula (target_ltv) would give the bug value — keeps the
    # documentation contract obvious to future readers.
    cv = strategy._total_collateral * Decimal("100")
    bv = strategy._total_borrowed * Decimal("1")
    wrong = (cv * strategy.target_ltv) / bv
    assert wrong == Decimal("1.00")
    assert strategy._current_health_factor != wrong, (
        "_current_health_factor must not equal the target_ltv-based formula"
    )


# ─── D1.S5 — strategy formula matches the gateway formula by construction ──


def test_strategy_hf_matches_gateway_formula() -> None:
    """Gateway's HF formula (from MORPHO_MARKETS / position()) is
    (collateral_value * lltv) / borrow_value. The strategy's post-fix
    implementation must produce the same value for any (collateral_value,
    borrow_value, lltv) tuple — verified by driving ``_handle_complete_state``
    and inspecting the stamped ``_current_health_factor``, NOT by re-deriving
    the formula in test code (would regress to "tautology test")."""
    cases = [
        (Decimal("100"),  Decimal("50"),   Decimal("0.86"),  Decimal("1.72")),
        (Decimal("48"),   Decimal("33.6"), Decimal("0.86"),  Decimal("48") * Decimal("0.86") / Decimal("33.6")),
        (Decimal("1000"), Decimal("500"),  Decimal("0.915"), Decimal("1.83")),
        (Decimal("250"),  Decimal("100"),  Decimal("0.945"), Decimal("250") * Decimal("0.945") / Decimal("100")),
    ]
    for cv, bv, lltv, expected in cases:
        # Encode (cv, bv) as (collateral_amount=1 @ price=cv, borrow_amount=1 @ price=bv)
        # — the strategy multiplies them inside _handle_complete_state, so the test
        # exercises that multiplication too.
        strategy = _strategy(
            lltv=lltv,
            total_collateral=Decimal("1"),
            total_borrowed=Decimal("1"),
        )
        strategy._loop_state = "complete"
        strategy._handle_complete_state(collateral_price=cv, borrow_price=bv)
        assert strategy._current_health_factor == expected, (
            f"strategy HF mismatch: cv={cv} bv={bv} lltv={lltv} "
            f"got {strategy._current_health_factor} expected {expected}"
        )


# ─── D2.M2 — Base chain unit-level coverage (same code path) ─────────────


def test_borrow_guard_works_for_base_chain_market() -> None:
    """Same code path on Base — only chain + market_id + LLTV differ. The
    projected-HF guard fires identically when projection < target_min_hf.
    """
    strategy = _strategy(
        chain="base",
        market_id="0xbase_wsteth_usdc_market_id_synthetic",
        lltv=Decimal("0.86"),
        target_ltv=Decimal("0.81"),
        target_min_hf=Decimal("1.10"),
        total_collateral=Decimal("0.014"),
        total_borrowed=Decimal("0"),
    )
    intent = strategy._create_borrow_intent(
        collateral_price=Decimal("3400"),
        borrow_price=Decimal("1"),
    )
    assert intent.intent_type == IntentType.HOLD


# ─── D2.M3 — target_min_hf threshold variance ────────────────────────────


@pytest.mark.parametrize("target_min_hf,expected_type", [
    (Decimal("1.05"), "BORROW"),   # 1.06 ≥ 1.05 ⇒ allow
    (Decimal("1.10"), "HOLD"),     # 1.06 < 1.10 ⇒ refuse
    (Decimal("1.20"), "HOLD"),     # 1.06 < 1.20 ⇒ refuse (more strict)
])
def test_target_min_hf_threshold_variance(target_min_hf: Decimal, expected_type: str) -> None:
    """Same borrow ratio, different target_min_hf — the guard activates at the threshold."""
    strategy = _strategy(
        lltv=Decimal("0.86"),
        target_ltv=Decimal("0.81"),
        target_min_hf=target_min_hf,
        total_collateral=Decimal("0.014"),
        total_borrowed=Decimal("0"),
    )
    intent = strategy._create_borrow_intent(
        collateral_price=Decimal("3400"),
        borrow_price=Decimal("1"),
    )
    name = intent.intent_type.name if hasattr(intent.intent_type, "name") else str(intent.intent_type)
    assert name == expected_type, (
        f"target_min_hf={target_min_hf}: expected {expected_type}, got {name}"
    )


# ─── D2.M4 — LLTV variance across markets ─────────────────────────────────


@pytest.mark.parametrize("lltv,expected_hf_approx", [
    (Decimal("0.86"), Decimal("1.72")),
    (Decimal("0.91"), Decimal("1.82")),
    (Decimal("0.77"), Decimal("1.54")),
])
def test_lltv_variance_across_markets(lltv: Decimal, expected_hf_approx: Decimal) -> None:
    """Strategy HF scales linearly with LLTV — verified by driving the actual
    strategy method, not by re-deriving the formula in test code. (cv=$100, bv=$50.)"""
    strategy = _strategy(
        lltv=lltv,
        total_collateral=Decimal("1"),   # × collateral_price=100 → cv=$100
        total_borrowed=Decimal("50"),    # × borrow_price=1 → bv=$50
    )
    strategy._loop_state = "complete"
    strategy._handle_complete_state(collateral_price=Decimal("100"), borrow_price=Decimal("1"))
    assert strategy._current_health_factor == expected_hf_approx, (
        f"LLTV={lltv}: expected HF {expected_hf_approx}, got {strategy._current_health_factor}"
    )


# ─── D3.F1 — missing oracle (None price) refuses BORROW ──────────────────


def test_missing_oracle_refuses_borrow(caplog: pytest.LogCaptureFixture) -> None:
    """Trust statement §5: missing oracle (None) MUST refuse via HOLD with a
    documented reason. Stale-but-non-null oracle is out of scope (see card)."""
    strategy = _strategy()
    with caplog.at_level(logging.ERROR):
        intent = strategy._create_borrow_intent(
            collateral_price=None,
            borrow_price=Decimal("1"),
        )
    assert intent.intent_type == IntentType.HOLD
    reason = getattr(intent, "reason", "") or ""
    # The guard refuses on None / zero / negative — the HOLD reason names the
    # broader "invalid_oracle" failure mode (matching strategy.py's contract).
    assert "invalid_oracle" in reason, (
        f"HOLD reason should name the failure: got {reason!r}"
    )
    # Silent-error observability: no ERROR-level records emitted by this path.
    assert not any(
        rec.levelno >= logging.ERROR
        for rec in caplog.records
        if rec.name.startswith("almanak.demo_strategies.morpho_looping")
    ), f"expected no ERROR records, got: {[r.message for r in caplog.records if r.levelno >= logging.ERROR]}"


# ─── D3.F2 — zero-collateral edge ────────────────────────────────────────


def test_zero_collateral_safe_handling() -> None:
    """If total_collateral=0 but total_borrowed=0, projected_hf = Infinity but
    the guard's available_borrow_value check refuses anyway (no capacity).
    If total_collateral=0 and total_borrowed>0, projected_hf would be 0 and
    refused. In neither case does a ZeroDivisionError propagate."""
    # Case A: zero collateral, zero debt
    s_a = _strategy(total_collateral=Decimal("0"), total_borrowed=Decimal("0"))
    intent_a = s_a._create_borrow_intent(Decimal("3400"), Decimal("1"))
    assert intent_a.intent_type == IntentType.HOLD
    # available_borrow_value is 0 — refused before HF projection.

    # Case B: zero collateral, non-zero debt
    s_b = _strategy(
        total_collateral=Decimal("0"),
        total_borrowed=Decimal("100"),
    )
    intent_b = s_b._create_borrow_intent(Decimal("3400"), Decimal("1"))
    assert intent_b.intent_type == IntentType.HOLD
    # available_borrow_value = 0 - 100 = -100 ⇒ refused at the "No additional
    # borrowing capacity" guard before HF projection. No exception raised.


# ─── D3.F6 — missing target_min_hf uses safe default ────────────────────


def _build_config_only_strategy(
    cfg: dict[str, object],
) -> "MorphoLoopingStrategy":
    """Construct a strategy with the real __init__ by mocking only the framework
    plumbing it touches before reaching the config-read branches under test.

    The __init__ super-call goes through IntentStrategy → BaseStrategy and emits a
    timeline event; everything else (config reads, value parsing, log emission)
    runs as in production. Using ``__new__`` would bypass exactly the branches we
    want to exercise — so this helper is the behavioral path."""
    from unittest.mock import patch

    with patch("almanak.framework.api.timeline.add_event"):
        return MorphoLoopingStrategy(
            config=cfg,
            chain="ethereum",
            wallet_address="0x" + "1" * 40,
        )


def test_missing_target_min_hf_uses_safe_default(caplog: pytest.LogCaptureFixture) -> None:
    """If `target_min_hf` is absent from config, the strategy must:
    1. Default to Decimal("1.10").
    2. Log a WARN explaining the fallback (audit trail).
    3. Keep the guard active (a tight-LTV BORROW must still be refused).
    """
    cfg = {
        "market_id": "0x" + "b" * 64,
        "collateral_token": "wstETH",
        "borrow_token": "USDC",
        "initial_collateral": "1.0",
        "target_loops": 1,
        "target_ltv": 0.81,
        "lltv": 0.86,
        # target_min_hf intentionally omitted
        "min_health_factor": 1.5,
        "swap_slippage": 0.005,
    }
    with caplog.at_level(logging.WARNING, logger="almanak.demo_strategies.morpho_looping.strategy"):
        strategy = _build_config_only_strategy(cfg)

    assert strategy.target_min_hf == Decimal("1.10"), (
        f"target_min_hf must default to 1.10, got {strategy.target_min_hf}"
    )
    assert any("using default target_min_hf=1.10" in rec.message for rec in caplog.records), (
        f"missing target_min_hf must emit a WARN with the fallback value; got: "
        f"{[r.message for r in caplog.records]}"
    )

    # Guard still fires when projected_hf < 1.10. cv=$47.6, bv=$38.55 ⇒ HF≈1.06 < 1.10.
    strategy._total_collateral = Decimal("0.014")
    strategy._total_borrowed = Decimal("0")
    strategy._loop_state = "supplied"
    intent = strategy._create_borrow_intent(Decimal("3400"), Decimal("1"))
    assert intent.intent_type == IntentType.HOLD, (
        "Default-active guard must refuse a tight-LTV BORROW at projected_hf < 1.10"
    )


# ─── D3.F7 — missing LLTV is a hard configuration error ────────────────


def test_missing_lltv_refuses_to_start() -> None:
    """If `lltv` is absent from config, the strategy MUST raise — not silently
    default to target_ltv or 1.0 (the pre-fix bug)."""
    cfg = {
        "market_id": "0x" + "b" * 64,
        "collateral_token": "wstETH",
        "borrow_token": "USDC",
        "initial_collateral": "1.0",
        "target_loops": 1,
        "target_ltv": 0.50,
        # lltv intentionally omitted
        "target_min_hf": 1.10,
        "min_health_factor": 1.5,
        "swap_slippage": 0.005,
    }
    with pytest.raises(ValueError, match=r"missing required `lltv`"):
        _build_config_only_strategy(cfg)
