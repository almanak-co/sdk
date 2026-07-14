"""VIB-5746: morpho_looping must never persist a fabricated health factor of 0.

On Robinhood the strategy's ``current_health_factor`` was persisted as the string
"0" after a failed recycle-SWAP iteration, even though the true on-chain HF was a
healthy 1.83 the whole time. An operator (or downstream consumer) reading the
persisted field would see a false liquidation-risk signal, and "0" is a string in
a numeric field. Per Empty ≠ Zero, an unmeasured HF is ``None`` (persisted as
JSON null), never a fabricated 0.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.demo_strategies.morpho_looping.strategy import MorphoLoopingStrategy


def _strategy(
    *,
    total_collateral: Decimal = Decimal("0.014"),
    total_borrowed: Decimal = Decimal("0"),
) -> MorphoLoopingStrategy:
    s = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    s._chain = "robinhood"
    s._wallet_address = "0x64E817FEd3Ec20EaD2dDc5cda3d666eF40655226"
    s.market_id = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
    s.collateral_token = "USDe"
    s.borrow_token = "USDG"
    s.initial_collateral = Decimal("0.014")
    s.lltv = Decimal("0.915")
    s.target_ltv = Decimal("0.50")
    s.target_min_hf = Decimal("1.10")
    s.min_health_factor = Decimal("1.5")
    s.swap_slippage = Decimal("0.005")
    s.target_loops = 1
    s._total_collateral = total_collateral
    s._total_borrowed = total_borrowed
    s._pending_swap_amount = Decimal("0")
    s._pending_wallet_collateral = Decimal("0")
    s._current_health_factor = None
    s._loop_state = "borrowed"
    s._previous_stable_state = "borrowed"
    s._current_loop = 0
    s._loops_completed = 0
    return s


def test_initial_hf_is_unmeasured_none() -> None:
    """Before the position is live, HF is unmeasured (None), never Decimal(0)."""
    s = _strategy()
    assert s._current_health_factor is None


def test_failed_swap_does_not_clobber_hf_to_zero() -> None:
    """A failed (unrelated) recycle SWAP reverts loop state but must leave HF
    unmeasured — it must NOT be clobbered to a fabricated 0."""
    s = _strategy()
    swap_intent = s._create_swap_intent(Decimal("1.25"), Decimal("1.0"))
    s.on_intent_executed(swap_intent, success=False, result=None)
    # State reverted, HF untouched and still unmeasured.
    assert s._loop_state == "borrowed"
    assert s._current_health_factor is None


def test_persistent_state_serialises_unmeasured_hf_as_null_not_string_zero() -> None:
    """The persisted field is JSON null when unmeasured — never the string "0"."""
    s = _strategy()
    persisted = s.get_persistent_state()
    assert persisted["current_health_factor"] is None
    assert persisted["current_health_factor"] != "0"


def test_status_reports_unmeasured_hf_as_null() -> None:
    s = _strategy()
    status = s.get_status()
    assert status["state"]["health_factor"] is None


def test_measured_hf_round_trips_as_numeric_string() -> None:
    """When HF IS measured it persists as its numeric value and round-trips."""
    s = _strategy(total_borrowed=Decimal("1.25"))
    s._current_health_factor = Decimal("1.83")
    persisted = s.get_persistent_state()
    assert persisted["current_health_factor"] == "1.83"

    restored = _strategy()
    restored.load_persistent_state(persisted)
    assert restored._current_health_factor == Decimal("1.83")


def test_load_persistent_state_tolerates_null_hf() -> None:
    """A persisted null HF loads back as unmeasured (None)."""
    s = _strategy()
    s.load_persistent_state({"current_health_factor": None})
    assert s._current_health_factor is None


def test_load_persistent_state_maps_legacy_zero_to_unmeasured() -> None:
    """Legacy rows stored "0" as the pre-fix "not yet measured" sentinel, so they
    must restore as None (unmeasured), NOT Decimal(0) — a stored 0 never meant a
    true HF of 0 (that position would already be liquidated). Otherwise the
    restored strategy keeps reporting HF 0.00 and trips false liquidation
    warnings until the next measurement (Empty!=Zero)."""
    s = _strategy()
    s.load_persistent_state({"current_health_factor": "0"})
    assert s._current_health_factor is None
    s.load_persistent_state({"current_health_factor": "0.0"})
    assert s._current_health_factor is None
    # A genuine measured HF still round-trips.
    s.load_persistent_state({"current_health_factor": "1.83"})
    assert s._current_health_factor == Decimal("1.83")


def test_complete_state_hold_reason_handles_unmeasured_hf() -> None:
    """The COMPLETE-state HOLD reason must not crash formatting an unmeasured HF
    (no debt ⇒ no liquidation surface ⇒ HF stays None)."""
    s = _strategy(total_borrowed=Decimal("0"))
    s._loop_state = "complete"
    intent = s._handle_complete_state(Decimal("1.0"), Decimal("1.0"))
    assert "n/a" in intent.reason
    assert s._current_health_factor is None
