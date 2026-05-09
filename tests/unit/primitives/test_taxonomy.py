"""Tests for ``primitives/taxonomy.py`` — the canonical TAXONOMY table + lookup API."""

from __future__ import annotations

import pytest

from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.taxonomy import (
    ALIASES,
    TAXONOMY,
    UnknownIntentTypeError,
    classify,
    is_async,
    position_type_for,
    record_for,
)
from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    PositionKind,
    Primitive,
    PrimitiveRecord,
)


def test_taxonomy_covers_every_intent_type() -> None:
    """Every value in ``IntentType`` must have a row in :data:`TAXONOMY`.

    The 5 placeholder rows from T5 (LIQUIDATE, OPEN_CDP, MINT_STABLE,
    REPAY_STABLE, CLOSE_CDP) are added in VIB-4165; T1 covers everything
    declared today.
    """
    declared = {it.value for it in IntentType}
    covered = set(TAXONOMY.keys())
    missing = declared - covered
    assert missing == set(), (
        f"TAXONOMY missing rows for declared IntentType values: {sorted(missing)}"
    )


def test_taxonomy_has_no_extra_rows() -> None:
    """Every TAXONOMY key must be either a declared IntentType OR a
    declared payload-only ``AccountingEventType`` (no ghost keys).

    T2 (VIB-4162) extended TAXONOMY with rows for event-type values
    emitted by the typed accounting models (PendleAccountingEvent,
    PerpAccountingEvent, VaultAccountingEvent, PredictionAccountingEvent,
    LendingAccountingEvent, LPAccountingEvent) that are NOT declared in
    IntentType — e.g. ``PT_BUY``, ``PENDLE_LP_OPEN``, ``PERP_INCREASE``,
    ``VAULT_HARVEST``, ``PREDICTION_OPEN``, ``LIQUIDATION_RISK_UPDATE``.
    These rows are required so the writer's augment chokepoint can stamp
    a per-primitive ``matching_policy_version`` for legitimate handler
    output without raising in live mode. Without them, every Pendle /
    Prediction / extended-Perp/Vault/Lending live write would halt the
    runner.

    Aliases (e.g. ``VAULT_WITHDRAW``) live in :data:`ALIASES`, not in the
    table itself, so this assertion still holds for the deliberate
    ghost-name scrub.
    """
    from almanak.framework.accounting.models import ALL_ACCOUNTING_EVENT_TYPES

    declared_intents = {it.value for it in IntentType}
    declared_event_types = set(ALL_ACCOUNTING_EVENT_TYPES)
    allowed = declared_intents | declared_event_types
    extras = set(TAXONOMY.keys()) - allowed
    assert extras == set(), (
        f"TAXONOMY has rows not declared in IntentType nor "
        f"ALL_ACCOUNTING_EVENT_TYPES: {sorted(extras)}"
    )


def test_aliases_map_vault_withdraw_to_vault_redeem() -> None:
    """The ``VAULT_WITHDRAW`` ghost name (classifier.py:24) is scrubbed via ALIASES."""
    assert ALIASES["VAULT_WITHDRAW"] == "VAULT_REDEEM"
    # The alias resolves so classify() and record_for() return the redeem row.
    assert classify("VAULT_WITHDRAW") == classify("VAULT_REDEEM")
    assert record_for("VAULT_WITHDRAW") is record_for("VAULT_REDEEM")


def test_taxonomy_does_not_contain_alias_keys() -> None:
    """Aliases must live in ALIASES, not as a duplicated TAXONOMY row."""
    for alias in ALIASES:
        assert alias not in TAXONOMY, (
            f"Alias {alias!r} must not be a TAXONOMY key — it would silently shadow the canonical row"
        )


def test_classify_lp_open_returns_lp() -> None:
    assert classify("LP_OPEN") == AccountingCategory.LP


def test_classify_lp_open_pendle_returns_pendle_lp() -> None:
    assert classify("LP_OPEN", protocol="pendle_v2") == AccountingCategory.PENDLE_LP


def test_classify_swap_pendle_pt_token_returns_pendle_pt() -> None:
    assert (
        classify("SWAP", protocol="pendle_v2", token_out="PT-stETH-26DEC2024")
        == AccountingCategory.PENDLE_PT
    )


def test_classify_swap_non_pendle_returns_swap() -> None:
    assert classify("SWAP", protocol="uniswap_v3") == AccountingCategory.SWAP


def test_classify_swap_pendle_non_pt_token_returns_swap() -> None:
    """Pendle-protocol SWAP without a PT- prefix is just a swap (regression guard)."""
    assert classify("SWAP", protocol="pendle_v2", token_out="USDC") == AccountingCategory.SWAP


def test_classify_lending_intents() -> None:
    for intent in ("SUPPLY", "WITHDRAW", "BORROW", "REPAY", "DELEVERAGE"):
        assert classify(intent) == AccountingCategory.LENDING


def test_classify_perp_intents() -> None:
    assert classify("PERP_OPEN") == AccountingCategory.PERP
    assert classify("PERP_CLOSE") == AccountingCategory.PERP


def test_classify_vault_intents() -> None:
    for intent in ("VAULT_DEPOSIT", "VAULT_REDEEM", "VAULT_REALLOCATE", "VAULT_MANAGE"):
        assert classify(intent) == AccountingCategory.VAULT


def test_classify_prediction_intents() -> None:
    for intent in ("PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"):
        assert classify(intent) == AccountingCategory.PREDICTION


def test_classify_no_accounting_intents() -> None:
    """T1 keeps BRIDGE on NO_ACCOUNTING for back-compat. T4 flips it to TRANSFER."""
    for intent in ("BRIDGE", "HOLD", "WRAP_NATIVE", "UNWRAP_NATIVE", "ENSURE_BALANCE", "FLASH_LOAN"):
        assert classify(intent) == AccountingCategory.NO_ACCOUNTING


def test_classify_staking_intents_are_no_accounting() -> None:
    """Staking has its own primitive but no dedicated accounting handler yet."""
    assert classify("STAKE") == AccountingCategory.NO_ACCOUNTING
    assert classify("UNSTAKE") == AccountingCategory.NO_ACCOUNTING


def test_classify_unknown_intent_returns_no_accounting() -> None:
    """Unknown intents must fall through to NO_ACCOUNTING (matches the legacy classifier)."""
    assert classify("FROBNICATE") == AccountingCategory.NO_ACCOUNTING


def test_classify_is_case_insensitive_on_input() -> None:
    """The legacy classifier upper-cases input; the taxonomy version mirrors that."""
    assert classify("lp_open") == AccountingCategory.LP
    assert classify("Lp_Open") == AccountingCategory.LP


def test_position_type_for_lp_intents() -> None:
    assert position_type_for("LP_OPEN") == PositionKind.LP
    assert position_type_for("LP_CLOSE") == PositionKind.LP


def test_position_type_for_lending_intents() -> None:
    """SUPPLY/WITHDRAW are collateral; BORROW/REPAY/DELEVERAGE are debt (VIB-4085)."""
    assert position_type_for("SUPPLY") == PositionKind.LENDING_COLLATERAL
    assert position_type_for("WITHDRAW") == PositionKind.LENDING_COLLATERAL
    assert position_type_for("BORROW") == PositionKind.LENDING_DEBT
    assert position_type_for("REPAY") == PositionKind.LENDING_DEBT
    assert position_type_for("DELEVERAGE") == PositionKind.LENDING_DEBT


def test_position_type_for_perp_intents() -> None:
    assert position_type_for("PERP_OPEN") == PositionKind.PERP
    assert position_type_for("PERP_CLOSE") == PositionKind.PERP


def test_position_type_for_swap_returns_none() -> None:
    """Swap does not create a tracked position."""
    assert position_type_for("SWAP") is None


def test_position_type_for_bridge_returns_none() -> None:
    """Bridge is a transfer, not a position. T4 keeps position=None even after the TRANSFER flip."""
    assert position_type_for("BRIDGE") is None


def test_position_type_for_unknown_returns_none() -> None:
    assert position_type_for("FROBNICATE") is None


def test_is_async_bridge_is_true() -> None:
    """Bridge has a settlement gap by construction (PRD §4)."""
    assert is_async("BRIDGE") is True


def test_is_async_atomic_intents_are_false() -> None:
    for intent in ("SWAP", "LP_OPEN", "SUPPLY", "PERP_OPEN", "VAULT_DEPOSIT"):
        assert is_async(intent) is False


def test_is_async_unknown_returns_false() -> None:
    """Safe default for unknown intents — T2 fail-fasts instead."""
    assert is_async("FROBNICATE") is False


def test_record_for_returns_full_record() -> None:
    record = record_for("LP_OPEN")
    assert isinstance(record, PrimitiveRecord)
    assert record.intent_type == "LP_OPEN"
    assert record.primitive is Primitive.LP
    assert record.accounting_category is AccountingCategory.LP
    assert record.position_type is PositionKind.LP
    assert record.event_kind is EventKind.OPEN
    assert record.is_async is False
    assert record.lifecycle_phase is LifecyclePhase.ATOMIC
    assert "LP_OPEN" in record.required_lifecycle
    assert "LP_CLOSE" in record.required_lifecycle


def test_record_for_unknown_raises() -> None:
    """Unknown intents fail-fast in :func:`record_for` (vs. silent in classify())."""
    with pytest.raises(UnknownIntentTypeError) as exc_info:
        record_for("FROBNICATE")
    assert "FROBNICATE" in str(exc_info.value)
    assert exc_info.value.intent_type == "FROBNICATE"


def test_record_for_is_alias_resolved() -> None:
    """``record_for("VAULT_WITHDRAW")`` returns the VAULT_REDEEM canonical row."""
    record = record_for("VAULT_WITHDRAW")
    assert record.intent_type == "VAULT_REDEEM"


def test_record_for_is_case_insensitive() -> None:
    assert record_for("lp_open").intent_type == "LP_OPEN"


@pytest.mark.parametrize("intent_type", [it.value for it in IntentType])
def test_every_intent_type_has_consistent_record(intent_type: str) -> None:
    """For every declared IntentType, the record obeys the documented invariants.

    Invariants (from PRD §4 and PrimitiveRecord docstring):
    - is_async=True implies lifecycle_phase != ATOMIC
    - every entry in required_lifecycle resolves to a row in TAXONOMY
    - required_lifecycle entries share the record's primitive (no cross-primitive bleeding)
    """
    record = record_for(intent_type)
    assert record.intent_type == intent_type

    for step in record.required_lifecycle:
        assert step in TAXONOMY, (
            f"{intent_type}'s required_lifecycle references unknown intent {step!r}"
        )
        sibling = TAXONOMY[step]
        assert sibling.primitive is record.primitive, (
            f"{intent_type}.required_lifecycle includes {step!r} which belongs to a "
            f"different primitive ({sibling.primitive} vs {record.primitive})"
        )

    if record.is_async:
        assert record.lifecycle_phase is not LifecyclePhase.ATOMIC, (
            f"{intent_type} is is_async=True but lifecycle_phase=ATOMIC; "
            "async intents must declare a non-atomic phase (request/claim/settle)."
        )


def test_taxonomy_lp_lifecycle_includes_open_and_close() -> None:
    """Sanity check on the canonical LP lifecycle declaration."""
    record = record_for("LP_OPEN")
    assert "LP_OPEN" in record.required_lifecycle
    assert "LP_CLOSE" in record.required_lifecycle


def test_taxonomy_perp_lifecycle_open_close() -> None:
    record = record_for("PERP_OPEN")
    assert record.required_lifecycle == ("PERP_OPEN", "PERP_CLOSE")


def test_taxonomy_lending_lifecycle_full_loop() -> None:
    """Lending fixtures must exercise SUPPLY → BORROW → REPAY → WITHDRAW (the looping primitive)."""
    record = record_for("SUPPLY")
    assert record.required_lifecycle == ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")


def test_classify_back_compat_delegates_to_legacy_classifier() -> None:
    """The classifier in accounting/classifier.py must yield the same result as taxonomy.classify().

    T1 keeps both routing implementations live; T2 deletes the legacy copy.
    Equivalence ensures consumers can be migrated incrementally without a
    correctness regression.
    """
    from almanak.framework.accounting import classifier as legacy

    cases = [
        ("SWAP", "uniswap_v3", "USDC"),
        ("SWAP", "pendle_v2", "PT-stETH-26DEC2024"),
        ("SWAP", "pendle_v2", "USDC"),
        ("LP_OPEN", "uniswap_v3", ""),
        ("LP_OPEN", "pendle_v2", ""),
        ("LP_CLOSE", "aerodrome", ""),
        ("LP_COLLECT_FEES", "uniswap_v3", ""),
        ("SUPPLY", "aave_v3", ""),
        ("WITHDRAW", "aave_v3", ""),
        ("BORROW", "aave_v3", ""),
        ("REPAY", "aave_v3", ""),
        ("DELEVERAGE", "aave_v3", ""),
        ("PERP_OPEN", "gmx_v2", ""),
        ("PERP_CLOSE", "gmx_v2", ""),
        ("VAULT_DEPOSIT", "metamorpho", ""),
        ("VAULT_REDEEM", "metamorpho", ""),
        ("VAULT_WITHDRAW", "metamorpho", ""),  # alias
        ("PREDICTION_BUY", "polymarket", ""),
        ("PREDICTION_SELL", "polymarket", ""),
        ("PREDICTION_REDEEM", "polymarket", ""),
        ("BRIDGE", "across", ""),
        ("HOLD", "", ""),
        ("WRAP_NATIVE", "", ""),
        ("UNWRAP_NATIVE", "", ""),
        ("ENSURE_BALANCE", "", ""),
        ("FLASH_LOAN", "aave_v3", ""),
        ("FROBNICATE", "", ""),  # unknown
    ]
    for intent_type, protocol, token_out in cases:
        assert classify(intent_type, protocol, token_out) == legacy.classify(
            intent_type, protocol, token_out
        ), f"taxonomy.classify and legacy.classify diverge on ({intent_type}, {protocol}, {token_out})"
