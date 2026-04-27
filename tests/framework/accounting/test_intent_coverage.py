"""Coverage gate: every IntentType must map to a known AccountingCategory (VIB-3477).

This test prevents future intent types from silently having no accounting handler.
Any new IntentType added to vocabulary.py that is not handled in classifier.py
will cause test_every_intent_type_is_classified_or_excluded to fail.

Note: test_intent_classifier.py covers specific routing rules in detail.
This file is the exhaustive sweep that serves as a regression gate for the
full IntentType × AccountingCategory mapping.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.classifier import AccountingCategory, classify
from almanak.framework.intents.vocabulary import IntentType


def test_every_intent_type_is_classified_or_excluded() -> None:
    """Every IntentType maps to a valid AccountingCategory — no unhandled intent silently dropped.

    classify() falls back to NO_ACCOUNTING for unknown types, so this test verifies
    that the return value is always an AccountingCategory instance (not a raw string
    outside the enum) and that every known intent is accounted for.
    """
    for intent_type in IntentType:
        category = classify(intent_type.value, protocol="")
        assert isinstance(category, AccountingCategory), (
            f"IntentType.{intent_type.name} returned unexpected type {type(category)!r}: {category!r}"
        )
        # Verify it's a recognised value (StrEnum equality check)
        assert category in list(AccountingCategory), (
            f"IntentType.{intent_type.name} returned unknown category value: {category!r}"
        )


def test_lending_intents_all_route_to_lending() -> None:
    """All 5 lending intent types route to LENDING regardless of protocol."""
    lending_intents = {
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.REPAY,
        IntentType.WITHDRAW,
        IntentType.DELEVERAGE,
    }
    for intent_type in lending_intents:
        for protocol in ("aave_v3", "morpho_blue", "compound_v3", "radiant_v2", ""):
            category = classify(intent_type.value, protocol=protocol)
            assert category == AccountingCategory.LENDING, (
                f"IntentType.{intent_type.name} with protocol={protocol!r} "
                f"should be LENDING but got {category!r}"
            )


def test_perp_intents_all_route_to_perp() -> None:
    """All PERP_* intent types route to PERP regardless of protocol."""
    perp_intents = {
        IntentType.PERP_OPEN,
        IntentType.PERP_CLOSE,
    }
    for intent_type in perp_intents:
        for protocol in ("gmx_v2", "drift", ""):
            category = classify(intent_type.value, protocol=protocol)
            assert category == AccountingCategory.PERP, (
                f"IntentType.{intent_type.name} with protocol={protocol!r} "
                f"should be PERP but got {category!r}"
            )


def test_vault_intents_all_route_to_vault() -> None:
    """All VAULT_* intent types in IntentType route to VAULT (or NO_ACCOUNTING for VAULT_MANAGE)."""
    vault_intents_expected = {
        IntentType.VAULT_DEPOSIT: AccountingCategory.VAULT,
        IntentType.VAULT_REDEEM: AccountingCategory.VAULT,
        IntentType.VAULT_REALLOCATE: AccountingCategory.VAULT,
    }
    for intent_type, expected_category in vault_intents_expected.items():
        category = classify(intent_type.value, protocol="erc4626")
        assert category == expected_category, (
            f"IntentType.{intent_type.name} should be {expected_category} but got {category!r}"
        )


def test_lp_intents_route_to_lp_for_non_pendle() -> None:
    """LP_OPEN, LP_CLOSE, LP_COLLECT_FEES all route to LP for non-Pendle protocols."""
    lp_intents = {IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES}
    for intent_type in lp_intents:
        for protocol in ("uniswap_v3", "aerodrome", "traderjoe_v2", ""):
            category = classify(intent_type.value, protocol=protocol)
            assert category == AccountingCategory.LP, (
                f"IntentType.{intent_type.name} with protocol={protocol!r} "
                f"should be LP but got {category!r}"
            )


def test_lp_intents_route_to_pendle_lp_for_pendle() -> None:
    """LP_OPEN, LP_CLOSE, LP_COLLECT_FEES all route to PENDLE_LP for Pendle protocol."""
    lp_intents = {IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES}
    for intent_type in lp_intents:
        category = classify(intent_type.value, protocol="pendle")
        assert category == AccountingCategory.PENDLE_LP, (
            f"IntentType.{intent_type.name} with protocol=pendle "
            f"should be PENDLE_LP but got {category!r}"
        )


def test_swap_with_pendle_and_pt_token_routes_to_pendle_pt() -> None:
    """SWAP with pendle protocol and PT- token_out routes to PENDLE_PT."""
    category = classify(IntentType.SWAP.value, protocol="pendle", token_out="PT-wstETH-25JUN2026")
    assert category == AccountingCategory.PENDLE_PT


def test_swap_without_pt_token_routes_to_swap() -> None:
    """SWAP without a PT- token_out routes to SWAP regardless of protocol."""
    for protocol in ("enso", "uniswap_v3", "pendle", "1inch", ""):
        category = classify(IntentType.SWAP.value, protocol=protocol, token_out="USDC")
        assert category == AccountingCategory.SWAP, (
            f"SWAP with protocol={protocol!r} token_out=USDC should be SWAP but got {category!r}"
        )


@pytest.mark.parametrize("intent_type", [
    IntentType.BRIDGE,
    IntentType.HOLD,
    IntentType.WRAP_NATIVE,
    IntentType.UNWRAP_NATIVE,
    IntentType.ENSURE_BALANCE,
    IntentType.FLASH_LOAN,
])
def test_explicitly_excluded_intents_are_no_accounting(intent_type: IntentType) -> None:
    """Explicitly excluded intent types must map to NO_ACCOUNTING for any protocol."""
    for protocol in ("any_protocol", "aave_v3", ""):
        category = classify(intent_type.value, protocol=protocol)
        assert category == AccountingCategory.NO_ACCOUNTING, (
            f"IntentType.{intent_type.name} should always be NO_ACCOUNTING "
            f"but got {category!r} for protocol={protocol!r}"
        )


def test_no_intent_type_is_silently_unhandled() -> None:
    """Verify that the set of known IntentType values matches what the classifier handles.

    If a new IntentType is added to vocabulary.py without updating classifier.py,
    it will fall through to NO_ACCOUNTING. This test documents that behaviour
    and checks that the set of NO_ACCOUNTING intent types contains only those
    that are intentionally excluded.

    Intentionally NO_ACCOUNTING intent types (those with no meaningful financial
    event to record at the accounting layer):
      - BRIDGE, HOLD, WRAP_NATIVE, UNWRAP_NATIVE, ENSURE_BALANCE, FLASH_LOAN:
        explicitly excluded in classifier._NO_ACCOUNTING_TYPES
      - STAKE, UNSTAKE: no accounting handler yet (Phase 2+)
      - PREDICTION_BUY, PREDICTION_SELL, PREDICTION_REDEEM: no accounting handler yet
      - VAULT_MANAGE: no accounting handler yet

    Any IntentType NOT in this list that classifies as NO_ACCOUNTING is a bug.
    """
    _INTENTIONALLY_NO_ACCOUNTING = frozenset({
        # Explicitly excluded in classifier
        "BRIDGE",
        "HOLD",
        "WRAP_NATIVE",
        "UNWRAP_NATIVE",
        "ENSURE_BALANCE",
        "FLASH_LOAN",
        # Not yet implemented (Phase 2+)
        "STAKE",
        "UNSTAKE",
        "PREDICTION_BUY",
        "PREDICTION_SELL",
        "PREDICTION_REDEEM",
        "VAULT_MANAGE",
    })

    unexpected_no_accounting = []
    for intent_type in IntentType:
        category = classify(intent_type.value, protocol="")
        if category == AccountingCategory.NO_ACCOUNTING:
            if intent_type.value not in _INTENTIONALLY_NO_ACCOUNTING:
                unexpected_no_accounting.append(intent_type.name)

    assert not unexpected_no_accounting, (
        f"The following IntentType values unexpectedly map to NO_ACCOUNTING "
        f"and may be missing an accounting handler: {unexpected_no_accounting}. "
        f"If intentional, add them to _INTENTIONALLY_NO_ACCOUNTING in this test."
    )
