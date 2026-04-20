"""Tests for _MinimalIntent intent_type -> IntentType mapping in inner_runner.

Regression guard for VIB-3143: `wrap_native` (and its counterpart
`unwrap_native`) was missing from `_TYPE_MAP`, causing the ResultEnricher
to fall back to `IntentType.SWAP` and log a "Unknown intent type
'wrap_native'; defaulting to SWAP" warning.

Extended for VIB-3183: full enum-coverage audit added entries for
`lp_collect_fees`, `vault_deposit`, `vault_redeem`, `vault_reallocate`,
`vault_manage`, `prediction_buy`, `prediction_sell`, `prediction_redeem`,
`ensure_balance`, and `flash_loan`. The full-coverage test below pins
every IntentType enum value to a `_TYPE_MAP` entry so future enum
additions force a corresponding map update.
"""

import pytest

from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.runner.inner_runner import _MinimalIntent


class TestMinimalIntentTypeMap:
    """Pins the string -> IntentType mapping in _MinimalIntent."""

    def test_wrap_native_maps_to_wrap_native_enum(self):
        """`wrap_native` must map to IntentType.WRAP_NATIVE, not fall back to SWAP."""
        intent = _MinimalIntent("wrap_native", {})
        assert intent.intent_type is IntentType.WRAP_NATIVE

    def test_wrap_native_case_insensitive(self):
        """Upper-case `WRAP_NATIVE` must also resolve correctly (enum value form)."""
        intent = _MinimalIntent("WRAP_NATIVE", {})
        assert intent.intent_type is IntentType.WRAP_NATIVE

    def test_unwrap_native_maps_to_unwrap_native_enum(self):
        """`unwrap_native` must map to IntentType.UNWRAP_NATIVE, not fall back to SWAP."""
        intent = _MinimalIntent("unwrap_native", {})
        assert intent.intent_type is IntentType.UNWRAP_NATIVE

    def test_unwrap_native_case_insensitive(self):
        """Upper-case `UNWRAP_NATIVE` must also resolve correctly (enum value form)."""
        intent = _MinimalIntent("UNWRAP_NATIVE", {})
        assert intent.intent_type is IntentType.UNWRAP_NATIVE

    def test_swap_still_maps_correctly(self):
        """Sanity check that existing mappings are untouched."""
        intent = _MinimalIntent("swap", {})
        assert intent.intent_type is IntentType.SWAP

    # ------------------------------------------------------------------
    # VIB-3183: Gemini-flagged + full enum-coverage audit additions
    # ------------------------------------------------------------------

    def test_lp_collect_fees_maps_to_lp_collect_fees_enum(self):
        """`lp_collect_fees` must map to IntentType.LP_COLLECT_FEES (Gemini-flagged)."""
        intent = _MinimalIntent("lp_collect_fees", {})
        assert intent.intent_type is IntentType.LP_COLLECT_FEES

    def test_lp_collect_fees_case_insensitive(self):
        intent = _MinimalIntent("LP_COLLECT_FEES", {})
        assert intent.intent_type is IntentType.LP_COLLECT_FEES

    def test_vault_deposit_maps_to_vault_deposit_enum(self):
        """`vault_deposit` must map to IntentType.VAULT_DEPOSIT (Gemini-flagged)."""
        intent = _MinimalIntent("vault_deposit", {})
        assert intent.intent_type is IntentType.VAULT_DEPOSIT

    def test_vault_deposit_case_insensitive(self):
        intent = _MinimalIntent("VAULT_DEPOSIT", {})
        assert intent.intent_type is IntentType.VAULT_DEPOSIT

    def test_vault_redeem_maps_to_vault_redeem_enum(self):
        intent = _MinimalIntent("vault_redeem", {})
        assert intent.intent_type is IntentType.VAULT_REDEEM

    def test_vault_redeem_case_insensitive(self):
        intent = _MinimalIntent("VAULT_REDEEM", {})
        assert intent.intent_type is IntentType.VAULT_REDEEM

    def test_vault_reallocate_maps_to_vault_reallocate_enum(self):
        intent = _MinimalIntent("vault_reallocate", {})
        assert intent.intent_type is IntentType.VAULT_REALLOCATE

    def test_vault_reallocate_case_insensitive(self):
        intent = _MinimalIntent("VAULT_REALLOCATE", {})
        assert intent.intent_type is IntentType.VAULT_REALLOCATE

    def test_vault_manage_maps_to_vault_manage_enum(self):
        intent = _MinimalIntent("vault_manage", {})
        assert intent.intent_type is IntentType.VAULT_MANAGE

    def test_vault_manage_case_insensitive(self):
        intent = _MinimalIntent("VAULT_MANAGE", {})
        assert intent.intent_type is IntentType.VAULT_MANAGE

    def test_prediction_buy_maps_to_prediction_buy_enum(self):
        intent = _MinimalIntent("prediction_buy", {})
        assert intent.intent_type is IntentType.PREDICTION_BUY

    def test_prediction_buy_case_insensitive(self):
        intent = _MinimalIntent("PREDICTION_BUY", {})
        assert intent.intent_type is IntentType.PREDICTION_BUY

    def test_prediction_sell_maps_to_prediction_sell_enum(self):
        intent = _MinimalIntent("prediction_sell", {})
        assert intent.intent_type is IntentType.PREDICTION_SELL

    def test_prediction_sell_case_insensitive(self):
        intent = _MinimalIntent("PREDICTION_SELL", {})
        assert intent.intent_type is IntentType.PREDICTION_SELL

    def test_prediction_redeem_maps_to_prediction_redeem_enum(self):
        intent = _MinimalIntent("prediction_redeem", {})
        assert intent.intent_type is IntentType.PREDICTION_REDEEM

    def test_prediction_redeem_case_insensitive(self):
        intent = _MinimalIntent("PREDICTION_REDEEM", {})
        assert intent.intent_type is IntentType.PREDICTION_REDEEM

    def test_ensure_balance_maps_to_ensure_balance_enum(self):
        intent = _MinimalIntent("ensure_balance", {})
        assert intent.intent_type is IntentType.ENSURE_BALANCE

    def test_ensure_balance_case_insensitive(self):
        intent = _MinimalIntent("ENSURE_BALANCE", {})
        assert intent.intent_type is IntentType.ENSURE_BALANCE

    def test_flash_loan_maps_to_flash_loan_enum(self):
        intent = _MinimalIntent("flash_loan", {})
        assert intent.intent_type is IntentType.FLASH_LOAN

    def test_flash_loan_case_insensitive(self):
        intent = _MinimalIntent("FLASH_LOAN", {})
        assert intent.intent_type is IntentType.FLASH_LOAN

    # ------------------------------------------------------------------
    # Full enum-coverage guard: every IntentType.value (lower-cased) must
    # round-trip through _MinimalIntent without falling back to SWAP. This
    # ensures any future IntentType addition forces a `_TYPE_MAP` update.
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("intent_type_enum", list(IntentType))
    def test_every_intent_type_enum_value_is_mapped(self, intent_type_enum: IntentType):
        """Every IntentType enum value must round-trip via its lower-cased name.

        Skips SWAP (which is the fallback target) by asserting the round-trip
        equals the source enum. If a future enum entry is added without a
        corresponding `_TYPE_MAP` entry, the silent SWAP fallback would make
        SWAP != the source enum and this parametrized test would fail.
        """
        key = intent_type_enum.value.lower()
        intent = _MinimalIntent(key, {})
        assert intent.intent_type is intent_type_enum, (
            f"IntentType.{intent_type_enum.name} (key='{key}') is missing from "
            "_TYPE_MAP in inner_runner.py and silently falls back to SWAP."
        )
