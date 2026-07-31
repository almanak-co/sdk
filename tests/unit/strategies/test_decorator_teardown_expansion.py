"""Tests for @almanak_strategy decorator teardown complement auto-expansion.

Verifies that the decorator automatically expands intent_types to include
teardown complements (e.g., SUPPLY -> WITHDRAW, BORROW -> REPAY) so
strategies don't need to declare them explicitly.
"""

import pytest

from almanak.core.intent_types import IntentType
from almanak.framework.strategies import IntentStrategy, almanak_strategy


def test_supply_auto_expands_withdraw():
    """SUPPLY should auto-expand to include WITHDRAW."""

    @almanak_strategy(
        name="test_supply_expand",
        intent_types=[IntentType.SUPPLY, IntentType.HOLD],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert IntentType.SUPPLY in TestStrategy.STRATEGY_METADATA.intent_types
    assert IntentType.WITHDRAW in TestStrategy.STRATEGY_METADATA.intent_types
    assert IntentType.HOLD in TestStrategy.STRATEGY_METADATA.intent_types


def test_borrow_auto_expands_repay():
    """BORROW should auto-expand to include REPAY."""

    @almanak_strategy(
        name="test_borrow_expand",
        intent_types=[IntentType.SUPPLY, IntentType.BORROW],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert IntentType.REPAY in types, "BORROW should auto-expand to include REPAY"
    assert IntentType.WITHDRAW in types, "SUPPLY should auto-expand to include WITHDRAW"


def test_lp_open_auto_expands_lp_close():
    """LP_OPEN should auto-expand to include LP_CLOSE."""

    @almanak_strategy(
        name="test_lp_expand",
        intent_types=[IntentType.SWAP, IntentType.LP_OPEN],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert IntentType.LP_CLOSE in TestStrategy.STRATEGY_METADATA.intent_types


def test_perp_open_auto_expands_perp_close_and_cancel():
    """PERP_OPEN should auto-expand to include PERP_CLOSE and PERP_CANCEL_ORDER.

    VIB-5569: cancel is the teardown-recovery verb for a stranded pending order;
    auto-expanding it into intent_types is what lets the generated Safe manifest
    authorise ExchangeRouter.cancelOrder for a gmx_v2 perp strategy that only
    declared PERP_OPEN.
    """

    @almanak_strategy(
        name="test_perp_expand",
        intent_types=[IntentType.PERP_OPEN],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert IntentType.PERP_CLOSE in TestStrategy.STRATEGY_METADATA.intent_types
    assert IntentType.PERP_CANCEL_ORDER in TestStrategy.STRATEGY_METADATA.intent_types


def test_vault_deposit_auto_expands_vault_redeem():
    """VAULT_DEPOSIT should auto-expand to include VAULT_REDEEM."""

    @almanak_strategy(
        name="test_vault_expand",
        intent_types=[IntentType.VAULT_DEPOSIT],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert IntentType.VAULT_REDEEM in TestStrategy.STRATEGY_METADATA.intent_types


def test_no_expansion_when_complements_already_declared():
    """No duplicates when teardown complements are already in intent_types."""

    @almanak_strategy(
        name="test_no_dup_expand",
        intent_types=[IntentType.SUPPLY, IntentType.WITHDRAW, IntentType.BORROW, IntentType.REPAY],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types.count(IntentType.WITHDRAW) == 1, "Should not duplicate WITHDRAW"
    assert types.count(IntentType.REPAY) == 1, "Should not duplicate REPAY"


def test_repeated_open_intents_add_complement_once():
    """Duplicate SUPPLY entries should only produce a single WITHDRAW."""

    @almanak_strategy(
        name="test_dedup_expand",
        intent_types=[IntentType.SUPPLY, IntentType.SUPPLY, IntentType.BORROW],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types.count(IntentType.WITHDRAW) == 1, "Duplicate SUPPLY should produce single WITHDRAW"
    assert types.count(IntentType.REPAY) == 1, "BORROW should produce single REPAY"


def test_close_only_does_not_expand_to_open():
    """WITHDRAW-only should NOT auto-expand to include SUPPLY (one-way expansion)."""

    @almanak_strategy(
        name="test_close_only",
        intent_types=[IntentType.WITHDRAW],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert IntentType.SUPPLY not in types, "Close-only should NOT expand to open"
    assert IntentType.WITHDRAW in types


def test_swap_only_no_expansion():
    """SWAP-only strategies should not have any teardown expansion."""

    @almanak_strategy(
        name="test_swap_only",
        intent_types=[IntentType.SWAP, IntentType.HOLD],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types == [IntentType.SWAP, IntentType.HOLD], "SWAP+HOLD should not trigger expansion"


def test_empty_intent_types_no_expansion():
    """Empty intent_types should stay empty."""

    @almanak_strategy(
        name="test_empty_types",
        intent_types=[],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert TestStrategy.STRATEGY_METADATA.intent_types == []


def test_enum_declaration_keeps_legacy_serialized_values():
    """Typed declarations still expose and serialize the historical strings."""

    @almanak_strategy(name="test_typed_metadata", intent_types=[IntentType.SWAP, IntentType.HOLD])
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

    metadata = TestStrategy.STRATEGY_METADATA
    assert metadata.intent_types == [IntentType.SWAP, IntentType.HOLD]
    assert metadata.to_dict()["intent_types"] == ["SWAP", "HOLD"]


def test_legacy_string_declaration_is_parsed_and_typo_rejected():
    """The public compatibility boundary parses strings immediately."""

    with pytest.raises(ValueError, match=r"invalid intent type 'SWAAP'.*SWAP"):

        @almanak_strategy(name="test_typo", intent_types=["SWAAP"])  # type: ignore[list-item]
        class TestStrategy(IntentStrategy):
            def decide(self, market):
                pass
