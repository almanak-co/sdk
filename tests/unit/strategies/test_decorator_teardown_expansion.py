"""Tests for @almanak_strategy decorator teardown complement auto-expansion.

Verifies that the decorator automatically expands intent_types to include
teardown complements (e.g., SUPPLY -> WITHDRAW, BORROW -> REPAY) so
strategies don't need to declare them explicitly.
"""

from almanak.framework.strategies import IntentStrategy, almanak_strategy


def test_supply_auto_expands_withdraw():
    """SUPPLY should auto-expand to include WITHDRAW."""

    @almanak_strategy(
        name="test_supply_expand",
        intent_types=["SUPPLY", "HOLD"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert "SUPPLY" in TestStrategy.STRATEGY_METADATA.intent_types
    assert "WITHDRAW" in TestStrategy.STRATEGY_METADATA.intent_types
    assert "HOLD" in TestStrategy.STRATEGY_METADATA.intent_types


def test_borrow_auto_expands_repay():
    """BORROW should auto-expand to include REPAY."""

    @almanak_strategy(
        name="test_borrow_expand",
        intent_types=["SUPPLY", "BORROW"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert "REPAY" in types, "BORROW should auto-expand to include REPAY"
    assert "WITHDRAW" in types, "SUPPLY should auto-expand to include WITHDRAW"


def test_lp_open_auto_expands_lp_close():
    """LP_OPEN should auto-expand to include LP_CLOSE."""

    @almanak_strategy(
        name="test_lp_expand",
        intent_types=["SWAP", "LP_OPEN"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert "LP_CLOSE" in TestStrategy.STRATEGY_METADATA.intent_types


def test_perp_open_auto_expands_perp_close():
    """PERP_OPEN should auto-expand to include PERP_CLOSE."""

    @almanak_strategy(
        name="test_perp_expand",
        intent_types=["PERP_OPEN"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert "PERP_CLOSE" in TestStrategy.STRATEGY_METADATA.intent_types


def test_vault_deposit_auto_expands_vault_redeem():
    """VAULT_DEPOSIT should auto-expand to include VAULT_REDEEM."""

    @almanak_strategy(
        name="test_vault_expand",
        intent_types=["VAULT_DEPOSIT"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    assert "VAULT_REDEEM" in TestStrategy.STRATEGY_METADATA.intent_types


def test_no_expansion_when_complements_already_declared():
    """No duplicates when teardown complements are already in intent_types."""

    @almanak_strategy(
        name="test_no_dup_expand",
        intent_types=["SUPPLY", "WITHDRAW", "BORROW", "REPAY"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types.count("WITHDRAW") == 1, "Should not duplicate WITHDRAW"
    assert types.count("REPAY") == 1, "Should not duplicate REPAY"


def test_repeated_open_intents_add_complement_once():
    """Duplicate SUPPLY entries should only produce a single WITHDRAW."""

    @almanak_strategy(
        name="test_dedup_expand",
        intent_types=["SUPPLY", "SUPPLY", "BORROW"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types.count("WITHDRAW") == 1, "Duplicate SUPPLY should produce single WITHDRAW"
    assert types.count("REPAY") == 1, "BORROW should produce single REPAY"


def test_close_only_does_not_expand_to_open():
    """WITHDRAW-only should NOT auto-expand to include SUPPLY (one-way expansion)."""

    @almanak_strategy(
        name="test_close_only",
        intent_types=["WITHDRAW"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert "SUPPLY" not in types, "Close-only should NOT expand to open"
    assert "WITHDRAW" in types


def test_swap_only_no_expansion():
    """SWAP-only strategies should not have any teardown expansion."""

    @almanak_strategy(
        name="test_swap_only",
        intent_types=["SWAP", "HOLD"],
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            pass

        def get_open_positions(self):
            pass

        def generate_teardown_intents(self, mode, market=None):
            return []

    types = TestStrategy.STRATEGY_METADATA.intent_types
    assert types == ["SWAP", "HOLD"], "SWAP+HOLD should not trigger expansion"


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
