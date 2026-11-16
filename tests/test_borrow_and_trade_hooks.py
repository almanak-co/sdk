"""Borrow-and-trade references advance one phase per outcome and unwind in a fixed order."""

from types import SimpleNamespace

import pytest

from tests.test_optional_hold_seconds import Actions, strategy_module

BORROW_AND_TRADE = (
    "ethereum_aave_v4_borrow_and_trade",
    "avalanche_aave_v4_borrow_and_trade",
    "base_compound_borrow_and_trade",
    "base_morpho_borrow_and_trade",
)
AAVE_V4 = BORROW_AND_TRADE[:2]


def outcome(name, status, *, kind="supply", **memory):
    strategy = strategy_module(name)
    context = SimpleNamespace(
        memory=strategy.State(**memory),
        result=SimpleNamespace(status=status, intent=SimpleNamespace(kind=kind)),
    )
    return strategy.on_outcome(context)


@pytest.mark.parametrize("name", BORROW_AND_TRADE)
def test_completed_entries_advance_one_phase_and_mark_the_trade(name):
    after = outcome(name, "completed", phase=0)
    assert (after.phase, after.traded, after.exiting) == (1, False, False)
    traded = outcome(name, "completed", kind="swap", phase=2)
    assert (traded.phase, traded.traded) == (3, True)


@pytest.mark.parametrize("name", BORROW_AND_TRADE)
def test_a_failed_outcome_starts_the_exit_and_an_unfinished_one_changes_nothing(name):
    failed = outcome(name, "failed", phase=1)
    assert (failed.phase, failed.exiting) == (1, True)
    pending = outcome(name, "submitted", phase=1)
    assert (pending.phase, pending.exiting, pending.traded) == (1, False, False)


@pytest.mark.parametrize("name", BORROW_AND_TRADE)
def test_exiting_outcomes_only_clear_the_trade(name):
    reversed_trade = outcome(
        name, "completed", kind="exit_exchange", phase=3, traded=True, exiting=True
    )
    assert (reversed_trade.traded, reversed_trade.phase, reversed_trade.exiting) == (
        False,
        3,
        True,
    )
    repaid = outcome(name, "completed", kind="repay", phase=3, exiting=True)
    assert (repaid.phase, repaid.exiting) == (3, True)


def loan(**values):
    fields = {
        "total_debt": 0,
        "drawn_shares": 0,
        "premium_shares": 0,
        "premium_offset_ray": 0,
        "allowance": 0,
        "collateral": (),
    }
    fields.update(values)
    return SimpleNamespace(
        position=SimpleNamespace(total_debt=fields.pop("total_debt")),
        collateral=tuple(SimpleNamespace(enabled=e) for e in fields.pop("collateral")),
        **fields,
    )


def collateral(supplied=0, enabled=False, allowance=0):
    return SimpleNamespace(
        position=SimpleNamespace(supplied_shares=supplied),
        collateral_enabled=enabled,
        allowance=allowance,
    )


class Exits(Actions):
    def __init__(self, exit_intent):
        self.exit_intent = exit_intent

    def exit(self, allocation):
        return self.exit_intent


def close(name, *, traded=False, exit_intent=("exit", "trade"), cleanup=False, **observed):
    strategy = strategy_module(name)
    observations = {
        "trade": SimpleNamespace(needs_cleanup=cleanup),
        "loan": observed.get("loan", loan()),
        "collateral": observed.get("collateral", collateral()),
    }
    context = SimpleNamespace(
        memory=strategy.State(traded=traded, exiting=True),
        actions=Exits(exit_intent),
        require_observation=observations.__getitem__,
    )
    return strategy.close(context)


@pytest.mark.parametrize("name", AAVE_V4)
def test_aave_close_reverses_the_trade_before_cleanup(name):
    assert close(name, traded=True) == ("exit", "trade")
    assert close(name, traded=True, exit_intent=None, cleanup=True) == ("revoke", "trade")
    assert close(name, cleanup=True) == ("revoke", "trade")


@pytest.mark.parametrize("name", AAVE_V4)
@pytest.mark.parametrize(
    "remaining",
    [
        {"total_debt": 1},
        {"drawn_shares": 1},
        {"premium_shares": 1},
        {"premium_offset_ray": 1},
        {"allowance": 1},
        {"collateral": (False, True)},
    ],
)
def test_aave_close_repays_while_any_debt_fact_remains(name, remaining):
    assert close(name, loan=loan(**remaining), collateral=collateral(supplied=5)) == (
        "repay",
        "loan",
    )


@pytest.mark.parametrize("name", AAVE_V4)
@pytest.mark.parametrize("remaining", [{"supplied": 1}, {"enabled": True}, {"allowance": 1}])
def test_aave_close_withdraws_collateral_after_debt_is_cleared(name, remaining):
    assert close(name, loan=loan(collateral=(False,)), collateral=collateral(**remaining)) == (
        "withdraw",
        "collateral",
    )


@pytest.mark.parametrize("name", AAVE_V4)
def test_aave_close_finishes_once_nothing_remains(name):
    assert close(name, traded=True, exit_intent=None) is None
