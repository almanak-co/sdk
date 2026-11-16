"""Reference strategies say why they hold or act, read in process from each reference."""

import importlib.util
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1]
BORROW_AND_TRADE = (
    "arbitrum_aave_borrow_and_trade",
    "optimism_aave_borrow_and_trade",
    "arbitrum_compound_borrow_and_trade",
    "arbitrum_morpho_borrow_and_trade",
    "avalanche_aave_v4_borrow_and_trade",
)
V4_LP = ("uniswap_v4_arbitrum_lp", "uniswap_v4_base_lp", "uniswap_v4_optimism_lp")


def load(name):
    path = ROOT / "strategies" / name / "strategy.py"
    spec = importlib.util.spec_from_file_location(f"hold_reasons_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def actions():
    def action(kind):
        return lambda name, *_: (kind, name)

    kinds = ("supply", "borrow", "swap", "exit", "revoke", "repay", "withdraw")
    return SimpleNamespace(**{kind: action(kind) for kind in kinds})


def decision(intent=None, memory=None, reason=None):
    return SimpleNamespace(intent=intent, reason=reason)


@pytest.mark.parametrize("name", BORROW_AND_TRADE)
def test_borrow_and_trade_states_its_reason(name):
    module = load(name)
    # Nothing supplied, owed, enabled or approved, in each lending connector's terms.
    position = SimpleNamespace(
        supplied_scaled=0, debt_scaled=0, supplied_shares=0, total_debt=0, collateral_enabled=False
    )
    empty = SimpleNamespace(
        position=position,
        allowance=0,
        needs_cleanup=False,
        collateral_enabled=False,
        drawn_shares=0,
        premium_shares=0,
        premium_offset_ray=0,
        collateral=(),
    )
    owned = [
        SimpleNamespace(target=SimpleNamespace(allocation=name), empty=True)
        for name in ("collateral", "loan", "trade")
    ]

    def decide(observed, teardown=False, **memory):
        context = SimpleNamespace(
            memory=module.State(**memory),
            teardown=teardown,
            observation=lambda n: observed.get(n),
            require_observation=lambda n: observed[n],
            owned=owned,
            actions=actions(),
            config=SimpleNamespace(allocation=lambda n: SimpleNamespace(capital=1)),
            decision=decision,
        )
        return module.decide(context)

    every = {"collateral": empty, "loan": empty, "trade": empty}
    assert "Waiting" in decide({"trade": empty}).reason
    assert decide(every, teardown=True).reason == "Everything is closed; nothing left to do"
    traded = decide(every, teardown=True, traded=True)
    assert traded.intent == ("exit", "trade") and traded.reason is None
    unwinding = decide(every, exiting=True, traded=True)
    assert unwinding.reason == "Unwinding after a failed step or a lending event"
    entered = decide(every)
    assert entered.intent is not None and entered.reason is None
    held = decide(every, phase=3)
    assert held.intent is None and "holding until the strategy stops" in held.reason


@pytest.mark.parametrize("name", V4_LP)
def test_a_v4_liquidity_position_states_why_it_holds(name):
    module = load(name)
    now = datetime(2026, 10, 1, tzinfo=UTC)
    position = SimpleNamespace(owned=SimpleNamespace(position=object()), effective_at=now)

    def reason(observed=position, teardown=False, hold=None, collect=None, **memory):
        context = SimpleNamespace(memory=module.State(**memory), teardown=teardown)
        allocation = SimpleNamespace(name="lp", hold_seconds=hold, collect_seconds=collect)
        return module.hold_reason(context, allocation, observed)

    opened = int(now.timestamp())
    closed = SimpleNamespace(owned=SimpleNamespace(position=None), effective_at=now)
    assert reason(teardown=True) == reason(closed) == "Liquidity closed; nothing left to do"
    assert "collecting fees" in reason(collect=60, minted_at={"lp": opened})
    assert "until the strategy stops" in reason(minted_at={"lp": opened})
    assert "hold time ends" in reason(hold=600, minted_at={"lp": opened})


def test_a_supplier_with_debt_states_its_health_against_the_unwind_level():
    module = load("aave_v4_weth_supply")
    debt = SimpleNamespace(name="loan", minimum_health_factor=10**18)
    config = SimpleNamespace(
        debts=(debt,), parameters=SimpleNamespace(unwind_margin=Decimal("0.2"))
    )

    def reason(observations, owned, **memory):
        context = SimpleNamespace(memory=module.State(**memory), config=config)
        return module.hold_reason(context, observations, owned)

    def observed(health, debt=1):
        return SimpleNamespace(health_factor=health, position=SimpleNamespace(total_debt=debt))

    assert reason({}, {"supply": False}, supplied_at={"supply": 1}) == (
        "Withdrawn; nothing left to do"
    )
    assert reason({"loan": observed(15 * 10**17)}, {"supply": True}) == (
        "Health 1.50, above the 1.20 unwind level; holding"
    )
    assert reason({"loan": observed(None)}, {"supply": True}).startswith("Supplied; holding")
