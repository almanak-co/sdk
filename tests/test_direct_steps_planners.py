"""The step lists a preview states are held to the planners they restate.

Each case drives the connector's real planner step by step over a small model of the
wallet's allowance and position, and compares the sequence it plans with the
connector's `steps` hook for the first observation. The model applies each step the
way the chain does and ends a request by the connector's own completion rule
(`strategy_result` in its `state.py`), restated here.
"""

from types import SimpleNamespace
from typing import Any

import pytest

from almanak import bridge_intents, lending_intents
from almanak.connectors import pool_lending_direct
from almanak.connectors.aave_v3 import planning as pool_planning
from almanak.connectors.across import direct as across_direct
from almanak.connectors.across import planning as across_planning

TARGET = SimpleNamespace(asset="asset", allocation="position")
AMOUNT = 5
POOL_INTENTS: dict[str, Any] = {
    "supply": lending_intents.SupplyIntent,
    "borrow": lending_intents.BorrowIntent,
    "collateral": lending_intents.CollateralIntent,
    "withdraw": lending_intents.WithdrawIntent,
    "withdraw_amount": lending_intents.WithdrawAmountIntent,
    "repay_amount": lending_intents.RepayAmountIntent,
}
OPERATIONS = ("Approve", "Supply", "Borrow", "Repay", "Revoke", "Withdraw", "Collateral")


def amount(value: int):
    return SimpleNamespace(asset="asset", amount=value)


def plain(kind: str):
    """An operation as a plain record of its kind and fields."""
    return lambda **fields: SimpleNamespace(kind=kind, **fields)


@pytest.fixture
def pool_planner(monkeypatch):
    """The Pool planner's own `operation`, with authority admitted and plain operations."""
    for name in OPERATIONS:
        monkeypatch.setattr(pool_planning, f"{name}Operation", plain(name.lower()))
    monkeypatch.setattr(pool_planning, "AssetAmount", lambda **fields: SimpleNamespace(**fields))
    monkeypatch.setattr(pool_planning, "admit", lambda capabilities, intent: None)
    return pool_planning.operation


def pool_intent(kind: str):
    fields: dict = {"target": TARGET, "kind": kind}
    if kind in ("supply", "borrow", "withdraw_amount", "repay_amount"):
        fields["amount"] = amount(AMOUNT)
    if kind == "collateral":
        fields["enabled"] = True
    return POOL_INTENTS[kind].model_construct(**fields)


def pool_before(wallet):
    position = SimpleNamespace(
        supplied_scaled=wallet.supplied,
        supplied_assets=wallet.supplied,
        debt_scaled=wallet.debt,
        debt_assets=wallet.debt,
        collateral_enabled=bool(wallet.supplied),
    )
    return SimpleNamespace(target=TARGET, allowance=wallet.allowance, position=position)


def apply_pool(wallet, operation) -> None:
    """What the mined step leaves: an approval sets the allowance, a supply or a
    repayment spends it, a full withdrawal takes the whole supply."""
    kind = operation.kind
    if kind == "approve":
        wallet.allowance = operation.amount.amount
    elif kind == "revoke":
        wallet.allowance = 0
    elif kind == "supply":
        wallet.allowance -= AMOUNT
        wallet.supplied += AMOUNT
    elif kind == "repay":
        wallet.allowance -= AMOUNT
        wallet.debt -= AMOUNT
    elif kind == "withdraw":
        wallet.supplied -= wallet.supplied if operation.full else AMOUNT


def pool_complete(intent, kind: str, wallet) -> bool:
    """`Outcome.strategy_result().complete` of the Pool connectors, restated."""
    if kind == "approve":
        return False
    if isinstance(intent, lending_intents.WithdrawIntent):
        return not (wallet.supplied or wallet.allowance)
    return kind == "revoke" or not wallet.allowance


def pool_sequence(plan, kind: str, wallet) -> list[str]:
    intent = pool_intent(kind)
    config = SimpleNamespace(target=lambda allocation: TARGET)
    decision = SimpleNamespace(intent=intent, identity="decision")
    state = SimpleNamespace(decision=decision, capabilities=object(), outcomes=[])
    planned = []
    for _ in range(6):
        operation = plan(config, state, intent, pool_before(wallet))
        planned.append(operation.kind)
        apply_pool(wallet, operation)
        state.outcomes.append(SimpleNamespace(success=True, decision=decision, operation=operation))
        if pool_complete(intent, operation.kind, wallet):
            return planned
    raise AssertionError(f"{kind} did not complete: {planned}")


@pytest.mark.parametrize(
    ("kind", "allowance", "supplied", "debt", "expected"),
    [
        ("supply", 0, 0, 0, ["approve", "supply"]),
        ("supply", AMOUNT, 0, 0, ["supply"]),
        ("borrow", 0, 9, 0, ["borrow"]),
        ("borrow", 7, 9, 0, ["borrow", "revoke"]),
        ("collateral", 7, 9, 0, ["collateral", "revoke"]),
        ("repay_amount", 0, 0, 9, ["approve", "repay"]),
        ("repay_amount", AMOUNT, 0, 9, ["repay"]),
        ("repay_amount", 7, 0, 0, ["revoke"]),
        ("withdraw", 0, 9, 0, ["withdraw"]),
        ("withdraw", 7, 9, 0, ["withdraw", "revoke"]),
        ("withdraw_amount", 7, 9, 0, ["withdraw", "revoke"]),
        ("withdraw", 7, 0, 0, ["revoke"]),
    ],
)
def test_the_pool_hook_lists_what_the_pool_planner_plans(
    pool_planner, kind, allowance, supplied, debt, expected
):
    first = SimpleNamespace(allowance=allowance, supplied=supplied, debt=debt)
    hook = pool_lending_direct.steps(
        SimpleNamespace(
            decision=SimpleNamespace(intent=SimpleNamespace(kind=kind)),
            plan=SimpleNamespace(before=pool_before(first)),
        )
    )
    planned = pool_sequence(pool_planner, kind, first)
    assert hook == planned == expected


def across_target(native: bool) -> SimpleNamespace:
    return SimpleNamespace(
        asset="asset", allocation="position", route=SimpleNamespace(native=native)
    )


def across_sequence(monkeypatch, kind: str, allowance: int, native: bool = False) -> list[str]:
    """Drive `planning.step` until the connector's completion rule ends the request."""
    made: dict[str, Any] = {
        "bridge": bridge_intents.BridgeIntent,
        "revoke_bridge": bridge_intents.RevokeBridgeIntent,
    }
    intent = made[kind].model_construct(
        kind=kind, target=across_target(native), amount=amount(AMOUNT)
    )
    wallet = SimpleNamespace(allowance=allowance, deposited=False)
    monkeypatch.setattr(across_planning, "require_scope", lambda *scope: None)
    monkeypatch.setattr(across_planning, "executed", lambda state, decided: wallet.deposited)
    planned = []
    for _ in range(6):
        before = SimpleNamespace(allowance=wallet.allowance, target=intent.target)
        step = across_planning.step(None, None, intent, before)
        # A `clear` is a revocation that does not end the request.
        planned.append("revoke" if step == "clear" else step)
        wallet.allowance = AMOUNT if step == "approve" else 0
        wallet.deposited = wallet.deposited or step == "deposit"
        if step in ("deposit", "revoke"):
            return planned
    raise AssertionError(f"{kind} did not complete: {planned}")


@pytest.mark.parametrize(
    ("kind", "allowance", "expected"),
    [
        ("bridge", 0, ["approve", "deposit"]),
        ("bridge", AMOUNT, ["deposit"]),
        ("bridge", 7, ["revoke", "approve", "deposit"]),
        ("revoke_bridge", 7, ["revoke"]),
    ],
)
def test_the_across_hook_lists_what_the_across_planner_plans(
    monkeypatch, kind, allowance, expected
):
    intent = SimpleNamespace(kind=kind, amount=amount(AMOUNT), target=across_target(False))
    state = SimpleNamespace(
        decision=SimpleNamespace(intent=intent),
        plan=SimpleNamespace(before=SimpleNamespace(allowance=allowance)),
    )
    hook = across_direct.steps(state)
    assert hook == across_sequence(monkeypatch, kind, allowance) == expected


@pytest.mark.parametrize("allowance", [0, 7, AMOUNT])
def test_a_native_bridge_is_the_deposit_alone_whatever_allowance_stands(monkeypatch, allowance):
    """A native route gives the SpokePool no approval, so the wrapped token's allowance
    changes neither what the planner plans nor what the hook lists."""
    intent = SimpleNamespace(kind="bridge", amount=amount(AMOUNT), target=across_target(True))
    state = SimpleNamespace(
        decision=SimpleNamespace(intent=intent),
        plan=SimpleNamespace(before=SimpleNamespace(allowance=allowance)),
    )
    planned = across_sequence(monkeypatch, "bridge", allowance, native=True)
    assert across_direct.steps(state) == planned == ["deposit"]
