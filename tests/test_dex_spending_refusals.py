"""Aerodrome and Curve gateway spending and entry admission, without retained evidence."""

from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome import validation as aerodrome
from almanak.connectors.curve import validation as curve
from almanak.dex_intents import (
    ExitExchange,
    IncreaseLiquidity,
    IncreasePoolLiquidity,
    OpenLiquidity,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError

A, B = "eip155:8453/erc20:0xa", "eip155:8453/erc20:0xb"
TARGET = SimpleNamespace(allocation="pool")
OTHER = SimpleNamespace(allocation="other")


def amount(asset, value):
    return SimpleNamespace(asset=asset, amount=value)


def amounts(*values):
    return SimpleNamespace(root=values)


def done(operation, success=True, target=TARGET):
    return SimpleNamespace(
        operation=operation.model_copy(update={"target": target}),
        success=success,
        accounting_issues=(),
    )


def state(allocation, outcomes=()):
    config = SimpleNamespace(allocation=lambda name: allocation)
    return SimpleNamespace(config=config, outcomes=tuple(outcomes), mode="running")


def refusal(call):
    with pytest.raises(DataError) as refused:
        call()
    return refused.value.kind, str(refused.value)


def aerodrome_before(inventory=(100, 100), position=None, exit_quote=None):
    owned = SimpleNamespace(
        inventory0=amount(A, inventory[0]), inventory1=amount(B, inventory[1]), position=position
    )
    return SimpleNamespace(owned=owned, exit_quote=exit_quote)


INVENTORY = ("inventory", "economic action exceeds attributed input inventory")


def test_aerodrome_swaps_are_bounded_by_inventory_only():
    # There is no cumulative budget (blueprint 27): only the inventory held now limits a swap.
    swap = SwapExactInput.model_construct(target=TARGET, amount=amount(A, 100))
    aerodrome.validate_inventory(swap, aerodrome_before())
    over = swap.model_copy(update={"amount": amount(A, 101)})
    assert refusal(lambda: aerodrome.validate_inventory(over, aerodrome_before())) == INVENTORY


@pytest.mark.parametrize("kind", [OpenLiquidity, IncreaseLiquidity])
def test_aerodrome_liquidity_desired_inputs_are_bounded_by_inventory(kind):
    intent = kind.model_construct(target=TARGET, maximum0=amount(A, 100), maximum1=amount(B, 100))
    aerodrome.validate_inventory(intent, aerodrome_before())
    over = intent.model_copy(update={"maximum1": amount(B, 101)})
    assert refusal(lambda: aerodrome.validate_inventory(over, aerodrome_before())) == INVENTORY


def test_aerodrome_exit_swap_spends_its_quote_only_without_a_position():
    exit_intent = ExitExchange.model_construct(target=TARGET)
    quote = SimpleNamespace(amount_in=amount(B, 100))
    aerodrome.validate_inventory(exit_intent, aerodrome_before(exit_quote=quote))
    beyond = SimpleNamespace(amount_in=amount(B, 101))
    short = aerodrome_before(exit_quote=beyond)
    assert refusal(lambda: aerodrome.validate_inventory(exit_intent, short)) == INVENTORY
    # An exit that still holds a position, or has no quote, spends nothing here.
    held = aerodrome_before(position=object(), exit_quote=beyond)
    aerodrome.validate_inventory(exit_intent, held)
    aerodrome.validate_inventory(exit_intent, aerodrome_before())
    revoke = RevokeExchange.model_construct(target=TARGET)
    aerodrome.validate_inventory(revoke, aerodrome_before())


def curve_before(inventory=(100, 100), position=None, exit_quotes=()):
    owned = SimpleNamespace(
        inventory=amounts(amount(A, inventory[0]), amount(B, inventory[1])), position=position
    )
    return SimpleNamespace(owned=owned, exit_quotes=exit_quotes)


def test_curve_swaps_are_bounded_by_inventory_only():
    # There is no cumulative budget (blueprint 27): only the inventory held now limits a swap.
    swap = SwapExactInput.model_construct(target=TARGET, amount=amount(A, 100))
    curve.validate_inventory(swap, curve_before())
    over = swap.model_copy(update={"amount": amount(A, 101)})
    assert refusal(lambda: curve.validate_inventory(over, curve_before())) == INVENTORY


@pytest.mark.parametrize("kind", [OpenPoolLiquidity, IncreasePoolLiquidity])
def test_curve_liquidity_inputs_are_bounded_by_inventory(kind):
    intent = kind.model_construct(target=TARGET, maximums=amounts(amount(A, 100), amount(B, 100)))
    curve.validate_inventory(intent, curve_before())
    over = intent.model_copy(update={"maximums": amounts(amount(A, 1), amount(B, 101))})
    assert refusal(lambda: curve.validate_inventory(over, curve_before())) == INVENTORY


def test_curve_exit_swap_spends_its_first_quote_only_without_a_position():
    exit_intent = ExitExchange.model_construct(target=TARGET)
    quotes = (SimpleNamespace(amount_in=amount(B, 101)),)
    short = curve_before(exit_quotes=quotes)
    assert refusal(lambda: curve.validate_inventory(exit_intent, short)) == INVENTORY
    curve.validate_inventory(exit_intent, curve_before(exit_quotes=quotes[:0]))
    held = curve_before(position=object(), exit_quotes=quotes)
    curve.validate_inventory(exit_intent, held)
    revoke = RevokeExchange.model_construct(target=TARGET)
    curve.validate_inventory(revoke, curve_before())


@pytest.mark.parametrize("module", [aerodrome, curve])
def test_entry_requires_a_running_reconciled_deployment_and_the_minimum_balance(module):
    allocation = SimpleNamespace(minimum_native_balance=25)
    eth = "eip155:8453/slip44:60"
    funded = SimpleNamespace(native_balance=amount(eth, 25))
    module.validate_entry(state(None), funded, allocation)
    short = SimpleNamespace(native_balance=amount(eth, 24))
    assert refusal(lambda: module.validate_entry(state(None), short, allocation)) == (
        "gas_budget",
        f"gas payer is below the configured entry minimum: it holds 24 {eth}, "
        "minimum_native_balance is 25 (base units)",
    )
    unobserved = SimpleNamespace(native_balance=None)
    assert (
        "an unobserved balance"
        in refusal(lambda: module.validate_entry(state(None), unobserved, allocation))[1]
    )
    paused = SimpleNamespace(config=None, outcomes=(), mode="teardown")
    assert refusal(lambda: module.validate_entry(paused, funded, allocation))[0] == "accounting"
    unreconciled = state(None, (SimpleNamespace(accounting_issues=("drift",)),))
    assert refusal(lambda: module.validate_entry(unreconciled, funded, allocation))[0] == (
        "accounting"
    )
