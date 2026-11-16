"""Scope-only exchange authority (blueprint 27) against retained pre-change captures.

Captures made before grants became scope-only keep the per-pool spending maxima
`maximum0`/`maximum1` on allocations and capabilities, and their installed manifests
carry quota-bound grants and a `quotas` list per partition. The maxima names are live
deposit fields on liquidity intents and operations, so only allocations and
capabilities lose them here. Curve allocations and capabilities kept the per-coin
`maximums` the same way; a Curve liquidity intent keeps its own `maximums` deposit.
"""

import copy

RETIRED = ("maximum0", "maximum1")
CURVE_RETIRED = ("maximums",)
ALLOWANCE_OPERATOR = 28


def budget_holder(value: dict) -> bool:
    # An allocation states its capital; a capability is its target and its maxima.
    shape = set(value)
    return "capital" in value or shape in ({"target", *RETIRED}, {"target", *CURVE_RETIRED})


def retired_keys(value: dict) -> tuple[str, ...]:
    if budget_holder(value):
        return RETIRED + CURVE_RETIRED
    # A permission partition retained its Roles allowances beside its grants.
    return ("quotas",) if "grants" in value else ()


def without_exchange_budgets(value):
    """A retained value without the retired maxima and the partition quotas."""
    if isinstance(value, list):
        return [without_exchange_budgets(item) for item in value]
    if not isinstance(value, dict):
        return value
    drop = retired_keys(value)
    return {
        name: without_exchange_budgets(item) for name, item in value.items() if name not in drop
    }


def operators(condition):
    yield condition.operator
    for child in condition.children:
        yield from operators(child)


def assert_scope_only(grants):
    """No generated grant carries a Roles allowance."""
    for grant in grants:
        if grant.condition is not None:
            assert ALLOWANCE_OPERATOR not in set(operators(grant.condition)), grant.signature


def without_repeated_entry_quotes(current: dict, retained: dict) -> dict:
    """A current run state without the entry quotes a retained pre-change run lacked.

    Entry quotes now repeat while funding inventory remains (blueprint 27 section 7), so a
    decision made after first exposure also carries the entry slice. Only those quotes
    leave the comparison; every other fact of the run must still match.
    """
    result = copy.deepcopy(current)
    pairs = zip(result.get("completed", ()), retained.get("completed", ()), strict=False)
    for now, then in pairs:
        before, original = (
            now["transition"]["result"]["before"],
            then["transition"]["result"]["before"],
        )
        if original.get("entry_quote") is None and before["owned"].get("entry_complete"):
            before["entry_quote"] = original.get("entry_quote")
    return result
