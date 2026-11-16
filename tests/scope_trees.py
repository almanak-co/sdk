"""Scope-only grants (blueprint 27) against condition trees installed before the change.

Retained fork evidence installed quota-bound grants. Scope-only derivation keeps every
pinned scope word of those trees: contracts, functions, assets, fees, pool keys and hooks,
Safe recipients and owners, routes, vaults, receivers, deadlines and nonzero minimum
outputs. `assert_same_scope` walks a retained tree and a derived tree in parallel and
requires every node to match exactly (parameter type, operator, value and children),
with two exceptions:

- an amount slot: a retained author amount bound may become an amount condition of the
  same zero admission (`NONZERO` or `OPTIONAL`). A Roles allowance term (operator 28)
  inside a bound is part of the slot, and a bound beside a kept fee floor may leave only
  the floor;
- a mint tick word: a pinned or banded tick may become the protocol's whole tick domain.

OR alternatives match as sets, since combining grants merges branches that became equal.

A bound sized by a protocol width (2**k - 1) is not an author amount, and neither is a
bound the caller keeps (GMX keeper fees): both must match exactly.
"""

import json

from eth_abi import decode

from almanak.connectors.v3_pool_domain import MAX_TICK, MIN_TICK
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    bounded,
    either,
    equal,
    positive,
    signed_at_least,
    signed_at_most,
)

MAXIMUM = 2**128 - 1
# Roles 2.1 operators and the static parameter type.
AND, OR, EQUAL, GREATER, LESS = 1, 2, 16, 17, 18
SIGNED_GREATER, SIGNED_LESS, ALLOWANCE = 19, 20, 28
STATIC = 1
# The amount conditions a retained author bound may become, by whether zero is admitted.
NONZERO = (positive(), bounded(MAXIMUM))
OPTIONAL = (any_amount(), either(equal("uint256", 0), bounded(MAXIMUM)))
LOWER_TICK, UPPER_TICK = signed_at_least(MIN_TICK), signed_at_most(MAX_TICK)
ZERO = equal("uint256", 0)


def word(node: Condition) -> int:
    return int.from_bytes(bytes.fromhex(node.value[2:]), "big")


def signed_word(node: Condition) -> int:
    return decode(("int256",), bytes.fromhex(node.value[2:]))[0]


def compares(node: Condition, operator: int) -> bool:
    """A leaf comparing one static ABI word."""
    return (
        node.parameter == STATIC
        and node.operator == operator
        and not node.children
        and len(node.value) == 66
    )


def without_allowance(node: Condition) -> Condition:
    """An AND without its Roles allowance terms, collapsed when one term remains."""
    if node.operator != AND or not any(c.operator == ALLOWANCE for c in node.children):
        return node
    children = tuple(c for c in node.children if c.operator != ALLOWANCE)
    if len(children) == 1:
        return children[0]
    return node.model_copy(update={"children": children})


def author_bound(node: Condition, kept: frozenset[int]) -> bool:
    """A retained `bounded(maximum, allowance)` sized from an author amount."""
    if node.operator != AND or node.parameter != 0:
        return False
    terms = [c for c in node.children if c.operator != ALLOWANCE]
    if len(terms) != 2 or terms[0] != positive() or not compares(terms[1], LESS):
        return False
    maximum = word(terms[1]) - 1
    if maximum in kept:
        return False
    allowance = len(terms) != len(node.children)
    protocol_width = maximum & (maximum + 1) == 0
    return allowance or not protocol_width


def amount_slot(node: Condition, kept: frozenset[int]):
    """The derived forms a retained author amount bound may take, or None."""
    if author_bound(node, kept):
        return NONZERO
    if (
        node.operator == OR
        and len(node.children) == 2
        and node.children[0] == ZERO
        and author_bound(node.children[1], kept)
    ):
        return OPTIONAL
    return None


def tick_word(retained: Condition, derived: Condition) -> bool:
    """A pinned or banded mint tick that now spans the whole tick domain."""
    if derived == LOWER_TICK:
        banded = SIGNED_GREATER
    elif derived == UPPER_TICK:
        banded = SIGNED_LESS
    else:
        return False
    if compares(retained, banded):
        return MIN_TICK - 1 <= signed_word(retained) <= MAX_TICK + 1
    return compares(retained, EQUAL) and MIN_TICK <= signed_word(retained) <= MAX_TICK


def kept_floor(retained: Condition, derived: Condition, kept: frozenset[int]) -> bool:
    """A retained author bound beside a fee floor, of which only the floor remains."""
    node = without_allowance(retained)
    if node.operator != AND or node.parameter != 0 or len(node.children) != 2:
        return False
    bound, floor = node.children
    return author_bound(bound, kept) and compares(floor, GREATER) and derived == floor


def branches(node: Condition) -> list[Condition]:
    """An OR's alternatives, flattened, since combined grants may nest or merge them."""
    if node.operator != OR:
        return [node]
    return [leaf for child in node.children for leaf in branches(child)]


def describe(node: Condition | None) -> str:
    if node is None:
        return "no condition"
    value = "" if node.value == "0x" else f" value={node.value}"
    return f"param={node.parameter} op={node.operator}{value} children={len(node.children)}"


def differences(retained, derived, kept=frozenset(), path="") -> list[str]:
    """Every node path at which a derived tree leaves a retained tree's scope."""
    if retained is None or derived is None:
        same = retained is derived
        return [] if same else [f"{path or '/'}: {describe(retained)} -> {describe(derived)}"]
    allowed = amount_slot(retained, kept)
    if allowed is not None and derived in allowed:
        return []
    if tick_word(retained, derived) or kept_floor(retained, derived, kept):
        return []
    retained = without_allowance(retained)
    if retained.operator == OR or derived.operator == OR:
        return alternatives(retained, derived, kept, path)
    if (retained.parameter, retained.operator, retained.value) != (
        derived.parameter,
        derived.operator,
        derived.value,
    ) or len(retained.children) != len(derived.children):
        return [f"{path or '/'}: {describe(retained)} -> {describe(derived)}"]
    return [
        problem
        for index, (old, new) in enumerate(zip(retained.children, derived.children, strict=True))
        for problem in differences(old, new, kept, f"{path}/{index}")
    ]


def alternatives(retained, derived, kept, path) -> list[str]:
    """OR alternatives match as sets: equal derived branches merge when combined."""
    if retained.parameter != derived.parameter:
        return [f"{path or '/'}: {describe(retained)} -> {describe(derived)}"]
    old, new = branches(retained), branches(derived)
    problems = [
        f"{path}/{index}: retained branch has no derived match: {describe(branch)}"
        for index, branch in enumerate(old)
        if all(differences(branch, other, kept) for other in new)
    ]
    problems += [
        f"{path}/{index}: derived branch has no retained match: {describe(branch)}"
        for index, branch in enumerate(new)
        if all(differences(other, branch, kept) for other in old)
    ]
    if problems and len(old) == len(new):
        # Also name the differing words of each positional pair for a readable failure.
        for index, (a, b) in enumerate(zip(old, new, strict=True)):
            problems += differences(a, b, kept, f"{path}/{index}")
    return problems


def by_function(grants) -> dict:
    result = {}
    for grant in grants:
        if isinstance(grant, dict):
            grant = FunctionGrant.model_validate_json(json.dumps(grant))
        key = (grant.target, grant.signature)
        assert key not in result, f"repeated grant {key}"
        result[key] = grant.condition
    return result


def manifest_grants(manifest: dict) -> list:
    """Every grant an installed retained manifest lists, across its partitions."""
    return [grant for partition in manifest["partitions"] for grant in partition["grants"]]


def assert_same_scope(derived, retained, kept=frozenset()):
    """Derived grants keep every scope word of the retained installed grants by value."""
    new, old = by_function(derived), by_function(retained)
    assert set(new) == set(old), (
        f"functions: derived only {set(new) - set(old)}, retained only {set(old) - set(new)}"
    )
    problems = [
        f"{target} {signature} {problem}"
        for (target, signature), condition in sorted(old.items())
        for problem in differences(condition, new[(target, signature)], frozenset(kept))
    ]
    assert not problems, "\n".join(problems)
