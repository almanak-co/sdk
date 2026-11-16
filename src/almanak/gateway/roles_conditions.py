"""Zodiac 2.1.0 ABI predicates. Branches retain whole correlated argument tuples."""

import re
from collections import deque
from typing import Annotated, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.gateway.accounts import HexData
from almanak.models import Address, Model


class Condition(Model):
    parameter: Literal[0, 1, 2, 3, 4, 5, 6]
    operator: Literal[1, 2, 5, 7, 15, 16, 17, 18, 19, 20, 21, 28]
    value: HexData = "0x"
    children: tuple["Condition", ...] = ()


class FlatCondition(Model):
    parent: Annotated[int, Field(ge=0, le=255)]
    parameter: int
    operator: int
    value: HexData

    def abi(self):
        return self.parent, self.parameter, self.operator, bytes.fromhex(self.value[2:])


class FunctionGrant(Model):
    target: Address
    signature: str
    condition: Condition | None
    explanation: tuple[str, ...]

    @model_validator(mode="after")
    def argument_scope(self):
        if (
            self.condition is None
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\(\)", self.signature) is None
        ):
            raise ValueError("selector-only grants require a zero-argument function")
        return self

    @property
    def selector(self) -> bytes:
        return keccak(text=self.signature)[:4]

    @property
    def conditions(self) -> tuple[FlatCondition, ...]:
        return flatten(self.condition) if self.condition is not None else ()


def flatten(root: Condition) -> tuple[FlatCondition, ...]:
    queue = deque([(0, root)])
    result: list[FlatCondition] = []
    while queue:
        parent, node = queue.popleft()
        index = len(result)
        # Parent indices are uint8: refuse the oversized tree before a node names one.
        if index >= 256:
            raise ValueError("condition tree exceeds the supported Roles contract limits")
        result.append(
            FlatCondition(
                parent=parent, parameter=node.parameter, operator=node.operator, value=node.value
            )
        )
        queue.extend((index, child) for child in node.children)
    # Parent indices are uint8. WriteOnce runtime bytecode also obeys EIP-170.
    if len(result) > 256 or len(packed(tuple(result))) + 1 > 24576:
        raise ValueError("condition tree exceeds the supported Roles contract limits")
    return tuple(result)


def packed(conditions: tuple[FlatCondition, ...]) -> bytes:
    prefix = b"".join(bytes((c.parent, (c.parameter << 5) | c.operator)) for c in conditions)
    suffix = []
    for index, condition in enumerate(conditions):
        if condition.operator >= 16:
            value = bytes.fromhex(condition.value[2:])
            # The deployed Packer removes the head offset for dynamic equality.
            if condition.operator == 16 and not inline(conditions, index):
                value = value[32:]
            suffix.append(keccak(value) if condition.operator == 16 else value)
    return prefix + b"".join(suffix)


def inline(conditions: tuple[FlatCondition, ...], index: int) -> bool:
    parameter = conditions[index].parameter
    if parameter == 1:
        return True
    if parameter in (2, 4, 5, 6):
        return False
    return all(
        inline(conditions, child)
        for child in range(index + 1, len(conditions))
        if conditions[child].parent == index
    )


def comparison(operator, value: bytes) -> Condition:
    return Condition(parameter=1, operator=operator, value="0x" + value.hex())


def equal(type_: str, value) -> Condition:
    return comparison(16, encode((type_,), (value,)))


def matches(*children: Condition, parameter=5) -> Condition:
    return Condition(parameter=parameter, operator=5, children=children)


def either(*children: Condition) -> Condition:
    return (
        children[0] if len(children) == 1 else Condition(parameter=0, operator=2, children=children)
    )


def nested_call(signature: str, *arguments: Condition) -> Condition:
    return nested_header(keccak(text=signature)[:4], *arguments)


def nested_header(header: bytes, *arguments: Condition) -> Condition:
    # CalldataMatches skips the first four bytes. Pair it with a dynamic-byte bitmask;
    # Roles requires the decoded calldata branch first to establish its layout.
    return Condition(parameter=0, operator=1, children=(matches(*arguments), *pinned(0, header)))


# A Bitmask compares at most 15 bytes of a Dynamic value, read from its 2-byte shift.
BITMASK_BYTES = 15


def pinned(offset: int, expected: bytes, mask: bytes | None = None) -> tuple[Condition, ...]:
    """Dynamic-byte bitmasks that require `value & mask == expected` at `offset` of a
    `bytes` value; the mask defaults to every bit, pinning `expected` exactly.

    A value shorter than `offset` fails closed (BitmaskOverflow); past its end Roles reads
    zeros, so a trailing pin of zero bytes also passes a value that ends early.
    """
    mask = b"\xff" * len(expected) if mask is None else mask
    if not expected or len(mask) != len(expected) or offset + len(expected) > 2**16:
        raise ValueError("a bitmask pin needs bytes and a mask within a 16-bit shift")
    if any(e & ~m for e, m in zip(expected, mask, strict=True)):
        raise ValueError("a bitmask pin expects only bits its mask selects")
    result = []
    for start in range(0, len(expected), BITMASK_BYTES):
        chunk = expected[start : start + BITMASK_BYTES].ljust(BITMASK_BYTES, b"\x00")
        selected = mask[start : start + BITMASK_BYTES].ljust(BITMASK_BYTES, b"\x00")
        value = (offset + start).to_bytes(2, "big") + selected + chunk
        result.append(Condition(parameter=2, operator=21, value="0x" + value.hex()))
    return tuple(result)


def every(*children: Condition) -> Condition:
    return (
        children[0] if len(children) == 1 else Condition(parameter=0, operator=1, children=children)
    )


def bounded(maximum: int) -> Condition:
    if not 0 < maximum < 2**128:
        raise ValueError("bounded permission requires a positive uint128 amount")
    return Condition(
        parameter=0,
        operator=1,
        children=(
            comparison(17, encode(("uint256",), (0,))),
            comparison(18, encode(("uint256",), (maximum + 1,))),
        ),
    )


def positive() -> Condition:
    """Any nonzero amount: scope-only authority bounds where value goes, not how much."""
    return comparison(17, encode(("uint256",), (0,)))


def any_amount() -> Condition:
    """Any amount, zero included: an approval or revocation, or an optional deposit side."""
    return either(equal("uint256", 0), positive())


def signed_at_least(minimum: int) -> Condition:
    # Roles compares the ABI word as int256, and int24 fields are sign-extended,
    # so an inclusive tick bound is a strict comparison against its neighbour.
    return comparison(19, encode(("int256",), (minimum - 1,)))


def signed_at_most(maximum: int) -> Condition:
    return comparison(20, encode(("int256",), (maximum + 1,)))


def combine(grants: list[FunctionGrant]) -> tuple[FunctionGrant, ...]:
    groups: dict[tuple, list[FunctionGrant]] = {}
    for grant in grants:
        groups.setdefault((grant.target, grant.selector), []).append(grant)
    result = []
    for key in sorted(groups):
        branches = groups[key]
        if len({g.signature for g in branches}) != 1:
            raise ValueError("ABI selector collision cannot be authorized")
        unique = tuple(dict.fromkeys(g.condition for g in branches))
        if None in unique and unique != (None,):
            raise ValueError("cannot combine argument conditions with selector-only authority")
        conditions = tuple(condition for condition in unique if condition is not None)
        grant = FunctionGrant(
            target=key[0],
            signature=branches[0].signature,
            condition=either(*conditions) if conditions else None,
            explanation=tuple(e for g in branches for e in g.explanation),
        )
        if grant.condition is not None:
            require_compatible(grant.condition, grant.signature)
        _ = grant.conditions
        result.append(grant)
    return tuple(result)


LOGICAL = (1, 2, 3)
ARRAY, CALLDATA, DYNAMIC, ABI_ENCODED = 4, 5, 2, 6


def type_tree(condition: Condition) -> tuple:
    """Roles 2.1 Topology.typeTree: logical nodes take their first child; arrays one element."""
    if condition.operator in LOGICAL:
        return type_tree(condition.children[0])
    children = condition.children[:1] if condition.parameter == ARRAY else condition.children
    return condition.parameter, tuple(type_tree(child) for child in children)


def require_compatible(condition: Condition, signature: str):
    """Integrity._compatibleSiblingTypes, checked before an installation could revert.

    Branches of a logical node and members of an array must share the first one's type tree,
    or be a Dynamic beside an encoded first member.
    """
    if (condition.operator in LOGICAL or condition.parameter == ARRAY) and len(
        condition.children
    ) > 1:
        first = type_tree(condition.children[0])
        for child in condition.children[1:]:
            tree = type_tree(child)
            if tree != first and not (first[0] in (CALLDATA, ABI_ENCODED) and tree[0] == DYNAMIC):
                raise ValueError(f"Roles conditions on {signature} combine incompatible types")
    for child in condition.children:
        require_compatible(child, signature)
