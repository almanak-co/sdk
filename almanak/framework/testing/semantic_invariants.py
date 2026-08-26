"""Reusable contracts for semantic and boundary-fidelity QA.

Example-based tests prove one spelling and one object shape.  These helpers make
the stronger claim explicit: observations that are economically equivalent must
stay equivalent under representation mutations, and test doubles may not carry
evidence that the production boundary cannot carry.

The module deliberately has no blockchain or pytest dependency.  Unit, intent,
connector, and Quant evidence tests can therefore share the same vocabulary.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import StrEnum
from typing import Any, TypeVar

InputT = TypeVar("InputT")
ObservationT = TypeVar("ObservationT")


class InvariantFamily(StrEnum):
    """Cross-cutting QA laws; ticket numbers are ownership, not taxonomy."""

    IDENTITY = "identity"
    TEARDOWN_ALGEBRA = "teardown_algebra"
    BOUNDARY_FIDELITY = "boundary_fidelity"
    PROJECTION_PARITY = "projection_parity"
    LIFECYCLE_COVERAGE = "lifecycle_coverage"


@dataclass(frozen=True)
class Mutation[InputT]:
    """A named, deterministic mutation of one scientific control."""

    name: str
    value: InputT


@dataclass(frozen=True)
class ObservationDiff[ObservationT]:
    """A mutation whose observation differs from the control."""

    mutation: str
    control: ObservationT
    observed: ObservationT


@dataclass(frozen=True)
class InvariantSpec:
    """Machine-readable coverage row for the foundational invariant matrix."""

    invariant_id: str
    family: InvariantFamily
    positive_controls: tuple[str, ...]
    mutations: tuple[str, ...]
    production_surface: str

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.invariant_id.startswith(f"{self.family.value}."):
            errors.append("invariant_id must be namespaced by its family")
        if not self.positive_controls:
            errors.append("at least one positive control is required")
        if not self.mutations:
            errors.append("at least one discriminating mutation is required")
        if not self.production_surface.strip():
            errors.append("production_surface is required")
        return tuple(errors)


def equivalent_observation_diffs(
    control: InputT,
    mutations: Iterable[Mutation[InputT]],
    observe: Callable[[InputT], ObservationT],
) -> tuple[ObservationDiff[ObservationT], ...]:
    """Return mutations that change an observation expected to be invariant.

    ``observe`` must call the production surface.  The harness intentionally
    does not normalize inputs itself: normalization performed only in the test
    would reproduce the desired implementation and manufacture a false green.
    """

    expected = observe(control)
    diffs: list[ObservationDiff[ObservationT]] = []
    for mutation in mutations:
        actual = observe(mutation.value)
        if actual != expected:
            diffs.append(ObservationDiff(mutation.name, expected, actual))
    return tuple(diffs)


def declared_shape(value_or_type: object) -> frozenset[str]:
    """Fields an authoritative production type promises to carry.

    Dataclass fields are the serialization boundary.  Annotated attributes and
    properties are included for typed non-dataclass result objects.  Incidental
    attributes placed on one instance are deliberately excluded.
    """

    cls = value_or_type if isinstance(value_or_type, type) else type(value_or_type)
    names: set[str] = set()
    if is_dataclass(cls):
        names.update(field.name for field in fields(cls))
    for base in reversed(cls.__mro__):
        names.update(getattr(base, "__annotations__", {}))
        names.update(name for name, member in vars(base).items() if isinstance(member, property))
    return frozenset(names)


def missing_evidence_fields(value_or_type: object, required_fields: Iterable[str]) -> tuple[str, ...]:
    """Evidence required downstream but absent from the producer's type."""

    shape = declared_shape(value_or_type)
    return tuple(sorted(set(required_fields) - shape))


def richer_than_production_fields(test_double: object, production_type: type[Any]) -> tuple[str, ...]:
    """Evidence-like fields a double invents beyond its production boundary.

    Private/mock bookkeeping is ignored.  This is not a blanket ban on helper
    attributes; it detects public evidence that could make a scientific test
    pass although the real result type drops it.
    """

    production = declared_shape(production_type)
    if isinstance(test_double, Mapping):
        candidate = {str(key) for key in test_double}
    else:
        candidate = set(vars(test_double))
    return tuple(sorted(name for name in candidate - production if not name.startswith("_")))


def identity_operations(
    operations: Iterable[object],
    *,
    source: Callable[[object], object | None],
    target: Callable[[object], object | None],
    domain: Callable[[object], object | None],
) -> tuple[object, ...]:
    """Operations that move an asset to itself inside one identity domain.

    The domain callback prevents an unsafe blanket rule: same symbols on two
    chains are not the same asset, while two spellings resolved to the same
    chain/address are.
    """

    matches: list[object] = []
    for operation in operations:
        src = source(operation)
        dst = target(operation)
        identity_domain = domain(operation)
        if src is not None and dst is not None and identity_domain is not None and src == dst:
            matches.append(operation)
    return tuple(matches)


def validate_invariant_catalog(specs: Iterable[InvariantSpec]) -> tuple[str, ...]:
    """Validate uniqueness and scientific controls for a matrix catalog."""

    errors: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        if spec.invariant_id in seen:
            errors.append(f"duplicate invariant_id: {spec.invariant_id}")
        seen.add(spec.invariant_id)
        errors.extend(f"{spec.invariant_id}: {error}" for error in spec.validation_errors())
    return tuple(errors)
