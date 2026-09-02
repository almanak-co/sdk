"""Per-primitive cutover boot guard — VIB-4198 / T12.

Implements the runner-side half of the migration cutover spec
(``docs/internal/migration-cutover-position-registry.md`` §2 boot guard +
§3 idempotent backfill).

Each per-primitive cutover ticket adds an entry to :data:`ACTIVE_CUTOVERS`
declaring the (primitive, cutover_key, BackfillReader) triple. The shared
:func:`enforce_or_run_cutover` helper runs the boot-time guard for that
entry — fetch migration_state, run the backfill if not complete, halt the
runner on any structural inconsistency.

T12 ships UniV3 LP. Future PRs append GMX V2 (T16) / Pendle LP (T23) /
Aave V3 (T28). The shape of the entry is stable and the helper is shared
so each cutover follows the same discipline.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from almanak.framework.migration import (
    BackfillFailedError,
    BackfillReader,
    CutoverStorageNotSupported,
    RegistryBackfillIncompleteError,
    RegistryCutoverNotDeployedError,
)
from almanak.framework.migration.backfill import (
    LendingCutoverReader,
    PendleCutoverReader,
    PerpCutoverReader,
    UniV3LPCutoverReader,
    UniV4LPCutoverReader,
)
from almanak.framework.primitives.types import Primitive

if TYPE_CHECKING:
    from almanak.framework.runner.strategy_runner import StrategyRunner


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CutoverSpec:
    """Static declaration of a per-primitive registry cutover.

    A cutover ticket adds one of these to :data:`ACTIVE_CUTOVERS` to opt
    its primitive into the boot-guard sweep.

    Attributes:
        primitive: Canonical :class:`Primitive` enum member.
        cutover_key: Narrower scope key (matches ``AccountingCategory.value``
            for LP / Pendle LP / etc.).
        reader_factory: Zero-arg constructor for the per-primitive
            :class:`BackfillReader` subclass (the constructor is bound to
            the runner's state_manager at call time, see
            :func:`enforce_or_run_cutover`).
    """

    primitive: Primitive
    cutover_key: str
    reader_factory: Callable[..., BackfillReader]


# Semantically distinct cutovers use separate migration_state rows so their
# completion flags and grouping-policy versions remain independent.
ACTIVE_CUTOVERS: tuple[CutoverSpec, ...] = (
    CutoverSpec(
        primitive=Primitive.LP,
        cutover_key="lp",
        reader_factory=UniV3LPCutoverReader,
    ),
    # V4 LP state is isolated from V3 LP state.
    CutoverSpec(
        primitive=Primitive.LP_V4,
        cutover_key="lp_v4",
        reader_factory=UniV4LPCutoverReader,
    ),
    # Lending protocols share an independent, protocol-agnostic cutover state.
    CutoverSpec(
        primitive=Primitive.LENDING,
        cutover_key="lending",
        reader_factory=LendingCutoverReader,
    ),
    # Perpetual positions have independent cutover state.
    CutoverSpec(
        primitive=Primitive.PERP,
        cutover_key="perp",
        reader_factory=PerpCutoverReader,
    ),
    # Pendle PT and LP share one kind-agnostic, market-keyed cutover state.
    CutoverSpec(
        primitive=Primitive.SWAP,
        cutover_key="pendle",
        reader_factory=PendleCutoverReader,
    ),
)


async def enforce_or_run_cutover(
    *,
    runner: StrategyRunner,
    deployment_id: str,
    primitive: Primitive,
    cutover_key: str,
) -> None:
    """Boot-guard driver for one (primitive, cutover_key) pair.

    Cutover spec §2.2 — three terminal outcomes:

    a. ``complete=1`` → return (registry mode is live for this primitive).
    b. row exists, ``complete=0`` → invoke the per-primitive backfill
       inline. On clean exit, ``complete=1`` is set and the function
       returns. On :class:`BackfillFailedError`, propagate and halt.
    c. row missing → :class:`RegistryCutoverNotDeployedError`.

    The runner caches a per-primitive "complete" flag after first hit
    (frozenset on the runner instance) so subsequent intent-dispatch
    checks are O(1) — a defense-in-depth check inside the per-intent
    dispatch path uses the same cache. T12's runtime registry-mode
    write site checks the cache before calling
    ``save_ledger_and_registry(mode='registry')``.

    Failure semantics: all exceptions propagate. Cutover spec §2.2:
    even paper / dry_run modes halt on backfill failure (stricter than
    VIB-3762's general rule), because a half-finished backfill produces
    a corrupt-by-construction registry state.
    """
    sm = runner.state_manager
    spec = next(
        (s for s in ACTIVE_CUTOVERS if s.primitive == primitive and s.cutover_key == cutover_key),
        None,
    )
    if spec is None:
        # Unknown pairs are programmer errors and must never bypass the guard.
        raise RegistryCutoverNotDeployedError(deployment_id, primitive, cutover_key)

    cache: set[tuple[Primitive, str]] = getattr(runner, "_cutover_complete_cache", set())
    # Unsupported cutover storage keeps registry mode off and preserves the
    # legacy writer; adapters may signal this by omitting the accessor.
    try:
        await sm.upsert_migration_state(
            deployment_id=deployment_id,
            primitive=primitive.value,
            cutover_key=cutover_key,
        )

        state = await sm.get_migration_state(
            deployment_id=deployment_id,
            primitive=primitive.value,
            cutover_key=cutover_key,
        )
    except (CutoverStorageNotSupported, AttributeError) as exc:
        logger.warning(
            "Cutover guard: cutover storage unsupported on this backend "
            "(deployment=%s, primitive=%s, cutover_key=%s, sm=%s): %s. "
            "Registry mode OFF for this build; legacy save_ledger_entry "
            "path remains the live writer. T19/VIB-4205 ships the hosted "
            "equivalent.",
            deployment_id,
            primitive.value,
            cutover_key,
            type(sm).__name__,
            exc,
        )
        runner._cutover_complete_cache = cache  # type: ignore[attr-defined]
        return
    if state is None:
        # A missing row after successful upsert is a writer contract violation.
        raise RegistryCutoverNotDeployedError(deployment_id, primitive, cutover_key)

    if state.position_registry_backfill_complete:
        cache.add((primitive, cutover_key))
        runner._cutover_complete_cache = cache  # type: ignore[attr-defined]
        logger.info(
            "Cutover guard: backfill complete for (%s, %s); registry mode active",
            primitive.value,
            cutover_key,
        )
        return

    reader = spec.reader_factory(state_manager=sm)
    try:
        report = await reader.run(deployment_id=deployment_id)
    except BackfillFailedError:
        raise
    except Exception as exc:
        # Normalize unexpected failures to the canonical cutover failure type.
        raise BackfillFailedError(
            f"Backfill driver loop crashed for (deployment_id={deployment_id!r}, "
            f"primitive={primitive.value!r}, cutover_key={cutover_key!r}): "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    state2 = await sm.get_migration_state(
        deployment_id=deployment_id,
        primitive=primitive.value,
        cutover_key=cutover_key,
    )
    if state2 is None or not state2.position_registry_backfill_complete:
        raise RegistryBackfillIncompleteError(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            rows_synthesized=report.rows_synthesized,
        )
    cache.add((primitive, cutover_key))
    runner._cutover_complete_cache = cache  # type: ignore[attr-defined]


def is_cutover_active(runner: StrategyRunner, primitive: Primitive, cutover_key: str) -> bool:
    """Return True iff the boot guard has cleared the (primitive, cutover_key) pair.

    O(1) check used by per-intent dispatch (defense-in-depth — the boot
    guard ran once at startup; this prevents a hot-flip mid-iteration
    from accidentally bypassing the gate).
    """
    cache: set[tuple[Primitive, str]] = getattr(runner, "_cutover_complete_cache", set())
    return (primitive, cutover_key) in cache
