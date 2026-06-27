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


# T12 (VIB-4198): UniV3 LP is the proof-case cutover. Subsequent cutovers
# (T16 perp, T23 Pendle LP, T28 Aave) append entries here.
#
# VIB-4583: UniV4 LP is its own isolated cutover (Primitive.LP_V4 / 'lp_v4').
# It is tracked by a SEPARATE migration_state row from the V3 'lp' cutover so
# their backfill-complete flags and grouping-policy versions stay independent.
#
# TD-04 (VIB-5462): LENDING (Aave canonical) is its own isolated cutover
# (Primitive.LENDING / 'lending'). The registry row shape (market_id + leg +
# protocol) is protocol-agnostic, so enabling Spark / Fluid / Morpho / Compound
# is a thin add to ``_LENDING_REGISTRY_PROTOCOLS`` in migration/backfill.py — no
# new entry here. Kept minimal and self-contained so the parallel GMX/Pendle
# cutover tickets (TD-02/TD-03) append cleanly after.
ACTIVE_CUTOVERS: tuple[CutoverSpec, ...] = (
    CutoverSpec(
        primitive=Primitive.LP,
        cutover_key="lp",
        reader_factory=UniV3LPCutoverReader,
    ),
    CutoverSpec(
        primitive=Primitive.LP_V4,
        cutover_key="lp_v4",
        reader_factory=UniV4LPCutoverReader,
    ),
    CutoverSpec(
        primitive=Primitive.LENDING,
        cutover_key="lending",
        reader_factory=LendingCutoverReader,
    ),
    # TD-02 (VIB-5460): PERP (GMX V2 canonical) is its own isolated cutover
    # (Primitive.PERP / 'perp'). The registry row shape (venue position_key anchor
    # + market/collateral/direction/size payload) is protocol-agnostic, so
    # enabling another GMX-shape perp venue is a thin add to the GMX_V2_PERP
    # protocol family — no new entry here. Kept minimal and self-contained so the
    # parallel Pendle cutover ticket (TD-03) appends cleanly after.
    CutoverSpec(
        primitive=Primitive.PERP,
        cutover_key="perp",
        reader_factory=PerpCutoverReader,
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
    # Late import — runner module + cutover-spec ordering would otherwise
    # cause a circular at module-load time. The factory takes the bound
    # state manager.
    spec = next(
        (s for s in ACTIVE_CUTOVERS if s.primitive == primitive and s.cutover_key == cutover_key),
        None,
    )
    if spec is None:
        # Defensive: caller passed an (primitive, cutover_key) pair not in
        # ACTIVE_CUTOVERS. We treat this as a programmer error rather than
        # silently skipping — the boot guard's purpose is to prevent
        # routing on a half-deployed cutover.
        raise RegistryCutoverNotDeployedError(deployment_id, primitive, cutover_key)

    # Ensure the row exists before reading it. The cutover spec calls for
    # the cutover ticket to create the row at deploy time; in this PR we
    # create it lazily on first runner start (functionally equivalent for
    # local SDK + Tier-1 hosted gateway boot).
    #
    # Audit M3 (CodeRabbit): the state-manager surface now raises
    # ``CutoverStorageNotSupported`` (instead of silently returning
    # ``None`` / ``[]`` / no-op) on backends that don't implement the
    # cutover accessors. The boot guard is the canonical place to
    # decide degrade vs hard refusal:
    #
    # - Local SQLite implements the full surface → no exception, the
    #   normal outcome a/b/c path applies.
    # - GatewayStateManager (hosted, Postgres) does not implement
    #   migration_state until T19/VIB-4205 ships → catch
    #   ``CutoverStorageNotSupported`` and degrade controlled-ly:
    #   registry mode stays OFF, the runner runs the legacy
    #   ``save_ledger_entry`` path. Cutover spec §2.4 explicitly
    #   sanctions this pre-T19.
    cache: set[tuple[Primitive, str]] = getattr(runner, "_cutover_complete_cache", set())
    # Audit M3: the canonical "this backend doesn't support cutover storage"
    # signal is :class:`CutoverStorageNotSupported`. ``AttributeError`` is
    # also accepted here as a controlled-degrade trigger because some
    # state-manager surfaces (test mocks, third-party adapters that
    # don't subclass ``StateManager``) may not even define the method.
    # Either is treated as "registry mode OFF for this build" — never
    # silent default.
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
        # Outcome (c): row genuinely missing AFTER upsert succeeded.
        # That's a programmer error in the writer (or a race on a
        # concurrent delete); halt loud rather than silently degrade.
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

    # Outcome (b): row exists, complete=0 → run the backfill inline.
    reader = spec.reader_factory(state_manager=sm)
    try:
        report = await reader.run(deployment_id=deployment_id)
    except BackfillFailedError:
        raise
    except Exception as exc:
        # Wrap every other exception so the runner sees the canonical
        # failure type per cutover spec §2.2 / §3.3.
        raise BackfillFailedError(
            f"Backfill driver loop crashed for (deployment_id={deployment_id!r}, "
            f"primitive={primitive.value!r}, cutover_key={cutover_key!r}): "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    # Re-fetch — the writer set complete=1 just before returning.
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
