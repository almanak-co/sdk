"""Boot-guard tests for the UniV3 LP cutover (VIB-4198 / T12).

Validates ``almanak.framework.runner.cutover.enforce_or_run_cutover``
behavior per migration cutover spec §2.2:

- Outcome (a): complete=1 → cache + return.
- Outcome (b): row exists, complete=0 → run inline backfill, then flag
  flips to 1.
- Outcome (c): no warm backend / state-manager support → degrade
  gracefully (registry mode OFF for this build, accounting_only stays).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.migration.backfill import BackfillFailedError
from almanak.framework.primitives.types import Primitive
from almanak.framework.runner.cutover import (
    ACTIVE_CUTOVERS,
    enforce_or_run_cutover,
    is_cutover_active,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig


def _make_runner(tmp_path: Path) -> SimpleNamespace:
    sm = StateManager(
        StateManagerConfig(),
        warm_backend=SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "guard.db"))),
    )
    return SimpleNamespace(state_manager=sm)


@pytest.mark.asyncio
async def test_active_cutovers_includes_lp(tmp_path) -> None:
    """ACTIVE_CUTOVERS contains exactly the V3 LP + V4 LP + lending + perp entries.

    T12 shipped UniV3 LP (``Primitive.LP`` / ``'lp'``). VIB-4583 adds the
    isolated UniV4 LP cutover (``Primitive.LP_V4`` / ``'lp_v4'``). TD-04 /
    VIB-5462 adds the lending cutover (``Primitive.LENDING`` / ``'lending'``;
    Aave canonical). TD-02 / VIB-5460 adds the perp cutover (``Primitive.PERP`` /
    ``'perp'``; GMX V2 canonical). TD-03 / VIB-5461 adds the Pendle PT+LP cutover
    keyed on the otherwise-empty swap-primitive partition (``Primitive.SWAP`` /
    ``'pendle'``). The boot loop iterates every entry, so this pins the exact set
    so a future PR can't slip an extra entry in (a primitive whose backfill
    reader / writer aren't yet integrated) without explicit test churn.
    """
    pairs = {(s.primitive, s.cutover_key) for s in ACTIVE_CUTOVERS}
    assert pairs == {
        (Primitive.LP, "lp"),
        (Primitive.LP_V4, "lp_v4"),
        (Primitive.LENDING, "lending"),
        (Primitive.PERP, "perp"),
        (Primitive.SWAP, "pendle"),
    }, f"unexpected ACTIVE_CUTOVERS: {[(s.primitive.value, s.cutover_key) for s in ACTIVE_CUTOVERS]}"


@pytest.mark.asyncio
async def test_enforce_runs_backfill_on_first_call_outcome_b(tmp_path) -> None:
    """Outcome (b): no migration_state row exists → upsert + run backfill +
    cache the cleared cutover."""
    runner = _make_runner(tmp_path)
    await runner.state_manager.initialize()
    try:
        await enforce_or_run_cutover(
            runner=runner,
            deployment_id="DepBoot:1",
            primitive=Primitive.LP,
            cutover_key="lp",
        )
        # Cache populated.
        assert is_cutover_active(runner, Primitive.LP, "lp")
        # Migration state flag flipped.
        state = await runner.state_manager.get_migration_state(
            deployment_id="DepBoot:1", primitive="lp", cutover_key="lp"
        )
        assert state is not None
        assert state.position_registry_backfill_complete is True
    finally:
        await runner.state_manager.close()


@pytest.mark.asyncio
async def test_enforce_short_circuits_on_complete_already_outcome_a(tmp_path) -> None:
    """Outcome (a): complete=1 → return without running backfill again."""
    runner = _make_runner(tmp_path)
    await runner.state_manager.initialize()
    try:
        # First call lands the row + flips complete=1.
        await enforce_or_run_cutover(
            runner=runner,
            deployment_id="DepBoot:2",
            primitive=Primitive.LP,
            cutover_key="lp",
        )
        # Second call should short-circuit. If the backfill ran a second
        # time, it would still be a no-op (DO NOTHING), but the outcome (a)
        # branch in the guard returns BEFORE invoking the reader. We
        # observe behavior by patching the reader factory to raise — if
        # outcome (a) is taken, the patch is never hit.
        async def boom(*args, **kwargs):
            raise RuntimeError("backfill must NOT run on complete=1")

        # Replace the get_position_events_filtered method to ensure
        # outcome (a) skips the reader entirely.
        runner.state_manager.get_position_events_filtered = boom  # type: ignore[assignment]

        # Should NOT raise.
        await enforce_or_run_cutover(
            runner=runner,
            deployment_id="DepBoot:2",
            primitive=Primitive.LP,
            cutover_key="lp",
        )
        assert is_cutover_active(runner, Primitive.LP, "lp")
    finally:
        await runner.state_manager.close()


@pytest.mark.asyncio
async def test_enforce_propagates_BackfillFailedError(tmp_path) -> None:
    """When the backfill driver loop raises, the guard propagates
    ``BackfillFailedError`` and does NOT cache the cutover as cleared."""
    runner = _make_runner(tmp_path)
    await runner.state_manager.initialize()
    try:
        # Sabotage the read so the backfill driver crashes.
        async def boom(*args, **kwargs):
            raise RuntimeError("simulated crash")

        runner.state_manager.get_position_events_filtered = boom  # type: ignore[assignment]

        with pytest.raises(BackfillFailedError):
            await enforce_or_run_cutover(
                runner=runner,
                deployment_id="DepBoot:3",
                primitive=Primitive.LP,
                cutover_key="lp",
            )
        assert not is_cutover_active(runner, Primitive.LP, "lp")
    finally:
        await runner.state_manager.close()


@pytest.mark.asyncio
async def test_enforce_degrades_on_unsupported_warm_backend() -> None:
    """Audit M3 (CodeRabbit): when the WARM backend explicitly raises
    ``CutoverStorageNotSupported`` (the canonical signal that this
    backend does not yet ship cutover storage — e.g. GatewayStateManager
    until T19), the guard leaves ``is_cutover_active`` False and the
    runner stays on the legacy ``save_ledger_entry`` path.

    The previous test's silent-``None``-returning stub is no longer a
    valid degraded-backend simulator: a backend that is missing
    cutover-storage support must say so explicitly via
    ``CutoverStorageNotSupported``, not via ``None``. Returning ``None``
    after a successful upsert is now a programmer-error case
    (``RegistryCutoverNotDeployedError``), not a degraded backend.
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    runner = SimpleNamespace(state_manager=MagicMock(spec=[]))

    class _StubSMUnsupported:
        async def upsert_migration_state(self, **kwargs):
            raise CutoverStorageNotSupported(
                "stub backend: cutover storage not implemented"
            )

        async def get_migration_state(self, **kwargs):
            raise CutoverStorageNotSupported(
                "stub backend: cutover storage not implemented"
            )

        async def initialize(self):
            return None

    runner.state_manager = _StubSMUnsupported()

    await enforce_or_run_cutover(
        runner=runner,
        deployment_id="DepStub:1",
        primitive=Primitive.LP,
        cutover_key="lp",
    )
    assert not is_cutover_active(runner, Primitive.LP, "lp")


@pytest.mark.asyncio
async def test_enforce_raises_when_state_none_after_upsert() -> None:
    """Audit M3 (CodeRabbit): if ``upsert_migration_state`` succeeds but
    ``get_migration_state`` returns ``None``, that's a writer programmer-
    error or a concurrent-delete race. The guard halts loud with
    ``RegistryCutoverNotDeployedError`` — silent degrade would let the
    runner continue with ``is_cutover_active = False`` even though the
    state-manager surface IS implemented (just buggy).
    """
    from almanak.framework.migration.backfill import (
        RegistryCutoverNotDeployedError,
    )

    runner = SimpleNamespace(state_manager=MagicMock(spec=[]))

    class _StubSMBuggy:
        async def upsert_migration_state(self, **kwargs):
            return None  # claims the row exists

        async def get_migration_state(self, **kwargs):
            return None  # but read returns None — buggy / racy

        async def initialize(self):
            return None

    runner.state_manager = _StubSMBuggy()

    with pytest.raises(RegistryCutoverNotDeployedError):
        await enforce_or_run_cutover(
            runner=runner,
            deployment_id="DepStub:2",
            primitive=Primitive.LP,
            cutover_key="lp",
        )
    assert not is_cutover_active(runner, Primitive.LP, "lp")
