"""Integration test — VIB-4208 / T22 boot guard via GatewayStateManager.

Boots a fresh local SQLite gateway (no pre-seeded ``migration_state``,
no ``position_events``), runs ``enforce_or_run_cutover`` through a
``GatewayStateManager`` wired to an in-process ``StateServiceServicer``,
and asserts:

1. A ``migration_state`` row appears for ``(deployment_id, lp, lp)``.
2. The row's ``position_registry_backfill_complete=1`` after the
   no-op backfill (fresh deployment has zero ``position_events`` to
   migrate, so the backfill loop iterates zero groups and the terminal
   ``MarkBackfillComplete`` flip lands).
3. ``runner._cutover_complete_cache`` carries ``(Primitive.LP, "lp")``.
4. ``is_cutover_active(runner, Primitive.LP, "lp")`` returns True.
5. Re-running the boot guard is idempotent — outcome (a) short-circuits.

This is the D1.S8 / D3.F2 acceptance gate for T22.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import grpc
import pytest
import pytest_asyncio

from almanak.framework.primitives.types import Primitive
from almanak.framework.runner.cutover import enforce_or_run_cutover, is_cutover_active
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.state_service import StateServiceServicer


class _SyntheticRpcError(grpc.RpcError):
    """Emulates the blocking-stub raise-on-non-OK behaviour for the
    in-process direct client.

    A real ``grpc.insecure_channel`` blocking stub raises
    :class:`grpc.RpcError` (with ``.code()`` / ``.details()``) when the
    servicer marks a non-OK status code on its context. The framework's
    ``_translate_unimplemented`` shim depends on that exception path —
    if the stub only returns ``response.success=False``, the translation
    contract is not exercised. CodeRabbit (PR #2230) flagged the gap.
    """

    def __init__(self, status_code: grpc.StatusCode, details: str = "") -> None:
        super().__init__()
        self._status_code = status_code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._status_code

    def details(self) -> str:
        return self._details


class _DirectServiceClient:
    """In-process SYNC gRPC-stub stand-in mirroring the production
    ``grpc.insecure_channel`` blocking-stub surface.

    Drives the async servicer coroutines to completion via a worker
    thread so the call shape matches what GatewayStateManager actually
    sees in production. When the servicer marks a non-OK status code on
    the mock context, the stub synthesizes a :class:`grpc.RpcError`
    matching what a real blocking stub raises — letting the
    ``_translate_unimplemented`` adapter shim be exercised end-to-end
    (CodeRabbit PR #2230).
    """

    def __init__(self, svc: StateServiceServicer) -> None:
        self._svc = svc
        self._ctx = MagicMock(spec=grpc.aio.ServicerContext)
        self._ctx.set_code = MagicMock()
        self._ctx.set_details = MagicMock()

    def _run_sync(self, coro: Any) -> Any:
        import threading

        # Per-call reset so each call's status is observed in isolation.
        # MagicMock holds the LAST call_args, so a prior call's non-OK
        # status would leak into the next call without this reset.
        self._ctx.set_code.reset_mock()
        self._ctx.set_details.reset_mock()

        container: dict[str, Any] = {}

        def _worker() -> None:
            import asyncio

            loop = asyncio.new_event_loop()
            try:
                container["result"] = loop.run_until_complete(coro)
            except Exception as exc:  # noqa: BLE001
                container["error"] = exc
            finally:
                loop.close()

        t = threading.Thread(target=_worker)
        t.start()
        t.join()
        if "error" in container:
            raise container["error"]
        # Promote a non-OK servicer status to RpcError, matching the
        # production blocking-stub contract.
        if self._ctx.set_code.called:
            status = self._ctx.set_code.call_args.args[0]
            if status != grpc.StatusCode.OK:
                details = ""
                if self._ctx.set_details.called:
                    details = self._ctx.set_details.call_args.args[0]
                raise _SyntheticRpcError(status, details)
        return container["result"]

    def UpsertMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.UpsertMigrationState(req, self._ctx))

    def GetMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetMigrationState(req, self._ctx))

    def UpdateMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.UpdateMigrationState(req, self._ctx))

    def MarkBackfillComplete(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.MarkBackfillComplete(req, self._ctx))

    def GetPositionEventsFiltered(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetPositionEventsFiltered(req, self._ctx))

    def GetPositionRegistryOpenRows(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetPositionRegistryOpenRows(req, self._ctx))

    def SaveLedgerAndRegistry(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.SaveLedgerAndRegistry(req, self._ctx))


@pytest_asyncio.fixture
async def gsm_runner(tmp_path) -> Any:
    """Build a runner-shaped object whose ``state_manager`` is a real GSM
    backed by an in-process SQLite-backed state service."""
    settings = GatewaySettings()
    svc = StateServiceServicer(settings)
    sm = StateManager(
        StateManagerConfig(),
        warm_backend=SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "boot_guard.db"))),
    )
    await sm.initialize()
    svc._state_manager = sm
    svc._initialized = True
    svc._snapshot_pool = None

    fake_gateway_client = MagicMock()
    fake_gateway_client.state = _DirectServiceClient(svc)
    gsm = GatewayStateManager(fake_gateway_client)

    runner = SimpleNamespace(state_manager=gsm)
    yield runner
    await sm.close()


@pytest.mark.asyncio
async def test_fresh_deployment_seeds_with_complete_true_via_gateway(gsm_runner):
    """D1.S8 + D3.F2 — fresh deployment boot guard via GSM ends with complete=1.

    Outcome (b) of the cutover spec: no migration_state row exists at
    boot. The guard runs Upsert (creates complete=0 row), then enters
    the BackfillReader. The reader streams position_events with
    position_type='LP' — empty list on a fresh DB — iterates zero
    groups, then calls MarkBackfillComplete. The final Get returns
    complete=1; cache is populated.
    """
    runner = gsm_runner
    await enforce_or_run_cutover(
        runner=runner,
        deployment_id="FreshDep:vib4208",
        primitive=Primitive.LP,
        cutover_key="lp",
    )
    # 1. migration_state row appeared.
    state = await runner.state_manager.get_migration_state(
        deployment_id="FreshDep:vib4208", primitive="lp", cutover_key="lp"
    )
    assert state is not None
    # 2. complete=1 after the no-op backfill.
    assert state.position_registry_backfill_complete is True
    # Counters at 0 since no legacy data existed.
    assert state.rows_synthesized == 0
    assert state.rows_skipped_already_present == 0
    # 3. Cache populated.
    assert (Primitive.LP, "lp") in getattr(runner, "_cutover_complete_cache", set())
    # 4. is_cutover_active returns True.
    assert is_cutover_active(runner, Primitive.LP, "lp") is True


@pytest.mark.asyncio
async def test_subsequent_boot_short_circuits_outcome_a(gsm_runner):
    """D1.S8 — re-running the boot guard is idempotent (outcome (a))."""
    runner = gsm_runner
    # First boot — runs backfill + flips complete=1.
    await enforce_or_run_cutover(
        runner=runner,
        deployment_id="IdempotentDep:vib4208",
        primitive=Primitive.LP,
        cutover_key="lp",
    )
    state_before = await runner.state_manager.get_migration_state(
        deployment_id="IdempotentDep:vib4208", primitive="lp", cutover_key="lp"
    )
    assert state_before is not None
    assert state_before.position_registry_backfill_complete is True
    completed_at_before = state_before.backfill_completed_at
    assert completed_at_before is not None

    # Reset cache so the second call exercises the get_migration_state
    # short-circuit path (outcome (a)) rather than reading the cache.
    runner._cutover_complete_cache = set()

    # Second boot — must short-circuit on complete=1 without re-running
    # the backfill. The completed_at timestamp MUST NOT change (proves
    # the no-op behaviour).
    await enforce_or_run_cutover(
        runner=runner,
        deployment_id="IdempotentDep:vib4208",
        primitive=Primitive.LP,
        cutover_key="lp",
    )
    state_after = await runner.state_manager.get_migration_state(
        deployment_id="IdempotentDep:vib4208", primitive="lp", cutover_key="lp"
    )
    assert state_after is not None
    assert state_after.position_registry_backfill_complete is True
    assert state_after.backfill_completed_at == completed_at_before
    # Cache repopulated.
    assert is_cutover_active(runner, Primitive.LP, "lp") is True


@pytest.mark.asyncio
async def test_postgres_backend_degrades_via_unimplemented(tmp_path):
    """D3.F1 — A GSM pointed at a Postgres-shaped servicer raises
    CutoverStorageNotSupported (translated from gRPC UNIMPLEMENTED).
    The boot guard catches it and degrades cleanly (cache stays empty,
    is_cutover_active returns False, runner does NOT crash)."""
    settings = GatewaySettings()
    svc = StateServiceServicer(settings)
    svc._snapshot_pool = object()  # Postgres flag — every handler returns UNIMPLEMENTED
    svc._initialized = True

    fake_gateway_client = MagicMock()
    fake_gateway_client.state = _DirectServiceClient(svc)
    gsm = GatewayStateManager(fake_gateway_client)
    runner = SimpleNamespace(state_manager=gsm)

    await enforce_or_run_cutover(
        runner=runner,
        deployment_id="PgDep:vib4208",
        primitive=Primitive.LP,
        cutover_key="lp",
    )
    # Degrade contract: cache stays empty (proves the boot guard caught
    # CutoverStorageNotSupported BEFORE populating cache); is_cutover_active
    # therefore False. Asserting both sides ensures a regression that
    # populates the cache despite the degrade still fails this test
    # (CodeRabbit PR #2230).
    assert getattr(runner, "_cutover_complete_cache", set()) == set()
    assert is_cutover_active(runner, Primitive.LP, "lp") is False
