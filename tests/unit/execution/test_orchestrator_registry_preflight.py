"""VIB-4614 — pre-execution LP registry-collision preflight.

Proves the orchestrator's ``_phase_registry_preflight`` rejects a second
auto-mode (handle-less) LP_OPEN into a pool that already has an open auto-mode
registry row BEFORE any signing / submission — so no orphan NFT is minted.

Three layers:
1. The orchestrator phase in isolation (callback returns reject / allow / None).
2. The runner-injected callback (``build_registry_preflight_check``) over a real
   in-memory SQLite StateManager + position_registry row.
3. End-to-end through ``ExecutionOrchestrator.execute`` — the second open
   short-circuits at VALIDATION, never reaching sign/submit.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution._pipeline_state import ExecutionPipelineState
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

_POOL = "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443"
_CHAIN = "arbitrum"


def _orchestrator(registry_preflight=None) -> ExecutionOrchestrator:
    signer = MagicMock()
    signer.address = "0x1234567890abcdef1234567890abcdef12345678"
    return ExecutionOrchestrator(
        signer=signer,
        submitter=MagicMock(),
        simulator=MagicMock(),
        chain=_CHAIN,
        registry_preflight=registry_preflight,
    )


def _lp_open_state(orch: ExecutionOrchestrator, *, registry_handle=None) -> ExecutionPipelineState:
    bundle = ActionBundle(
        intent_type="LP_OPEN",
        transactions=[{"to": "0x00", "data": "0x", "value": 0}],
        metadata={
            "pool": _POOL,
            "chain": _CHAIN,
            "protocol": "uniswap_v3",
            "registry_handle": registry_handle,
        },
    )
    context = ExecutionContext(
        deployment_id="DoubleLpOpenReproStrategy:abc123",
        intent_id="i1",
        chain=_CHAIN,
        wallet_address=orch.signer.address,
    )
    result = ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION, correlation_id=context.correlation_id)
    return ExecutionPipelineState(action_bundle=bundle, context=context, result=result)


# =============================================================================
# Layer 1 — phase in isolation
# =============================================================================


class TestPhaseInIsolation:
    @pytest.mark.asyncio
    async def test_no_callback_is_noop(self):
        orch = _orchestrator(registry_preflight=None)
        state = _lp_open_state(orch)
        assert await orch._phase_registry_preflight(state) is None

    @pytest.mark.asyncio
    async def test_callback_allows_returns_none(self):
        orch = _orchestrator(registry_preflight=AsyncMock(return_value=None))
        state = _lp_open_state(orch)
        assert await orch._phase_registry_preflight(state) is None

    @pytest.mark.asyncio
    async def test_callback_rejects_short_circuits_at_validation(self):
        orch = _orchestrator(registry_preflight=AsyncMock(return_value="collision: pool already open"))
        state = _lp_open_state(orch)

        early = await orch._phase_registry_preflight(state)

        assert early is not None
        assert early.success is False
        assert early.error_phase == ExecutionPhase.VALIDATION
        assert "Registry preflight blocked" in (early.error or "")
        assert "collision: pool already open" in (early.error or "")

    @pytest.mark.asyncio
    async def test_callback_exception_fails_open(self):
        orch = _orchestrator(registry_preflight=AsyncMock(side_effect=RuntimeError("db down")))
        state = _lp_open_state(orch)
        # Fail-open: the commit-path unique index is the backstop.
        assert await orch._phase_registry_preflight(state) is None

    @pytest.mark.asyncio
    async def test_phase_is_in_pipeline_between_build_and_validate(self):
        orch = _orchestrator()
        # The phase ordering is a contract: preflight must run after build (so
        # we know it is a real open) and before validate/sign.
        import inspect

        src = inspect.getsource(orch.execute)
        i_build = src.index("_phase_build")
        i_pre = src.index("_phase_registry_preflight")
        i_validate = src.index("_phase_validate")
        assert i_build < i_pre < i_validate


# =============================================================================
# Layer 2 — runner-injected callback over a real SQLite StateManager
# =============================================================================


async def _state_manager_with_open_lp_row(*, deployment_id: str, handle=None):
    """Return a StateManager whose position_registry has one open UniV3 LP row."""
    from almanak.framework.migration import semantic_grouping_key_univ3
    from almanak.framework.state.state_manager import (
        StateManager,
        StateManagerConfig,
        WarmBackendType,
    )

    config = StateManagerConfig(warm_backend=WarmBackendType.SQLITE)
    config.sqlite_config.db_path = ":memory:"
    sm = StateManager(config)
    await sm.initialize()

    sgk = semantic_grouping_key_univ3(chain=_CHAIN, pool_address=_POOL)
    # Insert directly via the warm backend connection (the test seeds the prior
    # open position the second open would collide with).
    warm = sm._warm
    warm._conn.execute(
        """
        INSERT INTO position_registry (
            deployment_id, chain, primitive, accounting_category,
            physical_identity_hash, semantic_grouping_key, grouping_policy_version,
            handle, status, payload, matching_policy_version
        ) VALUES (?, ?, 'lp', 'lp', ?, ?, 'v1', ?, 'open', '{}', 1)
        """,
        (deployment_id, _CHAIN, "0xpih_existing", sgk, handle),
    )
    warm._conn.commit()
    return sm


class TestRunnerInjectedCallback:
    @pytest.mark.asyncio
    async def test_second_auto_mode_open_is_rejected(self):
        from almanak.framework.accounting.registry_preflight import (
            build_registry_preflight_check,
        )

        deployment_id = "DoubleLpOpenReproStrategy:abc123"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        check = build_registry_preflight_check(sm, deployment_id)

        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00"}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        reason = await check(bundle)
        assert reason is not None
        assert "would collide" in reason
        assert "0xpih_existing" in reason
        await sm.close()

    @pytest.mark.asyncio
    async def test_handle_supplied_open_is_allowed(self):
        from almanak.framework.accounting.registry_preflight import (
            build_registry_preflight_check,
        )

        deployment_id = "S:1"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        check = build_registry_preflight_check(sm, deployment_id)

        # A handle-supplied open is excluded from ix_registry_auto_mode → allow.
        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00"}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": "leg_b"},
        )
        assert await check(bundle) is None
        await sm.close()

    @pytest.mark.asyncio
    async def test_no_existing_row_is_allowed(self):
        from almanak.framework.accounting.registry_preflight import (
            build_registry_preflight_check,
        )
        from almanak.framework.state.state_manager import (
            StateManager,
            StateManagerConfig,
            WarmBackendType,
        )

        deployment_id = "S:2"
        config = StateManagerConfig(warm_backend=WarmBackendType.SQLITE)
        config.sqlite_config.db_path = ":memory:"
        sm = StateManager(config)
        await sm.initialize()
        check = build_registry_preflight_check(sm, deployment_id)

        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00"}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        assert await check(bundle) is None
        await sm.close()

    @pytest.mark.asyncio
    async def test_non_lp_open_is_allowed(self):
        from almanak.framework.accounting.registry_preflight import (
            build_registry_preflight_check,
        )

        deployment_id = "S:3"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        check = build_registry_preflight_check(sm, deployment_id)

        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x00"}], metadata={"chain": _CHAIN})
        assert await check(bundle) is None
        await sm.close()


# =============================================================================
# Layer 3 — end-to-end: second open never reaches sign/submit
# =============================================================================


class TestEndToEndNoMint:
    @pytest.mark.asyncio
    async def test_execute_blocks_before_sign_when_collision(self):
        from almanak.framework.accounting.registry_preflight import (
            build_registry_preflight_check,
        )

        deployment_id = "DoubleLpOpenReproStrategy:e2e"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        orch = _orchestrator(registry_preflight=build_registry_preflight_check(sm, deployment_id))

        # Spy on the sign/submit phases — they MUST NOT run on a blocked open.
        orch._phase_sign = AsyncMock()  # type: ignore[method-assign]
        orch._phase_submit_and_confirm = AsyncMock()  # type: ignore[method-assign]
        orch._check_token_balance_before_submit = AsyncMock()  # type: ignore[method-assign]

        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00", "data": "0x", "value": 0}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        context = ExecutionContext(
            deployment_id=deployment_id,
            intent_id="i2",
            chain=_CHAIN,
            wallet_address=orch.signer.address,
        )

        result = await orch.execute(bundle, context)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.VALIDATION
        orch._phase_sign.assert_not_awaited()
        orch._phase_submit_and_confirm.assert_not_awaited()
        await sm.close()


# =============================================================================
# Layer 4 — boot-time wiring: _install_registry_preflight() installs the hook
# =============================================================================


class TestBootWiringInstallsPreflight:
    """Guards the runner boot hook (``_install_registry_preflight``) — without
    this, a regression that stops installing the preflight would be invisible
    to the isolated-callback / phase tests above (which inject the callback by
    hand). These exercise the REAL installation path on a real
    ``ExecutionOrchestrator`` + real StateManager.
    """

    @pytest.mark.asyncio
    async def test_install_wires_callback_onto_direct_orchestrator(self):
        from almanak.framework.runner._run_loop_helpers import _install_registry_preflight

        deployment_id = "S:install"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        orch = _orchestrator(registry_preflight=None)
        assert orch.registry_preflight is None  # not wired yet

        runner = MagicMock()
        runner.execution_orchestrator = orch
        runner.state_manager = sm

        _install_registry_preflight(runner, deployment_id)

        # The hook installed a real callable.
        assert orch.registry_preflight is not None
        await sm.close()

    @pytest.mark.asyncio
    async def test_install_rebinds_existing_callback_to_current_deployment(self):
        """The hook ALWAYS rebinds (no no-clobber early-return). Re-entry with
        the SAME deployment rebuilds the callback (behaviorally equivalent —
        still blocks a collision for that deployment), not keeps the old one.
        Guards against the stale-closure footgun (VIB-4614 CodeRabbit Major).
        """
        from almanak.framework.runner._run_loop_helpers import _install_registry_preflight

        deployment_id = "S:rebind"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        existing = AsyncMock(return_value=None)
        orch = _orchestrator(registry_preflight=existing)
        runner = MagicMock()
        runner.execution_orchestrator = orch
        runner.state_manager = sm

        _install_registry_preflight(runner, deployment_id)

        # Rebound to a fresh callable (NOT the old one).
        assert orch.registry_preflight is not existing
        # Behaviorally correct for this deployment: still blocks a collision.
        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00"}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        assert await orch.registry_preflight(bundle) is not None
        await sm.close()

    @pytest.mark.asyncio
    async def test_install_rebind_to_different_deployment_drops_stale_closure(self):
        """Re-installing on the SAME orchestrator for a DIFFERENT deployment
        must check the NEW deployment's registry — proving the stale-closure
        bug is gone. An open auto-mode row under the OLD deployment must NOT
        block an LP_OPEN under the NEW deployment; a row under the NEW one DOES.
        """
        from almanak.framework.migration import semantic_grouping_key_univ3
        from almanak.framework.runner._run_loop_helpers import _install_registry_preflight

        old_deployment = "OldStrat:1"
        new_deployment = "NewStrat:1"
        # StateManager seeded with an open auto-mode row under the OLD deployment only.
        sm = await _state_manager_with_open_lp_row(deployment_id=old_deployment, handle=None)

        orch = _orchestrator(registry_preflight=None)
        runner = MagicMock()
        runner.execution_orchestrator = orch
        runner.state_manager = sm

        # 1) Install for OLD deployment — its open row blocks.
        _install_registry_preflight(runner, old_deployment)
        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00"}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        assert await orch.registry_preflight(bundle) is not None  # OLD blocks

        # 2) Reuse the SAME orchestrator for a DIFFERENT deployment.
        _install_registry_preflight(runner, new_deployment)
        # The OLD deployment's row must NOT leak through a stale closure —
        # the NEW deployment has no open row, so the open is ALLOWED.
        assert await orch.registry_preflight(bundle) is None

        # 3) Seed an open row under the NEW deployment → now it blocks.
        sgk = semantic_grouping_key_univ3(chain=_CHAIN, pool_address=_POOL)
        sm._warm._conn.execute(
            """
            INSERT INTO position_registry (
                deployment_id, chain, primitive, accounting_category,
                physical_identity_hash, semantic_grouping_key, grouping_policy_version,
                handle, status, payload, matching_policy_version
            ) VALUES (?, ?, 'lp', 'lp', '0xpih_new', ?, 'v1', NULL, 'open', '{}', 1)
            """,
            (new_deployment, _CHAIN, sgk),
        )
        sm._warm._conn.commit()
        assert await orch.registry_preflight(bundle) is not None  # NEW now blocks
        await sm.close()

    @pytest.mark.asyncio
    async def test_install_is_noop_when_orchestrator_lacks_hook(self):
        """Gateway-routed / multi-chain orchestrators have no registry_preflight
        attribute — the hook must be a safe no-op (no AttributeError)."""
        from almanak.framework.runner._run_loop_helpers import _install_registry_preflight

        class _NoHookOrchestrator:
            pass

        runner = MagicMock()
        runner.execution_orchestrator = _NoHookOrchestrator()
        runner.state_manager = MagicMock()

        # Must not raise and must not add the attribute.
        _install_registry_preflight(runner, "S:nohook")
        assert not hasattr(runner.execution_orchestrator, "registry_preflight")

    @pytest.mark.asyncio
    async def test_boot_wired_preflight_blocks_collision_end_to_end(self):
        """After the real boot hook installs the preflight, a 2nd auto-mode
        LP_OPEN into an already-open pool is blocked end-to-end — proving the
        installed callback (not a hand-injected one) does the real work."""
        from almanak.framework.runner._run_loop_helpers import _install_registry_preflight

        deployment_id = "DoubleLpOpenReproStrategy:boot"
        sm = await _state_manager_with_open_lp_row(deployment_id=deployment_id, handle=None)
        orch = _orchestrator(registry_preflight=None)
        orch._phase_sign = AsyncMock()  # type: ignore[method-assign]
        orch._phase_submit_and_confirm = AsyncMock()  # type: ignore[method-assign]
        orch._check_token_balance_before_submit = AsyncMock()  # type: ignore[method-assign]

        runner = MagicMock()
        runner.execution_orchestrator = orch
        runner.state_manager = sm
        _install_registry_preflight(runner, deployment_id)

        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"to": "0x00", "data": "0x", "value": 0}],
            metadata={"pool": _POOL, "chain": _CHAIN, "protocol": "uniswap_v3", "registry_handle": None},
        )
        context = ExecutionContext(
            deployment_id=deployment_id,
            intent_id="boot-i2",
            chain=_CHAIN,
            wallet_address=orch.signer.address,
        )

        result = await orch.execute(bundle, context)

        assert result.success is False
        assert result.error_phase == ExecutionPhase.VALIDATION
        orch._phase_sign.assert_not_awaited()
        orch._phase_submit_and_confirm.assert_not_awaited()
        await sm.close()
