"""VIB-5670 Stage 0 — gateway-backed per-chain balance provider + ExecutionProgress
accounting-pending marker.

Covers the three Stage-0 surfaces added by variant A′ (single-chain-neutral):

(a) ``ExecutionProgress.accounting_pending_step_index`` — round-trips through
    ``to_dict``/``from_dict``, drives ``next_step_to_execute`` to ``i + 1`` (advance
    past a broadcast-confirmed step, never re-broadcast), toggles
    ``is_accounting_pending``, and leaves ``is_stuck`` scoped to
    ``failed_at_step_index``.
(b) ``StrategyRunner._balance_provider_for_chain`` — reuses the primary provider on
    the primary chain, builds + caches a gateway-backed ``GatewayBalanceProvider``
    on a non-primary chain, raises a config error in hosted mode with no gateway,
    and degrades to ``None`` in local mode with no gateway.
(c) recon helpers — ``balance_provider=None`` is neutral (reads
    ``runner.balance_provider``); an explicit provider is used instead.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import almanak.framework.deployment as deployment_mode
from almanak.config.runtime import ConfigurationError
from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.runner_models import ExecutionProgress
from almanak.framework.runner.runner_state import snapshot_balances_for_intent
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

# =============================================================================
# Helpers
# =============================================================================


def _make_progress(**overrides) -> ExecutionProgress:
    base = {
        "execution_id": "exec-1",
        "deployment_id": "deployment:abc123",
        "intents_hash": "hash-1",
        "total_steps": 3,
    }
    base.update(overrides)
    return ExecutionProgress(**base)


def _make_runner(*, balance_provider=None, orchestrator=None, gateway_client=None):
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider or MagicMock(),
        execution_orchestrator=orchestrator or MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )
    if gateway_client is not None:
        runner._gateway_client = gateway_client
    return runner


# =============================================================================
# (a) ExecutionProgress.accounting_pending_step_index
# =============================================================================


class TestExecutionProgressAccountingPending:
    def test_round_trips_through_to_dict_from_dict(self):
        progress = _make_progress(accounting_pending_step_index=1)
        restored = ExecutionProgress.from_dict(progress.to_dict())
        assert restored.accounting_pending_step_index == 1
        assert "accounting_pending_step_index" in progress.to_dict()

    def test_default_is_none_and_backward_compatible_from_dict(self):
        # Legacy serialized rows have no such key — from_dict must tolerate it.
        progress = _make_progress()
        assert progress.accounting_pending_step_index is None
        legacy = progress.to_dict()
        del legacy["accounting_pending_step_index"]
        restored = ExecutionProgress.from_dict(legacy)
        assert restored.accounting_pending_step_index is None

    def test_next_step_advances_past_pending_step(self):
        # Broadcast for step 1 confirmed on-chain → next is step 2 (never re-broadcast 1).
        progress = _make_progress(completed_step_index=0, accounting_pending_step_index=1)
        assert progress.next_step_to_execute == 2

    def test_pending_takes_precedence_over_failed_and_completed(self):
        # The two markers are mutually exclusive per step, but if both are set the
        # accounting-pending (broadcast-confirmed) branch must win — never re-broadcast.
        progress = _make_progress(
            completed_step_index=0,
            failed_at_step_index=1,
            accounting_pending_step_index=1,
        )
        assert progress.next_step_to_execute == 2

    def test_next_step_unaffected_when_pending_is_none(self):
        # Failed branch still applies when there is no accounting-pending marker.
        failed = _make_progress(completed_step_index=0, failed_at_step_index=1)
        assert failed.next_step_to_execute == 1
        # Plain completed branch.
        completed = _make_progress(completed_step_index=1)
        assert completed.next_step_to_execute == 2

    def test_steps_completed_after_pending_are_never_re_executed(self):
        """Audit fix (CodeRabbit): a resume can run steps PAST a still-set
        pending marker (kept for operator replay visibility). A second restart
        must not point back at the already-completed later step — that would
        re-broadcast confirmed money-moves."""
        # pending=0 stamped, restart resumed and completed step 1, restart again:
        progress = _make_progress(completed_step_index=1, accounting_pending_step_index=0)
        assert progress.next_step_to_execute == 2

    def test_later_failed_step_still_re_executes_with_pending_set(self):
        # pending=0 (broadcast-confirmed, never re-run); step 2 genuinely failed
        # pre-broadcast on a later resume → re-execute step 2.
        progress = _make_progress(
            completed_step_index=1,
            failed_at_step_index=2,
            accounting_pending_step_index=0,
        )
        assert progress.next_step_to_execute == 2

    def test_is_accounting_pending_toggles(self):
        assert _make_progress().is_accounting_pending is False
        assert _make_progress(accounting_pending_step_index=0).is_accounting_pending is True

    def test_is_stuck_scoped_to_failed_only(self):
        # accounting-pending must NOT be conflated with a re-executable stuck step.
        assert _make_progress(accounting_pending_step_index=2).is_stuck is False
        assert _make_progress(failed_at_step_index=2).is_stuck is True


# =============================================================================
# (b) StrategyRunner._balance_provider_for_chain
# =============================================================================


class TestBalanceProviderForChain:
    def test_primary_chain_reuses_primary_provider(self):
        primary_bp = MagicMock()
        primary_bp.chain = "arbitrum"
        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        runner = _make_runner(balance_provider=primary_bp, orchestrator=orchestrator)

        # Case-insensitive on the primary chain.
        assert runner._balance_provider_for_chain("Arbitrum") is primary_bp
        assert runner._leg_balance_providers == {}

    def test_non_primary_builds_and_caches_gateway_provider(self):
        primary_bp = MagicMock()
        primary_bp.chain = "arbitrum"
        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        orchestrator.chain_wallets = {"base": "0xBASEwallet"}
        client = MagicMock()
        runner = _make_runner(
            balance_provider=primary_bp, orchestrator=orchestrator, gateway_client=client
        )

        provider = runner._balance_provider_for_chain("base")
        assert isinstance(provider, GatewayBalanceProvider)
        assert provider.chain == "base"
        assert provider.wallet_address == "0xBASEwallet"
        # Cached: a second call returns the very same instance (no rebuild).
        assert runner._balance_provider_for_chain("base") is provider
        assert runner._leg_balance_providers["base"] is provider

    def test_non_primary_wallet_falls_back_to_uniform_wallet(self):
        primary_bp = MagicMock()
        primary_bp.chain = "arbitrum"
        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        orchestrator.chain_wallets = None  # no per-chain registry
        orchestrator.wallet_address = "0xUNIFORM"
        client = MagicMock()
        runner = _make_runner(
            balance_provider=primary_bp, orchestrator=orchestrator, gateway_client=client
        )

        provider = runner._balance_provider_for_chain("base")
        assert provider.wallet_address == "0xUNIFORM"

    def test_hosted_no_gateway_non_primary_raises(self, monkeypatch):
        primary_bp = MagicMock()
        primary_bp.chain = "arbitrum"
        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        # No gateway client set → _get_gateway_client returns None.
        runner = _make_runner(balance_provider=primary_bp, orchestrator=orchestrator)
        monkeypatch.setattr(deployment_mode, "is_hosted", lambda: True)

        with pytest.raises(ConfigurationError) as exc:
            runner._balance_provider_for_chain("base")
        assert "gateway" in str(exc.value).lower()

    def test_local_no_gateway_non_primary_degrades_to_none(self, monkeypatch):
        primary_bp = MagicMock()
        primary_bp.chain = "arbitrum"
        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        runner = _make_runner(balance_provider=primary_bp, orchestrator=orchestrator)
        monkeypatch.setattr(deployment_mode, "is_hosted", lambda: False)

        # Never a false-clean: caller degrades to legacy post-only recon.
        assert runner._balance_provider_for_chain("base") is None
        assert runner._leg_balance_providers == {}


# =============================================================================
# (c) recon helpers — balance_provider threading is single-chain neutral
# =============================================================================


def _recording_provider():
    provider = AsyncMock()
    provider.get_balance = AsyncMock(return_value=SimpleNamespace(balance=Decimal("1")))
    return provider


class TestReconHelperProviderThreading:
    @pytest.mark.asyncio
    async def test_snapshot_defaults_to_runner_balance_provider(self):
        default_bp = _recording_provider()
        explicit_bp = _recording_provider()
        runner = _make_runner(balance_provider=default_bp)
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))

        snap = await snapshot_balances_for_intent(runner, intent, balance_provider=None)

        assert snap is not None
        assert default_bp.get_balance.await_count > 0
        assert explicit_bp.get_balance.await_count == 0

    @pytest.mark.asyncio
    async def test_snapshot_uses_explicit_provider_when_given(self):
        default_bp = _recording_provider()
        explicit_bp = _recording_provider()
        runner = _make_runner(balance_provider=default_bp)
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))

        snap = await snapshot_balances_for_intent(runner, intent, balance_provider=explicit_bp)

        assert snap is not None
        assert explicit_bp.get_balance.await_count > 0
        assert default_bp.get_balance.await_count == 0
