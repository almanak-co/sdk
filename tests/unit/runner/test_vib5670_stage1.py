"""VIB-5670 Stage 1 — explicit chain/wallet threading + per-leg accounting helpers.

Covers the two Stage-1 deliverables on ``StrategyRunner``:

1. Neutrality of the additive ``chain`` / ``wallet_address`` params on the shared
   accounting sub-functions: with ``None`` (the single-chain default) each
   function reproduces today's ``strategy``/``config``-derived value; with an
   explicit value it threads that value into the ledger / outbox / position-event
   rows.
2. The new multi-chain lane helpers ``_adapt_leg_to_execution_result`` (both leg
   shapes) and ``_persist_executed_leg`` (isolated: ledger + outbox with per-leg
   chain/wallet, exactly-once notify with the enriched result, recon skipped when
   no pre-snapshot, and a settlement-degraded leg stamped + no user success).

These helpers are NOT yet wired into ``_execute_multi_chain`` (Stage 2/3); the
tests exercise them directly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

# =============================================================================
# Helpers
# =============================================================================


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    state_mgr = AsyncMock()
    state_mgr.get_accounting_events_sync = MagicMock(return_value=[])
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    runner._accounting_processor = MagicMock()
    runner._accounting_processor.drain_one = AsyncMock()
    runner._accounting_processor._deployment_id = ""
    # Default: non-live so persistence failures don't raise in these unit tests.
    runner._is_live_mode = MagicMock(return_value=False)  # type: ignore[method-assign]
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xstrategywallet"
    return strategy


def _make_swap_intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = "SWAP"
    intent.protocol = "uniswap_v3"
    return intent


# =============================================================================
# 1. Neutrality + explicit threading of the shared sub-functions
# =============================================================================


class TestWriteLedgerEntryChainWalletThreading:
    """``_write_ledger_entry`` threads eff-chain/eff-wallet to its callees."""

    async def _run(self, *, chain, wallet_address):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        result = MagicMock(success=True)

        # Spy the callees that receive the effective chain/wallet.
        runner._maybe_enrich_result_with_runner_hooks = MagicMock()  # type: ignore[method-assign]
        runner._maybe_save_ledger_with_registry = AsyncMock(return_value=False)  # type: ignore[method-assign]
        runner._emit_position_event_for_intent = AsyncMock()  # type: ignore[method-assign]

        entry = MagicMock()
        entry.id = "entry-1"
        with (
            patch(
                "almanak.framework.observability.ledger.build_ledger_entry",
                return_value=entry,
            ) as build_mock,
            patch("almanak.framework.observability.context.get_cycle_id", return_value="cyc"),
        ):
            await runner._write_ledger_entry(
                strategy,
                intent,
                result=result,
                success=True,
                chain=chain,
                wallet_address=wallet_address,
            )
        return runner, build_mock

    @pytest.mark.asyncio
    async def test_none_defaults_reproduce_strategy_values(self):
        runner, build_mock = await self._run(chain=None, wallet_address=None)

        # build_ledger_entry gets the strategy-derived chain.
        assert build_mock.call_args.kwargs["chain"] == "arbitrum"
        # enrichment hook gets (result, eff_chain) + wallet kwarg.
        enrich_args = runner._maybe_enrich_result_with_runner_hooks.call_args
        assert enrich_args.args[1] == "arbitrum"
        assert enrich_args.kwargs["wallet_address"] == "0xstrategywallet"
        # position event gets eff chain/wallet.
        emit_kwargs = runner._emit_position_event_for_intent.call_args.kwargs
        assert emit_kwargs["chain"] == "arbitrum"
        assert emit_kwargs["wallet_address"] == "0xstrategywallet"
        # registry dispatch gets the raw (None) params — it owns its own fallback.
        reg_kwargs = runner._maybe_save_ledger_with_registry.call_args.kwargs
        assert reg_kwargs["chain"] is None
        assert reg_kwargs["wallet_address"] is None

    @pytest.mark.asyncio
    async def test_explicit_values_thread_through(self):
        runner, build_mock = await self._run(chain="polygon", wallet_address="0xleg")

        assert build_mock.call_args.kwargs["chain"] == "polygon"
        enrich_args = runner._maybe_enrich_result_with_runner_hooks.call_args
        assert enrich_args.args[1] == "polygon"
        assert enrich_args.kwargs["wallet_address"] == "0xleg"
        emit_kwargs = runner._emit_position_event_for_intent.call_args.kwargs
        assert emit_kwargs["chain"] == "polygon"
        assert emit_kwargs["wallet_address"] == "0xleg"
        reg_kwargs = runner._maybe_save_ledger_with_registry.call_args.kwargs
        assert reg_kwargs["chain"] == "polygon"
        assert reg_kwargs["wallet_address"] == "0xleg"


class TestWriteOutboxChainWalletThreading:
    """``_write_outbox_and_fire_processor`` uses eff-chain/eff-wallet."""

    async def _run(self, *, chain, wallet_address):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_swap_intent()

        captured: dict = {}

        def _spy_key(intent_, itype, chn, wal, resolved_pool=None):
            captured["chain"] = chn
            captured["wallet"] = wal
            return "pos-key", "mkt"

        runner._compute_outbox_position_key = MagicMock(side_effect=_spy_key)  # type: ignore[method-assign]

        with (
            patch(
                "almanak.framework.accounting.processor.write_outbox_entry",
                new=AsyncMock(return_value="outbox-1"),
            ) as wob,
            patch("almanak.framework.observability.context.get_cycle_id", return_value="cyc"),
        ):
            await runner._write_outbox_and_fire_processor(
                strategy,
                intent,
                "ledger-1",
                chain=chain,
                wallet_address=wallet_address,
            )
        return captured, wob

    @pytest.mark.asyncio
    async def test_none_defaults_reproduce_strategy_values(self):
        captured, wob = await self._run(chain=None, wallet_address=None)
        assert captured == {"chain": "arbitrum", "wallet": "0xstrategywallet"}
        assert wob.call_args.kwargs["wallet_address"] == "0xstrategywallet"

    @pytest.mark.asyncio
    async def test_explicit_values_thread_through(self):
        captured, wob = await self._run(chain="base", wallet_address="0xleg")
        assert captured == {"chain": "base", "wallet": "0xleg"}
        assert wob.call_args.kwargs["wallet_address"] == "0xleg"


class TestEmitPositionEventWalletThreading:
    """``_emit_position_event_for_intent`` prefers an explicit wallet_address."""

    async def _run(self, *, wallet_address):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        result = MagicMock(success=True)
        entry = MagicMock()
        entry.id = "entry-1"

        pos_event = MagicMock()
        pos_event.position_id = "p1"
        pos_event.event_type = "OPEN"
        pos_event.position_type = "LP"

        runner._update_recent_open_events_cache = MagicMock()  # type: ignore[method-assign]
        runner._run_position_event_attribution = AsyncMock()  # type: ignore[method-assign]
        runner.state_manager.save_position_event = AsyncMock(return_value=True)

        with patch(
            "almanak.framework.observability.position_events.build_position_event_from_intent",
            return_value=pos_event,
        ) as build_mock:
            await runner._emit_position_event_for_intent(
                strategy=strategy,
                intent=intent,
                result=result,
                entry=entry,
                chain="polygon",
                deployment_id="dep-1",
                execution_mode="paper",
                cycle_id="cyc",
                price_oracle=None,
                post_state=None,
                wallet_address=wallet_address,
            )
        return build_mock

    @pytest.mark.asyncio
    async def test_none_falls_back_to_runtime_then_strategy(self):
        build_mock = await self._run(wallet_address=None)
        # No _runtime_config on this runner → falls back to strategy wallet.
        assert build_mock.call_args.kwargs["wallet_address"] == "0xstrategywallet"

    @pytest.mark.asyncio
    async def test_explicit_wallet_wins(self):
        build_mock = await self._run(wallet_address="0xleg")
        assert build_mock.call_args.kwargs["wallet_address"] == "0xleg"


class TestRegistryResolveChainParam:
    """``_registry_resolve_chain_and_nft_manager`` honours an explicit chain."""

    def test_none_uses_strategy_chain(self):
        runner = _make_runner()
        strategy = _make_strategy()
        with patch(
            "almanak.framework.migration.backfill._nft_manager_for_protocol_chain",
            return_value="0xNPM",
        ) as nft:
            out = runner._registry_resolve_chain_and_nft_manager(strategy, "LP_OPEN", "uniswap_v3")
        assert out == ("arbitrum", "0xNPM")
        assert nft.call_args.args[1] == "arbitrum"

    def test_explicit_chain_wins(self):
        runner = _make_runner()
        strategy = _make_strategy()
        with patch(
            "almanak.framework.migration.backfill._nft_manager_for_protocol_chain",
            return_value="0xNPM",
        ) as nft:
            out = runner._registry_resolve_chain_and_nft_manager(
                strategy, "LP_OPEN", "uniswap_v3", chain="polygon"
            )
        assert out == ("polygon", "0xNPM")
        assert nft.call_args.args[1] == "polygon"


# =============================================================================
# 2a. _adapt_leg_to_execution_result
# =============================================================================


class TestAdaptLegToExecutionResult:
    def test_gateway_execution_result(self):
        runner = _make_runner()
        gw = GatewayExecutionResult(
            success=True,
            tx_hashes=["0xdead"],
            total_gas_used=21000,
            receipts=[
                {
                    "status": 1,
                    "gas_used": 21000,
                    "effective_gas_price": 2,
                    "block_number": 100,
                    "logs": [],
                }
            ],
            execution_id="exec-1",
        )
        leg = MagicMock()
        leg.tx_result = gw

        exec_result, tx_results = runner._adapt_leg_to_execution_result(leg)
        assert isinstance(exec_result, ExecutionResult)
        assert exec_result.success is True
        assert exec_result.phase == ExecutionPhase.COMPLETE
        assert exec_result.total_gas_used == 21000
        # Reuses the gateway's own transaction_results property.
        assert tx_results == exec_result.transaction_results
        assert len(tx_results) == 1
        assert tx_results[0].tx_hash == "0xdead"

    def test_transaction_execution_result(self):
        runner = _make_runner()
        receipt = TransactionReceipt(
            tx_hash="0xbeef",
            block_number=200,
            block_hash="0xblk",
            gas_used=50000,
            effective_gas_price=3,
            status=1,
            logs=[],
        )
        tx = TransactionExecutionResult(
            success=True,
            tx_hash="0xbeef",
            receipt=receipt,
            gas_used=50000,
            gas_cost_wei=150000,
        )
        leg = MagicMock()
        leg.tx_result = tx

        exec_result, tx_results = runner._adapt_leg_to_execution_result(leg)
        assert isinstance(exec_result, ExecutionResult)
        assert exec_result.success is True
        assert exec_result.phase == ExecutionPhase.COMPLETE
        assert exec_result.total_gas_used == 50000
        assert exec_result.total_gas_cost_wei == 150000
        assert len(tx_results) == 1
        tr = tx_results[0]
        assert isinstance(tr, TransactionResult)
        assert tr.tx_hash == "0xbeef"
        assert tr.success is True
        assert tr.receipt is receipt
        assert tr.gas_used == 50000

    def test_unsupported_shape_raises(self):
        runner = _make_runner()
        leg = MagicMock()
        leg.tx_result = object()
        with pytest.raises(TypeError):
            runner._adapt_leg_to_execution_result(leg)


# =============================================================================
# 2b. _persist_executed_leg
# =============================================================================


def _persist_runner_with_spies():
    """Runner with the enricher + all persistence callees spied for isolation."""
    runner = _make_runner()

    runner._build_pool_key_lookup = MagicMock(return_value=None)  # type: ignore[method-assign]
    runner._build_curve_pool_meta_lookup = MagicMock(return_value=None)  # type: ignore[method-assign]
    runner._balance_provider_for_chain = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
    runner._reconcile_post_execution_balances = AsyncMock(  # type: ignore[method-assign]
        return_value={"incident": False}
    )
    runner._write_ledger_entry = AsyncMock(return_value="ledger-1")  # type: ignore[method-assign]
    runner._write_outbox_and_fire_processor = AsyncMock()  # type: ignore[method-assign]
    runner._notify_intent_executed = MagicMock()  # type: ignore[method-assign]
    runner._record_success = MagicMock()  # type: ignore[method-assign]
    return runner


class TestPersistExecutedLeg:
    @pytest.mark.asyncio
    async def test_settled_writes_ledger_and_outbox_with_leg_chain_wallet(self):
        runner = _persist_runner_with_spies()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        raw_result = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)
        enriched = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)

        enricher = MagicMock()
        enricher.enrich.return_value = enriched
        with (
            patch(
                "almanak.framework.runner.strategy_runner.ResultEnricher",
                return_value=enricher,
            ),
            patch(
                "almanak.framework.accounting.sidecar.AccountingSidecarWriter"
            ) as sidecar_cls,
        ):
            await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=raw_result,
                execution_context=MagicMock(),
                pre_snapshot=MagicMock(),
            )

        # Ledger written with the per-leg chain/wallet + user success True.
        lk = runner._write_ledger_entry.call_args.kwargs
        assert lk["chain"] == "polygon"
        assert lk["wallet_address"] == "0xleg"
        assert lk["success"] is True
        assert lk["error"] == ""
        assert lk["result"] is enriched

        # Outbox written with the per-leg chain/wallet.
        ok = runner._write_outbox_and_fire_processor.call_args.kwargs
        assert ok["chain"] == "polygon"
        assert ok["wallet_address"] == "0xleg"

        # Sidecar append uses the leg chain.
        assert sidecar_cls.return_value.append.call_args.kwargs["chain"] == "polygon"

        # Notify fired exactly once with the ENRICHED result, framework_success True.
        runner._notify_intent_executed.assert_called_once()
        n_args = runner._notify_intent_executed.call_args
        assert n_args.args[3] is enriched
        assert n_args.args[2] is True  # user success
        assert n_args.kwargs["framework_success"] is True

        # Recon ran (pre_snapshot supplied) bound to the leg's provider.
        runner._reconcile_post_execution_balances.assert_awaited_once()
        runner._balance_provider_for_chain.assert_called_with("polygon")
        # record_metrics defaults False → no per-leg success metric.
        runner._record_success.assert_not_called()

    @pytest.mark.asyncio
    async def test_enforced_recon_incident_downgrades_leg_and_returns_marker(self):
        """Audit fix (Codex P2): enforcement turns a landed leg into a recon failure."""
        runner = _persist_runner_with_spies()
        runner.config.reconciliation_enforcement = True
        runner._reconcile_post_execution_balances = AsyncMock(  # type: ignore[method-assign]
            return_value={"incident": True, "reconciliation_degraded": False}
        )
        runner._format_reconciliation_error = MagicMock(return_value="delta out of range")  # type: ignore[method-assign]
        strategy = _make_strategy()
        intent = _make_swap_intent()
        result = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)

        enricher = MagicMock()
        enricher.enrich.return_value = result
        with (
            patch("almanak.framework.runner.strategy_runner.ResultEnricher", return_value=enricher),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            marker = await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=result,
                execution_context=MagicMock(),
                pre_snapshot=MagicMock(),
            )

        assert "Reconciliation incident (enforced)" in marker
        # Ledger row carries the downgrade: success False + error marker.
        lk = runner._write_ledger_entry.call_args.kwargs
        assert lk["success"] is False
        assert "Reconciliation incident (enforced)" in lk["error"]
        # Strategy hears user failure; framework tracker hears chain truth.
        n_args = runner._notify_intent_executed.call_args
        assert n_args.args[2] is False
        assert n_args.kwargs["framework_success"] is True

    @pytest.mark.asyncio
    async def test_recon_incident_observation_mode_stays_clean_success(self):
        """Default observation mode: incident logged, verdict unchanged (no marker)."""
        runner = _persist_runner_with_spies()
        assert runner.config.reconciliation_enforcement is False
        runner._reconcile_post_execution_balances = AsyncMock(  # type: ignore[method-assign]
            return_value={"incident": True, "reconciliation_degraded": False}
        )
        runner._format_reconciliation_error = MagicMock(return_value="delta out of range")  # type: ignore[method-assign]
        strategy = _make_strategy()
        intent = _make_swap_intent()
        result = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)

        enricher = MagicMock()
        enricher.enrich.return_value = result
        with (
            patch("almanak.framework.runner.strategy_runner.ResultEnricher", return_value=enricher),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            marker = await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=result,
                execution_context=MagicMock(),
                pre_snapshot=MagicMock(),
            )

        assert marker == ""
        assert runner._write_ledger_entry.call_args.kwargs["success"] is True

    @pytest.mark.asyncio
    async def test_enforced_recon_incident_on_degraded_report_not_enforced(self):
        """VIB-3350 parity: a DEGRADED incident is never enforced, only logged."""
        runner = _persist_runner_with_spies()
        runner.config.reconciliation_enforcement = True
        runner._reconcile_post_execution_balances = AsyncMock(  # type: ignore[method-assign]
            return_value={"incident": True, "reconciliation_degraded": True}
        )
        runner._format_reconciliation_error = MagicMock(return_value="no pinned block")  # type: ignore[method-assign]
        strategy = _make_strategy()
        intent = _make_swap_intent()
        result = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)

        enricher = MagicMock()
        enricher.enrich.return_value = result
        with (
            patch("almanak.framework.runner.strategy_runner.ResultEnricher", return_value=enricher),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            marker = await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=result,
                execution_context=MagicMock(),
                pre_snapshot=MagicMock(),
            )

        assert marker == ""
        assert runner._write_ledger_entry.call_args.kwargs["success"] is True

    @pytest.mark.asyncio
    async def test_recon_skipped_when_no_pre_snapshot(self):
        runner = _persist_runner_with_spies()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        enriched = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)
        enricher = MagicMock()
        enricher.enrich.return_value = enriched
        with (
            patch(
                "almanak.framework.runner.strategy_runner.ResultEnricher",
                return_value=enricher,
            ),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE),
                execution_context=MagicMock(),
                pre_snapshot=None,
            )
        runner._reconcile_post_execution_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_degraded_settlement_stamps_marker_and_no_user_success(self):
        runner = _persist_runner_with_spies()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        enriched = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)
        enricher = MagicMock()
        enricher.enrich.return_value = enriched
        with (
            patch(
                "almanak.framework.runner.strategy_runner.ResultEnricher",
                return_value=enricher,
            ),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE),
                execution_context=MagicMock(),
                settlement_status="degraded",
                pre_snapshot=None,
            )

        # Ledger stamped with the settlement-degraded marker + success False.
        lk = runner._write_ledger_entry.call_args.kwargs
        assert lk["success"] is False
        assert lk["error"] == "settlement_degraded"
        assert enriched.error == "settlement_degraded"

        # Notify: user verdict False, but framework_success True (chain reality).
        runner._notify_intent_executed.assert_called_once()
        n_args = runner._notify_intent_executed.call_args
        assert n_args.args[2] is False
        assert n_args.kwargs["framework_success"] is True

    @pytest.mark.asyncio
    async def test_record_metrics_flag_fires_success_once(self):
        runner = _persist_runner_with_spies()
        strategy = _make_strategy()
        intent = _make_swap_intent()
        enriched = ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE)
        enricher = MagicMock()
        enricher.enrich.return_value = enriched
        with (
            patch(
                "almanak.framework.runner.strategy_runner.ResultEnricher",
                return_value=enricher,
            ),
            patch("almanak.framework.accounting.sidecar.AccountingSidecarWriter"),
        ):
            await runner._persist_executed_leg(
                strategy=strategy,
                intent=intent,
                chain="polygon",
                wallet_address="0xleg",
                execution_result=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE),
                execution_context=MagicMock(),
                record_metrics=True,
                pre_snapshot=None,
            )
        runner._record_success.assert_called_once_with(execution_proved=True)
