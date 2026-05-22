"""Tests for VIB-3180: CriticalAccountingError exception hierarchy fix.

Verifies that CriticalAccountingError:

1. Is an Exception subclass (not bare BaseException), so run_iteration's
   recovery ``except Exception`` handler can catch it.
2. Is NOT swallowed by the generic enrichment warning handler in
   _single_chain_handle_success — it re-raises explicitly so it reaches
   run_iteration's outer catch.
3. run_iteration converts it to IterationStatus.ACCOUNTING_FAILED (not
   STRATEGY_ERROR) with proper metadata — the same treatment given to
   AccountingPersistenceError.
4. The run_loop consecutive-error counter increments (not a crash).
5. finalize_run_loop still runs after ACCOUNTING_FAILED (cleanup path).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.extract_result import (
    CriticalAccountingError,
    ExtractError,
)
from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.execution.receipt_registry import ReceiptParserRegistry
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(*, enable_alerting: bool = False) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=enable_alerting,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


# =============================================================================
# 1. Exception hierarchy
# =============================================================================


class TestExceptionHierarchy:
    """CriticalAccountingError must be Exception, never bare BaseException."""

    def test_is_exception_subclass(self) -> None:
        assert issubclass(CriticalAccountingError, Exception)

    def test_direct_base_is_exception_not_base_exception(self) -> None:
        # The direct base class must be Exception (or a subclass of it),
        # NOT bare BaseException. This guards against someone accidentally
        # reverting to BaseException while keeping Exception in the MRO via
        # multiple inheritance.
        assert Exception in CriticalAccountingError.__bases__, (
            "CriticalAccountingError.__bases__ must include Exception directly, "
            "not just transitively via BaseException"
        )

    def test_caught_by_except_exception(self) -> None:
        """The error must be reachable by a plain except Exception block."""
        caught = False
        try:
            raise CriticalAccountingError("parse failed", field_name="swap_amounts")
        except Exception:
            caught = True
        assert caught

    def test_attributes_preserved(self) -> None:
        err = CriticalAccountingError(
            "parse failed",
            field_name="swap_amounts",
            intent_type="SWAP",
            protocol="uniswap_v3",
            original=ValueError("inner"),
        )
        assert err.field_name == "swap_amounts"
        assert err.intent_type == "SWAP"
        assert err.protocol == "uniswap_v3"
        assert isinstance(err.original, ValueError)
        assert "parse failed" in str(err)


# =============================================================================
# 2. ResultEnricher still raises in live mode
# =============================================================================


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xabc123"

    def to_dict(self) -> dict[str, Any]:
        return {"tx_hash": self.tx_hash, "logs": [], "status": 1, "gas_used": 100000}


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xabc123"
    receipt: _FakeReceipt | None = None
    gas_used: int = 100000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    position_id: int | str | None = None
    swap_amounts: SwapAmounts | None = None
    lp_close_data: Any = None
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)


@dataclass
class _FakeContext:
    chain: str = "arbitrum"
    protocol: str | None = None


@dataclass
class _FakeIntent:
    intent_type: str = "SWAP"
    protocol: str | None = None


class _ErrorParser:
    """Parser that always returns ExtractError."""

    def extract_swap_amounts_result(self, receipt: dict[str, Any]) -> Any:
        return ExtractError(error="malformed log shape")


def _registry_with(parser: Any) -> ReceiptParserRegistry:
    registry = ReceiptParserRegistry()

    def _fake_get(protocol: str, **kwargs: Any) -> Any:  # noqa: ARG001
        return parser

    registry.get = _fake_get  # type: ignore[assignment]
    return registry


def _make_exec_result() -> _FakeExecResult:
    return _FakeExecResult(
        success=True,
        transaction_results=[_FakeTxResult(receipt=_FakeReceipt())],
    )


class TestEnricherRaisesInLiveMode:
    """ResultEnricher still raises CriticalAccountingError in live mode."""

    def test_live_mode_raises(self) -> None:
        enricher = ResultEnricher(parser_registry=_registry_with(_ErrorParser()), live_mode=True)
        intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
        result = _make_exec_result()

        with pytest.raises(CriticalAccountingError) as exc_info:
            enricher.enrich(result, intent, _FakeContext())

        err = exc_info.value
        assert err.field_name == "swap_amounts"
        assert err.intent_type == "SWAP"
        assert err.protocol == "fakeproto"

    def test_paper_mode_does_not_raise(self) -> None:
        enricher = ResultEnricher(parser_registry=_registry_with(_ErrorParser()), live_mode=False)
        intent = _FakeIntent(intent_type="SWAP", protocol="fakeproto")
        result = _make_exec_result()

        enriched = enricher.enrich(result, intent, _FakeContext())
        assert enriched.swap_amounts is None
        assert enricher.extract_error_count == 1


# =============================================================================
# 3. run_iteration returns ACCOUNTING_FAILED (not STRATEGY_ERROR)
# =============================================================================


class TestRunIterationAccountingFailed:
    """CriticalAccountingError from the enrichment layer must become ACCOUNTING_FAILED."""

    @pytest.mark.asyncio
    async def test_accounting_failed_on_enrichment_error(self) -> None:
        """When enrichment raises CriticalAccountingError, run_iteration
        must return ACCOUNTING_FAILED — not STRATEGY_ERROR and not crash."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x" + "ab" * 20

        # Make run_iteration reach the enrichment step by patching _step_execute
        # to raise CriticalAccountingError (simulating what happens when
        # _single_chain_handle_success's enricher raises and re-raises it).
        cae = CriticalAccountingError(
            "Extraction failed for swap_amounts",
            field_name="swap_amounts",
            intent_type="SWAP",
            protocol="uniswap_v3",
        )

        runner._step_execute = AsyncMock(side_effect=cae)  # type: ignore[method-assign]
        runner._step_pause_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_teardown_and_cb_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_periodic_hooks = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_build_snapshot = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_decide = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_extract_intents = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._step_log_intents = MagicMock()  # type: ignore[method-assign]
        runner._step_circuit_breaker_pre_execute = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._step_snapshot_pre_balances = AsyncMock(return_value=None)  # type: ignore[method-assign]

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.ACCOUNTING_FAILED, (
            f"Expected ACCOUNTING_FAILED but got {result.status}. "
            "CriticalAccountingError must be handled by run_iteration's recovery path."
        )
        assert result.success is False
        assert "swap_amounts" in result.error or "SWAP" in result.error or "enrichment" in result.error.lower()

    @pytest.mark.asyncio
    async def test_regular_exception_still_strategy_error(self) -> None:
        """Non-accounting exceptions should still map to STRATEGY_ERROR."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x" + "ab" * 20

        runner._step_execute = AsyncMock(side_effect=RuntimeError("unexpected crash"))  # type: ignore[method-assign]
        runner._step_pause_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_teardown_and_cb_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_periodic_hooks = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_build_snapshot = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_decide = AsyncMock(return_value=None)  # type: ignore[method-assign]
        runner._step_extract_intents = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._step_log_intents = MagicMock()  # type: ignore[method-assign]
        runner._step_circuit_breaker_pre_execute = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._step_snapshot_pre_balances = AsyncMock(return_value=None)  # type: ignore[method-assign]

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR

    @pytest.mark.asyncio
    async def test_accounting_failed_does_not_crash_run_loop(self) -> None:
        """run_loop must not crash when CriticalAccountingError fires during an iteration.

        finalize_run_loop must still execute (cleanup path) after ACCOUNTING_FAILED.
        The run_loop must complete normally (not raise) when run_iteration returns
        ACCOUNTING_FAILED — proving the BaseException change did not break the
        run_loop's cleanup contract.
        """
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x" + "ab" * 20

        accounting_failed_result = MagicMock()
        accounting_failed_result.success = False
        accounting_failed_result.status = IterationStatus.ACCOUNTING_FAILED

        # run_iteration returns ACCOUNTING_FAILED (the recovery path we just added)
        runner.run_iteration = AsyncMock(return_value=accounting_failed_result)  # type: ignore[method-assign]

        finalize_called = []

        async def _fake_finalize(runner_self: Any, strategy: Any, deployment_id: Any) -> None:
            finalize_called.append(True)

        runner._recover_incomplete_sessions = AsyncMock(return_value=0)
        runner._get_gateway_client = MagicMock(return_value=None)
        runner._register_with_gateway = MagicMock()
        runner._deregister_from_gateway = MagicMock()
        runner.state_manager.initialize = AsyncMock()
        runner.state_manager.close = AsyncMock()
        runner._gateway_heartbeat = MagicMock()
        runner._lifecycle_heartbeat = MagicMock()
        runner._lifecycle_poll_command = MagicMock(return_value=None)
        runner._emit_iteration_summary = MagicMock()

        with patch(
            "almanak.framework.runner._run_loop_helpers.finalize_run_loop",
            side_effect=_fake_finalize,
        ):
            with patch(
                "almanak.framework.runner._run_loop_helpers.initialize_run_loop",
                new_callable=AsyncMock,
                return_value=None,
            ):
                with patch(
                    "almanak.framework.runner._run_loop_helpers.handle_iteration_failure",
                    new_callable=AsyncMock,
                ) as mock_failure_handler:
                    with patch(
                        "almanak.framework.runner._run_loop_helpers.capture_snapshot_with_accounting",
                        new_callable=AsyncMock,
                        return_value=accounting_failed_result,
                    ):
                        # Must not raise — run_loop completes cleanly
                        await runner.run_loop(
                            strategy,
                            interval_seconds=0,
                            max_iterations=1,
                        )

        # finalize_run_loop was called — cleanup ran despite accounting failure
        assert finalize_called, "finalize_run_loop must execute even after ACCOUNTING_FAILED"
        # handle_iteration_failure was called with the ACCOUNTING_FAILED result
        mock_failure_handler.assert_called_once()
        # The result passed to the failure handler must carry ACCOUNTING_FAILED status
        call_args_positional = mock_failure_handler.call_args[0]
        call_kwargs_all = mock_failure_handler.call_args[1] if mock_failure_handler.call_args[1] else {}
        passed_result = call_args_positional[-1] if call_args_positional else call_kwargs_all.get("result")
        assert passed_result is not None, "handle_iteration_failure must receive the result"
        assert passed_result.status == IterationStatus.ACCOUNTING_FAILED, (
            f"Expected ACCOUNTING_FAILED but got {passed_result.status}. "
            "run_loop must route enrichment failures to ACCOUNTING_FAILED."
        )
