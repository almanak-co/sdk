"""Tests for _emit_iteration_summary structured log emission (VIB-524)."""

import logging
from dataclasses import dataclass
from unittest.mock import MagicMock

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)

# Logger used by strategy_runner module
_RUNNER_LOGGER = "almanak.framework.runner.strategy_runner"


def _make_runner(**config_overrides) -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
        **config_overrides,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=config,
    )
    return runner


def _capture_summary(runner, result, chain=None):
    """Run _emit_iteration_summary and return the captured LogRecord."""
    records = []

    class Collector(logging.Handler):
        def emit(self, record):
            if record.getMessage() == "iteration_summary":
                records.append(record)

    target_logger = logging.getLogger(_RUNNER_LOGGER)
    original_level = target_logger.level
    target_logger.setLevel(logging.DEBUG)
    handler = Collector()
    handler.setLevel(logging.DEBUG)
    target_logger.addHandler(handler)
    try:
        runner._emit_iteration_summary(result, chain=chain)
    finally:
        target_logger.removeHandler(handler)
        target_logger.setLevel(original_level)

    assert len(records) == 1, f"Expected 1 iteration_summary record, got {len(records)}"
    return records[0]


def test_emit_iteration_summary_hold():
    """HOLD iteration emits structured summary with decision=HOLD."""
    from almanak.framework.intents.vocabulary import HoldIntent

    runner = _make_runner()
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.HOLD,
        intent=HoldIntent(reason="Waiting for entry"),
        strategy_id="test-strat",
        duration_ms=42.5,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    assert record.event_type == "iteration_summary"
    assert record.strategy_id == "test-strat"
    assert record.chain == "arbitrum"
    assert record.iteration == 1
    assert record.decision == "HOLD"
    assert record.status == "HOLD"
    assert record.duration_ms == 42.5
    assert record.dry_run is False
    assert record.txs_planned == 0
    assert record.txs_sent == 0
    assert record.tx_hashes == []
    assert record.error is None


def test_emit_iteration_summary_success_with_execution():
    """Successful execution includes tx_hashes and counts."""
    runner = _make_runner()
    runner._total_iterations = 3

    @dataclass
    class FakeTxResult:
        tx_hash: str

    exec_result = MagicMock()
    exec_result.transaction_results = [
        FakeTxResult(tx_hash="0xabc123"),
        FakeTxResult(tx_hash="0xdef456"),
    ]
    exec_result.tx_hashes = None
    exec_result.receipts = [{"status": 1}, {"status": 1}]

    intent = MagicMock()
    intent.intent_type.value = "SWAP"
    intent.serialize.return_value = {"intent_type": "SWAP", "token_in": "USDC"}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        execution_result=exec_result,
        strategy_id="swap-strat",
        duration_ms=1234.0,
    )

    record = _capture_summary(runner, result, chain="base")

    assert record.event_type == "iteration_summary"
    assert record.strategy_id == "swap-strat"
    assert record.chain == "base"
    assert record.iteration == 3
    assert record.decision == "SWAP"
    assert record.intents == [{"intent_type": "SWAP", "token_in": "USDC"}]
    assert record.txs_sent == 2
    assert record.txs_planned == 2
    assert record.tx_hashes == ["0xabc123", "0xdef456"]
    assert record.status == "SUCCESS"
    assert record.duration_ms == 1234.0
    assert record.error is None


def test_emit_iteration_summary_dry_run_flag():
    """Dry run config flag is reflected in the summary."""
    runner = _make_runner(dry_run=True)
    runner._total_iterations = 1

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        strategy_id="dry-strat",
        duration_ms=10.0,
    )

    record = _capture_summary(runner, result)

    assert record.dry_run is True
    assert record.chain is None


def test_emit_iteration_summary_error():
    """Error iterations include the error message."""
    runner = _make_runner()
    runner._total_iterations = 5

    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        error="Transaction reverted: insufficient balance",
        strategy_id="err-strat",
        duration_ms=500.0,
    )

    record = _capture_summary(runner, result, chain="ethereum")

    assert record.status == "EXECUTION_FAILED"
    assert record.error == "Transaction reverted: insufficient balance"
    assert record.strategy_id == "err-strat"
    assert record.iteration == 5


def test_emit_iteration_summary_gateway_execution_result():
    """GatewayExecutionResult with tx_hashes list is handled correctly."""
    runner = _make_runner()
    runner._total_iterations = 2

    exec_result = MagicMock()
    exec_result.transaction_results = []
    exec_result.tx_hashes = ["0x111", "0x222", "0x333"]
    exec_result.receipts = [{"status": 1}, {"status": 1}, {"status": 1}]

    intent = MagicMock()
    intent.intent_type.value = "LP_OPEN"
    intent.serialize.return_value = {"intent_type": "LP_OPEN"}

    result = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        execution_result=exec_result,
        strategy_id="lp-strat",
        duration_ms=2000.0,
    )

    record = _capture_summary(runner, result, chain="arbitrum")

    assert record.tx_hashes == ["0x111", "0x222", "0x333"]
    assert record.txs_sent == 3
    assert record.txs_planned == 3
