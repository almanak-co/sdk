"""VIB-3892 — ``almanak strat teardown execute`` wires the accounting pipeline.

Pre-fix the CLI built ``TeardownManager(orchestrator=…, compiler=…,
state_manager=teardown_state_adapter)`` without ``runner_helpers``. The
default empty :class:`TeardownRunnerHelpers` made ``has_commit=False``, so
the augmentation pipeline (enrich → ledger → outbox+fire → sidecar) never
fired for any teardown via this CLI command — discover or not. On-chain
LP_CLOSE landed; ``transaction_ledger`` / ``accounting_events`` /
``position_events`` stayed empty.

These tests verify the construction path used by the CLI now wires the
runner helpers so ``has_commit`` and ``has_snapshot`` are both True.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle
from almanak.framework.runner.runner_models import RunnerConfig
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.teardown.runner_helpers import build_runner_helpers
from almanak.framework.teardown.teardown_manager import TeardownManager


def _stub_gateway_client() -> MagicMock:
    """Minimal duck-typed GatewayClient — every attribute access returns a MagicMock."""
    client = MagicMock(name="GatewayClient")
    return client


def test_cli_construction_yields_runner_helpers_with_commit_and_snapshot():
    """The exact construction the CLI performs (mirrors
    framework/cli/teardown.py:execute_teardown) produces a runner whose
    helpers expose both ``commit`` and ``capture_snapshot`` callables.
    """
    gateway_client = _stub_gateway_client()
    chain = "arbitrum"
    wallet = "0xWALLET"
    orchestrator = MagicMock(name="GatewayExecutionOrchestrator")

    state_manager = GatewayStateManager(gateway_client)
    price_oracle = GatewayPriceOracle(gateway_client, default_chain=chain)
    balance_provider = GatewayBalanceProvider(
        client=gateway_client, wallet_address=wallet, chain=chain
    )
    runner_config = RunnerConfig(dry_run=False, enable_state_persistence=True)
    runner = StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=orchestrator,  # type: ignore[arg-type]
        state_manager=state_manager,
        config=runner_config,
    )
    helpers = build_runner_helpers(runner)

    assert helpers.has_commit, (
        "VIB-3892: build_runner_helpers must produce a callable .commit; "
        "without this the discover-lane teardown writes no accounting rows."
    )
    assert helpers.has_snapshot, (
        "VIB-3892: build_runner_helpers must produce a callable .capture_snapshot; "
        "without this the post-teardown portfolio_snapshots row never lands."
    )


def test_teardown_manager_with_helpers_drives_commit_pipeline():
    """``TeardownManager(runner_helpers=helpers)`` exposes ``has_commit``
    on the helpers bag, which the inner ``_execute_intents`` loop checks
    at line 960 of teardown_manager.py before firing the commit pipeline.
    """
    gateway_client = _stub_gateway_client()
    state_manager = GatewayStateManager(gateway_client)
    price_oracle = GatewayPriceOracle(gateway_client, default_chain="arbitrum")
    balance_provider = GatewayBalanceProvider(
        client=gateway_client, wallet_address="0xWALLET", chain="arbitrum"
    )
    runner = StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=RunnerConfig(dry_run=False, enable_state_persistence=True),
    )
    helpers = build_runner_helpers(runner)

    manager = TeardownManager(
        orchestrator=MagicMock(name="orchestrator"),
        compiler=MagicMock(name="compiler"),
        state_manager=MagicMock(name="teardown_state_adapter"),
        runner_helpers=helpers,
    )

    # The TeardownManager loop does ``if self.runner_helpers.has_commit:``
    # before calling ``self.runner_helpers.commit(...)``. Locking that gate
    # to True is what flips the discover lane from "writes nothing" to
    # "writes the canonical augmentation pipeline."
    assert manager.runner_helpers.has_commit is True
    assert manager.runner_helpers.has_snapshot is True


def test_default_teardown_manager_without_helpers_still_safe():
    """Backwards compat: a manager built without ``runner_helpers`` (the
    pre-VIB-3892 CLI shape) still constructs cleanly. The discover lane
    will not write accounting in this configuration — that's the bug we
    are fixing — but legacy callers that haven't been migrated must not
    crash on import.
    """
    manager = TeardownManager(
        orchestrator=MagicMock(),
        compiler=MagicMock(),
        state_manager=MagicMock(),
    )

    # has_commit is False — the very condition that hid LP_CLOSE
    # accounting writes from the dashboard pre-fix.
    assert manager.runner_helpers.has_commit is False
    assert manager.runner_helpers.has_snapshot is False
