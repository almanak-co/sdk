"""Simulation preference survives CLI resolution, execution and independent exits."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.cli._run_modes import _prepare_runtime_bootstrap
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator
from almanak.framework.execution.interfaces import SimulationResult
from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.runner.runner_models import RunnerConfig
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.teardown.teardown_manager import TeardownManager
from almanak.gateway.proto import gateway_pb2

WALLET = "0x" + "1" * 40


@pytest.mark.parametrize(
    "configured,override,expected",
    [(False, None, False), (True, None, True), (False, True, True), (True, False, False)],
)
def test_cli_override_resolves_before_components(configured, override, expected):
    bootstrap = SimpleNamespace(
        strategy_class=object,
        strategy_config={"deployment_id": "deployment:test"},
        multi_chain=False,
        strategy_chains=["base"],
        strategy_protocols=[],
        config_display_name="test",
    )
    runtime = SimpleNamespace(simulation_enabled=configured)
    with (
        patch("almanak.framework.cli._run_modes._resolve_config_chain_with_echo", return_value="base"),
        patch("almanak.framework.cli._run_modes._echo_runtime_network", return_value="mainnet"),
        patch("almanak.framework.cli._run_modes._build_runtime_config", return_value=(runtime, {})),
        patch("almanak.framework.cli._run_modes._resolve_identity", return_value=SimpleNamespace(run_id="run")),
    ):
        result = _prepare_runtime_bootstrap(
            strategy_bootstrap=bootstrap,
            no_gateway=True,
            gateway_client=MagicMock(),
            gateway_network="mainnet",
            fresh=False,
            simulation_override=override,
        )
    assert result.runtime_config.simulation_enabled is expected


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.asyncio
async def test_runner_iteration_context_reaches_gateway_request(enabled):
    runner = object.__new__(StrategyRunner)
    runner.config = RunnerConfig(simulation_enabled=enabled)
    runner.execution_orchestrator = MagicMock()
    runner._single_chain_pre_retry_confirmed = AsyncMock(return_value=True)
    state = SimpleNamespace(
        strategy=SimpleNamespace(chain="base", wallet_address=WALLET),
        intent=SimpleNamespace(protocol="uniswap_v4", intent_id="intent"),
        deployment_id="deployment:test",
        state_machine=MagicMock(),
        compiler=MagicMock(),
    )
    bundle = ActionBundle(intent_type="SWAP", transactions=[], metadata={})
    await runner._single_chain_execute_step(state, SimpleNamespace(action_bundle=bundle))
    assert state.last_execution_context.simulation_enabled is enabled
    gateway = MagicMock()
    gateway.execution.Execute.return_value = gateway_pb2.ExecutionResult(success=False, error="test-only")
    orchestrator = GatewayExecutionOrchestrator(gateway, chain="base", wallet_address=WALLET)
    await orchestrator.execute(bundle, context=state.last_execution_context)
    request = gateway.execution.Execute.call_args.args[0]
    assert request.simulation_enabled is enabled


@pytest.mark.parametrize("enabled", [False, True])
def test_teardown_context_keeps_simulation_preference(enabled):
    manager = TeardownManager(simulation_enabled=enabled)
    strategy = SimpleNamespace(chain="base", deployment_id="deployment:test", wallet_address=WALLET)
    intent = SimpleNamespace(chain="base", intent_type=SimpleNamespace(value="SWAP"), from_token="USDC", to_token="ETH")
    context = manager._build_execution_context(strategy, intent, "td", 0)
    assert context.simulation_enabled is enabled
    assert context.wallet_address == WALLET


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.asyncio
async def test_multichain_gateway_does_not_override_preference(enabled):
    orchestrator = MultiChainOrchestrator.from_gateway(
        gateway_client=MagicMock(),
        chains=["base"],
        wallet_address=WALLET,
        simulation_enabled=enabled,
    )
    gateway_orchestrator = MagicMock()
    gateway_orchestrator.execute = AsyncMock()
    orchestrator._get_gateway_orchestrator = MagicMock(return_value=gateway_orchestrator)
    bundle = ActionBundle(intent_type="SWAP", transactions=[], metadata={})
    gateway_orchestrator.compile_intent = AsyncMock(return_value=bundle)
    await orchestrator._gateway_compile_and_execute(MagicMock(), "base")
    gateway_orchestrator.execute.assert_awaited_once_with(bundle, simulation_enabled=enabled)


@pytest.mark.parametrize("enabled,simulated,refuses", [(False, False, False), (True, False, True), (True, True, False)])
@pytest.mark.asyncio
async def test_requested_simulation_cannot_degrade_to_pass_through(enabled, simulated, refuses, caplog):
    caplog.set_level(logging.INFO, logger="almanak.framework.execution.orchestrator")
    orchestrator = object.__new__(ExecutionOrchestrator)
    orchestrator.signer = MagicMock()
    orchestrator.simulator = MagicMock()
    orchestrator.simulator.simulate = AsyncMock(return_value=SimulationResult(success=True, simulated=simulated))
    orchestrator._emit_event = MagicMock()
    orchestrator._complete_session = MagicMock()
    state = SimpleNamespace(
        context=ExecutionContext(deployment_id="deployment:test", chain="base", simulation_enabled=enabled),
        result=ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION),
        session=None,
        unsigned_txs=[],
        action_bundle=ActionBundle(intent_type="SWAP", transactions=[], metadata={}),
    )
    refusal = await orchestrator._phase_simulate(state)
    assert (refusal is not None) is refuses
    assert orchestrator.simulator.simulate.await_count == int(enabled)
    evidence = [r.message for r in caplog.records if "execution_simulation_result" in r.message]
    assert len(evidence) == int(enabled)
    if enabled:
        from almanak.framework.execution.submission import execution_plan_hash

        assert f"simulated={simulated}" in evidence[0]
        assert f"plan_hash={execution_plan_hash(state.action_bundle)}" in evidence[0]
    if refuses:
        assert refusal.error_phase is ExecutionPhase.SIMULATION
        assert "did not simulate" in refusal.error


def test_direct_runner_default_does_not_enable_new_simulation():
    assert RunnerConfig().simulation_enabled is False


@pytest.mark.parametrize("enabled", [False, True])
def test_component_runner_and_signal_teardown_share_preference(enabled):
    from almanak.framework.cli._run_components import _build_runner
    from almanak.framework.cli._run_context import ComponentBundle
    from almanak.framework.runner._teardown_helpers import build_teardown_manager

    components = ComponentBundle()
    with patch("almanak.framework.runner.StrategyRunner") as factory:
        _build_runner(
            interval=60,
            effective_dry_run=False,
            deployment_id="deployment:test",
            components=components,
            vault_lifecycle=None,
            simulation_enabled=enabled,
        )
    config = factory.call_args.kwargs["config"]
    assert config.simulation_enabled is enabled
    runner = SimpleNamespace(
        config=config,
        execution_orchestrator=MagicMock(),
        alert_manager=None,
        _get_gateway_client=lambda: MagicMock(),
    )
    with (
        patch("almanak.framework.teardown.create_teardown_state_adapter_for_runtime", return_value=MagicMock()),
        patch("almanak.framework.teardown.runner_helpers.build_runner_helpers", return_value=None),
    ):
        manager, _ = build_teardown_manager(runner, MagicMock(), SimpleNamespace(db_path=None))
    assert manager.simulation_enabled is enabled


@pytest.mark.asyncio
async def test_cached_gateway_backend_remains_capable_when_environment_default_is_disabled():
    from almanak.framework.execution.simulator.config import SimulationConfig
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    service = object.__new__(ExecutionServiceServicer)
    service.settings = SimpleNamespace(network="mainnet")
    service.wallet_registry = None
    service._orchestrator_cache = {}
    service._orchestrator_locks = {}
    service._orchestrator_default_gas_caps = {}
    service._create_signer = MagicMock()
    with (
        patch("almanak.gateway.utils.get_rpc_url", return_value="https://rpc.invalid"),
        patch("almanak.framework.execution.submitter.PublicMempoolSubmitter"),
        patch(
            "almanak.framework.execution.simulator.config.SimulationConfig.from_env",
            return_value=SimulationConfig.disabled(),
        ),
        patch("almanak.framework.execution.simulator.create_simulator") as factory,
        patch("almanak.framework.execution.orchestrator.ExecutionOrchestrator"),
    ):
        first = await service._get_orchestrator("base", WALLET)
        second = await service._get_orchestrator("base", WALLET)
    assert first is second
    factory.assert_called_once()
    assert factory.call_args.kwargs["config"].enabled is True


@pytest.mark.parametrize("flag,expected", [(None, None), ("--simulate-tx", True), ("--no-simulate-tx", False)])
def test_actual_run_command_forwards_simulation_override(tmp_path, flag, expected):
    from contextlib import ExitStack

    from click.testing import CliRunner

    from almanak.framework.cli.run import run

    class BootstrapReached(Exception):
        pass

    prefix = "almanak.framework.cli.run_helpers."
    with ExitStack() as stack:
        for name in ("_configure_logging_and_validate", "_wire_token_resolver", "_load_strategy_bootstrap"):
            stack.enter_context(patch(prefix + name))
        for name in ("_handle_list_all", "_maybe_handle_run_early_exit"):
            stack.enter_context(patch(prefix + name, return_value=False))
        stack.enter_context(
            patch(prefix + "_setup_gateway", return_value=(MagicMock(), None, "localhost", 50051, "mainnet", None, None, None))
        )
        bootstrap = stack.enter_context(patch(prefix + "_prepare_runtime_bootstrap", side_effect=BootstrapReached))
        args = ["--working-dir", str(tmp_path)] + ([flag] if flag else [])
        result = CliRunner().invoke(run, args)
    assert isinstance(result.exception, BootstrapReached), result.output
    assert bootstrap.call_args.kwargs["simulation_override"] is expected
