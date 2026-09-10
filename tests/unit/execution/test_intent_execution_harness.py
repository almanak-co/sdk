"""Managed Anvil execution declares its provenance without disabling simulation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from tests.intents._execution_harness import SimulatedIntentOrchestrator


@pytest.mark.parametrize(
    "declaration,expected", [({}, True), ({"managed_fork": False}, False), ({"managed_fork": None}, None)]
)
@pytest.mark.asyncio
async def test_fork_declaration_preserves_simulation(declaration, expected):
    simulator = MagicMock()
    signer = MagicMock(address="0x" + "1" * 40)
    orchestrator = SimulatedIntentOrchestrator(
        signer=signer, submitter=MagicMock(), simulator=simulator, chain="base", **declaration
    )
    assert orchestrator.managed_fork is expected
    assert orchestrator.simulator is simulator
    bundle = MagicMock()
    with patch.object(ExecutionOrchestrator, "execute", new_callable=AsyncMock) as execute:
        result = await orchestrator.execute(bundle)
    execute.assert_awaited_once()
    forwarded_bundle, context = execute.call_args.args
    assert forwarded_bundle is bundle
    assert context.simulation_enabled is True
    assert context.chain == "base"
    assert context.wallet_address == signer.address
    assert result is execute.return_value


@pytest.mark.asyncio
async def test_explicit_execution_context_is_preserved():
    orchestrator = SimulatedIntentOrchestrator(signer=MagicMock(), submitter=MagicMock(), simulator=MagicMock())
    context = ExecutionContext(simulation_enabled=True, chain="base")
    bundle = MagicMock()
    with patch.object(ExecutionOrchestrator, "execute", new_callable=AsyncMock) as execute:
        await orchestrator.execute(bundle, context)
    execute.assert_awaited_once_with(bundle, context)
