"""Receipt clients share descriptor-driven asynchronous block formatting."""

import ast
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from web3 import AsyncHTTPProvider

from almanak.framework.execution.chain_executor import ChainExecutor
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.gateway.utils.async_web3_cleanup import drain_failed_client_cleanup
from almanak.gateway.utils.rpc_provider import create_async_web3


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_type", [ChainExecutor, ExecutionOrchestrator])
async def test_execution_client_formats_poa_blocks_and_reuses_owned_client(owner_type):
    owner = object.__new__(owner_type)
    owner._web3 = None
    owner._rpc_url = owner.rpc_url = "https://unused.invalid"
    owner._chain = "bsc"
    if owner_type is ExecutionOrchestrator:
        owner.chain = "bsc"
    provider = AsyncHTTPProvider("https://unused.invalid")
    provider.make_request = AsyncMock(
        return_value={
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"number": "0x7", "extraData": "0x" + "ab" * 97},
        }
    )
    with patch("web3.AsyncHTTPProvider", return_value=provider):
        client = await owner._get_web3()
    block = await client.eth.get_block(7)
    assert len(block["proofOfAuthorityData"]) == 97
    assert await owner._get_web3() is client


@pytest.mark.asyncio
async def test_chain_discovery_failure_closes_owned_provider():
    provider = AsyncHTTPProvider("https://unused.invalid")
    provider.make_request = AsyncMock(side_effect=RuntimeError("unavailable"))
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch.object(provider, "disconnect", new=AsyncMock()) as disconnect,
        pytest.raises(RuntimeError, match="unavailable"),
    ):
        await create_async_web3("https://unused.invalid")
    await drain_failed_client_cleanup()
    disconnect.assert_awaited_once()


def test_receipt_entrypoints_cannot_construct_unconfigured_async_web3():
    root = Path(__file__).resolve().parents[2]
    paths = [
        "almanak/framework/execution/submitter/public.py",
        "almanak/framework/execution/orchestrator.py",
        "almanak/framework/execution/chain_executor.py",
        "almanak/gateway/services/execution_service.py",
    ]
    violations = []
    for path in paths:
        tree = ast.parse((root / path).read_text())
        constructors = set()
        modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "web3":
                constructors.update(alias.asname or alias.name for alias in node.names if alias.name == "AsyncWeb3")
            if isinstance(node, ast.Import):
                modules.update(alias.asname or alias.name for alias in node.names if alias.name == "web3")
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            direct = isinstance(node.func, ast.Name) and node.func.id in constructors
            qualified = (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "AsyncWeb3"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in modules
            )
            if direct or qualified:
                violations.append(f"{path}:{node.lineno}")
    assert not violations, f"Receipt clients must use create_async_web3: {violations}"
