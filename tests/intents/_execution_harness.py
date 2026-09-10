"""Execution defaults for real managed-fork EOA intent proofs."""

from typing import Any

from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator, ExecutionResult
from almanak.framework.models.reproduction_bundle import ActionBundle


class SimulatedIntentOrchestrator(ExecutionOrchestrator):
    """Require real simulation for the ordinary context-free intent test path."""

    def __init__(self, *args: Any, managed_fork: bool | None = True, **kwargs: Any) -> None:
        super().__init__(*args, managed_fork=managed_fork, **kwargs)

    async def execute(self, action_bundle: ActionBundle, context: ExecutionContext | None = None) -> ExecutionResult:
        if context is None:
            context = ExecutionContext(
                wallet_address=self.signer.address,
                chain=self.chain,
                simulation_enabled=True,
            )
        return await super().execute(action_bundle, context)


def prepare_fork_eoa(web3: Web3, wallet: str) -> None:
    """Remove upstream delegation from a declared test EOA before funding."""
    address = Web3.to_checksum_address(wallet)
    if web3.eth.get_code(address):
        response = web3.provider.make_request("anvil_setCode", [address, "0x"])
        assert "error" not in response, response.get("error")
    assert not web3.eth.get_code(address)
