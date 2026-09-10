"""Gateway-owned sequential simulation against a pinned execution-node state."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import threading
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.core.rpc_network import Network
from almanak.framework.execution.interfaces import SimulationError, SimulationResult, Simulator, UnsignedTransaction
from almanak.gateway.utils.rpc_provider import get_cached_web3

logger = logging.getLogger(__name__)


def create_gateway_simulator(*, config: Any, rpc_url: str, chain: str, network: Network) -> Simulator:
    """Select an explicitly configured node backend without changing auto defaults."""
    from almanak.framework.execution.simulator import create_simulator

    if config.backend == "rpc" and config.enabled:
        return GatewayRpcSimulator(chain=chain, network=network, timeout_seconds=config.timeout_seconds)
    if config.backend not in ("auto", "rpc"):
        raise ValueError(f"Unknown simulation backend: {config.backend}")
    return create_simulator(config=config, rpc_url=rpc_url)


def _quantity(value: Any, name: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value) is None:
        raise ValueError(f"Simulation {name} is not a hex quantity")
    return int(value, 16)


class GatewayRpcSimulator(Simulator):
    """Evaluate ordered calls with eth_simulateV1; never broadcast state setup.

    The node carries each call's actual storage changes into the next call.
    Unsupported endpoints and incomplete responses remain unmeasured failures.
    """

    def __init__(self, *, chain: str, network: Network, timeout_seconds: float = 40) -> None:
        if type(network) is not Network:
            raise TypeError("RPC simulator requires a typed gateway network")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int | float):
            raise ValueError("RPC simulation timeout must be a finite positive number")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("RPC simulation timeout must be a finite positive number")
        self._chain = ChainRegistry.get(chain).name
        self._chain_id = ChainRegistry.get(chain).chain_id
        self._web3 = get_cached_web3(self._chain, network)
        self._timeout = timeout_seconds
        self._lock = asyncio.Lock()
        self._worker_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "rpc_sequential"

    def supports_chain(self, chain: str) -> bool:
        return chain == self._chain

    def _rpc(self, method: str, params: list[Any]) -> Any:
        response = self._web3.provider.make_request(method, params)
        if not isinstance(response, dict):
            raise ValueError(f"{method} returned a malformed RPC envelope")
        if response.get("error") is not None:
            raise SimulationError(f"{method}: {response['error']}")
        if "result" not in response or response["result"] is None:
            raise ValueError(f"{method} returned no result")
        return response["result"]

    def _simulate(self, txs: list[UnsignedTransaction], state_overrides: dict[str, Any] | None) -> SimulationResult:
        header = self._rpc("eth_getBlockByNumber", ["latest", False])
        if not isinstance(header, dict):
            raise ValueError("Simulation parent header is absent")
        number = _quantity(header.get("number"), "parent number")
        parent_hash = header.get("hash")
        if not isinstance(parent_hash, str) or re.fullmatch(r"0x[0-9a-fA-F]{64}", parent_hash) is None:
            raise ValueError("Simulation parent hash is malformed")
        _quantity(header.get("timestamp"), "parent timestamp")
        calls = [{"from": tx.from_address, "to": tx.to, "value": hex(tx.value), "data": tx.data} for tx in txs]
        block: dict[str, Any] = {"calls": calls}
        if state_overrides:
            block["stateOverrides"] = state_overrides
        # Fees/nonces are assigned later by the orchestrator; native transfer
        # balances and contract execution still use the pinned real state.
        payload = {"blockStateCalls": [block], "validation": False}
        response = self._rpc("eth_simulateV1", [payload, hex(number)])
        if not isinstance(response, list) or len(response) != 1 or not isinstance(response[0], dict):
            raise ValueError("Sequential simulation did not return exactly one block")
        simulated_block = response[0]
        if simulated_block.get("parentHash", "").lower() != parent_hash.lower():
            raise ValueError("Sequential simulation used a different parent block")
        results = simulated_block.get("calls")
        if not isinstance(results, list) or len(results) != len(txs):
            raise ValueError("Sequential simulation omitted transaction results")
        gas = []
        failures = []
        logs = []
        for index, result in enumerate(results):
            if not isinstance(result, dict) or result.get("status") not in ("0x0", "0x1"):
                raise ValueError(f"Sequential simulation call {index} has no measured status")
            used = _quantity(result.get("gasUsed"), f"call {index} gas")
            if used <= 0:
                raise ValueError(f"Sequential simulation call {index} has no measured gas")
            gas.append(used)
            if result["status"] == "0x0":
                failures.append(f"call {index}: {result.get('error') or result.get('returnData') or 'reverted'}")
            call_logs = result.get("logs", [])
            if not isinstance(call_logs, list):
                raise ValueError(f"Sequential simulation call {index} logs are malformed")
            logs.extend(call_logs)
        closing = self._rpc("eth_getBlockByNumber", [hex(number), False])
        if not isinstance(closing, dict) or closing.get("hash", "").lower() != parent_hash.lower():
            raise ValueError("Simulation parent was reorganized during evaluation")
        evidence = {
            "method": "eth_simulateV1",
            "parent_block": number,
            "parent_hash": parent_hash.lower(),
            "parent_timestamp": _quantity(header["timestamp"], "parent timestamp"),
            "evaluated_indices": list(range(len(txs))),
            "calls": results,
        }
        logger.info(
            "Sequential RPC simulation: parent_block=%s parent_hash=%s calls=%d",
            number,
            evidence["parent_hash"],
            len(results),
        )
        logger.debug("Sequential RPC simulation evidence: %s", evidence)
        return SimulationResult(
            success=not failures,
            simulated=True,
            gas_estimates=gas,
            revert_reason="; ".join(failures) if failures else None,
            logs=logs,
            simulator_name=self.name,
            evidence=evidence,
        )

    def _simulate_exclusive(
        self, txs: list[UnsignedTransaction], state_overrides: dict[str, Any] | None
    ) -> SimulationResult:
        # Cancelling an asyncio waiter does not stop its synchronous RPC worker.
        # Refuse retries until that worker finishes instead of multiplying calls.
        if not self._worker_lock.acquire(blocking=False):
            raise SimulationError("Previous sequential simulation is still running")
        try:
            return self._simulate(txs, state_overrides)
        finally:
            self._worker_lock.release()

    async def simulate(
        self, txs: list[UnsignedTransaction], chain: str, state_overrides: dict[str, Any] | None = None
    ) -> SimulationResult:
        if not self.supports_chain(chain) or any(tx.chain_id != self._chain_id for tx in txs):
            raise SimulationError("RPC simulation chain differs from gateway binding", recoverable=False)
        if not txs or len(txs) > 64 or any(not tx.from_address for tx in txs):
            raise SimulationError("RPC simulation requires 1–64 calls with explicit senders", recoverable=False)
        async with self._lock:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(self._simulate_exclusive, txs, state_overrides), self._timeout
                )
            except TimeoutError as exc:
                raise SimulationError("Sequential simulation timed out; outcome is unmeasured") from exc
            except SimulationError:
                raise
            except (ValueError, TypeError, AttributeError) as exc:
                raise SimulationError(f"Incomplete sequential simulation: {exc}", recoverable=False) from exc
            except Exception as exc:
                raise SimulationError(f"Sequential simulation unavailable: {exc}") from exc
