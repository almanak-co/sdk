"""Gateway-owned sequential simulation against a pinned execution-node state."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import threading
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.core.rpc_network import Network
from almanak.framework.execution.interfaces import SimulationError, SimulationResult, Simulator, UnsignedTransaction
from almanak.gateway.utils.rpc_provider import get_cached_web3

logger = logging.getLogger(__name__)

# Nitro's virtual NodeInterface; gasEstimateL1Component(address,bool,bytes).
_NODE_INTERFACE = "0x00000000000000000000000000000000000000C8"
_GAS_ESTIMATE_L1_COMPONENT = "0x77d488a2"


def requires_node_simulation(chain: str, network: Network) -> bool:
    """Whether ``auto`` must simulate on the chain's own node.

    True for a live EVM chain that no vendor covers but whose node declares
    ``eth_simulateV1``. The vendor-less framework fallback cannot snapshot a
    live node, so it refuses every dependent bundle there. Keyed on declared
    capability, never on credentials: a vendor-covered chain with missing
    credentials must stay loud. A managed fork keeps the snapshotting local
    simulator.
    """
    if network is Network.ANVIL:
        return False
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.family is not ChainFamily.EVM:
        return False
    profile = descriptor.simulation
    return profile.node_simulate_v1 and not (profile.tenderly_supported or profile.alchemy_network)


def create_gateway_simulator(*, config: Any, rpc_url: str, chain: str, network: Network) -> Simulator:
    """Select the node backend when configured or when it is the only one covering the chain."""
    from almanak.framework.execution.simulator import create_simulator

    if config.backend not in ("auto", "rpc"):
        raise ValueError(f"Unknown simulation backend: {config.backend}")
    if config.enabled and (config.backend == "rpc" or requires_node_simulation(chain, network)):
        return GatewayRpcSimulator(chain=chain, network=network, timeout_seconds=config.timeout_seconds)
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

    def _l1_gas(self, txs: list[UnsignedTransaction], number: int) -> list[int]:
        """L1 data gas each call is charged on an Arbitrum-family chain, at the simulation parent.

        Simulation runs with a zero base fee, so its gasUsed omits the poster
        charge that the node adds to intrinsic gas at admission; a limit sized
        from gasUsed alone is refused as "intrinsic gas too low" whenever the
        chain's L1 price is non-zero. The read is required, never defaulted.
        """
        if ChainRegistry.get(self._chain).gas.l1_fee_oracle_kind != "arbitrum_nodeinterface":
            return [0] * len(txs)
        from eth_abi import encode

        l1_gas = []
        for index, tx in enumerate(txs):
            data = bytes.fromhex(tx.data.removeprefix("0x")) if tx.data else b""
            target = tx.to or "0x" + "0" * 40
            payload = (
                _GAS_ESTIMATE_L1_COMPONENT + encode(["address", "bool", "bytes"], [target, tx.to is None, data]).hex()
            )
            raw = self._rpc("eth_call", [{"to": _NODE_INTERFACE, "data": payload}, hex(number)])
            if not isinstance(raw, str) or re.fullmatch(r"0x[0-9a-fA-F]{192,}", raw) is None:
                raise ValueError(f"L1 gas estimate for call {index} is malformed")
            l1_gas.append(int(raw[2:66], 16))
        return l1_gas

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
        l1_gas = self._l1_gas(txs, number)
        gas = [execution + poster for execution, poster in zip(gas, l1_gas, strict=True)]
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
            "l1_gas": l1_gas,
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
