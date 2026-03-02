"""Local Simulator - Uses eth_estimateGas against local or remote RPC.

This module provides a simulator implementation that uses the standard
eth_estimateGas RPC method to get accurate gas estimates. This is ideal for:

- Local fork testing (Anvil, Hardhat, Ganache) where you want accurate gas limits
- Any RPC endpoint where you want real gas estimation instead of static estimates
- Testing scenarios where static gas estimates may be insufficient

Unlike Tenderly/Alchemy simulators which simulate against their own state,
LocalSimulator simulates against the actual state of the connected RPC,
making it perfect for Anvil forks where you've modified state (e.g., funded wallets).

Example:
    from almanak.framework.execution.simulator import LocalSimulator
    from almanak.framework.execution.interfaces import UnsignedTransaction

    # For Anvil testing
    simulator = LocalSimulator(rpc_url="http://127.0.0.1:8545")

    unsigned_tx = UnsignedTransaction(
        to="0x1234...",
        value=0,
        data="0x...",
        chain_id=1,
        gas_limit=100000,  # Will be replaced by actual estimate
        from_address="0xYourWallet...",
    )

    result = await simulator.simulate([unsigned_tx], chain="ethereum")
    # result.gas_estimates contains accurate estimates from eth_estimateGas
"""

import logging
from typing import Any

from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.types import RPCEndpoint, TxParams, Wei

from almanak.framework.execution.interfaces import (
    SimulationResult,
    Simulator,
    UnsignedTransaction,
)

logger = logging.getLogger(__name__)


class LocalSimulator(Simulator):
    """Simulator that uses eth_estimateGas against a local or remote RPC.

    This simulator calls eth_estimateGas for each transaction to get accurate
    gas estimates based on the actual state of the connected node. This is
    particularly useful for:

    1. Anvil/Hardhat forks where you've modified state (funded wallets, etc.)
    2. Testing scenarios where static gas estimates are insufficient
    3. Any case where you want the most accurate gas estimation possible

    Key differences from other simulators:
    - DirectSimulator: Returns no gas estimates (pass-through)
    - TenderlySimulator: Simulates against Tenderly's mainnet fork (not your Anvil state)
    - AlchemySimulator: Simulates against Alchemy's state (not your Anvil state)
    - LocalSimulator: Simulates against YOUR RPC's actual state

    Attributes:
        rpc_url: The RPC endpoint to use for gas estimation
        gas_buffer: Backward-compatible constructor argument. LocalSimulator now
                    returns raw gas estimates to avoid double-buffering.

    Example:
        simulator = LocalSimulator(
            rpc_url="http://127.0.0.1:8545",
        )

        result = await simulator.simulate([tx], chain="ethereum")

        if result.success:
            print(f"Gas estimates: {result.gas_estimates}")
        else:
            print(f"Simulation failed: {result.revert_reason}")
    """

    def __init__(
        self,
        rpc_url: str,
        gas_buffer: float = 1.0,
        name: str = "local",
    ) -> None:
        """Initialize the LocalSimulator.

        Args:
            rpc_url: RPC endpoint URL (e.g., "http://127.0.0.1:8545" for Anvil)
            gas_buffer: Multiplier for gas estimates (default 1.0 = raw estimates).
                        The orchestrator's _update_gas_estimate applies the chain-specific
                        buffer, so simulators should return raw gas_used to avoid
                        double-buffering.
            name: Identifier for this simulator instance (default: "local")
        """
        self._rpc_url = rpc_url
        self._gas_buffer = gas_buffer
        self._name = name
        self._web3: AsyncWeb3 | None = None

        logger.info(
            "LocalSimulator initialized",
            extra={
                "simulator_name": self._name,
                "rpc_url": rpc_url,
                "gas_buffer": gas_buffer,
            },
        )
        if gas_buffer != 1.0:
            logger.warning(
                "LocalSimulator gas_buffer is deprecated and ignored. "
                "Returning raw gas estimates; orchestrator applies the single buffer.",
                extra={"gas_buffer": gas_buffer},
            )

    @property
    def name(self) -> str:
        """Return the simulator name."""
        return self._name

    async def _get_web3(self) -> AsyncWeb3:
        """Get or create AsyncWeb3 instance."""
        if self._web3 is None:
            self._web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
        return self._web3

    async def _estimate_gas(
        self,
        tx: UnsignedTransaction,
    ) -> tuple[int, str | None]:
        """Estimate gas for a single transaction.

        Args:
            tx: Transaction to estimate gas for

        Returns:
            Tuple of (gas_estimate, error_message)
            If successful, error_message is None
            If failed, gas_estimate is 0 and error_message contains the reason
        """
        web3 = await self._get_web3()

        try:
            tx_params: TxParams = {
                "value": Wei(tx.value),
                "data": HexBytes(tx.data) if tx.data else HexBytes("0x"),
            }
            if tx.from_address:
                tx_params["from"] = web3.to_checksum_address(tx.from_address)
            if tx.to:
                tx_params["to"] = web3.to_checksum_address(tx.to)

            gas_estimate = await web3.eth.estimate_gas(tx_params)

            logger.debug(
                f"Gas estimated (raw): {gas_estimate}",
                extra={
                    "to": tx.to,
                    "raw_estimate": gas_estimate,
                },
            )

            return gas_estimate, None

        except Exception as e:
            error_str = str(e)
            # Try to extract revert reason from common error formats
            revert_reason = self._parse_revert_reason(error_str)
            logger.warning(
                f"Gas estimation failed: {revert_reason}",
                extra={"to": tx.to, "error": error_str},
            )
            return 0, revert_reason

    def _parse_revert_reason(self, error_str: str) -> str:
        """Parse revert reason from error string.

        Different RPC providers format revert reasons differently.
        This method attempts to extract a clean revert reason.

        Args:
            error_str: Raw error string from RPC

        Returns:
            Cleaned revert reason string
        """
        error_lower = error_str.lower()

        # Check for common revert patterns
        if "execution reverted" in error_lower:
            # Try to extract the reason after "execution reverted:"
            if "execution reverted:" in error_lower:
                idx = error_lower.find("execution reverted:")
                return error_str[idx:].split("\n")[0].strip()
            return "execution reverted"

        if "revert" in error_lower:
            return error_str[:200]  # Truncate long messages

        if "insufficient funds" in error_lower:
            return "insufficient funds for gas * price + value"

        if "nonce too low" in error_lower:
            return "nonce too low"

        if "gas required exceeds allowance" in error_lower:
            return "gas required exceeds allowance"

        # Return truncated original error
        return error_str[:200] if len(error_str) > 200 else error_str

    # Approval call selectors that can safely fall back to compiler gas limit when
    # eth_estimateGas fails (e.g., due to "missing trie node" on Anvil fork).
    # For these, the compiler-provided gas limit is a reliable upper bound.
    _APPROVAL_SELECTORS: frozenset[str] = frozenset(
        {
            "0x095ea7b3",  # ERC20 approve(address,uint256)
            "0xa22cb465",  # ERC1155 setApprovalForAll(address,bool) - standard ERC1155
            "0xe584b654",  # TraderJoe V2 LBPair approveForAll(address,bool) - custom name
        }
    )

    def _is_approve_tx(self, tx: UnsignedTransaction) -> bool:
        """Check if transaction is an approval call.

        Handles ERC20 approve, ERC1155 setApprovalForAll, and TraderJoe V2
        LBPair approveForAll. All are safe to fall back to the compiler-provided
        gas limit when eth_estimateGas fails (e.g., due to missing trie nodes on
        Anvil forks). TraderJoe V2 uses approveForAll (0xe584b654) rather than
        the standard ERC1155 setApprovalForAll (0xa22cb465).
        """
        if not tx.data or len(tx.data) < 10:
            return False
        return tx.data[:10].lower() in self._APPROVAL_SELECTORS

    async def _execute_tx(self, tx: UnsignedTransaction, gas_limit: int) -> tuple[bool, str | None]:
        """Execute a transaction on Anvil and wait for receipt.

        Returns:
            Tuple of (success, error_message)
        """
        web3 = await self._get_web3()
        try:
            tx_params: TxParams = {
                "value": Wei(tx.value),
                "data": HexBytes(tx.data) if tx.data else HexBytes("0x"),
                "gas": gas_limit,
            }
            if tx.from_address:
                tx_params["from"] = web3.to_checksum_address(tx.from_address)
            if tx.to:
                tx_params["to"] = web3.to_checksum_address(tx.to)

            tx_hash = await web3.eth.send_transaction(tx_params)
            receipt = await web3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)

            if receipt["status"] != 1:
                return False, "Transaction reverted"

            logger.debug(f"Executed tx for state setup: {tx_hash.hex()}")
            return True, None

        except Exception as e:
            return False, str(e)

    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Simulate transactions using eth_estimateGas.

        For multi-transaction bundles:
        - Creates an EVM snapshot before simulation
        - Executes transactions sequentially to build up state
        - Reverts to snapshot after simulation (so actual execution starts fresh)

        Args:
            txs: List of unsigned transactions to simulate
            chain: Chain name (used for logging)
            state_overrides: Not supported by LocalSimulator (ignored with warning)

        Returns:
            SimulationResult with gas estimates for each transaction
        """
        tx_count = len(txs)

        if state_overrides:
            logger.warning(
                "LocalSimulator does not support state_overrides - they will be ignored. "
                "Use TenderlySimulator or AlchemySimulator for state override support."
            )

        logger.info(
            f"Simulating {tx_count} transaction(s) via eth_estimateGas",
            extra={
                "simulator_name": self._name,
                "chain": chain,
                "transaction_count": tx_count,
                "rpc_url": self._rpc_url,
            },
        )

        gas_estimates: list[int] = []
        warnings: list[str] = []
        web3 = await self._get_web3()

        # Create snapshot for multi-tx bundles (to restore state after simulation)
        snapshot_id = None
        snapshot_unavailable = False
        if tx_count > 1:
            try:
                result = await web3.provider.make_request(RPCEndpoint("evm_snapshot"), [])
                snapshot_id = result.get("result")
                if snapshot_id is not None:
                    logger.debug(f"Created EVM snapshot: {snapshot_id}")
                else:
                    snapshot_unavailable = True
                    logger.warning(
                        "evm_snapshot returned None - snapshot not supported. "
                        "Proceeding with gas estimation only (no state-mutating execution)."
                    )
            except Exception as e:
                snapshot_unavailable = True
                logger.warning(
                    f"evm_snapshot failed (not Anvil?): {e}. "
                    "Proceeding with gas estimation only (no state-mutating execution)."
                )

            # Add warning to result when snapshot is unavailable for multi-tx bundles
            if snapshot_unavailable:
                warnings.append(
                    "Snapshot unavailable: multi-tx simulation ran without state setup. "
                    "Gas estimates for later transactions may be inaccurate if they depend on "
                    "earlier transactions (e.g., approvals). Consider using an Anvil fork."
                )

        is_multi_tx_bundle = tx_count > 1

        try:
            for i, tx in enumerate(txs):
                is_approve = self._is_approve_tx(tx)
                is_last = i == tx_count - 1

                # For multi-TX bundles, skip estimation for non-first transactions.
                # Subsequent TXs depend on state changes from prior TXs (e.g., approve
                # must execute before addLiquidity/multicall), so eth_estimateGas against
                # the current chain state will revert even though the bundle would succeed
                # when executed sequentially. Use the compiler-provided gas_limit instead.
                # This mirrors the VIB-157 fix for _maybe_estimate_gas_limits().
                if is_multi_tx_bundle and i > 0:
                    fallback_gas = tx.gas_limit if tx.gas_limit and tx.gas_limit > 0 else 300_000
                    gas_estimates.append(fallback_gas)
                    logger.info(
                        f"Transaction {i + 1}/{tx_count}: skipping estimation (multi-TX dependent), "
                        f"using compiler gas_limit={fallback_gas}",
                        extra={"tx_index": i, "to": tx.to},
                    )

                    # Execute non-last txs for state setup (if snapshot available)
                    if not is_last and snapshot_id is not None:
                        success, exec_error = await self._execute_tx(tx, fallback_gas)
                        if not success:
                            logger.warning(f"Failed to execute tx {i + 1} for state setup: {exec_error}")
                            return SimulationResult(
                                success=False,
                                simulated=True,
                                gas_estimates=gas_estimates,
                                revert_reason=f"Transaction {i + 1} execution failed: {exec_error}",
                            )
                    continue

                gas_estimate, error = await self._estimate_gas(tx)

                if error and is_approve:
                    # Approve fallback: if eth_estimateGas fails for approve calls
                    # (e.g., proxy contracts like Avalanche USDC), fall back to the
                    # connector-provided gas_limit from the original transaction.
                    if not tx.gas_limit or tx.gas_limit <= 0:
                        logger.warning(
                            f"Transaction {i + 1}/{tx_count}: approve eth_estimateGas failed "
                            "and tx.gas_limit is missing/zero",
                            extra={"tx_index": i, "to": tx.to},
                        )
                        return SimulationResult(
                            success=False,
                            simulated=True,
                            gas_estimates=gas_estimates,
                            revert_reason=error,
                        )
                    gas_estimate = tx.gas_limit
                    error = None
                    logger.info(
                        f"Transaction {i + 1}/{tx_count}: approve eth_estimateGas failed, "
                        f"using connector-provided gas_limit={gas_estimate}",
                        extra={"tx_index": i, "to": tx.to},
                    )

                if error:
                    logger.warning(
                        f"Simulation failed at transaction {i + 1}/{tx_count}: {error}",
                        extra={"tx_index": i, "to": tx.to, "error": error},
                    )
                    return SimulationResult(
                        success=False,
                        simulated=True,
                        gas_estimates=gas_estimates,
                        revert_reason=error,
                    )

                gas_estimates.append(gas_estimate)
                logger.debug(
                    f"Transaction {i + 1}/{tx_count} estimated: {gas_estimate} gas",
                    extra={"tx_index": i, "to": tx.to, "gas_estimate": gas_estimate},
                )

                # Execute if not last, so subsequent txs can see state changes
                # CRITICAL: Only execute if we have snapshot support to revert later
                # Without snapshot, execution would permanently mutate RPC state
                if not is_last:
                    if snapshot_id is not None:
                        success, error = await self._execute_tx(tx, gas_estimate)
                        if not success:
                            logger.warning(f"Failed to execute tx {i + 1} for state setup: {error}")
                            return SimulationResult(
                                success=False,
                                simulated=True,
                                gas_estimates=gas_estimates,
                                revert_reason=f"Transaction {i + 1} execution failed: {error}",
                            )
                    else:
                        # No snapshot - skip execution to avoid permanent state mutation
                        logger.info(
                            f"Skipping tx execution (no snapshot support) - tx {i + 1}/{tx_count}. "
                            "Subsequent gas estimates may be inaccurate."
                        )

            total_gas = sum(gas_estimates)
            logger.info(
                f"Simulation successful: {tx_count} transaction(s), total gas: {total_gas}",
                extra={
                    "simulator_name": self._name,
                    "chain": chain,
                    "transaction_count": tx_count,
                    "total_gas": total_gas,
                    "gas_estimates": gas_estimates,
                },
            )

            return SimulationResult(
                success=True,
                simulated=True,
                gas_estimates=gas_estimates,
                warnings=warnings,
            )

        finally:
            # Revert to snapshot to restore original state
            if snapshot_id is not None:
                try:
                    await web3.provider.make_request(RPCEndpoint("evm_revert"), [snapshot_id])
                    logger.debug(f"Reverted to snapshot: {snapshot_id}")
                except Exception as e:
                    logger.warning(f"Failed to revert snapshot: {e}")


__all__ = [
    "LocalSimulator",
]
