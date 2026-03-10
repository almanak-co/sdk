"""Gateway-backed ExecutionOrchestrator implementation.

This module provides an ExecutionOrchestrator that executes transactions through
the gateway sidecar instead of directly signing and submitting. Used in strategy
containers that have no access to private keys.

The GatewayExecutionResult class supports the same enrichment interface as
ExecutionResult, allowing strategy authors to access extracted data directly:

    result = await orchestrator.execute(intent)
    if result.position_id:
        print(f"Position ID: {result.position_id}")
    if result.swap_amounts:
        print(f"Swapped: {result.swap_amounts.amount_out_decimal}")
"""

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from almanak.framework.execution.gas.constants import (
    CHAIN_GRPC_EXECUTE_TIMEOUTS,
    CHAIN_TX_TIMEOUTS,
    DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS,
    DEFAULT_TX_TIMEOUT_SECONDS,
)
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from .extracted_data import LPCloseData, SwapAmounts

logger = logging.getLogger(__name__)


@dataclass
class GatewayExecutionResult:
    """Result from gateway execution.

    Supports the same enrichment interface as ExecutionResult, allowing
    strategy authors to access extracted data (position_id, swap_amounts, etc.)
    directly without manual receipt parsing.

    The ResultEnricher populates these fields after execution:
        - position_id: LP position NFT tokenId (for LP_OPEN intents)
        - swap_amounts: Swap execution data (for SWAP intents)
        - lp_close_data: LP close data (for LP_CLOSE intents)
        - bin_ids: TraderJoe V2 bin IDs (for LP positions)
        - extracted_data: Flexible dict for protocol-specific data

    Example:
        result = await orchestrator.execute(bundle)
        # After enrichment by StrategyRunner:
        if result.position_id:
            strategy.state["position_id"] = result.position_id
    """

    success: bool
    tx_hashes: list[str]
    total_gas_used: int
    receipts: list[dict]
    execution_id: str
    error: str | None = None
    error_code: str | None = None

    @property
    def tx_hash(self) -> str | None:
        """First transaction hash (compatibility with TransactionExecutionResult)."""
        return self.tx_hashes[0] if self.tx_hashes else None

    # === Enriched Data (populated by ResultEnricher) ===
    # These fields mirror ExecutionResult for strategy author UX consistency
    position_id: int | None = None
    swap_amounts: "SwapAmounts | None" = None
    lp_close_data: "LPCloseData | None" = None
    bin_ids: list[int] | None = None
    extracted_data: dict[str, Any] = field(default_factory=dict)
    extraction_warnings: list[str] = field(default_factory=list)

    @property
    def transaction_results(self) -> list:
        """Compatibility property for StrategyRunner and ResultEnricher.

        Converts receipts to TransactionResult-like objects with receipt data
        attached, enabling the ResultEnricher to extract intent-specific data
        (position_id, swap_amounts, etc.) from transaction logs.

        Returns empty list if execution failed.
        """
        if not self.success or not self.receipts:
            return []

        from almanak.framework.execution.interfaces import TransactionReceipt
        from almanak.framework.execution.orchestrator import TransactionResult

        results = []
        if len(self.tx_hashes) < len(self.receipts):
            logger.warning(
                "tx_hashes/receipts length mismatch: %d tx_hashes vs %d receipts. "
                "Some TransactionResults will have empty tx_hash.",
                len(self.tx_hashes),
                len(self.receipts),
            )
        for i, receipt_data in enumerate(self.receipts):
            tx_hash = self.tx_hashes[i] if i < len(self.tx_hashes) else ""

            # Normalize receipt values once to ensure consistency between
            # TransactionReceipt and TransactionResult
            if isinstance(receipt_data, dict):
                status = receipt_data.get("status", 1)
                gas_used = receipt_data.get("gas_used", 0)
                logs = receipt_data.get("logs", [])
            else:
                status = 1
                gas_used = 0
                logs = []

            # Build a TransactionReceipt from the gateway receipt data
            # The ResultEnricher needs access to receipt.logs to extract position_id, etc.
            tx_receipt = None
            if isinstance(receipt_data, dict):
                tx_receipt = TransactionReceipt(
                    tx_hash=tx_hash,
                    block_number=receipt_data.get("block_number", 0),
                    block_hash=receipt_data.get("block_hash", ""),
                    gas_used=gas_used,
                    effective_gas_price=receipt_data.get("effective_gas_price", 0),
                    status=status,
                    logs=logs,
                    from_address=receipt_data.get("from_address"),
                    to_address=receipt_data.get("to_address"),
                )

            results.append(
                TransactionResult(
                    success=status == 1,
                    tx_hash=tx_hash,
                    gas_used=gas_used,
                    logs=logs,
                    receipt=tx_receipt,  # Proper TransactionReceipt for ResultEnricher
                )
            )
        return results

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "tx_hashes": self.tx_hashes,
            "total_gas_used": self.total_gas_used,
            "execution_id": self.execution_id,
            "error": self.error,
            "error_code": self.error_code,
        }

    def get_extracted(self, key: str, expected_type: type | None = None, default: Any = None) -> Any:
        """Get extracted data with optional type checking.

        Provides safe access to protocol-specific extracted data with
        optional type validation. Mirrors ExecutionResult interface.

        Args:
            key: Data key to retrieve from extracted_data
            expected_type: Optional type to validate against
            default: Default value if key not found or wrong type

        Returns:
            Extracted value or default

        Example:
            tick_lower = result.get_extracted("tick_lower", int, 0)
            liquidity = result.get_extracted("liquidity")
        """
        value = self.extracted_data.get(key)
        if value is None:
            return default
        if expected_type is not None and not isinstance(value, expected_type):
            return default
        return value


class GatewayExecutionOrchestrator:
    """ExecutionOrchestrator that executes through the gateway.

    This implementation routes all execution requests to the gateway sidecar,
    which has access to private keys and can sign/submit transactions.

    The interface is intentionally simpler than the full ExecutionOrchestrator
    since the complex signing and submission logic lives in the gateway.

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator

        with GatewayClient() as client:
            orchestrator = GatewayExecutionOrchestrator(
                client=client,
                chain="arbitrum",
                wallet_address="0x1234...",
            )
            result = await orchestrator.execute(action_bundle)
            print(f"Execution success: {result.success}")
    """

    def __init__(
        self,
        client: GatewayClient,
        chain: str = "arbitrum",
        wallet_address: str | None = None,
        timeout: float | None = None,
        execute_timeout: float | None = None,
        max_gas_price_gwei: int = 0,
    ):
        """Initialize gateway-backed execution orchestrator.

        Args:
            client: Connected GatewayClient instance
            chain: Chain name for execution
            wallet_address: Wallet address for signing
            timeout: gRPC timeout for CompileIntent/GetTransactionStatus calls in
                seconds. If None, uses chain-specific TX confirmation timeout
                (300s for Ethereum L1, 120s for L2s).
            execute_timeout: gRPC timeout for the Execute call in seconds. Must be
                larger than ``timeout`` to account for gas estimation overhead before
                TX submission. If None, uses chain-specific execute timeout
                (600s for Ethereum, 300s for L2s). Complex intents like LP_CLOSE can
                take 100s+ to estimate gas on Anvil forks; this timeout must cover
                gas estimation + TX confirmation combined.
            max_gas_price_gwei: Gas price cap in gwei (0 = use gateway default).
                Passed to the gateway so the ExecutionOrchestrator enforces the cap.
        """
        self._client = client
        self._chain = chain
        self._wallet_address = wallet_address
        self._timeout = (
            timeout if timeout is not None else CHAIN_TX_TIMEOUTS.get(chain.lower(), DEFAULT_TX_TIMEOUT_SECONDS)
        )
        self._execute_timeout = (
            execute_timeout
            if execute_timeout is not None
            else CHAIN_GRPC_EXECUTE_TIMEOUTS.get(chain.lower(), DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS)
        )
        self._max_gas_price_gwei = max_gas_price_gwei

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

    @property
    def wallet_address(self) -> str | None:
        """Get the wallet address."""
        return self._wallet_address

    async def compile_intent(
        self,
        intent: Any,
        wallet_address: str | None = None,
        price_map: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Compile an intent into an action bundle through gateway.

        Args:
            intent: Intent object to compile
            wallet_address: Override wallet address
            price_map: Token symbol -> USD price string for real pricing.
                If provided, the gateway compiler uses these instead of
                placeholder prices. Values are strings to preserve Decimal precision.

        Returns:
            Action bundle as dictionary

        Raises:
            CompilationError: If compilation fails
        """
        wallet = wallet_address or self._wallet_address
        if not wallet:
            raise ValueError("wallet_address is required")

        try:
            # Serialize intent to JSON
            intent_type = type(intent).__name__.lower().replace("intent", "")
            intent_data = json.dumps(intent.model_dump(mode="json")).encode("utf-8")

            request = gateway_pb2.CompileIntentRequest(
                intent_type=intent_type,
                intent_data=intent_data,
                chain=self._chain,
                wallet_address=wallet,
                price_map=price_map or {},
            )
            response = self._client.execution.CompileIntent(request, timeout=self._timeout)

            if not response.success:
                raise RuntimeError(f"Compilation failed: {response.error} ({response.error_code})")

            # Deserialize action bundle
            return json.loads(response.action_bundle.decode("utf-8"))

        except Exception as e:
            logger.error(f"Gateway compile intent failed: {e}")
            raise

    async def execute(
        self,
        action_bundle: Any,
        context: Any | None = None,
        strategy_id: str = "",
        intent_id: str = "",
        dry_run: bool = False,
        simulation_enabled: bool = True,
        wallet_address: str | None = None,
    ) -> GatewayExecutionResult:
        """Execute an action bundle through gateway.

        Args:
            action_bundle: Action bundle to execute (object or dict)
            context: Optional ExecutionContext for interface compatibility with
                ExecutionOrchestrator. If provided, extracts strategy_id, intent_id,
                dry_run, simulation_enabled, and wallet_address from it.
            strategy_id: Strategy identifier for tracking
            intent_id: Intent identifier for tracking
            dry_run: If True, simulate only without submitting
            simulation_enabled: If True, run simulation before execution
            wallet_address: Override wallet address

        Returns:
            GatewayExecutionResult with tx hashes and receipts

        Raises:
            ExecutionError: If execution fails
        """
        # Extract values from context if provided (interface compatibility)
        if context is not None:
            strategy_id = getattr(context, "strategy_id", "") or strategy_id
            intent_id = getattr(context, "intent_id", "") or intent_id
            dry_run = getattr(context, "dry_run", dry_run)
            simulation_enabled = getattr(context, "simulation_enabled", simulation_enabled)
            wallet_address = getattr(context, "wallet_address", None) or wallet_address

        wallet = wallet_address or self._wallet_address
        if not wallet:
            raise ValueError("wallet_address is required")

        try:
            # Serialize action bundle to JSON
            if hasattr(action_bundle, "to_dict"):
                bundle_dict = action_bundle.to_dict()
            elif hasattr(action_bundle, "model_dump"):
                bundle_dict = action_bundle.model_dump()
            else:
                bundle_dict = action_bundle

            bundle_bytes = json.dumps(bundle_dict).encode("utf-8")

            request = gateway_pb2.ExecuteRequest(
                action_bundle=bundle_bytes,
                dry_run=dry_run,
                simulation_enabled=simulation_enabled,
                strategy_id=strategy_id,
                intent_id=intent_id,
                chain=self._chain,
                wallet_address=wallet,
                max_gas_price_gwei=self._max_gas_price_gwei,
            )
            response = self._client.execution.Execute(request, timeout=self._execute_timeout)

            # Deserialize receipts
            receipts = []
            if response.receipts:
                receipts = json.loads(response.receipts.decode("utf-8"))

            # Extract warnings if available (future-proofing for proto additions)
            extraction_warnings = list(getattr(response, "extraction_warnings", []))

            # Normalize tx_hashes to include 0x prefix
            tx_hashes = [h if h.startswith("0x") else f"0x{h}" for h in response.tx_hashes]

            return GatewayExecutionResult(
                success=response.success,
                tx_hashes=tx_hashes,
                total_gas_used=response.total_gas_used,
                receipts=receipts,
                execution_id=response.execution_id,
                error=response.error if response.error else None,
                error_code=response.error_code if response.error_code else None,
                extraction_warnings=extraction_warnings,
            )

        except Exception as e:
            logger.error(f"Gateway execute failed: {e}")
            return GatewayExecutionResult(
                success=False,
                tx_hashes=[],
                total_gas_used=0,
                receipts=[],
                execution_id="",
                error=str(e),
            )

    async def get_transaction_status(self, tx_hash: str, chain: str | None = None) -> dict[str, Any]:
        """Get transaction status from gateway.

        Args:
            tx_hash: Transaction hash to check
            chain: Chain to query (defaults to orchestrator chain)

        Returns:
            Status dictionary with status, confirmations, block_number
        """
        try:
            request = gateway_pb2.TxStatusRequest(
                tx_hash=tx_hash,
                chain=chain or self._chain,
            )
            response = self._client.execution.GetTransactionStatus(request, timeout=self._timeout)

            return {
                "status": response.status,
                "confirmations": response.confirmations,
                "block_number": response.block_number,
                "gas_used": response.gas_used,
                "error": response.error if response.error else None,
            }

        except Exception as e:
            logger.error(f"Gateway get tx status failed for {tx_hash}: {e}")
            return {"status": "unknown", "error": str(e)}
