"""Multi-Chain Orchestrator for coordinating transactions across multiple chains.

This module provides the MultiChainOrchestrator class that manages multiple
ChainExecutor instances and coordinates intent execution across chains.

Key Features:
    - Manages a pool of ChainExecutor instances, one per configured chain
    - Routes intents to the correct chain executor based on intent.chain
    - Supports parallel execution of independent intents across chains
    - Supports sequential execution of dependent intents
    - Per-chain failures are isolated and don't crash unrelated chain executions
    - Aggregates results into unified MultiChainExecutionResult

Example:
    from almanak.framework.execution.multichain import MultiChainOrchestrator

    # Create orchestrator from multi-chain config
    orchestrator = MultiChainOrchestrator.from_config(multi_chain_config)

    # Execute a single intent
    result = await orchestrator.execute(intent)

    # Execute multiple independent intents in parallel
    results = await orchestrator.execute_parallel(intents)

    # Execute dependent intents sequentially
    results = await orchestrator.execute_sequence(intents)
"""

import asyncio
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.execution.chain_executor import ChainExecutor, TransactionExecutionResult
from almanak.framework.execution.config import (
    ConfigurationError,
    MultiChainRuntimeConfig,
)
from almanak.framework.execution.gateway_orchestrator import (
    GatewayExecutionOrchestrator,
    GatewayExecutionResult,
)
from almanak.framework.execution.interfaces import (
    ExecutionError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import (
    AnyIntent,
    Intent,
    InvalidAmountError,
    InvalidChainError,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class ExecutionStatus(StrEnum):
    """Status of an intent execution."""

    PENDING = "pending"
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class IntentExecutionResult:
    """Result of executing a single intent.

    Attributes:
        intent: The original intent that was executed
        chain: Chain the intent was executed on
        status: Execution status
        tx_result: Transaction execution result (if applicable)
        error: Error message if execution failed
        execution_time_ms: Time taken for execution in milliseconds
        intent_id: Unique identifier of the intent
    """

    intent: AnyIntent
    chain: str
    status: ExecutionStatus
    tx_result: TransactionExecutionResult | GatewayExecutionResult | None = None
    error: str | None = None
    execution_time_ms: float = 0.0
    intent_id: str = ""

    def __post_init__(self) -> None:
        """Set intent_id from intent if not provided."""
        if not self.intent_id and hasattr(self.intent, "intent_id"):
            self.intent_id = self.intent.intent_id

    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self.status == ExecutionStatus.SUCCESS

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "intent_id": self.intent_id,
            "chain": self.chain,
            "status": self.status.value,
            "tx_result": self.tx_result.to_dict() if self.tx_result else None,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
            "intent_type": self.intent.intent_type.value,
        }


@dataclass
class MultiChainExecutionResult:
    """Aggregated result of executing intents across multiple chains.

    Attributes:
        results: List of individual intent execution results
        success: Overall success (True if all intents succeeded)
        total_execution_time_ms: Total time for all executions
        chains_used: Set of chains that were used
        errors_by_chain: Mapping of chain to error messages
        created_at: Timestamp when execution started
        execution_id: Unique identifier for this execution batch
    """

    results: list[IntentExecutionResult]
    success: bool = field(init=False)
    total_execution_time_ms: float = 0.0
    chains_used: set[str] = field(default_factory=set)
    errors_by_chain: dict[str, list[str]] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        """Compute success and aggregate errors."""
        # Success only if all results are successful
        self.success = all(r.success for r in self.results) if self.results else True

        # Aggregate chains used
        self.chains_used = {r.chain for r in self.results}

        # Aggregate errors by chain
        for result in self.results:
            if result.error:
                if result.chain not in self.errors_by_chain:
                    self.errors_by_chain[result.chain] = []
                self.errors_by_chain[result.chain].append(result.error)

    @property
    def successful_count(self) -> int:
        """Count of successful executions."""
        return sum(1 for r in self.results if r.success)

    @property
    def failed_count(self) -> int:
        """Count of failed executions."""
        return sum(1 for r in self.results if not r.success)

    def get_results_for_chain(self, chain: str) -> list[IntentExecutionResult]:
        """Get all results for a specific chain.

        Args:
            chain: Chain name

        Returns:
            List of results for that chain
        """
        chain_lower = chain.lower()
        return [r for r in self.results if r.chain == chain_lower]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "execution_id": self.execution_id,
            "success": self.success,
            "successful_count": self.successful_count,
            "failed_count": self.failed_count,
            "total_execution_time_ms": self.total_execution_time_ms,
            "chains_used": list(self.chains_used),
            "errors_by_chain": self.errors_by_chain,
            "created_at": self.created_at.isoformat(),
            "results": [r.to_dict() for r in self.results],
        }


# =============================================================================
# Exceptions
# =============================================================================


class MultiChainExecutionError(ExecutionError):
    """Raised when multi-chain execution encounters an error.

    Attributes:
        chain: Chain where error occurred (if applicable)
        intent_id: Intent that caused the error (if applicable)
        reason: Human-readable explanation
    """

    def __init__(
        self,
        reason: str,
        chain: str | None = None,
        intent_id: str | None = None,
    ) -> None:
        self.reason = reason
        self.chain = chain
        self.intent_id = intent_id
        msg = f"Multi-chain execution error: {reason}"
        if chain:
            msg = f"[{chain}] {msg}"
        if intent_id:
            msg = f"{msg} (intent: {intent_id[:8]}...)"
        super().__init__(msg)


# =============================================================================
# Multi-Chain Orchestrator
# =============================================================================


class MultiChainOrchestrator:
    """Coordinates transaction execution across multiple blockchain networks.

    The MultiChainOrchestrator manages a pool of ChainExecutor instances and
    provides methods for executing intents on the appropriate chains. It handles:

    - Routing intents to the correct chain based on intent.chain field
    - Parallel execution of independent intents across different chains
    - Sequential execution of dependent intents with proper ordering
    - Error isolation so per-chain failures don't crash unrelated chains
    - Aggregation of results into unified MultiChainExecutionResult

    DESIGN PRINCIPLES:
    - Each chain has exactly one ChainExecutor instance
    - Executors are lazily initialized on first use
    - Per-chain failures are isolated and logged but don't halt other chains
    - All execution methods return results (never raise for intent failures)

    Example:
        # Create from config
        config = MultiChainRuntimeConfig(
            chains=['arbitrum', 'optimism'],
            protocols={'arbitrum': ['aave_v3'], 'optimism': ['uniswap_v3']},
            private_key="0x...",
        )
        orchestrator = MultiChainOrchestrator.from_config(config)

        # Execute single intent
        swap_intent = Intent.swap("USDC", "ETH", amount=Decimal("100"), chain="arbitrum")
        result = await orchestrator.execute(swap_intent)

        # Execute parallel intents on different chains
        intents = [
            Intent.supply("aave_v3", "WETH", Decimal("1"), chain="arbitrum"),
            Intent.swap("USDC", "ETH", amount=Decimal("100"), chain="optimism"),
        ]
        results = await orchestrator.execute_parallel(intents)

        # Execute sequential dependent intents
        sequence = [
            Intent.swap("USDC", "WETH", amount=Decimal("100"), chain="base"),
            Intent.supply("aave_v3", "WETH", Decimal("0.05"), chain="arbitrum"),
        ]
        results = await orchestrator.execute_sequence(sequence)
    """

    def __init__(
        self,
        config: MultiChainRuntimeConfig | None = None,
        *,
        _gateway_client: "GatewayClient | None" = None,
        _chains: list[str] | None = None,
        _wallet_address: str | None = None,
        _primary_chain: str | None = None,
        _max_gas_price_gwei: int = 0,
        chain_wallets: dict[str, str] | None = None,
    ) -> None:
        """Initialize the MultiChainOrchestrator.

        Use from_config() for config-based mode or from_gateway() for gateway mode.

        Args:
            config: Multi-chain runtime configuration (config mode)
            _gateway_client: GatewayClient for gateway mode (internal, use from_gateway)
            _chains: Chains for gateway mode (internal)
            _wallet_address: Wallet address for gateway mode (internal)
            _primary_chain: Primary chain for gateway mode (internal)
            _max_gas_price_gwei: Gas price cap for gateway mode (internal)
        """
        # Gateway mode
        self._use_gateway = _gateway_client is not None
        self._gateway_client = _gateway_client
        self._gateway_orchestrators: dict[str, GatewayExecutionOrchestrator] = {}

        # Config mode (legacy)
        self._config = config
        self._executors: dict[str, ChainExecutor] = {}
        self._compilers: dict[str, IntentCompiler] = {}
        self._compiler_locks: dict[str, asyncio.Lock] = {}
        self._initialized = False

        # Per-chain wallet overrides from wallet registry
        self._gw_chain_wallets = chain_wallets

        # Shared state for both modes
        if self._use_gateway:
            self._gw_chains = [c.lower() for c in (_chains or [])]
            self._gw_wallet_address = _wallet_address or ""
            self._gw_primary_chain = (_primary_chain or self._gw_chains[0]).lower() if self._gw_chains else ""
            self._gw_max_gas_price_gwei = _max_gas_price_gwei
            wallet_display = self._gw_wallet_address[:10] if self._gw_wallet_address else "unknown"
            logger.info(
                f"MultiChainOrchestrator created (gateway mode): chains={self._gw_chains}, wallet={wallet_display}..."
            )
        else:
            if config is None:
                raise ConfigurationError(field="config", reason="Either config or gateway_client must be provided")
            logger.info(
                f"MultiChainOrchestrator created: chains={config.chains}, wallet={config.wallet_address[:10]}..."
            )

    @classmethod
    def from_config(cls, config: MultiChainRuntimeConfig) -> "MultiChainOrchestrator":
        """Create orchestrator from configuration.

        Args:
            config: Multi-chain runtime configuration

        Returns:
            MultiChainOrchestrator instance
        """
        return cls(config)

    @classmethod
    def from_gateway(
        cls,
        gateway_client: "GatewayClient",
        chains: list[str],
        wallet_address: str,
        primary_chain: str | None = None,
        max_gas_price_gwei: int = 0,
        chain_wallets: dict[str, str] | None = None,
    ) -> "MultiChainOrchestrator":
        """Create orchestrator backed by the gateway.

        In gateway mode, all compilation and execution is routed through
        GatewayExecutionOrchestrator instances (one per chain), which
        delegate to the gateway sidecar via gRPC.

        Args:
            gateway_client: Connected GatewayClient instance
            chains: List of chain names to support
            wallet_address: Wallet address (derived from gateway private key)
            primary_chain: Default chain (first chain if not specified)
            max_gas_price_gwei: Gas price cap (0 = use gateway default)
            chain_wallets: Per-chain wallet addresses from wallet registry

        Returns:
            MultiChainOrchestrator in gateway mode
        """
        return cls(
            config=None,
            _gateway_client=gateway_client,
            _chains=chains,
            _wallet_address=wallet_address,
            _primary_chain=primary_chain,
            _max_gas_price_gwei=max_gas_price_gwei,
            chain_wallets=chain_wallets,
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def chains(self) -> list[str]:
        """Get list of configured chains."""
        if self._use_gateway:
            return self._gw_chains
        assert self._config is not None
        return self._config.chains

    @property
    def primary_chain(self) -> str:
        """Get the primary (default) chain."""
        if self._use_gateway:
            return self._gw_primary_chain
        assert self._config is not None
        return self._config.primary_chain

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        if self._use_gateway:
            return self._gw_wallet_address
        assert self._config is not None
        return self._config.wallet_address

    # =========================================================================
    # Executor Management
    # =========================================================================

    def _get_executor(self, chain: str) -> ChainExecutor:
        """Get or create the ChainExecutor for a chain.

        Args:
            chain: Chain name

        Returns:
            ChainExecutor for the chain

        Raises:
            ConfigurationError: If chain is not configured
        """
        assert self._config is not None, "Config required for _get_executor (config mode only)"
        chain_lower = chain.lower()

        if chain_lower not in self._config.chains:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self._config.chains}",
            )

        if chain_lower not in self._executors:
            # Create new executor for this chain
            rpc_url = self._config.get_rpc_url(chain_lower)
            self._executors[chain_lower] = ChainExecutor(
                chain=chain_lower,
                rpc_url=rpc_url,
                private_key=self._config.private_key,
                max_gas_price_gwei=self._config.max_gas_price_gwei,
                tx_timeout_seconds=self._config.tx_timeout_seconds,
                max_retries=self._config.max_retries,
                base_retry_delay=self._config.base_retry_delay,
                max_retry_delay=self._config.max_retry_delay,
                safe_signer=self._config.safe_signer,
            )
            logger.debug(f"Created ChainExecutor for {chain_lower} (safe_mode={self._config.is_safe_mode})")

        return self._executors[chain_lower]

    def _get_compiler(self, chain: str) -> IntentCompiler:
        """Get or create the IntentCompiler for a chain.

        Uses the execution_address (Safe wallet in Safe mode, EOA otherwise)
        for compiling intents, as this is the address that will execute txs.

        Args:
            chain: Chain name

        Returns:
            IntentCompiler for the chain
        """
        assert self._config is not None, "Config required for _get_compiler (config mode only)"
        chain_lower = chain.lower()

        if chain_lower not in self._compilers:
            rpc_url = self._config.get_rpc_url(chain_lower)
            # Determine default protocol based on chain
            default_protocol = "uniswap_v3"
            if chain_lower == "arbitrum":
                default_protocol = "uniswap_v3"  # or could be gmx for perps

            # Use execution_address so that Enso/protocols see the correct sender
            # (Safe address in Safe mode, EOA otherwise)
            # Per-chain wallet from registry takes precedence
            effective_wallet = self._config.execution_address
            if self._gw_chain_wallets and chain_lower in self._gw_chain_wallets:
                effective_wallet = self._gw_chain_wallets[chain_lower]
            self._compilers[chain_lower] = IntentCompiler(
                chain=chain_lower,
                wallet_address=effective_wallet,
                default_protocol=default_protocol,
                rpc_url=rpc_url,
                config=IntentCompilerConfig(allow_placeholder_prices=True),
                chain_wallets=self._gw_chain_wallets,
            )
            logger.debug(f"Created IntentCompiler for {chain_lower} (wallet={self._config.execution_address[:10]}...)")

        return self._compilers[chain_lower]

    def _get_gateway_orchestrator(self, chain: str) -> GatewayExecutionOrchestrator:
        """Get or create GatewayExecutionOrchestrator for a chain.

        Args:
            chain: Chain name

        Returns:
            GatewayExecutionOrchestrator for the chain

        Raises:
            ConfigurationError: If chain is not in configured chains
        """
        assert self._gateway_client is not None, "Gateway client required for _get_gateway_orchestrator"
        chain_lower = chain.lower()

        if chain_lower not in self._gw_chains:
            raise ConfigurationError(
                field="chain",
                reason=f"Chain '{chain}' is not configured. Configured chains: {self._gw_chains}",
            )

        if chain_lower not in self._gateway_orchestrators:
            effective_wallet = self._gw_wallet_address
            if self._gw_chain_wallets and chain_lower in self._gw_chain_wallets:
                effective_wallet = self._gw_chain_wallets[chain_lower]
            self._gateway_orchestrators[chain_lower] = GatewayExecutionOrchestrator(
                client=self._gateway_client,
                chain=chain_lower,
                wallet_address=effective_wallet,
                max_gas_price_gwei=self._gw_max_gas_price_gwei,
            )
            logger.debug(f"Created GatewayExecutionOrchestrator for {chain_lower}")

        return self._gateway_orchestrators[chain_lower]

    async def _gateway_compile_and_execute(
        self,
        intent: AnyIntent,
        chain: str,
        price_map: dict[str, str] | None = None,
    ) -> GatewayExecutionResult:
        """Compile and execute intent via gateway.

        Args:
            intent: Intent to compile and execute
            chain: Target chain
            price_map: Token symbol -> USD price string for real pricing

        Returns:
            GatewayExecutionResult from the gateway
        """
        orchestrator = self._get_gateway_orchestrator(chain)
        bundle = await orchestrator.compile_intent(intent, price_map=price_map)
        return await orchestrator.execute(bundle)

    async def _compile_and_execute_intent(
        self,
        intent: AnyIntent,
        executor: ChainExecutor,
        price_oracle: dict | None = None,
    ) -> TransactionExecutionResult:
        """Compile an intent and execute ALL transactions in the bundle.

        This method compiles intents using the IntentCompiler and executes
        all transactions in the resulting bundle (e.g., approve + swap).

        SAFE MODE BEHAVIOR:
        When executor is in Safe mode and there are multiple transactions,
        they are executed ATOMICALLY via Safe's MultiSend contract. This ensures
        that approve + swap operations either both succeed or both fail.

        EOA MODE BEHAVIOR:
        When not in Safe mode, transactions are executed sequentially.
        If any transaction fails, remaining transactions are not executed.

        Args:
            intent: Intent to compile
            executor: ChainExecutor for the target chain
            price_oracle: Token symbol -> USD price dict for real pricing.
                If provided, temporarily overrides the compiler's price_oracle.

        Returns:
            TransactionExecutionResult from the execution

        Raises:
            ExecutionError: If compilation fails
        """
        assert self._config is not None, "Config required for _compile_and_execute_intent (config mode only)"
        chain = executor._chain
        compiler = self._get_compiler(chain)

        # Serialize override+compile+restore per cached compiler to
        # prevent execute_parallel() from interleaving price state.
        compiler_lock = self._compiler_locks.setdefault(chain, asyncio.Lock())

        async with compiler_lock:
            original_oracle = compiler.price_oracle
            original_placeholders = compiler._using_placeholders
            if price_oracle:
                compiler.update_prices(price_oracle)

            try:
                result = compiler.compile(intent)
            finally:
                if price_oracle:
                    compiler.restore_prices(original_oracle, original_placeholders)

        if result.status != CompilationStatus.SUCCESS:
            raise ExecutionError(
                f"Intent compilation failed: {result.error} (intent_id={intent.intent_id}, chain={chain})"
            )

        if not result.action_bundle or not result.action_bundle.transactions:
            raise ExecutionError(f"Compilation produced no transactions (intent_id={intent.intent_id}, chain={chain})")

        transactions = result.action_bundle.transactions
        logger.info(f"Compiled intent {intent.intent_id[:8]}... to {len(transactions)} transaction(s)")

        # Build unsigned transactions from compiled data
        unsigned_txs: list[UnsignedTransaction] = []
        gas_params = await executor.get_gas_params()

        for _i, tx_data in enumerate(transactions):
            # Build the unsigned transaction
            gas_limit = tx_data.get("gas_estimate", 500000)
            if isinstance(gas_limit, str):
                gas_limit = int(gas_limit, 16) if gas_limit.startswith("0x") else int(gas_limit)

            # Ensure value is int
            value = tx_data.get("value", 0)
            if isinstance(value, str):
                value = int(value, 16) if value.startswith("0x") else int(value)

            unsigned_tx = UnsignedTransaction(
                to=tx_data["to"],
                value=value,
                data=tx_data.get("data", "0x"),
                chain_id=executor._chain_id,
                gas_limit=int(gas_limit * 1.2),  # Add 20% buffer
                tx_type=TransactionType.EIP_1559,
                from_address=self._config.execution_address,
                max_fee_per_gas=gas_params["max_fee_per_gas"],
                max_priority_fee_per_gas=gas_params["max_priority_fee_per_gas"],
            )
            unsigned_txs.append(unsigned_tx)

        # SAFE MODE: Execute all transactions atomically via MultiSend
        if executor.is_safe_mode and len(unsigned_txs) > 1:
            logger.info(f"Safe mode: bundling {len(unsigned_txs)} transactions atomically via MultiSend")
            return await executor.execute_bundle(unsigned_txs)

        # EOA MODE (or single tx in Safe mode): Execute transactions sequentially
        last_result: TransactionExecutionResult | None = None

        for i, unsigned_tx in enumerate(unsigned_txs):
            tx_desc = transactions[i].get("description", f"tx {i + 1}/{len(unsigned_txs)}")
            logger.info(f"Executing {tx_desc}")

            # Get nonce for this specific transaction
            nonce = await executor.get_next_nonce()
            unsigned_tx.nonce = nonce

            logger.info(
                f"Transaction {i + 1}/{len(unsigned_txs)}: "
                f"to={(unsigned_tx.to or '')[:10]}..., gas={unsigned_tx.gas_limit}, nonce={nonce}"
            )

            # Execute this transaction
            tx_result = await executor.execute_transaction(unsigned_tx)

            if not tx_result.success:
                logger.error(f"Transaction {i + 1}/{len(unsigned_txs)} failed: {tx_result.error}")
                return tx_result

            logger.info(f"Transaction {i + 1}/{len(unsigned_txs)} confirmed: {tx_result.tx_hash}")
            last_result = tx_result

        if last_result is None:
            raise ExecutionError("No transactions were executed")
        return last_result

    async def initialize(self) -> None:
        """Initialize all chain executors and verify connections.

        In gateway mode, creates GatewayExecutionOrchestrator instances for
        all configured chains. In config mode, creates ChainExecutors and
        verifies RPC connections.

        Raises:
            MultiChainExecutionError: If any chain fails to initialize
        """
        if self._initialized:
            return

        errors: list[str] = []

        if self._use_gateway:
            assert self._gateway_client is not None, "Gateway client required in gateway mode"
            # Verify gateway connectivity before creating orchestrators
            try:
                if not self._gateway_client.health_check():
                    raise MultiChainExecutionError(reason="Gateway health check failed")
            except MultiChainExecutionError:
                raise
            except Exception as e:
                raise MultiChainExecutionError(reason=f"Gateway connectivity check failed: {e}") from e

            for chain in self._gw_chains:
                try:
                    self._get_gateway_orchestrator(chain)
                except Exception as e:
                    errors.append(f"{chain}: {e}")
        else:
            assert self._config is not None, "Config required in config mode"
            for chain in self._config.chains:
                try:
                    executor = self._get_executor(chain)
                    if not await executor.check_connection():
                        errors.append(f"{chain}: Failed to connect to RPC")
                except Exception as e:
                    errors.append(f"{chain}: {e}")

        if errors:
            raise MultiChainExecutionError(reason=f"Failed to initialize chains: {'; '.join(errors)}")

        self._initialized = True
        logger.info(f"MultiChainOrchestrator initialized: {len(self.chains)} chains ready")

    # =========================================================================
    # Intent Resolution
    # =========================================================================

    def _resolve_chain(self, intent: AnyIntent) -> str:
        """Resolve the target chain for an intent.

        Args:
            intent: The intent to resolve chain for

        Returns:
            Resolved chain name (lowercase)

        Raises:
            InvalidChainError: If intent's chain is not configured
        """
        return Intent.validate_chain(
            intent,
            self.chains,
            self.primary_chain,
        )

    # =========================================================================
    # Single Intent Execution
    # =========================================================================

    async def execute(
        self,
        intent: AnyIntent,
        build_tx_func: Any | None = None,
        price_map: dict[str, str] | None = None,
        price_oracle: dict | None = None,
    ) -> IntentExecutionResult:
        """Execute a single intent on the appropriate chain.

        Routes the intent to the correct ChainExecutor based on the intent's
        chain field. If no chain is specified, uses the primary chain.

        Args:
            intent: The intent to execute
            build_tx_func: Optional function to build transaction from intent.
                          If not provided, a placeholder transaction is used.
                          Signature: async (intent, executor) -> UnsignedTransaction
            price_map: Token symbol -> USD price string for gateway compilation.
                Used only in gateway mode.
            price_oracle: Token symbol -> Decimal price dict for local compilation.
                Used only in config mode.

        Returns:
            IntentExecutionResult with execution details

        Note:
            This method never raises for intent execution failures. Instead,
            it returns an IntentExecutionResult with status=FAILED and error set.
        """
        start_time = datetime.now(UTC)

        try:
            # Resolve target chain
            chain = self._resolve_chain(intent)

            tx_result: TransactionExecutionResult | GatewayExecutionResult
            if self._use_gateway:
                # Gateway mode: compile and execute via gateway
                if build_tx_func:
                    logger.warning("build_tx_func is ignored in gateway mode")
                tx_result = await self._gateway_compile_and_execute(intent, chain, price_map=price_map)
            else:
                # Config mode: use local ChainExecutor
                executor = self._get_executor(chain)
                if build_tx_func:
                    unsigned_tx = await build_tx_func(intent, executor)
                    tx_result = await executor.execute_transaction(unsigned_tx)
                else:
                    tx_result = await self._compile_and_execute_intent(intent, executor, price_oracle=price_oracle)

            # Calculate execution time
            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            if tx_result.success:
                tx_hash_display = getattr(tx_result, "tx_hash", None) or ""
                if tx_hash_display:
                    tx_hash_display = tx_hash_display[:16] + "..."
                logger.info(
                    f"Intent executed successfully: chain={chain}, "
                    f"intent_id={intent.intent_id[:8]}..., tx_hash={tx_hash_display}"
                )
                return IntentExecutionResult(
                    intent=intent,
                    chain=chain,
                    status=ExecutionStatus.SUCCESS,
                    tx_result=tx_result,
                    execution_time_ms=execution_time,
                )
            else:
                error = getattr(tx_result, "error", None) or "Unknown error"
                logger.warning(
                    f"Intent execution failed: chain={chain}, intent_id={intent.intent_id[:8]}..., error={error}"
                )
                return IntentExecutionResult(
                    intent=intent,
                    chain=chain,
                    status=ExecutionStatus.FAILED,
                    tx_result=tx_result,
                    error=error,
                    execution_time_ms=execution_time,
                )

        except InvalidChainError as e:
            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            logger.error(f"Invalid chain for intent: {e}")
            return IntentExecutionResult(
                intent=intent,
                chain=str(e.chain),
                status=ExecutionStatus.FAILED,
                error=str(e),
                execution_time_ms=execution_time,
            )

        except Exception as e:
            execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            chain = Intent.get_chain(intent) or self.primary_chain
            logger.exception(f"Unexpected error executing intent on {chain}")
            return IntentExecutionResult(
                intent=intent,
                chain=chain,
                status=ExecutionStatus.FAILED,
                error=f"Unexpected error: {e}",
                execution_time_ms=execution_time,
            )

    # =========================================================================
    # Parallel Execution
    # =========================================================================

    async def execute_parallel(
        self,
        intents: Sequence[AnyIntent],
        build_tx_func: Any | None = None,
        price_map: dict[str, str] | None = None,
        price_oracle: dict | None = None,
    ) -> MultiChainExecutionResult:
        """Execute multiple independent intents in parallel.

        Executes all intents concurrently. This is suitable for intents that
        have no dependencies on each other. Per-chain failures do not affect
        executions on other chains.

        Args:
            intents: Sequence of intents to execute in parallel
            build_tx_func: Optional function to build transactions from intents
            price_map: Token symbol -> USD price string for gateway compilation
            price_oracle: Token symbol -> Decimal price dict for local compilation

        Returns:
            MultiChainExecutionResult aggregating all execution results

        Note:
            Intents are truly executed in parallel using asyncio.gather.
            This provides maximum throughput when intents are independent.
        """
        start_time = datetime.now(UTC)

        if not intents:
            return MultiChainExecutionResult(
                results=[],
                total_execution_time_ms=0.0,
            )

        # Execute all intents in parallel
        tasks = [
            self.execute(intent, build_tx_func, price_map=price_map, price_oracle=price_oracle) for intent in intents
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Calculate total execution time
        total_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

        result = MultiChainExecutionResult(
            results=list(results),
            total_execution_time_ms=total_time,
        )

        logger.info(
            f"Parallel execution complete: {result.successful_count}/{len(results)} succeeded, "
            f"chains={list(result.chains_used)}, time={total_time:.0f}ms"
        )

        return result

    # =========================================================================
    # Sequential Execution
    # =========================================================================

    async def execute_sequence(
        self,
        intents: Sequence[AnyIntent],
        build_tx_func: Any | None = None,
        stop_on_failure: bool = True,
        price_map: dict[str, str] | None = None,
        price_oracle: dict | None = None,
    ) -> MultiChainExecutionResult:
        """Execute intents sequentially with proper ordering and amount chaining.

        Executes intents one at a time in order. This is suitable for intents
        that have dependencies where later intents depend on earlier ones.

        When an intent uses amount="all", the actual received amount from the
        previous step (post-slippage, post-fees) is resolved at execution time
        and used for the current step.

        Args:
            intents: Sequence of intents to execute in order
            build_tx_func: Optional function to build transactions from intents
            stop_on_failure: If True, stop execution on first failure.
                           If False, continue with remaining intents but mark
                           them as SKIPPED if a dependency failed.

        Returns:
            MultiChainExecutionResult aggregating all execution results

        Raises:
            InvalidAmountError: If amount="all" is used on the first step

        Note:
            When stop_on_failure=True and an intent fails, remaining intents
            are not executed. When stop_on_failure=False, execution continues
            but subsequent intents are marked as SKIPPED (NOT executed).
        """
        from decimal import Decimal

        start_time = datetime.now(UTC)

        if not intents:
            return MultiChainExecutionResult(
                results=[],
                total_execution_time_ms=0.0,
            )

        # Convert to list for indexed access
        intents_list = list(intents)

        # Validate: amount="all" cannot be used on first step
        first_intent = intents_list[0]
        if Intent.has_chained_amount(first_intent):
            intent_type = first_intent.intent_type.value if hasattr(first_intent, "intent_type") else "Unknown"
            raise InvalidAmountError(
                intent_type=intent_type,
                reason="amount='all' cannot be used on the first step of a sequence "
                "because there is no previous step output to reference",
            )

        results: list[IntentExecutionResult] = []
        failed = False
        previous_amount_received: Decimal | None = None

        for _i, intent in enumerate(intents_list):
            if failed:
                # Previous intent failed - skip or stop
                if stop_on_failure:
                    break
                else:
                    # Mark as skipped - do NOT execute
                    chain = Intent.get_chain(intent) or self.primary_chain
                    results.append(
                        IntentExecutionResult(
                            intent=intent,
                            chain=chain,
                            status=ExecutionStatus.SKIPPED,
                            error="Skipped due to previous failure in sequence",
                        )
                    )
                    continue

            # Resolve amount="all" if needed
            intent_to_execute = intent
            if Intent.has_chained_amount(intent):
                if previous_amount_received is None:
                    # This shouldn't happen if validation passed, but be safe
                    chain = Intent.get_chain(intent) or self.primary_chain
                    results.append(
                        IntentExecutionResult(
                            intent=intent,
                            chain=chain,
                            status=ExecutionStatus.FAILED,
                            error="amount='all' used but no previous step amount available",
                        )
                    )
                    failed = True
                    continue

                # Resolve the amount to the actual received value
                logger.info(
                    f"Resolving amount='all' to {previous_amount_received} for intent {intent.intent_id[:8]}..."
                )
                intent_to_execute = Intent.set_resolved_amount(intent, previous_amount_received)

            # Execute the intent
            result = await self.execute(
                intent_to_execute, build_tx_func, price_map=price_map, price_oracle=price_oracle
            )
            results.append(result)

            if not result.success:
                failed = True
                logger.warning(
                    f"Sequential execution failed at intent {result.intent_id[:8]}..., "
                    f"chain={result.chain}: {result.error}"
                )
            else:
                # Track the amount received from this step for potential chaining
                # In production, this would come from the transaction result
                # For now, we track it from the tx_result if available
                if result.tx_result and hasattr(result.tx_result, "actual_amount_received"):
                    previous_amount_received = result.tx_result.actual_amount_received
                else:
                    # Fallback: use the intent's amount if available
                    amount_field = Intent.get_amount_field(intent_to_execute)
                    if amount_field is not None and isinstance(amount_field, Decimal):
                        previous_amount_received = amount_field
                    else:
                        # No amount tracking available
                        previous_amount_received = None

        # Calculate total execution time
        total_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

        exec_result = MultiChainExecutionResult(
            results=results,
            total_execution_time_ms=total_time,
        )

        logger.info(
            f"Sequential execution complete: {exec_result.successful_count}/{len(intents)} succeeded, "
            f"chains={list(exec_result.chains_used)}, time={total_time:.0f}ms"
        )

        return exec_result

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def check_chain_health(self) -> dict[str, bool]:
        """Check health of all configured chains.

        Returns:
            Dict mapping chain name to health status (True = healthy)
        """
        health: dict[str, bool] = {}

        if self._use_gateway:
            assert self._gateway_client is not None, "Gateway client required in gateway mode"
            from almanak.gateway.proto import gateway_pb2

            for chain in self._gw_chains:
                try:
                    response = await asyncio.to_thread(
                        self._gateway_client.rpc.Call,
                        gateway_pb2.RpcRequest(chain=chain, method="eth_blockNumber", params="[]"),
                        timeout=10.0,
                    )
                    health[chain] = response.success
                except Exception:
                    health[chain] = False
        else:
            assert self._config is not None, "Config required in config mode"
            for chain in self._config.chains:
                try:
                    executor = self._get_executor(chain)
                    health[chain] = await executor.check_connection()
                except Exception:
                    health[chain] = False

        return health

    async def get_balances(self) -> dict[str, int]:
        """Get native token balance on all configured chains.

        Returns:
            Dict mapping chain name to balance in wei
        """
        balances: dict[str, int] = {}

        if self._use_gateway:
            assert self._gateway_client is not None, "Gateway client required in gateway mode"
            from almanak.gateway.proto import gateway_pb2
            from almanak.gateway.services.onchain_lookup import NATIVE_TOKEN_INFO

            for chain in self._gw_chains:
                try:
                    native_symbol = NATIVE_TOKEN_INFO.get(chain, {}).get("symbol", "ETH")
                    effective_wallet = self._gw_wallet_address
                    if self._gw_chain_wallets and chain in self._gw_chain_wallets:
                        effective_wallet = self._gw_chain_wallets[chain]
                    response = await asyncio.to_thread(
                        self._gateway_client.market.GetBalance,
                        gateway_pb2.BalanceRequest(
                            token=native_symbol,
                            chain=chain,
                            wallet_address=effective_wallet,
                        ),
                        timeout=10.0,
                    )
                    balances[chain] = int(response.raw_balance) if response.raw_balance else 0
                except Exception as e:
                    logger.warning(f"Failed to get balance on {chain}: {e}")
                    balances[chain] = 0
        else:
            assert self._config is not None, "Config required in config mode"
            for chain in self._config.chains:
                try:
                    executor = self._get_executor(chain)
                    balances[chain] = await executor.get_balance()
                except Exception as e:
                    logger.warning(f"Failed to get balance on {chain}: {e}")
                    balances[chain] = 0

        return balances

    @property
    def chain_wallets(self) -> dict[str, str] | None:
        """Get per-chain wallet overrides from wallet registry."""
        return self._gw_chain_wallets

    def __repr__(self) -> str:
        """Return string representation."""
        mode = "gateway" if self._use_gateway else "config"
        return f"MultiChainOrchestrator(chains={self.chains}, wallet={self.wallet_address[:10]}..., mode={mode})"

    def __str__(self) -> str:
        """Return string representation."""
        return f"MultiChainOrchestrator({self.chains})"


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "MultiChainOrchestrator",
    "MultiChainExecutionResult",
    "IntentExecutionResult",
    "ExecutionStatus",
    "MultiChainExecutionError",
]
