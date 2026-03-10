"""ExecutionService implementation - handles intent compilation and execution.

This service provides intent compilation and transaction execution for strategy
containers via gRPC. All signing, simulation, and submission happens here in
the gateway; strategy containers never see private keys.
"""

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Any

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    validate_address,
    validate_chain,
    validate_tx_hash,
)

logger = logging.getLogger(__name__)

# TTL for cached compilers (5 minutes) - prevents stale price data in long-running services
COMPILER_CACHE_TTL_SECONDS = 300

# Intent types that require real prices on mainnet (VIB-523).
# Normalized: uppercase, underscores stripped, so both "lp_open" and "lpopen" match.
PRICE_SENSITIVE_INTENT_TYPES = frozenset(
    {
        "SWAP",
        "LPOPEN",
        "LPCLOSE",
        "SUPPLY",
        "REPAY",
        "BORROW",
        "WITHDRAW",
        "PERPOPEN",
        "PERPCLOSE",
    }
)


class ExecutionServiceServicer(gateway_pb2_grpc.ExecutionServiceServicer):
    """Implements ExecutionService gRPC interface.

    Provides intent compilation and execution for strategy containers:
    - CompileIntent: Compile an intent into an action bundle
    - Execute: Sign, submit, and confirm transactions
    - GetTransactionStatus: Check transaction status
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize ExecutionService.

        Args:
            settings: Gateway settings with private keys and RPC config.
        """
        self.settings = settings
        self._orchestrator_cache: dict[str, object] = {}
        self._orchestrator_locks: dict[str, asyncio.Lock] = {}
        self._orchestrator_default_gas_caps: dict[str, int] = {}
        # Cache IntentCompiler per chain/wallet pair with TTL to prevent stale prices
        # Format: {cache_key: (compiler, created_timestamp)}
        self._compiler_cache: dict[str, tuple[object, float]] = {}
        self._compiler_locks: dict[str, asyncio.Lock] = {}
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of execution components."""
        if self._initialized:
            return

        self._initialized = True
        logger.info("ExecutionService initialized")

    def _get_compiler(self, chain: str, wallet_address: str):
        """Get or create IntentCompiler for a chain/wallet pair.

        The IntentCompiler requires chain, wallet_address, and rpc_url to perform
        on-chain queries (allowance checks, balance queries). Each chain/wallet
        combination needs its own compiler instance.

        Compilers are cached with a TTL to avoid expensive re-initialization (RPC
        setup, chain config). Real prices are applied per-request in CompileIntent()
        via the price_map field, so allow_placeholder_prices=True is safe here --
        the cached compiler is just a container for chain/wallet/rpc state.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")
            wallet_address: Wallet address for queries

        Returns:
            IntentCompiler configured for the specified chain/wallet
        """
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        now = time.time()

        # Check cache with TTL
        if cache_key in self._compiler_cache:
            compiler, created_at = self._compiler_cache[cache_key]
            if now - created_at < COMPILER_CACHE_TTL_SECONDS:
                return compiler
            else:
                logger.debug(f"Compiler cache expired for {cache_key}, recreating...")
                del self._compiler_cache[cache_key]

        # Get RPC URL for the chain
        network = self.settings.network
        rpc_url = get_rpc_url(chain, network=network)

        # Create compiler with allow_placeholder_prices=True. Real prices are
        # injected per-request in CompileIntent() via price_map field.
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=wallet_address,
            rpc_url=rpc_url,
            config=config,
        )

        self._compiler_cache[cache_key] = (compiler, now)
        logger.info(f"Created IntentCompiler for chain={chain}, wallet={wallet_address[:10]}...")
        return compiler

    def _is_safe_address(self, wallet_address: str) -> bool:
        """Check if a wallet address matches the configured Safe address."""
        if not self.settings.safe_address or not self.settings.safe_mode:
            return False
        return wallet_address.lower() == self.settings.safe_address.lower()

    def _create_signer(self, wallet_address: str):
        """Create the appropriate signer based on wallet address.

        If wallet_address matches the configured Safe address, creates a
        ZodiacRolesSigner (zodiac mode) or DirectSafeSigner (direct mode).
        Otherwise creates a LocalKeySigner.
        """
        from almanak.framework.execution.signer import LocalKeySigner

        if self._is_safe_address(wallet_address):
            from almanak.framework.execution.signer.safe.config import SafeSignerConfig, SafeWalletConfig

            safe_mode = self.settings.safe_mode or "direct"

            if safe_mode == "zodiac":
                if not self.settings.eoa_address:
                    raise ValueError("EOA_ADDRESS must be configured when ALMANAK_GATEWAY_SAFE_MODE=zodiac")
                if not self.settings.signer_service_url:
                    raise ValueError("SIGNER_SERVICE_URL must be configured when ALMANAK_GATEWAY_SAFE_MODE=zodiac")
                eoa_address = self.settings.eoa_address
            else:
                private_key = self.settings.private_key
                if not private_key:
                    raise ValueError("PRIVATE_KEY not configured in gateway settings")
                from eth_account import Account

                eoa_address = Account.from_key(private_key).address

            assert self.settings.safe_address is not None  # guarded by _is_safe_address
            wallet_config = SafeWalletConfig(
                safe_address=self.settings.safe_address,
                eoa_address=eoa_address,
                zodiac_roles_address=self.settings.zodiac_roles_address if safe_mode == "zodiac" else None,
            )
            safe_config = SafeSignerConfig(
                mode=safe_mode,
                wallet_config=wallet_config,
                private_key=self.settings.private_key if safe_mode != "zodiac" else None,
                signer_service_url=self.settings.signer_service_url if safe_mode == "zodiac" else None,
                signer_service_jwt=self.settings.signer_service_jwt if safe_mode == "zodiac" else None,
            )

            if safe_mode == "zodiac":
                from almanak.framework.execution.signer.safe.zodiac import ZodiacRolesSigner

                logger.info("Using ZodiacRolesSigner for wallet %s", wallet_address[:10])
                return ZodiacRolesSigner(safe_config)

            from almanak.framework.execution.signer.safe.direct import DirectSafeSigner

            logger.info("Using DirectSafeSigner for wallet %s", wallet_address[:10])
            return DirectSafeSigner(safe_config)

        # Non-Safe EOA wallet
        private_key = self.settings.private_key
        if not private_key:
            raise ValueError("PRIVATE_KEY not configured in gateway settings")
        return LocalKeySigner(private_key=private_key)

    async def _get_orchestrator(self, chain: str, wallet_address: str):
        """Get or create execution orchestrator for a chain.

        If wallet_address matches the configured Safe address, the orchestrator
        uses a DirectSafeSigner instead of LocalKeySigner.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")
            wallet_address: Wallet address for signing

        Returns:
            ExecutionOrchestrator for the specified chain
        """
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator
        from almanak.framework.execution.simulator import create_simulator
        from almanak.framework.execution.submitter import PublicMempoolSubmitter
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        if cache_key in self._orchestrator_cache:
            return self._orchestrator_cache[cache_key]

        network = self.settings.network
        rpc_url = get_rpc_url(chain, network=network)

        signer = self._create_signer(wallet_address)
        submitter = PublicMempoolSubmitter(rpc_url=rpc_url)
        simulator = create_simulator(rpc_url=rpc_url)

        orchestrator = ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain=chain,
            rpc_url=rpc_url,
        )

        self._orchestrator_cache[cache_key] = orchestrator
        self._orchestrator_locks[cache_key] = asyncio.Lock()
        self._orchestrator_default_gas_caps[cache_key] = orchestrator.tx_risk_config.max_gas_price_gwei
        return orchestrator

    async def CompileIntent(
        self,
        request: gateway_pb2.CompileIntentRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CompilationResult:
        """Compile an intent into an action bundle.

        Args:
            request: Compile request with intent_type, intent_data, chain, wallet
            context: gRPC context

        Returns:
            CompilationResult with action bundle or error
        """
        # Validate inputs BEFORE initialization
        intent_type = request.intent_type
        if not intent_type:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("intent_type is required")
            return gateway_pb2.CompilationResult(success=False, error="intent_type required")

        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.CompilationResult(success=False, error=str(e))

        wallet_address = request.wallet_address
        if wallet_address:
            try:
                wallet_address = validate_address(wallet_address, "wallet_address")
            except ValidationError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return gateway_pb2.CompilationResult(success=False, error=str(e))

        await self._ensure_initialized()

        # Validate and parse price_map before entering the main try block
        # so invalid client input returns INVALID_ARGUMENT, not INTERNAL.
        price_map_raw = dict(request.price_map) if request.price_map else {}
        parsed_prices: dict[str, Decimal] | None = None
        if price_map_raw:
            try:
                parsed_prices = {}
                for symbol, price_str in price_map_raw.items():
                    price = Decimal(price_str)
                    if not price.is_finite() or price <= 0:
                        raise ValueError(f"{symbol} price must be finite and > 0, got {price_str}")
                    parsed_prices[symbol] = price
            except (ValueError, ArithmeticError) as e:
                error_msg = f"Invalid price_map value: {e}"
                logger.warning(f"CompileIntent rejected for {intent_type}: {error_msg}")
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(error_msg)
                return gateway_pb2.CompilationResult(
                    success=False,
                    error=error_msg,
                    error_code="INVALID_PRICE_MAP",
                )

        try:
            # Deserialize intent data
            intent_data = json.loads(request.intent_data.decode("utf-8"))

            # Create intent object from type and data
            intent = self._create_intent(intent_type, intent_data)

            # Get compiler for this chain/wallet pair
            cache_key = f"{chain}:{wallet_address}"
            compiler = self._get_compiler(chain, wallet_address)
            compiler_lock = self._compiler_locks.setdefault(cache_key, asyncio.Lock())

            # Serialize override+compile+restore per cached compiler to
            # prevent concurrent requests from seeing each other's prices.
            from almanak.framework.intents.compiler import CompilationStatus

            async with compiler_lock:
                original_oracle = getattr(compiler, "price_oracle", None)
                original_placeholders = getattr(compiler, "_using_placeholders", True)

                if parsed_prices and hasattr(compiler, "update_prices"):
                    compiler.update_prices(parsed_prices)
                    logger.debug(
                        f"Applied {len(parsed_prices)} real prices for compilation: {list(parsed_prices.keys())}"
                    )
                elif (
                    self.settings.network == "mainnet"
                    and self._normalize_intent_type(intent_type).upper() in PRICE_SENSITIVE_INTENT_TYPES
                ):
                    # VIB-523: On mainnet, fail compilation for price-sensitive intents
                    # if no real prices are available, instead of silently using
                    # placeholder prices with incorrect slippage calculations.
                    error_msg = (
                        f"No real prices available for {intent_type} compilation on mainnet. "
                        f"Price oracle returned no data (CoinGecko rate-limited or Chainlink "
                        f"unavailable). Refusing to compile with placeholder prices. "
                        f"Retry after price sources recover."
                    )
                    logger.warning(error_msg)
                    return gateway_pb2.CompilationResult(
                        success=False,
                        error=error_msg,
                        error_code="NO_PRICES_AVAILABLE",
                    )

                try:
                    compilation_result = compiler.compile(intent=intent)
                finally:
                    if hasattr(compiler, "restore_prices"):
                        compiler.restore_prices(original_oracle, original_placeholders)

            # Check compilation status
            if compilation_result.status != CompilationStatus.SUCCESS:
                error_msg = compilation_result.error or "Compilation failed"
                logger.warning(f"CompileIntent failed for {intent_type}: {error_msg}")
                return gateway_pb2.CompilationResult(
                    success=False,
                    error=error_msg,
                    error_code="COMPILATION_FAILED",
                )

            if compilation_result.action_bundle is None:
                return gateway_pb2.CompilationResult(
                    success=False,
                    error="Compilation succeeded but no action bundle produced",
                    error_code="NO_ACTION_BUNDLE",
                )

            # Serialize action bundle
            bundle_bytes = json.dumps(compilation_result.action_bundle.to_dict()).encode("utf-8")

            return gateway_pb2.CompilationResult(
                success=True,
                action_bundle=bundle_bytes,
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"CompileIntent failed for {intent_type}: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="COMPILATION_FAILED",
            )

    def _create_intent(self, intent_type: str, intent_data: dict[str, Any]):
        """Create intent object from type and data.

        Args:
            intent_type: Intent type name (e.g., "swap", "lp_open")
            intent_data: Intent parameters

        Returns:
            Intent object
        """
        from almanak.framework.intents import BridgeIntent
        from almanak.framework.intents.vocabulary import (
            BorrowIntent,
            FlashLoanIntent,
            HoldIntent,
            LPCloseIntent,
            LPOpenIntent,
            PerpCloseIntent,
            PerpOpenIntent,
            PredictionBuyIntent,
            PredictionRedeemIntent,
            PredictionSellIntent,
            RepayIntent,
            StakeIntent,
            SupplyIntent,
            SwapIntent,
            UnstakeIntent,
            UnwrapNativeIntent,
            WithdrawIntent,
        )

        # Canonical class lookup keys match derivation:
        # type(intent).__name__.lower().replace("intent", "")
        # e.g. SwapIntent -> "swap", LPOpenIntent -> "lpopen".
        intent_classes = {
            "swap": SwapIntent,
            "hold": HoldIntent,
            "lpopen": LPOpenIntent,
            "lpclose": LPCloseIntent,
            "borrow": BorrowIntent,
            "repay": RepayIntent,
            "supply": SupplyIntent,
            "withdraw": WithdrawIntent,
            "perpopen": PerpOpenIntent,
            "perpclose": PerpCloseIntent,
            "flashloan": FlashLoanIntent,
            "stake": StakeIntent,
            "unstake": UnstakeIntent,
            "predictionbuy": PredictionBuyIntent,
            "predictionsell": PredictionSellIntent,
            "predictionredeem": PredictionRedeemIntent,
            "bridge": BridgeIntent,
            "unwrapnative": UnwrapNativeIntent,
        }

        normalized_intent_type = self._normalize_intent_type(intent_type)
        intent_class = intent_classes.get(normalized_intent_type)
        if not intent_class:
            raise ValueError(f"Unknown intent type: {intent_type}")

        # Use deserialize() to properly handle JSON string -> Python type coercion
        # (e.g., ISO datetime strings -> datetime objects, string -> Decimal).
        # Direct construction fails because AlmanakImmutableModel uses strict=True.
        return intent_class.deserialize(intent_data)  # type: ignore[attr-defined]

    @staticmethod
    def _normalize_intent_type(intent_type: str) -> str:
        """Normalize intent type to canonical lookup key.

        Accepts legacy and canonical aliases, for example:
        - swap / SWAP
        - lp_open / lpopen / LP_OPEN
        - lp_close / lpclose / LP_CLOSE
        - perp_open / perpopen / PERP_OPEN
        - perp_close / perpclose / PERP_CLOSE
        """
        return intent_type.strip().lower().replace("-", "").replace("_", "")

    async def Execute(
        self,
        request: gateway_pb2.ExecuteRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ExecutionResult:
        """Execute an action bundle.

        Args:
            request: Execute request with action bundle and options
            context: gRPC context

        Returns:
            ExecutionResult with tx hashes, gas used, receipts
        """
        # Validate inputs BEFORE initialization
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecutionResult(success=False, error=str(e))

        try:
            wallet_address = validate_address(request.wallet_address, "wallet_address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecutionResult(success=False, error=str(e))

        await self._ensure_initialized()

        try:
            # Deserialize action bundle (uses the framework ActionBundle dataclass)
            from almanak.framework.models.reproduction_bundle import ActionBundle

            bundle_data = json.loads(request.action_bundle.decode("utf-8"))
            action_bundle = ActionBundle.from_dict(bundle_data)

            # Get orchestrator for chain
            orchestrator = await self._get_orchestrator(chain, wallet_address)
            cache_key = f"{chain}:{wallet_address}"
            orchestrator_lock = self._orchestrator_locks.setdefault(cache_key, asyncio.Lock())
            default_gas_cap = self._orchestrator_default_gas_caps.setdefault(
                cache_key,
                orchestrator.tx_risk_config.max_gas_price_gwei,
            )

            # Build execution context
            from almanak.framework.execution.orchestrator import ExecutionContext

            # For Anvil (local fork) networks, always enable simulation so that the
            # LocalSimulator handles gas estimation. The default simulation_enabled=False
            # path (_maybe_estimate_gas_limits) calls eth_estimateGas against the public
            # RPC, which fails with "missing trie node" for storage slots that exist only
            # in the Anvil fork's local state (e.g., ERC1155 LP tokens minted by LP_OPEN).
            # LocalSimulator uses snapshot+execute to estimate gas against actual fork state.
            is_anvil_network = self.settings.network == "anvil"
            effective_simulation_enabled = request.simulation_enabled or is_anvil_network

            if is_anvil_network and not request.simulation_enabled:
                logger.debug(
                    "Anvil network: enabling simulation to use LocalSimulator "
                    "for accurate gas estimation of post-state-change transactions"
                )

            exec_context = ExecutionContext(
                strategy_id=request.strategy_id,
                intent_id=request.intent_id,
                chain=chain,
                wallet_address=wallet_address,
                simulation_enabled=effective_simulation_enabled,
                dry_run=request.dry_run,
            )

            # Execute with per-orchestrator serialization so request-specific gas caps
            # do not race or leak across concurrent requests.
            async with orchestrator_lock:
                orchestrator.tx_risk_config.max_gas_price_gwei = (
                    request.max_gas_price_gwei if request.max_gas_price_gwei > 0 else default_gas_cap
                )
                try:
                    result = await orchestrator.execute(action_bundle, exec_context)
                finally:
                    orchestrator.tx_risk_config.max_gas_price_gwei = default_gas_cap

            # Extract tx_hashes and receipts from transaction_results
            transaction_results = result.transaction_results or []
            tx_hashes = [tr.tx_hash for tr in transaction_results if tr.tx_hash]
            receipts_data = []
            for tr in transaction_results:
                if tr.receipt:
                    # Use to_dict if available (preferred method)
                    if hasattr(tr.receipt, "to_dict"):
                        try:
                            receipts_data.append(tr.receipt.to_dict())
                        except Exception as e:
                            logger.warning(
                                f"Failed to serialize receipt using to_dict(): {e}. Receipt type: {type(tr.receipt)}"
                            )
                            # Fallback: try to convert to dict manually
                            try:
                                receipts_data.append(dict(tr.receipt))
                            except Exception as e2:
                                logger.error(
                                    f"Failed to convert receipt to dict: {e2}. "
                                    f"Skipping receipt for transaction {tr.tx_hash}"
                                )
                    else:
                        # Fallback: convert to dict manually
                        try:
                            receipts_data.append(dict(tr.receipt))
                        except Exception as e:
                            logger.error(
                                f"Receipt type {type(tr.receipt)} does not support to_dict() "
                                f"and cannot be converted to dict: {e}. "
                                f"Skipping receipt for transaction {tr.tx_hash}"
                            )
            receipts_bytes = json.dumps(receipts_data).encode("utf-8")

            return gateway_pb2.ExecutionResult(
                success=result.success,
                tx_hashes=tx_hashes,
                total_gas_used=result.total_gas_used or 0,
                receipts=receipts_bytes,
                execution_id=result.correlation_id or "",
                error=result.error or "",
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Execute failed: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.ExecutionResult(
                success=False,
                error=error_msg,
                error_code="EXECUTION_FAILED",
            )

    async def GetTransactionStatus(
        self,
        request: gateway_pb2.TxStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TxStatus:
        """Get transaction status.

        Args:
            request: Status request with tx_hash and chain
            context: gRPC context

        Returns:
            TxStatus with confirmation status
        """
        # Validate tx_hash format
        try:
            tx_hash = validate_tx_hash(request.tx_hash)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.TxStatus(status="invalid", error=str(e))

        # Validate chain
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.TxStatus(status="invalid", error=str(e))

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            from almanak.gateway.utils import get_rpc_url

            rpc_url = get_rpc_url(chain, network=self.settings.network)
            w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

            # Get transaction receipt
            receipt = await w3.eth.get_transaction_receipt(tx_hash)  # type: ignore[arg-type]

            if receipt is None:
                return gateway_pb2.TxStatus(status="pending")

            # Check status
            if receipt["status"] == 1:
                current_block = await w3.eth.block_number
                confirmations = current_block - receipt["blockNumber"]

                return gateway_pb2.TxStatus(
                    status="confirmed",
                    confirmations=confirmations,
                    block_number=receipt["blockNumber"],
                    gas_used=receipt["gasUsed"],
                )
            else:
                return gateway_pb2.TxStatus(
                    status="reverted",
                    block_number=receipt["blockNumber"],
                    gas_used=receipt["gasUsed"],
                    error="Transaction reverted",
                )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"GetTransactionStatus failed for {tx_hash}: {error_msg}")

            # If tx not found, it's likely still pending
            if "not found" in error_msg.lower():
                return gateway_pb2.TxStatus(status="pending")

            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.TxStatus(status="unknown", error=error_msg)
