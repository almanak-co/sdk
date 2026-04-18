"""Intent to ActionBundle Compiler.

This module provides the IntentCompiler class that converts high-level
trading intents into executable ActionBundles containing transaction data.

The compiler:
1. Takes an Intent (e.g., SwapIntent)
2. Resolves token addresses and amounts
3. Builds necessary approve transactions
4. Builds the primary action transaction (swap, LP, etc.)
5. Estimates gas for all transactions
6. Returns an ActionBundle ready for execution

Example:
    from almanak.framework.intents import Intent
    from almanak.framework.intents.compiler import IntentCompiler

    compiler = IntentCompiler(chain="arbitrum")
    intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
    bundle = compiler.compile(intent)
"""

import logging
import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

# Note: FlashLoanSelector import is done lazily in _compile_flash_loan to avoid circular import
# Note: PolymarketAdapter import is done lazily in __init__ to avoid circular import and allow optional usage
# Note: MorphoBlueAdapter is imported lazily in _compile_* methods to avoid circular import
# Note: TokenNotFoundError and get_token_resolver are imported lazily to avoid circular import
# (compiler -> data/__init__ -> prediction_provider -> connectors/__init__ -> ... -> compiler)
from ..models.reproduction_bundle import ActionBundle
from ..utils.log_formatters import (
    _emojis_enabled,
    format_percentage,
    format_slippage_bps,
    format_token_amount,
)
from .vocabulary import (
    AnyIntent,
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    HoldIntent,
    IntentType,
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
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)

if TYPE_CHECKING:
    from web3 import Web3

    from ..connectors.bridges.selector import BridgeSelector
    from ..connectors.polymarket.adapter import PolymarketAdapter
    from ..data.tokens import TokenResolver as TokenResolverType
    from ..gateway_client import GatewayClient
    from .bridge import BridgeIntent
    from .pool_validation import PoolValidationResult
    from .vocabulary import UnwrapNativeIntent, WrapNativeIntent

logger = logging.getLogger(__name__)


# =============================================================================
# Extracted modules — re-exported for backward compatibility
# (all symbols that were importable from this module remain importable)
# =============================================================================

from .compiler_adapters import (  # noqa: F401
    AaveV3Adapter,
    BalancerAdapter,
    DefaultSwapAdapter,
    LendingProtocolAdapter,
    LPProtocolAdapter,
    SwapProtocolAdapter,
    UniswapV3LPAdapter,
)
from .compiler_constants import (  # noqa: F401
    AAVE_BORROW_SELECTOR,
    AAVE_COMPATIBLE_PROTOCOLS,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
    AAVE_V2_DEPOSIT_SELECTOR,
    AAVE_V2_FORKS,
    AAVE_VARIABLE_RATE_MODE,
    AAVE_WITHDRAW_SELECTOR,
    APPROVE_ZERO_FIRST_TOKENS,
    BALANCER_FLASH_LOAN_SELECTOR,
    BALANCER_VAULT_ADDRESSES,
    CHAIN_GAS_OVERRIDES,
    CHAIN_TOKENS,
    DEFAULT_GAS_ESTIMATES,
    DEFAULT_SWAP_FEE_TIER,
    ERC20_ALLOWANCE_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    ERC20_TRANSFER_FROM_SELECTOR,
    ERC20_TRANSFER_SELECTOR,
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    MAX_UINT128,
    MAX_UINT256,
    NFT_POSITION_BURN_SELECTOR,
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
    NFT_POSITION_INCREASE_SELECTOR,
    NFT_POSITION_MINT_SELECTOR,
    PROTOCOL_ROUTERS,
    SWAP_FEE_TIERS,
    SWAP_FEE_TIERS_CHAIN,
    SWAP_QUOTER_ADDRESSES,
    SWAP_ROUTER_ALGEBRA_PROTOCOLS,
    SWAP_ROUTER_V1_CHAIN_OVERRIDES,
    SWAP_ROUTER_V1_PROTOCOLS,
    get_gas_estimate,
)
from .compiler_models import (  # noqa: F401
    CompilationResult,
    CompilationStatus,
    IntentCompilerConfig,
    PriceInfo,
    TokenInfo,
    TransactionData,
)

# =============================================================================
# Intent Compiler
# =============================================================================


class IntentCompiler:
    """Compiles Intents into executable ActionBundles.

    The IntentCompiler takes high-level trading intents and converts them
    into low-level transaction data ready for execution on-chain.

    Example:
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x...",
            rpc_url="https://arb1.arbitrum.io/rpc",
        )
        intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
        result = compiler.compile(intent)
        if result.status == CompilationStatus.SUCCESS:
            # Execute result.action_bundle
            pass
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        wallet_address: str = "0x0000000000000000000000000000000000000000",
        default_protocol: str = "uniswap_v3",
        price_oracle: dict[str, Decimal] | None = None,
        default_deadline_seconds: int = 300,
        rpc_url: str | None = None,
        rpc_timeout: float = 10.0,
        default_lp_slippage: Decimal = Decimal("0.99"),
        config: IntentCompilerConfig | None = None,
        gateway_client: "GatewayClient | None" = None,
        token_resolver: "TokenResolverType | None" = None,
        chain_wallets: dict[str, str] | None = None,
    ) -> None:
        """Initialize the compiler.

        Args:
            chain: Target blockchain (ethereum, arbitrum, etc.)
            wallet_address: Address that will execute transactions
            default_protocol: Default DEX protocol for swaps
            price_oracle: Price oracle dict (token -> USD price). Required for
                production use to calculate accurate slippage amounts.
            default_deadline_seconds: Default transaction deadline
            rpc_url: RPC URL for on-chain queries (needed for LP close).
                DEPRECATED: Use gateway_client instead for production deployments.
            rpc_timeout: HTTP timeout for direct RPC calls in seconds.
            default_lp_slippage: Default slippage for LP operations (0.99 = 99%).
                This controls the minimum acceptable amounts when adding/removing liquidity.
                LP operations differ from swaps - for concentrated liquidity, the actual
                deposit ratio depends heavily on where the current price is relative to
                your tick range. A price near the range edge means most liquidity is in
                one token. Default 99% allows nearly full flexibility for this behavior.
                Can be lowered for tighter protection if needed.
            config: Optional configuration. If not provided, defaults to
                IntentCompilerConfig() which requires price_oracle.
            gateway_client: Optional gateway client for RPC queries. When provided,
                all on-chain queries (allowance, balance, position liquidity) go through
                the gateway instead of direct RPC. This is the preferred mode for
                production deployments where strategies run in isolated containers.
            token_resolver: Optional TokenResolver instance for token resolution.
                If not provided, uses the singleton instance from get_token_resolver().
                The resolver provides unified token lookup with caching and on-chain
                discovery support.

        Raises:
            ValueError: If no price_oracle is provided and allow_placeholder_prices is False.
        """
        # Use default config if not provided
        self._config = config or IntentCompilerConfig()

        # Validate price_oracle requirement
        self._using_placeholders = price_oracle is None
        if self._using_placeholders and not self._config.allow_placeholder_prices:
            raise ValueError(
                "IntentCompiler requires a price_oracle for production use. "
                "Pass a dict mapping token symbols to USD prices (e.g., {'ETH': Decimal('3400')}) "
                "or set config=IntentCompilerConfig(allow_placeholder_prices=True) for testing only. "
                "Using placeholder prices will cause incorrect slippage calculations and swap reverts."
            )

        # Normalize chain name (e.g., "bnb" -> "bsc") via central resolver
        try:
            from almanak.core.constants import resolve_chain_name

            self.chain = resolve_chain_name(chain)
        except (ValueError, ImportError):
            self.chain = chain
        self.wallet_address = wallet_address
        # Normalize protocol alias (e.g., "agni" -> "agni_finance" on mantle)
        from ..connectors.protocol_aliases import normalize_protocol

        self.default_protocol = normalize_protocol(self.chain, default_protocol)
        self.default_deadline_seconds = default_deadline_seconds
        self.rpc_url = rpc_url
        self.rpc_timeout = rpc_timeout
        self._web3: Web3 | None = None
        self._gateway_client = gateway_client
        self._chain_wallets = chain_wallets

        # LP slippage configuration (0.99 = 99% default, allows concentrated liquidity flexibility)
        self.default_lp_slippage = min(max(default_lp_slippage, Decimal("0")), Decimal("1"))

        # Token resolver - use provided or default singleton (lazy import to avoid circular dependency)
        if token_resolver is None:
            from ..data.tokens import get_token_resolver

            token_resolver = get_token_resolver()
        self._token_resolver = token_resolver

        # Price oracle - use provided or fall back to placeholders (only if allowed)
        self.price_oracle: dict[str, Decimal] | None
        if self._using_placeholders:
            logger.debug(
                "IntentCompiler created without price oracle, will use placeholders if not updated before compilation"
            )
            self.price_oracle = self._get_placeholder_prices()
        else:
            self.price_oracle = price_oracle
        self._placeholder_warning_logged = False

        # Allowance cache (token -> spender -> amount)
        self._allowance_cache: dict[str, dict[str, int]] = {}
        # Log stablecoin price fallbacks once per symbol per compiler instance.
        self._stablecoin_fallback_logged: set[str] = set()

        # Polymarket adapter for prediction market intents (Polygon only)
        self._polymarket_adapter: PolymarketAdapter | None = None
        self._bridge_selector: BridgeSelector | None = None
        self._init_polymarket_adapter()

        # Cached Solana adapter instances (lazily initialized)
        self._cached_jupiter_adapter: Any = None
        self._cached_kamino_adapter: Any = None
        self._cached_kamino_adapter_with_rpc: Any = None
        self._cached_jupiter_lend_adapter: Any = None
        self._cached_raydium_adapter: Any = None
        self._cached_raydium_adapter_with_rpc: Any = None
        self._cached_meteora_adapter: Any = None
        self._cached_meteora_adapter_with_rpc: Any = None
        self._cached_orca_adapter: Any = None
        self._cached_orca_adapter_with_rpc: Any = None
        self._cached_drift_adapter: Any = None

        effective_protocol = "jupiter" if self._is_solana_chain() else default_protocol
        logger.info(
            f"IntentCompiler initialized for chain={chain}, wallet={wallet_address[:10]}..., protocol={effective_protocol}, using_placeholders={self._using_placeholders}"
        )

    def update_prices(self, prices: dict[str, Decimal]) -> None:
        """Update the price oracle with real prices, clearing placeholder state."""
        self.price_oracle = prices
        self._using_placeholders = False

    def restore_prices(self, original_oracle: dict[str, Decimal] | None, original_using_placeholders: bool) -> None:
        """Restore prices to a previous state (used after temporary override)."""
        self.price_oracle = original_oracle
        self._using_placeholders = original_using_placeholders

    def _resolve_protocol(self, intent_protocol: str | None) -> str:
        """Resolve intent protocol to canonical key, falling back to default.

        Normalizes aliases (e.g., "agni" -> "agni_finance" on mantle) and falls
        back to self.default_protocol if intent_protocol is None.
        """
        if intent_protocol is None:
            return self.default_protocol
        from ..connectors.protocol_aliases import normalize_protocol

        return normalize_protocol(self.chain, intent_protocol)

    def _init_polymarket_adapter(self) -> None:
        """Initialize Polymarket adapter if on Polygon and config is available.

        This method lazily initializes the PolymarketAdapter for prediction market
        intents. The adapter is only initialized when:
        1. The chain is 'polygon' (case-insensitive)
        2. A PolymarketConfig is provided in the IntentCompilerConfig

        If on Polygon without a PolymarketConfig, the method silently returns.
        VIB-307: Warning is deferred to compile time so non-prediction Polygon
        strategies don't see noisy Polymarket warnings at startup.

        This lazy initialization ensures:
        - Non-Polygon usage is unaffected (no import overhead)
        - Missing config is handled gracefully
        - Clear error messages when prediction intents are attempted without config
        """
        # Only initialize for Polygon chain
        if self.chain.lower() != "polygon":
            return

        # Check if config is provided -- silently skip if not.
        # VIB-307: Warning deferred to compile time so non-prediction strategies on Polygon
        # don't see noisy Polymarket warnings at startup.
        polymarket_config = self._config.polymarket_config
        if polymarket_config is None:
            return

        # Lazy import to avoid circular imports and allow optional usage
        try:
            from ..connectors.polymarket.adapter import PolymarketAdapter

            # Initialize web3 for redemption intents if rpc_url is available
            web3_instance = None
            if self.rpc_url:
                from web3 import Web3

                if self._web3 is None:
                    self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))
                web3_instance = self._web3
                logger.debug("Web3 instance initialized for PolymarketAdapter (redemption support enabled)")

            self._polymarket_adapter = PolymarketAdapter(polymarket_config, web3=web3_instance)
            logger.info(f"PolymarketAdapter initialized for wallet={polymarket_config.wallet_address[:10]}...")
        except ImportError as e:
            logger.warning(f"Failed to import PolymarketAdapter: {e}. Prediction market intents will not be available.")
        except Exception as e:
            logger.warning(
                f"Failed to initialize PolymarketAdapter: {e}. Prediction market intents will not be available."
            )

    def _get_chain_rpc_url(self) -> str | None:
        """Get RPC URL for the current chain.

        If rpc_url is set on the compiler, use it. Otherwise, check if a managed
        Anvil fork is running (via ANVIL_{CHAIN}_PORT env var set by managed.py),
        and use that. Finally, fall back to the gateway's RPC provider.

        This is needed for protocol adapters (like Aerodrome, TraderJoe, Pendle)
        that need to make direct RPC calls for pool queries when the compiler is
        using gateway mode (rpc_url=None).

        Returns:
            RPC URL string or None if not available.
        """
        if self.rpc_url:
            return self.rpc_url

        # Check if a managed Anvil fork is running for this chain.
        # managed.py sets ANVIL_{CHAIN}_PORT when it starts an Anvil fork.
        # This MUST take priority over mainnet RPC so that protocol adapters
        # (e.g., TraderJoe, Aerodrome) query on-chain state from the fork
        # where LP positions actually exist, not mainnet.
        anvil_port_var = f"ANVIL_{self.chain.upper()}_PORT"
        anvil_port = os.environ.get(anvil_port_var)
        if anvil_port:
            anvil_url = f"http://127.0.0.1:{anvil_port}"
            logger.debug(
                f"Anvil fork detected for {self.chain} ({anvil_port_var}={anvil_port}), using fork URL: {anvil_url}"
            )
            return anvil_url

        try:
            from almanak.gateway.utils import get_rpc_url

            rpc_url = get_rpc_url(self.chain)
        except (ImportError, ValueError) as e:
            logger.debug(f"Failed to fetch mainnet RPC URL for {self.chain}: {e}")
        else:
            logger.debug(f"Fetched RPC URL for {self.chain} from gateway utils")
            return rpc_url

        # Fallback: try Anvil ONLY if no RPC source is configured.
        # If an RPC source is set but resolution failed (bad key, unsupported chain),
        # we should fail fast - not silently switch to localhost.
        from almanak.gateway.utils.rpc_provider import has_api_key_configured

        if has_api_key_configured():
            logger.warning(
                f"RPC source is configured but resolution failed for {self.chain}. Not falling back to Anvil."
            )
            return None

        try:
            from almanak.gateway.utils import get_rpc_url

            rpc_url = get_rpc_url(self.chain, network="anvil")
        except (ImportError, ValueError) as e:
            logger.warning(f"Failed to get RPC URL for {self.chain} (no API key, Anvil also unavailable): {e}")
            return None
        else:
            logger.debug(f"No API key configured, using Anvil RPC for {self.chain}: {rpc_url}")
            return rpc_url

    def _is_wallet_contract(self) -> bool | None:
        """Check if the wallet address is a contract (has bytecode).

        Uses eth_getCode via RPC to check if the wallet has deployed bytecode.
        Flash loans require a contract wallet to handle callbacks.

        Returns:
            True if contract, False if EOA, None if RPC unavailable.
        """
        rpc_url = self._get_chain_rpc_url()
        if not rpc_url:
            return None

        try:
            import httpx

            response = httpx.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_getCode",
                    "params": [self.wallet_address, "latest"],
                    "id": 1,
                },
                timeout=self.rpc_timeout,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("error") is not None:
                logger.debug("eth_getCode RPC error: %s", data)
                return None
            code = data.get("result")
            # EOA wallets return "0x" (empty bytecode)
            return code not in ("0x", "0x0", "", None)
        except Exception as e:
            logger.debug(f"Failed to check wallet bytecode via eth_getCode: {e}")
            return None

    def _validate_pool(self, result: "PoolValidationResult", intent_id: str) -> CompilationResult | None:
        """Check pool validation result and return FAILED CompilationResult if pool doesn't exist.

        Args:
            result: Pool validation result from pool_validation module.
            intent_id: Intent ID for error reporting.

        Returns:
            CompilationResult with FAILED status if pool doesn't exist, None if OK to proceed.
        """
        if result.exists is False:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=result.error or "Pool does not exist",
                intent_id=intent_id,
            )
        if result.warning:
            logger.warning("Pool validation: %s", result.warning)
        return None

    @property
    def polymarket_adapter(self) -> "PolymarketAdapter | None":
        """Get the Polymarket adapter for prediction market intents.

        Returns:
            PolymarketAdapter if initialized, None otherwise.
        """
        return self._polymarket_adapter

    def compile(self, intent: AnyIntent) -> CompilationResult:
        """Compile an intent into an ActionBundle.

        This is the main entry point for compiling intents. It dispatches
        to the appropriate handler based on intent type.

        Args:
            intent: The intent to compile

        Returns:
            CompilationResult with ActionBundle and metadata
        """
        try:
            # Step 0: Resolve amount="all" before dispatching.
            # This is the single mandatory resolution point for all intent types.
            # Protocol-position intents (withdraw, repay) query on-chain balances;
            # wallet-funded intents (swap, supply, bridge) are left for per-intent handlers.
            from .amount_resolver import resolve_amount_all

            intent = resolve_amount_all(
                intent,
                chain=self.chain,
                wallet_address=self.wallet_address,
                gateway_client=self._gateway_client,
            )

            intent_type = intent.intent_type

            # Suppress placeholder price warning for intent types that don't use prices.
            # STAKE/UNSTAKE amounts are in native units, not USD, so no price conversion needed.
            # HOLD is a no-op with no transactions.
            # VAULT_DEPOSIT/VAULT_REDEEM use ERC-4626 on-chain reads, no price oracle needed.
            _price_irrelevant = intent_type in (
                IntentType.STAKE,
                IntentType.UNSTAKE,
                IntentType.HOLD,
                IntentType.UNWRAP_NATIVE,
                IntentType.VAULT_DEPOSIT,
                IntentType.VAULT_REDEEM,
            )
            if self._using_placeholders and not self._placeholder_warning_logged and not _price_irrelevant:
                logger.warning(
                    "IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. "
                    "This is only acceptable for unit tests."
                )
                self._placeholder_warning_logged = True

            if intent_type == IntentType.SWAP:
                return self._compile_swap(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_OPEN:
                return self._compile_lp_open(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_CLOSE:
                return self._compile_lp_close(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.LP_COLLECT_FEES:
                return self._compile_collect_fees(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.BORROW:
                return self._compile_borrow(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.REPAY:
                return self._compile_repay(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.SUPPLY:
                return self._compile_supply(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.WITHDRAW:
                return self._compile_withdraw(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PERP_OPEN:
                return self._compile_perp_open(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PERP_CLOSE:
                return self._compile_perp_close(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.HOLD:
                return self._compile_hold(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.FLASH_LOAN:
                return self._compile_flash_loan(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.STAKE:
                return self._compile_stake_intent(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.UNSTAKE:
                return self._compile_unstake_intent(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_BUY:
                return self._compile_prediction_buy(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_SELL:
                return self._compile_prediction_sell(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_REDEEM:
                return self._compile_prediction_redeem(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.BRIDGE:
                return self._compile_bridge(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.VAULT_DEPOSIT:
                return self._compile_vault_deposit(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.VAULT_REDEEM:
                return self._compile_vault_redeem(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.ENSURE_BALANCE:
                return self._compile_ensure_balance(intent)
            elif intent_type == IntentType.WRAP_NATIVE:
                return self._compile_wrap_native(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.UNWRAP_NATIVE:
                return self._compile_unwrap_native(intent)  # type: ignore[arg-type]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Intent type {intent_type.value} is not supported by the compiler",
                    intent_id=intent.intent_id,
                )

        except Exception as e:
            logger.exception(f"Failed to compile intent: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )

    def _get_bridge_selector(self) -> "BridgeSelector":
        """Get lazily-initialized BridgeSelector with default bridge adapters."""
        if self._bridge_selector is not None:
            return self._bridge_selector

        from ..connectors.bridges.across.adapter import AcrossBridgeAdapter
        from ..connectors.bridges.selector import BridgeSelector
        from ..connectors.bridges.stargate.adapter import StargateBridgeAdapter

        bridges = [
            AcrossBridgeAdapter(token_resolver=self._token_resolver),
            StargateBridgeAdapter(token_resolver=self._token_resolver),
        ]
        self._bridge_selector = BridgeSelector(bridges=bridges)
        return self._bridge_selector

    def _compile_bridge(self, intent: "BridgeIntent") -> CompilationResult:
        """Compile a BRIDGE intent into an ActionBundle."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            from_chain = intent.from_chain.lower()
            to_chain = intent.to_chain.lower()
            token_symbol = intent.token

            token_info = self._resolve_token(token_symbol, chain=from_chain)
            if token_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token for bridge on {from_chain}: {token_symbol}",
                    intent_id=intent.intent_id,
                )

            if intent.amount == "all":
                # Resolve 'all' to the actual on-chain token balance for from_chain.
                # This mirrors how single-chain swaps/wraps handle amount='all'.
                if token_info.is_native:
                    balance_wei = self._query_native_balance_for_chain(self.wallet_address, from_chain)
                else:
                    balance_wei = self._query_erc20_balance_for_chain(
                        token_info.address, self.wallet_address, from_chain
                    )
                if balance_wei is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Failed to query {token_symbol} balance on {from_chain} — RPC unavailable",
                        intent_id=intent.intent_id,
                    )
                if balance_wei <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No {token_symbol} balance to bridge on {from_chain}",
                        intent_id=intent.intent_id,
                    )
                if token_info.is_native:
                    # Reserve gas for the bridge deposit transaction itself.
                    # Mirrors wrap compiler: deduct 0.001 native token as gas buffer.
                    gas_reserve_wei = int(Decimal("0.001") * Decimal(10**token_info.decimals))
                    balance_wei = max(balance_wei - gas_reserve_wei, 0)
                    if balance_wei <= 0:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Native balance too low to bridge {token_symbol} on {from_chain} after reserving gas",
                            intent_id=intent.intent_id,
                        )
                amount_decimal = Decimal(balance_wei) / Decimal(10**token_info.decimals)
            else:
                amount_decimal = intent.amount  # type: ignore[assignment]

            if not isinstance(amount_decimal, Decimal):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Bridge amount must be Decimal after resolution, got: {type(amount_decimal).__name__}",
                    intent_id=intent.intent_id,
                )

            selector = self._get_bridge_selector()

            # If preferred_bridge is set, exclude all other bridges
            preferred = getattr(intent, "preferred_bridge", None)
            excluded = None
            if preferred:
                excluded = [b.name.lower() for b in selector.bridges if b.name.lower() != preferred.lower()]

            if excluded:
                selection = selector.select_bridge_with_fallback(
                    token=token_symbol,
                    amount=amount_decimal,
                    from_chain=from_chain,
                    to_chain=to_chain,
                    max_slippage=intent.max_slippage,
                    excluded_bridges=excluded,
                )
            else:
                selection = selector.select_bridge(
                    token=token_symbol,
                    amount=amount_decimal,
                    from_chain=from_chain,
                    to_chain=to_chain,
                    max_slippage=intent.max_slippage,
                )
            if not selection.is_success or selection.bridge is None or selection.quote is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No bridge available for {token_symbol} from {from_chain} to {to_chain}",
                    intent_id=intent.intent_id,
                )

            quote = selection.quote
            bridge = selection.bridge
            # Use destination_address from intent or resolve from wallet registry
            dest_wallet = getattr(intent, "destination_address", None) or self._resolve_dest_wallet(to_chain)
            bridge_tx = bridge.build_deposit_tx(quote=quote, recipient=dest_wallet)

            amount_in_wei: int | None = None
            if quote.route_data and "amount_wei" in quote.route_data:
                try:
                    amount_in_wei = int(quote.route_data["amount_wei"])
                except (ValueError, TypeError):
                    amount_in_wei = None
            if amount_in_wei is None:
                amount_in_wei = int(amount_decimal * Decimal(10**token_info.decimals))

            transactions: list[TransactionData] = []
            if not token_info.is_native:
                transactions.extend(
                    self._build_approve_tx(
                        token_address=token_info.address,
                        spender=bridge_tx["to"],
                        amount=amount_in_wei,
                    )
                )

            bridge_transaction = TransactionData(
                to=bridge_tx["to"],
                value=int(bridge_tx.get("value", 0)),
                data=bridge_tx["data"],
                gas_estimate=int(bridge_tx.get("gas_estimate", get_gas_estimate(from_chain, "bridge_deposit"))),
                description=f"Bridge {amount_decimal} {token_symbol} from {from_chain} to {to_chain} via {bridge.name}",
                tx_type="bridge_deposit",
            )
            transactions.append(bridge_transaction)

            metadata: dict[str, Any] = {
                "from_chain": from_chain,
                "to_chain": to_chain,
                "token": token_symbol,
                "amount": str(amount_decimal),
                "bridge": bridge.name,
                "estimated_time": int(quote.estimated_time_seconds),
                "fee": str(quote.fee_amount),
                "is_cross_chain": from_chain != to_chain,
                "route": {"from_chain": quote.from_chain, "to_chain": quote.to_chain},
                "quote_id": quote.quote_id,
            }

            action_bundle = ActionBundle(
                intent_type=IntentType.BRIDGE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata=metadata,
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled BRIDGE intent: {amount_decimal} {token_symbol} {from_chain}->{to_chain} via {bridge.name}, "
                f"{len(transactions)} txs"
            )
        except Exception as e:
            logger.exception("Failed to compile BRIDGE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _is_solana_chain(self) -> bool:
        """Check if the compiler's target chain is in the Solana family."""
        from .compiler_solana import is_solana_chain

        return is_solana_chain(self)

    # =========================================================================
    # Solana adapter caching helpers (delegated to compiler_solana)
    # =========================================================================

    def _get_jupiter_adapter(self) -> Any:
        """Get or create a cached JupiterAdapter instance."""
        from .compiler_solana import get_jupiter_adapter

        return get_jupiter_adapter(self)

    def _get_kamino_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached KaminoAdapter instance."""
        from .compiler_solana import get_kamino_adapter

        return get_kamino_adapter(self, needs_rpc=needs_rpc)

    def _get_raydium_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached RaydiumAdapter instance."""
        from .compiler_solana import get_raydium_adapter

        return get_raydium_adapter(self, needs_rpc=needs_rpc)

    def _get_meteora_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached MeteoraAdapter instance."""
        from .compiler_solana import get_meteora_adapter

        return get_meteora_adapter(self, needs_rpc=needs_rpc)

    def _get_orca_adapter(self, *, needs_rpc: bool = False) -> Any:
        """Get or create a cached OrcaAdapter instance."""
        from .compiler_solana import get_orca_adapter

        return get_orca_adapter(self, needs_rpc=needs_rpc)

    def _get_drift_adapter(self) -> Any:
        """Get or create a cached DriftAdapter instance."""
        from .compiler_solana import get_drift_adapter

        return get_drift_adapter(self)

    # =========================================================================
    # Solana compilation methods (delegated to compiler_solana)
    # =========================================================================

    def _compile_jupiter_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent using Jupiter for Solana chains."""
        from .compiler_solana import compile_jupiter_swap

        return compile_jupiter_swap(self, intent)

    def _compile_kamino_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent using Kamino for Solana chains."""
        from .compiler_solana import compile_kamino_supply

        return compile_kamino_supply(self, intent)

    def _compile_kamino_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent using Kamino for Solana chains."""
        from .compiler_solana import compile_kamino_borrow

        return compile_kamino_borrow(self, intent)

    def _compile_kamino_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent using Kamino for Solana chains."""
        from .compiler_solana import compile_kamino_repay

        return compile_kamino_repay(self, intent)

    def _compile_kamino_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent using Kamino for Solana chains."""
        from .compiler_solana import compile_kamino_withdraw

        return compile_kamino_withdraw(self, intent)

    def _get_jupiter_lend_adapter(self) -> Any:
        """Get or create a cached JupiterLendAdapter instance."""
        from .compiler_solana import get_jupiter_lend_adapter

        return get_jupiter_lend_adapter(self)

    def _compile_jupiter_lend_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent using Jupiter Lend for Solana chains."""
        from .compiler_solana import compile_jupiter_lend_supply

        return compile_jupiter_lend_supply(self, intent)

    def _compile_jupiter_lend_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent using Jupiter Lend for Solana chains."""
        from .compiler_solana import compile_jupiter_lend_borrow

        return compile_jupiter_lend_borrow(self, intent)

    def _compile_jupiter_lend_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent using Jupiter Lend for Solana chains."""
        from .compiler_solana import compile_jupiter_lend_repay

        return compile_jupiter_lend_repay(self, intent)

    def _compile_jupiter_lend_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent using Jupiter Lend for Solana chains."""
        from .compiler_solana import compile_jupiter_lend_withdraw

        return compile_jupiter_lend_withdraw(self, intent)

    def _compile_raydium_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Raydium CLMM for Solana chains."""
        from .compiler_solana import compile_raydium_lp_open

        return compile_raydium_lp_open(self, intent)

    def _compile_raydium_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Raydium CLMM for Solana chains."""
        from .compiler_solana import compile_raydium_lp_close

        return compile_raydium_lp_close(self, intent)

    def _compile_meteora_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Meteora DLMM for Solana chains."""
        from .compiler_solana import compile_meteora_lp_open

        return compile_meteora_lp_open(self, intent)

    def _compile_meteora_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Meteora DLMM for Solana chains."""
        from .compiler_solana import compile_meteora_lp_close

        return compile_meteora_lp_close(self, intent)

    def _compile_orca_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent using Orca Whirlpools for Solana chains."""
        from .compiler_solana import compile_orca_lp_open

        return compile_orca_lp_open(self, intent)

    def _compile_orca_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent using Orca Whirlpools for Solana chains."""
        from .compiler_solana import compile_orca_lp_close

        return compile_orca_lp_close(self, intent)

    def _compile_wrap_native(self, intent: "WrapNativeIntent") -> CompilationResult:
        """Compile a WRAP_NATIVE intent into an ActionBundle.

        Generates a single transaction calling the wrapped native token's
        ``deposit()`` function with ``msg.value`` to convert native currency
        (ETH, MATIC, AVAX, etc.) to its wrapped ERC-20 equivalent.
        """

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            token_symbol = intent.token

            # Resolve the wrapped native token address
            weth_address = self._get_wrapped_native_address()
            if not weth_address:
                result.status = CompilationStatus.FAILED
                result.error = f"No wrapped native token found for chain {self.chain}"
                return result

            # Resolve token to verify it matches the chain's wrapped native
            resolved = self._resolve_token(token_symbol)
            if not resolved:
                result.status = CompilationStatus.FAILED
                result.error = f"Cannot resolve token {token_symbol} on {self.chain}"
                return result

            if resolved.address.lower() != weth_address.lower():
                result.status = CompilationStatus.FAILED
                result.error = (
                    f"{token_symbol} ({resolved.address}) is not the wrapped native token "
                    f"({weth_address}) on {self.chain}"
                )
                return result

            # Resolve amount
            amount = intent.amount
            decimals = resolved.decimals
            gas_reserve = int(Decimal("0.001") * Decimal(10**decimals))
            if isinstance(amount, str) and amount == "all":
                # Query native balance
                balance = self._query_native_balance(self.wallet_address)
                if balance is None or balance <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = f"No native balance to wrap on {self.chain}"
                    return result
                # Reserve gas (0.001 native token ~= minimal gas buffer)
                amount_raw = max(balance - gas_reserve, 0)
                if amount_raw <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = "Native balance too low to wrap after reserving gas"
                    return result
            else:
                amount_raw = int(Decimal(str(amount)) * Decimal(10**decimals))

            if amount_raw <= 0:
                result.status = CompilationStatus.FAILED
                result.error = "Wrap amount must be positive"
                return result

            # Pre-flight balance check (must cover wrap amount + gas for the tx)
            if not (isinstance(amount, str) and amount == "all"):
                balance = self._query_native_balance(self.wallet_address)
                if balance is not None and balance < amount_raw + gas_reserve:
                    have = Decimal(balance) / Decimal(10**decimals)
                    need = Decimal(str(intent.amount))
                    result.status = CompilationStatus.FAILED
                    result.error = f"Insufficient native balance: have {have}, need {need} + gas reserve"
                    return result

            # Build deposit() calldata
            # Function selector: 0xd0e30db0 = keccak256("deposit()")[:4]
            calldata = "0xd0e30db0"

            wrap_tx = TransactionData(
                to=weth_address,
                value=amount_raw,
                data=calldata,
                gas_estimate=get_gas_estimate(self.chain, "unwrap_eth"),  # similar gas cost
                description=f"Wrap {intent.amount} native to {token_symbol}",
                tx_type="wrap",
            )

            transactions = [wrap_tx]

            action_bundle = ActionBundle(
                intent_type=IntentType.WRAP_NATIVE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "token": token_symbol,
                    "amount": str(intent.amount),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = wrap_tx.gas_estimate

            logger.info(
                f"Compiled WRAP_NATIVE intent: {intent.amount} native -> {token_symbol} on {self.chain}, "
                f"1 tx, gas={wrap_tx.gas_estimate}"
            )
        except Exception as e:
            logger.exception("Failed to compile WRAP_NATIVE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_unwrap_native(self, intent: "UnwrapNativeIntent") -> CompilationResult:
        """Compile an UNWRAP_NATIVE intent into an ActionBundle.

        Generates a single ``WETH.withdraw(uint256)`` transaction to convert
        wrapped native tokens (WETH, WMATIC, WAVAX, etc.) back to the chain's
        native currency.
        """

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            token_symbol = intent.token

            # Resolve the wrapped native token address
            weth_address = self._get_wrapped_native_address()
            if not weth_address:
                result.status = CompilationStatus.FAILED
                result.error = f"No wrapped native token found for chain {self.chain}"
                return result

            # Resolve token to verify it matches the chain's wrapped native
            resolved = self._resolve_token(token_symbol)
            if not resolved:
                result.status = CompilationStatus.FAILED
                result.error = f"Cannot resolve token {token_symbol} on {self.chain}"
                return result

            if resolved.address.lower() != weth_address.lower():
                result.status = CompilationStatus.FAILED
                result.error = (
                    f"{token_symbol} ({resolved.address}) is not the wrapped native token "
                    f"({weth_address}) on {self.chain}"
                )
                return result

            # Resolve amount
            amount = intent.amount
            if isinstance(amount, str) and amount == "all":
                # Query balance of wrapped native token
                balance = self._query_erc20_balance(weth_address, self.wallet_address)
                if balance is None or balance <= 0:
                    result.status = CompilationStatus.FAILED
                    result.error = f"No {token_symbol} balance to unwrap"
                    return result
                amount_raw = balance
            else:
                decimals = resolved.decimals
                amount_raw = int(Decimal(str(amount)) * Decimal(10**decimals))

            if amount_raw <= 0:
                result.status = CompilationStatus.FAILED
                result.error = "Unwrap amount must be positive"
                return result

            # Pre-flight balance check: catch insufficient balance before on-chain revert
            if not (isinstance(amount, str) and amount == "all"):
                balance = self._query_erc20_balance(weth_address, self.wallet_address)
                if balance is not None and balance < amount_raw:
                    decimals = resolved.decimals
                    have = Decimal(balance) / Decimal(10**decimals)
                    need = Decimal(str(intent.amount))
                    result.status = CompilationStatus.FAILED
                    result.error = (
                        f"Insufficient {token_symbol} balance: have {have} {token_symbol}, need {need} {token_symbol}"
                    )
                    return result

            # Build withdraw(uint256) calldata
            # Function selector: 0x2e1a7d4d = keccak256("withdraw(uint256)")[:4]
            amount_hex = hex(amount_raw)[2:].zfill(64)
            calldata = f"0x2e1a7d4d{amount_hex}"

            unwrap_tx = TransactionData(
                to=weth_address,
                value=0,
                data=calldata,
                gas_estimate=get_gas_estimate(self.chain, "unwrap_eth"),
                description=f"Unwrap {intent.amount} {token_symbol} to native",
                tx_type="unwrap",
            )

            transactions = [unwrap_tx]

            action_bundle = ActionBundle(
                intent_type=IntentType.UNWRAP_NATIVE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "token": token_symbol,
                    "amount": str(intent.amount),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = unwrap_tx.gas_estimate

            logger.info(
                f"Compiled UNWRAP_NATIVE intent: {intent.amount} {token_symbol} on {self.chain}, "
                f"1 tx, gas={unwrap_tx.gas_estimate}"
            )
        except Exception as e:
            logger.exception("Failed to compile UNWRAP_NATIVE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _compile_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent into an ActionBundle.

        This method:
        1. Resolves token addresses
        2. Calculates amounts (USD to token if needed)
        3. Calculates minimum output with slippage
        4. Builds approve TX if needed
        5. Builds swap TX

        For cross-chain swaps (when destination_chain is set), uses Enso
        for routing which handles the bridging automatically.

        For Solana chains, routes to Jupiter aggregator.

        Args:
            intent: SwapIntent to compile

        Returns:
            CompilationResult with swap ActionBundle
        """
        # Route to Jupiter for Solana chains
        if self._is_solana_chain():
            protocol = intent.protocol
            allowed_solana_swap = {None, "jupiter"}
            if protocol and protocol.lower() not in allowed_solana_swap:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{protocol}' is not supported for SWAP on Solana. Supported: jupiter",
                )
            return self._compile_jupiter_swap(intent)

        # Check for cross-chain swap - route to appropriate aggregator
        # Preserve historical behavior: protocol=None defaults to Enso for cross-chain swaps.
        if intent.is_cross_chain:
            if intent.protocol is not None:
                from ..connectors.protocol_aliases import normalize_protocol

                protocol = normalize_protocol(self.chain, intent.protocol)
                if protocol == "lifi":
                    return self._compile_lifi_swap(intent)
            return self._compile_cross_chain_swap(intent)

        # Check for aggregator protocols
        protocol = self._resolve_protocol(intent.protocol)

        # Handle Pendle first: explicit protocol OR auto-detect PT-/YT- prefixed tokens.
        # Must run before other protocol dispatches so that PT-/YT- tokens are routed to
        # Pendle regardless of default_protocol (e.g., enso, aerodrome). VIB-2535.
        if protocol == "pendle":
            return self._compile_pendle_swap(intent)
        if intent.protocol is None:
            to_upper = (intent.to_token or "").upper()
            from_upper = (intent.from_token or "").upper()
            if to_upper.startswith(("PT-", "YT-")) or from_upper.startswith(("PT-", "YT-")):
                return self._compile_pendle_swap(intent)

        if protocol == "enso":
            return self._compile_enso_swap(intent)
        if protocol == "lifi":
            return self._compile_lifi_swap(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with different swap interface)
        # protocol is already resolved via _resolve_protocol() above (velodrome -> aerodrome on Optimism)
        if protocol == "aerodrome":
            return self._compile_swap_aerodrome(intent)

        # Handle Curve separately (pool-based AMM with direct pool addressing)
        if protocol == "curve":
            return self._compile_swap_curve(intent)

        # Handle Uniswap V4 separately (PoolManager-based singleton with different interface)
        if protocol == "uniswap_v4":
            return self._compile_swap_uniswap_v4(intent)

        # Handle Fluid DEX swaps (direct pool swapIn call)
        if protocol == "fluid":
            return self._compile_swap_fluid(intent)

        # Handle TraderJoe V2 swaps via dedicated LBRouter2 interface (VIB-1928).
        # LBRouter2 uses swapExactTokensForTokens with Path struct, NOT Uniswap V3's
        # exactInputSingle. Routed to dedicated method like Aerodrome/Curve.
        if protocol == "traderjoe_v2":
            return self._compile_swap_traderjoe_v2(intent)

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Resolve token addresses
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(
                    intent.amount_usd,
                    from_token,
                )
            elif intent.amount is not None:
                # Check for chained amount - must be resolved before compilation
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                # Type is validated above to be Decimal (not "all")
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Calculate minimum output with slippage
            try:
                expected_output = self._calculate_expected_output(amount_in, from_token, to_token)
            except ValueError as e:
                # Price unavailable -- fail-closed to prevent swaps with zero slippage protection.
                # Without a price, min_output would be 0 and the swap would be vulnerable to
                # sandwich attacks / MEV extraction.
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Cannot calculate slippage protection for {from_token.symbol} -> {to_token.symbol}: {e}. "
                        f"The price oracle does not have a price for one of the tokens. "
                        f"Ensure the token price is available via market.price() before swapping."
                    ),
                    intent_id=intent.intent_id,
                )

            # Step 4: Get protocol adapter
            protocol = self._resolve_protocol(intent.protocol)
            adapter = DefaultSwapAdapter(
                self.chain,
                protocol,
                pool_selection_mode=self._config.swap_pool_selection_mode,
                fixed_fee_tier=self._config.fixed_swap_fee_tier,
                rpc_url=self._get_chain_rpc_url(),
                rpc_timeout=self.rpc_timeout,
            )
            router_address = adapter.get_router_address()

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown router for protocol {protocol} on {self.chain}.",
                    intent_id=intent.intent_id,
                )

            # Step 5: Build approve TX if needed (skip for native token)
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 6: Build swap TX
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            # Handle native token wrapping if needed
            value = 0
            actual_from_token = from_token.address
            if from_token.is_native:
                # Swapping from native - send ETH value
                value = amount_in
                # Use WETH for the swap
                weth_address = self._get_wrapped_native_address() or from_token.address
                actual_from_token = weth_address
                warnings.append("Native token swap: will wrap to WETH before swapping")

            actual_to_token = to_token.address
            if to_token.is_native:
                # Swapping to native - receive WETH, then unwrap
                weth_address = self._get_wrapped_native_address() or to_token.address
                actual_to_token = weth_address
                warnings.append("Native token output: will receive WETH, unwrap separately")

            # Pre-select fee tier to make quoter data available for slippage adjustment.
            # This also parallelizes fee tier queries for faster compilation.
            # Wrapped in try/except so that RPC failures in the quoter path degrade
            # gracefully to the oracle-only slippage estimate instead of crashing compilation.
            try:
                adapter.select_fee_tier(actual_from_token, actual_to_token, amount_in)
            except Exception as exc:
                logger.warning("Fee tier pre-selection failed, falling back to oracle estimate: %s", exc)

            # Tighten slippage using quoter data when available.
            # The on-chain quoter reflects actual pool liquidity and is more accurate
            # than the price oracle estimate. Use the lower of the two to protect against
            # both quoter overestimates and stale oracle prices.
            oracle_estimate = expected_output
            quoter_amount = adapter.get_quoted_amount_out()
            if quoter_amount is not None and quoter_amount < expected_output:
                logger.info(
                    "Quoter amount (%s) is lower than price oracle estimate (%s) — "
                    "using quoter amount as slippage basis for safer execution",
                    quoter_amount,
                    expected_output,
                )
                expected_output = quoter_amount

            # Price impact guard: fail compilation if quoter deviates too far from oracle.
            # This catches zero/low-liquidity pools where slippage protection is meaningless
            # because the quoter amount itself is catastrophically bad.
            # Skip when using placeholder prices — oracle estimates are unreliable in that mode.
            if quoter_amount is not None and oracle_estimate > 0 and not self._using_placeholders:
                price_impact = Decimal(1) - (Decimal(quoter_amount) / Decimal(oracle_estimate))
                max_impact = (
                    intent.max_price_impact
                    if intent.max_price_impact is not None
                    else self._config.max_price_impact_pct
                )
                if price_impact > max_impact:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Price impact too high: quoter returned amount implying "
                            f"{price_impact:.1%} price impact "
                            f"(oracle estimate: {oracle_estimate}, quoter: {quoter_amount}). "
                            f"Maximum allowed: {max_impact:.0%}. "
                            f"Likely cause: pool has insufficient liquidity for {intent.from_token}->{intent.to_token}."
                        ),
                    )
            elif quoter_amount is None and oracle_estimate > 0 and not self._using_placeholders:
                logger.warning(
                    "Price impact guard skipped: quoter returned None (RPC may be unavailable). "
                    "Proceeding with oracle-only estimate for %s->%s.",
                    intent.from_token,
                    intent.to_token,
                )

            min_output = int(Decimal(str(expected_output)) * (Decimal("1") - intent.max_slippage))

            # Generate swap calldata (uses cached fee tier from select_fee_tier above)
            swap_calldata = adapter.get_swap_calldata(
                from_token=actual_from_token,
                to_token=actual_to_token,
                amount_in=amount_in,
                min_amount_out=min_output,
                recipient=self.wallet_address,
                deadline=deadline,
            )

            # Validate pool existence (best-effort, after fee tier is selected)
            selected_fee = adapter.last_fee_selection.get("selected_fee_tier")
            if selected_fee is not None:
                from .pool_validation import validate_v3_pool

                pool_check = validate_v3_pool(
                    self.chain, protocol, actual_from_token, actual_to_token, selected_fee, self._get_chain_rpc_url()
                )
                failed = self._validate_pool(pool_check, intent.intent_id)
                if failed is not None:
                    return failed

            # Estimate gas
            swap_gas = adapter.estimate_gas(actual_from_token, actual_to_token)

            swap_tx = TransactionData(
                to=router_address,
                value=value,
                data="0x" + swap_calldata.hex(),
                gas_estimate=swap_gas,
                description=(
                    f"Swap {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol} (min: {self._format_amount(min_output, to_token.decimals)})"
                ),
                tx_type="swap",
            )
            transactions.append(swap_tx)

            # Step 7: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_output),
                    "slippage": str(intent.max_slippage),
                    "protocol": protocol,
                    "router": router_address,
                    "pool_selection_mode": self._config.swap_pool_selection_mode,
                    "selected_fee_tier": adapter.last_fee_selection.get("selected_fee_tier"),
                    "fee_tier_candidates": adapter.last_fee_selection.get("candidate_fee_tiers"),
                    "fee_selection_source": adapter.last_fee_selection.get("source"),
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            expected_out_fmt = format_token_amount(expected_output, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)

            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(f"{ok} Compiled SWAP: {amount_in_fmt} → {expected_out_fmt} (min: {min_out_fmt})")
            logger.info(f"   Slippage: {slippage_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}")

        except Exception as e:
            logger.exception(f"Failed to compile SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_enso_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a same-chain SWAP intent using Enso DEX aggregator.

        Enso provides DEX aggregation which may find better prices by routing
        through multiple DEXes. This method:
        1. Resolves token addresses
        2. Gets optimal route from Enso API (via gateway gRPC or direct client)
        3. Builds approve TX if needed
        4. Returns the transaction from Enso

        When a gateway_client is available, the route is fetched via the gateway's
        EnsoService gRPC, keeping the API key in the gateway process. Falls back to
        the direct EnsoClient only when no gateway is connected (local dev).

        Args:
            intent: SwapIntent with protocol="enso"

        Returns:
            CompilationResult with Enso swap ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Resolve token addresses
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Get route from Enso (via gateway gRPC or direct client)
            logger.info(f"Getting Enso route: {from_token.symbol} -> {to_token.symbol}, amount={amount_in}")

            slippage_bps = int(intent.max_slippage * 10000)
            route_data = self._get_enso_route(from_token.address, to_token.address, str(amount_in), slippage_bps)

            # Step 4: Build approve TX if needed (skip for native token)
            router_address = route_data["to"]
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 5: Build swap TX from Enso route
            value = int(route_data["value"]) if route_data["value"] else 0
            swap_tx = TransactionData(
                to=route_data["to"],
                value=value,
                data=route_data["data"],
                gas_estimate=route_data["gas"] if route_data["gas"] else 200000,
                description=(
                    f"Swap via Enso: {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol}"
                ),
                tx_type="swap",
            )
            transactions.append(swap_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0

            # Calculate minimum output with slippage
            min_output = int(Decimal(str(amount_out)) * (Decimal("1") - intent.max_slippage))

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(min_output),
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "router": router_address,
                    "price_impact_bps": route_data.get("price_impact", 0),
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            price_impact_val = route_data.get("price_impact")
            price_impact_fmt = format_slippage_bps(price_impact_val) if price_impact_val is not None else "N/A"

            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(f"{ok} Compiled SWAP (Enso): {amount_in_fmt} → {amount_out_fmt} (min: {min_out_fmt})")
            logger.info(
                f"   Slippage: {slippage_fmt} | Impact: {price_impact_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Enso SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _get_enso_route(
        self,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via gateway gRPC or direct client.

        When a gateway_client is connected, routes through the gateway's
        EnsoService gRPC (API key stays in the gateway). Falls back to
        the direct EnsoClient for local development without a gateway.

        In deployed/managed mode (AGENT_ID set), the gateway is mandatory
        and no fallback to direct HTTP is attempted.

        Args:
            token_in: Input token address.
            token_out: Output token address.
            amount_in: Input amount in wei (as string).
            slippage_bps: Slippage tolerance in basis points.
            chain: Source chain override (defaults to self.chain).
            destination_chain_id: Target chain ID for cross-chain routes.
            receiver: Receiver address for cross-chain routes.
            refund_receiver: Refund receiver for cross-chain routes.

        Returns:
            Dict with keys: to, data, value, gas (int|None), amount_out, price_impact,
            and optionally bridge_fee, estimated_time, is_cross_chain for cross-chain routes.
        """
        if self._gateway_client is not None:
            if not self._gateway_client.is_connected:
                raise RuntimeError(
                    "Gateway client is configured but not connected; cannot fetch Enso route. "
                    "Ensure the gateway is running before compiling Enso intents."
                )
            return self._get_enso_route_via_gateway(
                token_in,
                token_out,
                amount_in,
                slippage_bps,
                chain=chain,
                destination_chain_id=destination_chain_id,
                receiver=receiver,
                refund_receiver=refund_receiver,
            )

        # No gateway client configured — only allowed in local dev.
        # In managed deployments the deployer always injects a gateway client;
        # this guard catches misconfiguration.
        if os.environ.get("AGENT_ID"):
            raise RuntimeError(
                "Enso route request failed: no gateway client configured. "
                "In deployed mode, all Enso API calls must go through the gateway."
            )

        return self._get_enso_route_direct(
            token_in,
            token_out,
            int(amount_in),
            slippage_bps,
            chain=chain,
            destination_chain_id=destination_chain_id,
            receiver=receiver,
            refund_receiver=refund_receiver,
        )

    def _get_enso_route_via_gateway(
        self,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via gateway's EnsoService gRPC."""
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.EnsoRouteRequest(
            chain=chain or self.chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            from_address=self.wallet_address,
            slippage_bps=slippage_bps,
            routing_strategy="router",
            destination_chain_id=destination_chain_id or 0,
            receiver=receiver or "",
            refund_receiver=refund_receiver or "",
        )
        response = self._gateway_client.enso.GetRoute(request, timeout=30.0)  # type: ignore[union-attr]

        if not response.success:
            raise RuntimeError(f"Gateway Enso GetRoute failed: {response.error}")

        gas_str = response.gas or response.gas_estimate
        result = {
            "to": response.to,
            "data": response.data,
            "value": response.value,
            "gas": int(gas_str) if gas_str and gas_str != "0" else None,
            "amount_out": response.amount_out,
            "price_impact": response.price_impact,
        }

        if response.is_cross_chain:
            result["bridge_fee"] = response.bridge_fee
            result["estimated_time"] = response.estimated_time
            result["is_cross_chain"] = True

        return result

    def _get_enso_route_direct(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        """Get Enso route via direct HTTP client (local dev fallback)."""
        from ..connectors.enso import EnsoClient, EnsoConfig

        config = EnsoConfig(
            chain=chain or self.chain,
            wallet_address=self.wallet_address,
        )
        client = EnsoClient(config)
        route = client.get_route(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            destination_chain_id=destination_chain_id,
            refund_receiver=refund_receiver,
        )

        result: dict[str, Any] = {
            "to": route.tx.to,
            "data": route.tx.data,
            "value": str(route.tx.value) if route.tx.value else "0",
            "gas": int(route.gas) if route.gas else None,
            "amount_out": str(route.get_amount_out_wei()),
            "price_impact": route.price_impact,
        }

        if destination_chain_id:
            result["bridge_fee"] = getattr(route, "bridge_fee", None)
            result["estimated_time"] = getattr(route, "estimated_time", None)
            result["is_cross_chain"] = True

        return result

    def _compile_lifi_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a SWAP intent using LiFi aggregator.

        LiFi is a cross-chain liquidity meta-aggregator that routes through
        bridges (Across, Stargate, Hop, etc.) and DEXs (1inch, 0x, etc.).
        Supports both same-chain swaps and cross-chain bridge+swap operations.

        This method:
        1. Resolves token addresses for source (and destination) chains
        2. Gets quote from LiFi API with transaction data
        3. Builds approve TX if needed (standard ERC-20, no Permit2)
        4. Returns ActionBundle with deferred swap markers

        Args:
            intent: SwapIntent with protocol="lifi"

        Returns:
            CompilationResult with LiFi swap ActionBundle
        """
        from ..connectors.lifi import CHAIN_MAPPING, LiFiAdapter, LiFiConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Determine source and destination chains
            source_chain = intent.chain or self.chain
            dest_chain = intent.destination_chain or source_chain
            is_cross_chain = source_chain != dest_chain

            # Resolve chain IDs
            source_chain_lower = source_chain.lower()
            dest_chain_lower = dest_chain.lower()

            if source_chain_lower not in CHAIN_MAPPING:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {source_chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                    intent_id=intent.intent_id,
                )
            if dest_chain_lower not in CHAIN_MAPPING:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {dest_chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                    intent_id=intent.intent_id,
                )

            from_chain_id = CHAIN_MAPPING[source_chain_lower]
            to_chain_id = CHAIN_MAPPING[dest_chain_lower]

            # Step 2: Resolve token addresses
            from_token = self._resolve_token(intent.from_token, chain=source_chain)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {source_chain}: {intent.from_token}",
                    intent_id=intent.intent_id,
                )

            to_token = self._resolve_token(intent.to_token, chain=dest_chain)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {dest_chain}: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 3: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 4: Translate native token addresses for LiFi API
            # Framework uses 0xEeee... sentinel for native tokens, but LiFi expects 0x0000...0000
            from ..connectors.lifi.client import NATIVE_TOKEN_ADDRESS as LIFI_NATIVE_ADDRESS

            lifi_from_address = LIFI_NATIVE_ADDRESS if from_token.is_native else from_token.address
            lifi_to_address = LIFI_NATIVE_ADDRESS if to_token.is_native else to_token.address

            # Step 5: Get quote from LiFi
            logger.info(
                f"Getting LiFi quote: {from_token.symbol}@{source_chain} -> {to_token.symbol}@{dest_chain}, "
                f"amount={amount_in}"
            )

            config = LiFiConfig(
                chain_id=from_chain_id,
                wallet_address=self.wallet_address,
            )
            adapter = LiFiAdapter(
                config,
                price_provider=self.price_oracle,
                allow_placeholder_prices=self._using_placeholders,
            )

            slippage = float(intent.max_slippage)
            quote = adapter.client.get_quote(
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                from_token=lifi_from_address,
                to_token=lifi_to_address,
                from_amount=str(amount_in),
                from_address=self.wallet_address,
                slippage=slippage,
            )

            # Step 5: Build approve TX if needed (skip for native token)
            approval_address = quote.estimate.approval_address if quote.estimate else ""
            if approval_address and not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    approval_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 6: Build swap/bridge TX from LiFi quote
            tx_request = quote.transaction_request
            if tx_request is None or not tx_request.to or not tx_request.data:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="LiFi quote missing transaction_request data",
                    intent_id=intent.intent_id,
                )
            tx_type = "bridge_deferred" if is_cross_chain else "swap_deferred"
            description_action = "Bridge" if is_cross_chain else "Swap"

            raw_value = tx_request.value if tx_request else None
            if raw_value:
                raw_str = str(raw_value)
                value = int(raw_str, 16) if raw_str.startswith("0x") else int(raw_str)
            else:
                value = 0
            gas_estimate = 200000
            if quote.estimate and quote.estimate.total_gas_estimate > 0:
                gas_estimate = quote.estimate.total_gas_estimate
            elif tx_request and tx_request.gas_limit:
                try:
                    gl = str(tx_request.gas_limit)
                    gas_estimate = int(gl, 16) if gl.startswith("0x") else int(gl)
                except (ValueError, TypeError):
                    pass

            swap_tx = TransactionData(
                to=tx_request.to if tx_request else "",
                value=value,
                data=tx_request.data if tx_request else "",
                gas_estimate=gas_estimate,
                description=(
                    f"{description_action} via LiFi ({quote.tool}): "
                    f"{self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol}"
                ),
                tx_type=tx_type,
            )
            transactions.append(swap_tx)

            # Step 7: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = quote.get_to_amount()
            amount_out_min = quote.get_to_amount_min()

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(amount_out_min),
                    "slippage": str(intent.max_slippage),
                    "protocol": "lifi",
                    "tool": quote.tool,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "is_cross_chain": is_cross_chain,
                    "deferred_swap": True,
                    "route_params": {
                        "from_chain_id": from_chain_id,
                        "to_chain_id": to_chain_id,
                        "from_token": lifi_from_address,
                        "to_token": lifi_to_address,
                        "from_amount": str(amount_in),
                        "from_address": self.wallet_address,
                        "to_address": self._resolve_dest_wallet(dest_chain) if is_cross_chain else self.wallet_address,
                        "slippage": slippage,
                    },
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(amount_out_min, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)

            chain_info = f"{source_chain}->{dest_chain}" if is_cross_chain else source_chain
            logger.info(
                f"Compiled SWAP (LiFi/{quote.tool}): {amount_in_fmt} -> {amount_out_fmt} "
                f"(min: {min_out_fmt}) [{chain_info}]"
            )
            logger.info(f"   Slippage: {slippage_fmt} | Txs: {len(transactions)} | Gas: {total_gas:,}")

        except Exception as e:
            logger.exception("Failed to compile LiFi SWAP intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_cross_chain_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile a cross-chain SWAP intent using Enso.

        Cross-chain swaps use Enso's routing which handles bridging automatically.
        This method:
        1. Resolves token addresses for source and destination chains
        2. Gets cross-chain route from Enso API (via gateway gRPC or direct client)
        3. Builds approve TX if needed
        4. Returns the transaction from Enso

        Args:
            intent: SwapIntent with destination_chain set

        Returns:
            CompilationResult with cross-chain swap ActionBundle
        """
        from ..connectors.enso import CHAIN_MAPPING

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            source_chain = intent.chain or self.chain
            dest_chain = intent.destination_chain

            if not dest_chain:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Cross-chain swap requires destination_chain to be set",
                    intent_id=intent.intent_id,
                )

            # Step 1: Resolve token addresses for source chain
            from_token = self._resolve_token(intent.from_token, chain=source_chain)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {source_chain}: {intent.from_token}",
                    intent_id=intent.intent_id,
                )

            # Resolve token on destination chain
            to_token = self._resolve_token(intent.to_token, chain=dest_chain)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {dest_chain}: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Calculate input amount
            if intent.amount_usd is not None:
                amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation for cross-chain swaps.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            # Step 3: Get cross-chain route from Enso (via gateway gRPC or direct client)
            logger.info(
                f"Getting cross-chain route: {source_chain} {from_token.symbol} -> {dest_chain} {to_token.symbol}, amount={amount_in}"
            )

            slippage_bps = int(intent.max_slippage * 10000)
            dest_chain_id = CHAIN_MAPPING.get(dest_chain.lower())
            if dest_chain_id is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported destination chain: {dest_chain}",
                    intent_id=intent.intent_id,
                )

            dest_wallet = self._resolve_dest_wallet(dest_chain)
            route_data = self._get_enso_route(
                from_token.address,
                to_token.address,
                str(amount_in),
                slippage_bps,
                chain=source_chain,
                destination_chain_id=dest_chain_id,
                receiver=dest_wallet,
                refund_receiver=dest_wallet,
            )

            # Step 4: Build approve TX if needed (skip for native token)
            router_address = route_data["to"]
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Step 5: Build swap TX from Enso route
            value = int(route_data["value"]) if route_data["value"] else 0
            swap_tx = TransactionData(
                to=route_data["to"],
                value=value,
                data=route_data["data"],
                gas_estimate=route_data["gas"] if route_data["gas"] else 300000,
                description=(
                    f"Cross-chain swap via Enso: {self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} ({source_chain}) -> {to_token.symbol} ({dest_chain})"
                ),
                tx_type="cross_chain_swap",
            )
            transactions.append(swap_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0
            bridge_fee = route_data.get("bridge_fee")
            estimated_time = route_data.get("estimated_time")

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "router": router_address,
                    "source_chain": source_chain,
                    "destination_chain": dest_chain,
                    "is_cross_chain": True,
                    "bridge_fee": bridge_fee,
                    "estimated_time": estimated_time,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled cross-chain SWAP intent: {from_token.symbol} ({source_chain}) -> {to_token.symbol} ({dest_chain}), {len(transactions)} txs, bridge_fee={bridge_fee}, est_time={estimated_time}s"
            )

        except Exception as e:
            logger.exception(f"Failed to compile cross-chain SWAP intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP_OPEN intent into an ActionBundle.

        This method:
        1. Resolves pool token addresses
        2. Converts price range to tick range (or bin range for TraderJoe)
        3. Calculates minimum amounts with slippage
        4. Builds approve TXs for both tokens
        5. Builds mint position TX

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with LP mint ActionBundle
        """
        # Route Meteora DLMM to Solana-specific adapter
        if intent.protocol == "meteora_dlmm":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Meteora DLMM is only supported on Solana",
                )
            return self._compile_meteora_lp_open(intent)

        # Route Orca Whirlpools to Solana-specific adapter
        if intent.protocol == "orca_whirlpools":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Orca Whirlpools is only supported on Solana",
                )
            return self._compile_orca_lp_open(intent)

        # Route Raydium CLMM to Solana-specific adapter (default LP protocol on Solana)
        if intent.protocol == "raydium_clmm" or (self._is_solana_chain() and intent.protocol is None):
            return self._compile_raydium_lp_open(intent)

        # Fail explicitly for unsupported protocols on Solana
        if self._is_solana_chain():
            allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for LP_OPEN on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
            )

        # Handle Uniswap V4 LP separately (flash accounting via PositionManager)
        if self._resolve_protocol(intent.protocol) == "uniswap_v4":
            return self._compile_lp_open_uniswap_v4(intent)

        # Handle TraderJoe V2 separately (different architecture - bins vs ticks)
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_open_traderjoe_v2(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with fungible LP tokens)
        # Resolve alias so velodrome -> aerodrome on Optimism (LP dispatch doesn't pre-resolve)
        if self._resolve_protocol(intent.protocol) == "aerodrome":
            return self._compile_lp_open_aerodrome(intent)

        # Handle Pendle LP (single-token liquidity provision)
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_open(intent)

        # Handle Curve LP (pool-based AMM with proportional liquidity)
        if intent.protocol == "curve":
            return self._compile_lp_open_curve(intent)

        # Handle Fluid DEX LP (Arbitrum only, unencumbered positions)
        if intent.protocol == "fluid":
            return self._compile_lp_open_fluid(intent)

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
            protocol = self._resolve_protocol(intent.protocol)
            adapter = UniswapV3LPAdapter(self.chain, protocol)
            position_manager = adapter.get_position_manager_address()

            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown position manager for protocol {protocol} on {self.chain}"),
                    intent_id=intent.intent_id,
                )

            # Step 2: Parse pool info to get token addresses
            # Pool format expected: "0xPoolAddress" or "TOKEN0/TOKEN1/FEE"
            pool_info = self._parse_pool_info(intent.pool)
            if pool_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Could not parse pool info: {intent.pool}",
                    intent_id=intent.intent_id,
                )

            token0_info, token1_info, fee_tier, tokens_swapped = pool_info

            # When tokens were reordered to match on-chain convention (token0 addr < token1 addr),
            # we must invert the price range and swap the amounts to stay consistent.
            # The user specified prices as "token1-per-token0" in their original ordering.
            # After swapping, that relationship is inverted: new price = 1 / old price.
            range_lower = intent.range_lower
            range_upper = intent.range_upper
            amount0 = intent.amount0
            amount1 = intent.amount1

            if tokens_swapped:
                # Invert price range: if user said 550-670 (WBNB in USDT), after swap
                # token0=USDT, token1=WBNB, so price is now WBNB-per-USDT = 1/550 to 1/670.
                # new_lower = 1/old_upper, new_upper = 1/old_lower (preserves lower < upper).
                range_lower = Decimal(1) / intent.range_upper
                range_upper = Decimal(1) / intent.range_lower
                # Swap amounts to match new token order
                amount0, amount1 = amount1, amount0
                logger.debug(
                    f"Tokens swapped: inverted price range [{intent.range_lower}, {intent.range_upper}] "
                    f"-> [{range_lower:.10f}, {range_upper:.10f}], swapped amounts"
                )

            # Validate pool existence (best-effort)
            from .pool_validation import validate_v3_pool

            pool_check = validate_v3_pool(
                self.chain,
                protocol,
                token0_info.address,
                token1_info.address,
                fee_tier,
                self._get_chain_rpc_url(),
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Step 3: Convert amounts to wei
            amount0_desired = int(amount0 * Decimal(10**token0_info.decimals))
            amount1_desired = int(amount1 * Decimal(10**token1_info.decimals))

            # Step 4: Convert price range to ticks
            # Uniswap V3 uses tick-based ranges: price = 1.0001^tick
            # Price must be adjusted for token decimals difference
            tick_lower = self._price_to_tick(
                range_lower,
                token0_decimals=token0_info.decimals,
                token1_decimals=token1_info.decimals,
            )
            tick_upper = self._price_to_tick(
                range_upper,
                token0_decimals=token0_info.decimals,
                token1_decimals=token1_info.decimals,
            )

            # Align ticks to tick spacing (60 for 0.3% fee tier)
            tick_spacing = self._get_tick_spacing(fee_tier)
            tick_lower = (tick_lower // tick_spacing) * tick_spacing
            tick_upper = (tick_upper // tick_spacing) * tick_spacing

            if tick_lower >= tick_upper:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "LP_OPEN tick range collapsed after applying pool tick spacing. "
                        "Widen the price range so lower and upper ticks differ."
                    ),
                    intent_id=intent.intent_id,
                )

            logger.debug(
                f"LP tick calculation: price_range=[{range_lower:.8f}, {range_upper:.8f}]"
                f"{' (inverted)' if tokens_swapped else ''}, "
                f"decimals=({token0_info.decimals}, {token1_info.decimals}), "
                f"ticks=[{tick_lower}, {tick_upper}], spacing={tick_spacing}"
            )

            # Step 4b: Recompute amounts from on-chain sqrtPriceX96 to prevent "Price slippage check" reverts.
            # When oracle price diverges from pool price, the pool takes a different token ratio than
            # expected; if desired amounts don't match the pool ratio, the NonfungiblePositionManager
            # reverts with "Price slippage check". Fetching slot0() and running getLiquidityForAmounts
            # + getAmountsForLiquidity aligns amounts to the pool's actual price.
            if pool_check.pool_address:
                rpc_url_for_slot0 = self._get_chain_rpc_url()
                if rpc_url_for_slot0:
                    from .lp_math import recompute_lp_amounts
                    from .pool_validation import fetch_v3_pool_sqrt_price_x96

                    try:
                        slot0_result = fetch_v3_pool_sqrt_price_x96(pool_check.pool_address, rpc_url_for_slot0)
                    except Exception as exc:
                        logger.warning(
                            "LP slot0 lookup failed for pool %s; proceeding with oracle-derived amounts "
                            "which may cause 'Price slippage check' revert if oracle/pool prices diverge: %s",
                            pool_check.pool_address,
                            exc,
                        )
                        slot0_result = None
                    if slot0_result is not None:
                        sqrt_price_x96, current_tick = slot0_result
                    else:
                        sqrt_price_x96, current_tick = None, None
                    if sqrt_price_x96 is not None and sqrt_price_x96 > 0:
                        a0_corrected, a1_corrected = recompute_lp_amounts(
                            sqrt_price_x96,
                            tick_lower,
                            tick_upper,
                            amount0_desired,
                            amount1_desired,
                            current_tick=current_tick,
                        )
                        if a0_corrected == 0 and a1_corrected == 0 and (amount0_desired > 0 or amount1_desired > 0):
                            return CompilationResult(
                                status=CompilationStatus.FAILED,
                                error=(
                                    "LP_OPEN cannot mint liquidity at the pool's current price for the "
                                    "supplied range/amounts. Adjust the tick range or token amounts."
                                ),
                                intent_id=intent.intent_id,
                            )
                        if a0_corrected > 0 or a1_corrected > 0:
                            logger.debug(
                                f"LP amounts recomputed from on-chain price: "
                                f"({amount0_desired}, {amount1_desired}) -> ({a0_corrected}, {a1_corrected})"
                            )
                            amount0_desired = a0_corrected
                            amount1_desired = a1_corrected

            # Step 5: Calculate minimum amounts using LP slippage
            # LP slippage is different from swap slippage:
            # - In swaps, slippage = receiving fewer tokens (real loss)
            # - In LP, slippage = different deposit ratio (no loss, just different position)
            # Default 20% slippage (80% minimum), configurable to 100% (0 minimum) for volatile pairs
            # protocol_params.lp_slippage overrides default (e.g. 1.0 = zero minimums, safe for testing)
            protocol_lp_slippage = (intent.protocol_params or {}).get("lp_slippage")
            lp_slippage = (
                min(max(Decimal(str(protocol_lp_slippage)), Decimal("0")), Decimal("1"))
                if protocol_lp_slippage is not None
                else (getattr(intent, "max_slippage", None) or self.default_lp_slippage)
            )
            min_multiplier = Decimal("1") - lp_slippage  # 0.80 for 20% slippage
            amount0_min = int(amount0_desired * min_multiplier)
            amount1_min = int(amount1_desired * min_multiplier)

            logger.debug(
                f"LP mint: slippage={float(lp_slippage) * 100:.1f}%, amount0={amount0_desired} (min={amount0_min}), amount1={amount1_desired} (min={amount1_min})"
            )

            # Step 6: Build approve TXs for both tokens
            if amount0_desired > 0 and not token0_info.is_native:
                approve_txs0 = self._build_approve_tx(
                    token0_info.address,
                    position_manager,
                    amount0_desired,
                )
                transactions.extend(approve_txs0)

            if amount1_desired > 0 and not token1_info.is_native:
                approve_txs1 = self._build_approve_tx(
                    token1_info.address,
                    position_manager,
                    amount1_desired,
                )
                transactions.extend(approve_txs1)

            # Step 7: Build mint TX
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            mint_calldata = adapter.get_mint_calldata(
                token0=token0_info.address,
                token1=token1_info.address,
                fee=fee_tier,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                amount0_min=amount0_min,
                amount1_min=amount1_min,
                recipient=self.wallet_address,
                deadline=deadline,
            )

            # Handle native token (ETH) - send value with transaction
            value = 0
            if token0_info.is_native:
                value = amount0_desired
                warnings.append("Token0 is native - sending ETH with transaction")
            elif token1_info.is_native:
                value = amount1_desired
                warnings.append("Token1 is native - sending ETH with transaction")

            mint_tx = TransactionData(
                to=position_manager,
                value=value,
                data="0x" + mint_calldata.hex(),
                gas_estimate=adapter.estimate_mint_gas(),
                description=(
                    f"Mint LP position: "
                    f"{self._format_amount(amount0_desired, token0_info.decimals)} "
                    f"{token0_info.symbol} + "
                    f"{self._format_amount(amount1_desired, token1_info.decimals)} "
                    f"{token1_info.symbol} "
                    f"[{intent.range_lower:.2f} - {intent.range_upper:.2f}]"
                ),
                tx_type="lp_mint",
            )
            transactions.append(mint_tx)

            # Step 8: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "token0": token0_info.to_dict(),
                    "token1": token1_info.to_dict(),
                    "fee_tier": fee_tier,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount0_desired": str(amount0_desired),
                    "amount1_desired": str(amount1_desired),
                    "amount0_min": str(amount0_min),
                    "amount1_min": str(amount1_min),
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled LP_OPEN intent: {token0_info.symbol}/{token1_info.symbol}, range [{intent.range_lower:.2f}-{intent.range_upper:.2f}], {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile LP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_fluid(self, intent: "LPOpenIntent") -> "CompilationResult":
        """Compile LP_OPEN intent for Fluid DEX T1 (Arbitrum only).

        Phase 1 limitation: LP deposit is not yet supported on-chain.
        Fluid DEX deposit() reverts due to complex Liquidity-layer routing.
        This method short-circuits with a clear FAILED status.
        """
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                "Fluid DEX LP_OPEN is not supported in phase 1. "
                "The Liquidity-layer routing causes on-chain reverts on all pools. "
                "LP deposit support is a follow-up. Use swap intents instead."
            ),
            intent_id=intent.intent_id,
        )

    def _compile_lp_close_fluid(self, intent: "LPCloseIntent") -> "CompilationResult":
        """Compile LP_CLOSE intent for Fluid DEX T1 (with encumbrance guard).

        ENCUMBRANCE GUARD: Rejects compilation if the pool has smart-collateral
        or smart-debt enabled, preventing liquidation risk.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            from almanak.framework.connectors.fluid import FluidAdapter, FluidConfig

            try:
                nft_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Fluid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            dex_address = intent.pool
            if not dex_address or not dex_address.startswith("0x"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Fluid LP_CLOSE requires pool address in pool field. Got pool={intent.pool}"),
                    intent_id=intent.intent_id,
                )

            # Prefer the gateway transport when a gateway client is injected
            # AND actually connected. An injected-but-disconnected client
            # (e.g., construction order bug in local setups) falls back to
            # the direct RPC URL — mirrors _get_enso_route() behavior.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            if gateway_client is None:
                rpc_url = self._get_chain_rpc_url()
                if not rpc_url:
                    raise ValueError("Connected gateway_client or RPC URL required for Fluid DEX adapter.")
            else:
                rpc_url = None

            config = FluidConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            fluid_adapter = FluidAdapter(config)

            # COMPILE-TIME ENCUMBRANCE GUARD — raises if pool has smart-debt/collateral
            lp_tx = fluid_adapter.build_remove_liquidity_transaction(
                dex_address=dex_address,
                nft_id=nft_id,
            )

            transactions.append(
                TransactionData(
                    to=lp_tx.to,
                    value=lp_tx.value,
                    data=lp_tx.data,
                    gas_estimate=lp_tx.gas,
                    description=lp_tx.description,
                    tx_type="fluid_operate_close",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "dex_address": dex_address,
                    "nft_id": nft_id,
                    "protocol": "fluid",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Fluid LP_CLOSE intent: nft_id={nft_id}, pool={dex_address}, "
                f"{len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Fluid LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_uniswap_v4(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Uniswap V4 via PositionManager.

        V4 uses flash accounting (modifyLiquidities + Actions-encoded bytes).
        This delegates to the UniswapV4Adapter which handles the full encoding.
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = UniswapV4Adapter(
                config=config, token_resolver=self._token_resolver, gateway_client=self._gateway_client
            )
            bundle = adapter.compile_lp_open_intent(intent, self.price_oracle)

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_OPEN compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="approve" if "approve" in tx.get("description", "").lower() else "lp_mint",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            # Forward warnings
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_OPEN intent: %d txs, %d gas, pool=%s",
                len(bundle.transactions),
                result.total_gas_estimate,
                intent.pool,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_OPEN intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_uniswap_v4(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Uniswap V4 via PositionManager.

        V4 uses flash accounting (modifyLiquidities + Actions-encoded bytes).
        This delegates to the UniswapV4Adapter which handles the full encoding.

        Note: In production, the caller should provide liquidity and currency addresses
        from an on-chain position query. For offline compilation, we use placeholder
        values that will be updated at execution time.
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            rpc_url = self._get_chain_rpc_url()
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
            )
            adapter = UniswapV4Adapter(
                config=config, token_resolver=self._token_resolver, gateway_client=self._gateway_client
            )

            # Extract liquidity and currency addresses from protocol_params if available
            # LPCloseIntent may not have protocol_params field
            liquidity = 0
            currency0 = ""
            currency1 = ""
            protocol_params = getattr(intent, "protocol_params", None) or {}
            if protocol_params:
                liquidity = int(protocol_params.get("liquidity", 0))
                currency0 = protocol_params.get("currency0", "")
                currency1 = protocol_params.get("currency1", "")

            # If pool is specified, try to resolve currency addresses
            if (not currency0 or not currency1) and intent.pool:
                try:
                    parts = intent.pool.split("/")
                    if len(parts) >= 2:
                        addr0, _ = adapter._resolve_token(parts[0], for_v4_pool=True)
                        addr1, _ = adapter._resolve_token(parts[1], for_v4_pool=True)
                        # Ensure sorted order
                        if int(addr0, 16) > int(addr1, 16):
                            addr0, addr1 = addr1, addr0
                        currency0 = addr0
                        currency1 = addr1
                except (ValueError, KeyError) as e:
                    logger.debug("Could not resolve currencies from pool '%s': %s", type(e).__name__, e)
                except Exception as e:
                    # TokenNotFoundError/TokenResolutionError or unexpected errors — log, don't swallow
                    logger.warning("Failed to resolve currencies from pool '%s': %s", intent.pool, e)

            # If liquidity not provided, query on-chain via PositionManager.getPositionLiquidity(tokenId)
            if liquidity == 0:
                try:
                    token_id = int(intent.position_id)
                except (ValueError, TypeError):
                    result.status = CompilationStatus.FAILED
                    result.error = f"V4 LP_CLOSE: invalid position_id '{intent.position_id}' (must be numeric)"
                    return result
                try:
                    liquidity = adapter.get_position_liquidity(token_id, rpc_url=rpc_url)
                    logger.info("V4 LP_CLOSE: queried on-chain liquidity=%d for position %d", liquidity, token_id)
                except Exception as e:
                    result.status = CompilationStatus.FAILED
                    result.error = (
                        f"V4 LP_CLOSE: could not determine position liquidity. "
                        f"Either provide 'liquidity' in protocol_params or ensure RPC is available. Error: {e}"
                    )
                    return result
            if not currency0 or not currency1:
                result.status = CompilationStatus.FAILED
                result.error = (
                    "V4 LP_CLOSE requires 'currency0' and 'currency1' in protocol_params "
                    "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                )
                return result

            # Enforce canonical V4 ordering: currency0 < currency1
            if int(currency0, 16) > int(currency1, 16):
                currency0, currency1 = currency1, currency0

            bundle = adapter.compile_lp_close_intent(
                intent,
                liquidity=liquidity,
                currency0=currency0,
                currency1=currency1,
            )

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_CLOSE compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="lp_close",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_CLOSE intent: position_id=%s, %d txs, %d gas",
                intent.position_id,
                len(bundle.transactions),
                result.total_gas_estimate,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_CLOSE intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_traderjoe_v2(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 uses discrete price bins instead of continuous ticks:
        - Price at bin ID: price = (1 + binStep/10000)^(binId - 8388608)
        - Liquidity is distributed across bins with explicit distributions
        - LP tokens are fungible ERC1155-like tokens per bin (not NFTs)

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Import TraderJoe V2 adapter (lazy import to avoid circular deps)
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses and info via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {token_x_symbol} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )
            if not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token {token_y_symbol} for chain {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Validate pool existence (best-effort)
            from .pool_validation import validate_traderjoe_pool

            pool_check = validate_traderjoe_pool(
                self.chain,
                token_x_addr,
                token_y_addr,
                bin_step,
                self._get_chain_rpc_url(),
                allow_empty_reserves=True,  # LP_OPEN can seed empty pools
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Convert amounts to wei
            amount_x_wei = int(intent.amount0 * Decimal(10**token_x_info.decimals))
            amount_y_wei = int(intent.amount1 * Decimal(10**token_y_info.decimals))

            # Get router address (position manager for TraderJoe V2)
            router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
                "traderjoe_v2", "0x0000000000000000000000000000000000000000"
            )

            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"TraderJoe V2 not configured for chain {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Build approval TXs for both tokens
            if amount_x_wei > 0 and not token_x_info.is_native:
                approve_txs_x = self._build_approve_tx(
                    token_x_info.address,
                    router_address,
                    amount_x_wei,
                )
                transactions.extend(approve_txs_x)

            if amount_y_wei > 0 and not token_y_info.is_native:
                approve_txs_y = self._build_approve_tx(
                    token_y_info.address,
                    router_address,
                    amount_y_wei,
                )
                transactions.extend(approve_txs_y)

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # (production path) or a direct RPC URL (local/backtest fallback).
            # Normalize a disconnected gateway_client to None so we can fall
            # back to rpc_url cleanly.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "TraderJoe V2 adapter requires either a connected gateway_client "
                    "or an RPC URL. Provide rpc_url to IntentCompiler or use "
                    "GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter to build the liquidity TX
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Number of bins on each side of active bin
            # Read from intent's protocol_params if provided, otherwise default to 5
            params = intent.protocol_params or {}
            bin_range = int(params.get("bin_range", 5))
            if bin_range < 1 or bin_range > 100:
                raise ValueError(f"bin_range must be between 1 and 100, got {bin_range}")
            id_slippage = int(params.get("id_slippage", 5))

            # Build add liquidity transaction
            lp_tx = tj_adapter.build_add_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                amount_x=intent.amount0,
                amount_y=intent.amount1,
                bin_step=bin_step,
                bin_range=bin_range,
                id_slippage=id_slippage,
            )

            # Convert to TransactionData format
            lp_tx_data = TransactionData(
                to=lp_tx.to,
                value=lp_tx.value,
                data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
                gas_estimate=lp_tx.gas or 400000,
                description=(
                    f"Add liquidity to TraderJoe V2: {intent.amount0} {token_x_symbol} + {intent.amount1} {token_y_symbol} (bin_step={bin_step})"
                ),
                tx_type="traderjoe_v2_add_liquidity",
            )
            transactions.append(lp_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "token_x": token_x_info.to_dict(),
                    "token_y": token_y_info.to_dict(),
                    "bin_step": bin_step,
                    "bin_range": bin_range,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount_x": str(amount_x_wei),
                    "amount_y": str(amount_y_wei),
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_OPEN intent: {token_x_symbol}/{token_y_symbol}, bin_step={bin_step}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP_CLOSE intent into an ActionBundle.

        This method:
        1. Builds decreaseLiquidity TX to remove all liquidity
        2. Builds collect TX to collect tokens and fees
        3. Optionally builds burn TX (if position is empty)

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with LP close ActionBundle
        """
        # Route Meteora DLMM to Solana-specific adapter
        if intent.protocol == "meteora_dlmm":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Meteora DLMM is only supported on Solana",
                )
            return self._compile_meteora_lp_close(intent)

        # Route Orca Whirlpools to Solana-specific adapter
        if intent.protocol == "orca_whirlpools":
            if not self._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Orca Whirlpools is only supported on Solana",
                )
            return self._compile_orca_lp_close(intent)

        # Route Raydium CLMM to Solana-specific adapter (default LP protocol on Solana)
        if intent.protocol == "raydium_clmm" or (self._is_solana_chain() and intent.protocol is None):
            return self._compile_raydium_lp_close(intent)

        # Fail explicitly for unsupported protocols on Solana
        if self._is_solana_chain():
            allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for LP_CLOSE on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
            )

        # Handle Uniswap V4 LP close separately (flash accounting via PositionManager)
        if self._resolve_protocol(intent.protocol) == "uniswap_v4":
            return self._compile_lp_close_uniswap_v4(intent)

        # Handle TraderJoe V2 separately
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_close_traderjoe_v2(intent)

        # Handle Aerodrome/Velodrome separately (Solidly-fork with fungible LP tokens)
        # Resolve alias so velodrome -> aerodrome on Optimism (LP dispatch doesn't pre-resolve)
        if self._resolve_protocol(intent.protocol) == "aerodrome":
            return self._compile_lp_close_aerodrome(intent)

        # Handle Pendle LP close
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_close(intent)

        # Handle Curve LP close (pool-based AMM, proportional removal)
        if intent.protocol == "curve":
            return self._compile_lp_close_curve(intent)

        # Handle Fluid DEX LP close (with encumbrance guard)
        if intent.protocol == "fluid":
            return self._compile_lp_close_fluid(intent)

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
            protocol = self._resolve_protocol(intent.protocol)
            adapter = UniswapV3LPAdapter(self.chain, protocol)
            position_manager = adapter.get_position_manager_address()

            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown position manager for protocol {protocol} on {self.chain}"),
                    intent_id=intent.intent_id,
                )

            # Step 2: Parse position ID to token ID
            try:
                token_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            # Step 3: Query position's actual liquidity and tokens owed from on-chain
            liquidity = self._query_position_liquidity(position_manager, token_id)
            if liquidity is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Could not query liquidity for position #{token_id}. Ensure rpc_url is provided to IntentCompiler.",
                    intent_id=intent.intent_id,
                )

            # Query tokens owed (fees + withdrawn liquidity that hasn't been collected)
            tokens_owed0, tokens_owed1 = self._query_position_tokens_owed(position_manager, token_id)
            tokens_owed_unknown = tokens_owed0 is None or tokens_owed1 is None
            if tokens_owed_unknown:
                warnings.append(f"Could not query tokens owed for position #{token_id} - collecting anyway")
            elif tokens_owed0 == 0 and tokens_owed1 == 0:
                warnings.append(
                    f"Position #{token_id} has no tokens owed pre-decrease - will still collect after close"
                )

            # Step 3a: Skip decreaseLiquidity if position has 0 liquidity
            # (position may already be closed or liquidity already removed)
            if liquidity == 0:
                warnings.append(f"Position #{token_id} has 0 liquidity - skipping decreaseLiquidity step")
            else:
                # Use 0 for min amounts to ensure position can be closed
                amount0_min = 0
                amount1_min = 0

                decrease_calldata = adapter.get_decrease_liquidity_calldata(
                    token_id=token_id,
                    liquidity=liquidity,
                    amount0_min=amount0_min,
                    amount1_min=amount1_min,
                    deadline=deadline,
                )

                decrease_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + decrease_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_decrease_liquidity"),
                    description=f"Decrease liquidity: position #{token_id} (remove all)",
                    tx_type="lp_decrease_liquidity",
                )
                transactions.append(decrease_tx)

            # Determine if position has anything to collect/burn
            # Treat unknown owed as potential activity (collect anyway to avoid leaving fees uncollected)
            position_has_activity = (
                liquidity > 0
                or tokens_owed_unknown
                or (tokens_owed0 is not None and tokens_owed1 is not None and (tokens_owed0 > 0 or tokens_owed1 > 0))
            )

            # Step 4: Build collect TX
            # Collect when requested AND position has activity (liquidity decreased or fees owed)
            # Skip collect on already-closed/burned positions to avoid guaranteed reverts
            if intent.collect_fees and position_has_activity:
                collect_calldata = adapter.get_collect_calldata(
                    token_id=token_id,
                    recipient=self.wallet_address,
                    amount0_max=MAX_UINT128,
                    amount1_max=MAX_UINT128,
                )

                collect_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + collect_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_collect"),
                    description=f"Collect tokens and fees: position #{token_id}",
                    tx_type="lp_collect",
                )
                transactions.append(collect_tx)
            elif intent.collect_fees:
                warnings.append(f"Skipping collect for position #{token_id} - position appears already closed")
            else:
                warnings.append("Skipping fee collection as collect_fees=False")

            # Step 5: Build burn TX
            # Only burn if position has activity (decreased liquidity or has tokens owed)
            # If position was already closed (0 liquidity, 0 tokens owed), skip burn
            # to avoid reverting on already-burned NFTs
            should_burn = position_has_activity

            if should_burn:
                burn_calldata = adapter.get_burn_calldata(token_id=token_id)

                burn_tx = TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + burn_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_burn"),
                    description=f"Burn position NFT: #{token_id}",
                    tx_type="lp_burn",
                )
                transactions.append(burn_tx)
            else:
                warnings.append(
                    f"Position #{token_id} appears already closed (0 liquidity, 0 tokens owed) - skipping burn"
                )

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "position_id": intent.position_id,
                    "token_id": token_id,
                    "pool": intent.pool,
                    "collect_fees": intent.collect_fees,
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "deadline": deadline,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled LP_CLOSE intent: position #{token_id}, collect_fees={intent.collect_fees}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_close_traderjoe_v2(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 LP close differs from Uniswap V3:
        - Need to query LP token balances per bin
        - Call removeLiquidity with bin IDs and amounts
        - No NFT to burn (fungible LP tokens)

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP close ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Import TraderJoe V2 adapter
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            if intent.pool is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="pool is required for TraderJoe V2 LP close",
                    intent_id=intent.intent_id,
                )
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # (production path) or a direct RPC URL (local/backtest fallback).
            # Treat a disconnected client as unavailable so we don't hand a
            # dead client to the adapter.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "Connected gateway_client or RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Get position to check if we have liquidity
            t0 = time.perf_counter()
            position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
            logger.debug(f"TraderJoe V2 get_position (LP_CLOSE): {time.perf_counter() - t0:.2f}s")

            if not position or not position.bin_ids:
                warnings.append("No LP position found to close")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build approval for LB tokens (ERC1155-like, need approveForAll)
            pool_addr = position.pool_address
            router_addr = tj_adapter.sdk.router_address
            approve_tx, approve_gas = tj_adapter.sdk.build_approve_for_all_transaction(
                pool_address=pool_addr,
                spender_address=router_addr,
                from_address=self.wallet_address,
            )
            approve_tx_data = TransactionData(
                to=approve_tx["to"],
                value=approve_tx.get("value", 0),
                data=approve_tx["data"].hex() if isinstance(approve_tx["data"], bytes) else approve_tx["data"],
                gas_estimate=approve_gas,
                description="Approve LB tokens for router",
                tx_type="approve",
            )
            transactions.append(approve_tx_data)

            # Build remove liquidity transaction - pass pre-fetched position to
            # avoid a redundant get_position() call (saves ~50 serial RPC calls)
            lp_tx = tj_adapter.build_remove_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
                position=position,
            )

            if lp_tx is None:
                warnings.append("No LP position found to close")
                # Return success with empty transactions
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            lp_tx_data = TransactionData(
                to=lp_tx.to,
                value=lp_tx.value,
                data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
                gas_estimate=lp_tx.gas or 300000,
                description=(f"Remove liquidity from TraderJoe V2: {token_x_symbol}/{token_y_symbol}"),
                tx_type="traderjoe_v2_remove_liquidity",
            )
            transactions.append(lp_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "position_id": intent.position_id,
                    "collect_fees": intent.collect_fees,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_CLOSE intent: {token_x_symbol}/{token_y_symbol}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_collect_fees(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile an LP_COLLECT_FEES intent into an ActionBundle.

        Routes to protocol-specific handlers for fee collection.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with fee collection ActionBundle
        """
        from .vocabulary import CollectFeesIntent

        if not isinstance(intent, CollectFeesIntent):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Expected CollectFeesIntent",
                intent_id=intent.intent_id,
            )

        protocol = self._resolve_protocol(intent.protocol)
        if protocol == "traderjoe_v2":
            return self._compile_collect_fees_traderjoe_v2(intent)

        if protocol == "uniswap_v4":
            return self._compile_collect_fees_uniswap_v4(intent)

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Protocol '{intent.protocol}' does not support LP_COLLECT_FEES. Supported: traderjoe_v2, uniswap_v4",
            intent_id=intent.intent_id,
        )

    def _compile_collect_fees_traderjoe_v2(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for TraderJoe V2 Liquidity Book.

        Calls LBPair.collectFees(account, binIds) to harvest accumulated fees
        without removing any liquidity from the position.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 fee collection ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # (production path) or a direct RPC URL (local/backtest fallback).
            # Treat a disconnected client as unavailable so we don't hand a
            # dead client to the adapter.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "Connected gateway_client or RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Get position to check if we have liquidity
            position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
            if not position or not position.bin_ids:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build collect fees transaction (no approval needed - calling LBPair directly)
            fee_tx = tj_adapter.build_collect_fees_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
            )

            if fee_tx is None:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            fee_tx_data = TransactionData(
                to=fee_tx.to,
                value=fee_tx.value,
                data=fee_tx.data if isinstance(fee_tx.data, str) else fee_tx.data,
                gas_estimate=fee_tx.gas or 200000,
                description=f"Collect fees from TraderJoe V2: {token_x_symbol}/{token_y_symbol}",
                tx_type="traderjoe_v2_collect_fees",
            )
            transactions.append(fee_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_COLLECT_FEES.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                    "bin_ids": position.bin_ids,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled TraderJoe V2 LP_COLLECT_FEES intent: {token_x_symbol}/{token_y_symbol}, "
                f"{len(position.bin_ids)} bins, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_COLLECT_FEES intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_collect_fees_uniswap_v4(self, intent: "CollectFeesIntent") -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for Uniswap V4 via PositionManager.

        Decreases liquidity by 0 (triggers fee accrual update) then takes the
        accrued fees via TAKE_PAIR.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with V4 fee collection ActionBundle
        """
        from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=self._get_chain_rpc_url(),
            )
            adapter = UniswapV4Adapter(
                config=config, token_resolver=self._token_resolver, gateway_client=self._gateway_client
            )

            # Extract required params
            protocol_params = getattr(intent, "protocol_params", None) or {}
            position_id = protocol_params.get("position_id") or getattr(intent, "position_id", None)
            if not position_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="V4 LP_COLLECT_FEES requires 'position_id' in protocol_params.",
                    intent_id=intent.intent_id,
                )

            currency0 = protocol_params.get("currency0", "")
            currency1 = protocol_params.get("currency1", "")

            # Try resolving from pool string if currencies not provided
            if (not currency0 or not currency1) and intent.pool:
                try:
                    parts = intent.pool.split("/")
                    if len(parts) >= 2:
                        addr0, _ = adapter._resolve_token(parts[0], for_v4_pool=True)
                        addr1, _ = adapter._resolve_token(parts[1], for_v4_pool=True)
                        if int(addr0, 16) > int(addr1, 16):
                            addr0, addr1 = addr1, addr0
                        currency0 = currency0 or addr0
                        currency1 = currency1 or addr1
                except Exception as e:
                    logger.warning("Failed to resolve currencies from pool '%s': %s", intent.pool, e)

            if not currency0 or not currency1:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "V4 LP_COLLECT_FEES requires 'currency0' and 'currency1' in protocol_params "
                        "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                    ),
                    intent_id=intent.intent_id,
                )

            # Enforce canonical V4 ordering: currency0 < currency1
            if int(currency0, 16) > int(currency1, 16):
                currency0, currency1 = currency1, currency0

            hook_data = b""
            hook_data_hex = protocol_params.get("hook_data", "")
            if hook_data_hex:
                hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))

            bundle = adapter.compile_collect_fees_intent(
                position_id=int(position_id),
                currency0=currency0,
                currency1=currency1,
                hook_data=hook_data,
            )

            if not bundle.transactions:
                error_msg = bundle.metadata.get("error", "Unknown error during V4 LP_COLLECT_FEES compilation")
                result.status = CompilationStatus.FAILED
                result.error = error_msg
                return result

            result.action_bundle = bundle
            result.transactions = [
                TransactionData(
                    to=tx["to"],
                    value=int(tx.get("value", 0)),
                    data=tx["data"],
                    gas_estimate=tx.get("gas_estimate", 0),
                    description=tx.get("description", ""),
                    tx_type="lp_collect_fees",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)

            # Forward warnings (e.g. hook warnings)
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_COLLECT_FEES intent: position_id=%s, %d txs",
                position_id,
                len(bundle.transactions),
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_COLLECT_FEES intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_lp_open_aerodrome(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Aerodrome Finance (Solidly fork on Base)."""
        from .compiler_aerodrome import compile_lp_open_aerodrome

        return compile_lp_open_aerodrome(self, intent)

    def _compile_lp_close_aerodrome(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Aerodrome Finance."""
        from .compiler_aerodrome import compile_lp_close_aerodrome

        return compile_lp_close_aerodrome(self, intent)

    def _compile_swap_aerodrome(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Aerodrome/Velodrome (Solidly forks)."""
        from .compiler_aerodrome import compile_swap_aerodrome

        return compile_swap_aerodrome(self, intent)

    def _compile_swap_traderjoe_v2(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for TraderJoe V2 Liquidity Book (VIB-1928).

        TraderJoe V2 uses LBRouter2 with a bin-based AMM interface:
        - swapExactTokensForTokens(amountIn, amountOutMin, Path, to, deadline)
        - Path struct: {pairBinSteps, versions, tokenPath}

        This is incompatible with DefaultSwapAdapter (Uniswap V3 exactInputSingle),
        hence the dedicated compilation path.

        Bin step is auto-detected across common bin steps (20, 25, 15, 10, 50, 5, 100, 1).

        Args:
            intent: SwapIntent with from_token, to_token, and amount

        Returns:
            CompilationResult with TraderJoe V2 swap ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            # Check chain support
            from almanak.core.contracts import TRADERJOE_V2 as TJ_ADDRESSES
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
            from almanak.framework.connectors.traderjoe_v2.sdk import PoolNotFoundError

            if self.chain not in TJ_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"TraderJoe V2 is not supported on {self.chain}. Supported: {list(TJ_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            # Resolve tokens (use the compiler's injected resolver to keep this
            # path consistent with _resolve_token() and test-time overrides)
            resolver = self._token_resolver
            from_token = self._resolve_token(intent.from_token)
            to_token = self._resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown from_token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown to_token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            # Wrap native tokens for pool probing/validation (LB pairs use ERC-20s)
            swap_from_token = (
                resolver.resolve_for_swap(intent.from_token, self.chain) if from_token.is_native else from_token
            )
            swap_to_token = resolver.resolve_for_swap(intent.to_token, self.chain) if to_token.is_native else to_token

            # Calculate input amount
            amount_decimal: Decimal
            if intent.amount_usd is not None:
                price = self._require_token_price(from_token.symbol)
                amount_decimal = intent.amount_usd / price
            elif intent.amount is not None:
                if intent.amount == "all":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                        intent_id=intent.intent_id,
                    )
                amount_decimal = intent.amount  # type: ignore[assignment]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            amount_in_wei = int(amount_decimal * Decimal(10**from_token.decimals))

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # or a direct RPC URL. Normalize a disconnected gateway client to
            # None so we fall back to rpc_url cleanly.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "Connected gateway_client or RPC URL required for TraderJoe V2 swap compilation. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Auto-detect bin_step (swap_params.bin_step override is not yet supported; see VIB-1846)
            requested_bin_step = None

            # Get router address for approvals
            router_address = TJ_ADDRESSES[self.chain]["router"]

            # Create adapter
            slippage_bps = int(intent.max_slippage * Decimal("10000"))
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                default_slippage_bps=slippage_bps,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Auto-detect bin_step if not specified: try common bin steps
            bin_step: int
            if requested_bin_step is not None:
                bin_step = int(requested_bin_step)
            else:
                # Try common bin steps in order of popularity (20 is most common)
                bin_step_order = [20, 25, 15, 10, 50, 5, 100, 1]
                found_bin_step = None
                for bs in bin_step_order:
                    try:
                        tj_adapter.sdk.get_pool_address(swap_from_token.address, swap_to_token.address, bs)
                        found_bin_step = bs
                        break
                    except PoolNotFoundError:
                        continue
                    except Exception as exc:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=f"Failed to probe TraderJoe V2 pool for bin_step={bs}: {exc}",
                            intent_id=intent.intent_id,
                        )

                if found_bin_step is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"No TraderJoe V2 pool found for {from_token.symbol}/{to_token.symbol} on {self.chain}. "
                            f"Tried bin steps: {bin_step_order}. "
                            f"The pair may not have a Liquidity Book pool."
                        ),
                        intent_id=intent.intent_id,
                    )
                bin_step = found_bin_step

            logger.info(
                "Compiling TraderJoe V2 SWAP: %s -> %s, amount=%s, bin_step=%d",
                from_token.symbol,
                to_token.symbol,
                amount_decimal,
                bin_step,
            )

            # Validate pool existence
            from .pool_validation import validate_traderjoe_pool

            pool_check = validate_traderjoe_pool(
                self.chain, swap_from_token.address, swap_to_token.address, bin_step, rpc_url
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Build approve TX for input token
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    amount_in_wei,
                )
                transactions.extend(approve_txs)

            # Build swap TX using adapter
            swap_tx = tj_adapter.build_swap_transaction(
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                bin_step=bin_step,
                slippage_bps=slippage_bps,
            )

            # Convert adapter TransactionData to compiler TransactionData
            swap_tx_data = TransactionData(
                to=swap_tx.to,
                value=swap_tx.value,
                data=swap_tx.data if isinstance(swap_tx.data, str) else f"0x{swap_tx.data.hex()}",
                gas_estimate=swap_tx.gas or DEFAULT_GAS_ESTIMATES.get("traderjoe_v2_swap", 200_000),
                description=(
                    f"TraderJoe V2 swap: {amount_decimal} {from_token.symbol} -> {to_token.symbol} (bin_step={bin_step})"
                ),
                tx_type="traderjoe_v2_swap",
            )
            transactions.append(swap_tx_data)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in_wei),
                    "bin_step": bin_step,
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled TraderJoe V2 SWAP: %s -> %s, bin_step=%d, %d txs, %d gas",
                from_token.symbol,
                to_token.symbol,
                bin_step,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile TraderJoe V2 SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_swap_fluid(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Fluid DEX (Arbitrum only).

        Uses the pool's swapIn() function directly. Automatically discovers
        the Fluid DEX pool for the token pair and determines swap direction.

        VIB-2822: All 20 Fluid DEX T1 pools on Arbitrum currently reject swaps
        at any amount (FluidDexSwapTooSmall / FluidDexLiquidityLimit). The
        connector fails fast here to spare strategy authors from debugging
        protocol-level reverts. Remove this guard once pools are confirmed
        functional again (re-check with a direct eth_call on mainnet).
        """
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=(
                "Fluid DEX connector is disabled (VIB-2822): all 20 Arbitrum T1 pools "
                "currently reject swaps at any amount (FluidDexSwapTooSmall / "
                "FluidDexLiquidityLimit). This is a protocol-level issue, not a "
                "compiler bug. Use uniswap_v3, sushiswap_v3, or camelot instead."
            ),
        )

    def _compile_swap_uniswap_v4(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Uniswap V4.

        Delegates to ``UniswapV4Adapter.compile_swap_intent()`` which handles
        amount resolution, slippage, token metadata (including ``is_native``),
        and ActionBundle construction via the canonical UniversalRouter.

        Args:
            intent: SwapIntent with from_token, to_token, and amount

        Returns:
            CompilationResult with V4 swap ActionBundle
        """
        try:
            from almanak.core.contracts import UNISWAP_V4
            from almanak.framework.connectors.uniswap_v4.adapter import (
                UniswapV4Adapter,
                UniswapV4Config,
            )

            # Check chain support before creating adapter
            if self.chain not in UNISWAP_V4:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Uniswap V4 is not supported on {self.chain}. Supported: {list(UNISWAP_V4.keys())}",
                    intent_id=intent.intent_id,
                )

            slippage_bps = int(intent.max_slippage * 10000)

            config = UniswapV4Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=self._get_chain_rpc_url(),
                default_slippage_bps=slippage_bps,
            )
            adapter = UniswapV4Adapter(
                config=config, token_resolver=self._token_resolver, gateway_client=self._gateway_client
            )

            action_bundle = adapter.compile_swap_intent(intent, price_oracle=self.price_oracle)

            # Empty bundles are invalid for swaps, even if the adapter did not
            # populate metadata["error"].
            if not action_bundle.transactions:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata.get(
                        "error",
                        "Uniswap V4 swap compilation returned no transactions",
                    ),
                    intent_id=intent.intent_id,
                )

            # Add protocol identifier to metadata (adapter sets protocol_version but not protocol)
            action_bundle.metadata["protocol"] = "uniswap_v4"

            total_gas = action_bundle.metadata.get("gas_estimate", 0)

            # Populate result.transactions for callers that read it directly
            # (e.g., permissions/discovery.py, cli/intent_debug.py)
            transactions = []
            for tx_dict in action_bundle.transactions:
                desc = tx_dict.get("description", "")
                if "approve" in desc.lower() and "permit2" not in desc.lower():
                    tx_type = "approve"
                elif "permit2" in desc.lower():
                    tx_type = "permit2_approve"
                else:
                    tx_type = "swap"
                value = tx_dict.get("value", 0)
                if isinstance(value, str):
                    value = int(value, 0) if value.startswith("0x") else int(value)
                transactions.append(
                    TransactionData(
                        to=tx_dict["to"],
                        value=value,
                        data=tx_dict["data"],
                        gas_estimate=tx_dict.get("gas_estimate", 0),
                        description=desc,
                        tx_type=tx_type,
                    )
                )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
            )

        except ValueError as e:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )
        except Exception as e:
            logger.exception("Failed to compile Uniswap V4 SWAP intent: %s", e)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )

    def _compile_swap_curve(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Curve Finance."""
        from .compiler_curve import compile_swap_curve

        return compile_swap_curve(self, intent)

    def _compile_lp_open_curve(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Curve Finance."""
        from .compiler_curve import compile_lp_open_curve

        return compile_lp_open_curve(self, intent)

    def _compile_lp_close_curve(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Curve Finance."""
        from .compiler_curve import compile_lp_close_curve

        return compile_lp_close_curve(self, intent)

    def _compile_pendle_swap(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Pendle Protocol (yield tokenization)."""
        from .compiler_pendle import compile_pendle_swap

        return compile_pendle_swap(self, intent)

    def _compile_pendle_lp_open(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Pendle Protocol (single-token liquidity)."""
        from .compiler_pendle import compile_pendle_lp_open

        return compile_pendle_lp_open(self, intent)

    def _compile_pendle_lp_close(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Pendle Protocol."""
        from .compiler_pendle import compile_pendle_lp_close

        return compile_pendle_lp_close(self, intent)

    def _compile_pendle_redeem(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile WITHDRAW intent as Pendle PT+YT redemption."""
        from .compiler_pendle import compile_pendle_redeem

        return compile_pendle_redeem(self, intent)

    def _compile_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent into an ActionBundle."""
        from .compiler_lending import compile_borrow

        return compile_borrow(self, intent)

    def _compile_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent into an ActionBundle."""
        from .compiler_lending import compile_repay

        return compile_repay(self, intent)

    def _compile_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent into an ActionBundle."""
        from .compiler_lending import compile_supply

        return compile_supply(self, intent)

    def _compile_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent into an ActionBundle."""
        from .compiler_lending import compile_withdraw

        return compile_withdraw(self, intent)

    def _compile_ensure_balance(self, intent: Any) -> CompilationResult:
        """Compile an ENSURE_BALANCE meta-intent by resolving it first.

        EnsureBalanceIntent is a meta-intent that resolves to either a
        HoldIntent or BridgeIntent depending on current balances. If the
        gateway client is available, the target chain balance is fetched
        automatically. Otherwise, the caller must resolve the intent before
        compilation.

        Args:
            intent: EnsureBalanceIntent to compile

        Returns:
            CompilationResult from compiling the resolved intent
        """
        from .ensure_balance import EnsureBalanceIntent

        if not isinstance(intent, EnsureBalanceIntent):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Expected EnsureBalanceIntent",
                intent_id=getattr(intent, "intent_id", ""),
            )

        # Try to resolve using gateway balances if available
        if self._gateway_client is not None:
            try:
                token_info = self._resolve_token(intent.token, intent.target_chain)
                if token_info is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve token '{intent.token}' on {intent.target_chain}",
                        intent_id=intent.intent_id,
                    )

                # Native tokens (ETH, MATIC, etc.) cannot be queried via
                # query_erc20_balance — fail fast until a native balance RPC exists
                if token_info.is_native:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Gateway auto-resolution does not yet support native-token balances. "
                            "Resolve EnsureBalanceIntent manually or use the wrapped token symbol "
                            f"(e.g., WETH instead of ETH) on {intent.target_chain}."
                        ),
                        intent_id=intent.intent_id,
                    )

                target_balance = Decimal("0")
                # Note: chain_balances is empty because the compiler is single-chain
                # scoped and cannot enumerate other configured chains. This means
                # auto-resolution only succeeds when target chain has sufficient balance
                # (producing HoldIntent). Cross-chain bridging requires the caller to
                # resolve the intent manually with multi-chain balance data.
                chain_balances: dict[str, Decimal] = {}

                raw_balance = self._gateway_client.query_erc20_balance(
                    chain=intent.target_chain,
                    token_address=token_info.address,
                    wallet_address=self.wallet_address,
                )
                if raw_balance is None:
                    raise RuntimeError(f"Gateway balance query failed for {intent.token} on {intent.target_chain}")
                target_balance = Decimal(raw_balance) / Decimal(10**token_info.decimals)

                resolved = intent.resolve(target_balance, chain_balances)
                return self.compile(resolved)  # type: ignore[arg-type]
            except Exception as e:  # noqa: BLE001 - best-effort gateway resolution; falls back to manual resolution
                logger.warning("Failed to auto-resolve EnsureBalanceIntent via gateway: %s", e)

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                "EnsureBalanceIntent must be resolved before compilation. "
                "Call intent.resolve(target_balance, chain_balances) to convert "
                "to a HoldIntent or BridgeIntent, then compile the result."
            ),
            intent_id=intent.intent_id,
        )

    def _compile_hold(self, intent: HoldIntent) -> CompilationResult:
        """Compile a HOLD intent (no-op).

        A HOLD intent produces an empty ActionBundle with no transactions.

        Args:
            intent: HoldIntent to compile

        Returns:
            CompilationResult with empty ActionBundle
        """
        action_bundle = ActionBundle(
            intent_type=IntentType.HOLD.value,
            transactions=[],
            metadata={
                "reason": intent.reason,
            },
        )

        return CompilationResult(
            status=CompilationStatus.SUCCESS,
            action_bundle=action_bundle,
            transactions=[],
            total_gas_estimate=0,
            intent_id=intent.intent_id,
        )

    def _compile_perp_open(self, intent: PerpOpenIntent) -> CompilationResult:
        """Compile a PERP_OPEN intent into an ActionBundle.

        Routes to protocol-specific adapter based on intent.protocol:
        - "drift": Drift Protocol on Solana (via DriftAdapter)
        - "gmx_v2": GMX V2 on Arbitrum/Avalanche (via GMXv2Adapter)

        Args:
            intent: PerpOpenIntent to compile

        Returns:
            CompilationResult with perp open ActionBundle
        """
        protocol = self._resolve_protocol(intent.protocol)
        if protocol == "drift":
            return self._compile_drift_perp_open(intent)
        if protocol == "pancakeswap_perps":
            return self._compile_pancakeswap_perps_perp_open(intent)

        # Fail explicitly for unsupported perp protocols on Solana
        if self._is_solana_chain():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for PERP_OPEN on Solana. Supported: drift",
            )

        # Gate the GMX path on the canonical key so a typo / unknown alias fails fast
        # instead of silently compiling a GMX order for the wrong venue.
        if protocol != "gmx_v2":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    f"Protocol '{intent.protocol}' is not supported for PERP_OPEN on "
                    f"{self.chain}. Supported: gmx_v2, pancakeswap_perps (bsc)."
                ),
            )

        from ..connectors import GMXv2Adapter, GMXv2Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Validate chain (GMX v2 only supports arbitrum and avalanche)
            if self.chain not in ["arbitrum", "avalanche"]:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Create GMX adapter
            slippage_bps = int(intent.max_slippage * 10000)
            gmx_config = GMXv2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
            )
            adapter = GMXv2Adapter(gmx_config)

            # Step 3: Calculate acceptable price
            # For longs: max price willing to pay (price * (1 + slippage))
            # For shorts: min price willing to accept (price * (1 - slippage))
            # We'll calculate based on current price estimate from intent size_usd
            acceptable_price = None  # Let adapter use default max/min
            if intent.is_long:
                acceptable_price = Decimal(10**30)  # Max uint for long
            else:
                acceptable_price = Decimal("0")  # Min for short

            # Step 3.5: Validate collateral amount is not chained
            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="collateral_amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Step 4: Build position open order
            order_result = adapter.open_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]  # Validated above
                size_delta_usd=intent.size_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price,
            )

            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create position order",
                    intent_id=intent.intent_id,
                )

            # Step 5: Create transaction data using GMX V2 SDK
            # Use the real SDK to build calldata with proper ABI encoding
            from ..connectors.gmx_v2 import GMX_V2_MARKETS, GMX_V2_TOKENS, GMXV2SDK, GMXV2OrderParams

            # GMX V2 SDK accepts either a connected gateway_client (production
            # path) or an RPC URL (local/backtest fallback). Normalize a
            # disconnected gateway_client to None so we fall back cleanly.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"GMX V2 requires either a connected gateway_client or an RPC URL. "
                        f"Set ALMANAK_{self.chain.upper()}_RPC_URL, RPC_URL, ALCHEMY_API_KEY, "
                        "or use GatewayExecutionOrchestrator."
                    ),
                    intent_id=intent.intent_id,
                )

            # Initialize SDK
            sdk = GMXV2SDK(rpc_url=rpc_url, chain=self.chain, gateway_client=gateway_client)

            # Resolve market address
            market_address = GMX_V2_MARKETS.get(self.chain, {}).get(intent.market)
            if not market_address:
                try:
                    market_address = sdk.get_market_address(intent.market)
                except ValueError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown market: {intent.market}",
                        intent_id=intent.intent_id,
                    )

            # Resolve collateral token address
            collateral_token_upper = intent.collateral_token.upper()
            collateral_address = GMX_V2_TOKENS.get(self.chain, {}).get(collateral_token_upper)
            if not collateral_address:
                if intent.collateral_token.startswith("0x"):
                    collateral_address = intent.collateral_token
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown collateral token: {intent.collateral_token}",
                        intent_id=intent.intent_id,
                    )

            # Calculate collateral in wei
            collateral_decimals = 18 if collateral_token_upper in ["WETH", "ETH"] else 6
            collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]
            collateral_wei = int(collateral_amount_decimal * Decimal(10**collateral_decimals))

            # Calculate size in USD (GMX uses 30 decimals for USD)
            size_delta_usd = int(intent.size_usd * Decimal(10**30))

            # Calculate acceptable price (GMX uses 30 decimals)
            acceptable_price_wei = int(acceptable_price)

            # Get dynamic execution fee
            execution_fee = sdk.get_execution_fee(order_type="increase")

            # Build order parameters
            order_params = GMXV2OrderParams(
                from_address=self.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=collateral_wei,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_wei,
                execution_fee=execution_fee,
            )

            # Build the multicall transaction with real calldata
            tx_data = sdk.build_increase_order_multicall(order_params)

            # Step 5.5: Prepend ERC-20 approval for collateral token.
            # ExchangeRouter.sendTokens() delegates to Router.pluginTransfer(),
            # which calls IERC20.safeTransferFrom() — so the Router is the msg.sender
            # that needs the allowance, NOT the ExchangeRouter.
            # Native tokens (WETH/ETH) are sent as msg.value via sendWnt(), no approval needed.
            is_native_collateral = collateral_token_upper in ("WETH", "ETH", "WAVAX", "AVAX")
            if not is_native_collateral and collateral_wei > 0:
                approve_txs = self._build_approve_tx(
                    token_address=collateral_address,
                    spender=sdk.ROUTER_ADDRESS,
                    amount=collateral_wei,
                )
                transactions.extend(approve_txs)

            open_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=(
                    f"Open {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: ${intent.size_usd} size, {intent.collateral_amount} collateral"
                ),
                tx_type="perp_open",
            )
            transactions.append(open_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "leverage": str(intent.leverage),
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled PERP_OPEN intent: {'LONG' if intent.is_long else 'SHORT'} {intent.market}, ${intent.size_usd} size, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PERP_OPEN intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_perp_close(self, intent: PerpCloseIntent) -> CompilationResult:
        """Compile a PERP_CLOSE intent into an ActionBundle.

        Routes to protocol-specific adapter based on intent.protocol:
        - "drift": Drift Protocol on Solana (via DriftAdapter)
        - "gmx_v2": GMX V2 on Arbitrum/Avalanche (via GMXv2Adapter)

        Args:
            intent: PerpCloseIntent to compile

        Returns:
            CompilationResult with perp close ActionBundle
        """
        protocol = self._resolve_protocol(intent.protocol)
        if protocol == "drift":
            return self._compile_drift_perp_close(intent)
        if protocol == "pancakeswap_perps":
            return self._compile_pancakeswap_perps_perp_close(intent)

        # Fail explicitly for unsupported perp protocols on Solana
        if self._is_solana_chain():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Protocol '{intent.protocol}' is not supported for PERP_CLOSE on Solana. Supported: drift",
            )

        # Gate the GMX path on the canonical key so a typo / unknown alias fails fast
        # instead of silently compiling a GMX close for the wrong venue.
        if protocol != "gmx_v2":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    f"Protocol '{intent.protocol}' is not supported for PERP_CLOSE on "
                    f"{self.chain}. Supported: gmx_v2, pancakeswap_perps (bsc)."
                ),
            )

        from ..connectors import GMXv2Adapter, GMXv2Config

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Validate chain
            if self.chain not in ["arbitrum", "avalanche"]:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {self.chain}",
                    intent_id=intent.intent_id,
                )

            # Step 2: Create GMX adapter
            slippage_bps = int(intent.max_slippage * 10000)
            gmx_config = GMXv2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                default_slippage_bps=slippage_bps,
            )
            adapter = GMXv2Adapter(gmx_config)

            # Step 3: Calculate acceptable price for closing
            # For closing longs: min price to sell at (price * (1 - slippage))
            # For closing shorts: max price to buy back at (price * (1 + slippage))
            acceptable_price = None
            if intent.is_long:
                acceptable_price = Decimal("0")  # Min price for closing long
            else:
                acceptable_price = Decimal(10**30)  # Max price for closing short

            # Step 4: Initialize SDK and resolve addresses (needed before adapter call
            # so we can query on-chain position size for full closes — VIB-1946)
            from ..connectors.gmx_v2 import GMX_V2_MARKETS, GMX_V2_TOKENS, GMXV2SDK, GMXV2OrderParams

            # GMX V2 SDK accepts either a connected gateway_client (production
            # path) or an RPC URL (local/backtest fallback). Normalize a
            # disconnected gateway_client to None so we fall back cleanly.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"GMX V2 requires either a connected gateway_client or an RPC URL. "
                        f"Set ALMANAK_{self.chain.upper()}_RPC_URL, RPC_URL, ALCHEMY_API_KEY, "
                        "or use GatewayExecutionOrchestrator."
                    ),
                    intent_id=intent.intent_id,
                )

            # Initialize SDK
            sdk = GMXV2SDK(rpc_url=rpc_url, chain=self.chain, gateway_client=gateway_client)

            # Resolve market address
            market_address = GMX_V2_MARKETS.get(self.chain, {}).get(intent.market)
            if not market_address:
                try:
                    market_address = sdk.get_market_address(intent.market)
                except ValueError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown market: {intent.market}",
                        intent_id=intent.intent_id,
                    )

            # Resolve collateral token address
            collateral_token_upper = intent.collateral_token.upper()
            collateral_address = GMX_V2_TOKENS.get(self.chain, {}).get(collateral_token_upper)
            if not collateral_address:
                if intent.collateral_token.startswith("0x"):
                    collateral_address = intent.collateral_token
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Unknown collateral token: {intent.collateral_token}",
                        intent_id=intent.intent_id,
                    )

            # Step 5: Resolve position size in USD (GMX uses 30 decimals)
            # GMX V2 validates sizeDeltaUsd <= position.sizeInUsd — max uint and any
            # overshoot burns keeper fees without closing (VIB-1946).
            resolved_size_usd = intent.size_usd
            if intent.size_usd:
                size_delta_usd = int(intent.size_usd * Decimal(10**30))
            else:
                queried_size = self._get_gmx_position_size_onchain(
                    sdk, market_address, collateral_address, intent.is_long
                )
                if queried_size is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Cannot close full GMX V2 position: unable to read position size on-chain. "
                            "Either specify size_usd explicitly or ensure RPC/API connectivity. "
                            "Refusing to guess — incorrect sizes burn keeper execution fees."
                        ),
                        intent_id=intent.intent_id,
                    )
                size_delta_usd = queried_size
                # Convert on-chain size (30-decimal int) to Decimal for adapter
                resolved_size_usd = Decimal(size_delta_usd) / Decimal(10**30)

            # Step 6: Build position close order via adapter (with resolved size)
            order_result = adapter.close_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                is_long=intent.is_long,
                size_delta_usd=resolved_size_usd,
                acceptable_price=acceptable_price,
            )

            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create close order",
                    intent_id=intent.intent_id,
                )

            # Calculate acceptable price (GMX uses 30 decimals)
            acceptable_price_wei = int(acceptable_price)

            # Get dynamic execution fee
            execution_fee = sdk.get_execution_fee(order_type="decrease")

            # Build order parameters
            order_params = GMXV2OrderParams(
                from_address=self.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=0,  # No additional collateral for decrease
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_wei,
                execution_fee=execution_fee,
            )

            # Build the decrease order transaction with real calldata
            tx_data = sdk.build_decrease_order_multicall(order_params)

            size_desc = f"${intent.size_usd}" if intent.size_usd else "full position"
            close_tx = TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=(f"Close {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: {size_desc}"),
                tx_type="perp_close",
            )
            transactions.append(close_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.PERP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "is_long": intent.is_long,
                    "size_usd": str(intent.size_usd) if intent.size_usd else None,
                    "close_full_position": intent.close_full_position,
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled PERP_CLOSE intent: {'LONG' if intent.is_long else 'SHORT'} {intent.market}, {size_desc}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PERP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _get_gmx_position_size_onchain(
        self,
        sdk: Any,
        market_address: str,
        collateral_address: str,
        is_long: bool,
    ) -> int | None:
        """Read exact GMX V2 position size from on-chain for close-full-position.

        GMX V2 validates sizeDeltaUsd <= position.sizeInUsd strictly.
        Any overshoot burns keeper fees without closing the position (VIB-1946).

        Args:
            sdk: GMXV2SDK instance (already initialized with RPC)
            market_address: Market contract address
            collateral_address: Collateral token address
            is_long: Position direction

        Returns:
            size_in_usd in 30-decimal int format, or None if query failed.
        """
        from ..connectors.gmx_v2.sdk import PositionQueryError

        try:
            positions = sdk.get_account_positions(self.wallet_address)
        except PositionQueryError as e:
            logger.warning("GMX V2 position query failed: %s", e)
            return None
        except Exception as e:
            logger.warning("Unexpected error querying GMX V2 positions: %s", e)
            return None

        if not positions:
            logger.warning("No GMX V2 positions found for %s", self.wallet_address)
            return None

        # Match position by (market, collateral_token, is_long)
        market_lower = market_address.lower()
        collateral_lower = collateral_address.lower()
        for pos in positions:
            if (
                pos.get("market", "").lower() == market_lower
                and pos.get("collateral_token", "").lower() == collateral_lower
                and pos.get("is_long") == is_long
                and pos.get("size_in_usd", 0) > 0
            ):
                size_in_usd = pos["size_in_usd"]  # Already in 30 decimals from chain
                logger.info(
                    "Read on-chain GMX V2 position size: %s (30-decimal) for market=%s is_long=%s",
                    size_in_usd,
                    market_address,
                    is_long,
                )
                return int(size_in_usd)

        logger.warning(
            "No matching GMX V2 position found for market=%s collateral=%s is_long=%s",
            market_address,
            collateral_address,
            is_long,
        )
        return None

    # ==========================================================================
    # DRIFT PERPS (Solana)
    # ==========================================================================

    def _compile_drift_perp_open(self, intent: PerpOpenIntent) -> CompilationResult:
        """Compile a PERP_OPEN intent using Drift for Solana chains."""
        from .compiler_solana import compile_drift_perp_open

        return compile_drift_perp_open(self, intent)

    def _compile_drift_perp_close(self, intent: PerpCloseIntent) -> CompilationResult:
        """Compile a PERP_CLOSE intent using Drift for Solana chains."""
        from .compiler_solana import compile_drift_perp_close

        return compile_drift_perp_close(self, intent)

    # ==========================================================================
    # PANCAKESWAP PERPS (ApolloX Diamond on BSC, broker id = 2)
    # ==========================================================================

    def _compile_pancakeswap_perps_perp_open(self, intent: PerpOpenIntent) -> CompilationResult:
        """Compile a PERP_OPEN intent via PancakeSwap Perps (ApolloX on BSC).

        v1 limitations (see docs/internal/discussions/pancakeswap-perps-integration-20260415.md):
          - chain must be 'bsc'
          - market must be in PANCAKESWAP_PERPS_MARKETS['bsc'] (BTC/USD, ETH/USD, BNB/USD)
          - native BNB margin (collateral_token='BNB') goes via openMarketTradeBNB (value-carrying)
          - ERC20 margin (USDT/USDC) goes via openMarketTrade (compiler prepends approve)
          - no SL/TP, no limit orders
        """
        from ..connectors.pancakeswap_perps import (
            PancakeSwapPerpsAdapter,
            PancakeSwapPerpsConfig,
        )

        if self.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"PancakeSwap Perps v1 requires chain='bsc', got '{self.chain}'",
            )

        if intent.collateral_amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "collateral_amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                ),
            )

        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            adapter = PancakeSwapPerpsAdapter(
                PancakeSwapPerpsConfig(chain=self.chain, wallet_address=self.wallet_address)
            )

            # Resolve mark price for the base asset (e.g. 'BTC/USD' -> 'BTC').
            # ApolloX trades crypto perps as synthetics; the *trading* market name
            # uses canonical symbols ('BTC', 'ETH'), but on-chain price oracles
            # for BSC are keyed on the wrapped/bridged token ('WBTC' for BTCB,
            # 'WETH' for ETH-bsc, etc.). We try the bare symbol first and fall
            # back to the wrapped symbol so strategies can pass either.
            base_symbol = intent.market.split("/")[0] if "/" in intent.market else intent.market
            _BSC_PERP_PRICE_ALIAS = {"BTC": "WBTC", "ETH": "WETH", "BNB": "WBNB"}
            try:
                mark_price = self._require_token_price(base_symbol)
            except ValueError:
                wrapped = _BSC_PERP_PRICE_ALIAS.get(base_symbol.upper())
                if not wrapped:
                    raise
                mark_price = self._require_token_price(wrapped)

            # Resolve collateral decimals and normalize the collateral input for the adapter.
            # Accept either symbol (case-insensitive) or a 0x-prefixed address — per the
            # PerpOpenIntent docstring both forms are supported.
            from almanak.core.contracts import PANCAKESWAP_PERPS_TOKENS

            from ..connectors.pancakeswap_perps.sdk import NATIVE_BNB_ADDRESS

            raw_collateral = intent.collateral_token
            # The native sentinel (address(0) via NATIVE_BNB_ADDRESS) is only honoured by
            # PancakeSwapPerpsAdapter.build_open() when spelled as a symbol ("BNB"/"NATIVE").
            # Normalize the sentinel address *to* the "BNB" symbol so address-form callers
            # still route through openMarketTradeBNB instead of falling into the ERC-20 branch.
            if (
                isinstance(raw_collateral, str)
                and raw_collateral.startswith("0x")
                and raw_collateral.lower() == NATIVE_BNB_ADDRESS.lower()
            ):
                normalized_collateral = "BNB"
                resolver_key = "BNB"
            elif isinstance(raw_collateral, str) and raw_collateral.startswith("0x"):
                normalized_collateral = raw_collateral
                resolver_key = raw_collateral  # preserve case for address lookups
            else:
                normalized_collateral = raw_collateral
                resolver_key = raw_collateral.upper()

            # Venue allowlist: PCS Perps only accepts BNB (native), WBNB, USDT, USDC as
            # margin. Reject anything else at compile time so we never approve an unrelated
            # ERC-20 to the router or submit an openMarketTrade that will revert on-chain.
            # The native sentinel was already normalized to "BNB" above, so the address
            # allowlist intentionally only contains real ERC-20 margin tokens.
            supported_tokens = PANCAKESWAP_PERPS_TOKENS.get(self.chain, {})
            allowed_symbols = {"BNB", "NATIVE"} | set(supported_tokens.keys())
            allowed_addresses = {addr.lower() for addr in supported_tokens.values()}
            if resolver_key.startswith("0x"):
                if resolver_key.lower() not in allowed_addresses:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=(
                            f"Collateral address '{intent.collateral_token}' is not a supported "
                            f"PancakeSwap Perps margin token on {self.chain}. "
                            f"Allowed: BNB (native) + {sorted(supported_tokens.keys())}."
                        ),
                    )
            elif resolver_key not in allowed_symbols:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=(
                        f"Collateral symbol '{intent.collateral_token}' is not a supported "
                        f"PancakeSwap Perps margin token on {self.chain}. "
                        f"Allowed: BNB (native) + {sorted(supported_tokens.keys())}."
                    ),
                )

            if resolver_key in ("BNB", "NATIVE", "WBNB"):
                collateral_decimals = 18
            else:
                try:
                    collateral_decimals = self._token_resolver.get_decimals(self.chain, resolver_key)
                except Exception as e:  # noqa: BLE001 — TokenNotFoundError or similar
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=(
                            f"Could not resolve decimals for collateral token "
                            f"'{intent.collateral_token}' on {self.chain}: {e}"
                        ),
                    )

            order = adapter.build_open(
                market=intent.market,
                collateral_token=normalized_collateral,
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]  # validated above
                collateral_decimals=collateral_decimals,
                size_usd=intent.size_usd,
                mark_price=mark_price,
                is_long=intent.is_long,
                max_slippage=intent.max_slippage,
            )
            if not order.success or order.tx is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=order.error or "Adapter failed to build open transaction",
                )

            # Prepend ERC20 approve when the margin is non-native.
            if not order.native and order.margin_token_address:
                approve_txs = self._build_approve_tx(
                    token_address=order.margin_token_address,
                    spender=order.tx.to,
                    amount=order.amount_in_wei,
                )
                transactions.extend(approve_txs)

            open_tx = TransactionData(
                to=order.tx.to,
                value=order.tx.value,
                data="0x" + order.tx.data.hex(),
                gas_estimate=order.tx.gas_estimate,
                description=order.tx.description,
                tx_type="perp_open",
            )
            transactions.append(open_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "pair_base": order.pair_base,
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "max_slippage": str(intent.max_slippage),
                    "qty_1e10": order.qty,  # qty is 10-decimal fixed-point on ApolloX
                    "limit_price_1e8": order.limit_price,
                    "native_margin": order.native,
                    "chain": self.chain,
                    "broker_id": adapter.config.broker_id,
                },
            )
            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings
            logger.info(
                f"Compiled PancakeSwap Perps PERP_OPEN: {'LONG' if intent.is_long else 'SHORT'} "
                f"{intent.market} size=${intent.size_usd} margin={intent.collateral_amount} "
                f"{intent.collateral_token} ({len(transactions)} txs, {total_gas} gas)"
            )
        except Exception as e:
            logger.exception(f"Failed to compile PancakeSwap Perps PERP_OPEN: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_pancakeswap_perps_perp_close(self, intent: PerpCloseIntent) -> CompilationResult:
        """Compile a PERP_CLOSE intent via PancakeSwap Perps (ApolloX on BSC).

        Closes the position identified by ``intent.position_id`` (a 0x-prefixed
        bytes32 ``tradeHash``). Strategies obtain the ``tradeHash`` from the
        open receipt's ``MarketPendingTrade`` event (surfaced as
        ``result.position_id`` / ``result.extracted_data['position_id']`` by
        the receipt parser + ``ResultEnricher``) and persist it across ticks.

        v1 limitations:
          - chain must be 'bsc'
          - ``closeTrade(bytes32)`` always flattens the full position; partial closes
            are NOT supported. If ``intent.size_usd`` is set, compilation fails fast
            (``CompilationStatus.FAILED``) instead of silently flattening the full
            position — callers must omit ``size_usd`` to opt into the full-close semantics.

        See ``almanak/framework/intents/perp_intents.py::PerpCloseIntent.position_id``
        for the cross-venue rationale.
        """
        from ..connectors.pancakeswap_perps import (
            PancakeSwapPerpsAdapter,
            PancakeSwapPerpsConfig,
        )

        if self.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"PancakeSwap Perps v1 requires chain='bsc', got '{self.chain}'",
            )

        position_id = intent.position_id
        if not position_id:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "PancakeSwap Perps PERP_CLOSE requires intent.position_id (the bytes32 "
                    "tradeHash returned from the open). Strategies must persist the tradeHash "
                    "from on_intent_executed(result.position_id) after the open."
                ),
            )

        # Strict bytes32 validation for the PCS path: 0x + 64 hex chars = 66 chars total,
        # all characters must be valid hex. Generic vocabulary validation accepts any
        # shape; the venue compiler enforces length + hex-ness so malformed hashes fail
        # at compile time instead of surfacing as an opaque adapter/tx error later.
        pid_clean = position_id.lower()
        if not pid_clean.startswith("0x") or len(pid_clean) != 66:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    f"PancakeSwap Perps requires a 0x-prefixed bytes32 tradeHash "
                    f"(66 chars total). Got: '{position_id}' (len={len(position_id)})."
                ),
            )
        try:
            int(pid_clean[2:], 16)
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    f"PancakeSwap Perps requires position_id to be a valid hex bytes32 tradeHash. Got: '{position_id}'."
                ),
            )

        # Partial closes (size_usd) are NOT representable on ApolloX's closeTrade(bytes32)
        # selector — it always closes 100% of the position. Reject fast so callers asking
        # for a partial close never silently execute a full close.
        if intent.size_usd is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "PancakeSwap Perps does not support partial PERP_CLOSE via size_usd. "
                    "Omit size_usd to close the full position identified by position_id."
                ),
            )
        warnings: list[str] = []

        try:
            adapter = PancakeSwapPerpsAdapter(
                PancakeSwapPerpsConfig(chain=self.chain, wallet_address=self.wallet_address)
            )
            close_tx = adapter.build_close(trade_hash=position_id)
        except Exception as e:
            logger.exception(f"Failed to build PancakeSwap Perps close transaction: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

        tx = TransactionData(
            to=close_tx.to,
            value=close_tx.value,
            data="0x" + close_tx.data.hex(),
            gas_estimate=close_tx.gas_estimate,
            description=close_tx.description,
            tx_type="perp_close",
        )
        action_bundle = ActionBundle(
            intent_type=IntentType.PERP_CLOSE.value,
            transactions=[tx.to_dict()],
            metadata={
                "protocol": intent.protocol,
                "market": intent.market,
                "collateral_token": intent.collateral_token,
                "is_long": intent.is_long,
                "max_slippage": str(intent.max_slippage),
                "position_id": position_id,
                "chain": self.chain,
            },
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.action_bundle = action_bundle
        result.transactions = [tx]
        result.total_gas_estimate = tx.gas_estimate
        result.warnings = warnings
        logger.info(
            f"Compiled PancakeSwap Perps PERP_CLOSE: tradeHash={position_id[:18]}... "
            f"market={intent.market} (1 tx, {tx.gas_estimate} gas)"
        )
        return result

    def _compile_flash_loan(self, intent: FlashLoanIntent) -> CompilationResult:
        """Compile a FLASH_LOAN intent into an ActionBundle."""
        from .compiler_flash_loan import compile_flash_loan

        return compile_flash_loan(self, intent)

    def _estimate_callback_output(
        self,
        callback_intent: AnyIntent,
        prev_output_amount: Decimal | None,
        prev_output_token: str | None,
    ) -> tuple[Decimal | None, str | None]:
        """Estimate the output token and amount from a compiled callback intent."""
        from .compiler_flash_loan import estimate_callback_output

        return estimate_callback_output(self, callback_intent, prev_output_amount, prev_output_token)

    def _build_aave_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build an Aave V3 flash loan transaction."""
        from .compiler_flash_loan import build_aave_flash_loan

        return build_aave_flash_loan(
            self,
            token_info=token_info,
            amount_wei=amount_wei,
            callback_params=callback_params,
            callback_gas_total=callback_gas_total,
        )

    def _build_balancer_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build a Balancer Vault flash loan transaction."""
        from .compiler_flash_loan import build_balancer_flash_loan

        return build_balancer_flash_loan(
            self,
            token_info=token_info,
            amount_wei=amount_wei,
            callback_params=callback_params,
            callback_gas_total=callback_gas_total,
        )

    def _build_morpho_flash_loan(
        self,
        token_info: "TokenInfo",
        amount_wei: int,
        callback_params: bytes,
        callback_gas_total: int,
    ) -> dict:
        """Build a Morpho Blue flash loan transaction."""
        from .compiler_flash_loan import build_morpho_flash_loan

        return build_morpho_flash_loan(
            self,
            token_info=token_info,
            amount_wei=amount_wei,
            callback_params=callback_params,
            callback_gas_total=callback_gas_total,
        )

    def _encode_flash_loan_callbacks(
        self,
        callback_transactions: list[TransactionData],
    ) -> bytes:
        """Encode callback transactions for flash loan params."""
        from .compiler_flash_loan import encode_flash_loan_callbacks

        return encode_flash_loan_callbacks(callback_transactions)

    def _compile_stake_intent(self, intent: StakeIntent) -> CompilationResult:
        """Compile a STAKE intent into an ActionBundle.

        Routes to the appropriate staking adapter based on protocol:
        - 'lido': Uses LidoAdapter for ETH staking (stETH/wstETH)
        - 'ethena': Uses EthenaAdapter for USDe staking (sUSDe)
        - 'gimo': Uses GimoAdapter for A0GI staking (st0G) on 0G Chain

        Args:
            intent: StakeIntent to compile

        Returns:
            CompilationResult with stake ActionBundle
        """
        from ..connectors import EthenaAdapter, EthenaConfig, LidoAdapter, LidoConfig
        from ..connectors.gimo import GimoAdapter, GimoConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        warnings: list[str] = []

        try:
            protocol = intent.protocol.lower()

            # Validate chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Route to appropriate adapter based on protocol
            action_bundle: ActionBundle
            if protocol == "lido":
                # Validate chain - Lido only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Lido staking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                lido_config = LidoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                lido_adapter = LidoAdapter(lido_config)
                action_bundle = lido_adapter.compile_stake_intent(intent)
                if action_bundle.metadata.get("error"):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=action_bundle.metadata["error"],
                        intent_id=intent.intent_id,
                    )

            elif protocol == "ethena":
                # Validate chain - Ethena only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Ethena staking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                ethena_config = EthenaConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                ethena_adapter = EthenaAdapter(ethena_config)
                action_bundle = ethena_adapter.compile_stake_intent(intent)

            elif protocol == "gimo":
                # Validate chain - Gimo only on 0G Chain
                if self.chain != "zerog":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Gimo staking only supported on 0G Chain (zerog), got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                gimo_config = GimoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                gimo_adapter = GimoAdapter(gimo_config)
                action_bundle = gimo_adapter.compile_stake_intent(intent)
                if action_bundle.metadata.get("error"):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=action_bundle.metadata["error"],
                        intent_id=intent.intent_id,
                    )

            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported staking protocol: {protocol}. Supported: lido, ethena, gimo",
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", "0x"),
                    gas_estimate=tx_dict.get("gas_estimate", 0),
                    description=tx_dict.get("description", ""),
                    tx_type=tx_dict.get("tx_type", "stake"),
                )
                transactions.append(tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled STAKE intent: {intent.amount} {intent.token_in} via {protocol}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile STAKE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_unstake_intent(self, intent: UnstakeIntent) -> CompilationResult:
        """Compile an UNSTAKE intent into an ActionBundle.

        Routes to the appropriate staking adapter based on protocol:
        - 'lido': Uses LidoAdapter for stETH/wstETH unstaking
        - 'ethena': Uses EthenaAdapter for sUSDe unstaking (initiates cooldown)
        - 'gimo': Uses GimoAdapter for st0G unstaking on 0G Chain

        Args:
            intent: UnstakeIntent to compile

        Returns:
            CompilationResult with unstake ActionBundle
        """
        from ..connectors import EthenaAdapter, EthenaConfig, LidoAdapter, LidoConfig
        from ..connectors.gimo import GimoAdapter, GimoConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        warnings: list[str] = []

        try:
            protocol = intent.protocol.lower()

            # Validate chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )

            # Route to appropriate adapter based on protocol
            action_bundle: ActionBundle
            if protocol == "lido":
                # Validate chain - Lido only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Lido unstaking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                lido_config = LidoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                lido_adapter = LidoAdapter(lido_config)
                action_bundle = lido_adapter.compile_unstake_intent(intent)

            elif protocol == "ethena":
                # Validate chain - Ethena only on Ethereum mainnet
                if self.chain not in ["ethereum", "mainnet"]:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Ethena unstaking only supported on Ethereum mainnet, got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                ethena_config = EthenaConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                ethena_adapter = EthenaAdapter(ethena_config)
                action_bundle = ethena_adapter.compile_unstake_intent(intent)

            elif protocol == "gimo":
                # Validate chain - Gimo only on 0G Chain
                if self.chain != "zerog":
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Gimo unstaking only supported on 0G Chain (zerog), got: {self.chain}",
                        intent_id=intent.intent_id,
                    )

                gimo_config = GimoConfig(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
                gimo_adapter = GimoAdapter(gimo_config)
                action_bundle = gimo_adapter.compile_unstake_intent(intent)
                if action_bundle.metadata.get("error"):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=action_bundle.metadata["error"],
                        intent_id=intent.intent_id,
                    )

            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported unstaking protocol: {protocol}. Supported: lido, ethena, gimo",
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", "0x"),
                    gas_estimate=tx_dict.get("gas_estimate", 0),
                    description=tx_dict.get("description", ""),
                    tx_type=tx_dict.get("tx_type", "unstake"),
                )
                transactions.append(tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled UNSTAKE intent: {intent.amount} {intent.token_in} via {protocol}, {len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile UNSTAKE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # =========================================================================
    # Prediction Market Intent Compilation
    # =========================================================================

    def _compile_prediction_buy(self, intent: PredictionBuyIntent) -> CompilationResult:
        """Compile a PREDICTION_BUY intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        The resulting ActionBundle contains CLOB order data in metadata,
        not on-chain transactions (buy orders are submitted off-chain).

        Args:
            intent: PredictionBuyIntent to compile

        Returns:
            CompilationResult with prediction buy ActionBundle
        """
        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionBuyIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # CLOB orders have no on-chain transactions (gas = 0)
            result.action_bundle = action_bundle
            result.transactions = []
            result.total_gas_estimate = 0

            logger.info(
                f"Compiled PREDICTION_BUY: market={intent.market_id}, "
                f"outcome={intent.outcome}, "
                f"amount_usd={intent.amount_usd}, shares={intent.shares}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_BUY intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_prediction_sell(self, intent: PredictionSellIntent) -> CompilationResult:
        """Compile a PREDICTION_SELL intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        The resulting ActionBundle contains CLOB order data in metadata,
        not on-chain transactions (sell orders are submitted off-chain).

        Args:
            intent: PredictionSellIntent to compile

        Returns:
            CompilationResult with prediction sell ActionBundle
        """
        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionSellIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # CLOB orders have no on-chain transactions (gas = 0)
            result.action_bundle = action_bundle
            result.transactions = []
            result.total_gas_estimate = 0

            logger.info(
                f"Compiled PREDICTION_SELL: market={intent.market_id}, outcome={intent.outcome}, shares={intent.shares}"
            )

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_SELL intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_prediction_redeem(self, intent: PredictionRedeemIntent) -> CompilationResult:
        """Compile a PREDICTION_REDEEM intent into an ActionBundle.

        This method delegates to the PolymarketAdapter for compilation.
        Unlike buy/sell, redemption is an on-chain CTF transaction that
        converts winning outcome tokens into USDC.

        Args:
            intent: PredictionRedeemIntent to compile

        Returns:
            CompilationResult with prediction redeem ActionBundle
        """
        from ..connectors.polymarket.exceptions import PolymarketMarketNotResolvedError

        # Check if adapter is available
        if self._polymarket_adapter is None:
            if self.chain.lower() != "polygon":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Prediction market intents are only supported on Polygon, not {self.chain}",
                    intent_id=intent.intent_id,
                )
            # VIB-307: Warn at compile time (not at init) so non-prediction Polygon strategies
            # don't see this warning unless they actually attempt a prediction intent.
            logger.warning(
                "PredictionRedeemIntent requires polymarket_config in IntentCompilerConfig. "
                "Provide polymarket_config to enable prediction market intents on Polygon."
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Provide polymarket_config in IntentCompilerConfig to enable prediction intents."
                ),
                intent_id=intent.intent_id,
            )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            # Delegate to PolymarketAdapter
            action_bundle = self._polymarket_adapter.compile_intent(intent)

            # Check if compilation failed (error in metadata)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            # Convert ActionBundle transactions to TransactionData objects
            transactions: list[TransactionData] = []
            for tx_dict in action_bundle.transactions:
                tx = TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", ""),
                    gas_estimate=tx_dict.get("gas_estimate", 200_000),
                    description=tx_dict.get("description", "Redeem prediction market positions"),
                    tx_type=tx_dict.get("tx_type", "redeem"),
                )
                transactions.append(tx)

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Compiled PREDICTION_REDEEM: market={intent.market_id}, "
                f"outcome={intent.outcome}, txs={len(transactions)}"
            )

        except PolymarketMarketNotResolvedError as e:
            # Re-raise with clear message for unresolved markets
            logger.warning(f"Cannot redeem - market not resolved: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        except Exception as e:
            logger.exception(f"Failed to compile PREDICTION_REDEEM intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # =================================================================
    # MetaMorpho Vault Operations
    # =================================================================

    def _compile_vault_deposit(self, intent: VaultDepositIntent) -> CompilationResult:
        """Compile a VAULT_DEPOSIT intent into an ActionBundle.

        This method:
        1. Creates MetaMorpho adapter with gateway client
        2. Queries vault asset address
        3. Resolves asset token for decimals
        4. Builds approve TX for the vault
        5. Builds deposit TX

        Args:
            intent: VaultDepositIntent to compile

        Returns:
            CompilationResult with vault deposit ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            # Check for chained amount
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            if amount_decimal <= Decimal("0"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Vault deposit amount must be positive",
                    intent_id=intent.intent_id,
                )

            if self._gateway_client is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="GatewayClient is required for MetaMorpho vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

            # Create adapter with gateway client
            vault_config = MetaMorphoConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = MetaMorphoAdapter(
                vault_config,
                gateway_client=self._gateway_client,
                token_resolver=self._token_resolver,
            )

            # Query vault asset address
            asset_address = adapter.sdk.get_vault_asset(intent.vault_address)

            # Resolve asset token for decimals
            asset_token = self._resolve_token(asset_address)
            if asset_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve vault asset token: {asset_address}",
                    intent_id=intent.intent_id,
                )

            amount_wei = int(amount_decimal * Decimal(10**asset_token.decimals))

            # Build approve TX
            approve_txs = self._build_approve_tx(
                asset_token.address,
                intent.vault_address,
                amount_wei,
            )
            transactions.extend(approve_txs)

            # Build deposit TX via SDK
            deposit_tx_data = adapter.sdk.build_deposit_tx(
                vault_address=intent.vault_address,
                assets=amount_wei,
                receiver=self.wallet_address,
            )

            deposit_tx = TransactionData(
                to=deposit_tx_data["to"],
                value=deposit_tx_data["value"],
                data=deposit_tx_data["data"],
                gas_estimate=deposit_tx_data["gas_estimate"],
                description=f"Deposit {amount_decimal} {asset_token.symbol} into MetaMorpho vault {intent.vault_address[:10]}...",
                tx_type="vault_deposit",
            )
            transactions.append(deposit_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_DEPOSIT.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "asset_address": asset_token.address,
                    "asset_symbol": asset_token.symbol,
                    "deposit_amount": str(amount_decimal),
                    "deposit_amount_wei": str(amount_wei),
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled VAULT_DEPOSIT: {amount_decimal} {asset_token.symbol} into vault {intent.vault_address[:10]}..."
            )
            return result

        except Exception as e:
            logger.exception(f"Failed to compile VAULT_DEPOSIT intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
            return result

    def _compile_vault_redeem(self, intent: VaultRedeemIntent) -> CompilationResult:
        """Compile a VAULT_REDEEM intent into an ActionBundle.

        This method:
        1. Creates MetaMorpho adapter with gateway client
        2. If shares="all", queries maxRedeem to get share count
        3. Builds redeem TX (no approve needed)

        Args:
            intent: VaultRedeemIntent to compile

        Returns:
            CompilationResult with vault redeem ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self._gateway_client is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="GatewayClient is required for MetaMorpho vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_vault.adapter import MetaMorphoAdapter, MetaMorphoConfig

            # Create adapter with gateway client
            vault_config = MetaMorphoConfig(
                chain=self.chain,
                wallet_address=self.wallet_address,
            )
            adapter = MetaMorphoAdapter(
                vault_config,
                gateway_client=self._gateway_client,
                token_resolver=self._token_resolver,
            )

            # Resolve shares amount
            if intent.shares == "all":
                # Query max redeemable shares
                shares_wei = adapter.sdk.get_max_redeem(intent.vault_address, self.wallet_address)
                if shares_wei <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="No shares to redeem",
                        intent_id=intent.intent_id,
                    )
            else:
                shares_decimal: Decimal = intent.shares  # type: ignore[assignment]
                # Resolve share decimals dynamically (vault address IS the share token for ERC-4626)
                share_decimals = adapter.sdk.get_decimals(intent.vault_address)
                shares_wei = int(shares_decimal * Decimal(10**share_decimals))

            if shares_wei <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Redeem shares must be positive",
                    intent_id=intent.intent_id,
                )

            # Build redeem TX via SDK (no approve needed - redeeming own shares)
            redeem_tx_data = adapter.sdk.build_redeem_tx(
                vault_address=intent.vault_address,
                shares=shares_wei,
                receiver=self.wallet_address,
                owner=self.wallet_address,
            )

            redeem_tx = TransactionData(
                to=redeem_tx_data["to"],
                value=redeem_tx_data["value"],
                data=redeem_tx_data["data"],
                gas_estimate=redeem_tx_data["gas_estimate"],
                description=f"Redeem {'all' if intent.shares == 'all' else intent.shares} shares from MetaMorpho vault {intent.vault_address[:10]}...",
                tx_type="vault_redeem",
            )
            transactions.append(redeem_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_REDEEM.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "shares_wei": str(shares_wei),
                    "redeem_all": intent.shares == "all",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled VAULT_REDEEM: {'all' if intent.shares == 'all' else intent.shares} shares from vault {intent.vault_address[:10]}..."
            )
            return result

        except Exception as e:
            logger.exception(f"Failed to compile VAULT_REDEEM intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
            return result

    def _resolve_token(self, token: str, chain: str | None = None) -> TokenInfo | None:
        """Resolve a token symbol or address to TokenInfo.

        Uses the TokenResolver for unified token lookup with caching and
        optional on-chain discovery via gateway.

        Args:
            token: Token symbol (e.g., "USDC") or address
            chain: Optional chain to resolve token for (defaults to self.chain)

        Returns:
            TokenInfo or None if not found
        """
        target_chain = chain or self.chain

        try:
            # Use TokenResolver for unified lookup
            resolved = self._token_resolver.resolve(token, target_chain)

            return TokenInfo(
                symbol=resolved.symbol,
                address=resolved.address,
                decimals=resolved.decimals,
                is_native=resolved.is_native,
            )
        except Exception as e:
            # Import lazily to avoid circular import
            from almanak.framework.data.tokens.exceptions import TokenNotFoundError

            if isinstance(e, TokenNotFoundError):
                # Token not found in registry or on-chain - return None for backward compatibility
                logger.debug(f"Token '{token}' not found on {target_chain}")
                return None
            raise

    def _get_token_decimals(self, symbol: str) -> int:
        """Get decimals for a token symbol.

        Uses the TokenResolver for unified lookup. NEVER defaults to 18 decimals -
        raises TokenNotFoundError if decimals are unknown.

        Args:
            symbol: Token symbol (e.g., "USDC")

        Returns:
            Number of decimal places for the token

        Raises:
            TokenNotFoundError: If token cannot be resolved
        """
        return self._token_resolver.get_decimals(self.chain, symbol)

    def _is_native_token(self, symbol: str) -> bool:
        """Check if token is the native token."""
        native_tokens = {"ETH", "MATIC", "AVAX", "XPL", "OKB"}
        return symbol.upper() in native_tokens

    def _get_wrapped_native_address(self) -> str | None:
        """Return the wrapped native token address for the current chain.

        Single source of truth: WRAPPED_NATIVE in
        ``almanak/framework/data/tokens/data/chains.json`` (exposed via
        ``defaults.WRAPPED_NATIVE``). Previously this method carried a
        duplicate per-chain symbol dict that drifted from the real registry
        (e.g. the ``zerog`` entry was missing until VIB-2896 surfaced it).
        Reading the canonical dict directly prevents that drift.
        """
        from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

        return WRAPPED_NATIVE.get(self.chain)

    def _usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        """Convert USD amount to token amount in wei.

        Args:
            usd_amount: Amount in USD
            token: Target token info

        Returns:
            Token amount in smallest units (wei)
        """
        price = self._require_token_price(token.symbol)
        token_amount = usd_amount / price
        return int(token_amount * Decimal(10**token.decimals))

    def _calculate_expected_output(
        self,
        amount_in: int,
        from_token: TokenInfo,
        to_token: TokenInfo,
    ) -> int:
        """Calculate expected output amount.

        In production, this would query the DEX for a quote.
        For now, uses price oracle to estimate.

        Args:
            amount_in: Input amount in wei
            from_token: Input token info
            to_token: Output token info

        Returns:
            Expected output amount in wei
        """
        # Get prices
        from_price = self._require_token_price(from_token.symbol)
        to_price = self._require_token_price(to_token.symbol)

        # Convert input to USD
        from_amount_decimal = Decimal(str(amount_in)) / Decimal(10**from_token.decimals)
        usd_value = from_amount_decimal * from_price

        # Convert USD to output tokens
        to_amount_decimal = usd_value / to_price

        # Apply a small fee estimate (0.3%)
        to_amount_decimal = to_amount_decimal * Decimal("0.997")

        return int(to_amount_decimal * Decimal(10**to_token.decimals))

    def _build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> list[TransactionData]:
        """Build approve transaction(s) if needed.

        For most tokens, returns a single approve TX if allowance is insufficient.
        For tokens like USDC/USDT that require approve(0) first when allowance > 0,
        returns two TXs: approve(0) followed by approve(amount).

        Args:
            token_address: ERC20 token to approve
            spender: Address to approve (router)
            amount: Amount to approve

        Returns:
            List of TransactionData for approval (may be empty, 1, or 2 transactions)
        """
        transactions: list[TransactionData] = []
        token_lower = token_address.lower()
        requires_zero_first = token_lower in APPROVE_ZERO_FIRST_TOKENS
        on_chain_allowance = 0

        # ALWAYS query on-chain allowance to avoid stale cache issues
        # This is critical for safety - never skip approve based on cache alone
        if self._gateway_client is not None or self.rpc_url:
            on_chain_allowance = self._query_allowance(token_address, spender)
            if on_chain_allowance >= amount:
                # Already have sufficient on-chain allowance - update cache and skip
                if token_lower not in self._allowance_cache:
                    self._allowance_cache[token_lower] = {}
                self._allowance_cache[token_lower][spender.lower()] = on_chain_allowance
                logger.debug(
                    f"Sufficient on-chain allowance exists for {token_address} -> {spender}: {on_chain_allowance}"
                )
                return []
        else:
            # No way to query on-chain - check cache as fallback but log warning
            cached = self._allowance_cache.get(token_lower, {}).get(spender.lower(), 0)
            if cached >= amount:
                logger.warning(
                    f"Using cached allowance for {token_address} -> {spender} (no RPC available). "
                    f"This may cause issues if allowance was revoked on-chain."
                )
                return []

        # Build approve calldata helper
        def build_approve_calldata(approve_amount: int) -> str:
            spender_padded = spender.lower().replace("0x", "").zfill(64)
            amount_padded = hex(approve_amount)[2:].zfill(64)
            return ERC20_APPROVE_SELECTOR + spender_padded + amount_padded

        # If token requires approve(0) first AND has existing on-chain allowance > 0
        if requires_zero_first and on_chain_allowance > 0:
            logger.debug(f"Token {token_address} requires approve(0) first (existing allowance: {on_chain_allowance})")
            # Add approve(0) transaction first
            transactions.append(
                TransactionData(
                    to=token_address,
                    value=0,
                    data=build_approve_calldata(0),
                    gas_estimate=get_gas_estimate(self.chain, "approve"),
                    description=f"Reset approval to 0 for {spender[:10]}...",
                    tx_type="approve_reset",
                )
            )

        # Build main approve TX
        # Use actual amount + 10% buffer, but cap at MAX_UINT256
        # to avoid overflow when building calldata (hex would be >64 chars)
        if amount >= MAX_UINT256:
            approval_amount = MAX_UINT256
        else:
            approval_amount = min(int(amount * 1.1), MAX_UINT256)  # 10% buffer, capped

        transactions.append(
            TransactionData(
                to=token_address,
                value=0,
                data=build_approve_calldata(approval_amount),
                gas_estimate=get_gas_estimate(self.chain, "approve"),
                description=f"Approve {spender[:10]}... to spend token",
                tx_type="approve",
            )
        )

        # Update cache
        if token_lower not in self._allowance_cache:
            self._allowance_cache[token_lower] = {}
        self._allowance_cache[token_lower][spender.lower()] = approval_amount

        return transactions

    def _query_allowance(self, token_address: str, spender: str) -> int:
        """Query on-chain allowance for a token/spender pair.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            token_address: ERC20 token address
            spender: Spender address

        Returns:
            Current allowance (0 if query fails)
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                result = self._gateway_client.query_allowance(
                    chain=self.chain,
                    token_address=token_address,
                    owner_address=self.wallet_address,
                    spender_address=spender,
                )
                return result if result is not None else 0
            except Exception as e:
                logger.warning(f"Gateway allowance query failed for {token_address}: {e}")
                return 0

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            return 0

        try:
            from web3 import Web3

            if self._web3 is None:
                logger.debug("Using direct Web3 RPC for allowance query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # Build allowance call: allowance(owner, spender)
            owner_padded = self.wallet_address.lower().replace("0x", "").zfill(64)
            spender_padded = spender.lower().replace("0x", "").zfill(64)
            calldata = ERC20_ALLOWANCE_SELECTOR + owner_padded + spender_padded

            raw_result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(token_address),
                    "data": calldata,  # type: ignore[typeddict-item]
                }
            )

            if raw_result:
                return int(raw_result.hex(), 16)
            return 0
        except Exception as e:
            logger.warning(f"Failed to query allowance for {token_address}: {e}")
            return 0

    # Lazy-loaded from almanak.core.constants to avoid circular import
    # (compiler -> data/__init__ -> prediction_provider -> connectors -> execution -> compiler)
    _KNOWN_STABLECOINS: ClassVar[frozenset[str] | None] = None

    @classmethod
    def _get_known_stablecoins(cls) -> frozenset[str]:
        known = cls._KNOWN_STABLECOINS
        if known is None:
            from almanak.core.constants import STABLECOINS

            known = frozenset(s.upper() for s in STABLECOINS)
            cls._KNOWN_STABLECOINS = known
        return known

    # Wrapped native tokens map to their native counterpart for price lookups.
    # Wrapped natives are 1:1 pegged by the WETH9 contract (deposit/withdraw at par),
    # so ETH price == WETH price. When the oracle only has "ETH", a lookup for
    # "WETH" should resolve to ETH's price rather than failing.
    _WRAPPED_TO_NATIVE: ClassVar[dict[str, str]] = {
        "WETH": "ETH",
        "WMATIC": "MATIC",
        "WAVAX": "AVAX",
        "WBNB": "BNB",
        "WMNT": "MNT",
        "WS": "S",
        "WXPL": "XPL",
        "WPOL": "POL",
        "WOKB": "OKB",
        "WMON": "MON",
        "WBERA": "BERA",
    }

    def _require_token_price(self, symbol: str) -> Decimal:
        """Look up a token price, failing fast on missing or zero prices.

        When ``_using_placeholders`` is True (test-only mode) a fallback of
        ``Decimal("1")`` is returned for unknown tokens so that compilation
        can proceed with approximate values.  In production mode (a real
        price oracle is provided) a missing or zero price raises
        ``ValueError`` so the caller surfaces a clear error instead of
        silently using a bogus price.

        For known stablecoins (USDC, USDT, DAI, etc.), falls back to $1.00
        if the price oracle doesn't have them cached. This prevents compilation
        failures when the strategy's decide() didn't explicitly fetch the price.

        For wrapped native tokens (WETH, WMATIC, WAVAX, etc.), falls back to
        the native token price (ETH, MATIC, AVAX) since they are 1:1 pegged
        by the WETH9 contract.

        Args:
            symbol: Token symbol to look up.

        Returns:
            Token price in USD as ``Decimal``.

        Raises:
            ValueError: If the price is missing/zero and we are *not*
                using placeholder prices.
        """
        if self.price_oracle is None:
            if self._using_placeholders:
                return Decimal("1")
            # Fall back for stablecoins even without an oracle
            if symbol.upper() in self._get_known_stablecoins():
                return Decimal("1")
            raise ValueError(
                f"No price oracle available and placeholder prices are disabled. Cannot resolve price for '{symbol}'."
            )

        price = self.price_oracle.get(symbol)
        if price is None or price == 0:
            # Case-insensitive fallback: Token.__post_init__ uppercases symbols
            # (e.g., "cbETH" -> "CBETH") but the price oracle may store them in
            # original case. Try case-insensitive match before giving up.
            symbol_upper = symbol.upper()
            for key, val in self.price_oracle.items():
                if key.upper() == symbol_upper and val is not None and val != 0:
                    price = val
                    logger.debug(f"Resolved '{symbol}' price via case-insensitive match (key='{key}')")
                    break

        if price is None or price == 0:
            # Try wrapped-native alias (WETH -> ETH, WMATIC -> MATIC, etc.)
            native_alias = self._WRAPPED_TO_NATIVE.get(symbol.upper())
            if native_alias:
                alias_price = self.price_oracle.get(native_alias)
                if alias_price is not None and alias_price != 0:
                    logger.debug(f"Resolved '{symbol}' price via native alias '{native_alias}'")
                    return alias_price

            if self._using_placeholders:
                return Decimal("1")
            # Stablecoin fallback: these are always ~$1, safe to assume
            if symbol.upper() in self._get_known_stablecoins():
                if symbol not in self._stablecoin_fallback_logged:
                    logger.info(f"Price for '{symbol}' not in oracle cache, using stablecoin fallback ($1.00)")
                    self._stablecoin_fallback_logged.add(symbol)
                else:
                    logger.debug(f"Reusing stablecoin fallback price for '{symbol}'")
                return Decimal("1")
            raise ValueError(
                f"Price for '{symbol}' is {'zero' if price == 0 else 'missing'} in the price oracle. "
                "Compilation requires a valid price to calculate amounts and slippage."
            )
        return price

    def _resolve_dest_wallet(self, dest_chain: str) -> str:
        """Resolve destination wallet for cross-chain operations.

        If chain_wallets is configured (from wallet registry), returns the
        wallet for the destination chain. Otherwise returns self.wallet_address.

        Args:
            dest_chain: Destination chain name

        Returns:
            Wallet address for the destination chain
        """
        if self._chain_wallets:
            return self._chain_wallets.get(dest_chain.lower(), self.wallet_address)
        return self.wallet_address

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only.

        WARNING: These prices are HARDCODED and OUTDATED.
        DO NOT USE IN PRODUCTION - they will cause:
        - Incorrect slippage calculations
        - Swap reverts (amountOutMinimum too high)
        - Position sizing errors
        - Health factor miscalculations

        Real prices as of 2026-01: ETH ~$3400, BTC ~$105,000
        These placeholders show ETH at $2000, BTC at $45,000 - 40-60% wrong!
        """
        logger.debug(
            "PLACEHOLDER PRICES being used - NOT SAFE FOR PRODUCTION. ETH=$2000 (real ~$3400), BTC=$45000 (real ~$105000)"
        )
        return {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
            "USDT": Decimal("1"),
            "DAI": Decimal("1"),
            "WBTC": Decimal("45000"),
            "MATIC": Decimal("0.80"),
            "WMATIC": Decimal("0.80"),
            "ARB": Decimal("1.20"),
            "OP": Decimal("2.50"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "BNB": Decimal("600"),
            "WBNB": Decimal("600"),
            "S": Decimal("0.50"),
            "WS": Decimal("0.50"),
            "MNT": Decimal("0.80"),
            "WMNT": Decimal("0.80"),
        }

    @staticmethod
    def _format_amount(amount: int, decimals: int) -> str:
        """Format a wei amount for display."""
        decimal_amount = Decimal(str(amount)) / Decimal(10**decimals)
        return f"{decimal_amount:,.4f}"

    def _parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None:
        """Parse pool identifier to extract token addresses and fee tier.

        Supports formats:
        - "TOKEN0/TOKEN1/FEE" (e.g., "WETH/USDC/3000")
        - "TOKEN0/TOKEN1" (defaults to 3000 fee tier)
        - "0xTOKEN0/0xTOKEN1/FEE" (raw token addresses also work)

        Bare pool addresses ("0x..." with no "/") are NOT supported. Resolving a
        pool address to its token pair requires an on-chain lookup (calling the
        pool contract's token0()/token1()/fee() view functions), which this
        compiler doesn't currently implement. Use the TOKEN0/TOKEN1/FEE format
        instead.

        Args:
            pool: Pool identifier string

        Returns:
            Tuple of (token0_info, token1_info, fee_tier, tokens_swapped) or None if parsing fails.
            tokens_swapped is True when the user-specified token order was reversed to match
            the on-chain convention (token0 address < token1 address). Callers must invert
            price ranges and swap amounts when this flag is True.
        """
        # Default fee tier (0.3%)
        default_fee = 3000

        # Reject bare pool address format (e.g., "0xbDbC38652D78AF..." with no "/").
        # The previous behavior here silently substituted WETH/USDC as a
        # placeholder pair, which would compile a working LP intent against the
        # WRONG pool and only fail on-chain (or worse, succeed in a different
        # pool entirely -- silent data corruption with real-money risk). Until
        # we implement an on-chain pool resolver that calls the pool contract's
        # token0()/token1()/fee() view functions, this path must fail hard.
        # See compiler.py:_parse_pool_info docstring for supported formats.
        if pool.startswith("0x") and "/" not in pool:
            logger.error(
                "Bare pool address '%s' is not supported by the LP compiler. "
                "Use 'TOKEN0/TOKEN1/FEE' format instead (e.g., 'WETH/USDC/3000'); "
                "raw token addresses are accepted, e.g. "
                "'0xToken0Addr.../0xToken1Addr.../3000'.",
                pool,
            )
            return None

        # Handle TOKEN0/TOKEN1/FEE or TOKEN0/TOKEN1 format
        parts = pool.split("/")
        if len(parts) < 2:
            return None

        token0_symbol = parts[0].strip()
        token1_symbol = parts[1].strip()

        # Parse fee tier if provided
        fee_tier = default_fee
        if len(parts) >= 3:
            try:
                fee_tier = int(parts[2].strip())
            except ValueError:
                logger.warning(f"Invalid fee tier: {parts[2]}, using default {default_fee}")

        # Resolve token addresses
        token0 = self._resolve_token(token0_symbol)
        token1 = self._resolve_token(token1_symbol)

        if token0 is None:
            logger.error(f"Unknown token: {token0_symbol}")
            return None
        if token1 is None:
            logger.error(f"Unknown token: {token1_symbol}")
            return None

        # Ensure tokens are sorted (token0 < token1 by address)
        tokens_swapped = False
        if token0.address.lower() > token1.address.lower():
            token0, token1 = token1, token0
            tokens_swapped = True
            logger.debug(f"Swapped tokens to maintain sorting: {token0.symbol}/{token1.symbol}")

        return (token0, token1, fee_tier, tokens_swapped)

    # Uniswap V3 tick bounds
    UNISWAP_MIN_TICK = -887272
    UNISWAP_MAX_TICK = 887272

    @staticmethod
    def _price_to_tick(
        price: Decimal,
        token0_decimals: int = 18,
        token1_decimals: int = 18,
    ) -> int:
        """Convert a price to a Uniswap V3 tick.

        Uniswap V3 uses tick-based pricing where:
            price = 1.0001^tick

        But the price must be adjusted for token decimals first:
            adjusted_price = price / 10^(token0_decimals - token1_decimals)
            tick = log(adjusted_price) / log(1.0001)

        For example, with WETH/USDC (18/6 decimals):
            price = 3400 USDC per WETH (nominal)
            adjusted = 3400 / 10^(18-6) = 3400 / 10^12 = 3.4e-9
            tick = log(3.4e-9) / log(1.0001) ≈ -194957

        Args:
            price: The price in nominal units (token1 per token0)
            token0_decimals: Decimals of token0
            token1_decimals: Decimals of token1

        Returns:
            The tick value (rounded down), bounded to valid Uniswap tick range
        """
        import math

        if price <= 0:
            raise ValueError("Price must be positive")

        # Adjust price for decimal difference
        decimal_adjustment = 10 ** (token0_decimals - token1_decimals)
        adjusted_price = float(price) / decimal_adjustment

        # tick = ln(adjusted_price) / ln(1.0001)
        tick = math.floor(math.log(adjusted_price) / math.log(1.0001))

        # Bound to valid tick range
        tick = max(tick, IntentCompiler.UNISWAP_MIN_TICK)
        tick = min(tick, IntentCompiler.UNISWAP_MAX_TICK)

        return tick

    @staticmethod
    def _tick_to_price(tick: int) -> Decimal:
        """Convert a Uniswap V3 tick to a price.

        Args:
            tick: The tick value

        Returns:
            The price (1.0001^tick)
        """
        return Decimal(str(1.0001**tick))

    @staticmethod
    def _get_tick_spacing(fee_tier: int) -> int:
        """Get the tick spacing for a given fee tier.

        Standard tick spacings by fee tier:
        - 100 (0.01%): tick spacing 1
        - 500 (0.05%): tick spacing 10
        - 2500 (0.25%): tick spacing 50  (PancakeSwap V3)
        - 3000 (0.30%): tick spacing 60
        - 10000 (1.00%): tick spacing 200

        Args:
            fee_tier: The fee tier in basis points

        Returns:
            The tick spacing
        """
        tick_spacings = {
            100: 1,
            500: 10,
            2500: 50,
            3000: 60,
            10000: 200,
        }
        if fee_tier not in tick_spacings:
            logger.warning(
                "Unknown fee tier %d -- defaulting to tick_spacing=60. "
                "Known fee tiers: %s. "
                "If this is a protocol-specific fee tier, add it to _get_tick_spacing().",
                fee_tier,
                list(tick_spacings.keys()),
            )
        return tick_spacings.get(fee_tier, 60)

    def set_allowance(self, token_address: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing or after on-chain approval).

        Args:
            token_address: Token contract address
            spender: Spender address
            amount: Allowance amount
        """
        if token_address not in self._allowance_cache:
            self._allowance_cache[token_address] = {}
        self._allowance_cache[token_address][spender] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()

    def _query_position_liquidity(self, position_manager: str, token_id: int) -> int | None:
        """Query the liquidity of a Uniswap V3 position from on-chain.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            position_manager: NonfungiblePositionManager contract address
            token_id: Position NFT token ID

        Returns:
            Liquidity amount, or None if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_position_liquidity(
                    chain=self.chain,
                    position_manager=position_manager,
                    token_id=token_id,
                )
            except Exception as e:
                error_msg = str(e)
                if "invalid token id" in error_msg.lower():
                    logger.info(
                        "Gateway position liquidity query returned invalid token id; treating as closed position",
                        extra={"token_id": token_id, "error": error_msg},
                    )
                    return 0
                logger.error(f"Gateway position liquidity query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position liquidity")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # positions(uint256) returns a tuple with liquidity at index 7
            # Encode the call: positions(tokenId)
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(position_manager),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode result - liquidity is at offset 7 * 32 = 224 bytes
            # Position struct: nonce, operator, token0, token1, fee, tickLower, tickUpper, liquidity, ...
            if len(result) >= 256:  # 8 * 32 bytes minimum
                liquidity_offset = 7 * 32
                liquidity = int.from_bytes(result[liquidity_offset : liquidity_offset + 32], byteorder="big")
                logger.debug(f"Position #{token_id} liquidity: {liquidity}")
                return liquidity
            else:
                logger.warning(f"Unexpected result length from positions call: {len(result)}")
                return None

        except Exception as e:
            logger.error(f"Failed to query position liquidity: {e}")
            return None

    def _query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]:
        """Query tokens owed (fees + withdrawn liquidity) for a Uniswap V3 position.

        Args:
            position_manager: NonfungiblePositionManager contract address
            token_id: Position NFT token ID

        Returns:
            Tuple of (tokensOwed0, tokensOwed1) or (None, None) if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                # Use gateway's dedicated QueryPositionTokensOwed method
                from almanak.gateway.proto import gateway_pb2

                request = gateway_pb2.PositionTokensOwedRequest(
                    chain=str(self.chain),
                    position_manager=position_manager,
                    token_id=token_id,
                )

                response = self._gateway_client.rpc.QueryPositionTokensOwed(request, timeout=10.0)

                if not response.success:
                    error_msg = response.error or ""
                    if "position not found" in error_msg.lower() or "invalid token id" in error_msg.lower():
                        logger.info(
                            "Gateway tokens owed query indicates closed position",
                            extra={"token_id": token_id, "error": error_msg},
                        )
                        return 0, 0
                    logger.error(f"Gateway QueryPositionTokensOwed failed: {error_msg}")
                    return None, None

                # Parse response - tokens are returned as decimal strings
                try:
                    tokens_owed0 = int(response.tokens_owed0) if response.tokens_owed0 else 0
                    tokens_owed1 = int(response.tokens_owed1) if response.tokens_owed1 else 0
                    logger.debug(f"Position #{token_id} tokens owed: {tokens_owed0} token0, {tokens_owed1} token1")
                    return tokens_owed0, tokens_owed1
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to parse tokens owed from gateway response: {e}")
                    return None, None
            except Exception as e:
                error_msg = str(e)
                if "invalid token id" in error_msg.lower():
                    logger.info(
                        "Gateway tokens owed query returned invalid token id; treating as closed position",
                        extra={"token_id": token_id, "error": error_msg},
                    )
                    return 0, 0
                logger.error(f"Gateway position tokens owed query failed: {e}")
                return None, None

        # Fallback to direct Web3 RPC
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position tokens owed")
            return None, None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # positions(uint256) returns a tuple
            # tokensOwed0 is at index 10, tokensOwed1 is at index 11
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(position_manager),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode result - tokensOwed0 is at offset 10 * 32 = 320 bytes, tokensOwed1 at 11 * 32 = 352 bytes
            if len(result) >= 384:  # 12 * 32 bytes minimum
                tokens_owed0_offset = 10 * 32
                tokens_owed1_offset = 11 * 32
                tokens_owed0 = int.from_bytes(result[tokens_owed0_offset : tokens_owed0_offset + 32], byteorder="big")
                tokens_owed1 = int.from_bytes(result[tokens_owed1_offset : tokens_owed1_offset + 32], byteorder="big")
                logger.debug(f"Position #{token_id} tokens owed: {tokens_owed0} token0, {tokens_owed1} token1")
                return tokens_owed0, tokens_owed1
            else:
                logger.warning(f"Unexpected result length from positions call: {len(result)}")
                return None, None

        except Exception as e:
            logger.error(f"Failed to query position tokens owed: {e}")
            return None, None

    def _get_aerodrome_pool_address(self, token_a: str, token_b: str, stable: bool) -> str | None:
        """Query Aerodrome pool address, preferring gateway RPC over direct calls."""
        from .compiler_aerodrome import get_aerodrome_pool_address

        return get_aerodrome_pool_address(self, token_a, token_b, stable)

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        """Query ERC-20 token balance from on-chain.

        Uses gateway RPC when gateway_client is configured, otherwise falls back
        to direct Web3 RPC (deprecated for production use).

        Args:
            token_address: ERC-20 token contract address
            wallet_address: Wallet address to query balance for

        Returns:
            Token balance in wei, or None if query fails
        """
        # Prefer gateway RPC when available
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_erc20_balance(
                    chain=self.chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query ERC-20 balance")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._web3 is None:
                logger.warning("Using direct Web3 RPC for balance query - this is deprecated")
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            # balanceOf(address) selector
            selector = "0x70a08231"
            # Pad address to 32 bytes (remove 0x prefix, left-pad with zeros)
            padded_address = wallet_address[2:].lower().zfill(64)
            data = selector + padded_address

            result = self._web3.eth.call(
                {
                    "to": self._web3.to_checksum_address(token_address),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )

            # Decode uint256 balance
            balance = int.from_bytes(result, byteorder="big")
            logger.debug(f"ERC-20 balance for {wallet_address} at {token_address}: {balance}")
            return balance

        except Exception as e:
            logger.error(f"Failed to query ERC-20 balance: {e}")
            return None

    def _query_erc20_balance_for_chain(self, token_address: str, wallet_address: str, chain: str) -> int | None:
        """Query ERC-20 balance on a specific chain (which may differ from self.chain).

        Used by cross-chain intents like BridgeIntent when amount='all' must be
        resolved from the source chain's actual token balance.

        Args:
            token_address: ERC-20 token contract address
            wallet_address: Wallet address to query balance for
            chain: Chain to query (e.g. "arbitrum" even if self.chain is "base")

        Returns:
            Token balance in wei, or None if query fails
        """
        if chain == self.chain:
            return self._query_erc20_balance(token_address, wallet_address)

        # Cross-chain query: prefer gateway (it supports any chain).
        # Fail-closed: if a gateway is configured but fails, do NOT fall through to direct RPC.
        # This matches the behavior of _query_erc20_balance which treats gateway failures as terminal.
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_erc20_balance(
                    chain=chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error("Gateway balance query failed for %s: %s", chain, e)
                return None

        # No gateway configured: fall back to direct Web3 RPC (local dev / Anvil only).
        rpc_url = self._get_rpc_url_for_chain(chain)
        if rpc_url is None:
            logger.warning(f"No RPC URL for chain {chain} — cannot query ERC-20 balance")
            return None

        try:
            from web3 import Web3
        except ImportError:
            logger.warning("web3 is not installed; cannot use direct RPC fallback for ERC-20 balance query")
            return None

        try:
            web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
            selector = "0x70a08231"
            padded_address = wallet_address[2:].lower().zfill(64)
            data = selector + padded_address
            result = web3.eth.call(
                {
                    "to": web3.to_checksum_address(token_address),
                    "data": data,  # type: ignore[typeddict-item]
                }
            )
            balance = int.from_bytes(result, byteorder="big")
            logger.debug(f"ERC-20 balance for {wallet_address} at {token_address} on {chain}: {balance}")
            return balance
        except Exception as e:
            logger.error(f"Failed to query ERC-20 balance on {chain}: {e}")
            return None

    def _query_native_balance_for_chain(self, wallet_address: str, chain: str) -> int | None:
        """Query native token balance on a specific chain (which may differ from self.chain).

        Used by BridgeIntent when amount='all' and the bridge token is a native asset
        (e.g. ETH, AVAX). Mirrors the gateway-first / fail-closed pattern of
        _query_erc20_balance_for_chain.

        Args:
            wallet_address: Wallet address to query balance for
            chain: Chain to query (e.g. "arbitrum" even if self.chain is "base")

        Returns:
            Native balance in wei, or None if query fails
        """
        if chain == self.chain:
            return self._query_native_balance(wallet_address)

        # Fail-closed: if a gateway is configured but fails, do NOT fall through to direct RPC.
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_native_balance(
                    chain=chain,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error("Gateway native balance query failed for %s: %s", chain, e)
                return None

        # No gateway configured: fall back to direct Web3 RPC (local dev / Anvil only).
        rpc_url = self._get_rpc_url_for_chain(chain)
        if rpc_url is None:
            logger.warning(f"No RPC URL for chain {chain} — cannot query native balance")
            return None

        try:
            from web3 import Web3
        except ImportError:
            logger.warning("web3 is not installed; cannot use direct RPC fallback for native balance query")
            return None

        try:
            web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
            balance = web3.eth.get_balance(web3.to_checksum_address(wallet_address))
            logger.debug(f"Native balance for {wallet_address} on {chain}: {balance}")
            return balance
        except Exception as e:
            logger.error(f"Failed to query native balance on {chain}: {e}")
            return None

    def _get_rpc_url_for_chain(self, chain: str) -> str | None:
        """Get RPC URL for an arbitrary chain.

        Unlike _get_chain_rpc_url() which is bound to self.chain, this method
        accepts an explicit chain parameter. Used for cross-chain queries (e.g.
        querying from_chain balance when compiling BridgeIntent).

        Args:
            chain: Chain name (e.g. "arbitrum", "base")

        Returns:
            RPC URL string or None if not available.
        """
        if chain == self.chain:
            return self._get_chain_rpc_url()

        # Check for managed Anvil fork on the target chain
        anvil_port_var = f"ANVIL_{chain.upper()}_PORT"
        anvil_port = os.environ.get(anvil_port_var)
        if anvil_port:
            return f"http://127.0.0.1:{anvil_port}"

        try:
            from almanak.gateway.utils import get_rpc_url

            return get_rpc_url(chain)
        except (ImportError, ValueError) as e:
            logger.debug(f"Failed to get RPC URL for {chain}: {e}")
            return None

    def _query_native_balance(self, wallet_address: str) -> int | None:
        """Query native token balance (ETH, MATIC, AVAX, etc.) from on-chain.

        Uses gateway RPC when available, otherwise falls back to direct Web3 RPC.

        Returns:
            Native balance in wei, or None if query fails
        """
        # Prefer gateway RPC via public API
        if self._gateway_client is not None:
            try:
                return self._gateway_client.query_native_balance(
                    chain=self.chain,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway native balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self.rpc_url is None and self._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query native balance")
            return None

        try:
            from web3 import Web3

            if self._web3 is None:
                self._web3 = Web3(Web3.HTTPProvider(self.rpc_url))

            assert self._web3 is not None
            balance = self._web3.eth.get_balance(self._web3.to_checksum_address(wallet_address))
            logger.debug(f"Native balance for {wallet_address}: {balance}")
            return balance
        except Exception as e:
            logger.error(f"Failed to query native balance: {e}")
            return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "IntentCompiler",
    "CompilationResult",
    "CompilationStatus",
    "TransactionData",
    "TokenInfo",
    "PriceInfo",
    "DefaultSwapAdapter",
    "SwapProtocolAdapter",
    "UniswapV3LPAdapter",
    "LPProtocolAdapter",
    "AaveV3Adapter",
    "LendingProtocolAdapter",
    "DEFAULT_GAS_ESTIMATES",
    "CHAIN_GAS_OVERRIDES",
    "get_gas_estimate",
    "PROTOCOL_ROUTERS",
    "LP_POSITION_MANAGERS",
    "LENDING_POOL_ADDRESSES",
    "AAVE_VARIABLE_RATE_MODE",
]
