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
from typing import TYPE_CHECKING, Any, ClassVar, cast

import grpc

# Note: FlashLoanSelector import is done lazily in _compile_flash_loan to avoid circular import
# Note: PolymarketAdapter import is done lazily in __init__ to avoid circular import and allow optional usage
# Note: MorphoBlueAdapter is imported lazily in _compile_* methods to avoid circular import
# Note: TokenNotFoundError and get_token_resolver are imported lazily to avoid circular import
# (compiler -> data/__init__ -> prediction_provider -> connectors/__init__ -> ... -> compiler)
from ..models.reproduction_bundle import ActionBundle
from ..utils.grpc_utils import (
    get_grpc_retry_after_seconds,
    get_grpc_status_code,
    is_transient_grpc_error,
)
from ..utils.log_formatters import (
    _emojis_enabled,
    format_percentage,
    format_slippage_bps,
    format_token_amount,
)
from .intent_errors import InvalidCollateralForMarketError
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

    from ..connectors.polymarket.adapter import PolymarketAdapter
    from ..data.tokens import TokenResolver as TokenResolverType
    from ..gateway_client import GatewayClient
    from .bridge import BridgeIntent
    from .bridge_selector import BridgeSelector
    from .pool_validation import PoolValidationResult
    from .vocabulary import UnwrapNativeIntent, WrapNativeIntent

logger = logging.getLogger(__name__)

# One-shot deprecation signal: the ``pancakeswap_perps`` protocol key is the
# legacy attribution (broker_id=2) for Aster v1. New strategies should use
# ``protocol="aster_perps"`` directly. Warn at compile time (not just at
# module import) so strategies that import from aster_perps but keep the old
# intent-level key still see the signal.
_PCS_PERPS_KEY_WARNED = False


def _warn_pcs_perps_protocol_key_once() -> None:
    global _PCS_PERPS_KEY_WARNED
    if _PCS_PERPS_KEY_WARNED:
        return
    _PCS_PERPS_KEY_WARNED = True
    logger.warning(
        "Intent protocol='pancakeswap_perps' is deprecated; route through "
        "'aster_perps' (canonical, broker_id=0) unless you explicitly need "
        "PancakeSwap attribution (broker_id=2). This compiler path will be "
        "removed once pancakeswap_delta_neutral_lp migrates (VIB-3044 Phase 4)."
    )


# =============================================================================
# Extracted modules — re-exported for backward compatibility
# (all symbols that were importable from this module remain importable)
# =============================================================================

from ._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_lifi_gas_estimate,
    choose_safer_quote,
    compute_min_amount_out,
    normalise_gateway_or_rpc,
    parse_lifi_tx_value,
    probe_traderjoe_bin_step,
    sum_transaction_gas,
)
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
# Native-token symbol table (VIB-3135)
# =============================================================================
#
# Per-chain set of symbols that refer to that chain's native gas token. Used
# by ``IntentCompiler._resolve_token`` as a defense-in-depth check: if the
# token registry sets ``is_native=False`` for an entry but the symbol matches
# the chain's native gas token (e.g. POL on Polygon with the 0x...1010
# precompile address), we coerce ``is_native`` to True so the swap compile
# path skips ERC20 allowance/approve on a non-ERC20 address. Inlined here —
# not imported from ``almanak.gateway.data.balance.web3_provider`` — to keep
# the compiler free of the gateway web3 import chain (unit tests that mock
# ``web3`` would otherwise crash during ``_resolve_token``).
#
# Both the pre-rebrand (MATIC) and current (POL) Polygon symbols are treated
# as native. Mantle (MNT), Monad (MON), Berachain (BERA), Sonic (S) and
# zerog (A0GI) are included for future coverage but their entries in the
# token registry already carry ``is_native=True`` so this is belt-and-braces.
_CHAIN_NATIVE_SYMBOLS: dict[str, frozenset[str]] = {
    "ethereum": frozenset({"ETH"}),
    "arbitrum": frozenset({"ETH"}),
    "optimism": frozenset({"ETH"}),
    "base": frozenset({"ETH"}),
    "blast": frozenset({"ETH"}),
    "linea": frozenset({"ETH"}),
    "polygon": frozenset({"MATIC", "POL"}),
    "avalanche": frozenset({"AVAX"}),
    "bsc": frozenset({"BNB"}),
    "sonic": frozenset({"S"}),
    "plasma": frozenset({"XPL"}),
    "mantle": frozenset({"MNT"}),
    "berachain": frozenset({"BERA"}),
    "monad": frozenset({"MON"}),
    "xlayer": frozenset({"OKB"}),
    "zerog": frozenset({"A0GI"}),
}


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
        # VIB-3136: Copy the provided dict so alias expansion below doesn't mutate
        # the caller's dict (which may be shared across compiler instances or
        # reused elsewhere, e.g. MarketSnapshot's internal price cache).
        self.price_oracle: dict[str, Decimal] | None
        if self._using_placeholders:
            logger.debug(
                "IntentCompiler created without price oracle, will use placeholders if not updated before compilation"
            )
            self.price_oracle = self._get_placeholder_prices()
        else:
            self.price_oracle = dict(price_oracle) if price_oracle is not None else None
        # VIB-3136: Ensure adapters that consume ``price_oracle`` directly (via
        # ``dict.get(symbol)``) see both native and wrapped-native keys. The
        # compiler's own ``_require_token_price`` already walks the alias map,
        # but ``UniswapV3Adapter`` et al. treat the dict as frozen and silently
        # fall back to $1 on miss. Expand once here so the consumed dict is
        # bidirectionally complete.
        self._expand_native_aliases_in_price_oracle()
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
        """Update the price oracle with real prices, clearing placeholder state.

        VIB-3136: Copies the incoming dict so subsequent alias expansion does
        not mutate the caller's dict.
        """
        self.price_oracle = dict(prices)
        self._using_placeholders = False
        self._expand_native_aliases_in_price_oracle()

    def restore_prices(self, original_oracle: dict[str, Decimal] | None, original_using_placeholders: bool) -> None:
        """Restore prices to a previous state (used after temporary override).

        VIB-3136: Copies the incoming dict so subsequent alias expansion does
        not mutate the caller's dict.
        """
        self.price_oracle = dict(original_oracle) if original_oracle is not None else None
        self._using_placeholders = original_using_placeholders
        self._expand_native_aliases_in_price_oracle()

    def _resolve_protocol(self, intent_protocol: str | None) -> str:
        """Resolve intent protocol to canonical key, falling back to default.

        Normalizes aliases (e.g., "agni" -> "agni_finance" on mantle) and falls
        back to self.default_protocol if intent_protocol is None.
        """
        if intent_protocol is None:
            return self.default_protocol
        from ..connectors.protocol_aliases import normalize_protocol

        return normalize_protocol(self.chain, intent_protocol)

    def _ensure_polymarket_adapter(self) -> None:
        """Retry adapter init if gateway has connected since construction."""
        if self._polymarket_adapter is not None:
            return
        if self.chain.lower() != "polygon":
            return
        if self._gateway_client is None or not self._gateway_client.is_connected:
            return
        self._init_polymarket_adapter()

    def _init_polymarket_adapter(self) -> None:
        """Initialize the Polymarket adapter for gateway-backed Polygon intents.

        This method lazily initializes the PolymarketAdapter for prediction market
        intents. The adapter is only initialized when:
        1. The chain is 'polygon' (case-insensitive)
        2. A PolymarketConfig is provided in the IntentCompilerConfig

        If on Polygon without a PolymarketConfig, the method silently returns.
        VIB-307: Warning is deferred to compile time so non-prediction Polygon
        strategies don't see noisy Polymarket warnings at startup.

        This lazy initialization ensures:
        - Non-Polygon usage is unaffected (no import overhead)
        - Missing gateway connectivity is handled gracefully
        - Clear error messages when prediction intents are attempted without a
          gateway-backed Polymarket client
        """
        # Only initialize for Polygon chain
        if self.chain.lower() != "polygon":
            return

        # Lazy import to avoid circular imports and allow optional usage
        try:
            from ..connectors.polymarket.adapter import PolymarketAdapter
            from ..connectors.polymarket.gateway_client import GatewayPolymarketClient

            if self._gateway_client is None or not self._gateway_client.is_connected:
                return

            from web3 import Web3

            from ..web3.gateway_provider import GatewayWeb3Provider

            if self._web3 is None:
                self._web3 = Web3(GatewayWeb3Provider(self._gateway_client, chain=self.chain))
            web3_instance = self._web3
            polymarket_client = GatewayPolymarketClient(self._gateway_client)

            self._polymarket_adapter = PolymarketAdapter(
                client=polymarket_client,
                wallet_address=self.wallet_address,
                web3=web3_instance,
            )
            logger.info("PolymarketAdapter initialized for wallet=%s...", self.wallet_address[:10])
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
        if self._gateway_client is not None and self._gateway_client.is_connected:
            try:
                import json

                from almanak.gateway.proto import gateway_pb2

                response = self._gateway_client.rpc.Call(
                    gateway_pb2.RpcRequest(
                        chain=self.chain,
                        method="eth_getCode",
                        params=json.dumps([self.wallet_address, "latest"]),
                        id="wallet_contract_check",
                    ),
                    timeout=self.rpc_timeout,
                )
                if not response.success:
                    logger.debug("Gateway eth_getCode RPC error: %s", response.error)
                    return None
                code = json.loads(response.result) if response.result else None
                return code not in ("0x", "0x0", "", None)
            except Exception as e:
                logger.debug("Failed to check wallet bytecode via gateway eth_getCode: %s", e)
                return None

        rpc_url = self._get_chain_rpc_url()
        if not rpc_url:
            return None

        try:
            import httpx

            response = httpx.post(  # vib-2986-exempt: local-only fallback when no connected gateway client is available
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
        """Check pool validation result and fail-closed on definitive/attempted-but-failed outcomes.

        Fail-closed on:
            - NOT_FOUND: factory confirmed the pool is absent.
            - RPC_FAILED: RPC was attempted but errored — we can't trust downstream execution.

        Warn-and-proceed on genuinely impossible-to-verify states (no RPC configured, unknown
        protocol, missing factory entry, malformed response) since these are environmental
        and refusing to compile would block legitimate flows like offline permission discovery.

        Offline-only paths (placeholder-price mode, permission-discovery mode) relax the
        RPC_FAILED fail-closed rule to a warning because those paths legitimately run against
        unreachable RPC endpoints and only need calldata shapes, not on-chain truth.

        Args:
            result: Pool validation result from pool_validation module.
            intent_id: Intent ID for error reporting.

        Returns:
            CompilationResult with FAILED status when fail-closed, None if OK to proceed.
        """
        from .pool_validation import PoolValidationReason

        offline_mode = self._using_placeholders or getattr(self._config, "permission_discovery", False)

        fail_closed_reasons = {PoolValidationReason.NOT_FOUND}
        if not offline_mode:
            fail_closed_reasons.add(PoolValidationReason.RPC_FAILED)

        if result.exists is False or result.reason in fail_closed_reasons:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=result.error or result.warning or f"Pool validation failed ({result.reason.value})",
                intent_id=intent_id,
            )
        if result.warning:
            logger.warning("Pool validation: %s (reason=%s)", result.warning, result.reason.value)
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
            elif intent_type == IntentType.DELEVERAGE:
                # DELEVERAGE is structurally identical to REPAY at the protocol level.
                # The intent carries extra risk-event context (trigger_reason, observed_hf,
                # target_hf) but the on-chain transaction is the same repay call.
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

        except grpc.RpcError as e:
            status_code = get_grpc_status_code(e)
            is_transient = is_transient_grpc_error(e)
            retry_after_seconds = get_grpc_retry_after_seconds(e)
            log = logger.warning if is_transient else logger.error
            log(
                "Failed to compile intent due to gRPC error (code=%s transient=%s retry_after=%s): %s",
                status_code,
                is_transient,
                retry_after_seconds,
                e,
            )
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
                is_transient=is_transient,
                retry_after_seconds=retry_after_seconds,
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

        from ..connectors.across.adapter import AcrossBridgeAdapter
        from ..connectors.stargate.adapter import StargateBridgeAdapter
        from .bridge_selector import BridgeSelector

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
        # Phase 6B.3: route to dedicated protocol helpers when applicable.
        # Falls through to the Uniswap-V3-style body when ``None`` is returned.
        routed = self._dispatch_swap_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        return self._compile_swap_v3_body(intent, protocol)

    def _dispatch_swap_protocol_route(self, intent: SwapIntent) -> CompilationResult | None:
        """Route a SWAP intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when the generic Uniswap-V3-style body in
        ``_compile_swap_v3_body`` should handle it.

        Extracted in Phase 6B.3 so ``_compile_swap`` itself stays small.
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

                if normalize_protocol(self.chain, intent.protocol) == "lifi":
                    return self._compile_lifi_swap(intent)
            return self._compile_cross_chain_swap(intent)

        # Check for aggregator protocols
        protocol = self._resolve_protocol(intent.protocol)

        # Handle Pendle first: explicit protocol OR auto-detect PT-/YT- prefixed tokens.
        # Must run before other protocol dispatches so that PT-/YT- tokens are routed to
        # Pendle regardless of default_protocol (e.g., enso, aerodrome). VIB-2535.
        if protocol == "pendle":
            return self._compile_pendle_swap(intent)
        if intent.protocol is None and self._has_pendle_token_prefix(intent):
            return self._compile_pendle_swap(intent)

        # Protocols with dedicated compile helpers. Order preserved from the pre-refactor
        # method so dispatch semantics (consensus-critical) stay unchanged.
        dedicated_compilers = {
            "enso": self._compile_enso_swap,
            "lifi": self._compile_lifi_swap,
            # Aerodrome/Velodrome - Solidly-fork with different swap interface.
            # protocol is already resolved (velodrome -> aerodrome on Optimism).
            "aerodrome": self._compile_swap_aerodrome,
            # Curve - pool-based AMM with direct pool addressing.
            "curve": self._compile_swap_curve,
            # Uniswap V4 - PoolManager-based singleton with different interface.
            "uniswap_v4": self._compile_swap_uniswap_v4,
            # Fluid DEX - direct pool swapIn call.
            "fluid": self._compile_swap_fluid,
            # TraderJoe V2 - LBRouter2 with Path struct (VIB-1928), NOT Uniswap V3's
            # exactInputSingle.
            "traderjoe_v2": self._compile_swap_traderjoe_v2,
        }
        handler = dedicated_compilers.get(protocol)
        if handler is not None:
            return handler(intent)

        return None

    @staticmethod
    def _has_pendle_token_prefix(intent: SwapIntent) -> bool:
        """True iff either swap leg is a PT-/YT- token that must route to Pendle."""
        to_upper = (intent.to_token or "").upper()
        from_upper = (intent.from_token or "").upper()
        return to_upper.startswith(("PT-", "YT-")) or from_upper.startswith(("PT-", "YT-"))

    def _compile_swap_v3_body(self, intent: SwapIntent, protocol: str) -> CompilationResult:
        """Uniswap-V3-style swap body shared across uniswap_v3, v2, sushiswap, pancakeswap_v3, etc.

        Phase 6B.3: extracted from ``_compile_swap`` onto the shared helpers in
        ``_compiler_helpers.py``. Behaviour — error messages, approval-chain
        ordering, metadata shape — is preserved byte-for-byte.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Resolve token addresses
            tokens_or_fail = self._resolve_swap_tokens(intent)
            if isinstance(tokens_or_fail, CompilationResult):
                return tokens_or_fail
            from_token, to_token = tokens_or_fail

            # Step 2: Calculate input amount
            amount_or_fail = self._resolve_swap_amount_in(intent, from_token)
            if isinstance(amount_or_fail, CompilationResult):
                return amount_or_fail
            amount_in = amount_or_fail

            # Step 3: Calculate oracle-based expected output (fail-closed if oracle missing)
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

            # Step 4: Build protocol adapter + resolve router
            adapter = DefaultSwapAdapter(
                self.chain,
                protocol,
                pool_selection_mode=self._config.swap_pool_selection_mode,
                fixed_fee_tier=self._config.fixed_swap_fee_tier,
                rpc_url=self._get_chain_rpc_url(),
                rpc_timeout=self.rpc_timeout,
                gateway_client=self._gateway_client,
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
                transactions.extend(self._build_approve_tx(from_token.address, router_address, amount_in))

            # Step 6: Handle native wrapping + select fee tier + quoter.
            # Use direct arithmetic (not ``compute_deadline``) here because
            # ``default_deadline_seconds`` is not validated in ``__init__`` and
            # the pre-refactor swap path silently produced ``now + N`` for any
            # ``N`` (including 0 / negative); ``compute_deadline`` would raise.
            # Preserving byte-for-byte behaviour avoids a runtime regression
            # for any deployment that intentionally or accidentally uses
            # ``default_deadline_seconds <= 0``.
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds
            value, actual_from_token, actual_to_token = self._resolve_swap_wrap_addresses(
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                warnings=warnings,
            )

            # Pre-select fee tier so the on-chain quoter is available for slippage
            # and price-impact checks. Wrapped so RPC failures degrade gracefully
            # to the oracle estimate rather than crashing compilation.
            try:
                adapter.select_fee_tier(actual_from_token, actual_to_token, amount_in)
            except Exception as exc:
                logger.warning("Fee tier pre-selection failed, falling back to oracle estimate: %s", exc)

            # Slippage + price-impact guard via shared helpers.
            slippage_or_fail = self._apply_swap_slippage_and_impact(
                intent=intent,
                oracle_estimate=expected_output,
                quoter_amount=adapter.get_quoted_amount_out(),
            )
            if isinstance(slippage_or_fail, CompilationResult):
                return slippage_or_fail
            min_output, quoted_output_for_metrics, clamped_expected = slippage_or_fail

            # VIB-3203: Human-readable expected output for the realized slippage calculation
            # performed by ResultEnricher after execution. Use the un-clamped quote
            # (``quoted_output_for_metrics``) here so the metric reflects the pool's
            # best pre-execution estimate, not the safety-lowered value used for
            # on-chain ``min_amount_out``. If we used the clamped expected the realized
            # slippage would be biased toward zero whenever the quoter's number beat the oracle's.
            expected_output_human = Decimal(str(quoted_output_for_metrics)) / Decimal(10**to_token.decimals)

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
            pool_failed = self._validate_swap_pool_after_fee_selection(
                adapter=adapter,
                protocol=protocol,
                actual_from_token=actual_from_token,
                actual_to_token=actual_to_token,
                intent_id=intent.intent_id,
            )
            if pool_failed is not None:
                return pool_failed

            # Estimate gas + assemble swap tx
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

            # Step 7: Assemble ActionBundle
            total_gas = sum_transaction_gas(transactions)

            action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_output),
                    # VIB-3203: Pre-slippage-discount quote (human-readable Decimal string).
                    # Threaded through ResultEnricher to extract_swap_amounts() for realized
                    # slippage_bps computation on the resulting on-chain receipt.
                    "expected_output_human": str(expected_output_human),
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
            expected_out_fmt = format_token_amount(clamped_expected, to_token.symbol, to_token.decimals)
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

    def _resolve_swap_tokens(self, intent: SwapIntent) -> tuple[TokenInfo, TokenInfo] | CompilationResult:
        """Resolve from/to token infos, returning a FAILED result for unknown tokens.

        Extracted in Phase 6B.3.
        """
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
        return from_token, to_token

    def _resolve_swap_amount_in(self, intent: SwapIntent, from_token: TokenInfo) -> int | CompilationResult:
        """Resolve the swap's input amount in wei, or return a FAILED result.

        Handles ``amount_usd``, ``amount`` (Decimal), the unresolved ``"all"``
        sentinel, and the missing-input case.
        """
        if intent.amount_usd is not None:
            return self._usd_to_token_amount(intent.amount_usd, from_token)
        if intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            return int(amount_decimal * Decimal(10**from_token.decimals))
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Either amount_usd or amount must be provided",
            intent_id=intent.intent_id,
        )

    def _resolve_swap_wrap_addresses(
        self,
        *,
        from_token: TokenInfo,
        to_token: TokenInfo,
        amount_in: int,
        warnings: list[str],
    ) -> tuple[int, str, str]:
        """Return ``(value, actual_from, actual_to)`` handling native wrap/unwrap.

        Appends the existing warning strings into ``warnings`` in-place so the
        observable CompilationResult.warnings list stays identical.
        """
        value = 0
        actual_from = from_token.address
        if from_token.is_native:
            value = amount_in
            actual_from = self._get_wrapped_native_address() or from_token.address
            warnings.append("Native token swap: will wrap to WETH before swapping")

        actual_to = to_token.address
        if to_token.is_native:
            actual_to = self._get_wrapped_native_address() or to_token.address
            warnings.append("Native token output: will receive WETH, unwrap separately")
        return value, actual_from, actual_to

    def _apply_swap_slippage_and_impact(
        self,
        *,
        intent: SwapIntent,
        oracle_estimate: int,
        quoter_amount: int | None,
    ) -> tuple[int, int, int] | CompilationResult:
        """Apply the price-impact guard and compute ``min_output``.

        Returns ``(min_output, quoted_for_metrics, clamped_expected)`` on success
        or a FAILED CompilationResult. Uses ``choose_safer_quote``,
        ``check_price_impact``, and ``compute_min_amount_out`` from the shared
        helper module.
        """
        # Pick the safer of oracle vs quoter as the slippage basis.
        clamped_expected, used_quoter = choose_safer_quote(oracle_estimate, quoter_amount)
        if used_quoter:
            logger.info(
                "Quoter amount (%s) is lower than price oracle estimate (%s) — "
                "using quoter amount as slippage basis for safer execution",
                quoter_amount,
                oracle_estimate,
            )

        # Price-impact guard (shared helper encapsulates the decision table).
        offline_mode = self._using_placeholders or getattr(self._config, "permission_discovery", False)
        impact = check_price_impact(
            oracle_estimate=oracle_estimate,
            quoter_amount=quoter_amount,
            intent_max_impact=intent.max_price_impact,
            config_max_impact=self._config.max_price_impact_pct,
            offline_mode=offline_mode,
            using_placeholders=self._using_placeholders,
        )
        if impact.decision is PriceImpactDecision.IMPACT_TOO_HIGH:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Price impact too high: quoter returned amount implying "
                    f"{impact.price_impact:.1%} price impact "
                    f"(oracle estimate: {oracle_estimate}, quoter: {quoter_amount}). "
                    f"Maximum allowed: {impact.effective_max_impact:.0%}. "
                    f"Likely cause: pool has insufficient liquidity for "
                    f"{intent.from_token}->{intent.to_token}."
                ),
            )
        if impact.decision is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Price impact guard: on-chain quoter returned no amount for "
                    f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity "
                    f"or price impact. Refusing to compile a swap backed only by the oracle price. "
                    f"Check RPC availability and that the pool has liquidity at the selected fee tier."
                ),
                intent_id=intent.intent_id,
            )

        # VIB-3203: the best pre-execution quote (used for realized-slippage
        # metrics) is the quoter when available, otherwise the oracle estimate.
        # This must NOT be lowered by the execution-safety clamp used for min_output.
        quoted_for_metrics = quoter_amount if quoter_amount is not None else oracle_estimate
        min_output = compute_min_amount_out(clamped_expected, intent.max_slippage)
        return min_output, quoted_for_metrics, clamped_expected

    def _validate_swap_pool_after_fee_selection(
        self,
        *,
        adapter: DefaultSwapAdapter,
        protocol: str,
        actual_from_token: str,
        actual_to_token: str,
        intent_id: str,
    ) -> CompilationResult | None:
        """Validate V3-style pool existence after fee-tier selection. Returns FAILED or None."""
        selected_fee = adapter.last_fee_selection.get("selected_fee_tier")
        if selected_fee is None:
            return None
        from .pool_validation import validate_v3_pool

        pool_check = validate_v3_pool(
            self.chain,
            protocol,
            actual_from_token,
            actual_to_token,
            selected_fee,
            self._get_chain_rpc_url(),
            gateway_client=self._gateway_client,
        )
        return self._validate_pool(pool_check, intent_id)

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
            #
            # MAX_UINT256 matches the Enso adapter (`_build_approve_transaction`)
            # and the deferred-refresh approval-rewrite path; downgrading to an
            # exact amount here would diverge from the rest of the Enso code
            # path and still wouldn't mitigate the main risk (spender change on
            # route refresh, which deferred_refresh patches explicitly).
            router_address = route_data["to"]
            if not from_token.is_native:
                approve_txs = self._build_approve_tx(
                    from_token.address,
                    router_address,
                    MAX_UINT256,
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
                tx_type="swap_deferred",
            )
            transactions.append(swap_tx)

            # Step 6: Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0

            # Calculate minimum output with slippage
            min_output = int(Decimal(str(amount_out)) * (Decimal("1") - intent.max_slippage))

            # VIB-3203: Record pre-slippage-discount quote in human units so ResultEnricher
            # can compute realized slippage_bps from the on-chain receipt.
            expected_output_human = Decimal(str(amount_out)) / Decimal(10**to_token.decimals) if amount_out else None

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(min_output),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "chain": self.chain,
                    "router": router_address,
                    "price_impact_bps": route_data.get("price_impact", 0),
                    "deferred_swap": True,
                    "route_params": {
                        "token_in": from_token.address,
                        "token_out": to_token.address,
                        "amount_in": str(amount_in),
                        "slippage_bps": slippage_bps,
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

    def _validate_lifi_chains(
        self,
        intent: SwapIntent,
        chain_mapping: dict[str, int],
    ) -> tuple[str, str, int, int, bool] | CompilationResult:
        """Resolve LiFi source/destination chains to IDs or fail closed.

        Preserves the exact error strings tested by
        ``tests/unit/intents/test_compiler_lifi.py`` (``does not support chain``).
        """
        source_chain = intent.chain or self.chain
        dest_chain = intent.destination_chain or source_chain

        for chain in (source_chain, dest_chain):
            if chain.lower() not in chain_mapping:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {chain}. Supported: {', '.join(chain_mapping.keys())}",
                    intent_id=intent.intent_id,
                )

        from_chain_id = chain_mapping[source_chain.lower()]
        to_chain_id = chain_mapping[dest_chain.lower()]
        # Compare normalised IDs rather than raw chain names: aliases or
        # casing differences (e.g. "Arbitrum" vs "arbitrum", "eth" vs
        # "ethereum") resolve to the same LiFi chain id and should not
        # take the cross-chain path.
        is_cross_chain = from_chain_id != to_chain_id
        return source_chain, dest_chain, from_chain_id, to_chain_id, is_cross_chain

    def _resolve_lifi_tokens_and_amount(
        self,
        intent: SwapIntent,
        source_chain: str,
        dest_chain: str,
    ) -> tuple[TokenInfo, TokenInfo, int] | CompilationResult:
        """Resolve LiFi source/destination tokens and input amount in wei.

        Preserves the exact error messages tested in
        ``tests/unit/intents/test_compiler_lifi.py``:
            - ``Unknown token on {chain}: {symbol}``
            - ``amount='all' must be resolved before compilation.``
            - ``Either amount_usd or amount must be provided``
        """
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
        return from_token, to_token, amount_in

    def _build_lifi_swap_transaction(
        self,
        *,
        intent: SwapIntent,
        quote: Any,
        from_token: TokenInfo,
        to_token: TokenInfo,
        amount_in: int,
        is_cross_chain: bool,
    ) -> TransactionData | CompilationResult:
        """Build the deferred swap/bridge ``TransactionData`` from a LiFi quote.

        The deferred-swap pattern (``tx_type`` = ``swap_deferred`` /
        ``bridge_deferred``) is LiFi-specific and must not be collapsed into
        the regular swap pattern — the executor re-fetches the route at
        submission time using ``route_params`` from bundle metadata.
        """
        tx_request = quote.transaction_request
        if tx_request is None or not tx_request.to or not tx_request.data:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="LiFi quote missing transaction_request data",
                intent_id=intent.intent_id,
            )

        tx_type = "bridge_deferred" if is_cross_chain else "swap_deferred"
        description_action = "Bridge" if is_cross_chain else "Swap"

        value = parse_lifi_tx_value(tx_request.value)
        gas_estimate = choose_lifi_gas_estimate(
            total_gas_estimate=(quote.estimate.total_gas_estimate if quote.estimate else 0),
            gas_limit=tx_request.gas_limit,
        )

        return TransactionData(
            to=tx_request.to,
            value=value,
            data=tx_request.data,
            gas_estimate=gas_estimate,
            description=(
                f"{description_action} via LiFi ({quote.tool}): "
                f"{self._format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> {to_token.symbol}"
            ),
            tx_type=tx_type,
        )

    @staticmethod
    def _compute_lifi_expected_output_human(amount_out: object | None, to_token: TokenInfo) -> Decimal | None:
        """Parse LiFi ``quote.get_to_amount()`` into a Decimal token amount.

        Returns ``None`` when the string isn't a positive integer, matching
        the pre-refactor ``int(amount_out) if amount_out else 0`` path.
        """
        if not amount_out:
            return None
        try:
            # LiFi returns amount_out as a numeric string; normalise via str()
            # so any repr (int, str, numeric-like) round-trips cleanly through int().
            amount_out_int = int(str(amount_out))
        except (TypeError, ValueError):
            return None
        if amount_out_int <= 0:
            return None
        return Decimal(str(amount_out_int)) / Decimal(10**to_token.decimals)

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
        from ..connectors.lifi.client import NATIVE_TOKEN_ADDRESS as LIFI_NATIVE_ADDRESS

        transactions: list[TransactionData] = []

        try:
            chain_check = self._validate_lifi_chains(intent, CHAIN_MAPPING)
            if isinstance(chain_check, CompilationResult):
                return chain_check
            source_chain, dest_chain, from_chain_id, to_chain_id, is_cross_chain = chain_check

            tokens_check = self._resolve_lifi_tokens_and_amount(intent, source_chain, dest_chain)
            if isinstance(tokens_check, CompilationResult):
                return tokens_check
            from_token, to_token, amount_in = tokens_check

            # Translate native-token sentinel (framework uses 0xEeee..., LiFi uses 0x0000...)
            lifi_from_address = LIFI_NATIVE_ADDRESS if from_token.is_native else from_token.address
            lifi_to_address = LIFI_NATIVE_ADDRESS if to_token.is_native else to_token.address

            logger.info(
                f"Getting LiFi quote: {from_token.symbol}@{source_chain} -> {to_token.symbol}@{dest_chain}, "
                f"amount={amount_in}"
            )
            adapter = LiFiAdapter(
                LiFiConfig(chain_id=from_chain_id, wallet_address=self.wallet_address),
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

            # Build approve TX if needed (skip native; LiFi gives us the exact approval target)
            approval_address = quote.estimate.approval_address if quote.estimate else ""
            if approval_address and not from_token.is_native:
                transactions.extend(self._build_approve_tx(from_token.address, approval_address, amount_in))

            swap_or_err = self._build_lifi_swap_transaction(
                intent=intent,
                quote=quote,
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                is_cross_chain=is_cross_chain,
            )
            if isinstance(swap_or_err, CompilationResult):
                return swap_or_err
            transactions.append(swap_or_err)

            amount_out = quote.get_to_amount()
            amount_out_min = quote.get_to_amount_min()
            expected_output_human = self._compute_lifi_expected_output_human(amount_out, to_token)

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(amount_out_min),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
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

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception("Failed to compile LiFi SWAP intent")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

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

            # VIB-3203: Pre-slippage-discount quote in human units for realized slippage math.
            expected_output_human = Decimal(str(amount_out)) / Decimal(10**to_token.decimals) if amount_out else None

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
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
        # Phase 6B.4: route to dedicated protocol helpers when applicable.
        # Returns the routed CompilationResult, or None if the generic
        # Uniswap-V3-style body should handle this intent.
        routed = self._dispatch_lp_open_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        return self._compile_lp_open_v3_body(intent, protocol)

    def _dispatch_lp_open_protocol_route(self, intent: LPOpenIntent) -> CompilationResult | None:
        """Route an LP_OPEN intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when the Uniswap-V3-style body in
        ``_compile_lp_open_v3_body`` should handle it.

        Extracted in Phase 6B.4 so ``_compile_lp_open`` itself stays small.
        Dispatch order is preserved from the pre-refactor method.
        """
        # Solana chains route to Solana-only adapters or fail (delegated to keep CC small).
        if self._is_solana_chain() or intent.protocol in {"meteora_dlmm", "orca_whirlpools", "raydium_clmm"}:
            return self._dispatch_lp_open_solana_route(intent)

        # Protocols with dedicated LP_OPEN compile helpers. Some are dispatched by
        # the resolved-alias name (uniswap_v4, aerodrome), others by the raw
        # ``intent.protocol`` (traderjoe_v2, aerodrome_slipstream, pendle, curve,
        # fluid). The pre-refactor method interleaved these based on subtle
        # ordering differences that tests pin; preserve them exactly.
        resolved = self._resolve_protocol(intent.protocol)

        # Uniswap V4 LP (flash accounting via PositionManager)
        if resolved == "uniswap_v4":
            return self._compile_lp_open_uniswap_v4(intent)
        # TraderJoe V2 (different architecture - bins vs ticks)
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_open_traderjoe_v2(intent)
        # Aerodrome Slipstream CL (concentrated liquidity, NFT positions)
        if intent.protocol == "aerodrome_slipstream":
            return self._compile_lp_open_aerodrome_slipstream(intent)
        # Aerodrome/Velodrome Solidly fork (fungible LP tokens).
        # Resolve alias so velodrome -> aerodrome on Optimism.
        if resolved == "aerodrome":
            return self._compile_lp_open_aerodrome(intent)
        # Pendle LP (single-token liquidity provision)
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_open(intent)
        # Curve LP (pool-based AMM with proportional liquidity)
        if intent.protocol == "curve":
            return self._compile_lp_open_curve(intent)
        # Fluid DEX LP (Arbitrum only, unencumbered positions)
        if intent.protocol == "fluid":
            return self._compile_lp_open_fluid(intent)

        return None

    def _dispatch_lp_open_solana_route(self, intent: LPOpenIntent) -> CompilationResult:
        """Solana-side LP_OPEN dispatch.

        Covers the Meteora/Orca/Raydium cases plus the explicit failure for
        other protocols on Solana chains. Always returns a CompilationResult -
        this helper is only entered when the caller has already established
        that the intent is bound for Solana.
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

        # Fail explicitly for unsupported protocols on Solana. (Only reachable
        # when ``_is_solana_chain()`` is True - the caller gates this helper.)
        allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Protocol '{intent.protocol}' is not supported for LP_OPEN on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
        )

    def _compile_lp_open_v3_body(self, intent: LPOpenIntent, protocol: str) -> CompilationResult:
        """Uniswap-V3-style LP_OPEN body shared across uniswap_v3, pancakeswap_v3, sushiswap_v3, etc.

        Phase 6B.4: extracted from ``_compile_lp_open`` onto the shared helpers
        in ``_compiler_helpers.py``. Behaviour - error messages, approval-chain
        ordering, metadata shape - is preserved byte-for-byte.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
            adapter = UniswapV3LPAdapter(self.chain, protocol)
            position_manager = adapter.get_position_manager_address()

            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown position manager for protocol {protocol} on {self.chain}"),
                    intent_id=intent.intent_id,
                )

            # Step 2: Parse pool info, normalize token order, invert ranges if needed
            resolved_pool = self._resolve_lp_pool_and_amounts(intent)
            if isinstance(resolved_pool, CompilationResult):
                return resolved_pool
            token0_info, token1_info, fee_tier, range_lower, range_upper, amount0, amount1 = resolved_pool

            # Validate pool existence (best-effort)
            from .pool_validation import validate_v3_pool

            pool_check = validate_v3_pool(
                self.chain,
                protocol,
                token0_info.address,
                token1_info.address,
                fee_tier,
                self._get_chain_rpc_url(),
                gateway_client=self._gateway_client,
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            # Step 3: Convert amounts to wei
            amount0_desired = int(amount0 * Decimal(10**token0_info.decimals))
            amount1_desired = int(amount1 * Decimal(10**token1_info.decimals))

            # Step 4: Price range -> ticks (aligned to tick spacing)
            ticks_or_fail = self._compute_lp_ticks(
                range_lower=range_lower,
                range_upper=range_upper,
                fee_tier=fee_tier,
                token0_info=token0_info,
                token1_info=token1_info,
                intent_id=intent.intent_id,
            )
            if isinstance(ticks_or_fail, CompilationResult):
                return ticks_or_fail
            tick_lower, tick_upper, tick_spacing = ticks_or_fail

            logger.debug(
                f"LP tick calculation: price_range=[{range_lower:.8f}, {range_upper:.8f}], "
                f"decimals=({token0_info.decimals}, {token1_info.decimals}), "
                f"ticks=[{tick_lower}, {tick_upper}], spacing={tick_spacing}"
            )

            # Step 4b: Align amounts to pool's current price when slot0 is available,
            # preventing "Price slippage check" reverts when oracle price diverges.
            recomputed_or_fail = self._maybe_recompute_lp_amounts_from_slot0(
                pool_check=pool_check,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                intent_id=intent.intent_id,
            )
            if isinstance(recomputed_or_fail, CompilationResult):
                return recomputed_or_fail
            amount0_desired, amount1_desired = recomputed_or_fail

            # Step 5: LP slippage-based minimums
            amount0_min, amount1_min = self._compute_lp_slippage_mins(
                intent=intent,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
            )

            # Step 6: Build approve TXs for both tokens (in token0 -> token1 order)
            self._extend_lp_approvals(
                transactions=transactions,
                token0_info=token0_info,
                token1_info=token1_info,
                position_manager=position_manager,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
            )

            # Step 7: Build mint TX. Use direct arithmetic (see swap path
            # comment) to preserve byte-for-byte behaviour for non-positive
            # ``default_deadline_seconds`` configurations.
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
            value, native_warning = self._resolve_lp_native_value(
                token0_info=token0_info,
                token1_info=token1_info,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
            )
            if native_warning:
                warnings.append(native_warning)

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

            # Step 8: Assemble ActionBundle
            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=transactions,
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

    def _resolve_lp_pool_and_amounts(
        self, intent: LPOpenIntent
    ) -> tuple[TokenInfo, TokenInfo, int, Decimal, Decimal, Decimal, Decimal] | CompilationResult:
        """Parse the pool spec and normalize token order for LP_OPEN.

        Returns ``(token0, token1, fee_tier, range_lower, range_upper, amount0, amount1)``
        with the token0-addr < token1-addr invariant enforced (ranges and
        amounts inverted as needed), or a FAILED CompilationResult.
        """
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

        return token0_info, token1_info, fee_tier, range_lower, range_upper, amount0, amount1

    def _compute_lp_ticks(
        self,
        *,
        range_lower: Decimal,
        range_upper: Decimal,
        fee_tier: int,
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        intent_id: str,
    ) -> tuple[int, int, int] | CompilationResult:
        """Convert a price range to spacing-aligned ticks. Returns FAILED on collapse."""
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
                intent_id=intent_id,
            )
        return tick_lower, tick_upper, tick_spacing

    def _maybe_recompute_lp_amounts_from_slot0(
        self,
        *,
        pool_check: "PoolValidationResult",
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        intent_id: str,
    ) -> tuple[int, int] | CompilationResult:
        """Align desired amounts to the pool's current price using ``slot0``.

        Returns the (possibly recomputed) ``(amount0, amount1)`` pair, or a
        FAILED result if recomputation yields ``(0, 0)`` from non-zero input.
        When the pool address or RPC is unavailable, returns the inputs unchanged.

        Prevents "Price slippage check" reverts when the oracle-derived ratio
        diverges from the pool's live ratio.
        """
        if not pool_check.pool_address:
            return amount0_desired, amount1_desired

        rpc_url_for_slot0 = self._get_chain_rpc_url()
        gateway_connected = self._gateway_client is not None and self._gateway_client.is_connected
        if not (rpc_url_for_slot0 or gateway_connected):
            return amount0_desired, amount1_desired

        from .lp_math import recompute_lp_amounts
        from .pool_validation import fetch_v3_pool_sqrt_price_x96

        try:
            slot0_result = fetch_v3_pool_sqrt_price_x96(
                pool_check.pool_address,
                rpc_url_for_slot0,
                chain=self.chain,
                gateway_client=self._gateway_client,
            )
        except Exception as exc:
            logger.warning(
                "LP slot0 lookup failed for pool %s; proceeding with oracle-derived amounts "
                "which may cause 'Price slippage check' revert if oracle/pool prices diverge: %s",
                pool_check.pool_address,
                exc,
            )
            slot0_result = None

        if slot0_result is None:
            return amount0_desired, amount1_desired
        sqrt_price_x96, current_tick = slot0_result
        if sqrt_price_x96 is None or sqrt_price_x96 <= 0:
            return amount0_desired, amount1_desired

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
                intent_id=intent_id,
            )
        if a0_corrected > 0 or a1_corrected > 0:
            logger.debug(
                f"LP amounts recomputed from on-chain price: "
                f"({amount0_desired}, {amount1_desired}) -> ({a0_corrected}, {a1_corrected})"
            )
            return a0_corrected, a1_corrected
        return amount0_desired, amount1_desired

    def _compute_lp_slippage_mins(
        self,
        *,
        intent: LPOpenIntent,
        amount0_desired: int,
        amount1_desired: int,
    ) -> tuple[int, int]:
        """Compute ``(amount0_min, amount1_min)`` from the effective LP slippage.

        LP slippage differs from swap slippage: in swaps slippage represents a
        real loss, while in LP it just means a different deposit ratio (no
        loss). Default 20% slippage (80% minimum), configurable to 100% (zero
        minimum) for volatile pairs. ``protocol_params.lp_slippage`` overrides
        the default.

        Uses ``compute_min_amount_out`` from the shared helper module for both
        legs so truncation behaviour matches the swap path exactly.
        """
        protocol_lp_slippage = (intent.protocol_params or {}).get("lp_slippage")
        lp_slippage = (
            min(max(Decimal(str(protocol_lp_slippage)), Decimal("0")), Decimal("1"))
            if protocol_lp_slippage is not None
            else (getattr(intent, "max_slippage", None) or self.default_lp_slippage)
        )
        amount0_min = compute_min_amount_out(amount0_desired, lp_slippage)
        amount1_min = compute_min_amount_out(amount1_desired, lp_slippage)
        logger.debug(
            f"LP mint: slippage={float(lp_slippage) * 100:.1f}%, "
            f"amount0={amount0_desired} (min={amount0_min}), "
            f"amount1={amount1_desired} (min={amount1_min})"
        )
        return amount0_min, amount1_min

    def _extend_lp_approvals(
        self,
        *,
        transactions: list[TransactionData],
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        position_manager: str,
        amount0_desired: int,
        amount1_desired: int,
    ) -> None:
        """Append approve txs for each non-native token with positive amount.

        Ordering (token0 before token1) is preserved from the pre-refactor
        method because approval-chain ordering is consensus-critical.
        """
        if amount0_desired > 0 and not token0_info.is_native:
            transactions.extend(self._build_approve_tx(token0_info.address, position_manager, amount0_desired))
        if amount1_desired > 0 and not token1_info.is_native:
            transactions.extend(self._build_approve_tx(token1_info.address, position_manager, amount1_desired))

    @staticmethod
    def _resolve_lp_native_value(
        *,
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        amount0_desired: int,
        amount1_desired: int,
    ) -> tuple[int, str | None]:
        """Return ``(value, warning)`` for the LP mint tx.

        ``value`` is the native-ETH amount to attach. ``warning`` is the
        human-facing warning string (or ``None`` when neither token is native).
        """
        if token0_info.is_native:
            return amount0_desired, "Token0 is native - sending ETH with transaction"
        if token1_info.is_native:
            return amount1_desired, "Token1 is native - sending ETH with transaction"
        return 0, None

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

    @staticmethod
    def _parse_traderjoe_v2_pool_spec(
        intent: LPOpenIntent,
    ) -> tuple[str, str, int] | CompilationResult:
        """Parse ``intent.pool`` as ``TOKEN_X/TOKEN_Y[/BIN_STEP]``.

        Defaults ``BIN_STEP`` to 20 (most common for TraderJoe V2) when
        omitted. Preserves the exact "Invalid pool format..." error string
        pinned by the LP characterization tests.
        """
        pool_parts = intent.pool.split("/")
        if len(pool_parts) < 2:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP"
                ),
                intent_id=intent.intent_id,
            )
        token_x_symbol = pool_parts[0]
        token_y_symbol = pool_parts[1]
        bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20
        return token_x_symbol, token_y_symbol, bin_step

    def _resolve_traderjoe_v2_lp_tokens(
        self,
        *,
        intent: LPOpenIntent,
        token_x_symbol: str,
        token_y_symbol: str,
    ) -> tuple[TokenInfo, TokenInfo] | CompilationResult:
        """Resolve both pool tokens or fail with the exact pinned error string.

        Error format matches the pre-refactor compiler:
        ``Unknown token {symbol} for chain {self.chain}``.
        """
        token_x_info = self._resolve_token(token_x_symbol)
        if not token_x_info:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token {token_x_symbol} for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        token_y_info = self._resolve_token(token_y_symbol)
        if not token_y_info:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token {token_y_symbol} for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        return token_x_info, token_y_info

    def _resolve_traderjoe_v2_lp_router(self, intent: LPOpenIntent) -> str | CompilationResult:
        """Return the TraderJoe V2 LP position-manager router for the chain."""
        router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
            "traderjoe_v2", "0x0000000000000000000000000000000000000000"
        )
        if router_address == "0x0000000000000000000000000000000000000000":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"TraderJoe V2 not configured for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        return router_address

    @staticmethod
    def _extract_traderjoe_v2_bin_range_params(
        intent: LPOpenIntent,
    ) -> tuple[int, int]:
        """Read ``bin_range`` / ``id_slippage`` from ``intent.protocol_params``.

        Raises ``ValueError`` (caught by the caller's generic try/except,
        surfacing as ``result.error``) when ``bin_range`` is out of range
        ``[1, 100]``. Defaults match pre-refactor behaviour (bin_range=5,
        id_slippage=5).
        """
        params = intent.protocol_params or {}
        bin_range = int(params.get("bin_range", 5))
        if bin_range < 1 or bin_range > 100:
            raise ValueError(f"bin_range must be between 1 and 100, got {bin_range}")
        id_slippage = int(params.get("id_slippage", 5))
        return bin_range, id_slippage

    @staticmethod
    def _build_traderjoe_v2_lp_open_tx_data(
        *,
        lp_tx: Any,
        intent: LPOpenIntent,
        token_x_symbol: str,
        token_y_symbol: str,
        bin_step: int,
    ) -> TransactionData:
        """Convert the adapter's add-liquidity TransactionData into compiler form."""
        return TransactionData(
            to=lp_tx.to,
            value=lp_tx.value,
            data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
            gas_estimate=lp_tx.gas or 400000,
            description=(
                f"Add liquidity to TraderJoe V2: {intent.amount0} {token_x_symbol} + "
                f"{intent.amount1} {token_y_symbol} (bin_step={bin_step})"
            ),
            tx_type="traderjoe_v2_add_liquidity",
        )

    def _build_traderjoe_v2_lp_approvals(
        self,
        *,
        token_x_info: TokenInfo,
        token_y_info: TokenInfo,
        amount_x_wei: int,
        amount_y_wei: int,
        router_address: str,
    ) -> list[TransactionData]:
        """Build ERC-20 approval TXs for both LP tokens, in X-then-Y order.

        Native tokens and zero amounts are skipped, matching pre-refactor
        behaviour. The X-before-Y ordering is load-bearing: the approval
        chain ordering is preserved across the compile -> sign -> submit
        pipeline and tests assert it.
        """
        approvals: list[TransactionData] = []
        if amount_x_wei > 0 and not token_x_info.is_native:
            approvals.extend(self._build_approve_tx(token_x_info.address, router_address, amount_x_wei))
        if amount_y_wei > 0 and not token_y_info.is_native:
            approvals.extend(self._build_approve_tx(token_y_info.address, router_address, amount_y_wei))
        return approvals

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
        transactions: list[TransactionData] = []

        try:
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            pool_spec = self._parse_traderjoe_v2_pool_spec(intent)
            if isinstance(pool_spec, CompilationResult):
                return pool_spec
            token_x_symbol, token_y_symbol, bin_step = pool_spec

            tokens = self._resolve_traderjoe_v2_lp_tokens(
                intent=intent,
                token_x_symbol=token_x_symbol,
                token_y_symbol=token_y_symbol,
            )
            if isinstance(tokens, CompilationResult):
                return tokens
            token_x_info, token_y_info = tokens
            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Resolve transport up front so pool validation AND the adapter
            # use the same gateway/RPC pair. A disconnected ``self._gateway_client``
            # would otherwise make ``validate_traderjoe_pool`` fail against a
            # stale client even though the adapter falls back to RPC.
            gateway_client, rpc_url = self._resolve_traderjoe_v2_gateway_rpc(
                adapter_name="TraderJoe V2 adapter",
            )

            # Validate pool existence (best-effort; LP_OPEN can seed empty pools).
            from .pool_validation import validate_traderjoe_pool

            pool_check = validate_traderjoe_pool(
                self.chain,
                token_x_addr,
                token_y_addr,
                bin_step,
                rpc_url,
                gateway_client=gateway_client,
                allow_empty_reserves=True,
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            amount_x_wei = int(intent.amount0 * Decimal(10**token_x_info.decimals))
            amount_y_wei = int(intent.amount1 * Decimal(10**token_y_info.decimals))

            router_or_err = self._resolve_traderjoe_v2_lp_router(intent)
            if isinstance(router_or_err, CompilationResult):
                return router_or_err
            router_address: str = router_or_err

            # Approval chain — X before Y, native/zero skipped. Ordering is
            # preserved across compile -> sign -> submit; tests assert it.
            transactions.extend(
                self._build_traderjoe_v2_lp_approvals(
                    token_x_info=token_x_info,
                    token_y_info=token_y_info,
                    amount_x_wei=amount_x_wei,
                    amount_y_wei=amount_y_wei,
                    router_address=router_address,
                )
            )
            tj_adapter = TraderJoeV2Adapter(
                TraderJoeV2Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=rpc_url,
                    gateway_client=gateway_client,
                )
            )

            bin_range, id_slippage = self._extract_traderjoe_v2_bin_range_params(intent)

            lp_tx = tj_adapter.build_add_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                amount_x=intent.amount0,
                amount_y=intent.amount1,
                bin_step=bin_step,
                bin_range=bin_range,
                id_slippage=id_slippage,
            )
            transactions.append(
                self._build_traderjoe_v2_lp_open_tx_data(
                    lp_tx=lp_tx,
                    intent=intent,
                    token_x_symbol=token_x_symbol,
                    token_y_symbol=token_y_symbol,
                    bin_step=bin_step,
                )
            )

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=transactions,
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

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_OPEN intent: {token_x_symbol}/{token_y_symbol}, "
                f"bin_step={bin_step}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_OPEN intent: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

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
        # Phase 6B backlog: route to dedicated protocol helpers when applicable.
        # Falls through to the Uniswap-V3-style body when ``None`` is returned.
        routed = self._dispatch_lp_close_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        return self._compile_lp_close_v3_body(intent, protocol)

    def _dispatch_lp_close_protocol_route(self, intent: LPCloseIntent) -> CompilationResult | None:
        """Route an LP_CLOSE intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when the generic Uniswap-V3-style body in
        ``_compile_lp_close_v3_body`` should handle it.

        Extracted in Phase 6B backlog so ``_compile_lp_close`` itself stays small.
        Dispatch order is preserved from the pre-refactor method.
        """
        # Solana chains route to Solana-only adapters or fail (delegated to keep CC small).
        if self._is_solana_chain() or intent.protocol in {"meteora_dlmm", "orca_whirlpools", "raydium_clmm"}:
            return self._dispatch_lp_close_solana_route(intent)

        # Protocols with dedicated LP_CLOSE compile helpers. Some are dispatched by
        # the resolved-alias name (uniswap_v4, aerodrome), others by the raw
        # ``intent.protocol`` (traderjoe_v2, aerodrome_slipstream, pendle, curve,
        # fluid). Preserve interleaved order from the pre-refactor method.
        resolved = self._resolve_protocol(intent.protocol)

        # Uniswap V4 LP close (flash accounting via PositionManager)
        if resolved == "uniswap_v4":
            return self._compile_lp_close_uniswap_v4(intent)
        # TraderJoe V2 (fungible LP tokens, bins not ticks)
        if intent.protocol == "traderjoe_v2":
            return self._compile_lp_close_traderjoe_v2(intent)
        # Aerodrome Slipstream CL (NFT tokenId-based, concentrated liquidity)
        if intent.protocol == "aerodrome_slipstream":
            return self._compile_lp_close_aerodrome_slipstream(intent)
        # Aerodrome/Velodrome Solidly fork (fungible LP tokens).
        # Resolve alias so velodrome -> aerodrome on Optimism.
        if resolved == "aerodrome":
            return self._compile_lp_close_aerodrome(intent)
        # Pendle LP close
        if intent.protocol == "pendle":
            return self._compile_pendle_lp_close(intent)
        # Curve LP close (pool-based AMM, proportional removal)
        if intent.protocol == "curve":
            return self._compile_lp_close_curve(intent)
        # Fluid DEX LP close (with encumbrance guard)
        if intent.protocol == "fluid":
            return self._compile_lp_close_fluid(intent)

        return None

    def _dispatch_lp_close_solana_route(self, intent: LPCloseIntent) -> CompilationResult:
        """Solana-side LP_CLOSE dispatch.

        Covers the Meteora/Orca/Raydium cases plus the explicit failure for
        other protocols on Solana chains. Always returns a CompilationResult -
        this helper is only entered when the caller has already established
        that the intent is bound for Solana.
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

        # Fail explicitly for unsupported protocols on Solana. (Only reachable
        # when ``_is_solana_chain()`` is True - the caller gates this helper.)
        allowed_solana_lp = {"raydium_clmm", "meteora_dlmm", "orca_whirlpools"}
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Protocol '{intent.protocol}' is not supported for LP_CLOSE on Solana. Supported: {', '.join(sorted(allowed_solana_lp))}",
        )

    def _compile_lp_close_v3_body(self, intent: LPCloseIntent, protocol: str) -> CompilationResult:
        """Uniswap-V3-style LP_CLOSE body shared across uniswap_v3, pancakeswap_v3, sushiswap_v3, etc.

        Phase 6B backlog: extracted from ``_compile_lp_close`` onto local
        per-step helpers. Behaviour - error messages, approval-chain ordering,
        metadata shape - is preserved byte-for-byte.
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Step 1: Get LP adapter (resolve alias e.g. "agni" -> "uniswap_v3")
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

            # Use direct arithmetic (see swap path comment) to preserve
            # byte-for-byte behaviour for non-positive
            # ``default_deadline_seconds`` configurations.
            deadline = int(datetime.now(UTC).timestamp()) + self.default_deadline_seconds

            # Step 3: Query position's on-chain state (liquidity + tokens owed)
            state_or_fail = self._query_lp_close_position_state(
                position_manager=position_manager,
                token_id=token_id,
                intent_id=intent.intent_id,
                warnings=warnings,
            )
            if isinstance(state_or_fail, CompilationResult):
                return state_or_fail
            liquidity, position_has_activity = state_or_fail

            # Step 4: Build decrease / collect / burn transactions (ordering preserved)
            self._extend_lp_close_transactions(
                transactions=transactions,
                warnings=warnings,
                adapter=adapter,
                position_manager=position_manager,
                token_id=token_id,
                liquidity=liquidity,
                position_has_activity=position_has_activity,
                collect_fees=intent.collect_fees,
                deadline=deadline,
            )

            # Step 5: Assemble ActionBundle
            total_gas = sum_transaction_gas(transactions)
            no_op = not transactions
            metadata: dict[str, Any] = {
                "position_id": intent.position_id,
                "token_id": token_id,
                "pool": intent.pool,
                "collect_fees": intent.collect_fees,
                "protocol": protocol,
                "position_manager": position_manager,
                "deadline": deadline,
                "chain": self.chain,
            }
            if no_op:
                metadata["no_op"] = True
                metadata["reason"] = f"Position #{token_id} already closed (0 liquidity, 0 tokens owed); LP_CLOSE no-op"

            action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=transactions,
                metadata=metadata,
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

    def _query_lp_close_position_state(
        self,
        *,
        position_manager: str,
        token_id: int,
        intent_id: str,
        warnings: list[str],
    ) -> tuple[int, bool] | CompilationResult:
        """Fetch position liquidity + tokens-owed and classify activity.

        Returns ``(liquidity, position_has_activity)`` on success, or a
        FAILED ``CompilationResult`` when liquidity cannot be queried. Appends
        context warnings to ``warnings`` in place to match the pre-refactor
        message ordering.

        ``position_has_activity`` is True when any of:
            - liquidity > 0, OR
            - tokens_owed is unknown (fail-open: collect anyway), OR
            - either tokens_owed leg is > 0.
        """
        liquidity = self._query_position_liquidity(position_manager, token_id)
        if liquidity is None:
            # In offline permission-discovery mode there is no RPC to query,
            # but the manifest still needs to see the full decrease + collect
            # + burn selector surface. Synthesize a non-zero liquidity so the
            # downstream builder emits all three TXs. Mirrors the same
            # permission-discovery short-circuit in compile_lp_close_aerodrome.
            if getattr(self._config, "permission_discovery", False):
                logger.debug(
                    "Permission discovery mode: using synthetic liquidity for position #%d",
                    token_id,
                )
                return 10**18, True
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Could not query liquidity for position #{token_id}. Ensure rpc_url is provided to IntentCompiler.",
                intent_id=intent_id,
            )

        tokens_owed0, tokens_owed1 = self._query_position_tokens_owed(position_manager, token_id)
        tokens_owed_unknown = tokens_owed0 is None or tokens_owed1 is None
        if tokens_owed_unknown:
            warnings.append(f"Could not query tokens owed for position #{token_id} - collecting anyway")
        elif tokens_owed0 == 0 and tokens_owed1 == 0:
            warnings.append(f"Position #{token_id} has no tokens owed pre-decrease - will still collect after close")

        # Treat unknown owed as potential activity (collect anyway to avoid
        # leaving fees uncollected).
        position_has_activity = (
            liquidity > 0
            or tokens_owed_unknown
            or (tokens_owed0 is not None and tokens_owed1 is not None and (tokens_owed0 > 0 or tokens_owed1 > 0))
        )
        return liquidity, position_has_activity

    def _extend_lp_close_transactions(
        self,
        *,
        transactions: list[TransactionData],
        warnings: list[str],
        adapter: UniswapV3LPAdapter,
        position_manager: str,
        token_id: int,
        liquidity: int,
        position_has_activity: bool,
        collect_fees: bool,
        deadline: int,
    ) -> None:
        """Append decrease / collect / burn TXs (consensus-critical ordering).

        Extends ``transactions`` and ``warnings`` in-place so the caller keeps
        ownership of the final assembly. Ordering is pinned by the
        characterization tests — do not re-order, add, or drop branches
        without updating the tests in lockstep.
        """
        # Decrease: skip on 0 liquidity, warn; else build decreaseLiquidity TX.
        if liquidity == 0:
            warnings.append(f"Position #{token_id} has 0 liquidity - skipping decreaseLiquidity step")
        else:
            # Use 0 for min amounts to ensure position can be closed.
            decrease_calldata = adapter.get_decrease_liquidity_calldata(
                token_id=token_id,
                liquidity=liquidity,
                amount0_min=0,
                amount1_min=0,
                deadline=deadline,
            )
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + decrease_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_decrease_liquidity"),
                    description=f"Decrease liquidity: position #{token_id} (remove all)",
                    tx_type="lp_decrease_liquidity",
                )
            )

        # Collect: requested AND position has activity; else emit skip-warning.
        if collect_fees and position_has_activity:
            collect_calldata = adapter.get_collect_calldata(
                token_id=token_id,
                recipient=self.wallet_address,
                amount0_max=MAX_UINT128,
                amount1_max=MAX_UINT128,
            )
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + collect_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_collect"),
                    description=f"Collect tokens and fees: position #{token_id}",
                    tx_type="lp_collect",
                )
            )
        elif collect_fees:
            warnings.append(f"Skipping collect for position #{token_id} - position appears already closed")
        else:
            warnings.append("Skipping fee collection as collect_fees=False")

        # Burn: only when position had activity - avoid reverting on
        # already-burned NFTs.
        if position_has_activity:
            burn_calldata = adapter.get_burn_calldata(token_id=token_id)
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + burn_calldata.hex(),
                    gas_estimate=get_gas_estimate(self.chain, "lp_burn"),
                    description=f"Burn position NFT: #{token_id}",
                    tx_type="lp_burn",
                )
            )
        else:
            warnings.append(f"Position #{token_id} appears already closed (0 liquidity, 0 tokens owed) - skipping burn")

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

            protocol_params = getattr(intent, "protocol_params", None) or {}
            known_bin_ids_raw = protocol_params.get("bin_ids") or []
            known_bin_ids = [int(bin_id) for bin_id in known_bin_ids_raw]

            position = None
            used_targeted_position = False
            if known_bin_ids:
                t0 = time.perf_counter()
                pool_addr = tj_adapter.sdk.get_pool_address(token_x_addr, token_y_addr, bin_step)
                balances = tj_adapter.sdk.get_position_balances_for_ids(
                    pool_addr,
                    self.wallet_address,
                    known_bin_ids,
                )
                logger.debug(
                    "TraderJoe V2 targeted balance lookup (LP_CLOSE): %.2fs",
                    time.perf_counter() - t0,
                )
                if balances:
                    from almanak.framework.connectors.traderjoe_v2 import LiquidityPosition

                    position = LiquidityPosition(
                        pool_address=pool_addr,
                        token_x=token_x_addr,
                        token_y=token_y_addr,
                        bin_step=bin_step,
                        bin_ids=list(balances.keys()),
                        balances=balances,
                        amount_x=0,
                        amount_y=0,
                        active_bin=0,
                    )
                    used_targeted_position = True

            if position is None:
                # Fall back to full discovery when the strategy did not provide
                # known bin IDs or the targeted lookup no longer finds liquidity.
                # Note: we intentionally let build_remove_liquidity_transaction
                # derive slippage-protected minimums for this path (below).
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
            # Only bypass slippage-derived minimums (pass explicit 0) when the
            # targeted lookup actually produced the position. If we fell back
            # to full discovery, let the adapter compute proper slippage mins.
            lp_tx = tj_adapter.build_remove_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
                position=position,
                amount_x_min=0 if used_targeted_position else None,
                amount_y_min=0 if used_targeted_position else None,
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

    def _compile_lp_open_aerodrome_slipstream(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Aerodrome Slipstream CL (concentrated liquidity)."""
        from .compiler_aerodrome import compile_lp_open_aerodrome_slipstream

        return compile_lp_open_aerodrome_slipstream(self, intent)

    def _compile_lp_close_aerodrome_slipstream(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Aerodrome Slipstream CL."""
        from .compiler_aerodrome import compile_lp_close_aerodrome_slipstream

        return compile_lp_close_aerodrome_slipstream(self, intent)

    def _resolve_traderjoe_v2_swap_tokens(
        self,
        intent: SwapIntent,
    ) -> tuple[TokenInfo, TokenInfo, Any, Any] | CompilationResult:
        """Resolve from/to tokens and their wrapped-for-swap equivalents.

        TraderJoe V2 LB pairs are ERC-20 only; native input/output must probe
        and swap against the wrapped token. Preserves exact error strings
        ("Unknown from_token: ...", "Unknown to_token: ...") pinned by
        ``tests/unit/intents/test_compiler_traderjoe_v2_swap.py``.

        Returns ``(from_token, to_token, swap_from_token, swap_to_token)`` on
        success or a ``CompilationResult`` (FAILED) on unknown-token. The
        swap tokens are either ``TokenInfo`` (non-native path) or
        ``ResolvedToken`` (native path via ``resolve_for_swap``); both expose
        ``.address`` which is all downstream consumers need, so the return
        type is widened to ``Any`` for the last two elements rather than
        importing ``ResolvedToken`` here.
        """
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

        swap_from_token: Any = (
            resolver.resolve_for_swap(intent.from_token, self.chain) if from_token.is_native else from_token
        )
        swap_to_token: Any = resolver.resolve_for_swap(intent.to_token, self.chain) if to_token.is_native else to_token
        return from_token, to_token, swap_from_token, swap_to_token

    def _resolve_traderjoe_v2_swap_amount(
        self,
        intent: SwapIntent,
        from_token: TokenInfo,
    ) -> Decimal | CompilationResult:
        """Resolve a SwapIntent's amount to a Decimal in token units.

        Preserves the exact error strings tested in
        ``tests/unit/intents/test_compiler_traderjoe_v2_swap.py::...::
        test_amount_all_rejected`` and the "Either amount_usd or amount must
        be provided" branch.
        """
        if intent.amount_usd is not None:
            price = self._require_token_price(from_token.symbol)
            return intent.amount_usd / price
        if intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            return intent.amount  # type: ignore[return-value]
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Either amount_usd or amount must be provided",
            intent_id=intent.intent_id,
        )

    def _resolve_traderjoe_v2_gateway_rpc(self, adapter_name: str) -> tuple["GatewayClient | None", str | None]:
        """Return ``(gateway_client, rpc_url)`` for a TraderJoe V2 adapter.

        Normalises a disconnected gateway to None and falls back to the
        chain RPC URL. Raises ``ValueError`` (caught by the caller's generic
        try/except, surfacing as ``result.error``) when neither is usable.

        The ``GatewayClient`` cast is sound because the input to
        ``normalise_gateway_or_rpc`` is already ``self._gateway_client:
        GatewayClient | None`` — the helper only narrows via
        ``is_connected``, it does not widen the type.
        """
        client, rpc_url = normalise_gateway_or_rpc(
            gateway_client=self._gateway_client,
            rpc_url_supplier=self._get_chain_rpc_url,
        )
        if client is None and not rpc_url:
            raise ValueError(
                f"Connected gateway_client or RPC URL required for {adapter_name}. "
                "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
            )
        # Cast: the helper's `object | None` return is really the same
        # `GatewayClient | None` the caller passed in.
        return cast("GatewayClient | None", client), rpc_url

    def _autodetect_traderjoe_v2_bin_step(
        self,
        *,
        intent: SwapIntent,
        tj_adapter: Any,
        swap_from_token: TokenInfo,
        swap_to_token: TokenInfo,
        from_token_symbol: str,
        to_token_symbol: str,
        pool_not_found_exc: type[BaseException],
    ) -> int | CompilationResult:
        """Auto-detect a TraderJoe V2 bin step by probing the SDK.

        Iterates common bin steps (20, 25, 15, 10, 50, 5, 100, 1) and returns
        the first one with a pool. Preserves the exact error strings pinned
        by ``test_compiler_traderjoe_v2_swap``:
            - "Failed to probe TraderJoe V2 pool for bin_step={bs}: {exc}"
            - "No TraderJoe V2 pool found for {X}/{Y} on {chain}. Tried bin
              steps: [...]. The pair may not have a Liquidity Book pool."
        """
        bin_step_order = [20, 25, 15, 10, 50, 5, 100, 1]
        found_bin_step, broken_bs, unexpected_exc = probe_traderjoe_bin_step(
            probe=tj_adapter.sdk.get_pool_address,
            token_a=swap_from_token.address,
            token_b=swap_to_token.address,
            not_found_exception=pool_not_found_exc,
            candidates=tuple(bin_step_order),
        )
        if unexpected_exc is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to probe TraderJoe V2 pool for bin_step={broken_bs}: {unexpected_exc}",
                intent_id=intent.intent_id,
            )
        if found_bin_step is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"No TraderJoe V2 pool found for {from_token_symbol}/{to_token_symbol} on {self.chain}. "
                    f"Tried bin steps: {bin_step_order}. "
                    f"The pair may not have a Liquidity Book pool."
                ),
                intent_id=intent.intent_id,
            )
        return found_bin_step

    @staticmethod
    def _fetch_traderjoe_v2_swap_quote(
        *,
        intent: SwapIntent,
        tj_adapter: Any,
        from_token_symbol: str,
        to_token_symbol: str,
        amount_decimal: Decimal,
        bin_step: int,
        pool_not_found_exc: type[BaseException],
        sdk_error_exc: type[BaseException],
    ) -> Any:
        """Fetch the Phase-B quote once (reused for both min-out and metadata).

        Returns the raw quote on success or a ``CompilationResult`` (FAILED)
        when the quote call fails or returns zero amount_out. See VIB-3203
        Phase B for the "anchor both reads to the same on-chain quote"
        rationale.
        """
        try:
            quote = tj_adapter.get_swap_quote(
                token_in=from_token_symbol,
                token_out=to_token_symbol,
                amount_in=amount_decimal,
                bin_step=bin_step,
            )
        except (pool_not_found_exc, sdk_error_exc) as quote_exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"TraderJoe V2 quote failed for {from_token_symbol} -> {to_token_symbol} "
                    f"(bin_step={bin_step}): {quote_exc}"
                ),
                intent_id=intent.intent_id,
            )

        if quote.amount_out <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"TraderJoe V2 quote returned zero amount_out for {from_token_symbol} -> "
                    f"{to_token_symbol} (bin_step={bin_step}); refusing to build swap with no "
                    f"slippage floor"
                ),
                intent_id=intent.intent_id,
            )
        return quote

    @staticmethod
    def _build_traderjoe_v2_swap_tx_data(
        *,
        swap_tx: Any,
        amount_decimal: Decimal,
        from_token_symbol: str,
        to_token_symbol: str,
        bin_step: int,
    ) -> TransactionData:
        """Convert the adapter's TransactionData into compiler TransactionData.

        Extracted so the main compile method stays small. Gas default matches
        pre-refactor behaviour (``DEFAULT_GAS_ESTIMATES["traderjoe_v2_swap"]``
        falling back to 200_000).
        """
        return TransactionData(
            to=swap_tx.to,
            value=swap_tx.value,
            data=swap_tx.data if isinstance(swap_tx.data, str) else f"0x{swap_tx.data.hex()}",
            gas_estimate=swap_tx.gas or DEFAULT_GAS_ESTIMATES.get("traderjoe_v2_swap", 200_000),
            description=(
                f"TraderJoe V2 swap: {amount_decimal} {from_token_symbol} -> {to_token_symbol} (bin_step={bin_step})"
            ),
            tx_type="traderjoe_v2_swap",
        )

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
        transactions: list[TransactionData] = []

        try:
            from almanak.core.contracts import TRADERJOE_V2 as TJ_ADDRESSES
            from almanak.framework.connectors.traderjoe_v2 import (
                TraderJoeV2Adapter,
                TraderJoeV2Config,
            )
            from almanak.framework.connectors.traderjoe_v2.sdk import (
                PoolNotFoundError as _TJPoolNotFoundError,
            )
            from almanak.framework.connectors.traderjoe_v2.sdk import (
                TraderJoeV2SDKError as _TJSDKError,
            )

            if self.chain not in TJ_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"TraderJoe V2 is not supported on {self.chain}. Supported: {list(TJ_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            tokens = self._resolve_traderjoe_v2_swap_tokens(intent)
            if isinstance(tokens, CompilationResult):
                return tokens
            from_token, to_token, swap_from_token, swap_to_token = tokens

            amount_resolution = self._resolve_traderjoe_v2_swap_amount(intent, from_token)
            if isinstance(amount_resolution, CompilationResult):
                return amount_resolution
            amount_decimal: Decimal = amount_resolution
            amount_in_wei = int(amount_decimal * Decimal(10**from_token.decimals))

            gateway_client, rpc_url = self._resolve_traderjoe_v2_gateway_rpc(
                adapter_name="TraderJoe V2 swap compilation",
            )

            router_address = TJ_ADDRESSES[self.chain]["router"]
            slippage_bps = int(intent.max_slippage * Decimal("10000"))
            tj_adapter = TraderJoeV2Adapter(
                TraderJoeV2Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=rpc_url,
                    default_slippage_bps=slippage_bps,
                    gateway_client=gateway_client,
                )
            )

            bin_step_or_err = self._autodetect_traderjoe_v2_bin_step(
                intent=intent,
                tj_adapter=tj_adapter,
                swap_from_token=swap_from_token,
                swap_to_token=swap_to_token,
                from_token_symbol=from_token.symbol,
                to_token_symbol=to_token.symbol,
                pool_not_found_exc=_TJPoolNotFoundError,
            )
            if isinstance(bin_step_or_err, CompilationResult):
                return bin_step_or_err
            bin_step: int = bin_step_or_err

            logger.info(
                "Compiling TraderJoe V2 SWAP: %s -> %s, amount=%s, bin_step=%d",
                from_token.symbol,
                to_token.symbol,
                amount_decimal,
                bin_step,
            )

            from .pool_validation import validate_traderjoe_pool

            # Use the same normalised gateway as the adapter: a disconnected
            # ``self._gateway_client`` would otherwise make validation fail
            # against a stale client while the adapter succeeds via RPC.
            pool_check = validate_traderjoe_pool(
                self.chain,
                swap_from_token.address,
                swap_to_token.address,
                bin_step,
                rpc_url,
                gateway_client=gateway_client,
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            if not from_token.is_native:
                transactions.extend(self._build_approve_tx(from_token.address, router_address, amount_in_wei))

            # VIB-3203 Phase B: quote once; re-use for both amount_out_min and metadata.
            quote_or_err = self._fetch_traderjoe_v2_swap_quote(
                intent=intent,
                tj_adapter=tj_adapter,
                from_token_symbol=from_token.symbol,
                to_token_symbol=to_token.symbol,
                amount_decimal=amount_decimal,
                bin_step=bin_step,
                pool_not_found_exc=_TJPoolNotFoundError,
                sdk_error_exc=_TJSDKError,
            )
            if isinstance(quote_or_err, CompilationResult):
                return quote_or_err
            quote = quote_or_err
            expected_output_human: Decimal = quote.amount_out

            swap_tx = tj_adapter.build_swap_transaction(
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                bin_step=bin_step,
                slippage_bps=slippage_bps,
                quote=quote,
            )
            transactions.append(
                self._build_traderjoe_v2_swap_tx_data(
                    swap_tx=swap_tx,
                    amount_decimal=amount_decimal,
                    from_token_symbol=from_token.symbol,
                    to_token_symbol=to_token.symbol,
                    bin_step=bin_step,
                )
            )

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in_wei),
                    "bin_step": bin_step,
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                    # Anchored to the same on-chain read as ``amount_out_min``.
                    # Consumed by ResultEnricher -> extract_swap_amounts for
                    # realized slippage_bps (VIB-3203).
                    "expected_output_human": str(expected_output_human),
                },
            )

            logger.info(
                "Compiled TraderJoe V2 SWAP: %s -> %s, bin_step=%d, %d txs, %d gas",
                from_token.symbol,
                to_token.symbol,
                bin_step,
                len(transactions),
                total_gas,
            )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception("Failed to compile TraderJoe V2 SWAP intent: %s", e)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

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
            # Step 1.5 (Drift): Validate the collateral token is a supported
            # Drift spot-market mint BEFORE dispatching to the Drift compiler.
            # Drift is cross-margin, so the rule is a single global allow-list
            # (see almanak.framework.connectors.drift.market_rules). An invalid
            # mint would otherwise fail opaquely at Solana submission time
            # with a Drift program error — validating up-front surfaces a
            # clean, actionable compile-time error instead.
            from ..connectors.drift.market_rules import validate_drift_collateral as _validate_drift_collateral

            try:
                _validate_drift_collateral(intent.collateral_token)
            except InvalidCollateralForMarketError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=str(exc),
                    intent_id=intent.intent_id,
                )
            return self._compile_drift_perp_open(intent)
        # Aster Perps powers PancakeSwap Perps (PCS = broker id 2 on Aster).
        # Route both protocol keys through the same compiler method, differing
        # only in the broker_id attribution we plumb to the adapter.
        if protocol == "pancakeswap_perps":
            from ..connectors.aster_perps import PCS_BROKER_ID

            _warn_pcs_perps_protocol_key_once()
            return self._compile_aster_perps_perp_open(intent, broker_id=PCS_BROKER_ID)
        if protocol == "aster_perps":
            from ..connectors.aster_perps import ASTER_BROKER_RAW

            return self._compile_aster_perps_perp_open(intent, broker_id=ASTER_BROKER_RAW)

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
                    f"{self.chain}. Supported: gmx_v2, aster_perps, pancakeswap_perps (bsc)."
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

            # Step 1.5: Validate the (market, collateral_token) pair BEFORE
            # emitting any transactions. GMX V2 silently burns keeper fees when
            # orders are submitted with collateral that is not the market's
            # longToken/shortToken. See almanak.framework.connectors.gmx_v2.market_rules
            # for the authoritative rule table.
            from ..connectors.gmx_v2.market_rules import validate_collateral as _validate_gmx_collateral

            try:
                _validate_gmx_collateral(
                    chain=self.chain,
                    market=intent.market,
                    collateral_token=intent.collateral_token,
                )
            except InvalidCollateralForMarketError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=str(exc),
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
        # PERP_CLOSE does not attribute a broker fee (the broker_id is on the
        # open payload only), but we plumb it through the adapter for symmetry
        # so the same adapter instance can close positions it did not open.
        if protocol == "pancakeswap_perps":
            from ..connectors.aster_perps import PCS_BROKER_ID

            _warn_pcs_perps_protocol_key_once()
            return self._compile_aster_perps_perp_close(intent, broker_id=PCS_BROKER_ID)
        if protocol == "aster_perps":
            from ..connectors.aster_perps import ASTER_BROKER_RAW

            return self._compile_aster_perps_perp_close(intent, broker_id=ASTER_BROKER_RAW)

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
                    f"{self.chain}. Supported: gmx_v2, aster_perps, pancakeswap_perps (bsc)."
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
    # ASTER PERPS (Aster/ApolloX Diamond on BSC; PCS = broker id 2)
    # ==========================================================================

    def _compile_aster_perps_perp_open(
        self,
        intent: PerpOpenIntent,
        *,
        broker_id: int,
    ) -> CompilationResult:
        """Compile a PERP_OPEN intent via Aster Perps (BSC, Phase 1).

        Used by both ``protocol="aster_perps"`` (broker_id=0, no attribution)
        and ``protocol="pancakeswap_perps"`` (broker_id=2, PCS attribution).
        See docs/internal/discussions/aster-dex-integration-20260418.md (PRD).

        Phase 1 limitations:
          - chain must be 'bsc'
          - market must be in ASTER_PERPS_MARKETS['bsc'] (BTC/USD, ETH/USD, BNB/USD)
          - native BNB margin (collateral_token='BNB') goes via openMarketTradeBNB (value-carrying)
          - ERC20 margin (USDT/USDC) goes via openMarketTrade (compiler prepends approve)
          - no SL/TP, no limit orders
        """
        from ..connectors.aster_perps import AsterPerpsAdapter, AsterPerpsConfig

        if self.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Aster Perps Phase 1 requires chain='bsc', got '{self.chain}'",
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
            adapter = AsterPerpsAdapter(
                AsterPerpsConfig(
                    broker_id=broker_id,
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
            )

            # Resolve mark price for the base asset (e.g. 'BTC/USD' -> 'BTC').
            # Aster trades crypto perps as synthetics; the *trading* market name
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
            from almanak.core.contracts import ASTER_PERPS_TOKENS

            from ..connectors.aster_perps.sdk import NATIVE_BNB_ADDRESS

            raw_collateral = intent.collateral_token
            # The native sentinel (address(0) via NATIVE_BNB_ADDRESS) is only honoured by
            # AsterPerpsAdapter.build_open() when spelled as a symbol ("BNB"/"NATIVE").
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

            # Venue allowlist: Aster Perps only accepts BNB (native), WBNB, USDT, USDC as
            # margin. Reject anything else at compile time so we never approve an unrelated
            # ERC-20 to the router or submit an openMarketTrade that will revert on-chain.
            # The native sentinel was already normalized to "BNB" above, so the address
            # allowlist intentionally only contains real ERC-20 margin tokens.
            supported_tokens = ASTER_PERPS_TOKENS.get(self.chain, {})
            allowed_symbols = {"BNB", "NATIVE"} | set(supported_tokens.keys())
            allowed_addresses = {addr.lower() for addr in supported_tokens.values()}
            if resolver_key.startswith("0x"):
                if resolver_key.lower() not in allowed_addresses:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        intent_id=intent.intent_id,
                        error=(
                            f"Collateral address '{intent.collateral_token}' is not a supported "
                            f"Aster Perps margin token on {self.chain}. "
                            f"Allowed: BNB (native) + {sorted(supported_tokens.keys())}."
                        ),
                    )
            elif resolver_key not in allowed_symbols:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=(
                        f"Collateral symbol '{intent.collateral_token}' is not a supported "
                        f"Aster Perps margin token on {self.chain}. "
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
                f"Compiled Aster Perps PERP_OPEN (broker_id={broker_id}): "
                f"{'LONG' if intent.is_long else 'SHORT'} "
                f"{intent.market} size=${intent.size_usd} margin={intent.collateral_amount} "
                f"{intent.collateral_token} ({len(transactions)} txs, {total_gas} gas)"
            )
        except Exception as e:
            logger.exception(f"Failed to compile Aster Perps PERP_OPEN: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_aster_perps_perp_close(
        self,
        intent: PerpCloseIntent,
        *,
        broker_id: int,
    ) -> CompilationResult:
        """Compile a PERP_CLOSE intent via Aster Perps (BSC, Phase 1).

        Used by both ``protocol="aster_perps"`` and ``protocol="pancakeswap_perps"``.
        Closes the position identified by ``intent.position_id`` (a 0x-prefixed
        bytes32 ``tradeHash``). Strategies obtain the ``tradeHash`` from the
        open receipt's ``MarketPendingTrade`` event (surfaced as
        ``result.position_id`` / ``result.extracted_data['position_id']`` by
        the receipt parser + ``ResultEnricher``) and persist it across ticks.

        Phase 1 limitations:
          - chain must be 'bsc'
          - ``closeTrade(bytes32)`` always flattens the full position; partial closes
            are NOT supported. If ``intent.size_usd`` is set, compilation fails fast
            (``CompilationStatus.FAILED``) instead of silently flattening the full
            position — callers must omit ``size_usd`` to opt into the full-close semantics.

        See ``almanak/framework/intents/perp_intents.py::PerpCloseIntent.position_id``
        for the cross-venue rationale.
        """
        from ..connectors.aster_perps import AsterPerpsAdapter, AsterPerpsConfig

        if self.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Aster Perps Phase 1 requires chain='bsc', got '{self.chain}'",
            )

        position_id = intent.position_id
        if not position_id:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "Aster Perps PERP_CLOSE requires intent.position_id (the bytes32 "
                    "tradeHash returned from the open). Strategies must persist the tradeHash "
                    "from on_intent_executed(result.position_id) after the open."
                ),
            )

        # Strict bytes32 validation for the Aster path: 0x + 64 hex chars = 66 chars total,
        # all characters must be valid hex. Generic vocabulary validation accepts any
        # shape; the venue compiler enforces length + hex-ness so malformed hashes fail
        # at compile time instead of surfacing as an opaque adapter/tx error later.
        pid_clean = position_id.lower()
        if not pid_clean.startswith("0x") or len(pid_clean) != 66:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    f"Aster Perps requires a 0x-prefixed bytes32 tradeHash "
                    f"(66 chars total). Got: '{position_id}' (len={len(position_id)})."
                ),
            )
        try:
            int(pid_clean[2:], 16)
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(f"Aster Perps requires position_id to be a valid hex bytes32 tradeHash. Got: '{position_id}'."),
            )

        # Partial closes (size_usd) are NOT representable on the closeTrade(bytes32)
        # selector — it always closes 100% of the position. Reject fast so callers asking
        # for a partial close never silently execute a full close.
        if intent.size_usd is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "Aster Perps does not support partial PERP_CLOSE via size_usd. "
                    "Omit size_usd to close the full position identified by position_id."
                ),
            )
        warnings: list[str] = []

        try:
            adapter = AsterPerpsAdapter(
                AsterPerpsConfig(
                    broker_id=broker_id,
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                )
            )
            close_tx = adapter.build_close(trade_hash=position_id)
        except Exception as e:
            logger.exception(f"Failed to build Aster Perps close transaction: {e}")
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
                "broker_id": broker_id,
            },
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.action_bundle = action_bundle
        result.transactions = [tx]
        result.total_gas_estimate = tx.gas_estimate
        result.warnings = warnings
        logger.info(
            f"Compiled Aster Perps PERP_CLOSE (broker_id={broker_id}): "
            f"tradeHash={position_id[:18]}... market={intent.market} (1 tx, {tx.gas_estimate} gas)"
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
        self._ensure_polymarket_adapter()
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
            logger.warning("PredictionBuyIntent requires a gateway-backed Polymarket client on Polygon.")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Connect the compiler to the gateway to enable prediction intents."
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
        self._ensure_polymarket_adapter()
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
            logger.warning("PredictionSellIntent requires a gateway-backed Polymarket client on Polygon.")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Connect the compiler to the gateway to enable prediction intents."
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

        self._ensure_polymarket_adapter()
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
            logger.warning("PredictionRedeemIntent requires a gateway-backed Polymarket client on Polygon.")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Connect the compiler to the gateway to enable prediction intents."
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

        Dispatches to a vault adapter registered for ``intent.protocol`` (see
        :mod:`almanak.framework.connectors.vaults`). Steps:

        1. Resolve adapter for the protocol via the vault registry.
        2. Query vault asset address and decimals.
        3. Build approve TX for the vault.
        4. Build deposit TX via the adapter SDK.
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

            if self._gateway_client is None or not self._gateway_client.is_connected:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="A connected GatewayClient is required for vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.vaults import build_vault_adapter

            adapter = build_vault_adapter(
                intent.protocol,
                chain=self.chain,
                wallet_address=self.wallet_address,
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
                description=f"Deposit {amount_decimal} {asset_token.symbol} into {intent.protocol} vault {intent.vault_address[:10]}...",
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
                f"Compiled VAULT_DEPOSIT: {amount_decimal} {asset_token.symbol} into "
                f"{intent.protocol} vault {intent.vault_address[:10]}..."
            )
            return result

        except Exception as e:
            logger.exception(f"Failed to compile VAULT_DEPOSIT intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
            return result

    def _compile_vault_redeem(self, intent: VaultRedeemIntent) -> CompilationResult:
        """Compile a VAULT_REDEEM intent into an ActionBundle.

        Dispatches to a vault adapter registered for ``intent.protocol`` (see
        :mod:`almanak.framework.connectors.vaults`). Steps:

        1. Resolve adapter for the protocol via the vault registry.
        2. If shares="all", query maxRedeem to get share count.
        3. Build redeem TX (no approve needed — redeeming own shares).
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            if self._gateway_client is None or not self._gateway_client.is_connected:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="A connected GatewayClient is required for vault compilation (on-chain reads).",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.vaults import build_vault_adapter

            adapter = build_vault_adapter(
                intent.protocol,
                chain=self.chain,
                wallet_address=self.wallet_address,
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
                description=f"Redeem {'all' if intent.shares == 'all' else intent.shares} shares from {intent.protocol} vault {intent.vault_address[:10]}...",
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

        Applies a defensive native-token cross-check against
        ``NATIVE_TOKEN_SYMBOLS``: any token whose symbol matches the chain's
        native gas-token symbol is coerced to ``is_native=True`` even when
        the underlying registry entry uses a chain-specific precompile
        address (e.g. Polygon's POL at ``0x...1010``) rather than the shared
        sentinel. This closes VIB-3135 — an unconditional ERC20 ``allowance``
        query against the precompile address because ``is_native`` was
        ``False``.

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

            is_native = resolved.is_native
            # Restrict the symbol-table override to symbol-form inputs (e.g.
            # "POL", "MATIC"). For raw address-form inputs we trust the
            # resolver verbatim — flipping is_native based on a resolved
            # symbol could mis-classify a custom ERC20 deployed at an
            # arbitrary address that happens to share a native ticker
            # (e.g. a wrapper contract symbolised "POL"), forcing it down
            # the no-allowance native path and breaking real ERC20 swaps.
            input_is_address = isinstance(token, str) and token.startswith("0x")
            if not is_native and not input_is_address:
                # Defense-in-depth: if the registry address for a chain's gas
                # token doesn't match the native sentinel (e.g. POL on
                # Polygon uses the 0x...1010 precompile address), the
                # resolver may set is_native=False even though the token IS
                # the chain's native gas token. Cross-check a local symbol
                # table to avoid ERC20-path operations (allowance, approve)
                # against addresses that aren't real ERC20s.
                #
                # This table is intentionally inlined here rather than
                # imported from ``almanak.gateway.data.balance.web3_provider``
                # to keep the compiler free of gateway-side web3 imports —
                # unit tests monkeypatch ``web3`` and that import chain
                # breaks if the resolver touches it during compile.
                #
                # Normalize aliases (``bnb`` -> ``bsc``, ``eth`` ->
                # ``ethereum``, ``avax`` -> ``avalanche``) so a caller that
                # passes a non-canonical chain name still hits the table.
                # Without this, the resolver could succeed with a chain
                # alias while this lookup misses and the ERC20 path is
                # incorrectly taken.
                lookup_chain = target_chain.lower()
                try:
                    from almanak.core.constants import resolve_chain_name

                    lookup_chain = resolve_chain_name(target_chain)
                except (ImportError, ValueError):
                    # ImportError shouldn't happen (constants is local), and
                    # ValueError means the chain is unknown — fall back to
                    # the raw lowercased name (table miss is the safe default).
                    pass
                chain_native = _CHAIN_NATIVE_SYMBOLS.get(lookup_chain, ())
                if chain_native and resolved.symbol.upper() in chain_native:
                    is_native = True

            return TokenInfo(
                symbol=resolved.symbol,
                address=resolved.address,
                decimals=resolved.decimals,
                is_native=is_native,
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

    def _expand_native_aliases_in_price_oracle(self) -> None:
        """Fill missing wrapped/native counterparts in ``self.price_oracle``.

        Rationale (VIB-3136): ``MarketSnapshot.get_price_oracle_dict()`` returns
        only the symbols the strategy actually touched — typically the native
        token (e.g. ``POL``). DEX swap adapters then ask the dict for the
        wrapped symbol (e.g. ``WPOL``, since ``resolve_for_swap`` wraps native
        tokens for routing) and silently fall back to ``Decimal("1")`` on miss,
        producing broken slippage. The compiler's ``_require_token_price`` walks
        the alias map at lookup time, but adapters that take the dict by value
        don't share that code path — so we pre-expand the dict here.

        Rule: for each ``wrapped -> native`` pair, if either side is present
        and the other is missing (or zero), copy the known price across. Never
        overwrite an existing non-zero entry.
        """
        prices = self.price_oracle
        if not prices:
            return
        for wrapped, native in self._WRAPPED_TO_NATIVE.items():
            w_price = prices.get(wrapped)
            n_price = prices.get(native)
            # Truthiness here intentionally treats Decimal(0) and None identically
            # (a zero price is as useless as a missing one). Using truthiness also
            # lets mypy narrow ``Decimal | None`` -> ``Decimal`` without ``type: ignore``.
            if w_price and not n_price:
                prices[native] = w_price
            elif n_price and not w_price:
                prices[wrapped] = n_price

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
        """Convert a price to a Uniswap V3 tick using Decimal arithmetic end-to-end.

        Uniswap V3 uses tick-based pricing where::

            price = 1.0001^tick
            adjusted_price = price / 10^(token0_decimals - token1_decimals)
            tick = floor(ln(adjusted_price) / ln(1.0001))

        Previously this conversion cast the adjusted price through ``float`` before
        taking ``math.log``. For decimal-asymmetric pairs like WETH/USDC the adjusted
        value (``price / 1e12``) fell in the narrow window where float rounding made
        the resulting ``math.floor`` non-deterministic at tick-spacing boundaries,
        producing different ticks for mathematically equivalent Decimal inputs and
        silently shifting multi-million-dollar LP ranges. We compute the logarithm
        with ``Decimal.ln()`` at 50-digit precision instead.

        Args:
            price: Price in nominal units (token1 per token0), must be positive.
            token0_decimals: Decimals of token0.
            token1_decimals: Decimals of token1.

        Returns:
            The tick value (rounded down), clamped to the Uniswap V3 valid range.

        Raises:
            ValueError: If price is zero or negative.
        """
        from decimal import Decimal as _Decimal
        from decimal import localcontext

        if price <= 0:
            raise ValueError("Price must be positive")

        price_dec = price if isinstance(price, _Decimal) else _Decimal(str(price))

        # 50 digits exceeds any realistic Uniswap V3 price magnitude (|tick| <= 887272,
        # price range >250 orders of magnitude) so floor(ln / ln(1.0001)) is invariant
        # under further precision increases.
        with localcontext() as ctx:
            ctx.prec = 50

            decimal_diff = token0_decimals - token1_decimals
            if decimal_diff >= 0:
                adjusted_price = price_dec / (_Decimal(10) ** decimal_diff)
            else:
                adjusted_price = price_dec * (_Decimal(10) ** (-decimal_diff))

            if adjusted_price <= 0:
                return IntentCompiler.UNISWAP_MIN_TICK

            ratio = adjusted_price.ln() / _Decimal("1.0001").ln()

            # Decimal.__floor__ truncates toward negative infinity — matches math.floor semantics.
            import math

            tick = math.floor(ratio)

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
