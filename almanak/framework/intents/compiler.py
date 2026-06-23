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
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

import grpc

from almanak.config.cli_runtime import anvil_port_for_chain
from almanak.connectors._strategy_base import concentrated_liquidity_math as cl_math
from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseConcentratedLiquidityCompiler,
    BaseProtocolCompiler,
    CLAdapterFactoryContext,
    CLCompilerContext,
    PerpCompilerContext,
    SwapCompilerContext,
)
from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter
from almanak.connectors._strategy_base.compiler_registry import get_compiler as get_connector_compiler
from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason
from almanak.core.chains import DEFAULT_CHAIN
from almanak.core.chains._helpers import is_solana_chain

from ..chain_family import ChainFamilyAdapter, all_families, family_for

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
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)

if TYPE_CHECKING:
    from web3 import Web3

    from almanak.connectors._strategy_base.pool_validation_base import PoolValidationResult

    from ..data.tokens import TokenResolver as TokenResolverType
    from ..gateway_client import GatewayClient
    from .bridge import BridgeIntent
    from .vocabulary import UnwrapNativeIntent, WrapNativeIntent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ConnectorCompilerServices:
    """Bound framework services exposed to connector-owned compilers."""

    compiler: "IntentCompiler"

    def resolve_token(self, token: str, chain: str | None = None) -> "TokenInfo | None":
        if chain is None:
            return self.compiler._resolve_token(token)
        return self.compiler._resolve_token(token, chain=chain)

    def resolve_dest_wallet(self, dest_chain: str) -> str:
        return self.compiler._resolve_dest_wallet(dest_chain)

    def require_token_price(self, symbol: str) -> Decimal:
        return self.compiler._require_token_price(symbol)

    def usd_to_token_amount(self, usd_amount: Decimal, token: "TokenInfo") -> int:
        return self.compiler._usd_to_token_amount(usd_amount, token)

    def calculate_expected_output(self, amount_in: int, from_token: "TokenInfo", to_token: "TokenInfo") -> int:
        return self.compiler._calculate_expected_output(amount_in, from_token, to_token)

    def build_approve_tx(self, token_address: str, spender: str, amount: int) -> list["TransactionData"]:
        return self.compiler._build_approve_tx(token_address, spender, amount)

    def get_chain_rpc_url(self) -> str | None:
        return self.compiler._get_chain_rpc_url()

    def validate_pool(self, result: "PoolValidationResult", intent_id: str) -> "CompilationResult | None":
        return self.compiler._validate_pool(result, intent_id)

    def format_amount(self, amount: int, decimals: int) -> str:
        return self.compiler._format_amount(amount, decimals)

    def parse_pool_info(self, pool: str) -> tuple["TokenInfo", "TokenInfo", int, bool] | None:
        return self.compiler._parse_pool_info(pool)

    def price_to_tick(self, price: Decimal, *, token0_decimals: int, token1_decimals: int) -> int:
        return self.compiler._price_to_tick(
            price,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
        )

    def get_tick_spacing(self, fee_tier: int) -> int:
        return self.compiler._get_tick_spacing(fee_tier)

    def get_wrapped_native_address(self) -> str | None:
        return self.compiler._get_wrapped_native_address()

    def query_position_liquidity(self, position_manager: str, token_id: int) -> int | None:
        return self.compiler._query_position_liquidity(position_manager, token_id)

    def query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]:
        return self.compiler._query_position_tokens_owed(position_manager, token_id)

    def query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        return self.compiler._query_erc20_balance(token_address, wallet_address)

    def query_erc20_balance_for_chain(self, token_address: str, wallet_address: str, chain: str) -> int | None:
        return self.compiler._query_erc20_balance_for_chain(token_address, wallet_address, chain)

    def query_native_balance_for_chain(self, wallet_address: str, chain: str) -> int | None:
        return self.compiler._query_native_balance_for_chain(wallet_address, chain)

    def eth_call(self, to: str, data: str, *, chain: str | None = None) -> str | None:
        return self.compiler._eth_call(to, data, chain=chain)

    def default_swap_adapter(self, protocol: str) -> DefaultSwapAdapter:
        """Construct a ``DefaultSwapAdapter`` for a V3 pre-swap leg.

        Exposes the pre-swap capability to non-CL connectors (Pendle today)
        without forcing them to depend on ``CLCompilerContext``-only fields
        (``swap_pool_selection_mode`` / ``fixed_swap_fee_tier``). Mirrors the
        adapter construction the CL connectors do inline.
        """
        config = self.compiler._config
        return DefaultSwapAdapter(
            chain=self.compiler.chain,
            protocol=protocol,
            pool_selection_mode=config.swap_pool_selection_mode,
            fixed_fee_tier=config.fixed_swap_fee_tier,
            rpc_url=self.compiler._get_chain_rpc_url(),
            rpc_timeout=self.compiler.rpc_timeout,
            gateway_client=self.compiler._gateway_client,
        )


def _bridge_registry_protocol(intent: "BridgeIntent") -> str:
    """Return the bridge compiler registry key for a BRIDGE intent."""
    preferred = getattr(intent, "preferred_bridge", None)
    if preferred and get_connector_compiler(preferred) is not None:
        return preferred
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

    default = CompilerRegistry.default_protocol("BRIDGE")
    # The registry is the single source of truth for which bridge is the
    # fallback. If no default is configured something is structurally wrong
    # (the dispatch key disappeared) — fail loud rather than silently swap
    # in a stale string. ``RuntimeError`` (not ``assert``) so the check
    # survives ``python -O``.
    if default is None:
        raise RuntimeError("CompilerRegistry missing BRIDGE default")
    return default


# =============================================================================
# Extracted modules — re-exported for backward compatibility
# (all symbols that were importable from this module remain importable)
# =============================================================================

from ._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_safer_quote,
    compute_min_amount_out,
    sum_transaction_gas,
)
from .compiler_adapters import (  # noqa: F401
    AaveV3Adapter,
    BalancerAdapter,
    LendingProtocolAdapter,
    SwapProtocolAdapter,
)
from .compiler_constants import (  # noqa: F401
    AAVE_BORROW_SELECTOR,
    AAVE_COMPATIBLE_PROTOCOLS,
    AAVE_FLASH_LOAN_SELECTOR,
    AAVE_FLASH_LOAN_SIMPLE_SELECTOR,
    AAVE_REPAY_SELECTOR,
    AAVE_SET_COLLATERAL_SELECTOR,
    AAVE_SUPPLY_SELECTOR,
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
from .compiler_queries import (  # noqa: F401
    _CHAIN_NATIVE_SYMBOLS,
    CompilerQueries,
    _is_solana_mint,
)
from .compiler_queries import (
    format_amount as _qformat_amount,
)
from .compiler_queries import (
    get_placeholder_prices as _qget_placeholder_prices,
)
from .compiler_queries import (
    get_tick_spacing as _qget_tick_spacing,
)
from .compiler_queries import (
    price_to_tick as _qprice_to_tick,
)
from .compiler_queries import (
    tick_to_price as _qtick_to_price,
)

# =============================================================================
# Native-token symbol table (VIB-3135)
# =============================================================================
# _CHAIN_NATIVE_SYMBOLS and _is_solana_mint are re-exported from compiler_queries
# (the import block above marks them noqa so ruff keeps the re-export) so that
# existing importers continue to work:
#   from almanak.framework.intents.compiler import _CHAIN_NATIVE_SYMBOLS
# Used by: teardown.oracle_warmup, permissions.synthetic_intents,
#           tests/unit/intents/test_compiler_solana_native_vib_3816.py


def _normalize_wallet_address(addr: str) -> str:
    """Return the EIP-55 checksum form of an EVM address; pass other forms through.

    Why: web3.py rejects non-checksum addresses inside ``get_transaction_count``
    and ``contract.functions.X(...).build_transaction({"from": ...})``. Storing
    the verbatim string at the IntentCompiler boundary leaks the case-validation
    burden into every connector SDK and crashes when a wallet's lowercase form
    is not EIP-55 valid (VIB-3961, prod 2026-05-04 Aerodrome CL LP_OPEN).

    EVM signal is the ``0x`` prefix — Solana base58 pubkeys never start with 0x.
    A malformed hex address ("0xINVALID") raises ``ValueError`` from web3 here,
    which is the correct fail-fast behaviour at a system boundary.
    """
    if isinstance(addr, str) and addr.startswith("0x"):
        from web3 import Web3

        return Web3.to_checksum_address(addr)
    return addr


# =============================================================================
# Intent Compiler
# =============================================================================


# Sentinel for the LP_OPEN slot0 dedup path (VIB-3823 follow-up).
# ``None`` is a valid post-fetch value meaning "fetch attempted, missed";
# this sentinel preserves the legacy "argument not supplied — please fetch"
# semantic so the helper does not double-fetch on transient slot0 failures.
_SLOT0_NOT_FETCHED: Any = object()


# =============================================================================
# P0 placeholder fail-fast (VIB-4165 / VIB-4160 T5) — Hard Ratification
# Condition #5 of the primitives refactor.
#
# These five ``IntentType`` values exist so that future code paths (LLM tool
# calls, strategy templates, the agent_tools PolicyEngine) cannot silently
# smuggle CDP / liquidation / stablecoin-mint operations through generic
# BORROW / REPAY / SUPPLY and pollute lending accounting before the real
# connectors land in P1. The compiler MUST refuse to compile any of these.
#
# The set is the single canonical source of truth — adding or removing a
# placeholder is a one-line change here. ``_raise_if_placeholder_intent`` is
# called from ``IntentCompiler.compile`` immediately after ``intent_type`` is
# resolved.
#
# Anti-drift contract: ``tests/unit/intents/test_placeholder_compilers.py``
# asserts (a) the set equals exactly these 5 values, (b) the helper raises on
# each, (c) the helper does NOT raise on any of the 24 real intent types, (d)
# every placeholder has a TAXONOMY row, (e) ``IntentCompiler.compile`` still
# references the helper by name. Without (e), the helper could exist in
# isolation while ``compile`` silently skips it — that is the literal
# silent-failure mode this guard was created to prevent.
# =============================================================================

_PLACEHOLDER_INTENT_TYPES: frozenset[IntentType] = frozenset(
    {
        IntentType.LIQUIDATE,
        IntentType.OPEN_CDP,
        IntentType.MINT_STABLE,
        IntentType.REPAY_STABLE,
        IntentType.CLOSE_CDP,
    }
)


def _raise_if_placeholder_intent(intent_type: IntentType) -> None:
    """Fail-fast guard for P0 placeholder ``IntentType`` values.

    Args:
        intent_type: The intent type to check.

    Raises:
        NotImplementedError: if ``intent_type`` is one of the 5 P0 placeholders
            declared by VIB-4165 (locked design item #5 of the primitives
            refactor PRD). The error message names the offending value so the
            caller can locate it without reading the helper source.
    """
    if intent_type in _PLACEHOLDER_INTENT_TYPES:
        raise NotImplementedError(
            f"IntentType {intent_type.value!r} is a P0 placeholder declared "
            f"by VIB-4160 (primitives refactor, locked design item #5). "
            f"Compilation is intentionally disabled until the real connector "
            f"lands in P1. See "
            f"docs/internal/discussions/primitives-refactor-20260508.md §5."
        )


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

    @property
    def _queries(self) -> CompilerQueries:
        """Lazy-init collaborator for read-only query helpers (plan 016).

        Using a property instead of ``__init__``-assignment lets tests that
        bypass ``__init__`` via ``IntentCompiler.__new__(IntentCompiler)``
        still call the delegating wrappers without an AttributeError. The
        collaborator holds a live reference to ``self`` and reads all state
        at call time, so lazy creation is safe.
        """
        try:
            return self.__dict__["_queries_cache"]
        except KeyError:
            q = CompilerQueries(self)
            self.__dict__["_queries_cache"] = q
            return q

    def __init__(
        self,
        chain: str = DEFAULT_CHAIN,
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
        # VIB-4803: resolve the ChainFamily behavior adapter once at construction.
        # Every per-intent dispatch site reads ``self._family`` instead of running
        # an ad-hoc ``chain == "solana"`` test, so the family is a real seam: a
        # hypothetical MoveFamily lands as one new adapter class without further
        # edits in this file.
        self._family: ChainFamilyAdapter = family_for(self.chain)
        # Checksum EVM wallet addresses at the boundary so every downstream
        # consumer (web3 build_transaction, get_transaction_count, balance
        # queries, bridge from_address) sees an EIP-55 string. Solana base58
        # pubkeys do not start with 0x and pass through unchanged.
        self.wallet_address = _normalize_wallet_address(wallet_address)
        # Normalize protocol alias (e.g., "agni" -> "agni_finance" on mantle)
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        self.default_protocol = normalize_protocol(self.chain, default_protocol)
        self.default_deadline_seconds = default_deadline_seconds
        self.rpc_url = rpc_url
        self.rpc_timeout = rpc_timeout
        self._web3: Web3 | None = None
        self._gateway_client = gateway_client
        # Lowercase chain keys at construction so lookups via ``dest_chain.lower()``
        # match — otherwise a caller passing e.g. ``{"Base": "0x..."}`` would silently
        # fall through to ``self.wallet_address`` and misroute a bridge destination.
        self._chain_wallets = (
            {k.strip().lower(): _normalize_wallet_address(v) for k, v in chain_wallets.items()}
            if chain_wallets
            else None
        )

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
        self._connector_compiler_cache: dict[str, Any] = {}
        # Log stablecoin price fallbacks once per symbol per compiler instance.
        self._stablecoin_fallback_logged: set[str] = set()

        effective_protocol = self._family.default_swap_protocol() or default_protocol
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
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        return normalize_protocol(self.chain, intent_protocol)

    def _base_compiler_context_kwargs(self, *, resolve_rpc_url: bool = True) -> dict[str, Any]:
        """Build common connector compiler context fields."""
        config = getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))

        return {
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "rpc_url": self._get_chain_rpc_url() if resolve_rpc_url else getattr(self, "rpc_url", None),
            "rpc_timeout": getattr(self, "rpc_timeout", 10.0),
            "permission_discovery": getattr(config, "permission_discovery", False),
            "allow_placeholder_prices": config.allow_placeholder_prices,
            "token_resolver": getattr(self, "_token_resolver", None),
            "gateway_client": getattr(self, "_gateway_client", None),
            "price_oracle": getattr(self, "price_oracle", None),
            "cache": getattr(self, "_connector_compiler_cache", {}),
            "services": _ConnectorCompilerServices(self),
            "default_protocol": getattr(self, "default_protocol", ""),
            # Universal tx concept (lending/perp/bridge also need a deadline
            # when those connectors get folded). Swap-specific knobs
            # (max_price_impact_pct, using_placeholders) live on
            # SwapCompilerContext — see _swap_compiler_context_kwargs.
            "default_deadline_seconds": self.default_deadline_seconds,
        }

    def _swap_compiler_context_kwargs(self) -> dict[str, Any]:
        """Build kwargs for a swap-pipeline context (base + swap-specific knobs).

        Used by both :class:`SwapCompilerContext` and :class:`CLCompilerContext`
        construction. Lending/perp/bridge compilers that don't compile swaps
        should NOT call this — they construct ``BaseCompilerContext`` directly.
        """
        config = getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))
        return {
            **self._base_compiler_context_kwargs(),
            "max_price_impact_pct": config.max_price_impact_pct,
            "using_placeholders": getattr(self, "_using_placeholders", False),
        }

    def _build_compiler_context(self, protocol: str, connector_compiler: BaseProtocolCompiler) -> BaseCompilerContext:
        """Build the context type requested by a connector compiler.

        Order matters: ``CLCompilerContext`` is a subclass of
        ``SwapCompilerContext`` which is a subclass of
        ``BaseCompilerContext``, so the most-specific check comes first.
        """
        context_type = getattr(connector_compiler, "context_type", BaseCompilerContext)
        if issubclass(context_type, CLCompilerContext):
            if not isinstance(connector_compiler, BaseConcentratedLiquidityCompiler):
                raise TypeError(
                    f"Connector compiler {type(connector_compiler).__name__} declares CLCompilerContext "
                    "but is not a BaseConcentratedLiquidityCompiler"
                )
            return self._build_cl_compiler_context(protocol, connector_compiler)
        if issubclass(context_type, PerpCompilerContext):
            return PerpCompilerContext(**self._base_compiler_context_kwargs(resolve_rpc_url=False), protocol=protocol)
        if issubclass(context_type, SwapCompilerContext):
            return SwapCompilerContext(**self._swap_compiler_context_kwargs())
        # Solana-only connectors (Meteora DLMM, Orca Whirlpools, Raydium CLMM) hold
        # their own Solana RPC client — they do NOT route through the gateway.
        # ``_get_chain_rpc_url()`` returns ``None`` when a gateway client is
        # connected, which would silently strand these adapters with an empty
        # RPC URL. Force the raw ``self.rpc_url`` pathway for them. VIB-4121.
        resolve_rpc_url = not self._is_solana_only_connector(connector_compiler)
        if context_type is not BaseCompilerContext:
            return context_type(**self._base_compiler_context_kwargs(resolve_rpc_url=resolve_rpc_url))
        return BaseCompilerContext(**self._base_compiler_context_kwargs(resolve_rpc_url=resolve_rpc_url))

    @staticmethod
    def _is_solana_only_connector(connector_compiler: BaseProtocolCompiler) -> bool:
        """Return True when the connector compiler is registered only for Solana.

        Defensive against test doubles that don't expose a real iterable
        ``chains`` classvar — we only opt into the raw-RPC pathway when we
        can prove the connector restricts itself to Solana.
        """
        chains = getattr(connector_compiler, "chains", None)
        if not isinstance(chains, frozenset | set | tuple | list):
            return False
        chains_set = frozenset(chains)
        return bool(chains_set) and all(is_solana_chain(c) for c in chains_set)

    def _build_cl_compiler_context(
        self,
        protocol: str,
        connector_compiler: BaseConcentratedLiquidityCompiler,
    ) -> CLCompilerContext:
        """Build the connector compiler context for concentrated-liquidity protocols."""
        config = getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))
        factory_context = CLAdapterFactoryContext(
            chain=self.chain,
            rpc_url=self._get_chain_rpc_url(),
            rpc_timeout=getattr(self, "rpc_timeout", 10.0),
            gateway_client=getattr(self, "_gateway_client", None),
            swap_pool_selection_mode=config.swap_pool_selection_mode,
            fixed_swap_fee_tier=config.fixed_swap_fee_tier,
            default_swap_adapter_cls=DefaultSwapAdapter,
        )

        return CLCompilerContext(
            **self._swap_compiler_context_kwargs(),
            protocol=protocol,
            default_swap_adapter_factory=connector_compiler.build_default_swap_adapter_factory(factory_context),
            lp_adapter_factory=connector_compiler.build_lp_adapter_factory(factory_context),
            swap_pool_selection_mode=config.swap_pool_selection_mode,
            fixed_swap_fee_tier=config.fixed_swap_fee_tier,
            default_lp_slippage=self.default_lp_slippage,
        )

    def _get_chain_rpc_url(self) -> str | None:
        """Get RPC URL for the current chain.

        When a connected gateway client is available, return ``None``: every
        downstream consumer (``validate_v3_pool``, the pool slot0 fetch, adapter
        configs) routes eth_calls through the gateway, so resolving a direct RPC
        URL here is dead weight. It also emits the misleading "free public RPC"
        log noise that has confused Infra into believing the strategy container
        has a network bypass — the strategy container holds no RPC credentials
        by design (gateway-boundary rule), so resolution always falls through to
        public RPC even when the gateway is correctly configured. See VIB-4429.

        Otherwise: if rpc_url is set on the compiler, use it. Otherwise, check if
        a managed Anvil fork is running (via ANVIL_{CHAIN}_PORT env var set by
        managed.py), and use that. Finally, fall back to the gateway's RPC
        provider.

        This is needed for protocol adapters (like Aerodrome, TraderJoe, Pendle)
        that need to make direct RPC calls for pool queries when the compiler is
        running without a connected gateway (local dev / Anvil).

        Returns:
            RPC URL string or None if not available.
        """
        # Gateway-first: a connected gateway client means every consumer routes
        # eth_calls through it. The condition mirrors pool_validation._eth_call's
        # own gateway-vs-direct decision so the two stay consistent.
        if self._gateway_client is not None and getattr(self._gateway_client, "is_connected", False):
            return None

        if self.rpc_url:
            return self.rpc_url

        # Check if a managed Anvil fork is running for this chain.
        # managed.py sets ANVIL_{CHAIN}_PORT when it starts an Anvil fork.
        # This MUST take priority over mainnet RPC so that protocol adapters
        # (e.g., TraderJoe, Aerodrome) query on-chain state from the fork
        # where LP positions actually exist, not mainnet.
        anvil_port = anvil_port_for_chain(self.chain)
        if anvil_port:
            anvil_url = f"http://127.0.0.1:{anvil_port}"
            logger.debug(
                f"Anvil fork detected for {self.chain} (ANVIL_{self.chain.upper()}_PORT={anvil_port}), "
                f"using fork URL: {anvil_url}"
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

    # crap-allowlist: VIB-4222 — pre-existing primitive-dispatch ladder
    # (cc=31, the SWAP/LP_OPEN/LP_CLOSE/.../UNWRAP_NATIVE if/elif chain). T5
    # (VIB-4165) only added a 1-line `_raise_if_placeholder_intent` call plus
    # an 8-line comment ABOVE the dispatch — zero new branches, zero new
    # control flow. The function was already over threshold on main; the
    # registry-pattern refactor that decomposes this ladder is tracked under
    # VIB-4222.
    def compile(self, intent: AnyIntent) -> CompilationResult:  # noqa: C901
        """Compile an intent into an ActionBundle.

        This is the main entry point for compiling intents. It dispatches
        to the appropriate handler based on intent type.

        Args:
            intent: The intent to compile

        Returns:
            CompilationResult with ActionBundle and metadata
        """
        # VIB-4165 fail-fast: refuse to compile P0 placeholder intent types
        # BEFORE entering the outer try/except below. The outer block catches
        # ``Exception`` and converts errors to ``CompilationResult.FAILED`` —
        # if the placeholder check ran inside it, ``NotImplementedError`` would
        # be silently swallowed and the placeholder would compile to a FAILED
        # result rather than raise. That is exactly the silent-failure mode
        # HRC-5 of the primitives refactor PRD was created to prevent.
        _raise_if_placeholder_intent(intent.intent_type)

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
                return self._compile_perp_via_registry(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PERP_CLOSE:
                return self._compile_perp_via_registry(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.HOLD:
                return self._compile_hold(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.FLASH_LOAN:
                return self._compile_flash_loan(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.STAKE:
                return self._compile_staking_via_registry(intent, "STAKE")
            elif intent_type == IntentType.UNSTAKE:
                return self._compile_staking_via_registry(intent, "UNSTAKE")
            elif intent_type == IntentType.PREDICTION_BUY:
                return self._compile_prediction_via_registry(intent)
            elif intent_type == IntentType.PREDICTION_SELL:
                return self._compile_prediction_via_registry(intent)
            elif intent_type == IntentType.PREDICTION_REDEEM:
                return self._compile_prediction_via_registry(intent)
            elif intent_type == IntentType.BRIDGE:
                bridge_protocol = _bridge_registry_protocol(intent)  # type: ignore[arg-type]
                connector_compiler = get_connector_compiler(bridge_protocol)
                if connector_compiler is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"No connector compiler registered for bridge protocol {bridge_protocol}",
                        intent_id=intent.intent_id,
                    )
                return connector_compiler.compile(
                    self._build_compiler_context(bridge_protocol, connector_compiler),
                    intent,
                )
            elif intent_type == IntentType.VAULT_DEPOSIT:
                return self._compile_vault_via_registry(intent)
            elif intent_type == IntentType.VAULT_REDEEM:
                return self._compile_vault_via_registry(intent)
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

    # =========================================================================
    # ChainFamily dispatch (VIB-4803)
    #
    # Solana SWAP / LP / lending compilation is owned by
    # :class:`SvmFamily.compile_intent` (almanak.framework.chain_family). The
    # historical ``_compile_jupiter_swap`` / ``_get_jupiter_adapter`` etc.
    # wrapper methods on this class have been removed - they were thin
    # delegates, and dispatch now goes through :meth:`_family_compile_intent`
    # below.
    #
    # Jupiter swap lives in ``connectors.jupiter.compiler.JupiterCompiler``.
    # Per-protocol Solana LP compilation (Meteora / Orca / Raydium) is owned
    # by the per-connector compilers (#2416) and dispatched via
    # :data:`CompilerRegistry`. Direct unit tests should target the
    # connector compilers' ``compile()`` methods, or test dispatch through
    # ``IntentCompiler.compile``.
    # =========================================================================

    def _family_compile_intent(self, intent: AnyIntent) -> CompilationResult | None:
        """Ask every registered :class:`ChainFamilyAdapter` if it owns ``intent``.

        Returns the first non-None response. Used by the per-intent dispatch
        helpers (``_dispatch_swap_protocol_route`` /
        ``_dispatch_lp_open_protocol_route`` / ``_dispatch_lp_close_protocol_route``)
        to fold the historical ``self._is_solana_chain()`` branches into one
        polymorphic call.

        Iterating every family (instead of only ``self._family``) catches the
        cross-chain edge case where an SVM-only protocol (``meteora_dlmm`` /
        ``orca_whirlpools`` / ``raydium_clmm``) is submitted against an EVM
        chain — SvmFamily returns an explicit "supported only on Solana"
        :class:`CompilationResult` rather than letting the intent fall
        through to the generic "unsupported protocol on <chain>" error.
        """
        for family in all_families():
            result = family.compile_intent(self, intent)
            if result is not None:
                return result
        return None

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
        routed = self._dispatch_swap_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)
        return self._compile_default_router_swap_body(intent, protocol)

    def _dispatch_swap_protocol_route(self, intent: SwapIntent) -> CompilationResult | None:
        """Route a SWAP intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when connector/default-router dispatch should
        handle it.

        Extracted in Phase 6B.3 so ``_compile_swap`` itself stays small.

        VIB-4803: ask every registered ChainFamily whether it owns the
        intent (in declaration order). The first non-None response wins.
        This keeps protocol-level routing (e.g. Solana-only protocols
        rejected on EVM chains with an explicit error) inside the family
        adapters, not the compiler. EvmFamily currently returns None for
        every intent — its dispatch is the default fall-through path.
        """
        family_result = self._family_compile_intent(intent)
        if family_result is not None:
            return family_result

        # Check for cross-chain swap - route to appropriate aggregator.
        # Preserve historical behavior: protocol=None defaults to the registry's
        # cross-chain aggregator. An explicit ``lifi`` override is honoured;
        # other protocol names fall through to the default. The strings live
        # in ``CompilerRegistry`` so framework code stays connector-agnostic.
        if intent.is_cross_chain:
            from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

            aggregator_protocol = CompilerRegistry.default_protocol("SWAP_CROSS_CHAIN")
            if aggregator_protocol is None:
                raise RuntimeError("CompilerRegistry missing SWAP_CROSS_CHAIN default")
            if intent.protocol is not None:
                from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

                # Historical: only ``lifi`` overrides the default. Other
                # protocol names on a cross-chain intent fall through to the
                # default aggregator (preserves behaviour pre-VIB-4818).
                if normalize_protocol(self.chain, intent.protocol) == "lifi":
                    aggregator_protocol = "lifi"
            connector_compiler = get_connector_compiler(aggregator_protocol)
            if connector_compiler is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Connector compiler for protocol '{aggregator_protocol}' is not registered.",
                )
            return connector_compiler.compile(
                self._build_compiler_context(aggregator_protocol, connector_compiler), intent
            )

        # Connector-owned inference runs before default-protocol dispatch.
        # Example: a connector can claim synthetic token symbols that should not
        # be routed to the user's default DEX.
        if intent.protocol is None:
            from almanak.connectors._strategy_swap_route_inference_registry import SWAP_ROUTE_INFERENCE_REGISTRY

            inferred_protocol = SWAP_ROUTE_INFERENCE_REGISTRY.infer_protocol(intent)
            if inferred_protocol is None:
                return None
            connector_compiler = get_connector_compiler(inferred_protocol)
            if connector_compiler is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Connector compiler for inferred protocol '{inferred_protocol}' is not registered.",
                )
            return connector_compiler.compile(
                self._build_compiler_context(inferred_protocol, connector_compiler), intent
            )

        return None

    def _compile_default_router_swap_body(self, intent: SwapIntent, protocol: str) -> CompilationResult:
        """Compile non-folded default-router swaps such as uniswap_v2/sushiswap/camelot.

        Uniswap-V3-family protocols are connector-owned and must not reach this
        fallback. This path exists only for older router-backed swap protocols
        that still use ``DefaultSwapAdapter`` and do not have connector
        compilers yet.
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
        # Algebra V1.9 protocols (Camelot V3) have a different factory ABI
        # (poolByPair(tokenA, tokenB)) and dynamic fees, so the V3
        # `getPool(tokenA, tokenB, fee)` selector cannot be used. The Algebra
        # quoter call performed during fee-tier selection already exercises
        # the pool — a non-zero `last_quoted_amount_out` proves both
        # existence and minimum liquidity at the requested size, and a
        # quoter that returns 0 / errors is surfaced via the
        # price-impact guard's QUOTER_MISSING_FAIL_CLOSED branch. Skip the
        # V3-style validation here to avoid a misleading PROTOCOL_UNKNOWN
        # warning. (VIB-3750)

        if protocol in SWAP_ROUTER_ALGEBRA_PROTOCOLS:
            return None

        selected_fee = adapter.last_fee_selection.get("selected_fee_tier")
        if selected_fee is None:
            return None
        from almanak.connectors._strategy_base.pool_validation_registry import PoolValidationRegistry

        pool_check = PoolValidationRegistry.validate(
            protocol,
            self.chain,
            actual_from_token,
            actual_to_token,
            {"fee_tier": selected_fee},
            self._get_chain_rpc_url(),
            gateway_client=self._gateway_client,
        )
        return self._validate_pool(pool_check, intent_id)

    def _unsupported_lp_compilation_error(
        self, intent: "LPOpenIntent | LPCloseIntent", intent_label: str
    ) -> CompilationResult:
        """Build a capability-scoped FAILED result for an unroutable LP intent.

        Beyond the bare "not supported on <chain>" message, list the protocols
        that DO support this LP verb so the user can correct the selection.
        This closes the docs/runtime capability mismatch where the support
        matrix advertises a protocol generically (e.g. Balancer, which is
        flash-loan-only) while its LP intents are not routable (ALM-2729).

        Mirrors the existing LP_COLLECT_FEES capability error and uses the same
        ``CompilerRegistry`` — the registry connector compilers populate as they
        register, so it reflects what is actually routable in this process.
        """
        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        protocol = self._resolve_protocol(intent.protocol)
        message = f"Protocol '{protocol}' is not supported for {intent_label} on {self.chain}."

        supporting = CompilerRegistry.protocols_for_intent(intent.intent_type)
        if supporting:
            message += f" Protocols supporting {intent_label}: {', '.join(supporting)}."

        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=message,
        )

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
        routed = self._dispatch_lp_open_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)
        return self._unsupported_lp_compilation_error(intent, "LP_OPEN")

    def _dispatch_lp_open_protocol_route(self, intent: LPOpenIntent) -> CompilationResult | None:
        """Route an LP_OPEN intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when connector dispatch should handle it.

        Extracted in Phase 6B.4 so ``_compile_lp_open`` itself stays small.
        Dispatch order is preserved from the pre-refactor method.

        VIB-4803: Solana protocol routing (meteora_dlmm / orca_whirlpools /
        raydium_clmm) now lives in :class:`SvmFamily.compile_intent`. The
        family.compile_intent contract:

        * EvmFamily returns None -> fall through to connector dispatch.
        * SvmFamily returns a CompilationResult for every (chain, protocol)
          tuple it owns, including cross-chain mismatches (e.g. meteora_dlmm
          on an EVM chain -> FAILED with explicit error).

        We iterate every registered family (in :class:`ChainFamily` enum
        order) so a cross-chain protocol mismatch (Solana-only protocol on
        EVM chain) is caught by SvmFamily even when ``self._family`` is
        :class:`EvmFamily`.
        """
        return self._family_compile_intent(intent)

    # crap-allowlist: VIB-4688 — pre-existing method (cc=10, under threshold); coverage-driven score from docstring touch during phase-2 fold. Unit-coverage backfill tracked in VIB-4688.
    def _fetch_lp_pool_slot0(
        self,
        pool_check: "PoolValidationResult",
    ) -> tuple[int, int] | None:
        """Fetch slot0 for CL pool-alignment helpers.

        Returns ``(sqrt_price_x96, current_tick)`` or ``None`` when the
        pool address / transport is unavailable or the call fails. Connector
        compilers access this through the shared service context so they use
        the running ``IntentCompiler`` gateway/RPC transport.
        """
        if not pool_check.pool_address:
            return None

        rpc_url = self._get_chain_rpc_url()
        gateway_connected = self._gateway_client is not None and self._gateway_client.is_connected
        if not (rpc_url or gateway_connected):
            return None

        from almanak.connectors._strategy_base.pool_validation_registry import PoolValidationRegistry

        try:
            slot0_result = PoolValidationRegistry.fetch_sqrt_price(
                self.default_protocol,
                pool_check.pool_address,
                self.chain,
                rpc_url,
                gateway_client=self._gateway_client,
            )
        except Exception as exc:
            logger.warning(
                "LP slot0 lookup failed for pool %s; proceeding with oracle-derived amounts "
                "which may cause 'Price slippage check' revert if oracle/pool prices diverge: %s",
                pool_check.pool_address,
                exc,
            )
            return None

        if slot0_result is None:
            return None
        sqrt_price_x96, current_tick = slot0_result
        # Guard against partial returns: downstream math (recompute, preflight)
        # consumes both fields and would type-error on a None tick. The
        # gateway/Web3 plumbing should always return both, but a defensive
        # None-check keeps the return shape strictly matching the type hint
        # (gemini-code-assist).
        if sqrt_price_x96 is None or sqrt_price_x96 <= 0 or current_tick is None:
            return None
        return sqrt_price_x96, current_tick

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
        routed = self._dispatch_lp_close_protocol_route(intent)
        if routed is not None:
            return routed

        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)
        return self._unsupported_lp_compilation_error(intent, "LP_CLOSE")

    def _dispatch_lp_close_protocol_route(self, intent: LPCloseIntent) -> CompilationResult | None:
        """Route an LP_CLOSE intent to the correct protocol-specific compiler.

        Returns the routed ``CompilationResult`` when a dedicated helper owns
        the protocol, or ``None`` when connector dispatch should handle it.

        Extracted in Phase 6B backlog so ``_compile_lp_close`` itself stays small.
        Dispatch order is preserved from the pre-refactor method.

        VIB-4803: Solana protocol routing moved to :class:`SvmFamily.compile_intent`.
        See :meth:`_dispatch_lp_open_protocol_route` for the contract.
        """
        return self._family_compile_intent(intent)

    # crap-allowlist: PR is pure string-content cleanup (chore: VIB removal); zero branches added, function was already over threshold on main. Refactor tracked in VIB-4139.
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
        connector_compiler = get_connector_compiler(protocol)
        # Dispatch only when the connector compiler actually declares
        # LP_COLLECT_FEES. A compiler may be registered for the protocol (to
        # handle swaps / LP open / close) without supporting standalone fee
        # collection — e.g. Pendle and Curve. Dispatching anyway would let the
        # connector return a bespoke FAILED result that masquerades as
        # protocol-handled, the silent-FAILED path VIB-5308 closes. Gating on
        # the compiler's declared ``intents`` ClassVar makes that declaration
        # authoritative for dispatch and routes unsupported
        # (protocol, LP_COLLECT_FEES) pairs to the canonical
        # "not supported, here is what is" error below.
        if connector_compiler is not None and intent.intent_type in connector_compiler.intents:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)
        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        supported = CompilerRegistry.protocols_for_intent(intent.intent_type)
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(f"Protocol '{protocol}' does not support LP_COLLECT_FEES. Supported: {', '.join(supported)}"),
            intent_id=intent.intent_id,
        )

    def _compile_borrow(self, intent: BorrowIntent) -> CompilationResult:
        """Compile a BORROW intent into an ActionBundle."""
        return self._compile_lending_via_registry(intent, "BORROW")

    def _compile_repay(self, intent: RepayIntent) -> CompilationResult:
        """Compile a REPAY intent into an ActionBundle."""
        return self._compile_lending_via_registry(intent, "REPAY")

    def _compile_supply(self, intent: SupplyIntent) -> CompilationResult:
        """Compile a SUPPLY intent into an ActionBundle."""
        return self._compile_lending_via_registry(intent, "SUPPLY")

    def _compile_withdraw(self, intent: WithdrawIntent) -> CompilationResult:
        """Compile a WITHDRAW intent into an ActionBundle."""
        return self._compile_lending_via_registry(intent, "WITHDRAW")

    def _compile_perp_via_registry(self, intent: PerpOpenIntent | PerpCloseIntent) -> CompilationResult:
        """Compile a PERP intent through a connector-owned compiler."""
        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        primitive = intent.intent_type.value if hasattr(intent.intent_type, "value") else str(intent.intent_type)
        supported = CompilerRegistry.protocols_for_intent(intent.intent_type)
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Protocol '{intent.protocol}' is not supported for {primitive} on {self.chain}. "
                f"Known perp protocols (each compiles only on its own chains): {', '.join(supported)}."
            ),
            intent_id=intent.intent_id,
        )

    def _compile_lending_via_registry(self, intent: Any, primitive: str) -> CompilationResult:
        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        supported = CompilerRegistry.protocols_for_intent(intent.intent_type)
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Unsupported lending protocol for {primitive}: {intent.protocol}. Supported: {', '.join(supported)}"
            ),
            intent_id=intent.intent_id,
        )

    def _compile_staking_via_registry(self, intent: Any, primitive: str) -> CompilationResult:
        """Compile a staking intent through a connector-owned compiler."""
        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        action = "staking" if primitive == "STAKE" else "unstaking"
        supported = CompilerRegistry.protocols_for_intent(intent.intent_type)
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Unsupported {action} protocol: {intent.protocol}. Supported: {', '.join(supported)}",
            intent_id=intent.intent_id,
        )

    def _compile_prediction_via_registry(self, intent: Any) -> CompilationResult:
        """Compile a prediction-market intent through its connector compiler."""
        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        # VIB-4989: resolve the protocol from the intent (falling back to the
        # registered PREDICTION default) instead of hardcoding "polymarket".
        protocol = intent.protocol or CompilerRegistry.default_protocol("PREDICTION") or ""
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is None:
            supported = CompilerRegistry.protocols_for_intent(intent.intent_type)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Connector compiler for prediction protocol {protocol!r} is not "
                    f"registered. Supported: {', '.join(supported)}"
                ),
                intent_id=intent.intent_id,
            )
        return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

    def _compile_vault_via_registry(self, intent: Any) -> CompilationResult:
        """Compile a vault intent through a connector-owned compiler."""
        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Vault protocol '{protocol}' is not supported: connector compiler is not registered.",
                intent_id=intent.intent_id,
            )
        return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

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

    def _resolve_token(self, token: str, chain: str | None = None) -> TokenInfo | None:
        """Delegates to CompilerQueries.resolve_token (see compiler_queries.py)."""
        return self._queries.resolve_token(token, chain=chain)

    def _get_token_decimals(self, symbol: str) -> int:
        """Delegates to CompilerQueries.get_token_decimals (see compiler_queries.py)."""
        return self._queries.get_token_decimals(symbol)

    def _get_wrapped_native_address(self) -> str | None:
        """Return the wrapped native token address for the current chain via WRAPPED_NATIVE.

        Delegates to CompilerQueries.get_wrapped_native_address (see compiler_queries.py),
        which reads from ``almanak.framework.data.tokens.defaults.WRAPPED_NATIVE``.
        """
        return self._queries.get_wrapped_native_address()

    def _usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        """Delegates to CompilerQueries.usd_to_token_amount (see compiler_queries.py)."""
        return self._queries.usd_to_token_amount(usd_amount, token)

    def _calculate_expected_output(
        self,
        amount_in: int,
        from_token: TokenInfo,
        to_token: TokenInfo,
    ) -> int:
        """Delegates to CompilerQueries.calculate_expected_output (see compiler_queries.py)."""
        return self._queries.calculate_expected_output(amount_in, from_token, to_token)

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
        # Keep symmetric with ``_NATIVE_TO_WRAPPED`` in
        # ``almanak/framework/data/models.py`` (VIB-3970): every native added
        # there should have its inverse here so price-oracle alias expansion
        # bridges both directions for new chains.
        "W0G": "A0GI",
        "WSOL": "SOL",
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
        """Delegates to CompilerQueries.require_token_price (see compiler_queries.py)."""
        return self._queries.require_token_price(symbol)

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
        """Delegates to compiler_queries.get_placeholder_prices (see compiler_queries.py)."""
        return _qget_placeholder_prices()

    @staticmethod
    def _format_amount(amount: int, decimals: int) -> str:
        """Delegates to compiler_queries.format_amount (see compiler_queries.py)."""
        return _qformat_amount(amount, decimals)

    def _parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None:
        """Delegates to CompilerQueries.parse_pool_info (see compiler_queries.py)."""
        return self._queries.parse_pool_info(pool)

    # Uniswap V3 tick bounds
    UNISWAP_MIN_TICK = cl_math.MIN_TICK
    UNISWAP_MAX_TICK = cl_math.MAX_TICK

    @staticmethod
    def _price_to_tick(
        price: Decimal,
        token0_decimals: int = 18,
        token1_decimals: int = 18,
    ) -> int:
        """Delegates to compiler_queries.price_to_tick (see compiler_queries.py)."""
        return _qprice_to_tick(price, token0_decimals, token1_decimals)

    @staticmethod
    def _tick_to_price(tick: int) -> Decimal:
        """Delegates to compiler_queries.tick_to_price (see compiler_queries.py)."""
        return _qtick_to_price(tick)

    @staticmethod
    def _get_tick_spacing(fee_tier: int) -> int:
        """Delegates to compiler_queries.get_tick_spacing (see compiler_queries.py)."""
        return _qget_tick_spacing(fee_tier)

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
        """Delegates to CompilerQueries.query_position_liquidity (see compiler_queries.py)."""
        return self._queries.query_position_liquidity(position_manager, token_id)

    def _query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]:
        """Delegates to CompilerQueries.query_position_tokens_owed (see compiler_queries.py)."""
        return self._queries.query_position_tokens_owed(position_manager, token_id)

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        """Delegates to CompilerQueries.query_erc20_balance (see compiler_queries.py)."""
        return self._queries.query_erc20_balance(token_address, wallet_address)

    def _query_erc20_balance_for_chain(self, token_address: str, wallet_address: str, chain: str) -> int | None:
        """Delegates to CompilerQueries.query_erc20_balance_for_chain (see compiler_queries.py)."""
        return self._queries.query_erc20_balance_for_chain(token_address, wallet_address, chain)

    def _query_native_balance_for_chain(self, wallet_address: str, chain: str) -> int | None:
        """Delegates to CompilerQueries.query_native_balance_for_chain (see compiler_queries.py)."""
        return self._queries.query_native_balance_for_chain(wallet_address, chain)

    def _eth_call(self, to: str, data: str, *, chain: str | None = None) -> str | None:
        """Delegates to CompilerQueries.eth_call (see compiler_queries.py)."""
        return self._queries.eth_call(to, data, chain=chain)

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
        anvil_port = anvil_port_for_chain(chain)
        if anvil_port:
            return f"http://127.0.0.1:{anvil_port}"

        try:
            from almanak.gateway.utils import get_rpc_url

            return get_rpc_url(chain)
        except (ImportError, ValueError) as e:
            logger.debug(f"Failed to get RPC URL for {chain}: {e}")
            return None

    def _query_native_balance(self, wallet_address: str) -> int | None:
        """Delegates to CompilerQueries.query_native_balance (see compiler_queries.py)."""
        return self._queries.query_native_balance(wallet_address)


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
