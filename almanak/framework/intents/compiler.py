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
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

import grpc

from almanak.config.cli_runtime import anvil_port_for_chain

from ..chain_family import ChainFamilyAdapter, SvmFamily, all_families, family_for
from ..connectors.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    CLCompilerContext,
    PerpCompilerContext,
    SwapCompilerContext,
)
from ..connectors.base.swap_adapter import DefaultSwapAdapter
from ..connectors.compiler_registry import get_compiler as get_connector_compiler

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
    SupplyIntent,
    SwapIntent,
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
    from .pool_validation import PoolValidationResult
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
    return "across"


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
# Inlined to avoid pulling the resolver's web3-touching import chain into the
# compiler's hot path (unit tests monkeypatch ``web3``). Mirrors
# ``almanak.framework.data.tokens.resolver.SOLANA_ADDRESS_PATTERN``.
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# Lending connectors with connector-owned compilers — used only to build the
# "Supported: ..." hint in the unsupported-protocol error. Keep in sync with
# the lending entries in connectors.compiler_registry.CompilerRegistry.
_LENDING_PROTOCOLS = (
    "aave_v3",
    "benqi",
    "compound_v3",
    "curvance",
    "euler_v2",
    "jupiter_lend",
    "kamino",
    "morpho",
    "morpho_blue",
    "radiant_v2",
    "silo_v2",
    "spark",
)

# Perp connectors with connector-owned compilers — used only to build the
# "known perp protocols" hint in the unsupported-protocol error. Grouped by
# chain family (Solana vs non-Solana); the hint deliberately does NOT claim
# per-chain support — each venue compiles only on its own chains. Keep in
# sync with the perp entries in connectors.compiler_registry.CompilerRegistry.
_PERP_PROTOCOLS_SOLANA = ("drift",)
_PERP_PROTOCOLS_NON_SOLANA = ("gmx_v2", "aster_perps", "pancakeswap_perps", "hyperliquid")

# Staking connectors with connector-owned compilers — used only to build the
# "Supported: ..." hint in the unsupported-protocol error. Keep in sync with
# the staking entries in connectors.compiler_registry.CompilerRegistry.
_STAKING_PROTOCOLS = ("ethena", "gimo", "lido")


def _is_solana_mint(token: str) -> bool:
    """True when ``token`` matches a Solana base58 mint (32-44 chars)."""
    return bool(_SOLANA_ADDRESS_RE.match(token))


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
    "solana": frozenset({"SOL"}),
}


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
        from ..connectors.protocol_aliases import normalize_protocol

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
        # Log stablecoin price fallbacks once per symbol per compiler instance.
        self._stablecoin_fallback_logged: set[str] = set()

        # Polymarket adapter for prediction market intents (Polygon only)
        self._polymarket_adapter: PolymarketAdapter | None = None
        self._init_polymarket_adapter()

        # Cached Solana adapter instances (lazily initialized)
        self._cached_jupiter_adapter: Any = None
        self._cached_kamino_adapter: Any = None
        self._cached_kamino_adapter_with_rpc: Any = None
        self._cached_jupiter_lend_adapter: Any = None
        self._cached_drift_adapter: Any = None

        effective_protocol = "jupiter" if isinstance(self._family, SvmFamily) else default_protocol
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
            "cache": getattr(self, "_allowance_cache", {}),
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
            return self._build_cl_compiler_context(protocol)
        if issubclass(context_type, PerpCompilerContext):
            return PerpCompilerContext(**self._base_compiler_context_kwargs(resolve_rpc_url=False), protocol=protocol)
        if issubclass(context_type, SwapCompilerContext):
            return SwapCompilerContext(**self._swap_compiler_context_kwargs())
        # Solana-only connectors (Meteora DLMM, Orca Whirlpools, Raydium CLMM) hold
        # their own Solana RPC client — they do NOT route through the gateway.
        # ``_get_chain_rpc_url()`` returns ``None`` when a gateway client is
        # connected, which would silently strand these adapters with an empty
        # RPC URL. Force the raw ``self.rpc_url`` pathway for them, matching
        # the pre-fold ``compiler_solana.py`` behaviour. VIB-4121.
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
        return bool(chains_set) and chains_set <= frozenset({"solana"})

    def _build_cl_compiler_context(self, protocol: str) -> CLCompilerContext:
        """Build the connector compiler context for concentrated-liquidity protocols."""
        config = getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))

        def default_swap_adapter_factory(adapter_protocol: str) -> DefaultSwapAdapter:
            return DefaultSwapAdapter(
                self.chain,
                adapter_protocol,
                pool_selection_mode=config.swap_pool_selection_mode,
                fixed_fee_tier=config.fixed_swap_fee_tier,
                rpc_url=self._get_chain_rpc_url(),
                rpc_timeout=getattr(self, "rpc_timeout", 10.0),
                gateway_client=getattr(self, "_gateway_client", None),
            )

        def lp_adapter_factory(adapter_protocol: str) -> Any:
            from ..connectors.uniswap_v3.adapter import UniswapV3LPAdapter

            return UniswapV3LPAdapter(self.chain, adapter_protocol)

        return CLCompilerContext(
            **self._swap_compiler_context_kwargs(),
            protocol=protocol,
            default_swap_adapter_factory=default_swap_adapter_factory,
            lp_adapter_factory=lp_adapter_factory,
            swap_pool_selection_mode=config.swap_pool_selection_mode,
            fixed_swap_fee_tier=config.fixed_swap_fee_tier,
            default_lp_slippage=self.default_lp_slippage,
        )

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
                return self._compile_prediction_buy(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_SELL:
                return self._compile_prediction_sell(intent)  # type: ignore[arg-type]
            elif intent_type == IntentType.PREDICTION_REDEEM:
                return self._compile_prediction_redeem(intent)  # type: ignore[arg-type]
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

    # =========================================================================
    # ChainFamily dispatch (VIB-4803)
    #
    # Solana SWAP / LP / lending compilation is owned by
    # :class:`SvmFamily.compile_intent` (almanak.framework.chain_family). The
    # historical ``_compile_jupiter_swap`` / ``_get_jupiter_adapter`` etc.
    # wrapper methods on this class have been removed - they were thin
    # delegates to the module-level helpers in ``compiler_solana``, and the
    # dispatch now goes through :meth:`_family_compile_intent` below.
    #
    # Jupiter swap still lives in ``compiler_solana.compile_jupiter_swap``.
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

        from ..connectors.protocol_aliases import UNISWAP_V3_FORKS

        if protocol in UNISWAP_V3_FORKS:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Connector compiler for protocol '{protocol}' is not registered.",
            )
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
        # Preserve historical behavior: protocol=None defaults to Enso for cross-chain swaps.
        if intent.is_cross_chain:
            aggregator_protocol = "enso"
            if intent.protocol is not None:
                from ..connectors.protocol_aliases import normalize_protocol

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

        # Auto-detect PT-/YT- prefixed tokens before generic default-protocol dispatch.
        # Must run before other protocol dispatches so that PT-/YT- tokens are routed to
        # Pendle regardless of default_protocol (e.g., enso, aerodrome). VIB-2535.
        if intent.protocol is None and self._has_pendle_token_prefix(intent):
            connector_compiler = get_connector_compiler("pendle")
            if connector_compiler is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Pendle connector compiler is not registered for PT-/YT- token swaps.",
                )
            return connector_compiler.compile(self._build_compiler_context("pendle", connector_compiler), intent)

        return None

    @staticmethod
    def _has_pendle_token_prefix(intent: SwapIntent) -> bool:
        """True iff either swap leg is a PT-/YT- token that must route to Pendle."""
        to_upper = (intent.to_token or "").upper()
        from_upper = (intent.from_token or "").upper()
        return to_upper.startswith(("PT-", "YT-")) or from_upper.startswith(("PT-", "YT-"))

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

        from ..connectors.protocol_aliases import UNISWAP_V3_FORKS

        if protocol in UNISWAP_V3_FORKS:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Connector compiler for protocol '{protocol}' is not registered.",
            )
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Protocol '{protocol}' is not supported for LP_OPEN on {self.chain}.",
        )

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

        from .pool_validation import fetch_v3_pool_sqrt_price_x96

        try:
            slot0_result = fetch_v3_pool_sqrt_price_x96(
                pool_check.pool_address,
                rpc_url,
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

        from ..connectors.protocol_aliases import UNISWAP_V3_FORKS

        if protocol in UNISWAP_V3_FORKS:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Connector compiler for protocol '{protocol}' is not registered.",
            )
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=f"Protocol '{protocol}' is not supported for LP_CLOSE on {self.chain}.",
        )

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
        from ..connectors.protocol_aliases import UNISWAP_V3_FORKS

        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)

        if protocol in UNISWAP_V3_FORKS:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Connector compiler for protocol '{protocol}' is not registered.",
                intent_id=intent.intent_id,
            )

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Protocol '{protocol}' does not support LP_COLLECT_FEES. "
                f"Supported: traderjoe_v2, uniswap_v4, aerodrome_slipstream, "
                f"and Uniswap-V3 forks ({', '.join(sorted(UNISWAP_V3_FORKS))})"
            ),
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

        primitive = intent.intent_type.value if hasattr(intent.intent_type, "value") else str(intent.intent_type)
        supported = _PERP_PROTOCOLS_SOLANA if isinstance(self._family, SvmFamily) else _PERP_PROTOCOLS_NON_SOLANA
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
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Unsupported lending protocol for {primitive}: {intent.protocol}. "
                f"Supported: {', '.join(_LENDING_PROTOCOLS)}"
            ),
            intent_id=intent.intent_id,
        )

    def _compile_staking_via_registry(self, intent: Any, primitive: str) -> CompilationResult:
        """Compile a staking intent through a connector-owned compiler."""
        protocol = self._resolve_protocol(intent.protocol)
        connector_compiler = get_connector_compiler(protocol)
        if connector_compiler is not None:
            return connector_compiler.compile(self._build_compiler_context(protocol, connector_compiler), intent)
        action = "staking" if primitive == "STAKE" else "unstaking"
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Unsupported {action} protocol: {intent.protocol}. Supported: {', '.join(_STAKING_PROTOCOLS)}",
            intent_id=intent.intent_id,
        )

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
            from ..connectors.vaults import (
                build_vault_adapter,
                is_vault_chain_supported,
                supported_vault_chains,
            )

            # VIB-3827: fail-fast chain support check. Without this, the
            # adapter constructor raises a generic ``ValueError("Invalid
            # chain: …")`` which the state machine cannot classify as
            # permanent — the runner then retries forever on a deterministic
            # mis-configuration. Surfaces the "not supported" keyword that
            # ``_categorize_error`` maps to ``COMPILATION_PERMANENT``.
            #
            # Two distinct rejection cases are handled separately so neither
            # falls through to a misclassified error:
            #   1. Unknown protocol (``model_construct`` bypassed the pydantic
            #      validator, e.g. when an intent is restored from serialized
            #      state). ``supported_vault_chains`` would raise ``KeyError``
            #      and the broad ``except`` below would strip the message to
            #      the bare protocol name, missing ``permanent_keywords``.
            #   2. Known protocol, unsupported chain (the headline VIB-3827
            #      case — Sonic on metamorpho today).
            if not is_vault_chain_supported(intent.protocol, self.chain):
                try:
                    supported = supported_vault_chains(intent.protocol)
                except KeyError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Vault protocol '{intent.protocol}' is not supported "
                            "(no vault adapter registered). Register the adapter "
                            "or correct the intent's protocol field before retrying."
                        ),
                        intent_id=intent.intent_id,
                    )
                supported_str = ", ".join(sorted(supported)) if supported else "(none declared)"
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Vault protocol '{intent.protocol}' is not supported on chain "
                        f"'{self.chain}'. Supported chains: {supported_str}. "
                        "File a vault registry / native connector ticket for the missing "
                        "chain before retrying."
                    ),
                    intent_id=intent.intent_id,
                )

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
            from ..connectors.vaults import (
                build_vault_adapter,
                is_vault_chain_supported,
                supported_vault_chains,
            )

            # VIB-3827: fail-fast chain support check (mirrors deposit lane).
            # Keeps redeem failures classifiable as ``COMPILATION_PERMANENT``
            # so the runner does not retry indefinitely when the wallet has
            # an open position on a chain the vault adapter does not (yet)
            # support — a real scenario for stale state during chain rollouts.
            # Unknown-protocol case is split out so ``supported_vault_chains``
            # never raises ``KeyError`` into the broad ``except`` below.
            if not is_vault_chain_supported(intent.protocol, self.chain):
                try:
                    supported = supported_vault_chains(intent.protocol)
                except KeyError:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Vault protocol '{intent.protocol}' is not supported "
                            "(no vault adapter registered). Register the adapter "
                            "or correct the intent's protocol field before retrying."
                        ),
                        intent_id=intent.intent_id,
                    )
                supported_str = ", ".join(sorted(supported)) if supported else "(none declared)"
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Vault protocol '{intent.protocol}' is not supported on chain "
                        f"'{self.chain}'. Supported chains: {supported_str}. "
                        "File a vault registry / native connector ticket for the missing "
                        "chain before retrying."
                    ),
                    intent_id=intent.intent_id,
                )

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
            #
            # Chain-aware address detection (CodeRabbit P2 on PR #2005):
            # EVM uses 0x-prefixed hex; Solana uses base58 mints (no 0x).
            # Without the Solana branch the cross-check could flip
            # is_native=True for a raw SPL mint that resolves to symbol
            # "SOL", bypassing the SPL-token path.
            input_is_address = isinstance(token, str) and (
                token.startswith("0x") or (target_chain.lower() == "solana" and _is_solana_mint(token))
            )
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
