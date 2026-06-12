"""Read-only token/price/balance/pool query helpers extracted from ``IntentCompiler``.

Scope / contract:
    - Every helper here reads on-chain or oracle state via the ``CompilerQueryHost``
      Protocol; none writes application state or builds transactions.
    - ``CompilerQueryHost`` names exactly the attributes and methods the collaborator
      reads/calls on the compiler. The collaborator holds a live reference to the host
      and reads every attribute at call time — NOT captured at ``__init__``. This is
      deliberate: ~15 test files patch helpers as instance attributes on the compiler
      (e.g. ``patch.object(compiler, "_query_erc20_balance", ...)``) and reassign
      compiler state post-construction (e.g. ``compiler.price_oracle = {...}``).
      Capturing values at init would break those seams silently.
    - Cross-helper calls go back through the host wrappers (``self._host._resolve_token``,
      etc.) so that instance-level patches on the compiler propagate into composite
      helpers (e.g. a test that patches ``compiler._resolve_token`` still affects
      ``compiler._parse_pool_info`` because ``parse_pool_info`` calls
      ``self._host._resolve_token``).
    - ``self._host._web3`` is both read AND written here (lazy-init Web3 cache shared
      with ``_query_allowance``, which stays in ``compiler.py``). The assignment
      ``self._host._web3 = Web3(...)`` is intentional.
    - No imports from ``.compiler`` (one-way dependency: ``compiler`` imports from
      ``compiler_queries``; never the reverse).
    - No ``almanak.gateway.utils`` imports: all vib-2986 RPC-URL fallback resolution
      that calls ``almanak.gateway.utils.get_rpc_url`` stays in ``compiler.py``
      (``_get_rpc_url_for_chain``, ``_get_chain_rpc_url``). Those are migration debt
      per AGENTS.md §Gateway boundary; keeping them compiler-side keeps all
      ``gateway.utils`` import entries in plan 013's baseline valid in either landing
      order.
    - Lazy imports inside method bodies stay lazy exactly as they were in
      ``compiler.py`` (e.g. ``TokenNotFoundError``, ``WRAPPED_NATIVE``, ``web3``,
      ``gateway_pb2``) to preserve the circular-import workaround documented at
      ``compiler.py:52–54``.

Design rationale (why host-Protocol instead of constructor injection):
    Fifteen test files patch these helpers as *instance attributes* on the compiler
    (``patch.object(compiler, "_resolve_token", return_value=None)``) and reassign
    state such as ``compiler.price_oracle``, ``compiler.rpc_url``,
    ``compiler._gateway_client`` after construction. Converting to captured
    constructor args or collaborator-internal cross-calls would silently break those
    seams. If a future cleanup wants true constructor injection, the test fixtures
    must migrate first. See plan 016 maintenance notes.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar, Protocol

from almanak.connectors._strategy_base import concentrated_liquidity_math as cl_math
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import is_solana_chain, native_symbols_for

from .compiler_models import TokenInfo

if TYPE_CHECKING:
    from web3 import Web3

    from ..data.tokens import TokenResolver
    from ..gateway_client import GatewayClient

logger = logging.getLogger(__name__)

# =============================================================================
# Module-level symbols (moved from compiler.py:288–294 and 321–324)
# =============================================================================

# Mirrors ``almanak.framework.data.tokens.resolver.SOLANA_ADDRESS_PATTERN``.
# Inlined to avoid pulling the resolver's web3-touching import chain into the
# compiler's hot path (unit tests monkeypatch ``web3``).
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _is_solana_mint(token: str) -> bool:
    """True when ``token`` matches a Solana base58 mint (32-44 chars)."""
    return bool(_SOLANA_ADDRESS_RE.match(token))


# Per-chain native-gas symbols, derived from the chain registry (VIB-4851 A1):
# each chain owns its native symbol(s) on ``ChainDescriptor.native`` and the set
# is ``{symbol, *accepted_symbols}`` (e.g. polygon -> {"MATIC", "POL"}). Kept under
# the legacy name as a read-only view so the external importers
# (permissions.synthetic_intents, teardown.oracle_warmup) read it unchanged.
_CHAIN_NATIVE_SYMBOLS: MappingProxyType[str, frozenset[str]] = MappingProxyType(
    {d.name: native_symbols_for(d.name) for d in ChainRegistry.all()}
)


# =============================================================================
# Module-level pure functions (formerly @staticmethod on IntentCompiler)
# =============================================================================


def format_amount(amount: int, decimals: int) -> str:
    """Format a wei amount for display."""
    decimal_amount = Decimal(str(amount)) / Decimal(10**decimals)
    return f"{decimal_amount:,.4f}"


def get_placeholder_prices() -> dict[str, Decimal]:
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


def price_to_tick(
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
    return cl_math.price_to_tick(price, decimals0=token0_decimals, decimals1=token1_decimals)


def tick_to_price(tick: int) -> Decimal:
    """Convert a Uniswap V3 tick to a price.

    Args:
        tick: The tick value

    Returns:
        The price (1.0001^tick)
    """
    return cl_math.tick_to_price(tick)


def get_tick_spacing(fee_tier: int) -> int:
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


# =============================================================================
# Host Protocol — names exactly what CompilerQueries reads/calls on the compiler
# =============================================================================


class CompilerQueryHost(Protocol):
    """Narrow structural type of the IntentCompiler attributes this collaborator accesses.

    ``CompilerQueries`` holds a live reference of this type and reads every attribute
    at call time. This is deliberate: tests reassign ``price_oracle``, ``rpc_url``,
    etc. after construction; capturing at init would silently break those seams.
    """

    chain: str
    rpc_url: str | None
    rpc_timeout: float
    price_oracle: dict[str, Decimal] | None
    _using_placeholders: bool
    _web3: Web3 | None  # shared lazy cache; collaborator may ASSIGN it
    _gateway_client: GatewayClient | None
    _token_resolver: TokenResolver
    _stablecoin_fallback_logged: set[str]
    _WRAPPED_TO_NATIVE: ClassVar[dict[str, str]]  # ClassVar on IntentCompiler; stays there

    def _resolve_token(self, token: str, chain: str | None = ...) -> TokenInfo | None: ...

    def _require_token_price(self, symbol: str) -> Decimal: ...

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None: ...

    def _query_native_balance(self, wallet_address: str) -> int | None: ...

    def _get_rpc_url_for_chain(self, chain: str) -> str | None: ...

    def _get_known_stablecoins(self) -> frozenset[str]: ...


# =============================================================================
# CompilerQueries collaborator
# =============================================================================


class CompilerQueries:
    """Read-only query collaborator for ``IntentCompiler``.

    Holds a live reference to ``IntentCompiler`` via the narrow ``CompilerQueryHost``
    Protocol; all state reads happen at call time (never captured at init).
    ``IntentCompiler`` exposes each method here under the original ``_<name>`` wrapper
    so instance-level monkeypatches and post-construction state reassignments continue
    to work without change.
    """

    def __init__(self, host: CompilerQueryHost) -> None:
        self._host = host

    # ------------------------------------------------------------------
    # Token resolution
    # ------------------------------------------------------------------

    def resolve_token(self, token: str, chain: str | None = None) -> TokenInfo | None:
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
        target_chain = chain or self._host.chain

        try:
            # Use TokenResolver for unified lookup
            resolved = self._host._token_resolver.resolve(token, target_chain)

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
                token.startswith("0x") or (is_solana_chain(target_chain) and _is_solana_mint(token))
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

    def get_token_decimals(self, symbol: str) -> int:
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
        return self._host._token_resolver.get_decimals(self._host.chain, symbol)

    def get_wrapped_native_address(self) -> str | None:
        """Return the wrapped native token address for the current chain.

        Single source of truth: WRAPPED_NATIVE in
        ``almanak/framework/data/tokens/data/chains.json`` (exposed via
        ``defaults.WRAPPED_NATIVE``). Previously this method carried a
        duplicate per-chain symbol dict that drifted from the real registry
        (e.g. the ``zerog`` entry was missing until VIB-2896 surfaced it).
        Reading the canonical dict directly prevents that drift.
        """
        from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

        return WRAPPED_NATIVE.get(self._host.chain)

    def usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        """Convert USD amount to token amount in wei.

        Args:
            usd_amount: Amount in USD
            token: Target token info

        Returns:
            Token amount in smallest units (wei)
        """
        price = self._host._require_token_price(token.symbol)
        token_amount = usd_amount / price
        return int(token_amount * Decimal(10**token.decimals))

    def calculate_expected_output(
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
        from_price = self._host._require_token_price(from_token.symbol)
        to_price = self._host._require_token_price(to_token.symbol)

        # Convert input to USD
        from_amount_decimal = Decimal(str(amount_in)) / Decimal(10**from_token.decimals)
        usd_value = from_amount_decimal * from_price

        # Convert USD to output tokens
        to_amount_decimal = usd_value / to_price

        # Apply a small fee estimate (0.3%)
        to_amount_decimal = to_amount_decimal * Decimal("0.997")

        return int(to_amount_decimal * Decimal(10**to_token.decimals))

    # ------------------------------------------------------------------
    # Price lookups
    # ------------------------------------------------------------------

    # crap-allowlist: plan-016 verbatim extraction — function body moved unchanged from compiler.py where it pre-existed at the same cc/CRAP score; the gate sees it as new code only because the host file changed. Coverage backfill tracked in plan-016 maintenance notes.
    def require_token_price(self, symbol: str) -> Decimal:
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
        if self._host.price_oracle is None:
            if self._host._using_placeholders:
                return Decimal("1")
            # Fall back for stablecoins even without an oracle
            if symbol.upper() in self._host._get_known_stablecoins():
                return Decimal("1")
            raise ValueError(
                f"No price oracle available and placeholder prices are disabled. Cannot resolve price for '{symbol}'."
            )

        price = self._host.price_oracle.get(symbol)
        if price is None or price == 0:
            # Case-insensitive fallback: Token.__post_init__ uppercases symbols
            # (e.g., "cbETH" -> "CBETH") but the price oracle may store them in
            # original case. Try case-insensitive match before giving up.
            symbol_upper = symbol.upper()
            for key, val in self._host.price_oracle.items():
                if key.upper() == symbol_upper and val is not None and val != 0:
                    price = val
                    logger.debug(f"Resolved '{symbol}' price via case-insensitive match (key='{key}')")
                    break

        if price is None or price == 0:
            # Try wrapped-native alias (WETH -> ETH, WMATIC -> MATIC, etc.)
            native_alias = self._host._WRAPPED_TO_NATIVE.get(symbol.upper())
            if native_alias:
                alias_price = self._host.price_oracle.get(native_alias)
                if alias_price is not None and alias_price != 0:
                    logger.debug(f"Resolved '{symbol}' price via native alias '{native_alias}'")
                    return alias_price

            if self._host._using_placeholders:
                return Decimal("1")
            # Stablecoin fallback: these are always ~$1, safe to assume
            if symbol.upper() in self._host._get_known_stablecoins():
                if symbol not in self._host._stablecoin_fallback_logged:
                    logger.info(f"Price for '{symbol}' not in oracle cache, using stablecoin fallback ($1.00)")
                    self._host._stablecoin_fallback_logged.add(symbol)
                else:
                    logger.debug(f"Reusing stablecoin fallback price for '{symbol}'")
                return Decimal("1")
            raise ValueError(
                f"Price for '{symbol}' is {'zero' if price == 0 else 'missing'} in the price oracle. "
                "Compilation requires a valid price to calculate amounts and slippage."
            )
        return price

    # ------------------------------------------------------------------
    # Pool parsing
    # ------------------------------------------------------------------

    def parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None:
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

        # Resolve token addresses — go through host wrappers so instance-level patches propagate
        token0 = self._host._resolve_token(token0_symbol)
        token1 = self._host._resolve_token(token1_symbol)

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

    # ------------------------------------------------------------------
    # On-chain position queries
    # ------------------------------------------------------------------

    def query_position_liquidity(self, position_manager: str, token_id: int) -> int | None:
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
        if self._host._gateway_client is not None:
            try:
                return self._host._gateway_client.query_position_liquidity(
                    chain=self._host.chain,
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
        if self._host.rpc_url is None and self._host._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position liquidity")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._host._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._host._web3 = Web3(Web3.HTTPProvider(self._host.rpc_url))

            assert self._host._web3 is not None
            # positions(uint256) returns a tuple with liquidity at index 7
            # Encode the call: positions(tokenId)
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._host._web3.eth.call(
                {
                    "to": self._host._web3.to_checksum_address(position_manager),
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

    def query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]:
        """Query tokens owed (fees + withdrawn liquidity) for a Uniswap V3 position.

        Args:
            position_manager: NonfungiblePositionManager contract address
            token_id: Position NFT token ID

        Returns:
            Tuple of (tokensOwed0, tokensOwed1) or (None, None) if query fails
        """
        # Prefer gateway RPC when available
        if self._host._gateway_client is not None:
            try:
                # Use gateway's dedicated QueryPositionTokensOwed method
                from almanak.gateway.proto import gateway_pb2

                request = gateway_pb2.PositionTokensOwedRequest(
                    chain=str(self._host.chain),
                    position_manager=position_manager,
                    token_id=token_id,
                )

                response = self._host._gateway_client.rpc.QueryPositionTokensOwed(request, timeout=10.0)

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
        if self._host.rpc_url is None and self._host._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query position tokens owed")
            return None, None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._host._web3 is None:
                logger.warning("Using direct Web3 RPC for position query - this is deprecated")
                self._host._web3 = Web3(Web3.HTTPProvider(self._host.rpc_url))

            assert self._host._web3 is not None
            # positions(uint256) returns a tuple
            # tokensOwed0 is at index 10, tokensOwed1 is at index 11
            selector = "0x99fbab88"  # positions(uint256)
            data = selector + hex(token_id)[2:].zfill(64)

            result = self._host._web3.eth.call(
                {
                    "to": self._host._web3.to_checksum_address(position_manager),
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

    # ------------------------------------------------------------------
    # On-chain balance queries
    # ------------------------------------------------------------------

    def query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
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
        if self._host._gateway_client is not None:
            try:
                return self._host._gateway_client.query_erc20_balance(
                    chain=self._host.chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self._host.rpc_url is None and self._host._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query ERC-20 balance")
            return None

        try:
            # Lazy import web3
            from web3 import Web3

            if self._host._web3 is None:
                logger.warning("Using direct Web3 RPC for balance query - this is deprecated")
                self._host._web3 = Web3(Web3.HTTPProvider(self._host.rpc_url))

            assert self._host._web3 is not None
            # balanceOf(address) selector
            selector = "0x70a08231"
            # Pad address to 32 bytes (remove 0x prefix, left-pad with zeros)
            padded_address = wallet_address[2:].lower().zfill(64)
            data = selector + padded_address

            result = self._host._web3.eth.call(
                {
                    "to": self._host._web3.to_checksum_address(token_address),
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

    def query_erc20_balance_for_chain(self, token_address: str, wallet_address: str, chain: str) -> int | None:
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
        if chain == self._host.chain:
            return self._host._query_erc20_balance(token_address, wallet_address)

        # Cross-chain query: prefer gateway (it supports any chain).
        # Fail-closed: if a gateway is configured but fails, do NOT fall through to direct RPC.
        # This matches the behavior of _query_erc20_balance which treats gateway failures as terminal.
        if self._host._gateway_client is not None:
            try:
                return self._host._gateway_client.query_erc20_balance(
                    chain=chain,
                    token_address=token_address,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error("Gateway balance query failed for %s: %s", chain, e)
                return None

        # No gateway configured: fall back to direct Web3 RPC (local dev / Anvil only).
        rpc_url = self._host._get_rpc_url_for_chain(chain)
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

    def query_native_balance_for_chain(self, wallet_address: str, chain: str) -> int | None:
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
        if chain == self._host.chain:
            return self._host._query_native_balance(wallet_address)

        # Fail-closed: if a gateway is configured but fails, do NOT fall through to direct RPC.
        if self._host._gateway_client is not None:
            try:
                return self._host._gateway_client.query_native_balance(
                    chain=chain,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error("Gateway native balance query failed for %s: %s", chain, e)
                return None

        # No gateway configured: fall back to direct Web3 RPC (local dev / Anvil only).
        rpc_url = self._host._get_rpc_url_for_chain(chain)
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

    def query_native_balance(self, wallet_address: str) -> int | None:
        """Query native token balance (ETH, MATIC, AVAX, etc.) from on-chain.

        Uses gateway RPC when available, otherwise falls back to direct Web3 RPC.

        Returns:
            Native balance in wei, or None if query fails
        """
        # Prefer gateway RPC via public API
        if self._host._gateway_client is not None:
            try:
                return self._host._gateway_client.query_native_balance(
                    chain=self._host.chain,
                    wallet_address=wallet_address,
                )
            except Exception as e:
                logger.error(f"Gateway native balance query failed: {e}")
                return None

        # Fallback to direct Web3 RPC (deprecated)
        if self._host.rpc_url is None and self._host._web3 is None:
            logger.warning("No RPC URL or gateway client - cannot query native balance")
            return None

        try:
            from web3 import Web3

            if self._host._web3 is None:
                self._host._web3 = Web3(Web3.HTTPProvider(self._host.rpc_url))

            assert self._host._web3 is not None
            balance = self._host._web3.eth.get_balance(self._host._web3.to_checksum_address(wallet_address))
            logger.debug(f"Native balance for {wallet_address}: {balance}")
            return balance
        except Exception as e:
            logger.error(f"Failed to query native balance: {e}")
            return None
