"""TokenService implementation - unified token resolution and on-chain discovery.

This service provides token resolution and metadata discovery to strategy containers
via gRPC. It uses the TokenResolver for cached/static lookups and OnChainLookup
for discovering unknown tokens by querying their smart contracts directly.

Key Features:
    - ResolveToken: Resolve by symbol or address using cache/static registry
    - GetTokenMetadata: On-chain ERC20 metadata query for unknown tokens
    - GetTokenDecimals: Lightweight endpoint for decimals only
    - BatchResolveTokens: Resolve multiple tokens in a single call
    - Rate limiting: Prevents RPC abuse (max 10 on-chain lookups/second)
    - Timeout handling: Configurable timeout for on-chain queries
"""

import asyncio
import logging
import re
import time
from typing import Any
from urllib.parse import quote as _url_quote

import grpc

from almanak.core.enums import Chain
from almanak.framework.data.tokens import (
    InvalidTokenAddressError,
    ResolvedToken,
    TokenNotFoundError,
    TokenResolutionError,
    get_token_resolver,
)
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.gateway.core.settings import GatewaySettings

# Single source of truth for chain -> CoinGecko platform IDs lives alongside
# the price source so both search/resolver paths and the contract-address
# price endpoint use the same mapping.
from almanak.gateway.data.price.coingecko import COINGECKO_PLATFORM_IDS
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services.dexscreener_lookup import (
    DexScreenerError,
    chain_slug_for,
)
from almanak.gateway.services.dexscreener_lookup import (
    find_token_address as dexscreener_find_token_address,
)
from almanak.gateway.services.onchain_lookup import OnChainLookup, TokenMetadata
from almanak.gateway.services.spl_mint_lookup import SplMintInfo, SplMintLookup
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.validation import (
    ValidationError,
    validate_address_for_chain,
    validate_batch_size,
    validate_chain,
)

logger = logging.getLogger(__name__)

# EVM address pattern
_EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

# Solana base58 mint address pattern (32-44 chars; base58 excludes 0, O, I, l)
_SOLANA_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# CoinGecko free-tier search endpoint
COINGECKO_SEARCH_URL = "https://api.coingecko.com/api/v3/search?query={symbol}"


# =============================================================================
# Constants
# =============================================================================

# Default timeout for on-chain lookups (seconds)
DEFAULT_ONCHAIN_TIMEOUT = 10.0

# Rate limiting: max on-chain lookups per second
DEFAULT_RATE_LIMIT = 10  # lookups per second

# Marker prefix for ambiguous-symbol errors returned via gRPC NOT_FOUND.
# The resolver looks for this prefix in the error details string to
# distinguish ambiguity (which should raise AmbiguousTokenError client-side
# with the candidate list) from a genuine "not found" (which is safe to
# negative-cache for 5 minutes).
AMBIGUOUS_SYMBOL_MARKER = "AMBIGUOUS_SYMBOL"


# =============================================================================
# Protocol symbol detection (Pendle / Aave)
# =============================================================================

# Prefixes that identify a Pendle-authored token symbol.  Bare-bones
# startswith check: Pendle symbols are consistently namespaced as
# ``PT-...``, ``YT-...``, ``SY-...``, or ``LP-...`` (plus the legacy
# ``PENDLE-LPT`` for pool tokens).  Anything matching these patterns goes
# through the Pendle API tier before falling back to CoinGecko/DexScreener.
_PENDLE_SYMBOL_PREFIXES: tuple[str, ...] = ("PT-", "YT-", "SY-", "LP-")


def _looks_like_pendle_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` follows the Pendle PT/YT/SY/LP naming convention."""
    if not symbol:
        return False
    upper = symbol.upper()
    if upper.startswith(_PENDLE_SYMBOL_PREFIXES):
        return True
    # Legacy pool token naming, seen on early markets.
    return upper == "PENDLE-LPT"


# Prefixes for Aave v3 receipt tokens.  Aave uses a chain abbreviation
# (``aEth``, ``aArb``, ...) plus an optional deployment tag
# (``aEthLido``, ``aEthEtherFi``, ``aHorRwa``) before the underlying
# asset symbol.  Variable-debt tokens carry the ``variableDebt`` prefix
# instead of ``a``.  Anything matching these patterns gets routed to the
# Aave API tier before falling back to CoinGecko/DexScreener.
_AAVE_SYMBOL_PREFIXES: tuple[str, ...] = (
    "AETH",  # aEthUSDC, aEthLidoWETH, aEthEtherFiweETH, ...
    "AARB",  # Arbitrum
    "AOPT",  # Optimism
    "ABAS",  # Base
    "ABNB",  # BSC
    "APOL",  # Polygon
    "AAVA",  # Avalanche
    "AGNO",  # Gnosis
    "ALIN",  # Linea
    "AHORRWA",  # Horizon RWA deployment on ethereum
    "VARIABLEDEBT",  # vToken: variableDebtEthUSDC, variableDebtArbUSDT, ...
)


def _looks_like_aave_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` follows the Aave v3 aToken / vToken naming."""
    if not symbol:
        return False
    return symbol.upper().startswith(_AAVE_SYMBOL_PREFIXES)


def _looks_like_compound_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` looks like a Compound v3 (Comet) cToken.

    Comet markets are consistently named ``c<BASE>v3`` — ``cUSDCv3``,
    ``cWETHv3``, ``cWstETHv3``, etc. — on every deployment. Matches
    the ``c``-prefix case-insensitively (so ``cUSDCv3``, ``CUSDCV3``,
    and ``cusdcv3`` all qualify — ``lookup_by_symbol`` is case-
    insensitive anyway, so the predicate has no business being stricter
    than the lookup). The ``v3`` suffix check keeps out governance
    tokens like ``COMP``, Aave aTokens (``aEth...``), and unrelated
    c-prefixed tokens (``cbBTC``, ``crvUSD``) that don't terminate in
    ``v3``.
    """
    if not symbol or len(symbol) < 4:
        return False
    upper = symbol.upper()
    return upper.startswith("C") and upper.endswith("V3")


def _looks_like_beefy_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` looks like a Beefy mooToken.

    Beefy's active vaults consistently prefix share-token symbols with
    ``moo`` — ``mooCurveUSDC-USDf``, ``mooAaveV3WETH``, ``MooSkyUSDS_SPK``,
    etc.  False positives are cheap (just a dict miss before falling
    through to CoinGecko/DexScreener), so we keep the check loose:
    any symbol whose uppercased form starts with ``MOO`` qualifies.
    """
    if not symbol:
        return False
    return symbol.upper().startswith("MOO")


def _looks_like_yearn_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` looks like a Yearn yvToken.

    Yearn's v2 and v3 vaults share a strict ``yv<...>`` naming scheme
    (``yvUSDC``, ``yvDAI``, ``yvCurve-stETH-frxETH-f``, ...).  Tight
    prefix check — any false positive costs a dict miss and falls
    through to CoinGecko/DexScreener.
    """
    if not symbol:
        return False
    return symbol.upper().startswith("YV")


def _looks_like_fluid_symbol(symbol: str) -> bool:
    """Return True if ``symbol`` looks like a Fluid fToken.

    Fluid's canonical shape is ``f`` + underlying symbol — ``fUSDC``,
    ``fWETH``, ``fGHO``, ``fwstETH``, ``fARB``, etc. The predicate
    matches the ``f`` prefix case-insensitively (so ``fUSDC``, ``FUSDC``,
    and ``fusdc`` all qualify) because ``FluidMarketLookup.lookup_by_symbol``
    is already case-insensitive — a stricter gate here would skip the
    Fluid tier for lowercase or all-caps user input. Symbols like
    ``FRAX`` / ``FXS`` still drop out at the dict-miss step; a false
    positive costs one dict miss, which is acceptable.
    """
    if not symbol or len(symbol) < 2:
        return False
    return symbol[:1].casefold() == "f"


# =============================================================================
# Chain enum helper
# =============================================================================


# Pre-built lookup: lowercased enum value → Chain member.  The protocol
# lookup services return ``meta.chain`` as a lowercased string (the same
# key they use to index tokens per chain), but ``Chain`` enum *values*
# are uppercase ("ETHEREUM", "ARBITRUM", ...).  Naive ``Chain(meta.chain)``
# therefore always raises ``ValueError`` for lowercase inputs, silently
# falling back to whatever the caller uses as its default — which in the
# ``_build_resolved_from_*`` helpers is ``Chain.ETHEREUM``, meaning every
# non-ethereum protocol resolution would stamp the wrong chain onto the
# returned ``ResolvedToken``.  This helper makes the lookup explicit and
# case-insensitive.
def _resolve_chain_enum(name: str) -> Chain:
    """Return the ``Chain`` enum for a lowercased chain name.

    Accepts any case ("ethereum", "ETHEREUM", "Ethereum") and falls back
    to ``Chain.ETHEREUM`` for unknown / empty names.  Callers expect the
    fallback so they can still construct a ``ResolvedToken`` even when a
    protocol lookup returns a chain we don't recognize (e.g. a newer
    Pendle/Morpho deployment on a chain that isn't in our ``Chain`` enum
    yet).
    """
    if not name:
        return Chain.ETHEREUM
    try:
        return Chain[name.upper()]
    except KeyError:
        return Chain.ETHEREUM


# =============================================================================
# Rate Limiter
# =============================================================================


class TokenRateLimiter:
    """Simple token bucket rate limiter for on-chain lookups.

    Prevents RPC abuse by limiting the number of on-chain lookups per second.
    Uses a sliding window approach for smooth rate limiting.
    """

    def __init__(self, max_rate: int = DEFAULT_RATE_LIMIT):
        """Initialize rate limiter.

        Args:
            max_rate: Maximum lookups per second
        """
        self._max_rate = max_rate
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire permission for an on-chain lookup.

        Returns:
            True if permitted, False if rate limited
        """
        async with self._lock:
            now = time.monotonic()

            # Remove timestamps older than 1 second
            self._timestamps = [t for t in self._timestamps if now - t < 1.0]

            # Check if we're at the limit
            if len(self._timestamps) >= self._max_rate:
                return False

            # Record this lookup
            self._timestamps.append(now)
            return True

    async def wait_and_acquire(self, timeout: float = 1.0) -> bool:
        """Wait until we can acquire permission, with timeout.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timed out
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if await self.acquire():
                return True
            await asyncio.sleep(0.1)
        return False


# =============================================================================
# TokenService Implementation
# =============================================================================


class TokenServiceServicer(gateway_pb2_grpc.TokenServiceServicer):
    """Implements TokenService gRPC interface.

    Provides token resolution and metadata discovery for strategy containers:
    - ResolveToken: Cached/static resolution by symbol or address
    - GetTokenMetadata: On-chain ERC20 contract queries
    - GetTokenDecimals: Lightweight decimals-only lookup
    - BatchResolveTokens: Batch resolution for multiple tokens
    """

    def __init__(
        self,
        settings: GatewaySettings,
        onchain_timeout: float = DEFAULT_ONCHAIN_TIMEOUT,
        rate_limit: int = DEFAULT_RATE_LIMIT,
    ):
        """Initialize TokenService.

        Args:
            settings: Gateway settings with network configuration
            onchain_timeout: Timeout for on-chain lookups in seconds
            rate_limit: Maximum on-chain lookups per second
        """
        self.settings = settings
        self._onchain_timeout = onchain_timeout
        self._rate_limiter = TokenRateLimiter(max_rate=rate_limit)

        # Lazy-initialized OnChainLookup instances per chain
        self._onchain_lookups: dict[str, OnChainLookup] = {}
        self._lookups_lock = asyncio.Lock()

        # Get the shared TokenResolver instance (no gateway client for circular ref)
        self._resolver = get_token_resolver()

        # Protocol lookup singletons live at module scope in each
        # ``*_lookup.py`` file and are accessed via ``_get_<protocol>``
        # below. We deliberately do NOT cache an instance on ``self``
        # here: each accessor round-trips through the module factory,
        # which owns the singleton + retry-after-failure backoff so a
        # transient first-load failure can recover without a gateway
        # restart.

        # SPL mint RPC lookup (Solana on-chain fallback for any valid mint,
        # including long-tail tokens Jupiter's curated list doesn't cover).
        # Lazy-initialised because the Solana RPC URL isn't needed until a
        # Solana resolution request lands.
        self._spl_lookup: SplMintLookup | None = None
        self._spl_lookup_lock = asyncio.Lock()

        logger.debug(
            "TokenService initialized",
            extra={
                "onchain_timeout": onchain_timeout,
                "rate_limit": rate_limit,
            },
        )

    async def _get_onchain_lookup(self, chain: str) -> OnChainLookup:
        """Get or create OnChainLookup for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            OnChainLookup instance for the chain
        """
        async with self._lookups_lock:
            if chain not in self._onchain_lookups:
                network = self.settings.network
                rpc_url = get_rpc_url(chain, network=network)
                self._onchain_lookups[chain] = OnChainLookup(
                    rpc_url=rpc_url,
                    timeout=self._onchain_timeout,
                )
                logger.debug(f"Created OnChainLookup for {chain} (network={network})")

            return self._onchain_lookups[chain]

    def _resolved_to_response(
        self,
        resolved: ResolvedToken,
        success: bool = True,
        error: str = "",
    ) -> gateway_pb2.TokenMetadataResponse:
        """Convert ResolvedToken to gRPC response.

        Args:
            resolved: Resolved token data
            success: Whether resolution succeeded
            error: Error message if failed

        Returns:
            TokenMetadataResponse protobuf message
        """
        return gateway_pb2.TokenMetadataResponse(
            success=success,
            error=error,
            symbol=resolved.symbol,
            address=resolved.address,
            decimals=resolved.decimals,
            name=resolved.name or "",
            is_verified=resolved.is_verified,
            source=resolved.source,
        )

    def _metadata_to_response(
        self,
        metadata: TokenMetadata,
        success: bool = True,
        error: str = "",
        *,
        source: str = "on_chain",
    ) -> gateway_pb2.TokenMetadataResponse:
        """Convert TokenMetadata to gRPC response.

        Args:
            metadata: On-chain token metadata
            success: Whether lookup succeeded
            error: Error message if failed
            source: Provenance of the metadata ("on_chain", "coingecko_dynamic",
                "dexscreener_dynamic"). The resolver persists this on the
                ResolvedToken so observability can distinguish dynamic lookups
                from address-only on-chain queries.

        Returns:
            TokenMetadataResponse protobuf message
        """
        return gateway_pb2.TokenMetadataResponse(
            success=success,
            error=error,
            symbol=metadata.symbol,
            address=metadata.address,
            decimals=metadata.decimals,
            name=metadata.name or "",
            is_verified=False,  # Dynamic lookups are not verified
            source=source,
        )

    def _error_response(self, error: str) -> gateway_pb2.TokenMetadataResponse:
        """Create error response.

        Args:
            error: Error message

        Returns:
            TokenMetadataResponse with error
        """
        return gateway_pb2.TokenMetadataResponse(
            success=False,
            error=error,
            symbol="",
            address="",
            decimals=0,
            name="",
            is_verified=False,
            source="",
        )

    # ------------------------------------------------------------------
    # Protocol lookup accessors
    #
    # Each accessor delegates to the corresponding module-level factory
    # (``get_pendle_lookup``, ``get_aave_lookup``, ...).  Do NOT cache the
    # returned instance on ``self`` here — the factory is idempotent and
    # owns both the singleton lifecycle and the retry-after-failure
    # backoff (``ProtocolTokenLookup._load`` re-enters after the backoff
    # window passes).  A per-instance cache would pin a failed first load
    # and silently disable retries until the gateway restarts.
    # ------------------------------------------------------------------

    async def _get_jupiter(self) -> Any:
        """Get the JupiterTokenLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.jupiter_token_lookup import get_jupiter_lookup

        return await get_jupiter_lookup()

    async def _get_pendle(self) -> Any:
        """Get the PendleMarketLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.pendle_market_lookup import get_pendle_lookup

        return await get_pendle_lookup()

    async def _get_aave(self) -> Any:
        """Get the AaveMarketLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.aave_market_lookup import get_aave_lookup

        return await get_aave_lookup()

    async def _get_morpho(self) -> Any:
        """Get the MorphoVaultLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.morpho_vault_lookup import get_morpho_lookup

        return await get_morpho_lookup()

    async def _get_compound(self) -> Any:
        """Get the CompoundMarketLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.compound_market_lookup import get_compound_lookup

        return await get_compound_lookup()

    async def _get_beefy(self) -> Any:
        """Get the BeefyVaultLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.beefy_vault_lookup import get_beefy_lookup

        return await get_beefy_lookup()

    async def _get_yearn(self) -> Any:
        """Get the YearnVaultLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.yearn_vault_lookup import get_yearn_lookup

        return await get_yearn_lookup()

    async def _get_fluid(self) -> Any:
        """Get the FluidMarketLookup singleton (factory handles retry/load)."""
        from almanak.gateway.services.fluid_market_lookup import get_fluid_lookup

        return await get_fluid_lookup()

    async def _get_spl_lookup(self) -> SplMintLookup:
        """Get (or lazily create) the SplMintLookup for Solana.

        Reuses the gateway's standard RPC configuration via ``get_rpc_url`` —
        same URL the ExecutionService already uses for signing and submission,
        so there's no new config surface.
        """
        if self._spl_lookup is not None:
            return self._spl_lookup
        async with self._spl_lookup_lock:
            if self._spl_lookup is None:
                rpc_url = get_rpc_url("solana", network=self.settings.network)
                self._spl_lookup = SplMintLookup(rpc_url=rpc_url, timeout=self._onchain_timeout)
            return self._spl_lookup

    def _build_resolved_from_spl(self, info: SplMintInfo) -> ResolvedToken:
        """Build a ResolvedToken from on-chain SPL mint metadata.

        Symbol and name are not stored in the mint account itself — those live
        in off-chain registries or (sometimes) Metaplex metadata. We use the
        mint address as the canonical symbol: unique, unambiguous, and honest
        about what we know. Consumers that need a human-readable symbol should
        pass it in via ``register_token()`` alongside the Edge-provided address.
        """
        from datetime import UTC, datetime

        from almanak.core.enums import Chain
        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = Chain.SOLANA
        return ResolvedToken(
            symbol=info.address,  # mint address as stable identifier
            address=info.address,
            decimals=info.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=info.address,
            bridge_type=BridgeType.NATIVE,
            source="spl_onchain",
            is_verified=False,  # on-chain read, no off-chain attestation
            resolved_at=datetime.now(UTC),
        )

    async def _try_spl_mint_rpc_lookup(self, mint: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Resolve a Solana mint via direct SPL mint account RPC read.

        This is the last-resort fallback after Jupiter. Works for any valid
        SPL/Token-2022 mint, regardless of whether it's in Jupiter's curated
        list.

        Returns:
            ``TokenMetadataResponse`` on success, ``None`` on a definitive
            miss (account does not exist, wrong owner, malformed data,
            uninitialized mint, decimals out of range).

        Raises:
            TimeoutError: SPL RPC call timed out.
            SolanaRpcError: SPL RPC returned an error.
            Exception: Other network / transport failures.

        Transient errors propagate so the caller can emit the appropriate
        gRPC status and avoid negative-caching a mint that is actually
        valid but temporarily unreachable.
        """
        lookup = await self._get_spl_lookup()
        info = await lookup.lookup(mint)
        if info is None:
            return None

        # Route through _cache_discovered_token so the first-write-wins
        # provenance guard applies: an existing higher-ranked entry
        # (static=100, coingecko_dynamic=60, on_chain=50, jupiter=30) is
        # preserved, while unranked or lower-ranked entries are replaced.
        # Without this, a later SPL fallback for a mint that was already
        # resolved via Jupiter would silently replace a real symbol with
        # the mint address.
        metadata = TokenMetadata(
            symbol=info.address,  # mint address as stable identifier
            name=None,
            decimals=info.decimals,
            address=info.address,
            is_native=False,
        )
        self._cache_discovered_token(metadata, "solana", source="spl_onchain")

        logger.info(
            "token_onchain_resolved_solana_mint mint=%s decimals=%d owner=%s",
            mint,
            info.decimals,
            info.owner_program,
        )
        resolved = self._build_resolved_from_spl(info)
        return self._resolved_to_response(resolved)

    async def _try_solana_symbol_lookup(self, symbol: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Solana token by symbol via Jupiter token list.

        Returns a TokenMetadataResponse on success, None if not found.
        """
        try:
            jupiter = await self._get_jupiter()
            meta = jupiter.lookup_by_symbol(symbol)
            if meta is None:
                return None

            resolved = self._build_resolved_from_jupiter(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_solana: symbol=%s mint=%s decimals=%d",
                symbol,
                meta.address,
                meta.decimals,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Jupiter symbol lookup failed for %s: %s", symbol, exc)
            return None

    async def _try_solana_mint_lookup(self, mint: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Resolve a Solana mint address with a two-stage fallback.

        Stage 1: Jupiter token list — preferred when present because it gives
            us a human-readable symbol and name.
        Stage 2: Direct SPL mint account RPC read — the safety net that
            guarantees any valid mint resolves with correct decimals, even if
            Jupiter's curated list doesn't know about it. This is what lets
            long-tail tokens (new launches, meme coins) work E2E.

        Returns a TokenMetadataResponse on success, None if both stages miss
        (truly invalid mint) or the RPC is unreachable.
        """
        # Stage 1: Jupiter
        try:
            jupiter = await self._get_jupiter()
            meta = jupiter.lookup_by_mint(mint)
            if meta is not None:
                resolved = self._build_resolved_from_jupiter(meta)
                self._resolver.register(resolved)
                logger.info(
                    "token_dynamic_resolved_solana_mint: mint=%s symbol=%s decimals=%d",
                    mint,
                    meta.symbol,
                    meta.decimals,
                )
                return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Jupiter mint lookup failed for %s: %s", mint, exc)
            # Fall through to SPL RPC — a Jupiter failure must not block the
            # on-chain fallback.

        # Stage 2: SPL mint account RPC read
        return await self._try_spl_mint_rpc_lookup(mint)

    def _build_resolved_from_jupiter(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from JupiterTokenMetadata."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = Chain.SOLANA
        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="jupiter",
            is_verified=True,  # Jupiter is a trusted source
            resolved_at=datetime.now(UTC),
        )

    async def _try_aave_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve an Aave v3 aToken or vToken via the Aave markets API.

        Aave's own GraphQL API is authoritative for these receipt tokens —
        the protocol authors both the API and the contracts it references,
        so the returned (symbol, address, decimals) tuple is trusted the
        same way Jupiter / Pendle are trusted.

        Returns a TokenMetadataResponse on success, None if the lookup
        misses or the Aave API is unavailable.
        """
        try:
            aave = await self._get_aave()
            meta = aave.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_aave(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_aave symbol=%s chain=%s address=%s type=%s underlying=%s",
                symbol,
                chain,
                meta.address,
                meta.token_type,
                meta.underlying_symbol,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Aave symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_aave(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from AaveReserveToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        source = "aave_atoken" if meta.token_type == "A" else "aave_vtoken"

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source=source,
            is_verified=True,  # Aave is a trusted source
            resolved_at=datetime.now(UTC),
        )

    async def _try_fluid_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Fluid fToken via Fluid's per-chain API.

        Fluid's API is the protocol's own authoritative source — same
        trust model as every other protocol tier. Gated behind the
        lowercase-``f`` prefix predicate to avoid loading the vault
        list for unrelated tokens like ``FRAX``.
        """
        try:
            fluid = await self._get_fluid()
            meta = fluid.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_fluid(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_fluid symbol=%s chain=%s address=%s underlying=%s",
                symbol,
                chain,
                meta.address,
                meta.underlying_symbol,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Fluid symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_fluid(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from FluidMarketToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="fluid_ftoken",
            is_verified=True,  # Fluid-authored contracts; Fluid API is trusted
            resolved_at=datetime.now(UTC),
        )

    async def _try_yearn_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Yearn yvToken via ydaemon.

        ydaemon is Yearn-authored, authoritative for every yvToken
        address and decimals. Gated behind the ``yv`` prefix predicate.

        Returns a TokenMetadataResponse on success, None if the lookup
        misses or the ydaemon endpoint is unavailable.
        """
        try:
            yearn = await self._get_yearn()
            meta = yearn.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_yearn(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_yearn symbol=%s chain=%s address=%s underlying=%s kind=%s ver=%s",
                symbol,
                chain,
                meta.address,
                meta.underlying_symbol,
                meta.kind,
                meta.version,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Yearn symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_yearn(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from YearnVaultToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="yearn_vault",
            is_verified=True,  # Yearn-authored contracts; ydaemon is trusted
            resolved_at=datetime.now(UTC),
        )

    async def _try_beefy_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Beefy mooToken via the Beefy vaults API.

        Beefy's API is the protocol's own authoritative source — same
        trust model as every other protocol tier. Gated behind the
        ``moo`` prefix predicate so unrelated symbols never trigger
        the initial vault-list fetch.

        Returns a TokenMetadataResponse on success, None if the lookup
        misses or the Beefy API is unavailable.
        """
        try:
            beefy = await self._get_beefy()
            meta = beefy.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_beefy(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_beefy symbol=%s chain=%s address=%s underlying=%s platform=%s",
                symbol,
                chain,
                meta.address,
                meta.underlying_symbol,
                meta.platform,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Beefy symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_beefy(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from BeefyVaultToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="beefy_vault",
            is_verified=True,  # Beefy-authored contracts; Beefy API is trusted
            resolved_at=datetime.now(UTC),
        )

    async def _try_compound_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Compound v3 (Comet) cToken via the aggregator JSON.

        The aggregator mirrors Compound's own deployment configs across
        every Comet network in one HTTP call.  Compound-authored
        contracts + Compound-recommended source ⇒ trusted, same model
        as Pendle / Aave / Morpho.

        Returns a TokenMetadataResponse on success, None if the lookup
        misses or the aggregator is unavailable.
        """
        try:
            compound = await self._get_compound()
            meta = compound.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_compound(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_compound symbol=%s chain=%s address=%s underlying=%s",
                symbol,
                chain,
                meta.address,
                meta.underlying_symbol,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Compound symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_compound(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from CompoundMarketToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="compound_ctoken",
            is_verified=True,  # Compound-authored contracts; aggregator is trusted
            resolved_at=datetime.now(UTC),
        )

    async def _try_morpho_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Morpho vault share token via the Morpho vaults API.

        Morpho's GraphQL API is authoritative for whitelisted vault
        addresses — same trust model used for Jupiter / Pendle / Aave.
        Vault symbols are curator-chosen (``gtUSDC``, ``sparkUSDCbc``,
        ``kpk_USDC_Prime``, ...) so there is no prefix predicate to
        gate this tier; callers invoke it on every EVM symbol miss.

        Returns a TokenMetadataResponse on success, None if the lookup
        misses or the Morpho API is unavailable.
        """
        try:
            morpho = await self._get_morpho()
            meta = morpho.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_morpho(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_morpho symbol=%s chain=%s address=%s underlying=%s",
                symbol,
                chain,
                meta.address,
                meta.underlying_symbol,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Morpho symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_morpho(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from MorphoVaultToken."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source="morpho_vault",
            is_verified=True,  # Morpho whitelisted vaults are a trusted source
            resolved_at=datetime.now(UTC),
        )

    async def _try_pendle_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve a Pendle PT/YT/SY/LP token via the Pendle assets API.

        The Pendle v1 ``/assets/all`` endpoint is authoritative for these
        derivative tokens: Pendle authors both the API and the contracts it
        references, so the returned (symbol, address, decimals) tuple is
        trusted the same way Jupiter's list is trusted for Solana.

        Returns a TokenMetadataResponse on success, None if the lookup misses
        or the Pendle API is unavailable.
        """
        try:
            pendle = await self._get_pendle()
            meta = pendle.lookup_by_symbol(symbol, chain)
            if meta is None:
                return None

            resolved = self._build_resolved_from_pendle(meta)
            self._resolver.register(resolved)
            logger.info(
                "token_dynamic_resolved_pendle symbol=%s chain=%s address=%s type=%s",
                symbol,
                chain,
                meta.address,
                meta.token_type,
            )
            return self._resolved_to_response(resolved)
        except Exception as exc:
            logger.warning("Pendle symbol lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

    def _build_resolved_from_pendle(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from PendleTokenMetadata."""
        from datetime import UTC, datetime

        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

        chain_enum = _resolve_chain_enum(meta.chain)

        return ResolvedToken(
            symbol=meta.symbol,
            address=meta.address,
            decimals=meta.decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=meta.name or None,
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol=meta.symbol,
            bridge_type=BridgeType.NATIVE,
            source=f"pendle_{meta.token_type.lower()}",
            is_verified=True,  # Pendle is a trusted source
            resolved_at=datetime.now(UTC),
        )

    async def _try_evm_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:  # noqa: C901
        """Try to resolve an EVM token by symbol via Pendle, Aave, Compound, Beefy, Yearn, Morpho, CoinGecko, then DexScreener.

        Resolution tiers:
        1. Pendle markets (PT-/YT-/SY-/LP- tokens).
        2. Aave v3 reserves (aToken / vToken symbols).
        3. Compound v3 markets (``c<BASE>v3`` cToken symbols).
        4. Beefy vaults (``moo<...>`` share tokens, active only).
        5. Yearn vaults (``yv<...>`` share tokens, v2 + v3).
        6. Fluid fTokens (lending receipts like ``fUSDC``, ``fWETH``).
        7. Morpho whitelisted vaults (ERC4626 share tokens — curator-
           chosen symbols that neither CoinGecko nor DexScreener surface).
        8. CoinGecko free-tier search.
        9. DexScreener symbol search (4-gate scam-resistance).

        For CoinGecko/DexScreener results, an on-chain ERC20 lookup confirms
        decimals and name. Pendle/Aave/Compound/Beefy/Yearn/Morpho responses
        are trusted directly — same as Jupiter for Solana — because the
        protocol authors both the API/data source and the contracts it
        references.

        Returns:
            TokenMetadataResponse on success, None if no source produced
            a confirmable address.

        Raises:
            AmbiguousTokenError: DexScreener found multiple liquid contracts
                claiming ``symbol`` on ``chain`` with no dominant leader. The
                caller should surface the message so strategy authors can
                disambiguate with an explicit address.
        """
        # Tier 0: Pendle (PT/YT/SY symbols — these never appear on CoinGecko
        # or DexScreener in a useful way, and the /assets/all endpoint is cheap)
        if _looks_like_pendle_symbol(symbol):
            pendle_response = await self._try_pendle_symbol_lookup(symbol, chain)
            if pendle_response is not None:
                return pendle_response

        # Tier 0b: Aave v3 (aToken / vToken symbols — same rationale:
        # CoinGecko/DexScreener don't list receipt tokens usefully, and
        # Aave's /markets query is cheap and gated behind a prefix check).
        if _looks_like_aave_symbol(symbol):
            aave_response = await self._try_aave_symbol_lookup(symbol, chain)
            if aave_response is not None:
                return aave_response

        # Tier 0c: Compound v3 (c<BASE>v3 cTokens — tight prefix check
        # avoids loading the aggregator for unrelated tokens).
        if _looks_like_compound_symbol(symbol):
            compound_response = await self._try_compound_symbol_lookup(symbol, chain)
            if compound_response is not None:
                return compound_response

        # Tier 0d: Beefy vaults (moo* mooTokens — prefix check keeps
        # unrelated symbols from loading the vault list).
        if _looks_like_beefy_symbol(symbol):
            beefy_response = await self._try_beefy_symbol_lookup(symbol, chain)
            if beefy_response is not None:
                return beefy_response

        # Tier 0e: Yearn vaults (yv* yvTokens — tight prefix check).
        if _looks_like_yearn_symbol(symbol):
            yearn_response = await self._try_yearn_symbol_lookup(symbol, chain)
            if yearn_response is not None:
                return yearn_response

        # Tier 0f: Fluid fTokens (lowercase-f prefix + uppercase suffix
        # distinguishes from ``FRAX``/``FXS``-style tokens).
        if _looks_like_fluid_symbol(symbol):
            fluid_response = await self._try_fluid_symbol_lookup(symbol, chain)
            if fluid_response is not None:
                return fluid_response

        # Tier 0g: Morpho vaults (curator-chosen symbols — no clean prefix
        # predicate, so we always consult the in-memory index. First call
        # triggers a one-time API fetch + cache; subsequent calls are
        # O(1) dict lookups.)
        morpho_response = await self._try_morpho_symbol_lookup(symbol, chain)
        if morpho_response is not None:
            return morpho_response

        # Tier 1: CoinGecko (if the chain is listed)
        platform = COINGECKO_PLATFORM_IDS.get(chain.lower())
        if platform:
            try:
                cg_address = await self._coingecko_find_address(symbol, platform)
                if cg_address:
                    cg_metadata = await self._confirm_address_on_chain(
                        cg_address,
                        chain,
                        expected_symbol=symbol,
                    )
                    if cg_metadata is not None:
                        self._cache_discovered_token(cg_metadata, chain, source="coingecko_dynamic")
                        logger.info(
                            "token_dynamic_resolved_evm symbol=%s chain=%s address=%s decimals=%d source=coingecko",
                            symbol,
                            chain,
                            cg_address,
                            cg_metadata.decimals,
                        )
                        return self._metadata_to_response(cg_metadata, source="coingecko_dynamic")
            except Exception as exc:
                logger.warning("CoinGecko symbol lookup failed for %s on %s: %s", symbol, chain, exc)

        # Tier 2: DexScreener (primary source on non-CoinGecko chains and new launches)
        if chain_slug_for(chain) is None:
            return None

        try:
            ds_result = await dexscreener_find_token_address(symbol, chain)
        except AmbiguousTokenError:
            # Propagate so ResolveToken can surface the candidate list.
            raise
        except DexScreenerError as exc:
            logger.warning("DexScreener API error for %s on %s: %s", symbol, chain, exc)
            return None
        except Exception as exc:
            logger.warning("DexScreener lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

        if ds_result is None:
            return None

        ds_metadata = await self._confirm_address_on_chain(
            ds_result.address,
            chain,
            expected_symbol=symbol,
        )
        if ds_metadata is None:
            logger.warning(
                "dexscreener_onchain_confirm_failed symbol=%s chain=%s address=%s",
                symbol,
                chain,
                ds_result.address,
            )
            return None

        self._cache_discovered_token(ds_metadata, chain, source="dexscreener_dynamic")
        logger.info(
            "token_dynamic_resolved_evm symbol=%s chain=%s address=%s decimals=%d source=dexscreener liq=%.0f vol24h=%.0f pair_url=%s",
            symbol,
            chain,
            ds_result.address,
            ds_metadata.decimals,
            ds_result.liquidity_usd,
            ds_result.volume_24h_usd,
            ds_result.pair_url or "",
        )
        return self._metadata_to_response(ds_metadata, source="dexscreener_dynamic")

    async def _confirm_address_on_chain(
        self,
        address: str,
        chain: str,
        *,
        expected_symbol: str | None = None,
    ) -> TokenMetadata | None:
        """Confirm a dynamically-discovered address via on-chain ERC20 lookup.

        Every dynamic-path lookup also passes through the shared RPC rate
        limiter so a burst of unknown-symbol queries cannot exhaust the
        Alchemy budget or push the provider into 429s.

        Args:
            address: EVM contract address to confirm.
            chain: Chain name.
            expected_symbol: If provided, the on-chain ``symbol()`` reading
                must case-insensitively match this value. A mismatch
                indicates the external data source (DexScreener or
                CoinGecko) returned a contract whose on-chain identity
                does not match the requested symbol — treat as untrusted
                and return ``None``. This is the integrity check that
                prevents a scam contract reporting ``symbol() = "USDC"``
                on chain from being silently accepted when a different
                symbol was requested.

        Returns:
            TokenMetadata if confirmation succeeds, None otherwise.
        """
        # Gate on the same RPC rate limiter used by GetTokenMetadata. Bursts
        # of unknown-symbol resolves must not bypass the budget.
        if not await self._rate_limiter.wait_and_acquire(timeout=2.0):
            logger.warning(
                "onchain_confirm_rate_limited address=%s chain=%s",
                address,
                chain,
            )
            return None

        try:
            lookup = await self._get_onchain_lookup(chain)
            metadata = await asyncio.wait_for(
                lookup.lookup(chain, address),
                timeout=self._onchain_timeout,
            )
        except Exception as exc:
            logger.warning("On-chain confirm failed for %s on %s: %s", address, chain, exc)
            return None

        if metadata is None:
            return None

        # Identity check: protect against dynamic sources returning a contract
        # whose on-chain ``symbol()`` does not match the requested symbol.
        # Without this, a scam contract that reports ``symbol() = "USDC"`` on
        # a chain with no static USDC entry would silently resolve to the
        # attacker address for any ``resolve("USDC", chain)`` call.
        if expected_symbol is not None:
            expected = expected_symbol.strip().casefold()
            got = (metadata.symbol or "").strip().casefold()
            if expected != got:
                logger.warning(
                    "onchain_symbol_mismatch requested=%s got=%s address=%s chain=%s",
                    expected_symbol,
                    metadata.symbol,
                    address,
                    chain,
                )
                return None

        return metadata

    async def _coingecko_find_address(self, symbol: str, platform: str) -> str | None:
        """Search CoinGecko for a token symbol and return its address on the given platform.

        Uses the free-tier /search endpoint.  Rate limit is ~30 req/min; callers
        rely on the static-registry cache to avoid repeated calls for the same token.
        """
        try:
            import aiohttp

            from almanak.gateway.utils.ssl_context import build_ssl_context

            url = COINGECKO_SEARCH_URL.format(symbol=_url_quote(symbol, safe=""))
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.debug("CoinGecko search HTTP %d for symbol=%s", resp.status, symbol)
                        return None
                    data = await resp.json(content_type=None)

            coins = data.get("coins", [])
            symbol_upper = symbol.upper()

            # Walk candidates in market-cap-rank order (best match first)
            for coin in coins:
                if coin.get("symbol", "").upper() != symbol_upper:
                    continue

                # Fetch the coin details to get platform addresses
                coin_id = coin.get("id")
                if not coin_id:
                    continue

                address = await self._coingecko_get_platform_address(coin_id, platform)
                if address:
                    return address

            return None

        except Exception as exc:
            logger.warning("CoinGecko search error for %s: %s", symbol, exc)
            return None

    async def _coingecko_get_platform_address(self, coin_id: str, platform: str) -> str | None:
        """Fetch the contract address for a coin on a specific platform from CoinGecko."""
        try:
            import aiohttp

            from almanak.gateway.utils.ssl_context import build_ssl_context

            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false"
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)

            platforms = data.get("platforms", {})
            raw_address = platforms.get(platform, "")
            if raw_address and _EVM_ADDRESS_RE.match(raw_address):
                return raw_address.lower()

            return None

        except Exception as exc:
            logger.debug("CoinGecko platform address lookup failed for %s/%s: %s", coin_id, platform, exc)
            return None

    async def ResolveToken(  # noqa: C901
        self,
        request: gateway_pb2.ResolveTokenRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TokenMetadataResponse:
        """Resolve a token by symbol or address.

        Resolution order:
        1. Static registry + caches (fast path via TokenResolver)
        2. For Solana symbols/mints not in registry: Jupiter token list
        3. For EVM symbols not in registry: CoinGecko search + on-chain confirm

        Args:
            request: ResolveTokenRequest with token and chain
            context: gRPC context

        Returns:
            TokenMetadataResponse with token metadata
        """
        token = request.token
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        if not token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Token is required")
            return self._error_response("Token is required")

        try:
            resolved = self._resolver.resolve(token, chain)
            return self._resolved_to_response(resolved)

        except InvalidTokenAddressError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        except TokenNotFoundError:
            pass  # Fall through to dynamic resolution

        except TokenResolutionError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

        except Exception as e:
            logger.error("ResolveToken failed for %s on %s: %s", token, chain, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Dynamic resolution fallback
        is_solana = chain.lower() == "solana"
        # EVM address: 0x-prefixed 42-char hex
        is_evm_address = bool(_EVM_ADDRESS_RE.match(token))
        # Solana mint: base58, 32-44 chars (no 0, O, I, l) -- use strict base58 pattern to
        # avoid false-positives from long symbol names
        is_solana_mint = is_solana and not is_evm_address and bool(_SOLANA_MINT_RE.match(token))

        if is_solana:
            if is_solana_mint:
                # Rate-limit the Solana mint path the same way
                # GetTokenMetadata does. Without this guard, ResolveToken is
                # a second unthrottled entry point into the Solana RPC — a
                # stream of unique base58 mints would sidestep the EVM-side
                # 10/sec budget entirely.
                if not await self._rate_limiter.wait_and_acquire(timeout=2.0):
                    error_msg = "Rate limit exceeded for on-chain lookups"
                    context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                    context.set_details(error_msg)
                    logger.warning("Rate limited Solana mint lookup for %s", token)
                    return self._error_response(error_msg)
                # Catch transient RPC failures so they surface with a
                # retryable gRPC status instead of escaping as an
                # uncategorized INTERNAL and poisoning the client-side
                # negative cache. Mirrors GetTokenMetadata exactly.
                try:
                    result = await self._try_solana_mint_lookup(token)
                except TimeoutError:
                    error_msg = f"SPL mint RPC lookup timed out for {token}"
                    context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                    context.set_details(error_msg)
                    logger.warning(error_msg)
                    return self._error_response(error_msg)
                except Exception as exc:
                    error_msg = f"SPL mint RPC lookup failed for {token}: {exc}"
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details(error_msg)
                    logger.warning(error_msg)
                    return self._error_response(error_msg)
            else:
                # Try Jupiter by symbol
                result = await self._try_solana_symbol_lookup(token)

            if result is not None:
                return result
        else:
            # EVM: dynamic symbol lookup via CoinGecko -> DexScreener.
            # Address lookups go through GetTokenMetadata / on-chain ERC20 instead.
            if not is_evm_address:
                try:
                    result = await self._try_evm_symbol_lookup(token, chain)
                except AmbiguousTokenError as exc:
                    # DexScreener found multiple liquid contracts with no
                    # dominant leader -- surface the candidate list so the
                    # resolver can raise AmbiguousTokenError with the
                    # addresses on the client side. The error payload is
                    # prefixed with AMBIGUOUS_SYMBOL_MARKER so the resolver
                    # can distinguish ambiguity from a plain NOT_FOUND and
                    # avoid poisoning its negative cache on this path.
                    candidates = ",".join(exc.matching_addresses)
                    marker_error = f"{AMBIGUOUS_SYMBOL_MARKER}|addresses={candidates}|{exc}"
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(marker_error)
                    return self._error_response(marker_error)
                if result is not None:
                    return result

        error_msg = f"Token '{token}' not found on {chain} (checked static registry and dynamic resolution)"
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(error_msg)
        return self._error_response(error_msg)

    async def GetTokenMetadata(
        self,
        request: gateway_pb2.GetTokenMetadataRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TokenMetadataResponse:
        """Get on-chain ERC20 metadata for a token address.

        Queries the token contract directly for decimals, symbol, name.
        Results are cached in the gateway-side TokenResolver.

        Args:
            request: GetTokenMetadataRequest with address and chain
            context: gRPC context

        Returns:
            TokenMetadataResponse with on-chain metadata
        """
        address = request.address
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Validate address (chain-aware: accepts EVM hex or Solana base58 based on chain).
        # Using the plain EVM validator here would reject all SPL mints before
        # they ever reach the Solana branch below.
        try:
            address = validate_address_for_chain(address, chain, "address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Fast path: static registry / memory cache / disk cache. Applies
        # uniformly to EVM and Solana so that pre-registered mints
        # (``register_token()``) or previously discovered tokens are served
        # without issuing any external RPC.  ``skip_gateway=True`` keeps the
        # resolution local — the gateway process must not recurse through
        # itself for a cache check.
        try:
            resolved = self._resolver.resolve(address, chain, skip_gateway=True)
            return self._resolved_to_response(resolved)
        except TokenNotFoundError:
            pass  # Fall through to dynamic lookup
        except TokenResolutionError as exc:
            # Ambiguous / invalid-address / resolver-internal failure. Surface
            # explicitly rather than silently falling through to the RPC
            # path — the resolver already evaluated the input and the answer
            # is "can't resolve locally for a non-missing reason".
            logger.warning(
                "token_fastpath_resolver_error address=%s chain=%s error=%s",
                address,
                chain,
                exc,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return self._error_response(str(exc))

        # Rate limit the on-chain / dynamic path. Applies to BOTH the EVM
        # ERC-20 branch and the Solana Jupiter+SPL branch so a strategy that
        # issues a stream of unique unknown addresses cannot bypass the
        # service's 10/sec bucket by simply targeting solana.
        if not await self._rate_limiter.wait_and_acquire(timeout=2.0):
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            error_msg = "Rate limit exceeded for on-chain lookups"
            context.set_details(error_msg)
            logger.warning(f"Rate limited on-chain lookup for {address} on {chain}")
            return self._error_response(error_msg)

        # Solana: route through the Solana-specific lookup chain (Jupiter,
        # then direct SPL mint account RPC read). ERC-20 ABI queries are
        # EVM-only and would just time out against an SPL mint account
        # layout.
        if chain.lower() == "solana":
            try:
                result = await self._try_solana_mint_lookup(address)
            except TimeoutError:
                # Transient: caller must NOT negative-cache.
                context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                error_msg = f"SPL mint RPC lookup timed out for {address}"
                context.set_details(error_msg)
                logger.warning(error_msg)
                return self._error_response(error_msg)
            except Exception as exc:  # SolanaRpcError + network/transport
                # Transient: caller must NOT negative-cache. UNAVAILABLE
                # signals "try again" to the resolver's error handling.
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                error_msg = f"SPL mint RPC lookup failed for {address}: {exc}"
                context.set_details(error_msg)
                logger.warning(error_msg)
                return self._error_response(error_msg)

            if result is not None:
                return result
            # Definitive miss: Jupiter did not know the mint and the SPL
            # mint account either does not exist, has the wrong owner, or
            # fails integrity checks. Safe to negative-cache.
            error_msg = (
                f"Could not resolve Solana mint {address}: not in Jupiter token list "
                f"and the on-chain account is not a valid SPL mint."
            )
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(error_msg)
            return self._error_response(error_msg)

        try:
            # On-chain lookup (EVM)
            lookup = await self._get_onchain_lookup(chain)
            metadata = await asyncio.wait_for(
                lookup.lookup(chain, address),
                timeout=self._onchain_timeout,
            )

            if metadata is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                error_msg = f"Could not fetch metadata for {address} on {chain}"
                context.set_details(error_msg)
                return self._error_response(error_msg)

            # Cache the discovered token in resolver
            self._cache_discovered_token(metadata, chain)

            return self._metadata_to_response(metadata)

        except TimeoutError:
            context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
            error_msg = f"On-chain lookup timed out for {address} on {chain}"
            context.set_details(error_msg)
            logger.warning(error_msg)
            return self._error_response(error_msg)

        except Exception as e:
            logger.error(f"GetTokenMetadata failed for {address} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

    # Provenance hierarchy: a higher-ranked existing cache entry must never
    # be overwritten by a newly-discovered (lower-ranked) one. This is the
    # first-write-wins invariant extended from the static JSON registry into
    # the in-memory/disk cache.
    _SOURCE_RANK: dict[str, int] = {
        "static": 100,
        # Protocol-authoritative sources for receipt / vault share tokens.
        # Each of these comes straight from the protocol's own API (Pendle
        # assets, Aave GraphQL, Compound aggregator, Morpho vaults,
        # Beefy vaults, Yearn ydaemon, Fluid lending) and is trusted at
        # the same level as hand-curated static registry entries — rank
        # them above the generic CoinGecko/DexScreener dynamic sources so
        # a later DexScreener hit on the same symbol can't silently
        # overwrite a Pendle / Aave / Compound / Morpho / Beefy / Yearn /
        # Fluid entry. Using a shared rank (90) because they're equally
        # authoritative for their respective token families; static
        # remains top-ranked (100) for intentional curator overrides.
        "pendle_pt": 90,
        "pendle_yt": 90,
        "pendle_sy": 90,
        "pendle_lp": 90,
        "aave_atoken": 90,
        "aave_vtoken": 90,
        "compound_ctoken": 90,
        "morpho_vault": 90,
        "beefy_vault": 90,
        "yearn_vault": 90,
        "fluid_ftoken": 90,
        "coingecko_dynamic": 60,
        "on_chain": 50,
        "dexscreener_dynamic": 40,
        "jupiter": 30,
        # SPL on-chain fallback has the correct decimals but no off-chain
        # symbol/name (we store the mint address as a stand-in). It ranks
        # below Jupiter so a later Jupiter hit with real metadata can
        # replace an SPL entry. Do not drop it to 0 / unranked — that
        # would block ANY future overwrite and pin the low-quality entry.
        "spl_onchain": 20,
    }

    def _cache_discovered_token(self, metadata: TokenMetadata, chain: str, *, source: str = "on_chain") -> None:
        """Cache a discovered token in the resolver.

        Refuses to overwrite an existing cached entry whose source is ranked
        higher than the incoming one (see ``_SOURCE_RANK``). This prevents
        a later ``dexscreener_dynamic`` lookup from clobbering a prior
        ``coingecko_dynamic`` or ``static`` entry for the same address or
        symbol, which would corrupt long-running gateway state.

        Args:
            metadata: On-chain token metadata
            chain: Chain name
            source: Provenance tag stored on the ResolvedToken (e.g.
                ``"on_chain"``, ``"dexscreener_dynamic"``, ``"coingecko_dynamic"``).
        """
        try:
            from datetime import UTC, datetime

            from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

            # Find Chain enum
            chain_enum = None
            for c in Chain:
                if c.value.lower() == chain.lower():
                    chain_enum = c
                    break

            if chain_enum is None:
                logger.warning(f"Unknown chain {chain} - not caching discovered token")
                return

            incoming_rank = self._SOURCE_RANK.get(source, 0)

            # Check for an existing cache entry under either the address key
            # or the symbol key. Using the resolver's own cache guarantees we
            # see both memory and disk layers. ``skip_gateway=True`` keeps the
            # lookup local (the gateway process must not recurse through
            # itself for a cache existence check).
            def _existing_rank(token: str | None) -> int:
                if not token:
                    return -1
                try:
                    existing = self._resolver.resolve(token, chain, skip_gateway=True)
                except TokenResolutionError:
                    return -1
                existing_source = getattr(existing, "source", "") or ""
                return self._SOURCE_RANK.get(existing_source, 0)

            best_existing = max(_existing_rank(metadata.address), _existing_rank(metadata.symbol))
            # First-write-wins: block BOTH higher-ranked and equal-ranked
            # overwrites. Two dexscreener_dynamic lookups resolving the same
            # symbol to different answers must not silently replace each
            # other — the first one is treated as authoritative for the
            # session; subsequent conflicting results are logged and dropped.
            if best_existing >= incoming_rank and best_existing >= 0:
                logger.info(
                    "token_dynamic_overwrite_blocked symbol=%s address=%s chain=%s "
                    "incoming_source=%s incoming_rank=%d existing_rank=%d",
                    metadata.symbol,
                    metadata.address,
                    chain,
                    source,
                    incoming_rank,
                    best_existing,
                )
                return

            # Create ResolvedToken for caching
            resolved = ResolvedToken(
                symbol=metadata.symbol,
                address=metadata.address,
                decimals=metadata.decimals,
                chain=chain_enum,
                chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
                name=metadata.name,
                coingecko_id=None,
                is_stablecoin=False,
                is_native=metadata.is_native,
                is_wrapped_native=False,
                canonical_symbol=metadata.symbol,
                bridge_type=BridgeType.NATIVE,
                source=source,
                is_verified=False,
                resolved_at=datetime.now(UTC),
            )

            # Register in resolver (which handles caching)
            self._resolver.register(resolved)
            logger.debug(f"Cached discovered token {metadata.symbol} at {metadata.address} on {chain}")

        except Exception as e:
            # Caching failure shouldn't break the response
            logger.warning(f"Failed to cache discovered token: {e}")

    async def GetTokenDecimals(
        self,
        request: gateway_pb2.GetTokenDecimalsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTokenDecimalsResponse:
        """Get token decimals (lightweight endpoint).

        This is a convenience method when only decimals are needed.
        Faster than full resolution as it doesn't need all metadata.

        Args:
            request: GetTokenDecimalsRequest with token and chain
            context: gRPC context

        Returns:
            GetTokenDecimalsResponse with decimals
        """
        token = request.token
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        if not token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Token is required")
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error="Token is required")

        try:
            decimals = self._resolver.get_decimals(chain, token)
            return gateway_pb2.GetTokenDecimalsResponse(success=True, decimals=decimals, error="")

        except TokenNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        except TokenResolutionError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        except Exception as e:
            logger.error(f"GetTokenDecimals failed for {token} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

    async def BatchResolveTokens(
        self,
        request: gateway_pb2.BatchResolveTokensRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BatchResolveTokensResponse:
        """Resolve multiple tokens in a single call.

        More efficient than individual ResolveToken calls for multiple tokens.
        Returns results for all tokens, with individual errors for failures.

        Args:
            request: BatchResolveTokensRequest with tokens and chain
            context: gRPC context

        Returns:
            BatchResolveTokensResponse with list of token metadata
        """
        tokens = list(request.tokens)
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error=str(e),
            )

        if not tokens:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("At least one token is required")
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error="At least one token is required",
            )

        # Validate batch size
        try:
            validate_batch_size(tokens, "tokens")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error=str(e),
            )

        results: list[gateway_pb2.TokenMetadataResponse] = []
        all_success = True

        for token in tokens:
            try:
                # Suppress per-token resolution warnings in batch context to avoid
                # noisy logs for tokens that don't exist on a chain (e.g. USDT on Base)
                resolved = self._resolver.resolve(token, chain, log_errors=False)
                results.append(self._resolved_to_response(resolved))

            except TokenResolutionError as e:
                all_success = False
                results.append(self._error_response(str(e)))

            except Exception as e:
                all_success = False
                logger.error(f"BatchResolveTokens failed for {token} on {chain}: {e}")
                results.append(self._error_response(str(e)))

        return gateway_pb2.BatchResolveTokensResponse(
            success=all_success,
            tokens=results,
            error="" if all_success else "Some tokens failed to resolve",
        )

    async def health_check(self) -> dict[str, Any]:
        """Check the health of the token service.

        Returns a health report with resolver stats, cache status, and gateway connectivity.
        This can be used by the gateway server to report token service health
        via the standard gRPC health check protocol.

        Returns:
            Dict with health status, resolver stats, cache stats, and gateway info
        """
        resolver_stats = self._resolver.stats()
        cache_stats = self._resolver.cache_stats()
        gateway_connected = self._resolver.is_gateway_connected()

        total_lookups = resolver_stats.get("cache_hits", 0) + resolver_stats.get("static_hits", 0)
        error_count = resolver_stats.get("errors", 0)

        # Determine health: degraded if gateway is expected but down, or high error rate
        healthy = True
        status = "serving"
        if total_lookups >= 100 and error_count / max(total_lookups, 1) > 0.1:
            healthy = False
            status = "degraded_high_error_rate"

        return {
            "healthy": healthy,
            "status": status,
            "resolver_stats": resolver_stats,
            "cache_stats": cache_stats,
            "gateway_connected": gateway_connected,
            "onchain_lookups_active": len(self._onchain_lookups),
        }

    async def close(self) -> None:
        """Close the service and release resources."""
        async with self._lookups_lock:
            for lookup in self._onchain_lookups.values():
                await lookup.close()
            self._onchain_lookups.clear()
        async with self._spl_lookup_lock:
            if self._spl_lookup is not None:
                await self._spl_lookup.close()
                self._spl_lookup = None
        logger.info("TokenService closed")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "TokenServiceServicer",
    "TokenRateLimiter",
    "DEFAULT_ONCHAIN_TIMEOUT",
    "DEFAULT_RATE_LIMIT",
]
