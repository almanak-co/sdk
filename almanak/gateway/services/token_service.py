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

from almanak.framework.data.tokens import (
    InvalidTokenAddressError,
    ResolvedToken,
    TokenNotFoundError,
    TokenResolutionError,
    get_token_resolver,
)
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services.dexscreener_lookup import (
    DexScreenerError,
    chain_slug_for,
)
from almanak.gateway.services.dexscreener_lookup import (
    find_token_address as dexscreener_find_token_address,
)
from almanak.gateway.services.onchain_lookup import OnChainLookup, TokenMetadata
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.validation import ValidationError, validate_address, validate_batch_size, validate_chain

logger = logging.getLogger(__name__)

# EVM address pattern
_EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

# Solana base58 mint address pattern (32-44 chars; base58 excludes 0, O, I, l)
_SOLANA_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# CoinGecko free-tier search endpoint
COINGECKO_SEARCH_URL = "https://api.coingecko.com/api/v3/search?query={symbol}"

# CoinGecko platform IDs for each chain
COINGECKO_PLATFORM_IDS: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "base": "base",
    "polygon": "polygon-pos",
    "avalanche": "avalanche",
    "bsc": "binance-smart-chain",
    "sonic": "sonic",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "xlayer": "xlayer",
    "zerog": "zerog",
}


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

        # Jupiter token lookup (Solana dynamic resolution) -- loaded lazily on first use
        self._jupiter: Any = None  # JupiterTokenLookup, typed as Any to avoid import cycle
        self._jupiter_lock = asyncio.Lock()

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

    async def _get_jupiter(self) -> Any:
        """Get (or lazily load) the JupiterTokenLookup singleton."""
        if self._jupiter is not None:
            return self._jupiter
        async with self._jupiter_lock:
            if self._jupiter is None:
                from almanak.gateway.services.jupiter_token_lookup import get_jupiter_lookup

                self._jupiter = await get_jupiter_lookup()
            return self._jupiter

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
        """Try to resolve a Solana mint address via Jupiter token list.

        Returns a TokenMetadataResponse on success, None if not found.
        """
        try:
            jupiter = await self._get_jupiter()
            meta = jupiter.lookup_by_mint(mint)
            if meta is None:
                return None

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
            return None

    def _build_resolved_from_jupiter(self, meta: Any) -> ResolvedToken:
        """Build a ResolvedToken from JupiterTokenMetadata."""
        from datetime import datetime

        from almanak.core.enums import Chain
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
            resolved_at=datetime.now(),
        )

    async def _try_evm_symbol_lookup(self, symbol: str, chain: str) -> gateway_pb2.TokenMetadataResponse | None:
        """Try to resolve an EVM token by symbol via CoinGecko, then DexScreener.

        Resolution tiers:
        1. CoinGecko free-tier search (established tokens with broad listings)
        2. DexScreener symbol search with 4-gate scam-resistance policy
           (new launches and chains CoinGecko does not index)

        For any positive result, an on-chain ERC20 lookup confirms decimals and
        name before the address is returned and cached.

        Returns:
            TokenMetadataResponse on success, None if neither source produced
            a confirmable address.

        Raises:
            AmbiguousTokenError: DexScreener found multiple liquid contracts
                claiming ``symbol`` on ``chain`` with no dominant leader. The
                caller should surface the message so strategy authors can
                disambiguate with an explicit address.
        """
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

            url = COINGECKO_SEARCH_URL.format(symbol=_url_quote(symbol, safe=""))
            async with aiohttp.ClientSession() as session:
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

            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false"
            async with aiohttp.ClientSession() as session:
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

    async def ResolveToken(
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
                # Try Jupiter by mint address
                result = await self._try_solana_mint_lookup(token)
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

        # Validate address
        try:
            address = validate_address(address, "address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Guard: on-chain ERC20 lookup is EVM-only.
        # Solana mints are SPL tokens -- querying them via the EVM ABI would hang
        # for ~30 seconds then fail.  Route Solana addresses through Jupiter instead.
        if chain.lower() == "solana":
            result = await self._try_solana_mint_lookup(address)
            if result is not None:
                return result
            error_msg = (
                f"On-chain ERC20 lookup is not supported for Solana. "
                f"Add '{address}' to the static registry or ensure it is in the Jupiter token list."
            )
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(error_msg)
            return self._error_response(error_msg)

        # Check rate limit
        if not await self._rate_limiter.wait_and_acquire(timeout=2.0):
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            error_msg = "Rate limit exceeded for on-chain lookups"
            context.set_details(error_msg)
            logger.warning(f"Rate limited on-chain lookup for {address} on {chain}")
            return self._error_response(error_msg)

        try:
            # First try static resolution (fast path)
            try:
                resolved = self._resolver.resolve(address, chain)
                return self._resolved_to_response(resolved)
            except TokenNotFoundError:
                pass  # Fall through to on-chain lookup

            # On-chain lookup
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
        "coingecko_dynamic": 60,
        "on_chain": 50,
        "dexscreener_dynamic": 40,
        "jupiter": 30,
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
            from datetime import datetime

            from almanak.core.enums import Chain
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
                resolved_at=datetime.now(),
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
