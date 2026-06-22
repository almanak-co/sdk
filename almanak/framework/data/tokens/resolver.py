"""Token resolver - unified API for all token lookups.

This module provides the TokenResolver class which is the main entry point
for all token resolution in the Almanak framework. It implements a multi-layer
resolution strategy with caching for optimal performance.

Resolution Order:
    1. Memory cache (fastest, <1ms)
    2. Disk cache (fast, <10ms)
    3. Static registry (fast, <5ms)
    4. Gateway on-chain lookup (slower, <500ms, requires gateway connection)

Key Components:
    - TokenResolver: Main resolver class (thread-safe singleton)
    - get_token_resolver(): Get the singleton instance

Performance Targets:
    - Cache hit: <1ms
    - Static registry: <5ms
    - Gateway on-chain: <500ms

Gateway Connection:
    The resolver can optionally connect to a gateway for on-chain token discovery.
    If the gateway is unavailable, static resolution still works (graceful fallback).

    # With gateway (enables on-chain discovery)
    import grpc
    channel = grpc.insecure_channel("localhost:50051")
    resolver = get_token_resolver(gateway_channel=channel)

    # Without gateway (static resolution only)
    resolver = get_token_resolver()

Example:
    from almanak.framework.data.tokens.resolver import get_token_resolver

    resolver = get_token_resolver()

    # Resolve by symbol
    usdc = resolver.resolve("USDC", "arbitrum")
    print(f"{usdc.symbol} has {usdc.decimals} decimals at {usdc.address}")

    # Resolve by address
    token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

    # Get decimals directly
    decimals = resolver.get_decimals("arbitrum", "USDC")

    # Resolve a trading pair
    usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")
"""

import logging
import re
import threading
import time
from collections.abc import Mapping
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from almanak.core.chains import ChainRegistry
from almanak.core.enums import Chain, ChainFamily

from .cache import TokenCacheManager
from .defaults import DEFAULT_TOKENS, NATIVE_SENTINEL, SYMBOL_ALIASES, WRAPPED_NATIVE
from .exceptions import (
    AmbiguousTokenError,
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
)
from .models import CHAIN_ID_MAP, BridgeType, ResolvedToken, Token, normalize_token_address_for_chain

if TYPE_CHECKING:
    import grpc

logger = logging.getLogger(__name__)


def _try_record_metric(func_name: str, *args: Any, **kwargs: Any) -> None:
    """Attempt to record a Prometheus metric, silently ignoring import failures.

    This allows the resolver to work without the gateway metrics module installed
    (e.g., in framework-only deployments or tests).
    """
    try:
        from almanak.gateway import metrics

        func = getattr(metrics, func_name, None)
        if func:
            func(*args, **kwargs)
    except ImportError:
        pass


# Address validation patterns
ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
# Pattern to detect strings that look like addresses (start with 0x and are ~42 chars)
ADDRESS_LIKE_PATTERN = re.compile(r"^0x[a-zA-Z0-9]{38,42}$")
# Solana base58 address pattern (32-44 chars, base58 alphabet: no 0, O, I, l)
SOLANA_ADDRESS_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _is_solana_chain(chain: str | None) -> bool:
    """Return True when ``chain`` resolves to the SOLANA family.

    ``ChainRegistry.try_resolve`` returns ``None`` for unknown names, so
    a missing / unrecognized chain falls through to the EVM branch —
    matches the legacy ``chain.lower() == "solana"`` contract.
    """
    if not chain:
        return False
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor is not None and descriptor.family is ChainFamily.SOLANA


def _is_address(token: str, chain: str | None = None) -> bool:
    """Check if a token string is a valid address.

    If chain is provided, checks format for that chain's family.
    If chain is None, checks if it matches ANY known address format.
    """
    if chain and _is_solana_chain(chain):
        return bool(SOLANA_ADDRESS_PATTERN.match(token))
    if ADDRESS_PATTERN.match(token):
        return True
    # When chain is unspecified, also accept Solana addresses
    if chain is None and SOLANA_ADDRESS_PATTERN.match(token):
        return True
    return False


def _looks_like_address(token: str) -> bool:
    """Check if a token string looks like it's trying to be an address.

    This catches cases like "0xGHIJ..." which are malformed addresses.
    """
    return bool(ADDRESS_LIKE_PATTERN.match(token))


def _validate_address(address: str, chain: str) -> None:
    """Validate an address format for the given chain.

    For EVM chains: must be 0x-prefixed, 42-char hex.
    For Solana: must be 32-44 char base58 (no 0, O, I, l).

    Args:
        address: The address to validate
        chain: Chain name for error context

    Raises:
        InvalidTokenAddressError: If address format is invalid
    """
    if _is_solana_chain(chain):
        if not SOLANA_ADDRESS_PATTERN.match(address):
            raise InvalidTokenAddressError(
                token=address,
                chain=chain,
                reason="Solana address must be 32-44 base58 characters",
            )
        return

    # EVM validation
    if not address.startswith("0x"):
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason="Address must start with '0x'",
        )
    if len(address) != 42:
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason=f"Address must be 42 characters, got {len(address)}",
        )
    if not ADDRESS_PATTERN.match(address):
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason="Address contains invalid hex characters",
        )


def _normalize_address_for_chain(address: str, chain: str) -> str:
    """Normalize an address for comparison/indexing.

    EVM addresses are case-insensitive -> lowercase.
    Solana base58 addresses are case-sensitive -> preserve case.
    """
    return normalize_token_address_for_chain(address, chain)


_STATIC_ADDRESS_CANONICAL_SYMBOLS: dict[tuple[str, str], str] = {
    # Polygon's native gas token was renamed from MATIC to POL. Keep both
    # symbols resolvable, but prefer POL when callers resolve the native
    # sentinel address directly so address->symbol canon matches current naming.
    ("polygon", NATIVE_SENTINEL.lower()): "POL",
}

# Lazy cache of the CoinGecko-ID -> canonical-symbol reverse map.  Populated
# on first call to _normalize_symbol_input(); ~1357 entries built from
# DEFAULT_TOKENS.  Cheap to build but we cache to avoid the cost on every
# resolve() call.
_CG_ID_TO_SYMBOL_CACHE: dict[str, str] | None = None


def _cg_id_to_symbol_map() -> dict[str, str]:
    global _CG_ID_TO_SYMBOL_CACHE
    if _CG_ID_TO_SYMBOL_CACHE is None:
        from almanak.framework.data.tokens.defaults import (
            get_coingecko_id_to_canonical_symbol,
        )

        _CG_ID_TO_SYMBOL_CACHE = get_coingecko_id_to_canonical_symbol()
    return _CG_ID_TO_SYMBOL_CACHE


def _normalize_symbol_input(token: str) -> str:
    """Normalize a symbol input before lookup.

    Callers (Edge, AlmanakCode, CLI users) sometimes ship CoinGecko IDs
    (``"tether"``, ``"usd-coin"``, ``"wrapped-bitcoin"``) in the symbol
    field instead of the canonical symbol. Recognise those up front and
    translate to the canonical symbol (``USDT``, ``USDC``, ``WBTC``) so
    the rest of the resolve cascade can hit the static registry cleanly.

    Also strips leading/trailing whitespace — a cheap guard against
    copy-paste errors in prompts and notebooks.

    Only non-address inputs are considered; address-shaped strings are
    passed through unchanged (those go down the address resolve path).
    Symbol strings that don't look like a CG ID (i.e. not all-lowercase
    or not hyphenated) are returned with just whitespace stripped —
    standard case-insensitive symbol lookup handles the rest.
    """
    stripped = token.strip()
    if not stripped:
        return stripped

    # CG ID shape: all-lowercase, may contain hyphens, no dots/underscores
    # (the registry uses ``.e`` / ``.E`` for bridged symbols — those are
    # symbol-shaped, not ID-shaped, so leave them alone).
    if stripped == stripped.lower() and "." not in stripped and "_" not in stripped:
        canonical = _cg_id_to_symbol_map().get(stripped)
        if canonical:
            return canonical

    return stripped


def _normalize_chain(chain: str | Chain) -> tuple[str, Chain]:
    """Normalize chain input to both string and Chain enum.

    Uses the central resolve_chain_name() for alias resolution.

    Args:
        chain: Chain as string or Chain enum

    Returns:
        Tuple of (chain_name_lower, Chain enum)

    Raises:
        TokenResolutionError: If chain is not recognized
    """
    if isinstance(chain, Chain):
        return chain.value.lower(), chain

    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain)
    except (ValueError, ImportError):
        chain_lower = chain.lower()

    # Try to find matching Chain enum
    for c in Chain:
        if c.value.lower() == chain_lower:
            return chain_lower, c

    raise TokenResolutionError(
        token="",
        chain=chain,
        reason=f"Unknown chain '{chain}'",
        suggestions=[f"Supported chains: {', '.join(c.value.lower() for c in Chain)}"],
    )


def _parse_ambiguous_candidates(error_str: str) -> list[str]:
    """Parse candidate addresses out of an AMBIGUOUS_SYMBOL marker string.

    The gateway encodes ambiguity as
    ``AMBIGUOUS_SYMBOL|addresses=0xA,0xB,0xC|<exc-details>`` inside the gRPC
    error details. This helper pulls the address list so the client-side
    AmbiguousTokenError can carry it into the strategy author's error path.
    """
    for segment in error_str.split("|"):
        if segment.startswith("addresses="):
            raw = segment[len("addresses=") :]
            return [addr for addr in raw.split(",") if addr]
    return []


class TokenResolver:
    """Unified token resolver with multi-layer caching.

    This class provides the main API for token resolution in the Almanak framework.
    It implements a singleton pattern for thread-safe global access.

    Resolution Order:
        1. Memory cache - fastest, O(1)
        2. Disk cache - loads from JSON, promotes to memory
        3. Static registry - DEFAULT_TOKENS from defaults.py
        4. Gateway on-chain lookup - queries ERC20 contracts (if gateway_client provided)

    Thread Safety:
        Uses threading.RLock for all operations. Safe for concurrent access.

    Attributes:
        gateway_client: Optional gateway client for on-chain lookups

    Example:
        resolver = TokenResolver.get_instance()

        # Resolve by symbol
        token = resolver.resolve("USDC", "arbitrum")

        # Resolve by address
        token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

        # Register a custom token
        resolver.register(my_custom_token)
    """

    _instance: "TokenResolver | None" = None
    _instance_lock = threading.Lock()

    def __init__(
        self,
        gateway_client: Any | None = None,
        cache_file: str | None = None,
        gateway_channel: "grpc.Channel | None" = None,
    ) -> None:
        """Initialize the TokenResolver.

        NOTE: Prefer using get_instance() for singleton access.

        Args:
            gateway_client: DEPRECATED - Use gateway_channel instead.
                           Kept for backward compatibility.
            cache_file: Optional path to cache file. Defaults to ~/.almanak/token_cache.json
            gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                            If None, only static resolution is available.
                            On-chain discovery will gracefully fall back to static
                            resolution if the gateway becomes unavailable.
        """
        # Handle backward compatibility - gateway_client is deprecated
        self._gateway_client = gateway_client
        self._gateway_channel = gateway_channel
        self._gateway_stub: Any | None = None  # Lazy initialized TokenServiceStub
        self._gateway_available: bool | None = None  # None = unknown, True/False = cached state
        self._gateway_check_time: float = 0  # Last time we checked gateway availability
        self._cache = TokenCacheManager(cache_file=cache_file)
        self._lock = threading.RLock()

        # Build static registry index for fast lookups
        # Maps: chain_lower -> symbol_upper -> Token
        self._static_registry: dict[str, dict[str, Token]] = {}
        # Maps: chain_lower -> address_lower -> Token
        self._static_address_index: dict[str, dict[str, Token]] = {}

        self._build_static_indices()
        self._register_protocol_metadata_tokens()
        self._refresh_canonical_address_cache_entries()

        # Performance tracking
        self._stats = {
            "cache_hits": 0,
            "static_hits": 0,
            "gateway_lookups": 0,
            "gateway_errors": 0,
            "errors": 0,
            "negative_cache_hits": 0,
        }

        # Negative cache (VIB-2715): remember failed resolutions so we
        # don't burn a 30s gateway timeout on every subsequent attempt
        # for the same (chain, key). Map (chain_lower, key_lower) ->
        # expiry_monotonic_seconds. Symbol + address lookups share the
        # cache since the key space doesn't collide (addresses are 42-
        # char hex or 32-44 char base58; symbols aren't).
        self._negative_cache: dict[tuple[str, str], float] = {}

        # Thread-local flag set by the gateway helpers to distinguish a
        # "definitive not found" (gateway reached and said the token
        # doesn't exist) from a "transient failure" (timeout, UNAVAILABLE,
        # integrity-check reject). Only definitive misses should be
        # cached — a transient outage must not poison the resolver for
        # 5 minutes once the gateway recovers.
        self._gateway_miss_state = threading.local()

        # Defaults first, then env overrides. 5 minutes TTL is long enough
        # that repeat balance/price queries for the same unknown token in
        # one strategy iteration stop hammering the gateway; short enough
        # that newly-listed tokens are picked up within a reasonable
        # window. 10000 is a soft cap — when we insert and the map
        # exceeds this, expired entries are swept before the new write.
        self._negative_cache_ttl_seconds: float = 300.0
        self._negative_cache_max_size: int = 10000

        # Operators can tune both without subclassing. Long-lived
        # platforms (kitchen loop, backtester) may want a longer TTL;
        # tests may want a tiny TTL for fast expiry checks.
        from almanak.config.framework import framework_config_from_env

        fc = framework_config_from_env()
        if fc.token_negative_cache_ttl_s is not None:
            self._negative_cache_ttl_seconds = fc.token_negative_cache_ttl_s
        if fc.token_negative_cache_max is not None:
            self._negative_cache_max_size = fc.token_negative_cache_max

    def _build_static_indices(self) -> None:
        """Build indices for fast static registry lookups.

        Protects against two data-integrity hazards in tokens.json:

        1. **Symbol collisions on the same chain.** Multiple records can
           share ``(chain, symbol)`` when the fetcher adds long-tail tokens
           with chain-suffixed ``var_name`` — e.g. ``MNT`` vs ``MNT_MANTLE``
           on Mantle. Without protection, later records would silently
           shadow hand-curated ones (MNT native sentinel replaced by the
           CoinGecko-indexed ERC20, breaking native auto-wrap).
           **We use first-write-wins and emit a WARNING for every
           subsequent collision.** The JSON file order is therefore the
           ground truth: hand-curated entries come first, bulk imports last.
        2. **Address collisions on the same chain.** Same policy: the
           first record registered for an address wins; subsequent
           duplicates are skipped and logged.

        Records that lose a symbol collision remain findable by address
        (they're still valid ``Token`` records), but cannot be looked up
        by their ambiguous symbol. Consumers of such tokens must pass the
        address explicitly or use ``register_token()``.
        """
        for token in DEFAULT_TOKENS:
            for chain_name in token.chains:
                chain_lower = chain_name.lower()
                symbol_upper = token.symbol.upper()
                address = token.get_address(chain_name)

                # Index by symbol (first-write-wins).
                chain_symbols = self._static_registry.setdefault(chain_lower, {})
                if symbol_upper in chain_symbols:
                    existing = chain_symbols[symbol_upper]
                    if existing is not token:
                        logger.debug(
                            "token_registry_symbol_collision chain=%s symbol=%s "
                            "kept=%s dropped=%s reason=first-write-wins",
                            chain_lower,
                            symbol_upper,
                            existing.get_address(chain_name) or existing.symbol,
                            address or token.symbol,
                        )
                else:
                    chain_symbols[symbol_upper] = token

                # Index by address (first-write-wins; addresses identify
                # one contract uniquely, so a collision here is always a
                # JSON authoring bug rather than legitimate ambiguity).
                if address:
                    addr_key = _normalize_address_for_chain(address, chain_lower)
                    chain_addresses = self._static_address_index.setdefault(chain_lower, {})
                    if addr_key in chain_addresses:
                        existing_addr = chain_addresses[addr_key]
                        if existing_addr is not token:
                            preferred_symbol = _STATIC_ADDRESS_CANONICAL_SYMBOLS.get((chain_lower, addr_key))
                            existing_is_preferred = (
                                preferred_symbol is not None
                                and existing_addr.symbol.upper() == preferred_symbol.upper()
                            )
                            candidate_is_preferred = (
                                preferred_symbol is not None and token.symbol.upper() == preferred_symbol.upper()
                            )
                            if candidate_is_preferred and not existing_is_preferred:
                                chain_addresses[addr_key] = token
                                logger.debug(
                                    "token_registry_address_alias_collision chain=%s address=%s "
                                    "canonical_symbol=%s kept_symbol=%s dropped_symbol=%s "
                                    "reason=canonical-symbol-preference",
                                    chain_lower,
                                    addr_key,
                                    preferred_symbol,
                                    token.symbol,
                                    existing_addr.symbol,
                                )
                                continue
                            if existing_is_preferred:
                                continue
                            logger.warning(
                                "token_registry_address_collision chain=%s address=%s "
                                "kept_symbol=%s dropped_symbol=%s reason=first-write-wins",
                                chain_lower,
                                addr_key,
                                existing_addr.symbol,
                                token.symbol,
                            )
                    else:
                        chain_addresses[addr_key] = token

    def _register_protocol_metadata_tokens(self) -> None:
        """Auto-register synthetic tokens from connector-owned metadata.

        This avoids requiring strategies to manually call ``register_token()``
        for connector-published synthetic tokens. Tokens are indexed by symbol
        (uppercased) and address so both resolve paths work.
        """
        try:
            from almanak.connectors._strategy_protocol_metadata_registry import (
                PROTOCOL_METADATA_REGISTRY,
            )
        except ImportError:
            return

        seen_by_chain: dict[str, set[str]] = {}
        for metadata in PROTOCOL_METADATA_REGISTRY.synthetic_tokens():
            chain = metadata.chain
            chain_lower = chain.lower()
            addr_key = _normalize_address_for_chain(metadata.address, chain_lower)
            seen_addresses = seen_by_chain.setdefault(chain_lower, set())
            if addr_key in seen_addresses:
                continue  # Skip case-variant duplicates
            seen_addresses.add(addr_key)

            token = Token(
                symbol=metadata.symbol,
                name=metadata.symbol,
                decimals=metadata.decimals,
                addresses={chain_lower: metadata.address},
            )
            symbol_upper = metadata.symbol.upper()

            # Respect first-write-wins: if tokens.json (loaded in
            # ``_build_static_indices``) already registered this (chain,
            # symbol) or (chain, address), keep the JSON entry as the source
            # of truth. Connector metadata is useful for tokens the JSON
            # doesn't know about, but must never silently shadow a curated
            # entry.
            chain_symbols = self._static_registry.setdefault(chain_lower, {})
            if symbol_upper not in chain_symbols:
                chain_symbols[symbol_upper] = token
            elif chain_symbols[symbol_upper] is not token:
                logger.debug(
                    "protocol_metadata_symbol_collision chain=%s protocol=%s symbol=%s kept=%s dropped_address=%s",
                    chain_lower,
                    metadata.protocol,
                    symbol_upper,
                    chain_symbols[symbol_upper].get_address(chain_lower),
                    metadata.address,
                )

            chain_addresses = self._static_address_index.setdefault(chain_lower, {})
            if addr_key not in chain_addresses:
                chain_addresses[addr_key] = token
            elif chain_addresses[addr_key] is not token:
                logger.warning(
                    "protocol_metadata_address_collision chain=%s protocol=%s address=%s "
                    "kept_symbol=%s dropped_symbol=%s",
                    chain_lower,
                    metadata.protocol,
                    addr_key,
                    chain_addresses[addr_key].symbol,
                    metadata.symbol,
                )

    def _refresh_canonical_address_cache_entries(self) -> None:
        """Overwrite stale cache rows for address->symbol canonical overrides.

        Address lookups hit cache before the static registry. When the preferred
        symbol for a known address changes but the address itself stays stable,
        old disk-cache rows would otherwise pin the pre-migration symbol.
        """
        for (chain_lower, _addr_key), preferred_symbol in _STATIC_ADDRESS_CANONICAL_SYMBOLS.items():
            token = self._static_registry.get(chain_lower, {}).get(preferred_symbol.upper())
            if token is None:
                continue

            _, chain_enum = _normalize_chain(chain_lower)
            resolved = self._token_to_resolved(token, chain_lower, chain_enum, source="static")
            self._cache.put(resolved)

    @classmethod
    def get_instance(
        cls,
        gateway_client: Any | None = None,
        cache_file: str | None = None,
        gateway_channel: "grpc.Channel | None" = None,
    ) -> "TokenResolver":
        """Get the singleton TokenResolver instance.

        This is the recommended way to get a TokenResolver. The first call
        creates the instance, subsequent calls return the same instance.

        Args:
            gateway_client: DEPRECATED - Use gateway_channel instead.
            cache_file: Optional path to cache file. Only used on first call.
            gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                            Only used on first call when creating instance.
                            Pass a grpc.Channel connected to the gateway server.

        Returns:
            The singleton TokenResolver instance

        Example:
            # Without gateway (static resolution only)
            resolver = TokenResolver.get_instance()
            token = resolver.resolve("USDC", "arbitrum")

            # With gateway (enables on-chain discovery)
            import grpc
            channel = grpc.insecure_channel("localhost:50051")
            resolver = TokenResolver.get_instance(gateway_channel=channel)
        """
        if cls._instance is None:
            with cls._instance_lock:
                # Double-check locking
                if cls._instance is None:
                    cls._instance = cls(
                        gateway_client=gateway_client,
                        cache_file=cache_file,
                        gateway_channel=gateway_channel,
                    )
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance. Primarily for testing."""
        with cls._instance_lock:
            cls._instance = None

    def resolve(  # noqa: C901
        self, token: str, chain: str | Chain, *, log_errors: bool = True, skip_gateway: bool = False
    ) -> ResolvedToken:
        """Resolve a token by symbol or address on a specific chain.

        This is the main resolution method. It checks:
        1. Memory cache
        2. Disk cache
        3. Static registry
        4. Gateway on-chain lookup (if token is an address and gateway available)

        Args:
            token: Token symbol (e.g., "USDC") or address (e.g., "0x...")
            chain: Chain name or Chain enum
            log_errors: If False, suppress warning logs on resolution failure (default True).
                Use False for best-effort lookups where failures are expected and handled.
            skip_gateway: If True, skip the slow gateway on-chain lookup and fail fast
                after cache + static registry. Use for cosmetic/best-effort lookups
                where a 30s gateway timeout is unacceptable.

        Returns:
            ResolvedToken with full metadata

        Raises:
            TokenNotFoundError: If token cannot be resolved
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: For other resolution errors

        Example:
            # By symbol
            usdc = resolver.resolve("USDC", "arbitrum")

            # By address
            token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
        """
        start_time = time.perf_counter()
        chain_lower, chain_enum = _normalize_chain(chain)

        # Reset the per-call miss flag at the top. The gateway helpers
        # set ``definitive = True`` only when they actually reach the
        # gateway and get a "not found" answer. If we leave the flag
        # carrying state from a prior call, a later ``resolve(...,
        # skip_gateway=True)`` on the same thread could write a
        # negative-cache entry for a static-only failure.
        self._gateway_miss_state.definitive = False

        try:
            # Determine if input is address or symbol (pure functions, no lock needed)
            is_address = _is_address(token, chain_lower)

            if is_address:
                _validate_address(token, chain_lower)
            elif _looks_like_address(token):
                _validate_address(token, chain_lower)

            # Symbol-input normalization: handle stray whitespace and translate
            # CoinGecko IDs that leak through as symbols (``"usd-coin"`` ->
            # ``"USDC"``, ``"tether"`` -> ``"USDT"``). Only applies to
            # non-address inputs; the rest of the cascade (negative cache,
            # static registry, gateway) then uses the canonical form.
            if not is_address:
                token = _normalize_symbol_input(token)

            # Negative cache short-circuit (VIB-2715). Normalize the key
            # the same way we store it so hits don't depend on caller
            # casing. Skipped when ``skip_gateway`` is set -- that caller
            # is explicitly asking for a static-only answer and shouldn't
            # be blocked by a stale gateway-era miss.
            neg_key = (
                chain_lower,
                _normalize_address_for_chain(token, chain_lower) if is_address else token.upper(),
            )
            if not skip_gateway and self._check_negative_cache(neg_key):
                self._record_negative_cache_hit(token, chain_lower, neg_key, start_time)
                raise TokenNotFoundError(
                    token=token,
                    chain=chain_lower,
                    reason=(
                        f"{'Address' if is_address else f'Symbol {token!r}'} not found "
                        f"(negative cache; next retry in ~{self._negative_cache_ttl_seconds:.0f}s)"
                    ),
                    suggestions=(self._get_symbol_suggestions(token.upper(), chain_lower) if not is_address else [])
                    + ["Use register_token() if you know the address"],
                )

            # Fast path: cache + static registry (under lock)
            symbol_needs_gateway = False
            with self._lock:
                if is_address:
                    result = self._try_fast_resolve_address(token, chain_lower, chain_enum)
                else:
                    result = self._resolve_by_symbol(token, chain_lower, chain_enum)
                    # _resolve_by_symbol returns None (sentinel) when gateway is available
                    # and the symbol wasn't found statically, signalling us to try the
                    # gateway's dynamic resolution path outside the lock.
                    if result is None:
                        symbol_needs_gateway = True

            if result is not None:
                self._record_resolution_success(token, chain_lower, result, start_time)
                return result

            # Slow path: gateway lookup (NO lock held)
            if not skip_gateway and (self._gateway_channel is not None or self._gateway_client is not None):
                if symbol_needs_gateway:
                    # Symbol not in static registry -- try gateway's dynamic resolution
                    # (Jupiter for Solana, CoinGecko for EVM)
                    resolved = self._resolve_symbol_via_gateway(token, chain_lower, chain_enum)
                else:
                    # Address not in static registry -- try on-chain ERC20 lookup
                    resolved = self._resolve_via_gateway(token, chain_lower, chain_enum)

                if resolved:
                    # Write back to cache (under lock)
                    with self._lock:
                        self._cache.put(resolved)
                    self._record_resolution_success(token, chain_lower, resolved, start_time)
                    return resolved

            # Token not found - provide helpful error
            _try_record_metric("record_token_resolution_cache_miss", chain_lower)
            if is_address or not symbol_needs_gateway:
                suggestions = [
                    "Verify the contract address is correct",
                    "Check if the address is deployed on this chain",
                    "Use register() to add custom tokens",
                ]
                reason = f"Address not found in registry for {chain_lower}"
            else:
                suggestions = self._get_symbol_suggestions(token.upper(), chain_lower)
                reason = f"Symbol '{token}' not found in registry for {chain_lower}"
            if self._gateway_channel is None and self._gateway_client is None:
                suggestions.append("Connect to gateway for on-chain token discovery")

            # VIB-2715: remember this miss so the next request for the
            # same (chain, key) returns instantly instead of hitting
            # the gateway again. Only cache DEFINITIVE misses — the
            # gateway helpers set ``_gateway_miss_state.definitive`` to
            # True only when the gateway actually answered "token does
            # not exist". Timeouts, UNAVAILABLE errors, and integrity-
            # reject paths stay False, so transient gateway trouble
            # doesn't get locked in for 5 minutes.
            definitive_miss = getattr(self._gateway_miss_state, "definitive", False)
            gateway_attempted = self._gateway_channel is not None or self._gateway_client is not None
            if gateway_attempted and definitive_miss:
                self._store_negative_cache(neg_key)

            raise TokenNotFoundError(
                token=token,
                chain=chain_lower,
                reason=reason,
                suggestions=suggestions,
            )

        except TokenResolutionError as e:
            with self._lock:
                self._stats["errors"] += 1
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            error_type = type(e).__name__
            latency_ms = round(elapsed_ms, 3)
            if log_errors:
                logger.warning(
                    "token_resolution_error token=%s chain=%s error_type=%s detail=%s latency_ms=%.3f",
                    token,
                    chain_lower,
                    error_type,
                    str(e),
                    latency_ms,
                    extra={
                        "token": token,
                        "chain": chain_lower,
                        "error_type": error_type,
                        "latency_ms": latency_ms,
                        "error_detail": str(e),
                    },
                )
            _try_record_metric("record_token_resolution_error", chain_lower, error_type)
            raise

    def _try_fast_resolve_address(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken | None:
        """Try to resolve an address from cache or static registry (must be called under lock).

        Returns:
            ResolvedToken if found in cache or static, None if gateway lookup needed.
        """
        addr_key = _normalize_address_for_chain(address, chain_lower)

        # 1. Check cache (memory + disk)
        cached = self._cache.get(chain_lower, address=addr_key)
        if cached:
            self._stats["cache_hits"] += 1
            logger.debug(
                "token_cache_hit",
                extra={"token": address, "chain": chain_lower, "cache_type": "memory"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "memory")
            return cached

        # 2. Check static registry address index
        chain_index = self._static_address_index.get(chain_lower, {})
        static_token = chain_index.get(addr_key)

        if static_token:
            self._stats["static_hits"] += 1
            resolved = self._token_to_resolved(static_token, chain_lower, chain_enum, source="static")
            self._cache.put(resolved)
            logger.debug(
                "token_cache_miss",
                extra={"token": address, "chain": chain_lower, "resolved_via": "static"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "static")
            return resolved

        # Not found in fast path - needs gateway lookup
        return None

    def _record_resolution_success(
        self, token: str, chain_lower: str, result: ResolvedToken, start_time: float
    ) -> None:
        """Record metrics and logging for a successful resolution."""
        elapsed_s = time.perf_counter() - start_time
        elapsed_ms = elapsed_s * 1000
        logger.debug(
            "token_resolved",
            extra={
                "token": token,
                "chain": chain_lower,
                "resolution_source": result.source,
                "latency_ms": round(elapsed_ms, 3),
            },
        )
        _try_record_metric("record_token_resolution_latency", chain_lower, result.source, elapsed_s)

    def _resolve_by_symbol(self, symbol: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken | None:
        """Resolve a token by symbol.

        Returns:
            ResolvedToken if found in cache/static registry/aliases.
            None (sentinel) if a gateway is available and dynamic resolution
            should be attempted by the caller outside the lock.
            Raises TokenNotFoundError if no gateway is available and the symbol
            is not in the registry.
        """
        symbol_upper = symbol.upper()

        # 1. Check cache (memory + disk)
        cached = self._cache.get(chain_lower, symbol=symbol_upper)
        if cached:
            self._stats["cache_hits"] += 1
            logger.debug(
                "token_cache_hit",
                extra={"token": symbol, "chain": chain_lower, "cache_type": "memory"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "memory")
            return cached

        # 2. Check static registry
        chain_registry = self._static_registry.get(chain_lower, {})
        static_token = chain_registry.get(symbol_upper)

        if static_token:
            self._stats["static_hits"] += 1
            resolved = self._token_to_resolved(static_token, chain_lower, chain_enum, source="static")
            # Cache for future lookups
            self._cache.put(resolved)
            logger.debug(
                "token_cache_miss",
                extra={"token": symbol, "chain": chain_lower, "resolved_via": "static"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "static")
            return resolved

        # 3. Check symbol aliases (bridged tokens like USDC.e, USDbC, USDT.e, WETH.e)
        alias_address = SYMBOL_ALIASES.get((chain_lower, symbol_upper))
        if alias_address:
            logger.debug(
                "token_alias_resolved",
                extra={"token": symbol, "chain": chain_lower, "alias_address": alias_address},
            )
            # Resolve by the canonical address
            return self._resolve_by_address(alias_address, chain_lower, chain_enum)

        # 4. Try gateway dynamic symbol resolution (Jupiter for Solana, CoinGecko for EVM).
        # Must be called WITHOUT the lock held to avoid blocking while waiting for network.
        # We raise here if no gateway -- this check is inside the lock.
        if self._gateway_channel is None and self._gateway_client is None:
            _try_record_metric("record_token_resolution_cache_miss", chain_lower)
            raise TokenNotFoundError(
                token=symbol,
                chain=chain_lower,
                reason=f"Symbol '{symbol}' not found in registry for {chain_lower}",
                suggestions=self._get_symbol_suggestions(symbol_upper, chain_lower),
            )

        # Signal to the caller (resolve()) that gateway symbol resolution should be attempted
        # by returning None instead of raising.  resolve() will handle this outside the lock.
        return None  # sentinel: gateway path needed

    def _resolve_by_address(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken:
        """Resolve a token by address from cache or static registry (must be called under lock).

        This method does NOT call the gateway. It is used for alias resolution
        (from _resolve_by_symbol) where the address should be in the static registry.
        Gateway-based address resolution is handled in resolve() outside the lock.
        """
        result = self._try_fast_resolve_address(address, chain_lower, chain_enum)
        if result is not None:
            return result

        # Address not in cache or static -- this shouldn't happen for aliases
        raise TokenNotFoundError(
            token=address,
            chain=chain_lower,
            reason=f"Address not found in registry for {chain_lower}",
            suggestions=[
                "Verify the contract address is correct",
                "Check if the address is deployed on this chain",
                "Use register() to add custom tokens",
            ],
        )

    def _get_gateway_stub(self) -> Any:
        """Get or create the gateway TokenService stub.

        Returns:
            TokenServiceStub for gateway communication, or None if unavailable
        """
        if self._gateway_stub is not None:
            return self._gateway_stub

        if self._gateway_channel is None:
            return None

        try:
            # Lazy import to avoid circular dependencies
            from almanak.gateway.proto import gateway_pb2_grpc

            self._gateway_stub = gateway_pb2_grpc.TokenServiceStub(self._gateway_channel)
            return self._gateway_stub
        except Exception as e:
            logger.warning(f"Failed to create gateway stub: {e}")
            return None

    def _check_gateway_available(self) -> bool:
        """Check if gateway is available.

        Uses cached state with 30-second TTL to avoid excessive checks.

        Returns:
            True if gateway is available, False otherwise
        """
        # Cache gateway availability state for 30 seconds
        cache_ttl = 30.0
        now = time.time()

        if self._gateway_available is not None and (now - self._gateway_check_time) < cache_ttl:
            return self._gateway_available

        # Check if we have a gateway channel
        if self._gateway_channel is None:
            self._gateway_available = False
            self._gateway_check_time = now
            return False

        # Try to get the stub - this validates the channel
        stub = self._get_gateway_stub()
        if stub is None:
            self._gateway_available = False
            self._gateway_check_time = now
            return False

        # Assume available - actual availability will be determined on use
        self._gateway_available = True
        self._gateway_check_time = now
        return True

    def _cross_check_decimals_with_static(self, address: str, chain_lower: str, gateway_decimals: int) -> int | None:
        """Cross-check gateway decimals against static registry.

        If the address exists in the static registry with known decimals,
        and the gateway returned different decimals, return the static
        decimals (indicating a mismatch). Returns None if no conflict.

        Args:
            address: Token contract address
            chain_lower: Chain name (lowercase)
            gateway_decimals: Decimals value returned by the gateway

        Returns:
            Static decimals if there's a mismatch, None if OK or not in registry
        """
        addr_key = _normalize_address_for_chain(address, chain_lower)
        chain_index = self._static_address_index.get(chain_lower, {})
        static_token = chain_index.get(addr_key)
        if static_token is not None:
            static_decimals = static_token.get_decimals(chain_lower)
            if static_decimals != gateway_decimals:
                return static_decimals
        return None

    def _resolve_via_gateway(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken | None:
        """Attempt to resolve token via gateway on-chain lookup.

        Makes a gRPC call to the gateway's GetTokenMetadata RPC to query
        the token contract directly for metadata.

        Args:
            address: Token contract address
            chain_lower: Chain name (lowercase)
            chain_enum: Chain enum value

        Returns:
            ResolvedToken if successful, None otherwise

        Note:
            - Gracefully returns None if gateway is unavailable (no error raised)
            - Caches discovered tokens for future lookups
            - Logs warnings on gateway errors but doesn't fail
        """
        # Reset the definitive-miss flag — any None return below that
        # doesn't set ``definitive = True`` is a transient failure and
        # must NOT be negative-cached by the caller.
        self._gateway_miss_state.definitive = False

        # Check if gateway is available
        if not self._check_gateway_available():
            logger.debug(f"Gateway not available for on-chain lookup of {address} on {chain_lower}")
            return None

        stub = self._get_gateway_stub()
        if stub is None:
            return None

        with self._lock:
            self._stats["gateway_lookups"] += 1
        gateway_start = time.perf_counter()

        try:
            # Import proto message type
            from almanak.gateway.proto import gateway_pb2

            # Create request
            request = gateway_pb2.GetTokenMetadataRequest(
                address=address,
                chain=chain_lower,
            )

            # Make the RPC call with timeout.
            # 30s allows the server-side OnChainLookup to complete on cold starts
            # (first call initializes Web3 connection and queries ERC20 contract).
            response = stub.GetTokenMetadata(request, timeout=30.0)

            # Check if successful
            if not response.success:
                with self._lock:
                    self._stats["gateway_errors"] += 1
                logger.info(
                    "token_onchain_lookup_failed",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": response.error,
                        "latency_ms": round((time.perf_counter() - gateway_start) * 1000, 3),
                    },
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "not_found")
                # The gateway reached the chain and said "no token here" —
                # cache this so we don't keep hitting it.
                self._gateway_miss_state.definitive = True
                return None

            # Validate gateway response before creating ResolvedToken.
            # A gateway misconfiguration returning wrong decimals (e.g., 18 instead of 6
            # for USDC) would cause 10^12x amount miscalculation. Reject obviously
            # invalid data before it can poison the cache.
            decimals = response.decimals
            if not isinstance(decimals, int) or decimals < 0 or decimals > 77:
                logger.warning(
                    "token_gateway_integrity_rejected: decimals out of range (address=%s, chain=%s, decimals=%s)",
                    address,
                    chain_lower,
                    decimals,
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "integrity_rejected")
                return None

            # Validate the returned address: reject if it doesn't match the
            # requested address (a faulty gateway could return metadata for a
            # different token, poisoning the cache under the wrong key).
            resolved_address = response.address or address
            try:
                _validate_address(resolved_address, chain_lower)
            except InvalidTokenAddressError:
                logger.warning(
                    "token_gateway_integrity_rejected: invalid returned address (requested=%s, returned=%s, chain=%s)",
                    address,
                    resolved_address,
                    chain_lower,
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "integrity_rejected")
                return None

            if _normalize_address_for_chain(resolved_address, chain_lower) != _normalize_address_for_chain(
                address, chain_lower
            ):
                logger.warning(
                    "token_gateway_integrity_rejected: returned address mismatch (requested=%s, returned=%s, chain=%s)",
                    address,
                    resolved_address,
                    chain_lower,
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "integrity_rejected")
                return None

            # Cross-check against static registry: if we have a known decimals value
            # for this address, reject the gateway response if it disagrees.
            # This prevents a faulty gateway from overwriting trusted static data.
            static_check = self._cross_check_decimals_with_static(resolved_address, chain_lower, decimals)
            if static_check is not None:
                logger.warning(
                    "token_gateway_integrity_rejected: decimals mismatch with static registry "
                    "(address=%s, chain=%s, gateway_decimals=%d, static_decimals=%d)",
                    address,
                    chain_lower,
                    decimals,
                    static_check,
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "integrity_rejected")
                return None

            # Convert response to ResolvedToken
            resolved = ResolvedToken(
                symbol=response.symbol,
                address=resolved_address,
                decimals=decimals,
                chain=chain_enum,
                chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
                name=response.name or None,
                coingecko_id=None,
                is_stablecoin=False,  # Can't determine from on-chain
                is_native=False,  # If it has an address, it's not native
                is_wrapped_native=False,  # Can't determine from on-chain
                canonical_symbol=response.symbol,
                bridge_type=BridgeType.NATIVE,
                source="on_chain",
                is_verified=False,  # On-chain lookups are not verified
                resolved_at=datetime.now(),
            )

            # Note: cache write is done by the caller (resolve()) under lock
            gateway_elapsed_ms = (time.perf_counter() - gateway_start) * 1000
            logger.info(
                "token_onchain_discovered",
                extra={
                    "token": address,
                    "chain": chain_lower,
                    "symbol": response.symbol,
                    "decimals": response.decimals,
                    "latency_ms": round(gateway_elapsed_ms, 3),
                },
            )
            _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "success")

            return resolved

        except Exception as e:
            # Log the error but don't fail - graceful fallback
            error_str = str(e)
            with self._lock:
                self._stats["gateway_errors"] += 1
            gateway_elapsed_ms = (time.perf_counter() - gateway_start) * 1000

            # Check for common gRPC errors
            is_timeout = "DEADLINE_EXCEEDED" in error_str.upper()
            is_unavailable = "UNAVAILABLE" in error_str.upper()

            if is_unavailable:
                # Gateway is truly unreachable - cache as unavailable for TTL
                self._gateway_available = False
                self._gateway_check_time = time.time()
                status = "error"
                logger.warning(
                    "token_gateway_unavailable",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )
            elif is_timeout:
                # Timeout - gateway is reachable but slow (e.g. cold on-chain lookup).
                # Do NOT cache as unavailable so the next attempt can retry.
                status = "timeout"
                logger.warning(
                    "token_gateway_timeout",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )
            else:
                status = "error"
                logger.warning(
                    "token_onchain_lookup_error",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )

            _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, status)
            return None

    def _resolve_symbol_via_gateway(self, symbol: str, chain_lower: str, chain_enum: Chain) -> "ResolvedToken | None":
        """Attempt to resolve a symbol via the gateway's dynamic ResolveToken RPC.

        The gateway's ResolveToken now includes dynamic fallbacks (Jupiter for
        Solana, CoinGecko for EVM) that go beyond the static registry.  Calling
        ResolveToken here (rather than GetTokenMetadata) allows us to use those
        dynamic paths for symbol lookups.

        Args:
            symbol: Token symbol (e.g., "swETH", "USDS")
            chain_lower: Chain name (lowercase)
            chain_enum: Chain enum value

        Returns:
            ResolvedToken if successful, None otherwise
        """
        # Reset the definitive-miss flag — only the explicit "gateway
        # said this symbol doesn't exist" path below sets it to True.
        # Timeouts, UNAVAILABLE, and integrity rejects stay False and
        # therefore do NOT poison the negative cache.
        self._gateway_miss_state.definitive = False

        if not self._check_gateway_available():
            logger.debug("Gateway not available for symbol lookup of %s on %s", symbol, chain_lower)
            return None

        stub = self._get_gateway_stub()
        if stub is None:
            return None

        with self._lock:
            self._stats["gateway_lookups"] += 1
        gateway_start = time.perf_counter()

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.ResolveTokenRequest(
                token=symbol,
                chain=chain_lower,
            )
            # VIB-2715: budget for symbol lookups. The gateway's
            # CoinGecko/Jupiter path is usually <1s for cache hits, but
            # the on-chain confirm step (CoinGecko search + eth_chain_id
            # + decimals() + symbol()) takes ~3.5-4s on a fresh lookup.
            # 5s was clipping legitimate resolutions mid-confirm; 15s
            # gives headroom over observed p99 (~4.1s) while staying
            # well under the 30s CoinGecko rate-limit worst case that
            # VIB-2715 was originally trying to avoid.
            response = stub.ResolveToken(request, timeout=15.0)

            if not response.success:
                with self._lock:
                    self._stats["gateway_errors"] += 1
                logger.debug(
                    "token_gateway_symbol_not_found: symbol=%s chain=%s error=%s",
                    symbol,
                    chain_lower,
                    response.error,
                )
                # Definitive "not found" from the gateway — safe to cache.
                self._gateway_miss_state.definitive = True
                return None

            decimals = response.decimals
            if not isinstance(decimals, int) or decimals < 0 or decimals > 77:
                logger.warning(
                    "token_gateway_symbol_integrity_rejected: decimals out of range (symbol=%s chain=%s decimals=%s)",
                    symbol,
                    chain_lower,
                    decimals,
                )
                return None

            # Validate the returned address format before caching.
            # A misconfigured gateway could return an empty or malformed address —
            # reject it so it doesn't poison the resolver cache.
            returned_address = response.address
            if not returned_address:
                logger.warning(
                    "token_gateway_symbol_integrity_rejected: empty address returned (symbol=%s chain=%s)",
                    symbol,
                    chain_lower,
                )
                return None
            try:
                _validate_address(returned_address, chain_lower)
            except InvalidTokenAddressError:
                logger.warning(
                    "token_gateway_symbol_integrity_rejected: invalid address format (symbol=%s chain=%s address=%s)",
                    symbol,
                    chain_lower,
                    returned_address,
                )
                return None

            resolved = ResolvedToken(
                symbol=response.symbol or symbol,
                address=returned_address,
                decimals=decimals,
                chain=chain_enum,
                chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
                name=response.name or None,
                coingecko_id=None,
                is_stablecoin=False,
                is_native=False,
                is_wrapped_native=False,
                canonical_symbol=response.symbol or symbol,
                bridge_type=BridgeType.NATIVE,
                source=response.source or "gateway_dynamic",
                is_verified=response.is_verified,
                resolved_at=datetime.now(),
            )

            gateway_elapsed_ms = (time.perf_counter() - gateway_start) * 1000
            logger.info(
                "token_symbol_dynamic_discovered: symbol=%s chain=%s address=%s decimals=%d source=%s latency_ms=%.1f",
                symbol,
                chain_lower,
                returned_address,
                decimals,
                response.source,
                gateway_elapsed_ms,
            )
            _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "gateway_symbol")
            return resolved

        except Exception as e:
            error_str = str(e)
            with self._lock:
                self._stats["gateway_errors"] += 1
            is_unavailable = "UNAVAILABLE" in error_str.upper()
            if is_unavailable:
                self._gateway_available = False
                self._gateway_check_time = time.time()

            # Ambiguity marker: the gateway surfaced multiple liquid contracts
            # claiming this symbol. Raise AmbiguousTokenError client-side with
            # the candidate list so the strategy author can disambiguate with
            # an explicit address. This path must NOT poison the negative
            # cache — the symbol is not missing, it is ambiguous.
            if "AMBIGUOUS_SYMBOL" in error_str:
                candidates = _parse_ambiguous_candidates(error_str)
                logger.info(
                    "token_gateway_symbol_ambiguous: symbol=%s chain=%s candidates=%s",
                    symbol,
                    chain_lower,
                    candidates,
                )
                raise AmbiguousTokenError(
                    token=symbol,
                    chain=chain_lower,
                    reason=(
                        f"Gateway returned multiple liquid contracts claiming '{symbol}' on "
                        f"{chain_lower}. Disambiguate with an explicit address."
                    ),
                    matching_addresses=candidates,
                    suggestions=[f"Candidate: {addr}" for addr in candidates],
                ) from e

            logger.debug("token_gateway_symbol_lookup_error: symbol=%s chain=%s error=%s", symbol, chain_lower, e)
            return None

    def is_gateway_connected(self) -> bool:
        """Check if gateway is connected and available for on-chain lookups.

        This method checks if a gateway channel is configured and appears to be
        connected. Note that the actual availability is verified lazily - the
        gateway might become unavailable between this check and actual use.

        Returns:
            True if gateway channel is configured and appears available,
            False otherwise.

        Example:
            resolver = get_token_resolver(gateway_channel=channel)
            if resolver.is_gateway_connected():
                print("Gateway available for on-chain token discovery")
            else:
                print("Static resolution only")
        """
        with self._lock:
            return self._check_gateway_available()

    def set_gateway_channel(self, channel: "grpc.Channel | None") -> None:
        """Set or update the gateway channel.

        This allows changing the gateway connection after initialization.
        Useful for reconnection scenarios or testing.

        Args:
            channel: gRPC channel to gateway, or None to disable gateway

        Example:
            import grpc
            resolver = get_token_resolver()

            # Connect to gateway later
            channel = grpc.insecure_channel("localhost:50051")
            resolver.set_gateway_channel(channel)

            # Disconnect from gateway
            resolver.set_gateway_channel(None)
        """
        with self._lock:
            self._gateway_channel = channel
            self._gateway_stub = None  # Reset stub to force re-creation
            self._gateway_available = None  # Reset availability state
            self._gateway_check_time = 0

            if channel is not None:
                logger.info("Gateway channel configured for on-chain token discovery")
            else:
                logger.info("Gateway channel disconnected - static resolution only")

    def _token_to_resolved(
        self,
        token: Token,
        chain_lower: str,
        chain_enum: Chain,
        source: str = "static",
    ) -> ResolvedToken:
        """Convert a Token to ResolvedToken for a specific chain."""
        address = token.get_address(chain_lower)
        if not address:
            raise TokenNotFoundError(
                token=token.symbol,
                chain=chain_lower,
                reason=f"Token '{token.symbol}' not available on {chain_lower}",
            )

        decimals = token.get_decimals(chain_lower)
        chain_config = token.get_chain_config(chain_lower)

        # Determine if native token
        addr_norm = _normalize_address_for_chain(address, chain_lower)
        is_native = addr_norm == _normalize_address_for_chain(NATIVE_SENTINEL, chain_lower)
        if chain_config:
            is_native = chain_config.is_native

        # Determine bridge type
        bridge_type = BridgeType.NATIVE
        if chain_config:
            bridge_type = chain_config.bridge_type

        # Check if wrapped native by comparing address to WRAPPED_NATIVE registry
        wrapped_addr = WRAPPED_NATIVE.get(chain_lower, "")
        is_wrapped_native = bool(wrapped_addr and addr_norm == _normalize_address_for_chain(wrapped_addr, chain_lower))

        return ResolvedToken(
            symbol=token.symbol,
            address=addr_norm,
            decimals=decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=token.name,
            coingecko_id=token.coingecko_id,
            is_stablecoin=token.is_stablecoin,
            is_native=is_native,
            is_wrapped_native=is_wrapped_native,
            canonical_symbol=token.symbol,
            bridge_type=bridge_type,
            source=source,
            is_verified=True,
            resolved_at=datetime.now(),
        )

    def _get_symbol_suggestions(self, symbol: str, chain: str) -> list[str]:
        """Get suggestions for similar symbols."""
        suggestions = []

        # Look for similar symbols in registry
        chain_registry = self._static_registry.get(chain, {})
        all_symbols = list(chain_registry.keys())

        # Find symbols that start with same letters or contain similar parts
        for s in all_symbols:
            if s.startswith(symbol[:2]) or symbol[:3] in s:
                suggestions.append(f"Did you mean '{s}'?")

        # Limit suggestions
        return suggestions[:3]

    def resolve_pair(
        self,
        token_in: str,
        token_out: str,
        chain: str | Chain,
    ) -> tuple[ResolvedToken, ResolvedToken]:
        """Resolve a pair of tokens for a swap operation.

        Convenience method for resolving both tokens in a trading pair.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            chain: Chain name or Chain enum

        Returns:
            Tuple of (resolved_token_in, resolved_token_out)

        Raises:
            TokenNotFoundError: If either token cannot be resolved
            TokenResolutionError: For other resolution errors

        Example:
            usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")
        """
        resolved_in = self.resolve(token_in, chain)
        resolved_out = self.resolve(token_out, chain)
        return resolved_in, resolved_out

    def get_decimals(self, chain: str | Chain, token: str) -> int:
        """Get the decimals for a token on a specific chain.

        Convenience method that extracts just the decimals from resolution.
        NEVER defaults to 18 - always raises TokenNotFoundError if unknown.

        Args:
            chain: Chain name or Chain enum
            token: Token symbol or address

        Returns:
            Number of decimal places

        Raises:
            TokenNotFoundError: If token cannot be resolved

        Example:
            decimals = resolver.get_decimals("arbitrum", "USDC")
            # Returns 6
        """
        resolved = self.resolve(token, chain)
        return resolved.decimals

    def known_static_tokens_by_chain(self) -> Mapping[str, Mapping[str, ResolvedToken]]:
        """Return a read-only snapshot of static token metadata by chain/address.

        This is intentionally static-only: it exposes the JSON-backed token
        catalogue plus connector-published synthetic metadata that was registered
        into the resolver at construction time. Gateway-discovered and manually
        registered runtime tokens remain available through ``resolve()``.
        """
        snapshot: dict[str, Mapping[str, ResolvedToken]] = {}
        with self._lock:
            for chain_lower, tokens_by_address in self._static_address_index.items():
                descriptor = ChainRegistry.try_resolve(chain_lower)
                if descriptor is None:
                    continue

                chain_tokens: dict[str, ResolvedToken] = {}
                for address, token in tokens_by_address.items():
                    try:
                        chain_tokens[address] = self._token_to_resolved(
                            token,
                            descriptor.name,
                            descriptor.enum,
                            source="static",
                        )
                    except TokenNotFoundError:
                        logger.debug(
                            "Skipping static token snapshot row with no chain address: chain=%s symbol=%s",
                            descriptor.name,
                            token.symbol,
                        )

                if chain_tokens:
                    snapshot[descriptor.name] = MappingProxyType(chain_tokens)

        return MappingProxyType(snapshot)

    def get_address(self, chain: str | Chain, symbol: str) -> str:
        """Get the address for a token symbol on a specific chain.

        Convenience method that extracts just the address from resolution.

        Args:
            chain: Chain name or Chain enum
            symbol: Token symbol (e.g., "USDC")

        Returns:
            Contract address

        Raises:
            TokenNotFoundError: If token cannot be resolved

        Example:
            address = resolver.get_address("arbitrum", "USDC")
            # Returns "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        """
        resolved = self.resolve(symbol, chain)
        return resolved.address

    def resolve_for_swap(self, token: str, chain: str | Chain) -> ResolvedToken:
        """Resolve a token for swap operations, auto-wrapping native tokens.

        This method resolves a token and if it's a native token (ETH, MATIC, AVAX, BNB),
        automatically returns the wrapped version instead (WETH, WMATIC, WAVAX, WBNB).
        This is because most DEX protocols cannot swap native tokens directly.

        For non-native tokens, this behaves identically to resolve().

        Args:
            token: Token symbol (e.g., "ETH", "USDC") or address
            chain: Chain name or Chain enum

        Returns:
            ResolvedToken - wrapped version if native, original otherwise

        Raises:
            TokenNotFoundError: If token or wrapped version cannot be resolved
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: For other resolution errors

        Example:
            # ETH on Arbitrum returns WETH
            token = resolver.resolve_for_swap("ETH", "arbitrum")
            assert token.symbol == "WETH"

            # USDC returns USDC (not native)
            token = resolver.resolve_for_swap("USDC", "arbitrum")
            assert token.symbol == "USDC"
        """
        resolved = self.resolve(token, chain)

        # If it's a native token, resolve to wrapped version
        if resolved.is_native:
            chain_lower, _ = _normalize_chain(chain)
            wrapped_address = WRAPPED_NATIVE.get(chain_lower)

            if wrapped_address:
                logger.debug(f"Auto-wrapping native token {resolved.symbol} -> wrapped version on {chain_lower}")
                # Resolve the wrapped token by address to get full metadata
                return self.resolve(wrapped_address, chain)
            else:
                # No wrapped native defined for this chain - log warning and return original
                logger.warning(f"No wrapped native token defined for {chain_lower}, returning native {resolved.symbol}")
                return resolved

        return resolved

    def resolve_for_protocol(
        self,
        token: str,
        chain: str | Chain,
        protocol: str,
    ) -> ResolvedToken:
        """Resolve a token with protocol-specific handling.

        This method provides a hook for future protocol-specific token resolution.
        Currently, it simply delegates to resolve_for_swap() for DEX protocols
        and to resolve() for other protocols.

        This allows for future expansion where specific protocols might have
        unique token requirements (e.g., protocol-specific wrapped tokens,
        canonical bridge tokens, etc.).

        Args:
            token: Token symbol or address
            chain: Chain name or Chain enum
            protocol: Protocol identifier (e.g., "uniswap_v3", "aave_v3")

        Returns:
            ResolvedToken with appropriate protocol handling

        Raises:
            TokenNotFoundError: If token cannot be resolved
            TokenResolutionError: For other resolution errors

        Example:
            # DEX protocols get auto-wrapped native tokens
            token = resolver.resolve_for_protocol("ETH", "arbitrum", "uniswap_v3")
            assert token.symbol == "WETH"

            # Lending protocols get the original token
            token = resolver.resolve_for_protocol("ETH", "ethereum", "aave_v3")
            assert token.symbol == "ETH"
        """
        # List of DEX protocols that need native token wrapping
        dex_protocols = {
            "uniswap_v3",
            "uniswap_v2",
            "sushiswap_v3",
            "sushiswap_v2",
            "pancakeswap_v3",
            "pancakeswap_v2",
            "aerodrome",
            "velodrome",
            "traderjoe_v2",
            "traderjoe_v1",
            "curve",
            "balancer",
            "camelot",
        }

        protocol_lower = protocol.lower()

        if protocol_lower in dex_protocols:
            # DEX protocols need wrapped native tokens
            return self.resolve_for_swap(token, chain)
        else:
            # Other protocols (lending, etc.) may accept native tokens
            return self.resolve(token, chain)

    # ------------------------------------------------------------------
    # Negative cache (VIB-2715)
    # ------------------------------------------------------------------

    def _check_negative_cache(self, key: tuple[str, str]) -> bool:
        """Return True if this (chain, key) is still in the negative cache."""
        with self._lock:
            expiry = self._negative_cache.get(key)
            if expiry is None:
                return False
            if time.monotonic() >= expiry:
                # Entry expired -- evict so the next attempt is a real try.
                self._negative_cache.pop(key, None)
                return False
            return True

    def _store_negative_cache(self, key: tuple[str, str]) -> None:
        with self._lock:
            now = time.monotonic()
            # Lazy sweep: long-running resolvers that accumulate many
            # unique bad symbols would otherwise let the map grow
            # unbounded because expired entries are only dropped when
            # the exact same key is looked up again. Sweep on insert
            # when the map is larger than the soft cap.
            if len(self._negative_cache) > self._negative_cache_max_size:
                self._negative_cache = {k: exp for k, exp in self._negative_cache.items() if exp > now}
            self._negative_cache[key] = now + self._negative_cache_ttl_seconds

    def _invalidate_negative_cache(self, chain_lower: str, *keys: str) -> None:
        """Drop matching entries so a freshly-registered token is visible.

        ``keys`` are looked up case-insensitively for symbols and
        address-normalized for addresses (the same normalization used when
        we insert).
        """
        with self._lock:
            for raw in keys:
                if not raw:
                    continue
                # Try the raw key (address path) and the uppercase key
                # (symbol path). Both are cheap.
                self._negative_cache.pop((chain_lower, _normalize_address_for_chain(raw, chain_lower)), None)
                self._negative_cache.pop((chain_lower, raw.upper()), None)

    def clear_negative_cache(self) -> None:
        """Drop all negative-cache entries. Useful in tests and after
        a large registry refresh."""
        with self._lock:
            self._negative_cache.clear()

    def _record_negative_cache_hit(
        self,
        token: str,
        chain_lower: str,
        key: tuple[str, str],
        start_time: float,
    ) -> None:
        with self._lock:
            self._stats["negative_cache_hits"] += 1
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.debug(
            "token_negative_cache_hit token=%s chain=%s key=%s latency_ms=%.3f",
            token,
            chain_lower,
            key[1],
            elapsed_ms,
        )
        _try_record_metric("record_token_resolution_cache_hit", chain_lower, "negative")

    def register(self, token: ResolvedToken) -> None:
        """Register a token explicitly at runtime.

        This allows adding custom tokens that aren't in the static registry.
        Registered tokens are stored in the cache.

        Args:
            token: ResolvedToken to register

        Example:
            custom_token = ResolvedToken(
                symbol="CUSTOM",
                address="0x...",
                decimals=18,
                chain=Chain.ARBITRUM,
                chain_id=42161,
                name="Custom Token",
            )
            resolver.register(custom_token)
        """
        chain_lower = token.chain.value.lower()
        # One critical section: populate the cache AND clear any pending
        # negative-cache entry atomically. A concurrent resolve() must
        # never see "cache has the token" while "negative cache still
        # says it's missing" -- that would let a stale miss short-
        # circuit a resolve for a token that has just been registered.
        with self._lock:
            self._cache.put(token)
            self._negative_cache.pop((chain_lower, _normalize_address_for_chain(token.symbol, chain_lower)), None)
            self._negative_cache.pop((chain_lower, token.symbol.upper()), None)
            self._negative_cache.pop((chain_lower, _normalize_address_for_chain(token.address, chain_lower)), None)
            logger.debug(f"Registered token {token.symbol} on {chain_lower}")

    def register_token(
        self,
        symbol: str,
        chain: str | Chain,
        address: str,
        decimals: int,
        *,
        name: str | None = None,
        coingecko_id: str | None = None,
        is_stablecoin: bool = False,
    ) -> ResolvedToken:
        """Register a custom token by its basic properties.

        Convenience wrapper around register() for strategy authors who need to
        register protocol-specific tokens (e.g., Pendle PT/YT, LP tokens) that
        aren't in the static registry.

        After registration, the token is resolvable via resolve(), get_address(),
        and get_decimals() within the same process.

        Note: This registers tokens in the local resolver only. Gateway-backed
        lookups (e.g., MarketSnapshot.balance() by symbol) require the gateway
        to also know the token. For balance queries on custom tokens, use the
        token address directly: market.balance("0x...").

        Args:
            symbol: Token symbol (e.g., "PT-wstETH-25JUN2026")
            chain: Chain name or Chain enum
            address: Token contract address
            decimals: Token decimal places
            name: Optional human-readable name
            coingecko_id: Optional CoinGecko ID for price fetching
            is_stablecoin: Whether this is a stablecoin (default False)

        Returns:
            The registered ResolvedToken (can be used immediately)

        Raises:
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: If chain is not recognized

        Example:
            resolver = get_token_resolver()
            resolver.register_token(
                symbol="PT-wstETH-25JUN2026",
                chain="arbitrum",
                address="0x71FBF40651E9d4bC027876E5aA4a3806d8E0B243",
                decimals=18,
            )
            # Now works:
            token = resolver.resolve("PT-wstETH-25JUN2026", "arbitrum")
        """
        chain_lower, chain_enum = _normalize_chain(chain)
        _validate_address(address, chain_lower)

        from almanak.core.constants import get_chain_id

        try:
            chain_id = get_chain_id(chain_enum)
        except ValueError:
            chain_id = 0  # Fallback for chains without EIP-155 IDs (e.g., Solana)

        try:
            resolved = ResolvedToken(
                symbol=symbol,
                address=_normalize_address_for_chain(address, chain_lower),
                decimals=decimals,
                chain=chain_enum,
                chain_id=chain_id,
                name=name or symbol,
                coingecko_id=coingecko_id,
                is_stablecoin=is_stablecoin,
                is_native=False,
                is_wrapped_native=False,
                canonical_symbol=symbol.upper(),
                source="registered",
                is_verified=False,
            )
        except ValueError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=chain_lower,
                reason=str(e),
            ) from e

        self.register(resolved)
        logger.info(f"Registered custom token {symbol} ({address}) on {chain_lower} with {decimals} decimals")
        return resolved

    def stats(self) -> dict[str, int]:
        """Get resolver performance statistics.

        Returns:
            Dict with cache_hits, static_hits, gateway_lookups, errors
        """
        with self._lock:
            return dict(self._stats)

    def cache_stats(self) -> dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dict with memory_hits, disk_hits, misses, evictions
        """
        return self._cache.stats()


def get_token_resolver(
    gateway_client: Any | None = None,
    cache_file: str | None = None,
    gateway_channel: "grpc.Channel | None" = None,
) -> TokenResolver:
    """Get the singleton TokenResolver instance.

    This is the recommended entry point for token resolution.

    Args:
        gateway_client: DEPRECATED - Use gateway_channel instead.
        cache_file: Optional path to cache file. Only used on first call.
        gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                        If None, only static resolution is available.
                        On-chain discovery gracefully falls back to static
                        resolution if the gateway becomes unavailable.

    Returns:
        The singleton TokenResolver instance

    Example:
        from almanak.framework.data.tokens import get_token_resolver

        # Static resolution only
        resolver = get_token_resolver()
        usdc = resolver.resolve("USDC", "arbitrum")

        # With gateway for on-chain discovery
        import grpc
        channel = grpc.insecure_channel("localhost:50051")
        resolver = get_token_resolver(gateway_channel=channel)
    """
    return TokenResolver.get_instance(
        gateway_client=gateway_client,
        cache_file=cache_file,
        gateway_channel=gateway_channel,
    )


def create_token_resolver(
    gateway_client: Any | None = None,
    cache_file: str | None = None,
    gateway_channel: "grpc.Channel | None" = None,
) -> TokenResolver:
    """Create a dedicated TokenResolver instance.

    Unlike :func:`get_token_resolver`, this does not touch the process-wide
    singleton. Use it for short-lived CLI commands or isolated runtime scopes
    that need a gateway channel without mutating global resolver state.

    Args:
        gateway_client: DEPRECATED - Use gateway_channel instead.
        cache_file: Optional path to cache file. Defaults to the standard cache.
        gateway_channel: Optional gRPC channel for dynamic/on-chain lookups.

    Returns:
        A fresh ``TokenResolver`` instance.
    """
    return TokenResolver(
        gateway_client=gateway_client,
        cache_file=cache_file,
        gateway_channel=gateway_channel,
    )


__all__ = [
    "TokenResolver",
    "get_token_resolver",
    "create_token_resolver",
]
