"""MarketService implementation - provides market data to strategies.

This service provides price, balance, and indicator data to strategy containers
via gRPC. All external API calls (CoinGecko, Web3 RPC) are made here in the
gateway; strategy containers only see the results.
"""

import asyncio
import logging
import re
import time
from decimal import Decimal
from types import MappingProxyType
from typing import Any

import grpc

from almanak.connectors._base.gateway_capabilities import (
    GatewayPoolKeyCacheCapability,
    PoolKeyCacheError,
    PoolKeyCacheProtocol,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services._grpc_errors import set_error_from_upstream
from almanak.gateway.validation import (
    ValidationError,
    is_solana_chain,
    validate_address_for_chain,
    validate_chain,
)

# Pattern for detecting EVM contract addresses in price requests.
_EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class MultiChainAmbiguousPriceRequest(Exception):
    """Raised when a multi-chain gateway receives an EVM-address price lookup
    with no explicit ``PriceRequest.chain``.

    On a gateway serving more than one chain, silently falling back to an
    arbitrary "primary" chain would route the RPC at the wrong network and
    either return "not a contract" or (worse) a bogus price for a same-address
    deployment on another chain. The resolver raises this; ``GetPrice``
    translates it to gRPC ``INVALID_ARGUMENT`` so the caller sees a clear
    contract violation instead of a silent wrong-chain price.
    """

    def __init__(self, token: str, configured_chains: list[str]) -> None:
        self.token = token
        self.configured_chains = list(configured_chains)
        short = f"{token[:6]}...{token[-4:]}" if len(token) > 12 else token
        super().__init__(
            "Multi-chain gateway requires PriceRequest.chain for address-based "
            f"lookups (token={short}, configured_chains={self.configured_chains}). "
            "Set PriceRequest.chain to one of the configured chains."
        )


# Timeout for gateway-local on-chain ERC20 metadata lookups driven by GetPrice.
# Mirrors TokenService's DEFAULT_ONCHAIN_TIMEOUT so both code paths bound slow
# RPCs the same way.
_ONCHAIN_LOOKUP_TIMEOUT_SECONDS: float = 10.0

logger = logging.getLogger(__name__)

# Native-to-wrapped token price aliases.
# When a price lookup for the native token fails, retry with the wrapped equivalent.
# This handles chains where the native token (e.g. MNT) has poor exchange coverage
# but the wrapped version (e.g. WMNT) is listed on major exchanges.
NATIVE_PRICE_ALIASES: dict[str, str] = {
    "MNT": "WMNT",
    "MATIC": "WMATIC",
    # POL is the Sep-2024 rename of MATIC on Polygon (1:1). Route native-price
    # failures through WMATIC (aka WPOL) — same asset, better exchange coverage.
    "POL": "WMATIC",
    "AVAX": "WAVAX",
    "FTM": "WFTM",
    "BNB": "WBNB",
    "S": "WS",  # Sonic
}

# Chain-scoped native gas tokens, derived from the chain registry (VIB-4851 A1).
# A symbol routes through `provider.get_native_balance()` ONLY if it is native to
# THIS chain — the set is `{descriptor.native.symbol, *accepted_symbols}` (e.g.
# Polygon accepts both MATIC and POL, the Sep-2024 1:1 rename). This prevents
# `GetBalance(token="POL", chain="ethereum")` from returning ETH balance, etc.
# Kept under the legacy name as a read-only view for the shape-lock test snapshots.
# Deriving from `descriptor.name` also (a) drops the dead unregistered keys this
# map used to carry (`scroll`/`zksync`/`fantom` — unreachable past `validate_chain`,
# which both call sites run first) and (b) corrects the legacy `"x-layer"` typo to
# the canonical `"xlayer"`, which previously left `GetBalance(token="OKB",
# chain="xlayer")` mis-routed down the ERC-20 path. Adding a chain needs no edit here.
NATIVE_SYMBOLS_BY_CHAIN: MappingProxyType[str, frozenset[str]] = MappingProxyType(
    {d.name: native_symbols_for(d.name) for d in ChainRegistry.all()}
)


def _is_native_symbol(token: str, chain: str) -> bool:
    """Return True iff `token` is the native gas symbol for `chain`.

    Fails CLOSED for unregistered chains: an unknown chain yields an empty set, so
    the request falls through to `provider.get_balance(token)` (the safe ERC-20
    path) instead of silently routing to `get_native_balance()` and returning the
    wrong asset (the VIB-3137 contract). Native symbols are owned per-chain on
    `ChainDescriptor.native`; adding a chain needs no edit here (VIB-4851 A1).
    """
    return token.upper() in native_symbols_for(chain)


def _block_pin_unsupported_reason(chain: str, block_tag: int | None) -> str | None:
    """Return an error string iff a block-pinned read was requested on a chain
    whose balance provider cannot honour it, else ``None``.

    VIB-3350: block-anchored reads (``block_tag > 0``) are an EVM read-after-write
    construct served by ``Web3BalanceProvider``'s block-keyed cache. The Solana
    balance provider has no historical-block semantics and its ``get_balance`` /
    ``get_native_balance`` accept no ``block`` kwarg — passing one raises
    ``TypeError`` and surfaces as an opaque ``INTERNAL`` on the gateway (the
    production perimeter). Reject the request loudly with ``INVALID_ARGUMENT`` so
    the caller gets a clear contract violation instead of a 500. Guarding on
    ``is_solana_chain`` mirrors the existing non-EVM routing branches in this file
    (the only non-EVM provider is Solana; every EVM provider accepts ``block``).
    """
    if block_tag is not None and is_solana_chain(chain):
        return f"block-pinned balance reads (block_tag={block_tag}) are not supported on non-EVM chain '{chain}'"
    return None


# VIB-5310 — PT/YT-USD confidence numerics. The coarse ``confidence_band`` is
# AUTHORITATIVE (consumers MUST NOT re-threshold the raw ``confidence`` double);
# the double is informational and kept CONSISTENT with the band: HIGH carries the
# measured underlying-price confidence (≤ 1.0), ESTIMATED degrades it (the
# underlying was measured but STALE), UNAVAILABLE is 0.0. Per the ratified AC,
# ESTIMATED is reserved for measured-but-degraded inputs — a MISSING required
# read (e.g. pt_to_asset_rate) is UNMEASURED, never AVAILABLE/ESTIMATED with a
# fabricated at-par rate. The ``stale`` bool still rides separately so a consumer
# sees the raw freshness signal alongside the band.
_PT_ESTIMATED_CONF_CAP = 0.5


class _UnpriceableUnderlying(Exception):
    """The SY underlying could not be priced by any source (EXPECTED → UNMEASURED).

    Distinguished from an unexpected read error (→ ERRORED): an unpriceable
    underlying is a measured "no price" (Empty≠Zero), not an infrastructure
    failure. Carries no numeric price.
    """


def _build_pt_price_response(
    *,
    symbol: str,
    chain: str,
    quote: str,
    availability: "gateway_pb2.PtPriceAvailability.ValueType",
    confidence_band: "gateway_pb2.PtPriceConfidenceBand.ValueType",
    price: str = "",
    underlying_price: str = "",
    pt_to_asset_rate: str = "",
    source: str = "",
    confidence: float = 0.0,
    timestamp: int = 0,
    stale: bool = False,
    maturity_ts: int = 0,
    days_to_maturity: int = 0,
) -> "gateway_pb2.PtPriceResponse":
    """Construct a ``PtPriceResponse``, enforcing the never-empty-AVAILABLE guard.

    Structural Empty≠Zero invariant (VIB-5309/5310): a response may carry
    ``availability=AVAILABLE`` ONLY with a non-empty ``price``. Any attempt to
    emit AVAILABLE without a price is a provider bug, not a runtime condition —
    so this raises ``ValueError`` rather than silently shipping a wire message a
    consumer would trust. Every GetPtPrice return path goes through here, making
    the corrupt state unrepresentable.
    """
    if availability == gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE and not price:
        raise ValueError(
            "PT price provider invariant violated: availability=AVAILABLE requires a "
            "non-empty price (Empty≠Zero, VIB-5309). Emit UNMEASURED/ERRORED instead."
        )
    return gateway_pb2.PtPriceResponse(
        symbol=symbol,
        chain=chain,
        quote=quote,
        price=price,
        availability=availability,
        confidence=confidence,
        confidence_band=confidence_band,
        underlying_price=underlying_price,
        pt_to_asset_rate=pt_to_asset_rate,
        source=source,
        timestamp=timestamp,
        stale=stale,
        maturity_ts=maturity_ts,
        days_to_maturity=days_to_maturity,
    )


class MarketServiceServicer(gateway_pb2_grpc.MarketServiceServicer):
    """Implements MarketService gRPC interface.

    Provides market data access for strategy containers:
    - GetPrice: Token prices from aggregated sources
    - GetBalance: Token balances from on-chain
    - GetIndicator: Technical indicators (RSI, MACD, etc.)
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize MarketService.

        Args:
            settings: Gateway settings with API keys and configuration.
        """
        self.settings = settings
        self._price_aggregator: Any = None
        # Last-resort price fallback (populated by _do_initialize). Consulted
        # by GetPrice only when the primary aggregator raises AllDataSourcesFailed.
        self._manual_price_override: Any = None
        self._balance_providers: dict[str, object] = {}
        # Per-chain OnChainLookup for address-based price resolution. Lets an
        # unknown contract address in a GetPrice request be resolved on-chain
        # (symbol/decimals) without going through gRPC to our own TokenService.
        self._onchain_lookups: dict[str, Any] = {}
        self._onchain_lookups_lock = asyncio.Lock()
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self.wallet_registry: object | None = None
        # Wired up by GatewayServer after both services are registered so that
        # balance providers can fall back to TokenService's dynamic resolution
        # stack (CoinGecko / DexScreener / protocol APIs) for symbols absent
        # from the static registry.
        self._token_servicer: Any = None
        # Negative-miss cache: (chain, symbol) -> expiry monotonic time.
        # Prevents repeated slow API calls for symbols that don't exist.
        self._dynamic_miss_cache: dict[tuple[str, str], float] = {}
        # pool_id -> PoolKey cache (VIB-4472 / T03). Lazy-built via the
        # ``GatewayPoolKeyCacheCapability`` provider's ``build_cache`` on
        # first ``LookupV4PoolKey`` lookup miss. Today's sole provider is
        # the Uniswap V4 connector — adding another pool-keyed protocol
        # is a connector edit, not a market_service edit (VIB-4818).
        # VIB-4426 — ``_pool_key_cache_lock`` serialises concurrent
        # first-call construction. Without it, two concurrent
        # ``LookupV4PoolKey`` requests could both observe ``None``, both
        # instantiate, and the second one would silently overwrite the
        # first — discarding any in-flight backfill progress.
        self._pool_key_cache: PoolKeyCacheProtocol | None = None
        self._pool_key_cache_lock = asyncio.Lock()

    async def close(self) -> None:
        """Close resources held by MarketService (HTTP sessions, etc.)."""
        if self._price_aggregator is not None and hasattr(self._price_aggregator, "close"):
            await self._price_aggregator.close()
        for provider in self._balance_providers.values():
            if hasattr(provider, "close"):
                await provider.close()
        self._balance_providers.clear()
        # Dispose per-chain OnChainLookup instances. Snapshot under the lock,
        # then close outside it so one blocking close can't hold the lock and
        # starve anything else that acquires it. Each close is bounded by a
        # short timeout so a hung RPC client can't wedge shutdown.
        async with self._onchain_lookups_lock:
            lookups = list(self._onchain_lookups.items())
            self._onchain_lookups.clear()
        for chain, lookup in lookups:
            if not hasattr(lookup, "close"):
                continue
            try:
                await asyncio.wait_for(lookup.close(), timeout=2.0)
            except TimeoutError:
                logger.warning("Timed out closing OnChainLookup for %s; continuing shutdown", chain)
            except Exception as e:
                logger.warning("Error closing OnChainLookup for %s: %s", chain, e)

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of data providers."""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            self._do_initialize()

    def _do_initialize(self) -> None:
        """Build price sources and aggregator based on current settings.chains.

        Must be called while holding self._init_lock.
        """
        from almanak.framework.data.interfaces import BasePriceSource
        from almanak.gateway.data.price.aggregator import PriceAggregator
        from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource
        from almanak.gateway.data.price.manual_override import ManualPriceOverrideSource
        from almanak.gateway.data.price.onchain import OnChainPriceSource
        from almanak.gateway.validation import is_solana_chain

        # Determine primary chain for on-chain pricing.
        # IMPORTANT: Never default to a hardcoded chain -- that silently gives wrong
        # Chainlink oracle data for strategies running on a different chain (QA #4/#7/#8).
        chain = self.settings.chains[0] if self.settings.chains else None

        # Create price sources
        cg_source = CoinGeckoPriceSource(
            api_key=self.settings.coingecko_api_key if self.settings.coingecko_api_key is not None else "",
            cache_ttl=30,
        )

        sources: list[BasePriceSource]
        if not chain:
            # No chain configured -- on-chain pricing unavailable.
            # This can happen with standalone `almanak gateway` without --chains.
            sources = [cg_source]
            logger.warning(
                "MarketService: No chain configured -- on-chain (Chainlink) pricing DISABLED. "
                "Only CoinGecko is available. Pass --chains to the gateway or set "
                "ALMANAK_GATEWAY_CHAINS for accurate on-chain pricing."
            )
        elif is_solana_chain(chain):
            # Solana: Pyth (primary) + DexScreener (secondary) + CoinGecko (fallback)
            # OnChainPriceSource is EVM-only (Chainlink), skip it for Solana
            from almanak.gateway.data.price.dexscreener import DexScreenerPriceSource
            from almanak.gateway.data.price.pyth import PythPriceSource

            pyth_source = PythPriceSource(cache_ttl=15)
            # Solana-only gateway: keep a default chain so tokens arriving
            # without a ResolvedToken still dispatch to the right platform.
            dexscreener_source = DexScreenerPriceSource(default_chain_id="solana", cache_ttl=30)
            sources = [pyth_source, dexscreener_source, cg_source]
            logger.info("MarketService: Pyth (primary) + DexScreener + CoinGecko (fallback), chain=%s", chain)
        else:
            # EVM: 4-source pricing for production resilience.
            # All sources are queried concurrently; PriceAggregator returns the
            # median with outlier detection. Sources that don't support a token
            # raise DataSourceUnavailable, which the aggregator handles gracefully.
            from almanak.framework.data.tokens import get_token_resolver
            from almanak.gateway.data.price.binance import BinancePriceSource
            from almanak.gateway.data.price.dexscreener import DexScreenerPriceSource

            onchain_source = OnChainPriceSource(chain=chain, network=self.settings.network)
            binance_source = BinancePriceSource(cache_ttl=30, request_timeout=5.0)
            # Keep the primary chain as the default so bare-symbol requests
            # (no ResolvedToken) still dispatch correctly. Multi-chain price
            # requests carry a ResolvedToken whose .chain overrides this.
            dexscreener_source = DexScreenerPriceSource(
                default_chain_id=chain.lower(),
                cache_ttl=30,
                token_resolver=get_token_resolver(),
            )

            sources = [onchain_source, binance_source, dexscreener_source, cg_source]
            logger.info(
                "MarketService: 4-source EVM pricing (Chainlink + Binance + DexScreener + CoinGecko), chain=%s",
                chain,
            )

        # VIB-4841 / FR-5002: pass the stablecoin peg fast-path config from
        # gateway settings. getattr with defaults keeps older Settings shapes
        # (and test stubs) working without the new fields.
        self._price_aggregator = PriceAggregator(
            sources=sources,
            stablecoin_verify=getattr(self.settings, "stablecoin_verify", False),
            stablecoin_chainlink_check_interval=getattr(self.settings, "stablecoin_chainlink_check_interval", 50),
        )
        # Last-resort manual override source — reads ALMANAK_PRICE_OVERRIDE_<TOKEN>
        # env vars. Kept OUT of the aggregator's median vote because
        # PriceAggregator computes a plain median without weighting by
        # confidence (a live $0.20 + override $0.12 would yield a corrupt
        # $0.16). Consulted only in GetPrice when the aggregator raises
        # AllDataSourcesFailed. Added for Bug 3 of the 0G DogFooding report
        # (2026-04-16). Off by default — a mis-set env var could corrupt
        # teardown/slippage decisions, so operators must explicitly opt in.
        if getattr(self.settings, "enable_manual_price_overrides", False):
            self._manual_price_override = ManualPriceOverrideSource()
            logger.info(
                "MarketService: manual price override fallback ENABLED. "
                "ALMANAK_PRICE_OVERRIDE_<TOKEN> env vars will be consulted "
                "if every primary oracle source fails for a given token."
            )
        else:
            self._manual_price_override = None

        self._initialized = True

    async def reinitialize(self, chain: str) -> None:
        """Re-initialize price sources with full pricing stack for the given chain.

        Called by RegisterChains when chain info becomes available after startup.
        Upgrades from CoinGecko-only to the full 4-source stack.
        """
        async with self._init_lock:
            if self._price_aggregator is not None and hasattr(self._price_aggregator, "close"):
                try:
                    await self._price_aggregator.close()
                except Exception as e:
                    logger.warning("Error closing old price aggregator during reinit: %s", e)
                self._price_aggregator = None

            if not self.settings.chains:
                self.settings.chains = [chain]
            else:
                # Always ensure the requested chain is at index 0 (primary),
                # since _do_initialize uses chains[0] for on-chain pricing.
                if chain in self.settings.chains:
                    self.settings.chains.remove(chain)
                self.settings.chains.insert(0, chain)

            self._initialized = False
            self._do_initialize()

        logger.info("MarketService re-initialized with chain=%s", chain)

    async def warmup(self, wallet_address: str | None = None) -> None:
        """Pre-warm price caches and balance providers to avoid first-call delays.

        Fetches a common price (ETH/USD) to warm all HTTP connections and caches
        in the price sources. Optionally pre-warms the balance provider for the
        configured chain/wallet.

        Args:
            wallet_address: Optional wallet address to pre-warm balance provider.
        """
        await self._ensure_initialized()

        # Warm price sources by fetching a common token price.
        # This forces HTTP connection setup, API auth, and cache population
        # so the first strategy price() call doesn't block for 30s+.
        if self._price_aggregator is not None:
            try:
                await self._price_aggregator.get_aggregated_price("ETH", "USD")
                logger.info("Price cache pre-warmed (ETH/USD fetched)")
            except Exception as e:
                logger.warning("Price cache warmup failed (will retry on first call): %s", e)

        # Pre-warm balance provider for the configured chain if a wallet is available
        chain = self.settings.chains[0] if self.settings.chains else None
        if chain and wallet_address:
            try:
                await self._get_balance_provider(chain, wallet_address)
                logger.info("Balance provider pre-warmed for chain=%s", chain)
            except Exception as e:
                logger.warning("Balance provider warmup failed for chain=%s: %s", chain, e)

    def _make_dynamic_symbol_resolver(self):
        """Return an async callable that resolves unknown EVM symbols via TokenService.

        The returned callable has signature:
            async (symbol: str, chain: str) -> tuple[str, str, int] | None
        returning (symbol, address, decimals) on success.

        Reads self._token_servicer at call time so providers created before
        the token servicer is wired up still benefit once it is set.
        """

        async def _resolver(symbol: str, chain: str):
            if self._token_servicer is None:
                logger.warning(
                    "dynamic_symbol_resolver invoked but _token_servicer not wired; check server.py registration order"
                )
                return None

            # Short-circuit repeated API calls for symbols confirmed absent.
            miss_key = (chain, symbol)
            now = time.monotonic()
            if miss_key in self._dynamic_miss_cache and self._dynamic_miss_cache[miss_key] > now:
                return None

            try:
                response = await asyncio.wait_for(
                    self._token_servicer._try_evm_symbol_lookup(symbol, chain),
                    timeout=8.0,
                )
                if response is not None and response.address:
                    return (response.symbol, response.address, response.decimals)
                # Definitive miss — cache to avoid repeating the full tier walk.
                self._dynamic_miss_cache[miss_key] = now + 60.0
            except TimeoutError:
                logger.warning("Dynamic token lookup timed out for %s on %s (8 s)", symbol, chain)
                self._dynamic_miss_cache[miss_key] = now + 30.0
            except AmbiguousTokenError:
                raise  # Preserve the candidate-address message; GetBalance surfaces it as INVALID_ARGUMENT
            except Exception as exc:
                logger.warning("TokenService dynamic lookup failed for %s on %s: %s", symbol, chain, exc)
            return None

        return _resolver

    async def _get_balance_provider(self, chain: str, wallet_address: str):
        """Get or create balance provider for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "base", "solana")
            wallet_address: Wallet address to query

        Returns:
            Balance provider for the specified chain (Web3 for EVM, Solana for Solana)
        """
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        if cache_key not in self._balance_providers:
            network = self.settings.network
            rpc_url = get_rpc_url(chain, network=network)

            if is_solana_chain(chain):
                from almanak.gateway.data.balance.solana_provider import SolanaBalanceProvider

                self._balance_providers[cache_key] = SolanaBalanceProvider(
                    rpc_url=rpc_url,
                    wallet_address=wallet_address,
                    chain=chain,
                )
            else:
                from almanak.gateway.data.balance import Web3BalanceProvider

                # Anvil tests run pre-read + tx + post-read inside the cache
                # window, returning stale pre-tx values to reconciliation
                # (zero deltas despite swaps landing on-chain). Disable caching
                # on anvil so post-tx reads always hit the fork; mainnet uses
                # Web3BalanceProvider's own default cache_ttl.
                extra_kwargs: dict[str, Any] = {"cache_ttl": 0} if network == "anvil" else {}
                self._balance_providers[cache_key] = Web3BalanceProvider(
                    rpc_url=rpc_url,
                    wallet_address=wallet_address,
                    chain=chain,
                    dynamic_symbol_resolver=self._make_dynamic_symbol_resolver(),
                    **extra_kwargs,
                )

        return self._balance_providers[cache_key]

    async def _get_onchain_lookup(self, chain: str) -> Any:
        """Get or create an OnChainLookup for a chain (gateway-internal).

        Mirrors the pattern in TokenService but kept local so GetPrice can
        resolve addresses without making a cross-service gRPC call.
        """
        from almanak.gateway.services.onchain_lookup import OnChainLookup
        from almanak.gateway.utils import get_rpc_url

        async with self._onchain_lookups_lock:
            if chain not in self._onchain_lookups:
                rpc_url = get_rpc_url(chain, network=self.settings.network)
                self._onchain_lookups[chain] = OnChainLookup(rpc_url=rpc_url)
            return self._onchain_lookups[chain]

    async def _resolve_token_for_pricing(
        self,
        token: str,
        requested_chain: str,
    ) -> Any | None:
        """Resolve a token input (symbol or address) into a ResolvedToken.

        Only resolves EVM contract addresses via on-chain ERC20 metadata.
        Returns None when the input is a symbol, chain is unknown, or the
        on-chain lookup fails — callers then fall through to the normal
        symbol-based aggregator path.

        The returned ResolvedToken carries the chain and address needed by
        price sources that support address-based lookups (e.g. CoinGecko's
        /simple/token_price/{platform} endpoint).
        """
        if not _EVM_ADDRESS_RE.match(token):
            return None

        chain = (requested_chain or "").lower()
        if not chain:
            # No explicit chain. Only infer from settings if it is UNAMBIGUOUS —
            # exactly one configured chain. A multi-chain gateway with no hint
            # would otherwise silently query the wrong RPC for a token that
            # lives on a secondary chain, returning either "not a contract" or
            # (worse) a price from a same-address token on the wrong chain.
            configured = [c for c in (self.settings.chains or []) if c]
            if len(configured) == 1:
                chain = configured[0].lower()
            elif len(configured) > 1:
                # Strict contract (Phase 2, VIB-3259): multi-chain gateway MUST
                # receive an explicit chain for address-based lookups. Raise
                # so GetPrice can translate to gRPC INVALID_ARGUMENT — silently
                # skipping here would just cascade into a confusing "Unknown
                # token" downstream with no hint at the real cause.
                raise MultiChainAmbiguousPriceRequest(token, configured)
            else:
                # Zero configured chains: nothing we can do. Fall through to
                # symbol-based resolution (caller may still get a price from a
                # chain-agnostic symbol source like CoinGecko's /simple/price).
                return None

        # Enforce the gateway's chain allowlist. Without this, a caller could
        # pass any enum-valid chain name (e.g. a dev chain the operator never
        # wired up) and make the gateway dial an RPC it wasn't meant to —
        # crossing the trust boundary `GetBalance` already protects.
        try:
            chain = validate_chain(chain)
        except ValidationError as e:
            logger.info(
                "Address price lookup for %s skipped: chain %r not allowed (%s)",
                token,
                requested_chain or chain,
                e,
            )
            return None

        # Require the chain to be one this gateway is configured for. A chain
        # can be in ALLOWED_CHAINS but not in this gateway's settings.chains,
        # which would still let a caller force an on-chain lookup on a chain
        # the operator never opted into.
        configured_chains = {c.lower() for c in (self.settings.chains or []) if c}
        if configured_chains and chain not in configured_chains:
            logger.info(
                "Address price lookup for %s on %s skipped: chain not in gateway's configured chains %s",
                token,
                chain,
                sorted(configured_chains),
            )
            return None

        if is_solana_chain(chain):
            return None

        try:
            from almanak.core.enums import Chain
            from almanak.framework.data.tokens import ResolvedToken
            from almanak.framework.data.tokens.models import CHAIN_ID_MAP

            # Chain enum values are uppercased (e.g. Chain("BASE")); config
            # usually surfaces them lowercased. Try uppercase first, fall
            # back to the raw string so callers using either form work.
            try:
                chain_enum = Chain(chain.upper())
            except ValueError:
                chain_enum = Chain(chain)
        except (ImportError, ValueError) as e:
            logger.debug("Cannot map %s to Chain enum for address resolution: %s", chain, e)
            return None

        try:
            lookup = await self._get_onchain_lookup(chain)
            metadata = await asyncio.wait_for(
                lookup.lookup(chain, token),
                timeout=_ONCHAIN_LOOKUP_TIMEOUT_SECONDS,
            )
        except Exception as e:
            logger.info("On-chain metadata lookup failed for %s on %s: %s", token, chain, e)
            return None

        if metadata is None:
            return None

        chain_id = CHAIN_ID_MAP.get(chain_enum, 0)

        try:
            return ResolvedToken(
                symbol=metadata.symbol,
                address=metadata.address,
                decimals=metadata.decimals,
                chain=chain_enum,
                chain_id=chain_id,
                name=metadata.name,
                source="on_chain",
                is_verified=False,
            )
        except Exception as e:  # Defensive: ResolvedToken.__post_init__ validates inputs
            logger.warning("Failed to build ResolvedToken from on-chain metadata for %s: %s", token, e)
            return None

    async def GetPrice(
        self,
        request: gateway_pb2.PriceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PriceResponse:
        """Get token price from aggregated sources.

        Args:
            request: Price request with token, quote currency, and optional chain hint
            context: gRPC context

        Returns:
            PriceResponse with price, timestamp, source, confidence
        """
        await self._ensure_initialized()

        token = request.token
        quote = request.quote or "USD"

        # Validate the optional chain hint up front. Empty is fine — the
        # address resolver falls back to settings.chains when unambiguous.
        # But a non-empty, bad chain is caller error; mirror GetBalance /
        # RpcService / ExecutionService and surface INVALID_ARGUMENT rather
        # than silently letting it slip through.
        requested_chain = ""
        if request.chain:
            try:
                requested_chain = validate_chain(request.chain)
            except ValidationError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return gateway_pb2.PriceResponse()

        # If the caller sent a contract address, resolve it on-chain so every
        # downstream price source can use address-based endpoints. This is
        # what unlocks pricing for tokens absent from our hardcoded registry.
        try:
            resolved_token = await self._resolve_token_for_pricing(token, requested_chain)
        except MultiChainAmbiguousPriceRequest as e:
            # Multi-chain gateway + empty chain + EVM address = caller contract
            # violation. Surface as INVALID_ARGUMENT so the caller sees the
            # real cause (missing chain hint) instead of a generic pricing miss.
            logger.info("GetPrice rejected: %s", e)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PriceResponse()

        try:
            result = await self._price_aggregator.get_aggregated_price(token, quote, resolved_token=resolved_token)
            details = self._price_aggregator.get_last_details(token, quote)

            response = gateway_pb2.PriceResponse(
                price=str(result.price),
                timestamp=int(result.timestamp.timestamp()),
                source=result.source,
                confidence=result.confidence,
                stale=result.stale,
            )
            if details:
                response.sources_ok.extend(details.get("sources_ok", []))
                for k, v in details.get("sources_failed", {}).items():
                    response.sources_failed[k] = v
                response.outliers.extend(details.get("outliers", []))
            return response
        except Exception as e:
            # Try native->wrapped alias fallback (e.g., MNT->WMNT).
            # The alias is a symbol, so any address-based resolved_token no
            # longer applies — forward None so the alias is priced by symbol.
            alias = NATIVE_PRICE_ALIASES.get(token.upper())
            if alias:
                try:
                    result = await self._price_aggregator.get_aggregated_price(alias, quote, resolved_token=None)
                    logger.info(f"GetPrice: {token} resolved via alias {alias}")
                    response = gateway_pb2.PriceResponse(
                        price=str(result.price),
                        timestamp=int(result.timestamp.timestamp()),
                        source=result.source,
                        confidence=result.confidence,
                        stale=result.stale,
                    )
                    details = self._price_aggregator.get_last_details(alias, quote)
                    if details:
                        response.sources_ok.extend(details.get("sources_ok", []))
                        for k, v in details.get("sources_failed", {}).items():
                            response.sources_failed[k] = v
                        response.outliers.extend(details.get("outliers", []))
                    return response
                except Exception as alias_err:
                    logger.debug(f"GetPrice: alias {alias} also failed for {token}/{quote}: {alias_err}")

            from almanak.framework.data.interfaces import (
                AllDataSourcesFailed,
                DataSourceUnavailable,
            )
            from almanak.gateway.data.price.aggregator import _is_known_unpriceable

            # Last-resort fallback: consult the manual override source if all
            # real oracle sources failed. Kept out of the aggregator's median
            # vote so a low-confidence override never corrupts a live price;
            # only activates when no real source produced a result. Logged
            # at WARNING so audit trails always show when a price came from
            # an operator-supplied env var instead of a real oracle.
            # ``getattr`` tolerates ``__new__``-constructed test doubles that
            # bypass ``__init__``.
            manual_override = getattr(self, "_manual_price_override", None)
            if isinstance(e, AllDataSourcesFailed) and manual_override is not None:
                try:
                    override_result = await manual_override.get_price(token, quote)
                    logger.warning(
                        "GetPrice: %s/%s unresolved by every primary oracle source; "
                        "returning MANUAL OVERRIDE price=%s confidence=%s. "
                        "This value came from an ALMANAK_PRICE_OVERRIDE_* env var, "
                        "not a real oracle — confirm it is current before acting on it.",
                        token,
                        quote,
                        override_result.price,
                        override_result.confidence,
                    )
                    return gateway_pb2.PriceResponse(
                        price=str(override_result.price),
                        timestamp=int(override_result.timestamp.timestamp()),
                        source=override_result.source,
                        confidence=override_result.confidence,
                        stale=override_result.stale,
                    )
                except DataSourceUnavailable:
                    pass  # No override configured — fall through to the normal error path

            # Only downgrade to WARNING for known-unpriceable tokens when the failure
            # is "all sources failed" (expected). Keep ERROR for infra/unexpected failures.
            if isinstance(e, AllDataSourcesFailed) and _is_known_unpriceable(token):
                logger.warning(f"GetPrice failed for {token}/{quote}: {e}")
            else:
                logger.error(f"GetPrice failed for {token}/{quote}: {e}")
            set_error_from_upstream(context, e, upstream="price_aggregator")
            return gateway_pb2.PriceResponse()

    async def GetPtPrice(
        self,
        request: gateway_pb2.PtPriceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PtPriceResponse:
        """Compose a Pendle PT/YT-USD price (VIB-5310, epic VIB-5299, M1).

        The gateway is the PRICE AUTHORITY: ``pt_usd = underlying/USD ×
        pt_to_asset_rate`` is composed HERE from two gateway-sourced legs —
        ``pt_to_asset_rate`` via the connector's on-chain market reader (a
        gateway-internal eth_call, no new egress) and underlying/USD via the
        existing price aggregator. There is no direct Pendle price feed.

        Honest availability per the ratified AC (Empty≠Zero — never ``"0"`` for
        unmeasured, never an at-par fabrication):

        * both legs measured AND fresh → ``AVAILABLE`` + band ``HIGH``.
        * both legs measured but underlying STALE → ``AVAILABLE`` + band
          ``ESTIMATED`` + ``stale=True`` (HIGH requires freshness; ESTIMATED is
          measured-but-degraded only).
        * ``pt_to_asset_rate`` missing → ``UNMEASURED`` (NO price). The at-par
          (rate=1.0) default is FORBIDDEN — it overvalues the PT to par.
        * underlying unpriceable → ``UNMEASURED`` (NO price). Never fabricate.
        * underlying-price read raised unexpectedly → ``ERRORED`` (NO price).
        * YT in M1 → ``UNMEASURED`` (held-YT valuation is VIB-5322/M3), never a
          guess.

        ``confidence_band`` is AUTHORITATIVE; the ``confidence`` double is kept
        consistent with it (see ``_PT_ESTIMATED_CONF_CAP``). The ``stale`` flag
        rides separately so consumers see the raw freshness signal too.
        """
        await self._ensure_initialized()

        symbol = (request.symbol or "").strip()
        quote = request.quote or "USD"

        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PtPriceResponse()

        if not symbol:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("symbol is required for GetPtPrice")
            return gateway_pb2.PtPriceResponse()

        # M1 is USD-only. Reject any other quote LOUDLY rather than compose a USD
        # number and silently echo the caller's label onto it (a EUR-labelled USD
        # price at AVAILABLE+HIGH is wrong-label money). Empty == default USD.
        if request.quote and request.quote.upper() != "USD":
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"GetPtPrice supports quote=USD only in M1 (got {request.quote!r})")
            return gateway_pb2.PtPriceResponse()

        unmeasured_band = gateway_pb2.PT_PRICE_CONFIDENCE_BAND_UNAVAILABLE

        # 1. Resolve symbol → market + underlying (connector-owned, no egress).
        ref = self._resolve_principal_token_ref(symbol, chain, request.maturity_ts)
        if ref is None:
            return _build_pt_price_response(
                symbol=symbol,
                chain=chain,
                quote=quote,
                availability=gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED,
                confidence_band=unmeasured_band,
                source="unmeasured:symbol-not-resolved-or-no-underlying",
                maturity_ts=request.maturity_ts,
            )

        # 2. YT held-valuation is deferred to VIB-5322 / M3 — never a guess.
        if ref.family == "YT":
            return _build_pt_price_response(
                symbol=symbol,
                chain=chain,
                quote=quote,
                availability=gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED,
                confidence_band=unmeasured_band,
                source="unmeasured:yt-valuation-deferred-VIB-5322",
                maturity_ts=ref.maturity_ts,
            )

        # 3. Price the underlying (existing aggregator). Unpriceable → UNMEASURED
        #    (expected); unexpected error → ERRORED. Both carry NO price.
        try:
            underlying_result = await self._price_underlying_usd(ref.underlying_token, chain)
        except _UnpriceableUnderlying as e:
            logger.warning("GetPtPrice: underlying %s unpriceable for %s: %s", ref.underlying_token, symbol, e)
            return _build_pt_price_response(
                symbol=symbol,
                chain=chain,
                quote=quote,
                availability=gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED,
                confidence_band=unmeasured_band,
                source=f"unmeasured:underlying-unpriceable:{ref.underlying_token}",
                maturity_ts=ref.maturity_ts,
            )
        except Exception as e:
            logger.error("GetPtPrice: underlying price read errored for %s: %s", symbol, e)
            return _build_pt_price_response(
                symbol=symbol,
                chain=chain,
                quote=quote,
                availability=gateway_pb2.PT_PRICE_AVAILABILITY_ERRORED,
                confidence_band=unmeasured_band,
                source="errored:underlying-price-read",
                maturity_ts=ref.maturity_ts,
            )

        underlying_price = underlying_result.price

        # 4. Read pt_to_asset_rate + days-to-maturity (gateway-internal eth_call).
        #    The direct-mode reader does a BLOCKING web3 .call(); run it OFF the
        #    asyncio event loop via to_thread so a slow RPC can't stall every
        #    concurrent gRPC handler on the gateway (perimeter liveness). The
        #    reader itself carries a bounded web3 request timeout so the worker
        #    thread can't hang forever either.
        rate, days_to_maturity, rate_reason = await asyncio.to_thread(self._read_pt_market, ref, chain)

        # 5. Missing PT rate → UNMEASURED. Per the ratified AC, a missing
        #    required read is NEVER fabricated: defaulting pt_to_asset_rate to
        #    1.0 (at-par) would overvalue the PT to its maximum redemption value
        #    (PT trades at ≤ par before maturity), so there is no AVAILABLE path
        #    here. ``underlying_price`` is echoed for transparency (it WAS
        #    measured); ``price`` stays empty (Empty≠Zero).
        if rate is None:
            return _build_pt_price_response(
                symbol=symbol,
                chain=chain,
                quote=quote,
                availability=gateway_pb2.PT_PRICE_AVAILABILITY_UNMEASURED,
                confidence_band=unmeasured_band,
                underlying_price=str(underlying_price),
                source=f"unmeasured:pt-rate-unavailable({rate_reason})",
                timestamp=int(underlying_result.timestamp.timestamp()),
                maturity_ts=ref.maturity_ts,
                days_to_maturity=days_to_maturity or 0,
            )

        # 6. Both legs measured → compose. Band: HIGH only when ALL inputs are
        #    measured AND fresh; a measured-but-STALE underlying drops the band
        #    to ESTIMATED (with the stale flag set) — ESTIMATED is reserved for
        #    measured-but-degraded, NEVER for a fabricated/missing input.
        return self._compose_available_pt_response(
            symbol=symbol,
            chain=chain,
            quote=quote,
            ref=ref,
            underlying_result=underlying_result,
            rate=rate,
            days_to_maturity=days_to_maturity,
        )

    def _compose_available_pt_response(
        self,
        *,
        symbol: str,
        chain: str,
        quote: str,
        ref: Any,
        underlying_result: Any,
        rate: Decimal,
        days_to_maturity: int | None,
    ) -> "gateway_pb2.PtPriceResponse":
        """Stamp an AVAILABLE PT/USD response from two measured legs.

        ``HIGH`` only when the underlying is fresh; a stale underlying →
        ``ESTIMATED`` + ``stale=True`` (the band carries freshness, the separate
        ``stale`` flag carries the raw signal). ``confidence_band`` is
        authoritative; the ``confidence`` double is kept consistent.
        """
        underlying_price = underlying_result.price
        pt_usd = underlying_price * rate
        fresh = not underlying_result.stale
        if fresh:
            band = gateway_pb2.PT_PRICE_CONFIDENCE_BAND_HIGH
            confidence = underlying_result.confidence
        else:
            band = gateway_pb2.PT_PRICE_CONFIDENCE_BAND_ESTIMATED
            confidence = min(underlying_result.confidence, _PT_ESTIMATED_CONF_CAP)

        return _build_pt_price_response(
            symbol=symbol,
            chain=chain,
            quote=quote,
            availability=gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE,
            confidence_band=band,
            price=str(pt_usd),
            underlying_price=str(underlying_price),
            pt_to_asset_rate=str(rate),
            source=f"composition:getPtToAssetRate×{underlying_result.source}",
            confidence=confidence,
            timestamp=int(underlying_result.timestamp.timestamp()),
            stale=underlying_result.stale,
            maturity_ts=ref.maturity_ts,
            days_to_maturity=days_to_maturity or 0,
        )

    def _resolve_principal_token_ref(self, symbol: str, chain: str, maturity_ts: int):
        """Dispatch symbol→market+underlying to connector capability providers.

        Capability dispatch (never a protocol-name literal): any connector
        implementing ``GatewayPrincipalTokenPriceCapability`` for ``chain`` may
        resolve the symbol. Returns the first non-None
        ``PrincipalTokenMarketRef`` or ``None`` when unresolved (→ UNMEASURED).
        A misbehaving provider is logged and skipped, never crashing the call.
        """
        from almanak.connectors._base.gateway_capabilities import (
            GatewayPrincipalTokenPriceCapability,
        )
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

        for provider in GATEWAY_REGISTRY.capability_providers(GatewayPrincipalTokenPriceCapability):  # type: ignore[type-abstract]
            try:
                if chain not in provider.principal_token_price_chains():
                    continue
                ref = provider.resolve_principal_token_ref(symbol=symbol, chain=chain, maturity_ts=maturity_ts)
            except Exception as e:
                logger.warning(
                    "GetPtPrice: principal-token resolver %s failed for %s: %s", type(provider).__name__, symbol, e
                )
                continue
            if ref is not None:
                return ref
        return None

    async def _price_underlying_usd(self, token: str, chain: str):
        """Price the SY underlying in USD via the existing aggregator.

        Raises :class:`_UnpriceableUnderlying` for an EXPECTED "no source has a
        price" (→ UNMEASURED); propagates any other exception (→ ERRORED).
        """
        from almanak.framework.data.interfaces import (
            AllDataSourcesFailed,
            DataSourceUnavailable,
        )

        try:
            resolved_token = await self._resolve_token_for_pricing(token, chain)
        except Exception:
            # Address metadata resolution is best-effort; fall back to symbol path.
            resolved_token = None

        try:
            return await self._price_aggregator.get_aggregated_price(token, "USD", resolved_token=resolved_token)
        except (AllDataSourcesFailed, DataSourceUnavailable) as e:
            raise _UnpriceableUnderlying(str(e)) from e

    def _read_pt_market(self, ref, chain: str) -> tuple[Decimal | None, int | None, str]:
        """Read ``pt_to_asset_rate`` + days-to-maturity via the connector reader.

        Returns ``(rate_or_None, days_or_None, reason)``. ``rate`` is ``None``
        when the reader cannot be built or the read fails / is non-positive; the
        CALLER maps a ``None`` rate to ``UNMEASURED`` (no price) — deliberately
        NOT ``valuation.py``'s at-par (1.0) fallback, which overvalues the PT and
        is forbidden by the ratified AC. Reads run through the connector's
        on-chain market reader in direct mode, a gateway-internal eth_call path
        that reuses the EXISTING ``# vib-2986-exempt`` marker (no new egress, no
        new marker).
        """
        reader = self._build_pt_reader(ref.protocol, chain)
        if reader is None:
            return None, None, "rate-reader-unavailable"

        rate: Decimal | None = None
        reason = ""
        try:
            raw = reader.get_pt_to_asset_rate(ref.market_address)
            if raw is not None and raw > 0:
                rate = raw
            else:
                reason = "pt_to_asset_rate-non-positive"
        except Exception as e:
            logger.debug("GetPtPrice: pt_to_asset_rate read failed for %s: %s", ref.market_address, e)
            reason = "pt_to_asset_rate-read-failed"

        days: int | None = None
        try:
            days = reader.get_days_to_maturity(ref.market_address)
        except Exception as e:
            logger.debug("GetPtPrice: days_to_maturity read failed for %s: %s", ref.market_address, e)

        return rate, days, reason

    def _build_pt_reader(self, protocol: str, chain: str):
        """Build the connector's on-chain principal-token market reader, or None.

        Routes through the SAME ``GatewayPrincipalTokenPriceCapability`` provider
        that resolved the symbol (gateway/connector isolation, VIB-4121 — the
        gateway never reaches into a strategy-side connector registry). The
        provider builds its reader in direct (rpc_url) mode, reusing the on-chain
        reader's established ``# vib-2986-exempt`` web3 path (no new egress).
        Returns ``None`` (→ caller emits ``UNMEASURED``, never at-par) when no
        provider/reader/chain supports the read.
        """
        from almanak.connectors._base.gateway_capabilities import (
            GatewayPrincipalTokenPriceCapability,
        )
        from almanak.connectors._base.types import ProtocolName
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
        from almanak.gateway.utils import get_rpc_url

        # Protocol-keyed lookup (no ``.protocol`` access on the capability
        # Protocol): the connector that owns ``protocol`` is also the capability
        # provider. ``isinstance`` narrows it to the capability so the reader
        # build is typed without touching the connector base ClassVar.
        connector = GATEWAY_REGISTRY.get(ProtocolName(protocol))
        if not isinstance(connector, GatewayPrincipalTokenPriceCapability):
            return None
        try:
            rpc_url = get_rpc_url(chain, network=self.settings.network)
            return connector.build_principal_token_market_reader(chain=chain, rpc_url=rpc_url)
        except Exception as e:
            logger.debug("GetPtPrice: could not build %s reader for %s: %s", protocol, chain, e)
            return None

    async def GetBalance(
        self,
        request: gateway_pb2.BalanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BalanceResponse:
        """Get token balance for wallet.

        Args:
            request: Balance request with token, chain, wallet_address
            context: gRPC context

        Returns:
            BalanceResponse with balance in human-readable units
        """
        await self._ensure_initialized()

        # If we initialized with CoinGecko-only (no chain at startup) and
        # now have a chain from the request, upgrade to full pricing stack.
        if request.chain and not self.settings.chains:
            try:
                chain = validate_chain(request.chain)
                await self.reinitialize(chain)
            except Exception as e:
                logger.warning("MarketService auto-reinit failed for chain %s: %s", request.chain, e)

        token = request.token

        # Validate chain
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

        # Validate wallet address format (chain-aware: EVM hex or Solana base58)
        try:
            wallet_address = validate_address_for_chain(request.wallet_address, chain, "wallet_address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

        try:
            provider = await self._get_balance_provider(chain, wallet_address)

            # VIB-3350: a block-pinned read (block_tag > 0) is read-after-write
            # correct by construction and uses the immutable block-keyed cache,
            # so it must NOT be perturbed by force_refresh. Only unpinned reads
            # honour force_refresh's cache eviction.
            block_tag = request.block_tag if request.block_tag > 0 else None
            pin_unsupported = _block_pin_unsupported_reason(chain, block_tag)
            if pin_unsupported is not None:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(pin_unsupported)
                return gateway_pb2.BalanceResponse()
            if block_tag is None and request.force_refresh and hasattr(provider, "invalidate_cache"):
                provider.invalidate_cache(token)

            # Chain-scoped native check: only route to get_native_balance when
            # the symbol is actually native to THIS chain. Prevents POL on
            # Ethereum from returning ETH balance, etc.
            if _is_native_symbol(token, chain):
                result = (
                    await provider.get_native_balance(block=block_tag)
                    if block_tag is not None
                    else await provider.get_native_balance()
                )
            else:
                result = (
                    await provider.get_balance(token, block=block_tag)
                    if block_tag is not None
                    else await provider.get_balance(token)
                )

            # Get USD value if available
            balance_usd = ""
            try:
                price_result = await self._price_aggregator.get_aggregated_price(token, "USD")
                balance_usd = str(result.balance * price_result.price)
            except Exception:
                # USD conversion optional. Try the native->wrapped alias (e.g.
                # MATIC/POL -> WMATIC) so that a symbol with weak exchange
                # coverage still gets a price via its wrapped equivalent.
                alias = NATIVE_PRICE_ALIASES.get(token.upper())
                if alias:
                    try:
                        price_result = await self._price_aggregator.get_aggregated_price(alias, "USD")
                        balance_usd = str(result.balance * price_result.price)
                    except Exception:
                        pass

            return gateway_pb2.BalanceResponse(
                balance=str(result.balance),
                balance_usd=balance_usd,
                address=result.address,
                decimals=result.decimals,
                raw_balance=str(result.raw_balance),
                timestamp=int(result.timestamp.timestamp()),
                stale=result.stale,
                # VIB-3350: echo the block this balance was read at. A pinned
                # read at block N returns state at N by construction; 0 for
                # unpinned "latest" reads.
                block_number=block_tag or 0,
            )
        except AmbiguousTokenError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()
        except Exception as e:
            # VIB-2580: In single-chain Anvil mode, balance queries for non-running
            # chains are expected failures. Downgrade to WARNING to avoid noise.
            is_connection_error = "Cannot connect to host" in str(e)
            log_fn = logger.warning if is_connection_error else logger.error
            log_fn(f"GetBalance failed for {token} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

    async def BatchGetBalances(
        self,
        request: gateway_pb2.BatchBalanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BatchBalanceResponse:
        """Get balances for multiple tokens/chains in a single call.

        Executes individual balance queries concurrently. Partial success
        is allowed -- per-response errors are returned for failed queries.

        Args:
            request: Batch balance request with list of BalanceRequest
            context: gRPC context

        Returns:
            BatchBalanceResponse with per-request BalanceResponse
        """
        await self._ensure_initialized()

        async def _get_single_balance(req: gateway_pb2.BalanceRequest) -> gateway_pb2.BalanceResponse:
            """Get a single balance, returning error in response on failure."""
            try:
                chain = validate_chain(req.chain or "arbitrum")
            except ValidationError as e:
                return gateway_pb2.BalanceResponse(error=str(e))

            try:
                wallet_address = validate_address_for_chain(req.wallet_address, chain, "wallet_address")
            except ValidationError as e:
                return gateway_pb2.BalanceResponse(error=str(e))

            token = req.token
            try:
                provider = await self._get_balance_provider(chain, wallet_address)

                # VIB-3350 (audit M3): honour per-request block_tag in batch too —
                # a batched pinned read must NOT silently return "latest" while the
                # wire message claims pinning. Mirrors the unary GetBalance path:
                # a pinned read uses the immutable block-keyed cache and is not
                # perturbed by force_refresh; only unpinned reads evict the cache.
                block_tag = req.block_tag if req.block_tag > 0 else None
                pin_unsupported = _block_pin_unsupported_reason(chain, block_tag)
                if pin_unsupported is not None:
                    return gateway_pb2.BalanceResponse(error=pin_unsupported)
                if block_tag is None and req.force_refresh and hasattr(provider, "invalidate_cache"):
                    provider.invalidate_cache(token)

                # Chain-scoped native check: only route to get_native_balance when
                # the symbol is actually native to THIS chain. Prevents POL on
                # Ethereum from returning ETH balance, etc.
                if _is_native_symbol(token, chain):
                    result = (
                        await provider.get_native_balance(block=block_tag)
                        if block_tag is not None
                        else await provider.get_native_balance()
                    )
                else:
                    result = (
                        await provider.get_balance(token, block=block_tag)
                        if block_tag is not None
                        else await provider.get_balance(token)
                    )

                balance_usd = ""
                try:
                    price_result = await self._price_aggregator.get_aggregated_price(token, "USD")
                    balance_usd = str(result.balance * price_result.price)
                except Exception:
                    # Try native->wrapped alias (MATIC/POL -> WMATIC) before giving up.
                    alias = NATIVE_PRICE_ALIASES.get(token.upper())
                    if alias:
                        try:
                            price_result = await self._price_aggregator.get_aggregated_price(alias, "USD")
                            balance_usd = str(result.balance * price_result.price)
                        except Exception:
                            pass

                return gateway_pb2.BalanceResponse(
                    balance=str(result.balance),
                    balance_usd=balance_usd,
                    address=result.address,
                    decimals=result.decimals,
                    raw_balance=str(result.raw_balance),
                    timestamp=int(result.timestamp.timestamp()),
                    stale=result.stale,
                    block_number=block_tag or 0,  # VIB-3350 (M3): echo the pinned block
                )
            except Exception as e:
                # Log at DEBUG for batch context — individual token failures (e.g. USDT not
                # existing on Base) are expected and should not spam user-facing logs.
                logger.debug("BatchGetBalances: skipped %s on %s: %s", token, chain, e)
                return gateway_pb2.BalanceResponse(error=str(e))

        tasks = [_get_single_balance(req) for req in request.requests]
        responses = await asyncio.gather(*tasks)

        return gateway_pb2.BatchBalanceResponse(responses=list(responses))

    async def GetIndicator(
        self,
        request: gateway_pb2.IndicatorRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.IndicatorResponse:
        """Get technical indicator value.

        Args:
            request: Indicator request with type, token, params
            context: gRPC context

        Returns:
            IndicatorResponse with indicator value and metadata
        """
        indicator_type = request.indicator_type.upper()
        token = request.token
        params = dict(request.params)

        try:
            if indicator_type == "RSI":
                # RSI indicator
                from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider, RSICalculator

                period = int(params.get("period", "14"))
                timeframe = params.get("timeframe", "1h")

                api_key = self.settings.coingecko_api_key if self.settings.coingecko_api_key is not None else ""
                async with CoinGeckoOHLCVProvider(api_key=api_key) as ohlcv_provider:
                    indicator = RSICalculator(ohlcv_provider=ohlcv_provider, default_period=period)
                    value = await indicator.calculate_rsi(token, period=period, timeframe=timeframe)

                return gateway_pb2.IndicatorResponse(
                    value=str(value),
                    metadata={"period": str(period), "timeframe": timeframe},
                    timestamp=int(time.time()),
                )
            else:
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(f"Indicator type '{indicator_type}' not supported")
                return gateway_pb2.IndicatorResponse()

        except Exception as e:
            logger.error(f"GetIndicator failed for {indicator_type} on {token}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.IndicatorResponse()

    async def _get_pool_key_cache(self) -> PoolKeyCacheProtocol:
        """Lazy-construct the pool-key cache (VIB-4472 / T03; VIB-4818).

        Constructed on first ``LookupV4PoolKey`` call so the gateway boot
        path is unaffected when no caller exercises V4. Single instance
        shared across the gateway lifecycle so the cache's backfill cursor
        and in-memory index survive between requests.

        Construction routes through
        ``GATEWAY_REGISTRY.capability_providers(GatewayPoolKeyCacheCapability)``
        and the chosen provider's ``build_cache(network=...)`` returns a
        ready-to-query :class:`PoolKeyCacheProtocol` instance (including any
        canonical-pair seeding the connector requires). The gateway holds
        the cache behind the Protocol — no concrete connector class is
        named here.

        Today exactly one provider is expected (Uniswap V4). The
        single-provider invariant is enforced loudly: zero providers is a
        misconfigured deployment; more than one is an ambiguity the
        gateway refuses to silently resolve (winner-takes-all would mask
        the bug).

        VIB-4426 — construction is guarded by ``_pool_key_cache_lock`` so
        two concurrent first-callers cannot each instantiate (which would
        discard the loser's in-flight backfill state). The seed step runs
        INSIDE the lock and BEFORE publishing the cache to
        ``self._pool_key_cache`` so a concurrent reader cannot observe a
        partially-seeded cache.
        """
        if self._pool_key_cache is not None:
            return self._pool_key_cache
        async with self._pool_key_cache_lock:
            # Double-checked: another coroutine may have constructed while
            # we were waiting on the lock.
            if self._pool_key_cache is None:
                # mypy: ``@runtime_checkable`` Protocol is the registry
                # contract; see ``pool_history_service._derive_pool_history_tables``.
                providers = list(
                    GATEWAY_REGISTRY.capability_providers(GatewayPoolKeyCacheCapability)  # type: ignore[type-abstract]
                )
                if not providers:
                    raise RuntimeError(
                        "No GatewayPoolKeyCacheCapability provider registered; cannot serve LookupV4PoolKey"
                    )
                if len(providers) > 1:
                    # Two providers would silently winner-takes-all. If a
                    # second pool-keyed protocol ever lands we want a
                    # boot-time error so the dispatcher can be designed
                    # explicitly (e.g. chain-keyed) rather than implicit.
                    names = sorted(type(p).__name__ for p in providers)
                    raise RuntimeError(
                        f"Ambiguous GatewayPoolKeyCacheCapability: {len(providers)} providers registered ({names})"
                    )
                self._pool_key_cache = providers[0].build_cache(network=self.settings.network)
            return self._pool_key_cache

    async def LookupV4PoolKey(
        self,
        request: gateway_pb2.LookupV4PoolKeyRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.LookupV4PoolKeyResponse:
        """Resolve a Uniswap V4 ``pool_id`` to its canonical ``PoolKey``.

        Validation:
        - ``pool_id`` must be exactly 32 bytes (INVALID_ARGUMENT otherwise).
        - ``chain`` must be one of the validator-accepted chains
          (INVALID_ARGUMENT otherwise).

        Cache miss -> bounded backfill against the chain's PoolManager
        Initialize logs -> re-check. If still unknown, returns NOT_FOUND
        with an empty body. Callers MUST distinguish NOT_FOUND from a
        zero-valued PoolKey (Empty != Zero, per AGENTS.md §Accounting).
        """
        # Validate pool_id shape up front. Empty bytes / wrong length is a
        # caller contract violation, not a "not found".
        if not request.pool_id or len(request.pool_id) != 32:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"pool_id must be 32 bytes, got {len(request.pool_id)}")
            return gateway_pb2.LookupV4PoolKeyResponse()

        # Chain is required (no implicit fallback). V4 PoolManager is
        # deployed per chain with different addresses; "I don't know which
        # chain" is unrecoverable.
        if not request.chain:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("chain is required for LookupV4PoolKey")
            return gateway_pb2.LookupV4PoolKeyResponse()

        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.LookupV4PoolKeyResponse()

        # Cache acquisition is INSIDE the try/except so the
        # zero-provider / multi-provider ``RuntimeError`` from
        # ``_get_pool_key_cache`` flows through the same sanitised
        # ``INTERNAL`` mapping as any other backend error — a stack
        # trace from a misconfigured deployment must not cross the
        # gRPC trust boundary.
        try:
            cache = await self._get_pool_key_cache()
            cached = await cache.lookup(chain, request.pool_id)
        except PoolKeyCacheError as exc:
            # VIB-4426 P1 #2 — distinguish typed cache-refresh failures
            # (config / upstream-RPC) from genuinely unknown pools. Pre-fix,
            # both surfaced as ``NOT_FOUND``, which made operator
            # observability lie ("pool not found" when actually the gateway
            # had no RPC URL or could not call ``eth_blockNumber``).
            logger.warning(
                "LookupV4PoolKey: refresh failed for chain=%s pool_id=0x%s code=%s: %s",
                chain,
                request.pool_id.hex(),
                exc.code,
                exc,
            )
            if exc.code == "failed_precondition":
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"V4 pool key resolution unavailable on chain {chain}: "
                    "gateway is not configured to query this chain"
                )
            else:  # "unavailable"
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(
                    f"V4 pool key resolution temporarily unavailable on chain {chain}: "
                    "upstream RPC failed; see gateway logs"
                )
            return gateway_pb2.LookupV4PoolKeyResponse()
        except Exception:  # noqa: BLE001
            # VIB-4426 — log full diagnostic context server-side; return a
            # sanitised generic message to the gRPC client. ``str(exc)`` on
            # an unexpected backend error can leak SDK paths, RPC URLs, or
            # provider-specific status strings across the trust boundary
            # (CodeRabbit Major on PR #2335). ``logger.exception`` attaches
            # the full traceback to the gateway log.
            logger.exception(
                "LookupV4PoolKey: unexpected error for chain=%s pool_id=0x%s",
                chain,
                request.pool_id.hex(),
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("internal error resolving V4 pool key; see gateway logs")
            return gateway_pb2.LookupV4PoolKeyResponse()

        if cached is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"V4 pool_id 0x{request.pool_id.hex()} not found on chain {chain}")
            return gateway_pb2.LookupV4PoolKeyResponse()

        # currency0 < currency1 invariant is enforced inside CachedPoolKey;
        # an out-of-order pair would have failed at decode time and never
        # reached the cache. No defence-in-depth check needed here.
        return gateway_pb2.LookupV4PoolKeyResponse(
            pool_key=gateway_pb2.PoolKey(
                currency0=cached.currency0,
                currency1=cached.currency1,
                fee=cached.fee,
                tick_spacing=cached.tick_spacing,
                hooks=cached.hooks,
            ),
            chain=chain,
        )
