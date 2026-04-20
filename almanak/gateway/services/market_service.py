"""MarketService implementation - provides market data to strategies.

This service provides price, balance, and indicator data to strategy containers
via gRPC. All external API calls (CoinGecko, Web3 RPC) are made here in the
gateway; strategy containers only see the results.
"""

import asyncio
import logging
import re
import time
from typing import Any

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
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

# Chain-scoped native gas tokens. A symbol is treated as the chain's native
# coin (and routed through `provider.get_native_balance()`) ONLY if it appears
# in this chain's set. This prevents `GetBalance(token="POL", chain="ethereum")`
# from returning ETH balance, etc. Both MATIC and POL are accepted on Polygon
# because POL is the Sep-2024 1:1 rename of MATIC and many wallets still use
# the old symbol.
NATIVE_SYMBOLS_BY_CHAIN: dict[str, frozenset[str]] = {
    "ethereum": frozenset({"ETH"}),
    "arbitrum": frozenset({"ETH"}),
    "optimism": frozenset({"ETH"}),
    "base": frozenset({"ETH"}),
    "linea": frozenset({"ETH"}),
    "blast": frozenset({"ETH"}),
    "scroll": frozenset({"ETH"}),
    "zksync": frozenset({"ETH"}),
    "polygon": frozenset({"MATIC", "POL"}),
    "avalanche": frozenset({"AVAX"}),
    "bsc": frozenset({"BNB"}),
    "sonic": frozenset({"S"}),
    "fantom": frozenset({"FTM"}),
    "mantle": frozenset({"MNT"}),
    "berachain": frozenset({"BERA"}),
    "monad": frozenset({"MON"}),
    "plasma": frozenset({"XPL"}),
    "x-layer": frozenset({"OKB"}),
    "solana": frozenset({"SOL"}),
}


def _is_native_symbol(token: str, chain: str) -> bool:
    """Return True iff `token` is the native gas symbol for `chain`.

    Fails CLOSED for chains not in NATIVE_SYMBOLS_BY_CHAIN: an unmapped
    chain returns False for every symbol so the request falls through to
    `provider.get_balance(token)` (the safe ERC-20 path) instead of
    silently routing to `get_native_balance()` and returning the wrong
    asset. New chains MUST be added to the map in the same change that
    adds chain support — see VIB-3137 follow-up.
    """
    natives = NATIVE_SYMBOLS_BY_CHAIN.get(chain.lower())
    if natives is None:
        return False
    return token.upper() in natives


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

        self._price_aggregator = PriceAggregator(sources=sources)
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

                self._balance_providers[cache_key] = Web3BalanceProvider(
                    rpc_url=rpc_url,
                    wallet_address=wallet_address,
                    chain=chain,
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
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.PriceResponse()

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

            # Chain-scoped native check: only route to get_native_balance when
            # the symbol is actually native to THIS chain. Prevents POL on
            # Ethereum from returning ETH balance, etc.
            if _is_native_symbol(token, chain):
                result = await provider.get_native_balance()
            else:
                result = await provider.get_balance(token)

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
            )
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

                # Chain-scoped native check: only route to get_native_balance when
                # the symbol is actually native to THIS chain. Prevents POL on
                # Ethereum from returning ETH balance, etc.
                if _is_native_symbol(token, chain):
                    result = await provider.get_native_balance()
                else:
                    result = await provider.get_balance(token)

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
