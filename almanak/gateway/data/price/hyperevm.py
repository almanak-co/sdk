"""HyperCore oracle price source for HyperEVM (chain 999, Hyperliquid).

On HyperEVM there is no Chainlink and (for perp majors like ``ETH`` / ``BTC``)
no ERC-20 to price by contract address. The canonical, venue-native price for a
perp is the HyperCore **oracle** price, read synchronously from the ``0x0807``
read precompile — the same mark the connector's compiler anchors its slippage
band against (``compiler.py:_read_oracle_price``). This source exposes that read
through the gateway's :class:`BasePriceSource` protocol so ``MarketSnapshot.price("ETH")``
resolves on chain 999 without an off-chain API round-trip.

The venue-specific bits (which precompile to read, how to encode the query, how
to decode + scale the return, symbol→asset resolution) are NOT imported from the
connector — the gateway↔connector isolation ratchet (VIB-4121) forbids a gateway
module importing ``almanak.connectors.hyperliquid.*`` directly. Instead the perp
connector publishes them through ``GatewayOraclePriceCapability`` (resolved from
``GATEWAY_REGISTRY.capability_providers`` keyed on ``oracle_price_chain()``); this
source owns only the RPC plumbing + Empty≠Zero miss semantics.

Scope (deliberately narrow — this is HyperCore, not a generic EVM feed):

* **Perp symbol** resolvable via the connector capability
  (``"ETH"``, ``"BTC"``, ``"HYPE"``, …) → eth_call ``0x0807`` with the
  capability-encoded calldata, decoded with the exact precompile scale
  ``raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals)`` inside the capability.
* **Stablecoin** (USDC / USDT0 on HyperEVM) → the $1.00 peg, no RPC (matches the
  other sources' stablecoin fast-path). This is the ONE deliberate constant;
  Empty≠Zero holds everywhere else.
* **Anything else** → :class:`DataSourceUnavailable` (a "miss"), so the
  aggregator falls through to DexScreener / CoinGecko for spot tokens.

Egress is correct HERE: this is the gateway layer (blueprint 20). The RPC URL is
resolved via :func:`get_rpc_url` against the ChainRegistry's ``hyperevm``
descriptor (public RPC ``https://rpc.hyperliquid.xyz/evm``), exactly as
:class:`OnChainPriceSource` does — no hardcoded URL. The eth_call is bounded by a
tight per-request timeout so a slow/hanging precompile read can never bleed the
``decide()`` budget; an empty / zero / failed read is a MISS (Empty≠Zero), never
a fabricated ``0``.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from almanak.framework.data.tokens.models import ResolvedToken

from almanak.connectors._base.gateway_capabilities import (
    GatewayOraclePriceCapability,
    OraclePriceQuery,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

# HyperEVM chain name (ChainRegistry descriptor / gateway chain string).
_HYPEREVM_CHAIN = "hyperevm"

# HyperEVM stablecoins that peg to $1.00 (verified on-chain, 6 decimals). We
# return the peg directly rather than round-tripping an external API — matching
# the stablecoin fast-path in the other price sources. This is the ONLY
# fabricated constant here; every other unresolved input is a MISS, not a zero.
#
# Scoped to the stablecoins ACTUALLY registered on hyperevm in the static token
# registry (tokens.json: USDC + USDT0, both is_stablecoin=True). We deliberately
# do NOT peg symbols with no hyperevm registry entry (e.g. USDT, USDC.E): pegging
# a symbol the rest of the stack cannot resolve is a soft Empty≠Zero violation —
# the source would assert a price for something the resolver treats as
# unresolvable. Keeping this list to registered symbols means the peg set and the
# registry cannot drift.
_STABLECOIN_SYMBOLS = frozenset({"USDC", "USDT0"})
_STABLECOIN_PEG_PRICE = Decimal("1.00")


class HypercoreOraclePriceSource(BasePriceSource):
    """Price source that reads HyperCore oracle prices from the ``0x0807`` precompile.

    Implements the same :class:`BasePriceSource` contract as
    :class:`OnChainPriceSource` / :class:`PythPriceSource`:
    ``async get_price(token, quote="USD", *, resolved_token=None) -> PriceResult``,
    raising :class:`DataSourceUnavailable` on a miss.

    Args:
        network: Network environment for RPC URL resolution ("mainnet" or "anvil").
        cache_ttl: In-memory cache TTL in seconds (default 10).
        request_timeout: Per-eth_call timeout in seconds (default 5) — tight so a
            slow precompile read can never bleed the decide() budget.
    """

    def __init__(
        self,
        network: str = "mainnet",
        cache_ttl: float = 10.0,
        request_timeout: float = 5.0,
    ) -> None:
        self._network = network
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout

        # Venue-native oracle-price provider, resolved once from the gateway
        # registry (NOT a direct connector import — gateway↔connector isolation,
        # VIB-4121). The perp connector (Hyperliquid) publishes the precompile
        # address, query encoding, and decode/scale via
        # ``GatewayOraclePriceCapability`` keyed on ``oracle_price_chain()``; this
        # source owns only the RPC plumbing + Empty≠Zero miss semantics. Resolved
        # at construction so dispatch stays O(1) with no per-request registry walk.
        self._oracle_provider: GatewayOraclePriceCapability | None = None
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayOraclePriceCapability):  # type: ignore[type-abstract]
            if provider.oracle_price_chain() == _HYPEREVM_CHAIN:
                self._oracle_provider = provider
                break

        # In-memory cache: key -> (PriceResult, timestamp_seconds)
        self._cache: dict[str, tuple[PriceResult, float]] = {}

        # Lazy-initialized aiohttp session.
        self._session: aiohttp.ClientSession | None = None

        # Monotonic JSON-RPC request id for correlation.
        self._rpc_request_id = 0

        # Resolve the HyperEVM RPC URL once (via ChainRegistry, not a literal).
        self._rpc_url: str | None = None
        try:
            self._rpc_url = get_rpc_url(_HYPEREVM_CHAIN, network=self._network)
        except ValueError:
            self._rpc_url = None
            logger.warning(
                "HypercoreOraclePriceSource: no RPC URL for chain=%s network=%s -- "
                "HyperCore oracle pricing will be unavailable",
                _HYPEREVM_CHAIN,
                self._network,
            )

    @property
    def source_name(self) -> str:
        return "hypercore_oracle"

    @property
    def cache_ttl_seconds(self) -> int:
        return int(self._cache_ttl)

    async def get_price(
        self, token: str, quote: str = "USD", *, resolved_token: ResolvedToken | None = None
    ) -> PriceResult:
        """Fetch the HyperCore oracle price for a perp symbol (or the stablecoin peg).

        Resolution order:
        1. In-memory cache.
        2. Stablecoin (USDC / USDT0) -> $1.00 peg, no RPC.
        3. Perp symbol resolvable via ``resolve_market`` -> eth_call ``0x0807``.
        4. Otherwise -> :class:`DataSourceUnavailable` (miss; aggregator falls through).

        Args:
            token: Token / market symbol (e.g. "ETH", "BTC", "HYPE", "USDC").
            quote: Quote currency (only "USD" supported).

        Returns:
            PriceResult with the oracle price (or $1.00 peg for stables).

        Raises:
            DataSourceUnavailable: On an unknown symbol, missing RPC, or an
                empty / zero / failed precompile read (Empty≠Zero — never a 0).
        """
        if quote.upper() != "USD":
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Only USD quote supported, got {quote}",
            )

        token_upper = token.upper()

        # 1. Cache.
        cache_key = f"{token_upper}/USD"
        cached = self._cache.get(cache_key)
        if cached is not None:
            result, cached_at = cached
            # Monotonic clock for TTL: unaffected by wall-clock/NTP adjustments.
            if time.monotonic() - cached_at < self._cache_ttl:
                return result

        # 2. Stablecoin peg (no RPC). Deliberate constant — see module docstring.
        if token_upper in _STABLECOIN_SYMBOLS:
            result = PriceResult(
                price=_STABLECOIN_PEG_PRICE,
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=0.99,
                stale=False,
            )
            self._cache[cache_key] = (result, time.monotonic())
            return result

        # 3. Perp symbol -> HyperCore oracle precompile (via the connector's
        # GatewayOraclePriceCapability — no direct connector import here).
        if self._oracle_provider is None:
            # No registered oracle-price provider for HyperEVM -> miss.
            raise DataSourceUnavailable(
                source=self.source_name,
                reason="No HyperCore oracle-price provider registered for chain=hyperevm",
            )

        query = self._oracle_provider.resolve_oracle_query(token_upper)
        if query is None:
            # Not a resolvable perp symbol and not a stablecoin -> miss, so the
            # aggregator falls through to DexScreener / CoinGecko for spot tokens.
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"{token_upper} is not a HyperCore perp market or known HyperEVM stablecoin",
            )

        if not self._rpc_url:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No RPC URL available for chain={_HYPEREVM_CHAIN}",
            )

        price = await self._read_oracle_price(query)
        result = PriceResult(
            price=price,
            source=self.source_name,
            timestamp=datetime.now(UTC),
            confidence=0.95,
            stale=False,
        )
        self._cache[cache_key] = (result, time.monotonic())
        return result

    async def _read_oracle_price(self, query: OraclePriceQuery) -> Decimal:
        """Read + decode the HyperCore oracle price for a resolved perp query.

        The gateway owns the RPC read (bounded eth_call) and the Empty≠Zero miss
        semantics; the connector's capability owns the decode + fixed-point scale
        (``raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals)`` — verified live: BTC
        szDecimals 5, raw 598970 -> 59897). A None decode (empty / undecodable /
        non-positive read) -> :class:`DataSourceUnavailable`, never a fabricated 0.
        """
        assert self._oracle_provider is not None  # guarded by caller
        try:
            raw = await self._eth_call(query.to_address, query.calldata)
        except Exception as exc:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"HyperCore oracle read failed for {query.symbol}: {exc}",
            ) from exc

        # Decode via the connector capability: a malformed payload must be a MISS
        # (the capability returns None, never raises), not an unhandled error that
        # crashes the aggregator.
        price = self._oracle_provider.decode_oracle_price(query, raw)
        if price is None or price <= 0:
            # Empty / undecodable / non-positive: an unavailable price is NOT a
            # measured zero (Empty≠Zero).
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"HyperCore oracle returned no usable price for {query.symbol}",
            )

        logger.debug("HyperCore oracle %s: price=%s", query.symbol, price)
        return price

    async def _eth_call(self, to: str, data: str) -> str:
        """Make a bounded async eth_call via JSON-RPC to the HyperEVM RPC.

        Args:
            to: Precompile address.
            data: Hex-encoded calldata (raw ABI args, no selector — a precompile
                is not a Solidity function).

        Returns:
            Hex-encoded response data.

        Raises:
            RuntimeError: On RPC HTTP error, JSON-RPC error, or empty result.
        """
        if not self._rpc_url:
            raise RuntimeError("No RPC URL configured")

        session = await self._get_session()
        self._rpc_request_id += 1
        request_id = self._rpc_request_id
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
            "id": request_id,
        }

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with session.post(self._rpc_url, json=payload, timeout=timeout) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"RPC HTTP {resp.status}: {text[:200]}")
            body = await resp.json()

        if "error" in body:
            raise RuntimeError(f"RPC eth_call error (id={request_id}, to={to}): {body['error']}")

        result = body.get("result", "0x")
        if result in ("0x", "0x0", ""):
            raise RuntimeError(f"Empty eth_call result (id={request_id}, to={to})")
        return result

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


__all__ = ["HypercoreOraclePriceSource"]
