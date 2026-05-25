"""Aave v3 markets fetcher for aToken / vToken metadata discovery across EVM chains.

Fetches every Aave v3 reserve in a single GraphQL call via
``https://api.v3.aave.com/graphql`` (which returns all reserves on every
supported chain in one response) and provides fast per-chain symbol and
address lookups for ``aToken`` (supply receipt) and ``vToken``
(variable-debt receipt) tokens. Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Two token types per reserve: aToken (``aEthUSDC`` style) and
      vToken (``variableDebtEthUSDC`` style)
    - Covers multiple Aave deployments per chain — Core, Prime,
      EtherFi, Lido, Horizon RWA — which show up as separate markets
      but share the naming scheme (``aEthLidoUSDC`` vs ``aEthUSDC``).
    - Disk cache at ``~/.almanak/aave_market_cache.json`` with 24h TTL
    - Single HTTP POST at startup — no pagination, no per-chain iteration
    - Graceful degradation: network/parse failure returns None, never raises.

Inherits plumbing (disk cache, load orchestration, backoff, singleton lock)
from ``ProtocolTokenLookup``. Aave's API is the protocol's own
authoritative source for receipt-token addresses, so on-chain
``symbol()`` confirm is skipped — matching the trust model already used
for Jupiter on Solana and Pendle on EVM.

Usage:
    from almanak.connectors.aave_v3.gateway.market_lookup import get_aave_lookup

    lookup = await get_aave_lookup()
    token = lookup.lookup_by_symbol("aEthUSDC", "ethereum")
    if token:
        print(f"{token.symbol} at {token.address} has {token.decimals} decimals")
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from almanak.gateway.services._protocol_lookup import ProtocolTokenLookup

logger = logging.getLogger(__name__)

# Aave v3 public GraphQL endpoint (free, no auth).  A single ``markets`` query
# with every chainId returns the full reserve list across chains — no
# pagination, no per-chain iteration.
AAVE_GRAPHQL_URL = "https://api.v3.aave.com/graphql"

# EVM chains supported by Aave v3 that we map to gateway chain names.
# Aave also deploys on a few testnets (Base Sepolia, etc.) that we skip.
AAVE_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "bsc": 56,
    "polygon": 137,
    "avalanche": 43114,
    "linea": 59144,
    # Note: Aave also deploys on Gnosis (chain id 100), but the gateway's
    # ``ALLOWED_CHAINS`` and the ``Chain`` enum don't include Gnosis today,
    # so any Gnosis request would be rejected upstream before it reaches
    # this lookup. Adding ``"gnosis": 100`` here would be dead code.
    # Re-add once Gnosis lands in both ``almanak.core.enums.Chain`` and
    # ``almanak.gateway.validation.ALLOWED_CHAINS``.
}
_CHAIN_NAME_BY_ID: dict[int, str] = {v: k for k, v in AAVE_CHAIN_IDS.items()}

# GraphQL query — keep the shape minimal.  We only need symbol/address/decimals
# for aToken + vToken + underlyingToken.  Including underlyingToken lets the
# lookup also answer "what's the underlying for aEthUSDC?" without a second
# call, though we don't index it by symbol (it collides with the plain USDC
# entries that live in the static registry already).
_AAVE_MARKETS_QUERY = """\
query AllAaveMarkets($chainIds: [Int!]!) {
  markets(request: { chainIds: $chainIds }) {
    chain { chainId }
    reserves {
      underlyingToken { symbol address decimals }
      aToken { symbol name address decimals }
      vToken { symbol name address decimals }
    }
  }
}
"""

# Disk cache path and TTL — mirrors Jupiter / Pendle conventions.
CACHE_PATH = Path.home() / ".almanak" / "aave_market_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "AaveMarketLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class AaveReserveToken:
    """Aave v3 receipt-token metadata (aToken or vToken)."""

    address: str  # lowercased
    symbol: str
    name: str
    decimals: int
    token_type: str  # 'A' (supply receipt) or 'V' (variable debt)
    chain: str  # 'ethereum', 'arbitrum', ...
    underlying_symbol: str  # e.g., 'USDC' for 'aEthUSDC'
    underlying_address: str  # lowercased


class AaveMarketLookup(ProtocolTokenLookup):
    """Aave v3 markets lookup with per-chain indexing and disk caching.

    Fetches all Aave v3 reserves across supported chains in one GraphQL
    call and registers the aToken and vToken for each reserve.  The full
    set is fetched once per 24 hours and cached on disk.

    Thread-safety: safe to use from multiple coroutines after
    initialisation because indexing is read-only after ``_load()`` completes.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Aave markets",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, AaveReserveToken]] = {}
        self._address_indices: dict[str, dict[str, AaveReserveToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} tokens indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # /markets query returns a flat list of market objects; reject
        # any cached/fetched payload that isn't a *non-empty* list so we
        # fall through to a retry instead of building empty indices.
        # An empty list would otherwise silently pin the lookup in a
        # zero-index state until the 24h disk TTL expires.
        return isinstance(data, list) and bool(data)

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch all Aave v3 markets across supported chains in one GraphQL call."""
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            payload = {
                "query": _AAVE_MARKETS_QUERY,
                "variables": {"chainIds": list(AAVE_CHAIN_IDS.values())},
            }
            logger.info("Fetching Aave markets from %s", AAVE_GRAPHQL_URL)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.post(
                    AAVE_GRAPHQL_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.warning("Aave markets fetch returned HTTP %d", resp.status)
                        return None
                    body = await resp.json(content_type=None)

            if not isinstance(body, dict):
                logger.warning(
                    "Aave markets response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            if body.get("errors"):
                logger.warning("Aave GraphQL returned errors: %s", body["errors"])
                return None

            markets = body.get("data", {}).get("markets", [])
            if not isinstance(markets, list) or not markets:
                return None

            self._write_disk_cache(markets)
            return markets

        except Exception as exc:
            logger.warning("Aave markets fetch failed: %s", exc)
            return None

    def _build_indices(self, markets: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from raw markets data.

        Each market entry is ``{chain: {chainId: N}, reserves: [...]}``.
        Aave returns several markets per chain on Ethereum (Core, EtherFi,
        Lido, Horizon RWA); we merge their reserves into the same chain
        index since the symbols are globally unique (``aEthUSDC`` vs
        ``aEthLidoUSDC``).  Unsupported chains (testnets, chains the
        gateway doesn't speak) are silently dropped.
        """
        skipped_chains: set[int] = set()

        for market in markets:
            if not isinstance(market, dict):
                continue
            chain_obj = market.get("chain") or {}
            chain_id_raw = chain_obj.get("chainId")
            chain_id = int(chain_id_raw) if isinstance(chain_id_raw, int | float) else -1
            chain = _CHAIN_NAME_BY_ID.get(chain_id)
            if chain is None:
                skipped_chains.add(chain_id)
                continue

            symbol_idx = self._symbol_indices.setdefault(chain, {})
            address_idx = self._address_indices.setdefault(chain, {})

            for reserve in market.get("reserves", []):
                if not isinstance(reserve, dict):
                    continue

                underlying = reserve.get("underlyingToken") or {}
                underlying_symbol = str(underlying.get("symbol", "")).strip()
                underlying_address = str(underlying.get("address", "")).strip().lower()

                for token_key, token_type in (("aToken", "A"), ("vToken", "V")):
                    t = reserve.get(token_key)
                    if not isinstance(t, dict):
                        continue
                    try:
                        address = str(t.get("address", "")).strip().lower()
                        symbol = str(t.get("symbol", "")).strip()
                        name = str(t.get("name", "")).strip() or symbol
                        decimals_raw = t.get("decimals", 18)
                        decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                        if not address or not symbol:
                            continue

                        meta = AaveReserveToken(
                            address=address,
                            symbol=symbol,
                            name=name,
                            decimals=decimals,
                            token_type=token_type,
                            chain=chain,
                            underlying_symbol=underlying_symbol,
                            underlying_address=underlying_address,
                        )

                        # Symbol index (case-insensitive); first entry wins.
                        symbol_upper = symbol.upper()
                        if symbol_upper not in symbol_idx:
                            symbol_idx[symbol_upper] = meta

                        # Address index: 1:1 — addresses are unique on-chain.
                        if address not in address_idx:
                            address_idx[address] = meta

                    except Exception as exc:
                        logger.debug(
                            "Skipping malformed Aave %s entry in chain=%s: %s",
                            token_key,
                            chain,
                            exc,
                        )
                        continue

        if skipped_chains:
            logger.debug("Aave reserves skipped on unmapped chains: %s", sorted(skipped_chains))
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug(
                "Aave %s: %d unique symbols, %d unique addresses",
                chain,
                len(symbol_idx),
                len(self._address_indices.get(chain, {})),
            )

    def lookup_by_symbol(self, symbol: str, chain: str) -> AaveReserveToken | None:
        """Look up an Aave aToken / vToken by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> AaveReserveToken | None:
        """Look up an Aave aToken / vToken by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_aave_lookup() -> AaveMarketLookup:
    """Get (or create) the singleton AaveMarketLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = AaveMarketLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
