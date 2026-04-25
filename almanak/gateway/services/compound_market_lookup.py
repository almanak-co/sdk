"""Compound v3 (Comet) cToken fetcher for receipt-token metadata discovery.

Fetches the Compound v3 market list from the community-maintained Woof
Software aggregator (``raw.githubusercontent.com/woof-software/
compound-docs-aggregator/main/output.json``), which Compound's official
docs reference as the canonical cross-network market snapshot.  One
HTTP call returns every Comet market's cToken symbol, contract
address, decimals, and underlying metadata across all deployed chains.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Covers every deployed Comet market (~28 across 10 chains today)
    - cTokens follow a consistent ``c<BASE>v3`` naming scheme
      (``cUSDCv3``, ``cWETHv3``, ``cWstETHv3``, ...)
    - Disk cache at ``~/.almanak/compound_market_cache.json`` with 24h TTL
    - Single HTTP GET at startup — no pagination, no per-chain iteration

Inherits plumbing (disk cache, load orchestration, backoff) from
``ProtocolTokenLookup``. Compound-recommended aggregator is treated
as an authoritative source — same trust model as Jupiter / Pendle /
Aave / Morpho. A compromised or stale aggregator degrades coverage
(new markets stop resolving) but can't invent addresses: Compound's
deployed contracts are what they are.

Usage:
    from almanak.gateway.services.compound_market_lookup import get_compound_lookup

    lookup = await get_compound_lookup()
    token = lookup.lookup_by_symbol("cUSDCv3", "ethereum")
    if token:
        print(f"{token.symbol} at {token.address}, underlying={token.underlying_symbol}")
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from almanak.gateway.services._protocol_lookup import ProtocolTokenLookup

logger = logging.getLogger(__name__)

# Woof Software's Compound v3 docs aggregator — a single JSON that pulls
# together every Comet market config across every deployed network.
# Compound's own documentation references this repo as the canonical
# cross-network snapshot (no official Compound aggregator exists).
COMPOUND_AGGREGATOR_URL = "https://raw.githubusercontent.com/woof-software/compound-docs-aggregator/main/output.json"

# Aggregator network names → gateway chain names.  ``mainnet`` is the
# only one that differs; everything else matches already.  Networks
# the gateway doesn't speak (ronin, mantle, scroll, unichain) are
# skipped at index-build time.
_NETWORK_TO_CHAIN: dict[str, str] = {
    "mainnet": "ethereum",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "polygon": "polygon",
    "linea": "linea",
}

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "compound_market_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "CompoundMarketLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class CompoundMarketToken:
    """Compound v3 cToken metadata (supply-side receipt token)."""

    address: str  # lowercased
    symbol: str
    name: str
    decimals: int
    chain: str
    underlying_symbol: str  # e.g., 'USDC' for 'cUSDCv3'
    underlying_address: str  # lowercased


class CompoundMarketLookup(ProtocolTokenLookup):
    """Compound v3 (Comet) market lookup with per-chain indexing.

    Fetches the full cross-network market snapshot from Woof Software's
    aggregator in a single GET.  Every Comet market's cToken is
    registered under its ``c<BASE>v3`` symbol.  Since the aggregator
    caches the data itself, one 24h-cached HTTP call covers every
    deployed market.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Compound v3 markets",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, CompoundMarketToken]] = {}
        self._address_indices: dict[str, dict[str, CompoundMarketToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} markets indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # The aggregator wraps the market set in a ``markets`` dict keyed
        # by network name.  Reject anything else so we fall through to a
        # retry rather than building empty indices.
        if not isinstance(data, dict):
            return False
        markets = data.get("markets")
        return isinstance(markets, dict) and bool(markets)

    async def _fetch_from_network(self) -> dict[str, Any] | None:
        """Fetch the Compound v3 aggregator JSON via a single GET."""
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            logger.info("Fetching Compound v3 markets from %s", COMPOUND_AGGREGATOR_URL)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.get(
                    COMPOUND_AGGREGATOR_URL,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.warning("Compound markets fetch returned HTTP %d", resp.status)
                        return None
                    body = await resp.json(content_type=None)

            if not isinstance(body, dict) or not isinstance(body.get("markets"), dict):
                logger.warning(
                    "Compound markets response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            self._write_disk_cache(body)
            return body

        except Exception as exc:
            logger.warning("Compound markets fetch failed: %s", exc)
            return None

    def _build_indices(self, data: dict[str, Any]) -> None:
        """Build per-chain indices from the aggregator's ``markets`` dict.

        Aggregator shape: ``{"markets": {<network>: {<cSymbol>: <config>}}}``.
        Each ``<config>`` has ``contracts.comet`` (cToken address) and
        ``baseToken.{symbol, address, decimals}``. Networks outside the
        gateway's known chain set are silently dropped.
        """
        skipped_networks: set[str] = set()
        markets_by_network = data.get("markets", {})
        if not isinstance(markets_by_network, dict):
            return

        for network, markets in markets_by_network.items():
            chain = _NETWORK_TO_CHAIN.get(network)
            if chain is None:
                skipped_networks.add(network)
                continue
            if not isinstance(markets, dict):
                continue

            symbol_idx = self._symbol_indices.setdefault(chain, {})
            address_idx = self._address_indices.setdefault(chain, {})

            for csymbol, config in markets.items():
                if not isinstance(config, dict):
                    continue
                try:
                    contracts = config.get("contracts") or {}
                    address = str(contracts.get("comet", "")).strip().lower()

                    base = config.get("baseToken") or {}
                    underlying_symbol = str(base.get("symbol", "")).strip()
                    underlying_address = str(base.get("address", "")).strip().lower()
                    decimals_raw = base.get("decimals", 18)
                    decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                    symbol = str(csymbol).strip()
                    if not address or not symbol:
                        continue

                    # Compound doesn't expose a separate cToken name field
                    # in the aggregator; build one that matches the
                    # ``configuration.json`` convention (``Compound <BASE>``).
                    name = f"Compound {underlying_symbol}" if underlying_symbol else symbol

                    meta = CompoundMarketToken(
                        address=address,
                        symbol=symbol,
                        name=name,
                        decimals=decimals,
                        chain=chain,
                        underlying_symbol=underlying_symbol,
                        underlying_address=underlying_address,
                    )

                    symbol_upper = symbol.upper()
                    if symbol_upper not in symbol_idx:
                        symbol_idx[symbol_upper] = meta
                    if address not in address_idx:
                        address_idx[address] = meta

                except Exception as exc:
                    logger.debug(
                        "Skipping malformed Compound market %s/%s: %s",
                        network,
                        csymbol,
                        exc,
                    )
                    continue

        if skipped_networks:
            logger.debug(
                "Compound markets skipped on unmapped networks: %s",
                sorted(skipped_networks),
            )
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug("Compound %s: %d markets indexed", chain, len(symbol_idx))

    def lookup_by_symbol(self, symbol: str, chain: str) -> CompoundMarketToken | None:
        """Look up a Compound v3 cToken by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> CompoundMarketToken | None:
        """Look up a Compound v3 cToken by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_compound_lookup() -> CompoundMarketLookup:
    """Get (or create) the singleton CompoundMarketLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = CompoundMarketLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
