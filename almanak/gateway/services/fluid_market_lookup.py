"""Fluid (Instadapp) fToken fetcher for lending receipt-token discovery.

Fetches every Fluid lending fToken across supported chains via
``api.fluid.instadapp.io/v2/lending/{chainId}/tokens`` and provides
per-chain symbol and address lookups.  Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Fluid markets are small per chain (~6) but cover major assets:
      ``fUSDC``, ``fUSDT``, ``fGHO``, ``fWETH``, ``fwstETH``, ...
    - Parallel fetch across chains (``asyncio.gather``) — no single
      cross-chain endpoint exists, so one small GET per chain
    - Disk cache at ``~/.almanak/fluid_market_cache.json`` with 24h TTL
    - Graceful degradation: a chain's failure does not block the others

Inherits plumbing (disk cache, load orchestration, backoff) from
``ProtocolTokenLookup``. Fluid's API is the protocol's own
authoritative source, so on-chain ``symbol()`` confirm is skipped —
matching the trust model used for every other protocol tier.

Usage:
    from almanak.gateway.services.fluid_market_lookup import get_fluid_lookup

    lookup = await get_fluid_lookup()
    token = lookup.lookup_by_symbol("fUSDC", "ethereum")
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

# Per-chain endpoint pattern; Fluid has no cross-chain aggregate.
_FLUID_CHAIN_URL_TEMPLATE = "https://api.fluid.instadapp.io/v2/lending/{chain_id}/tokens"

# chainID → gateway chain name.  Fluid deploys on ethereum, arbitrum,
# base, polygon today.  Anything unmapped is silently dropped.
FLUID_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "base": 8453,
    "polygon": 137,
}

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "fluid_market_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "FluidMarketLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class FluidMarketToken:
    """Fluid lending receipt-token metadata."""

    address: str  # fToken address, lowercased
    symbol: str  # e.g. ``fUSDC``
    name: str  # e.g. ``Fluid USD Coin``
    decimals: int
    chain: str
    underlying_symbol: str  # e.g. ``USDC``
    underlying_address: str  # lowercased


class FluidMarketLookup(ProtocolTokenLookup):
    """Fluid lending market lookup with per-chain indexing.

    Fetches Fluid's per-chain lending token list in parallel and
    registers each fToken share symbol → address mapping.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Fluid markets",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, FluidMarketToken]] = {}
        self._address_indices: dict[str, dict[str, FluidMarketToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} fTokens indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # Payload is a dict keyed by chain name, each value a list of
        # token dicts. Reject anything else so we fall through to a
        # retry rather than building empty indices.
        if not isinstance(data, dict):
            return False
        return any(isinstance(v, list) and v for v in data.values())

    async def _fetch_from_network(self) -> dict[str, list[dict[str, Any]]] | None:
        """Fetch Fluid markets for every supported chain in parallel.

        Returns a mapping of chain name -> list of raw token dicts.
        Per-chain failures do not abort the whole fetch; the returned
        dict contains only chains that produced data.  Returns None
        if every chain failed.
        """
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            async def fetch_chain(
                session: "aiohttp.ClientSession", chain: str, chain_id: int
            ) -> tuple[str, list[dict[str, Any]]]:
                url = _FLUID_CHAIN_URL_TEMPLATE.format(chain_id=chain_id)
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status != 200:
                            logger.warning(
                                "Fluid fetch for %s returned HTTP %d",
                                chain,
                                resp.status,
                            )
                            return (chain, [])
                        body = await resp.json(content_type=None)
                except Exception as exc:
                    logger.warning("Fluid fetch for %s failed: %s", chain, exc)
                    return (chain, [])

                if not isinstance(body, dict):
                    return (chain, [])
                tokens = body.get("data")
                if not isinstance(tokens, list):
                    return (chain, [])
                return (chain, tokens)

            logger.info(
                "Fetching Fluid markets (%d chains in parallel)",
                len(FLUID_CHAIN_IDS),
            )
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                tasks = [fetch_chain(session, chain, cid) for chain, cid in FLUID_CHAIN_IDS.items()]
                results = await asyncio.gather(*tasks)

            data: dict[str, list[dict[str, Any]]] = {chain: tokens for chain, tokens in results if tokens}
            if not data:
                return None

            self._write_disk_cache(data)
            return data

        except Exception as exc:
            logger.warning("Fluid markets fetch failed: %s", exc)
            return None

    def _build_indices(self, data: dict[str, list[dict[str, Any]]]) -> None:
        """Build per-chain indices from the {chain: [token, ...]} map.

        Each token has ``address``, ``symbol``, ``decimals``, ``name``,
        ``assetAddress``, and a nested ``asset`` dict with underlying
        metadata.  Fluid gives us vault decimals directly, so no
        inheritance guesswork.
        """
        for chain, tokens in data.items():
            if chain not in FLUID_CHAIN_IDS:
                continue
            if not isinstance(tokens, list):
                continue

            symbol_idx = self._symbol_indices.setdefault(chain, {})
            address_idx = self._address_indices.setdefault(chain, {})

            for token in tokens:
                if not isinstance(token, dict):
                    continue
                try:
                    symbol = str(token.get("symbol", "")).strip()
                    address = str(token.get("address", "")).strip().lower()
                    if not symbol or not address:
                        continue

                    decimals_raw = token.get("decimals", 18)
                    decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                    name = str(token.get("name", "")).strip() or symbol
                    underlying_address = str(token.get("assetAddress", "")).strip().lower()
                    asset = token.get("asset") or {}
                    underlying_symbol = str(asset.get("symbol", "")).strip()

                    meta = FluidMarketToken(
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
                        "Skipping malformed Fluid entry %s on %s: %s",
                        token.get("address", "unknown"),
                        chain,
                        exc,
                    )
                    continue

        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug("Fluid %s: %d fTokens indexed", chain, len(symbol_idx))

    def lookup_by_symbol(self, symbol: str, chain: str) -> FluidMarketToken | None:
        """Look up a Fluid fToken by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> FluidMarketToken | None:
        """Look up a Fluid fToken by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_fluid_lookup() -> FluidMarketLookup:
    """Get (or create) the singleton FluidMarketLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = FluidMarketLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
