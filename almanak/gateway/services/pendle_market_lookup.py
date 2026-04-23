"""Pendle markets fetcher for PT/YT/SY/LP token metadata discovery across EVM chains.

Fetches every Pendle asset in a single call via
https://api-v2.pendle.finance/core/v1/assets/all — which returns all PT, YT,
SY, and LP tokens across all Pendle-supported chains in one response — and
provides fast per-chain symbol and address lookups. Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Four token types per market: PT (principal), YT (yield), SY (standardised
      yield wrapper), PENDLE_LP (market LP token)
    - Disk cache at ~/.almanak/pendle_market_cache.json with 24h TTL
    - Single HTTP call at startup — no pagination, no per-chain iteration
    - Graceful degradation: network/parse failure returns None, never raises.

Inherits plumbing (disk cache, load orchestration, backoff, singleton lock)
from ``ProtocolTokenLookup``. The Pendle API is the protocol's own
authoritative source, so on-chain ``symbol()`` confirmation is skipped —
matching the trust model already used for Jupiter on Solana.

Usage:
    from almanak.gateway.services.pendle_market_lookup import get_pendle_lookup

    lookup = await get_pendle_lookup()
    token = lookup.lookup_by_symbol("PT-rsETH-26JUN2026", "ethereum")
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

# Pendle cross-chain bulk assets endpoint (free, no auth, single call covers every chain)
PENDLE_ASSETS_URL = "https://api-v2.pendle.finance/core/v1/assets/all"

# EVM chains supported by Pendle that we currently map to gateway chain names.
# Kept in sync with ``almanak.framework.data.pendle.api_client.CHAIN_ID_MAP``.
# Pendle's /assets/all endpoint also returns entries for newer chains (Sonic,
# Mantle, Berachain, ...); those are skipped during index building because the
# gateway doesn't yet know how to talk to them.
PENDLE_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "bsc": 56,
}
_CHAIN_NAME_BY_ID: dict[int, str] = {v: k for k, v in PENDLE_CHAIN_IDS.items()}

# Pendle's /assets/all returns these tag values for the token types we care
# about.  PENDLE_LP is the market/pool token; keep it under the generic
# LP designation on our side but do not rename (symbol matching is by the
# raw Pendle symbol string).
_PENDLE_TOKEN_TAGS: frozenset[str] = frozenset({"PT", "YT", "SY", "PENDLE_LP"})

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "pendle_market_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Singleton instance (module-level)
_instance: "PendleMarketLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class PendleTokenMetadata:
    """Pendle token metadata (PT, YT, SY, or LP)."""

    address: str  # checksum-insensitive; stored lowercased
    symbol: str
    name: str
    decimals: int
    token_type: str  # 'PT', 'YT', 'SY', or 'LP' (Pendle pool LP token)
    chain: str  # 'ethereum', 'arbitrum', ...


class PendleMarketLookup(ProtocolTokenLookup):
    """Pendle markets lookup with per-chain indexing and disk caching.

    Fetches all active markets from Pendle's v1 endpoint for each supported
    chain and registers the PT, YT, SY, and LP token for each market. The
    full set is fetched once per 24 hours and cached on disk.

    Thread-safety: safe to use from multiple coroutines after initialisation
    because indexing is read-only after ``_load()`` completes.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Pendle markets",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        # Per-chain indices: chain -> {symbol_upper -> PendleTokenMetadata}
        self._symbol_indices: dict[str, dict[str, PendleTokenMetadata]] = {}
        # Per-chain address indices: chain -> {address_lower -> PendleTokenMetadata}
        self._address_indices: dict[str, dict[str, PendleTokenMetadata]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} tokens indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # The /assets/all endpoint returns a flat list; bail out of any
        # cache or network payload that isn't a *non-empty* list so we
        # fall through to a retry. An empty list (``[]``) would otherwise
        # be accepted as valid, pinning the lookup in a zero-index state
        # until the 24h disk TTL expires.
        return isinstance(data, list) and bool(data)

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch all Pendle assets across every chain in a single call.

        Returns the raw flat list of asset dicts (one per PT/YT/SY/LP token),
        or None if the request failed or the response was malformed.
        """
        try:
            import aiohttp  # lazy import — gateway dep

            logger.info("Fetching Pendle assets from %s", PENDLE_ASSETS_URL)
            async with aiohttp.ClientSession() as session:
                async with session.get(PENDLE_ASSETS_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "Pendle assets fetch returned HTTP %d",
                            resp.status,
                        )
                        return None
                    body = await resp.json(content_type=None)

            # The endpoint wraps the array in ``{"assets": [...]}``.
            if isinstance(body, dict) and isinstance(body.get("assets"), list):
                assets = body["assets"]
            elif isinstance(body, list):
                assets = body
            else:
                logger.warning(
                    "Pendle assets response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            if not assets:
                return None

            self._write_disk_cache(assets)
            return assets

        except Exception as exc:
            logger.warning("Pendle assets fetch failed: %s", exc)
            return None

    def _build_indices(self, assets: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from the flat asset list.

        Each asset carries its ``chainId`` and a ``tags`` list.  We only keep
        assets tagged PT/YT/SY/PENDLE_LP whose chain has a gateway mapping in
        ``PENDLE_CHAIN_IDS``.  Newer chains that Pendle returns but the
        gateway does not yet know how to talk to are silently dropped.
        """
        skipped_chains: set[int] = set()

        for asset in assets:
            if not isinstance(asset, dict):
                continue
            try:
                chain_id_raw = asset.get("chainId")
                chain_id = int(chain_id_raw) if isinstance(chain_id_raw, int | float) else -1
                chain = _CHAIN_NAME_BY_ID.get(chain_id)
                if chain is None:
                    skipped_chains.add(chain_id)
                    continue

                tags = asset.get("tags") or []
                token_type: str | None = None
                for tag in tags:
                    if tag in _PENDLE_TOKEN_TAGS:
                        # PENDLE_LP is shortened to LP on our side for consistency
                        # with PT/YT/SY naming; the raw symbol is unchanged.
                        token_type = "LP" if tag == "PENDLE_LP" else tag
                        break
                if token_type is None:
                    continue

                address = str(asset.get("address", "")).strip().lower()
                symbol = str(asset.get("symbol", "")).strip()
                name = str(asset.get("name", "")).strip() or symbol
                decimals_raw = asset.get("decimals", 18)
                decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                if not address or not symbol:
                    continue

                meta = PendleTokenMetadata(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    token_type=token_type,
                    chain=chain,
                )

                symbol_idx = self._symbol_indices.setdefault(chain, {})
                address_idx = self._address_indices.setdefault(chain, {})

                # Symbol index: first entry wins.  Pendle's asset list is
                # roughly ordered by activity so the most-liquid expiry ends
                # up owning a bare/ambiguous symbol.
                symbol_upper = symbol.upper()
                if symbol_upper not in symbol_idx:
                    symbol_idx[symbol_upper] = meta

                # Address index is 1:1 — addresses are globally unique on-chain.
                if address not in address_idx:
                    address_idx[address] = meta

            except Exception as exc:
                logger.debug(
                    "Skipping malformed Pendle asset %s: %s",
                    asset.get("address", "unknown"),
                    exc,
                )
                continue

        if skipped_chains:
            logger.debug(
                "Pendle assets skipped on unmapped chains: %s",
                sorted(skipped_chains),
            )
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug(
                "Pendle %s: %d unique symbols, %d unique addresses",
                chain,
                len(symbol_idx),
                len(self._address_indices.get(chain, {})),
            )

    def lookup_by_symbol(self, symbol: str, chain: str) -> PendleTokenMetadata | None:
        """Look up a Pendle PT/YT/SY/LP token by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> PendleTokenMetadata | None:
        """Look up a Pendle PT/YT/SY/LP token by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_pendle_lookup() -> PendleMarketLookup:
    """Get (or create) the singleton PendleMarketLookup, ensuring it is loaded.

    The lookup is initialised on first call and returned immediately on
    subsequent calls.  If a prior load failed and the backoff window has
    passed, this will retry.
    """
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = PendleMarketLookup()

    # Load outside the creation lock so concurrent callers can share the instance
    if not _instance.is_loaded:
        await _instance._load()

    return _instance
