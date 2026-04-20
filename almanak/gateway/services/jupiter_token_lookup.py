"""Jupiter token list fetcher for Solana token metadata discovery.

Fetches the Jupiter token list (https://token.jup.ag/all) and provides
fast symbol and mint address lookups.  Cached to disk for 24 hours.

Key Features:
    - Mint-address index for O(1) lookups by address
    - Symbol index for lookups by symbol (deduped by highest market-cap tag order)
    - Disk cache at ~/.almanak/jupiter_token_cache.json with 24h TTL
    - Graceful degradation: network errors return None, never raise

Usage:
    from almanak.gateway.services.jupiter_token_lookup import get_jupiter_lookup

    lookup = await get_jupiter_lookup()
    token = lookup.lookup_by_mint("USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA")
    if token:
        print(f"{token.symbol} has {token.decimals} decimals")
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Jupiter "all tokens" endpoint (returns ~30k tokens)
JUPITER_TOKEN_LIST_URL = "https://token.jup.ag/all"

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "jupiter_token_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Singleton instance (module-level)
_instance: "JupiterTokenLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class JupiterTokenMetadata:
    """Token metadata from Jupiter token list."""

    address: str  # Mint address (base58)
    symbol: str
    name: str
    decimals: int
    tags: list[str]


class JupiterTokenLookup:
    """Jupiter token list lookup with disk caching.

    Provides fast symbol and mint address lookups for Solana tokens.
    The full Jupiter list is fetched once and cached on disk for 24 hours.

    Thread-safety: the class is safe to use from multiple coroutines after
    initialisation because indexing is read-only after _load() completes.
    """

    def __init__(self) -> None:
        self._mint_index: dict[str, JupiterTokenMetadata] = {}
        self._symbol_index: dict[str, JupiterTokenMetadata] = {}
        self._loaded: bool = False
        self._load_lock = asyncio.Lock()
        # Retry state: allow re-fetch after transient network failures
        self._load_failed: bool = False
        self._retry_after: float = 0.0  # monotonic time after which a retry is allowed

    async def _load(self) -> None:
        """Load the Jupiter token list (disk cache or network fetch)."""
        async with self._load_lock:
            if self._loaded:
                return

            # If a previous load failed, only retry after the backoff period
            if self._load_failed and time.time() < self._retry_after:
                return

            data = self._read_disk_cache()
            if data is None:
                data = await self._fetch_from_network()

            if data is not None:
                self._build_indices(data)
                self._loaded = True
                self._load_failed = False
                logger.info("Jupiter token list loaded: %d tokens indexed", len(self._mint_index))
            else:
                # Transient failure — do NOT permanently mark as loaded.
                # Allow a retry after a 5-minute backoff so the gateway can
                # recover from transient network issues without requiring a restart.
                self._load_failed = True
                self._retry_after = time.time() + 300  # 5-minute backoff
                logger.warning(
                    "Jupiter token list unavailable; Solana dynamic resolution will be limited. "
                    "Will retry in 5 minutes."
                )

    def _read_disk_cache(self) -> list[dict[str, Any]] | None:
        """Read cached token list from disk if still fresh."""
        if not CACHE_PATH.exists():
            return None

        try:
            mtime = CACHE_PATH.stat().st_mtime
            if time.time() - mtime > CACHE_TTL_SECONDS:
                logger.debug("Jupiter disk cache expired, will re-fetch")
                return None

            with CACHE_PATH.open("r", encoding="utf-8") as fh:
                data = json.load(fh)

            if not isinstance(data, list):
                logger.warning("Jupiter disk cache has unexpected format, re-fetching")
                return None

            logger.debug("Jupiter token list loaded from disk cache (%d tokens)", len(data))
            return data

        except Exception as exc:
            logger.warning("Failed to read Jupiter disk cache: %s", exc)
            return None

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch the Jupiter token list from the network."""
        try:
            import aiohttp  # lazy import -- gateway dep

            logger.info("Fetching Jupiter token list from %s", JUPITER_TOKEN_LIST_URL)
            async with aiohttp.ClientSession() as session:
                async with session.get(JUPITER_TOKEN_LIST_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning("Jupiter token list fetch returned HTTP %d", resp.status)
                        return None
                    data = await resp.json(content_type=None)

            if not isinstance(data, list):
                logger.warning("Jupiter token list unexpected format: %s", type(data))
                return None

            # Write to disk cache
            self._write_disk_cache(data)
            return data

        except Exception as exc:
            logger.warning("Jupiter token list fetch failed: %s", exc)
            return None

    def _write_disk_cache(self, data: list[dict[str, Any]]) -> None:
        """Write token list to disk cache."""
        try:
            CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = CACHE_PATH.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp_path, CACHE_PATH)
            logger.debug("Jupiter token list cached to disk (%d tokens)", len(data))
        except Exception as exc:
            logger.warning("Failed to write Jupiter disk cache: %s", exc)

    def _build_indices(self, data: list[dict[str, Any]]) -> None:
        """Build mint and symbol indices from raw token list."""
        for raw in data:
            try:
                address = str(raw.get("address", "")).strip()
                symbol = str(raw.get("symbol", "")).strip()
                name = str(raw.get("name", "")).strip()
                decimals_raw = raw.get("decimals", 0)
                tags = list(raw.get("tags", []))

                if not address or not symbol:
                    continue

                decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 0

                meta = JupiterTokenMetadata(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    tags=tags,
                )

                # Index by mint (case-sensitive for Solana)
                self._mint_index[address] = meta

                # Index by symbol (uppercase); first entry wins
                # (Jupiter list is roughly sorted by liquidity/importance)
                symbol_key = symbol.upper()
                if symbol_key not in self._symbol_index:
                    self._symbol_index[symbol_key] = meta

            except Exception as exc:
                logger.debug("Skipping malformed Jupiter token entry %s: %s", raw.get("address", "unknown"), exc)
                continue

    def lookup_by_mint(self, mint: str) -> JupiterTokenMetadata | None:
        """Look up a token by its Solana mint address.

        Args:
            mint: Solana mint address (base58, case-sensitive)

        Returns:
            JupiterTokenMetadata or None if not found
        """
        return self._mint_index.get(mint)

    def lookup_by_symbol(self, symbol: str) -> JupiterTokenMetadata | None:
        """Look up a token by symbol.

        Returns the highest-priority result (first entry in Jupiter list,
        which is roughly sorted by liquidity/importance).

        Args:
            symbol: Token symbol (case-insensitive)

        Returns:
            JupiterTokenMetadata or None if not found
        """
        return self._symbol_index.get(symbol.upper())

    @property
    def is_loaded(self) -> bool:
        """Return True if the token list has been successfully loaded.

        Returns False if the last load attempt failed (even if _loaded was not set),
        so that get_jupiter_lookup() knows to retry after the backoff period.
        """
        return self._loaded and not self._load_failed


async def get_jupiter_lookup() -> JupiterTokenLookup:
    """Get (or create) the singleton JupiterTokenLookup, ensuring it is loaded.

    This is the preferred entry point. The lookup is initialised on first call
    and returned immediately on subsequent calls.

    Returns:
        Loaded JupiterTokenLookup instance
    """
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = JupiterTokenLookup()

    # Load outside the creation lock so concurrent callers can share the instance
    if not _instance.is_loaded:
        await _instance._load()

    return _instance
