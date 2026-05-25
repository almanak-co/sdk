"""Yearn Finance vault fetcher for yvToken metadata discovery.

Fetches every active Yearn vault (v2 legacy + v3) from ``ydaemon.yearn.fi``
in a single GET and provides per-chain symbol and address lookups for
the ``yv<...>`` share tokens that users hold when they deposit into a
Yearn vault.  Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Covers every Yearn vault ydaemon considers worth surfacing (the
      ``hideAlways=true`` query drops deprecated/test vaults).
    - yvTokens follow a consistent ``yv<...>`` naming scheme
      (``yvUSDC``, ``yvDAI``, ``yvCurve-stETH-frxETH-f``, ...).
    - Vault-share decimals come straight from the API (unlike Beefy,
      where we inherit from underlying).
    - Disk cache at ``~/.almanak/yearn_vault_cache.json`` with 24h TTL
    - Single HTTP GET at startup — no pagination, no per-chain iteration

Inherits plumbing (disk cache, load orchestration, backoff) from
``ProtocolTokenLookup``. ydaemon is Yearn-authored, so on-chain
``symbol()`` confirm is skipped — matching the trust model used for
every other protocol tier.

Usage:
    from almanak.connectors.yearn.gateway.vault_lookup import get_yearn_lookup

    lookup = await get_yearn_lookup()
    token = lookup.lookup_by_symbol("yvUSDC-1", "ethereum")
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

# ydaemon is Yearn's canonical data source (v2 + v3 aggregated).
# ``hideAlways=true`` filters out deprecated/test vaults; ``limit=1000``
# covers the full set today with room to grow.
YEARN_VAULTS_URL = "https://ydaemon.yearn.fi/vaults?hideAlways=true&limit=1000"

# chainID → gateway chain name.  ydaemon also returns Katana (747474)
# which we don't support yet; unmapped chains are silently dropped.
_CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    42161: "arbitrum",
    8453: "base",
    137: "polygon",
    # Note: ydaemon also reports Gnosis (chain id 100), but the gateway's
    # ``ALLOWED_CHAINS`` and the ``Chain`` enum don't include Gnosis
    # today, so any Gnosis request would be rejected upstream before it
    # reaches this lookup. Re-add once Gnosis lands in both
    # ``almanak.core.enums.Chain`` and ``ALLOWED_CHAINS``.
}

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "yearn_vault_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "YearnVaultLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class YearnVaultToken:
    """Yearn vault share token (yvToken) metadata."""

    address: str  # vault share address, lowercased
    symbol: str  # e.g. ``yvUSDC-1``
    name: str  # vault name from ydaemon
    decimals: int  # vault-share decimals (from API, not inherited)
    chain: str
    underlying_symbol: str
    underlying_address: str
    category: str  # e.g., 'Curve', 'Stablecoin', 'Volatile'
    version: str  # e.g., '0.4.6', '3.0.4' (legacy vs v3)
    kind: str  # 'Legacy', 'Single Strategy', 'Multi Strategy'


class YearnVaultLookup(ProtocolTokenLookup):
    """Yearn vault lookup with per-chain indexing and disk caching.

    Fetches every ``hideAlways=true``-filtered vault from ydaemon in one
    GET and registers the ``symbol`` → ``address`` mapping per chain.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Yearn vaults",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, YearnVaultToken]] = {}
        self._address_indices: dict[str, dict[str, YearnVaultToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} vaults indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # ydaemon returns a flat list of vault dicts; reject empty-list
        # payloads so a malformed cache triggers a refetch instead of
        # silently pinning the lookup at zero indices.
        return isinstance(data, list) and bool(data)

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch all Yearn vaults via a single GET from ydaemon."""
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            logger.info("Fetching Yearn vaults from %s", YEARN_VAULTS_URL)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.get(YEARN_VAULTS_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning("Yearn vaults fetch returned HTTP %d", resp.status)
                        return None
                    body = await resp.json(content_type=None)

            if not isinstance(body, list) or not body:
                logger.warning(
                    "Yearn vaults response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            self._write_disk_cache(body)
            return body

        except Exception as exc:
            logger.warning("Yearn vaults fetch failed: %s", exc)
            return None

    def _build_indices(self, vaults: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from raw ydaemon data.

        Drops vaults on chains the gateway doesn't speak and vaults with
        empty symbol/address.  Vault decimals come from the entry itself
        (ydaemon includes them; Beefy and Morpho don't).
        """
        skipped_chains: set[int] = set()

        for vault in vaults:
            if not isinstance(vault, dict):
                continue
            try:
                chain_id_raw = vault.get("chainID")
                chain_id = int(chain_id_raw) if isinstance(chain_id_raw, int | float) else -1
                chain = _CHAIN_ID_TO_NAME.get(chain_id)
                if chain is None:
                    skipped_chains.add(chain_id)
                    continue

                symbol = str(vault.get("symbol", "")).strip()
                address = str(vault.get("address", "")).strip().lower()
                if not symbol or not address:
                    continue

                decimals_raw = vault.get("decimals", 18)
                decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                name = str(vault.get("name", "")).strip() or symbol
                category = str(vault.get("category", "")).strip()
                version = str(vault.get("version", "")).strip()
                kind = str(vault.get("kind", "")).strip()

                underlying = vault.get("token") or {}
                underlying_symbol = str(underlying.get("symbol", "")).strip()
                underlying_address = str(underlying.get("address", "")).strip().lower()

                meta = YearnVaultToken(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    chain=chain,
                    underlying_symbol=underlying_symbol,
                    underlying_address=underlying_address,
                    category=category,
                    version=version,
                    kind=kind,
                )

                symbol_idx = self._symbol_indices.setdefault(chain, {})
                address_idx = self._address_indices.setdefault(chain, {})

                # Symbol index (case-insensitive); first entry wins.  Yearn
                # sometimes has symbol collisions across v2/v3 (e.g., two
                # ``yvUSDC`` vaults with different addresses); keeping the
                # first seen entry matches the API's ordering, which tends
                # to prefer featured/newer vaults.
                symbol_upper = symbol.upper()
                if symbol_upper not in symbol_idx:
                    symbol_idx[symbol_upper] = meta

                # Address index: 1:1.
                if address not in address_idx:
                    address_idx[address] = meta

            except Exception as exc:
                logger.debug(
                    "Skipping malformed Yearn vault %s: %s",
                    vault.get("address", "unknown"),
                    exc,
                )
                continue

        if skipped_chains:
            logger.debug(
                "Yearn vaults skipped on unmapped chains: %s",
                sorted(skipped_chains),
            )
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug("Yearn %s: %d vaults indexed", chain, len(symbol_idx))

    def lookup_by_symbol(self, symbol: str, chain: str) -> YearnVaultToken | None:
        """Look up a Yearn yvToken by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> YearnVaultToken | None:
        """Look up a Yearn yvToken by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_yearn_lookup() -> YearnVaultLookup:
    """Get (or create) the singleton YearnVaultLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = YearnVaultLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
