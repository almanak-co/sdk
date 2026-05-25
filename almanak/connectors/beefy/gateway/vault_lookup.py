"""Beefy Finance vault fetcher for mooToken metadata discovery.

Fetches every Beefy vault from ``https://api.beefy.finance/vaults`` in a
single GET and provides per-chain symbol and address lookups for the
``moo<...>`` share tokens that users hold when they deposit into a
Beefy vault.  Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Active vaults only — EOL (end-of-life) and paused vaults are
      dropped.  Users shouldn't be touching deprecated vaults, and
      including them would bloat the index ~6x (3900 → 645).
    - mooTokens follow a consistent ``moo<...>`` naming scheme
      (``mooCurveUSDC-USDf``, ``mooAaveWBTC``, ``mooBalancerGHO-USDC``, ...)
    - Disk cache at ``~/.almanak/beefy_vault_cache.json`` with 24h TTL
    - Single HTTP GET at startup — no pagination, no per-chain iteration

Inherits plumbing (disk cache, load orchestration, backoff) from
``ProtocolTokenLookup``. Beefy's API is the protocol's own
authoritative source, so on-chain ``symbol()`` confirm is skipped —
matching the trust model used for Jupiter / Pendle / Aave /
Morpho / Compound.

Usage:
    from almanak.connectors.beefy.gateway.vault_lookup import get_beefy_lookup

    lookup = await get_beefy_lookup()
    token = lookup.lookup_by_symbol("mooCurveUSDC-USDf", "ethereum")
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

# Beefy's canonical vault list (free, no auth).  One flat array per call.
BEEFY_VAULTS_URL = "https://api.beefy.finance/vaults"

# Beefy chain name → gateway chain name.  Most match 1:1 (``ethereum``,
# ``base``, ``arbitrum``, ...); Beefy uses ``avax`` where we use
# ``avalanche``.  Chains outside this map (``monad``, ``sonic``,
# ``fraxtal``, ...) are silently dropped at index-build time since
# the gateway doesn't speak them yet.
_BEEFY_CHAIN_TO_GATEWAY: dict[str, str] = {
    "ethereum": "ethereum",
    "base": "base",
    "optimism": "optimism",
    "arbitrum": "arbitrum",
    "bsc": "bsc",
    "avax": "avalanche",
    "polygon": "polygon",
    "linea": "linea",
}

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "beefy_vault_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "BeefyVaultLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class BeefyVaultToken:
    """Beefy vault share token (mooToken) metadata."""

    address: str  # vault share address, lowercased
    symbol: str  # e.g. ``mooCurveUSDC-USDf``
    name: str  # vault name from Beefy (human-readable)
    decimals: int  # inherited from underlying (safe default)
    chain: str  # 'ethereum', 'arbitrum', ...
    underlying_symbol: str  # first asset in the vault (e.g., 'USDC' or 'USDC/USDf')
    underlying_address: str  # lowercased; may be the LP token address for LP vaults
    platform: str  # 'curve', 'aave', 'balancer', ... (useful diagnostic)


class BeefyVaultLookup(ProtocolTokenLookup):
    """Beefy vault lookup with per-chain indexing and disk caching.

    Fetches every Beefy vault, filters to ``status == 'active'``, and
    registers the ``earnedToken`` (mooToken symbol) → ``earnedTokenAddress``
    mapping per chain.  The full set is fetched once per 24 hours and
    cached on disk.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Beefy vaults",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, BeefyVaultToken]] = {}
        self._address_indices: dict[str, dict[str, BeefyVaultToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} active vaults indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # /vaults returns a flat list of vault dicts. An empty list
        # should be rejected so a bogus/partial cache triggers a
        # refetch rather than locking the lookup at zero entries.
        return isinstance(data, list) and bool(data)

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch all Beefy vaults via a single GET."""
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            logger.info("Fetching Beefy vaults from %s", BEEFY_VAULTS_URL)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.get(BEEFY_VAULTS_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning("Beefy vaults fetch returned HTTP %d", resp.status)
                        return None
                    body = await resp.json(content_type=None)

            if not isinstance(body, list) or not body:
                logger.warning(
                    "Beefy vaults response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            self._write_disk_cache(body)
            return body

        except Exception as exc:
            logger.warning("Beefy vaults fetch failed: %s", exc)
            return None

    def _build_indices(self, vaults: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from raw vault list.

        Filters:
            - ``status == 'active'`` only (skip EOL + paused)
            - Chain must map to a gateway-known name
            - Non-empty ``earnedToken`` symbol + ``earnedTokenAddress``

        mooToken decimals are not in the API response; we default to the
        underlying's ``tokenDecimals`` which matches Beefy's convention
        for most vaults.  Callers who need a precise decimals value can
        do an on-chain check.
        """
        skipped_chains: set[str] = set()
        skipped_status = 0

        for vault in vaults:
            if not isinstance(vault, dict):
                continue
            try:
                if vault.get("status") != "active":
                    skipped_status += 1
                    continue

                beefy_chain = str(vault.get("chain", "")).strip()
                chain = _BEEFY_CHAIN_TO_GATEWAY.get(beefy_chain)
                if chain is None:
                    skipped_chains.add(beefy_chain)
                    continue

                symbol = str(vault.get("earnedToken", "")).strip()
                address = str(vault.get("earnedTokenAddress", "")).strip().lower()
                if not symbol or not address:
                    continue

                decimals_raw = vault.get("tokenDecimals", 18)
                decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                name = str(vault.get("name", "")).strip() or symbol
                platform = str(vault.get("platformId", "")).strip()

                underlying_symbol = str(vault.get("token", "")).strip()
                underlying_address = str(vault.get("tokenAddress", "")).strip().lower()

                meta = BeefyVaultToken(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    chain=chain,
                    underlying_symbol=underlying_symbol,
                    underlying_address=underlying_address,
                    platform=platform,
                )

                symbol_idx = self._symbol_indices.setdefault(chain, {})
                address_idx = self._address_indices.setdefault(chain, {})

                # Symbol index (case-insensitive); first entry wins.  Beefy
                # occasionally has a migrated vault pair with near-identical
                # symbols (``mooFoo`` / ``mooFooV2``); keeping the first
                # seen entry matches the order the API returns, which is
                # roughly by createdAt ascending — older wins.
                symbol_upper = symbol.upper()
                if symbol_upper not in symbol_idx:
                    symbol_idx[symbol_upper] = meta

                # Address index: 1:1.
                if address not in address_idx:
                    address_idx[address] = meta

            except Exception as exc:
                logger.debug(
                    "Skipping malformed Beefy vault %s: %s",
                    vault.get("id", "unknown"),
                    exc,
                )
                continue

        if skipped_chains:
            logger.debug(
                "Beefy vaults skipped on unmapped chains: %s",
                sorted(skipped_chains),
            )
        if skipped_status:
            logger.debug("Beefy vaults skipped as EOL/paused: %d", skipped_status)
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug("Beefy %s: %d vaults indexed", chain, len(symbol_idx))

    def lookup_by_symbol(self, symbol: str, chain: str) -> BeefyVaultToken | None:
        """Look up a Beefy mooToken by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> BeefyVaultToken | None:
        """Look up a Beefy mooToken by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_beefy_lookup() -> BeefyVaultLookup:
    """Get (or create) the singleton BeefyVaultLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = BeefyVaultLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
