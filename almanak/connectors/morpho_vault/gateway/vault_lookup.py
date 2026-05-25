"""Morpho vault (MetaMorpho) fetcher for ERC4626 share-token metadata discovery.

Fetches every whitelisted Morpho vault across supported chains via the
Morpho GraphQL API (``https://blue-api.morpho.org/graphql``) and provides
per-chain symbol and address lookups for the vault share tokens.
Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Whitelisted vaults only — avoids scam / unlisted deployments
    - Vault share tokens are ERC4626; they carry the vault's own symbol
      (e.g., ``gtUSDC``, ``sparkUSDCbc``, ``kpk_USDC_Prime``)
    - Disk cache at ``~/.almanak/morpho_vault_cache.json`` with 24h TTL
    - Single GraphQL POST at startup — no pagination, no per-chain iteration

Inherits plumbing (disk cache, load orchestration, backoff) from
``ProtocolTokenLookup``. The Morpho API is the protocol's own
authoritative source for vault addresses, so on-chain ``symbol()``
confirm is skipped — matching the trust model used for Jupiter /
Pendle / Aave.

Usage:
    from almanak.connectors.morpho_vault.gateway.vault_lookup import get_morpho_lookup

    lookup = await get_morpho_lookup()
    vault = lookup.lookup_by_symbol("gtUSDC", "ethereum")
    if vault:
        print(f"{vault.symbol} at {vault.address}, underlying={vault.underlying_symbol}")
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from almanak.gateway.services._protocol_lookup import ProtocolTokenLookup

logger = logging.getLogger(__name__)

# Morpho public GraphQL endpoint (free, no auth). Single ``vaults`` query
# with a large ``first`` returns all whitelisted vaults in one response.
MORPHO_GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

# EVM chains supported by Morpho that we map to gateway chain names.
# Morpho also operates on newer chains (Katana, HyperEVM, Sonic) that
# the gateway does not yet know how to talk to; those are skipped at
# index-build time.
MORPHO_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "polygon": 137,
}
_CHAIN_NAME_BY_ID: dict[int, str] = {v: k for k, v in MORPHO_CHAIN_IDS.items()}

# GraphQL query — ask only for whitelisted vaults (avoids scam / unlisted
# deployments).  We pull ``first: 1000`` which is the Morpho API's maximum
# and comfortably exceeds the ~185 whitelisted vaults that exist today.
_MORPHO_VAULTS_QUERY = """\
query AllMorphoVaults {
  vaults(first: 1000, where: { whitelisted: true }) {
    items {
      address
      name
      symbol
      chain { id }
      asset { symbol address decimals }
    }
    pageInfo { count countTotal }
  }
}
"""

# Disk cache path and TTL
CACHE_PATH = Path.home() / ".almanak" / "morpho_vault_cache.json"
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Module-level singleton
_instance: "MorphoVaultLookup | None" = None
_instance_lock = asyncio.Lock()


@dataclass
class MorphoVaultToken:
    """MetaMorpho vault share token metadata."""

    address: str  # vault address (the ERC4626 share token), lowercased
    symbol: str
    name: str
    decimals: int
    chain: str
    underlying_symbol: str  # e.g., 'USDC' for a gtUSDC vault
    underlying_address: str  # lowercased


class MorphoVaultLookup(ProtocolTokenLookup):
    """Morpho vault lookup with per-chain indexing and disk caching.

    Fetches all whitelisted MetaMorpho vaults across supported chains in
    one GraphQL call and registers the vault share token for each.  The
    full set is fetched once per 24 hours and cached on disk.

    Thread-safety: safe to use from multiple coroutines after
    initialisation because indexing is read-only after ``_load()`` completes.
    """

    def __init__(self) -> None:
        super().__init__(
            cache_path=CACHE_PATH,
            protocol_name="Morpho vaults",
            cache_ttl_seconds=CACHE_TTL_SECONDS,
        )
        self._symbol_indices: dict[str, dict[str, MorphoVaultToken]] = {}
        self._address_indices: dict[str, dict[str, MorphoVaultToken]] = {}

    def _loaded_summary(self) -> str:
        total = sum(len(idx) for idx in self._symbol_indices.values())
        return f"loaded: {total} vaults indexed across {len(self._symbol_indices)} chains"

    def _validate_payload(self, data: Any) -> bool:
        # ``items`` is a flat list of vault entries.  Reject any cached or
        # fetched payload that isn't a list so we fall through to a retry
        # instead of building empty indices.
        return isinstance(data, list)

    async def _fetch_from_network(self) -> list[dict[str, Any]] | None:
        """Fetch all whitelisted Morpho vaults via a single GraphQL call."""
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            payload = {"query": _MORPHO_VAULTS_QUERY}
            logger.info("Fetching Morpho vaults from %s", MORPHO_GRAPHQL_URL)
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                async with session.post(
                    MORPHO_GRAPHQL_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.warning("Morpho vaults fetch returned HTTP %d", resp.status)
                        return None
                    body = await resp.json(content_type=None)

            if not isinstance(body, dict):
                logger.warning(
                    "Morpho vaults response has unexpected format: %s",
                    type(body).__name__,
                )
                return None

            if body.get("errors"):
                logger.warning("Morpho GraphQL returned errors: %s", body["errors"])
                return None

            vaults = body.get("data", {}).get("vaults", {}).get("items", [])
            if not isinstance(vaults, list) or not vaults:
                return None

            self._write_disk_cache(vaults)
            return vaults

        except Exception as exc:
            logger.warning("Morpho vaults fetch failed: %s", exc)
            return None

    def _build_indices(self, vaults: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from raw vault data.

        Each entry has ``{address, symbol, name, chain.id, asset.{...}}``.
        Vaults on chains we don't map (Katana, HyperEVM, Sonic, ...) are
        silently dropped.  MetaMorpho vaults are ERC4626 and inherit the
        underlying asset's decimals — we treat ``asset.decimals`` as the
        share-token decimals.
        """
        skipped_chains: set[int] = set()

        for vault in vaults:
            if not isinstance(vault, dict):
                continue
            try:
                chain_obj = vault.get("chain") or {}
                chain_id_raw = chain_obj.get("id")
                chain_id = int(chain_id_raw) if isinstance(chain_id_raw, int | float) else -1
                chain = _CHAIN_NAME_BY_ID.get(chain_id)
                if chain is None:
                    skipped_chains.add(chain_id)
                    continue

                address = str(vault.get("address", "")).strip().lower()
                symbol = str(vault.get("symbol", "")).strip()
                name = str(vault.get("name", "")).strip() or symbol

                asset = vault.get("asset") or {}
                underlying_symbol = str(asset.get("symbol", "")).strip()
                underlying_address = str(asset.get("address", "")).strip().lower()
                decimals_raw = asset.get("decimals", 18)
                decimals = int(decimals_raw) if isinstance(decimals_raw, int | float) else 18

                if not address or not symbol:
                    continue

                meta = MorphoVaultToken(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    chain=chain,
                    underlying_symbol=underlying_symbol,
                    underlying_address=underlying_address,
                )

                symbol_idx = self._symbol_indices.setdefault(chain, {})
                address_idx = self._address_indices.setdefault(chain, {})

                # Symbol index (case-insensitive); first entry wins.  Vault
                # symbols are curator-chosen and occasionally collide across
                # chains (``gtUSDC`` exists on Ethereum and Base); per-chain
                # scoping keeps them addressable.
                symbol_upper = symbol.upper()
                if symbol_upper not in symbol_idx:
                    symbol_idx[symbol_upper] = meta

                # Address index: 1:1 — vault addresses are unique per chain.
                if address not in address_idx:
                    address_idx[address] = meta

            except Exception as exc:
                logger.debug(
                    "Skipping malformed Morpho vault %s: %s",
                    vault.get("address", "unknown"),
                    exc,
                )
                continue

        if skipped_chains:
            logger.debug(
                "Morpho vaults skipped on unmapped chains: %s",
                sorted(skipped_chains),
            )
        for chain, symbol_idx in self._symbol_indices.items():
            logger.debug(
                "Morpho %s: %d vaults indexed",
                chain,
                len(symbol_idx),
            )

    def lookup_by_symbol(self, symbol: str, chain: str) -> MorphoVaultToken | None:
        """Look up a Morpho vault by symbol on a given chain."""
        chain_idx = self._symbol_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(symbol.upper())

    def lookup_by_address(self, address: str, chain: str) -> MorphoVaultToken | None:
        """Look up a Morpho vault by contract address on a given chain."""
        chain_idx = self._address_indices.get(chain.lower())
        if chain_idx is None:
            return None
        return chain_idx.get(address.lower())


async def get_morpho_lookup() -> MorphoVaultLookup:
    """Get (or create) the singleton MorphoVaultLookup, ensuring it is loaded."""
    global _instance

    async with _instance_lock:
        if _instance is None:
            _instance = MorphoVaultLookup()

    if not _instance.is_loaded:
        await _instance._load()

    return _instance
