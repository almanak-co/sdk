"""Morpho vault (MetaMorpho v1 + Morpho Vault V2) fetcher for ERC4626 share-token metadata discovery.

Fetches every *listed* Morpho vault across supported chains via the
Morpho GraphQL API (``https://blue-api.morpho.org/graphql``) and provides
per-chain symbol and address lookups for the vault share tokens.
Cached to disk for 24 hours.

Key Features:
    - Per-chain symbol and address indices for O(1) lookups
    - Listed vaults only — avoids scam / unlisted deployments. (Morpho renamed
      the API filter from ``whitelisted`` to ``listed``; the old field is
      rejected outright, so a query still using it indexes ZERO vaults.)
    - Both vault generations: MetaMorpho v1 (``vaults`` query) and Morpho
      Vault V2 (``vaultV2s`` query). Every entry is tagged ``vault_version``
      (``"v1"`` / ``"v2"``) so callers can tell them apart — the SDK's
      redeem path is v1-only today (V2 returns 0 for ``maxRedeem``).
    - Vault share tokens are ERC4626; they carry the vault's own symbol
      (e.g., ``gtUSDC``, ``sparkUSDCbc``, ``steakUSDC``). Symbols are
      curator-chosen and DO collide across generations on one chain
      (Base has two v1 ``steakUSDC`` vaults and one V2); the address
      index is the exact one, the symbol index is first-seen-wins.
    - Disk cache at ``~/.almanak/morpho_vault_cache.json`` with 24h TTL
    - Two GraphQL POSTs at startup (one per generation) — no pagination,
      no per-chain iteration

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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from almanak.connectors._base.chain_ids import chain_ids_from_supported_chains, chain_names_by_id
from almanak.connectors.morpho_vault.connector import CONNECTOR
from almanak.gateway.services._protocol_lookup import ProtocolTokenLookup

logger = logging.getLogger(__name__)

# Morpho public GraphQL endpoint (free, no auth). Single ``vaults`` query
# with a large ``first`` returns all whitelisted vaults in one response.
MORPHO_GRAPHQL_URL = "https://blue-api.morpho.org/graphql"

# EVM chains are projected from the connector's end-to-end support truth.
MORPHO_CHAIN_IDS: Mapping[str, int] = chain_ids_from_supported_chains(CONNECTOR.supported_chains)
_CHAIN_NAME_BY_ID: Mapping[int, str] = chain_names_by_id(MORPHO_CHAIN_IDS)

# GraphQL queries — ask only for LISTED vaults (avoids scam / unlisted
# deployments).  We pull ``first: 1000`` which is the Morpho API's maximum
# and comfortably exceeds the ~100 listed v1 + ~180 listed V2 vaults that
# exist today.
#
# NOTE: the filter field is ``listed``. Morpho's API used to call it
# ``whitelisted``; that name is no longer in ``VaultFilters`` and the API
# answers ``Field "whitelisted" is not defined by type "VaultFilters"`` —
# a hard GraphQL validation error, which ``_fetch_from_network`` turns into
# ``None`` (zero vaults indexed, every vault symbol resolves ``not_found``).
# ``tests/smoke/test_morpho_vault_lookup_live.py`` pins the live schema.
_MORPHO_VAULTS_QUERY = """\
query AllMorphoVaults {
  vaults(first: 1000, where: { listed: true }) {
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

# Morpho Vault V2 lives under a separate root field with the same item shape.
_MORPHO_VAULTS_V2_QUERY = """\
query AllMorphoVaultsV2 {
  vaultV2s(first: 1000, where: { listed: true }) {
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

# (root field, query, vault_version tag) — fetched in this order; v1 first so
# the first-seen-wins symbol index keeps preferring the generation the SDK
# can fully round-trip today.
_MORPHO_VAULT_QUERIES: tuple[tuple[str, str, str], ...] = (
    ("vaults", _MORPHO_VAULTS_QUERY, "v1"),
    ("vaultV2s", _MORPHO_VAULTS_V2_QUERY, "v2"),
)

VAULT_VERSION_V1 = "v1"
VAULT_VERSION_V2 = "v2"


def share_decimals_for(underlying_decimals: int) -> int:
    """Share-token decimals of a Morpho vault over an asset with ``underlying_decimals``.

    Both generations mint shares at ``assetDecimals + max(0, 18 - assetDecimals)``,
    i.e. ``max(18, assetDecimals)``: a 6-decimal USDC vault has 18-decimal shares.
    """
    return max(18, int(underlying_decimals))


def _is_truncated_page(body: Any, root_field: str) -> bool:
    """True when the API reports more vaults (``pageInfo.countTotal``) than it returned.

    The query asks for ``first: 1000``; if the listed universe ever exceeds that,
    caching the page as the complete index would silently drop every vault past
    the cut for 24 hours. Treated as a failed generation instead.
    """
    data = body.get("data") if isinstance(body, dict) else None
    root = data.get(root_field) if isinstance(data, dict) else None
    if not isinstance(root, dict):
        return False
    page = root.get("pageInfo")
    total = page.get("countTotal") if isinstance(page, dict) else None
    items = root.get("items")
    return isinstance(total, int) and isinstance(items, list) and total > len(items)


def _has_items_list(body: Any, root_field: str) -> bool:
    """True only when ``body.data.<root_field>.items`` is a real list (possibly empty)."""
    if not isinstance(body, dict):
        return False
    data = body.get("data")
    root = data.get(root_field) if isinstance(data, dict) else None
    return isinstance(root, dict) and isinstance(root.get("items"), list)


def tag_vault_payload(body: Any, root_field: str, version: str) -> list[dict[str, Any]]:
    """Extract ``data.<root_field>.items`` from a GraphQL body and tag each entry.

    Pure helper (no I/O) so the schema-drift handling is unit-testable:
    a body carrying ``errors`` yields ``[]`` and logs the API's message.
    """
    if not isinstance(body, dict):
        logger.warning("Morpho %s vaults response has unexpected format: %s", version, type(body).__name__)
        return []
    if body.get("errors"):
        logger.warning("Morpho GraphQL returned errors for %s (%s): %s", root_field, version, body["errors"])
        return []
    data = body.get("data") or {}
    root = data.get(root_field) if isinstance(data, dict) else None
    items = root.get("items", []) if isinstance(root, dict) else []
    if not isinstance(items, list):
        return []
    tagged: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            entry = dict(item)
            entry["vault_version"] = version
            tagged.append(entry)
    return tagged


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
    underlying_decimals: int | None = None  # None only for entries loaded from a pre-fix cache
    vault_version: str = VAULT_VERSION_V1  # "v1" (MetaMorpho) | "v2" (Morpho Vault V2)


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
        """Fetch all listed Morpho vaults (v1 + V2) via two GraphQL calls.

        Each generation is fetched independently and tagged with
        ``vault_version`` before the lists are merged, so one failing root
        field degrades to a partial in-memory index instead of an empty one.
        Returns ``None`` only when *neither* generation produced a vault.

        The 24-hour disk cache is written ONLY when every generation
        answered: a partial merge on disk would hide the failed generation
        (every one of its symbols and addresses) for a day, long after the
        API recovered, whereas an unwritten cache makes the next load retry.
        """
        try:
            import aiohttp  # lazy import — gateway dep

            from almanak.gateway.utils.ssl_context import build_ssl_context

            merged: list[dict[str, Any]] = []
            complete = True
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=build_ssl_context())) as session:
                for root_field, query, version in _MORPHO_VAULT_QUERIES:
                    vaults = await self._fetch_generation(session, root_field, query, version)
                    if vaults is None:
                        complete = False
                        continue
                    merged.extend(vaults)
            if not merged:
                return None
            if complete:
                self._write_disk_cache(merged)
            else:
                logger.warning(
                    "Morpho vault index is PARTIAL (a generation query failed); serving %d vaults in memory "
                    "and leaving the disk cache untouched so the next load retries",
                    len(merged),
                )
            return merged

        except Exception as exc:
            logger.warning("Morpho vaults fetch failed: %s", exc)
            return None

    async def _fetch_generation(
        self,
        session: Any,
        root_field: str,
        query: str,
        version: str,
    ) -> list[dict[str, Any]] | None:
        """POST one generation's query; return its items tagged ``vault_version``.

        Returns ``None`` when the generation could not be fetched (transport
        error, non-200, or a GraphQL ``errors`` payload such as a renamed
        filter field) — distinct from an empty ``[]`` answer — so the caller
        can withhold the disk cache. The API's own message is logged at
        WARNING so schema drift is visible in gateway logs instead of
        surfacing as ``not_found`` for every vault.
        """
        import aiohttp  # lazy import — gateway dep

        logger.info("Fetching Morpho %s vaults (%s) from %s", version, root_field, MORPHO_GRAPHQL_URL)
        try:
            async with session.post(
                MORPHO_GRAPHQL_URL,
                json={"query": query},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.warning("Morpho %s vaults fetch returned HTTP %d", version, resp.status)
                    return None
                body = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("Morpho %s vaults fetch failed: %s", version, exc)
            return None
        if not isinstance(body, dict) or body.get("errors"):
            tag_vault_payload(body, root_field, version)  # logs the API's own message
            return None
        if (
            not isinstance(body, dict)
            or body.get("errors")
            or not _has_items_list(body, root_field)
            or _is_truncated_page(body, root_field)
        ):
            # A 200 with ``{"data": {}}`` / a non-list ``items`` is a malformed
            # answer, not an empty one: treating it as ``[]`` would cache the
            # other generation alone for 24h. Only a real list counts as answered.
            logger.warning(
                "Morpho %s vaults response lacks data.%s.items; treating the generation as failed", version, root_field
            )
            return None
        return tag_vault_payload(body, root_field, version)

    def _build_indices(self, vaults: list[dict[str, Any]]) -> None:
        """Build per-chain symbol and address indices from raw vault data.

        Each entry has ``{address, symbol, name, chain.id, asset.{...}}``
        plus the ``vault_version`` tag added at fetch time (entries from a
        pre-V2 disk cache lack it and default to ``"v1"``).
        Vaults on chains we don't map (Katana, HyperEVM, Sonic, ...) are
        silently dropped.  Morpho vaults are ERC4626 and inherit the
        underlying asset's decimals PLUS an offset that brings the share
        token to at least 18 decimals (MetaMorpho v1: ``DECIMALS_OFFSET =
        max(0, 18 - assetDecimals)``; Vault V2: ``decimals = assetDecimals +
        zeroFloorSub(18, assetDecimals)``). So a USDC vault's shares are
        18-decimal even though USDC is 6 — the share decimals are derived
        here (``share_decimals_for``), never copied from the underlying.
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
                decimals_raw = asset.get("decimals")
                if not isinstance(decimals_raw, int | float) or isinstance(decimals_raw, bool):
                    # Empty != Zero: without the underlying's decimals the share
                    # decimals cannot be derived; skip the entry loudly rather
                    # than publish a guess.
                    logger.warning(
                        "Skipping Morpho vault %s (%s): underlying decimals missing from the API payload",
                        address,
                        symbol,
                    )
                    continue
                underlying_decimals = int(decimals_raw)
                decimals = share_decimals_for(underlying_decimals)

                if not address or not symbol:
                    continue

                version_raw = str(vault.get("vault_version", VAULT_VERSION_V1)).strip().lower()
                vault_version = VAULT_VERSION_V2 if version_raw == VAULT_VERSION_V2 else VAULT_VERSION_V1

                meta = MorphoVaultToken(
                    address=address,
                    symbol=symbol,
                    name=name,
                    decimals=decimals,
                    chain=chain,
                    underlying_symbol=underlying_symbol,
                    underlying_address=underlying_address,
                    underlying_decimals=underlying_decimals,
                    vault_version=vault_version,
                )

                symbol_idx = self._symbol_indices.setdefault(chain, {})
                address_idx = self._address_indices.setdefault(chain, {})

                # Symbol index (case-insensitive); first entry wins.  Vault
                # symbols are curator-chosen and occasionally collide across
                # chains (``gtUSDC`` exists on Ethereum and Base) AND across
                # generations on one chain (Base ``steakUSDC``: two v1 + one
                # V2); per-chain scoping keeps them addressable and v1 is
                # fetched first so it wins ties. The address index is exact.
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
