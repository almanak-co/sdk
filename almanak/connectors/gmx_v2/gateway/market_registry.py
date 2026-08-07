"""Gateway-owned GMX V2 market discovery and identity verification (VIB-6561)."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors._base.gateway_capabilities import (
    PerpMarketCatalogueUnavailable,
    PerpMarketRecord,
    PerpMarketVerificationError,
)
from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

GMX_API_BASE_URLS = {
    "arbitrum": "https://arbitrum-api.gmxinfra.io",
    "avalanche": "https://avalanche-api.gmxinfra.io",
}
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_ZERO_ADDRESS = "0x" + "0" * 40
_GET_MARKET_SELECTOR = keccak(text="getMarket(address,address)")[:4]
_CACHE_TTL_SECONDS = 60.0
_REQUEST_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True)
class _ApiMarket:
    name: str
    label: str
    market_token: str
    index_token: str
    long_token: str
    short_token: str


def _normalise_label(value: str) -> str:
    candidate = value.strip().upper().replace("-", "/").replace("_", "/").replace(":", "/")
    return candidate if "/" in candidate else f"{candidate}/USD"


def _address(value: Any, field: str) -> str:
    candidate = str(value or "").strip()
    if not _ADDRESS_RE.fullmatch(candidate):
        raise ValueError(f"GMX API returned invalid {field}: {candidate!r}")
    return candidate.lower()


class GmxV2MarketRegistry:
    """Resolve one GMX API market and prove its address tuple on-chain.

    The official API is a discovery source, not an identity authority. Every
    returned record is compared byte-for-byte with ``Reader.getMarket`` before
    it is cached or exposed to a strategy.
    """

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str], tuple[float, PerpMarketRecord]] = {}
        self._catalog_cache: dict[str, tuple[float, Any, Any]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Last-known-good verified records, keyed by market-token address, with
        # NO TTL. ``Reader.getMarket`` is a pure DataStore read — the tuple
        # behind an address cannot change — so a verified record only ever goes
        # stale in its API-derived listing status, never in its identity. This
        # is what keeps the CLOSE path (address-first, ``allow_delisted_address``)
        # off the venue API's uptime: an API outage must not strand a position
        # this process already verified.
        self._verified_history: dict[tuple[str, str], PerpMarketRecord] = {}

    async def resolve(
        self,
        *,
        chain: str,
        market: str,
        eth_call: Any,
        allow_delisted_address: bool = True,
        allow_index_equivalent: bool = False,
    ) -> PerpMarketRecord | None:
        chain_key = chain.strip().lower()
        if chain_key not in GMX_API_BASE_URLS:
            raise ValueError(f"GMX V2 market discovery is unsupported on chain {chain!r}")
        query = market.strip()
        if not query:
            raise ValueError("market is required")
        cache_scope = f"{int(allow_delisted_address)}:{int(allow_index_equivalent)}"
        cache_key = (chain_key, f"{cache_scope}:{query.lower()}")
        cached = self._cached_record(cache_key)
        if cached is not None:
            return cached

        lock = self._locks.setdefault(chain_key, asyncio.Lock())
        async with lock:
            cached = self._cached_record(cache_key)
            if cached is not None:
                return cached
            try:
                markets_payload, tokens_payload = await self._catalogue(chain_key)
            except Exception as exc:
                stale = self._stale_verified_record(chain_key, query, allow_delisted_address, exc)
                if stale is not None:
                    return stale
                raise
            candidates = self._matching_markets(
                markets_payload,
                query,
                allow_delisted_address=allow_delisted_address,
            )
            if not candidates:
                return None
            if len(candidates) > 1 and not allow_index_equivalent:
                raise self._ambiguous(query, candidates)

            verified_candidates = [
                (candidate, await self._verify_and_build(chain_key, candidate, tokens_payload, eth_call))
                for candidate in candidates
            ]
            records = [record for _, record in verified_candidates]
            self._require_single_index_identity(query, records)
            # Price history is index-scoped, not collateral-market-scoped. If
            # every matching market was independently verified on-chain and all
            # carry one index identity, any member names the same candle plane.
            # Pick by address so API row order cannot affect replay provenance.
            selected_candidate, record = min(
                verified_candidates,
                key=lambda item: item[1].market_token.lower(),
            )
            self._remember_verified(chain_key, cache_scope, query, selected_candidate, record)
            return record

    def _cached_record(self, cache_key: tuple[str, str]) -> PerpMarketRecord | None:
        """Return the unexpired verified record for ``cache_key``, if any."""
        cached = self._cache.get(cache_key)
        if cached is not None and cached[0] > time.monotonic():
            return cached[1]
        return None

    async def _catalogue(self, chain_key: str) -> tuple[Any, Any]:
        """Return the TTL-cached raw ``(markets_payload, tokens_payload)``."""
        catalog = self._catalog_cache.get(chain_key)
        if catalog is not None and catalog[0] > time.monotonic():
            return catalog[1], catalog[2]
        # Cache the raw venue catalogue as well as verified records. Both
        # endpoints enumerate the whole chain, so re-downloading them for
        # each distinct market would scale compile traffic with strategy
        # breadth. The short TTL bounds listing/delisting staleness.
        markets_payload, tokens_payload = await asyncio.gather(
            self._get_json(chain_key, "/markets"),
            self._get_json(chain_key, "/tokens"),
        )
        self._catalog_cache[chain_key] = (
            time.monotonic() + _CACHE_TTL_SECONDS,
            markets_payload,
            tokens_payload,
        )
        return markets_payload, tokens_payload

    def _stale_verified_record(
        self,
        chain_key: str,
        query: str,
        allow_delisted_address: bool,
        exc: Exception,
    ) -> PerpMarketRecord | None:
        # Serve-stale: an ADDRESS query this registry has verified
        # before stays answerable through a catalogue outage — the
        # address names an immutable tuple. Label queries get no
        # such grace (labels are catalogue vocabulary), and neither
        # do ``allow_delisted_address=False`` callers: the tuple is
        # immutable but the LISTING status is not, and a stale
        # record cannot prove the market is still listed — the
        # listing-sensitive caller asked for exactly that proof.
        stale = self._verified_history.get((chain_key, query.lower())) if allow_delisted_address else None
        if stale is not None:
            logger.warning(
                "GMX catalogue unavailable on %s; serving last verified record for %s: %s",
                chain_key,
                query,
                exc,
            )
        return stale

    @staticmethod
    def _ambiguous(query: str, candidates: list[_ApiMarket]) -> ValueError:
        """Build the fail-closed ambiguity error for a multi-candidate query."""
        choices = ", ".join(f"{item.name} ({item.market_token})" for item in candidates)
        return ValueError(f"GMX market {query!r} is ambiguous; pass the exact full name or address: {choices}")

    @staticmethod
    def _require_single_index_identity(query: str, records: list[PerpMarketRecord]) -> None:
        """Fail closed when verified records span distinct index-price identities."""
        if len(records) > 1:
            price_identities = {
                (record.index_token.lower(), record.index_symbol.upper(), record.index_token_decimals)
                for record in records
            }
            if len(price_identities) != 1:
                choices = ", ".join(
                    f"{record.label} ({record.market_token}, index={record.index_token}/{record.index_symbol})"
                    for record in records
                )
                raise ValueError(f"GMX market {query!r} is ambiguous across distinct index-price identities: {choices}")

    def _remember_verified(
        self,
        chain_key: str,
        cache_scope: str,
        query: str,
        selected: _ApiMarket,
        record: PerpMarketRecord,
    ) -> None:
        """Seed the verified-record cache and the no-TTL verified history."""
        expiry = time.monotonic() + _CACHE_TTL_SECONDS
        # Never seed the non-unique short label from a full-name or address
        # lookup. Otherwise resolving one ETH collateral variant would make a
        # later ETH/USD query silently order-dependent for the cache TTL.
        for alias in (query, selected.name, selected.market_token):
            self._cache[(chain_key, f"{cache_scope}:{alias.lower()}")] = (expiry, record)
        self._verified_history[(chain_key, record.market_token.lower())] = record

    async def _get_json(self, chain: str, path: str) -> Any:
        """Fetch one catalogue endpoint with explicit connection ownership.

        The connector registry has no async shutdown hook, so retaining a
        long-lived ``ClientSession`` here would leak lifecycle ownership. The
        chain-wide raw catalogue cache limits this to two short-lived sessions
        per TTL refresh instead of two sessions per market compile.
        """
        url = f"{GMX_API_BASE_URLS[chain]}{path}"
        timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(ssl=build_ssl_context())
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers={"Accept": "application/json"}, timeout=timeout) as response:
                    response.raise_for_status()
                    return await response.json()
        except Exception as exc:
            raise RuntimeError(f"GMX metadata request failed for {chain}{path}") from exc

    @staticmethod
    def _matching_markets(
        payload: Any,
        query: str,
        *,
        allow_delisted_address: bool = True,
    ) -> list[_ApiMarket]:
        rows = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ValueError("GMX /markets response does not contain a markets list")
        query_is_address = bool(_ADDRESS_RE.fullmatch(query))
        normalized = _normalise_label(query) if not query_is_address else ""
        matches: list[_ApiMarket] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            # Risk-reducing closes can address a delisted market by its exact
            # market token. Label discovery excludes delisted rows so new opens
            # cannot select them. The exact tuple is still verified on-chain.
            if raw.get("isListed") is False and (not query_is_address or not allow_delisted_address):
                continue
            name = str(raw.get("name") or "").strip()
            label = _normalise_label(name.split("[", 1)[0].strip()) if name else ""
            candidate = _ApiMarket(
                name=name,
                label=label,
                market_token=_address(raw.get("marketToken"), "marketToken"),
                index_token=_address(raw.get("indexToken"), "indexToken"),
                long_token=_address(raw.get("longToken"), "longToken"),
                short_token=_address(raw.get("shortToken"), "shortToken"),
            )
            if candidate.index_token == _ZERO_ADDRESS:
                continue
            if query_is_address:
                selected = candidate.market_token == query.lower()
            else:
                selected = query.strip().upper() == name.upper() or normalized == label
            if selected:
                matches.append(candidate)
        return matches

    @classmethod
    def _select_market(
        cls,
        payload: Any,
        query: str,
        *,
        allow_delisted_address: bool = True,
    ) -> _ApiMarket | None:
        """Resolve exactly one execution market; ambiguous labels fail closed."""
        matches = cls._matching_markets(
            payload,
            query,
            allow_delisted_address=allow_delisted_address,
        )
        if not matches:
            return None
        if len(matches) > 1:
            raise cls._ambiguous(query, matches)
        return matches[0]

    async def _verify_and_build(
        self,
        chain: str,
        candidate: _ApiMarket,
        tokens_payload: Any,
        eth_call: Any,
    ) -> PerpMarketRecord:
        addresses = GMX_V2[chain]
        calldata = (
            "0x"
            + (
                _GET_MARKET_SELECTOR + encode(["address", "address"], [addresses["data_store"], candidate.market_token])
            ).hex()
        )
        raw_hex = await eth_call(addresses["reader"], calldata)
        try:
            decoded = decode(["(address,address,address,address)"], bytes.fromhex(raw_hex.removeprefix("0x")))[0]
        except Exception as exc:
            raise PerpMarketVerificationError(
                f"Reader.getMarket returned an invalid payload for {candidate.market_token} on {chain}"
            ) from exc
        actual = tuple(str(value).lower() for value in decoded)
        expected = (
            candidate.market_token,
            candidate.index_token,
            candidate.long_token,
            candidate.short_token,
        )
        if actual != expected:
            raise PerpMarketVerificationError(
                f"GMX API/on-chain market mismatch for {candidate.name} on {chain}: expected={expected!r} actual={actual!r}"
            )

        rows = tokens_payload.get("tokens") if isinstance(tokens_payload, dict) else None
        if not isinstance(rows, list):
            raise PerpMarketCatalogueUnavailable("GMX /tokens response does not contain a tokens list")
        tokens: dict[str, tuple[str, int]] = {}
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            try:
                token_address = _address(raw.get("address"), "token address")
                decimals = int(raw["decimals"])
            except (KeyError, TypeError, ValueError):
                continue
            if decimals < 0 or decimals > 30:
                continue
            symbol = str(raw.get("symbol") or "").strip()
            tokens[token_address] = (symbol, decimals)

        index_meta = tokens.get(candidate.index_token)
        if index_meta is None or not index_meta[0]:
            raise PerpMarketCatalogueUnavailable(
                f"GMX /tokens has no index metadata for {candidate.index_token} on {chain}"
            )
        long_meta = tokens.get(candidate.long_token, ("", 0))
        short_meta = tokens.get(candidate.short_token, ("", 0))
        return PerpMarketRecord(
            protocol="gmx_v2",
            chain=chain,
            label=candidate.label,
            market_token=candidate.market_token,
            index_token=candidate.index_token,
            index_symbol=index_meta[0],
            index_token_decimals=index_meta[1],
            long_token=candidate.long_token,
            long_token_symbol=long_meta[0],
            short_token=candidate.short_token,
            short_token_symbol=short_meta[0],
            verified=True,
        )


__all__ = ["GMX_API_BASE_URLS", "GmxV2MarketRegistry"]
