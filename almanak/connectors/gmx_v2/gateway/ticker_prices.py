"""Gateway-owned GMX V2 venue ticker prices for synthetic indices (ALM-3177).

GMX's ``/prices/tickers`` endpoint publishes the signed oracle price bounds the
keeper settles orders against, for every token the venue prices. This module
descales those bounds to USD mids and exposes ONLY the synthetic index symbols
(``synthetic: true`` in ``/tokens``) — tokens with a deployed contract are the
address-based sources' job, and answering for them here would silently change
the aggregation vote for symbols that already price correctly.

Egress is correct here: this is the connector's gateway package (blueprint 20 /
blueprint 05 §gateway), the same egress home as the VIB-6561 market registry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any

import aiohttp

from almanak.connectors._base.gateway_capabilities import (
    PerpMarketCatalogueUnavailable,
    VenueTickerPrice,
)
from almanak.connectors.gmx_v2.gateway.market_registry import GMX_API_BASE_URLS
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

# GMX ticker prices are USD per smallest index-token unit scaled by
# 10**(30 - token_decimals) — the venue's fixed 30-decimal price plane
# (same scale contract as acceptable_price.GMX_PRICE_SCALE_EXPONENT).
_GMX_PRICE_SCALE_EXPONENT = 30

# The token catalogue (addresses / decimals / synthetic flags) is immutable per
# listing; the 60s TTL matches the VIB-6561 catalogue cache and only bounds
# listing staleness. Tickers are live oracle observations — cache them just
# long enough to batch one compile's symbol lookups into one fetch.
_TOKENS_CACHE_TTL_SECONDS = 60.0
_TICKERS_CACHE_TTL_SECONDS = 5.0
_REQUEST_TIMEOUT_SECONDS = 15.0


class GmxV2TickerPriceReader:
    """Fetch, filter, and descale the GMX ticker page for one chain.

    The whole-page cache mirrors ``GmxV2MarketRegistry``'s catalogue cache
    rationale: both endpoints enumerate the entire chain, so per-symbol
    fetching would scale gateway egress with strategy breadth.
    """

    def __init__(self) -> None:
        self._tokens_cache: dict[str, tuple[float, dict[str, tuple[str, int, bool]]]] = {}
        self._tickers_cache: dict[str, tuple[float, dict[str, VenueTickerPrice]]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def chains(self) -> frozenset[str]:
        return frozenset(GMX_API_BASE_URLS)

    async def fetch(self, *, chain: str) -> dict[str, VenueTickerPrice]:
        chain_key = chain.strip().lower()
        if chain_key not in GMX_API_BASE_URLS:
            raise ValueError(f"GMX V2 ticker prices are unsupported on chain {chain!r}")
        now = time.monotonic()
        cached = self._tickers_cache.get(chain_key)
        if cached is not None and cached[0] > now:
            return cached[1]
        lock = self._locks.setdefault(chain_key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            cached = self._tickers_cache.get(chain_key)
            if cached is not None and cached[0] > now:
                return cached[1]
            tokens = await self._synthetic_tokens(chain_key)
            payload = await self._get_json(chain_key, "/prices/tickers")
            page = self._build_page(chain_key, payload, tokens)
            self._tickers_cache[chain_key] = (time.monotonic() + _TICKERS_CACHE_TTL_SECONDS, page)
            return page

    async def _synthetic_tokens(self, chain: str) -> dict[str, tuple[str, int, bool]]:
        """Return ``lowercase address -> (symbol, decimals, synthetic)`` for a chain."""
        now = time.monotonic()
        cached = self._tokens_cache.get(chain)
        if cached is not None and cached[0] > now:
            return cached[1]
        payload = await self._get_json(chain, "/tokens")
        rows = payload.get("tokens") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise PerpMarketCatalogueUnavailable("GMX /tokens response does not contain a tokens list")
        tokens: dict[str, tuple[str, int, bool]] = {}
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            address = str(raw.get("address") or "").strip().lower()
            symbol = str(raw.get("symbol") or "").strip()
            try:
                decimals = int(raw["decimals"])
            except (KeyError, TypeError, ValueError):
                continue
            if not address.startswith("0x") or not symbol or decimals < 0 or decimals > 30:
                continue
            # Absent means "not synthetic" (the venue omits the flag on deployed
            # tokens), but a non-boolean value is a malformed row, never a guess:
            # bool("false") is True, which would put a deployed token's venue
            # price into the aggregation vote.
            synthetic = raw.get("synthetic", False)
            if not isinstance(synthetic, bool):
                continue
            tokens[address] = (symbol, decimals, synthetic)
        self._tokens_cache[chain] = (time.monotonic() + _TOKENS_CACHE_TTL_SECONDS, tokens)
        return tokens

    def _build_page(
        self,
        chain: str,
        payload: Any,
        tokens: dict[str, tuple[str, int, bool]],
    ) -> dict[str, VenueTickerPrice]:
        if not isinstance(payload, list):
            raise PerpMarketCatalogueUnavailable("GMX /prices/tickers response is not a list")
        page: dict[str, VenueTickerPrice] = {}
        for raw in payload:
            if not isinstance(raw, dict):
                continue
            address = str(raw.get("tokenAddress") or "").strip().lower()
            meta = tokens.get(address)
            if meta is None:
                continue
            symbol, decimals, synthetic = meta
            # Scope contract (GatewayVenueTickerPriceCapability): synthetic
            # indices only. A deployed token's price is the address-based
            # sources' vote, not this venue's.
            if not synthetic:
                continue
            price = self._descale_mid(raw, decimals)
            if price is None:
                logger.debug("GMX ticker row for %s on %s is malformed; skipping", symbol, chain)
                continue
            try:
                updated_at = int(raw.get("timestamp") or 0)
            except (TypeError, ValueError):
                continue
            if updated_at <= 0:
                continue
            page[symbol.upper()] = VenueTickerPrice(
                symbol=symbol.upper(),
                price_usd=price,
                updated_at=updated_at,
            )
        return page

    @staticmethod
    def _descale_mid(raw: dict[str, Any], decimals: int) -> Decimal | None:
        """Mid of the signed oracle bounds in USD per whole token, or ``None``.

        A malformed / non-positive / inverted bound pair is a MISS for that
        symbol (Empty≠Zero) — one bad row must never poison the page.
        """
        try:
            min_price = int(raw["minPrice"])
            max_price = int(raw["maxPrice"])
        except (KeyError, TypeError, ValueError):
            return None
        if min_price <= 0 or max_price <= 0 or min_price > max_price:
            return None
        scale = Decimal(10) ** (_GMX_PRICE_SCALE_EXPONENT - decimals)
        mid = (Decimal(min_price) + Decimal(max_price)) / 2 / scale
        if not mid.is_finite() or mid <= 0:
            return None
        return mid

    async def _get_json(self, chain: str, path: str) -> Any:
        """Fetch one endpoint with explicit connection ownership.

        Short-lived sessions for the same lifecycle reason as
        ``GmxV2MarketRegistry._get_json``: the connector registry has no async
        shutdown hook, and the page caches bound refresh traffic.
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
            raise PerpMarketCatalogueUnavailable(f"GMX ticker request failed for {chain}{path}") from exc


__all__ = ["GmxV2TickerPriceReader"]
