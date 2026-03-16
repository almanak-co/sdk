"""Solana Liquid Staking Token (LST) data provider.

Tracks exchange rates, staking APY, and metadata for Solana LSTs
(jitoSOL, mSOL, bSOL, INF) using the Sanctum extra API and Jupiter
price API as data sources.

Example:
    from almanak.framework.data.staking import SolanaLSTProvider

    provider = SolanaLSTProvider()
    rate = await provider.get_exchange_rate("jitoSOL")
    # rate.rate = 1.145 (1 jitoSOL = 1.145 SOL)
    # rate.apy = 7.82 (annualized staking yield)

    all_rates = await provider.get_all_rates()
    # Returns dict of all tracked LST rates
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import aiohttp

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta

logger = logging.getLogger(__name__)


# =============================================================================
# LST Protocol Registry
# =============================================================================


class LSTProtocol(Enum):
    """Supported Solana LST protocols."""

    JITO = "jito"
    MARINADE = "marinade"
    BLAZE = "blaze"
    SANCTUM_INF = "sanctum_inf"


# LST mint addresses on Solana mainnet
_LST_MINTS: dict[str, str] = {
    "jitoSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
    "mSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "bSOL": "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
    "INF": "5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm",
}

# LST symbol -> protocol mapping
_LST_PROTOCOL: dict[str, LSTProtocol] = {
    "jitoSOL": LSTProtocol.JITO,
    "mSOL": LSTProtocol.MARINADE,
    "bSOL": LSTProtocol.BLAZE,
    "INF": LSTProtocol.SANCTUM_INF,
}

# Known LST symbol aliases (case-insensitive lookup)
_LST_ALIASES: dict[str, str] = {
    "jitosol": "jitoSOL",
    "msol": "mSOL",
    "bsol": "bSOL",
    "inf": "INF",
    "sanctum": "INF",
    "sanctum_inf": "INF",
}

# Sanctum extra API for LST APY data
_SANCTUM_EXTRA_API = "https://extra-api.sanctum.so/v1"

# Jupiter price API for exchange rates
_JUPITER_PRICE_API = "https://lite-api.jup.ag/price/v2"

# SOL mint address (used as quote for exchange rates)
_SOL_MINT = "So11111111111111111111111111111111111111112"


# =============================================================================
# Data Models
# =============================================================================


@dataclass(frozen=True)
class LSTExchangeRate:
    """Exchange rate and yield data for a Solana LST.

    Attributes:
        symbol: LST token symbol (e.g. "jitoSOL", "mSOL").
        protocol: LST protocol enum.
        mint: Solana mint address.
        rate: Exchange rate vs SOL (e.g. 1.145 means 1 LST = 1.145 SOL).
        apy: Annualized staking yield as percentage (e.g. 7.82 for 7.82%).
            None if APY data is unavailable.
        tvl_sol: Total SOL staked in the protocol. None if unavailable.
        observed_at: Timestamp when the data was observed.
    """

    symbol: str
    protocol: LSTProtocol
    mint: str
    rate: float
    apy: float | None = None
    tvl_sol: float | None = None
    observed_at: datetime | None = None


# =============================================================================
# SolanaLSTProvider
# =============================================================================


class SolanaLSTProvider:
    """Solana LST exchange rate and yield data provider.

    Fetches exchange rates from Jupiter price API and staking APY from
    Sanctum extra API. Results are cached with configurable TTL.

    Args:
        cache_ttl: Cache TTL in seconds. Default 300 (5 minutes).
        request_timeout: HTTP request timeout in seconds. Default 10.
    """

    def __init__(
        self,
        cache_ttl: int = 300,
        request_timeout: float = 10.0,
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._cache: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()
        self._successes = 0
        self._failures = 0

    # -- Public API -----------------------------------------------------------

    async def get_exchange_rate(self, symbol: str) -> LSTExchangeRate:
        """Get exchange rate and APY for a single LST.

        Args:
            symbol: LST symbol (e.g. "jitoSOL", "mSOL", "INF").
                Case-insensitive aliases are supported.

        Returns:
            LSTExchangeRate with rate vs SOL and APY data.

        Raises:
            DataSourceUnavailable: If data cannot be fetched.
            ValueError: If the symbol is not a known LST.
        """
        canonical = self._resolve_symbol(symbol)
        rates = await self._get_all_rates_cached()
        if canonical not in rates:
            raise DataSourceUnavailable(
                source="solana_lst",
                reason=f"No rate data for {canonical}",
            )
        return rates[canonical]

    async def get_all_rates(self) -> dict[str, LSTExchangeRate]:
        """Get exchange rates and APY for all tracked LSTs.

        Returns:
            Dict mapping symbol -> LSTExchangeRate.

        Raises:
            DataSourceUnavailable: If data cannot be fetched.
        """
        return await self._get_all_rates_cached()

    async def get_exchange_rate_envelope(self, symbol: str) -> DataEnvelope[LSTExchangeRate]:
        """Get exchange rate wrapped in a DataEnvelope with provenance metadata.

        Args:
            symbol: LST symbol (case-insensitive).

        Returns:
            DataEnvelope[LSTExchangeRate] with provenance metadata.
        """
        start = time.monotonic()
        rate = await self.get_exchange_rate(symbol)
        latency_ms = int((time.monotonic() - start) * 1000)

        meta = DataMeta(
            source="sanctum_jupiter",
            observed_at=rate.observed_at or datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.9,
            cache_hit=False,
        )
        return DataEnvelope(
            value=rate,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    def get_supported_symbols(self) -> list[str]:
        """Return list of supported LST symbols."""
        return list(_LST_MINTS.keys())

    def is_lst(self, symbol: str) -> bool:
        """Check if a symbol is a known Solana LST."""
        try:
            self._resolve_symbol(symbol)
            return True
        except ValueError:
            return False

    # -- Data Fetching --------------------------------------------------------

    async def _get_all_rates_cached(self) -> dict[str, LSTExchangeRate]:
        """Get all rates with caching."""
        cache_key = "all_rates"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        rates = await self._fetch_all_rates()
        self._update_cache(cache_key, rates)
        return rates

    async def _fetch_all_rates(self) -> dict[str, LSTExchangeRate]:
        """Fetch exchange rates and APY data from APIs."""
        now = datetime.now(UTC)

        # Fetch exchange rates and APY data in parallel
        # return_exceptions=True means gather never raises; exceptions are returned as values
        exchange_rates, apy_data = await asyncio.gather(
            self._fetch_jupiter_prices(),
            self._fetch_sanctum_apy(),
            return_exceptions=True,
        )

        # Handle partial failures - exchange rates are required, APY is optional
        if isinstance(exchange_rates, BaseException):
            self._failures += 1
            raise DataSourceUnavailable(
                source="solana_lst",
                reason=f"Failed to fetch exchange rates: {exchange_rates}",
            ) from exchange_rates

        if isinstance(apy_data, BaseException):
            logger.warning("Failed to fetch LST APY data: %s", apy_data)
            apy_data = {}

        rates: dict[str, LSTExchangeRate] = {}
        for symbol, mint in _LST_MINTS.items():
            rate_value = exchange_rates.get(mint)
            if rate_value is None:
                continue

            apy = apy_data.get(symbol)

            rates[symbol] = LSTExchangeRate(
                symbol=symbol,
                protocol=_LST_PROTOCOL[symbol],
                mint=mint,
                rate=rate_value,
                apy=apy,
                observed_at=now,
            )

        if not rates:
            self._failures += 1
            raise DataSourceUnavailable(
                source="solana_lst",
                reason="Jupiter returned HTTP 200 but no usable rate data for any tracked LST",
            )

        self._successes += 1
        return rates

    async def _fetch_jupiter_prices(self) -> dict[str, float]:
        """Fetch LST/SOL exchange rates from Jupiter price API.

        Returns:
            Dict mapping mint address -> price in SOL.
        """
        mint_ids = ",".join(_LST_MINTS.values())
        url = f"{_JUPITER_PRICE_API}?ids={mint_ids}&vsToken={_SOL_MINT}"

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"}) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    text = await response.text()
                    raise DataSourceUnavailable(
                        source="jupiter_price",
                        reason=f"Jupiter price API HTTP {response.status}: {text[:200]}",
                    )
                data = await response.json()

        prices: dict[str, float] = {}
        price_data = data.get("data", {})
        for mint in _LST_MINTS.values():
            entry = price_data.get(mint)
            if entry and entry.get("price") is not None:
                try:
                    price = float(entry["price"])
                    if price > 0:
                        prices[mint] = price
                except (ValueError, TypeError):
                    continue

        return prices

    async def _fetch_sanctum_apy(self) -> dict[str, float]:
        """Fetch annualized LST APY data from Sanctum extra API.

        Uses the /apy/latest endpoint which returns actual trailing annualized
        APY rather than attempting to derive it from a single exchange rate snapshot.

        Returns:
            Dict mapping symbol -> APY as percentage (e.g. 7.82 for 7.82%).
        """
        url = f"{_SANCTUM_EXTRA_API}/apy/latest"
        params = {"lst": ",".join(_LST_MINTS.values())}

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"}) as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise DataSourceUnavailable(
                        source="sanctum_api",
                        reason=f"Sanctum API HTTP {response.status}: {text[:200]}",
                    )
                data = await response.json()

        apy_data: dict[str, float] = {}

        # Sanctum /apy/latest returns {apys: {mint: apy_as_fraction, ...}}
        # where apy_as_fraction is e.g. 0.0782 for 7.82%
        apys = data.get("apys", {})
        mint_to_symbol = {v: k for k, v in _LST_MINTS.items()}

        for mint, apy_value in apys.items():
            symbol = mint_to_symbol.get(mint)
            if symbol is None:
                continue
            try:
                apy_fraction = float(apy_value)
                # Convert fraction to percentage (0.0782 -> 7.82)
                apy_data[symbol] = apy_fraction * 100
            except (ValueError, TypeError):
                continue

        return apy_data

    # -- Helpers --------------------------------------------------------------

    def _resolve_symbol(self, symbol: str) -> str:
        """Resolve a symbol to its canonical form.

        Args:
            symbol: LST symbol (case-insensitive).

        Returns:
            Canonical symbol string.

        Raises:
            ValueError: If symbol is not a known LST.
        """
        # Direct match
        if symbol in _LST_MINTS:
            return symbol

        # Case-insensitive alias
        canonical = _LST_ALIASES.get(symbol.lower())
        if canonical is not None:
            return canonical

        raise ValueError(f"Unknown LST symbol '{symbol}'. Supported: {', '.join(_LST_MINTS.keys())}")

    def _get_cached(self, key: str) -> Any | None:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, cached_at = entry
            if time.monotonic() - cached_at > self._cache_ttl:
                return None
            return value

    def _update_cache(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = (value, time.monotonic())

    def health(self) -> dict[str, int]:
        """Return health metrics."""
        return {"successes": self._successes, "failures": self._failures}


__all__ = [
    "LSTExchangeRate",
    "LSTProtocol",
    "SolanaLSTProvider",
]
