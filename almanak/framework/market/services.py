"""Provider Protocol layer for VIB-4062 MarketSnapshot.

PRD §4.3 — every builder factory normalizes async data-layer providers into
the canonical *sync* service Protocols at the builder boundary.
``MarketSnapshot`` itself never sees an async provider, never decides whether
to ``await`` or not, never re-implements the sync bridge.

Production sync adapters wrap the async data-layer providers (under
``framework/data``); the wrap centralizes the sync bridge logic in
``framework/market/sync_bridge.py``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .models import (
        ADXData,
        ATRData,
        BollingerBandsData,
        CCIData,
        IchimokuData,
        MACDData,
        MAData,
        OBVData,
        PriceData,
        RSIData,
        StochasticData,
        TokenBalance,
    )


class PriceService(Protocol):
    def price(self, token: str, quote: str, chain: str) -> Decimal: ...
    def price_full(self, token: str, quote: str, chain: str) -> PriceData: ...


class BalanceService(Protocol):
    def balance(
        self,
        token: str,
        chain: str,
        wallet_address: str,
        protocol: str | None = None,
    ) -> TokenBalance: ...


class IndicatorService(Protocol):
    def rsi(self, token: str, period: int, timeframe: str, chain: str) -> RSIData: ...
    def sma(self, token: str, period: int, timeframe: str, chain: str) -> MAData: ...
    def ema(self, token: str, period: int, timeframe: str, chain: str) -> MAData: ...
    def macd(self, token: str, timeframe: str, chain: str) -> MACDData: ...
    def bollinger_bands(self, token: str, timeframe: str, chain: str) -> BollingerBandsData: ...
    def atr(self, token: str, period: int, timeframe: str, chain: str) -> ATRData: ...
    def stochastic(self, token: str, timeframe: str, chain: str) -> StochasticData: ...
    def adx(self, token: str, timeframe: str, chain: str) -> ADXData: ...
    def cci(self, token: str, timeframe: str, chain: str) -> CCIData: ...
    def obv(self, token: str, timeframe: str, chain: str) -> OBVData: ...
    def ichimoku(self, token: str, timeframe: str, chain: str) -> IchimokuData: ...


class RateService(Protocol):
    def lending_rate(self, protocol: str, token: str, side: str, chain: str, market_id: str | None = None) -> Any: ...


class FundingService(Protocol):
    def funding_rate(self, venue: str, market: str) -> Any: ...


class PoolService(Protocol):
    def pool_price(self, pool_address: str, chain: str) -> Any: ...
    def pool_reserves(self, pool_address: str, chain: str) -> Any: ...


class PredictionService(Protocol):
    def prediction(self, market_id: str) -> Any: ...


class RiskService(Protocol):
    def portfolio_risk(self) -> Any: ...


class OHLCVService(Protocol):
    def ohlcv(self, token: str, timeframe: str, chain: str) -> Any: ...
