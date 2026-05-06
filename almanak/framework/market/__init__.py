"""Canonical MarketSnapshot package — VIB-4062.

The single source of truth for the strategy-facing market-data interface. All
strategies and runtime surfaces consume ``MarketSnapshot`` through this
package; legacy import paths under ``almanak.framework.strategies`` and
``almanak.framework.data.market_snapshot`` are removed in commit 6 of the
VIB-4062 migration.

Public surface:

* ``MarketSnapshot`` — the only concrete snapshot class.
* ``MarketSnapshotBuilder`` — builder with factories per runtime surface.
* ``MultiChainMarketSnapshot`` — type alias for ``MarketSnapshot`` (see PRD §4.2).
* Typed errors — ``MarketSnapshotError``, ``ChainNotConfiguredError``,
  ``AmbiguousChainError``, ``PriceUnavailableError``, …
* Typed return models — ``TokenBalance``, ``PriceData``, ``RSIData``,
  ``MACDData``, …
"""

from __future__ import annotations

# Lean-import budget (PRD §4.8): no pandas/numpy/web3/streamlit at module level.
from typing import TYPE_CHECKING, TypeAlias

from .errors import (
    AmbiguousChainError,
    BalanceUnavailableError,
    ChainNotConfiguredError,
    DexQuoteUnavailableError,
    FundingRateHistoryUnavailableError,
    FundingRateUnavailableError,
    GasUnavailableError,
    HealthUnavailableError,
    ILExposureUnavailableError,
    IndicatorUnavailableError,
    LendingRateHistoryUnavailableError,
    LendingRateUnavailableError,
    LiquidityDepthUnavailableError,
    LSTDataUnavailableError,
    MarketSnapshotError,
    OHLCVUnavailableError,
    PoolAnalyticsUnavailableError,
    PoolHistoryUnavailableError,
    PoolPriceUnavailableError,
    PoolReservesUnavailableError,
    PortfolioRiskUnavailableError,
    PredictionMarketNotFoundError,
    PredictionUnavailableError,
    PriceUnavailableError,
    RollingSharpeUnavailableError,
    RSIUnavailableError,
    SlippageEstimateUnavailableError,
    StaleDataError,
    VolatilityUnavailableError,
    VolConeUnavailableError,
    YieldOpportunitiesUnavailableError,
)
from .models import (
    ADXData,
    ATRData,
    BalanceProvider,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    IndicatorProvider,
    MACDData,
    MAData,
    OBVData,
    PriceData,
    PriceOracle,
    RSIData,
    RSIProvider,
    StochasticData,
    TokenBalance,
)
from .snapshot import MarketSnapshot

# CodeRabbit (2026-05-06): keep ``FreshnessConfig`` and ``StablecoinConfig``
# lazy. Importing them here would resolve ``models.__getattr__()`` at package
# load and pull in ``almanak.framework.data.market_snapshot`` (pandas / pyarrow
# / numpy), violating the lean-import budget.
if TYPE_CHECKING:
    from .models import FreshnessConfig, StablecoinConfig  # noqa: F401

if TYPE_CHECKING:
    from .builders import MarketSnapshotBuilder

# MultiChainMarketSnapshot is a TypeAlias to MarketSnapshot (PRD §4.2).
MultiChainMarketSnapshot: TypeAlias = MarketSnapshot  # noqa: UP040


def __getattr__(name: str):
    # Lazy-load heavy / transitively-heavy attributes so the import cost of
    # ``from almanak.framework.market import MarketSnapshot`` stays minimal
    # (PRD §4.8).
    if name == "MarketSnapshotBuilder":
        from .builders import MarketSnapshotBuilder

        return MarketSnapshotBuilder
    if name in ("FreshnessConfig", "StablecoinConfig"):
        # These resolve through ``models.__getattr__`` which pulls in
        # ``almanak.framework.data.market_snapshot`` (pandas / pyarrow / numpy);
        # only do that on actual access.
        from .models import FreshnessConfig, StablecoinConfig

        return {"FreshnessConfig": FreshnessConfig, "StablecoinConfig": StablecoinConfig}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "MarketSnapshot",
    "MultiChainMarketSnapshot",
    "MarketSnapshotBuilder",
    # Errors
    "MarketSnapshotError",
    "ChainNotConfiguredError",
    "AmbiguousChainError",
    "StaleDataError",
    "PriceUnavailableError",
    "BalanceUnavailableError",
    "RSIUnavailableError",
    "OHLCVUnavailableError",
    "GasUnavailableError",
    "PoolPriceUnavailableError",
    "PoolReservesUnavailableError",
    "PoolHistoryUnavailableError",
    "HealthUnavailableError",
    "LendingRateUnavailableError",
    "FundingRateUnavailableError",
    "DexQuoteUnavailableError",
    "ILExposureUnavailableError",
    "PredictionUnavailableError",
    "PredictionMarketNotFoundError",
    "LendingRateHistoryUnavailableError",
    "FundingRateHistoryUnavailableError",
    "LiquidityDepthUnavailableError",
    "SlippageEstimateUnavailableError",
    "VolatilityUnavailableError",
    "VolConeUnavailableError",
    "PortfolioRiskUnavailableError",
    "RollingSharpeUnavailableError",
    "PoolAnalyticsUnavailableError",
    "YieldOpportunitiesUnavailableError",
    "LSTDataUnavailableError",
    "IndicatorUnavailableError",
    # Models
    "TokenBalance",
    "PriceData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
    "IndicatorProvider",
    "RSIData",
    "MACDData",
    "BollingerBandsData",
    "StochasticData",
    "ATRData",
    "MAData",
    "ADXData",
    "OBVData",
    "CCIData",
    "IchimokuData",
    "StablecoinConfig",
    "FreshnessConfig",
]
