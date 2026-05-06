"""Backward-compat re-export shim — VIB-4062.

The canonical home for these dataclasses is now
``almanak.framework.market.models``. This module re-exports them so existing
``from almanak.framework.strategies.indicator_models import RSIData`` paths
keep working. The shim is slated for removal in a future migration once the
codemod has rewritten all remaining callers; see
``docs/migration/vib-4062-marketsnapshot.md``.
"""

from __future__ import annotations

from ..market.models import (
    ADXData,
    ATRData,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    IndicatorProvider,
    MACDData,
    MAData,
    OBVData,
    RSIData,
    StochasticData,
)

__all__ = [
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
    "IndicatorProvider",
]
