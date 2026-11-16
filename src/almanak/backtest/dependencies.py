"""Finite strategy data declarations and source-preserving observations."""

from decimal import Decimal
from typing import Annotated

from pydantic import Field

from almanak.backtest.dataset import DataKey
from almanak.backtest.pricing import PriceBinding
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.models import Model


class DataDependencies(Model):
    assets: tuple[AssetType, ...] = ()
    markets: tuple[DataKey, ...] = ()
    prices: tuple[PriceBinding, ...] = ()


class PriceObservation(Model):
    binding: PriceBinding
    value: Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
    source_time: UnixTime
    available_at: UnixTime
    evidence: tuple[str, ...]


def validate_dependencies(dependencies, bindings, markets):
    declared = tuple(b for b in bindings if b.purpose == "strategy")
    if set(dependencies.prices) != set(declared):
        raise ValueError("strategy price sources differ from declared backtest bindings")
    if any(b.purpose != "strategy" for b in dependencies.prices):
        raise ValueError("strategy dependencies must identify strategy-purpose prices")
    if any(m not in markets for m in dependencies.markets):
        raise ValueError("strategy market dependency has no supported connector model")
