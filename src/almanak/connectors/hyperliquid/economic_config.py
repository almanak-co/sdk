"""HyperCore backtest settings: the venue's own `/info` history under D4 (blueprint 30, 12).

The data policy names the trade candle interval the model reads and how stale a carried
candle may be. The fill model is the author's statement of what the missing book would
have cost: an `Ioc` order fills at the latest trade close moved against it by
`slippage_bps`, and pays `taker_fee_bps` (HyperCore's base tier is 4.5).
"""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.gateway.hypercore_info import CANDLE_SECONDS, CandleInterval
from almanak.models import Model
from almanak.triggers import PositiveSeconds

Bps = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]+)?$")]
BASE_TAKER_FEE_BPS = "4.5"


class FillModel(Model):
    slippage_bps: Bps
    taker_fee_bps: Bps = BASE_TAKER_FEE_BPS

    @model_validator(mode="after")
    def bounds(self):
        if Decimal(self.slippage_bps) >= 1000 or Decimal(self.taker_fee_bps) >= 100:
            raise ValueError("HyperCore fill costs are basis points of notional")
        return self


class DataPolicy(Model):
    source: Literal["hyperliquid_info"]
    candles: CandleInterval
    max_age_seconds: PositiveSeconds
    fills: FillModel

    @property
    def candle_seconds(self) -> int:
        return CANDLE_SECONDS[self.candles]

    @model_validator(mode="after")
    def age(self):
        # A candle is available when it closes and is carried until the next one closes.
        if self.max_age_seconds < 2 * self.candle_seconds:
            raise ValueError("max_age_seconds must cover two candles of the declared interval")
        return self


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    position_model: Literal["hypercore_trade_candles"]
    costs: ExcludedCosts
    data: DataPolicy
