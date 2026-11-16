"""Named markets carry their connector discriminator alongside the exact target."""

import json
from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from almanak.configuration.selection import require_connector
from almanak.connectors.aave_v3.domain import Reserve
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aerodrome.domain import AerodromePool
from almanak.connectors.aerodrome.domain import Contracts as AerodromeContracts
from almanak.connectors.aerodrome_slipstream.domain import Contracts as SlipstreamContracts
from almanak.connectors.aerodrome_slipstream.domain import SlipstreamPool
from almanak.connectors.compound_v3.domain import Reserve as CompoundReserve
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.gmx_v2.domain import Market as GmxPositionMarket
from almanak.connectors.gmx_v2.exchange_domain import ExchangeMarket as GmxExchangeMarket
from almanak.connectors.morpho_blue.domain import Reserve as MorphoReserve
from almanak.connectors.pancakeswap_v3.domain import Contracts as PancakeContracts
from almanak.connectors.pancakeswap_v3.domain import PancakePool
from almanak.connectors.sparklend.domain import Reserve as SparkReserve
from almanak.connectors.uniswap_v3.domain import Contracts, UniswapPool
from almanak.connectors.uniswap_v4.domain import UniswapPool as V4Pool
from almanak.connectors.uniswap_v4.models import Deployment as V4Contracts
from almanak.models import Model


class AaveV3Market(Model):
    connector: Literal["aave_v3"]
    reserve: Reserve

    def allocation_target(self):
        return self.reserve.model_dump(mode="json")


class AaveV4Market(Model):
    connector: Literal["aave_v4"]
    reserve: AaveReserve

    def allocation_target(self):
        return self.reserve.model_dump(mode="json")


class SparkMarket(Model):
    connector: Literal["sparklend"]
    reserve: SparkReserve

    def allocation_target(self):
        return self.reserve.model_dump(mode="json")


class UniswapV3Market(Model):
    connector: Literal["uniswap_v3"]
    pool: UniswapPool
    contracts: Contracts

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class PancakeMarket(Model):
    connector: Literal["pancakeswap_v3"]
    pool: PancakePool
    contracts: PancakeContracts

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class AerodromeMarket(Model):
    connector: Literal["aerodrome"]
    pool: AerodromePool
    contracts: AerodromeContracts

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class SlipstreamMarket(Model):
    connector: Literal["aerodrome_slipstream"]
    pool: SlipstreamPool
    contracts: SlipstreamContracts

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class UniswapV4Market(Model):
    connector: Literal["uniswap_v4"]
    pool: V4Pool
    contracts: V4Contracts

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class MorphoMarket(Model):
    connector: Literal["morpho_blue"]
    reserve: MorphoReserve

    def allocation_target(self):
        return self.reserve.model_dump(mode="json")


class CompoundMarket(Model):
    connector: Literal["compound_v3"]
    reserve: CompoundReserve

    def allocation_target(self):
        return self.reserve.model_dump(mode="json")


class CurveMarket(Model):
    connector: Literal["curve"]
    pool: CurvePool

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


class GmxMarket(Model):
    connector: Literal["gmx_v2"]
    market: GmxExchangeMarket | GmxPositionMarket

    def allocation_target(self):
        return self.model_dump(mode="json", exclude={"connector"})


Market = Annotated[
    AaveV3Market
    | AaveV4Market
    | SparkMarket
    | UniswapV4Market
    | UniswapV3Market
    | PancakeMarket
    | AerodromeMarket
    | SlipstreamMarket
    | MorphoMarket
    | CompoundMarket
    | CurveMarket
    | GmxMarket,
    Field(discriminator="connector"),
]
MARKET: TypeAdapter[Market] = TypeAdapter(Market)


def named_markets(raw):
    from almanak.configuration.yaml import ConfigurationError

    if not isinstance(raw, dict) or not raw:
        raise ConfigurationError("markets require an explicit named mapping", field="markets")
    result = {}
    for name, value in raw.items():
        if isinstance(value, dict):
            require_connector(value.get("connector"), field=f"markets.{name}.connector")
        try:
            result[name] = MARKET.validate_json(json.dumps(value))
        except ValueError as error:
            raise ConfigurationError(str(error), field=f"markets.{name}") from error
    return result
