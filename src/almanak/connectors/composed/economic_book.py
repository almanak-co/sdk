"""A frozen descriptor binds existing native datasets to one composed run."""

import hashlib
import json
from contextlib import ExitStack
from typing import Annotated, Literal

from pydantic import Field

from almanak.backtest.dataset import file_digest
from almanak.connectors.aave_v3.economic_book import Book as LendingBook
from almanak.connectors.aerodrome.economic_book import PoolBook as AerodromeBook
from almanak.connectors.aerodrome_slipstream.economic_book import PoolBook as SlipstreamBook
from almanak.connectors.composed.aave_v4_economic import Book as AaveV4Book
from almanak.connectors.composed.economic_prices import Prices
from almanak.connectors.compound_v3.economic_book import Book as CompoundBook
from almanak.connectors.curve.economic_book import PoolBook as CurveBook
from almanak.connectors.gmx_v2.economic_book import MarketBook as GmxBook
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.economic_book import Book as MorphoBook
from almanak.connectors.pancakeswap_v3.economic_book import PoolBook as PancakeBook
from almanak.connectors.sparklend.economic_book import Book as SparkBook
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_book import PoolBook as V4Book
from almanak.models import Model

MODEL: Literal["composed-native-economic-v1"] = "composed-native-economic-v1"
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Descriptor(Model):
    model: Literal["composed-native-economic-v1"] = MODEL
    policy_digest: Digest
    components: dict[ConnectorId, Digest]
    prices: Digest | None = Field(default=None, exclude_if=lambda value: value is None)


def identity(config, settings):
    value = {
        "model": MODEL,
        "config": config.model_dump(mode="json"),
        "settings": settings.model_dump(mode="json"),
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class Book:
    def __init__(self, directory, config, settings, digest=None):
        path = directory / "dataset.json"
        if digest is not None and file_digest(path) != digest:
            raise ValueError("composed dataset descriptor changed")
        self.manifest = Descriptor.model_validate_json(path.read_text())
        if self.manifest.policy_digest != identity(config, settings):
            raise ValueError("composed dataset differs from the frozen configuration and settings")
        if set(self.manifest.components) != set(config.parameter_components):
            raise ValueError("composed dataset must cover every configured connector exactly")
        factories: dict[
            ConnectorId,
            type[LendingBook]
            | type[AaveV4Book]
            | type[SparkBook]
            | type[PoolBook]
            | type[PancakeBook]
            | type[AerodromeBook]
            | type[SlipstreamBook]
            | type[V4Book]
            | type[MorphoBook]
            | type[CompoundBook]
            | type[CurveBook]
            | type[GmxBook],
        ] = {
            ConnectorId("aave_v3"): LendingBook,
            ConnectorId("aave_v4"): AaveV4Book,
            ConnectorId("sparklend"): SparkBook,
            ConnectorId("uniswap_v3"): PoolBook,
            ConnectorId("pancakeswap_v3"): PancakeBook,
            ConnectorId("aerodrome"): AerodromeBook,
            ConnectorId("aerodrome_slipstream"): SlipstreamBook,
            ConnectorId("uniswap_v4"): V4Book,
            ConnectorId("morpho_blue"): MorphoBook,
            ConnectorId("compound_v3"): CompoundBook,
            ConnectorId("curve"): CurveBook,
            ConnectorId("gmx_v2"): GmxBook,
        }
        self.components: dict[
            ConnectorId,
            LendingBook
            | AaveV4Book
            | SparkBook
            | PoolBook
            | PancakeBook
            | AerodromeBook
            | SlipstreamBook
            | V4Book
            | MorphoBook
            | CompoundBook
            | CurveBook
            | GmxBook,
        ] = {}
        self._stack = ExitStack()
        try:
            self.prices = Prices(
                directory, self.manifest.policy_digest, settings, self.manifest.prices
            )
            self._stack.callback(self.prices.close)
            for connector, parameters in config.parameter_components.items():
                native = factories[connector](
                    directory / connector.value,
                    parameters,
                    settings.component(connector, parameters),
                    self.manifest.components[connector],
                )
                self._stack.callback(native.close)
                self.components[connector] = native
        except BaseException:
            self.close()
            raise

    def close(self):
        self._stack.close()
