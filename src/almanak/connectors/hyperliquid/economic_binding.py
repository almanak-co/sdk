"""Bind the HyperCore backtest model and its prepared `/info` history to the shared runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.dex_economic_binding import EconomicBinding
from almanak.connectors.hyperliquid import economic_state
from almanak.connectors.hyperliquid.economic_book import HyperCoreBook
from almanak.connectors.hyperliquid.economic_config import EconomicConfig
from almanak.connectors.hyperliquid.economic_model import EconomicModel
from almanak.connectors.hyperliquid.economic_parameters import initial, parameters
from almanak.connectors.hyperliquid.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.hyperliquid.economic_reporting import report
from almanak.connectors.hyperliquid.economic_state import Decision, State
from almanak.connectors.hyperliquid.models import Parameters


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["hypercore-trade-candle-perpetual-v1"] = "hypercore-trade-candle-perpetual-v1"


class HyperliquidEconomic(EconomicBinding):
    settings_type = EconomicConfig
    decision_type = Decision
    state_type = State
    manifest_type = Manifest
    driver = EconomicModel
    initial = staticmethod(initial)
    parameters = staticmethod(parameters)
    prepare = staticmethod(prepare)
    dataset_directory = staticmethod(dataset_directory)
    validate_sources = staticmethod(validate_sources)
    report = staticmethod(report)
    state_types = economic_state
    parameter_type = Parameters
    book_type = HyperCoreBook
