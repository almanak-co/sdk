"""Prepared V3 model and journal types bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.dex_economic_binding import EconomicBinding
from almanak.connectors.uniswap_v3 import economic_state
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_config import EconomicConfig
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_parameters import initial, parameters
from almanak.connectors.uniswap_v3.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.uniswap_v3.economic_reporting import valued_report
from almanak.connectors.uniswap_v3.economic_state import State
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.connectors.uniswap_v3.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["uniswap-v3-economic-integer-fixed-growth-v1"] = (
        "uniswap-v3-economic-integer-fixed-growth-v1"
    )


class UniswapEconomic(EconomicBinding):
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
    report = staticmethod(valued_report)

    state_types = economic_state
    parameter_type = Parameters
    book_type = PoolBook
