"""Prepared Aerodrome model and journal types bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.aerodrome import economic_state
from almanak.connectors.aerodrome.economic_book import PoolBook
from almanak.connectors.aerodrome.economic_config import EconomicConfig
from almanak.connectors.aerodrome.economic_model import EconomicModel
from almanak.connectors.aerodrome.economic_parameters import initial, parameters
from almanak.connectors.aerodrome.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.aerodrome.economic_reporting import report
from almanak.connectors.aerodrome.economic_state import State
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.state import Decision
from almanak.connectors.dex_economic_binding import EconomicBinding


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["aerodrome-economic-integer-fixed-indices-v1"] = (
        "aerodrome-economic-integer-fixed-indices-v1"
    )


class AerodromeEconomic(EconomicBinding):
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
    book_type = PoolBook
