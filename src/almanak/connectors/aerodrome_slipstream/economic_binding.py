"""Prepared V3 model and journal types bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.aerodrome_slipstream import economic_state
from almanak.connectors.aerodrome_slipstream.economic_book import PoolBook
from almanak.connectors.aerodrome_slipstream.economic_config import EconomicConfig
from almanak.connectors.aerodrome_slipstream.economic_model import EconomicModel
from almanak.connectors.aerodrome_slipstream.economic_parameters import initial, parameters
from almanak.connectors.aerodrome_slipstream.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.aerodrome_slipstream.economic_reporting import valued_report
from almanak.connectors.aerodrome_slipstream.economic_state import State
from almanak.connectors.aerodrome_slipstream.models import Parameters
from almanak.connectors.aerodrome_slipstream.state import Decision
from almanak.connectors.dex_economic_binding import EconomicBinding


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["slipstream-economic-integer-fixed-growth-v1"] = (
        "slipstream-economic-integer-fixed-growth-v1"
    )


class SlipstreamEconomic(EconomicBinding):
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
