"""Bind hypothetical GMX escrow and prepared publications to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.dex_economic_binding import EconomicBinding
from almanak.connectors.gmx_v2 import economic_state
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_config import EconomicConfig
from almanak.connectors.gmx_v2.economic_model import EconomicModel
from almanak.connectors.gmx_v2.economic_parameters import initial, parameters
from almanak.connectors.gmx_v2.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.gmx_v2.economic_reporting import report
from almanak.connectors.gmx_v2.economic_state import State
from almanak.connectors.gmx_v2.models import Parameters
from almanak.connectors.gmx_v2.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal[
        "gmx-v2-published-opportunity-integer-swap-v1",
        "gmx-v2-published-opportunity-integer-gm-v1",
        "gmx-v2-published-opportunity-integer-perpetual-v1",
    ] = "gmx-v2-published-opportunity-integer-swap-v1"


class GmxEconomic(EconomicBinding):
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
    book_type = MarketBook
