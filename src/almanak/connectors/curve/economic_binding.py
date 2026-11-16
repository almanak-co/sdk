"""Prepared Curve model and journal types bound to the shared offline runner."""

from typing import Literal

from pydantic import model_validator

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.curve import economic_data, economic_state
from almanak.connectors.curve.economic_book import PoolBook
from almanak.connectors.curve.economic_config import EconomicConfig
from almanak.connectors.curve.economic_model import EconomicModel
from almanak.connectors.curve.economic_parameters import initial, parameters
from almanak.connectors.curve.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.curve.economic_reporting import report
from almanak.connectors.curve.economic_state import State
from almanak.connectors.curve.models import Parameters
from almanak.connectors.curve.state import Decision
from almanak.connectors.dex_economic_binding import EconomicBinding


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal[
        "curve-stableswap-fixed-external-unit-balances-v1",
        "curve-ng-fixed-external-unit-balances-v1",
    ] = "curve-stableswap-fixed-external-unit-balances-v1"

    @model_validator(mode="after")
    def family_model(self):
        expected = economic_data.model_identity(self.config)[0]
        if "model" not in self.model_fields_set:
            object.__setattr__(self, "model", expected)
        elif self.model != expected:
            raise ValueError("Curve manifest model differs from its configured pool families")
        return self


class CurveEconomic(EconomicBinding):
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
