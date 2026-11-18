"""Prepared SparkLend inputs and journal types bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.sparklend.economic_book import Book
from almanak.connectors.sparklend.economic_config import EconomicConfig
from almanak.connectors.sparklend.economic_events import EconomicDriver
from almanak.connectors.sparklend.economic_parameters import initial, parameters
from almanak.connectors.sparklend.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.sparklend.economic_reporting import report
from almanak.connectors.sparklend.economic_state import (
    DecisionInput,
    EventInput,
    OutcomeInput,
    State,
)
from almanak.connectors.sparklend.models import Parameters
from almanak.connectors.sparklend.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["sparklend-revision4-economic-indices-v1"] = (
        "sparklend-revision4-economic-indices-v1"
    )


class SparkEconomic:
    settings_type = EconomicConfig
    decision_type = Decision
    state_type = State
    manifest_type = Manifest
    driver = EconomicDriver
    initial = staticmethod(initial)
    parameters = staticmethod(parameters)
    prepare = staticmethod(prepare)
    dataset_directory = staticmethod(dataset_directory)
    validate_sources = staticmethod(validate_sources)
    report = staticmethod(report)

    @staticmethod
    def request_type(mode):
        from almanak.strategy import (
            EconomicDataDependencyRequest,
            EconomicDecisionRequest,
            EconomicOutcomeRequest,
            EconomicValidationRequest,
            EventRequest,
        )

        return {
            "decision": EconomicDecisionRequest[DecisionInput],
            "outcome": EconomicOutcomeRequest[OutcomeInput],
            "validate": EconomicValidationRequest[Parameters],
            "dependencies": EconomicDataDependencyRequest[Parameters],
            "event": EventRequest[EventInput],
        }[mode]

    @staticmethod
    def open_book(manifest):
        return Book(manifest.dataset, manifest.config, manifest.settings, manifest.dataset_sha256)

    @staticmethod
    def create(args, author, settings):
        from almanak.backtest.economic_cli import create

        return create(args, author, settings)

    @staticmethod
    def resume(args):
        from almanak.backtest.economic_cli import resume

        return resume(args)

    @staticmethod
    def reproduce(args):
        from almanak.backtest.economic_cli import reproduce

        return reproduce(args)
