"""Composed native datasets and one portfolio bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.composed.economic_book import Book
from almanak.connectors.composed.economic_config import EconomicConfig
from almanak.connectors.composed.economic_inventory import initial, parameters
from almanak.connectors.composed.economic_model import EconomicDriver
from almanak.connectors.composed.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.composed.economic_reporting import report
from almanak.connectors.composed.economic_state import (
    DecisionInput,
    EventInput,
    OutcomeInput,
    State,
)
from almanak.connectors.composed.models import OwnedParameters as Parameters
from almanak.connectors.composed.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["composed-native-economic-v1"] = "composed-native-economic-v1"


class ComposedEconomic:
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
