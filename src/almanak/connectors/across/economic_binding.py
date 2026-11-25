"""The bridge model's journal types bound to the shared offline runner."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.across.economic_config import EconomicConfig
from almanak.connectors.across.economic_model import EconomicDriver, initial, parameters
from almanak.connectors.across.economic_prepare import (
    Book,
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.across.economic_reporting import report
from almanak.connectors.across.economic_state import DecisionInput, OutcomeInput, State
from almanak.connectors.across.models import Parameters
from almanak.connectors.across.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["across-full-fee-ceiling-fixed-latency-v1"] = (
        "across-full-fee-ceiling-fixed-latency-v1"
    )


class AcrossEconomic:
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
        )

        return {
            "decision": EconomicDecisionRequest[DecisionInput],
            "outcome": EconomicOutcomeRequest[OutcomeInput],
            "validate": EconomicValidationRequest[Parameters],
            "dependencies": EconomicDataDependencyRequest[Parameters],
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
