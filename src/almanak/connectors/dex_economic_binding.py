"""Shared runner entry points bind concrete prepared connector schemas."""

from types import ModuleType


class EconomicBinding:
    state_types: ModuleType
    parameter_type: type
    book_type: type

    @classmethod
    def request_type(cls, mode):
        from almanak.strategy import (
            EconomicDataDependencyRequest,
            EconomicDecisionRequest,
            EconomicOutcomeRequest,
            EconomicValidationRequest,
        )

        return {
            "decision": EconomicDecisionRequest.__class_getitem__(cls.state_types.DecisionInput),
            "outcome": EconomicOutcomeRequest.__class_getitem__(cls.state_types.OutcomeInput),
            "validate": EconomicValidationRequest.__class_getitem__(cls.parameter_type),
            "dependencies": EconomicDataDependencyRequest.__class_getitem__(cls.parameter_type),
        }[mode]

    @classmethod
    def open_book(cls, manifest):
        return cls.book_type(
            manifest.dataset, manifest.config, manifest.settings, manifest.dataset_sha256
        )

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
