"""One prepared Uniswap model bound to both verified chain deployments."""

from typing import Literal

from almanak.backtest.economic_manifest import Envelope
from almanak.connectors.uniswap_v4.backtest import economic_parameters
from almanak.connectors.uniswap_v4.backtest_binding import result_report
from almanak.connectors.uniswap_v4.economic_band import pending_bound
from almanak.connectors.uniswap_v4.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_config import EconomicConfig
from almanak.connectors.uniswap_v4.economic_data import MODEL
from almanak.connectors.uniswap_v4.economic_model import EconomicModel, initial_state
from almanak.connectors.uniswap_v4.economic_prepare import (
    dataset_directory,
    prepare,
    validate_sources,
)
from almanak.connectors.uniswap_v4.economic_state import DecisionInput, OutcomeInput, State
from almanak.connectors.uniswap_v4.models import Parameters
from almanak.connectors.uniswap_v4.reporting import quantity
from almanak.connectors.uniswap_v4.state import Decision


class Manifest(Envelope[Parameters, EconomicConfig, State]):
    model: Literal["uniswap-v4-economic-integer-fixed-growth-v1"] = (
        "uniswap-v4-economic-integer-fixed-growth-v1"
    )


class UniswapEconomic:
    settings_type = EconomicConfig
    decision_type = Decision
    state_type = State
    manifest_type = Manifest
    driver = EconomicModel
    initial = staticmethod(initial_state)
    parameters = staticmethod(economic_parameters)
    prepare = staticmethod(prepare)
    dataset_directory = staticmethod(dataset_directory)
    validate_sources = staticmethod(validate_sources)

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
        return PoolBook(
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

    @staticmethod
    def report(state, model):
        report = result_report(state, model)
        report.update(
            model=MODEL,
            decision_interval=state.interval.report(),
            decision_evaluations=state.evaluations,
            nominal_decision_opportunities=state.schedule.opportunities(
                state.interval.effective.seconds
            ),
            data_policy=model.settings.data.model_dump(mode="json"),
            cost_interpretation="excluded or fixed native cost per economic operation",
            assumptions=[
                "assumed execution access; approvals, inclusion and transaction mechanics excluded",
                "each action uses sampled pool state without changing future history",
                "LP fees use historical range growth without dilution or own-trade fees",
                "latest available pool sample; no future interpolation",
                *band_assumptions(model.config),
            ],
        )
        for entry, held in zip(report["positions"], state.portfolio.positions, strict=True):
            if held.payload.target.band is not None:
                entry |= band_report(state, model, held.payload)
        return report


def band_assumptions(config):
    if all(config.target(a.name).band is None for a in config.allocations):
        return ()
    return (
        "banded LP fees credit only growth that continuously initialized sampled ticks "
        "prove accrued inside each concrete range; the upper bracket is reported, not credited",
        "banded fee growth is measured between the latest prepared samples at or before "
        "each liquidity action",
    )


def band_report(state, model, payload):
    """The upper bracket of historical growth the credited lower bracket excludes."""
    target, position = payload.target, payload.owned.position
    pending = (0, 0)
    if position is not None:
        record = model.book.at(target, state.time)
        pending = pending_bound(position, model.band_growth(record, target, position.range))
    return {
        "fee_attribution": "bracketed_lower_bound",
        "uncollected_fee_upper_bound": [
            quantity(a, v) for a, v in zip(target.assets, pending, strict=True)
        ],
        "collected_fee_upper_bound_excess": [
            quantity(target.assets[0], payload.unattributed0),
            quantity(target.assets[1], payload.unattributed1),
        ],
    }
