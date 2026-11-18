"""Aave owns prepared input validation, acquisition and economic execution."""

from typing import Literal

from almanak.backtest import progress
from almanak.backtest.config import EconomicBacktestConfig
from almanak.backtest.economic_availability import availability, unavailable
from almanak.backtest.economic_book import EconomicBook
from almanak.backtest.economic_data import MODEL
from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.economic_manifest import Envelope
from almanak.backtest.economic_model import initial_state, parameters
from almanak.backtest.economic_state import EconomicDecision, EconomicParameters, EconomicState


class Manifest(Envelope[EconomicParameters, EconomicBacktestConfig, EconomicState]):
    schema_version: Literal[1] = 1
    model: Literal["aave-v4-economic-rates-v1"] = "aave-v4-economic-rates-v1"


class AaveEconomic:
    settings_type = EconomicBacktestConfig
    decision_type = EconomicDecision
    state_type = EconomicState
    manifest_type = Manifest
    driver = EconomicDriver
    initial = staticmethod(initial_state)

    @staticmethod
    def parameters(author, settings, interval):
        return parameters(settings, author, interval)

    @staticmethod
    def open_book(manifest):
        return open_book(manifest)

    @staticmethod
    def report(state, driver):
        return result_report(state, driver)

    @staticmethod
    def validate_sources(args, author, settings):
        from almanak.backtest.economic_prepare import (
            validate_sources,
            validate_strategy_dependencies,
        )

        reserves = tuple(dict.fromkeys(a.reserve for a in (*author.allocations, *author.debts)))
        validate_sources(settings, reserves)
        validate_strategy_dependencies(args, author, settings, reserves)

    @staticmethod
    def request_type(mode):
        from almanak.backtest.economic_state import (
            EconomicDecisionInput,
            EconomicEventInput,
            EconomicOutcomeInput,
            EconomicParameters,
        )
        from almanak.strategy import (
            EconomicDataDependencyRequest,
            EconomicDecisionRequest,
            EconomicOutcomeRequest,
            EconomicValidationRequest,
            EventRequest,
        )

        return {
            "decision": EconomicDecisionRequest[EconomicDecisionInput],
            "outcome": EconomicOutcomeRequest[EconomicOutcomeInput],
            "validate": EconomicValidationRequest[EconomicParameters],
            "dependencies": EconomicDataDependencyRequest[EconomicParameters],
            "event": EventRequest[EconomicEventInput],
        }[mode]

    @staticmethod
    def create(args, author, settings):
        from almanak.backtest.economic_cli import create

        return create(args, author, settings)

    @staticmethod
    def prepare(args):
        from almanak.backtest.economic_prepare import prepare

        return prepare(args)

    @staticmethod
    def resume(args):
        from almanak.backtest.economic_cli import resume

        return resume(args)

    @staticmethod
    def reproduce(args):
        from almanak.backtest.economic_cli import reproduce

        return reproduce(args)

    @staticmethod
    def dataset_directory(args, author, settings):
        return dataset_directory(args, author, settings)


def open_book(manifest):
    progress.simulation_setup()
    return EconomicBook(
        manifest.dataset,
        tuple(a.reserve for a in manifest.config.positions),
        manifest.settings.schedule.start,
        manifest.settings.schedule.deadline,
        expected_hash=manifest.dataset_sha256,
        data=manifest.settings.data,
    )


def dataset_directory(args, author, settings):
    import hashlib

    from almanak.backtest.config import financial_policy
    from almanak.backtest.economic_prepare import oracle_revision

    reserves = tuple(dict.fromkeys(a.reserve for a in (*author.allocations, *author.debts)))
    identity = financial_policy(settings.data) + str(
        (settings.schedule.start, settings.schedule.deadline)
    )
    identity += "".join(r.model_dump_json() for r in reserves)
    identity += oracle_revision(reserves)
    identity += "".join(h.model_dump_json(exclude={"collection"}) for h in settings.history)
    directory = (
        args.output.resolve().parent
        / ".backtest-datasets"
        / hashlib.sha256(identity.encode()).hexdigest()
    )
    return directory


def result_report(state, driver):
    from almanak.connectors.aave_v4.economic_valuation import valuation_layer

    observations = driver.observe(state)
    observed = {o.target: o for o in observations}
    positions = [
        position_report(p.payload, observed.get(p.payload.target))
        for p in state.portfolio.positions
    ]
    report = {
        "run_id": state.run_id,
        "model": MODEL,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "decision_interval": state.interval.report(),
        "decision_evaluations": state.evaluations,
        "nominal_decision_opportunities": state.schedule.opportunities(
            state.interval.effective.seconds
        ),
        "closed": driver.closed(state, observations),
        "pending": state.pending is not None,
        "completed_steps": len(state.completed),
        "cancelled_steps": len(state.cancelled),
        "delivered_outcomes": state.delivered,
        "liquidation_events": sum(bool(e.transition.result.liquidations) for e in state.events),
        "accounting_complete": not unavailable(state) and len(observations) == len(positions),
        "availability": [a.model_dump(mode="json") for a in availability(state, observations)],
        "unavailable_inputs": [gap.model_dump(mode="json") for gap in unavailable(state)],
        "positions": positions,
        "wallets": [b.model_dump(mode="json") for b in state.portfolio.balances],
        "costs": driver.settings.costs.model_dump(mode="json"),
        "valuation": "explicit_sources"
        if any(b.purpose == "valuation" for b in driver.settings.data.prices)
        else "asset_units_only",
        "valuation_prices": [
            p.model_dump(mode="json") for p in driver.book.observations("valuation", state.time)
        ]
        if any(b.purpose == "valuation" for b in driver.settings.data.prices)
        else [],
        "price_bindings": [b.model_dump(mode="json") for b in driver.settings.data.prices],
        "net_quote_return": None,
        "strategy_sha256": driver.strategy.sha256,
        "assumptions": [
            "fixed historical market path; hypothetical actions do not change market rates",
            "rates sampled every " + str(driver.settings.data.rate_sample_seconds) + " seconds",
            "risk configuration visible at end of historical block",
            "risk pricing policy: " + driver.settings.data.risk_prices,
            "selected vendor prices are retrospective approximations of protocol risk prices",
            "unlimited liquidation liquidity with deterministic pair selection",
            "economic intents exclude approvals, gas estimation and transaction mechanics",
        ],
    }
    # Quote-valued equity, prices and return appear only with declared
    # valuation bindings and usable frozen observations; otherwise the report
    # keeps asset-units fields exactly as they are.
    report.update(valuation_layer(state, driver))
    return report


def position_report(p, observed):
    return {
        **p.model_dump(mode="json"),
        "asset": str(p.target.asset),
        "accounting_complete": observed is not None,
        "marked_assets": str(observed.position.supplied_assets) if observed else None,
        "total_debt": str(observed.position.total_debt) if observed else None,
        "gross_supply_yield": str(
            p.withdrawn + p.liquidated_collateral + observed.position.supplied_assets - p.supplied
        )
        if observed
        else None,
        "accrued_borrow_cost": str(
            p.repaid + p.liquidated_debt + p.deficit + observed.position.total_debt - p.borrowed
        )
        if observed
        else None,
        "risk": observed.risk.model_dump(mode="json") if observed else None,
    }
