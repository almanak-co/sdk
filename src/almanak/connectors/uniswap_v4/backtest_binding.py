"""Uniswap historical authority, continuous-boundary proof and report binding."""

from almanak.backtest.gateway import ContractBinding
from almanak.backtest.manifest import Manifest as Envelope
from almanak.connectors.dex_reporting import residual_report
from almanak.connectors.uniswap_v4.backtest import (
    CONNECTOR,
    State,
    UniswapModel,
    capability,
    initial_state,
    parameters,
)
from almanak.connectors.uniswap_v4.backtest_config import BacktestConfig
from almanak.connectors.uniswap_v4.deployments import historical_addresses
from almanak.connectors.uniswap_v4.history import BoundaryEvidence, log_scope, preflight
from almanak.connectors.uniswap_v4.models import Capabilities, Parameters
from almanak.connectors.uniswap_v4.reporting import mark, position_values, quantity

Manifest = Envelope[Parameters, BacktestConfig, Capabilities, BoundaryEvidence, State]


def contract_bindings(settings, config):
    if {a.pool.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("every pool chain requires an explicit archival provider")
    bindings = []
    for source in settings.history:
        targets = tuple(
            config.target(a.name) for a in config.allocations if a.pool.chain == source.chain
        )
        bindings.append(
            ContractBinding(
                chain=source.chain,
                contracts=tuple(
                    sorted({a for target in targets for a in historical_addresses(target)})
                ),
                logs=tuple(dict.fromkeys(log_scope(t) for t in targets if t.liquidity_enabled)),
            )
        )
    return tuple(bindings)


class UniswapHistorical:
    settings_type = BacktestConfig
    state_type = State
    manifest_type = Manifest
    initial = staticmethod(initial_state)
    model = UniswapModel
    contracts = staticmethod(contract_bindings)
    parameters = staticmethod(parameters)

    @staticmethod
    def capability(chain):
        # The shared historical adapter passes only the chain, so resolve the same
        # installed binding the model paths receive through connector_for.
        from almanak.connectors.composition import connector_from_wire

        return capability(connector_from_wire({"connector": str(CONNECTOR)}), chain)

    @staticmethod
    def baselines(model, initial):
        return tuple(
            preflight(model.histories[a.pool.chain], model.config.target(a.name), initial.schedule)
            for a in model.config.allocations
        )

    @staticmethod
    def report(state, model):
        return result_report(state, model)


def allocation_report(model, payload, observed):
    owned, target = payload.owned, payload.target
    principal, fees = position_values(owned, observed)
    total = (
        None
        if None in fees
        else mark(
            owned.inventory0.amount + principal[0] + fees[0],
            owned.inventory1.amount + principal[1] + fees[1],
            observed,
        )
    )
    benchmark = (
        None
        if payload.benchmark is None
        else mark(payload.benchmark[0].amount, payload.benchmark[1].amount, observed)
    )
    initial = model.config.allocation(target.allocation).capital.amount
    return {
        "target": target.model_dump(mode="json"),
        "owned": owned.model_dump(mode="json"),
        "valuation_source": target.pool.model_dump(mode="json"),
        "block": observed.block.model_dump(mode="json"),
        "complete": total is not None,
        **residual_report(observed),
        "gas_included": False,
        "wallet": [
            observed.wallet0.model_dump(mode="json"),
            observed.wallet1.model_dump(mode="json"),
        ],
        "principal": [quantity(a, v) for a, v in zip(target.assets, principal, strict=True)],
        "uncollected_fees": [quantity(a, v) for a, v in zip(target.assets, fees, strict=True)],
        "collected_fees": [
            quantity(target.assets[0], payload.fees0),
            quantity(target.assets[1], payload.fees1),
        ],
        "fund_value_excluding_gas": quantity(target.funding_currency, total),
        "pnl_excluding_gas": quantity(
            target.funding_currency, None if total is None else total - initial
        ),
        "premint_hold_benchmark": quantity(target.funding_currency, benchmark),
        "lp_minus_hold": quantity(
            target.funding_currency,
            None if total is None or benchmark is None else total - benchmark,
        ),
    }


def gas_payers(state, model):
    entries = []
    for payer in model.settings.payers:
        results = [
            c.transition.result
            for c in state.completed
            if model.settings.payer(c.pending.intent.target.account).name == payer.name
        ]
        assets = dict.fromkeys(r.gas_paid.asset for r in results)
        entries.append(
            {
                "account": payer.name,
                "submitter": payer.fund.model_dump(mode="json"),
                "amounts": [
                    quantity(a, sum(r.gas_paid.amount for r in results if r.gas_paid.asset == a))
                    for a in assets
                ],
                "operation_count": sum(len(r.steps) for r in results),
            }
        )
    return entries


def result_report(state, model):
    observations = model.observe(state)
    return {
        "run_id": state.run_id,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "closed": model.closed(state, observations),
        "pending": state.pending is not None,
        "completed_intents": len(state.completed),
        "delivered_outcomes": state.delivered,
        "positions": [
            allocation_report(model, p.payload, o)
            for p, o in zip(state.portfolio.positions, observations, strict=True)
        ],
        "wallets": [b.model_dump(mode="json") for b in state.portfolio.balances],
        "gas_payers": gas_payers(state, model),
        "costs": model.settings.costs.model_dump(mode="json"),
        "cost_interpretation": (
            "fixed native assumptions from maximum measured Safe fork receipt costs; "
            "not historical gas"
        ),
        "model": (
            "hypothetical participation along the historical market path; "
            "future states exclude own earlier market impact"
        ),
        "strategy_sha256": model.strategy.sha256,
        "boundary_evidence": [b.model_dump(mode="json") for b in model.baselines],
    }
