"""Aave binding for shared historical CLI orchestration and its retained wire format."""

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.gateway import ContractBinding
from almanak.backtest.manifest import Manifest as Envelope
from almanak.connectors.aave_v4.backtest import (
    AaveModel,
    AaveState,
    capability,
    initial_state,
    parameters,
)
from almanak.connectors.aave_v4.history import Market
from almanak.connectors.aave_v4.history_config import AaveBacktestConfig
from almanak.connectors.aave_v4.models import AaveCapabilities, LendingParameters
from almanak.gateway.evm import erc20_address

Manifest = Envelope[LendingParameters, AaveBacktestConfig, AaveCapabilities, Market, AaveState]


def contract_bindings(settings, config):
    contracts = []
    for binding in settings.history:
        addresses: set[str] = set()
        for allocation in config.allocations:
            source = allocation.reserve
            if source.chain == binding.chain:
                addresses.update(
                    (source.spoke, source.hub, erc20_address(source.asset, source.chain.chain_id))
                )
        if not addresses:
            raise ValueError("historical provider binding has no configured Aave target")
        contracts.append(ContractBinding(chain=binding.chain, contracts=tuple(sorted(addresses))))
    if {a.reserve.chain for a in config.allocations} != {h.chain for h in settings.history}:
        raise ValueError("each allocation requires an explicit historical provider")
    return tuple(contracts)


class AaveHistorical:
    settings_type = AaveBacktestConfig
    state_type = AaveState
    manifest_type = Manifest
    initial = staticmethod(initial_state)
    capability = staticmethod(capability)
    model = AaveModel
    contracts = staticmethod(contract_bindings)

    @staticmethod
    def parameters(author, settings, interval):
        if isinstance(author, LendingParameters):
            # Resolved parameters are authoritative; only the effective cadence changes.
            for debt in author.debts:
                capability(debt.reserve.chain).require(settings.schedule.start, "borrow")
            return author.model_copy(update={"decision_trigger": interval.effective})
        return parameters(
            settings, author.allocations, interval.effective, author.debts
        ).model_copy(
            update={"asset_metadata": author.asset_metadata, "parameters": author.parameters}
        )

    @staticmethod
    def baselines(model, initial):
        return tuple(
            model.market(p.payload.target, initial.time, 0) for p in initial.portfolio.positions
        )

    @staticmethod
    def report(state, model):
        return result_report(state, model)


def result_report(state, model):
    observations = model.observe(state)
    positions = []
    for position, observed in zip(state.portfolio.positions, observations, strict=True):
        payload = position.payload
        positions.append(
            {
                "target": payload.target.model_dump(mode="json"),
                "owned_shares": payload.shares,
                "supplied": str(payload.supplied),
                "withdrawn": str(payload.withdrawn),
                "marked_assets": str(observed.position.supplied_assets),
                "allowance": payload.allowance,
                "gross_yield": str(
                    payload.withdrawn + observed.position.supplied_assets - payload.supplied
                ),
                "asset": str(payload.target.asset),
            }
        )
    return {
        "run_id": state.run_id,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "closed": model.closed(state, observations),
        "pending": state.pending is not None,
        "completed_steps": len(state.completed),
        "cancelled_steps": len(state.cancelled),
        "delivered_outcomes": state.delivered,
        "positions": positions,
        "wallets": [balance.model_dump(mode="json") for balance in state.portfolio.balances],
        "costs": "excluded"
        if isinstance(model.settings.costs, ExcludedCosts)
        else "fixed_native_estimate",
        "valuation": "asset_units_only",
        "net_quote_return": None,
        "model": "historical_path",
        "strategy_sha256": model.strategy.sha256,
        "evidence": "simulated_outcomes_with_captured_historical_market_inputs",
    }
