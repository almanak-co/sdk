"""Hypothetical Morpho ownership, liquidation and explicit financial measurement limits."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.morpho_blue.economic_data import MODEL
from almanak.connectors.morpho_blue.economic_model import gaps
from almanak.connectors.morpho_blue.economic_parameters import payer_wallet
from almanak.connectors.morpho_blue.planning import economic_kind
from almanak.connectors.morpho_blue.reporting import missing_data, quantity
from almanak.effects import WalletAsset


def position_report(state, model, position):
    try:
        observation, missing = model.snapshot(state, position), None
    except InputUnavailable as error:
        observation, missing = None, error.gap.model_dump(mode="json")
    return {
        "target": position.target.model_dump(mode="json"),
        "ownership": position.model_dump(mode="json"),
        "ownership_unit": "morpho_loan_shares"
        if position.target.reserve.is_loan
        else "collateral_base_units",
        "modeled_supplied_assets": quantity(
            position.target.asset, observation.account.supplied_assets
        )
        if observation is not None and position.target.reserve.is_loan
        else None,
        "modeled_borrowed_assets": quantity(
            position.target.asset, observation.account.borrowed_assets
        )
        if observation is not None and position.target.reserve.is_loan
        else None,
        "measurement_status": "modeled" if observation else "unavailable",
        "observation": observation.model_dump(mode="json") if observation else None,
        "missing_input": missing,
        "missing_data": missing_data(observation),
        "effects": {
            field: quantity(position.target.asset, getattr(position, field))
            for field in (
                "supplied",
                "withdrawn",
                "borrowed",
                "repaid",
                "seized_collateral",
            )
        },
    }


def payer_reports(state, model):
    accounts: dict[WalletAsset, set[str]] = {}
    amounts: dict[WalletAsset, int] = {}
    for allocation in model.config.positions:
        target = model.config.target(allocation.name)
        key = payer_wallet(model.settings, target, allocation.native_asset)
        accounts.setdefault(key, set()).add(target.account)
        amounts.setdefault(key, 0)
    for completed in state.completed:
        if not completed.transition.result.success or model.settings.costs.kind == "excluded":
            continue
        intent = completed.pending.intent
        asset = model.config.allocation(intent.target.allocation).native_asset
        key = payer_wallet(model.settings, intent.target, asset)
        amounts[key] += getattr(model.settings.costs, economic_kind(intent))
    return [
        {
            "wallet": key.model_dump(mode="json"),
            "fund_accounts": sorted(funds),
            "cost": quantity(key.asset, amounts[key])
            if model.settings.costs.kind != "excluded"
            else None,
            "measurement_status": "excluded"
            if model.settings.costs.kind == "excluded"
            else "assumed",
        }
        for key, funds in accounts.items()
    ]


def report(state, model):
    observations = model.observe(state)
    return {
        "run_id": state.run_id,
        "connector": "morpho_blue",
        "model": MODEL,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "closed": model.closed(state, observations),
        "pending": state.pending is not None,
        "completed_intents": len(state.completed),
        "delivered_outcomes": state.delivered,
        "delivered_events": state.events_delivered,
        "decision_interval": state.interval.report(),
        "decision_evaluations": state.evaluations,
        "nominal_decision_opportunities": state.schedule.opportunities(
            state.interval.effective.seconds
        ),
        "positions": [position_report(state, model, p.payload) for p in state.portfolio.positions],
        "wallets": [b.model_dump(mode="json") for b in state.portfolio.balances],
        "gas_payers": payer_reports(state, model),
        "costs": model.settings.costs.model_dump(mode="json"),
        "cost_interpretation": "excluded or fixed native cost per successful economic action",
        "events": [e.model_dump(mode="json") for e in state.events],
        "missing_inputs": [gap.model_dump(mode="json") for gap in gaps(state)],
        "data_policy": model.settings.data.model_dump(mode="json"),
        "strategy_sha256": model.strategy.sha256,
        "assumptions": [
            "assumed market access; approvals, transaction inclusion and fee-market "
            "mechanics excluded",
            "one hypothetical market ledger shares liquidity, adaptive interest and "
            "bad debt across funds",
            "archive accounting boundaries add external net flows; hypothetical flows "
            "and losses persist",
            "adaptive target rate and fee reset to archive samples at configured "
            "boundaries; this is an archive-anchored model",
            "hypothetical actions do not alter subsequent external market history",
            "configured market oracle becomes visible at the end of each historical "
            "timestamp group",
            "modeled accounting accrues at economic action time, including collateral-only actions",
            "immediate liquidation on the integer-second clock; third-party liquidator "
            "capital, costs and profit are excluded",
            "liquidated and bad-debt shares are ownership counters, not token asset amounts",
            "rewards, fiat PnL and transaction replacement are excluded",
        ],
    }
