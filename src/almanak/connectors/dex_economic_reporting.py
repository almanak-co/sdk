"""Shared hypothetical wallet, payer, scheduling and missing-input reporting."""

from almanak.connectors.dex_economic_parameters import payer_wallet
from almanak.effects import WalletAsset


def quantity(asset, amount):
    return {
        "asset": str(asset),
        "amount": None if amount is None else str(amount),
        "unit": "base_units",
    }


def payer_reports(state, model):
    accounts: dict[WalletAsset, set[str]] = {}
    costs: dict[WalletAsset, int] = {}
    for allocation in model.config.allocations:
        target = model.config.target(allocation.name)
        key = payer_wallet(model.settings, target, allocation.native_asset)
        accounts.setdefault(key, set()).add(target.account)
        costs.setdefault(key, 0)
    for completed in state.completed:
        result = completed.transition.result
        if result.success:
            key = payer_wallet(model.settings, result.intent.target, result.gas_paid.asset)
            costs[key] += result.gas_paid.amount
    excluded = model.settings.costs.kind == "excluded"
    return [
        {
            "wallet": key.model_dump(mode="json"),
            "fund_accounts": sorted(funds),
            "cost": quantity(key.asset, None if excluded else costs[key]),
            "measurement_status": "excluded" if excluded else "assumed",
        }
        for key, funds in accounts.items()
    ]


def report(state, model, *, position_report, assumptions, evidence):
    from almanak.connectors.dex_reporting import residual_report

    observed = {o.target: o for o in model.observe(state)}
    positions = [
        {
            **position_report(state, model, p.payload),
            **residual_report(observed.get(p.payload.target)),
        }
        for p in state.portfolio.positions
    ]
    return {
        "run_id": state.run_id,
        "connector": str(model.data.CONNECTOR),
        "model": model.model_name,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "closed": model.closed(state, tuple(observed.values())),
        "pending": state.pending is not None,
        "completed_intents": len(state.completed),
        "delivered_outcomes": state.delivered,
        "decision_interval": state.interval.report(),
        "decision_evaluations": state.evaluations,
        "nominal_decision_opportunities": state.schedule.opportunities(
            state.interval.effective.seconds
        ),
        "positions": positions,
        "wallets": [b.model_dump(mode="json") for b in state.portfolio.balances],
        "gas_payers": payer_reports(state, model),
        "costs": model.settings.costs.model_dump(mode="json"),
        "cost_interpretation": "excluded or fixed native cost per successful economic operation",
        "failures": [
            c.transition.result.model_dump(mode="json")
            for c in state.completed
            if not c.transition.result.success
        ],
        "missing_inputs": [p["missing_input"] for p in positions if p["missing_input"]],
        "data_policy": model.settings.data.model_dump(mode="json"),
        "strategy_sha256": model.strategy.sha256,
        "boundary_evidence": evidence,
        "assumptions": assumptions,
    }
