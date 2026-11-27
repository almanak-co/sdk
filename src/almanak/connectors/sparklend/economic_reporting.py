"""Hypothetical SparkLend balances, liquidation losses and explicit measurement gaps."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.sparklend.economic_data import MODEL
from almanak.connectors.sparklend.economic_events import groups, indebted
from almanak.connectors.sparklend.economic_model import gaps
from almanak.connectors.sparklend.economic_parameters import payer_wallet
from almanak.connectors.sparklend.planning import economic_kind
from almanak.connectors.sparklend.reporting import quantity
from almanak.effects import WalletAsset


def position_report(state, model, position):
    try:
        observation = model.snapshot(state, position)
        missing = None
    except InputUnavailable as error:
        observation = None
        missing = error.gap.model_dump(mode="json")
    return {
        "target": position.target.model_dump(mode="json"),
        "ownership": position.model_dump(mode="json"),
        "supply_share_unit": "sparklend_a_token_scaled_units",
        "debt_share_unit": "sparklend_variable_debt_scaled_units",
        "measurement_status": "modeled" if observation else "unavailable",
        "observation": observation.model_dump(mode="json") if observation else None,
        "missing_input": missing,
        "effects": {
            field: quantity(position.target.asset, getattr(position, field))
            for field in (
                "supplied",
                "withdrawn",
                "borrowed",
                "repaid",
                "liquidated_collateral",
                "liquidated_debt",
                "liquidation_fees",
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


def liquidation_status(state, model):
    statuses = []
    for target in groups(state):
        if not indebted(state, target):
            continue
        try:
            projected = model.preview(state, target, state.time)
            blocked = list(projected.blocked)
            unavailable = None
        except InputUnavailable as error:
            blocked = []
            unavailable = error.gap.model_dump(mode="json")
        statuses.append(
            {
                "fund": target.fund.model_dump(mode="json"),
                "pool": target.reserve.pool,
                "blocked": blocked,
                "missing_input": unavailable,
            }
        )
    return statuses


def report(state, model):
    observations = model.observe(state)
    return {
        "run_id": state.run_id,
        "connector": "sparklend",
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
        "liquidation_status": liquidation_status(state, model),
        "missing_inputs": [gap.model_dump(mode="json") for gap in gaps(state)],
        "data_policy": model.settings.data.model_dump(mode="json"),
        "strategy_sha256": model.strategy.sha256,
        "assumptions": [
            "assumed execution access; approvals, inclusion and transaction mechanics excluded",
            "sampled historical rates integrate hypothetical scaled balances "
            "without changing future history",
            "accounting indexes equal each sample's on-chain indexes at its boundary "
            "and accrue at that sample's rates until the next; a balance or a debt can "
            "step down at a boundary by the excess those rates accrued",
            "configured Pool oracle and reserve risk changes become visible at end of block",
            "liquidation uses SparkLend revision 4 underlying settlement "
            "on the integer-second simulation clock",
            "liquidation fees and unpaid variable debt remain explicit; "
            "third-party liquidator costs are excluded",
            "external reserve balances and caps use retained history plus hypothetical inventory; "
            "additional treasury accrual from hypothetical borrowing is excluded from supply caps",
        ],
    }
