"""Report simulated request obligations, fund refunds and separately scoped submitter costs."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import dex_economic_reporting as shared
from almanak.connectors.dex_economic_reporting import quantity
from almanak.connectors.gmx_v2.economic_parameters import payer_wallet
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicPerpetualResult,
)
from almanak.connectors.gmx_v2.economic_state import EconomicResult
from almanak.effects import WalletAsset

RequestResult = (EconomicResult, EconomicPerpetualResult)


def position_report(state, model, payload):
    try:
        observed = model.snapshot(state, payload, state.time)
        missing = None
    except InputUnavailable as error:
        observed = None
        missing = error.gap.model_dump(mode="json")
    return {
        "target": payload.target.model_dump(mode="json"),
        "ownership": payload.owned.model_dump(mode="json"),
        "escrow": payload.escrow.model_dump(mode="json") if payload.escrow else None,
        "remaining_obligations": model.remaining(payload),
        "measurement_status": "modeled" if observed else "unavailable",
        "observation": observed.model_dump(mode="json") if observed else None,
        "missing_input": missing,
        "spent": [value.model_dump(mode="json") for value in payload.spent],
        "allowances": [value.model_dump(mode="json") for value in payload.allowances],
    }


def request_results(state):
    return [
        value
        for completed in state.completed
        if isinstance(value := completed.transition.result, RequestResult)
    ]


def payer_reports(state, model):
    accounts: dict[WalletAsset, set[str]] = {}
    costs: dict[WalletAsset, int] = {}
    for allocation in model.config.allocations:
        target = model.config.target(allocation.name)
        key = payer_wallet(model.settings, target, allocation.native_asset)
        accounts.setdefault(key, set()).add(target.account)
        costs.setdefault(key, 0)
    for result in request_results(state):
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


def liquidated(result):
    return {
        "outcome": "liquidated",
        "allocation": result.allocation,
        "effective_at": result.effective_at.isoformat(),
        "reason": result.reason,
        "assets": [value.model_dump(mode="json") for value in result.assets],
        "unpaid_cost_usd_e30": str(result.unpaid_usd_e30),
        "fees_collected": result.fees_collected,
        # The liquidator's gas is external to the fund and its payer; it is not measured.
        "keeper_cost": {"measurement_status": "unmeasured"},
    }


def deferred(value):
    return {
        "outcome": "deferred",
        "allocation": value.allocation,
        "effective_at": value.effective_at.isoformat(),
        "eligibility": value.eligibility,
        "reason": value.reason,
        "unavailable": None
        if value.unavailable is None
        else value.unavailable.model_dump(mode="json"),
    }


def liquidation_reports(state):
    entries: list[dict[str, object]] = []
    for event in state.events:
        batch = event.transition.result
        if isinstance(batch, EconomicLiquidationBatch):
            entries.extend(liquidated(result) for result in batch.results)
            entries.extend(deferred(value) for value in batch.deferrals)
    return entries


def assumptions(settings):
    selected: tuple[str, ...]
    if settings.position_model:
        selected = (
            "Hypothetical positions settle with deployed market-order accounting against "
            "verified publications; liquidations are evaluated only at verified publications "
            "strictly after the position's last change and settle without an author hook.",
            "Deferred impact collateral remains an obligation; the model refuses to claim it "
            "until the deployed claim window and factor are verified.",
        )
    elif settings.liquidity_model:
        selected = (
            "Hypothetical GM requests use sequential deposit execution and external pool "
            "valuation.",
        )
    else:
        selected = ("Hypothetical swaps use integer GMX accounting against verified publications.",)
    return [
        *selected,
        "Only publications eligible after request acceptance can provide keeper opportunities.",
        "Keeper access is assumed at eligible publications with a fixed native payment.",
        "Approvals are sized to each request and modeled without native transactions or "
        "receipts; live runs pay one or two approval transactions per request, which reported "
        "submitter cost omits.",
        "Hypothetical actions do not change subsequent external market history.",
        "Asset quantities have no common-currency valuation or invented native-token peg.",
    ]


def report(state, model):
    result = shared.report(
        state,
        model,
        position_report=position_report,
        evidence=[
            timeline.rows[0].reference.model_dump(mode="json")
            for timeline in model.book.timelines.values()
        ],
        assumptions=assumptions(model.settings),
    )
    result["gas_payers"] = payer_reports(state, model)
    result["cost_interpretation"] = (
        "Excluded or fixed native submitter cost per acceptance, owner cancellation, revocation "
        "or claim; keeper payment consumes fund escrow separately."
    )
    result["keeper_policy"] = model.settings.keeper.model_dump(mode="json")
    result["request_steps"] = [
        {
            "kind": value.intent.kind,
            "phase": value.phase,
            "success": value.success,
            "gas_paid": value.gas_paid.model_dump(mode="json"),
            "keeper_paid": value.keeper_paid.model_dump(mode="json"),
            "fund_refund": value.fund_refund.model_dump(mode="json"),
        }
        for value in request_results(state)
    ]
    result["liquidations"] = liquidation_reports(state)
    return result
