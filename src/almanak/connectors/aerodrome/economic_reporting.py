"""Prepared Aerodrome reports retain asset units, missing inputs and hypothetical ownership."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import dex_economic_reporting as shared
from almanak.connectors.dex_economic_reporting import payer_reports as payer_reports
from almanak.connectors.dex_economic_reporting import quantity


def measurement(state, model, payload):
    from almanak.connectors.aerodrome.economic_simulation import principal as principal_amounts
    from almanak.connectors.aerodrome.ownership import at_indices

    observed = model.snapshot(state, payload, state.time, quotes=False)
    principal, fees = (0, 0), (0, 0)
    if payload.owned.position is not None:
        principal = principal_amounts(observed.pool, payload.owned.position.shares.amount)
        current = at_indices(payload.owned.position, observed.pool)
        fees = current.fees0.amount, current.fees1.amount
    return observed, principal, fees


def position_report(state, model, payload):
    try:
        observed, principal, fees = measurement(state, model, payload)
        missing = None
    except InputUnavailable as error:
        observed, principal, fees = None, (None, None), (None, None)
        missing = error.gap.model_dump(mode="json")
    target = payload.target
    return {
        "target": target.model_dump(mode="json"),
        "ownership": payload.owned.model_dump(mode="json"),
        "measurement_status": "modeled" if observed else "unavailable",
        "observation": observed.model_dump(mode="json") if observed else None,
        "missing_input": missing,
        "principal": [quantity(a, v) for a, v in zip(target.assets, principal, strict=True)],
        "uncollected_fees": [quantity(a, v) for a, v in zip(target.assets, fees, strict=True)],
        "collected_fees": [
            quantity(target.assets[0], payload.fees0),
            quantity(target.assets[1], payload.fees1),
        ],
    }


def report(state, model):
    return shared.report(
        state,
        model,
        position_report=position_report,
        evidence=[],
        assumptions=[
            "assumed execution access; approvals, inclusion and transaction mechanics excluded",
            "size-dependent integer fills use the latest eligible stable or volatile pool sample",
            "pool fees and pause state are sampled and held within the explicit maximum source age",
            "changes between samples are not reconstructed from factory events",
            "hypothetical actions do not change subsequent external market history",
            "LP principal uses fixed external balances and supply without hypothetical dilution",
            "LP fees use fixed external fee indices without dilution or own-trade fees",
            "fee claims remain separate from shares, including after the last share is burned",
            "close cost covers the modeled aggregate withdrawal and separate fee claim",
            "staking, gauges and incentive rewards are excluded",
            "asset quantities are reported without a common-currency valuation or gas conversion",
        ],
    )
