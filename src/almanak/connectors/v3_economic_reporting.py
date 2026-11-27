"""Prepared V3 reports retain asset units, missing inputs and hypothetical ownership."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import dex_economic_reporting as shared
from almanak.connectors.dex_economic_reporting import payer_reports as payer_reports
from almanak.connectors.dex_economic_reporting import quantity
from almanak.connectors.v3_economic_simulation import pending_bound
from almanak.connectors.v3_economic_valuation import position_claims, valuation_layer
from almanak.connectors.v3_fee_dilution import estimate

DILUTION = (
    "diluted_fee_estimate scales each sample interval's credited fees by the pool's active "
    "liquidity over that liquidity plus the position's, assuming unchanged historical "
    "swaps; it is an estimate and is credited nowhere"
)
TIMING = (
    "an action executes on the latest sample at or before its eligible time; when that is "
    "the decision's own sample, fill_on_decision_sample is true and the configured latency "
    "did not change the price it executed at"
)


def measurement(state, model, payload):
    observed = model.snapshot(state, payload, state.time, quotes=False)
    principal, fees, bound = (0, 0), (0, 0), (0, 0)
    position = payload.owned.position
    if position is not None:
        record = model.book.at(payload.target, state.time)
        principal, fees, bounds = position_claims(model, payload.target, position, record)
        bound = pending_bound(position, bounds)
    return observed, principal, fees, bound


def position_report(state, model, payload):
    try:
        observed, principal, fees, bound = measurement(state, model, payload)
        missing = None
    except InputUnavailable as error:
        observed, principal, fees, bound = None, (None, None), (None, None), (None, None)
        missing = error.gap.model_dump(mode="json")
    target = payload.target
    result = {
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
    if target.band is not None:
        result |= band_report(target, payload, bound)
    return result | {"diluted_fee_estimate": dilution_report(state, model, target)}


def dilution_report(state, model, target):
    """What the credited fees would be if the position had diluted the pool: an estimate.

    It covers the fees credited so far, collected and uncollected, of every position the
    allocation held. `interval_fees` re-measures them per sample interval, so it can fall
    short of the credited fees by the rounding of each interval.
    """
    measured = estimate(state, model.book, target)
    if measured["measurement_status"] != "estimated":
        return measured
    return measured | {
        name: [quantity(a, v) for a, v in zip(target.assets, measured[name], strict=True)]
        for name in ("interval_fees", "diluted_fees")
    }


def band_report(target, payload, bound):
    """The upper bracket of historical growth the credited lower bracket excludes."""
    return {
        "fee_attribution": "bracketed_lower_bound",
        "uncollected_fee_upper_bound": [
            quantity(a, v) for a, v in zip(target.assets, bound, strict=True)
        ],
        "collected_fee_upper_bound_excess": [
            quantity(target.assets[0], payload.unattributed0),
            quantity(target.assets[1], payload.unattributed1),
        ],
    }


def valued_report(state, model):
    """A standalone run's report, valued in its funding asset where its pools allow.

    Valuation projects the finished run and never withholds its report. A composed run
    reports each connector over its slice of one shared cash ledger, which has no equity
    of its own, so it takes `report` instead.
    """
    try:
        valued = valuation_layer(state, model)
    except ValueError:
        valued = {}
    return report(state, model, valued=valued)


def report(state, model, *, valued=None):
    valued = valued or {}
    result = shared.report(
        state,
        model,
        position_report=position_report,
        evidence=[continuity_summary(model.book, target) for target in model.book.baselines],
        assumptions=[
            "assumed execution access; approvals, inclusion and transaction mechanics excluded",
            "size-dependent integer fills use the latest eligible bounded pool sample",
            TIMING,
            "hypothetical actions do not change subsequent external market history",
            *fee_assumptions(model),
            "modeled principal settlement excludes NFPM aggregate-position collection rounding",
            "hypothetical position identities do not identify on-chain NFTs",
            valuation_assumption(valued),
        ],
    )
    return result | valued


def continuity_summary(book, target):
    """What the continuity proof covers; the proof itself stays in the dataset by digest."""
    baseline = book.baselines[target]
    summary = baseline.model_dump(mode="json", exclude={"events", "inputs"})
    # Every sample names the one proof. The proof's first block can precede the window
    # start by up to a block interval, so the first sample is read no earlier than it.
    start = book.timelines[target].times[0]  # the requirement start
    sample = book.at(target, max(int(baseline.first.timestamp), start))
    return summary | {
        "liquidity_events": len(baseline.events),
        "inputs": len(baseline.inputs),
        "evidence_sha256": sample.payload.baseline_digest,
    }


def valuation_assumption(valued):
    if not valued:
        return "asset quantities are reported without a common-currency valuation or gas conversion"
    return (
        "pool assets are valued in the funding asset at the latest sampled pool price; "
        "other balances and gas are reported in asset units, without conversion"
    )


def fee_assumptions(model):
    # A fixed-range run keeps its established wording; a run with a band states the
    # shared basis once and then how band fees are attributed, so the two cannot conflict.
    if all(model.config.target(a.name).band is None for a in model.config.allocations):
        return (
            "LP fees use fixed historical range growth without dilution or own-trade fees",
            DILUTION,
        )
    return (
        "LP fees use historical range growth without dilution or own-trade fees",
        DILUTION,
        "banded LP fees credit only growth that continuously initialized sampled ticks "
        "prove accrued inside each concrete range; the upper bracket is reported, not credited",
        "banded fee growth is measured between the latest prepared samples at or before "
        "each liquidity action",
    )
