"""Report external-unit LP claims, explicit approximation and missing financial inputs."""

from almanak.backtest.dataset import InputUnavailable
from almanak.connectors import dex_economic_reporting as shared
from almanak.connectors.curve.economic_simulation import removal_amounts, virtual_pool
from almanak.connectors.dex_economic_reporting import quantity


def measurement(state, model, payload):
    observed = model.snapshot(state, payload, state.time, quotes=False)
    position = payload.owned.position
    principal = (0,) * len(payload.target.assets)
    if position is not None:
        principal = removal_amounts(
            payload.target,
            virtual_pool(observed.pool, payload.owned, state.time),
            position.shares.amount,
            None,
        )
    return observed, principal


def position_report(state, model, payload):
    try:
        observed, principal = measurement(state, model, payload)
        missing = None
    except InputUnavailable as error:
        observed, principal = None, (None,) * len(payload.target.assets)
        missing = error.gap.model_dump(mode="json")
    target = payload.target
    return {
        "target": target.model_dump(mode="json"),
        "ownership": payload.owned.model_dump(mode="json"),
        "measurement_status": "modeled" if observed else "unavailable",
        "observation": observed.model_dump(mode="json") if observed else None,
        "missing_input": missing,
        "proportional_lp_claims": [
            quantity(asset, amount) for asset, amount in zip(target.assets, principal, strict=True)
        ],
        "fee_income": [quantity(asset, None) for asset in target.assets],
        "fee_measurement": "Fees remain embedded in external LP unit balances; "
        "separate fee income is unmeasured.",
        "cumulative_inputs": payload.spent.model_dump(mode="json"),
    }


def report(state, model):
    return shared.report(
        state,
        model,
        position_report=position_report,
        evidence=[path.model_dump(mode="json") for path in model.book.paths.values()],
        assumptions=[
            "assumed execution access; approvals, inclusion and transaction mechanics excluded",
            "integer fills use the latest bounded-age sample plus a virtual overlay of held "
            "LP shares",
            "every supported amplification and fee change is retained independently of sampling",
            "known amplification ramps advance at simulation time; standard coin rates "
            "remain fixed",
            "hypothetical shares follow sampled external balances per share, with integer "
            "claims rounded down",
            "each operation adds held shares and proportional coin claims to a virtual copy "
            "of the external pool",
            "cash and shares persist; virtual pool composition resets before the next operation",
            "hypothetical actions do not change subsequent external market history",
            "external LP unit growth excludes hypothetical dilution and subsequent fees "
            "from own transactions",
            "LP fees are embedded; separate fee income and common-currency valuation are "
            "unmeasured",
            "metapools, rebasing coins, rate-oracle coins, gauges and incentives are excluded",
            *crypto_assumptions(model.config),
        ],
    )


def crypto_assumptions(config):
    if all(allocation.pool.family == "stableswap_ng" for allocation in config.allocations):
        return []
    return [
        "Crypto samples retain A/gamma ramps, price scales, cached oracle state, dynamic fees "
        "and LP profit accounting; time-dependent transitions use action eligibility",
        (
            "Crypto virtual overlays scale stored and custody balances and invariant together; "
            "sampled price and profit ratios remain fixed; "
            "TriCrypto admin LP mints dilute fund shares"
            if any(a.pool.family == "tricrypto_ng" for a in config.allocations)
            else "Crypto virtual overlays scale balances, invariant and virtual admin shares "
            "together; "
            "sampled price and profit ratios remain fixed"
        ),
        "Crypto rebalancing and admin claims use the selected native revision within each "
        "operation; their hypothetical effects do not feed subsequent external samples",
        "factory fee-receiver changes are retained; views implementation changes refuse the window",
    ]
