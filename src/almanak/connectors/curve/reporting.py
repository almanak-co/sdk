"""Curve fees stay embedded in LP economics; no separate fee cash is invented."""

from almanak.connectors.dex_reporting import observations, quantity
from almanak.connectors.dex_reporting import report as exchange_report


def position_report(state, owned):
    target = owned.target
    history = observations(state, target)
    latest = history[-1] if history else None
    events = [outcome for outcome in state.outcomes if outcome.operation.target == target]
    return {
        "target": target.model_dump(mode="json"),
        "ownership": owned.model_dump(mode="json"),
        "last_observation": latest.model_dump(mode="json") if latest else None,
        "measurement_status": "retained" if latest else "unobserved",
        "shares": quantity(
            target.pool.shares_asset,
            (owned.position.shares.amount if owned.position else 0) if latest else None,
        ),
        "fee_measurement": "LP fees remain embedded in the pool share value; "
        "separate fee income is unmeasured.",
        "transactions": [
            {
                "kind": outcome.operation.kind,
                "hash": outcome.tx_hash,
                "nonce": outcome.nonce,
                "success": outcome.success,
                "asset_changes": [
                    change.model_dump(mode="json") for change in outcome.asset_changes
                ],
                "share_change": outcome.share_change.model_dump(mode="json"),
            }
            for outcome in events
        ],
    }


def report(state):
    return exchange_report(state, position_reporter=position_report)
