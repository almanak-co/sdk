"""What a modeled bridge run reports: each transfer, labeled modeled, and the assumptions."""

from almanak.connectors.across.economic_model import ASSUMPTIONS, MODEL, payer_wallet
from almanak.effects import WalletAsset


def quantity(asset, amount):
    return {"asset": str(asset), "amount": str(amount), "unit": "base_units"}


def transfer_report(flight):
    """One transfer. An open one states no received amount: missing is not zero."""
    transfer = flight.transfer
    arrived = transfer.resolution is not None
    fee = transfer.sent.amount - transfer.offered.amount
    return {
        "reference": transfer.reference,
        "allocation": transfer.allocation,
        "source": transfer.source.model_dump(mode="json"),
        "destination": transfer.destination.model_dump(mode="json"),
        "state": "arrived" if arrived else "in_flight",
        "sent": quantity(transfer.sent.asset, transfer.sent.amount),
        "sent_at": int(flight.sent_at),
        "offered": quantity(transfer.offered.asset, transfer.offered.amount),
        "received": quantity(transfer.offered.asset, transfer.resolution.received.amount)
        if arrived
        else None,
        "arrived_at": int(flight.arrives_at) if arrived else None,
        "modeled_arrival_at": int(flight.arrives_at),
        "modeled_fee": quantity(transfer.sent.asset, fee),
        "measurement_status": "modeled",
        "quote_source": "modeled_full_fee_ceiling",
    }


def position_report(state, model, position):
    target = position.target
    flights = [transfer_report(f) for f in position.flights]
    arrived = [f for f in position.flights if not f.transfer.open]
    report = {
        "target": target.model_dump(mode="json"),
        "max_fee_bps": target.route.max_fee_bps,
        "transfers": flights,
        "in_flight": len(position.in_flight),
        "arrived": len(arrived),
        "sent": quantity(target.asset, sum(f.transfer.sent.amount for f in position.flights)),
        "in_flight_sent": quantity(
            target.asset, sum(f.transfer.sent.amount for f in position.in_flight)
        ),
        "measurement_status": "modeled",
    }
    if arrived:
        # A sum over arrived transfers only. It is absent while nothing has arrived: an
        # open transfer has no received amount, and a zero here would read as one.
        report["received_by_arrived_transfers"] = quantity(
            target.destination_asset, sum(f.transfer.offered.amount for f in arrived)
        )
    return report


def payer_reports(state, model):
    accounts: dict[WalletAsset, set[str]] = {}
    amounts: dict[WalletAsset, int] = {}
    excluded = model.settings.costs.kind == "excluded"
    for allocation in model.config.positions:
        target = model.config.target(allocation.name)
        key = payer_wallet(model.settings, target, allocation.native_asset)
        accounts.setdefault(key, set()).add(target.account)
        amounts.setdefault(key, 0)
    for completed in state.completed:
        if not completed.transition.result.success or excluded:
            continue
        target = completed.pending.intent.target
        asset = model.config.allocation(target.allocation).native_asset
        amounts[payer_wallet(model.settings, target, asset)] += model.settings.costs.bridge
    return [
        {
            "wallet": key.model_dump(mode="json"),
            "fund_accounts": sorted(funds),
            "cost": None if excluded else quantity(key.asset, amounts[key]),
            "measurement_status": "excluded" if excluded else "assumed",
        }
        for key, funds in accounts.items()
    ]


def report(state, model):
    observations = model.observe(state)
    return {
        "run_id": state.run_id,
        "connector": "across",
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
        "cost_interpretation": "excluded or fixed native cost per successful modeled bridge, "
        "on its source chain only",
        "bridge_model": {
            "kind": model.settings.bridge_model,
            "arrival_latency_seconds": model.settings.data.arrival_latency_seconds,
            "fee": "the market's whole max_fee_bps of the amount sent",
            "refunds": "not modeled",
        },
        "events": [e.model_dump(mode="json") for e in state.events],
        "strategy_sha256": model.strategy.sha256,
        "assumptions": list(ASSUMPTIONS),
    }
