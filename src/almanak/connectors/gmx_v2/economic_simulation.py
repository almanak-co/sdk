"""One economic step preserves accepted escrow through waiting, settlement and cleanup."""

from dataclasses import dataclass
from typing import Literal

from almanak.connectors.gmx_v2.economic_accounting import submitter_cost
from almanak.connectors.gmx_v2.economic_perpetual_state import PerpetualEscrow
from almanak.connectors.gmx_v2.economic_requests import (
    admit,
    cancellation_eligible,
    eligible_opportunity,
    fill_amount,
    release,
)
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_state import LiquidityEscrow, Payload
from almanak.connectors.gmx_v2.intents import RevokePosition
from almanak.dex_intents import RevokeExchange
from almanak.effects import AssetDelta
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount

Phase = Literal["accepted", "waiting", "settled", "revoked", "cancelled", "claimed"]


@dataclass(frozen=True)
class Step:
    after: Payload
    assets: tuple[AssetDelta, ...]
    phase: Phase
    gas: AssetAmount
    keeper: AssetAmount
    refund: AssetAmount
    reason: str | None = None


def zero(target):
    return AssetAmount(asset=native_asset(target.fund.chain.chain_id), amount=0)


def revocation(settings, payload):
    if not any(value.amount for value in payload.allowances):
        raise ValueError("GMX modeled spending authority is already closed")
    after = payload.model_copy(
        update={
            "allowances": tuple(
                value.model_copy(update={"amount": 0}) for value in payload.allowances
            )
        }
    )
    return Step(
        after,
        (),
        "revoked",
        submitter_cost(settings, payload.target, "revocation"),
        zero(payload.target),
        zero(payload.target),
    )


def position_cleanup(settings, payload, risk):
    from almanak.connectors.gmx_v2.economic_perpetual_requests import cleanup

    if any(value.amount for value in payload.allowances):
        return revocation(settings, payload)
    after, movements, phase = cleanup(payload, risk)
    return Step(
        after,
        movements,
        phase,
        submitter_cost(settings, payload.target, "claim"),
        zero(payload.target),
        zero(payload.target),
    )


def new_request(config, settings, payload, pending, risk):
    if isinstance(pending.intent, RevokePosition):
        return position_cleanup(settings, payload, risk)
    if isinstance(pending.intent, RevokeExchange):
        return revocation(settings, payload)
    after, movements = admit(config, payload, pending, risk)
    return Step(
        after,
        movements,
        "accepted",
        submitter_cost(settings, payload.target, "acceptance"),
        zero(payload.target),
        zero(payload.target),
    )


def owner_cancellation(payload, settings):
    after, movements, keeper, refund = release(
        payload, settings, payload.escrow.inputs, owner_cancellation=True
    )
    return Step(
        after,
        movements,
        "cancelled",
        submitter_cost(settings, payload.target, "cancellation"),
        keeper,
        refund,
        "Fund cancels the request after the current historical expiration interval",
    )


def fill(payload, opportunity):
    """Ownership after a keeper fill and the outputs the fund receives with its refund."""
    from almanak.connectors.gmx_v2.economic_liquidity_requests import fill_outputs
    from almanak.connectors.gmx_v2.economic_perpetual_requests import settle

    if isinstance(payload.escrow, PerpetualEscrow):
        return settle(payload, opportunity)
    if isinstance(payload.escrow, LiquidityEscrow):
        return payload, fill_outputs(payload.escrow, opportunity)
    return payload, (fill_amount(payload.escrow, opportunity),)


def keeper_settlement(payload, settings, opportunity):
    try:
        filled, outputs = fill(payload, opportunity)
    except SwapConstraint as error:
        after, movements, keeper, refund = release(payload, settings, payload.escrow.inputs)
        return Step(after, movements, "cancelled", zero(payload.target), keeper, refund, str(error))
    after, movements, keeper, refund = release(filled, settings, outputs)
    return Step(after, movements, "settled", zero(payload.target), keeper, refund)


def advance(config, settings, payload, pending, record):
    risk = record.payload.risk[-1]
    if payload.escrow is None:
        return new_request(config, settings, payload, pending, risk)
    if payload.escrow.intent != pending.intent and not isinstance(
        pending.intent, (RevokeExchange, RevokePosition)
    ):
        raise ValueError("GMX continuation cannot replace its retained economic request")
    if cancellation_eligible(payload.escrow, risk, pending.eligible_at):
        return owner_cancellation(payload, settings)
    for opportunity in record.payload.opportunities:
        if eligible_opportunity(payload.escrow, opportunity, settings, pending.eligible_at):
            return keeper_settlement(payload, settings, opportunity)
    return Step(
        payload, (), "waiting", zero(payload.target), zero(payload.target), zero(payload.target)
    )
