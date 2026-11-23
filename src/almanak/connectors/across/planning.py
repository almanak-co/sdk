"""One bridge is an exact approval, the quoted deposit, and a revocation of any residue."""

from almanak.connectors.across.authority import admit
from almanak.connectors.across.operations import (
    ApproveOperation,
    DepositOperation,
    RevokeOperation,
)
from almanak.gateway.ethereum import DataError


def executed(state, intent) -> bool:
    """Whether this decision's deposit is already mined; it is never sent again."""
    if state.decision is None or state.decision.intent != intent:
        raise ValueError("Across planning requires the exact retained economic decision")
    return any(
        outcome.success
        and outcome.decision.identity == state.decision.identity
        and outcome.operation.kind == "deposit"
        for outcome in state.outcomes
    )


def require_scope(config, state, intent, before):
    if before.target != intent.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("Across operation cannot change its configured target or snapshot scope")
    if state.capabilities is None:
        raise ValueError("Across planning requires retained economic capabilities")
    admit(state.capabilities, intent)


def step(config, state, intent, before) -> str:
    """The next step of the retained decision: approve, deposit or revoke."""
    require_scope(config, state, intent, before)
    if executed(state, intent):
        if before.allowance:
            return "revoke"
        raise DataError("incomplete_action", "a recorded Across deposit cannot be sent again")
    if before.allowance == 0:
        return "approve"
    if before.allowance != intent.amount.amount:
        raise DataError(
            "allowance_mismatch", "SpokePool allowance differs from the exact retained bound"
        )
    return "deposit"


def operation(config, state, intent, before, quote=None):
    """The planned step. Only a deposit carries a quote, which the gateway obtains."""
    kind = step(config, state, intent, before)
    if kind == "revoke":
        return RevokeOperation(target=intent.target)
    if kind == "approve":
        return ApproveOperation(target=intent.target, amount=intent.amount)
    if quote is None:
        raise DataError("quote_required", "an Across deposit is planned only from a venue quote")
    return DepositOperation(
        target=intent.target,
        amount=intent.amount,
        quote=quote,
        quote_time_buffer=before.pool.quote_time_buffer,
    )
