"""Independently admit runtime-requested family transitions. No scheduling loop."""

import time

from almanak.gateway.ethereum import DataError
from almanak.gateway.production_config import require_execution_gas
from almanak.gateway.production_fees import quoted_price
from almanak.transaction_family import (
    Anchor,
    Candidate,
    ReplacementProposal,
    candidate_view,
    digest,
    eligibility,
    minimum_price,
    original_plan,
    with_family,
)

TERMINAL = {"candidate_limit", "elapsed_limit", "clock_reversed"}


def validate_family(config, pending):
    family = pending.family
    if family is None:
        return
    if (
        family.configuration_revision != digest(config)
        or family.plan_hash != digest(original_plan(pending))
        or family.identity != pending.attempt_id
    ):
        raise DataError(
            "inconsistent", "transaction family differs from its original plan/revision"
        )
    first = family.candidates[0]
    if (first.tx_hash, first.raw_transaction, first.gas_price, first.fee_quote) != (
        pending.tx_hash,
        pending.raw_transaction,
        pending.gas_price,
        pending.fee_quote,
    ):
        raise DataError("inconsistent", "family must preserve its original signed candidate")


def command(backend, state, action, identity, index):
    pending = state.pending
    if pending is None:
        if any(
            family.identity == identity
            for o in state.outcomes
            if (family := getattr(o, "family", None)) is not None
        ):
            return state
        raise ValueError("family command requires retained pending execution")
    family = pending.family
    if family is None or family.identity != identity:
        raise ValueError("family command has a different admitted identity")
    backend.validate_pending(pending)
    if completed_command(family, action, index):
        return state
    try:
        return transition(backend, state, action, index)
    except DataError as exc:
        if exc.kind in ("reorganization", "inconsistent", "chain_mismatch", "fork_mismatch"):
            raise
        # A refused increase leaves every signed candidate and its reservation intact.
        return with_family(
            state,
            family.model_copy(
                update={
                    "blocked": family.blocked if family.stopped else exc.kind,
                    "stopped": family.stopped or exc.kind in TERMINAL,
                }
            ),
        )


def completed_command(family, action, index):
    if action in ("reserve", "sign", "abandon"):
        if index < len(family.candidates):
            return True
        if index != len(family.candidates):
            raise ValueError("replacement command skipped a candidate identity")
        return action == "reserve" and family.proposal is not None
    if index != len(family.candidates) - 1:
        raise ValueError("submission/observation must identify the latest candidate")
    return action == "attempt" and family.candidates[-1].submission is not None


def transition(backend, state, action, index):
    if action == "abandon":
        return abandon(state)
    pending = state.pending
    if backend.receipt(pending) is not None:
        raise DataError("receipt_available", "reconcile the mined family before further submission")
    now = Anchor(at=time.time(), block=backend.reader.pin_block("latest"))
    check_nonce(backend, pending, now.block)
    if action == "inspect":
        return inspect(state, now)
    if action == "attempt":
        return mark_attempt(backend, state, now)
    require_eligible(state, now)
    if action == "reserve":
        return reserve(backend, state, now, index)
    if action == "sign":
        return sign(backend, state, now)
    raise ValueError("unsupported family transition")


def abandon(state):
    family = state.pending.family
    proposal = family.proposal
    if proposal is None:
        return state
    return with_family(
        state,
        family.model_copy(
            update={
                "proposal": None,
                "abandoned": (*family.abandoned, proposal),
                "blocked": family.blocked if family.stopped else "unsigned_proposal_abandoned",
            }
        ),
    )


def inspect(state, now):
    family = state.pending.family
    reason = eligibility(state.config, state.pending, now)
    if reason != "clock_reversed":
        family = family.model_copy(update={"observation": now})
    return with_family(
        state,
        family.model_copy(
            update={
                "blocked": reason,
                "stopped": family.stopped or reason in TERMINAL,
            }
        ),
    )


def require_eligible(state, now):
    reason = eligibility(state.config, state.pending, now)
    if reason:
        raise DataError(reason, "replacement is not eligible under the retained bounds")
    require_mode(state)


def require_mode(state):
    from almanak.connectors.composition import connector_for

    if state.mode == "teardown" and connector_for(state.config).execution.increases_exposure(
        state.config, state.pending.operation
    ):
        raise DataError("teardown", "teardown stops new-risk fee escalation")


def validate_operation(backend, state, view):
    backend.connector.execution.validate_replacement(backend.config, view.operation)
    backend.validate_operation(state, view.operation, view.before)
    backend.verify_authority(state.permissions, view.operation.account)
    current = backend.validate_current(view)
    backend.validate_fee_budget(state, view, current.native_balance)


def reserve(backend, state, now, index):
    pending = state.pending
    family = pending.family
    backend.require_fresh(now.block)
    policy = require_execution_gas(state.config).policy(pending.operation.account)
    if policy.replacement is None:
        raise DataError("replacement_disabled", "gas policy does not authorize replacement")
    price, quote = quoted_price(backend.reader, policy, now.block)
    price = max(
        price, minimum_price(family.candidates[-1].gas_price, policy.replacement.minimum_bump_bps)
    )
    exposure = backend.op_exposure(
        pending.operation, quote.block, pending.execution.data, pending.gas
    )
    quote = quote.model_copy(update={"additional_fee_exposure": exposure})
    view = pending.model_copy(update={"gas_price": price, "fee_quote": quote})
    validate_operation(backend, state, view)
    now = finish_admission(backend, state, now, quote.block)
    proposal = ReplacementProposal(
        index=index,
        configuration_revision=family.configuration_revision,
        gas_price=price,
        fee_quote=quote,
        reserved=now,
    )
    return with_family(
        state,
        family.model_copy(
            update={
                "proposal": proposal,
                "observation": now,
                "blocked": None,
            }
        ),
    )


def sign(backend, state, now):
    pending = state.pending
    family = pending.family
    proposal = family.proposal
    if proposal is None:
        raise ValueError("replacement signing requires a durable fee reservation")
    backend.require_fresh(proposal.fee_quote.block)
    policy = require_execution_gas(state.config).policy(pending.operation.account)
    if policy.replacement is None:
        raise DataError("replacement_disabled", "gas policy does not authorize replacement")
    market, quote = quoted_price(
        backend.reader,
        policy,
        proposal.fee_quote.block,
        node_gas_price=proposal.fee_quote.node_gas_price,
    )
    exposure = backend.op_exposure(
        pending.operation, quote.block, pending.execution.data, pending.gas
    )
    quote = quote.model_copy(update={"additional_fee_exposure": exposure})
    expected = max(
        market, minimum_price(family.candidates[-1].gas_price, policy.replacement.minimum_bump_bps)
    )
    if (quote, expected) != (proposal.fee_quote, proposal.gas_price):
        raise DataError("inconsistent", "replacement fee differs from its retained observation")
    view = pending.model_copy(
        update={"gas_price": proposal.gas_price, "fee_quote": proposal.fee_quote}
    )
    validate_operation(backend, state, view)
    now = finish_admission(backend, state, now, proposal.fee_quote.block)
    signed = backend._sign(view.operation, view.execution, view.nonce, view.gas, view.gas_price)
    candidate = Candidate(
        index=proposal.index,
        configuration_revision=family.configuration_revision,
        gas_price=proposal.gas_price,
        fee_quote=proposal.fee_quote,
        raw_transaction="0x" + signed.raw_transaction.hex(),
        tx_hash="0x" + signed.hash.hex(),
    )
    return with_family(
        state,
        family.model_copy(
            update={
                "candidates": (*family.candidates, candidate),
                "proposal": None,
                "observation": now,
                "blocked": None,
            }
        ),
    )


def finish_admission(backend, state, now, quote_block):
    # Slow external reads cannot carry a fee increase past its original time bounds.
    observed = now.model_copy(update={"at": time.time()})
    if observed.at < now.at:
        raise DataError("clock_reversed", "replacement clock moved backwards during admission")
    require_eligible(state, observed)
    backend.require_fresh(quote_block)
    return observed


def mark_attempt(backend, state, now):
    pending = state.pending
    family = pending.family
    candidate = family.candidates[-1]
    if candidate.index:
        require_mode(state)
        require_attempt_time(state, now)
    backend.verify_authority(state.permissions, pending.operation.account)
    view = candidate_view(pending, candidate)
    current = backend.validate_current(view)
    backend.validate_fee_budget(state, view, current.native_balance)
    if family.observation and now.at < family.observation.at:
        raise DataError("clock_reversed", "submission clock moved backwards")
    completed_at = time.time()
    if completed_at < now.at:
        raise DataError("clock_reversed", "submission clock moved backwards during admission")
    now = now.model_copy(update={"at": completed_at})
    if candidate.index:
        require_attempt_time(state, now)
    candidate = candidate.model_copy(update={"submission": now})
    return with_family(
        state,
        family.model_copy(
            update={
                "candidates": (*family.candidates[:-1], candidate),
                "observation": now,
                "blocked": family.blocked if family.stopped else None,
            }
        ),
    )


def require_attempt_time(state, now):
    family = state.pending.family
    if family.observation and now.at < family.observation.at:
        raise DataError("clock_reversed", "submission clock moved backwards")
    first = family.candidates[0].submission
    policy = require_execution_gas(state.config).policy(state.pending.operation.account).replacement
    if policy is None:
        raise DataError("replacement_disabled", "gas policy does not authorize replacement")
    if first is None or now.at - first.at >= policy.max_elapsed_seconds:
        raise DataError("elapsed_limit", "first replacement submission exceeds the family window")
    if family.stopped and family.blocked in ("elapsed_limit", "clock_reversed"):
        raise DataError(family.blocked, "family escalation is permanently stopped")


def check_nonce(backend, pending, block):
    sender = pending.execution.submitter.address
    latest = int(backend.reader._rpc("eth_getTransactionCount", [sender, hex(block.number)]), 16)
    queued = int(backend.reader._rpc("eth_getTransactionCount", [sender, "pending"]), 16)
    known = any(
        backend.reader._rpc("eth_getTransactionByHash", [c.tx_hash], allow_none=True) is not None
        for c in pending.family.candidates
    )
    if latest != pending.nonce or queued not in (pending.nonce, pending.nonce + int(known)):
        raise DataError(
            "nonce_conflict", "unknown nonce consumption; retain and reconcile the family"
        )
    if backend.reader.pin_block("latest") != block:
        raise DataError("head_changed", "chain head advanced during nonce inspection")


def broadcast(backend, state):
    pending = state.pending
    if backend.receipt(pending) is not None:
        return False
    candidate = pending.family.candidates[-1]
    if candidate.submission is None:
        raise DataError("submission_unattempted", "record submission identity before broadcast")
    if (
        backend.reader._rpc("eth_getTransactionByHash", [candidate.tx_hash], allow_none=True)
        is not None
    ):
        return False
    check_nonce(backend, pending, backend.reader.pin_block("latest"))
    backend.verify_authority(state.permissions, pending.operation.account)
    view = candidate_view(pending, candidate)
    current = backend.validate_current(view)
    backend.validate_fee_budget(state, view, current.native_balance)
    returned = backend.reader._rpc("eth_sendRawTransaction", [candidate.raw_transaction])
    if returned != candidate.tx_hash:
        raise DataError("inconsistent", "RPC returned a different transaction hash")
    return True


def record_broadcast(backend, state):
    error = None
    sent = False
    try:
        sent = backend.broadcast(state.pending, state.permissions, state=state)
    except DataError as exc:
        if exc.kind in ("inconsistent", "reorganization", "chain_mismatch", "fork_mismatch"):
            raise
        error = exc.kind
    family = state.pending.family
    candidate = family.candidates[-1].model_copy(update={"submission_error": error})
    family = family.model_copy(update={"candidates": (*family.candidates[:-1], candidate)})
    return with_family(state, family), sent
