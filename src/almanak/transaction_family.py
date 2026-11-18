"""Same-nonce legacy candidates. Economic plans remain owned by their connector."""

import hashlib
from typing import Annotated, Any, Literal, cast

from pydantic import Field, model_validator

from almanak.gateway.accounts import HexData
from almanak.models import Block, Hash32, Model, UInt
from almanak.state import FeeQuote

Revision = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Price = Annotated[UInt, Field(gt=0, lt=2**256)]


class Anchor(Model):
    at: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    block: Block


class Candidate(Model):
    index: UInt
    configuration_revision: Revision
    gas_price: Price
    fee_quote: FeeQuote
    raw_transaction: HexData
    tx_hash: Hash32
    submission: Anchor | None = None
    submission_error: str | None = None


class ReplacementProposal(Model):
    index: UInt
    configuration_revision: Revision
    gas_price: Price
    fee_quote: FeeQuote
    reserved: Anchor


class TransactionFamily(Model):
    schema_version: Literal[1] = 1
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    configuration_revision: Revision
    plan_hash: Revision
    candidates: Annotated[tuple[Candidate, ...], Field(min_length=1)]
    proposal: ReplacementProposal | None = None
    abandoned: tuple[ReplacementProposal, ...] = ()
    observation: Anchor | None = None
    blocked: str | None = None
    stopped: bool = False

    @model_validator(mode="after")
    def coherent(self):
        entries = (*self.candidates, *((self.proposal,) if self.proposal else ()))
        if any(
            e.index != i or e.configuration_revision != self.configuration_revision
            for i, e in enumerate(entries)
        ):
            raise ValueError("family candidates require consecutive identities and one revision")
        if any(b.gas_price <= a.gas_price for a, b in zip(entries, entries[1:], strict=False)):
            raise ValueError("family gas prices must strictly increase")
        if len({c.tx_hash for c in self.candidates}) != len(self.candidates):
            raise ValueError("family transaction hashes must be unique")
        return self

    @property
    def maximum_price(self):
        return self.proposal.gas_price if self.proposal else self.candidates[-1].gas_price


def digest(model):
    # Same revision representation as retained permission manifests and dashboards.
    return hashlib.sha256(model.model_dump_json().encode()).hexdigest()


def original_plan(pending):
    # Each concrete Pending extends its connector-owned TransactionPlan.
    plan_type = cast(type[Model], type(pending).__bases__[0])
    return plan_type.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash", "family", "unsent"})
    )


def start_family(config, pending):
    policy = replacement_policy(config, pending)
    if policy is None:
        return pending
    revision = digest(config)
    family = TransactionFamily(
        identity=pending.attempt_id,
        configuration_revision=revision,
        plan_hash=digest(original_plan(pending)),
        candidates=(
            Candidate(
                index=0,
                configuration_revision=revision,
                gas_price=pending.gas_price,
                fee_quote=pending.fee_quote,
                raw_transaction=pending.raw_transaction,
                tx_hash=pending.tx_hash,
            ),
        ),
    )
    return pending.model_copy(update={"family": family})


def replacement_policy(config, pending):
    from almanak.gateway.production_config import execution_gas

    admitted = execution_gas(config)
    if admitted is None:
        return None
    return admitted.policy(pending.operation.account).replacement


def candidate_view(pending, candidate):
    return pending.model_copy(
        update={
            "gas_price": candidate.gas_price,
            "fee_quote": candidate.fee_quote,
            "raw_transaction": candidate.raw_transaction,
            "tx_hash": candidate.tx_hash,
            "family": None,
        }
    )


def is_signed(plan) -> bool:
    """Whether a plan already carries recorded signed bytes, as a pending attempt does."""
    return getattr(plan, "raw_transaction", None) is not None


def signed_candidates(pending):
    if pending.family is None:
        return (pending,)
    return tuple(candidate_view(pending, c) for c in pending.family.candidates)


def with_family(state, family):
    return state.model_copy(update={"pending": state.pending.model_copy(update={"family": family})})


def minimum_price(previous, bump):
    return max(previous + 1, (previous * (10000 + bump) + 9999) // 10000)


def eligibility(config, pending, now):
    family = pending.family
    policy = replacement_policy(config, pending)
    if policy is None or family is None:
        return "replacement_disabled"
    if family.stopped:
        return family.blocked or "escalation_stopped"
    if family.observation is not None and now.at < family.observation.at:
        return "clock_reversed"
    first = family.candidates[0].submission
    latest = family.candidates[-1].submission
    if first is None or latest is None:
        return "submission_unattempted"
    if now.at - first.at >= policy.max_elapsed_seconds:
        return "elapsed_limit"
    if len(family.candidates) >= policy.max_candidates:
        return "candidate_limit"
    if now.at - latest.at < policy.interval_seconds or now.block.number <= latest.block.number:
        return "replacement_interval"
    return None


def family_report(state):
    if state.work:
        from almanak.work import project

        return {
            "chains": [
                {
                    "chain": work.chain.model_dump(mode="json"),
                    **family_report(project(state, work.chain)),
                }
                for work in state.work
            ]
        }
    pending = state.pending
    family = pending.family if pending else None
    result: dict[str, Any] = {"pending": None, "completed": []}
    if family is not None:
        policy = replacement_policy(state.config, pending)
        anchor = family.candidates[-1].submission
        scheduled = anchor is not None and policy is not None and not family.stopped
        result["pending"] = {
            **family.model_dump(mode="json"),
            **exposure_report(pending, family),
            "next_eligible_at": anchor.at + policy.interval_seconds if scheduled else None,
            "next_eligible_after_block": anchor.block.model_dump(mode="json")
            if scheduled
            else None,
        }
    result["completed"] = [
        {
            "family": o.family.model_dump(mode="json"),
            "winner": o.tx_hash,
            "gas_paid": o.gas_paid.model_dump(mode="json"),
            "receipt": o.receipt,
            **exposure_report(o.transaction_plan, o.family),
        }
        for o in state.outcomes
        if getattr(o, "family", None) is not None
    ]
    return result


def exposure_report(plan, family):
    if plan is None or plan.execution is None:
        raise ValueError("family reporting requires the original scoped execution plan")
    entries = (*family.candidates, *((family.proposal,) if family.proposal else ()))
    maximum_fee = max(
        plan.gas * entry.gas_price + entry.fee_quote.additional_fee_exposure for entry in entries
    )
    return {
        "account": plan.operation.account,
        "fund": plan.call.fund.model_dump(mode="json"),
        "payer": plan.execution.submitter.model_dump(mode="json"),
        "maximum_reserved_fee": {
            "asset": str(plan.before.native_balance.asset),
            "amount": str(maximum_fee),
        },
    }


def is_unsent(pending) -> bool:
    """Whether the gateway retains proof that a signed attempt's bytes were never sent.

    Signing marks an attempt unsent; the gateway durably clears the mark before its
    first send. A missing mark, as on attempts signed before it existed, means maybe sent.
    """
    return getattr(pending, "unsent", False) is True


# Current-state refusals of a signed, unsent attempt after which it is released and
# planned afresh, as an unsigned plan refused at signing is discarded. A maybe-sent
# attempt whose calldata enforces no deadline instead stops with
# `signed_attempt_unsendable`: it can never be proven dead by waiting.
CURRENT_REFUSALS = (
    "stale_plan",
    "simulation_reverted",
    "price_movement",
    "hook_terms",
    "precondition_changed",
)
