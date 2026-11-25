"""Direct actions for Across: one bridge of a stated amount, and the revocation of a
SpokePool approval a bridge left standing.

A bridge deployment holds one allocation on the chain it sends from. Only the wallet is
bound. The recipient is stated as an address on the destination chain, in the
allocation's `adopt` statement, so the deployment has one chain and never reads or
signs on the other. The deployment sends exactly the stated amount and binds no more of
the wallet.

The steps are the approval of exactly the stated amount to the SpokePool, then the
deposit, which spends that approval whole. An allowance of another size that the wallet
already gave the SpokePool is cleared first. The command ends with the deposit's
receipt and reports the transfer by its reference; the arrival is read later, by
`ax bridge-status`, because it happens on another chain. A deposit refused after its
approval was mined leaves that approval standing, and `ax revoke-bridge` clears it.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet

# The label the unbound recipient carries as the allocation's destination account.
RECIPIENT = "recipient"


def label(wallet: Wallet) -> str:
    """The recipient's label, which must differ from the wallet's account name."""
    return RECIPIENT if wallet.account != RECIPIENT else "destination"


def recipient(wallet: Wallet, request: ActionRequest) -> str:
    """The stated recipient. A revocation sends nothing and states none: its route then
    names the wallet's own address, which receives nothing either."""
    if request.recipient is not None:
        return request.recipient
    if request.action == "bridge":
        raise ValueError("bridge requires --recipient")
    return wallet.address


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The deployment's one allocation along the resolved and completed route."""
    from almanak.chains import evm_network

    route = resolved.route
    if request.action == "bridge" and request.amount is None:
        raise ValueError("bridge requires --amount")
    fund = {
        "chain": {"chain_id": str(route.destination_chain), "environment": "mainnet"},
        "address": recipient(wallet, request),
    }
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "destination_account": label(wallet),
                "route": route.model_dump(mode="json"),
                "capital": request.amount or 0,
                "native_asset": str(evm_network(route.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
                "adopt": {"fund": fund},
            }
        ]
    }


def intent(config, observed, request: ActionRequest):
    if request.action == "revoke-bridge":
        return config.revoke(ALLOCATION)
    return config.bridge(ALLOCATION, request.amount)


def last_observed(state):
    """The latest observation the deployment retains: after its last step, else its start."""
    if state.outcomes:
        return state.outcomes[-1].after
    return state.baseline[0] if state.baseline else None


def transfer(outcome) -> dict:
    """A transfer the deployment opened, as the command reports it. It states no
    received amount: that is measured on the destination chain, by `ax bridge-status`."""
    opened, quote = outcome.transfer, outcome.operation.quote
    return {
        "reference": opened.reference,
        "sent": str(opened.sent.amount),
        "offered": str(opened.offered.amount),
        "fill_deadline": quote.fill_deadline,
        "quote_source": quote.source,
        "recipient": outcome.operation.target.destination_fund.address,
        "destination_chain": str(opened.destination.chain.chain_id),
    }


def position(state) -> dict:
    """What the wallet lets the SpokePool spend, at the last block the deployment
    observed, and the transfers this command opened."""
    seen = last_observed(state)
    opened = [o for o in state.outcomes if getattr(o, "transfer", None) is not None]
    return {
        "allowance": None if seen is None else str(seen.allowance),
        "transfers": [transfer(outcome) for outcome in opened],
    }


def planned_steps(intent_kind: str, allowance: int, amount: int | None) -> list[str]:
    """The steps the request takes from the observed allowance, in order."""
    if intent_kind == "revoke_bridge":
        return ["revoke"]
    if allowance == amount:
        return ["deposit"]
    return ["revoke", "approve", "deposit"] if allowance else ["approve", "deposit"]


def quoted_terms(quote, target, amount: int) -> dict:
    from almanak.connectors.across.quote import fee_ceiling

    return {
        "source": quote.source,
        "input_amount": str(amount),
        "output_amount": str(quote.output_amount),
        "fee": str(amount - quote.output_amount),
        "fee_ceiling": str(fee_ceiling(amount, target.route.max_fee_bps)),
        "max_fee_bps": target.route.max_fee_bps,
        "timestamp": quote.timestamp,
        "fill_deadline": quote.fill_deadline,
    }


def details(backend, state) -> dict:
    """What the admitted request rests on beyond its first step: the steps it takes, and
    for a bridge the venue's quote, admitted under the stated ceiling.

    The quote is the gateway's own answer for this request at the planned block, the one
    its first step was admitted with. The deposit asks again and signs its own quote,
    held to the same ceiling, so the terms here are those of the preview, not a promise.
    """
    from almanak.connectors.across import execution
    from almanak.connectors.across import quote as venue

    decided, plan = state.decision.intent, state.plan
    before = plan.before
    amount = decided.amount.amount if decided.kind == "bridge" else None
    stated: dict = {"steps": planned_steps(decided.kind, before.allowance, amount)}
    if amount is None:
        return stated
    quote = execution.quoted(backend, state, decided, before)
    venue.admit(quote, decided.target, amount, before.pool, before.block.timestamp)
    receiving = decided.target.destination_fund.address
    return {
        **stated,
        "recipient": receiving,
        "recipient_is_sender": receiving == decided.target.fund.address,
        "quote": quoted_terms(quote, decided.target, amount),
    }


def sender(config) -> str:
    return config.accounts.bindings[0].profile.fund.address


def unchecked(stated: str) -> bool:
    """Whether an address was stated in one case, which carries no checksum."""
    body = stated[2:]
    return body in (body.lower(), body.upper())


def recipient_note(config, request: ActionRequest) -> str:
    """What the operator must see about a recipient before the deposit is signed."""
    stated = request.recipient or ""
    notes = []
    if stated.lower() != sender(config):
        notes.append(
            f" The recipient {stated.lower()} is not the sending wallet: whoever controls"
            " that address on the destination chain receives the funds, and a deposit is"
            " not undone."
        )
    if unchecked(stated):
        notes.append(
            " The recipient was stated without a checksum, so a mistyped character in it"
            " is not detected; compare it before executing."
        )
    return "".join(notes)


def note(config, observed, request: ActionRequest) -> str | None:
    if request.action != "bridge":
        return None
    return (
        "The command ends when the deposit is mined. The transfer arrives later, on the "
        "destination chain: read it with `ax bridge-status` and the reported reference, or "
        "with the deposit's transaction hash. "
        "Only the gateway holds the quote to --max-fee-bps; the SpokePool checks no price."
        + recipient_note(config, request)
    )


DIRECT = DirectActions(
    actions=("bridge", "revoke-bridge"),
    positions=positions,
    intent=intent,
    position=position,
    note=note,
    details=details,
    public_recovery=False,
)
