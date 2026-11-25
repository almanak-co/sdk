"""Read-only transfer amounts from retained deposits, arrivals and refunds.

A bridge holds no position: a transfer is a claim in flight until its measured end. An
amount that was not measured has no figure here, never a zero.
"""

from almanak.connectors.across.models import Parameters
from almanak.connectors.across.settlement_types import (
    AWAITING_NOTE,
    ArrivalOutcome,
    RefundOutcome,
    followed_by_unattributed,
)
from almanak.dashboard.bound import MAX_TABLE_ROWS
from almanak.dashboard.models import Column, Context, Ref, Scope, Value
from almanak.dashboard.projection import snapshot
from almanak.dashboard.tables import accounts_dataset, decimals_of, symbol_of, utc
from almanak.transfers import Arrived, Refunded

AMOUNTS = ("sent", "offered", "received", "returned")
COVERAGE = (
    "Transfers the strategy's deposits opened, newest last. Sent and offered come from the "
    "mined deposit; received and returned are measured on the chain that paid them and "
    "stay empty until then. The quote source names where the offered amount came from; a "
    "fork-constructed quote is not the venue's."
)
UNATTRIBUTED_COVERAGE = (
    "Refund-leaf payments to the fund attributed to no transfer, each at the block that paid "
    "it: one that matched no transfer proven unfilled, or one the SpokePool paid in another "
    "amount than its leaf names, shown beside it. They are in the wallet and attributed to "
    "nothing: not a transfer amount and not part of owned cash. Do not add them to either."
)


def status(transfer) -> str:
    if isinstance(transfer.resolution, Arrived):
        return "arrived"
    if isinstance(transfer.resolution, Refunded):
        return "refunded"
    return "unfilled" if transfer.unfilled else "open"


AWAITING = "awaiting"
AWAITING_AFTER_PAYMENT = "awaiting; an unattributed payment followed its fill deadline"


def refund_status(state, transfer) -> str:
    """Whether a transfer proven unfilled waits for its refund, and whether an
    unattributed payment that could have been that refund was read meanwhile."""
    if not (transfer.open and transfer.unfilled):
        return ""
    return AWAITING_AFTER_PAYMENT if followed_by_unattributed(state, transfer) else AWAITING


def deposits(state) -> dict:
    """Each transfer's deposit outcome, by the transfer's reference."""
    opened = (o for o in state.outcomes if getattr(o, "transfer", None) is not None)
    return {o.transfer.reference: o for o in opened if not isinstance(o, ArrivalOutcome)}


def settlements(state) -> dict:
    """The block that measured each transfer's end, by the transfer's reference."""
    blocks = {}
    for outcome in state.outcomes:
        if isinstance(outcome, ArrivalOutcome):
            blocks[outcome.transfer] = outcome.block
        elif isinstance(outcome, RefundOutcome):
            blocks.update(dict.fromkeys(outcome.transfers, outcome.block))
    return blocks


def measured(transfer) -> dict:
    """The four amounts of a transfer with the wallet each belongs to; None is unmeasured."""
    resolution = transfer.resolution
    return {
        "sent": (transfer.source, transfer.sent.amount),
        "offered": (transfer.destination, transfer.offered.amount),
        "received": (
            transfer.destination,
            resolution.received.amount if isinstance(resolution, Arrived) else None,
        ),
        "returned": (
            transfer.source,
            resolution.returned.amount if isinstance(resolution, Refunded) else None,
        ),
    }


def reason(name: str, amount, deposit) -> str:
    if amount is None:
        return f"No measured {name} amount: the transfer did not end this way, or not yet."
    if name == "sent":
        return "Left the source fund in the mined deposit."
    if name == "offered":
        source = deposit.operation.quote.source if deposit else "an unretained quote"
        return f"The output the deposit states, from its {source} quote. Not a received amount."
    wallet = "destination" if name == "received" else "source"
    return f"Measured on the {wallet} chain."


def transfer_values(state, index, transfer, deposit, settled) -> dict:
    values = {}
    for name, (wallet, amount) in measured(transfer).items():
        key = f"transfer-{index}-{name}"
        # The offered amount is a term of the deposit, observed on no destination block.
        block = {"sent": deposit and deposit.after.block, "offered": None}.get(name, settled)
        values[key] = Value(
            id=key,
            amount=None if amount is None else str(amount),
            unit="base_units",
            asset=wallet.asset,
            decimals=decimals_of(state, wallet.asset),
            symbol=symbol_of(state, wallet.asset),
            scope=Scope(account=wallet.account, fund=state.config.profile(wallet.account).fund),
            evidence="recorded",
            block=block if amount is not None else None,
            reason=reason(name, amount, deposit),
        )
    return values


def row(state, index, transfer, deposit) -> dict:
    return {
        "allocation": transfer.allocation,
        "account": transfer.source.account,
        "destination": transfer.destination.account,
        "status": status(transfer),
        "refund": refund_status(state, transfer),
        "quote_source": deposit.operation.quote.source if deposit else "not retained",
        "fill_deadline": utc(int(transfer.fill_deadline.timestamp())),
        **{name: Ref(ref=f"transfer-{index}-{name}") for name in AMOUNTS},
    }


def coverage(rows) -> str:
    followed = any(row["refund"] == AWAITING_AFTER_PAYMENT for row in rows)
    return f"{COVERAGE} {AWAITING_NOTE}" if followed else COVERAGE


def transfer_table(state):
    """The transfer rows and the values they reference, for this and a composed dashboard."""
    opened, ended = deposits(state), settlements(state)
    kept = list(enumerate(state.transfers))[-MAX_TABLE_ROWS:]
    values: dict[str, Value] = {}
    rows = []
    for index, transfer in kept:
        deposit = opened.get(transfer.reference)
        values.update(
            transfer_values(state, index, transfer, deposit, ended.get(transfer.reference))
        )
        rows.append(row(state, index, transfer, deposit))
    dataset = accounts_dataset(
        keep=True,
        columns=(
            Column(key="allocation", label="Bridge"),
            Column(key="account", label="Source account"),
            Column(key="destination", label="Destination account"),
            Column(key="status", label="Status"),
            Column(key="refund", label="Refund"),
            Column(key="quote_source", label="Quote source"),
            Column(key="fill_deadline", label="Fill deadline"),
            Column(key="sent", label="Sent", kind="value"),
            Column(key="offered", label="Offered", kind="value"),
            Column(key="received", label="Received", kind="value"),
            Column(key="returned", label="Returned", kind="value"),
        ),
        rows=tuple(rows),
        coverage=coverage(rows),
        truncated=len(state.transfers) > len(kept),
    )
    return values, dataset


LEAF_AGREES = "The refund leaf names the paid amount; no separate leaf amount is retained."
LEAF_DIFFERS = "What the refund leaf names for the fund; the SpokePool paid another amount."


def payment_value(state, page, key, asset, amount, reason) -> Value:
    target = page.target
    return Value(
        id=key,
        amount=None if amount is None else str(amount),
        unit="base_units",
        asset=asset,
        decimals=decimals_of(state, asset),
        symbol=symbol_of(state, asset),
        scope=Scope(account=target.account, fund=state.config.profile(target.account).fund),
        evidence="recorded",
        block=page.end if amount is not None else None,
        reason=reason,
    )


def unattributed_row(state, values, index, page, payment) -> dict:
    key, paid, leaf = f"unattributed-refund-{index}", payment.amount, payment.leaf_amount
    values[key] = payment_value(
        state,
        page,
        key,
        paid.asset,
        paid.amount,
        "A refund-leaf payment to the fund attributed to nothing.",
    )
    values[f"{key}-leaf"] = payment_value(
        state,
        page,
        f"{key}-leaf",
        paid.asset,
        leaf and leaf.amount,
        LEAF_DIFFERS if leaf else LEAF_AGREES,
    )
    return {
        "allocation": page.target.allocation,
        "account": page.target.account,
        "block": str(page.end.number),
        "transaction": payment.tx_hash,
        "amount": Ref(ref=key),
        "leaf_amount": Ref(ref=f"{key}-leaf"),
    }


def unattributed_table(state):
    """Refund-leaf payments to the fund attributed to no transfer, each with its block.

    They are in the wallet and attributed to nothing: no transfer amount and no part of
    owned cash. One the SpokePool paid in another amount than its leaf names shows the
    leaf amount beside it. The table is absent while there are none.
    """
    payments = [
        (page, payment)
        for page in getattr(state, "external_scans", ())
        for payment in page.unsolicited
    ]
    kept = list(enumerate(payments))[-MAX_TABLE_ROWS:]
    values: dict[str, Value] = {}
    rows = [unattributed_row(state, values, index, *entry) for index, entry in kept]
    if not rows:
        return {}, {}
    dataset = accounts_dataset(
        keep=True,
        columns=(
            Column(key="allocation", label="Bridge"),
            Column(key="account", label="Paid account"),
            Column(key="block", label="Block"),
            Column(key="transaction", label="Transaction"),
            Column(key="amount", label="Paid", kind="value"),
            Column(key="leaf_amount", label="Leaf names", kind="value"),
        ),
        rows=tuple(rows),
        coverage=UNATTRIBUTED_COVERAGE,
        truncated=len(payments) > len(kept),
    )
    return values, {"unattributed_refund_payments": dataset}


def project(state, *, captured_at=None):
    if state.integrity_error:
        raise ValueError("invalidated Across evidence cannot produce a financial dashboard")
    values, dataset = transfer_table(state)
    extra_values, extra = unattributed_table(state)
    values = {**values, **extra_values}
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=(),
        values=values,
        datasets={"transfers": dataset, **extra},
    )
    return context, snapshot(state, values, captured_at)
