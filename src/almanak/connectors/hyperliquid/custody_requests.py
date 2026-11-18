"""Custody actions as asynchronous requests (blueprint 30, section 4.4).

A custody transaction's receipt shows only that HyperCore was asked: CoreWriter emits
`RawAction`, and CoreDepositWallet a `Transfer` to the USDC system address. HyperCore
applies the action after the block or drops it without a trace (T7), so each request
stays unresolved until evidence that identifies its own effect is observed. A balance
change is never that evidence: an external `depositFor`, or any other credit, can match
the amount while the Safe's own action was dropped.

No ledger entry names the HyperEVM transaction or the `RawAction` (T13). An entry is
attributed to a request by its delta type, amount and direction and by its `time`,
which falls in the second of the action's HyperEVM block. A second candidate in that
second, or another account's credit that could have written the same entry, leaves the
request unresolved. A request still unresolved past its window halts the deployment
with its evidence; it is never resubmitted, because CoreWriter gives no proof that a
queued action will not still execute.
"""

from collections.abc import Callable
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, JsonValue

from almanak.connectors.hyperliquid.agents import Submission
from almanak.connectors.hyperliquid.corewriter import USDC_SYSTEM, usdc_text
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import InfoResult, typed
from almanak.gateway.hypercore_shapes import Millis
from almanak.models import Address, Hash32, Model, UInt

# A ledger page this long may be truncated, so it cannot show that a candidate is unique.
LEDGER_PAGE_LIMIT = 500
USDC_NAME = "USDC"


class CreditLog(Model):
    """A CoreDepositWallet `Transfer(recipient, 0x2000...0000, amount)`: one credit."""

    transaction: Hash32
    log_index: UInt
    block: UInt
    recipient: Address
    amount: UInt


class SystemTransfer(Model):
    """A transaction from the USDC system address calling `CoreDepositWallet.transfer`."""

    transaction: Hash32
    block: UInt
    recipient: Address
    amount: UInt


class Request(Model):
    key: str
    safe: Address
    submission: Submission
    status: Literal["unresolved", "resolved"] = "unresolved"
    # What resolved it: the identifying entry, transfer or answer, as observed.
    evidence: tuple[JsonValue, ...] = ()


class Deposit(Request):
    kind: Literal["deposit"] = "deposit"
    amount: UInt
    credit: CreditLog


class ClassTransfer(Request):
    kind: Literal["class_transfer"] = "class_transfer"
    amount: UInt
    to_perp: bool
    payload: str


class Withdrawal(Request):
    kind: Literal["withdrawal"] = "withdrawal"
    amount: UInt
    payload: str


class StandardMode(Request):
    kind: Literal["standard_mode"] = "standard_mode"
    payload: str


CustodyRequest = Annotated[
    Deposit | ClassTransfer | Withdrawal | StandardMode, Field(discriminator="kind")
]


class Evidence(Model):
    """What one reconciliation observed for one request."""

    # The Safe's ledger over the second of the request's block.
    ledger: InfoResult | None = None
    # Every CoreDepositWallet credit to the Safe in blocks of that second.
    credits: tuple[CreditLog, ...] = ()
    # Every `RawAction` payload the Safe emitted in blocks of that second.
    actions: tuple[str, ...] = ()
    # The executed system transfers to the Safe of the request's amount in the block after
    # its own, where HyperCore delivers a HyperEVM credit.
    system: tuple[SystemTransfer, ...] = ()
    abstraction: InfoResult | None = None
    # The HyperEVM head's time when the transfers were read.
    head_time: Millis | None = None

    def observed_at(self) -> int | None:
        """When the earliest of these observations was taken: none can show an absence
        later than that."""
        times = [r.received_at for r in (self.ledger, self.abstraction) if r is not None]
        if self.head_time is not None:
            times.append(self.head_time)
        return min(times) if times else None


def second(submission: Submission) -> tuple[int, int]:
    start = submission.time - submission.time % 1000
    return start, start + 999


def entries(evidence: Evidence, kind: str) -> list[dict]:
    if evidence.ledger is None:
        return []
    page = typed(evidence.ledger)
    if len(page) >= LEDGER_PAGE_LIMIT:
        return []
    return [{"time": e.time, "hash": e.hash, **e.delta} for e in page if e.delta["type"] == kind]


def amount_is(entry: dict, field: str, usdc: int) -> bool:
    try:
        return Decimal(str(entry[field])) == usdc_text(usdc)
    except (KeyError, ArithmeticError):
        return False


def deposit_candidates(request: Deposit, evidence: Evidence) -> list[dict]:
    return [
        e
        for e in entries(evidence, "spotTransfer")
        if e.get("user") == USDC_SYSTEM
        and e.get("destination") == request.safe
        and e.get("token") == USDC_NAME
        and amount_is(e, "amount", request.amount)
    ]


def attribute_deposit(request: Deposit, evidence: Evidence) -> tuple[JsonValue, ...] | None:
    # A credit to the Safe of the same amount in the same second, from any transaction
    # but this one, writes an identical entry: the two cannot be told apart.
    rivals = [
        c
        for c in evidence.credits
        if c.recipient == request.safe and c.amount == request.amount and c != request.credit
    ]
    candidates = deposit_candidates(request, evidence)
    if rivals or len(candidates) != 1:
        return None
    return (candidates[0],)


def attribute_class_transfer(
    request: ClassTransfer, evidence: Evidence
) -> tuple[JsonValue, ...] | None:
    candidates = [
        e
        for e in entries(evidence, "accountClassTransfer")
        if e.get("toPerp") is request.to_perp and amount_is(e, "usdc", request.amount)
    ]
    if evidence.actions.count(request.payload) != 1 or len(candidates) != 1:
        return None
    return (candidates[0],)


def attribute_withdrawal(request: Withdrawal, evidence: Evidence) -> tuple[JsonValue, ...] | None:
    candidates = [
        e
        for e in entries(evidence, "send")
        if e.get("user") == request.safe
        and e.get("destination") == USDC_SYSTEM
        and e.get("token") == USDC_NAME
        and (e.get("sourceDex"), e.get("destinationDex")) == ("spot", "spot")
        and amount_is(e, "amount", request.amount)
    ]
    # Only the next block's payout is this withdrawal's, and only a unique one.
    transfers = [
        t
        for t in evidence.system
        if t.block == request.submission.block + 1
        and (t.recipient, t.amount) == (request.safe, request.amount)
    ]
    if evidence.actions.count(request.payload) != 1 or len(candidates) != 1 or len(transfers) != 1:
        return None
    return candidates[0], transfers[0].model_dump(mode="json")


def attribute_standard_mode(request: StandardMode, evidence: Evidence):
    answer = evidence.abstraction
    if answer is None or answer.received_at < request.submission.time:
        return None
    return (answer.model_dump(mode="json"),) if typed(answer) == "disabled" else None


ATTRIBUTION: dict[str, Callable[..., tuple[JsonValue, ...] | None]] = {
    "deposit": attribute_deposit,
    "class_transfer": attribute_class_transfer,
    "withdrawal": attribute_withdrawal,
    "standard_mode": attribute_standard_mode,
}


def attributed(request, evidence: Evidence, now: int, window: int):
    """The request resolved by its own effect, unchanged while it may still appear, or a
    halt with the evidence once its window has passed."""
    if request.status == "resolved":
        return request
    found = ATTRIBUTION[request.kind](request, evidence)
    if found is not None:
        return request.model_copy(update={"status": "resolved", "evidence": found})
    deadline = request.submission.time + window
    observed = evidence.observed_at()
    if now >= deadline and observed is not None and observed >= deadline:
        raise DataError(
            "custody_unresolved",
            f"HyperCore custody {request.kind} {request.key} has no identified effect",
            details={
                "request": request.model_dump(mode="json"),
                "evidence": evidence.model_dump(mode="json"),
            },
        )
    return request
