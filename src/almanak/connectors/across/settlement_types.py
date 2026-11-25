"""How a transfer ends: a measured arrival or a measured refund, each an external effect.

Neither is an SDK transaction. A relayer fills on the destination chain and the
SpokePool refunds on the source chain; both are read from bounded canonical pages.
"""

from datetime import UTC, datetime
from typing import Literal

from pydantic import Field, model_validator

from almanak.connectors.across.domain import Target
from almanak.connectors.identifiers import ConnectorId
from almanak.external_effects import ExternalEffect, ExternalIdentity, ExternalScan
from almanak.models import AssetAmount, Block, Hash32, Model
from almanak.transfers import Reference
from almanak.work import ObservationScope

CONNECTOR = ConnectorId("across")
# FillType of the SpokePool, in its own order.
FillType = Literal["fast", "replaced_slow", "slow"]
FILL_TYPES: tuple[FillType, ...] = ("fast", "replaced_slow", "slow")


def arrival_scope(target: Target) -> ObservationScope:
    return ObservationScope(
        chain=target.destination_fund.chain,
        account=target.destination_account,
        allocation=target.allocation,
    )


def refund_scope(target: Target) -> ObservationScope:
    return ObservationScope(
        chain=target.fund.chain, account=target.account, allocation=target.allocation
    )


class ArrivalResult(Model):
    """What the strategy learns when a transfer arrives.

    A fork or live run delivers a `MeasuredArrival`, read from the destination chain. A
    backtest delivers a modeled arrival of the same kind, which names no block.
    """

    kind: Literal["bridge_arrival"] = "bridge_arrival"
    target: Target
    transfer: Reference
    # What the destination account received. `offered` is what the deposit asked for.
    received: AssetAmount
    offered: AssetAmount


class MeasuredArrival(ArrivalResult):
    """An arrival measured on the destination chain, with the fill that paid it."""

    identity: ExternalIdentity
    fill_type: FillType
    block: Block
    tx_hash: Hash32
    accounting_issues: tuple[str, ...] = ()

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)


class RefundResult(Model):
    """What the strategy learns when the venue refunds transfers that were never filled."""

    kind: Literal["bridge_refund"] = "bridge_refund"
    identity: ExternalIdentity
    target: Target
    transfers: tuple[Reference, ...]
    # Measured on the source chain: the whole amount the refund paid the fund.
    returned: AssetAmount
    block: Block
    tx_hash: Hash32
    accounting_issues: tuple[str, ...] = ()

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)


class Settlement(ExternalEffect):
    target: Target
    block: Block
    tx_hash: Hash32
    receipt: dict
    accounting_issues: tuple[str, ...] = ()

    def require_evidence(self, scope: ObservationScope):
        if (
            self.identity.connector != CONNECTOR
            or self.identity.scope != scope
            or self.block.chain != scope.chain
            or self.effective_at != datetime.fromtimestamp(self.block.timestamp, UTC)
        ):
            raise ValueError("Across settlement differs from its exact fund, chain or time")
        self.require_receipt(self.tx_hash, self.receipt)

    def require_receipt(self, tx_hash, receipt):
        expected = {
            "transactionHash": tx_hash,
            "blockHash": self.block.hash,
            "blockNumber": hex(self.block.number),
            "status": "0x1",
        }
        if any(receipt.get(key) != value for key, value in expected.items()):
            raise ValueError("Across settlement lacks its canonical successful receipt")


class ArrivalOutcome(Settlement):
    kind: Literal["across_arrival"] = "across_arrival"
    transfer: Reference
    received: AssetAmount
    offered: AssetAmount
    fill_type: FillType
    log_index: int

    @model_validator(mode="after")
    def evidence(self):
        self.require_evidence(arrival_scope(self.target))
        asset = self.target.destination_asset
        if self.received.asset != asset or self.offered.asset != asset or not self.received.amount:
            raise ValueError("an arrival is a positive amount of the received asset")
        return self

    def strategy_result(self):
        return MeasuredArrival(
            identity=self.identity,
            target=self.target,
            transfer=self.transfer,
            received=self.received,
            offered=self.offered,
            fill_type=self.fill_type,
            block=self.block,
            tx_hash=self.tx_hash,
            accounting_issues=self.accounting_issues,
        )


class Companion(Model):
    """A further refund transaction of the same block, attributed after the first."""

    tx_hash: Hash32
    receipt: dict


class RefundOutcome(Settlement):
    kind: Literal["across_refund"] = "across_refund"
    transfers: tuple[Reference, ...]
    # The whole amount of every refund transaction of the block that pays a transfer.
    returned: AssetAmount
    # Refunds mined in one block are one outcome: a page ends at its settlement block.
    companions: tuple[Companion, ...] = Field(default=(), exclude_if=lambda value: not value)

    @model_validator(mode="after")
    def evidence(self):
        self.require_evidence(refund_scope(self.target))
        if self.returned.asset != self.target.asset or not self.returned.amount:
            raise ValueError("a refund is a positive amount of the sent asset")
        if not self.transfers or len(set(self.transfers)) != len(self.transfers):
            raise ValueError("a refund resolves at least one transfer, each once")
        hashes = [self.tx_hash, *(companion.tx_hash for companion in self.companions)]
        if len(set(hashes)) != len(hashes):
            raise ValueError("a refund names each of its transactions once")
        for companion in self.companions:
            self.require_receipt(companion.tx_hash, companion.receipt)
        return self

    def strategy_result(self):
        return RefundResult(
            identity=self.identity,
            target=self.target,
            transfers=self.transfers,
            returned=self.returned,
            block=self.block,
            tx_hash=self.tx_hash,
            accounting_issues=self.accounting_issues,
        )


class Unsolicited(Model):
    """A refund-leaf payment to the fund that is attributed to no transfer.

    Either it matches no set of transfers proven unfilled, or the SpokePool paid the
    fund another amount than the leaf names. Retained as evidence and never
    attributed. It is no outcome and carries no accounting issue of one, so a
    stranger's payment cannot stop new exposure.
    """

    tx_hash: Hash32
    # What the SpokePool paid the fund in the transaction.
    amount: AssetAmount
    # What the transaction's leaves name for the fund, kept only where the SpokePool's
    # payments differ from them, entry by entry. Such a payment is never attributed.
    leaf_amount: AssetAmount | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def one_asset(self):
        if self.leaf_amount is not None and self.leaf_amount.asset != self.amount.asset:
            raise ValueError("a leaf and its payment are amounts of one asset")
        return self


class Scan(ExternalScan):
    """One bounded page of one side of settlement, with every log its query returned."""

    kind: Literal["across_scan"] = "across_scan"
    side: Literal["arrival", "refund"]
    target: Target
    # An arrival page follows one transfer. A refund page serves every transfer of its
    # allocation proven unfilled, because one refund can pay several at once.
    transfer: Reference | None = None
    start: Block
    end: Block
    events: tuple[dict, ...] = ()
    # An arrival page that ends past the fill deadline without a fill: none can follow.
    expired: bool = False
    # Payments to the fund in a refund page's last block that it attributed to nothing.
    unsolicited: tuple[Unsolicited, ...] = Field(default=(), exclude_if=lambda value: not value)
    outcome: ArrivalOutcome | RefundOutcome | None = None

    @model_validator(mode="after")
    def bounded_scope(self):
        arrival = self.side == "arrival"
        scope = arrival_scope(self.target) if arrival else refund_scope(self.target)
        if (
            self.connector != CONNECTOR
            or self.scope != scope
            or self.start.chain != scope.chain
            or self.end.chain != scope.chain
            or self.start.number > self.end.number
        ):
            raise ValueError("Across scan page requires one ordered explicit execution scope")
        if arrival != (self.transfer is not None) or (self.expired and not arrival):
            raise ValueError("an arrival page follows one transfer; a refund page follows none")
        outcome = self.outcome
        if self.unsolicited and arrival:
            raise ValueError("an unattributed payment ends a refund page")
        named = [payment.tx_hash for payment in self.unsolicited]
        if isinstance(outcome, RefundOutcome):
            named += [outcome.tx_hash, *(companion.tx_hash for companion in outcome.companions)]
        if len(set(named)) != len(named):
            raise ValueError("a refund page names each transaction of its last block once")
        last = {
            row.get("transactionHash")
            for row in self.events
            if row.get("blockNumber") == hex(self.end.number)
        }
        if not arrival and not set(named) <= last:
            raise ValueError("a refund page ends at the block of the payments it names")
        if outcome is not None:
            if isinstance(outcome, ArrivalOutcome) != arrival or outcome.block != self.end:
                raise ValueError("Across scan ends at its own side's settlement block")
            if self.expired or outcome.target != self.target:
                raise ValueError("Across scan outcome differs from its page")
            if isinstance(outcome, ArrivalOutcome) and outcome.transfer != self.transfer:
                raise ValueError("Across arrival differs from the transfer its page follows")
        return self


AWAITING_NOTE = (
    "An unattributed refund payment of the same bridge was paid after the fill deadline "
    "of each transfer named here. It may have been that transfer's refund, and it is "
    "never applied: the transfer stays open, and its deployment unclosed, until a refund "
    "of exactly its sent amount is read."
)


def followed_by_unattributed(state, transfer) -> bool:
    """Whether an unattributed refund payment could concern the transfer: one of its
    allocation, paid after its fill deadline. Such a payment is never applied to it."""
    deadline = int(transfer.fill_deadline.timestamp())
    return any(
        page.unsolicited
        and page.target.allocation == transfer.allocation
        and page.end.timestamp > deadline
        for page in getattr(state, "external_scans", ())
    )
