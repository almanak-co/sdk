"""Connector-owned accepted request identity, retained independently of outer attempts."""

from typing import Literal, Self

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.events import Event, decode_event
from almanak.connectors.identifiers import Name
from almanak.models import Account, Address, AssetAmount, Block, Hash32, Model, UInt

RequestKind = Literal["order", "deposit", "withdrawal"]
RequestStatus = Literal["accepted", "frozen", "executed", "cancelled"]


class ReceiptIdentity(Model):
    block: Block
    transaction_hash: Hash32
    log_index: UInt
    emitter: Address
    handler: Address


class RequestEvidence(Model):
    status: RequestStatus
    key: Hash32
    identity: ReceiptIdentity
    receipt: dict

    @model_validator(mode="after")
    def receipt_identity(self) -> Self:
        expected = {
            "transactionHash": self.identity.transaction_hash,
            "blockHash": self.identity.block.hash,
            "blockNumber": hex(self.identity.block.number),
            "status": "0x1",
        }
        if any(self.receipt.get(k) != v for k, v in expected.items()):
            raise ValueError("request evidence requires its successful canonical receipt identity")
        event = self.event()
        suffix = {
            "accepted": "Created",
            "frozen": "Frozen",
            "executed": "Executed",
            "cancelled": "Cancelled",
        }[self.status]
        if not event.topics or not event.name.endswith(suffix) or event.topics[0] != self.key:
            raise ValueError("request evidence must match its lifecycle status and key")
        return self

    def event(self) -> Event:
        logs = [
            log
            for log in self.receipt.get("logs", ())
            if int(log.get("logIndex", "-1"), 16) == self.identity.log_index
        ]
        if len(logs) != 1:
            raise ValueError("request receipt requires exactly one retained lifecycle event")
        event = decode_event(logs[0], emitter=self.identity.emitter, sender=self.identity.handler)
        if event is None or (event.transaction_hash, event.block_hash, event.block_number) != (
            self.identity.transaction_hash,
            self.identity.block.hash,
            self.identity.block.number,
        ):
            raise ValueError("request event differs from its retained receipt identity")
        return event


class Request(Model):
    fund: Account
    allocation: Name
    decision_identity: str
    kind: RequestKind
    key: Hash32
    market: AssetType
    escrow: tuple[AssetAmount, ...]
    execution_fee: AssetAmount
    admission_deadline: UInt | None = None
    events: tuple[RequestEvidence, ...]

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.market.chain != self.fund.chain.chain_id:
            raise ValueError("GMX request market must retain its fund chain")
        assets = (*self.escrow, self.execution_fee)
        if any(a.asset.chain != self.fund.chain.chain_id for a in assets):
            raise ValueError("request escrow and execution fee must retain the fund chain")
        if not self.events or self.events[0].status != "accepted":
            raise ValueError("request history must begin with acceptance evidence")
        self.validate_history()
        return self

    def validate_history(self) -> None:
        previous = None
        for event in self.events:
            if event.key != self.key or event.identity.block.chain != self.fund.chain:
                raise ValueError("request history must retain its key and execution environment")
            decoded = event.event()
            prefix = {"order": "Order", "deposit": "Deposit", "withdrawal": "Withdrawal"}[self.kind]
            if (
                not decoded.name.startswith(prefix)
                or decoded.value("address", "account") != self.fund.address
            ):
                raise ValueError("request event must identify its kind and fund owner")
            if previous is not None:
                validate_transition(previous, event)
            previous = event

    @property
    def status(self) -> RequestStatus:
        return self.events[-1].status

    @property
    def unresolved(self) -> bool:
        return self.status in ("accepted", "frozen")

    def append(self, event: RequestEvidence) -> Self:
        if event in self.events:
            return self
        return type(self).model_validate({**self.model_dump(), "events": (*self.events, event)})


def validate_transition(previous: RequestEvidence, event: RequestEvidence) -> None:
    if previous.status in ("executed", "cancelled") or event.status == "accepted":
        raise ValueError("request terminal history is immutable and acceptance occurs once")
    prior = previous.identity
    current = event.identity
    if (current.block.number, current.log_index) <= (prior.block.number, prior.log_index):
        raise ValueError("request lifecycle evidence must advance in canonical event order")
    if current.block.number == prior.block.number and current.block.hash != prior.block.hash:
        raise ValueError("request events cannot mix competing blocks at the same height")
    if (current.emitter, current.handler) != (prior.emitter, prior.handler):
        raise ValueError("request lifecycle must retain the exact emitter and handler")


def validate_cancellation(request: Request, fund: Account, kind: RequestKind, key: Hash32) -> None:
    """Gateway admission also needs a current chain read before planning or signing."""
    if (request.fund, request.kind, request.key) != (fund, kind, key):
        raise ValueError("cancellation requires the exact retained fund, request kind and key")
    if not request.unresolved:
        raise ValueError("a settled request cannot be cancelled")
