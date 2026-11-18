"""Hypothetical market accounting retains own flows and losses across archival samples."""

from pydantic import model_validator

from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.accounting import accrue
from almanak.connectors.morpho_blue.domain import Accounting, Market, MarketObservation, Uint256
from almanak.models import Model, UInt

TOTALS = ("supply_assets", "supply_shares", "borrow_assets", "borrow_shares")


class MarketLedger(Model):
    source: Market
    accounting: Accounting
    rate_at_target: Uint256
    effective_at: UnixTime
    anchor_time: UnixTime
    generation: UInt = 0

    @model_validator(mode="after")
    def time_scope(self):
        if self.accounting.last_update > self.effective_at or self.anchor_time > self.effective_at:
            raise ValueError("modeled Morpho accounting or anchor cannot follow its logical time")
        return self


class MarketChange(Model):
    before: MarketLedger
    after: MarketLedger

    @model_validator(mode="after")
    def continuity(self):
        if self.after.source != self.before.source:
            raise ValueError("modeled Morpho market changes cannot cross market identities")
        if self.after.generation != self.before.generation + 1:
            raise ValueError("modeled Morpho market changes require the next generation")
        if (
            self.after.effective_at < self.before.effective_at
            or self.after.anchor_time < self.before.anchor_time
        ):
            raise ValueError("modeled Morpho market changes cannot move backward in time")
        return self


def sampled_accounting(observed: MarketObservation, time: int):
    if time < observed.block.timestamp:
        raise ValueError("modeled Morpho accounting cannot use future archival evidence")
    return accrue(observed.accounting, observed.rate_at_target, int(time))


def seed(observed: MarketObservation, time: UnixTime) -> MarketLedger:
    projected = sampled_accounting(observed, time)
    return MarketLedger(
        source=observed.source,
        accounting=projected.accounting,
        rate_at_target=projected.rate_at_target,
        effective_at=time,
        anchor_time=time,
    )


def project(ledger: MarketLedger, time: UnixTime) -> MarketLedger:
    if time < ledger.effective_at:
        raise ValueError("modeled Morpho accrual cannot precede a committed market change")
    accrued = accrue(ledger.accounting, ledger.rate_at_target, int(time))
    return MarketLedger(
        **(
            ledger.model_dump()
            | {
                "accounting": accrued.accounting,
                "rate_at_target": accrued.rate_at_target,
                "effective_at": time,
            }
        )
    )


def reanchor(
    ledger: MarketLedger,
    previous: MarketObservation,
    following: MarketObservation,
    time: UnixTime,
) -> MarketLedger:
    if previous.source != ledger.source or following.source != ledger.source:
        raise ValueError("modeled Morpho external samples require the same market identity")
    if time <= ledger.anchor_time or previous.block.timestamp > ledger.anchor_time:
        raise ValueError("modeled Morpho external sample boundary is out of order")
    if following.block.number < previous.block.number:
        raise ValueError("modeled Morpho external samples cannot move backward in chain history")
    anchored = sampled_accounting(previous, ledger.anchor_time)
    before = accrue(anchored.accounting, anchored.rate_at_target, int(time))
    after = sampled_accounting(following, time)
    own = project(ledger, time)
    totals = {
        name: getattr(own.accounting, name)
        + getattr(after.accounting, name)
        - getattr(before.accounting, name)
        for name in TOTALS
    }
    # Integer bounds and market solvency must hold after the external net change.
    # Replacing invalid totals with the historical sample would erase owned losses.
    accounting = Accounting(**totals, last_update=int(time), fee=after.accounting.fee)
    return MarketLedger(
        source=ledger.source,
        accounting=accounting,
        rate_at_target=after.rate_at_target,
        effective_at=time,
        anchor_time=time,
        generation=ledger.generation,
    )


def committed(before: MarketLedger, after: MarketLedger) -> MarketChange:
    return MarketChange(
        before=before,
        after=MarketLedger(**(after.model_dump() | {"generation": before.generation + 1})),
    )


def latest(initial: MarketLedger, changes: tuple[MarketChange, ...]) -> MarketLedger:
    retained = initial
    ordered = sorted(
        (change for change in changes if change.before.source == initial.source),
        key=lambda change: change.after.generation,
    )
    for change in ordered:
        if change.before != retained:
            raise ValueError(
                "modeled Morpho market journal has a missing or conflicting generation"
            )
        retained = change.after
    return retained
