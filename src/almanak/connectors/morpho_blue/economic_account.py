"""Hypothetical fund positions use modeled totals and retain raw oracle evidence."""

from pydantic import model_validator

from almanak.connectors.morpho_blue.domain import MarketObservation, Position
from almanak.connectors.morpho_blue.economic_market import MarketLedger
from almanak.connectors.morpho_blue.liquidation import maximum_debt
from almanak.connectors.morpho_blue.math import WAD, mul_div, to_assets
from almanak.models import Account, Model


class AccountView(Model):
    market: MarketObservation
    ledger: MarketLedger
    fund: Account
    position: Position

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.market.source.chain or self.ledger.source != self.market.source:
            raise ValueError("modeled Morpho fund, ledger and evidence have different scopes")
        if self.fund.address == self.market.fee_recipient:
            raise ValueError("Morpho protocol fee recipient ownership is unsupported")
        if self.market.block.timestamp > self.ledger.effective_at:
            raise ValueError("modeled Morpho account cannot use future market evidence")
        totals = self.ledger.accounting
        if totals.last_update != self.ledger.effective_at:
            raise ValueError("modeled Morpho account requires accounting at its logical time")
        if (
            self.position.supply_shares > totals.supply_shares
            or self.position.borrow_shares > totals.borrow_shares
        ):
            raise ValueError("modeled Morpho owned shares exceed modeled market totals")
        return self

    @property
    def supplied_assets(self) -> int:
        totals = self.ledger.accounting
        return to_assets(self.position.supply_shares, totals.supply_assets, totals.supply_shares)

    @property
    def borrowed_assets(self) -> int:
        totals = self.ledger.accounting
        return to_assets(
            self.position.borrow_shares, totals.borrow_assets, totals.borrow_shares, round_up=True
        )

    @property
    def health_factor(self) -> int | None:
        if not self.position.borrow_shares:
            return None
        if self.market.price is None:
            raise ValueError("modeled Morpho health requires the configured market oracle")
        maximum = maximum_debt(
            self.position.collateral, self.market.price.price, self.market.source.lltv
        )
        return mul_div(maximum, WAD, self.borrowed_assets)


def validate_ownership(ledger: MarketLedger, accounts: tuple[AccountView, ...]) -> None:
    """Count each fund once, even when several allocations reference its balance."""
    retained: dict[Account, Position] = {}
    for account in accounts:
        if account.ledger != ledger:
            raise ValueError("modeled Morpho ownership requires one current market ledger")
        previous = retained.setdefault(account.fund, account.position)
        if previous != account.position:
            raise ValueError("modeled Morpho allocations disagree about their fund position")
    if (
        sum(position.supply_shares for position in retained.values())
        > ledger.accounting.supply_shares
        or sum(position.borrow_shares for position in retained.values())
        > ledger.accounting.borrow_shares
    ):
        raise ValueError("modeled Morpho aggregate owned shares exceed market totals")
