"""Allocation ownership and cumulative economic flows are durable run facts."""

from almanak.connectors.morpho_blue.domain import Position as FundPosition
from almanak.connectors.morpho_blue.domain import Uint256
from almanak.connectors.morpho_blue.economic_account import AccountView, validate_ownership
from almanak.connectors.morpho_blue.economic_market import project
from almanak.connectors.morpho_blue.observations import Ownership


class Position(Ownership):
    supplied: Uint256 = 0
    withdrawn: Uint256 = 0
    borrowed: Uint256 = 0
    repaid: Uint256 = 0
    seized_collateral: Uint256 = 0
    liquidated_shares: Uint256 = 0
    bad_debt_shares: Uint256 = 0


def account_view(positions, market, ledger, fund) -> AccountView:
    targets = tuple(p.target for p in positions)
    if len(set(targets)) != len(targets) or len({t.asset for t in targets}) != len(targets):
        raise ValueError("modeled Morpho fund positions alias an asset balance")
    if any(t.fund != fund or t.reserve.market != ledger.source for t in targets):
        raise ValueError("modeled Morpho positions require one fund and market")
    return AccountView(
        market=market,
        ledger=ledger,
        fund=fund,
        position=FundPosition(
            supply_shares=sum(p.supply_shares for p in positions),
            borrow_shares=sum(p.borrow_shares for p in positions),
            collateral=sum(p.collateral for p in positions),
        ),
    )


def update_positions(positions, fund_position):
    return tuple(
        Position(
            **(
                before.model_dump()
                | {
                    "supply_shares": fund_position.supply_shares
                    if before.target.reserve.is_loan
                    else 0,
                    "borrow_shares": fund_position.borrow_shares
                    if before.target.reserve.is_loan
                    else 0,
                    "collateral": 0 if before.target.reserve.is_loan else fund_position.collateral,
                }
            )
        )
        for before in positions
    )


def funds(positions):
    return tuple(dict.fromkeys(p.target.fund for p in positions))


def validate_positions(positions, market, ledger, time):
    projected = project(ledger, time)
    accounts = tuple(
        account_view(tuple(p for p in positions if p.target.fund == fund), market, projected, fund)
        for fund in funds(positions)
    )
    validate_ownership(projected, accounts)
