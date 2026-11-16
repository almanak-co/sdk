"""Immediate modeled liquidation shares bad debt through the common market ledger."""

from almanak.backtest.types import UnixTime
from almanak.connectors.morpho_blue.economic_market import (
    MarketChange,
    MarketLedger,
    committed,
    project,
)
from almanak.connectors.morpho_blue.economic_positions import (
    Position,
    account_view,
    update_positions,
)
from almanak.connectors.morpho_blue.liquidation import (
    Liquidation,
    liquidate,
    liquidation_amounts,
)
from almanak.connectors.morpho_blue.math import WAD
from almanak.models import Model


class LiquidationChange(Model):
    positions: tuple[Position, ...]
    market: MarketChange
    liquidation: Liquidation


def quantities(account):
    price = account.market.price
    if price is None:
        raise ValueError("modeled Morpho liquidation requires its configured oracle")
    if price.price == 0:
        return account.position.collateral, 0
    _, _, seized = liquidation_amounts(
        account.ledger.accounting,
        price.price,
        account.market.source.lltv,
        0,
        account.position.borrow_shares,
    )
    if seized > account.position.collateral:
        return account.position.collateral, 0
    return 0, account.position.borrow_shares


def apply(positions, market, ledger, fund, *, time: UnixTime) -> LiquidationChange | None:
    retained = ledger
    ledger = project(retained, time)
    account = account_view(positions, market, ledger, fund)
    health = account.health_factor
    if health is None or health >= WAD:
        return None
    seized, shares = quantities(account)
    assert market.price is not None
    result = liquidate(
        ledger.accounting,
        account.position,
        market.price.price,
        market.source.lltv,
        seized=seized,
        shares=shares,
    )
    after = MarketLedger(**(ledger.model_dump() | {"accounting": result.accounting}))
    updated = update_positions(positions, result.position)
    counted = tuple(count(position, result) for position in updated)
    return LiquidationChange(
        positions=counted, market=committed(retained, after), liquidation=result
    )


def count(position, result):
    changes = (
        {
            "liquidated_shares": position.liquidated_shares + result.repaid_shares,
            "bad_debt_shares": position.bad_debt_shares + result.bad_debt_shares,
        }
        if position.target.reserve.is_loan
        else {"seized_collateral": position.seized_collateral + result.seized_collateral}
    )
    return Position(**(position.model_dump() | changes))
