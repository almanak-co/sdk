"""Comet simulation views preserve raw market inputs beside modeled fund balances."""

from almanak.connectors.compound_v3.domain import AccountObservation, CollateralBalance
from almanak.connectors.compound_v3.economic_data import EconomicMarket
from almanak.connectors.compound_v3.economic_state import Position
from almanak.models import Account


def account_view(positions: tuple[Position, ...], market: EconomicMarket, fund: Account):
    scoped = tuple(
        p
        for p in positions
        if p.target.fund == fund and p.target.reserve.market == market.observed.source
    )
    if len({p.target.asset for p in scoped}) != len(scoped):
        raise ValueError("economic Comet fund positions alias the same asset balance")
    if fund.chain != market.observed.source.chain:
        raise ValueError("economic Comet fund differs from its market chain")
    principal = next((p.principal for p in scoped if p.target.reserve.is_base), 0)
    collateral = {p.target.asset: p for p in scoped if not p.target.reserve.is_base}
    assets = {c.asset_type for c in market.observed.collaterals}
    if not collateral.keys() <= assets:
        raise ValueError("economic Comet ownership includes an unsupported collateral")
    bitmap = sum(
        1 << c.offset
        for c in market.observed.collaterals
        if c.asset_type in collateral and collateral[c.asset_type].collateral_enabled
    )
    balance = market.balance(principal)
    return AccountObservation(
        market=market.observed,
        fund=fund,
        principal=principal,
        tracking_index=0,
        tracking_accrued=0,
        assets_in=bitmap & 0xFFFF,
        extended_assets_in=bitmap >> 16,
        supplied_base=max(0, balance),
        borrowed_base=max(0, -balance),
        collaterals=tuple(
            CollateralBalance(
                asset=c.asset_type,
                balance=collateral[c.asset_type].collateral if c.asset_type in collateral else 0,
            )
            for c in market.observed.collaterals
        ),
    )


def risk_account(account: AccountObservation, market: EconomicMarket):
    if account.market != market.observed:
        raise ValueError("economic Comet risk arithmetic has another market basis")
    # Protocol arithmetic consumes indices from an account's market. This private
    # adapter is never retained as an observed historical market or strategy snapshot.
    accounting = market.observed.accounting.model_copy(
        update={
            "supply_index": market.supply_index,
            "borrow_index": market.borrow_index,
        }
    )
    return account.model_copy(
        update={"market": market.observed.model_copy(update={"accounting": accounting})}
    )
