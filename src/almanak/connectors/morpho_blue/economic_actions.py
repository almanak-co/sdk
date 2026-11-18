"""Offline Morpho actions use the same integer transitions as contract comparisons."""

from almanak.backtest.types import UnixTime
from almanak.connectors.lending_health import health_floor
from almanak.connectors.morpho_blue.authority import admit, declared
from almanak.connectors.morpho_blue.economic_account import AccountView
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
from almanak.connectors.morpho_blue.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.transitions import transition
from almanak.lending_intents import (
    BorrowIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount, Model, UInt


class Change(Model):
    positions: tuple[Position, ...]
    market: MarketChange
    inventory_delta: int
    assets: UInt


def economic_operation(intent, account):
    target = intent.target
    if isinstance(intent, SupplyIntent):
        return SupplyOperation(target=target, amount=intent.amount)
    if isinstance(intent, BorrowIntent):
        return BorrowOperation(target=target, amount=intent.amount)
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        if not account.position.borrow_shares:
            raise ValueError("modeled Morpho repayment requires owned debt shares")
        if isinstance(intent, RepayIntent):
            debt = AssetAmount(asset=target.asset, amount=account.borrowed_assets)
            return RepayOperation(target=target, amount=debt, shares=account.position.borrow_shares)
        if intent.amount.amount > account.borrowed_assets:
            raise ValueError("modeled Morpho partial repayment exceeds the owned debt")
        return RepayOperation(target=target, amount=intent.amount)
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        full = isinstance(intent, WithdrawIntent)
        quantity = (
            account.supplied_assets if target.reserve.is_loan else account.position.collateral
        )
        return WithdrawOperation(
            target=target,
            amount=AssetAmount(asset=target.asset, amount=quantity)
            if isinstance(intent, WithdrawIntent)
            else intent.amount,
            full=full,
            shares=account.position.supply_shares if full and target.reserve.is_loan else 0,
        )
    raise ValueError("unsupported modeled Morpho intent")


def count_flows(position, operation, quantity, allocation):
    field = {
        "supply": "supplied",
        "withdraw": "withdrawn",
        "borrow": "borrowed",
        "repay": "repaid",
    }[operation.kind]
    after = Position(**(position.model_dump() | {field: getattr(position, field) + quantity}))
    if isinstance(operation, BorrowOperation) and not allocation.borrow:
        raise ValueError("modeled Morpho borrow is outside the allocation's authority")
    if isinstance(operation, RepayOperation) and quantity > operation.amount.amount:
        raise ValueError("modeled Morpho repayment exceeds its approved amount")
    return after


def check_risk(operation, after: AccountView, allocation):
    needs = isinstance(operation, BorrowOperation) or (
        isinstance(operation, WithdrawOperation) and not operation.target.reserve.is_loan
    )
    if needs:
        health = after.health_factor
        if health is not None and health < health_floor(allocation.minimum_health_factor):
            raise ValueError("modeled Morpho action violates the configured health floor")


def apply(config, intent, positions, market, ledger, inventory, *, time: UnixTime):
    retained = ledger
    ledger = project(retained, time)
    if config.target(intent.target.allocation) != intent.target:
        raise ValueError("modeled Morpho intent differs from its configured target")
    admit(declared(config), intent)
    if inventory < 0 or not any(p.target == intent.target for p in positions):
        raise ValueError("modeled Morpho action requires owned position and inventory")
    expected = {
        config.target(a.name)
        for a in config.allocations
        if config.target(a.name).fund == intent.target.fund and a.reserve.market == ledger.source
    }
    if {p.target for p in positions} != expected:
        raise ValueError("modeled Morpho action requires every configured fund/market position")
    before = account_view(positions, market, ledger, intent.target.fund)
    allocation = config.allocation(intent.target.allocation)
    operation = economic_operation(intent, before)
    result = transition(ledger.accounting, before.position, operation)
    delta = (
        -result.assets
        if isinstance(operation, (SupplyOperation, RepayOperation))
        else result.assets
    )
    if inventory + delta < 0:
        raise ValueError("modeled Morpho action cannot spend missing fund inventory")
    after_ledger = MarketLedger(**(ledger.model_dump() | {"accounting": result.accounting}))
    after = AccountView(
        **(before.model_dump() | {"ledger": after_ledger, "position": result.position})
    )
    check_risk(operation, after, allocation)
    updated = update_positions(positions, result.position)
    updated = tuple(
        count_flows(p, operation, result.assets, allocation) if p.target == intent.target else p
        for p in updated
    )
    return Change(
        positions=updated,
        market=committed(retained, after_ledger),
        inventory_delta=delta,
        assets=result.assets,
    )
