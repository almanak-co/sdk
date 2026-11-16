"""Comet exits, authority and funding checks built from literal market data."""

from types import SimpleNamespace

import pytest
from eth_abi import decode

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.compound_v3.authority import admit, declared
from almanak.connectors.compound_v3.capacity import funding
from almanak.connectors.compound_v3.domain import (
    Accounting,
    CollateralBalance,
    CollateralObservation,
    Market,
    MarketObservation,
    OracleQuote,
    RateCurve,
    Reserve,
    Revision,
    Target,
)
from almanak.connectors.compound_v3.economic_actions import (
    apply,
    changed_position,
    economic_operation,
)
from almanak.connectors.compound_v3.economic_data import EconomicMarket, RiskSample
from almanak.connectors.compound_v3.economic_state import Position
from almanak.connectors.compound_v3.models import Allocation, Parameters
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.planning import exit_operation, operation
from almanak.connectors.compound_v3.transitions import base_amount
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.gateway.permission_models import ContractIdentity
from almanak.lending_intents import CollateralIntent, SupplyIntent
from almanak.models import (
    Account,
    AccountBinding,
    Asset,
    AssetAmount,
    Block,
    Chain,
    Eoa,
    StrategyAccounts,
)
from almanak.triggers import IntervalTrigger
from tests.strategy_fork.compound_authority import forbidden_calls

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
COMET = "0xc3d688b66703497daa19211eedff47f25384cdc3"
BASE_FEED = "0x" + "a1" * 20
WETH_FEED = "0x" + "a2" * 20
BORROWER = Account(chain=CHAIN, address=AccountReference("0x" + "11" * 20))
LENDER = Account(chain=CHAIN, address=AccountReference("0x" + "22" * 20))
MARKET = Market(chain=CHAIN, comet=COMET, base_asset=USDC, base_price_feed=BASE_FEED)
INDEX = 10**15
# Accrued indices of 1.01 (supply) and 1.02 (borrow) make interest visible in base units.
SUPPLY_INDEX = 101 * 10**13
BORROW_INDEX = 102 * 10**13
USD = 10**8


def parameters():
    reserves = {
        "collateral": Reserve(market=MARKET, asset=WETH, price_feed=WETH_FEED),
        "loan": Reserve(market=MARKET, asset=USDC, price_feed=BASE_FEED),
        "earn": Reserve(market=MARKET, asset=USDC, price_feed=BASE_FEED),
    }
    scopes = {
        "collateral": ("borrower", 10**18, False),
        "loan": ("borrower", 0, True),
        "earn": ("lender", 1000 * 10**6, False),
    }
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=StrategyAccounts(
            bindings=(
                AccountBinding(name="borrower", profile=Eoa(kind="eoa", fund=BORROWER)),
                AccountBinding(name="lender", profile=Eoa(kind="eoa", fund=LENDER)),
            )
        ),
        allocations=tuple(
            Allocation(
                name=name,
                account=account,
                reserve=reserves[name],
                capital=capital,
                borrow=borrow,
                minimum_health_factor=2 * 10**18,
                native_asset=native_asset(CHAIN.chain_id),
                minimum_native_balance=0,
            )
            for name, (account, capital, borrow) in scopes.items()
        ),
    )


@pytest.fixture
def config():
    return parameters()


def observation(
    *,
    weth_price=2000 * USD,
    supply_cap=1000 * 10**18,
    base_available=5_000_000 * 10**6,
    base_borrow_minimum=10**6,
    pause_flags=0,
):
    block = Block(chain=CHAIN, number=100, hash="0x" + "ab" * 32, timestamp=1_700_000_000)

    def quote(asset, feed, price):
        return OracleQuote(block=block, asset=asset, comet=COMET, feed=feed, price=price)

    curve = RateCurve(
        kink=8 * 10**17, base_per_second=0, slope_low_per_second=10**9, slope_high_per_second=10**10
    )
    return MarketObservation(
        source=MARKET,
        block=block,
        revision=Revision.STANDARD,
        contracts=tuple(
            ContractIdentity(address=address, code_hash="0x" + "cd" * 32)
            for address in (COMET, "0x" + "b1" * 20, "0x" + "b2" * 20, "0x" + "b3" * 20)
        ),
        base_asset=Asset(chain=CHAIN, identifier=USDC, decimals=6),
        accounting=Accounting(
            supply_index=INDEX,
            borrow_index=INDEX,
            tracking_supply_index=0,
            tracking_borrow_index=0,
            total_supply_principal=10_000_000 * 10**6,
            total_borrow_principal=5_000_000 * 10**6,
            last_accrual_time=block.timestamp,
            pause_flags=pause_flags,
        ),
        controls=None,
        supply_curve=curve,
        borrow_curve=curve,
        base_borrow_minimum=base_borrow_minimum,
        base_available=base_available,
        base_price=quote(USDC, BASE_FEED, USD),
        collaterals=(
            CollateralObservation(
                asset=Asset(chain=CHAIN, identifier=WETH, decimals=18),
                offset=0,
                price_feed=WETH_FEED,
                scale=10**18,
                borrow_factor=8 * 10**17,
                liquidate_factor=85 * 10**16,
                liquidation_factor=93 * 10**16,
                supply_cap=supply_cap,
                total_supply=100 * 10**18,
                price=quote(WETH, WETH_FEED, weth_price),
            ),
        ),
    )


def economic_market(**changes):
    observed = observation(**changes)
    block = observed.block
    sample = RiskSample(
        market=observed,
        block_group=(block,),
        following_block=block.model_copy(
            update={"number": block.number + 1, "timestamp": block.timestamp + 12}
        ),
    )
    return EconomicMarket(sample, SUPPLY_INDEX, BORROW_INDEX, block.timestamp)


def empty(config, *names):
    return tuple(Position(target=config.target(n), principal=0, collateral=0) for n in names)


def step(config, intent, positions, market, inventory):
    return apply(config, intent, positions, market, inventory, positions)


def test_base_amount_keeps_signed_principal_from_crossing_zero(config):
    loan, earn = config.target("loan"), config.target("earn")

    def amount(target, value):
        return AssetAmount(asset=target.asset, amount=value)

    indices = (SUPPLY_INDEX, BORROW_INDEX)
    supply = SupplyOperation(target=earn, amount=amount(earn, 100))
    assert base_amount(0, supply, indices) == (100, 100)
    with pytest.raises(ValueError, match="silently repay"):
        base_amount(-100, supply, indices)
    borrow = BorrowOperation(target=loan, amount=amount(loan, 100))
    assert base_amount(0, borrow, indices) == (-100, 100)
    with pytest.raises(ValueError, match="silently withdraw"):
        base_amount(100, borrow, indices)
    # Principal -1000 at a 1.02 borrow index is a debt of 1020 base units.
    full = RepayOperation(target=loan, amount=amount(loan, 1020), full=True)
    assert base_amount(-1000, full, indices) == (0, 1020)
    with pytest.raises(ValueError, match="exceeds the debt or its approved amount"):
        base_amount(-1000, full.model_copy(update={"amount": amount(loan, 1019)}), indices)
    partial = RepayOperation(target=loan, amount=amount(loan, 20), full=False)
    assert base_amount(-1000, partial, indices) == (-1000, 20)
    with pytest.raises(ValueError, match="exceeds the debt"):
        base_amount(-1000, partial.model_copy(update={"amount": amount(loan, 1021)}), indices)
    with pytest.raises(ValueError, match="requires existing base debt"):
        base_amount(0, partial, indices)
    # Principal 1000 at a 1.01 supply index is an owned balance of 1010 base units.
    withdraw = WithdrawOperation(target=earn, amount=amount(earn, 1), full=True)
    assert base_amount(1000, withdraw, indices) == (0, 1010)
    some = WithdrawOperation(target=earn, amount=amount(earn, 10), full=False)
    assert base_amount(1000, some, indices) == (1000, 10)
    with pytest.raises(ValueError, match="exceeds its owned base supply"):
        base_amount(1000, some.model_copy(update={"amount": amount(earn, 1011)}), indices)
    with pytest.raises(ValueError, match="requires existing base supply"):
        base_amount(0, some, indices)
    with pytest.raises(ValueError, match="economic operation"):
        base_amount(
            0, ApproveOperation(target=earn, amount=amount(earn, 1), purpose="supply"), indices
        )


def test_repayment_requires_borrowing_authority_not_an_amount(config):
    capabilities = declared(config)
    # Repaying the fund's own debt has no amount bound.
    admit(capabilities, config.repay("loan"))
    admit(capabilities, config.repay_amount("loan", 10**12))
    for name in ("earn", "collateral"):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(capabilities, config.repay(name))


def test_supply_and_withdrawal_admit_any_amount_and_borrowing_needs_authority(config):
    capabilities = declared(config)
    # Amounts are the strategy's decision; capital is funding, not a limit.
    for intent in (
        config.withdraw("earn"),
        config.withdraw_amount("collateral", 10**30),
        config.withdraw("loan"),
        config.withdraw_amount("loan", 1),
        config.supply("collateral", 10**18 + 1),
        config.supply("loan", 1),
        config.supply("earn", 10**30),
        config.borrow("loan", 10**30),
    ):
        admit(capabilities, intent)
    with pytest.raises(ValueError, match="require a borrowing allocation"):
        admit(capabilities, config.borrow("earn", 1))
    foreign = config.target("earn").model_copy(update={"allocation": "other"})
    with pytest.raises(ValueError, match="exactly one retained reserve capability"):
        admit(
            capabilities,
            SupplyIntent[Target](target=foreign, amount=AssetAmount(asset=USDC, amount=1)),
        )


def test_borrower_round_trip_repays_the_accrued_debt_and_releases_collateral(config):
    market = economic_market()
    positions = empty(config, "loan", "collateral")
    supplied = step(config, config.supply("collateral", 10**18), positions, market, 10**18)
    assert supplied.inventory_delta == -(10**18) and supplied.collateral_delta == 10**18
    assert supplied.positions[1].collateral_enabled
    borrowed = step(config, config.borrow("loan", 100 * 10**6), supplied.positions, market, 0)
    assert borrowed.inventory_delta == 100 * 10**6 and borrowed.positions[0].borrowed == 100 * 10**6
    assert borrowed.positions[0].principal < 0
    paid = step(
        config, config.repay_amount("loan", 40 * 10**6), borrowed.positions, market, 40 * 10**6
    )
    assert paid.inventory_delta == -40 * 10**6 and paid.positions[0].principal < 0
    assert -market.balance(paid.positions[0].principal) == 60 * 10**6
    # A day later the borrow index has accrued, so the full repayment exceeds the principal paid.
    later = EconomicMarket(market.sample, SUPPLY_INDEX, 103 * 10**13, market.time + 86400)
    debt = -later.balance(paid.positions[0].principal)
    assert debt > 60 * 10**6
    with pytest.raises(ValueError, match="exceeds the debt"):
        step(config, config.repay_amount("loan", debt + 1), paid.positions, later, debt + 1)
    with pytest.raises(ValueError, match="missing fund inventory"):
        step(config, config.repay("loan"), paid.positions, later, debt - 1)
    full = step(config, config.repay("loan"), paid.positions, later, debt)
    assert full.positions[0].principal == 0 and full.inventory_delta == -debt
    assert full.assets == debt and full.positions[0].repaid == 40 * 10**6 + debt
    released = step(config, config.withdraw("collateral"), full.positions, later, 0)
    assert released.inventory_delta == 10**18
    assert released.positions[1].collateral == 0 and not released.positions[1].collateral_enabled


def test_lender_partial_and_full_withdrawal_return_the_accrued_balance(config):
    market = economic_market()
    positions = empty(config, "earn")
    supplied = step(config, config.supply("earn", 500 * 10**6), positions, market, 500 * 10**6)
    assert supplied.inventory_delta == -500 * 10**6
    part = step(config, config.withdraw_amount("earn", 100 * 10**6), supplied.positions, market, 0)
    assert part.inventory_delta == 100 * 10**6
    balance = market.balance(part.positions[0].principal)
    with pytest.raises(ValueError, match="exceeds its owned base supply"):
        step(config, config.withdraw_amount("earn", balance + 1), part.positions, market, 0)
    closed = step(config, config.withdraw("earn"), part.positions, market, 0)
    assert closed.inventory_delta == balance and closed.positions[0].principal == 0
    # Re-entry beyond the configured capital is not refused; inventory sizes it.
    again = step(config, config.supply("earn", 2000 * 10**6), closed.positions, market, 10**10)
    assert again.inventory_delta == -2000 * 10**6


def test_economic_operation_takes_full_quantities_from_modeled_balances(config):
    market = economic_market()
    loan = Position(target=config.target("loan"), principal=-1000, collateral=0)
    repay = economic_operation(config.repay("loan"), loan, market)
    assert isinstance(repay, RepayOperation) and repay.full and repay.amount.amount == 1020
    partial = economic_operation(config.repay_amount("loan", 5), loan, market)
    assert not partial.full and partial.amount.amount == 5
    earn = Position(target=config.target("earn"), principal=1000, collateral=0)
    withdraw = economic_operation(config.withdraw("earn"), earn, market)
    assert withdraw.full and withdraw.amount.amount == 1010
    collateral = Position(target=config.target("collateral"), principal=0, collateral=7)
    assert economic_operation(config.withdraw("collateral"), collateral, market).amount.amount == 7
    some = economic_operation(config.withdraw_amount("collateral", 3), collateral, market)
    assert not some.full and some.amount.amount == 3
    assert isinstance(economic_operation(config.borrow("loan", 1), loan, market), BorrowOperation)
    with pytest.raises(ValueError, match="unsupported"):
        economic_operation(
            CollateralIntent[Target](target=config.target("collateral"), enabled=True),
            loan,
            market,
        )


def test_economic_borrowing_refuses_missing_collateral_health_and_market_limits(config):
    positions = empty(config, "loan", "collateral")
    with pytest.raises(ValueError, match="collateral cannot cover"):
        step(config, config.borrow("loan", 10**6), positions, economic_market(), 0)
    for changes, message in (
        ({"weth_price": 150 * USD}, "health is below"),
        ({"base_available": 0}, "exceeds market inventory"),
        ({"base_borrow_minimum": 200 * 10**6}, "below the market borrow minimum"),
    ):
        market = economic_market(**changes)
        held = step(config, config.supply("collateral", 10**18), positions, market, 10**18)
        with pytest.raises(ValueError, match=message):
            step(config, config.borrow("loan", 100 * 10**6), held.positions, market, 0)
    market = economic_market()
    held = step(config, config.supply("collateral", 10**18), positions, market, 10**18)
    first = step(config, config.borrow("loan", 150 * 10**6), held.positions, market, 0)
    # A second borrow has no cumulative budget; health and market limits still apply.
    second = step(config, config.borrow("loan", 150 * 10**6), first.positions, market, 0)
    assert second.positions[0].borrowed == 300 * 10**6
    with pytest.raises(ValueError, match="health is below"):
        step(config, config.borrow("loan", 600 * 10**6), second.positions, market, 0)


def test_economic_borrow_without_borrowing_authority_is_refused(config):
    market = economic_market()
    loan = Position(target=config.target("loan"), principal=0, collateral=0)
    lender = config.allocation("loan").model_copy(update={"borrow": False})
    borrow = BorrowOperation(target=loan.target, amount=AssetAmount(asset=USDC, amount=10**6))
    with pytest.raises(ValueError, match="outside the allocation's authority"):
        changed_position(loan, borrow, market, lender)


def test_economic_actions_refuse_scope_inventory_cap_and_pause_faults(config):
    market = economic_market()
    positions = empty(config, "loan", "collateral")
    intent = config.supply("collateral", 10**18)
    moved = intent.model_copy(
        update={"target": config.target("collateral").model_copy(update={"account": "lender"})}
    )
    with pytest.raises(ValueError, match="configured target"):
        step(config, moved, positions, market, 10**18)
    with pytest.raises(ValueError, match="owned position and inventory"):
        step(config, intent, positions[:1], market, 10**18)
    with pytest.raises(ValueError, match="owned position and inventory"):
        step(config, intent, positions, market, -1)
    with pytest.raises(ValueError, match="one exact fund/market group"):
        step(config, intent, positions + empty(config, "earn"), market, 10**18)
    with pytest.raises(ValueError, match="missing fund inventory"):
        step(config, intent, positions, market, 10**18 - 1)
    capped = economic_market(supply_cap=100 * 10**18)
    with pytest.raises(ValueError, match="market supply cap"):
        step(config, intent, positions, capped, 10**18)
    with pytest.raises(DataError, match="paused"):
        step(config, intent, positions, economic_market(pause_flags=1), 10**18)


def snapshot(config, name, *, principal=0, collateral=0, allowance=0, wallet=1000 * 10**6):
    balance = principal * BORROW_INDEX // INDEX if principal < 0 else principal
    return SimpleNamespace(
        target=config.target(name),
        allowance=allowance,
        wallet_balance=wallet,
        account=SimpleNamespace(
            principal=principal,
            supplied_base=max(0, balance),
            borrowed_base=max(0, -balance),
            collaterals=(CollateralBalance(asset=WETH, balance=collateral),),
        ),
    )


def retained(config, intent, outcomes=()):
    return SimpleNamespace(
        capabilities=declared(config),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=outcomes,
    )


def test_full_repayment_approves_the_held_balance_then_repays_with_the_sentinel(config):
    intent = config.repay("loan")
    before = snapshot(config, "loan", principal=-100 * 10**6)
    debt = before.account.borrowed_base
    approval = operation(config, retained(config, intent), intent, before)
    assert isinstance(approval, ApproveOperation) and approval.purpose == "repay"
    assert approval.amount.amount == before.wallet_balance
    before.allowance = approval.amount.amount
    repayment = operation(config, retained(config, intent), intent, before)
    assert isinstance(repayment, RepayOperation) and repayment.full
    assert repayment.amount == approval.amount
    before.allowance = debt - 1
    assert isinstance(operation(config, retained(config, intent), intent, before), RevokeOperation)
    before.wallet_balance = debt - 7
    with pytest.raises(DataError, match="needs 7 more") as refused:
        operation(config, retained(config, intent), intent, before)
    assert refused.value.kind == "repayment_funding"


def test_partial_repayment_is_bounded_by_the_observed_debt(config):
    before = snapshot(config, "loan", principal=-100 * 10**6)
    debt = before.account.borrowed_base
    above = config.repay_amount("loan", debt + 1)
    with pytest.raises(DataError, match="exceeds the owned debt") as refused:
        operation(config, retained(config, above), above, before)
    assert refused.value.kind == "repayment_exceeds_debt"
    exact = config.repay_amount("loan", debt)
    approval = operation(config, retained(config, exact), exact, before)
    assert isinstance(approval, ApproveOperation) and approval.amount.amount == debt
    before.allowance = debt
    repayment = operation(config, retained(config, exact), exact, before)
    assert isinstance(repayment, RepayOperation) and not repayment.full
    before.allowance = debt + 1
    with pytest.raises(DataError, match="allowance differs") as refused:
        operation(config, retained(config, exact), exact, before)
    assert refused.value.kind == "allowance_mismatch"


def test_exits_withdraw_owned_units_and_closed_positions_only_revoke(config):
    earn = snapshot(config, "earn", principal=100)
    result = exit_operation(config.withdraw("earn"), earn)
    assert isinstance(result, WithdrawOperation) and result.full and result.amount.amount == 100
    collateral = snapshot(config, "collateral", collateral=10**18)
    result = exit_operation(config.withdraw("collateral"), collateral)
    assert result.full and result.amount.amount == 10**18
    result = exit_operation(config.withdraw_amount("collateral", 5), collateral)
    assert not result.full and result.amount.amount == 5
    for intent, before in (
        (config.withdraw("earn"), snapshot(config, "earn")),
        (config.repay("loan"), snapshot(config, "loan")),
        (config.repay_amount("loan", 1), snapshot(config, "loan", principal=10)),
    ):
        before.allowance = 3
        assert isinstance(exit_operation(intent, before), RevokeOperation)
        before.allowance = 0
        with pytest.raises(DataError, match="closed") as refused:
            exit_operation(intent, before)
        assert refused.value.kind == "no_position"
    with pytest.raises(ValueError, match="unsupported"):
        exit_operation(config.supply("earn", 1), earn)


def test_planning_refuses_scope_and_replays_and_routes_entries(config):
    supply = config.supply("collateral", 10**18)
    before = snapshot(config, "collateral")
    with pytest.raises(ValueError, match="target"):
        operation(config, retained(config, supply), supply, snapshot(config, "earn"))
    missing = retained(config, supply)
    missing.capabilities = None
    with pytest.raises(ValueError, match="capabilities"):
        operation(config, missing, supply, before)
    with pytest.raises(ValueError, match="retained economic decision"):
        operation(config, retained(config, config.supply("collateral", 1)), supply, before)
    repay = config.repay("earn")
    with pytest.raises(ValueError, match="borrowing allocation"):
        operation(config, retained(config, repay), repay, snapshot(config, "earn"))
    approval = operation(config, retained(config, supply), supply, before)
    assert isinstance(approval, ApproveOperation) and approval.purpose == "supply"
    before.allowance = 10**18
    assert isinstance(operation(config, retained(config, supply), supply, before), SupplyOperation)
    borrow = config.borrow("loan", 10**6)
    loan = snapshot(config, "loan")
    assert operation(config, retained(config, borrow), borrow, loan).amount == borrow.amount
    state = retained(config, supply)
    state.outcomes = (
        SimpleNamespace(
            success=True, decision=state.decision, operation=SimpleNamespace(kind="supply")
        ),
    )
    assert isinstance(operation(config, state, supply, before), RevokeOperation)
    before.allowance = 0
    with pytest.raises(DataError, match="again"):
        operation(config, state, supply, before)
    withdraw = config.withdraw("collateral")
    held = snapshot(config, "collateral", collateral=4)
    assert operation(config, retained(config, withdraw), withdraw, held).amount.amount == 4


def test_funding_requires_the_debt_or_amount_and_an_exact_allowance(config):
    loan, earn = config.target("loan"), config.target("earn")

    def before(wallet, allowance, debt=100):
        return SimpleNamespace(
            wallet_balance=wallet, allowance=allowance, account=SimpleNamespace(borrowed_base=debt)
        )

    def repay(amount, full):
        return RepayOperation(target=loan, amount=AssetAmount(asset=USDC, amount=amount), full=full)

    assert (
        funding(before(0, 0), BorrowOperation(target=loan, amount=repay(1, False).amount)) is None
    )
    funding(before(150, 150), repay(150, True))
    with pytest.raises(DataError, match="needs 1 more") as refused:
        funding(before(99, 150), repay(150, True))
    assert refused.value.kind == "repayment_funding"
    with pytest.raises(DataError, match="allowance covering") as refused:
        funding(before(150, 99), repay(99, True))
    assert refused.value.kind == "allowance_mismatch"
    funding(before(40, 40), repay(40, False))
    with pytest.raises(DataError, match="needs 1 more"):
        funding(before(39, 40), repay(40, False))
    with pytest.raises(DataError, match="allowance covering"):
        funding(before(40, 41), repay(40, False))
    supply = SupplyOperation(target=earn, amount=AssetAmount(asset=USDC, amount=10))
    funding(before(10, 10), supply)
    with pytest.raises(DataError, match="lacks inventory") as refused:
        funding(before(9, 10), supply)
    assert refused.value.kind == "funding_unavailable"


def bound(calldata):
    return decode(("address", "uint256"), bytes.fromhex(calldata[10:]))


def test_forbidden_comet_calls_probe_scope_without_amount_bounds(config):
    calls = {
        label: (target, data) for label, target, data, _, _ in forbidden_calls(config, "borrower")
    }
    assert not any("earn" in label for label in calls)
    # Scope-only grants admit any nonzero amount, including the sentinels, so no
    # probe sends an amount above a former bound.
    assert not any("above_bound" in label or "sentinel" in label for label in calls)
    for name in ("collateral", "loan"):
        for method in ("supply", "withdraw"):
            target, data = calls[f"{name}_{method}_zero"]
            assert target == COMET and bound(data)[1] == 0
            assert f"{name}_{method}_other_asset" in calls
        assert f"{name}_other_spender" in calls and f"{name}_supplyTo" in calls
    assert calls["manager_authority"][0] == COMET
    lender = [label for label, *_ in forbidden_calls(config, "lender")]
    assert lender[0] == "earn_supply_zero" and lender[-1] == "manager_authority"
