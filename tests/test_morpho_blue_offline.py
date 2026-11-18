"""Morpho exits, authority and modeled repayment built from literal market data."""

from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.backtest.types import UnixTime
from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.morpho_blue.authority import admit, declared
from almanak.connectors.morpho_blue.domain import (
    PARAMS_ABI,
    Accounting,
    AccountObservation,
    Market,
    MarketId,
    MarketObservation,
    OracleQuote,
    Position,
    Reserve,
    Target,
)
from almanak.connectors.morpho_blue.economic_actions import Change, apply, economic_operation
from almanak.connectors.morpho_blue.economic_market import latest, seed
from almanak.connectors.morpho_blue.economic_positions import Position as EconomicPosition
from almanak.connectors.morpho_blue.math import to_assets
from almanak.connectors.morpho_blue.models import Allocation, Parameters
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.planning import operation
from almanak.connectors.morpho_blue.risk import borrowed_assets, supplied_assets
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

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
CORE = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"
ORACLE = "0x" + "a1" * 20
IRM = "0x" + "a2" * 20
LLTV = 860_000_000_000_000_000
BORROWER = Account(chain=CHAIN, address=AccountReference("0x" + "11" * 20))
LENDER = Account(chain=CHAIN, address=AccountReference("0x" + "22" * 20))
# Morpho markets start at 10**6 shares per asset unit, the virtual share offset.
SHARES = 10**6


def market():
    draft = Market.model_construct(
        chain=CHAIN, loan_asset=USDC, collateral_asset=WETH, oracle=ORACLE, irm=IRM, lltv=LLTV
    )
    identity = MarketId("0x" + keccak(encode([PARAMS_ABI], [draft.parameters])).hex())
    return Market(
        chain=CHAIN,
        core=CORE,
        market_id=identity,
        loan_asset=USDC,
        collateral_asset=WETH,
        oracle=ORACLE,
        irm=IRM,
        lltv=LLTV,
    )


MARKET = market()


@pytest.fixture
def config():
    scopes = {
        "collateral": ("borrower", WETH, 10**18, False),
        "loan": ("borrower", USDC, 0, True),
        "earn": ("lender", USDC, 1000 * 10**6, False),
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
                reserve=Reserve(market=MARKET, asset=asset),
                capital=capital,
                borrow=borrow,
                minimum_health_factor=2 * 10**18,
                native_asset=native_asset(CHAIN.chain_id),
                minimum_native_balance=0,
            )
            for name, (account, asset, capital, borrow) in scopes.items()
        ),
    )


def observation():
    block = Block(chain=CHAIN, number=100, hash="0x" + "ab" * 32, timestamp=1_700_000_000)
    return MarketObservation(
        source=MARKET,
        block=block,
        contracts=tuple(
            ContractIdentity(address=address, code_hash="0x" + "cd" * 32)
            for address in (CORE, IRM, ORACLE)
        ),
        loan_asset=Asset(chain=CHAIN, identifier=USDC, decimals=6),
        collateral_asset=Asset(chain=CHAIN, identifier=WETH, decimals=18),
        accounting=Accounting(
            supply_assets=10_000_000 * 10**6,
            supply_shares=10_000_000 * 10**6 * SHARES,
            borrow_assets=5_000_000 * 10**6,
            borrow_shares=5_000_000 * 10**6 * SHARES,
            last_update=block.timestamp,
            fee=0,
        ),
        # About four percent a year, in wad per second.
        rate_at_target=1_268_391_679,
        borrow_rate=1_268_391_679,
        fee_recipient="0x" + "99" * 20,
        core_loan_balance=5_000_000 * 10**6,
        # 1 WETH (10**18 units) is worth 2000 USDC (2000 * 10**6 units), scaled by 10**36.
        price=OracleQuote(source=MARKET, block=block, price=2000 * 10**24),
    )


@pytest.fixture
def model(config):
    observed = observation()
    ledger = seed(observed, UnixTime(observed.block.timestamp))
    positions = tuple(
        EconomicPosition(
            target=config.target(a.name), supply_shares=0, borrow_shares=0, collateral=0
        )
        for a in config.allocations
    )
    return observed, ledger, positions


def execute(config, model, intent, inventory, *, time=None):
    observed, ledger, positions = model
    group = tuple(p for p in positions if p.target.fund == intent.target.fund)
    change = apply(
        config,
        intent,
        group,
        observed,
        ledger,
        inventory,
        time=ledger.effective_at if time is None else time,
    )
    assert latest(ledger, (change.market,)) == change.market.after
    assert Change.model_validate_json(change.model_dump_json()) == change
    changed = {p.target: p for p in change.positions}
    after = tuple(changed.get(p.target, p) for p in positions)
    return (observed, change.market.after, after), change


def test_repayment_requires_borrowing_authority_not_an_amount(config):
    capabilities = declared(config)
    admit(capabilities, config.repay("loan"))
    admit(capabilities, config.repay_amount("loan", 10**12))
    for name in ("earn", "collateral"):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(capabilities, config.repay(name))


def test_supply_and_withdrawal_are_always_admitted_without_an_amount_bound(config):
    capabilities = declared(config)
    for intent in (
        config.withdraw("earn"),
        config.withdraw("loan"),
        config.withdraw_amount("collateral", 10**30),
        config.supply("collateral", 10**30),
        config.supply("loan", 1),
        config.borrow("loan", 10**30),
    ):
        admit(capabilities, intent)
    with pytest.raises(ValueError, match="require a borrowing allocation"):
        admit(capabilities, config.borrow("earn", 1))
    foreign = config.target("earn").model_copy(update={"allocation": "other"})
    with pytest.raises(ValueError, match="exactly one retained target capability"):
        admit(
            capabilities,
            SupplyIntent[Target](target=foreign, amount=AssetAmount(asset=USDC, amount=1)),
        )


def test_modeled_round_trip_repays_every_debt_share_and_closes_positions(config, model):
    model, supplied = execute(config, model, config.supply("earn", 1000 * 10**6), 1000 * 10**6)
    assert supplied.inventory_delta == -1000 * 10**6
    with pytest.raises(ValueError, match="requires owned debt shares"):
        execute(config, model, config.repay("loan"), 10**6)
    model, _ = execute(config, model, config.supply("collateral", 10**18), 10**18)
    model, borrowed = execute(config, model, config.borrow("loan", 100 * 10**6), 0)
    assert borrowed.inventory_delta == 100 * 10**6
    ledger = model[1]
    model, partial = execute(
        config,
        model,
        config.repay_amount("loan", 10**6),
        10**6,
        time=UnixTime(ledger.effective_at + 86400),
    )
    assert partial.inventory_delta == -(10**6)
    loan = next(p for p in model[2] if p.target.allocation == "loan")
    totals = model[1].accounting
    debt = to_assets(loan.borrow_shares, totals.borrow_assets, totals.borrow_shares, round_up=True)
    # A day of interest leaves more than the 99 USDC principal still owed.
    assert debt > 99 * 10**6
    with pytest.raises(ValueError, match="partial repayment exceeds the owned debt"):
        execute(config, model, config.repay_amount("loan", debt + 1), debt + 1)
    with pytest.raises(ValueError, match="missing fund inventory"):
        execute(config, model, config.repay("loan"), debt - 1)
    model, repaid = execute(config, model, config.repay("loan"), debt)
    assert repaid.assets == debt and repaid.inventory_delta == -debt
    loan = next(p for p in model[2] if p.target.allocation == "loan")
    assert loan.borrow_shares == 0 and loan.repaid == 10**6 + debt
    model, _ = execute(config, model, config.withdraw_amount("collateral", 10**17), 0)
    model, collateral = execute(config, model, config.withdraw("collateral"), 0)
    assert collateral.inventory_delta == 9 * 10**17
    model, _ = execute(config, model, config.withdraw_amount("earn", 10**6), 0)
    model, earned = execute(config, model, config.withdraw("earn"), 0)
    assert earned.inventory_delta >= 999 * 10**6
    assert all(position.empty for position in model[2])
    # Scope-only authority: a second entry above the starting capital is admitted.
    model, again = execute(config, model, config.supply("earn", 2000 * 10**6), 2000 * 10**6)
    assert again.inventory_delta == -2000 * 10**6


def test_modeled_entries_beyond_the_starting_capital_are_not_refused(config, model):
    model, _ = execute(config, model, config.supply("collateral", 3 * 10**18), 3 * 10**18)
    model, borrowed = execute(config, model, config.borrow("loan", 1000 * 10**6), 0)
    assert borrowed.inventory_delta == 1000 * 10**6
    with pytest.raises(ValueError, match="borrowing allocation"):
        execute(config, model, config.borrow("earn", 1), 0)


def test_modeled_actions_refuse_scope_inventory_and_health_faults(config, model):
    observed, ledger, positions = model
    time = ledger.effective_at
    with pytest.raises(ValueError, match="health floor"):
        execute(config, model, config.borrow("loan", 10**6), 0)
    with pytest.raises(ValueError, match="missing fund inventory"):
        execute(config, model, config.supply("collateral", 10**18), 10**18 - 1)
    intent = config.supply("collateral", 1)
    borrower = positions[:2]
    moved = intent.model_copy(
        update={"target": intent.target.model_copy(update={"account": "lender"})}
    )
    with pytest.raises(ValueError, match="configured target"):
        apply(config, moved, borrower, observed, ledger, 1, time=time)
    with pytest.raises(ValueError, match="owned position and inventory"):
        apply(config, intent, borrower, observed, ledger, -1, time=time)
    with pytest.raises(ValueError, match="owned position and inventory"):
        apply(config, intent, positions[1:2], observed, ledger, 1, time=time)
    with pytest.raises(ValueError, match="every configured"):
        apply(config, intent, positions[:1], observed, ledger, 1, time=time)


def test_economic_operation_takes_full_quantities_from_the_modeled_account(config):
    account = SimpleNamespace(
        position=Position(supply_shares=7 * SHARES, borrow_shares=5 * SHARES, collateral=3),
        borrowed_assets=6,
        supplied_assets=8,
    )
    full = economic_operation(config.repay("loan"), account)
    assert isinstance(full, RepayOperation) and full.full
    assert (full.amount.amount, full.shares) == (6, 5 * SHARES)
    partial = economic_operation(config.repay_amount("loan", 6), account)
    assert not partial.full and partial.amount.amount == 6
    with pytest.raises(ValueError, match="exceeds the owned debt"):
        economic_operation(config.repay_amount("loan", 7), account)
    lender = economic_operation(config.withdraw("earn"), account)
    assert isinstance(lender, WithdrawOperation) and lender.full
    assert (lender.amount.amount, lender.shares) == (8, 7 * SHARES)
    collateral = economic_operation(config.withdraw("collateral"), account)
    assert (collateral.amount.amount, collateral.shares) == (3, 0)
    some = economic_operation(config.withdraw_amount("earn", 2), account)
    assert not some.full and (some.amount.amount, some.shares) == (2, 0)
    assert isinstance(economic_operation(config.supply("earn", 1), account), SupplyOperation)
    assert isinstance(economic_operation(config.borrow("loan", 1), account), BorrowOperation)
    with pytest.raises(ValueError, match="unsupported"):
        economic_operation(
            CollateralIntent[Target](target=config.target("collateral"), enabled=True), account
        )


def snapshot(config, name, *, supply=0, borrow=0, collateral=0, allowance=0, wallet=10**9):
    observed = observation()
    account = AccountObservation(
        market=observed,
        fund=config.target(name).fund,
        position=Position(supply_shares=supply, borrow_shares=borrow, collateral=collateral),
    )
    return SimpleNamespace(
        target=config.target(name),
        account=account,
        block=observed.block,
        allowance=allowance,
        wallet_balance=wallet,
    )


def retained(config, intent):
    return SimpleNamespace(
        capabilities=declared(config),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=(),
    )


def plan(config, intent, before):
    return operation(config, retained(config, intent), intent, before)


def test_full_repayment_approves_the_held_balance_and_repays_every_share(config):
    intent = config.repay("loan")
    before = snapshot(config, "loan", borrow=100 * 10**6 * SHARES)
    debt = borrowed_assets(before.account, before.block.timestamp)
    assert debt == 100 * 10**6
    approval = plan(config, intent, before)
    assert isinstance(approval, ApproveOperation) and approval.purpose == "repay"
    assert approval.amount.amount == before.wallet_balance
    before.allowance = approval.amount.amount
    repayment = plan(config, intent, before)
    assert isinstance(repayment, RepayOperation)
    assert repayment.shares == 100 * 10**6 * SHARES and repayment.amount == approval.amount
    before.allowance = debt - 1
    assert isinstance(plan(config, intent, before), RevokeOperation)
    before.wallet_balance = debt - 3
    with pytest.raises(DataError, match="needs 3 more") as refused:
        plan(config, intent, before)
    assert refused.value.kind == "repayment_funding"


def test_partial_repayment_is_bounded_by_the_observed_debt(config):
    before = snapshot(config, "loan", borrow=100 * 10**6 * SHARES)
    above = config.repay_amount("loan", 100 * 10**6 + 1)
    with pytest.raises(DataError, match="exceeds the owned debt") as refused:
        plan(config, above, before)
    assert refused.value.kind == "repayment_exceeds_debt"
    exact = config.repay_amount("loan", 100 * 10**6)
    assert isinstance(plan(config, exact, before), ApproveOperation)
    before.allowance = 100 * 10**6
    repayment = plan(config, exact, before)
    assert isinstance(repayment, RepayOperation) and not repayment.full
    before.allowance += 1
    with pytest.raises(DataError, match="allowance differs") as refused:
        plan(config, exact, before)
    assert refused.value.kind == "allowance_mismatch"


def test_exits_withdraw_owned_units_and_closed_positions_only_revoke(config):
    earn = snapshot(config, "earn", supply=1000 * 10**6 * SHARES)
    full = plan(config, config.withdraw("earn"), earn)
    assert isinstance(full, WithdrawOperation) and full.full
    assert full.shares == 1000 * 10**6 * SHARES
    assert full.amount.amount == supplied_assets(earn.account) == 1000 * 10**6
    some = plan(config, config.withdraw_amount("earn", 5), earn)
    assert not some.full and some.amount.amount == 5 and some.shares == 0
    held = snapshot(config, "collateral", collateral=10**18)
    collateral = plan(config, config.withdraw("collateral"), held)
    assert collateral.full and collateral.amount.amount == 10**18 and collateral.shares == 0
    for intent, name in (
        (config.withdraw("earn"), "earn"),
        (config.withdraw("collateral"), "collateral"),
        (config.repay("loan"), "loan"),
    ):
        before = snapshot(config, name, allowance=3)
        assert isinstance(plan(config, intent, before), RevokeOperation)
        before.allowance = 0
        with pytest.raises(DataError, match="closed") as refused:
            plan(config, intent, before)
        assert refused.value.kind == "no_position"


def test_planning_refuses_scope_and_replays_and_routes_entries(config):
    supply = config.supply("collateral", 10**18)
    before = snapshot(config, "collateral")
    with pytest.raises(ValueError, match="target"):
        plan(config, supply, snapshot(config, "earn"))
    missing = retained(config, supply)
    missing.capabilities = None
    with pytest.raises(ValueError, match="capabilities"):
        operation(config, missing, supply, before)
    with pytest.raises(ValueError, match="retained economic decision"):
        operation(config, retained(config, config.supply("collateral", 1)), supply, before)
    with pytest.raises(ValueError, match="borrowing allocation"):
        plan(config, config.repay("earn"), snapshot(config, "earn"))
    approval = plan(config, supply, before)
    assert isinstance(approval, ApproveOperation) and approval.purpose == "supply"
    before.allowance = 10**18
    assert isinstance(plan(config, supply, before), SupplyOperation)
    borrow = config.borrow("loan", 10**6)
    result = plan(config, borrow, snapshot(config, "loan"))
    assert isinstance(result, BorrowOperation) and result.amount == borrow.amount
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
