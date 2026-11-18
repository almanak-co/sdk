"""Supplemental actions over published chain-specific data, not lifecycle copies."""

import pytest

from almanak.backtest.economic_actions import apply_action
from almanak.backtest.economic_state import RefreshIntent
from almanak.dex_intents import RevokeExchange
from almanak.lending import SupplyTarget
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount


@pytest.mark.parametrize("chain", ["eip155:1", "eip155:43114"])
@pytest.mark.parametrize("connector", ["aave_v4", "uniswap_v4"])
def test_prepared_actions(tmp_path, connector, chain):
    if connector == "aave_v4":
        aave_actions(tmp_path, chain)
    else:
        uniswap_revoke(tmp_path, chain)


# The references borrow 5 USDC, which has six decimals on both chains.
BORROWED = 5_000_000


def aave_actions(directory, chain):
    from tests.aave_economic_support import prepared

    name = "ethereum-reference" if chain == "eip155:1" else "avalanche"
    manifest, _, model = prepared(directory, name)
    try:
        allocation, debt = manifest.config.allocations[0], manifest.config.debts[0]
        target, borrowed = (manifest.config.target(a.name) for a in (allocation, debt))
        group, markets = model.group(manifest.initial, target, manifest.initial.time)
        positions = tuple(p.payload for p in group)
        assert all(str(p.target.fund.chain.chain_id) == chain for p in positions)
        balances = {p.target: 0 for p in positions}
        balances[target] = allocation.capital
        # This component proof starts from the backtest configuration's explicit
        # loan-asset balance; with the borrowed proceeds it pays the observed debt.
        # It does not claim these actions came from the reference strategy.
        balances[borrowed] = configured_balance(manifest.settings, debt.account, borrowed.asset)
        before = dict(balances)
        actions = lending_actions(target, borrowed, allocation.capital, BORROWED)
        for intent in actions:
            changed = apply_action(intent, positions, markets, balances[intent.target])
            balances[intent.target] += changed.inventory_delta
            positions = changed.positions
        assert all(p.shares == p.drawn_shares == p.premium_shares == 0 for p in positions)
        for position in positions:
            t = position.target
            assert (
                balances[t]
                == before[t]
                - position.supplied
                + position.withdrawn
                + position.borrowed
                - position.repaid
            )
    finally:
        model.book.close()


def configured_balance(settings, account, asset):
    fund = next(f for f in settings.funds if f.name == account)
    return next(b.amount for b in fund.balances if b.asset == asset)


def lending_actions(target, debt, supply, borrowing):
    def amount(t, value):
        return AssetAmount(asset=t.asset, amount=value)

    return (
        SupplyIntent[SupplyTarget](target=target, amount=amount(target, supply)),
        CollateralIntent[SupplyTarget](target=target, enabled=True),
        BorrowIntent[SupplyTarget](target=debt, amount=amount(debt, borrowing)),
        RefreshIntent(target=target, dynamic_config=True),
        RepayAmountIntent[SupplyTarget](target=debt, amount=amount(debt, max(1, borrowing // 2))),
        RepayIntent[SupplyTarget](target=debt),
        CollateralIntent[SupplyTarget](target=target, enabled=False),
        WithdrawAmountIntent[SupplyTarget](
            target=target, amount=amount(target, max(1, supply // 2))
        ),
        WithdrawIntent[SupplyTarget](target=target),
    )


def uniswap_revoke(directory, chain):
    from tests.uniswap_economic_support import prepared

    manifest, expected, model = prepared(
        directory, "ethereum" if chain == "eip155:1" else "avalanche"
    )
    try:
        prior = expected.completed[0].pending
        intent = RevokeExchange[type(prior.intent.target)](target=prior.intent.target)
        pending = prior.model_copy(update={"intent": intent})
        change = model.execute(manifest.initial, pending)
        assert str(intent.target.fund.chain.chain_id) == chain
        assert change.result.delta0 == change.result.delta1 == 0
        assert change.result.fill is None
        assert manifest.initial.portfolio.apply(change.effects) == manifest.initial.portfolio
    finally:
        model.book.close()
