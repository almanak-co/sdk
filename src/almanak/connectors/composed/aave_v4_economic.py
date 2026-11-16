"""Bind native Aave V4 economics to shared cash, payer and acquisition interfaces."""

from copy import copy
from typing import Annotated, Literal

from pydantic import Field

from almanak.backtest import economic_prepare
from almanak.backtest.config import (
    EconomicBacktestConfig,
    EconomicDataPolicy,
    EconomicFixedCosts,
    ExcludedCosts,
    Fund,
)
from almanak.backtest.economic_book import EconomicBook
from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.inventory import InventoryConfig
from almanak.connectors.aave_v4.authority import admit_intent, declared
from almanak.connectors.aave_v4.backtest import position_key
from almanak.connectors.aave_v4.economic_binding import result_report
from almanak.connectors.dex_economic_parameters import payer_wallet
from almanak.connectors.dex_economic_reporting import quantity
from almanak.effects import AssetDelta, WalletAsset
from almanak.models import Model


class Policy(Model):
    data: EconomicDataPolicy
    costs: Annotated[ExcludedCosts | EconomicFixedCosts, Field(discriminator="kind")]


class Settings(InventoryConfig):
    market_model: Literal["economic"]
    data: EconomicDataPolicy
    costs: Annotated[ExcludedCosts | EconomicFixedCosts, Field(discriminator="kind")]

    def acquisition_settings(self):
        return EconomicBacktestConfig(
            schedule=self.schedule,
            history=self.history,
            funds=tuple(Fund(name=f.name, fund=f.fund, balances=f.balances) for f in self.funds),
            market_model=self.market_model,
            data=self.data,
            costs=self.costs,
        )


class Book(EconomicBook):
    def __init__(self, directory, config, settings, digest=None):
        super().__init__(
            directory,
            tuple(a.reserve for a in config.positions),
            settings.schedule.start,
            settings.schedule.deadline,
            expected_hash=digest,
            data=settings.data,
        )

    def close(self):
        self.reader.close()


class Driver(EconomicDriver):
    def __init__(self, config, settings, strategy, book, *, authority):
        super().__init__(config, settings, strategy, book)
        if authority != declared(config):
            raise ValueError("Aave V4 economic authority differs from configured allocations")
        self.authority = authority

    def validate_state(self, state):
        if (
            state.schedule != self.settings.schedule
            or state.interval.effective != self.config.decision_trigger
        ):
            raise ValueError("economic state differs from its frozen scheduling")
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(p.payload.target for p in state.portfolio.positions) != expected:
            raise ValueError("economic ownership differs from its configured targets")
        if any(p.key != position_key(p.payload.target) for p in state.portfolio.positions):
            raise ValueError("economic position key differs from its fund and reserve")

    def execute(self, state, pending):
        admit_intent(self.authority, pending.intent)
        return super().execute(state, pending)

    def costs(self, state, intent):
        if self.settings.costs.kind == "excluded":
            return ()
        action = {
            "withdraw_amount": "withdraw",
            "repay_amount": "repay",
            "aave_refresh": "refresh",
        }.get(intent.kind, intent.kind)
        cost = getattr(self.settings.costs, action)
        key = payer_wallet(
            self.settings,
            intent.target,
            self.config.allocation(intent.target.allocation).native_asset,
        )
        if state.portfolio.balance(key) < cost:
            raise ValueError("insufficient payer inventory for the declared economic cost")
        return (AssetDelta(key=key, amount=-cost),)


def validate_sources(args, author, settings):
    reserves = tuple(dict.fromkeys(a.reserve for a in author.positions))
    native = settings.acquisition_settings()
    economic_prepare.validate_sources(native, reserves)
    economic_prepare.validate_strategy_dependencies(args, author, native, reserves)


def prepare(args):
    from almanak.backtest.inputs import selected

    author, settings, strategy = selected(args)
    child = copy(args)
    child._backtest_inputs = author, settings.acquisition_settings(), strategy
    return economic_prepare.prepare(child)


def payer_accounts(driver):
    keys: dict[WalletAsset, set[str]] = {}
    for allocation in driver.config.positions:
        target = driver.config.target(allocation.name)
        key = payer_wallet(driver.settings, target, allocation.native_asset)
        keys.setdefault(key, set()).add(target.account)
    return keys


def payer_costs(state, keys):
    costs = {key: 0 for key in keys}
    for completed in state.completed:
        for delta in completed.transition.effects.assets:
            if delta.key in costs:
                costs[delta.key] -= delta.amount
    return costs


def payer_reports(state, driver):
    keys = payer_accounts(driver)
    costs = payer_costs(state, keys)
    excluded = driver.settings.costs.kind == "excluded"
    return [
        {
            "wallet": key.model_dump(mode="json"),
            "fund_accounts": sorted(accounts),
            "cost": quantity(key.asset, None if excluded else costs[key]),
            "measurement_status": "excluded" if excluded else "assumed",
        }
        for key, accounts in keys.items()
    ]


def report(state, driver):
    return {**result_report(state, driver), "gas_payers": payer_reports(state, driver)}
