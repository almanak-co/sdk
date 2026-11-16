"""Hypothetical fund inventory and separate payer scopes for connector simulations."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.config import Fund as Payer
from almanak.backtest.config import HistoryBinding
from almanak.backtest.engine import Schedule
from almanak.connectors.identifiers import Name
from almanak.effects import Balance, WalletAsset
from almanak.models import AssetAmount, Model, SafeRoles, SimulationAccount


class Fund(Model):
    name: Name
    profile: SafeRoles | SimulationAccount
    balances: tuple[AssetAmount, ...]

    @property
    def fund(self):
        return self.profile.fund

    @model_validator(mode="after")
    def scope(self):
        if any(a.asset.chain != self.fund.chain.chain_id for a in self.balances):
            raise ValueError("hypothetical fund inventory has another chain identity")
        return self

    def inventory(self):
        return tuple(
            Balance(
                key=WalletAsset(account=self.name, chain=self.fund.chain, asset=a.asset),
                amount=a.amount,
            )
            for a in self.balances
        )


class InventoryConfig(Model):
    schedule: Schedule
    history: Annotated[tuple[HistoryBinding, ...], Field(min_length=1)]
    funds: Annotated[tuple[Fund, ...], Field(min_length=1)]
    payers: tuple[Payer, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if not isinstance(value, dict):
            return value
        raw = dict(value)
        for field in ("history", "funds", "payers"):
            if isinstance(raw.get(field), list):
                raw[field] = tuple(raw[field])
        for fund in (*raw.get("funds", ()), *raw.get("payers", ())):
            if isinstance(fund, dict) and isinstance(fund.get("balances"), list):
                fund["balances"] = tuple(fund["balances"])
        return raw

    @model_validator(mode="after")
    def scope(self):
        for values in (
            [h.chain for h in self.history],
            [f.name for f in self.funds] + [p.name for p in self.payers],
            [f.fund for f in self.funds] + [p.fund for p in self.payers],
        ):
            if len(values) != len(set(values)):
                raise ValueError("historical providers, fund and payer bindings must be unique")
        if self.payers and {
            f.profile.submitter for f in self.funds if f.profile.submitter != f.fund
        } != {p.fund for p in self.payers}:
            raise ValueError("modeled gas payers must match the declared Safe operators exactly")
        self.validate_inventory()
        return self

    def validate_inventory(self):
        accounts: tuple[Fund | Payer, ...] = (*self.funds, *self.payers)
        for fund in accounts:
            assets = [a.asset for a in fund.balances]
            if len(set(assets)) != len(assets) or any(
                a.chain != fund.fund.chain.chain_id for a in assets
            ):
                raise ValueError(
                    "hypothetical inventory requires unique assets on its account chain"
                )

    def payer(self, account):
        profile = next(f.profile for f in self.funds if f.name == account)
        return next(p for p in self.payers if p.fund == profile.submitter)
