"""Explicit simulated balances and account topology, without fixture mechanics."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.configuration.anchors import AnchorSelection
from almanak.gateway.evm import native_metadata
from almanak.gateway.fork_config import Name, require_unique
from almanak.gateway.operator_execution import PayerCeiling, PolicyRefusal
from almanak.gateway.production_config import (
    ForkGasExecution,
    GasPolicy,
    UnenforcedGas,
    op_family,
    validate_gas_payers,
)
from almanak.models import AssetAmount, Model, StrategyAccounts


class EoaFixtureFund(Model):
    kind: Literal["eoa"]
    account: Name
    native_balance: AssetAmount
    assets: tuple[AssetAmount, ...]


class SafeFixtureFund(Model):
    kind: Literal["safe_roles"]
    account: Name
    operator: Name
    assets: tuple[AssetAmount, ...]


FixtureFund = Annotated[EoaFixtureFund | SafeFixtureFund, Field(discriminator="kind")]


class FixtureOperator(Model):
    name: Name
    native_balance: AssetAmount


class ForkFixtureInputs(Model):
    schema_version: Literal["anvil-fixture-input-v1"] = "anvil-fixture-input-v1"
    anchors: Annotated[tuple[AnchorSelection, ...], Field(min_length=1)]
    funds: Annotated[tuple[FixtureFund, ...], Field(min_length=1)]
    operators: tuple[FixtureOperator, ...] = ()
    gas: tuple[PayerCeiling, ...] = Field(default=(), exclude_if=lambda value: not value)
    # Base and Optimism forks cannot bound the L1 data and operator fees, so they refuse
    # authored gas ceilings. `unenforced` is the operator's explicit choice to run such a
    # fork gasless: the author policy stays retained but no fork ceiling enforces it.
    author_gas: Literal["enforced", "unenforced"] = Field(
        default="enforced", exclude_if=lambda value: value == "enforced"
    )

    @model_validator(mode="after")
    def scope(self):
        if self.author_gas == "unenforced" and self.gas:
            raise ValueError("an unenforced author gas policy takes no fork gas ceilings")
        require_unique([a.chain for a in self.anchors], "fixture anchor chains")
        require_unique([f.account for f in self.funds], "fixture fund names")
        require_unique([o.name for o in self.operators], "fixture operator names")
        require_unique(
            [name for ceiling in self.gas for name in ceiling.accounts],
            "fixture gas ceiling accounts",
        )
        if {f.operator for f in self.funds if isinstance(f, SafeFixtureFund)} != {
            o.name for o in self.operators
        }:
            raise ValueError("Safe fixture funds must reference exactly the configured operators")
        for fund in self.funds:
            self.validate_fund(fund)
        if {self.chain(fund) for fund in self.funds} != {a.chain for a in self.anchors}:
            raise ValueError("fixture anchors must cover exactly the fund chains")
        for anchor in self.anchors:
            kinds = {fund.kind for fund in self.funds if self.chain(fund) == anchor.chain}
            if len(kinds) != 1:
                raise ValueError("fixture accounts on a chain require one supported profile family")
        return self

    def check_gas(
        self, policies: tuple[GasPolicy, ...], accounts: StrategyAccounts | None = None
    ) -> ForkGasExecution | UnenforcedGas | None:
        if not policies:
            return None
        if self.author_gas == "unenforced":
            if not op_family(policies):
                raise PolicyRefusal(
                    "unenforced author gas is limited to Base and Optimism forks, whose L1 "
                    "data and operator fees have no verified bound"
                )
            # Unenforced, but still a policy for exactly these accounts and payers.
            self.gas_accounts(policies)
            if accounts is not None:
                validate_gas_payers(policies, accounts, ())
            return UnenforcedGas(gas=policies)
        if not self.gas:
            raise PolicyRefusal("fork operator has no gas ceilings for the authored policy")
        requested = self.gas_accounts(policies)
        for ceiling in self.gas:
            for name in ceiling.accounts:
                try:
                    ceiling.require(requested[name])
                except KeyError:
                    raise PolicyRefusal(
                        f"fork gas ceiling references unknown account {name}"
                    ) from None
        expected = self.payer_groups()
        actual = {frozenset(ceiling.accounts) for ceiling in self.gas}
        if actual != expected:
            raise PolicyRefusal("fork gas ceilings differ from the fixture payer topology")
        if accounts is not None:
            validate_gas_payers(policies, accounts, ())
            payers = [ceiling.payer(accounts) for ceiling in self.gas]
            if len(set(payers)) != len(payers):
                raise PolicyRefusal("fork gas ceilings do not identify distinct concrete payers")
        return ForkGasExecution(gas=policies)

    def gas_accounts(self, policies: tuple[GasPolicy, ...]) -> dict[str, GasPolicy]:
        names = [name for policy in policies for name in policy.accounts]
        if len(names) != len(set(names)):
            raise PolicyRefusal("each account requires exactly one gas policy")
        requested = {name: policy for policy in policies for name in policy.accounts}
        if set(requested) != {fund.account for fund in self.funds}:
            raise PolicyRefusal("fork gas policies must cover exactly the fixture fund accounts")
        return requested

    def payer_groups(self) -> set[frozenset[str]]:
        groups = {
            frozenset((fund.account,)) for fund in self.funds if isinstance(fund, EoaFixtureFund)
        }
        groups.update(
            frozenset(
                fund.account for fund in self.funds if getattr(fund, "operator", None) == name
            )
            for name in (operator.name for operator in self.operators)
        )
        return groups

    def native_balance(self, fund: FixtureFund) -> AssetAmount:
        if isinstance(fund, EoaFixtureFund):
            return fund.native_balance
        return next(o.native_balance for o in self.operators if o.name == fund.operator)

    def chain(self, fund: FixtureFund) -> ChainId:
        return self.native_balance(fund).asset.chain

    def validate_fund(self, fund: FixtureFund):
        chain = self.chain(fund)
        native, _ = native_metadata(chain)
        if self.native_balance(fund).asset != native:
            raise ValueError("fixture gas funding requires the chain's native asset")
        require_unique([asset.asset for asset in fund.assets], "fixture funding assets")
        if any(
            asset.asset.chain != chain or asset.asset.namespace != "erc20" for asset in fund.assets
        ):
            raise ValueError("fixture token balances require scoped ERC-20 assets")

    def validate_accounts(self, accounts: StrategyAccounts):
        known = {binding.name: binding for binding in accounts.bindings}
        for fund in self.funds:
            binding = known.get(fund.account)
            if binding is None or binding.profile.fund.chain.chain_id != self.chain(fund):
                raise ValueError(
                    f"fixture account {fund.account} has no matching operator chain binding"
                )
