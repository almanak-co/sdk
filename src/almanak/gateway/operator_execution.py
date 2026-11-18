"""Independent operator bounds checked before production execution admission."""

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.gateway.production_config import (
    GasPolicy,
    Positive,
    ProductionExecution,
    ProductionNetwork,
    validate_gas_payers,
)
from almanak.models import Account, AssetAmount, Chain, Model, StrategyAccounts

if TYPE_CHECKING:
    from almanak.gateway.operator_config import OperatorConfig

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class PolicyRefusal(ValueError):
    """A checked policy diagnostic containing no credentials or private file paths."""


class SigningFiles(Model):
    """Private file references only; loading this model never unlocks a signer."""

    key_file: Path | None = None
    password_file: Path | None = None
    signer_file: Path | None = None

    @model_validator(mode="after")
    def absolute_paths(self):
        selected = (self.key_file is not None) + (self.signer_file is not None)
        if selected != 1:
            raise ValueError("operator signing requires exactly one keystore or remote signer")
        if self.signer_file is not None and self.password_file is not None:
            raise ValueError("remote signing takes no keystore password")
        paths = tuple(
            path
            for path in (self.key_file, self.password_file, self.signer_file)
            if path is not None
        )
        if any(not path.is_absolute() for path in paths):
            raise ValueError("operator signing references require absolute file paths")
        return self


class PayerCeiling(Model):
    """One limit for one concrete payer, even when several funds share it."""

    accounts: Annotated[tuple[Name, ...], Field(min_length=1)]
    max_gas_price_wei: Positive
    max_gas_limit: Positive
    max_transaction_fee: AssetAmount
    # An operator total budget requires every author policy it covers to state one within it.
    total_budget: AssetAmount | None = Field(default=None, exclude_if=lambda value: value is None)
    allow_replacement: bool

    @model_validator(mode="after")
    def scope(self):
        from almanak.gateway.evm import native_metadata

        asset = self.max_transaction_fee.asset
        native, _ = native_metadata(asset.chain)
        if asset != native or (self.total_budget is not None and self.total_budget.asset != native):
            raise ValueError("operator fee ceilings require the payer's native asset")
        if not 0 < self.max_transaction_fee.amount:
            raise ValueError("operator transaction ceiling must be positive")
        if (
            self.total_budget is not None
            and self.max_transaction_fee.amount > self.total_budget.amount
        ):
            raise ValueError("operator transaction ceiling must fit its total budget")
        if len(set(self.accounts)) != len(self.accounts):
            raise ValueError("operator ceiling account names must be unique")
        return self

    def payer(self, accounts: StrategyAccounts) -> Account:
        if not set(self.accounts) <= {binding.name for binding in accounts.bindings}:
            raise ValueError("operator ceiling references an unbound account")
        selected = tuple(accounts.binding(name).profile for name in self.accounts)
        payers = {profile.submitter for profile in selected}
        if len(payers) != 1:
            raise ValueError("an operator ceiling must identify exactly one concrete payer")
        payer = next(iter(payers))
        if payer.chain.chain_id != self.max_transaction_fee.asset.chain:
            raise ValueError("operator ceiling asset differs from its bound payer chain")
        return payer

    def require(self, request: GasPolicy):
        if request.asset != self.max_transaction_fee.asset:
            raise ValueError("requested gas asset differs from the operator ceiling")
        if self.total_budget is not None and request.total_budget is None:
            raise PolicyRefusal(
                f"gas policy for {', '.join(request.accounts)} states no total_budget; "
                f"operator permits at most {self.total_budget.amount}"
            )
        for field in ("max_gas_price_wei", "max_gas_limit", "max_transaction_fee", "total_budget"):
            requested, permitted = getattr(request, field), getattr(self, field)
            if permitted is None:
                continue
            if isinstance(requested, AssetAmount):
                requested, permitted = requested.amount, permitted.amount
            if requested > permitted:
                raise PolicyRefusal(
                    f"gas policy for {', '.join(request.accounts)} requests {field}={requested}; "
                    f"operator permits at most {permitted}"
                )
        if request.replacement is not None and not self.allow_replacement:
            raise PolicyRefusal("requested gas replacement is disabled by the operator")


class ExecutionNetwork(Model):
    chain: ChainId
    max_observation_age_seconds: Positive


class ProductionAuthority(Model):
    networks: Annotated[tuple[ExecutionNetwork, ...], Field(min_length=1)]
    payers: Annotated[tuple[PayerCeiling, ...], Field(min_length=1)]
    signing: SigningFiles | None = None

    @model_validator(mode="after")
    def unique_scope(self):
        chains = [network.chain for network in self.networks]
        names = [name for ceiling in self.payers for name in ceiling.accounts]
        if len(set(chains)) != len(chains) or len(set(names)) != len(names):
            raise ValueError("operator execution networks and account ceilings must be unique")
        return self

    def validate_operator(self, operator: "OperatorConfig"):
        accounts = operator.accounts
        concrete = [ceiling.payer(accounts) for ceiling in self.payers]
        if len(set(concrete)) != len(concrete):
            raise ValueError("accounts sharing a payer require one operator ceiling")
        chains = {payer.chain.chain_id for payer in concrete}
        if chains != {network.chain for network in self.networks}:
            raise ValueError("execution networks must cover exactly the operator payer chains")
        for network in self.networks:
            self.network(operator, network.chain)

    def network(self, operator: "OperatorConfig", chain: ChainId) -> ProductionNetwork:
        selected = next((n for n in self.networks if n.chain == chain), None)
        if selected is None:
            raise ValueError(f"operator has no production authority on {chain}")
        return ProductionNetwork(
            chain=Chain(chain_id=chain, environment="mainnet"),
            provider=operator.network(chain).binding(),
            max_observation_age_seconds=selected.max_observation_age_seconds,
        )

    def check(
        self,
        operator: "OperatorConfig",
        accounts: StrategyAccounts,
        policies: tuple[GasPolicy, ...],
    ) -> ProductionExecution:
        """Check policy compatibility only, without granting installed account authority."""
        from almanak.connectors.support import RunMode

        if RunMode.PRODUCTION not in operator.modes or operator.production != self:
            raise ValueError("operator has not enabled this production authority")
        validate_gas_payers(policies, accounts, ())
        granted = {name: ceiling for ceiling in self.payers for name in ceiling.accounts}
        for binding in accounts.bindings:
            if binding not in operator.accounts.bindings:
                raise ValueError("requested production binding differs from operator authority")
            ceiling = granted.get(binding.name)
            if ceiling is None:
                raise ValueError(f"operator has no production ceiling for {binding.name}")
            ceiling.require(next(p for p in policies if binding.name in p.accounts))
        chains = dict.fromkeys(b.profile.fund.chain.chain_id for b in accounts.bindings)
        execution = ProductionExecution(
            networks=tuple(self.network(operator, chain) for chain in chains), gas=policies
        )
        execution.validate_accounts(accounts, ())
        return execution
