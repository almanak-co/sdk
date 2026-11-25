"""Explicit production network, fee source and payer spending authority."""

from typing import Annotated, Any, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.chains import evm_network
from almanak.gateway.providers import Provider
from almanak.models import Account, AssetAmount, Chain, Eoa, Model, SafeRoles, UInt

Positive = Annotated[UInt, Field(gt=0, lt=2**256)]
# The multiple of the base fee a bid may state, in basis points: the base fee itself up to
# five times it. An author gas policy and a direct action's flag take the same range.
MIN_BASE_FEE_MULTIPLIER_BPS = 10000
MAX_BASE_FEE_MULTIPLIER_BPS = 50000
BaseFeeMultiplierBps = Annotated[
    UInt, Field(ge=MIN_BASE_FEE_MULTIPLIER_BPS, le=MAX_BASE_FEE_MULTIPLIER_BPS)
]


class ProductionNetwork(Model):
    chain: Chain
    provider: Provider
    max_observation_age_seconds: Positive

    @model_validator(mode="before")
    @classmethod
    def remove_obsolete_finality(cls, value: Any) -> Any:
        # Retained pending deployments must decode without changing their transaction identity.
        if isinstance(value, dict) and value.get("finality") == "finalized":
            return {key: item for key, item in value.items() if key != "finality"}
        return value

    @model_validator(mode="after")
    def supported(self) -> Self:
        if self.chain.environment != "mainnet":
            raise ValueError("production execution requires mainnet environment identity")
        return self


class ReplacementPolicy(Model):
    interval_seconds: Positive
    minimum_bump_bps: Positive
    max_candidates: Annotated[UInt, Field(ge=2)]
    max_elapsed_seconds: Positive

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.max_elapsed_seconds <= self.interval_seconds:
            raise ValueError("replacement elapsed limit must exceed its interval")
        return self


class GasPolicy(Model):
    name: str | None = Field(default=None, exclude_if=lambda value: value is None)
    accounts: Annotated[tuple[str, ...], Field(min_length=1)]
    asset: AssetType
    source: Literal["network_rpc_fee_history"]
    base_fee_basis: Literal["next_block", "observed_block"] = Field(
        default="next_block", exclude_if=lambda v: v == "next_block"
    )
    reward_percentile: Annotated[UInt, Field(le=100)]
    base_fee_multiplier_bps: BaseFeeMultiplierBps
    max_gas_price_wei: Positive
    max_gas_limit: Positive
    max_transaction_fee: AssetAmount
    # Absent means no lifetime gas cap; per-transaction ceilings and the reserve still apply.
    total_budget: AssetAmount | None = Field(default=None, exclude_if=lambda value: value is None)
    exit_reserve: AssetAmount
    gas_limit_buffer_bps: Annotated[UInt, Field(le=10000)]
    replacement: ReplacementPolicy | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.asset != evm_network(self.asset.chain).native_asset:
            raise ValueError("gas policy requires the network's native asset identity")
        amounts = (self.max_transaction_fee, self.total_budget, self.exit_reserve)
        if any(value is not None and value.asset != self.asset for value in amounts):
            raise ValueError("gas amounts must identify the payer's native asset")
        if not 0 < self.max_transaction_fee.amount:
            raise ValueError("transaction fee ceiling must be positive")
        if self.total_budget is not None and not (
            self.max_transaction_fee.amount <= self.total_budget.amount
            and self.exit_reserve.amount <= self.total_budget.amount
        ):
            raise ValueError("transaction fee ceiling and exit reserve must fit the total budget")
        if len(set(self.accounts)) != len(self.accounts):
            raise ValueError("gas policy account names must be unique")
        return self


class ProductionExecution(Model):
    networks: Annotated[tuple[ProductionNetwork, ...], Field(min_length=1)]
    gas: Annotated[tuple[GasPolicy, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def scope(self) -> Self:
        if len({n.chain for n in self.networks}) != len(self.networks):
            raise ValueError("production networks must be unique")
        names = [name for policy in self.gas for name in policy.accounts]
        if len(names) != len(set(names)):
            raise ValueError("each account requires exactly one gas policy")
        return self

    def policy(self, account: str) -> GasPolicy:
        for policy in self.gas:
            if account in policy.accounts:
                return policy
        raise ValueError("account has no explicit gas policy")

    def validate_accounts(self, accounts, allocations):
        names = {name for policy in self.gas for name in policy.accounts}
        if names != {binding.name for binding in accounts.bindings}:
            raise ValueError("gas policies must cover exactly the bound accounts")
        chains = {network.chain for network in self.networks}
        if {b.profile.fund.chain for b in accounts.bindings} != chains:
            raise ValueError("production networks must cover exactly the bound account chains")
        for binding in accounts.bindings:
            if (
                not isinstance(binding.profile, (Eoa, SafeRoles))
                or binding.profile.fund.chain not in chains
            ):
                raise ValueError(
                    "production accounts require supported profiles on the configured network"
                )
        validate_gas_payers(self.gas, accounts, allocations)
        if any(a.chain not in chains for a in allocations):
            raise ValueError("production allocations must match network and payer asset scope")

    def network(self, chain: Chain) -> ProductionNetwork:
        for network in self.networks:
            if network.chain == chain:
                return network
        raise ValueError("chain has no explicit production network binding")


class ForkGasExecution(Model):
    """Admitted fee economics for identified forks, without production authority."""

    gas: Annotated[tuple[GasPolicy, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def supported(self) -> Self:
        names = [name for policy in self.gas for name in policy.accounts]
        if len(names) != len(set(names)):
            raise ValueError("each account requires exactly one gas policy")
        if any(evm_network(policy.asset.chain).family == "optimism" for policy in self.gas):
            raise ValueError(
                "OP fork gas admission is unavailable: "
                "L1 and operator costs have no verified exposure bound"
            )
        return self

    def policy(self, account: str) -> GasPolicy:
        for policy in self.gas:
            if account in policy.accounts:
                return policy
        raise ValueError("account has no explicit gas policy")


class UnenforcedGas(Model):
    """Retained author gas that grants no execution economics: the fork runs gasless."""

    gas: Annotated[tuple[GasPolicy, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def supported(self) -> Self:
        names = [name for policy in self.gas for name in policy.accounts]
        if len(names) != len(set(names)):
            raise ValueError("each account requires exactly one gas policy")
        if not op_family(self.gas):
            raise ValueError("unenforced author gas is limited to Base and Optimism forks")
        return self


def op_family(policies) -> bool:
    return bool(policies) and all(
        evm_network(policy.asset.chain).family == "optimism" for policy in policies
    )


def execution_gas(config) -> ProductionExecution | ForkGasExecution | None:
    production = getattr(config, "production", None)
    fork = getattr(config, "fork_gas", None)
    if production is not None and fork is not None:
        raise ValueError("fork and production gas execution are mutually exclusive")
    return production or fork


def require_execution_gas(config) -> ProductionExecution | ForkGasExecution:
    admitted = execution_gas(config)
    if admitted is None:
        raise ValueError("operation requires admitted gas execution policy")
    return admitted


def validate_gas_payers(policies, accounts, allocations):
    """Validate requested economics against concrete payers without granting authority."""
    names = [name for policy in policies for name in policy.accounts]
    if len(names) != len(set(names)):
        raise ValueError("each account requires exactly one gas policy")
    if set(names) != {binding.name for binding in accounts.bindings}:
        raise ValueError("gas policies must cover exactly the bound accounts")
    selected = {name: policy for policy in policies for name in policy.accounts}
    payers: dict[Account, GasPolicy] = {}
    for binding in accounts.bindings:
        payer = binding.profile.submitter
        policy = selected[binding.name]
        if policy.asset.chain != binding.profile.fund.chain.chain_id:
            raise ValueError("gas policy chain differs from its bound account")
        if payers.setdefault(payer, policy) != policy:
            raise ValueError("accounts sharing a gas payer require the same gas policy")
    if any(a.native_asset != selected[a.account].asset for a in allocations):
        raise ValueError("production allocations must match network and payer asset scope")
