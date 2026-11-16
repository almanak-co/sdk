"""Non-signing policy compatibility through the authenticated bootstrap context."""

from typing import Literal

from pydantic import model_validator

from almanak.configuration.resolution import ReadContext
from almanak.connectors.support import RunMode
from almanak.gateway.ethereum import DataError
from almanak.gateway.operator_config import Digest, OperatorConfig
from almanak.gateway.operator_execution import PayerCeiling, PolicyRefusal
from almanak.gateway.production_config import (
    ForkGasExecution,
    GasPolicy,
    ProductionExecution,
    UnenforcedGas,
    validate_gas_payers,
)
from almanak.models import Model, StrategyAccounts


class GasCompatibility(Model):
    status: Literal["gas_policy_compatible"] = "gas_policy_compatible"
    authority: Literal["read_only"] = "read_only"
    context_digest: Digest
    operator_digest: Digest
    accounts: StrategyAccounts
    execution: ProductionExecution
    ceilings: tuple[PayerCeiling, ...]

    @model_validator(mode="after")
    def scope(self):
        self.execution.validate_accounts(self.accounts, ())
        names = [name for ceiling in self.ceilings for name in ceiling.accounts]
        if len(set(names)) != len(names) or set(names) != {b.name for b in self.accounts.bindings}:
            raise ValueError("checked operator ceilings must cover exactly the bound accounts")
        payers = [ceiling.payer(self.accounts) for ceiling in self.ceilings]
        if len(set(payers)) != len(payers):
            raise ValueError("checked shared payers require one operator ceiling")
        for ceiling in self.ceilings:
            for name in ceiling.accounts:
                ceiling.require(self.execution.policy(name))
        return self

    def require(
        self, context: ReadContext, accounts: StrategyAccounts, policies: tuple[GasPolicy, ...]
    ):
        if (self.context_digest, self.operator_digest) != (context.digest, context.operator_digest):
            raise ValueError("gas policy result belongs to another operator context")
        if context.mode != RunMode.PRODUCTION:
            raise ValueError("production gas compatibility cannot authorize another run mode")
        if self.accounts != accounts or self.execution.gas != policies:
            raise ValueError("gas policy result changed the requested economics or payer binding")


class ForkGasCompatibility(Model):
    status: Literal["fork_gas_policy_compatible"] = "fork_gas_policy_compatible"
    authority: Literal["fixture_only"] = "fixture_only"
    context_digest: Digest
    operator_digest: Digest
    accounts: StrategyAccounts
    execution: ForkGasExecution
    ceilings: tuple[PayerCeiling, ...]

    @model_validator(mode="after")
    def scope(self):
        names = [name for ceiling in self.ceilings for name in ceiling.accounts]
        if len(set(names)) != len(names) or set(names) != {b.name for b in self.accounts.bindings}:
            raise ValueError("checked fork ceilings must cover exactly the generated accounts")
        payers = [ceiling.payer(self.accounts) for ceiling in self.ceilings]
        if len(set(payers)) != len(payers):
            raise ValueError("checked fork ceilings require one ceiling per generated payer")
        validate_gas_payers(self.execution.gas, self.accounts, ())
        for ceiling in self.ceilings:
            for name in ceiling.accounts:
                ceiling.require(self.execution.policy(name))
        return self

    def require(
        self, context: ReadContext, accounts: StrategyAccounts, policies: tuple[GasPolicy, ...]
    ):
        require_fork_scope(self, context, accounts, policies)


class ForkGasUnenforced(Model):
    status: Literal["fork_gas_unenforced"] = "fork_gas_unenforced"
    authority: Literal["fixture_only"] = "fixture_only"
    context_digest: Digest
    operator_digest: Digest
    accounts: StrategyAccounts
    execution: UnenforcedGas

    @model_validator(mode="after")
    def scope(self):
        validate_gas_payers(self.execution.gas, self.accounts, ())
        return self

    def require(
        self, context: ReadContext, accounts: StrategyAccounts, policies: tuple[GasPolicy, ...]
    ):
        require_fork_scope(self, context, accounts, policies)


def require_fork_scope(record, context, accounts, policies):
    if (record.context_digest, record.operator_digest) != (context.digest, context.operator_digest):
        raise ValueError("fork gas policy belongs to another operator context")
    if context.mode != RunMode.FORK:
        raise ValueError("fork gas compatibility cannot authorize another run mode")
    if record.accounts != accounts or record.execution.gas != policies:
        raise ValueError("fork gas policy changed the requested economics or payer binding")


def check_policy(context: ReadContext, operator: OperatorConfig, policies: tuple[GasPolicy, ...]):
    accounts = context.match_operator(operator, RunMode.PRODUCTION)
    authority = operator.production
    if authority is None:
        raise DataError(
            "operator_policy_refused",
            "operator has no production authority for gas policy checking",
        )
    try:
        execution = authority.check(operator, accounts, policies)
    except PolicyRefusal as error:
        raise DataError("operator_policy_refused", str(error)) from error
    names = {b.name for b in accounts.bindings}
    ceilings = tuple(
        PayerCeiling.model_validate(
            {
                **ceiling.model_dump(),
                "accounts": tuple(name for name in ceiling.accounts if name in names),
            }
        )
        for ceiling in authority.payers
        if names.intersection(ceiling.accounts)
    )
    result = GasCompatibility(
        context_digest=context.digest,
        operator_digest=operator.digest,
        accounts=accounts,
        execution=execution,
        ceilings=ceilings,
    )
    result.require(context, accounts, policies)
    return result


def check_fork_policy(
    context: ReadContext,
    operator: OperatorConfig,
    accounts: StrategyAccounts,
    policies: tuple[GasPolicy, ...],
) -> ForkGasCompatibility | ForkGasUnenforced | None:
    context.match_operator(operator, RunMode.FORK)
    fixture = operator.fork
    if fixture is None:
        raise ValueError("operator has no fork fixture configuration")
    try:
        execution = fixture.check_gas(policies, accounts)
    except PolicyRefusal as error:
        raise DataError("operator_policy_refused", str(error)) from error
    if execution is None:
        return None
    scope = dict(
        context_digest=context.digest,
        operator_digest=operator.digest,
        accounts=accounts,
        execution=execution,
    )
    result: ForkGasCompatibility | ForkGasUnenforced
    if isinstance(execution, UnenforcedGas):
        result = ForkGasUnenforced(**scope)
    else:
        result = ForkGasCompatibility(**scope, ceilings=fixture.gas)
    result.require(context, accounts, policies)
    return result
