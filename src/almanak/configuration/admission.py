"""Bind frozen preparation to a deployment without granting execution authority."""

from typing import Literal

from pydantic import Field, model_validator

from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.prepared import PreparationProgress
from almanak.connectors.composition import connector_for
from almanak.connectors.support import RunMode
from almanak.gateway.fixture_accounts import FixtureAccounts
from almanak.gateway.operator_config import OperatorConfig, content_digest
from almanak.gateway.policy_resolution import (
    ForkGasCompatibility,
    ForkGasUnenforced,
    check_fork_policy,
    check_policy,
)
from almanak.models import Model


class PreparedDeployment(Model):
    schema_version: Literal["prepared-deployment-v2"] = "prepared-deployment-v2"
    resolution: ResolutionProgress
    preparation: PreparationProgress
    fixture: FixtureAccounts | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def scope(self):
        result, prepared = self.resolution.result, self.preparation.prepared
        context, policy = self.resolution.context, self.preparation.policy
        if result is None:
            raise ValueError("deployment preparation requires complete resolution")
        if (prepared.context_digest, prepared.resolution_digest, prepared.mode) != (
            context.digest,
            result.digest,
            context.mode,
        ):
            raise ValueError("deployment preparation differs from its retained resolution")
        if context.mode == RunMode.FORK:
            self.require_fixture()
        else:
            if context.mode != RunMode.PRODUCTION or policy is None or self.fixture is not None:
                raise ValueError(
                    "deployment preparation requires complete production resolution and policy"
                )
            policy.require(context, prepared.parameters.accounts, prepared.parameters.gas)
            if result.binding.accounts != prepared.parameters.accounts:
                raise ValueError("deployment preparation changed the resolved account binding")
        return self

    def require_fixture(self):
        prepared = self.preparation.prepared
        policy = self.preparation.policy
        if self.fixture is None:
            raise ValueError(
                "fork preparation requires generated accounts and supported fork execution policy"
            )
        if prepared.parameters.gas:
            if not isinstance(policy, (ForkGasCompatibility, ForkGasUnenforced)):
                raise ValueError("fork author gas policy requires retained operator compatibility")
            policy.require(self.resolution.context, self.fixture.accounts, prepared.parameters.gas)
        elif policy is not None:
            raise ValueError("fork gas compatibility requires authored gas policy")
        self.fixture.require_resolution(self.resolution)
        if (prepared.fixture_digest, prepared.parameters.accounts) != (
            self.fixture.digest,
            self.fixture.accounts,
        ):
            raise ValueError("fork preparation changed its generated account binding")
        # A position acts on one chain. The account a position names as the receiver of
        # a bridge adds its own chain, where no position acts. No other bound account does.
        chains = {b.chain for b in self.fixture.bindings}
        positions = prepared.parameters.positions
        acting = {a.chain for a in positions}
        receivers = {getattr(a, "destination_account", None) for a in positions}
        receiving = {
            b.profile.fund.chain
            for b in prepared.parameters.accounts.bindings
            if b.name in receivers
        }
        if not acting <= chains <= acting | receiving:
            raise ValueError("fork positions differ from their generated account environments")

    @property
    def digest(self):
        return content_digest(self)

    def configuration(self) -> Model:
        parameters = self.preparation.prepared.parameters
        config = connector_for(parameters).codec.config
        policy = self.preparation.policy
        if self.fixture is not None:
            values = dict(
                **parameters.model_dump(),
                fork_bindings=self.fixture.bindings,
                preparation_digest=self.digest,
            )
            if isinstance(policy, ForkGasCompatibility):
                values["fork_gas"] = policy.execution
            elif isinstance(policy, ForkGasUnenforced):
                values["unenforced_gas"] = policy.execution
            return config(**values)
        assert policy is not None
        return config(
            **parameters.model_dump(), production=policy.execution, preparation_digest=self.digest
        )

    def require_configuration(self, config, strategy):
        self.preparation.prepared.require_configuration(config, strategy)
        if config != self.configuration():
            raise ValueError("deployment differs from its frozen execution preparation")

    def require_operator(self, operator: OperatorConfig):
        if self.fixture is not None:
            self.fixture.plan.require(self.resolution.context, operator, self.fixture.plan.anchors)
            policy = check_fork_policy(
                self.resolution.context,
                operator,
                self.fixture.accounts,
                self.preparation.prepared.parameters.gas,
            )
            if policy != self.preparation.policy:
                raise ValueError("operator fork gas policy differs from retained preparation")
            return
        policy = check_policy(
            self.resolution.context, operator, self.preparation.prepared.parameters.gas
        )
        if policy != self.preparation.policy:
            raise ValueError("operator policy differs from retained deployment preparation")

    def require_checkpoint(self, state):
        self.require_configuration(state.config, state.strategy)
        if state.capabilities != self.preparation.prepared.capabilities:
            raise ValueError("durable strategy authority differs from frozen preparation")
