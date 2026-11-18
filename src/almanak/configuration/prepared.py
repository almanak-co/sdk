"""Frozen hook inputs and results preceding independent execution admission."""

import fcntl
import json
import os
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator, model_validator

from almanak.backtest.dependencies import DataDependencies
from almanak.checks import check_parameters, checked, stage
from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.parameter_types import PARAMETERS
from almanak.configuration.parameters import admit_new_positions, construct_parameters
from almanak.configuration.unresolved import InputDocument
from almanak.connectors.composition import connector_for
from almanak.connectors.support import RunMode
from almanak.gateway.client import GatewayClient
from almanak.gateway.fixture_accounts import FixtureAccounts
from almanak.gateway.operator_config import Digest, content_digest
from almanak.gateway.policy_resolution import (
    ForkGasCompatibility,
    ForkGasUnenforced,
    GasCompatibility,
)
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from almanak.workspace import save


class PreparedParameters(Model):
    schema_version: Literal["prepared-parameters-v2"] = "prepared-parameters-v2"
    context_digest: Digest
    resolution_digest: Digest
    mode: RunMode
    strategy: StrategyDefinition
    parameters: PARAMETERS
    parameters_digest: Digest
    # Typed by the parameter connector's capability codec; see decode_capabilities.
    capabilities: Any
    dependencies: DataDependencies
    fixture_digest: Digest | None = Field(default=None, exclude_if=lambda value: value is None)

    @field_validator("capabilities", mode="before")
    @classmethod
    def decode_capabilities(cls, value, info: ValidationInfo):
        parameters = info.data.get("parameters")
        if parameters is None:
            return value
        adapter = connector_for(parameters).codec.capability.capabilities
        if isinstance(value, Model):
            return adapter.validate_python(value)
        return adapter.validate_json(json.dumps(value))

    @model_validator(mode="after")
    def scope(self):
        if self.parameters_digest != content_digest(self.parameters):
            raise ValueError("prepared parameters differ from their retained digest")
        if self.fixture_digest is not None and self.mode != RunMode.FORK:
            raise ValueError("generated fixture parameters require explicit fork mode")
        connector_for(self.parameters).codec.capability.validate(self.parameters, self.capabilities)
        return self

    @property
    def digest(self):
        return content_digest(self)

    def require_configuration(self, config, strategy):
        connector = connector_for(self.parameters)
        if not isinstance(config, connector.codec.config) or strategy != self.strategy:
            raise ValueError("prepared hooks require their exact strategy and connector")
        cls = type(self.parameters)
        parameters = cls.model_validate({name: getattr(config, name) for name in cls.model_fields})
        if parameters != self.parameters:
            raise ValueError("deployment differs from the frozen hook parameters")
        connector_for(config).codec.capability.validate(config, self.capabilities)

    def require(
        self,
        document: InputDocument,
        definition: StrategyDefinition,
        progress: ResolutionProgress,
        *,
        fixture: FixtureAccounts | None = None,
    ):
        if progress.result is None:
            raise ValueError("prepared hooks require complete retained resolution")
        if (self.context_digest, self.resolution_digest, self.mode, self.strategy) != (
            progress.context.digest,
            progress.result.digest,
            progress.context.mode,
            definition,
        ):
            raise ValueError("prepared hooks belong to different inputs, strategy or resolution")
        if self.fixture_digest != (fixture.digest if fixture is not None else None):
            raise ValueError("prepared parameters require their exact generated fixture accounts")
        if self.parameters != construct_parameters(document, progress, fixture=fixture):
            raise ValueError("prepared parameters differ from resolved author choices")


def build_prepared(
    document, definition, progress, stages=None, *, fixture=None
) -> PreparedParameters:
    stages = [] if stages is None else stages
    parameters = checked(
        stages,
        "parameters",
        document.source.file,
        lambda: new_parameters(document, progress, fixture),
    )
    stages.insert(
        1, stage("resolution", message="Matching retained contract reads replayed offline.")
    )
    capabilities, dependencies = check_parameters(parameters, definition, stages)
    return PreparedParameters(
        context_digest=progress.context.digest,
        resolution_digest=progress.result.digest,
        mode=progress.context.mode,
        strategy=definition,
        parameters=parameters,
        parameters_digest=content_digest(parameters),
        capabilities=capabilities,
        dependencies=DataDependencies.model_validate_json(json.dumps(dependencies)),
        fixture_digest=fixture.digest if fixture is not None else None,
    )


def new_parameters(document, progress, fixture):
    parameters = construct_parameters(document, progress, fixture=fixture)
    admit_new_positions(document, progress.result)
    return parameters


class PreparationProgress(Model):
    authority: Literal["unadmitted"] = "unadmitted"
    prepared: PreparedParameters
    prepared_digest: Digest
    policy: GasCompatibility | ForkGasCompatibility | ForkGasUnenforced | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def scope(self):
        if self.prepared_digest != self.prepared.digest:
            raise ValueError("prepared hook bundle differs from its retained digest")
        policy, prepared = self.policy, self.prepared
        if policy is not None and (
            policy.context_digest != prepared.context_digest
            or policy.accounts != prepared.parameters.accounts
            or policy.execution.gas != prepared.parameters.gas
        ):
            raise ValueError("policy check differs from the frozen author parameters")
        if isinstance(policy, GasCompatibility) and prepared.mode != RunMode.PRODUCTION:
            raise ValueError("production policy check requires production parameters")
        if (
            isinstance(policy, (ForkGasCompatibility, ForkGasUnenforced))
            and prepared.mode != RunMode.FORK
        ):
            raise ValueError("fork policy check requires fork parameters")
        return self


def read_preparation(path: Path) -> PreparationProgress:
    return PreparationProgress.model_validate_json(path.read_text())


def publish_preparation(path: Path, record: PreparationProgress):
    save(path, record.model_dump(mode="json"))
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare_production(
    path: Path,
    document: InputDocument,
    definition: StrategyDefinition,
    progress: ResolutionProgress,
    client: GatewayClient,
) -> PreparationProgress:
    """Retain hooks before I/O; a lost policy reply cannot trigger another validation hook."""
    if progress.context.mode != RunMode.PRODUCTION:
        raise ValueError("production preparation requires explicit production context")
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("parameter preparation already has an active owner") from None
        if path.exists():
            record = read_preparation(path)
            record.prepared.require(document, definition, progress)
        else:
            prepared = build_prepared(document, definition, progress)
            record = PreparationProgress(prepared=prepared, prepared_digest=prepared.digest)
            publish_preparation(path, record)
        policy = GasCompatibility.model_validate_json(
            json.dumps(
                client.request(
                    "resolution-gas-policy",
                    context_digest=progress.context.digest,
                    policies=[p.model_dump(mode="json") for p in record.prepared.parameters.gas],
                )
            )
        )
        policy.require(
            progress.context, record.prepared.parameters.accounts, record.prepared.parameters.gas
        )
        if record.policy is not None:
            if record.policy != policy:
                raise ValueError("gateway policy changed; retained preparation cannot be replaced")
            return record
        record = PreparationProgress(
            prepared=record.prepared, prepared_digest=record.prepared_digest, policy=policy
        )
        publish_preparation(path, record)
        return record
