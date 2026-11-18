"""Freeze one final hook bundle after actual fixture account provisioning."""

import fcntl

from almanak.configuration.parameters import gas_policies
from almanak.configuration.prepared import (
    PreparationProgress,
    build_prepared,
    publish_preparation,
    read_preparation,
)
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.support import RunMode
from almanak.gateway.ethereum import DataError
from almanak.gateway.operator_execution import PolicyRefusal
from almanak.gateway.policy_resolution import check_fork_policy
from almanak.gateway.production_config import ForkGasExecution
from almanak.gateway.replacement_assets import require_network


def require_fork_policy(document, operator):
    """Check authored ceilings before node setup without treating them as execution authority."""
    policies = gas_policies(document, RunMode.FORK)
    if not policies:
        return
    fixture = operator.fork
    if fixture is None or fixture.author_gas != "unenforced":
        try:
            ForkGasExecution(gas=policies)
        except ValueError as error:
            raise document.error(ConfigurationError(str(error), field="strategy.gas")) from error
        for policy in policies:
            if policy.replacement is not None:
                try:
                    require_network(policy.asset.chain)
                except DataError as error:
                    raise document.error(
                        ConfigurationError(str(error), field="strategy.gas")
                    ) from error
        if fixture is None:
            raise document.error(
                ConfigurationError(
                    "fork author gas policy requires explicit operator fixture ceilings",
                    field="strategy.gas",
                )
            )
    try:
        fixture.check_gas(policies)
    except PolicyRefusal as error:
        raise document.error(ConfigurationError(str(error), field="strategy.gas")) from error


def prepare_fork(path, document, definition, progress, fixture, operator) -> PreparationProgress:
    require_fork_policy(document, operator)
    fixture.require_resolution(progress)
    fixture.plan.require(progress.context, operator, progress.result.anchors)
    policies = gas_policies(document, RunMode.FORK)
    policy = check_fork_policy(progress.context, operator, fixture.accounts, policies)
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("fork parameter preparation already has an active owner") from None
        if path.exists():
            record = read_preparation(path)
            record.prepared.require(document, definition, progress, fixture=fixture)
            if record.policy != policy:
                raise ValueError("retained fork gas policy differs from operator authority")
            return record
        prepared = build_prepared(document, definition, progress, fixture=fixture)
        record = PreparationProgress(
            prepared=prepared, prepared_digest=prepared.digest, policy=policy
        )
        publish_preparation(path, record)
        return record
