"""Connector fork configurations assembled from public author inputs for internal drivers.

A test adapter, never a loader: the public root, the operator example and one
retained resolution decide every value. Nothing here reads provider credentials;
the read gateway process holds them for the duration of one resolution.
"""

import json
import secrets
from pathlib import Path
from typing import Any, cast

from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.bootstrap import read_progress, resolve_configuration
from almanak.configuration.fork_preparation import prepare_fork, require_fork_policy
from almanak.configuration.parameters import construct_parameters
from almanak.configuration.read_gateway import ReadGateway
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import author_configuration_from_json
from almanak.connectors.support import RunMode
from almanak.gateway.fixture_accounts import provision_accounts
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.fork_workspace import write_private
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.production_config import ProductionExecution, ProductionNetwork
from almanak.strategy import StrategyDefinition


def fork_context(config_path, operator_path):
    document = read_inputs(config_path)
    operator = read_operator_config(operator_path)
    if operator.fork is None:
        raise ValueError("fork adapter requires an operator file with fixture inputs")
    return (
        document,
        operator,
        document.inputs.context(operator, RunMode.FORK, operator.fork.anchors),
    )


def resolve_reference(config_path, operator_path, env_file, directory):
    """Resolve market facts once through the read gateway; resolution.json is retained."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "resolution.json"
    progress = read_progress(path) if path.exists() else None
    if progress is None or progress.result is None:
        if progress is None:
            _, _, context = fork_context(config_path, operator_path)
        else:
            context = progress.context
        gateway = ReadGateway(directory / "private", operator_path, context, env_file, RunMode.FORK)
        with gateway as client:
            resolve_configuration(path, context, client)
    return read_progress(path)


def fork_config_for(config_path, operator_path, progress):
    """The connector fork configuration the internal drivers accept, from resolved inputs.

    The retained context carries the resolution identity, so the operator file is
    matched against it rather than rebuilt into a second context.
    """
    document = read_inputs(config_path)
    operator = read_operator_config(operator_path)
    parameters = cast(Any, construct_parameters(document, progress))
    if parameters.gas:
        raise ValueError(
            "gas-bearing author inputs require admitted fork preparation or "
            "the production rehearsal adapter"
        )
    plan = compile_fixture(progress.context, operator, progress.result.anchors)
    return _fixture_config(parameters, plan)


def production_rehearsal_config_for(config_path, operator_path, progress):
    """Build an identified fork transport with the exact authored production policy."""
    document = read_inputs(config_path)
    operator = read_operator_config(operator_path)
    parameters = cast(Any, construct_parameters(document, progress))
    if not parameters.gas:
        raise ValueError("production rehearsal requires an explicit author gas policy")
    plan = compile_fixture(progress.context, operator, progress.result.anchors)
    execution = ProductionExecution(
        networks=tuple(
            ProductionNetwork(
                chain=target.chain,
                provider=target.provider,
                max_observation_age_seconds=180,
            )
            for target in plan.forks
        ),
        gas=parameters.gas,
    )
    return _fixture_config(parameters, plan, execution)


def fixture_plan_for(config_path, operator_path, progress):
    """Compile the identified unprovisioned fixture selected by retained resolution."""
    document = read_inputs(config_path)
    operator = read_operator_config(operator_path)
    require_fork_policy(document, operator)
    if progress.result is None:
        raise ValueError("fork fixture planning requires complete resolution")
    return compile_fixture(progress.context, operator, progress.result.anchors)


def prepare_admitted_fork(
    config_path, strategy_path, operator_path, progress, plan, nodes, directory
):
    """Provision actual fixture payers, freeze hooks and admit fork-only gas policy."""
    document = read_inputs(config_path)
    operator = read_operator_config(operator_path)
    fixture = provision_accounts(directory, plan, progress.context, operator, nodes)
    preparation = prepare_fork(
        directory / "preparation.json",
        document,
        StrategyDefinition.read(strategy_path),
        progress,
        fixture,
        operator,
    )
    bundle = PreparedDeployment(resolution=progress, preparation=preparation, fixture=fixture)
    config = bundle.configuration()
    write_private(directory / "deployment.json", config.model_dump_json(indent=2))
    write_private(directory / "prepared-deployment.json", bundle.model_dump_json(indent=2))
    write_private(directory / "operator.json", operator.model_dump_json(indent=2))
    write_private(directory / "gateway-auth.token", secrets.token_hex(32))
    return config


def _fixture_config(parameters: Any, plan: Any, production: ProductionExecution | None = None):
    wire = {
        # ForkConfig is an internal fixture driver. Only the dedicated rehearsal
        # adapter may translate author gas into a production execution profile.
        **parameters.model_dump(mode="json", exclude={"accounts", "gas"}),
        "forks": [target.model_dump(mode="json") for target in plan.forks],
        "funds": [fund.model_dump(mode="json") for fund in plan.funds],
        "operators": [item.model_dump(mode="json") for item in plan.operators],
    }
    if production is not None:
        wire["production"] = production.model_dump(mode="json")
    return author_configuration_from_json(json.dumps(wire))
