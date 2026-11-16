"""Per-case operator inputs for converted references, derived from the published examples.

Author files carry no provider, account binding or fixture. QA varies those
operator choices here and proves every derived file still loads and compiles.
"""

from pathlib import Path

import yaml

from almanak.chains import ChainReference, CheckpointReference
from almanak.configuration.resolution import ResolutionAnchors
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.support import RunMode
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.operator_fixture import FixtureOperator
from almanak.models import Chain
from tests.qa.catalog import ROOT
from tests.strategy_fork.supervised import eoa_inputs, revalidated

EXAMPLES = ROOT / "examples/operators"
# The bench passes provider credentials through the env file, so only the name binds here.
PROVIDER = {"kind": "alchemy", "api_key_env": "ALCHEMY_API_KEY"}


def backtest_example(chains) -> Path:
    """Backtest operator files are shared by every reference with the same account chains."""
    name = "_".join(str(chain).replace(":", "-") for chain in sorted(map(str, chains)))
    return EXAMPLES / "backtest" / f"{name}.yaml"


def example_path(case, mode) -> Path:
    if mode == "backtest":
        return backtest_example(case.chains)
    if case.scenario == "curve_mixed":
        return ROOT / "tests/qa/scenarios/curve_mixed/operators/curve_mixed.fork.yaml"
    return EXAMPLES / f"{case.reference}.{mode}.yaml"


def example_operator(case, mode):
    return read_operator_config(example_path(case, mode))


def case_inputs(case, directory):
    """The resolved case configuration when one exists, otherwise the author's own file."""
    path = directory / "config.yaml"
    if not path.is_file():
        path = ROOT / "strategies" / case.reference / "config.yaml"
    return read_inputs(path).inputs


def fresh_anchor(anchor, origins):
    block = origins[anchor.chain]
    return revalidated(anchor, height=block["number"], reference=ChainReference(block["hash"]))


def distinct_operators(fixture):
    """Each Safe fund submits through its own operator, funded with the shared gas balance."""
    operators, funds = [], []
    for index, fund in enumerate(fixture.funds):
        name = f"operator_{index}"
        operators.append(FixtureOperator(name=name, native_balance=fixture.native_balance(fund)))
        funds.append(revalidated(fund, operator=name))
    return revalidated(fixture, funds=tuple(funds), operators=tuple(operators))


def fork_fixture(case, origins):
    fixture = example_operator(case, "fork").fork
    if case.input_policy == "fresh":
        fixture = revalidated(
            fixture, anchors=tuple(fresh_anchor(a, origins) for a in fixture.anchors)
        )
    if case.profile == "eoa":
        return eoa_inputs(fixture)
    if case.operator_mode == "distinct":
        return distinct_operators(fixture)
    return fixture


def fork_operator(case, origins):
    return revalidated(example_operator(case, "fork"), fork=fork_fixture(case, origins))


def backtest_operator(case):
    operator = example_operator(case, "backtest")
    networks = tuple(revalidated(network, provider=PROVIDER) for network in operator.networks)
    return revalidated(operator, networks=networks)


def pinned_anchors(context):
    """Compilation binds the selected height and reference; the timestamp is unconstrained."""
    return ResolutionAnchors(
        context_digest=context.digest,
        checkpoints=tuple(
            CheckpointReference(
                chain=Chain(chain_id=anchor.chain, environment="mainnet"),
                height=anchor.height,
                reference=ChainReference(str(anchor.reference)),
                timestamp=0,
            )
            for anchor in context.anchors
        ),
    )


def require_fixture(case, directory, operator):
    inputs = case_inputs(case, directory)
    context = inputs.context(operator, RunMode.FORK, operator.fork.anchors)
    return compile_fixture(context, operator, pinned_anchors(context))


def case_operator(case, directory, origins) -> Path:
    """Write one case's operator YAML and refuse anything the gateway would not accept."""
    path = directory / "operator.yaml"
    fork = case.lane == "fork"
    operator = fork_operator(case, origins) if fork else backtest_operator(case)
    path.write_text(yaml.safe_dump(operator.model_dump(mode="json"), sort_keys=False))
    validated = read_operator_config(path)
    if fork:
        require_fixture(case, directory, validated)
    return path
