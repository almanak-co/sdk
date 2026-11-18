import json
from pathlib import Path

import httpx
import pytest
from hypothesis import settings

from almanak.caip import ChainId
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.connectors.uniswap_v4.domain import PoolPriceRequest
from almanak.gateway.ethereum import EthereumReader
from almanak.models import Chain
from almanak.strategy import StrategyDefinition

ROOT = Path(__file__).parents[1]
pytest_plugins = ("tests.evidence_guard", "pytester")

# Property tests seed their examples from each test's own source and keep no example
# database on disk. Hypothesis also mixes in constants it collects from loaded
# modules, so the cases can differ with what an xdist worker has imported; every
# failure therefore prints a @reproduce_failure blob that replays it exactly.
# Example counts stay small enough for the offline suite under -n auto; worker load
# makes timing deadlines meaningless. `--hypothesis-profile=explore` draws many
# fresh random cases for local bug hunting; a failure it finds must become a fixed
# example.
settings.register_profile(
    "almanak", derandomize=True, database=None, deadline=None, print_blob=True
)
settings.register_profile(
    "explore", derandomize=False, database=None, deadline=None, max_examples=5000
)
settings.load_profile("almanak")


@pytest.fixture
def reference_strategy():
    return StrategyDefinition.read(ROOT / "strategies/aave_v4_weth_supply/strategy.py")


@pytest.fixture
def recorded(reference_strategy):
    from almanak.connectors.aave_v4.reader import plan_call
    from almanak.runtime import strategy_capabilities
    from tests.lending_captures import read_capture

    state = Checkpoint.model_validate_json(
        read_capture(ROOT / "tests/fixtures/aave-v4-interval-lifecycle.json")
    )
    # Component inputs reuse historical observations, with today's actual author contract.
    # The raw fixture remains unchanged and is separately audited byte-for-byte.
    from almanak.memory_codec import freeze

    namespace = {}
    exec(compile(reference_strategy.source, "strategy.py", "exec"), namespace)
    memory = freeze(namespace, namespace["State"](**state.memory.values))
    outcomes = tuple(
        o.model_copy(
            update={
                "call": plan_call(
                    state.config.allocation(o.operation.allocation),
                    o.operation.target.fund,
                    o.operation,
                )
            }
        )
        for o in state.outcomes
    )
    return state.model_copy(
        update={
            "strategy": reference_strategy,
            "memory": memory,
            "capabilities": strategy_capabilities(reference_strategy, state.config),
            "outcomes": outcomes,
        }
    )


def pytest_addoption(parser):
    parser.addoption("--fork-env", type=Path, help="explicit provider env file for fork proofs")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--fork-env") is None:
        skip = pytest.mark.skip(reason="fork proof requires explicit --fork-env")
        for item in items:
            if item.get_closest_marker("fork"):
                item.add_marker(skip)


@pytest.fixture
def price_request():
    return PoolPriceRequest.model_validate_json(
        (ROOT / "examples/ethereum/uniswap-v4-weth-usdc.json").read_text()
    )


@pytest.fixture
def reserve_source():
    return AaveReserve.model_validate_json(
        (ROOT / "examples/ethereum/aave-v4-main-usdc.json").read_text()
    )


@pytest.fixture
def trace():
    return json.loads((ROOT / "tests/fixtures/ethereum-25908472.json").read_text())


@pytest.fixture
def rpc_factory(trace):
    clients = []
    responses = {
        json.dumps([row["method"], row["params"]], sort_keys=True): row["result"]
        for row in trace["requests"]
    }

    def factory(override=None, chain=None):
        requests = []

        def handle(request):
            body = json.loads(request.content)
            requests.append(body)
            replacement = override(body) if override is not None else None
            if replacement is not None:
                if isinstance(replacement, httpx.Response):
                    return replacement
                result = replacement
            else:
                key = json.dumps([body["method"], body["params"]], sort_keys=True)
                assert key in responses, f"unexpected RPC read: {body['method']} {body['params']}"
                result = {"result": responses[key]}
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], **result})

        client = httpx.Client(transport=httpx.MockTransport(handle))
        clients.append(client)
        reader = EthereumReader(
            chain or Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
            "https://rpc.invalid/secret-key",
            client=client,
        )
        return reader, requests

    yield factory
    for client in clients:
        client.close()
