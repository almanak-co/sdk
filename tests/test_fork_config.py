import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway import fork_fixture
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.fork_config import (
    AlchemyProvider,
    UrlProvider,
    provider_endpoint,
    validate_provider_url,
)
from almanak.gateway.fork_execution import allocation_signer, validate_fork_scope
from almanak.gateway.fork_fixture import Fork
from almanak.models import Block, Chain
from tests.fork.eoa import configuration

ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json"
PINNED = ROOT / "tests/fixtures/aave-v4-interval-config.json"


@pytest.fixture
def config_raw():
    return configuration()


@pytest.fixture
def pinned():
    return ForkConfig.model_validate_json(PINNED.read_text())


def parse(raw):
    return ForkConfig.model_validate_json(json.dumps(raw))


def other_chain(raw, reference="43114", native="9005"):
    serialized = json.dumps(raw).replace("eip155:1", f"eip155:{reference}")
    return json.loads(serialized.replace("slip44:60", f"slip44:{native}"))


def test_legacy_conversion_is_explicit_and_latest_requires_resolution(config_raw, pinned):
    assert pinned.single_target().provider.network == "eth-mainnet"
    assert pinned.funds[0].funding == "wrap_native"
    assert pinned.origin.number == 25908472
    latest = parse(config_raw)
    with pytest.raises(ValueError, match="resolve latest"):
        _ = latest.origin
    assert latest.resolved(pinned.origin).funds == latest.funds
    assert latest.resolved(pinned.origin).origin == pinned.origin


@pytest.mark.parametrize("field", ["provider", "block"])
def test_new_target_requires_explicit_choices(config_raw, field):
    del config_raw["forks"][0][field]
    with pytest.raises(ValidationError):
        parse(config_raw)


def test_funding_is_required_and_no_unsupported_method(config_raw):
    del config_raw["funds"][0]["funding"]
    with pytest.raises(ValidationError):
        parse(config_raw)
    config_raw["funds"][0]["funding"] = "storage_override"
    with pytest.raises(ValidationError):
        parse(config_raw)


def test_legacy_and_new_must_not_mix(config_raw):
    config_raw["origin"] = json.loads(PINNED.read_text())["origin"]
    with pytest.raises(ValidationError, match="not both"):
        parse(config_raw)


@pytest.mark.parametrize("change", ["chain", "asset", "native"])
def test_legacy_conversion_retains_original_limits(change):
    raw = json.loads(PINNED.read_text())
    if change == "chain":
        raw["origin"]["chain"]["chain_id"] = "eip155:8453"
    elif change == "asset":
        raw["funds"][0]["allocation"]["reserve"]["asset"] = "eip155:1/erc20:0x" + "12" * 20
    else:
        raw["funds"][0]["native_balance"]["asset"] = "eip155:1/slip44:966"
    with pytest.raises(ValidationError, match="legacy"):
        parse(raw)


def test_duplicate_and_multiple_target_boundaries(config_raw):
    config_raw["forks"].append(config_raw["forks"][0])
    with pytest.raises(ValidationError, match="unique"):
        parse(config_raw)
    second = other_chain(configuration())
    second["funds"][0]["account"] = "second"
    for allocation in second["allocations"]:
        allocation["account"] = "second"
        allocation["name"] += "_second"
    second["funds"][0]["foundry_account_index"] = 1
    config_raw["forks"][1] = second["forks"][0]
    config_raw["funds"].extend(second["funds"])
    config_raw["allocations"].extend(second["allocations"])
    config = parse(config_raw)
    assert len(config.forks) == 2
    with pytest.raises(ValueError, match="fork collection"):
        config.single_target()


@pytest.mark.parametrize("change", ["unbound", "gas", "empty", "index", "non_evm"])
def test_scope_refusals(config_raw, change):
    if change == "unbound":
        config_raw["forks"][0]["chain"]["environment"] = "other"
    elif change == "gas":
        config_raw["funds"][0]["native_balance"]["asset"] = "eip155:1/slip44:966"
    elif change == "empty":
        config_raw["funds"] = []
    elif change == "index":
        config_raw["funds"].append(dict(config_raw["funds"][0]))
        config_raw["funds"][1]["account"] = "second"
    else:
        config_raw["forks"][0]["chain"]["chain_id"] = "cosmos:cosmoshub-4"
    with pytest.raises(ValidationError):
        parse(config_raw)


def test_native_funding_does_not_claim_token_funding(config_raw):
    fund = config_raw["funds"][0]
    fund["funding"] = "native"
    with pytest.raises(ValidationError, match="zero token_balance"):
        parse(config_raw)
    fund["token_balance"] = 0
    assert parse(config_raw).funds[0].funding == "native"


def test_provider_selection_uses_only_requested_env(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text(
        "ALCHEMY_API_KEY=chosen\nBASE_RPC_URL=https://example.invalid/private\nCOINGECKO_API_KEY=unused\n"
    )
    monkeypatch.setenv("ALCHEMY_API_KEY", "wrong")
    alchemy = AlchemyProvider(kind="alchemy", network="base-mainnet", api_key_env="ALCHEMY_API_KEY")
    assert provider_endpoint(alchemy, path) == "https://base-mainnet.g.alchemy.com/v2/chosen"
    assert (
        provider_endpoint(UrlProvider(kind="rpc_url", url_env="BASE_RPC_URL"), path)
        == "https://example.invalid/private"
    )
    with pytest.raises(ValueError, match="MISSING is required"):
        provider_endpoint(UrlProvider(kind="rpc_url", url_env="MISSING"), path)


@pytest.mark.parametrize(
    "url",
    ["ftp://host/key", "https://host:invalid/key", "invalid-secret", "https://host/key#secret"],
)
def test_bad_provider_url_does_not_echo_secret(url):
    with pytest.raises(ValueError) as raised:
        validate_provider_url(url)
    assert url not in str(raised.value)


def mock_reader(chain, block, wrong_chain=False):
    def handle(request):
        body = json.loads(request.content)
        result = (
            hex(1 if wrong_chain else int(chain.chain_id.reference))
            if body["method"] == "eth_chainId"
            else block
        )
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    return EthereumReader(
        chain,
        "https://rpc.invalid/secret",
        client=httpx.Client(transport=httpx.MockTransport(handle)),
    )


def test_latest_and_pinned_resolution_verify_chain_and_identity(
    config_raw, pinned, monkeypatch, tmp_path
):
    config = parse(other_chain(config_raw))
    target = config.single_target()
    block = {"number": "0x42", "hash": "0x" + "12" * 32, "timestamp": "0x99"}
    reader = mock_reader(target.chain, block)
    monkeypatch.setattr("almanak.gateway.fork_config.EthereumReader", lambda *_: reader)
    env = tmp_path / ".env"
    env.write_text("ALCHEMY_API_KEY=secret\n")
    resolved = target.resolve(env)
    assert resolved.number == 66
    assert resolved.chain.chain_id == ChainId("eip155:43114")
    pinned_target = target.model_copy(update={"block": resolved})
    assert pinned_target.resolve(env) == resolved
    with pytest.raises(ValueError, match="pinned origin"):
        pinned_target.model_copy(
            update={"block": resolved.model_copy(update={"hash": "0x" + "34" * 32})}
        ).resolve(env)
    reader._client.close()
    wrong = mock_reader(target.chain, block, wrong_chain=True)
    monkeypatch.setattr("almanak.gateway.fork_config.EthereumReader", lambda *_: wrong)
    with pytest.raises(DataError, match="chain"):
        target.resolve(env)
    wrong._client.close()
    with pytest.raises(ValidationError, match="target chain"):
        config.resolved(pinned.origin)


def test_non_ethereum_preparation_preserves_native_identity(config_raw, tmp_path):
    from almanak.gateway.fork_workspace import ForkSet

    config = parse(other_chain(config_raw))
    block = Block(chain=config.single_target().chain, number=42, hash="0x" + "12" * 32, timestamp=1)
    config = config.resolved(block)
    node = object.__new__(Fork)
    node.origin = block
    node.metadata = {"instanceId": "0x" + "34" * 32}
    node.rpc = SimpleNamespace(endpoint="http://127.0.0.1:1234")
    node.funded_signer = lambda *_, **__: SimpleNamespace(
        address="0x" + "56" * 20, key=bytes.fromhex("78" * 32)
    )
    nodes = ForkSet(config, tmp_path / ".env")
    nodes.nodes = {block.chain: node}
    deployment = nodes.prepare(config, tmp_path / "deployment")
    allocation = deployment.allocations[0]
    assert allocation.native_asset == AssetType("eip155:43114/slip44:9005")
    assert deployment.fork_bindings[0].origin == block
    assert deployment.fork_bindings[0].signing_chain == ChainId("eip155:31337")
    validate_fork_scope(deployment, allocation.reserve.chain)
    with pytest.raises(ValueError, match="bound transaction submitter"):
        allocation_signer(deployment, allocation.reserve.chain, allocation, "0x" + "78" * 32)


def test_wrapping_verifies_balance_delta():
    node = object.__new__(Fork)
    rpc = SimpleNamespace(send=lambda *_: None)
    values = iter(["0x5", "0xf"])

    class Calls:
        send = rpc.send

        def __call__(self, *_):
            return next(values)

    node.rpc = Calls()
    signer = SimpleNamespace(address="0x" + "12" * 20)
    node.wrap_native(signer, "0x" + "34" * 20, 10)
    values = iter(["0x5", "0x5"])
    with pytest.raises(ValueError, match="declared token"):
        node.wrap_native(signer, "0x" + "34" * 20, 10)


def test_origin_validation_and_failed_start_cleanup(pinned, tmp_path, monkeypatch):
    node = Fork(tmp_path / ".env", pinned.origin)
    node.metadata = {
        "instanceId": "0x" + "11" * 32,
        "chainId": 31337,
        "forkedNetwork": {
            "forkBlockNumber": pinned.origin.number,
            "forkBlockHash": pinned.origin.hash,
        },
    }
    actual = json.loads(json.dumps(node.metadata))
    node.rpc = fork_fixture.Rpc("http://127.0.0.1:9")
    monkeypatch.setattr(fork_fixture.Rpc, "__call__", lambda *_: actual)
    node._validate_origin()
    actual["chainId"] = 1
    with pytest.raises(ValueError, match="Anvil origin"):
        node._validate_origin()
    node.rpc.signing_chain = ChainId("eip155:1")
    node._validate_origin()
    actual["instanceId"] = "0x" + "22" * 32
    with pytest.raises(ValueError, match="Anvil origin"):
        node._validate_origin()
    closed = []

    def fail():
        node.cleanup.callback(closed.append, True)
        raise RuntimeError("failed startup")

    monkeypatch.setattr(node, "_start", fail)
    with pytest.raises(RuntimeError, match="failed startup"), node:
        pass
    assert closed == [True]
    other = pinned.origin.model_copy(
        update={"chain": Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")}
    )
    with pytest.raises(ValueError, match="explicit provider"):
        fork_fixture.legacy_target(other)
    with pytest.raises(ValueError, match="different chains"):
        Fork(tmp_path / ".env", other, target=pinned.single_target())


@pytest.mark.parametrize("change", ["funds", "reserve", "funding"])
def test_malformed_legacy_inputs_are_validation_errors(change):
    raw = json.loads(PINNED.read_text())
    if change == "funds":
        del raw["funds"]
    elif change == "reserve":
        del raw["funds"][0]["allocation"]["reserve"]
    else:
        raw["funds"][0]["funding"] = "native"
    with pytest.raises(ValidationError, match="legacy"):
        parse(raw)


class FeeReader:
    def __init__(self, base_fee: str, gas_price: str):
        self.answers = {
            "eth_getBlockByNumber": {"baseFeePerGas": base_fee},
            "eth_gasPrice": gas_price,
        }

    def _rpc(self, method, params):
        return self.answers[method]


def fork_origin(chain):
    return Block(
        chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
        number=1,
        hash="0x" + "11" * 32,
        timestamp=1,
    )


def test_fork_starts_at_the_fork_block_base_fee_without_anvil_tips():
    arguments = fork_fixture.fee_arguments(
        FeeReader("0x1312d00", "0x3b9aca00"), fork_origin("eip155:42161")
    )
    assert arguments == ["--base-fee", str(0x1312D00), "--no-priority-fee"]


def test_chains_without_fee_history_tips_start_at_the_upstream_gas_price():
    arguments = fork_fixture.fee_arguments(FeeReader("0x0", "0x2faf080"), fork_origin("eip155:56"))
    assert arguments == ["--base-fee", str(0x2FAF080), "--no-priority-fee"]
