"""Offline chain-aware read endpoint resolution and transport verification."""

import json

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway import read_endpoints
from almanak.gateway.ethereum import BUSY_WAIT_LIMIT, DataError, EthereumReader
from almanak.gateway.read_endpoints import (
    alchemy_key,
    mainnet_reader,
    read_endpoint,
    verified_reader,
)

ETHEREUM = ChainId("eip155:1")
AVALANCHE = ChainId("eip155:43114")
KEY = "test-key/$literal"
ENCODED_KEY = "test-key%2F%24literal"


@pytest.fixture
def env_file(tmp_path):
    path = tmp_path / "operator.env"
    path.write_text(f"ALCHEMY_API_KEY={KEY}\n")
    return path


@pytest.fixture
def fake_rpc(monkeypatch):
    def install(reported):
        requests = []

        def handle(request):
            body = json.loads(request.content)
            requests.append(body)
            result = hex(reported) if body["method"] == "eth_chainId" else None
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

        def construct(chain, endpoint, **options):
            client = httpx.Client(transport=httpx.MockTransport(handle))
            return EthereumReader(chain, endpoint, client=client, **options)

        monkeypatch.setattr(read_endpoints, "EthereumReader", construct)
        return requests

    return install


def test_alchemy_key_reads_the_operator_env_file(env_file):
    assert alchemy_key(env_file) == KEY


def test_alchemy_key_missing_refuses_without_leaking_other_values(tmp_path):
    path = tmp_path / "operator.env"
    path.write_text("UNRELATED=other-secret\n")
    with pytest.raises(ValueError, match="ALCHEMY_API_KEY is required") as failure:
        alchemy_key(path)
    assert "other-secret" not in str(failure.value)


@pytest.mark.parametrize(
    ("chain", "slug"),
    [(ETHEREUM, "eth-mainnet"), (AVALANCHE, "avax-mainnet")],
)
def test_read_endpoint_resolves_the_chain_alchemy_url(env_file, chain, slug):
    assert read_endpoint(chain, env_file) == f"https://{slug}.g.alchemy.com/v2/{ENCODED_KEY}"


def test_read_endpoint_refuses_a_chain_without_alchemy_binding(env_file):
    with pytest.raises(ValueError, match="no Alchemy read endpoint") as failure:
        read_endpoint(ChainId("eip155:999999"), env_file)
    assert KEY not in str(failure.value)


def test_read_endpoint_requires_the_key_without_leaking_other_values(env_file):
    env_file.write_text("UNRELATED=other-secret\n")
    with pytest.raises(ValueError, match="ALCHEMY_API_KEY is required") as failure:
        read_endpoint(ETHEREUM, env_file)
    assert "other-secret" not in str(failure.value)


@pytest.mark.parametrize(
    ("chain", "reported"),
    [(ETHEREUM, 1), (AVALANCHE, 43114)],
)
def test_verified_reader_returns_a_verified_context_manager(env_file, fake_rpc, chain, reported):
    requests = fake_rpc(reported)
    reader = verified_reader(chain, env_file)
    assert reader.chain.chain_id == chain
    assert reader.chain.environment == "mainnet"
    assert [call["method"] for call in requests] == ["eth_chainId"]
    # Observation reads re-send a transient single read; other mainnet readers do not.
    assert reader._retry_budget == BUSY_WAIT_LIMIT
    assert not mainnet_reader(chain, "https://rpc.invalid")._retry_budget
    with reader as entered:
        assert entered is reader


def test_observation_readers_pin_tags_behind_a_head_the_provider_misplaces(env_file, fake_rpc):
    fake_rpc(999)
    hyperevm = ChainId("eip155:999")
    # Alchemy runs HyperEVM reads pinned near its head elsewhere; see `EvmNetwork`.
    assert verified_reader(hyperevm, env_file)._tag_depth == 256
    assert not mainnet_reader(hyperevm, "https://rpc.invalid")._tag_depth
    assert not mainnet_reader(ETHEREUM, "https://rpc.invalid", observation=True)._tag_depth


def test_resolution_readers_pin_a_fresh_anchor_behind_a_head_the_provider_misplaces(env_file):
    """A production check resolves at `latest`; on HyperEVM that anchor pins the observation
    depth behind the head, where Alchemy serves pinned state (blueprint 30, section 14.2)."""
    from types import SimpleNamespace

    from almanak.gateway.bootstrap import BootstrapService
    from almanak.gateway.providers import AlchemyProvider

    def service(network):
        provider = AlchemyProvider(kind="alchemy", network=network, api_key_env="ALCHEMY_API_KEY")
        bound = SimpleNamespace(binding=lambda: provider)
        built = object.__new__(BootstrapService)
        built.operator, built.env_file = SimpleNamespace(network=lambda chain: bound), env_file
        return built

    with service("hyperliquid-mainnet").reader(ChainId("eip155:999")) as reader:
        assert reader._tag_depth == 256 and reader._retry_budget == BUSY_WAIT_LIMIT
    with service("eth-mainnet").reader(ETHEREUM) as reader:
        assert not reader._tag_depth


def test_verified_reader_refuses_a_chain_mismatch(env_file, fake_rpc):
    fake_rpc(reported=43114)
    with pytest.raises(DataError, match="does not match the requested chain") as failure:
        verified_reader(ETHEREUM, env_file)
    assert failure.value.kind == "chain_mismatch"
    assert KEY not in str(failure.value)
