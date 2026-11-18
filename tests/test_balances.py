import json

import httpx
import pytest
from eth_abi import encode
from eth_utils import keccak
from pydantic import ValidationError

from almanak.balances import BalanceObservation, parse_balance_request, read_balance
from almanak.caip import AssetType, ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Account, Block, Chain

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
OTHER_CHAIN = Chain(chain_id=ChainId("eip155:43114"), environment="mainnet")
BLOCK_NUMBER = 25908472
BLOCK_HASH = "0x" + "ab" * 32
HOLDER = "0x" + "12" * 20
NATIVE = "eip155:1/slip44:60"
USDC = "eip155:1/erc20:0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


def abi(*values, types):
    return {"result": "0x" + encode(types, values).hex()}


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


def block_result():
    return {"number": hex(BLOCK_NUMBER), "hash": BLOCK_HASH, "timestamp": hex(1750000000)}


@pytest.fixture
def rpc_factory():
    clients = []

    def factory(handle, chain=CHAIN):
        requests = []

        def transport(request):
            body = json.loads(request.content)
            requests.append(body)
            reply = handle(body["method"], body["params"])
            if isinstance(reply, httpx.Response):
                return reply
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], **reply})

        client = httpx.Client(transport=httpx.MockTransport(transport))
        clients.append(client)
        reader = EthereumReader(chain, "https://rpc.invalid/secret-key", client=client)
        return reader, requests

    yield factory
    for client in clients:
        client.close()


def base_handle(method, params):
    if method == "eth_chainId":
        return {"result": "0x1"}
    if method == "eth_getBlockByNumber":
        return {"result": block_result()}
    raise AssertionError(f"unexpected RPC read: {method} {params}")


def holder(chain=CHAIN):
    return Account(chain=chain, address=HOLDER)


def test_native_balance_at_pinned_block(rpc_factory):
    def handle(method, params):
        if method == "eth_getBalance":
            return {"result": hex(1234567890123456789)}
        return base_handle(method, params)

    reader, requests = rpc_factory(handle)
    observation = read_balance(reader, AssetType(NATIVE), holder(), BLOCK_NUMBER)
    assert observation.asset == AssetType(NATIVE)
    assert observation.account == holder()
    assert observation.denomination == "native"
    assert observation.amount == "1234567890123456789"
    assert observation.decimals == 18
    assert (observation.block.number, observation.block.hash) == (BLOCK_NUMBER, BLOCK_HASH)
    (call,) = [r for r in requests if r["method"] == "eth_getBalance"]
    assert call["params"] == [HOLDER, {"blockHash": BLOCK_HASH, "requireCanonical": True}]
    assert BalanceObservation.model_validate_json(observation.model_dump_json()) == observation


def test_erc20_balance_at_pinned_block(rpc_factory):
    def handle(method, params):
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith(selector("balanceOf(address)")):
                return abi(42000001, types=("uint256",))
            if data == selector("decimals()"):
                return abi(6, types=("uint8",))
        return base_handle(method, params)

    reader, requests = rpc_factory(handle)
    observation = read_balance(reader, AssetType(USDC), holder(), "finalized")
    assert observation.asset == AssetType(USDC)
    assert observation.denomination == "erc20"
    assert observation.amount == "42000001"
    assert observation.decimals == 6
    calls = [r for r in requests if r["method"] == "eth_call"]
    assert len(calls) == 2
    for call in calls:
        assert call["params"][0]["to"] == AssetType(USDC).reference
        assert call["params"][1] == {"blockHash": BLOCK_HASH, "requireCanonical": True}
    balance_call = next(
        r for r in calls if r["params"][0]["data"].startswith(selector("balanceOf(address)"))
    )
    assert balance_call["params"][0]["data"].endswith(HOLDER[2:])


@pytest.mark.parametrize(
    "reply", [{"error": {"code": -32000, "message": "revert"}}, {"result": "0x"}]
)
def test_erc20_without_conforming_decimals_has_no_decimals(rpc_factory, reply):
    def handle(method, params):
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith(selector("balanceOf(address)")):
                return abi(7, types=("uint256",))
            if data == selector("decimals()"):
                return reply
        return base_handle(method, params)

    reader, _ = rpc_factory(handle)
    observation = read_balance(reader, AssetType(USDC), holder(), BLOCK_NUMBER)
    assert observation.amount == "7"
    assert observation.decimals is None


def test_decimals_transport_failure_is_not_tolerated(rpc_factory):
    def handle(method, params):
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith(selector("balanceOf(address)")):
                return abi(7, types=("uint256",))
            if data == selector("decimals()"):
                return httpx.Response(503)
        return base_handle(method, params)

    reader, _ = rpc_factory(handle)
    with pytest.raises(DataError, match="HTTP 503"):
        read_balance(reader, AssetType(USDC), holder(), BLOCK_NUMBER)


def test_decimals_envelope_failure_is_not_tolerated(rpc_factory):
    def handle(method, params):
        if method == "eth_call":
            data = params[0]["data"]
            if data.startswith(selector("balanceOf(address)")):
                return abi(7, types=("uint256",))
            if data == selector("decimals()"):
                return httpx.Response(200, text="not json")
        return base_handle(method, params)

    reader, _ = rpc_factory(handle)
    with pytest.raises(DataError, match="not JSON"):
        read_balance(reader, AssetType(USDC), holder(), BLOCK_NUMBER)


@pytest.mark.parametrize("amount", ["0junk", "junk1", "00", "1.0", "-1", " 1", "1 "])
def test_balance_observation_rejects_noncanonical_amounts(amount):
    block = Block(chain=CHAIN, number=BLOCK_NUMBER, hash=BLOCK_HASH, timestamp=1750000000)
    with pytest.raises(ValidationError):
        BalanceObservation(
            asset=AssetType(NATIVE),
            account=holder(),
            block=block,
            amount=amount,
            denomination="native",
        )


def test_malformed_native_balance_is_not_zero(rpc_factory):
    def handle(method, params):
        if method == "eth_getBalance":
            return {"result": "not-a-quantity"}
        return base_handle(method, params)

    reader, _ = rpc_factory(handle)
    with pytest.raises(DataError, match="invalid balance quantity"):
        read_balance(reader, AssetType(NATIVE), holder(), BLOCK_NUMBER)


def test_asset_chain_must_match_reader_chain(rpc_factory):
    reader, requests = rpc_factory(base_handle)
    with pytest.raises(ValueError, match="asset chain differs"):
        read_balance(reader, AssetType("eip155:43114/slip44:9005"), holder(), BLOCK_NUMBER)
    assert requests == []


def test_account_chain_must_match_reader_chain(rpc_factory):
    reader, requests = rpc_factory(base_handle)
    with pytest.raises(ValueError, match="account chain/environment differs"):
        read_balance(reader, AssetType(NATIVE), holder(OTHER_CHAIN), BLOCK_NUMBER)
    foreign_environment = Chain(chain_id=ChainId("eip155:1"), environment="anvil:mainnet")
    with pytest.raises(ValueError, match="account chain/environment differs"):
        read_balance(reader, AssetType(NATIVE), holder(foreign_environment), BLOCK_NUMBER)
    assert requests == []


def test_slip44_asset_must_be_the_reader_chains_native_asset(rpc_factory):
    reader, requests = rpc_factory(base_handle)
    with pytest.raises(ValueError, match="not the reader chain's native asset"):
        read_balance(reader, AssetType("eip155:1/slip44:9005"), holder(), BLOCK_NUMBER)
    assert requests == []


def test_unsupported_asset_namespace_is_rejected(rpc_factory):
    reader, requests = rpc_factory(base_handle)
    asset = AssetType("eip155:1/erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D")
    with pytest.raises(ValueError, match="only slip44"):
        read_balance(reader, asset, holder(), BLOCK_NUMBER)
    assert requests == []


def test_parse_balance_request_accepts_native_and_erc20():
    asset, account = parse_balance_request(NATIVE, HOLDER, None)
    assert asset == AssetType(NATIVE)
    assert account == Account(chain=CHAIN, address=HOLDER)
    asset, account = parse_balance_request(
        USDC, HOLDER.upper().replace("0X", "0x"), ChainId("eip155:1")
    )
    assert asset == AssetType(USDC)
    assert account.address == HOLDER


@pytest.mark.parametrize(
    "asset",
    [
        "ETH",
        "eip155:1",
        "eip155:1/erc20:0x123",
        "eip155:1/erc20:0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48/1",
        "eip155:1/erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
    ],
)
def test_parse_balance_request_rejects_malformed_or_unsupported_assets(asset):
    with pytest.raises(ValueError, match="CAIP-19|slip44"):
        parse_balance_request(asset, HOLDER, None)


@pytest.mark.parametrize(
    "account",
    [
        "0x123",
        "not-an-address",
        "0x" + "12" * 19,
        "0x" + "aB" * 20,
        "0x" + "00" * 20,
    ],
)
def test_parse_balance_request_rejects_malformed_addresses(account):
    with pytest.raises(ValueError):
        parse_balance_request(NATIVE, account, None)


def test_parse_balance_request_rejects_chain_hint_mismatch():
    with pytest.raises(ValueError, match="chain hint differs"):
        parse_balance_request(NATIVE, HOLDER, ChainId("eip155:43114"))


def test_parse_balance_request_rejects_non_evm_chain():
    with pytest.raises(ValueError, match="EIP-155"):
        parse_balance_request("bip122:" + "00" * 16 + "/slip44:0", HOLDER, None)
