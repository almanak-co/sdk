import json
from fractions import Fraction

import httpx
import pytest
from eth_abi import encode
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import ChainId
from almanak.connectors.aave_v4.reader import read_position, read_reserve
from almanak.connectors.uniswap_v4.domain import PoolObservation, PoolPriceRequest
from almanak.connectors.uniswap_v4.pool import pool_id, read_pool
from almanak.gateway.ethereum import DataError
from almanak.models import Account, Block, Chain, SafeRoles, address


def abi(*values, types):
    return {"result": "0x" + encode(types, values).hex()}


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


def test_live_pool_fixture_keeps_identity_and_exact_units(rpc_factory, price_request):
    reader, requests = rpc_factory()
    block = reader.pin_block(25908472)
    observation = read_pool(reader, price_request.source, block)
    assert pool_id(observation.source) == (
        "0x4f88f7c99022eace4740c6898f59ce6a2e798a1e64ce54589720b7153eb224a7"
    )
    assert (observation.currency0.decimals, observation.currency1.decimals) == (6, 18)
    assert observation.sqrt_price_x96 == 1599370490694547877509740815464336
    assert observation.liquidity == 29010600893462209
    for request in requests:
        if request["method"] == "eth_call":
            assert request["params"][1] == {"blockHash": block.hash, "requireCanonical": True}
    inverse = PoolPriceRequest(
        source=price_request.source, base=price_request.quote, quote=price_request.base
    )
    assert observation.spot_price(price_request) * observation.spot_price(inverse) == 1
    assert PoolObservation.model_validate_json(observation.model_dump_json()) == observation


def test_price_dimension_is_not_usd_or_an_assumed_peg(rpc_factory, price_request):
    def override(request):
        if request["method"] == "eth_call" and request["params"][0]["data"].startswith(
            selector("getSlot0(bytes32)")
        ):
            return abi(2**96, 0, 0, 500, types=("uint160", "int24", "uint24", "uint24"))

    reader, _ = rpc_factory(override)
    observation = read_pool(reader, price_request.source, reader.pin_block(25908472))
    assert observation.spot_price(price_request) == Fraction(10**12)


@pytest.mark.parametrize("change", ["missing", "vendor", "foreign_quote", "hooks"])
def test_price_source_is_explicit_and_validated(price_request, change):
    raw = price_request.model_dump()
    if change == "missing":
        del raw["source"]
    elif change == "vendor":
        raw["source"]["kind"] = "coingecko"
    elif change == "foreign_quote":
        raw["quote"] = "0x" + "11" * 20
    else:
        raw["source"]["hooks"] = "0x" + "11" * 20
    with pytest.raises(ValidationError):
        PoolPriceRequest.model_validate(raw)


@pytest.mark.parametrize("failure", ["uninitialized", "no_liquidity", "rpc", "missing_metadata"])
def test_failed_pool_reads_never_substitute_a_price(rpc_factory, price_request, failure):
    def override(request):
        if request["method"] != "eth_call":
            return None
        data = request["params"][0]["data"]
        if data.startswith(selector("getSlot0(bytes32)")):
            if failure == "uninitialized":
                return abi(0, 0, 0, 500, types=("uint160", "int24", "uint24", "uint24"))
            if failure == "rpc":
                return {"error": {"code": -32000, "message": "pool unavailable"}}
        if failure == "no_liquidity" and data.startswith(selector("getLiquidity(bytes32)")):
            return abi(0, types=("uint128",))
        if failure == "missing_metadata" and data == selector("decimals()"):
            return {"result": "0x"}

    reader, requests = rpc_factory(override)
    with pytest.raises((DataError, ValueError)):
        read_pool(reader, price_request.source, reader.pin_block(25908472)).spot_price(
            price_request
        )
    assert {r["method"] for r in requests} <= {"eth_chainId", "eth_getBlockByNumber", "eth_call"}


def test_pool_observation_cannot_satisfy_another_source(rpc_factory, price_request):
    reader, _ = rpc_factory()
    observation = read_pool(reader, price_request.source, reader.pin_block(25908472))
    raw = price_request.model_dump()
    raw["source"]["fee"] = 3000
    raw["source"]["tick_spacing"] = 60
    with pytest.raises(ValueError, match="requested source"):
        observation.spot_price(PoolPriceRequest.model_validate(raw))


def test_live_aave_fixture_uses_spoke_reserve_and_hub(rpc_factory, reserve_source):
    reader, _ = rpc_factory()
    reserve = read_reserve(reader, reserve_source, reader.pin_block(25908472))
    assert reserve.source.reserve_id == 7
    assert reserve.hub_asset_id == 5
    assert reserve.asset.decimals == 6
    assert reserve.borrowable and not reserve.paused and not reserve.frozen


@pytest.mark.parametrize("bad_total", [False, True])
def test_aave_position_preserves_fund_owner_and_premium_debt(
    rpc_factory,
    reserve_source,
    bad_total,
):
    owner = Account(chain=reserve_source.chain, address="0x" + "11" * 20)
    replies = {
        selector("getUserSuppliedAssets(uint256,address)"): abi(5000000, types=("uint256",)),
        selector("getUserSuppliedShares(uint256,address)"): abi(4200000, types=("uint256",)),
        selector("getUserDebt(uint256,address)"): abi(100, 7, types=("uint256", "uint256")),
        selector("getUserTotalDebt(uint256,address)"): abi(
            100 if bad_total else 107, types=("uint256",)
        ),
    }

    def override(request):
        if request["method"] == "eth_call":
            data = request["params"][0]["data"]
            if data[:10] in replies:
                assert data.endswith(owner.address[2:].rjust(64, "0"))
                return replies[data[:10]]

    reader, _ = rpc_factory(override)
    block = reader.pin_block(25908472)
    if bad_total:
        with pytest.raises(DataError, match="total debt"):
            read_position(reader, reserve_source, owner, block)
    else:
        position = read_position(reader, reserve_source, owner, block)
        assert position.account == owner
        assert position.supplied_assets != position.supplied_shares
        assert position.total_debt == 107


def test_wrong_hub_cannot_be_relabelled(rpc_factory, reserve_source):
    raw = reserve_source.model_dump()
    raw["hub"] = "0x" + "22" * 20
    reader, _ = rpc_factory()
    with pytest.raises(DataError, match="requested asset and Hub"):
        read_reserve(reader, type(reserve_source).model_validate(raw), reader.pin_block(25908472))


def test_chain_and_fork_scope_are_not_interchangeable(rpc_factory, price_request):
    reader, _ = rpc_factory(chain=Chain(chain_id=ChainId("eip155:10"), environment="mainnet"))
    with pytest.raises(DataError, match="RPC chain"):
        reader.pin_block(25908472)
    reader, requests = rpc_factory()
    block = reader.pin_block(25908472)
    raw = block.model_dump()
    raw["chain"]["environment"] = "fork/test"
    with pytest.raises(DataError, match="chain/environment"):
        read_pool(reader, price_request.source, Block.model_validate(raw))
    assert len(requests) == 2


def test_unverified_block_metadata_is_rejected(rpc_factory, price_request):
    reader, _ = rpc_factory()
    raw = reader.pin_block(25908472).model_dump()
    raw["timestamp"] += 1
    with pytest.raises(DataError, match="pin this block"):
        read_pool(reader, price_request.source, Block.model_validate(raw))


def test_two_safes_can_share_a_submitter_without_sharing_fund_identity(price_request):
    chain = price_request.source.chain
    submitter = Account(chain=chain, address="0x" + "11" * 20)
    profiles = [
        SafeRoles(
            kind="safe_roles",
            fund=Account(chain=chain, address="0x" + byte * 20),
            operator=submitter,
            roles_module="0x" + "44" * 20,
            role_key="0x" + "55" * 32,
        )
        for byte in ("22", "33")
    ]
    assert profiles[0].fund != profiles[1].fund
    assert profiles[0].submitter == profiles[1].submitter


@pytest.mark.parametrize("value", ["0x" + " 1" * 20, "0x" + "gg" * 20, "0x123", None])
def test_invalid_addresses_are_not_normalized_into_valid_ones(value):
    with pytest.raises(ValueError):
        address(value)


@pytest.mark.parametrize("failure", ["transport", "rpc", "http", "malformed"])
def test_errors_never_echo_rpc_credentials(rpc_factory, failure):
    def override(request):
        if failure == "transport":
            raise httpx.ConnectError("https://rpc.invalid/secret-key")
        if failure == "rpc":
            return {"error": {"code": -32000, "message": "secret-key"}}
        if failure == "http":
            return httpx.Response(403, text="secret-key")
        return httpx.Response(200, text="secret-key")

    reader, _ = rpc_factory(override)
    with pytest.raises(DataError) as exc:
        reader.pin_block(25908472)
    assert "secret-key" not in str(exc.value)
    assert "rpc.invalid" not in str(exc.value)


@pytest.mark.parametrize("product", ["pool-price", "lending-reserve"])
def test_ax_cli_rejects_invalid_sources_before_reading_credentials(
    tmp_path, monkeypatch, capsys, reserve_source, product
):
    from almanak.cli import main

    config = tmp_path / "source.json"
    raw = (
        {"base": "WETH", "quote": "USDC"}
        if product == "pool-price"
        else {**reserve_source.model_dump(mode="json"), "reserve_id": 2**256}
    )
    config.write_text(json.dumps(raw))
    monkeypatch.setattr(
        "sys.argv",
        [
            "almanak",
            "ax",
            product,
            "--connector",
            "uniswap_v4" if product == "pool-price" else "aave_v4",
            "--config",
            str(config),
            "--block",
            "finalized",
            "--env-file",
            str(tmp_path / "does-not-exist.env"),
        ],
    )
    assert main() == 1
    assert '"error": "invalid_config"' in capsys.readouterr().err
