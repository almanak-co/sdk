"""Target admission depends on protocol semantics and state, without sample addresses."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import AssetType
from almanak.connectors.aave_v4 import deployments as aave
from almanak.connectors.aave_v4.reader import read_reserve
from almanak.connectors.uniswap_v4 import deployments as uniswap
from almanak.connectors.uniswap_v4.pool import pool_id, read_pool
from almanak.gateway.ethereum import DataError
from almanak.models import Asset, Block
from tests.network_support import production_config
from tests.uniswap_support import live_state


def verified_proxy():
    trace = json.loads(
        gzip.decompress(
            (Path(__file__).parent / "fixtures/safe-roles-2.1.0-authority.json.gz").read_bytes()
        )
    )
    code = bytes.fromhex(
        next(
            r["result"][2:]
            for r in trace["requests"]
            if r["method"] == "eth_getCode"
            and r["params"][0] == aave.SUPPORTED_REVISIONS[0].address
        )
    )
    for offset, opcode in enumerate(code[:-32]):
        if opcode != 0x7F:
            continue
        word = code[offset + 1 : offset + 33]
        if (
            code.count(word) == 1
            and "0x" + keccak(code.replace(word, bytes(32))).hex() == aave.PROXY_TEMPLATE
        ):
            return code, word
    raise AssertionError("retained deployed proxy has no verified immutable admin operand")


@pytest.mark.parametrize("mutation", [None, "implementation", "proxy", "admin"])
def test_aave_implementation_revision_admits_a_new_market_address(monkeypatch, mutation):
    config = production_config("aave_v4_avalanche_borrowing")
    source = config.allocations[0].reserve.model_copy(
        update={"spoke": "0x" + "34" * 20, "hub": "0x" + "56" * 20, "reserve_id": 123}
    )
    aave.validate_markets(SimpleNamespace(positions=(SimpleNamespace(reserve=source),)))
    code, old_admin = verified_proxy()
    new_admin = bytes.fromhex("00" * 12 + "78" * 20)
    code = code.replace(old_admin, new_admin)
    if mutation == "proxy":
        code += b"\x00"
    authority = SimpleNamespace(
        code=lambda _: code,
        storage=lambda *_: 0 if mutation == "admin" else int.from_bytes(new_admin, "big"),
    )
    authority.identity = lambda address: SimpleNamespace(
        implementation_hash="0x" + "00" * 32
        if mutation == "implementation"
        else next(iter(aave.IMPLEMENTATIONS["spoke" if address == source.spoke else "hub"]))
    )
    monkeypatch.setattr("almanak.gateway.roles_storage.AuthorityReader", lambda *_: authority)
    reader = SimpleNamespace(pin_block=lambda *_: None)
    if mutation is None:
        aave.verify_deployments(reader, source, SimpleNamespace(number=1))
    else:
        with pytest.raises(DataError, match="Aave"):
            aave.verify_deployments(reader, source, SimpleNamespace(number=1))


@pytest.mark.parametrize("mutation", [None, "hub", "asset", "decimals"])
def test_additional_reserve_selection_validates_observed_relationships(mutation):
    source = production_config("aave_v4_avalanche_borrowing").allocations[0].reserve
    source = source.model_copy(
        update={
            "reserve_id": 123,
            "asset": AssetType(f"{source.chain.chain_id}/erc20:0x" + "ab" * 20),
        }
    )
    block = Block(chain=source.chain, hash="0x" + "12" * 32, number=1, timestamp=1)
    replies = {
        "getReserve(uint256)": (
            "0x" + "cd" * 20 if mutation == "asset" else source.asset.reference,
            "0x" + "ef" * 20 if mutation == "hub" else source.hub,
            7,
            18,
            8000,
            0,
            1,
        ),
        "getReserveConfig(uint256)": (8000, False, False, True, False),
    }
    reader = SimpleNamespace(
        chain=source.chain,
        call=lambda _, signature, *__, **___: replies[signature],
        asset=lambda asset, _: Asset(
            chain=source.chain, identifier=asset, decimals=6 if mutation == "decimals" else 18
        ),
    )
    if mutation is None:
        observed = read_reserve(reader, source, block)
        assert observed.source.reserve_id == 123 and observed.asset.identifier == source.asset
    else:
        with pytest.raises(DataError, match="reserve"):
            read_reserve(reader, source, block)


def alternative_pool():
    config = live_state().config
    original = config.target(config.allocations[0].name)
    pool = original.pool.model_copy(
        update={
            "currency0": AssetType(f"{original.pool.chain.chain_id}/erc20:0x" + "11" * 20),
            "currency1": AssetType(f"{original.pool.chain.chain_id}/erc20:0x" + "22" * 20),
            "fee": 3000,
            "tick_spacing": 60,
        }
    )
    return original.model_copy(update={"pool": pool}), original


@pytest.mark.parametrize("mutation", [None, "manager", "uninitialized", "hook"])
def test_another_poolkey_is_admitted_without_pool_or_token_allowlists(mutation):
    target, original = alternative_pool()
    source = target.pool
    if mutation == "hook":
        with pytest.raises(ValidationError, match="hooked"):
            type(source).model_validate({**source.model_dump(), "hooks": "0x" + "12" * 20})
        return
    assert pool_id(source) != pool_id(original.pool)
    assert set(a.reference for a in target.assets).issubset(uniswap.target_addresses(target))
    block = Block(chain=source.chain, number=1, hash="0x" + "12" * 32, timestamp=1)
    replies = {
        "poolManager()": ("0x" + "34" * 20 if mutation == "manager" else source.pool_manager,),
        "getSlot0(bytes32)": (0 if mutation == "uninitialized" else 2**96, 0, 0, 3000),
        "getLiquidity(bytes32)": (10**18,),
    }
    reader = SimpleNamespace(
        chain=source.chain,
        call=lambda _, signature, *__, **___: replies[signature],
        asset=lambda asset, _: Asset(chain=source.chain, identifier=asset, decimals=18),
    )
    if mutation is None:
        observed = read_pool(reader, source, block)
        assert observed.source == source and observed.liquidity == 10**18
        uniswap.verify_identities(target, uniswap.pinned_identities())
    else:
        with pytest.raises(DataError):
            read_pool(reader, source, block)


def test_additional_pool_with_retained_real_chain_observation_is_configurable():
    from almanak.connectors.composition import author_configuration_from_json
    from almanak.connectors.uniswap_v4.domain import PoolObservation

    root = Path(__file__).parents[1]
    data = json.loads((root / "tests/fixtures/uniswap-additional-pool.json").read_text())
    observed = PoolObservation.model_validate_json(json.dumps(data["observation"]))
    raw = json.loads((root / "tests/fixtures/author/uniswap_v4_lp/config.json").read_text())
    original = raw["allocations"][0]["pool"]
    raw["allocations"][0].update(
        pool=data["observation"]["source"], range={"lower": 198240, "upper": 198360}
    )
    config = author_configuration_from_json(json.dumps(raw))
    assert config.allocations[0].pool == observed.source
    assert observed.source.fee != original["fee"] and observed.liquidity > 0
    assert pool_id(observed.source) == data["pool_id"]
