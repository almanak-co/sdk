"""SparkLend reserve resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from referencing import Registry, Resource

from almanak.caip import AssetType, ChainId
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.sparklend.author_inputs import AllocationInput
from almanak.connectors.sparklend.models import Allocation
from almanak.connectors.sparklend.networks import POOLS, pool_for
from almanak.connectors.sparklend.resolution import (
    MarketSelection,
    ReserveAssertions,
    ResolvedReserve,
    resolve_market,
)
from almanak.connectors.sparklend.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.lending_captures import current_allocation

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "sparklend-ethereum-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/sparklend.json.gz"
REFERENCE = "sparklend_ethereum_lending"
CHAIN = ChainId("eip155:1")
POOL = "0xc13e21b648a5ee794902342038ff3adab66be987"
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
A_TOKEN = AssetType("eip155:1/erc20:0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db")
DEBT_TOKEN = AssetType("eip155:1/erc20:0x2e7576042566f8d6990e07a1b61ad1efd86ae70d")


@pytest.fixture(scope="module")
def recorded():
    raw = json.loads(gzip.decompress(READS.read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return block, rows


@pytest.fixture
def replay(recorded):
    block, rows = recorded
    readers = []

    def create(evidence=None, chain=None):
        reader = RetainedResolutionReader(
            chain or block.chain, rows if evidence is None else evidence
        )
        readers.append(reader)
        return reader

    yield create
    for reader in readers:
        reader.__exit__()


def selected(assertions=None, chain=None, asset=WETH, pool=None):
    return MarketSelection(
        connector="sparklend", chain=chain or CHAIN, asset=asset, pool=pool, assertions=assertions
    )


def legacy_allocations() -> dict[str, Allocation]:
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    value = raw["references"][REFERENCE]["files"]["config.yaml"]["value"]
    # The frozen value predates scope-only authority; its limits read as capital and borrow.
    return {
        entry["name"]: Allocation.model_validate_json(json.dumps(current_allocation(entry)))
        for entry in value["allocations"]
    }


def public_allocation(position: str, **overrides) -> AllocationInput:
    raw = yaml.safe_load((ROOT / "strategies" / REFERENCE / "config.yaml").read_text())
    values = dict(raw["strategy"]["allocations"][position])
    return AllocationInput.model_validate_json(json.dumps({**values, **overrides}))


def test_recorded_reads_resolve_the_legacy_ethereum_market(recorded, replay):
    block, _ = recorded
    legacy = legacy_allocations()["collateral"]
    result = resolve_market(replay(), selected(), block)
    assert result.reserve == legacy.reserve
    assert (result.reserve.pool, result.reserve.asset) == (POOL, WETH)
    assert result.asset.decimals == 18
    assert result.tokens == (A_TOKEN, DEBT_TOKEN)
    assert result.assets == (result.asset,)
    assert result.block == block
    assert [identity.address for identity in result.identities] == [
        POOL,
        A_TOKEN.reference,
        DEBT_TOKEN.reference,
    ]
    assert ResolvedReserve.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool",
        "asset",
        "decimals",
        "a_token",
        "variable_debt_token",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert (summary["pool"], summary["asset"], summary["decimals"]) == (POOL, str(WETH), 18)
    assert (summary["a_token"], summary["variable_debt_token"]) == (str(A_TOKEN), str(DEBT_TOKEN))
    assert summary["block"] == block.number


def test_unknown_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in POOLS
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    selection = selected(chain=foreign, asset=AssetType(f"{foreign}/erc20:{WETH.reference}"))
    with pytest.raises(DataError, match="no reviewed Pool on eip155:100") as error:
        resolve_market(reader, selection, block.model_copy(update={"chain": chain}))
    assert error.value.kind == "unsupported_chain"


def test_foreign_anchor_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    reader = replay(evidence=())
    foreign = block.model_copy(
        update={"chain": block.chain.model_copy(update={"environment": "x"})}
    )
    with pytest.raises(DataError, match="selected chain and anchor") as error:
        resolve_market(reader, selected(), foreign)
    assert error.value.kind == "chain_mismatch"


def test_unlisted_chain_has_no_reviewed_pool():
    with pytest.raises(DataError, match="no reviewed Pool on eip155:100") as error:
        pool_for(ChainId("eip155:100"))
    assert error.value.kind == "unsupported_chain"


def test_explicit_pool_outside_the_table_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    reader = replay(evidence=())
    with pytest.raises(DataError, match="outside the reviewed table") as error:
        resolve_market(reader, selected(pool="0x" + "12" * 20), block)
    assert error.value.kind == "deployment_mismatch"


def test_explicit_reviewed_pool_resolves_the_same_reserve(recorded, replay):
    block, _ = recorded
    explicit = resolve_market(replay(), selected(pool=POOL), block)
    assert explicit.reserve == resolve_market(replay(), selected(), block).reserve
    assert explicit.selection.pool == POOL


@pytest.mark.parametrize("field", ["a_token", "variable_debt_token", "decimals"])
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {"a_token": A_TOKEN, "variable_debt_token": DEBT_TOKEN, "decimals": 18}
    result = resolve_market(replay(), selected(ReserveAssertions(**expected)), block)
    assert result.reserve.asset == WETH
    bad = {
        "a_token": AssetType(f"{CHAIN}/erc20:0x" + "12" * 20),
        "variable_debt_token": AssetType(f"{CHAIN}/erc20:0x" + "34" * 20),
        "decimals": 6,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(ReserveAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("field", ["pool", "chain", "asset", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "pool": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "asset": AssetType("eip155:1/slip44:60"),
        "assertions": ReserveAssertions(a_token=AssetType("eip155:8453/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


@pytest.mark.parametrize("field", ["selection", "block", "asset", "a_token", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"asset": USDC}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset": result.asset.model_copy(update={"identifier": USDC}),
        "a_token": DEBT_TOKEN,
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedReserve.model_validate({**result.model_dump(), field: bad[field]})


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    kept = tuple(
        row
        for row in rows
        if not (row.method == "eth_getCode" and row.params[0] == DEBT_TOKEN.reference)
    )
    assert len(kept) < len(rows)
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


@pytest.mark.parametrize(("position", "asset"), [("collateral", WETH), ("loan", USDC)])
def test_public_positions_resolve_to_the_legacy_allocations(recorded, replay, position, asset):
    block, _ = recorded
    market = resolve_market(replay(), selected(asset=asset), block)
    resolved = public_allocation(position).resolve(position, market)
    assert resolved == legacy_allocations()[position]


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


def frozen_references():
    return sorted(json.loads(gzip.decompress(LEGACY.read_bytes()))["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_pool(name, entry):
    """The table holds the Pool every reference proved against before conversion."""
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    for market in document["markets"].values():
        assert "pool" not in market and ChainId(market["chain"]) in POOLS
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        assert "connector does not support" in record["message"]
        return
    for allocation in record["value"]["allocations"]:
        reserve = allocation["reserve"]
        assert reserve["pool"] == pool_for(ChainId(reserve["chain"]["chain_id"]))


def test_every_supported_chain_has_exactly_one_reviewed_pool():
    assert set(POOLS) == {entry.chain for entry in candidate_support()}
    assert POOLS[CHAIN] == POOL


def schema_validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in (ROOT / "docs/schemas").rglob("*.json")
    )
    return Draft202012Validator(
        {"$ref": (ROOT / "docs/schemas" / name).as_uri()}, registry=registry
    )


def test_editor_schemas_accept_the_public_selection_and_positions():
    market = schema_validator("markets/sparklend.schema.json")
    market.validate(selected().model_dump(mode="json"))
    assertions = ReserveAssertions(a_token=A_TOKEN, variable_debt_token=DEBT_TOKEN, decimals=18)
    market.validate(selected(assertions, pool=POOL).model_dump(mode="json"))
    position = schema_validator("positions/sparklend.schema.json")
    for name in ("collateral", "loan"):
        position.validate(json.loads(public_allocation(name).model_dump_json()))


@pytest.mark.parametrize("mutation", ["assetless", "unknown_connector", "float_pool"])
def test_market_schema_refuses_author_contract_errors(mutation):
    raw = selected().model_dump(mode="json")
    if mutation == "assetless":
        del raw["asset"]
    elif mutation == "unknown_connector":
        raw["connector"] = "spark_v2"
    else:
        raw["pool"] = 1
    assert not schema_validator("markets/sparklend.schema.json").is_valid(raw)


@pytest.mark.parametrize("mutation", ["derived_native_asset", "float_quantity", "marketless"])
def test_position_schema_refuses_author_contract_errors(mutation):
    raw = json.loads(public_allocation("collateral").model_dump_json())
    if mutation == "derived_native_asset":
        raw["native_asset"] = "eip155:1/slip44:60"
    elif mutation == "float_quantity":
        raw["capital"] = 0.01
    else:
        del raw["market"]
    assert not schema_validator("positions/sparklend.schema.json").is_valid(raw)


@pytest.mark.parametrize(
    ("overrides", "refused"),
    [({}, False), ({"collateral": False, "borrow": True}, False), ({"collateral": False}, True)],
)
def test_supply_only_allocation_needs_collateral_on_a_nonzero_ltv_reserve(
    recorded, replay, overrides, refused
):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    inputs = public_allocation("collateral", **overrides)
    if refused:
        with pytest.raises(ConfigurationError, match="set collateral: true") as error:
            inputs.check_resolved(replay(), market)
        assert error.value.field == "collateral"
    else:
        inputs.check_resolved(replay(), market)
