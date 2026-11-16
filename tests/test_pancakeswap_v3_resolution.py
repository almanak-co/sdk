"""PancakeSwap V3 pool resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from referencing import Registry, Resource

from almanak.caip import AssetType, ChainId
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.pancakeswap_v3.author_inputs import AllocationInput
from almanak.connectors.pancakeswap_v3.models import Allocation
from almanak.connectors.pancakeswap_v3.networks import DEPLOYMENTS, Deployment, deployment_for
from almanak.connectors.pancakeswap_v3.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.connectors.pancakeswap_v3.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
SCHEMAS = ROOT / "docs/schemas"
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "pancakeswap-v3-bnb-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/pancakeswap_v3.json.gz"
POOL = "0xc721deccd986d54b39e8c29428a1f06155c3671e"
# The pool deployer is read from the reviewed factory, never listed beside it.
DEPLOYER = "0x41ff9aa7e16b8b1a8a8dc4f0efacd93d02d071c9"
USDC = AssetType("eip155:56/erc20:0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d")
WBNB = AssetType("eip155:56/erc20:0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c")


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


def selected(assertions=None, chain=None, pool=POOL):
    return MarketSelection(
        connector="pancakeswap_v3",
        chain=chain or ChainId("eip155:56"),
        pool=pool,
        assertions=assertions,
    )


def snapshot():
    return json.loads(gzip.decompress(LEGACY.read_bytes()))


def legacy_reference(reference):
    return snapshot()["references"][reference]["files"]["config.yaml"]["value"]


def legacy_allocation(reference):
    value = legacy_reference(reference)
    return Allocation.model_validate_json(
        json.dumps(without_retired_fields(value["allocations"][0]))
    )


def test_recorded_reads_resolve_the_legacy_bnb_market(recorded, replay):
    block, _ = recorded
    legacy = legacy_reference("pancakeswap_v3_bnb_trading")
    allocation = Allocation.model_validate_json(
        json.dumps(without_retired_fields(legacy["allocations"][0]))
    )
    result = resolve_market(replay(), selected(), block)
    assert result.pool == allocation.pool
    assert result.contracts == allocation.contracts
    assert (result.pool.fee, result.pool.tick_spacing) == (2500, 50)
    assert (result.pool.currency0, result.pool.currency1) == (USDC, WBNB)
    assert [asset.decimals for asset in result.assets] == [
        legacy["asset_metadata"][str(asset.identifier)]["decimals"] for asset in result.assets
    ]
    assert result.assets == (result.asset0, result.asset1)
    assert result.block == block
    assert [identity.address for identity in result.identities] == [
        allocation.pool.factory,
        DEPLOYER,
        allocation.pool.pool,
        allocation.contracts.router,
        allocation.contracts.position_manager,
        allocation.contracts.quoter,
    ]
    assert ResolvedPool.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool",
        "currencies",
        "fee",
        "tick_spacing",
        "decimals",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert summary["currencies"] == [str(USDC), str(WBNB)]
    assert (summary["fee"], summary["tick_spacing"], summary["decimals"]) == (2500, 50, [18, 18])
    assert (summary["pool"], summary["block"]) == (POOL, block.number)


def test_unknown_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in {entry.chain for entry in candidate_support()}
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    with pytest.raises(DataError, match="no declared support") as error:
        resolve_market(reader, selected(chain=foreign), block.model_copy(update={"chain": chain}))
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


def test_unlisted_deployment_chain_refuses_with_a_remedy():
    with pytest.raises(DataError, match="no reviewed deployment on eip155:100") as error:
        deployment_for(ChainId("eip155:100"))
    assert error.value.kind == "unsupported_chain"


def test_pool_naming_another_factory_refuses(recorded, replay, monkeypatch):
    block, _ = recorded
    named = DEPLOYMENTS[ChainId("eip155:56")].factory
    foreign = DEPLOYMENTS[ChainId("eip155:56")].model_copy(update={"factory": "0x" + "12" * 20})
    monkeypatch.setitem(DEPLOYMENTS, ChainId("eip155:56"), foreign)
    with pytest.raises(DataError, match="outside its reviewed deployment") as error:
        resolve_market(replay(), selected(), block)
    assert error.value.kind == "deployment_changed"
    assert f"names factory {named}" in str(error.value)
    assert str(error.value).endswith(foreign.factory)


@pytest.mark.parametrize(
    "field", ["currency0", "currency1", "fee", "tick_spacing", "decimals0", "decimals1"]
)
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "currency0": USDC,
        "currency1": WBNB,
        "fee": 2500,
        "tick_spacing": 50,
        "decimals0": 18,
        "decimals1": 18,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool == POOL
    bad = {
        "currency0": AssetType("eip155:56/erc20:0x" + "12" * 20),
        "currency1": AssetType("eip155:56/erc20:0x" + "34" * 20),
        "fee": 500,
        "tick_spacing": 10,
        "decimals0": 6,
        "decimals1": 8,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(PoolAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("field", ["pool", "chain", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "pool": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "assertions": PoolAssertions(currency0=AssetType("eip155:1/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


@pytest.mark.parametrize("field", ["selection", "block", "asset0", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"pool": "0x" + "12" * 20}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset0": result.asset0.model_copy(update={"identifier": WBNB}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedPool.model_validate({**result.model_dump(), field: bad[field]})


def test_retained_result_refuses_a_dropped_read_role(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    # Dropping the pool deployer leaves the authored roles intact but shortens the set.
    without = (*result.identities[:1], *result.identities[2:])
    with pytest.raises(ValidationError, match="differ from the verified roles"):
        ResolvedPool.model_validate({**result.model_dump(), "identities": without})


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    quoter = DEPLOYMENTS[ChainId("eip155:56")].quoter
    kept = tuple(r for r in rows if not (r.method == "eth_getCode" and r.params[0] == quoter))
    assert len(kept) == len(rows) - 1
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def public_allocation(**overrides):
    raw = {
        "market": "pool",
        "account": "fund",
        "connector": "pancakeswap_v3",
        "capital": {"asset": str(WBNB), "quantity": {"base_units": 100000000000000000}},
        "entry_swap_bps": 10000,
        "hold_seconds": "2h",
        "slippage_bps": 100,
        "deadline_seconds": 300,
        "minimum_native_balance": {"base_units": 0},
    }
    return AllocationInput.model_validate_json(json.dumps({**raw, **overrides}))


@pytest.mark.parametrize(
    ("reference", "overrides"),
    [
        ("pancakeswap_v3_bnb_trading", {}),
        (
            "pancakeswap_v3_bnb_lp",
            {"entry_swap_bps": 5000, "range": {"lower": -66750, "upper": -65750}},
        ),
    ],
)
def test_public_positions_resolve_to_the_legacy_allocations(recorded, replay, reference, overrides):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    assert public_allocation(**overrides).resolve("position", market) == legacy_allocation(
        reference
    )


def test_position_amounts_must_name_the_resolved_pool_currencies(recorded, replay):
    field = "capital"
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    foreign = {"asset": "eip155:56/erc20:0x" + "12" * 20, "quantity": {"base_units": 1}}
    with pytest.raises(ConfigurationError) as error:
        public_allocation(**{field: foreign}).resolve("position", market)
    assert error.value.field == f"{field}.asset"


@pytest.mark.parametrize("field", ["maximum0", "maximum1"])
def test_retired_spending_maxima_are_unknown_fields(field):
    # Grants are scope-only (blueprint 27): no per-pool spending maximum remains.
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        public_allocation(**{field: {"asset": "ASSET", "quantity": {"base_units": 1}}})


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


MARKET_SCHEMA = "markets/pancakeswap_v3.schema.json"
POSITION_SCHEMA = "positions/pancakeswap_v3.schema.json"
MARKET = {
    "connector": "pancakeswap_v3",
    "chain": "eip155:56",
    "pool": POOL,
    "assertions": {"fee": 2500, "tick_spacing": 50, "decimals0": 18, "decimals1": 18},
}


def authored_position():
    return json.loads(public_allocation().model_dump_json())


def test_editor_schemas_accept_the_authored_bnb_market_and_positions():
    validator(MARKET_SCHEMA).validate(MARKET)
    validator(MARKET_SCHEMA).validate({k: v for k, v in MARKET.items() if k != "assertions"})
    position = authored_position()
    validator(POSITION_SCHEMA).validate(position)
    lp = {**position, "entry_swap_bps": 5000, "range": {"lower": -66750, "upper": -65750}}
    validator(POSITION_SCHEMA).validate(lp)


@pytest.mark.parametrize(
    "mutation", ["foreign_connector", "poolless", "chainless", "unknown_assertion", "float_fee"]
)
def test_market_schema_refuses_foreign_selection_keys(mutation):
    raw = dict(MARKET)
    if mutation == "foreign_connector":
        raw["connector"] = "uniswap_v3"
    elif mutation == "poolless":
        del raw["pool"]
    elif mutation == "chainless":
        del raw["chain"]
    elif mutation == "unknown_assertion":
        raw["assertions"] = {**MARKET["assertions"], "hooks": "0x" + "00" * 20}
    else:
        raw["assertions"] = {**MARKET["assertions"], "fee": 2500.5}
    assert not validator(MARKET_SCHEMA).is_valid(raw)


@pytest.mark.parametrize(
    "mutation", ["resolved_key", "spent_amount", "float_tick", "marketless", "float_quantity"]
)
def test_position_schema_refuses_resolved_and_contract_errors(mutation):
    raw = authored_position()
    if mutation == "resolved_key":
        raw["native_asset"] = "eip155:56/slip44:714"
    elif mutation == "spent_amount":
        raw["capital"] = {"base_units": 100000000000000000}
    elif mutation == "float_tick":
        raw["range"] = {"lower": -66750.5, "upper": -65750}
    elif mutation == "marketless":
        del raw["market"]
    else:
        raw["capital"]["quantity"] = 1000.5
    assert not validator(POSITION_SCHEMA).is_valid(raw)


def frozen_references():
    return sorted(snapshot()["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_deployment(name, entry):
    """The table holds the deployments every reference proved against before conversion."""
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        assert "connector does not support" in record["message"]
        return
    for allocation in record["value"]["allocations"]:
        pool, contracts = allocation["pool"], allocation["contracts"]
        deployment = deployment_for(ChainId(pool["chain"]["chain_id"]))
        assert pool["factory"] == deployment.factory
        assert contracts == {
            "router": deployment.router,
            "position_manager": deployment.position_manager,
            "quoter": deployment.quoter,
        }


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert set(DEPLOYMENTS) == {entry.chain for entry in candidate_support()}
    assert all(isinstance(value, Deployment) for value in DEPLOYMENTS.values())
    proved = {
        ChainId(allocation["pool"]["chain"]["chain_id"])
        for _, entry in frozen_references()
        for allocation in entry["files"]["config.yaml"]["value"]["allocations"]
    }
    assert proved == set(DEPLOYMENTS)
