"""Slipstream pool resolution replays retained reads, without provider access."""

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
from almanak.connectors.aerodrome_slipstream.author_inputs import AllocationInput
from almanak.connectors.aerodrome_slipstream.models import Allocation
from almanak.connectors.aerodrome_slipstream.networks import (
    DEPLOYMENTS,
    Deployment,
    deployment_for,
)
from almanak.connectors.aerodrome_slipstream.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.connectors.aerodrome_slipstream.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
SCHEMAS = ROOT / "docs/schemas"
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "aerodrome-slipstream-base-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/aerodrome_slipstream.json.gz"
BASE = ChainId("eip155:8453")
POOL = "0x3fe04a59ebd38cf06080a6f60a98d124eb59392a"
WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")


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
        connector="aerodrome_slipstream",
        chain=chain or BASE,
        pool=pool,
        assertions=assertions,
    )


def legacy_record(reference):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    return raw["references"][reference]["files"]["config.yaml"]["value"]


def legacy_allocation(reference):
    return Allocation.model_validate_json(
        json.dumps(without_retired_fields(legacy_record(reference)["allocations"][0]))
    )


def periphery_of(source):
    """Deployment tables and retained Contracts name the same three periphery roles."""
    return {
        "router": source.router,
        "position_manager": source.position_manager,
        "quoter": source.quoter,
    }


def test_recorded_reads_resolve_the_legacy_base_market(recorded, replay):
    block, _ = recorded
    legacy = legacy_allocation("aerodrome_slipstream_base_trading")
    result = resolve_market(replay(), selected(), block)
    assert (result.pool, result.contracts, result.block) == (legacy.pool, legacy.contracts, block)
    assert (result.pool.tick_spacing, result.pool.currency0, result.pool.currency1) == (
        50,
        WETH,
        USDC,
    )
    assert [asset.decimals for asset in result.assets] == [18, 6]
    assert result.assets == (result.asset0, result.asset1)


def test_resolved_identities_name_the_verified_pool_and_periphery(recorded, replay):
    block, _ = recorded
    legacy = legacy_allocation("aerodrome_slipstream_base_trading")
    addresses = [
        identity.address for identity in resolve_market(replay(), selected(), block).identities
    ]
    assert (addresses[0], addresses[2]) == (legacy.pool.factory, legacy.pool.pool)
    assert addresses[-3:] == list(periphery_of(legacy.contracts).values())
    assert len(set(addresses)) == len(addresses)


def test_retained_facts_round_trip_with_a_stable_digest(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    assert ResolvedPool.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_resolved_decimals_match_the_frozen_reference_metadata(recorded, replay):
    block, _ = recorded
    metadata = legacy_record("aerodrome_slipstream_base_trading")["asset_metadata"]
    result = resolve_market(replay(), selected(), block)
    assert [asset.decimals for asset in result.assets] == [
        metadata[str(asset.identifier)]["decimals"] for asset in result.assets
    ]


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool",
        "currencies",
        "tick_spacing",
        "decimals",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert summary["currencies"] == [str(WETH), str(USDC)]
    assert (summary["pool"], summary["block"], summary["tick_spacing"], summary["decimals"]) == (
        POOL,
        block.number,
        50,
        [18, 6],
    )


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
    named = DEPLOYMENTS[BASE].factory
    foreign = DEPLOYMENTS[BASE].model_copy(update={"factory": "0x" + "12" * 20})
    monkeypatch.setitem(DEPLOYMENTS, BASE, foreign)
    with pytest.raises(DataError, match="outside its reviewed deployment") as error:
        resolve_market(replay(), selected(), block)
    assert error.value.kind == "deployment_changed"
    assert f"names factory {named}" in str(error.value)
    assert str(error.value).endswith(foreign.factory)


@pytest.mark.parametrize(
    "field", ["currency0", "currency1", "tick_spacing", "decimals0", "decimals1"]
)
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "currency0": WETH,
        "currency1": USDC,
        "tick_spacing": 50,
        "decimals0": 18,
        "decimals1": 6,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool == POOL
    bad = {
        "currency0": AssetType("eip155:8453/erc20:0x" + "12" * 20),
        "currency1": AssetType("eip155:8453/erc20:0x" + "34" * 20),
        "tick_spacing": 100,
        "decimals0": 6,
        "decimals1": 18,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(PoolAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("field", ["pool", "chain", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "pool": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "assertions": PoolAssertions(currency0=AssetType("eip155:56/erc20:0x" + "12" * 20)),
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
        "asset0": result.asset0.model_copy(update={"identifier": USDC}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedPool.model_validate({**result.model_dump(), field: bad[field]})


def test_repeated_identity_addresses_refuse(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    repeated = (result.identities[0], *result.identities[1:-1], result.identities[0])
    with pytest.raises(ValidationError, match="repeat a contract address"):
        ResolvedPool.model_validate({**result.model_dump(), "identities": repeated})


def without_quoter_code(rows):
    quoter = DEPLOYMENTS[BASE].quoter
    return tuple(r for r in rows if r.method != "eth_getCode" or r.params[0] != quoter)


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    kept = without_quoter_code(rows)
    assert len(kept) == len(rows) - 1
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def public_allocation(**overrides):
    return AllocationInput.model_validate_json(json.dumps({**public_document(), **overrides}))


def public_document():
    return {
        "market": "pool",
        "account": "fund",
        "connector": "aerodrome_slipstream",
        "capital": {"asset": str(WETH), "quantity": {"base_units": 100000000000000000}},
        "entry_swap_bps": 10000,
        "hold_seconds": "2h",
        "slippage_bps": 100,
        "deadline_seconds": 300,
        "minimum_native_balance": {"base_units": 0},
    }


@pytest.mark.parametrize(
    ("reference", "overrides"),
    [
        ("aerodrome_slipstream_base_trading", {}),
        (
            "aerodrome_slipstream_base_lp",
            {"entry_swap_bps": 5000, "range": {"lower": -198650, "upper": -197650}},
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
    foreign = {"asset": "eip155:8453/erc20:0x" + "12" * 20, "quantity": {"base_units": 1}}
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


def frozen_references():
    return sorted(json.loads(gzip.decompress(LEGACY.read_bytes()))["references"].items())


def reference_markets(name):
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    return {(ChainId(market["chain"]), market["pool"]) for market in document["markets"].values()}


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_deployment(name, entry):
    """The table holds the deployment every reference proved against before conversion."""
    markets = reference_markets(name)
    for allocation in entry["files"]["config.yaml"]["value"]["allocations"]:
        pool = allocation["pool"]
        deployment = deployment_for(ChainId(pool["chain"]["chain_id"]))
        assert (ChainId(pool["chain"]["chain_id"]), pool["pool"]) in markets
        assert pool["factory"] == deployment.factory
        assert allocation["contracts"] == periphery_of(deployment)


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert set(DEPLOYMENTS) == {entry.chain for entry in candidate_support()}
    assert all(isinstance(value, Deployment) for value in DEPLOYMENTS.values())


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


MARKET_SCHEMA = "markets/aerodrome_slipstream.schema.json"
POSITION_SCHEMA = "positions/aerodrome_slipstream.schema.json"


def market_document(**overrides):
    return {"connector": "aerodrome_slipstream", "chain": str(BASE), "pool": POOL, **overrides}


@pytest.mark.parametrize(
    "assertions",
    [
        None,
        {},
        {"currency0": str(WETH), "currency1": str(USDC)},
        {"tick_spacing": 50, "decimals0": 18, "decimals1": 6},
    ],
)
def test_market_schema_accepts_the_authored_selection(assertions):
    document = market_document(assertions=assertions)
    validator(MARKET_SCHEMA).validate(document)
    assert MarketSelection.model_validate_json(json.dumps(document)).pool == POOL


MARKET_MUTATIONS = {
    "connector": {"connector": "uniswap_v3"},
    "pool": {"pool": "0x1234"},
    "unknown": {"factory": POOL},
    # Slipstream swap fees are mutable observations, so no fee assertion exists.
    "fee": {"assertions": {"fee": 500}},
    "unknown_assertion": {"assertions": {"spacing": 50}},
    "assertion_type": {"assertions": {"tick_spacing": "50"}},
}


@pytest.mark.parametrize("mutation", sorted(MARKET_MUTATIONS))
def test_market_schema_refuses_foreign_and_unknown_inputs(mutation):
    assert not validator(MARKET_SCHEMA).is_valid(market_document(**MARKET_MUTATIONS[mutation]))


@pytest.mark.parametrize("field", ["connector", "chain", "pool"])
def test_market_schema_requires_every_selected_identity(field):
    document = market_document()
    del document[field]
    assert not validator(MARKET_SCHEMA).is_valid(document)


def position_document(**overrides):
    document = {key: value for key, value in public_document().items() if key != "connector"}
    return {**document, **overrides}


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"range": None},
        {"range": {"lower": -198650, "upper": -197650}},
        {"hold_seconds": 7200, "minimum_native_balance": "0.01"},
        {"capital": {"asset": str(WETH), "quantity": "0.1"}},
    ],
)
def test_position_schema_accepts_the_shipped_economics(overrides):
    document = position_document(**overrides)
    validator(POSITION_SCHEMA).validate(document)
    assert AllocationInput.model_validate_json(json.dumps(document)).account == "fund"


POSITION_MUTATIONS = {
    # Resolved facts stay out of author inputs; the pool names the native asset.
    "resolved_field": {"native_asset": "eip155:8453/slip44:60"},
    "float": {"capital": {"asset": str(WETH), "quantity": 0.1}},
    # Zero offers no entry quote, which a trading allocation cannot enter without.
    "swap_bps": {"entry_swap_bps": 0, "range": None},
    "half_range": {"range": {"lower": -198650}},
    "duration": {"hold_seconds": "2 hours"},
    "slippage": {"slippage_bps": 10000},
}


@pytest.mark.parametrize("mutation", sorted(POSITION_MUTATIONS))
def test_position_schema_refuses_unsupported_economics(mutation):
    document = position_document(**POSITION_MUTATIONS[mutation])
    assert not validator(POSITION_SCHEMA).is_valid(document)


@pytest.mark.parametrize("field", ["market", "account", "capital", "slippage_bps"])
def test_position_schema_requires_every_economic_choice(field):
    document = position_document()
    del document[field]
    assert not validator(POSITION_SCHEMA).is_valid(document)
