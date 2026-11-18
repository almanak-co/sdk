"""Aerodrome pool resolution replays retained reads, without provider access."""

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
from almanak.connectors.aerodrome.author_inputs import AllocationInput
from almanak.connectors.aerodrome.models import Allocation
from almanak.connectors.aerodrome.networks import DEPLOYMENTS, Deployment, deployment_for
from almanak.connectors.aerodrome.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.connectors.aerodrome.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
SCHEMAS = ROOT / "docs/schemas"
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "aerodrome-base-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/aerodrome.json.gz"
STABLE = "0x27a8afa3bd49406e48a074350fb7b2020c43b2bd"
VOLATILE = "0xcdac0d6c6c59727a65f871236188350531885c43"
WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
USDBC = AssetType("eip155:8453/erc20:0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca")
REFERENCES = {
    "aerodrome_base_stable_lp": STABLE,
    "aerodrome_base_stable_trading": STABLE,
    "aerodrome_base_volatile_lp": VOLATILE,
    "aerodrome_base_volatile_trading": VOLATILE,
}


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


def selected(assertions=None, chain=None, pool=STABLE):
    return MarketSelection(
        connector="aerodrome",
        chain=chain or ChainId("eip155:8453"),
        pool=pool,
        assertions=assertions,
    )


def legacy_value(reference):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    return raw["references"][reference]["files"]["config.yaml"]["value"]


def legacy_allocation(reference):
    return Allocation.model_validate_json(
        json.dumps(without_retired_fields(legacy_value(reference)["allocations"][0]))
    )


def authored_position(reference):
    """The shipped harness allocation without the native asset the resolver now supplies."""
    document = yaml.safe_load((ROOT / "strategies" / reference / "config.yaml").read_text())
    (authored,) = document["strategy"]["allocations"].values()
    return {name: value for name, value in authored.items() if name != "native_asset"}


def precision(result):
    return [asset.decimals for asset in result.currencies]


def reviewed_roles(result):
    """The factory, pool and router of the six identities the verification order names."""
    addresses = [identity.address for identity in result.identities]
    assert len(set(addresses)) == 6
    return [addresses[0], addresses[2], addresses[4]]


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


@pytest.mark.parametrize(
    ("reference", "currencies", "decimals", "stable"),
    [
        ("aerodrome_base_stable_lp", (USDC, USDBC), [6, 6], True),
        ("aerodrome_base_volatile_lp", (WETH, USDC), [18, 6], False),
    ],
)
def test_recorded_reads_resolve_both_base_pools(
    recorded, replay, reference, currencies, decimals, stable
):
    block, _ = recorded
    legacy = legacy_allocation(reference)
    result = resolve_market(replay(), selected(pool=REFERENCES[reference]), block)
    assert (result.pool, result.contracts, result.block) == (legacy.pool, legacy.contracts, block)
    assert result.pool.stable is stable
    assert (result.pool.currency0, result.pool.currency1) == currencies
    assert precision(result) == decimals
    assert result.assets == (result.asset0, result.asset1, result.share)
    assert str(result.share.identifier) == f"eip155:8453/erc20:{REFERENCES[reference]}"
    assert result.share.decimals == 18
    assert reviewed_roles(result) == [
        legacy.pool.factory,
        legacy.pool.pool,
        legacy.contracts.router,
    ]
    assert ResolvedPool.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_both_pools_resolve_at_one_anchor_with_distinct_invariants(recorded, replay):
    block, _ = recorded
    reader = replay()
    stable = resolve_market(reader, selected(pool=STABLE), block)
    volatile = resolve_market(reader, selected(pool=VOLATILE), block)
    assert (stable.pool.stable, volatile.pool.stable) == (True, False)
    assert stable.pool.factory == volatile.pool.factory
    assert stable.digest != volatile.digest


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool",
        "currencies",
        "stable",
        "decimals",
        "share_decimals",
        "block",
    }
    assert summary["share_decimals"] == 18
    assert summary["selection"] == selected().model_dump(mode="json")
    assert summary["currencies"] == [str(USDC), str(USDBC)]
    assert (summary["stable"], summary["decimals"]) == (True, [6, 6])
    assert (summary["pool"], summary["block"]) == (STABLE, block.number)


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


def test_pool_outside_the_reviewed_factory_refuses(recorded, replay, monkeypatch):
    block, _ = recorded
    base = ChainId("eip155:8453")
    foreign = DEPLOYMENTS[base].model_copy(update={"factory": "0x" + "12" * 20})
    monkeypatch.setitem(DEPLOYMENTS, base, foreign)
    with pytest.raises(DataError, match="outside its reviewed deployment") as error:
        resolve_market(replay(), selected(), block)
    assert error.value.kind == "deployment_changed"


@pytest.mark.parametrize("field", ["currency0", "currency1", "stable", "decimals0", "decimals1"])
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "currency0": USDC,
        "currency1": USDBC,
        "stable": True,
        "decimals0": 6,
        "decimals1": 6,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool == STABLE
    bad = {
        "currency0": AssetType("eip155:8453/erc20:0x" + "12" * 20),
        "currency1": AssetType("eip155:8453/erc20:0x" + "34" * 20),
        "stable": False,
        "decimals0": 18,
        "decimals1": 18,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(PoolAssertions(**{field: bad[field]})), block)


def test_volatile_invariant_assertion_refuses_the_stable_pool(recorded, replay):
    block, _ = recorded
    volatile = resolve_market(
        replay(), selected(PoolAssertions(stable=False), pool=VOLATILE), block
    )
    assert volatile.pool.stable is False
    with pytest.raises(ValueError, match="assertion stable"):
        resolve_market(replay(), selected(PoolAssertions(stable=True), pool=VOLATILE), block)


@pytest.mark.parametrize("field", ["pool", "chain", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "pool": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "assertions": PoolAssertions(currency0=AssetType("eip155:10/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


@pytest.mark.parametrize("field", ["selection", "block", "asset0", "share", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"pool": "0x" + "12" * 20}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset0": result.asset0.model_copy(update={"identifier": WETH}),
        "share": result.share.model_copy(update={"identifier": USDC}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedPool.model_validate({**result.model_dump(), field: bad[field]})


def test_noncanonical_anchor_refuses_before_the_result_is_built(recorded, replay):
    block, rows = recorded
    reanchored = tuple(
        row.model_copy(update={"result": {**row.result, "hash": "0x" + "ab" * 32}})
        if row.method == "eth_getBlockByNumber"
        else row
        for row in rows
    )
    with pytest.raises(DataError, match="no longer canonical") as error:
        resolve_market(replay(evidence=reanchored), selected(), block)
    assert error.value.kind == "block_mismatch"


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    router = DEPLOYMENTS[ChainId("eip155:8453")].router
    kept = tuple(r for r in rows if not (r.method == "eth_getCode" and r.params[0] == router))
    assert len(kept) < len(rows)
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


@pytest.mark.parametrize("reference", sorted(REFERENCES))
def test_public_positions_resolve_to_the_legacy_allocations(recorded, replay, reference):
    block, _ = recorded
    market = resolve_market(replay(), selected(pool=REFERENCES[reference]), block)
    metadata = legacy_value(reference)["asset_metadata"]
    assert [asset.decimals for asset in market.assets] == [
        metadata[str(asset.identifier)]["decimals"] for asset in market.assets
    ]
    authored = AllocationInput.model_validate_json(json.dumps(authored_position(reference)))
    assert authored.resolve("position", market) == legacy_allocation(reference)


def test_position_amounts_must_name_the_resolved_pool_currencies(recorded, replay):
    field = "capital"
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    foreign = {"asset": "eip155:8453/erc20:0x" + "12" * 20, "quantity": {"base_units": 1}}
    authored = {**authored_position("aerodrome_base_stable_lp"), field: foreign}
    with pytest.raises(ConfigurationError) as error:
        AllocationInput.model_validate_json(json.dumps(authored)).resolve("position", market)
    assert error.value.field == f"{field}.asset"


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


@pytest.mark.parametrize("reference", sorted(REFERENCES))
def test_shipped_references_name_the_reviewed_deployment(reference):
    """The table holds the deployment every reference proved against before conversion."""
    document = yaml.safe_load((ROOT / "strategies" / reference / "config.yaml").read_text())
    markets = {market["pool"] for market in document["markets"].values()}
    allocation = legacy_value(reference)["allocations"][0]
    pool, contracts = allocation["pool"], allocation["contracts"]
    deployment = deployment_for(ChainId(pool["chain"]["chain_id"]))
    assert pool["pool"] in markets and pool["pool"] == REFERENCES[reference]
    assert pool["factory"] == deployment.factory
    assert contracts == {"router": deployment.router}


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert set(DEPLOYMENTS) == {entry.chain for entry in candidate_support()}
    assert all(isinstance(value, Deployment) for value in DEPLOYMENTS.values())


def editor_market(**overrides):
    return {"connector": "aerodrome", "chain": "eip155:8453", "pool": STABLE, **overrides}


@pytest.mark.parametrize("reference", sorted(REFERENCES))
def test_editor_schemas_accept_the_shipped_selections_and_positions(reference):
    market = editor_market(pool=REFERENCES[reference], assertions={"stable": True, "decimals0": 6})
    validator("markets/aerodrome.schema.json").validate(market)
    validator("markets/aerodrome.schema.json").validate(editor_market(assertions=None))
    validator("positions/aerodrome.schema.json").validate(authored_position(reference))


@pytest.mark.parametrize(
    "mutation", ["unknown_connector", "poolless", "author_invariant", "foreign_assertion"]
)
def test_editor_market_schema_refuses_author_contract_errors(mutation):
    raw = editor_market()
    if mutation == "unknown_connector":
        raw["connector"] = "aerodrome_slipstream"
    elif mutation == "poolless":
        del raw["pool"]
    elif mutation == "author_invariant":
        raw["stable"] = True
    else:
        raw["assertions"] = {"stable": "true"}
    assert not validator("markets/aerodrome.schema.json").is_valid(raw)


@pytest.mark.parametrize(
    "mutation", ["foreign_key", "liquidity", "spent_amount", "float_quantity", "unbounded_swap"]
)
def test_editor_position_schema_refuses_author_contract_errors(mutation):
    raw = authored_position("aerodrome_base_stable_lp")
    if mutation == "foreign_key":
        raw["native_asset"] = "eip155:8453/slip44:60"
    elif mutation == "liquidity":
        del raw["liquidity_enabled"]
    elif mutation == "spent_amount":
        raw["capital"] = {"base_units": 100000000}
    elif mutation == "float_quantity":
        raw["capital"]["quantity"] = 100.5
    else:
        raw["entry_swap_bps"] = 10001
    assert not validator("positions/aerodrome.schema.json").is_valid(raw)
