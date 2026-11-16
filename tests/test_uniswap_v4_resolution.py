"""Uniswap V4 pool resolution replays retained reads, without provider access."""

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
from almanak.connectors.support import candidate_uniswap_support
from almanak.connectors.uniswap_v4.author_inputs import AllocationInput
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.models import Allocation
from almanak.connectors.uniswap_v4.networks import roles_for
from almanak.connectors.uniswap_v4.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
SCHEMAS = ROOT / "docs/schemas"
READS = FIXTURES / "uniswap-v4-ethereum-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/uniswap_v4.json.gz"
CHAIN = ChainId("eip155:1")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
POOL_MANAGER = "0x000000000004444c5dc75cb358380d2e3de08a90"
STATE_VIEW = "0x7ffe42c4a5deea5b0fec41c94c136cf115597227"


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


def selected(assertions=None, chain=None, **overrides):
    raw = {
        "connector": "uniswap_v4",
        "chain": str(chain or CHAIN),
        "currency0": str(USDC),
        "currency1": str(WETH),
        "fee": 500,
        "tick_spacing": 10,
        "assertions": None if assertions is None else assertions.model_dump(mode="json"),
        **overrides,
    }
    return MarketSelection.model_validate_json(json.dumps(raw))


def legacy_allocation(reference):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    value = raw["references"][reference]["files"]["config.yaml"]["value"]
    allocation = without_retired_fields(value["allocations"][0])
    return Allocation.model_validate_json(json.dumps(allocation))


def test_recorded_reads_resolve_the_legacy_ethereum_market(recorded, replay):
    block, _ = recorded
    legacy = legacy_allocation("uniswap_v4_trading")
    result = resolve_market(replay(), selected(), block)
    assert result.pool == legacy.pool
    assert result.contracts == legacy.contracts
    assert (result.pool.fee, result.pool.tick_spacing) == (500, 10)
    assert (result.pool.currency0, result.pool.currency1) == (USDC, WETH)
    assert (result.pool.pool_manager, result.pool.state_view) == (POOL_MANAGER, STATE_VIEW)
    assert [asset.decimals for asset in result.assets] == [6, 18]
    assert result.assets == (result.asset0, result.asset1)
    assert result.block == block
    assert [identity.address for identity in result.identities] == sorted(
        roles_for(CHAIN).addresses
    )
    assert ResolvedPool.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool_id",
        "pool_manager",
        "currencies",
        "fee",
        "tick_spacing",
        "hooks",
        "decimals",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert summary["currencies"] == [str(USDC), str(WETH)]
    assert (summary["fee"], summary["tick_spacing"], summary["decimals"]) == (500, 10, [6, 18])
    assert (summary["pool_manager"], summary["block"]) == (POOL_MANAGER, block.number)
    assert summary["pool_id"].startswith("0x") and len(summary["pool_id"]) == 66


def test_unknown_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in {entry.chain for entry in candidate_uniswap_support()}
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    selection = selected(
        chain=foreign,
        currency0=f"{foreign}/erc20:0x" + "11" * 20,
        currency1=f"{foreign}/erc20:0x" + "22" * 20,
    )
    with pytest.raises(DataError, match="no declared support") as error:
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


def test_unlisted_deployment_chain_refuses_with_a_remedy():
    with pytest.raises(DataError, match="no reviewed deployment on eip155:100") as error:
        roles_for(ChainId("eip155:100"))
    assert error.value.kind == "unsupported_chain"


def test_hooked_pool_key_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    hooked = selected(hooks="0x" + "12" * 20)
    with pytest.raises(DataError, match="hooked pool behavior") as error:
        resolve_market(replay(evidence=()), hooked, block)
    assert error.value.kind == "unsupported_pool"


def test_another_fee_tier_resolves_as_its_own_pool(recorded, replay):
    """The 0.3% tier of the same pair exists; a changed fee never returns the authored pool."""
    block, _ = recorded
    authored = resolve_market(replay(), selected(), block)
    other = resolve_market(replay(), selected(fee=3000, tick_spacing=60), block)
    assert (other.pool.fee, other.pool.tick_spacing) == (3000, 60)
    assert other.summary()["pool_id"] != authored.summary()["pool_id"]
    assert (other.digest, other.pool) != (authored.digest, authored.pool)
    assert other.contracts == authored.contracts


def test_uninitialized_pool_key_refuses(recorded, replay):
    block, _ = recorded
    with pytest.raises(DataError, match="not initialized") as error:
        resolve_market(replay(), selected(tick_spacing=200), block)
    assert error.value.kind == "unavailable"


def test_periphery_naming_another_pool_manager_refuses(recorded, replay, monkeypatch):
    block, _ = recorded
    roles = {**NETWORK_ROLES["eip155:1"], "pool_manager": "0x" + "12" * 20}
    monkeypatch.setitem(NETWORK_ROLES, "eip155:1", roles)
    with pytest.raises(DataError, match="different PoolManager") as error:
        resolve_market(replay(), selected(), block)
    assert error.value.kind == "source_mismatch"


@pytest.mark.parametrize("field", ["pool_manager", "state_view", "decimals0", "decimals1"])
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "pool_manager": POOL_MANAGER,
        "state_view": STATE_VIEW,
        "decimals0": 6,
        "decimals1": 18,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool_manager == POOL_MANAGER
    bad = {
        "pool_manager": "0x" + "12" * 20,
        "state_view": "0x" + "34" * 20,
        "decimals0": 18,
        "decimals1": 6,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(PoolAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("field", ["chain", "currency0", "currency1", "fee", "tick_spacing"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "chain": "cosmos:osmosis-1",
        "currency0": str(WETH),
        "currency1": "eip155:56/erc20:0x" + "12" * 20,
        "fee": 1000001,
        "tick_spacing": 0,
    }
    raw = {**selected().model_dump(mode="json"), field: bad[field]}
    with pytest.raises(ValidationError):
        MarketSelection.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("field", ["selection", "block", "asset0", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"fee": 3000}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset0": result.asset0.model_copy(update={"identifier": WETH}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedPool.model_validate({**result.model_dump(), field: bad[field]})


def test_retained_result_refuses_unverified_deployment_code(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    identities = (result.identities[0].model_copy(update={"code_hash": "0x" + "12" * 32}),)
    with pytest.raises(ValidationError, match="unverified"):
        ResolvedPool.model_validate({**result.model_dump(), "identities": identities})


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    quoter = roles_for(CHAIN).quoter
    kept = tuple(r for r in rows if not (r.method == "eth_getCode" and r.params[0] == quoter))
    assert 0 < len(kept) < len(rows)
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def public_allocation(**overrides):
    raw = {
        "market": "pool",
        "account": "fund",
        "connector": "uniswap_v4",
        "kind": "trading",
        "capital": {"asset": str(USDC), "quantity": "100"},
        "hold_seconds": "43200s",
        "slippage_bps": 100,
        "deadline_seconds": "300s",
        "minimum_native_balance": "0.001",
    }
    return AllocationInput.model_validate_json(json.dumps({**raw, **overrides}))


LIQUIDITY = {
    "kind": "liquidity",
    "range": {"lower": 197860, "upper": 199270},
    "collect_seconds": "21600s",
}


@pytest.mark.parametrize(
    ("reference", "name", "overrides"),
    [
        ("uniswap_v4_trading", "trading", {}),
        ("uniswap_v4_lp", "lp", LIQUIDITY),
    ],
)
def test_public_positions_resolve_to_the_legacy_allocations(
    recorded, replay, reference, name, overrides
):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    account = {"account": name}
    resolved = public_allocation(**{**overrides, **account}).resolve(name, market)
    assert resolved == legacy_allocation(reference)


def test_position_capital_must_name_a_resolved_pool_currency(recorded, replay):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    foreign = {"asset": "eip155:1/erc20:0x" + "12" * 20, "quantity": {"base_units": 1}}
    with pytest.raises(ConfigurationError) as error:
        public_allocation(capital=foreign).resolve("position", market)
    assert error.value.field == "capital.asset"


def test_position_capital_and_entry_slice_resolve_at_the_resolved_precision(recorded, replay):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    capital = {"asset": str(USDC), "quantity": "250.5"}
    resolved = public_allocation(capital=capital).resolve("position", market)
    assert (resolved.capital.amount, resolved.entry_swap_bps) == (250500000, 10000)
    assert resolved.minimum_native_balance == 10**15
    # The whole-capital default is omitted, so configurations without a slice read as before.
    assert "entry_swap_bps" not in resolved.model_dump(mode="json")
    sliced = public_allocation(capital=capital, entry_swap_bps=2500).resolve("position", market)
    assert sliced.model_dump(mode="json")["entry_swap_bps"] == 2500
    with pytest.raises(ValidationError):
        public_allocation(entry_swap_bps=10001)
    # Zero offers no entry quote: a liquidity allocation may deposit without an entry
    # swap, while a trading allocation, which enters only through that quote, is refused.
    with pytest.raises(ValueError, match="positive entry_swap_bps"):
        public_allocation(entry_swap_bps=0).resolve("position", market)
    no_swap = public_allocation(entry_swap_bps=0, **LIQUIDITY).resolve("position", market)
    assert no_swap.entry_swap_bps == 0


@pytest.mark.parametrize("field", ["maximum0", "maximum1"])
def test_retired_spending_ceilings_are_refused_as_unknown(field):
    ceiling = {"asset": str(WETH), "quantity": "0.1"}
    with pytest.raises(ValidationError, match="Extra inputs"):
        public_allocation(**{field: ceiling})
    raw = json.loads(public_allocation().model_dump_json())
    assert not validator(POSITION_SCHEMA).is_valid({**raw, field: ceiling})


@pytest.mark.parametrize("overrides", [{"kind": "liquidity"}, {"range": LIQUIDITY["range"]}])
def test_position_kind_and_bounds_must_agree(overrides):
    with pytest.raises(ValidationError, match="liquidity allocation"):
        public_allocation(**overrides)


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


MARKET_SCHEMA = "markets/uniswap_v4.schema.json"
POSITION_SCHEMA = "positions/uniswap_v4.schema.json"


def required_fields(model):
    return {name for name, field in model.model_fields.items() if field.is_required()}


def schema_document(name):
    return json.loads((SCHEMAS / name).read_text())


def test_editor_schemas_and_models_declare_the_same_author_fields():
    market = schema_document(MARKET_SCHEMA)
    assert set(market["properties"]) == set(MarketSelection.model_fields)
    assert set(market["required"]) == required_fields(MarketSelection)
    position = schema_document(POSITION_SCHEMA)
    # The parser injects the connector tag from the named market; authors never write it.
    assert set(position["properties"]) == set(AllocationInput.model_fields) - {"connector"}
    assert set(position["required"]) == required_fields(AllocationInput)


def test_editor_schemas_accept_the_shipped_ethereum_market_and_positions():
    validator(MARKET_SCHEMA).validate(selected().model_dump(mode="json"))
    validator(MARKET_SCHEMA).validate(
        {
            "connector": "uniswap_v4",
            "chain": "eip155:1",
            "currency0": str(USDC),
            "currency1": str(WETH),
            "fee": 500,
            "tick_spacing": 10,
            "hooks": "0x0000000000000000000000000000000000000000",
            "assertions": {"decimals0": 6, "decimals1": 18},
        }
    )
    for overrides in ({}, LIQUIDITY):
        raw = json.loads(public_allocation(**overrides).model_dump_json())
        validator(POSITION_SCHEMA).validate(raw)


@pytest.mark.parametrize(
    "mutation", ["unknown", "currencyless", "float_fee", "short_hook", "string_fee"]
)
def test_market_schema_refuses_author_contract_errors(mutation):
    raw = selected().model_dump(mode="json")
    if mutation == "unknown":
        raw["pool"] = "0x" + "12" * 20
    elif mutation == "currencyless":
        del raw["currency1"]
    elif mutation == "float_fee":
        raw["fee"] = 500.5
    elif mutation == "short_hook":
        raw["hooks"] = "0x1234"
    else:
        raw["fee"] = "500"
    assert not validator(MARKET_SCHEMA).is_valid(raw)


@pytest.mark.parametrize(
    "mutation", ["harness_key", "kindless", "float_tick", "unquoted", "spent_amount"]
)
def test_position_schema_refuses_author_contract_errors(mutation):
    raw = json.loads(public_allocation(**LIQUIDITY).model_dump_json())
    if mutation == "harness_key":
        raw["native_asset"] = "eip155:1/slip44:60"
    elif mutation == "kindless":
        del raw["kind"]
    elif mutation == "float_tick":
        raw["range"] = {"lower": 197860.5, "upper": 199270}
    elif mutation == "unquoted":
        raw["capital"]["quantity"] = 100.5
    else:
        raw["capital"] = {"base_units": 100000000}
    assert not validator(POSITION_SCHEMA).is_valid(raw)


@pytest.mark.parametrize(
    "field,kind",
    [("range", "trading"), ("range", "liquidity"), ("collect_seconds", "trading")],
)
def test_editor_and_runtime_reject_mismatched_liquidity_fields(field, kind):
    raw = json.loads(public_allocation(**LIQUIDITY).model_dump_json())
    if kind == "liquidity":
        raw[field] = None
    else:
        raw["kind"] = "trading"
        raw[{"range": "collect_seconds", "collect_seconds": "range"}[field]] = None
    assert not validator(POSITION_SCHEMA).is_valid(raw)
    with pytest.raises(ValueError):
        AllocationInput.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("omit", [True, False])
def test_a_liquidity_allocation_may_omit_collection_timing(omit):
    raw = json.loads(public_allocation(**LIQUIDITY).model_dump_json())
    if omit:
        raw.pop("collect_seconds")
    else:
        raw["collect_seconds"] = None
    assert validator(POSITION_SCHEMA).is_valid(raw)
    assert AllocationInput.model_validate_json(json.dumps(raw)).collect_seconds is None


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert {ChainId(chain) for chain in NETWORK_ROLES} == {
        entry.chain for entry in candidate_uniswap_support()
    }
    assert all(roles_for(ChainId(chain)).addresses for chain in NETWORK_ROLES)


def frozen_references():
    return sorted(json.loads(gzip.decompress(LEGACY.read_bytes()))["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_deployment(name, entry):
    """The table holds the deployments every reference proved against before conversion."""
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    keys = {
        (market["currency0"], market["currency1"], market["fee"])
        for market in document["markets"].values()
    }
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        assert "connector does not support" in record["message"]
        return
    for allocation in record["value"]["allocations"]:
        pool, contracts = allocation["pool"], allocation["contracts"]
        roles = roles_for(ChainId(pool["chain"]["chain_id"]))
        assert (pool["currency0"], pool["currency1"], pool["fee"]) in keys
        assert (pool["pool_manager"], pool["state_view"]) == (roles.pool_manager, roles.state_view)
        assert contracts == roles.contracts.model_dump(mode="json")
