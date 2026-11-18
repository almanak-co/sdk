"""Curve pool resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from referencing import Registry, Resource

from almanak.caip import AssetType, ChainId
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.curve import networks, resolution
from almanak.connectors.curve.author_inputs import AllocationInput
from almanak.connectors.curve.models import Allocation
from almanak.connectors.curve.networks import FACTORIES, Factories, factories_for
from almanak.connectors.curve.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.connectors.curve.support import verified_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
SCHEMAS = ROOT / "docs/schemas"
READS = FIXTURES / "curve-ethereum-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/curve.json.gz"
ETHEREUM = ChainId("eip155:1")
POOLS = {
    "stable": "0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72",
    "twocrypto": "0xa19bf6fbf05624282cb6ed498f4761f22e084edd",
    "tricrypto": "0x7f86bf177dd4f3494b841a37e810a34dd56c829b",
}
UNREGISTERED = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
CRVUSD = f"{ETHEREUM}/erc20:0x4c9edd5852cd905f086c759e8383e09bff1e68b3"
USDC = f"{ETHEREUM}/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH = f"{ETHEREUM}/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WBTC = f"{ETHEREUM}/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
PAIRED = f"{ETHEREUM}/erc20:0x73968b9a57c6e53d41345fd57a6e6ae27d6cdb2f"
FAMILIES = {
    "stable": ("stableswap_ng", "stableswap-ng-2023-11-15", (CRVUSD, USDC), (18, 6)),
    "twocrypto": ("twocrypto_ng", "twocrypto-ng-v2.1.0", (WETH, PAIRED), (18, 18)),
    "tricrypto": ("tricrypto_ng", "tricrypto-ng-v2.0.0", (USDC, WBTC, WETH), (6, 8, 18)),
}


def assets(values):
    return tuple(AssetType(value) for value in values)


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


def selected(assertions=None, chain=None, pool=POOLS["stable"]):
    return MarketSelection(
        connector="curve", chain=chain or ETHEREUM, pool=pool, assertions=assertions
    )


def legacy_record(reference):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    return raw["references"][reference]["files"]["config.yaml"]


def legacy_allocation(reference, index=0):
    value = legacy_record(reference)["value"]
    allocation = without_retired_fields(value["allocations"][index])
    return Allocation.model_validate_json(json.dumps(allocation))


def legacy_decimals(reference):
    return {
        asset: entry["decimals"]
        for asset, entry in legacy_record(reference)["value"]["asset_metadata"].items()
    }


@pytest.mark.parametrize("key", sorted(FAMILIES))
def test_recorded_reads_resolve_each_ethereum_family(recorded, replay, key):
    block, _ = recorded
    family, revision, coins, decimals = FAMILIES[key]
    result = resolve_market(replay(), selected(pool=POOLS[key]), block)
    assert (result.pool.family, result.revision) == (family, revision)
    assert result.pool.pool == POOLS[key]
    assert result.pool.coins == assets(coins)
    assert tuple(asset.decimals for asset in result.coins) == decimals
    assert result.assets == (*result.coins, result.share)
    assert str(result.share.identifier) == f"{ETHEREUM}/erc20:{POOLS[key]}"
    assert result.share.decimals == 18
    reviewed = getattr(factories_for(ETHEREUM), family)
    assert (result.contracts.factory, result.pool.factory) == (reviewed, reviewed)
    assert [identity.address for identity in result.identities] == list(
        result.contracts.ordered(family)
    )
    assert result.block == block
    assert ResolvedPool.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


@pytest.mark.parametrize("key", sorted(FAMILIES))
def test_resolved_facts_match_the_frozen_reference_pool(recorded, replay, key):
    block, _ = recorded
    reference = f"curve_ethereum_{key}_lp"
    result = resolve_market(replay(), selected(pool=POOLS[key]), block)
    assert result.pool == legacy_allocation(reference).pool
    metadata = legacy_decimals(reference)
    assert {str(a.identifier): a.decimals for a in result.assets}.items() <= metadata.items()


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "pool",
        "family",
        "revision",
        "factory",
        "coins",
        "decimals",
        "share_decimals",
        "block",
    }
    assert summary["share_decimals"] == 18
    assert summary["selection"] == selected().model_dump(mode="json")
    assert summary["coins"] == [CRVUSD, USDC]
    assert (summary["family"], summary["decimals"]) == ("stableswap_ng", [18, 6])
    assert (summary["pool"], summary["block"]) == (POOLS["stable"], block.number)


def test_unlisted_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in FACTORIES
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    with pytest.raises(DataError, match="no reviewed factory") as error:
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
    with pytest.raises(DataError, match="no reviewed factory on eip155:100") as error:
        factories_for(ChainId("eip155:100"))
    assert error.value.kind == "unsupported_chain"


def test_pool_no_reviewed_factory_registers_refuses(recorded, replay):
    block, _ = recorded
    with pytest.raises(DataError, match="no reviewed Curve factory") as error:
        resolve_market(replay(), selected(pool=UNREGISTERED), block)
    assert error.value.kind == "unsupported_pool"


class EveryFactory:
    """Every listed factory claims the pool, so the family stays undecided."""

    def call(self, *_args, **_kwargs):
        return ((CRVUSD.split(":")[-1], USDC.split(":")[-1]),)


def test_two_registering_factories_refuse_without_choosing(recorded):
    block, _ = recorded
    listed = factories_for(ETHEREUM)
    with pytest.raises(DataError, match="more than one reviewed") as error:
        networks.registered_family(EveryFactory(), listed, POOLS["stable"], block)
    assert error.value.kind == "ambiguous_pool"


def refusing(kind, seen):
    def verify_pool(_reader, pool, _block):
        seen.append(pool.implementation_revision)
        raise DataError(kind, "stand-in Curve verification refusal")

    return verify_pool


def test_every_reviewed_revision_probed_before_refusing(recorded, replay, monkeypatch):
    seen = []
    block, _ = recorded
    monkeypatch.setattr(resolution.deployment, "verify_pool", refusing("revision", seen))
    with pytest.raises(DataError, match="no reviewed compiled revision") as error:
        resolve_market(replay(), selected(pool=POOLS["tricrypto"]), block)
    assert error.value.kind == "unsupported_revision"
    assert seen == [None, "tricrypto-ng-2024-01-04"]


def test_verification_failures_other_than_revision_are_not_retried(recorded, replay, monkeypatch):
    seen = []
    block, _ = recorded
    monkeypatch.setattr(resolution.deployment, "verify_pool", refusing("identity", seen))
    with pytest.raises(DataError) as error:
        resolve_market(replay(), selected(pool=POOLS["tricrypto"]), block)
    assert error.value.kind == "identity"
    assert seen == [None]


@pytest.mark.parametrize("field", ["family", "revision", "coins", "decimals"])
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    family, revision, coins, decimals = FAMILIES["stable"]
    expected = {
        "family": family,
        "revision": revision,
        "coins": assets(coins),
        "decimals": decimals,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool == POOLS["stable"]
    bad = {
        "family": "twocrypto_ng",
        "revision": "stableswap-ng-2023-12-12",
        "coins": assets((USDC, CRVUSD)),
        "decimals": (6, 18),
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(PoolAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("field", ["pool", "chain", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "pool": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "assertions": PoolAssertions(coins=assets(("eip155:56/erc20:0x" + "12" * 20, USDC))),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


@pytest.mark.parametrize(
    "field", ["selection", "block", "coins", "share", "identities", "revision"]
)
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"pool": "0x" + "12" * 20}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "coins": tuple(reversed(result.coins)),
        "share": result.share.model_copy(update={"identifier": AssetType(WETH)}),
        "identities": tuple(reversed(result.identities)),
        "revision": "twocrypto-ng-v2.1.0",
    }
    with pytest.raises(ValidationError):
        ResolvedPool.model_validate({**result.model_dump(), field: bad[field]})


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    kept = tuple(
        row
        for row in rows
        if not (row.method == "eth_getCode" and row.params[0] == POOLS["stable"])
    )
    assert len(kept) < len(rows)
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def amounts(pairs):
    return [{"asset": asset, "quantity": quantity} for asset, quantity in pairs]


def public_allocation(**fields):
    raw = {
        "market": "pool",
        "connector": "curve",
        "liquidity_enabled": True,
        "entry_swap_bps": 5000,
        "hold_seconds": "2h",
        "slippage_bps": 100,
        "deadline_seconds": "5m",
        "minimum_native_balance": {"base_units": 0},
    }
    return AllocationInput.model_validate_json(json.dumps({**raw, **fields}))


STABLE_ECONOMICS = {
    "funding_asset": USDC,
    "trading_asset": CRVUSD,
    "capital": amounts(((CRVUSD, "0"), (USDC, "100"))),
}
REFERENCES = [
    ("curve_ethereum_stable_lp", 0, "stable", "proportional", STABLE_ECONOMICS),
    (
        "curve_ethereum_stable_lp",
        1,
        "stable",
        "single_coin",
        {**STABLE_ECONOMICS, "single_coin_exit": USDC},
    ),
    (
        "curve_ethereum_stable_trading",
        0,
        "stable",
        "trade",
        {**STABLE_ECONOMICS, "liquidity_enabled": False},
    ),
    (
        "curve_ethereum_twocrypto_lp",
        0,
        "twocrypto",
        "proportional",
        {
            "funding_asset": WETH,
            "trading_asset": PAIRED,
            "capital": amounts(((WETH, "0.01"), (PAIRED, "0"))),
        },
    ),
    (
        "curve_ethereum_tricrypto_lp",
        0,
        "tricrypto",
        "proportional",
        {
            "funding_asset": WETH,
            "trading_asset": USDC,
            "capital": amounts(((USDC, "0"), (WBTC, "0"), (WETH, "0.01"))),
        },
    ),
]


@pytest.mark.parametrize(("reference", "index", "key", "name", "economics"), REFERENCES)
def test_public_positions_resolve_to_the_legacy_allocations(
    recorded, replay, reference, index, key, name, economics
):
    block, _ = recorded
    market = resolve_market(replay(), selected(pool=POOLS[key]), block)
    allocation = public_allocation(account=name, **economics)
    assert allocation.resolve(name, market) == legacy_allocation(reference, index)


def test_position_amounts_must_name_the_resolved_coins_in_order(recorded, replay):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    reordered = amounts(((USDC, "1"), (CRVUSD, "1")))
    with pytest.raises(ConfigurationError) as error:
        public_allocation(account="fund", **{**STABLE_ECONOMICS, "capital": reordered}).resolve(
            "position", market
        )
    assert error.value.field == "capital.asset"


def test_retired_input_budgets_are_refused_as_unknown():
    retired = amounts(((CRVUSD, "1000"), (USDC, "1000")))
    with pytest.raises(ValueError, match="Extra inputs"):
        public_allocation(account="fund", **STABLE_ECONOMICS, maximums=retired)
    assert not validator("positions/curve.schema.json").is_valid(
        position_document(maximums=retired)
    )


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


def market_document(**fields):
    return {"connector": "curve", "chain": str(ETHEREUM), "pool": POOLS["stable"], **fields}


def position_document(**fields):
    return {
        "market": "pool",
        "account": "fund",
        "liquidity_enabled": True,
        "entry_swap_bps": 5000,
        "hold_seconds": "2h",
        "slippage_bps": 100,
        "deadline_seconds": "5m",
        "minimum_native_balance": {"base_units": 0},
        **STABLE_ECONOMICS,
        **fields,
    }


def test_editor_schemas_accept_the_public_curve_inputs():
    market = market_document(assertions={"family": "stableswap_ng", "decimals": [18, 6]})
    validator("markets/curve.schema.json").validate(market)
    assert MarketSelection.model_validate_json(json.dumps(market))
    position = position_document(single_coin_exit=USDC)
    validator("positions/curve.schema.json").validate(position)
    assert AllocationInput.model_validate_json(json.dumps({**position, "connector": "curve"}))


@pytest.mark.parametrize(
    ("schema", "document"),
    [
        ("markets/curve.schema.json", market_document(connector="curve_v2")),
        ("markets/curve.schema.json", market_document(family="stableswap_ng")),
        ("markets/curve.schema.json", market_document(assertions={"family": "stableswap"})),
        ("markets/curve.schema.json", market_document(assertions={"decimals": [18]})),
        ("markets/curve.schema.json", market_document(pool="0x1234")),
        (
            "positions/curve.schema.json",
            position_document(liquidity_enabled=False, entry_swap_bps=0),
        ),
        ("positions/curve.schema.json", position_document(capital=amounts(((USDC, "1"),)))),
        ("positions/curve.schema.json", position_document(slippage_bps=10000)),
        ("positions/curve.schema.json", position_document(factory="0x" + "12" * 20)),
    ],
)
def test_editor_schemas_refuse_unsupported_author_documents(schema, document):
    assert not validator(schema).is_valid(document)


def test_position_schema_requires_every_economic_choice():
    required = json.loads((SCHEMAS / "positions/curve.schema.json").read_text())["required"]
    for field in required:
        document = {key: value for key, value in position_document().items() if key != field}
        assert not validator("positions/curve.schema.json").is_valid(document), field


def frozen_references():
    return sorted(json.loads(gzip.decompress(LEGACY.read_bytes()))["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_factory(name, entry):
    """The table holds the factories every reference proved against before conversion."""
    record = entry["files"]["config.yaml"]
    assert record["status"] == "normalized", record.get("message")
    for allocation in record["value"]["allocations"]:
        pool = allocation["pool"]
        listed = factories_for(ChainId(pool["chain"]["chain_id"]))
        assert getattr(listed, pool["family"]) == pool["factory"]


def test_every_supported_chain_lists_its_declared_families():
    declared = {entry.chain: entry.variants for entry in verified_support()}
    assert set(FACTORIES) == set(declared)
    for chain, listed in FACTORIES.items():
        assert isinstance(listed, Factories)
        families = {family for family, _ in listed.listed()}
        assert families == {str(variant).removesuffix("_2024") for variant in declared[chain]}


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))
