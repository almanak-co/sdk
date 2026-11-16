"""Uniswap V3 pool resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.uniswap_v3.author_inputs import AllocationInput
from almanak.connectors.uniswap_v3.models import Allocation
from almanak.connectors.uniswap_v3.networks import DEPLOYMENTS, Deployment, deployment_for
from almanak.connectors.uniswap_v3.resolution import (
    MarketSelection,
    PoolAssertions,
    ResolvedPool,
    resolve_market,
)
from almanak.connectors.uniswap_v3.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "uniswap-v3-base-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/uniswap_v3.json.gz"
POOL = "0xd0b53d9277642d899df5c87a3966a349a798f224"
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
        connector="uniswap_v3",
        chain=chain or ChainId("eip155:8453"),
        pool=pool,
        assertions=assertions,
    )


def legacy_allocation(reference):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    value = raw["references"][reference]["files"]["config.yaml"]["value"]
    return Allocation.model_validate_json(
        json.dumps(without_retired_fields(value["allocations"][0]))
    )


def test_recorded_reads_resolve_the_legacy_base_market(recorded, replay):
    block, _ = recorded
    legacy = legacy_allocation("uniswap_v3_base_trading")
    result = resolve_market(replay(), selected(), block)
    assert result.pool == legacy.pool
    assert result.contracts == legacy.contracts
    assert (result.pool.fee, result.pool.tick_spacing) == (500, 10)
    assert (result.pool.currency0, result.pool.currency1) == (WETH, USDC)
    assert [asset.decimals for asset in result.assets] == [18, 6]
    assert result.assets == (result.asset0, result.asset1)
    assert result.block == block
    assert [identity.address for identity in result.identities] == [
        legacy.pool.factory,
        legacy.pool.pool,
        legacy.contracts.router,
        legacy.contracts.position_manager,
        legacy.contracts.quoter,
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
    assert summary["currencies"] == [str(WETH), str(USDC)]
    assert (summary["fee"], summary["tick_spacing"], summary["decimals"]) == (500, 10, [18, 6])
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
    named = DEPLOYMENTS[ChainId("eip155:8453")].factory
    foreign = DEPLOYMENTS[ChainId("eip155:8453")].model_copy(update={"factory": "0x" + "12" * 20})
    monkeypatch.setitem(DEPLOYMENTS, ChainId("eip155:8453"), foreign)
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
        "currency0": WETH,
        "currency1": USDC,
        "fee": 500,
        "tick_spacing": 10,
        "decimals0": 18,
        "decimals1": 6,
    }
    result = resolve_market(replay(), selected(PoolAssertions(**expected)), block)
    assert result.pool.pool == POOL
    bad = {
        "currency0": AssetType("eip155:8453/erc20:0x" + "12" * 20),
        "currency1": AssetType("eip155:8453/erc20:0x" + "34" * 20),
        "fee": 3000,
        "tick_spacing": 60,
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


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    quoter = DEPLOYMENTS[ChainId("eip155:8453")].quoter
    kept = tuple(r for r in rows if not (r.method == "eth_getCode" and r.params[0] == quoter))
    assert len(kept) == len(rows) - 1
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def public_allocation(**overrides):
    raw = {
        "market": "pool",
        "account": "fund",
        "connector": "uniswap_v3",
        "capital": {"asset": str(USDC), "quantity": {"base_units": 100000000}},
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
        ("uniswap_v3_base_trading", {}),
        (
            "uniswap_v3_base_lp",
            {"entry_swap_bps": 5000, "range": {"lower": -198530, "upper": -197530}},
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


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_deployment(name, entry):
    """The table holds the deployments every reference proved against before conversion."""
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    markets = {
        (ChainId(market["chain"]), market["pool"]): market
        for market in document["markets"].values()
    }
    assert all(chain in DEPLOYMENTS for chain, _ in markets)
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        assert "connector does not support" in record["message"]
        return
    for allocation in record["value"]["allocations"]:
        pool, contracts = allocation["pool"], allocation["contracts"]
        chain = ChainId(pool["chain"]["chain_id"])
        deployment = deployment_for(chain)
        assert (chain, pool["pool"]) in markets
        assert pool["factory"] == deployment.factory
        assert contracts == {
            "router": deployment.router,
            "position_manager": deployment.position_manager,
            "quoter": deployment.quoter,
        }


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert set(DEPLOYMENTS) == {entry.chain for entry in candidate_support()}
    assert all(isinstance(value, Deployment) for value in DEPLOYMENTS.values())
