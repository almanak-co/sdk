"""Morpho Blue market resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
import yaml
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.morpho_blue.author_inputs import AllocationInput
from almanak.connectors.morpho_blue.deployment import REVISIONS
from almanak.connectors.morpho_blue.domain import MarketId
from almanak.connectors.morpho_blue.models import Allocation
from almanak.connectors.morpho_blue.networks import CORES, core_for
from almanak.connectors.morpho_blue.resolution import (
    MarketAssertions,
    MarketSelection,
    ResolvedMarket,
    resolve_market,
)
from almanak.connectors.morpho_blue.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import without_retired_fields
from tests.lending_captures import current_allocation

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "morpho-blue-base-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/morpho_blue.json.gz"
BASE = ChainId("eip155:8453")
CORE = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"
MARKET = MarketId("0x8793cf302b8ffd655ab97bd1c695dbd967807e8367a65cb2f4edaf1380ba1bda")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
ORACLE = "0xfea2d58cefcb9fcb597723c6bae66ffe4193afe4"
IRM = "0x46415998764c29ab2a25cbea6254146d50d22687"
LLTV = 860000000000000000
LEGS = {"loan": (USDC, 6), "collateral": (WETH, 18)}


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


def selected(leg="loan", assertions=None, chain=None, core=None, market_id=None):
    return MarketSelection(
        connector="morpho_blue",
        chain=chain or BASE,
        market_id=market_id or MARKET,
        leg=leg,
        core=core,
        assertions=assertions,
    )


def legacy_allocation(index):
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    value = raw["references"]["morpho_blue_base_lending"]["files"]["config.yaml"]["value"]
    # The frozen value predates scope-only authority; read it in today's fields.
    allocation = without_retired_fields(current_allocation(value["allocations"][index]))
    return Allocation.model_validate_json(json.dumps(allocation))


@pytest.mark.parametrize(("leg", "index"), [("loan", 0), ("collateral", 1)])
def test_recorded_reads_resolve_the_legacy_base_market(recorded, replay, leg, index):
    block, _ = recorded
    legacy = legacy_allocation(index)
    result = resolve_market(replay(), selected(leg), block)
    assert result.reserve == legacy.reserve
    market = result.market
    assert (market.chain, market.core, market.market_id) == (block.chain, CORE, MARKET)
    assert (market.loan_asset, market.collateral_asset) == (USDC, WETH)
    assert (market.oracle, market.irm, market.lltv) == (ORACLE, IRM, LLTV)
    assert (result.asset.identifier, result.asset.decimals) == LEGS[leg]
    assert result.assets == (result.asset,)
    assert result.block == block
    assert [identity.address for identity in result.identities] == [CORE, IRM, ORACLE]
    assert ResolvedMarket.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "core",
        "market_id",
        "loan_asset",
        "collateral_asset",
        "oracle",
        "irm",
        "lltv",
        "asset",
        "decimals",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert (summary["core"], summary["market_id"]) == (CORE, str(MARKET))
    assert (summary["loan_asset"], summary["collateral_asset"]) == (str(USDC), str(WETH))
    assert (summary["oracle"], summary["irm"], summary["lltv"]) == (ORACLE, IRM, LLTV)
    assert (summary["asset"], summary["decimals"], summary["block"]) == (str(USDC), 6, block.number)


def test_unknown_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in CORES
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    with pytest.raises(DataError, match="no reviewed core") as error:
        resolve_market(reader, selected(chain=foreign), block.model_copy(update={"chain": chain}))
    assert error.value.kind == "unsupported_chain"


def test_unlisted_core_chain_refuses_with_a_remedy():
    with pytest.raises(DataError, match="no reviewed core on eip155:100") as error:
        core_for(ChainId("eip155:100"))
    assert error.value.kind == "unsupported_chain"


def test_named_core_outside_the_table_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    reader = replay(evidence=())
    with pytest.raises(DataError, match="outside the reviewed deployment") as error:
        resolve_market(reader, selected(core="0x" + "12" * 20), block)
    assert error.value.kind == "deployment_mismatch"


def test_foreign_anchor_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    reader = replay(evidence=())
    foreign = block.model_copy(
        update={"chain": block.chain.model_copy(update={"environment": "x"})}
    )
    with pytest.raises(DataError, match="selected chain and anchor") as error:
        resolve_market(reader, selected(), foreign)
    assert error.value.kind == "chain_mismatch"


def unregistered_read(block, market_id):
    """The core's own mapping answers an unregistered id with the zero tuple."""
    data = keccak(text="idToMarketParams(bytes32)")[:4] + bytes.fromhex(str(market_id)[2:])
    return ResolutionRead(
        method="eth_call",
        params=(
            {"to": CORE, "data": "0x" + data.hex()},
            {"blockHash": block.hash, "requireCanonical": True},
        ),
        result="0x" + "00" * 160,
        retrieved_at=0.0,
    )


def test_unknown_market_identifier_refuses_without_naming_a_substitute(recorded, replay):
    block, rows = recorded
    unknown = MarketId("0x" + "11" * 32)
    reader = replay(evidence=(*rows, unregistered_read(block, unknown)))
    with pytest.raises(DataError, match="registers no market") as error:
        resolve_market(reader, selected(market_id=unknown), block)
    assert error.value.kind == "unknown_market"


@pytest.mark.parametrize(
    "field", ["loan_asset", "collateral_asset", "oracle", "irm", "lltv", "decimals"]
)
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "loan_asset": USDC,
        "collateral_asset": WETH,
        "oracle": ORACLE,
        "irm": IRM,
        "lltv": LLTV,
        "decimals": 6,
    }
    result = resolve_market(replay(), selected(assertions=MarketAssertions(**expected)), block)
    assert result.market.market_id == MARKET
    bad = {
        "loan_asset": WETH,
        "collateral_asset": USDC,
        "oracle": "0x" + "12" * 20,
        "irm": "0x" + "34" * 20,
        "lltv": 900000000000000000,
        "decimals": 18,
    }
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(
            replay(), selected(assertions=MarketAssertions(**{field: bad[field]})), block
        )


@pytest.mark.parametrize("field", ["core", "chain", "market_id", "leg", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "core": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "market_id": "0x" + "11" * 31,
        "leg": "supply",
        "assertions": MarketAssertions(loan_asset=AssetType("eip155:56/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


def rebuilt(result, **overrides):
    fields = {name: getattr(result, name) for name in ResolvedMarket.model_fields}
    return ResolvedMarket(**{**fields, **overrides})


@pytest.mark.parametrize("field", ["selection", "block", "asset", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"market_id": MarketId("0x" + "11" * 32)}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset": result.asset.model_copy(update={"identifier": WETH}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        rebuilt(result, **{field: bad[field]})


def test_resolved_leg_must_match_its_authored_side(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    with pytest.raises(ValidationError, match="leg differs"):
        rebuilt(result, selection=result.selection.model_copy(update={"leg": "collateral"}))


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    kept = tuple(r for r in rows if not (r.method == "eth_getCode" and r.params[0] == ORACLE))
    assert len(kept) == len(rows) - 2
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def public_allocation(account, **overrides):
    raw = {
        "market": "market",
        "account": account,
        "connector": "morpho_blue",
        "minimum_health_factor": "2",
        "minimum_native_balance": {"base_units": 0},
    }
    return AllocationInput.model_validate_json(json.dumps({**raw, **overrides}))


@pytest.mark.parametrize(
    ("name", "index", "leg", "account", "overrides"),
    [
        ("earn", 0, "loan", "lender", {"capital": "1000"}),
        ("collateral", 1, "collateral", "borrower", {"capital": "1"}),
        ("loan", 2, "loan", "borrower", {"borrow": True}),
    ],
)
def test_public_positions_resolve_to_the_legacy_allocations(
    recorded, replay, name, index, leg, account, overrides
):
    block, _ = recorded
    market = resolve_market(replay(), selected(leg), block)
    produced = public_allocation(account, **overrides).resolve(name, market)
    assert produced == legacy_allocation(index)


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_public_allocation_refuses_retired_budget_fields(field):
    with pytest.raises(ValidationError, match="Extra inputs"):
        public_allocation("lender", **{field: "1"})


def test_public_collateral_cannot_borrow(recorded, replay):
    block, _ = recorded
    market = resolve_market(replay(), selected("collateral"), block)
    with pytest.raises(ValidationError, match="cannot carry borrowing"):
        public_allocation("borrower", capital="1", borrow=True).resolve("collateral", market)


def test_moved_anchor_refuses_before_the_market_reads(recorded, replay):
    block, rows = recorded
    moved = []
    for row in rows:
        if row.method == "eth_getBlockByNumber":
            row = row.model_copy(update={"result": {**row.result, "hash": "0x" + "ab" * 32}})
        moved.append(row)
    with pytest.raises(DataError, match="no longer canonical") as error:
        resolve_market(replay(evidence=tuple(moved)), selected(), block)
    assert error.value.kind == "block_mismatch"


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


def frozen_references():
    return sorted(json.loads(gzip.decompress(LEGACY.read_bytes()))["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_core(name, entry):
    """The table holds the core every reference proved against before conversion."""
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    assert {ChainId(market["chain"]) for market in document["markets"].values()} <= set(CORES)
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        assert "connector does not support" in record["message"]
        return
    for allocation in record["value"]["allocations"]:
        market = allocation["reserve"]["market"]
        assert market["core"] == core_for(ChainId(market["chain"]["chain_id"]))


def test_every_verified_revision_chain_has_exactly_one_reviewed_core():
    assert set(CORES) == {ChainId(chain) for chain in REVISIONS}
    assert {entry.chain for entry in candidate_support()} <= set(CORES)
