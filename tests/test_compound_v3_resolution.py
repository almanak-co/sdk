"""Compound V3 market resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.compound_v3.author_inputs import AllocationInput
from almanak.connectors.compound_v3.models import Allocation
from almanak.connectors.compound_v3.resolution import (
    MarketAssertions,
    MarketSelection,
    ResolvedMarket,
    resolve_market,
)
from almanak.connectors.compound_v3.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import Block
from tests.legacy_references import harness_directory, without_retired_fields
from tests.lending_captures import current_allocation

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
READS = FIXTURES / "compound-v3-base-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/compound_v3.json.gz"
REFERENCE = "compound_v3_base_lending"
COMET = "0xb125e6687d4313864e53df431d5425969c15eb2f"
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
USDC_FEED = "0x3e6d1cca8eee6d02f1f578b613374eb53e6823b4"
WETH_FEED = "0xe6eb5b9b85cff2c84df3de6e7855bc9e76f034d5"
# The Comet lists WETH at collateral offset 1; this selector carries that index.
ASSET_INFO_1 = "0xc8c7fe6b" + "00" * 31 + "01"


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


def selected(assertions=None, chain=None, asset=USDC, comet=COMET):
    return MarketSelection(
        connector="compound_v3",
        chain=chain or ChainId("eip155:8453"),
        comet=comet,
        asset=asset,
        assertions=assertions,
    )


def legacy_allocations():
    raw = json.loads(gzip.decompress(LEGACY.read_bytes()))
    value = raw["references"][REFERENCE]["files"]["config.yaml"]["value"]
    # The recorded limits translate to today's capital and borrow, as captures do.
    rows = (without_retired_fields(current_allocation(row)) for row in value["allocations"])
    return {row["name"]: Allocation.model_validate_json(json.dumps(row)) for row in rows}


@pytest.mark.parametrize(
    ("position", "asset", "feed", "decimals"),
    [("earn", USDC, USDC_FEED, 6), ("collateral", WETH, WETH_FEED, 18)],
)
def test_recorded_reads_resolve_the_legacy_base_markets(
    recorded, replay, position, asset, feed, decimals
):
    block, _ = recorded
    legacy = legacy_allocations()[position].reserve
    result = resolve_market(replay(), selected(asset=asset), block)
    assert result.reserve == legacy
    assert result.reserve.market.comet == COMET
    assert result.reserve.market.base_asset == USDC
    assert result.reserve.market.base_price_feed == USDC_FEED
    assert (result.reserve.asset, result.reserve.price_feed) == (asset, feed)
    assert (result.asset.identifier, result.asset.decimals) == (asset, decimals)
    assert result.assets == (result.asset,)
    assert result.block == block
    assert str(result.revision) == "extended_asset_list_with_controls"
    assert [identity.address for identity in result.identities][0] == COMET
    assert ResolvedMarket.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(asset=WETH), block).summary()
    assert set(summary) == {
        "selection",
        "comet",
        "base_asset",
        "base_price_feed",
        "asset",
        "price_feed",
        "decimals",
        "revision",
        "block",
    }
    assert summary["selection"] == selected(asset=WETH).model_dump(mode="json")
    assert (summary["comet"], summary["base_asset"]) == (COMET, str(USDC))
    assert (summary["base_price_feed"], summary["price_feed"]) == (USDC_FEED, WETH_FEED)
    assert (summary["asset"], summary["decimals"]) == (str(WETH), 18)
    assert summary["revision"] == "extended_asset_list_with_controls"
    assert summary["block"] == block.number


def test_unknown_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:100")
    assert foreign not in {entry.chain for entry in candidate_support()}
    chain = block.chain.model_copy(update={"chain_id": foreign})
    reader = replay(evidence=(), chain=chain)
    asset = AssetType(f"{foreign}/erc20:{USDC.reference}")
    with pytest.raises(DataError, match="no declared support") as error:
        resolve_market(
            reader,
            selected(chain=foreign, asset=asset),
            block.model_copy(update={"chain": chain}),
        )
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


def test_asset_the_comet_does_not_list_refuses_with_a_remedy(recorded, replay):
    block, _ = recorded
    unlisted = AssetType("eip155:8453/erc20:0x" + "12" * 20)
    with pytest.raises(DataError, match="Select an asset this Comet lists") as error:
        resolve_market(replay(), selected(asset=unlisted), block)
    assert error.value.kind == "unsupported_asset"


@pytest.mark.parametrize("field", ["base_asset", "price_feed", "decimals"])
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {"base_asset": USDC, "price_feed": WETH_FEED, "decimals": 18}
    result = resolve_market(replay(), selected(MarketAssertions(**expected), asset=WETH), block)
    assert result.reserve.price_feed == WETH_FEED
    bad = {
        "base_asset": WETH,
        "price_feed": USDC_FEED,
        "decimals": 6,
    }
    assertions = MarketAssertions(**{field: bad[field]})
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(assertions, asset=WETH), block)


@pytest.mark.parametrize("field", ["comet", "chain", "asset", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "comet": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "asset": AssetType("eip155:8453/slip44:60"),
        "assertions": MarketAssertions(base_asset=AssetType("eip155:1/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected().model_dump(), field: bad[field]})


@pytest.mark.parametrize("field", ["selection", "block", "asset", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"comet": "0x" + "12" * 20}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset": result.asset.model_copy(update={"identifier": WETH}),
        "identities": tuple(reversed(result.identities)),
    }
    with pytest.raises(ValidationError):
        ResolvedMarket.model_validate({**result.model_dump(), field: bad[field]})


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    kept = tuple(
        row
        for row in rows
        if not (row.method == "eth_call" and row.params[0]["data"] == ASSET_INFO_1)
    )
    assert len(kept) == len(rows) - 1
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(asset=WETH), block)
    assert error.value.kind == "resolution_evidence"


AUTHORED = {
    "earn": {
        "market": "earn",
        "account": "lender",
        "capital": "1000",
    },
    "collateral": {
        "market": "collateral",
        "account": "borrower",
        "capital": "1",
    },
    "loan": {
        "market": "loan",
        "account": "borrower",
        "borrow": True,
    },
}


def public_allocation(position, **overrides):
    raw = {
        **AUTHORED[position],
        "minimum_health_factor": "2",
        "minimum_native_balance": "0",
        **overrides,
    }
    return AllocationInput.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("position", "asset"), [("earn", USDC), ("collateral", WETH), ("loan", USDC)]
)
def test_public_positions_resolve_to_the_legacy_allocations(recorded, replay, position, asset):
    block, _ = recorded
    market = resolve_market(replay(), selected(asset=asset), block)
    resolved = public_allocation(position).resolve(position, market)
    assert resolved == legacy_allocations()[position]
    assert resolved.minimum_health_factor == 2 * 10**18
    assert resolved.native_asset == AssetType("eip155:8453/slip44:60")


def test_author_quantities_convert_at_the_resolved_asset_precision(recorded, replay):
    block, _ = recorded
    base = resolve_market(replay(), selected(asset=USDC), block)
    collateral = resolve_market(replay(), selected(asset=WETH), block)
    authored = {"capital": "1.5"}
    assert public_allocation("earn", **authored).resolve("earn", base).capital == 1500000
    resolved = public_allocation("collateral", **authored).resolve("collateral", collateral)
    assert resolved.capital == 1500000000000000000


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_public_allocation_refuses_the_deleted_amount_fields(field):
    with pytest.raises(ValidationError, match="Extra inputs"):
        public_allocation("earn", **{field: "1"})


def test_omitted_capital_and_borrow_resolve_to_none(recorded, replay):
    block, _ = recorded
    base = resolve_market(replay(), selected(asset=USDC), block)
    raw = {
        "market": "earn",
        "account": "lender",
        "minimum_health_factor": "2",
        "minimum_native_balance": "0",
    }
    resolved = AllocationInput.model_validate_json(json.dumps(raw)).resolve("earn", base)
    assert (resolved.capital, resolved.borrow) == (0, False)


def test_recorded_reads_share_the_reference_anchor(recorded):
    """The reads were taken at the fork anchor the reference pinned before conversion."""
    block, _ = recorded
    frozen = yaml.safe_load((harness_directory(REFERENCE) / "config.yaml").read_text())
    assert (
        Block.model_validate_json(json.dumps(frozen["execution"]["fork"]["targets"][0]["block"]))
        == block
    )
    document = yaml.safe_load((ROOT / "strategies" / REFERENCE / "config.yaml").read_text())
    assert {market["comet"] for market in document["markets"].values()} == {COMET}


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))
