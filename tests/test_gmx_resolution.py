"""GMX V2 market resolution replays retained reads, without provider access."""

import gzip
import json
from pathlib import Path

import pytest
import yaml
from eth_abi import encode
from eth_utils import keccak
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter, ValidationError
from referencing import Registry, Resource

from almanak.caip import AssetType, ChainId
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.gmx_v2.author_inputs import (
    AllocationInput,
    LiquidityAllocationInput,
    PositionAllocationInput,
    SwapAllocationInput,
)
from almanak.connectors.gmx_v2.connector import candidate_support
from almanak.connectors.gmx_v2.deployments import compiled
from almanak.connectors.gmx_v2.market_request import (
    GmxMarketRequest,
    read_gmx_market,
    synthetic_index,
)
from almanak.connectors.gmx_v2.networks import roles_for
from almanak.connectors.gmx_v2.oracle_reader import key
from almanak.connectors.gmx_v2.resolution import (
    MarketAssertions,
    MarketSelection,
    ResolvedMarket,
    resolve_market,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import ZERO_ADDRESS, Block
from tests.gmx_retired import current
from tests.legacy_references import RETIRED_GMX_ROLES, current_gmx_roles
from tests.test_reference_conversion import reviewed_holds

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests/fixtures"
SCHEMAS = ROOT / "docs/schemas"
READS = FIXTURES / "gmx-v2-arbitrum-resolution-reads.json.gz"
LEGACY = FIXTURES / "author/legacy/gmx_v2.json.gz"
CHAIN = ChainId("eip155:42161")
GM = AssetType("eip155:42161/erc20:0x70d95587d40a2caf56bd97485ab3eec10bee6336")
WETH = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
USDC = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")
WBTC = AssetType("eip155:42161/erc20:0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f")
BTC_GM = AssetType("eip155:42161/erc20:0x47c031236e19d024b42f8ae6780e44a573170703")
BTC_INDEX = AssetType(f"{CHAIN}/erc20:{synthetic_index(CHAIN, 'BTC')}")
MARKET_SELECTOR = "0x" + keccak(text="getMarket(address,address)")[:4].hex()
ABSENT_MARKET = "0x" + encode(("(address,address,address,address)",), ((ZERO_ADDRESS,) * 4,)).hex()
ADAPTER: TypeAdapter = TypeAdapter(AllocationInput)


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
        "connector": "gmx_v2",
        "chain": str(chain or CHAIN),
        "market": str(GM),
        "assertions": None if assertions is None else assertions.model_dump(mode="json"),
        **overrides,
    }
    return MarketSelection.model_validate_json(json.dumps(raw))


def market_query(query: str) -> GmxMarketRequest:
    return GmxMarketRequest(chain=CHAIN, query=query)


def legacy_document() -> dict:
    return json.loads(gzip.decompress(LEGACY.read_bytes()))


def legacy_value(reference: str) -> dict:
    record = legacy_document()["references"][reference]["files"]["config.yaml"]
    return current(current_gmx_roles(record["value"]))


def is_market_call(row: ResolutionRead) -> bool:
    call = row.params[0] if row.method == "eth_call" and row.params else None
    return isinstance(call, dict) and str(call.get("data", "")).startswith(MARKET_SELECTOR)


def is_code_of(row: ResolutionRead, target: str) -> bool:
    return row.method == "eth_getCode" and row.params[0] == target


def is_call_to(row: ResolutionRead, target: str, selector: str) -> bool:
    call = row.params[0] if row.method == "eth_call" and row.params else None
    return (
        isinstance(call, dict)
        and call.get("to") == target
        and str(call.get("data", "")).startswith(selector)
    )


def answered(rows, matches, result):
    """Replay the same reads with one answer replaced; nothing here is a recorded fact."""
    return tuple(row.model_copy(update={"result": result}) if matches(row) else row for row in rows)


def test_recorded_reads_resolve_the_legacy_arbitrum_market(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    assert result.market.share_asset == GM
    assert (result.market.index_asset, result.market.long_asset) == (WETH, WETH)
    assert (result.market.short_asset, result.market.wrapped_native) == (USDC, WETH)
    assert result.market.contracts == roles_for(CHAIN)
    assert [asset.decimals for asset in result.retained] == [18, 18, 6, 18, 18]
    # Index, long and wrapped native are all WETH here, so the market names three assets.
    assert [str(asset.identifier) for asset in result.assets] == [str(WETH), str(USDC), str(GM)]
    assert result.block == block
    assert ResolvedMarket.model_validate_json(result.model_dump_json()) == result
    assert len(result.digest) == 64


def test_identities_cover_the_compiled_closure_and_the_market_token(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    addresses = [identity.address for identity in result.identities]
    assert len(addresses) == len(set(addresses))
    assert set(compiled()["chains"][str(CHAIN)]) <= set(addresses)
    assert GM.reference in addresses
    assert set(roles_for(CHAIN).model_dump().values()) <= set(addresses)


def test_summary_names_the_verified_identities_only(recorded, replay):
    block, _ = recorded
    summary = resolve_market(replay(), selected(), block).summary()
    assert set(summary) == {
        "selection",
        "share",
        "index",
        "long",
        "short",
        "wrapped_native",
        "decimals",
        "block",
    }
    assert summary["selection"] == selected().model_dump(mode="json")
    assert (summary["share"], summary["index"], summary["short"]) == (str(GM), str(WETH), str(USDC))
    assert (summary["decimals"], summary["block"]) == ([18, 18, 6, 18, 18], block.number)


def test_undeclared_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = ChainId("eip155:8453")
    assert foreign not in {entry.chain for entry in candidate_support()}
    chain = block.chain.model_copy(update={"chain_id": foreign})
    selection = selected(chain=foreign, market=f"{foreign}/erc20:0x" + "11" * 20)
    with pytest.raises(DataError, match="no declared support") as error:
        resolve_market(
            replay(evidence=(), chain=chain), selection, block.model_copy(update={"chain": chain})
        )
    assert error.value.kind == "unsupported_chain"


def test_foreign_anchor_chain_refuses_before_any_read(recorded, replay):
    block, _ = recorded
    foreign = block.model_copy(
        update={"chain": block.chain.model_copy(update={"environment": "x"})}
    )
    with pytest.raises(DataError, match="selected chain and anchor") as error:
        resolve_market(replay(evidence=()), selected(), foreign)
    assert error.value.kind == "chain_mismatch"


def test_uncanonical_anchor_refuses(recorded, replay):
    block, _ = recorded
    moved = block.model_copy(update={"hash": "0x" + "12" * 32})
    with pytest.raises(DataError, match="no longer canonical") as error:
        resolve_market(replay(), selected(), moved)
    assert error.value.kind == "block_mismatch"


def test_unlisted_deployment_chain_refuses_with_a_remedy():
    with pytest.raises(DataError, match="no compiled deployment on eip155:8453") as error:
        roles_for(ChainId("eip155:8453"))
    assert error.value.kind == "unsupported_chain"


def test_unregistered_market_token_refuses(recorded, replay):
    block, rows = recorded
    evidence = answered(rows, is_market_call, ABSENT_MARKET)
    with pytest.raises(DataError, match="not registered") as error:
        resolve_market(replay(evidence=evidence), selected(), block)
    assert error.value.kind == "unavailable"


def test_missing_retained_read_fails_closed(recorded, replay):
    block, rows = recorded
    reader = roles_for(CHAIN).reader
    kept = tuple(row for row in rows if not is_code_of(row, reader))
    assert 0 < len(kept) < len(rows)
    with pytest.raises(DataError, match="lacks a required read") as error:
        resolve_market(replay(evidence=kept), selected(), block)
    assert error.value.kind == "resolution_evidence"


def test_tampered_deployment_code_refuses(recorded, replay):
    block, rows = recorded
    store = roles_for(CHAIN).data_store
    evidence = answered(rows, lambda row: is_code_of(row, store), "0x60006000fd")
    with pytest.raises(DataError, match="compiled deployment closure") as error:
        resolve_market(replay(evidence=evidence), selected(), block)
    assert error.value.kind == "revision"


def role_read(row: ResolutionRead, store: str, holder: str, role: str) -> bool:
    selector = "0x" + keccak(text="hasRole(address,bytes32)")[:4].hex()
    arguments = encode(("address", "bytes32"), (holder, key(role))).hex()
    return is_call_to(row, store, selector + arguments)


def role_records():
    entries = compiled()["chains"][str(CHAIN)]
    return [
        (address, value["contract"], role)
        for address, value in entries.items()
        for role in value.get("roles", ())
    ]


def test_recorded_reads_hold_every_role_the_closure_records(recorded):
    _, rows = recorded
    store = next(
        a for a, v in compiled()["chains"][str(CHAIN)].items() if v["contract"] == "RoleStore"
    )
    held = "0x" + encode(("bool",), (True,)).hex()
    for address, _, role in role_records():
        (row,) = [row for row in rows if role_read(row, store, address, role)]
        assert row.result == held


@pytest.mark.parametrize("address,contract,role", role_records())
def test_revoked_role_refuses_the_retired_deployment(recorded, replay, address, contract, role):
    block, rows = recorded
    store = next(
        a for a, v in compiled()["chains"][str(CHAIN)].items() if v["contract"] == "RoleStore"
    )
    revoked = "0x" + encode(("bool",), (False,)).hex()
    evidence = answered(rows, lambda row: role_read(row, store, address, role), revoked)
    with pytest.raises(DataError, match=f"{contract} {address} no longer holds {role}") as error:
        resolve_market(replay(evidence=evidence), selected(), block)
    assert error.value.kind == "identity"


@pytest.mark.parametrize(
    "field", ["index", "long", "short", "index_decimals", "long_decimals", "short_decimals"]
)
def test_assertions_refuse_mismatch_without_overriding_facts(recorded, replay, field):
    block, _ = recorded
    expected = {
        "index": str(WETH),
        "long": str(WETH),
        "short": str(USDC),
        "index_decimals": 18,
        "long_decimals": 18,
        "short_decimals": 6,
    }
    accepted = MarketAssertions.model_validate_json(json.dumps(expected))
    result = resolve_market(replay(), selected(accepted), block)
    assert result.market.short_asset == USDC
    bad = {
        "index": str(USDC),
        "long": str(USDC),
        "short": str(WETH),
        "index_decimals": 6,
        "long_decimals": 6,
        "short_decimals": 18,
    }
    assertions = MarketAssertions.model_validate_json(json.dumps({field: bad[field]}))
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(replay(), selected(assertions), block)


@pytest.mark.parametrize("field", ["chain", "market", "assertions"])
def test_selection_refuses_invalid_identity_scope(field):
    bad = {
        "chain": "cosmos:osmosis-1",
        "market": "eip155:1/erc20:0x" + "12" * 20,
        "assertions": {"long": "eip155:1/erc20:0x" + "12" * 20},
    }
    raw = {**selected().model_dump(mode="json"), field: bad[field]}
    with pytest.raises(ValidationError):
        MarketSelection.model_validate_json(json.dumps(raw))


@pytest.mark.parametrize("field", ["selection", "block", "short", "identities"])
def test_retained_result_refuses_inconsistent_identity(recorded, replay, field):
    block, _ = recorded
    result = resolve_market(replay(), selected(), block)
    bad = {
        "selection": result.selection.model_copy(update={"market": WETH}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "short": result.short.model_copy(update={"identifier": WETH}),
        "identities": result.identities[:1],
    }
    with pytest.raises(ValidationError):
        ResolvedMarket.model_validate({**result.model_dump(), field: bad[field]})


def public_position(reference: str) -> dict:
    """The shipped public allocation of one converted reference."""
    raw = yaml.safe_load((ROOT / "strategies" / reference / "config.yaml").read_text())
    (position,) = raw["strategy"]["allocations"].values()
    return {**position, "connector": "gmx_v2"}


ARBITRUM_REFERENCES = (
    "gmx_v2_arbitrum_collateral_path",
    "gmx_v2_arbitrum_liquidity",
    "gmx_v2_arbitrum_position",
    "gmx_v2_arbitrum_position_path",
    "gmx_v2_arbitrum_swap",
)


@pytest.mark.parametrize("reference", ARBITRUM_REFERENCES)
def test_public_positions_resolve_to_the_legacy_allocations(recorded, replay, reference):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    (frozen,) = legacy_value(reference)["allocations"]
    value = ADAPTER.validate_json(json.dumps(public_position(reference)))
    resolved = value.resolve(frozen["name"], market).model_dump(mode="json")
    assert reviewed_holds(reference, {reference: resolved}) == reviewed_holds(
        reference, {reference: frozen}
    )


@pytest.mark.parametrize("field", ["capital", "entry_collateral", "execution_fee"])
def test_position_amounts_must_name_a_resolved_market_asset(recorded, replay, field):
    block, _ = recorded
    market = resolve_market(replay(), selected(), block)
    raw = public_position("gmx_v2_arbitrum_position")
    raw[field] = {"asset": "eip155:42161/erc20:0x" + "12" * 20, "quantity": {"base_units": 1}}
    with pytest.raises(ConfigurationError) as error:
        ADAPTER.validate_json(json.dumps(raw)).resolve("Main", market)
    assert error.value.field == f"{field}.asset"


RETIRED_BUDGETS = {
    "gmx_v2_arbitrum_position": (
        ("maximum_input", {"base_units": 1}),
        ("maximum_size_delta_usd_e30", 1),
        ("maximum_price_per_base_unit", 1),
        ("maximum_collateral_withdrawal_base_units", 0),
    ),
    "gmx_v2_arbitrum_swap": (
        ("maximum_inputs", []),
        ("maximum_market_token_input", {"base_units": 0}),
    ),
    "gmx_v2_arbitrum_liquidity": (
        ("maximum_inputs", []),
        ("maximum_market_token_input", {"base_units": 1}),
    ),
}


@pytest.mark.parametrize(
    "reference,field,value",
    [(name, field, value) for name, fields in RETIRED_BUDGETS.items() for field, value in fields],
)
def test_retired_budget_fields_are_refused_as_unknown(reference, field, value):
    """Grants are scope-only (blueprint 27): no author field sizes Roles authority."""
    raw = public_position(reference)
    ADAPTER.validate_json(json.dumps(raw))
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ADAPTER.validate_json(json.dumps(raw | {field: value}))


def test_recorded_reads_carry_no_provider_credentials():
    raw = gzip.decompress(READS.read_bytes()).decode().lower()
    assert not any(token in raw for token in ("alchemy", "api_key", "http"))


def validator(name):
    registry = Registry().with_resources(
        (path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
        for path in SCHEMAS.rglob("*.json")
    )
    return Draft202012Validator({"$ref": (SCHEMAS / name).as_uri()}, registry=registry)


MARKET_SCHEMA = "markets/gmx_v2.schema.json"
POSITION_SCHEMA = "positions/gmx_v2.schema.json"
KINDS = {
    "Position": PositionAllocationInput,
    "Swap": SwapAllocationInput,
    "Liquidity": LiquidityAllocationInput,
}


def required_fields(model):
    return {name for name, field in model.model_fields.items() if field.is_required()}


def schema_document(name):
    return json.loads((SCHEMAS / name).read_text())


def test_editor_schemas_and_models_declare_the_same_author_fields():
    market = schema_document(MARKET_SCHEMA)
    assert set(market["properties"]) == set(MarketSelection.model_fields)
    assert set(market["required"]) == required_fields(MarketSelection)
    position = schema_document(POSITION_SCHEMA)
    assert [branch["$ref"] for branch in position["oneOf"]] == [f"#/$defs/{name}" for name in KINDS]
    for name, model in KINDS.items():
        branch = position["$defs"][name]
        # The parser injects the connector tag from the named market; authors never write it.
        assert set(branch["properties"]) == set(model.model_fields) - {"connector"}
        assert set(branch["required"]) == required_fields(model)


@pytest.mark.parametrize("reference", ARBITRUM_REFERENCES)
def test_editor_schemas_accept_the_shipped_markets_and_positions(reference):
    raw = yaml.safe_load((ROOT / "strategies" / reference / "config.yaml").read_text())
    for market in raw["markets"].values():
        validator(MARKET_SCHEMA).validate(market)
    for position in raw["strategy"]["allocations"].values():
        validator(POSITION_SCHEMA).validate(position)
    validator(MARKET_SCHEMA).validate(
        {
            "connector": "gmx_v2",
            "chain": str(CHAIN),
            "market": str(GM),
            "assertions": {"index": str(WETH), "short": str(USDC), "short_decimals": 6},
        }
    )


@pytest.mark.parametrize(
    "mutation", ["harness_market", "marketless", "float_decimals", "unknown_assertion"]
)
def test_market_schema_refuses_author_contract_errors(mutation):
    raw = selected().model_dump(mode="json")
    if mutation == "harness_market":
        raw["contracts"] = {"reader": "0x" + "12" * 20}
    elif mutation == "marketless":
        del raw["market"]
    elif mutation == "float_decimals":
        raw["assertions"] = {"short_decimals": 6.5}
    else:
        raw["assertions"] = {"share_decimals": 18}
    assert not validator(MARKET_SCHEMA).is_valid(raw)


@pytest.mark.parametrize(
    "mutation", ["kindless", "harness_key", "unquoted", "spent_amount", "swap_fields"]
)
def test_position_schema_refuses_author_contract_errors(mutation):
    raw = public_position("gmx_v2_arbitrum_position")
    del raw["connector"]
    if mutation == "kindless":
        del raw["kind"]
    elif mutation == "harness_key":
        raw["native_asset"] = "eip155:42161/slip44:60"
    elif mutation == "unquoted":
        raw["capital"]["quantity"] = 0.5
    elif mutation == "spent_amount":
        raw["capital"] = {"base_units": 500000000000000000}
    else:
        raw["entry_amount"] = {"asset": str(WETH), "quantity": "0.1"}
    assert not validator(POSITION_SCHEMA).is_valid(raw)


def test_every_supported_chain_has_exactly_one_reviewed_deployment():
    assert {ChainId(chain) for chain in compiled()["chains"]} == {
        entry.chain for entry in candidate_support()
    }
    assert all(roles_for(ChainId(chain)) for chain in compiled()["chains"])


@pytest.mark.parametrize("chain", sorted(compiled()["chains"]))
def test_closure_records_the_roles_that_authorize_requests(chain):
    """Linked libraries and vaults hold no role; losing a recorded one retires the closure."""
    holders = {
        value["contract"]: tuple(value["roles"])
        for value in compiled()["chains"][chain].values()
        if "roles" in value
    }
    controllers = (
        "Oracle OrderHandler DepositHandler WithdrawalHandler LiquidationHandler MarketFactory "
        "IncreaseOrderExecutor DecreaseOrderExecutor SwapOrderExecutor SwapHandler"
    ).split()
    assert holders == {
        "ExchangeRouter": ("CONTROLLER", "ROUTER_PLUGIN"),
        **dict.fromkeys(controllers, ("CONTROLLER",)),
    }


def frozen_references():
    return sorted(legacy_document()["references"].items())


@pytest.mark.parametrize("name,entry", frozen_references())
def test_shipped_references_name_the_reviewed_deployment(name, entry):
    """Each frozen reference names the retired deployment the compiled closure succeeds."""
    document = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    shares = {market["market"] for market in document["markets"].values()}
    record = entry["files"]["config.yaml"]
    assert record["status"] == "normalized", record.get("message")
    for frozen, allocation in zip(
        record["value"]["allocations"],
        current_gmx_roles(record["value"])["allocations"],
        strict=True,
    ):
        market = allocation["market"]
        chain = market["chain"]["chain_id"]
        roles = roles_for(ChainId(chain)).model_dump(mode="json")
        assert market["share_asset"] in shares
        assert market["contracts"] == {key: roles[key] for key in market["contracts"]}
        retired = set(frozen["market"]["contracts"].values()) - set(market["contracts"].values())
        assert retired and retired <= set(RETIRED_GMX_ROLES[chain])
        assert not retired & set(compiled()["chains"][chain])


def test_recorded_reads_resolve_the_synthetic_btc_market(recorded, replay):
    block, _ = recorded
    result = resolve_market(replay(), selected(market=str(BTC_GM)), block)
    assert result.market.share_asset == BTC_GM
    assert (result.market.index_asset, result.market.long_asset) == (BTC_INDEX, WBTC)
    assert (result.market.short_asset, result.market.wrapped_native) == (USDC, WETH)
    # The synthetic index has no contract; its 8-decimal unit is the venue's stream precision.
    assert [asset.decimals for asset in result.retained] == [8, 8, 6, 18, 18]
    assert len(result.assets) == 5
    assert ResolvedMarket.model_validate_json(result.model_dump_json()) == result


def test_synthetic_market_discovery_replays_by_address_and_by_label(recorded, replay):
    block, _ = recorded

    def reader():
        # The ax command pins the block through its reader before reading a market.
        result = replay()
        assert result.pin_block(block.number) == block
        return result

    by_address = read_gmx_market(reader(), market_query(BTC_GM.reference), block)
    observation = by_address["observation"]
    assert (observation["share"], observation["index"]) == (str(BTC_GM), str(BTC_INDEX))
    assert (observation["index_symbol"], observation["index_kind"]) == ("BTC", "synthetic")
    assert observation["decimals"] == [8, 8, 6, 18, 18]
    by_label = read_gmx_market(reader(), market_query("BTC/USD [WBTC-USDC]"), block)
    assert by_label["observation"] == observation
    with pytest.raises(DataError, match="ambiguous") as error:
        read_gmx_market(reader(), market_query("BTC/USD"), block)
    assert error.value.kind == "ambiguous"
    # Three markets shared the synthetic BTC index at the recorded anchor.
    assert f"{BTC_GM.reference} [WBTC-USDC]" in str(error.value)
    assert str(error.value).count("[") == 3


def test_synthetic_index_without_stream_precision_refuses(recorded, replay):
    block, rows = recorded
    store = roles_for(CHAIN).data_store
    selector = "0x" + keccak(text="getUint(bytes32)")[:4].hex()
    multiplier = key("DATA_STREAM_MULTIPLIER", ("address",), (BTC_INDEX.reference,))
    is_precision = lambda row: is_call_to(row, store, selector + multiplier.hex())  # noqa: E731
    assert any(is_precision(row) for row in rows)
    evidence = answered(rows, is_precision, "0x" + encode(("uint256",), (3 * 10**33,)).hex())
    with pytest.raises(DataError, match="no data stream precision") as error:
        resolve_market(replay(evidence=evidence), selected(market=str(BTC_GM)), block)
    assert error.value.kind == "unavailable"


def test_deployed_index_metadata_failure_stays_an_abi_error(recorded, replay):
    block, rows = recorded
    selector = "0x" + keccak(text="decimals()")[:4].hex()
    evidence = answered(rows, lambda row: is_call_to(row, WETH.reference, selector), "0x")
    with pytest.raises(DataError, match="decimals\\(\\): invalid ABI response") as error:
        resolve_market(replay(evidence=evidence), selected(), block)
    assert error.value.kind == "abi"
