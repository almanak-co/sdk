"""Published vectors: CAIP-2/19/20 and CASA EIP155/BIP122/Cosmos/Solana profiles."""

import copy
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.caip import AssetId, AssetType, ChainId
from almanak.connectors.aave_v4.models import LendingConfig
from almanak.connectors.aave_v4.reader import plan_call, read_reserve
from almanak.connectors.aave_v4.state import Checkpoint, Outcome
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.evm import chain_number, currency_address, erc20_address
from almanak.models import Asset, AssetAmount, Chain
from almanak.runtime import report
from almanak.state import Journal, Lease


@pytest.mark.parametrize(
    "wire",
    [
        "eip155:1",
        "eip155:28945486",
        "bip122:000000000019d6689c085ae165831e93",
        "bip122:12a765e31ffd4059bada1e25190f6e98",
        "cosmos:cosmoshub-3",
        "cosmos:Binance-Chain-Tigris",
        "cosmos:hashed-c904589232422def",
        "solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ",
        "lip9:9ee11e9df416b18b",
        "hedera:mainnet",
        "future:Case_Sensitive-1",
    ],
)
def test_chain_vectors_and_wire_round_trip(wire):
    adapter = TypeAdapter(ChainId)
    parsed = adapter.validate_json(json.dumps(wire))
    assert parsed == ChainId(wire)
    assert str(parsed) == wire
    assert adapter.dump_json(parsed) == json.dumps(wire).encode()
    assert adapter.validate_python(parsed) is parsed
    assert not isinstance(parsed, str) and parsed != wire
    assert copy.deepcopy(parsed) == parsed
    with pytest.raises(AttributeError):
        parsed.value = "eip155:10"


@pytest.mark.parametrize(
    "value",
    [
        1,
        True,
        None,
        b"eip155:1",
        "ethereum",
        "ETH",
        "1",
        "EIP155:1",
        "ev:1",
        "longchain:1",
        "eip155:",
        "eip155:01",
        "eip155:-1",
        "eip155:0x1",
        "eip155:1\n",
        "eip155:1/erc20:1",
        "future:" + "a" * 33,
        "bip122:" + "F" * 32,
        "bip122:" + "a" * 31,
        "cosmos:chain_name",
        "cosmos:hashed-123",
        "solana:" + "0" * 32,
    ],
)
def test_invalid_chain_syntax_and_profiles(value):
    with pytest.raises(ValueError):
        ChainId(value)


@pytest.mark.parametrize(
    "wire",
    [
        "eip155:1/slip44:60",
        "bip122:000000000019d6689c085ae165831e93/slip44:0",
        "cosmos:cosmoshub-3/slip44:118",
        "cosmos:Binance-Chain-Tigris/slip44:714",
        "lip9:9ee11e9df416b18b/slip44:134",
        "eip155:1/erc20:0x6b175474e89094c44da98b954eedeac495271d0f",
        "hedera:mainnet/nft:0.0.55492",
        "future:Case_Sensitive-1/token:AbC%2Fdef.9",
    ],
)
def test_asset_type_vectors_and_wire_round_trip(wire):
    value = TypeAdapter(AssetType).validate_json(json.dumps(wire))
    assert str(value) == wire
    assert value.chain == ChainId(wire.split("/")[0])
    assert TypeAdapter(AssetType).dump_json(value) == json.dumps(wire).encode()
    assert value != wire


@pytest.mark.parametrize(
    "wire",
    [
        "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769",
        "hedera:mainnet/nft:0.0.55492/12",
        "future:Case_Sensitive-1/token:AbC%2Fdef.9/Id%2F9",
    ],
)
def test_individual_asset_is_distinct_from_its_type(wire):
    value = TypeAdapter(AssetId).validate_json(json.dumps(wire))
    assert value.asset_type == AssetType(wire.rsplit("/", 1)[0])
    assert value.token_id == wire.rsplit("/", 1)[1]
    assert value != value.asset_type
    assert TypeAdapter(AssetId).validate_json(TypeAdapter(AssetId).dump_json(value)) == value
    with pytest.raises(ValueError):
        AssetType(wire)


@pytest.mark.parametrize(
    "kind,wire",
    [
        (AssetType, "WETH"),
        (AssetType, "0x" + "11" * 20),
        (AssetType, "eip155:1/ERC20:0x" + "11" * 20),
        (AssetType, "eip155:1/erc20:0x123"),
        (AssetType, "eip155:1/slip44:ETH"),
        (AssetType, "eip155:1/slip44:-1"),
        (AssetType, "eip155:1/slip44:060"),
        (AssetType, "future:1/token:" + "x" * 129),
        (AssetId, "hedera:mainnet/nft:0.0.55492"),
        (AssetId, "hedera:mainnet/nft:0.0.55492/" + "x" * 79),
        (AssetId, "eip155:1/slip44:60/0"),
        (AssetId, "eip155:1/erc20:0x" + "11" * 20 + "/0"),
        (AssetId, "eip155:1/erc721:0x" + "11" * 20 + "/-1"),
        (AssetId, "eip155:1/erc721:0x" + "11" * 20 + "/01"),
        (AssetId, "eip155:1/erc721:0x" + "11" * 20 + "/" + str(2**256)),
    ],
)
def test_invalid_asset_syntax_and_namespace_semantics(kind, wire):
    with pytest.raises(ValueError):
        kind(wire)


def test_case_policy_is_namespace_specific_and_metadata_is_not_identity():
    lower = AssetType("eip155:1/erc20:0x6b175474e89094c44da98b954eedeac495271d0f")
    checksum = AssetType("eip155:1/erc20:0x6B175474E89094C44Da98b954EedeAC495271d0F")
    assert lower == checksum and hash(lower) == hash(checksum)
    assert AssetType("future:1/token:AbC") != AssetType("future:1/token:abc")
    assert ChainId("cosmos:Binance-Chain-Tigris") != ChainId("cosmos:binance-chain-tigris")
    assert AssetId("future:1/token:AbC/ID") != AssetId("future:1/token:AbC/id")
    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    first = Asset(chain=chain, identifier=lower, decimals=18)
    second = Asset(chain=chain, identifier=checksum, decimals=6)
    assert first.identifier == second.identifier
    assert first != second


def test_generic_grammar_boundaries_and_uint256_token_id():
    ChainId("abcdefgh:" + "a" * 32)
    AssetType("future:1/token:" + "x" * 128)
    AssetId("future:1/token:x/" + "x" * 78)
    value = AssetId("eip155:1/erc721:0x" + "11" * 20 + "/" + str(2**256 - 1))
    assert value.token_id == str(2**256 - 1)


@pytest.mark.parametrize(
    "kind,wire",
    [
        (ChainId, "eip155:1"),
        (AssetType, "eip155:1/slip44:60"),
        (AssetId, "hedera:mainnet/nft:0.0.55492/12"),
    ],
)
def test_python_inputs_require_dedicated_types_and_schemas_describe_strings(kind, wire):
    adapter = TypeAdapter(kind)
    for value in (wire, 1, {"value": wire}):
        with pytest.raises(ValidationError):
            adapter.validate_python(value)
    assert adapter.json_schema()["type"] == "string"
    assert "pattern" in adapter.json_schema()


def test_nested_models_preserve_types_in_python_and_strings_on_wire(recorded):
    python = recorded.model_dump()
    wire = recorded.model_dump(mode="json")
    assert isinstance(python["config"]["fork_origin"]["chain"]["chain_id"], ChainId)
    assert wire["config"]["fork_origin"]["chain"]["chain_id"] == "eip155:1"
    assert Checkpoint.model_validate(python) == recorded
    assert Checkpoint.model_validate_json(recorded.model_dump_json()) == recorded
    with pytest.raises(ValidationError):
        Checkpoint.model_validate(wire)


def test_individual_asset_metadata_round_trip_preserves_case_and_chain():
    identity = AssetId("hedera:mainnet/nft:0.0.55492/12")
    asset = Asset(
        chain=Chain(chain_id=ChainId("hedera:mainnet"), environment="mainnet"),
        identifier=identity,
        decimals=0,
    )
    assert Asset.model_validate_json(asset.model_dump_json()) == asset
    raw = asset.model_dump()
    raw["chain"] = Chain(chain_id=ChainId("hedera:testnet"), environment="testnet")
    with pytest.raises(ValidationError, match="different chains"):
        Asset.model_validate(raw)


def test_non_evm_chain_identity_does_not_authorize_evm_reader():
    chain = Chain(chain_id=ChainId("cosmos:cosmoshub-3"), environment="mainnet")
    with pytest.raises(ValueError, match="EIP-155"):
        EthereumReader(chain, "http://127.0.0.1:1")


@pytest.mark.parametrize(
    "asset",
    [
        "eip155:10/erc20:0x" + "11" * 20,
        "eip155:1/erc721:0x" + "11" * 20,
        "cosmos:cosmoshub-3/slip44:118",
        "eip155:1/erc20:0x" + "00" * 20,
    ],
)
def test_erc20_boundary_rejects_other_chain_namespace_and_sentinels(asset):
    with pytest.raises(ValueError):
        erc20_address(AssetType(asset), ChainId("eip155:1"))


def test_native_and_erc20_protocol_currency_ordering(price_request, rpc_factory):
    raw = price_request.source.model_dump()
    raw["currency0"] = AssetType("eip155:1/slip44:60")
    source = type(price_request.source).model_validate(raw)
    assert currency_address(source.currency0, source.chain.chain_id) == "0x" + "00" * 20
    assert pool_id(source) != pool_id(price_request.source)
    reader, calls = rpc_factory()
    block = reader.pin_block(25908472)
    native = reader.asset(source.currency0, block)
    assert native.identifier == source.currency0 and native.decimals == 18
    assert len(calls) == 2
    with pytest.raises(ValueError):
        reader.asset("0x" + "00" * 20, block)


def test_aave_adapter_rechecks_identity_before_io_and_encoding(
    reserve_source, rpc_factory, recorded
):
    reader, requests = rpc_factory()
    block = reader.pin_block(25908472)
    forged = reserve_source.model_copy(update={"asset": AssetType("eip155:1/slip44:60")})
    with pytest.raises(ValueError, match="ERC-20"):
        read_reserve(reader, forged, block)
    assert len(requests) == 2
    allocation = recorded.config.allocations[0]
    wrong = allocation.model_copy(
        update={
            "reserve": allocation.reserve.model_copy(
                update={"asset": AssetType("eip155:31337/erc721:0x" + "11" * 20)}
            )
        }
    )
    fund = recorded.config.accounts.binding(allocation.account).profile.fund
    operation = recorded.outcomes[1].operation
    operation = operation.model_copy(
        update={"target": operation.target.model_copy(update={"reserve": wrong.reserve})}
    )
    with pytest.raises(ValueError, match="ERC-20"):
        plan_call(wrong, fund, operation)


def test_open_reports_and_observations_attribute_native_gas_and_asset_totals(recorded):
    state = recorded.model_copy(update={"mode": "running", "closure": (), "outcomes": ()})
    result = report(state)["positions"][0]
    fund = report(state)["funds"][0]
    allocation = state.config.allocations[0]
    assert result["supplied"] == {"asset": str(allocation.reserve.asset), "amount": "0"}
    assert result["withdrawn"]["asset"] == str(allocation.reserve.asset)
    assert fund["gas_paid"]["asset"] == str(allocation.native_asset)
    assert fund["gas_paid"]["amount"] == "0"
    assert fund["gas_paid"]["payer"] == fund["fund"]
    assert result["realized_token_delta"] is None
    assert recorded.baseline[0].native_balance.asset == allocation.native_asset
    assert sum(o.gas_paid.amount for o in recorded.outcomes) == (
        recorded.baseline[0].native_balance.amount - recorded.closure[0].native_balance.amount
    )
    raw = recorded.outcomes[0].model_dump()
    raw["gas_paid"] = AssetAmount(asset=allocation.reserve.asset, amount=0)
    with pytest.raises(ValidationError, match="asset scope"):
        Outcome.model_validate(raw)


def test_fork_environment_separates_authority_without_changing_caip_identity(recorded):
    first = recorded.config.accounts.bindings[0].profile.fund
    second = first.model_copy(
        update={"chain": first.chain.model_copy(update={"environment": "other"})}
    )
    assert first.chain.chain_id == second.chain.chain_id
    assert first != second
    assert recorded.config.fork_origin.chain.chain_id == ChainId("eip155:1")
    assert first.chain.chain_id == ChainId("eip155:31337")


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_audit_preserves_raw_signed_evidence_and_refuses_mutation(
    tmp_path, version, monkeypatch, capsys
):
    raw = json.loads((Path(__file__).parent / "fixtures/aave-v4-fork-lifecycle.json").read_text())
    raw["schema_version"] = version
    raw["pending"] = {"raw_transaction": "0x1234", "tx_hash": "historical-unvalidated"}
    body = json.dumps(raw)
    journal = Journal(tmp_path / "legacy.sqlite")
    with sqlite3.connect(journal.path) as db:
        db.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT)")
        db.execute("INSERT INTO checkpoint VALUES (1, ?)", (body,))
    assert journal.audit() == {
        "audit_only": True,
        "caip_compliance": False,
        "legacy_checkpoint": raw,
    }
    from almanak.cli import main

    monkeypatch.setattr(
        "sys.argv", ["almanak", "service", "status", "--database", str(journal.path)]
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out) == journal.audit()
    with pytest.raises(ValueError, match="audit-only"):
        journal.load()
    with pytest.raises(ValueError, match="audit-only"):
        journal.teardown()
    with journal.connect() as db:
        assert db.execute("SELECT body FROM checkpoint").fetchone()[0] == body


def test_legacy_and_current_claims_fence_both_directions(
    tmp_path, monkeypatch, recorded, reference_strategy
):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    claim_root = tmp_path / ".local/state/almanak-v3/claims"
    claim_root.mkdir(parents=True)
    fund = recorded.config.accounts.bindings[0].profile.fund
    # Construct independently using the exact previous SDK field order/separators.
    old_key = json.dumps(
        {
            "chain": {"chain_id": 31337, "environment": fund.chain.environment},
            "address": fund.address,
        },
        separators=(",", ":"),
    )
    old_path = claim_root / (hashlib.sha256(old_key.encode()).hexdigest() + ".claim")
    old_path.write_text('{"database":"legacy.sqlite","deployment_id":"legacy"}')
    journal = Journal(tmp_path / "new.sqlite")
    journal.create(recorded.config, reference_strategy)
    with pytest.raises(RuntimeError, match="already claimed"):
        Lease(journal, journal.load())
    assert "legacy.sqlite" in old_path.read_text()
    # This is isolated synthetic test storage. Existing real claims are never removed.
    old_path.unlink()
    lease = Lease(journal, journal.load())
    lease.close()
    expected_owner = json.dumps(
        {"deployment_id": journal.load().deployment_id, "database": str(journal.path)},
        sort_keys=True,
    )
    assert old_path.read_text() == expected_owner
    modern_path = claim_root / (
        hashlib.sha256(fund.model_dump_json().encode()).hexdigest() + ".claim"
    )
    assert modern_path.read_text() == expected_owner
    old_competing_owner = json.dumps(
        {"deployment_id": "old-sdk-new-deployment", "database": "other.sqlite"}, sort_keys=True
    )
    # The old Lease refusal is exactly this owner inequality after flock succeeds.
    assert old_path.read_text() != old_competing_owner


def test_configuration_rejects_numeric_chain_and_bare_asset_wire_forms(recorded):
    for chain_value, asset_value in ((31337, None), (None, "WETH"), (None, "0x" + "11" * 20)):
        raw = recorded.config.model_dump(mode="json")
        source = raw["allocations"][0]["reserve"]
        if chain_value is not None:
            source["chain"]["chain_id"] = chain_value
        if asset_value is not None:
            source["asset"] = asset_value
        with pytest.raises(ValidationError):
            LendingConfig.model_validate_json(json.dumps(raw))
    assert chain_number(ChainId("eip155:31337")) == 31337
