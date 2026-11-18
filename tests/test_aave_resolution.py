"""Aave resolution replays retained archive responses, without provider access."""

import gzip
import json
from collections import Counter
from pathlib import Path

import httpx
import pytest
from eth_abi import decode, encode
from eth_utils import keccak
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.resolution import (
    MarketSelection,
    ReserveAssertions,
    ResolvedReserve,
    resolve_market,
)
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block

FIXTURES = Path(__file__).with_name("fixtures")


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


@pytest.fixture(scope="module")
def archive():
    raw = json.loads(gzip.decompress((FIXTURES / "aave-v4-economic-archive.json.gz").read_bytes()))
    rows = [
        *raw["evidence"],
        *json.loads((FIXTURES / "aave-v4-economic-authority-reads.json").read_text()),
    ]
    source = AaveReserve.model_validate_json(json.dumps(raw["config"]["allocations"][0]["reserve"]))
    data = selector("getReserve(uint256)") + f"{source.reserve_id:064x}"
    reserve = next(
        r
        for r in rows
        if r["request"]["method"] == "eth_call"
        and r["request"]["params"][0] == {"to": source.spoke, "data": data}
    )
    identity = reserve["request"]["params"][1]["blockHash"]
    header = next(
        r["result"]
        for r in rows
        if r["request"]["method"] == "eth_getBlockByNumber" and r["result"]["hash"] == identity
    )
    block = Block(
        chain=source.chain,
        number=int(header["number"], 16),
        hash=identity,
        timestamp=int(header["timestamp"], 16),
    )
    return source, block, rows


def selected(source, assertions=None):
    return MarketSelection(
        connector="aave_v4",
        chain=source.chain.chain_id,
        spoke=source.spoke,
        reserve_id=source.reserve_id,
        assertions=assertions,
    )


@pytest.fixture
def replay(archive):
    source, block, rows = archive
    responses = {
        json.dumps([r["request"]["method"], r["request"]["params"]], sort_keys=True): r["result"]
        for r in rows
    }
    clients = []

    def create(mutation=None):
        calls = []

        def dispatch(request):
            body = json.loads(request.content)
            calls.append(body)
            key = json.dumps([body["method"], body["params"]], sort_keys=True)
            if key not in responses:
                raise AssertionError(f"resolution requested an uncaptured read: {key}")
            value = responses[key]
            value = mutation(body, value) if mutation else value
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": value})

        client = httpx.Client(transport=httpx.MockTransport(dispatch))
        clients.append(client)
        reader = EthereumReader(source.chain, "https://archive.invalid", client=client)
        return reader, calls

    yield create
    for client in clients:
        client.close()


def test_minimal_selection_resolves_exact_captured_reserve(archive, replay):
    source, block, _ = archive
    reader, calls = replay()
    result = resolve_market(reader, selected(source), block)
    assert result.reserve == source
    assert result.asset.identifier == source.asset
    assert result.asset.decimals == 18
    assert result.hub_asset_id == 0
    assert result.block == block
    assert all(c.implementation_hash is not None for c in result.contracts)
    assert ResolvedReserve.model_validate_json(result.model_dump_json()) == result
    assert result.assets == (result.asset,)
    summary = result.summary()
    assert summary["selection"] == selected(source).model_dump(mode="json")
    assert (summary["hub"], summary["decimals"], summary["block"]) == (source.hub, 18, block.number)
    assert set(summary) == {"selection", "hub", "asset", "decimals", "hub_asset_id", "block"}
    counts = Counter(
        call["params"][0]["data"][:10] for call in calls if call["method"] == "eth_call"
    )
    assert counts[selector("getReserve(uint256)")] == 1
    assert counts[selector("decimals()")] == 1
    assert {call["method"] for call in calls} == {
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_call",
    }


@pytest.mark.parametrize("field", ["asset", "hub", "decimals"])
def test_assertions_refuse_mismatch_without_overriding_facts(archive, replay, field):
    source, block, _ = archive
    expected = {"asset": source.asset, "hub": source.hub, "decimals": 18}
    reader, _ = replay()
    assert (
        resolve_market(reader, selected(source, ReserveAssertions(**expected)), block).reserve
        == source
    )
    bad = {
        "asset": AssetType(f"{source.chain.chain_id}/erc20:0x" + "12" * 20),
        "hub": "0x" + "12" * 20,
        "decimals": 6,
    }
    reader, _ = replay()
    with pytest.raises(ValueError, match=f"assertion {field}"):
        resolve_market(reader, selected(source, ReserveAssertions(**{field: bad[field]})), block)


@pytest.mark.parametrize("fault", ["chain", "block", "unsupported_chain"])
def test_scope_mismatch_refuses_before_provider_reads(archive, replay, fault):
    source, block, _ = archive
    selection = selected(source)
    reader, calls = replay()
    if fault == "block":
        block = block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        )
    else:
        selection = selection.model_copy(update={"chain": ChainId("eip155:56")})
        if fault == "unsupported_chain":
            reader.chain = block.chain.model_copy(update={"chain_id": selection.chain})
            block = block.model_copy(update={"chain": reader.chain})
    with pytest.raises(DataError):
        resolve_market(reader, selection, block)
    assert calls == []


@pytest.mark.parametrize("when", [1, 2])
def test_canonical_anchor_must_survive_resolution(archive, replay, when):
    source, block, _ = archive
    reads = 0

    def changed(body, value):
        nonlocal reads
        if body["method"] == "eth_getBlockByNumber":
            reads += 1
            if reads == when:
                return {**value, "hash": "0x" + "12" * 32}
        return value

    reader, _ = replay(changed)
    with pytest.raises(DataError, match="no longer canonical"):
        resolve_market(reader, selected(source), block)


@pytest.mark.parametrize("fault", ["implementation", "hub_asset", "hub_decimals", "decimals"])
def test_revision_and_metadata_mismatch_fail_closed(archive, replay, fault):
    source, block, _ = archive

    def changed(body, value):
        if fault == "implementation" and body["method"] == "eth_getCode":
            if body["params"][0] not in (source.spoke, source.hub):
                return "0x00"
        if body["method"] != "eth_call":
            return value
        data = body["params"][0]["data"]
        if fault == "decimals" and data == selector("decimals()"):
            return "0x" + encode(("uint8",), (6,)).hex()
        if fault.startswith("hub_") and data.startswith(
            selector("getAssetUnderlyingAndDecimals(uint256)")
        ):
            token, decimals = decode(("address", "uint8"), bytes.fromhex(value[2:]))
            return (
                "0x"
                + encode(
                    ("address", "uint8"),
                    (
                        "0x" + "12" * 20 if fault == "hub_asset" else token,
                        6 if fault == "hub_decimals" else decimals,
                    ),
                ).hex()
            )
        return value

    reader, calls = replay(changed)
    with pytest.raises(DataError):
        resolve_market(reader, selected(source), block)
    if fault == "implementation":
        assert not any(call["method"] == "eth_call" for call in calls)


@pytest.mark.parametrize("value", [-1, 2**256, True])
def test_unresolved_selection_refuses_invalid_reserve_ids(archive, value):
    source, _, _ = archive
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected(source).model_dump(), "reserve_id": value})


@pytest.mark.parametrize("field", ["spoke", "chain", "assertions"])
def test_selection_refuses_invalid_identity_scope(archive, field):
    source, _, _ = archive
    bad = {
        "spoke": "0x" + "00" * 20,
        "chain": ChainId("cosmos:osmosis-1"),
        "assertions": ReserveAssertions(asset=AssetType("eip155:56/erc20:0x" + "12" * 20)),
    }
    with pytest.raises(ValidationError):
        MarketSelection.model_validate({**selected(source).model_dump(), field: bad[field]})


@pytest.mark.parametrize("field", ["selection", "block", "asset", "contracts", "hash"])
def test_retained_result_refuses_inconsistent_identity(archive, replay, field):
    source, block, _ = archive
    reader, _ = replay()
    result = resolve_market(reader, selected(source), block)
    forged = result.contracts[0].model_copy(update={"implementation_hash": "0x" + "ab" * 32})
    bad = {
        "hash": (forged, result.contracts[1]),
        "selection": result.selection.model_copy(update={"reserve_id": 12}),
        "block": block.model_copy(
            update={"chain": block.chain.model_copy(update={"environment": "x"})}
        ),
        "asset": result.asset.model_copy(
            update={"identifier": AssetType(f"{source.chain.chain_id}/erc20:0x" + "12" * 20)}
        ),
        "contracts": tuple(reversed(result.contracts)),
    }
    with pytest.raises(ValidationError):
        ResolvedReserve.model_validate({**result.model_dump(), field: bad[field]})
    if field == "hash":
        # A recomputed outer digest cannot launder a changed revision through reload.
        raw = json.loads(result.model_dump_json())
        raw["contracts"][0]["implementation_hash"] = forged.implementation_hash
        with pytest.raises(ValidationError):
            ResolvedReserve.model_validate_json(json.dumps(raw))
