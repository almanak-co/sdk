"""Replay pinned Pancake reads and reject changed code, constructor roles and market identity."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.pancakeswap_v3.deployments import revisions, verify_code
from almanak.connectors.pancakeswap_v3.domain import (
    Contracts,
    PancakePool,
    PoolObservation,
    PoolPriceRequest,
)
from almanak.connectors.pancakeswap_v3.reader import quote, read_pool
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import AssetAmount

ROOT = Path(__file__).parents[1]
CAPTURE = json.loads(
    gzip.decompress((ROOT / "tests/fixtures/pancakeswap-v3-bnb-reader.json.gz").read_bytes())
)


class Replay(EthereumReader):
    def __init__(self, row):
        self.source = PancakePool.model_validate_json(json.dumps(row["source"]))
        super().__init__(self.source.chain, "http://unused.invalid")
        self.records = {
            json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
            for r in row["requests"]
        }
        self.seen = set()

    def _rpc(self, method, params, *, allow_none=False):
        key = json.dumps([method, params], sort_keys=True)
        assert key in self.records, "test cannot obtain unrecorded provider data"
        self.seen.add(key)
        return self.records[key]


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: str(r["source"]["fee"]))
def test_deployed_pool_and_exact_quoter_reads_keep_requested_identity(row):
    with Replay(row) as reader:
        source = reader.source
        contracts = Contracts.model_validate_json(json.dumps(row["contracts"]))
        block = reader.pin_block(row["block"]["number"])
        if "refused" in row:
            with pytest.raises(DataError, match="callback revision is unverified"):
                read_pool(reader, source, block)
            assert reader.seen == set(reader.records)
            return
        observed = read_pool(reader, source, block)
        assert observed == PoolObservation.model_validate_json(json.dumps(row["observed"]))
        request = PoolPriceRequest(source=source, base=source.currency0, quote=source.currency1)
        reverse = PoolPriceRequest(source=source, base=source.currency1, quote=source.currency0)
        assert observed.spot_price(request) * observed.spot_price(reverse) == 1
        for retained in row["quotes"]:
            amount = AssetAmount.model_validate_json(json.dumps(retained["input"]))
            result = quote(reader, source, contracts, block, amount, source.currency1)
            assert result == tuple(retained["result"])
        assert reader.seen == set(reader.records)


@pytest.mark.parametrize(
    "role", ["factory", "deployer", "pool", "router", "position_manager", "quoter"]
)
def test_compiled_runtime_identity_rejects_changed_code_and_inconsistent_immutables(role):
    row = CAPTURE[2]
    if role == "deployer":
        address = "0x41ff9aa7e16b8b1a8a8dc4f0efacd93d02d071c9"
    else:
        address = row["source"][role] if role in ("factory", "pool") else row["contracts"][role]
    raw = next(
        r["result"]
        for r in row["requests"]
        if r["method"] == "eth_getCode" and r["params"][0] == address
    )
    values = verify_code(role, raw)
    assert values or role == "deployer"
    changed = bytearray.fromhex(raw[2:])
    changed[0] ^= 1
    with pytest.raises(DataError, match="code differs"):
        verify_code(role, "0x" + changed.hex())
    with pytest.raises(DataError, match="runtime length"):
        verify_code(role, raw[:-2])
    slots = next((s for s in revisions()[role]["immutables"].values() if len(s) > 1), None)
    if slots is None:
        assert role in ("factory", "deployer")
        return
    changed = bytearray.fromhex(raw[2:])
    changed[slots[0]["start"]] ^= 1
    with pytest.raises(DataError, match="occurrences disagree"):
        verify_code(role, "0x" + changed.hex())


@pytest.mark.parametrize("mutation", ["factory", "tokens", "fee", "spacing"])
def test_configured_market_cannot_substitute_another_source(mutation):
    with Replay(CAPTURE[2]) as reader:
        source = reader.source
        block = reader.pin_block(CAPTURE[2]["block"]["number"])
        if mutation == "factory":
            key = next(k for k in reader.records if "eth_getCode" in k and source.factory in k)
            raw = bytearray.fromhex(reader.records[key][2:])
            for slot in revisions()["factory"]["immutables"]["poolDeployer"]:
                raw[slot["start"] : slot["start"] + slot["length"]] = (2**160).to_bytes(
                    slot["length"], "big"
                )
            reader.records[key] = "0x" + raw.hex()
        elif mutation == "tokens":
            source = source.model_copy(update={"currency0": source.currency1})
        elif mutation == "fee":
            source = source.model_copy(update={"fee": source.fee + 1})
        else:
            source = source.model_copy(update={"tick_spacing": source.tick_spacing + 1})
        with pytest.raises(DataError, match="constructor immutables|deployer address"):
            read_pool(reader, source, block)
