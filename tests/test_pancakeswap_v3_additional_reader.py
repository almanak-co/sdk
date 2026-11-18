"""Pinned additional-chain reads keep exact revisions and reject unknown LM implementations."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.pancakeswap_v3.deployments import verify_factory, verify_periphery
from almanak.connectors.pancakeswap_v3.domain import Contracts, PancakePool, PoolObservation
from almanak.connectors.pancakeswap_v3.reader import quote, read_pool
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_pancakeswap_v3_reader import Replay

CAPTURE = json.loads(
    gzip.decompress(
        (
            Path(__file__).with_name("fixtures") / "pancakeswap-v3-additional-readers.json.gz"
        ).read_bytes()
    )
)


@pytest.mark.parametrize("row", CAPTURE, ids=lambda row: row["network"])
def test_pinned_revisions_observations_and_quotes_replay_without_network(row):
    first = next(m for m in row["markets"] if "source" in m)
    with Replay({"source": first["source"], "requests": row["requests"]}) as reader:
        block = reader.pin_block(row["block"]["number"])
        for market in row["markets"]:
            source = PancakePool.model_validate_json(json.dumps(market["source"]))
            contracts = Contracts.model_validate_json(json.dumps(market["contracts"]))
            identities = (
                *verify_factory(reader, source, block),
                *verify_periphery(reader, source, contracts, block),
            )
            assert [i.model_dump(mode="json") for i in identities] == market["identities"]
            if market["status"] != "verified_reader":
                with pytest.raises(DataError, match="callback revision is unverified"):
                    read_pool(reader, source, block)
                continue
            observed = read_pool(reader, source, block)
            assert observed == PoolObservation.model_validate_json(json.dumps(market["observed"]))
            for item in market["quotes"]:
                amount = AssetAmount.model_validate_json(json.dumps(item["input"]))
                output = next(a for a in (source.currency0, source.currency1) if a != amount.asset)
                assert quote(reader, source, contracts, block, amount, output) == tuple(
                    item["result"]
                )
        # The capture also retains its initial latest-header discovery.
        expected = {k for k in reader.records if '"latest"' not in k}
        assert reader.seen == expected


@pytest.mark.parametrize(
    "mutation,kind",
    [
        ("pool", "identity"),
        ("masterChef", "identity"),
        ("code", "revision"),
        ("uninitialized", "state"),
        ("unavailable", "unavailable"),
    ],
)
def test_verified_lm_revision_rejects_bad_bindings_state_and_missing_reads(mutation, kind):
    from eth_utils import keccak

    from almanak.connectors.pancakeswap_v3.deployments import revisions, verify_callback

    row = next(r for r in CAPTURE if r["network"] == "ethereum")
    market = next(m for m in row["markets"] if m["source"]["fee"] == 500)
    callback = market["observed"]["lm_pool"]
    with Replay({"source": market["source"], "requests": row["requests"]}) as reader:
        block = reader.pin_block(row["block"]["number"])
        key = next(
            k
            for k in reader.records
            if json.loads(k)[0] == "eth_getCode" and json.loads(k)[1][0] == callback
        )
        if mutation in ("pool", "masterChef", "code"):
            raw = bytearray.fromhex(reader.records[key][2:])
            if mutation == "code":
                raw[0] ^= 1
            else:
                value = 1 if mutation == "pool" else 0
                for slot in revisions()["lm_v4"]["immutables"][mutation]:
                    raw[slot["start"] : slot["start"] + slot["length"]] = value.to_bytes(
                        slot["length"]
                    )
            reader.records[key] = "0x" + raw.hex()
        elif mutation == "uninitialized":
            selector = "0x" + keccak(text="initialization()")[:4].hex()
            call = next(k for k in reader.records if selector in k and callback in k)
            reader.records[call] = "0x" + "00" * 32
        else:
            original = reader._rpc

            def unavailable(method, params, **kwargs):
                if method == "eth_getCode" and params[0] == callback:
                    raise DataError("unavailable", "missing pinned callback code")
                return original(method, params, **kwargs)

            reader._rpc = unavailable
        with pytest.raises(DataError) as error:
            verify_callback(reader, reader.source, block)
        assert error.value.kind == kind
