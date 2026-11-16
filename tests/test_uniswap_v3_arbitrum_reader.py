"""Pinned Arbitrum revisions and configured venue quotes replay without a provider."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.uniswap_v3.deployments import verify_code, verify_periphery, verify_pool
from almanak.connectors.uniswap_v3.domain import Contracts, PoolObservation
from almanak.connectors.uniswap_v3.reader import quote, read_pool
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_uniswap_v3_reader import Replay

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).parent / "fixtures/uniswap-v3-arbitrum-reader.json.gz").read_bytes()
    )
)


def test_arbitrum_pool_and_periphery_keep_compiled_identity_and_exact_quote_units():
    with Replay({**CAPTURE, "requests": CAPTURE["reads"]}) as reader:
        source = reader.source
        contracts = Contracts.model_validate_json(json.dumps(CAPTURE["contracts"]))
        block = reader.pin_block(CAPTURE["block"]["number"])
        identities = (
            *verify_pool(reader, source, block),
            *verify_periphery(reader, source, contracts, block),
        )
        assert [i.model_dump(mode="json") for i in identities] == CAPTURE["identities"]
        assert read_pool(reader, source, block) == PoolObservation.model_validate_json(
            json.dumps(CAPTURE["observation"])
        )
        for row in CAPTURE["quotes"]:
            amount = AssetAmount.model_validate_json(
                json.dumps({"asset": row["input_asset"], "amount": row["input_amount"]})
            )
            other = next(a for a in (source.currency0, source.currency1) if a != amount.asset)
            assert str(other) == row["output_asset"]
            assert quote(reader, source, contracts, block, amount, other) == tuple(row["values"])


@pytest.mark.parametrize("role", ["factory", "pool", "router", "position_manager", "quoter"])
def test_changed_arbitrum_runtime_is_not_an_accepted_revision(role):
    address = CAPTURE["source"][role] if role in ("factory", "pool") else CAPTURE["contracts"][role]
    raw = next(
        r["result"]
        for r in CAPTURE["reads"]
        if r["method"] == "eth_getCode" and r["params"][0] == address
    )
    verify_code(role, raw)
    changed = bytearray.fromhex(raw[2:])
    changed[0] ^= 1
    with pytest.raises(DataError, match="code differs"):
        verify_code(role, "0x" + changed.hex())
