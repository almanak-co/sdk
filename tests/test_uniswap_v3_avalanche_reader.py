"""New-chain revision and bidirectional quotes replay exact retained archive reads."""

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

PATH = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-09-dex-finish/discovery"


def capture(chain):
    return json.loads(
        gzip.decompress((PATH / f"uniswap-v3-{chain}-discovery.json.gz").read_bytes())
    )


@pytest.mark.parametrize("chain", ("avalanche", "bnb", "polygon", "robinhood"))
def test_revisions_preserve_configured_factory_immutables_and_quote_units(chain):
    retained = capture(chain)
    assert retained["status"] == "qualified_revision_market"
    with Replay({**retained, "requests": retained["reads"]}) as reader:
        source = reader.source
        contracts = Contracts.model_validate_json(json.dumps(retained["contracts"]))
        block = reader.pin_block(retained["block"]["number"])
        identities = (
            *verify_pool(reader, source, block),
            *verify_periphery(reader, source, contracts, block),
        )
        assert [value.model_dump(mode="json") for value in identities] == retained["identities"]
        assert read_pool(reader, source, block) == PoolObservation.model_validate_json(
            json.dumps(retained["observation"])
        )
        assert len(retained["quotes"]) == 2
        for row in retained["quotes"]:
            amount = AssetAmount.model_validate_json(
                json.dumps({"asset": row["input_asset"], "amount": row["input_amount"]})
            )
            other = next(
                asset for asset in (source.currency0, source.currency1) if asset != amount.asset
            )
            assert str(other) == row["output_asset"]
            assert quote(reader, source, contracts, block, amount, other) == tuple(row["values"])


@pytest.mark.parametrize("role", ["factory", "pool", "router", "position_manager", "quoter"])
@pytest.mark.parametrize("chain", ("avalanche", "bnb", "polygon", "robinhood"))
def test_mutated_runtime_is_refused(role, chain):
    retained = capture(chain)
    addresses = {**retained["source"], **retained["contracts"]}
    raw = next(
        row["result"]
        for row in retained["reads"]
        if row["method"] == "eth_getCode" and row["params"][0] == addresses[role]
    )
    verify_code(role, raw)
    changed = bytearray.fromhex(raw[2:])
    changed[0] ^= 1
    with pytest.raises(DataError, match="code differs"):
        verify_code(role, "0x" + changed.hex())
