"""Position execution must reject an unverified SwapHandler despite an empty swap route."""

import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.connectors.gmx_v2.deployments import records, verify_order_executors
from almanak.gateway.ethereum import DataError
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-30-gmx-deployment-rotation/anchor"


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_position_binding_rejects_a_changed_swap_handler(chain):
    harness = harness_directory(f"gmx_v2_{chain}_position") / "config.yaml"
    config = read_author(harness).fork_config()
    market = config.allocations[0].market
    expected = next(
        address for address, value in records(market).items() if value["contract"] == "SwapHandler"
    )

    class Reader:
        changed = False

        def call(self, address, getter, block, *, outputs):
            assert address == market.contracts.order_handler
            assert outputs == ("address",)
            if getter == "increaseOrderExecutor()":
                return (market.contracts.increase_order_executor,)
            if getter == "decreaseOrderExecutor()":
                return (market.contracts.decrease_order_executor,)
            assert getter == "swapHandler()"
            return (market.contracts.increase_order_executor if self.changed else expected,)

    reader = Reader()
    verify_order_executors(reader, market, None)
    reader.changed = True
    with pytest.raises(DataError, match="different compiled executor"):
        verify_order_executors(reader, market, None)


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_exact_pin_capture_covers_the_added_runtime_and_selected_handler(chain):
    harness = harness_directory(f"gmx_v2_{chain}_position") / "config.yaml"
    config = read_author(harness).fork_config()
    market = config.allocations[0].market
    expected = next(
        address for address, value in records(market).items() if value["contract"] == "SwapHandler"
    )
    capture = json.loads(gzip.decompress((EVIDENCE / f"{chain}-capture.json.gz").read_bytes()))
    code = next(
        row for row in capture if row["method"] == "eth_getCode" and row["params"][0] == expected
    )
    assert (
        "0x" + keccak(bytes.fromhex(code["result"][2:])).hex()
        == records(market)[expected]["code_hash"]
    )
    selector = "0x" + keccak(text="swapHandler()")[:4].hex()
    (selected,) = [
        row
        for row in capture
        if row["method"] == "eth_call"
        and row["params"][0] == {"to": market.contracts.order_handler, "data": selector}
    ]
    assert "0x" + selected["result"][-40:] == expected
    assert selected["params"][1] == code["params"][1]
    assert selected["params"][1] == {
        "blockHash": config.forks[0].block.hash,
        "requireCanonical": True,
    }
