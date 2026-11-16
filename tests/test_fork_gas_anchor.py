"""A time-dependent Slipstream fee cannot move an already observed fork gas estimate."""

import json
from types import MethodType, SimpleNamespace

import pytest

from almanak.gateway.execution import EvmGateway
from tests.slipstream_support import EVIDENCE, target


@pytest.mark.parametrize("direction", (0, 1))
def test_preparation_revalidation_keeps_the_measured_block_estimate(direction):
    samples = json.loads((EVIDENCE / "gas-time-samples.json").read_text())[direction]
    block = SimpleNamespace(hash="0x" + "ab" * 32, number=5)
    tx = {"data": "0x"}
    selector = {"blockHash": block.hash, "requireCanonical": True}

    class Reader:
        index = 0

        def pin_block(self, number):
            assert number == block.number
            return block

        def _rpc(self, method, params):
            if method == "eth_feeHistory":
                assert params == ["0x1", "0x5", []]
                return {"oldestBlock": "0x5", "baseFeePerGas": ["0x8", "0x9"]}
            assert method == "eth_estimateGas"
            sample = samples[self.index]
            self.index += 1
            return sample["pinned"] if params == [tx, selector] else sample["unpinned"]

    backend = SimpleNamespace(config=SimpleNamespace(production=None), reader=Reader())
    backend.estimate_gas = MethodType(EvmGateway.estimate_gas, backend)
    operation = SimpleNamespace(target=target())
    before = SimpleNamespace(block=block)
    fees = [EvmGateway.prepare_fees(backend, None, operation, before, tx) for _ in samples]
    assert len({sample["unpinned"] for sample in samples}) > 1
    assert all(fee == fees[0] for fee in fees)
    estimate = int(samples[0]["pinned"], 16)
    assert fees[0] == {
        "gas_estimate": estimate,
        "gas": (estimate * 5 + 3) // 4,
        "gas_price": 11,
    }
