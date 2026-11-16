"""Source-bound OP fee getters reconcile actual signed transactions to payer debits."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.gateway.op_fees import FjordParameters, read_parameters
from tests.test_op_fees import replay


@pytest.mark.parametrize("index", range(3))
def test_pinned_optimism_l1_and_operator_charges_match_payer(index):
    raw = json.loads(
        gzip.decompress(
            (Path(__file__).parent / "fixtures/optimism-jovian-fee-reads.json.gz").read_bytes()
        )
    )
    case = raw["cases"][index]
    expected = FjordParameters.model_validate_json(json.dumps(case["parameters"]))
    client, reader = replay(raw, expected.block.chain)
    reader.transport_chain = ChainId("eip155:31337")
    with client, reader:
        actual = read_parameters(reader, expected.block)
    assert actual == expected
    receipt = case["receipt"]
    gas = int(receipt["gasUsed"], 16)
    charged = gas * int(receipt["effectiveGasPrice"], 16)
    charged += actual.l1_fee(bytes.fromhex(case["raw_transaction"][2:]))
    charged += actual.operator_fee(gas)
    assert charged == case["payer_before"] - case["payer_after"]
    assert actual.l1_fee(bytes.fromhex(case["raw_transaction"][2:])) > 0
