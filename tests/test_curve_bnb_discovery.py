"""BNB candidates retain refused markets, source identities and measured fee units."""

import json

import pytest
from eth_utils import keccak

from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.curve.reader import read_pool
from almanak.connectors.curve.twocrypto_quotes import swap
from almanak.gateway.ethereum import DataError
from tests.test_curve_bnb_reader import capture
from tests.test_curve_reader import CapturedReader


def test_official_tricrypto_factory_has_no_pool_at_the_verified_checkpoint():
    data = capture("curve-bnb-agent-reader-probe")
    assert data["block"]["chain"]["chain_id"] == "eip155:56"
    selector = "0x" + keccak(text="pool_count()")[:4].hex()
    values = {
        record["params"][0]["to"].lower(): int(record["result"], 16)
        for record in data["records"]
        if record["method"] == "eth_call" and record["params"][0]["data"] == selector
    }
    assert values == {
        "0xd7e72f3615aa65b92a4dbdc211e296a35512988b": 295,
        "0x98ee851a00abee0d95d08cf4ca2bdce32aeaaf7f": 141,
        "0x38f8d93406fa2d9924dcfcb67db5b0521fb20f7d": 0,
    }
    assert capture("curve-bnb-agent-factory-tricrypto-api")["data"]["poolData"] == []


def test_api_implementation_label_does_not_admit_a_different_pool_revision():
    data = capture("curve-bnb-agent-reader-probe")
    source = CurvePool.model_validate_json(json.dumps(data["pools"][1]["source"]))
    with CapturedReader(source, data["records"]) as reader:
        block = reader.pin_block(data["block"]["number"])
        with pytest.raises(DataError, match="runtime length"):
            read_pool(reader, source, block)


def test_positive_wbnb_cake_balances_do_not_establish_usable_math_liquidity():
    data = capture("curve-bnb-agent-two-scan")
    row = next(
        row for row in data["pools"] if row["pool"] == "0x7b6c14ccca515d6d1f234054254bdced56ac173e"
    )
    observed = CryptoObservation.model_validate_json(json.dumps(row["observation"]))
    assert all(value > 0 for value in observed.balances)
    with pytest.raises(ValueError, match="invariant is outside"):
        swap(observed.math_state(), 0, 1, 5 * 10**11)


@pytest.mark.parametrize("row", capture("curve-bnb-agent-chain-proof")["fee_comparisons"])
def test_actual_bnb_sender_debit_reconciles_the_chain_fee_model(row):
    data = capture("curve-bnb-agent-chain-proof")
    receipts = {
        record["params"][0]: record["result"]
        for record in data["records"]
        if record["method"] == "eth_getTransactionReceipt"
    }
    receipt = receipts[row["transaction_hash"]]
    assert receipt["status"] == "0x1" and receipt["type"] == row["receipt_type"]
    gas_fee = int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16)
    assert row["balance_before"] - row["balance_after"] - row["native_value"] == gas_fee
    assert row["observed_cost"] == row["expected_cost"] == gas_fee
