"""SparkLend's retained archival reads verify its own revisions and accounting."""

import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.connectors.aave_v3.domain import PositionObservation as AavePosition
from almanak.connectors.sparklend.domain import PositionObservation, ReserveObservation
from almanak.connectors.sparklend.math import debt_assets, normalized_indices, supplied_assets
from almanak.connectors.sparklend.reader import read_position
from almanak.gateway.ethereum import DataError
from tests.test_aave_v3_reader import replay


def captured():
    path = Path(__file__).parent / "fixtures/sparklend-ethereum-revision4-reads.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


@pytest.mark.parametrize("index", [0, 1])
def test_reader_and_half_up_indices_match_archival_contracts(index):
    raw = captured()
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][index]))
    comparison = raw["comparisons"][index]
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        actual = read_position(reader, expected.reserve.source, expected.fund, block)
    assert actual == expected
    indices = normalized_indices(actual.reserve, block.timestamp)
    assert indices == tuple(comparison["normalized_indices"])
    assert comparison["elapsed_seconds"] > 0
    assert (
        supplied_assets(actual.reserve.total_scaled_supply, indices[0]),
        debt_assets(actual.reserve.total_scaled_debt, indices[1]),
    ) == tuple(comparison["total_balances"])
    assert actual.reserve.oracle.quote_unit == "sparklend_oracle_base_currency"
    assert actual.reserve.total_stable_debt == actual.stable_debt_assets == 0
    assert "virtual_underlying_balance" not in type(actual.reserve).model_fields
    with pytest.raises(ValueError):
        AavePosition.model_validate_json(actual.model_dump_json())


def test_changed_pool_bytecode_cannot_claim_the_verified_revision():
    raw = captured()
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    for row in raw["reads"]:
        if row["method"] == "eth_getCode" and row["params"][0] == expected.reserve.source.pool:
            row["result"] = "0x00"
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        with pytest.raises(DataError, match="unverified revision"):
            read_position(reader, expected.reserve.source, expected.fund, block)


@pytest.mark.parametrize(
    "signature,field",
    [("getAssetPrice(address)", "oracle"), ("getUserAccountData(address)", "risk")],
)
def test_missing_valuation_retains_measured_positions_with_a_reason(signature, field):
    raw = captured()
    selector = "0x" + keccak(text=signature)[:4].hex()
    for row in raw["reads"]:
        if row["method"] == "eth_call" and row["params"][0]["data"].startswith(selector):
            row["result"] = None
    expected = PositionObservation.model_validate_json(json.dumps(raw["positions"][0]))
    with replay(raw) as reader:
        block = reader.pin_block(expected.reserve.block.number)
        actual = read_position(reader, expected.reserve.source, expected.fund, block)
    evidence = actual.reserve if field == "oracle" else actual
    assert getattr(evidence, field) is None
    assert getattr(evidence, field + "_error") == "unavailable"
    assert actual.supplied_scaled == expected.supplied_scaled
    assert actual.debt_scaled == expected.debt_scaled


def test_missing_stable_debt_cannot_be_interpreted_as_measured_zero():
    raw = captured()["positions"][0]["reserve"]
    del raw["total_stable_debt"]
    with pytest.raises(ValueError, match="total_stable_debt"):
        ReserveObservation.model_validate_json(json.dumps(raw))
