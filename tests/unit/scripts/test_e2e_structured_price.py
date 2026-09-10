"""Structured runner identity owns new price proofs; console formatting does not."""

import json
import re
import subprocess
import sys

import pytest

from qa_lab.e2e_card import digest
from qa_lab.e2e_price_lineage import MEASUREMENT_POLICY, _records, validate_price_contract
from tests.unit.scripts import test_e2e_price_lineage as legacy

bundle = legacy.bundle
CELL = {
    "strategy_path": "strategies/accounting/lp_dual",
    "chain": "arbitrum",
    "protocol": "uniswap_v3",
    "primitive": "lp",
    "network": "anvil",
    "exec_path": "eoa",
}
CONTRACT = {"pool_price_inputs": {"schema_version": 3, "max_age_seconds": 60}}


@pytest.fixture
def event(bundle):
    text = (bundle / "run.log").read_text()
    prefix, body = text.split("LP_PRICE_INPUT ", 1)
    record, offset = json.JSONDecoder().raw_decode(body)
    for index in range(2):
        key = f"token{index}_observation_id"
        path = bundle / "price-observations" / f"{record[key]}.json"
        observation = json.loads(path.read_text())
        observation["measurement_policy"] = MEASUREMENT_POLICY
        raw = json.dumps(observation).encode()
        record[key] = digest(raw)
        (path.parent / f"{record[key]}.json").write_bytes(raw)
    return {
        "event": "LP_PRICE_INPUT " + json.dumps(record),
        "timestamp": prefix.split()[0],
        "deployment_id": re.search(r"deployment_id=(deployment:[0-9a-f]{12})", body[offset:])[1],
        "cycle_id": re.search(r"cycle_id=([a-zA-Z0-9-]+)", body[offset:])[1],
    }


def test_structured_price_contract_ignores_console_format_and_names_its_authority(bundle, event):
    (bundle / "runner-events.jsonl").write_text(json.dumps(event) + "\n")
    (bundle / "run.log").write_text("The console renderer changed completely.\n")
    result = validate_price_contract(bundle, CONTRACT, catalog_cell=CELL)
    assert result["status"] == "PASS"
    assert result["record_source"] == "runner-events.jsonl"
    assert result["records"][0]["cycle_id"] == event["cycle_id"]
    assert "runner-events.jsonl" in result["source_artifacts"]
    assert "run.log" not in result["source_artifacts"]


@pytest.mark.parametrize("fault", ["missing", "cycle", "deployment", "timestamp", "duplicate", "truncated", "payload"])
def test_structured_contract_never_falls_back_to_console_identity(bundle, event, fault):
    if fault == "cycle":
        event.pop("cycle_id")
    elif fault == "deployment":
        event.pop("deployment_id")
    elif fault == "timestamp":
        event["timestamp"] = "2026-09-07T00:00:00"
    elif fault == "payload":
        event["event"] = "LP_PRICE_INPUT []"
    raw = json.dumps(event) + "\n"
    if fault == "duplicate":
        raw = raw.replace('"cycle_id":', '"cycle_id":"spoof", "cycle_id":')
    elif fault == "truncated":
        raw = raw.rstrip("\n")
    if fault != "missing":
        (bundle / "runner-events.jsonl").write_text(raw)
    with pytest.raises(ValueError):
        validate_price_contract(bundle, CONTRACT, catalog_cell=CELL)


def test_actual_sdk_file_handler_preserves_separate_price_identity(tmp_path):
    path = tmp_path / "runner-events.jsonl"
    script = """
import io, json, logging, sys
from datetime import UTC, datetime
from almanak.framework.utils.logging import configure_logging, add_file_handler, add_context
configure_logging(stream=io.StringIO())
add_file_handler(sys.argv[1])
add_context(deployment_id="deployment:abcdef123456", cycle_id="actual-cycle")
record = {"schema_version":1, "pool":"WETH/USDC/500", "observed_at":datetime.now(UTC).isoformat()}
logging.getLogger("strategy").info("LP_PRICE_INPUT %s", json.dumps(record))
logging.shutdown()
"""
    subprocess.run([sys.executable, "-c", script, str(path)], check=True, capture_output=True, text=True, timeout=30)
    rows = list(_records(path, structured=True))
    assert len(rows) == 1
    assert rows[0][2:4] == ("deployment:abcdef123456", "actual-cycle")
    assert rows[0][4]["pool"] == "WETH/USDC/500"
