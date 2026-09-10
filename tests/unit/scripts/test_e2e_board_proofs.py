"""Execute the Board's actual proof renderer without a producer-authored substitute."""

import ast
import json
import shutil
import subprocess
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[3] / "qa_lab/qa_coverage.py"


def board_constants():
    return {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in ast.parse(SOURCE.read_text()).body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in {"LAB_JS", "LAB_CSS"}
    }


def render(row):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to execute the Board proof renderer")
    source = board_constants()["LAB_JS"]
    helpers = "\n".join(
        line for line in source.splitlines() if line.startswith(("const esc=", "function artifactHref("))
    )
    panel = "function e2eProofPanel" + source.split("function e2eProofPanel", 1)[1].split("\nfunction selectCell", 1)[0]
    script = helpers + "\n" + panel + "\nconsole.log(e2eProofPanel(JSON.parse(require('fs').readFileSync(0,'utf8'))));"
    return subprocess.run(
        [node, "-e", script], input=json.dumps(row), text=True, capture_output=True, check=True
    ).stdout.strip()


def fixture_row():
    tx, observation = "0x" + "aa" * 32, "bb" * 32
    return {
        "derived_claims": {
            "strategy": {
                "observer": "quant-sealer",
                "evidence": {
                    "rebalance_price_cycle": {
                        "status": "PASS",
                        "cycle_id": "subject-close-cycle",
                        "price_record_line": 42,
                        "close_transaction": "0x" + "cc" * 32,
                    },
                    "stimulus_price_link": {
                        "status": "PASS",
                        "block": 12345,
                        "stimulus_transaction": tx,
                        "observation_id": observation,
                    },
                },
            }
        },
        "artifacts": [
            {"relpath": f"runs/synthetic/stimulus-quantities/chain/receipt-{tx}.json"},
            {"relpath": f"runs/synthetic/price-observations/{observation}.json"},
        ],
    }


def test_admitted_links_display_their_cycle_and_raw_proof_paths():
    html = render(fixture_row())
    assert "Consumed price → narrow close · PASS" in html
    assert "Stimulus Swap → consumed price · PASS" in html
    assert "subject-close-cycle · log line 42" in html
    assert "Matched pool state at block 12345" in html
    assert 'href="../runs/synthetic/stimulus-quantities/chain/receipt-0x' in html
    assert "Stimulus Swap receipt</a>" in html
    assert "Consumed pool observation</a>" in html
    assert "full E2E certification require separate proofs" in html


def test_old_price_lineage_does_not_inherit_new_pass_claims():
    row = fixture_row()
    row["derived_claims"]["strategy"]["evidence"] = {"pool_price_inputs": {"status": "PASS", "records": []}}
    html = render(row)
    assert "Consumed price → narrow close · UNMEASURED" in html
    assert "Stimulus Swap → consumed price · UNMEASURED" in html
    assert "Stimulus Swap receipt</a>" not in html
    assert "confidence calibration and chain-head freshness are unmeasured" in html


def test_producer_fields_cannot_populate_the_admitted_proof_panel():
    row = fixture_row()
    row["derived_claims"]["strategy"]["observer"] = "producer"
    assert render(row) == ""


def test_proof_details_escape_recorded_identifiers():
    row = fixture_row()
    row["derived_claims"]["strategy"]["evidence"]["rebalance_price_cycle"]["cycle_id"] = "<img src=x onerror=alert(1)>"
    html = render(row)
    assert "<img" not in html
    assert "&lt;img" in html


def test_unmeasured_residual_policy_preserves_scoped_measured_proofs():
    row = fixture_row()
    claim = row["derived_claims"]["strategy"]
    claim["status"] = "UNMEASURED"
    claim["evidence"]["residual_policy"] = {
        "status": "UNMEASURED",
        "all_token_wallet_inventory": "UNMEASURED",
        "subject_pending_transactions": "UNMEASURED",
    }
    html = render(row)
    assert "Frozen residual policy · UNMEASURED" in html
    assert "All-token wallet inventory UNMEASURED · subject pending transactions UNMEASURED" in html
    assert "Stimulus Swap → consumed price · PASS" in html
