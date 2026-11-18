"""QA refuses missing calibration and distinguishes native comparison from dataset quotes."""

import json

import pytest

from almanak.caip import ChainId
from tests.qa import gmx_comparisons as comparisons


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
def test_qa_swap_packet_replays_all_native_reader_fields(chain):
    _, _, packet = comparisons.required_calibration(ChainId(chain), "swap")
    rows = comparisons.compare_swap_packet(packet, ChainId(chain))
    assert len(rows) == 8 and all(row["equal"] for row in rows)
    assert all("not_dataset_quote" in row["comparison"] for row in rows)


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
def test_qa_gm_uses_actual_executor_comparison_instead_of_reader_deposit_quote(chain):
    chain = ChainId(chain)
    path, _, packet = comparisons.required_calibration(chain, "liquidity")
    rows = comparisons.compare_gm_packet(path, packet, chain)
    assert [row["kind"] for row in rows] == ["deposit", "deposit", "withdrawal", "withdrawal"]
    assert all(row["equal"] and "not_reader_deposit_quote" in row["comparison"] for row in rows)


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
def test_qa_missing_required_perpetual_calibration_refuses_green(chain):
    with pytest.raises(AssertionError, match="required.*calibration is unavailable"):
        comparisons.required_calibration(ChainId(chain), "perpetual")


def test_qa_perpetual_family_cannot_fall_through_to_the_gm_packet_parser():
    chain = ChainId("eip155:42161")
    path, _, packet = comparisons.required_calibration(chain, "liquidity")
    with pytest.raises(AssertionError, match="perpetual native calibration is unavailable"):
        comparisons.calibration_rows(path, packet, chain, "perpetual")


def test_qa_calibration_digest_refuses_rewritten_results(tmp_path):
    path = tmp_path / "packet.json"
    path.write_text(json.dumps({"status": "passed"}))
    with pytest.raises(AssertionError, match="hash differs"):
        comparisons.sealed_json(path, "0" * 64)


def test_qa_native_gm_transfer_change_is_not_accepted():
    chain = ChainId("eip155:42161")
    path, _, packet = comparisons.required_calibration(chain, "liquidity")
    cases = comparisons.sealed_json(
        path.parent / "comparisons.json", packet["files"]["comparisons.json"]
    )
    cases[0]["actual"][2] += 1
    assert not comparisons.replay_gm_row(cases[0])["equal"]


@pytest.mark.parametrize("mutation", ["balance", "receipt", "handler", "timestamp"])
def test_qa_captured_executor_rejects_mismatched_native_evidence(mutation):
    chain = ChainId("eip155:43114")
    path, _, packet = comparisons.required_calibration(chain, "liquidity")
    row = comparisons.captured_files(path, packet)["comparisons.json"][0]
    if mutation == "balance":
        row["fund_balances_after"][row["request"]["market"]] += 1
    elif mutation == "receipt":
        row["receipt"]["transactionHash"] = "0x" + "00" * 32
    elif mutation == "handler":
        row["receipt"]["to"] = row["keeper_transaction"]["to"] = "0x" + "00" * 20
    else:
        row["settled_block"]["timestamp"] += 1
    with pytest.raises(AssertionError):
        comparisons.replay_captured_gm_row(row)
