"""Structured gateway refusals are retained from supervised coordinator output."""

import json

from tests.strategy_fork.supervised import gateway_refusal


def test_gateway_refusal_selects_only_an_exact_structured_error(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    expected = {
        "error": "authority",
        "message": "Uniswap code differs from the verified implementation",
    }
    (logs / "coordinator.log").write_text(
        "ordinary output\n"
        + json.dumps({"error": "ignored", "message": "extra", "context": "not retained"})
        + "\n"
        + json.dumps(expected)
        + "\ntraceback output\n"
    )
    assert gateway_refusal(tmp_path) == expected


def test_gateway_refusal_omits_unstructured_or_absent_output(tmp_path):
    assert gateway_refusal(tmp_path) is None
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "coordinator.log").write_text('{"error": "missing-message"}\n')
    assert gateway_refusal(tmp_path) is None
