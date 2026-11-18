"""Risk batching changes transport grouping without carrying earlier financial values."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.connectors.gmx_v2 import economic_prefetch
from almanak.connectors.gmx_v2.economic_spot_types import SpotState


def sample():
    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-prepared/arbitrum-spot-reader-comparison.json"
    )
    state = SpotState.model_validate_json(json.dumps(json.loads(path.read_text())["state"]))
    request = ReadRequest(
        chain=state.block.chain,
        method="eth_getCode",
        params=(
            state.market.contracts.reader,
            {"blockHash": state.block.hash, "requireCanonical": True},
        ),
    )
    return state, request


def test_only_canonical_request_shapes_are_carried_to_the_next_block(monkeypatch):
    state, request = sample()
    reader = SimpleNamespace(cache={}, used=set(), prefetch=Mock())

    def verify(actual, market, block):
        selected = request.model_copy(
            update={
                "params": (
                    market.contracts.reader,
                    {"blockHash": block.hash, "requireCanonical": True},
                )
            }
        )
        actual.cache[selected.key] = ReadEvidence(request=selected, result="0x12", retrieved_at=1)
        actual.used.add(selected.key)

    verify_mock = Mock(side_effect=verify)
    risk_mock = Mock(return_value="current-block-risk")
    monkeypatch.setattr(economic_prefetch, "verify_deployments", verify_mock)
    monkeypatch.setattr(economic_prefetch, "swap_risk", risk_mock)
    batch = economic_prefetch.RiskReads()
    assert batch.read(reader, state.market, state.block) == "current-block-risk"
    reader.prefetch.assert_called_once_with(())
    assert batch.templates == (request,)
    later = state.block.model_copy(
        update={"hash": "0x" + "12" * 32, "number": state.block.number + 1}
    )
    assert batch.read(reader, state.market, later) == "current-block-risk"
    sent = reader.prefetch.call_args.args[0]
    assert len(sent) == 1
    assert sent[0].chain == state.block.chain
    assert sent[0].params == (
        state.market.contracts.reader,
        {"blockHash": later.hash, "requireCanonical": True},
    )
    assert sent[0].key != request.key
    verify_mock.assert_called_with(reader, state.market, later)
    risk_mock.assert_called_with(reader, state.market, later)


def test_prefetch_cannot_bypass_current_block_deployment_verification(monkeypatch):
    state, request = sample()
    reader = SimpleNamespace(cache={}, used=set(), prefetch=Mock())
    batch = economic_prefetch.RiskReads()
    batch.templates = (request,)
    monkeypatch.setattr(
        economic_prefetch, "verify_deployments", Mock(side_effect=ValueError("changed code"))
    )
    risk = Mock()
    monkeypatch.setattr(economic_prefetch, "swap_risk", risk)
    with pytest.raises(ValueError, match="changed code"):
        batch.read(reader, state.market, state.block)
    risk.assert_not_called()
