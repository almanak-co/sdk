"""Golden-response parity for the fee-model exporter (VIB-4851 Phase D / D4).

The backtest service is separately deployed; its fee-model responses are an
external contract. The fixture was captured from the pre-D4 exporter (central
``_PROTOCOL_METADATA`` table); after D4 the standard fields derive from each
connector's ``fee_model.BACKTEST_EXPORT_METADATA`` — these tests prove the
HTTP-visible payloads are byte-identical.

If a connector legitimately changes its exported metadata, regenerate the
fixture in the same PR and call the change out explicitly — never let it
drift silently.
"""

import json
from pathlib import Path

from almanak.services.backtest.services.fee_model_exporter import (
    get_fee_model_detail,
    list_fee_models,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "fee_model_exporter_golden.json"


def _dump(model) -> dict:
    return model.model_dump() if hasattr(model, "model_dump") else dict(model.__dict__)


def test_list_fee_models_matches_golden() -> None:
    """Summaries are byte-identical to the pre-D4 captured responses."""
    golden = json.loads(_FIXTURE.read_text())
    live = sorted((_dump(s) for s in list_fee_models()), key=lambda d: d["protocol"])
    assert json.loads(json.dumps(live, default=str)) == golden["summaries"]


def test_fee_model_details_match_golden() -> None:
    """Every protocol's detail payload is byte-identical to the capture."""
    golden = json.loads(_FIXTURE.read_text())
    for protocol, want in golden["details"].items():
        detail = get_fee_model_detail(protocol)
        assert detail is not None, protocol
        assert json.loads(json.dumps(_dump(detail), default=str)) == want, protocol


def test_unknown_protocol_returns_none() -> None:
    """Unknown protocols still yield None (404 path)."""
    assert get_fee_model_detail("not_a_protocol") is None
