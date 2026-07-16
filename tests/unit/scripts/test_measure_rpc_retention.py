"""Unit tests for ``scripts/measure_rpc_state_retention.py`` (VIB-5869).

The probe is the tool that produces the checked-in retention table, which in
turn drives ``fork_requires_archive``. So a probe bug can corrupt the very
measurement the fix depends on. These pin the two failure modes flagged in
review:

* a young chain (``head < depth``) must never be asked for a NEGATIVE block
  and have the RPC's rejection misread as a pruning boundary; and
* a malformed (non-dict) JSON-RPC envelope must be inconclusive, never a
  boundary — and must not throw.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "measure_rpc_state_retention.py"
_spec = importlib.util.spec_from_file_location("measure_rpc_state_retention", _SCRIPT_PATH)
assert _spec and _spec.loader
mrr = importlib.util.module_from_spec(_spec)
sys.modules["measure_rpc_state_retention"] = mrr
_spec.loader.exec_module(mrr)


@pytest.fixture(autouse=True)
def _no_pacing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mrr.time, "sleep", lambda *_: None)


class TestNegativeBlockClamp:
    def test_state_at_never_requests_a_negative_block(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The bug: hex(-26) is passed to the RPC, which errors, and the search
        reads that error as 'pruned here'. The clamp must floor at 0."""
        seen: list[str] = []

        def fake_rpc(url: str, method: str, params: list, timeout: float = 25.0) -> dict:
            seen.append(params[1])  # the block tag
            return {"result": "0x1"}

        monkeypatch.setattr(mrr, "_rpc", fake_rpc)
        mrr._state_at("https://x", -26)
        assert seen == ["0x0"], f"expected genesis, got {seen}"

    def test_young_chain_reports_genesis_not_prune(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 100-block chain that serves everything back to genesis is UNBOUNDED
        retention, not a short window. The old code would bisect toward a
        negative block and report a false-short prune."""
        # head=100; every real block (>=0) serves state.
        monkeypatch.setattr(mrr, "_head", lambda url: (100, 1_700_000_000))
        monkeypatch.setattr(mrr, "_rpc", lambda *a, **k: {"result": "0x1"})
        out = mrr.measure("younglabs", "https://x", block_time=2.0)
        assert "PRUNED" not in out
        assert "genesis" in out and "unbounded" in out.lower()


class TestMalformedResponse:
    @pytest.mark.parametrize("payload", [[], ["batch"], "bare-string", 42, None])
    def test_non_dict_response_is_inconclusive_not_boundary(
        self, payload: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A list/None/scalar envelope must not throw and must not be read as a
        retention boundary (which would drive a wrong archive decision)."""
        monkeypatch.setattr(mrr, "_rpc", lambda *a, **k: payload)
        probe = mrr._state_at("https://x", 123)
        assert probe.ok is None  # inconclusive, not False

    def test_error_envelope_is_a_boundary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mrr, "_rpc", lambda *a, **k: {"error": {"message": "missing trie node"}})
        probe = mrr._state_at("https://x", 123)
        assert probe.ok is False
        assert "missing trie node" in probe.detail

    def test_non_dict_error_field_does_not_throw(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mrr, "_rpc", lambda *a, **k: {"error": "stringified error"})
        probe = mrr._state_at("https://x", 123)
        assert probe.ok is False
        assert "stringified error" in probe.detail
