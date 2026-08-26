"""Contracts for quant-test's connector-native terminal proof capture."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

from almanak.framework.teardown.models import PositionType

SCRIPT = Path(__file__).parents[3] / "scripts" / "quant-test" / "capture_terminal_proof.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("capture_terminal_proof", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Gateway:
    def __init__(self, block=123):
        self.block = block

    def block_number(self, _chain):
        return self.block


def _result(*, closed=False, unmeasured=False, not_applicable=False, residual=None, error=None):
    return SimpleNamespace(
        closed=closed,
        unmeasured=unmeasured,
        not_applicable=not_applicable,
        residual=residual or {},
        error=error,
    )


def test_all_measured_zero_is_confirmed_closed(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "get_teardown_post_condition", lambda _protocol: lambda *a, **k: _result(closed=True))
    proof = module.capture_terminal_proof(
        protocol="benqi",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"), (PositionType.BORROW, "USDT")),
        gateway_client=_Gateway(),
    )
    assert proof["status"] == module.CONFIRMED_CLOSED
    assert proof["block"] == 123
    assert [c["status"] for c in proof["checks"]] == [module.CONFIRMED_CLOSED] * 2


def test_measured_residual_dominates_unverified_read(monkeypatch):
    module = _load_module()
    results = iter(
        [
            _result(unmeasured=True, error="RPC fault"),
            _result(closed=False, residual={"asset": "USDT", "residual_wei": 1}),
        ]
    )
    monkeypatch.setattr(module, "get_teardown_post_condition", lambda _protocol: lambda *a, **k: next(results))
    proof = module.capture_terminal_proof(
        protocol="benqi",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"), (PositionType.BORROW, "USDT")),
        gateway_client=_Gateway(),
    )
    assert proof["status"] == module.CONFIRMED_OPEN


def test_pre_teardown_open_capture_is_positive_independent_venue_evidence(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(
        module,
        "get_teardown_post_condition",
        lambda _protocol: lambda *a, **k: _result(
            closed=False, residual={"asset": "USDC", "supplied_wei": 1_000_000}
        ),
    )
    proof = module.capture_terminal_proof(
        protocol="benqi",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"),),
        gateway_client=_Gateway(),
        phase="pre_teardown",
        expected_status=module.CONFIRMED_OPEN,
    )
    assert proof["status"] == module.CONFIRMED_OPEN
    assert proof["expectation_met"] is True
    assert proof["phase"] == "pre_teardown"
    assert "independent" in proof["evidence_role"]


def test_pre_teardown_flat_state_is_contradictory_not_a_pass(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(
        module, "get_teardown_post_condition", lambda _protocol: lambda *a, **k: _result(closed=True)
    )
    proof = module.capture_terminal_proof(
        protocol="benqi",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"),),
        gateway_client=_Gateway(),
        phase="pre_teardown",
        expected_status=module.CONFIRMED_OPEN,
    )
    assert proof["status"] == module.CONFIRMED_CLOSED
    assert proof["expectation_met"] is False


def test_missing_block_pin_is_unverified_and_performs_no_hook_reads(monkeypatch):
    module = _load_module()
    called = False

    def hook(*_args, **_kwargs):
        nonlocal called
        called = True
        return _result(closed=True)

    monkeypatch.setattr(module, "get_teardown_post_condition", lambda _protocol: hook)
    proof = module.capture_terminal_proof(
        protocol="benqi",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"),),
        gateway_client=_Gateway(block=None),
    )
    assert proof["status"] == module.UNVERIFIED
    assert proof["checks"] == []
    assert called is False


def test_no_registered_hook_is_unverified(monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "get_teardown_post_condition", lambda _protocol: None)
    proof = module.capture_terminal_proof(
        protocol="unknown",
        chain="avalanche",
        wallet="0x" + "11" * 20,
        legs=((PositionType.SUPPLY, "USDC"),),
        gateway_client=_Gateway(),
    )
    assert proof["status"] == module.UNVERIFIED
    assert "no registered" in proof["reason"]
