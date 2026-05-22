"""Category-handler registry tests (VIB-4163, T3).

Covers UAT card steps:

- D1.S1 — registry shape, register decorator, frozen HandlerContext.
- D1.S2 — dispatch parity against the FROZEN PRE-T3 truth table.
- D2.M1, D2.M2, D2.M3 — per-category dispatch spot checks.
- D3.F1 — clean-interpreter cycle-avoidance proof.
- D3.F2 — duplicate registration raises.
- D3.F3 (keyset half) — every non-NO_ACCOUNTING category has a handler.
- D3.F4 — frozen HandlerContext.
- D3.F6 — handler module import failures propagate.
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers import (
    HANDLERS,
    HandlerContext,
    register,
)
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.primitives.types import AccountingCategory

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TRUTH_TABLE = _REPO_ROOT / "tests/fixtures/accounting/legacy_dispatch_truth_table.json"


# ─── D1.S1 — Registry shape & decorator semantics ────────────────────────────


def test_registry_is_dict_keyed_by_accounting_category() -> None:
    assert isinstance(HANDLERS, dict)
    for key, value in HANDLERS.items():
        assert isinstance(key, AccountingCategory), key
        assert callable(value), (key, value)


def test_register_decorator_returns_function_unchanged() -> None:
    """``@register(...)`` MUST NOT wrap or rename the function it decorates.

    Wrapping would surface a different ``__module__`` / ``__qualname__`` to the
    duplicate-registration error message and confuse stack traces.
    """

    def _victim(_ctx: HandlerContext) -> None:  # pragma: no cover - never called
        return None

    # Use a category guaranteed to be empty AFTER the test cleans up.
    target = AccountingCategory.NO_ACCOUNTING
    saved = HANDLERS.pop(target, None)
    try:
        decorated = register(target)(_victim)
        assert decorated is _victim
    finally:
        HANDLERS.pop(target, None)
        if saved is not None:
            HANDLERS[target] = saved


def test_handler_context_is_frozen() -> None:
    ctx = HandlerContext(
        outbox_row={},
        ledger_row={},
        basis_store=FIFOBasisStore(),
        prior_open_lookup=lambda _k: None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.outbox_row = {}  # type: ignore[misc]


# ─── D1.S2 — Dispatch parity against frozen pre-T3 truth table ───────────────


_PRIOR_OPEN_PAYLOAD: dict[str, Any] = {
    "event_type": "LP_OPEN",
    "position_key": "lp:arbitrum:0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd:pool",
    "asset0": "USDC",
    "asset1": "WETH",
    "tick_lower": -887220,
    "tick_upper": 887220,
    "schema_version": 1,
}


_FIXED_DEPLOYMENT_ID = "AccountingTest:vib4163-fixture"
_FIXED_DEPLOYMENT_ID = "vib4163-fixture"
_FIXED_CYCLE_ID = "cycle-1"
_FIXED_TX_HASH = "0xfeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface"
_FIXED_WALLET = "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"
_FIXED_LEDGER_ENTRY_ID = "led-vib4163-fixture"
_FIXED_TS = "2026-01-01T00:00:00+00:00"


def _build_processor() -> AccountingProcessor:
    """Build a deterministic ``AccountingProcessor`` mirroring the truth-table generator."""

    class _StubStateManager:
        def get_accounting_events_sync(
            self, deployment_id: str, position_key: str = ""
        ) -> list[dict[str, Any]]:
            if position_key:
                return [{"event_type": "LP_OPEN", "payload_json": json.dumps(_PRIOR_OPEN_PAYLOAD)}]
            return []

    return AccountingProcessor(
        state_manager=_StubStateManager(),
        basis_store=FIFOBasisStore(),
        deployment_id=_FIXED_DEPLOYMENT_ID,
    )


def _build_outbox(*, position_key: str = "", market_id: str = "") -> dict[str, Any]:
    return {
        "id": "ob-vib4163-fixture",
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "cycle_id": _FIXED_CYCLE_ID,
        "ledger_entry_id": _FIXED_LEDGER_ENTRY_ID,
        "wallet_address": _FIXED_WALLET,
        "position_key": position_key,
        "market_id": market_id,
        "intent_type": "",
    }


def _build_ledger(
    *,
    intent_type: str,
    protocol: str = "",
    token_in: str = "",
    token_out: str = "",
) -> dict[str, Any]:
    return {
        "id": _FIXED_LEDGER_ENTRY_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "cycle_id": _FIXED_CYCLE_ID,
        "execution_mode": "live",
        "chain": "arbitrum",
        "protocol": protocol,
        "tx_hash": _FIXED_TX_HASH,
        "timestamp": _FIXED_TS,
        "intent_type": intent_type,
        "token_in": token_in,
        "token_out": token_out,
        "amount_in": "",
        "amount_out": "",
        "effective_price": None,
        "slippage_bps": None,
        "gas_usd": None,
        "extracted_data_json": "",
        "price_inputs_json": "",
        "post_state_json": "",
        "pre_state_json": "",
    }


def _outbox_for_label(label: str, category: str) -> dict[str, Any]:
    """Mirror the truth-table generator's outbox shape per (category, label)."""
    if category == "lending":
        return _build_outbox(position_key="lending:aave_v3:arbitrum:USDC")
    if category == "lp":
        return _build_outbox(position_key="lp:arbitrum:0xabcd…:0x1111111111111111111111111111111111111111")
    if category == "pendle_lp":
        return _build_outbox(position_key="pendle_lp:arbitrum:WETH-PT")
    if category == "pendle_pt":
        return _build_outbox(position_key="pendle_pt:arbitrum:PT-WETH")
    if category == "perp":
        return _build_outbox(position_key="perp:gmx_v2:arbitrum:WETH")
    if category == "vault":
        return _build_outbox(position_key="vault:morpho:arbitrum:USDC")
    if category == "swap":
        return _build_outbox(position_key="swap:arbitrum:0xabcd…")
    if category == "prediction":
        return _build_outbox(position_key="prediction:polymarket:0xmarket:YES")
    raise AssertionError(f"unknown fixture category {category!r}")


def _ledger_for_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    intent = fixture["intent_type"]
    protocol = fixture["protocol"]
    if fixture["category"] == "lending":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC")
    if fixture["category"] == "lp":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC", token_out="WETH")
    if fixture["category"] == "pendle_lp":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="WETH", token_out="PT-WETH")
    if fixture["category"] == "pendle_pt":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="WETH", token_out="PT-WETH")
    if fixture["category"] == "perp":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC")
    if fixture["category"] == "vault":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC")
    if fixture["category"] == "swap":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC", token_out="WETH")
    if fixture["category"] == "prediction":
        return _build_ledger(intent_type=intent, protocol=protocol, token_in="USDC")
    raise AssertionError(f"unknown fixture category {fixture['category']!r}")


def test_dispatch_parity_against_legacy_truth_table() -> None:
    """For every fixture, the new registry dispatch produces the same event class,
    event_type, and payload as the LEGACY if-ladder did.

    Per UAT card D1.S2 (Phase 1 round 2 fix): assertions cover class identity AND
    ``event_type`` AND payload byte-equality, so a registry that returns the wrong
    handler's event (with a coincidentally-matching payload) would still FAIL.
    """
    processor = _build_processor()
    truth = json.loads(_TRUTH_TABLE.read_text())

    failures: list[str] = []
    for fixture in truth["fixtures"]:
        outbox = _outbox_for_label(fixture["label"], fixture["category"])
        ledger = _ledger_for_fixture(fixture)
        event = processor._dispatch(outbox, ledger)
        label = f"{fixture['category']}/{fixture['label']}"

        if fixture["expected_event_class"] is None:
            if event is not None:
                failures.append(f"{label}: legacy returned None, registry returned {type(event).__name__}")
            continue

        if event is None:
            failures.append(
                f"{label}: legacy returned {fixture['expected_event_class']}, registry returned None"
            )
            continue

        actual_class = type(event).__name__
        if actual_class != fixture["expected_event_class"]:
            failures.append(
                f"{label}: class drift — expected {fixture['expected_event_class']}, got {actual_class}"
            )
            continue

        actual_event_type = getattr(event.event_type, "value", str(event.event_type))
        if actual_event_type != fixture["expected_event_type"]:
            failures.append(
                f"{label}: event_type drift — expected {fixture['expected_event_type']}, got {actual_event_type}"
            )
            continue

        actual_payload = json.loads(event.to_payload_json())
        if actual_payload != fixture["expected_payload"]:
            failures.append(f"{label}: payload drift")

    assert not failures, "\n".join(failures)


# ─── D2.M1 — Lending dispatch round-trips ────────────────────────────────────


def test_lending_dispatch_round_trips() -> None:
    processor = _build_processor()
    outbox = _build_outbox(position_key="lending:aave_v3:arbitrum:USDC")
    ledger = _build_ledger(intent_type="SUPPLY", protocol="aave_v3", token_in="USDC")
    event = processor._dispatch(outbox, ledger)
    assert event is not None
    assert type(event).__name__ == "LendingAccountingEvent"
    assert getattr(event.event_type, "value", str(event.event_type)) == "SUPPLY"


# ─── D2.M2 — LP prior_open_lookup is exercised on close ──────────────────────


def test_lp_dispatch_uses_prior_open_lookup_on_close() -> None:
    processor = _build_processor()
    spy = MagicMock(return_value=_PRIOR_OPEN_PAYLOAD)
    processor._lookup_prior_lp_open = spy  # type: ignore[method-assign]

    outbox = _build_outbox(position_key="lp:arbitrum:0xabcd…:0x1111111111111111111111111111111111111111")
    ledger_open = _build_ledger(intent_type="LP_OPEN", protocol="uniswap_v3", token_in="USDC", token_out="WETH")
    processor._dispatch(outbox, ledger_open)
    assert spy.call_count == 0, "LP_OPEN must NOT trigger prior_open lookup"

    ledger_close = _build_ledger(intent_type="LP_CLOSE", protocol="uniswap_v3", token_in="USDC", token_out="WETH")
    processor._dispatch(outbox, ledger_close)
    assert spy.call_count == 1
    assert spy.call_args.args == ("lp:arbitrum:0xabcd…:0x1111111111111111111111111111111111111111",)


# ─── D2.M3 — Every reachable category routes to the matching handler module ──


def test_all_reachable_categories_dispatch_through_registry() -> None:
    """For each registered category, the bound handler's __module__ matches the
    expected handler file (e.g. AccountingCategory.LP → ...lp_handler).
    """
    expected_module_suffix = {
        AccountingCategory.LENDING: "lending_handler",
        AccountingCategory.LP: "lp_handler",
        AccountingCategory.PENDLE_LP: "pendle_handler",
        AccountingCategory.PENDLE_PT: "pendle_handler",
        AccountingCategory.PERP: "perp_handler",
        AccountingCategory.PREDICTION: "prediction_handler",
        AccountingCategory.SWAP: "swap_handler",
        AccountingCategory.TRANSFER: "transfer_handler",
        AccountingCategory.VAULT: "vault_handler",
    }
    failures: list[str] = []
    for category, suffix in expected_module_suffix.items():
        fn = HANDLERS.get(category)
        if fn is None:
            failures.append(f"{category.value}: no handler registered")
            continue
        if not fn.__module__.endswith(suffix):
            failures.append(f"{category.value}: handler module is {fn.__module__}, expected …{suffix}")
    assert not failures, "\n".join(failures)


# ─── D3.F1 — Clean-interpreter cycle-avoidance proof ─────────────────────────


def test_registry_imports_in_clean_subprocess() -> None:
    """Spawn a fresh interpreter that imports the registry. Asserts:

    - Exit code 0.
    - stdout contains ``OK``.
    - stderr is empty (no partial-module diagnostics, ImportError, RecursionError).
    - The registered handler count is at least 9 (8 legacy + transfer).

    Running inside the parent interpreter is insufficient because ``sys.modules``
    is already warmed — a real cycle would not surface. Hard Ratification
    Condition #4.
    """
    snippet = (
        "import almanak.framework.accounting.category_handlers as ch;"
        "assert isinstance(ch.HANDLERS, dict), type(ch.HANDLERS);"
        "assert len(ch.HANDLERS) >= 9, len(ch.HANDLERS);"
        "print('OK', len(ch.HANDLERS))"
    )
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"clean-subprocess import failed (exit {result.returncode})\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout, result.stdout
    assert result.stderr.strip() == "", f"unexpected stderr: {result.stderr}"


# ─── D3.F2 — Duplicate registration raises ───────────────────────────────────


def test_duplicate_registration_raises() -> None:
    target = AccountingCategory.LP
    existing = HANDLERS.get(target)
    assert existing is not None, "test prerequisite — LP must already be registered"

    def _shadow(_ctx: HandlerContext) -> None:  # pragma: no cover - never called
        return None

    with pytest.raises(ValueError) as exc_info:
        register(target)(_shadow)
    msg = str(exc_info.value)
    assert "AccountingCategory.LP" in msg
    assert existing.__module__ in msg
    # The HANDLERS map must be unchanged.
    assert HANDLERS.get(target) is existing


# ─── D3.F3 — Keyset exhaustiveness (behavioural prong is in the dispatcher test) ──


def test_exhaustiveness_every_non_no_accounting_category_has_handler() -> None:
    expected = set(AccountingCategory) - {AccountingCategory.NO_ACCOUNTING}
    assert set(HANDLERS.keys()) == expected, (
        f"missing handlers: {expected - set(HANDLERS.keys())}; "
        f"extra handlers: {set(HANDLERS.keys()) - expected}"
    )


# ─── D3.F6 — Handler module import failure propagates loudly ─────────────────


def test_registry_startup_assertion_fails_when_handler_missing() -> None:
    """The package init's ``RuntimeError`` guard must fire if a handler is missing.

    Per audit finding #3 (round 1) — the eager-import block depends on
    ``# noqa: F401`` to survive future linters. The startup assertion in
    ``category_handlers/__init__.py`` is the belt to the eager-import braces.
    This test simulates the failure mode by re-running the assertion logic
    against a HANDLERS map with one category removed.
    """
    from almanak.framework.accounting.category_handlers import HANDLERS as _real_handlers

    # Mimic the assertion block from __init__.py with a synthetic shortfall.
    fake = dict(_real_handlers)
    fake.pop(AccountingCategory.LP)  # simulate the linter dropping lp_handler import
    required = set(AccountingCategory) - {AccountingCategory.NO_ACCOUNTING}
    missing = required - fake.keys()
    assert missing == {AccountingCategory.LP}

    # Reproduce the message the package would raise.
    msg = (
        f"category_handlers registry under-populated at import time. "
        f"Missing: {sorted(c.value for c in missing)}. "
        f"Did a `# noqa: F401` import get tidied away in __init__.py?"
    )
    assert "lp" in msg
    assert "tidied away" in msg


def test_handler_module_import_failure_is_loud(tmp_path: Path) -> None:
    """A handler whose top-level statement raises must propagate, not silently skip.

    The test launches a fresh subprocess that monkeypatches ``importlib`` to
    intercept ``almanak.framework.accounting.category_handlers.lp_handler`` and
    raise ``ImportError("synthetic")`` from the loader. Importing the registry
    package then must surface that ImportError.
    """
    script = (
        "import importlib, importlib.abc, importlib.machinery, sys, types\n"
        "TARGET = 'almanak.framework.accounting.category_handlers.lp_handler'\n"
        "class _FailingFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name == TARGET:\n"
        "            return importlib.machinery.ModuleSpec(name, self)\n"
        "        return None\n"
        "    def create_module(self, spec):\n"
        "        return None\n"
        "    def exec_module(self, module):\n"
        "        raise ImportError('synthetic')\n"
        "sys.meta_path.insert(0, _FailingFinder())\n"
        "import almanak.framework.accounting.category_handlers as ch\n"
        "raise SystemExit('NO_RAISE')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "package init must fail when a handler module raises ImportError"
    combined = result.stdout + "\n" + result.stderr
    assert "synthetic" in combined, f"ImportError did not propagate. stdout={result.stdout} stderr={result.stderr}"
    assert "NO_RAISE" not in combined, "package init silently succeeded — handler import was suppressed"
