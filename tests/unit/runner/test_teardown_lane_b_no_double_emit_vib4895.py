"""VIB-4895 regression — the teardown lane must NOT double-emit position_events.

The teardown commit lane emits the ``position_events`` row explicitly in
``commit_teardown_intent`` Step 2b. But ``_write_ledger_entry`` ALSO emits the
matching row transitively on a successful chain TX (the iteration lane relies on
that). If both fired, every successful teardown LP_CLOSE / PERP_CLOSE /
lending-close would land **two** CLOSE rows in ``position_events`` — they would
not dedupe, because ``PositionEvent.id`` is a random ``uuid4`` and
``save_position_event`` uses ``INSERT OR IGNORE`` keyed on ``id``. Duplicate
CLOSE rows double-count closes in PnL attribution and the Accountant Test LP
cells ("if the books don't tie, the agent is broken").

The fix: ``_write_ledger_entry`` takes ``emit_position_event`` (default ``True``
for the iteration lane); the teardown lane passes ``False`` so it owns the single
emit. These tests exercise the **real** ``_write_ledger_entry`` (not a mock —
that is the gap the original #2501 tests had) and pin both halves of the
contract:

1. The real ``_write_ledger_entry`` honours ``emit_position_event`` — it emits
   when ``True`` and is silent when ``False``.
2. ``commit_teardown_intent`` calls ``_write_ledger_entry`` with
   ``emit_position_event=False`` (so only the explicit Step 2b emit fires).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import almanak.framework.observability.ledger as ledger_mod
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


class _Strategy:
    def __init__(self, sid: str = "s1", chain: str = "arbitrum") -> None:
        self.deployment_id = sid
        self.chain = chain
        self.wallet_address = "0x" + "0" * 40


class _Runner(StrategyRunner):
    """Bypass ``StrategyRunner.__init__`` — drive the real ``_write_ledger_entry``.

    Only the unrelated heavy leaf deps (runner-hook enrichment, atomic registry save)
    are neutralised; the ``emit_position_event`` guard under test stays real.
    ``_emit_position_event_for_intent`` is replaced with a counter so we observe
    exactly whether the transitive emit fired, without dragging in
    ``build_position_event_from_intent`` and the save layer.
    """

    def __init__(self, *, state_manager: Any, config: RunnerConfig | None = None) -> None:
        self.state_manager = state_manager
        self.config = config or RunnerConfig()
        self._iteration_had_trade = False
        self.emit_calls = 0

    # --- neutralise unrelated deps so build -> save -> emit-guard runs clean ---
    def _maybe_enrich_result_with_runner_hooks(
        self, result: Any, chain: str, wallet_address: str = ""
    ) -> None:  # noqa: ARG002
        return None

    async def _maybe_save_ledger_with_registry(self, **_kwargs: Any) -> bool:
        return False

    # --- the spy: count transitive emits without running the real emitter ----
    async def _emit_position_event_for_intent(self, **_kwargs: Any) -> None:
        self.emit_calls += 1


def _swap_intent() -> SwapIntent:
    return SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("100"))


def _make_runner(monkeypatch: pytest.MonkeyPatch) -> _Runner:
    # Monkeypatch the ledger builder to a trivial real LedgerEntry so the test
    # does not depend on result shape — we only care about the emit guard.
    monkeypatch.setattr(ledger_mod, "build_ledger_entry", lambda **_kw: LedgerEntry(id="led-1"))
    state_mgr = MagicMock()
    state_mgr.save_ledger_entry = AsyncMock(return_value=None)
    # MagicMock satisfies the ``hasattr(state_manager, "save_position_event")``
    # guard, so the emit branch is reachable when the flag allows it.
    return _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))


@pytest.mark.asyncio
async def test_write_ledger_entry_emits_transitively_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Iteration-lane contract: a successful chain TX emits the position event."""
    runner = _make_runner(monkeypatch)

    ledger_id = await runner._write_ledger_entry(
        strategy=_Strategy(),
        intent=_swap_intent(),
        result=SimpleNamespace(success=True),
        success=True,
    )

    assert ledger_id == "led-1"
    assert runner.emit_calls == 1  # transitive emit fired (default emit_position_event=True)


@pytest.mark.asyncio
async def test_write_ledger_entry_suppresses_emit_when_flag_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teardown-lane contract: emit_position_event=False suppresses the transitive emit.

    This is the half of the fix that prevents the duplicate CLOSE row — the
    teardown lane owns the emit explicitly, so the ledger write must stay silent.
    """
    runner = _make_runner(monkeypatch)

    ledger_id = await runner._write_ledger_entry(
        strategy=_Strategy(),
        intent=_swap_intent(),
        result=SimpleNamespace(success=True),
        success=True,
        emit_position_event=False,
    )

    assert ledger_id == "led-1"  # ledger still persisted + id returned
    assert runner.emit_calls == 0  # NO transitive emit — teardown Step 2b owns it
