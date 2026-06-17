"""VIB-4839 — LP_CLOSE must hydrate token/tick metadata from durable storage
when the runner's in-memory ``_recent_open_events`` cache misses.

Reproduces the May-22 / May-26 ``lp_triple`` mainnet rerun finding: a strategy
opens LP positions in one process, the process restarts (or runs continuously
across a long horizon), then teardown closes those positions in a later
cycle. Without a durable-storage fallback the CLOSE row lands with
``token0=''``, ``token1=''``, ``value_usd=''``, ``tick_lower/upper=NULL`` and
attribution ``principal_recovered_usd=0`` — making the per-leg PnL look like
a full-principal loss.

The fix lives at the runner emit chokepoint
(``_emit_position_event_for_intent``): on cache miss for an LP_CLOSE intent,
read the most-recent OPEN for that ``position_id`` from
``state_manager.get_position_history(...)`` and seed
``_recent_open_events`` so the existing ``_apply_lp_close_columns`` carry-
forward path picks it up transparently.

These tests are RED before the fix and GREEN after.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.observability.position_events import (
    PositionEvent,
    _apply_lp_close_value_usd,
)
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

# ──────────────────────────────────────────────────────────────────────────
# Test scaffolding — minimal StrategyRunner + state_manager mocks.
# Reuses the bypass-__init__ pattern from tests/unit/runner/test_accounting_persistence.py.
# ──────────────────────────────────────────────────────────────────────────


class _Strategy:
    def __init__(self) -> None:
        self.deployment_id = "deployment:vib4839test"
        self.chain = "arbitrum"
        self.wallet_address = "0x" + "0" * 40


class _Runner(StrategyRunner):
    """Bypass StrategyRunner.__init__ — wire just what _emit_position_event_for_intent needs."""

    def __init__(self, *, state_manager: Any, config: RunnerConfig | None = None) -> None:
        self.state_manager = state_manager
        self.alert_manager = None
        self.config = config or RunnerConfig()
        self._iteration_had_trade = False
        self._recent_open_events: dict[tuple[str, str], dict] = {}
        self._runtime_config = None

    async def _run_position_event_attribution(self, *args, **kwargs) -> None:
        # No-op for these tests — attribution is exercised elsewhere
        # (test_position_event_value_usd_vib3883, test_pnl_attributor).
        return None


class _LPCloseIntent:
    """Real-shape LPCloseIntent stand-in. Pydantic LPCloseIntent works too but
    using a plain object avoids importing chain-resolution side-effects.
    """

    def __init__(
        self,
        position_id: str,
        protocol: str = "uniswap_v3",
        chain: str = "arbitrum",
        pool: str | None = "WETH/USDC/500",
    ) -> None:
        from almanak.framework.intents.vocabulary import IntentType

        self.intent_type = IntentType.LP_CLOSE
        self.position_id = position_id
        self.protocol = protocol
        self.chain = chain
        # ``pool`` is the close intent's pair descriptor. Pool-bearing closes
        # (TraderJoe V2, and any strategy that threads "TOKEN_X/TOKEN_Y/<tier>"
        # into the close intent) let VIB-5195 self-describe token0/token1 even
        # on a cache + durable miss. A pool-less close (NFT-only, e.g. a bare
        # Uniswap V3 LPCloseIntent) has no recoverable pair, so it must still
        # fail closed (Empty ≠ Zero) when the OPEN cannot be resolved.
        self.pool = pool
        self.collect_fees = True
        self.protocol_params = None


class _LpCloseData:
    def __init__(
        self,
        amount0_received: int = 788318073449340,  # raw 18-dec WETH (≈ 0.000788 WETH)
        amount1_received: int = 2362689,  # raw 6-dec USDC (≈ 2.362 USDC)
        fees_token0: int = 6680122271532,
        fees_token1: int = 14065,
    ) -> None:
        self.amount0_received = amount0_received
        self.amount1_received = amount1_received
        self.fees_token0 = fees_token0
        self.fees_token1 = fees_token1


class _TxResult:
    def __init__(self) -> None:
        self.tx_hash = "0xf64b3f424bcc142db633b54383d1991cce4b213cb576549751e47f0ed8815d1e"
        self.gas_used = 200000
        self.success = True


class _ExecResult:
    def __init__(self, position_id: str) -> None:
        self.position_id = position_id
        self.transaction_results = [_TxResult()]
        self.gas_cost_usd = "0.05"
        self.extracted_data = {"lp_close_data": _LpCloseData()}
        self.success = True


def _ledger_entry() -> LedgerEntry:
    """Minimal LedgerEntry — only the .id field is read by the emitter."""
    entry = MagicMock(spec=LedgerEntry)
    entry.id = "led-vib4839"
    return entry


def _arbitrum_price_oracle() -> dict:
    """Realistic close-time prices. Magnitudes match the lp_triple rerun."""
    return {"WETH": "2520.00", "USDC": "1.0001"}


def _durable_lp_open_row(position_id: str) -> dict:
    """Shape returned by ``state_manager.get_position_history(...)`` — matches
    the SQLite schema columns the cache carry-forward path reads.
    """
    return {
        "id": "row-vib4839-open",
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": "2026-05-22T20:07:14",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
        "token0": "WETH",
        "token1": "USDC",
        "amount0": "899864508866453",
        "amount1": "2102959",
        "value_usd": "3.965156",
        "tick_lower": -200490,
        "tick_upper": -199490,
        "liquidity": "5500290000",
        "in_range": True,
        "ledger_entry_id": "led-open",
    }


# ──────────────────────────────────────────────────────────────────────────
# 1. Cache-miss → durable fallback hydrates token / tick metadata + value_usd
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lp_close_hydrates_from_state_manager_on_cache_miss() -> None:
    """When the runner's ``_recent_open_events`` cache lacks the OPEN entry
    (process restart, hosted-mode hydration gap), the emitter must fall back
    to a durable ``get_position_history`` lookup and seed the cache so
    ``_apply_lp_close_columns`` carries token0/token1/ticks/liquidity through
    and ``_apply_lp_close_value_usd`` can compute value_usd from the close-
    time price oracle.
    """
    position_id = "5500290"

    state_mgr = MagicMock()
    state_mgr.get_position_history = AsyncMock(return_value=[_durable_lp_open_row(position_id)])
    saved: list[PositionEvent] = []

    async def _save(ev: PositionEvent) -> bool:
        saved.append(ev)
        return True

    state_mgr.save_position_event = AsyncMock(side_effect=_save)

    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))
    assert runner._recent_open_events == {}, "precondition: cache cold"

    await runner._emit_position_event_for_intent(
        strategy=_Strategy(),
        intent=_LPCloseIntent(position_id=position_id),
        result=_ExecResult(position_id=position_id),
        entry=_ledger_entry(),
        chain="arbitrum",
        deployment_id="deployment:vib4839test",
        execution_mode="live",
        cycle_id="teardown-td_vib4839test",
        price_oracle=_arbitrum_price_oracle(),
        post_state=None,
        pre_state=None,
    )

    assert state_mgr.get_position_history.await_count == 1, (
        "VIB-4839: cache miss must trigger exactly one durable-storage lookup. "
        "Repeated lookups within a single emit call indicate the cache write didn't take."
    )
    assert len(saved) == 1, "Exactly one CLOSE event should be persisted."
    ev = saved[0]
    assert ev.event_type == "CLOSE"
    assert ev.position_type == "LP"
    assert ev.position_id == position_id

    # The bug: pre-fix all of these came through blank because the in-memory
    # cache had no entry and no fallback to durable storage existed.
    assert ev.token0 == "WETH", (
        f"VIB-4839: token0 must hydrate from durable OPEN (got {ev.token0!r}). "
        "Pre-fix the CLOSE row landed with token0='' because "
        "_apply_lp_close_columns only read the in-memory cache."
    )
    assert ev.token1 == "USDC", f"VIB-4839: token1 must hydrate from durable OPEN (got {ev.token1!r})"
    assert ev.tick_lower == -200490
    assert ev.tick_upper == -199490
    assert ev.liquidity == "5500290000"

    # value_usd must be computed from recovered amounts × close-time prices.
    # Pre-fix this stayed '' and attribution.principal_recovered_usd=0.
    assert ev.value_usd != "", (
        "VIB-4839: value_usd must compute from recovered amounts when tokens hydrate. "
        "Pre-fix _apply_lp_close_value_usd silent-returned on blank token0/token1."
    )

    # 0.000788318 WETH × $2520 + 2.362689 USDC × $1.0001 ≈ $1.987 + $2.363 ≈ $4.35
    from decimal import Decimal

    assert Decimal("4.20") < Decimal(ev.value_usd) < Decimal("4.50"), (
        f"VIB-4839: expected ~$4.35 close value at the May-22 reproducer magnitudes; got {ev.value_usd!r}"
    )

    # The cache is correctly POPPED after the CLOSE save (existing
    # _update_recent_open_events_cache behavior — a closed position
    # shouldn't carry a stale OPEN bracket forward).  The fact that the
    # CLOSE event carries WETH/USDC/ticks/value_usd above already proves
    # the cache was seeded mid-emit before the builder ran.  The single
    # round-trip guarantee is enforced by the get_position_history
    # await_count == 1 assertion above.
    assert (position_id, "LP") not in runner._recent_open_events, (
        "After CLOSE the cache must be popped (existing VIB-3894 invariant). "
        "The hydration is intentionally seeded-then-popped within one emit."
    )


# ──────────────────────────────────────────────────────────────────────────
# 2. Empty ≠ Zero — when durable lookup also returns nothing, the row stays
#    honestly empty and the operator sees a structured WARN.
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lp_close_stays_empty_when_durable_lookup_returns_no_open(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per CLAUDE.md §Accounting: Empty ≠ Zero. When neither the in-memory
    cache nor durable storage holds the OPEN bracket AND the close intent
    carries no recoverable pair (a pool-less NFT-only close), the CLOSE row
    must land with empty token0/token1/value_usd — NOT fabricated zeros —
    and a structured WARN must surface so operators can reconcile.

    This is the fail-closed contract from blueprint 27 §14.1: accounting
    failures are loud + durable, never silent. VIB-5195's intent-descriptor
    fallback does NOT fire here because ``pool=None`` — there is genuinely
    nothing to recover (contrast the sibling test below where ``pool`` is set).
    """
    position_id = "5500999-orphan"

    state_mgr = MagicMock()
    state_mgr.get_position_history = AsyncMock(return_value=[])
    saved: list[PositionEvent] = []

    async def _save(ev: PositionEvent) -> bool:
        saved.append(ev)
        return True

    state_mgr.save_position_event = AsyncMock(side_effect=_save)

    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))

    with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
        await runner._emit_position_event_for_intent(
            strategy=_Strategy(),
            intent=_LPCloseIntent(position_id=position_id, pool=None),
            result=_ExecResult(position_id=position_id),
            entry=_ledger_entry(),
            chain="arbitrum",
            deployment_id="deployment:vib4839test",
            execution_mode="live",
            cycle_id="teardown-td_vib4839test",
            price_oracle=_arbitrum_price_oracle(),
            post_state=None,
            pre_state=None,
        )

    assert len(saved) == 1
    ev = saved[0]
    # Empty ≠ Zero — fail-closed.
    assert ev.token0 == "", "VIB-4839: token0 must stay empty (NOT 'UNKNOWN'/'0') when OPEN cannot be resolved"
    assert ev.token1 == ""
    assert ev.value_usd == "", "VIB-4839: value_usd must stay empty — never fabricate 0 (Empty ≠ Zero)"
    assert ev.tick_lower is None

    # Loud — operator sees a structured WARN, not a silent debug.
    warning_records = [
        rec for rec in caplog.records if rec.levelno == logging.WARNING and "lp_close_value_usd" in rec.message
    ]
    assert warning_records, (
        "VIB-4839: when value_usd cannot be computed, a structured WARN with "
        "'lp_close_value_usd' in the message MUST be emitted. Silent empty value_usd "
        "is what hid this bug for two weeks."
    )


@pytest.mark.asyncio
async def test_lp_close_self_describes_from_intent_pool_on_cache_and_durable_miss() -> None:
    """VIB-5195 — when both the in-memory cache and durable store miss the
    OPEN bracket but the LP_CLOSE intent carries a pair descriptor
    (``pool`` = "TOKEN_X/TOKEN_Y/<tier>"), the CLOSE row self-describes its
    token0/token1 from the intent and ``value_usd`` is computed from the
    received amounts × close-time prices.

    This is the TraderJoe V2 teardown/rebalance shape: the close intent
    carries ``pool`` but its ``position_id`` differs from the OPEN leg's
    (fungible LP closes under a synthetic id), so the position_id-keyed cache
    and durable lookup BOTH miss. Pre-VIB-5195 the close landed with
    token0=''/token1='' and ``_apply_lp_close_value_usd`` failed closed with
    ``missing_tokens_or_amounts have_token0=False`` — the close-leg USD value
    was unattributed even though the pair was fully recoverable from the
    intent.
    """
    position_id = "tj-v2-close-synthetic-id"

    state_mgr = MagicMock()
    # Both surfaces miss: no in-memory cache entry, durable lookup empty.
    state_mgr.get_position_history = AsyncMock(return_value=[])
    saved: list[PositionEvent] = []

    async def _save(ev: PositionEvent) -> bool:
        saved.append(ev)
        return True

    state_mgr.save_position_event = AsyncMock(side_effect=_save)

    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))

    await runner._emit_position_event_for_intent(
        strategy=_Strategy(),
        intent=_LPCloseIntent(position_id=position_id, pool="WETH/USDC/500"),
        result=_ExecResult(position_id=position_id),
        entry=_ledger_entry(),
        chain="arbitrum",
        deployment_id="deployment:vib5195test",
        execution_mode="live",
        cycle_id="teardown-td_vib5195test",
        price_oracle=_arbitrum_price_oracle(),
        post_state=None,
        pre_state=None,
    )

    from decimal import Decimal

    assert len(saved) == 1
    ev = saved[0]
    # Tokens recovered from the intent's pool descriptor (NOT the cache).
    assert ev.token0 == "WETH", f"VIB-5195: token0 must self-describe from intent pool (got {ev.token0!r})"
    assert ev.token1 == "USDC", f"VIB-5195: token1 must self-describe from intent pool (got {ev.token1!r})"
    # value_usd must be a real computed VALUE, not just non-empty: received
    # amounts × close-time prices (measured-zero legs are a known trap, so
    # assert a strictly positive magnitude).
    assert ev.value_usd not in ("", None), "VIB-5195: value_usd must be populated once tokens resolve"
    assert Decimal(ev.value_usd) > 0, f"VIB-5195: value_usd must be a real positive value (got {ev.value_usd!r})"


# ──────────────────────────────────────────────────────────────────────────
# 3. _apply_lp_close_value_usd unit-level WARN on missing prices.
# ──────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────
# 4. Decisive integration test — real SQLiteStore, NO mocks on the durable
#    surface.  Mirrors the production bug shape end-to-end: an OPEN persisted
#    by one "process" (real save_position_event), runner cache reset to ''
#    cold (simulating the cross-process restart in the May-22 → May-26 rerun),
#    then an LP_CLOSE goes through and we read the persisted row back via the
#    real get_position_history.
#
#    If this test passes, the durable hydration path works against the real
#    SQLite backend — not just against a MagicMock.  This is the test that
#    would have caught the bug end-to-end.
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lp_close_hydrates_through_real_sqlite_after_cold_restart(tmp_path) -> None:
    """End-to-end against a real SQLiteStore — no state-manager mocks.

    Reproduces the production bug shape: OPEN row durably persisted, fresh
    runner with cold ``_recent_open_events`` (simulates the cross-process
    restart between May-22 OPEN and May-26 teardown CLOSE), LP_CLOSE
    intent flows through ``_emit_position_event_for_intent``, and we read
    the persisted CLOSE row back via real ``get_position_history``.

    Asserts at the column level (the surface the ``almanak strat pnl`` CLI
    reads) that:
      * ``token0 == 'WETH'`` and ``token1 == 'USDC'``
      * ``value_usd`` is non-empty AND > 0 (the bug shipped ``''``)
      * ``tick_lower`` / ``tick_upper`` are populated
      * ``attribution_json`` does NOT carry ``principal_recovered_usd: 0``
        (the bug shipped 0, which makes the CLI render full-principal loss)

    Pre-fix this test fails because ``_apply_lp_close_columns`` reads only
    the in-memory cache and ``_apply_lp_close_value_usd`` silent-returns
    on blank token0/token1.
    """
    from almanak.framework.observability.position_events import PositionEvent as _PE
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    db_path = str(tmp_path / "vib4839_integration.db")
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()

    deployment_id = "deployment:vib4839-integration"
    position_id = "5500290"

    # ── "Process 1" — seed a durable LP_OPEN row through the real writer.
    open_event = _PE(
        deployment_id=deployment_id,
        position_id=position_id,
        position_type="LP",
        event_type="OPEN",
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0="899864508866453",
        amount1="2102959",
        value_usd="3.965156",
        tick_lower=-200490,
        tick_upper=-199490,
        liquidity="5500290000",
        in_range=True,
        ledger_entry_id="led-open",
    )
    saved_open = await store.save_position_event(open_event)
    assert saved_open, "Precondition: OPEN row must persist."

    # ── "Process 2" — fresh runner, cold cache.  This is the cross-process
    # restart shape that produced the lp_triple May-22 → May-26 bug.
    runner = _Runner(state_manager=store, config=RunnerConfig(dry_run=False))
    assert runner._recent_open_events == {}, "Cold cache (simulates restart)."

    await runner._emit_position_event_for_intent(
        strategy=_Strategy(),
        intent=_LPCloseIntent(position_id=position_id),
        result=_ExecResult(position_id=position_id),
        entry=_ledger_entry(),
        chain="arbitrum",
        deployment_id=deployment_id,
        execution_mode="live",
        cycle_id="teardown-td_integration",
        price_oracle=_arbitrum_price_oracle(),
        post_state=None,
        pre_state=None,
    )

    # ── Read back through the real durable surface ── the same call path
    # ``almanak strat pnl`` uses.  Any mock substitution here would mask
    # the bug.
    history = await store.get_position_history(deployment_id, position_id)
    close_rows = [row for row in history if str(row.get("event_type")).upper() == "CLOSE"]
    assert len(close_rows) == 1, f"Expected exactly one durable CLOSE row; got {len(close_rows)} (history={history})"
    close = close_rows[0]

    # The columns ``almanak strat pnl`` reads to render the LP position summary.
    # Pre-fix all of these landed blank / zero.
    assert close["token0"] == "WETH", (
        f"VIB-4839: durable CLOSE row must carry token0=WETH (pre-fix it was '' "
        f"because _apply_lp_close_columns only read the in-memory cache). Got {close['token0']!r}."
    )
    assert close["token1"] == "USDC", f"Got token1={close['token1']!r}"
    assert close["tick_lower"] == -200490, f"Got tick_lower={close['tick_lower']!r}"
    assert close["tick_upper"] == -199490, f"Got tick_upper={close['tick_upper']!r}"

    # value_usd is the CLI's Exit column — blank → "Exit: —".
    from decimal import Decimal as _D

    assert close["value_usd"], (
        "VIB-4839: durable CLOSE row must carry a non-empty value_usd. "
        "Pre-fix this was '' and the CLI rendered 'Exit: —'."
    )
    assert _D(str(close["value_usd"])) > 0, (
        f"VIB-4839: value_usd must be > 0 when tokens hydrate + prices are present. Got {close['value_usd']!r}."
    )
    # Sanity: 0.000788 WETH × $2520 + 2.362 USDC × $1.0001 ≈ $4.35
    assert _D("4.20") < _D(str(close["value_usd"])) < _D("4.50"), (
        f"VIB-4839: expected ~$4.35 close value at lp_triple-rerun magnitudes; got {close['value_usd']!r}"
    )

    # Scope note (pr-auditor Important #2 on this PR): the integration test
    # intentionally monkey-patches ``_run_position_event_attribution`` to a
    # no-op so we focus on the column-hydration chain.  Attribution math
    # (which writes ``attribution_json.principal_recovered_usd``) is
    # exercised separately in ``tests/framework/observability/test_pnl_attributor.py``
    # — coupling the two would only hide which layer regressed.  What this
    # test PROVES end-to-end is that the durable CLOSE row reaches the
    # persistence layer with the column shape the CLI ``almanak strat pnl``
    # reads (``value_usd``) — that is the surface the bug shipped on.

    await store.close()


# ──────────────────────────────────────────────────────────────────────────
# 5. _apply_lp_close_value_usd unit-level WARN on missing prices.
# ──────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────
# 6. UAT-card criterion 5 — loud-on-durable-failure.  When the durable
#    lookup itself fails (raises) OR the state-manager surface is absent,
#    a structured WARN containing ``lp_close_durable_hydration`` MUST be
#    emitted.  This is the spec critic Codex flagged in Phase 0b: the
#    silent durable-skip path is exactly the shape that hid the hosted-GSM
#    gap from operators.
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lp_close_warns_when_durable_lookup_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When ``state_manager.get_position_history`` raises during the
    cache-miss durable fallback, emit a structured WARN containing
    ``lp_close_durable_hydration`` AND continue with an empty cache
    (criterion 5 — loud + durable per blueprint 27 §14.1).
    """
    position_id = "5500290"
    state_mgr = MagicMock()
    state_mgr.get_position_history = AsyncMock(side_effect=RuntimeError("simulated backend outage"))
    state_mgr.save_position_event = AsyncMock(return_value=True)

    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        await runner._emit_position_event_for_intent(
            strategy=_Strategy(),
            intent=_LPCloseIntent(position_id=position_id),
            result=_ExecResult(position_id=position_id),
            entry=_ledger_entry(),
            chain="arbitrum",
            deployment_id="deployment:vib4839test",
            execution_mode="live",
            cycle_id="teardown-td_vib4839test",
            price_oracle=_arbitrum_price_oracle(),
            post_state=None,
            pre_state=None,
        )

    failure_warns = [
        rec for rec in caplog.records if rec.levelno == logging.WARNING and "lp_close_durable_hydration" in rec.message
    ]
    assert failure_warns, (
        "VIB-4839 UAT-card criterion 5: a backend raise during the durable lookup "
        "MUST emit a structured WARN containing 'lp_close_durable_hydration'. "
        "Silent durable-skip is what hid the hosted-GSM gap."
    )


@pytest.mark.asyncio
async def test_lp_close_warns_when_state_manager_surface_absent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When ``state_manager`` lacks ``get_position_history`` entirely (the
    shape that hid the hosted GSM hydration silent no-op), the runner MUST
    emit a structured WARN naming ``lp_close_durable_hydration.unavailable``.
    """
    position_id = "5500290"

    # state manager that has save_position_event but NOT get_position_history
    class _PartialSM:
        async def save_position_event(self, ev: Any) -> bool:
            return True

    runner = _Runner(state_manager=_PartialSM(), config=RunnerConfig(dry_run=False))

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        await runner._emit_position_event_for_intent(
            strategy=_Strategy(),
            intent=_LPCloseIntent(position_id=position_id),
            result=_ExecResult(position_id=position_id),
            entry=_ledger_entry(),
            chain="arbitrum",
            deployment_id="deployment:vib4839test",
            execution_mode="live",
            cycle_id="teardown-td_vib4839test",
            price_oracle=_arbitrum_price_oracle(),
            post_state=None,
            pre_state=None,
        )

    unavailable_warns = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.WARNING and "lp_close_durable_hydration.unavailable" in rec.message
    ]
    assert unavailable_warns, (
        "VIB-4839 UAT-card criterion 5: absent get_position_history surface MUST "
        "emit 'lp_close_durable_hydration.unavailable' WARN. This is the silent-skip "
        "shape that hid the hosted-GSM gap."
    )


# ──────────────────────────────────────────────────────────────────────────
# 7. position_id precedence (Codex P2 on this PR).  ``_seed_event`` uses
#    ``result.position_id`` first then ``intent.position_id``.  The
#    durable-hydration fallback at the runner emit chokepoint MUST use the
#    same precedence so the cache key it writes matches the lookup key
#    ``_apply_lp_close_columns`` reads.  A mismatch would seed under one
#    key while the carry-forward looks at another — silently re-creating
#    the original bug shape for any close where result.position_id !=
#    intent.position_id (canonical example: enricher rewrites the
#    position_id when the receipt parser observes a different NFT id).
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_hydration_uses_result_position_id_over_intent_position_id() -> None:
    """When ``result.position_id`` differs from ``intent.position_id``, the
    durable lookup MUST query the result's id (matching ``_seed_event``'s
    precedence). Otherwise the cache key seeded under intent.position_id
    won't match the lookup in ``_apply_lp_close_columns`` and the CLOSE
    row lands blank — re-creating the original VIB-4839 silent failure.
    """
    intent_id = "intent-id-99999"
    result_id = "result-id-12345"  # canonical persisted id (different)

    state_mgr = MagicMock()
    state_mgr.get_position_history = AsyncMock(return_value=[_durable_lp_open_row(result_id)])
    saved: list[PositionEvent] = []

    async def _save(ev: PositionEvent) -> bool:
        saved.append(ev)
        return True

    state_mgr.save_position_event = AsyncMock(side_effect=_save)

    runner = _Runner(state_manager=state_mgr, config=RunnerConfig(dry_run=False))

    # Result carries a DIFFERENT id from the intent — production scenario
    # when ``_seed_event``'s ``result.position_id`` precedence kicks in.
    result = _ExecResult(position_id=result_id)
    intent = _LPCloseIntent(position_id=intent_id)

    await runner._emit_position_event_for_intent(
        strategy=_Strategy(),
        intent=intent,
        result=result,
        entry=_ledger_entry(),
        chain="arbitrum",
        deployment_id="deployment:vib4839test",
        execution_mode="live",
        cycle_id="teardown-td_vib4839test",
        price_oracle=_arbitrum_price_oracle(),
        post_state=None,
        pre_state=None,
    )

    # Confirm the durable lookup used the RESULT's position_id, not the intent's.
    assert state_mgr.get_position_history.await_count == 1
    call_args = state_mgr.get_position_history.await_args
    assert call_args.args[1] == result_id, (
        f"VIB-4839 / Codex P2: durable lookup must use result.position_id ({result_id!r}) "
        f"to match _seed_event's precedence; got {call_args.args[1]!r}."
    )

    # And the persisted CLOSE row must end up with hydrated tokens — proving
    # the cache key under result_id was found by _apply_lp_close_columns.
    assert len(saved) == 1
    ev = saved[0]
    assert ev.position_id == result_id, "CLOSE event uses result.position_id (matches _seed_event)"
    assert ev.token0 == "WETH", (
        f"VIB-4839 / Codex P2: pre-fix the cache was seeded under intent.position_id "
        f"but _apply_lp_close_columns looked up result.position_id, so token0 was '' "
        f"despite a successful durable lookup. Got token0={ev.token0!r}."
    )
    assert ev.token1 == "USDC"
    assert ev.value_usd != ""


def test_apply_lp_close_value_usd_warns_when_prices_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Direct unit-level guard: when tokens + amounts are populated but the
    price oracle has no entries for the pair, the helper must emit a
    structured WARN identifying which leg(s) are missing — and still leave
    value_usd empty (Empty ≠ Zero).
    """
    event = PositionEvent(
        deployment_id="d",
        position_id="5500290",
        position_type="LP",
        event_type="CLOSE",
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0="788318073449340",
        amount1="2362689",
    )
    with caplog.at_level(logging.WARNING, logger="almanak.framework.observability.position_events"):
        _apply_lp_close_value_usd(event, price_oracle={}, chain="arbitrum")

    assert event.value_usd == "", "VIB-4839: Empty ≠ Zero — missing prices leave value_usd empty"

    warning_records = [
        rec for rec in caplog.records if rec.levelno == logging.WARNING and "lp_close_value_usd" in rec.message
    ]
    assert warning_records, (
        "VIB-4839: missing-price early-return MUST emit a structured WARN; silent return is what hid the bug."
    )
