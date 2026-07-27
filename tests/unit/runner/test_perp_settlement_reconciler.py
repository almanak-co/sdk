"""VIB-3872 WI-3 — perp settlement reconciler + commit lane acceptance tests.

Covers the named WI-3 acceptance criteria (design §3 D1/D2):
- restart-kill / booked-exactly-once (deterministic id + DB re-derivation),
- drain-first ordering guard (mutation-resistant),
- live-mode failure semantics (catch boundary: loud, non-terminal, never halts),
- watch-set derivation from persisted async_orders MINUS terminal settlements.

The connector verdict resolution (gateway/web3/registry) is stubbed via
``_resolve_all_verdicts`` so these tests isolate the runner-side derivation,
commit, and catch-boundary logic with no sockets.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import PerpSettlementState, PerpSettlementVerdict
from almanak.connectors.gmx_v2.receipt_parser import PerpFillData
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.runner import perp_settlement_reconciler as psr
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind

_ORDER_KEY = "0x585d42d95b9a4e84e78d53073d85fa6e67304c119fc000a6052661068200f9cf"
_DEPLOYMENT = "deployment:abc123"


def _async_orders_json(order_key: str, *, is_long: bool = True) -> str:
    """The persisted extracted_data_json shape: async_orders as repr strings."""
    repr_str = (
        f"AsyncOrderData(protocol='gmx_v2', order_id='{order_key}', "
        f"status=<AsyncOrderStatus.PENDING: 'pending'>, kind=<AsyncOrderKind.INCREASE: 'increase'>, "
        f"market='0xmkt', collateral_token='0xusdc', is_long={is_long}, size_delta_usd=None)"
    )
    return json.dumps({"async_orders": [repr_str]})


def _ledger(entry_id: str, intent_type: str, order_key: str) -> LedgerEntry:
    """A real LedgerEntry (the object the commit lane consumes and the reconciler
    rebuilds from the gateway hydrate). ``_FakeStateManager`` serves its dict
    projections through the GatewayStateManager measured-read API."""
    return LedgerEntry(
        id=entry_id,
        deployment_id=_DEPLOYMENT,
        cycle_id="cyc-0",
        execution_mode="paper",
        intent_type=intent_type,
        protocol="gmx_v2",
        success=True,
        tx_hash="0xsubmit",
        chain="arbitrum",
        extracted_data_json=_async_orders_json(order_key),
    )


class _FakeWriter:
    def __init__(self, *, raise_live: bool = False) -> None:
        self.written: list[Any] = []
        self.raise_live = raise_live

    async def write(self, event: Any) -> bool:
        if self.raise_live:
            raise AccountingPersistenceError(AccountingWriteKind.ACCOUNTING, deployment_id=_DEPLOYMENT, message="boom")
        self.written.append(event)
        return True


class _FakeProcessor:
    def __init__(self, writer: _FakeWriter) -> None:
        self._writer = writer
        self.calls: list[str] = []

    async def drain_one(self, ledger_entry_id: str) -> bool:
        self.calls.append(f"drain:{ledger_entry_id}")
        return True


class _FakeStateManager:
    """Shape-faithful GatewayStateManager stand-in (VIB-6107).

    Serves the SAME measured-read API a real ``strat run`` uses — a sync list read
    returning ``(list[dict], measured)`` (the ``LedgerEntryInfo`` projection, which
    deliberately DROPS ``extracted_data_json`` so the reconciler must hydrate), an async
    ``get_ledger_entry_by_id`` returning the FULL row (with ``extracted_data_json``), and
    a sync measured accounting-events read. It does NOT expose ``get_ledger_entries`` /
    ``get_accounting_events`` — a revert to the old probe breaks these WI-3 acceptance
    tests, which is the point.
    """

    def __init__(self, ledgers: list[LedgerEntry], writer: _FakeWriter) -> None:
        self._full: list[dict[str, Any]] = [le.to_dict() for le in ledgers]
        self._by_id: dict[str, dict[str, Any]] = {d["id"]: d for d in self._full}
        self._writer = writer

    def read_accounting_events_measured(self, deployment_id: str, position_key: str | None = None):
        # Re-derive the terminal-settlement set from what the writer actually wrote,
        # so a second reconciler tick sees the freshly-booked row (restart-safety).
        rows = [
            {"ledger_entry_id": ev.identity.ledger_entry_id, "event_type": ev.event_type} for ev in self._writer.written
        ]
        return rows, True

    def read_ledger_entries_measured(self, deployment_id: str):
        # LedgerEntryInfo projection: NO extracted_data_json (mirrors the prod list read).
        info = [{k: v for k, v in d.items() if k != "extracted_data_json"} for d in self._full]
        return info, True

    async def get_ledger_entry_by_id(self, ledger_entry_id: str) -> dict[str, Any] | None:
        return self._by_id.get(ledger_entry_id)

    async def get_position_history(self, deployment_id: str, position_key: str):
        return []


def _runner(state_manager: _FakeStateManager, processor: _FakeProcessor) -> SimpleNamespace:
    return SimpleNamespace(
        state_manager=state_manager,
        _accounting_processor=processor,
        config=SimpleNamespace(chain="arbitrum"),
        _is_live_mode=lambda: False,
    )


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id=_DEPLOYMENT, chain="arbitrum", wallet_address="0xwallet")


def _executed_verdict(order_key: str = _ORDER_KEY) -> PerpSettlementVerdict:
    fill = PerpFillData(
        is_open=True,
        is_long=True,
        market="0xmkt",
        collateral_token="0xusdc",
        position_key="0xposkey",
        order_key=order_key,
        entry_price=Decimal("3000"),
        size_delta_usd=Decimal("2422.85"),
        position_fee_usd=Decimal("1.45"),
        funding_fee_usd=Decimal("0"),
        keeper_tx_hash="0xkeeper",
        block_number=123,
    )
    return PerpSettlementVerdict(
        order_key=order_key,
        state=PerpSettlementState.EXECUTED,
        terminal=True,
        fill_data=fill,
        keeper_tx_hash="0xkeeper",
    )


def _stub_resolver(monkeypatch, verdict: PerpSettlementVerdict) -> None:
    def _fake_resolve(*, gateway_client, chain, wallet_address, watch):  # noqa: ARG001
        # Only return a verdict when the order is actually in the derived watch set.
        key = (verdict.state, str(verdict.order_key).lower())
        del key
        if any(ok == str(verdict.order_key).lower() for (_proto, ok) in watch):
            return {"gmx_v2": [verdict]}
        return {}

    monkeypatch.setattr(psr, "_resolve_all_verdicts", _fake_resolve)


@pytest.mark.asyncio
async def test_watch_set_derivation_parses_async_orders_and_applies_minus() -> None:
    writer = _FakeWriter()
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, _FakeProcessor(writer))
    watch = await psr._derive_watch_set(runner, _DEPLOYMENT)
    assert ("gmx_v2", _ORDER_KEY.lower()) in watch
    entry = watch[("gmx_v2", _ORDER_KEY.lower())]
    assert entry.is_open is True and entry.is_long is True
    # Once a PERP_SETTLEMENT event exists for that ledger id, the MINUS removes it.
    writer.written.append(
        SimpleNamespace(identity=SimpleNamespace(ledger_entry_id="ledger-1"), event_type="PERP_SETTLEMENT")
    )
    watch2 = await psr._derive_watch_set(runner, _DEPLOYMENT)
    assert watch2 == {}


@pytest.mark.asyncio
async def test_booked_exactly_once_across_two_ticks(monkeypatch) -> None:
    """Restart-kill semantics: two reconciler ticks book the settlement exactly once."""
    writer = _FakeWriter()
    processor = _FakeProcessor(writer)
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)
    _stub_resolver(monkeypatch, _executed_verdict())

    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="cyc-1", gateway_client=object()
    )
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="cyc-2", gateway_client=object()
    )

    assert len(writer.written) == 1
    ev = writer.written[0]
    assert ev.event_type == "PERP_SETTLEMENT"
    assert ev.settlement_state == "EXECUTED"
    assert ev.identity.ledger_entry_id == "ledger-1"
    assert ev.size_delta_usd == Decimal("2422.85")


@pytest.mark.asyncio
async def test_drain_precedes_settlement_write(monkeypatch) -> None:
    """Ordering guard (mutation-resistant): drain_one(submission_ledger_id) runs
    BEFORE the settlement write. Removing the drain call fails this test."""
    writer = _FakeWriter()
    processor = _FakeProcessor(writer)
    order: list[str] = []
    orig_drain = processor.drain_one
    orig_write = writer.write

    async def _drain(lid: str) -> bool:
        order.append("drain")
        return await orig_drain(lid)

    async def _write(ev: Any) -> bool:
        order.append("write")
        return await orig_write(ev)

    processor.drain_one = _drain  # type: ignore[method-assign]
    writer.write = _write  # type: ignore[method-assign]
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)
    _stub_resolver(monkeypatch, _executed_verdict())

    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="c", gateway_client=object()
    )
    assert order == ["drain", "write"]
    assert processor.calls == ["drain:ledger-1"]


@pytest.mark.asyncio
async def test_live_mode_write_failure_is_caught_and_non_terminal(monkeypatch) -> None:
    """Live-mode failure semantics: an AccountingPersistenceError from the writer is
    caught at the reconciler boundary (never halts the runner), and the entry stays
    non-terminal — nothing booked, so the next tick re-derives and retries."""
    writer = _FakeWriter(raise_live=True)
    processor = _FakeProcessor(writer)
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)
    runner._is_live_mode = lambda: True
    _stub_resolver(monkeypatch, _executed_verdict())

    # Must NOT raise out of the reconciler (catch boundary).
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="c", gateway_client=object()
    )
    assert writer.written == []  # nothing booked
    # The ledger is still un-settled → a subsequent tick re-derives it.
    watch = await psr._derive_watch_set(runner, _DEPLOYMENT)
    assert ("gmx_v2", _ORDER_KEY.lower()) in watch


@pytest.mark.asyncio
async def test_non_terminal_verdict_is_not_booked(monkeypatch) -> None:
    writer = _FakeWriter()
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, _FakeProcessor(writer))
    pending = PerpSettlementVerdict(order_key=_ORDER_KEY, state=PerpSettlementState.PENDING, terminal=False)
    _stub_resolver(monkeypatch, pending)
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="c", gateway_client=object()
    )
    assert writer.written == []


@pytest.mark.asyncio
async def test_no_gateway_is_noop() -> None:
    writer = _FakeWriter()
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, _FakeProcessor(writer))
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="c", gateway_client=None
    )
    assert writer.written == []


# --------------------------------------------------------------------------- #
# _backfill_position_events + its extracted pure helpers (CRAP refactor)
# --------------------------------------------------------------------------- #

from almanak.framework.runner import perp_settlement_commit as psc  # noqa: E402


class _AttrEvent:
    """Minimal event exposing the backfilled attribution fields + identity."""

    def __init__(self, ledger_entry_id: str = "ledger-1", **fields: Any) -> None:
        self.identity = SimpleNamespace(ledger_entry_id=ledger_entry_id)
        self.position_key = "0xpos"
        for key in ("entry_price", "exit_price", "funding_fee_usd", "realized_pnl_usd", "position_fee_usd"):
            setattr(self, key, fields.get(key))


class _BackfillSM:
    def __init__(self, history: list[Any]) -> None:
        self._history = history
        self.updates: list[tuple] = []

    async def get_position_history(self, deployment_id: str, position_key: str) -> list[Any]:
        return self._history

    async def update_position_attribution(self, event_id, attribution_json, version, deployment_id) -> bool:
        self.updates.append((event_id, attribution_json, version, deployment_id))
        return True


class _NoUpdateSM:
    """State manager missing update_position_attribution (best-effort no-op path)."""

    def __init__(self, history: list[Any]) -> None:
        self._history = history

    async def get_position_history(self, deployment_id: str, position_key: str) -> list[Any]:
        return self._history


def _pos_row(**kw: Any) -> dict:
    return {"id": "pe-1", "ledger_entry_id": "ledger-1", "attribution_json": "{}", "attribution_version": 1, **kw}


class TestLoadAttribution:
    def test_valid_object_json(self) -> None:
        assert psc._load_attribution(_pos_row(attribution_json='{"a": 1}')) == {"a": 1}

    def test_default_empty(self) -> None:
        assert psc._load_attribution(_pos_row(attribution_json=None)) == {}

    def test_non_object_json_is_none(self) -> None:
        assert psc._load_attribution(_pos_row(attribution_json="[1, 2]")) is None

    def test_unparseable_is_none(self) -> None:
        assert psc._load_attribution(_pos_row(attribution_json="{not json")) is None

    def test_dict_passthrough(self) -> None:
        assert psc._load_attribution(_pos_row(attribution_json={"x": 9})) == {"x": 9}


class TestMergeMeasuredAttribution:
    def test_only_measured_fields_stamped_and_preexisting_preserved(self) -> None:
        event = _AttrEvent(entry_price=Decimal("3000"), funding_fee_usd=None, realized_pnl_usd=Decimal("0"))
        merged = psc._merge_measured_attribution({"existing": "keep"}, event)
        assert merged["existing"] == "keep"  # preserved
        assert merged["entry_price"] == "3000"
        assert merged["realized_pnl_usd"] == "0"  # Decimal("0") IS stamped
        assert "funding_fee_usd" not in merged  # None NOT stamped (Empty ≠ Zero)
        assert "exit_price" not in merged

    def test_does_not_mutate_input(self) -> None:
        original = {"a": 1}
        psc._merge_measured_attribution(original, _AttrEvent(entry_price=Decimal("1")))
        assert original == {"a": 1}


class TestAttributionVersion:
    def test_int(self) -> None:
        assert psc._attribution_version(_pos_row(attribution_version=5)) == 5

    def test_str_int(self) -> None:
        assert psc._attribution_version(_pos_row(attribution_version="7")) == 7

    def test_default_and_unparseable(self) -> None:
        assert psc._attribution_version(_pos_row(attribution_version=None)) == 1
        assert psc._attribution_version(_pos_row(attribution_version="x")) == 1


@pytest.mark.asyncio
class TestBackfillPositionEvents:
    async def _run(self, sm: Any, event: Any) -> Any:
        runner = SimpleNamespace(state_manager=sm)
        return await psc._backfill_position_events(runner, event=event, deployment_id=_DEPLOYMENT)

    async def test_success_merges_and_preserves(self) -> None:
        sm = _BackfillSM([_pos_row(attribution_json='{"pre": "keep"}')])
        await self._run(sm, _AttrEvent(entry_price=Decimal("3000"), position_fee_usd=Decimal("1.45")))
        assert len(sm.updates) == 1
        event_id, attribution_json, version, dep = sm.updates[0]
        assert event_id == "pe-1" and version == 1 and dep == _DEPLOYMENT
        payload = json.loads(attribution_json)
        assert payload == {"pre": "keep", "entry_price": "3000", "position_fee_usd": "1.45"}

    async def test_no_position_key_returns_without_update(self) -> None:
        sm = _BackfillSM([_pos_row()])
        event = _AttrEvent()
        event.position_key = ""
        await self._run(sm, event)
        assert sm.updates == []

    async def test_no_history_match_returns_without_update(self) -> None:
        sm = _BackfillSM([_pos_row(ledger_entry_id="other")])
        await self._run(sm, _AttrEvent(entry_price=Decimal("1")))
        assert sm.updates == []

    async def test_unparseable_attribution_skips_update(self) -> None:
        sm = _BackfillSM([_pos_row(attribution_json="[bad]")])
        await self._run(sm, _AttrEvent(entry_price=Decimal("1")))
        assert sm.updates == []

    async def test_missing_update_method_returns_cleanly(self) -> None:
        sm = _NoUpdateSM([_pos_row()])
        result = await self._run(sm, _AttrEvent(entry_price=Decimal("1")))
        assert result is None  # no crash, best-effort no-op

    async def test_unexpected_error_is_swallowed(self) -> None:
        """The outer catch keeps a raising read from ever blocking the books."""

        class _RaisingSM:
            async def get_position_history(self, d, p):  # noqa: ANN001
                raise RuntimeError("db exploded")

        result = await self._run(_RaisingSM(), _AttrEvent(entry_price=Decimal("1")))
        assert result is None


# --------------------------------------------------------------------------- #
# _resolve_all_verdicts + _resolve_submission_block + full un-stubbed tick
# (these I/O paths were previously stubbed → 3% coverage; exercise them now)
# --------------------------------------------------------------------------- #

import almanak.connectors._strategy_runner_hook_registry as _srh  # noqa: E402
import almanak.framework.web3.gateway_provider as _gwp  # noqa: E402


class _FakeEthReceipt:
    def __init__(self, block: Any = 100, raise_exc: Exception | None = None) -> None:
        self._block = block
        self._raise = raise_exc

    def get_transaction_receipt(self, tx_hash: str) -> Any:
        if self._raise is not None:
            raise self._raise
        return {"blockNumber": self._block}


class _FakeWeb3:
    def __init__(self, eth: _FakeEthReceipt) -> None:
        self.eth = eth


def _watch_entry(order_key: str = _ORDER_KEY, *, tx: str = "0xsubmit", ts: datetime | None = None) -> Any:
    return psr._WatchEntry(
        order_key=order_key,
        is_open=True,
        is_long=True,
        protocol="gmx_v2",
        ledger=_ledger("ledger-1", "PERP_OPEN", order_key),
        submission_tx_hash=tx,
        submission_timestamp=ts,
    )


class TestResolveSubmissionBlock:
    def test_empty_tx_is_none(self) -> None:
        assert psr._resolve_submission_block(_FakeWeb3(_FakeEthReceipt()), "") is None

    def test_dict_receipt(self) -> None:
        assert psr._resolve_submission_block(_FakeWeb3(_FakeEthReceipt(block=555)), "0xa") == 555

    def test_object_receipt(self) -> None:
        class _Eth:
            def get_transaction_receipt(self, tx):  # noqa: ANN001
                return SimpleNamespace(blockNumber=777)

        assert psr._resolve_submission_block(_FakeWeb3(_Eth()), "0xa") == 777

    def test_none_block_is_none(self) -> None:
        assert psr._resolve_submission_block(_FakeWeb3(_FakeEthReceipt(block=None)), "0xa") is None

    def test_receipt_raise_is_none(self) -> None:
        assert psr._resolve_submission_block(_FakeWeb3(_FakeEthReceipt(raise_exc=RuntimeError("x"))), "0xa") is None


class TestResolveAllVerdicts:
    def _patch(self, monkeypatch, *, web3: Any, resolve: Any) -> None:
        monkeypatch.setattr(_gwp, "get_gateway_web3", lambda gc, chain: web3)
        monkeypatch.setattr(_srh.STRATEGY_RUNNER_HOOK_REGISTRY, "resolve_perp_settlements", resolve, raising=False)

    def test_happy_path_builds_entry_and_dispatches(self, monkeypatch) -> None:
        captured: dict = {}

        def _resolve(*, protocol, gateway_client, chain, wallet_address, watch_entries):  # noqa: ARG001
            captured["entries"] = watch_entries
            return [_executed_verdict()]

        self._patch(monkeypatch, web3=_FakeWeb3(_FakeEthReceipt(block=100)), resolve=_resolve)
        watch = {
            ("gmx_v2", _ORDER_KEY.lower()): _watch_entry(
                ts=datetime.now(UTC) - __import__("datetime").timedelta(seconds=40)
            )
        }
        out = psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch=watch)
        assert "gmx_v2" in out and len(out["gmx_v2"]) == 1
        we = captured["entries"][0]
        assert we.submission_block == 100
        assert we.seconds_since_submission is not None and we.seconds_since_submission >= 35

    def test_no_timestamp_leaves_elapsed_none(self, monkeypatch) -> None:
        captured: dict = {}

        def _resolve(*, watch_entries, **kw):  # noqa: ANN003
            captured["entries"] = watch_entries
            return [_executed_verdict()]

        self._patch(monkeypatch, web3=_FakeWeb3(_FakeEthReceipt(block=100)), resolve=_resolve)
        watch = {("gmx_v2", _ORDER_KEY.lower()): _watch_entry(ts=None)}
        psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch=watch)
        assert captured["entries"][0].seconds_since_submission is None

    def test_block_none_skips_entry(self, monkeypatch) -> None:
        called = {"n": 0}

        def _resolve(**kw):  # noqa: ANN003
            called["n"] += 1
            return []

        self._patch(monkeypatch, web3=_FakeWeb3(_FakeEthReceipt(block=None)), resolve=_resolve)
        watch = {("gmx_v2", _ORDER_KEY.lower()): _watch_entry()}
        out = psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch=watch)
        assert out == {} and called["n"] == 0  # no block → entry skipped → resolve not called

    def test_registry_raise_is_skipped(self, monkeypatch) -> None:
        def _resolve(**kw):  # noqa: ANN003
            raise RuntimeError("connector down")

        self._patch(monkeypatch, web3=_FakeWeb3(_FakeEthReceipt(block=100)), resolve=_resolve)
        watch = {("gmx_v2", _ORDER_KEY.lower()): _watch_entry()}
        out = psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch=watch)
        assert out == {}  # protocol skipped, no crash

    def test_empty_watch(self, monkeypatch) -> None:
        self._patch(monkeypatch, web3=_FakeWeb3(_FakeEthReceipt()), resolve=lambda **kw: [])
        assert (
            psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch={}) == {}
        )

    def test_gateway_web3_raise(self, monkeypatch) -> None:
        def _raise(gc, chain):  # noqa: ANN001
            raise RuntimeError("no gw")

        monkeypatch.setattr(_gwp, "get_gateway_web3", _raise)
        watch = {("gmx_v2", _ORDER_KEY.lower()): _watch_entry()}
        assert (
            psr._resolve_all_verdicts(gateway_client=object(), chain="arbitrum", wallet_address="0xw", watch=watch)
            == {}
        )


@pytest.mark.asyncio
async def test_full_tick_without_stub_books_settlement(monkeypatch) -> None:
    """End-to-end tick with the REAL _resolve_all_verdicts (only the gateway boundary
    mocked): derive → resolve → commit → write, exercising every I/O path."""
    writer = _FakeWriter()
    processor = _FakeProcessor(writer)
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)

    monkeypatch.setattr(_gwp, "get_gateway_web3", lambda gc, chain: _FakeWeb3(_FakeEthReceipt(block=100)))
    monkeypatch.setattr(
        _srh.STRATEGY_RUNNER_HOOK_REGISTRY,
        "resolve_perp_settlements",
        lambda **kw: [_executed_verdict()],
        raising=False,
    )
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="cyc-1", gateway_client=object()
    )
    assert len(writer.written) == 1
    assert writer.written[0].event_type == "PERP_SETTLEMENT"


# --------------------------------------------------------------------------- #
# Idempotency contract (CodeRabbit PR #3446)
# --------------------------------------------------------------------------- #


def test_event_id_is_cycle_independent() -> None:
    """FIX 1: the deterministic event id is keyed on STABLE settlement identity, NOT
    cycle_id — so a re-book on a later tick (fresh cycle_id) mints the SAME id."""
    from almanak.framework.accounting.perp_settlement_accounting import build_perp_settlement_event

    kw: dict[str, Any] = dict(
        verdict=_executed_verdict(),
        submission_ledger_entry_id="ledger-1",
        deployment_id=_DEPLOYMENT,
        execution_mode="paper",
        chain="arbitrum",
        protocol="gmx_v2",
        wallet_address="0xw",
        is_open=True,
    )
    e1 = build_perp_settlement_event(cycle_id="cyc-1", **kw)
    e2 = build_perp_settlement_event(cycle_id="cyc-2", **kw)
    assert e1.identity.id == e2.identity.id  # id stable across ticks (the idempotency backstop)
    assert e1.identity.cycle_id != e2.identity.cycle_id  # cycle_id still carried as metadata


@pytest.mark.asyncio
async def test_different_cycle_ids_still_book_exactly_once(monkeypatch) -> None:
    """FIX 1: two ticks with DIFFERENT cycle_ids book exactly one row with the SAME id."""
    writer = _FakeWriter()
    processor = _FakeProcessor(writer)
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)
    _stub_resolver(monkeypatch, _executed_verdict())

    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="cyc-AAA", gateway_client=object()
    )
    first_id = writer.written[0].identity.id
    await psr.reconcile_perp_settlements(
        runner, _strategy(), deployment_id=_DEPLOYMENT, cycle_id="cyc-ZZZ", gateway_client=object()
    )
    assert len(writer.written) == 1  # MINUS filter dedups; and even a miss would ON CONFLICT the same id
    assert writer.written[0].identity.id == first_id


@pytest.mark.asyncio
async def test_drain_failure_aborts_settlement_write() -> None:
    """FIX 2: a drain_one failure ABORTS the settlement write (hard precondition) —
    the write must NOT run, the entry stays non-terminal for retry. Mutation-resistant:
    removing the abort (writing anyway) fails this test."""
    from almanak.framework.runner.perp_settlement_commit import commit_perp_settlement

    writer = _FakeWriter()

    class _RaisingDrainProcessor(_FakeProcessor):
        async def drain_one(self, ledger_entry_id: str) -> bool:
            self.calls.append(f"drain:{ledger_entry_id}")
            raise RuntimeError("drain boom")

    processor = _RaisingDrainProcessor(writer)
    sm = _FakeStateManager([_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY)], writer)
    runner = _runner(sm, processor)

    outcome = await commit_perp_settlement(
        runner,
        _strategy(),
        verdict=_executed_verdict(),
        submission_ledger=_ledger("ledger-1", "PERP_OPEN", _ORDER_KEY),
        is_open=True,
        settlement_cycle_id="c",
        chain="arbitrum",
        protocol="gmx_v2",
        wallet_address="0xw",
    )
    assert processor.calls == ["drain:ledger-1"]  # drain attempted
    assert writer.written == []  # write ABORTED — never ran
    assert outcome.booked is False
    assert outcome.accounting_degraded is True
    assert "drain" in (outcome.degraded_reason or "").lower()
