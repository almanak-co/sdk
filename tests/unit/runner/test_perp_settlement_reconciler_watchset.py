"""VIB-6107 — the perp settlement reconciler must read via the PRODUCTION backend.

A real ``almanak strat run`` ALWAYS injects ``GatewayStateManager`` (never the plain
``StateManager``), so ``_derive_watch_set`` MUST consume its measured-read API
(``read_ledger_entries_measured`` / ``read_accounting_events_measured`` /
``get_ledger_entry_by_id``). The WI-3 reconciler probed for ``get_ledger_entries`` /
``get_accounting_events`` — methods GatewayStateManager does not have — and returned an
empty watch set SILENTLY on every real tick, so it never booked a settlement (proven on
mainnet, WI-5).

These tests exercise ``_derive_watch_set`` through a **shape-faithful GatewayStateManager
fake** that deliberately does NOT expose the old ``get_ledger_entries`` /
``get_accounting_events`` names. If someone reverts the reconciler to the old probe, the
fake yields no data → the watch set is empty → these tests FAIL. That is the guard that
stops the whole bug class from recurring.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.runner import perp_settlement_reconciler as reconciler
from almanak.framework.runner.perp_settlement_reconciler import _derive_watch_set

_DEPLOYMENT = "deployment:wi6"
_OPEN_KEY = "0x585d42d95b9a4e84e78d53073d85fa6e67304c119fc000a6052661068200f9cf"
_CLOSE_KEY = "0x1111111111111111111111111111111111111111111111111111111111111111"


def _async_orders_json(order_key: str, *, is_long: bool = True) -> str:
    repr_str = (
        f"AsyncOrderData(protocol='gmx_v2', order_id='{order_key}', "
        f"status=<AsyncOrderStatus.PENDING: 'pending'>, kind=<AsyncOrderKind.INCREASE: 'increase'>, "
        f"market='0xmkt', collateral_token='0xusdc', is_long={is_long}, size_delta_usd=None)"
    )
    return json.dumps({"async_orders": [repr_str]})


def _info_row(ledger_id: str, intent_type: str, *, success: bool = True) -> dict[str, Any]:
    """The LedgerEntryInfo-projection shape (list read) — NO extracted_data_json."""
    return {
        "id": ledger_id,
        "intent_type": intent_type,
        "protocol": "gmx_v2",
        "success": success,
        "tx_hash": "0x" + "ab" * 32,
        "chain": "arbitrum",
        "timestamp": "2026-07-26T23:17:44+00:00",
    }


def _full_row(ledger_id: str, intent_type: str, order_key: str) -> dict[str, Any]:
    """The LedgerEntryData-projection shape (get_ledger_entry_by_id) — WITH extracted_data_json."""
    row = _info_row(ledger_id, intent_type)
    row.update(
        {
            "deployment_id": _DEPLOYMENT,
            "cycle_id": "cyc-0",
            "execution_mode": "live",
            "extracted_data_json": _async_orders_json(order_key),
        }
    )
    return row


def _settlement_event(ledger_entry_id: str) -> dict[str, Any]:
    return {"event_type": "PERP_SETTLEMENT", "ledger_entry_id": ledger_entry_id}


class _FakeGatewaySM:
    """Shape-faithful stand-in for GatewayStateManager's measured-read API.

    Exposes EXACTLY the three methods GatewayStateManager has (sync list reads returning
    ``(list[dict], measured)``; async ``get_ledger_entry_by_id`` returning a dict|None).
    Deliberately does NOT expose ``get_ledger_entries`` / ``get_accounting_events`` — a
    revert to the old probe breaks every test here (VIB-6107 anti-regression).
    """

    def __init__(
        self,
        *,
        ledger_rows: list[dict[str, Any]],
        events: list[dict[str, Any]],
        full_by_id: dict[str, dict[str, Any]],
        ledger_measured: bool = True,
        events_measured: bool = True,
    ) -> None:
        self._ledger_rows = ledger_rows
        self._events = events
        self._full_by_id = full_by_id
        self._ledger_measured = ledger_measured
        self._events_measured = events_measured
        self.hydrate_calls: list[str] = []

    def read_ledger_entries_measured(self, deployment_id: str) -> tuple[list[dict[str, Any]], bool]:
        return list(self._ledger_rows), self._ledger_measured

    def read_accounting_events_measured(
        self, deployment_id: str, position_key: str | None = None
    ) -> tuple[list[dict[str, Any]], bool]:
        return list(self._events), self._events_measured

    async def get_ledger_entry_by_id(self, ledger_entry_id: str) -> dict[str, Any] | None:
        self.hydrate_calls.append(ledger_entry_id)
        return self._full_by_id.get(ledger_entry_id)


def _runner(sm: Any) -> Any:
    return SimpleNamespace(state_manager=sm)


@pytest.fixture(autouse=True)
def _reset_latches() -> Any:
    reconciler._MISSING_READER_WARNED.clear()
    reconciler._LARGE_FANOUT_NOTICED = False
    yield
    reconciler._MISSING_READER_WARNED.clear()
    reconciler._LARGE_FANOUT_NOTICED = False


@pytest.mark.asyncio
async def test_derives_watch_set_via_gateway_measured_api() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    watch = await _derive_watch_set(_runner(sm), _DEPLOYMENT)
    assert set(watch.keys()) == {("gmx_v2", _OPEN_KEY.lower())}
    entry = watch[("gmx_v2", _OPEN_KEY.lower())]
    assert entry.is_open is True
    assert entry.order_key == _OPEN_KEY
    assert entry.submission_tx_hash == "0x" + "ab" * 32
    assert entry.submission_timestamp is not None
    # The list projection lacks async_orders → we MUST hydrate the survivor.
    assert sm.hydrate_calls == ["l-open"]


@pytest.mark.asyncio
async def test_old_statemanager_api_is_not_consulted() -> None:
    # Anti-regression: a backend exposing ONLY the OLD names (the pre-VIB-6107 probe
    # targets) yields an EMPTY watch set — proving the reconciler no longer reads them.
    class _OldStateManager:
        async def get_ledger_entries(self, *a: Any, **k: Any) -> list[Any]:
            raise AssertionError("reconciler must NOT call get_ledger_entries (VIB-6107)")

        async def get_accounting_events(self, *a: Any, **k: Any) -> list[Any]:
            raise AssertionError("reconciler must NOT call get_accounting_events (VIB-6107)")

    watch = await _derive_watch_set(_runner(_OldStateManager()), _DEPLOYMENT)
    assert watch == {}


@pytest.mark.asyncio
async def test_missing_reader_warns_once_never_silent(caplog: Any) -> None:
    sm = SimpleNamespace()  # no measured-read API at all
    with caplog.at_level(logging.WARNING):
        assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}
        assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}  # second tick
    warnings = [r for r in caplog.records if "lacks the measured-read API" in r.message]
    assert len(warnings) == 1, "must WARN (not silent) and exactly once per backend class"
    assert warnings[0].levelno == logging.WARNING


@pytest.mark.asyncio
async def test_unmeasured_ledger_read_skips_tick() -> None:
    # Empty≠Zero: measured=False must NOT be read as "nothing to settle".
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
        ledger_measured=False,
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}
    assert sm.hydrate_calls == []  # skipped before hydrate


@pytest.mark.asyncio
async def test_unmeasured_events_read_skips_tick() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
        events_measured=False,
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_settled_row_is_excluded_by_minus() -> None:
    # A measured PERP_SETTLEMENT for the row removes it from the watch set (no re-book).
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[_settlement_event("l-open")],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}
    assert sm.hydrate_calls == []  # excluded before hydrate


@pytest.mark.asyncio
async def test_measured_empty_is_authoritative_nothing_to_do() -> None:
    sm = _FakeGatewaySM(ledger_rows=[], events=[], full_by_id={})
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_unsuccessful_row_skipped() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN", success=False)],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_non_perp_intent_skipped() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-swap", "SWAP")],
        events=[],
        full_by_id={"l-swap": _full_row("l-swap", "SWAP", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}
    assert sm.hydrate_calls == []


@pytest.mark.asyncio
async def test_hydrate_none_skips_row_no_fabrication() -> None:
    # get_ledger_entry_by_id returning None (row vanished / gateway error) is UNMEASURED
    # for THAT row → skip it, never fabricate an empty order list.
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={},  # hydrate returns None
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}
    assert sm.hydrate_calls == ["l-open"]


@pytest.mark.asyncio
async def test_events_read_exception_skips_tick() -> None:
    # A raising measured read is UNMEASURED for the tick → skip (never fabricate).
    class _RaisingEvents(_FakeGatewaySM):
        def read_accounting_events_measured(self, deployment_id: str, position_key: str | None = None):
            raise RuntimeError("gateway down")

    sm = _RaisingEvents(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_ledger_read_exception_skips_tick() -> None:
    class _RaisingLedger(_FakeGatewaySM):
        def read_ledger_entries_measured(self, deployment_id: str):
            raise RuntimeError("gateway down")

    sm = _RaisingLedger(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_hydrated_row_without_protocol_skipped() -> None:
    # A hydrated row missing protocol is skipped (can't key the verdict dispatch).
    full = _full_row("l-open", "PERP_OPEN", _OPEN_KEY)
    full["protocol"] = ""
    info = _info_row("l-open", "PERP_OPEN")
    info["protocol"] = ""
    sm = _FakeGatewaySM(ledger_rows=[info], events=[], full_by_id={"l-open": full})
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_non_dict_ledger_row_ignored() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    sm._ledger_rows = [None, "garbage", _info_row("l-open", "PERP_OPEN")]  # type: ignore[list-item]
    watch = await _derive_watch_set(_runner(sm), _DEPLOYMENT)
    assert set(watch.keys()) == {("gmx_v2", _OPEN_KEY.lower())}


class _RaisingHydrate(_FakeGatewaySM):
    """Fake whose per-row hydrate RAISES for a configured set of ledger ids (a transient
    gateway/transport error), succeeding for the rest."""

    def __init__(self, *, raise_ids: set[str], **kw: Any) -> None:
        super().__init__(**kw)
        self._raise_ids = raise_ids

    async def get_ledger_entry_by_id(self, ledger_entry_id: str) -> dict[str, Any] | None:
        if ledger_entry_id in self._raise_ids:
            raise RuntimeError("transient gateway error")
        return await super().get_ledger_entry_by_id(ledger_entry_id)


@pytest.mark.asyncio
async def test_hydrate_exception_skips_row_not_tick() -> None:
    # A per-row hydrate RAISE must be handled like a None return: skip that row, do NOT
    # propagate the exception out of _derive_watch_set.
    sm = _RaisingHydrate(
        raise_ids={"l-open"},
        ledger_rows=[_info_row("l-open", "PERP_OPEN")],
        events=[],
        full_by_id={"l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY)},
    )
    assert await _derive_watch_set(_runner(sm), _DEPLOYMENT) == {}


@pytest.mark.asyncio
async def test_hydrate_exception_is_per_row_partial_progress() -> None:
    # Two candidates: one hydrate raises, one succeeds. The succeeding order MUST still be
    # derived (partial progress) and the raising one absent — a flaky per-candidate RPC
    # never drops every other order's progress. Mutation-resistant: removing the try/except
    # makes this ERROR out instead of returning the one good entry.
    sm = _RaisingHydrate(
        raise_ids={"l-open"},
        ledger_rows=[_info_row("l-open", "PERP_OPEN"), _info_row("l-close", "PERP_CLOSE")],
        events=[],
        full_by_id={
            "l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY),
            "l-close": _full_row("l-close", "PERP_CLOSE", _CLOSE_KEY),
        },
    )
    watch = await _derive_watch_set(_runner(sm), _DEPLOYMENT)
    assert set(watch.keys()) == {("gmx_v2", _CLOSE_KEY.lower())}  # the good one survives
    assert ("gmx_v2", _OPEN_KEY.lower()) not in watch  # the raising one is deferred


@pytest.mark.asyncio
async def test_close_and_open_both_derived() -> None:
    sm = _FakeGatewaySM(
        ledger_rows=[_info_row("l-open", "PERP_OPEN"), _info_row("l-close", "PERP_CLOSE")],
        events=[],
        full_by_id={
            "l-open": _full_row("l-open", "PERP_OPEN", _OPEN_KEY),
            "l-close": _full_row("l-close", "PERP_CLOSE", _CLOSE_KEY),
        },
    )
    watch = await _derive_watch_set(_runner(sm), _DEPLOYMENT)
    assert set(watch.keys()) == {("gmx_v2", _OPEN_KEY.lower()), ("gmx_v2", _CLOSE_KEY.lower())}
    assert watch[("gmx_v2", _OPEN_KEY.lower())].is_open is True
    assert watch[("gmx_v2", _CLOSE_KEY.lower())].is_open is False


@pytest.mark.asyncio
async def test_large_candidate_fanout_notices_once_and_derives_all(caplog: Any) -> None:
    # A large unsettled backlog logs INFO once (visible N+1), and STILL hydrates every
    # candidate — the notice is not a cap (dropping rows would miss settlements).
    n = reconciler._HYDRATE_FANOUT_NOTICE + 5
    keys = [f"0x{i:064x}" for i in range(n)]
    rows = [_info_row(f"l-{i}", "PERP_OPEN") for i in range(n)]
    full = {f"l-{i}": _full_row(f"l-{i}", "PERP_OPEN", keys[i]) for i in range(n)}
    sm = _FakeGatewaySM(ledger_rows=rows, events=[], full_by_id=full)
    with caplog.at_level(logging.INFO):
        watch = await _derive_watch_set(_runner(sm), _DEPLOYMENT)
    assert len(watch) == n  # every candidate hydrated, none dropped
    notices = [r for r in caplog.records if "unsettled perp candidate rows this tick" in r.message]
    assert len(notices) == 1 and notices[0].levelno == logging.INFO


class TestGatewayStateManagerContract:
    """VIB-6107 belt-and-suspenders: pin that the REAL GatewayStateManager exposes the
    exact measured-read API the reconciler calls, so the signature-faithful fake above can
    never silently drift from the production class (which is how the original bug hid). If
    GatewayStateManager renames/removes a method, THIS fails — the guard the old code lacked.
    """

    def test_gateway_state_manager_exposes_measured_read_api(self) -> None:
        import inspect

        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        # read_ledger_entries_measured(self, deployment_id)
        m = getattr(GatewayStateManager, "read_ledger_entries_measured", None)
        assert callable(m)
        assert list(inspect.signature(m).parameters)[:2] == ["self", "deployment_id"]

        # read_accounting_events_measured(self, deployment_id, position_key=None)
        m = getattr(GatewayStateManager, "read_accounting_events_measured", None)
        assert callable(m)
        params = inspect.signature(m).parameters
        assert list(params)[:2] == ["self", "deployment_id"]
        assert "position_key" in params  # optional, but the reconciler must be able to omit it
        assert params["position_key"].default is None

        # get_ledger_entry_by_id(self, ledger_entry_id) — async
        m = getattr(GatewayStateManager, "get_ledger_entry_by_id", None)
        assert callable(m)
        assert inspect.iscoroutinefunction(m)
        assert list(inspect.signature(m).parameters)[:2] == ["self", "ledger_entry_id"]

    def test_gateway_state_manager_lacks_the_old_probe_names(self) -> None:
        # The old reconciler probed these — assert they're absent so nobody "restores"
        # them on GatewayStateManager as a shortcut instead of using the measured API.
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        assert not hasattr(GatewayStateManager, "get_ledger_entries")
        assert not hasattr(GatewayStateManager, "get_accounting_events")
