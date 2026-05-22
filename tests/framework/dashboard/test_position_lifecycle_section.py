"""Unit tests for ``render_position_lifecycle_section`` (PR 2 / Problem A2).

These tests prove the gateway-backed sibling of
``pages.detail.render_position_lifecycle`` renders the same shapes the
local-SQLite version does, but through a mocked ``DashboardAPIClient`` —
no filesystem, no direct SQLite reads. They also cover the
``_filter_open_only_events`` and ``_fetch_registry_handles_via_gateway``
helpers in isolation so regressions surface in one spot rather than
inside the streamlit-shaped render path.

Streamlit's render surface is stubbed (``_StubStreamlit``) so the tests
run from a normal pytest invocation — the contract being verified here
is "what does the section emit", not "what does Streamlit do with it".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from almanak.framework.dashboard import sections
from almanak.framework.dashboard.sections import (
    _fetch_registry_handles_via_gateway,
    _filter_open_only_events,
    render_position_lifecycle_section,
)


class _StubColumn:
    """Stand-in for the object returned by ``st.columns(...)`` so the
    ``with col1: st.metric(...)`` shape used by the section doesn't blow up
    in a non-Streamlit test environment."""

    def __init__(self, parent: _StubStreamlit) -> None:
        self._parent = parent

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def metric(self, label, value):
        self._parent.metrics.append((label, value))


class _StubColumnConfig:
    @staticmethod
    def LinkColumn(*_args, **_kwargs):  # noqa: N802
        return None


class _StubStreamlit:
    """Records the streamlit calls the section makes so the tests can
    assert on emitted shapes."""

    def __init__(self) -> None:
        self.dividers: int = 0
        self.markdowns: list[str] = []
        self.infos: list[str] = []
        self.captions: list[str] = []
        self.dataframes: list[list[dict[str, Any]]] = []
        self.metrics: list[tuple[str, Any]] = []
        self.download_buttons: list[dict[str, Any]] = []
        self.column_config = _StubColumnConfig()

    def divider(self) -> None:
        self.dividers += 1

    def markdown(self, msg: str) -> None:
        self.markdowns.append(msg)

    def info(self, msg: str) -> None:
        self.infos.append(msg)

    def caption(self, msg: str) -> None:
        self.captions.append(msg)

    def dataframe(self, data, **_kwargs) -> None:
        # Materialise to a list of dicts for assertion convenience.
        self.dataframes.append(list(data))

    def columns(self, n: int):
        return [_StubColumn(self) for _ in range(n)]

    def metric(self, label, value) -> None:
        # The section uses ``with col: st.metric(...)`` — Streamlit dispatches
        # the call to the active column under the hood, but in this stub we
        # don't model the dispatch (the column is a no-op context manager).
        # Record at the module level — tests only assert on contents.
        self.metrics.append((label, value))

    def download_button(self, **kwargs) -> None:
        self.download_buttons.append(kwargs)


class _FakeAPIClient:
    """Mock DashboardAPIClient — only implements get_position_events."""

    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = list(events)
        self.calls: list[Any] = []

    def get_position_events(self, position_types: list[str] | None = None):
        self.calls.append(position_types)
        if not position_types:
            return list(self._events)
        return [e for e in self._events if e.get("position_type") in position_types]


@dataclass
class _FakePositionEntry:
    handle: str
    physical_identity_hash: str
    primitive_payload_json: str


@dataclass
class _FakePositionsResult:
    positions: list[_FakePositionEntry]


class _FakeDashboardServiceClient:
    """Mock DashboardServiceClient — only implements get_positions + the
    connect contract the helper uses."""

    def __init__(self, positions: list[_FakePositionEntry]) -> None:
        self.is_connected = True
        self._positions = list(positions)
        self.connect_calls = 0

    def connect(self) -> None:
        self.connect_calls += 1
        self.is_connected = True

    def get_positions(self, _deployment_id: str):
        return _FakePositionsResult(positions=self._positions)


@pytest.fixture
def stub_st(monkeypatch) -> _StubStreamlit:
    stub = _StubStreamlit()
    monkeypatch.setattr(sections, "st", stub)
    return stub


def _make_event(
    position_id: str,
    event_type: str,
    *,
    position_type: str = "LP",
    timestamp: str = "2026-05-19T12:00:00",
    protocol: str = "uniswap_v3",
    chain: str = "arbitrum",
    value_usd: str = "1000.00",
    tx_hash: str = "0xabc",
    attribution_json: str = "{}",
) -> dict[str, Any]:
    return {
        "position_id": position_id,
        "event_type": event_type,
        "position_type": position_type,
        "timestamp": timestamp,
        "protocol": protocol,
        "chain": chain,
        "value_usd": value_usd,
        "tx_hash": tx_hash,
        "attribution_json": attribution_json,
    }


# -- _filter_open_only_events ---------------------------------------------------


def test_filter_open_only_drops_closed_positions_entire_history() -> None:
    events = [
        _make_event("p1", "OPEN"),
        _make_event("p1", "CLOSE"),
        _make_event("p2", "OPEN"),
    ]

    out = _filter_open_only_events(events)

    out_ids = {e["position_id"] for e in out}
    assert out_ids == {"p2"}


def test_filter_open_only_drops_close_rows_even_for_still_open_ids() -> None:
    events = [_make_event("p1", "OPEN"), _make_event("p1", "SNAPSHOT")]
    # SNAPSHOT rows survive — they aren't CLOSE and the id isn't in the closed set.
    out = _filter_open_only_events(events)
    assert len(out) == 2


# -- _fetch_registry_handles_via_gateway ---------------------------------------


def test_handles_keyed_by_token_id_and_physical_hash() -> None:
    """Mirrors the SQLite version: NFT primitives key on token_id, non-NFT
    on physical_identity_hash. Both shapes coexist in the same map so
    renderers don't have to know the primitive."""
    positions = [
        _FakePositionEntry(
            handle="leg_narrow",
            physical_identity_hash="hash_narrow",
            primitive_payload_json=json.dumps({"token_id": "1001"}),
        ),
        _FakePositionEntry(
            handle="leg_wide",
            physical_identity_hash="hash_wide",
            primitive_payload_json=json.dumps({"position_id": "2002"}),
        ),
        _FakePositionEntry(
            handle="leg_aave",
            physical_identity_hash="aave_hash",
            primitive_payload_json="{}",  # non-NFT — only the hash is the key
        ),
    ]
    client = _FakeDashboardServiceClient(positions)

    out = _fetch_registry_handles_via_gateway("strat", lambda: client)

    assert out["1001"] == "leg_narrow"
    assert out["hash_narrow"] == "leg_narrow"
    assert out["2002"] == "leg_wide"
    assert out["hash_wide"] == "leg_wide"
    assert out["aave_hash"] == "leg_aave"


def test_handles_skip_empty_or_blank_handles() -> None:
    positions = [
        _FakePositionEntry(handle="", physical_identity_hash="h1", primitive_payload_json="{}"),
        _FakePositionEntry(handle="   ", physical_identity_hash="h2", primitive_payload_json="{}"),
        _FakePositionEntry(handle="leg_mid", physical_identity_hash="h3", primitive_payload_json="{}"),
    ]
    client = _FakeDashboardServiceClient(positions)

    out = _fetch_registry_handles_via_gateway("strat", lambda: client)

    assert out == {"h3": "leg_mid"}


def test_handles_tolerate_malformed_payload() -> None:
    positions = [
        _FakePositionEntry(
            handle="leg_x",
            physical_identity_hash="h_x",
            primitive_payload_json="not-json{{",
        ),
    ]
    client = _FakeDashboardServiceClient(positions)

    out = _fetch_registry_handles_via_gateway("strat", lambda: client)

    # Empty payload still gets the physical-identity key written.
    assert out == {"h_x": "leg_x"}


def test_handles_returns_none_when_factory_returns_none() -> None:
    """Defensive guard: if a custom client_factory returns ``None`` (legitimate
    in test/embedding scenarios), the helper must return the documented
    failure sentinel ``None`` rather than crashing on ``client.is_connected``
    via the catch-all. Gemini-code-assist Important on PR #2373."""
    out = _fetch_registry_handles_via_gateway("strat", lambda: None)

    assert out is None


def test_handles_tolerate_missing_positions_attribute() -> None:
    """Defensive guard: if ``get_positions`` returns an object whose
    ``positions`` attribute is missing or None (malformed RPC payload),
    the helper returns an empty handle map rather than crashing. The
    caller treats this as the no-handles success case because we DID
    get a response (no caption fires). Gemini-code-assist Important on
    PR #2373."""

    class _BadPositionsResult:
        positions = None  # malformed; should be a list

    class _ClientReturningBadResult:
        is_connected = True

        def connect(self):
            pass

        def get_positions(self, _deployment_id):
            return _BadPositionsResult()

    out = _fetch_registry_handles_via_gateway("strat", lambda: _ClientReturningBadResult())

    assert out == {}


def test_handles_returns_none_on_client_failure() -> None:
    """``None`` is the documented sentinel for "RPC failed" so the renderer
    can distinguish the failure from the legitimate "no handles registered"
    case (which returns ``{}``). Conflating the two would silently hide
    alias-lookup failures on multi-position fixtures (UAT card Trust #7 §B
    / D3.F4b)."""

    def broken_factory():
        raise RuntimeError("gateway down")

    out = _fetch_registry_handles_via_gateway("strat", broken_factory)

    assert out is None


def test_handles_returns_empty_dict_when_no_handles_registered() -> None:
    """Empty registry → ``{}`` (NOT ``None``) so the renderer treats this
    as a successful "no aliases exist" case and renders the events table
    without an Alias column AND without the "aliases unavailable" caption."""
    client = _FakeDashboardServiceClient(positions=[])

    out = _fetch_registry_handles_via_gateway("strat", lambda: client)

    assert out == {}


# -- render_position_lifecycle_section -----------------------------------------


def _patch_handles_to_empty(monkeypatch) -> None:
    """Most lifecycle tests don't care about the registry join — pin it
    to empty so they don't accidentally trigger a real gateway call."""
    monkeypatch.setattr(
        sections,
        "_fetch_registry_handles_via_gateway",
        lambda *_args, **_kwargs: {},
    )


def test_render_handles_open_and_closed_positions(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    events = [
        _make_event("p1", "OPEN", value_usd="1500.00", tx_hash="0x111"),
        _make_event("p1", "CLOSE", value_usd="1600.00", tx_hash="0x222"),
        _make_event("p2", "OPEN", value_usd="900.00", tx_hash="0x333"),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    assert client.calls == [None]
    assert ("Positions Opened", 2) in stub_st.metrics
    assert ("Positions Closed", 1) in stub_st.metrics
    assert ("Total Events", 3) in stub_st.metrics
    assert len(stub_st.dataframes) == 1
    rows = stub_st.dataframes[0]
    types = [r["Type"] for r in rows]
    assert types == ["OPEN", "CLOSE", "OPEN"]


def test_render_includes_alias_column_when_handles_present(stub_st, monkeypatch) -> None:
    monkeypatch.setattr(
        sections,
        "_fetch_registry_handles_via_gateway",
        lambda *_args, **_kwargs: {
            "p1": "leg_narrow",
            "p2": "leg_mid",
            "p3": "leg_wide",
        },
    )
    events = [
        _make_event("p1", "OPEN"),
        _make_event("p2", "OPEN"),
        _make_event("p3", "OPEN"),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    rows = stub_st.dataframes[0]
    aliases = {r["Alias"] for r in rows}
    assert aliases == {"leg_narrow", "leg_mid", "leg_wide"}
    # Every row carries the Alias key when handles are present.
    assert all("Alias" in r for r in rows)


def test_render_skips_alias_column_when_no_handles(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    events = [_make_event("p1", "OPEN")]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    rows = stub_st.dataframes[0]
    assert "Alias" not in rows[0]
    # Legitimate "no handles registered" — no caption either.
    assert all("aliases unavailable" not in c.lower() for c in stub_st.captions), stub_st.captions


def test_render_surfaces_caption_when_registry_rpc_fails(stub_st, monkeypatch) -> None:
    """When the registry handle RPC fails, the events table renders without
    an Alias column AND a visible caption tells the operator aliases were
    unavailable. Without the caption, "no aliases exist" and "alias RPC
    failed" would be observationally identical — a silent degradation that
    UAT card Trust #7 §B / D3.F4b explicitly forbids."""
    # Patch the helper to return ``None`` (the documented RPC-failure sentinel).
    monkeypatch.setattr(
        sections,
        "_fetch_registry_handles_via_gateway",
        lambda *_args, **_kwargs: None,
    )
    events = [_make_event("p1", "OPEN")]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    rows = stub_st.dataframes[0]
    assert "Alias" not in rows[0], "Alias column must be omitted when handle lookup fails"
    assert any("aliases unavailable" in c.lower() for c in stub_st.captions), (
        f"a visible caption must explain why aliases are missing; got: {stub_st.captions!r}"
    )


def test_render_filters_open_only(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    events = [
        _make_event("p1", "OPEN"),
        _make_event("p1", "CLOSE"),
        _make_event("p2", "OPEN"),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client, open_only=True)

    rows = stub_st.dataframes[0]
    ids = [r["ID"] for r in rows]
    assert ids == ["p2"]


def test_render_empty_events_shows_info(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    client = _FakeAPIClient([])

    render_position_lifecycle_section("strat", client)

    assert any("No position events" in m for m in stub_st.infos), stub_st.infos
    assert stub_st.dataframes == []


def test_render_no_api_client_shows_info(stub_st) -> None:
    render_position_lifecycle_section("strat", None)

    assert any("no api_client" in m.lower() for m in stub_st.infos), stub_st.infos
    assert stub_st.dataframes == []


def test_render_tx_links_formatted_via_explorer(stub_st, monkeypatch) -> None:
    """The TX column must contain the explorer URL produced by
    ``get_explorer_url`` so the dataframe's LinkColumn can extract the
    short suffix — the same formatting the detail page uses."""
    _patch_handles_to_empty(monkeypatch)
    events = [_make_event("p1", "OPEN", chain="arbitrum", tx_hash="0xdeadbeef")]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    rows = stub_st.dataframes[0]
    tx_value = rows[0]["TX"]
    # The explorer URL convention is ".../tx/0x...". The LinkColumn regex
    # ".*/tx/(0x[a-fA-F0-9]{8})" depends on this shape, so we assert it
    # here rather than baking the exact host (which differs per chain).
    assert "/tx/0xdeadbeef" in tx_value, tx_value


def test_render_pnl_attribution_for_closed_positions(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    attribution = {
        "position_type": "LP",
        "net_pnl_usd": "12.34",
        "price_pnl_usd": "5.00",
        "fee_pnl_usd": "8.00",
        "gas_usd": "0.66",
        "version": 1,
    }
    events = [
        _make_event("p1", "OPEN"),
        _make_event("p1", "CLOSE", attribution_json=json.dumps(attribution)),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    # The lifecycle table is the first dataframe; the attribution table
    # is the second (only present when there's at least one closed
    # position with a non-empty attribution payload).
    assert len(stub_st.dataframes) == 2
    attr_rows = stub_st.dataframes[1]
    assert len(attr_rows) == 1
    assert attr_rows[0]["Type"] == "LP"
    assert attr_rows[0]["Version"] == "v1"


def test_render_attribution_skipped_for_empty_payload(stub_st, monkeypatch) -> None:
    """Empty != Zero — an attribution_json of '{}' or empty string means
    the writer did not emit attribution (legacy / paper-trading row).
    Those rows MUST NOT render in the attribution table; doing so would
    surface synthetic zeroes that look measured."""
    _patch_handles_to_empty(monkeypatch)
    events = [
        _make_event("p1", "OPEN"),
        _make_event("p1", "CLOSE", attribution_json=""),
        _make_event("p2", "OPEN"),
        _make_event("p2", "CLOSE", attribution_json="{}"),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    # Only one dataframe — the lifecycle table. No attribution sub-table.
    assert len(stub_st.dataframes) == 1


def test_render_forwards_position_types_filter(stub_st, monkeypatch) -> None:
    _patch_handles_to_empty(monkeypatch)
    events = [
        _make_event("p1", "OPEN", position_type="LP"),
        _make_event("p2", "OPEN", position_type="PERP"),
    ]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client, position_types=["LP"])

    assert client.calls == [["LP"]]
    rows = stub_st.dataframes[0]
    assert {r["Position"] for r in rows} == {"LP"}


def test_render_sorts_events_newest_first_before_applying_limit(stub_st, monkeypatch) -> None:
    """``GetPositionEventsFiltered`` orders by (position_id, timestamp, id)
    for backfill determinism — NOT newest-first. Without an explicit sort,
    a strategy whose total events exceed ``limit`` could drop newer rows
    for later position_ids while keeping older rows for earlier
    position_ids. Asserts the render sorts by timestamp DESC before
    slicing so the rendered "recent N" table genuinely is the most-recent
    N events. (Codex P2 / pr-audit-pr-2373.)"""
    _patch_handles_to_empty(monkeypatch)
    # Construct events deliberately ordered the way the gateway returns
    # them — grouped by position_id, with newer events on the "later"
    # position arriving AFTER older events on the "earlier" position.
    events = [
        _make_event("p_early", "OPEN", timestamp="2026-05-01T00:00:00"),
        _make_event("p_early", "CLOSE", timestamp="2026-05-02T00:00:00"),
        _make_event("p_late", "OPEN", timestamp="2026-05-19T00:00:00"),
        _make_event("p_late", "CLOSE", timestamp="2026-05-20T00:00:00"),
    ]
    client = _FakeAPIClient(events)

    # limit=2 — only the two newest events should render.
    render_position_lifecycle_section("strat", client, limit=2)

    rows = stub_st.dataframes[0]
    timestamps = [r["Time"] for r in rows]
    # Newest-first: 2026-05-20 then 2026-05-19. The older p_early rows
    # must NOT appear despite being earlier in the gateway's stream order.
    assert timestamps == ["2026-05-20T00:00:00", "2026-05-19T00:00:00"], (
        f"events must be sorted newest-first before applying limit; got: {timestamps!r}"
    )


def test_render_suppresses_divider_when_heading_empty(stub_st, monkeypatch) -> None:
    """An empty ``heading`` means "compose me inside a larger panel" — both
    the heading AND the leading divider should be suppressed so the
    section docks cleanly under another section without a doubled-up
    divider (Claude pr-auditor Important #5)."""
    _patch_handles_to_empty(monkeypatch)
    events = [_make_event("p1", "OPEN")]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client, heading="")

    # No leading divider (would be the 1st divider if heading were non-empty).
    assert stub_st.dividers == 0, (
        f"empty heading must also suppress the leading divider; got {stub_st.dividers} divider call(s)"
    )
    # And no markdown for the heading either.
    assert all(not m.startswith("###") for m in stub_st.markdowns), stub_st.markdowns


def test_render_emits_divider_when_heading_non_empty(stub_st, monkeypatch) -> None:
    """Inverse of the divider-suppress test — by default the section emits
    both the leading divider and the heading markdown."""
    _patch_handles_to_empty(monkeypatch)
    events = [_make_event("p1", "OPEN")]
    client = _FakeAPIClient(events)

    render_position_lifecycle_section("strat", client)

    assert stub_st.dividers == 1
    assert any("Position Lifecycle" in m for m in stub_st.markdowns), stub_st.markdowns
