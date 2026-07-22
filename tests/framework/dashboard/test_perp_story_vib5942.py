"""VIB-5942 / ALM-2977 — snapshot-derived perp position story.

Three layers pinned:

* **Gateway populate** (``DashboardServiceServicer._perp_summaries_from_snapshot``):
  the latest snapshot's PERP positions → ``PerpPositionSummary`` protos, plural,
  Empty≠Zero per field, with the snapshot timestamp as provenance. No-snapshot /
  no-perp / multi-perp / measured-vs-unmeasured are all covered.
* **Client mapping** (``gateway_client._convert_perp_position``): "" → None
  (unmeasured), measured → Decimal, ``optional bool is_long`` both directions.
* **Render** (``sections.render_perp_positions_section`` + helpers): measured
  value vs "— unmeasured", stale-snapshot warning, empty-state caption.

Plus a real-fork check on the avax + arb mainnet fixture DBs asserting the OPEN
snapshot's known LONG perp is rendered with measured entry / leverage / notional.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.framework.dashboard import sections
from almanak.framework.dashboard.gateway_client import (
    PerpPositionInfo,
    PnLSummary,
    _convert_perp_position,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer as Svc


def _perp_pos(details: dict, *, protocol="gmx_v2", chain="avalanche"):
    return SimpleNamespace(position_type="PERP", protocol=protocol, chain=chain, details=details)


def _snapshot(positions, ts="2026-07-21T02:23:23+00:00"):
    return SimpleNamespace(timestamp=datetime.fromisoformat(ts), positions=positions)


# ---------------------------------------------------------------------------
# Gateway populate
# ---------------------------------------------------------------------------


def test_no_snapshot_yields_empty():
    summaries, as_of = Svc._perp_summaries_from_snapshot(None)
    assert summaries == []
    assert as_of == ""


def test_snapshot_without_perp_yields_empty_but_stamps_ts():
    snap = _snapshot([SimpleNamespace(position_type="LP", protocol="uniswap_v3", chain="base", details={})])
    summaries, as_of = Svc._perp_summaries_from_snapshot(snap)
    assert summaries == []
    assert as_of == "2026-07-21T02:23:23+00:00"


def test_single_measured_perp_populates_all_fields():
    snap = _snapshot(
        [
            _perp_pos(
                {
                    "market": "ETH/USD",
                    "is_long": True,
                    "entry_price_usd": "1905.98",
                    "mark_price_usd": "1906.18",
                    "leverage": "2.0016",
                    "size_usd": "20",
                    "collateral_value_usd": "9.99",
                    "unrealized_pnl_usd": "0.002",
                }
            )
        ]
    )
    summaries, _ = Svc._perp_summaries_from_snapshot(snap)
    assert len(summaries) == 1
    s = summaries[0]
    assert s.market == "ETH/USD"
    assert s.HasField("is_long") and s.is_long is True
    assert s.direction == "LONG"
    assert s.entry_price_usd == "1905.98"
    assert s.leverage == "2.0016"
    assert s.notional_usd == "20"
    assert s.collateral_usd == "9.99"
    assert s.protocol == "gmx_v2"


def test_multiple_perps_all_rendered():
    snap = _snapshot(
        [
            _perp_pos({"market": "ETH/USD", "is_long": True, "leverage": "2"}),
            _perp_pos({"market": "BTC/USD", "is_long": False, "leverage": "3"}, chain="arbitrum"),
        ]
    )
    summaries, _ = Svc._perp_summaries_from_snapshot(snap)
    assert len(summaries) == 2
    assert summaries[1].direction == "SHORT"
    assert summaries[1].is_long is False


def test_unmeasured_fields_go_on_wire_as_empty_string():
    """Empty≠Zero: an absent detail is "" (UNMEASURED), never "0"."""
    snap = _snapshot([_perp_pos({"market": "ETH/USD"})])  # only market measured
    summaries, _ = Svc._perp_summaries_from_snapshot(snap)
    s = summaries[0]
    assert s.entry_price_usd == ""
    assert s.leverage == ""
    assert s.notional_usd == ""
    # is_long absent → optional not set → direction "" (never defaulted to LONG).
    assert not s.HasField("is_long")
    assert s.direction == ""


def test_direction_from_side_string_when_no_bool():
    snap = _snapshot([_perp_pos({"market": "ETH/USD", "side": "short"})])
    s = Svc._perp_summaries_from_snapshot(snap)[0][0]
    assert not s.HasField("is_long")
    assert s.direction == "SHORT"


@pytest.mark.parametrize("bad_details", ["corrupt-string", 42, ["not", "a", "dict"], None])
def test_non_dict_details_degrades_to_unmeasured_no_crash(bad_details):
    """CodeRabbit: a non-dict ``details`` (corrupt/legacy row, malformed JSON round-trip)
    must NOT AttributeError and take down the whole GetPnLSummary RPC — it degrades to
    all-unmeasured, consistent with the section's Empty≠Zero rendering."""
    pos = SimpleNamespace(position_type="PERP", protocol="gmx_v2", chain="avalanche", details=bad_details)
    summaries, _ = Svc._perp_summaries_from_snapshot(_snapshot([pos]))  # must not raise
    assert len(summaries) == 1
    s = summaries[0]
    assert s.market == "" and s.entry_price_usd == "" and s.leverage == "" and s.notional_usd == ""
    assert not s.HasField("is_long") and s.direction == ""
    assert s.protocol == "gmx_v2"  # top-level fields (not from details) still populate


# ---------------------------------------------------------------------------
# Client mapping
# ---------------------------------------------------------------------------


def test_convert_perp_position_empty_prices_are_none():
    proto = gateway_pb2.PerpPositionSummary(market="ETH/USD", entry_price_usd="", leverage="")
    info = _convert_perp_position(proto)
    assert info.entry_price_usd is None
    assert info.leverage is None
    assert info.market == "ETH/USD"
    assert info.is_long is None
    assert info.direction == ""


def test_convert_perp_position_measured_round_trips():
    proto = gateway_pb2.PerpPositionSummary(
        market="ETH/USD", is_long=True, entry_price_usd="1905.98", leverage="2.0", notional_usd="20"
    )
    info = _convert_perp_position(proto)
    assert info.is_long is True
    assert info.direction == "LONG"
    assert info.entry_price_usd == Decimal("1905.98")
    assert info.leverage == Decimal("2.0")
    assert info.notional_usd == Decimal("20")


def test_convert_perp_position_short_measured_false_is_not_none():
    proto = gateway_pb2.PerpPositionSummary(market="ETH/USD", is_long=False)
    info = _convert_perp_position(proto)
    assert info.is_long is False
    assert info.direction == "SHORT"


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def _pnl_with(perp_positions, positions_as_of=""):
    return PnLSummary(
        deployed_usd=Decimal("0"),
        nav_usd=Decimal("0"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        open_position_count=len(perp_positions),
        primary_risk_kind="perp",
        primary_risk_label="Leverage",
        primary_risk_value="2.0x",
        primary_risk_color="green",
        perp_positions=perp_positions,
        positions_as_of=positions_as_of,
    )


def _render(pnl) -> tuple[list[str], list[str], list[str]]:
    markdowns: list[str] = []
    captions: list[str] = []
    warnings: list[str] = []
    with (
        patch.object(sections, "get_pnl_summary", return_value=pnl),
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown", side_effect=lambda t, *a, **k: markdowns.append(t)),
        patch.object(sections.st, "caption", side_effect=lambda t, *a, **k: captions.append(t)),
        patch.object(sections.st, "warning", side_effect=lambda t, *a, **k: warnings.append(t)),
    ):
        sections.render_perp_positions_section("dep")
    return markdowns, captions, warnings


def test_render_empty_shows_caption_not_crash():
    _md, captions, _w = _render(_pnl_with([]))
    assert any("No open perp positions" in c for c in captions)


def test_render_gateway_outage_is_explicit_unavailable_not_empty():
    """VIB-5942 CodeRabbit #2: get_pnl_summary swallows a gateway outage → None. That
    must render an EXPLICIT unavailable state, NEVER a healthy 'no open perp positions'
    (fail-silent on money visibility)."""
    infos: list[str] = []
    captions: list[str] = []
    with (
        patch.object(sections, "get_pnl_summary", return_value=None),  # outage
        patch.object(sections.st, "divider"),
        patch.object(sections.st, "markdown"),
        patch.object(sections.st, "caption", side_effect=lambda t, *a, **k: captions.append(t)),
        patch.object(sections.st, "info", side_effect=lambda t, *a, **k: infos.append(t)),
    ):
        sections.render_perp_positions_section("dep")
    assert infos and "unavailable" in infos[0].lower()
    # It must NOT have rendered the healthy "no positions" caption on an outage.
    assert not any("No open perp positions" in c for c in captions)


def test_render_measured_perp_shows_values_and_direction():
    pos = PerpPositionInfo(
        market="ETH/USD",
        direction="LONG",
        is_long=True,
        entry_price_usd=Decimal("1905.98"),
        mark_price_usd=Decimal("1906.18"),
        leverage=Decimal("2.0016"),
        notional_usd=Decimal("20"),
        collateral_usd=Decimal("9.99"),
        unrealized_pnl_usd=Decimal("0.002"),
        protocol="gmx_v2",
    )
    fresh = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    md, _c, warnings = _render(_pnl_with([pos], positions_as_of=fresh))
    html = "\n".join(md)
    assert "LONG" in html
    assert "ETH/USD" in html
    assert "2.00×" in html  # leverage formatted
    assert "$1,905.98" in html
    assert "gmx_v2" in html
    assert "unmeasured" not in html  # every field measured
    assert warnings == []  # fresh snapshot → no stale warning


def test_render_unmeasured_fields_show_dash_not_zero():
    pos = PerpPositionInfo(market="ETH/USD", direction="", is_long=None)  # only market
    md, _c, _w = _render(_pnl_with([pos], positions_as_of=(datetime.now(UTC)).isoformat()))
    html = "\n".join(md)
    assert "— unmeasured" in html
    # Direction unmeasured must NOT default to LONG, and no "$0.00" fabrication.
    assert "LONG" not in html
    assert "$0.00" not in html


def test_render_stale_snapshot_warns():
    pos = PerpPositionInfo(market="ETH/USD", direction="LONG", is_long=True)
    stale = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    _md, _c, warnings = _render(_pnl_with([pos], positions_as_of=stale))
    assert warnings and "stale snapshot" in warnings[0].lower()


# ---------------------------------------------------------------------------
# Real-fork check — both mainnet fixture DBs (skips if the untracked DB is absent)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "db,dep",
    [
        (".fixtures/gmx-avax-mainnet.sqlite", "deployment:e32d997e1002"),
        (".fixtures/gmx-arb-mainnet.sqlite", "deployment:a5693f691bf1"),
    ],
)
@pytest.mark.asyncio
async def test_real_fork_open_snapshot_renders_long_perp(db, dep):
    if not os.path.exists(db):
        pytest.skip(f"fixture DB {db} not present (untracked)")
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

    store = SQLiteStore(SQLiteConfig(db_path=db))
    await store.initialize()
    snaps = await store.get_recent_snapshots(dep, limit=168)
    # Select the OPEN snapshot EXPLICITLY (VIB-5942 CodeRabbit — do not assume the
    # get_recent_snapshots ordering / snaps[0]): it is the one carrying a PERP
    # position. The CLOSED snapshot is the strictly-latest by timestamp (post-close).
    open_snaps = [sn for sn in snaps if Svc._perp_summaries_from_snapshot(sn)[0]]
    assert len(open_snaps) == 1, "exactly one snapshot must carry the open perp"
    summaries, as_of = Svc._perp_summaries_from_snapshot(open_snaps[0])
    assert len(summaries) == 1, "open snapshot must carry exactly one perp"
    s = summaries[0]
    assert s.direction == "LONG"
    assert s.HasField("is_long") and s.is_long is True
    assert Decimal(s.leverage) == pytest.approx(Decimal("2.0"), abs=Decimal("0.1"))
    assert Decimal(s.entry_price_usd) > 0
    assert Decimal(s.notional_usd) > 0
    assert s.protocol == "gmx_v2"
    # Latest (post-close) snapshot BY TIMESTAMP → no open perp.
    latest = max(snaps, key=lambda sn: sn.timestamp)
    latest_summaries, _ = Svc._perp_summaries_from_snapshot(latest)
    assert latest_summaries == []
    await store.close()
