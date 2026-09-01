"""Contracts for the Lab navigation projection.

The lane switcher and the Evidence hub read eight indexes that do not agree on
field names — ``status`` here, ``verdict`` there, ``official_verdict`` for
Quant. The danger is not that a reader breaks loudly; it is that it breaks
quietly, reporting a populated lane as ``0 sealed`` because the field it looks
for was renamed. An empty lane and an unreadable lane look identical on a board
and mean opposite things.

These tests pin the property that makes such a drift survivable: an unreadable
row is counted as ``unknown`` and turns the lane ``UNKNOWN``, never ``NEVER``.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _load(name: str) -> ModuleType:
    path = REPO_ROOT / "scripts" / "quant-test" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"{name}_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def nav() -> ModuleType:
    return _load("qa_nav")


@pytest.fixture(scope="module")
def coverage() -> ModuleType:
    return _load("qa_coverage")


def _store(
    tmp_path: Path,
    *,
    index: dict | None = None,
    name: str = "demo_latest.json",
    catalog: dict | None = None,
    catalog_name: str = "demo_cells.json",
) -> Path:
    (tmp_path / "index").mkdir(parents=True, exist_ok=True)
    (tmp_path / "catalog").mkdir(parents=True, exist_ok=True)
    if index is not None:
        (tmp_path / "index" / name).write_text(json.dumps(index), encoding="utf-8")
    if catalog is not None:
        (tmp_path / "catalog" / catalog_name).write_text(json.dumps(catalog), encoding="utf-8")
    return tmp_path


def _lane(states: list[dict], key: str) -> dict:
    return next(state for state in states if state["key"] == key)


# ---------------------------------------------------------------------------
# Fail-closed reads — the property that makes accessor drift loud
# ---------------------------------------------------------------------------


def test_row_missing_the_declared_field_is_unknown_not_absent(nav: ModuleType, tmp_path: Path) -> None:
    """The load-bearing test. A renamed writer field must not read as 0 sealed."""
    store = _store(
        tmp_path,
        index={"demo.a.anvil.eoa": {"result": "PASS", "completed_at": "2026-08-31T00:00:00+00:00"}},
        catalog={"cells": [{"cell_id": "demo.a.anvil.eoa"}]},
    )
    demo = _lane(nav.lane_states(store), "demo")
    assert demo["status"] == "UNKNOWN"
    assert demo["unknown"] == 1
    assert demo["pass"] == 0
    assert "may have drifted" in demo["detail"]


def test_unknown_lane_is_distinguishable_from_never_run(nav: ModuleType, tmp_path: Path) -> None:
    """ "Nothing ran" and "cannot be read" are opposite facts and must not collide."""
    empty = _lane(nav.lane_states(_store(tmp_path, index={}, catalog={"cells": []})), "demo")
    assert empty["status"] == "NEVER"
    assert empty["unknown"] == 0


def test_corrupt_index_is_unknown_not_empty(nav: ModuleType, tmp_path: Path) -> None:
    store = _store(tmp_path, catalog={"cells": []})
    (store / "index" / "demo_latest.json").write_text("{not json", encoding="utf-8")
    demo = _lane(nav.lane_states(store), "demo")
    assert demo["status"] == "UNKNOWN"
    assert demo["detail"] == "index unreadable"


def test_unreadable_catalog_yields_no_denominator_rather_than_zero(nav: ModuleType, tmp_path: Path) -> None:
    """A lane with no denominator must render "?", never a coverage figure it cannot support."""
    store = _store(tmp_path, index={})
    (store / "catalog" / "demo_cells.json").write_text("{not json", encoding="utf-8")
    assert _lane(nav.lane_states(store), "demo")["cells"] is None


def test_empty_verdict_string_counts_as_unknown(nav: ModuleType, tmp_path: Path) -> None:
    """Empty is unmeasured, never a pass — the Empty != Zero rule, applied to chrome."""
    store = _store(
        tmp_path,
        index={"demo.a.anvil.eoa": {"status": "", "completed_at": "2026-08-31T00:00:00+00:00"}},
        catalog={"cells": [{"cell_id": "demo.a.anvil.eoa"}]},
    )
    demo = _lane(nav.lane_states(store), "demo")
    assert demo["status"] == "UNKNOWN"
    assert demo["unknown"] == 1


# ---------------------------------------------------------------------------
# Verdict accounting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "verdict,expect_status,bucket",
    [
        ("PASS", "PASS", "pass"),
        ("FAIL", "FAIL", "fail"),
        ("VOID", "PARTIAL", "other"),
        ("UNVERIFIED", "PARTIAL", "other"),
    ],
)
def test_verdicts_land_in_the_right_bucket(
    nav: ModuleType, tmp_path: Path, verdict: str, expect_status: str, bucket: str
) -> None:
    store = _store(
        tmp_path,
        index={"demo.a.anvil.eoa": {"status": verdict, "completed_at": "2026-08-31T00:00:00+00:00"}},
        catalog={"cells": [{"cell_id": "demo.a.anvil.eoa"}]},
    )
    demo = _lane(nav.lane_states(store), "demo")
    assert demo["status"] == expect_status
    assert demo[bucket] == 1


def test_a_void_result_never_counts_toward_pass(nav: ModuleType, tmp_path: Path) -> None:
    """VOID means ungradeable. Rolling it into green would restore the exact
    failure the Demo lane's digest binding exists to prevent."""
    store = _store(
        tmp_path,
        index={
            "demo.a.anvil.eoa": {"status": "PASS", "completed_at": "2026-08-31T00:00:00+00:00"},
            "demo.b.anvil.eoa": {"status": "VOID", "completed_at": "2026-08-31T00:00:00+00:00"},
        },
        catalog={"cells": [{"cell_id": "demo.a.anvil.eoa"}, {"cell_id": "demo.b.anvil.eoa"}]},
    )
    demo = _lane(nav.lane_states(store), "demo")
    assert (demo["pass"], demo["other"], demo["status"]) == (1, 1, "PARTIAL")


def test_failure_dominates_the_lane_status(nav: ModuleType, tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        index={
            "demo.a.anvil.eoa": {"status": "PASS", "completed_at": "2026-08-31T00:00:00+00:00"},
            "demo.b.anvil.eoa": {"status": "FAIL", "completed_at": "2026-08-31T00:00:00+00:00"},
        },
        catalog={"cells": []},
    )
    assert _lane(nav.lane_states(store), "demo")["status"] == "FAIL"


def test_totals_flag_lanes_with_no_denominator(nav: ModuleType, tmp_path: Path) -> None:
    store = _store(tmp_path, index={})
    roll = nav.totals(nav.lane_states(store))
    assert roll["cells_unknown_lanes"] == len(nav.LANE_SOURCES)
    assert roll["sealed"] == 0


# ---------------------------------------------------------------------------
# Declaration integrity
# ---------------------------------------------------------------------------


def test_every_declared_verdict_field_appears_in_its_writer(nav: ModuleType) -> None:
    """A drift canary, not a proof.

    It cannot show the field is written into *this* lane's index row without
    duplicating the writer, but it does catch an outright rename — the most
    likely way a declaration goes stale. The fail-closed tests above are what
    make a drift this misses visible at runtime rather than silent.
    """
    for lane in nav.LANE_SOURCES:
        source = (REPO_ROOT / str(lane["writer"])).read_text(encoding="utf-8")
        needle = f'"{lane["verdict_field"]}":'
        assert needle in source, f"{lane['key']}: {needle} not found in {lane['writer']}"
        assert f'"{lane["time_field"]}":' in source, f"{lane['key']}: {lane['time_field']} missing"


def test_every_lane_writer_exists(nav: ModuleType) -> None:
    for lane in nav.LANE_SOURCES:
        assert (REPO_ROOT / str(lane["writer"])).is_file()


def test_lane_keys_and_pages_are_unique(nav: ModuleType) -> None:
    keys = [lane["key"] for lane in nav.LANE_SOURCES]
    pages = [lane["page"] for lane in nav.LANE_SOURCES]
    assert len(set(keys)) == len(keys)
    assert len(set(pages)) == len(pages)


# ---------------------------------------------------------------------------
# Navigation structure
# ---------------------------------------------------------------------------


def test_every_lane_page_is_reachable_through_evidence(nav: ModuleType, coverage: ModuleType) -> None:
    """A lane whose page is not mapped would be rendered but unreachable."""
    for lane in nav.LANE_SOURCES:
        page = str(lane["page"])
        assert page in coverage._PAGE_DESTINATION, f"{page} is not in the destination map"
        assert coverage._PAGE_DESTINATION[page] == nav.EVIDENCE_PAGE


def test_destination_hrefs_are_themselves_mapped(coverage: ModuleType) -> None:
    for href, _label in coverage._LAB_DESTINATIONS:
        assert href in coverage._PAGE_DESTINATION


def test_evidence_pages_are_derived_from_the_map(coverage: ModuleType) -> None:
    """Two hand-maintained lists would drift; this one is computed from the map."""
    expected = {page for page, destination in coverage._PAGE_DESTINATION.items() if destination == "evidence.html"}
    assert coverage._EVIDENCE_PAGES == expected


def test_nav_stays_small(coverage: ModuleType) -> None:
    """The regression this whole change exists to prevent: one tab per lane.

    Adding a lane must not widen the bar. If this fails, a lane was promoted to
    a top-level destination and the growth axis is back in a fixed container.
    """
    assert len(coverage._LAB_DESTINATIONS) <= 4


def test_unknown_page_is_rejected(coverage: ModuleType) -> None:
    page = (
        '<html><head><style></style></head><body><header><div class="brand">'
        '<div class="sub">x</div></div><nav><a class="btn active" href="nope.html">N</a>'
        "</nav></header></body></html>"
    )
    with pytest.raises(ValueError, match="Unknown QA Lab page"):
        coverage._with_extended_nav(page)


# ---------------------------------------------------------------------------
# Chrome rendering
# ---------------------------------------------------------------------------


def test_switcher_shows_state_for_every_lane(nav: ModuleType, tmp_path: Path) -> None:
    states = nav.lane_states(_store(tmp_path, index={}))
    markup = nav.lane_switcher_html(states, active_page="intent.html")
    for lane in nav.LANE_SOURCES:
        assert f'href="{lane["page"]}"' in markup
        assert str(lane["label"]) in markup
    assert "Evidence · Intent" in markup


def test_ledger_chip_reports_a_failed_verification(nav: ModuleType) -> None:
    """A broken ledger must not render as the same green chip as a good one."""
    good = nav.ledger_chip_html({"ledger_sha256": "abc1234def", "verified": True})
    bad = nav.ledger_chip_html({"ledger_sha256": "abc1234def", "verified": False})
    missing = nav.ledger_chip_html(None)
    assert "chip-ledger bad" not in good
    assert "chip-ledger bad" in bad
    assert "chip-ledger bad" in missing


def test_evidence_hub_renders_and_names_unknown_lanes(nav: ModuleType, tmp_path: Path) -> None:
    store = _store(tmp_path, catalog={"cells": [{"cell_id": "x"}]})
    (store / "index" / "demo_latest.json").write_text("{not json", encoding="utf-8")
    (store / "lab").mkdir(parents=True, exist_ok=True)
    out = nav.render_evidence_lab(store=store, lab_css="", history_verification={"ledger_sha256": "a" * 64})
    markup = out.read_text(encoding="utf-8")
    assert "chip-ledger" in markup, "the verification passed in must be rendered, not silently dropped"
    assert "lane(s) report UNKNOWN" in markup
    assert "never averaged into a single grade" in markup
