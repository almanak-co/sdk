"""The Polygon V4 replay admits only the capture source or a reviewed successor pair."""

import pytest

from tests.uniswap_v4_polygon_support import COMPATIBLE_PATHS, REVIEWED_SUCCESSORS, compatible

BOOK = "src/almanak/connectors/uniswap_v4/economic_book.py"
CAPTURED_BOOK, CURRENT_BOOK = REVIEWED_SUCCESSORS[BOOK]


def capture(style, **overrides):
    paths = (*COMPATIBLE_PATHS, f"strategies/uniswap_v4_polygon_{style}/strategy.py")
    files = {relative: f"captured:{relative}" for relative in paths}
    files[BOOK] = CAPTURED_BOOK
    files.update(overrides)
    return files


def source(files, **overrides):
    current = {**files, **overrides}
    return lambda relative: current[relative]


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_unchanged_source_is_compatible(style):
    files = capture(style)
    assert compatible(files, style, source(files))


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_reviewed_successor_is_compatible(style):
    files = capture(style)
    assert compatible(files, style, source(files, **{BOOK: CURRENT_BOOK}))


def test_unreviewed_change_refuses():
    files = capture("lp")
    assert not compatible(files, "lp", source(files, **{BOOK: "0" * 64}))
    worker = "tests/qa/worker.py"
    assert not compatible(files, "lp", source(files, **{worker: "changed"}))


def test_successor_requires_the_reviewed_capture():
    files = capture("lp", **{BOOK: "1" * 64})
    assert not compatible(files, "lp", source(files, **{BOOK: CURRENT_BOOK}))


def test_missing_recorded_path_refuses():
    files = capture("trading")
    current = source(files)
    del files["tests/qa/worker.py"]
    assert not compatible(files, "trading", current)


@pytest.mark.parametrize("relative", sorted(REVIEWED_SUCCESSORS))
def test_reviewed_successor_is_the_current_source(relative):
    from tests.uniswap_v4_polygon_support import ROOT, digest

    assert digest(ROOT / relative) == REVIEWED_SUCCESSORS[relative][1]


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_reviewed_strategy_successor_is_exactly_the_reviewed_guard_edit(style):
    import hashlib

    from almanak.strategy import StrategyDefinition
    from tests.legacy_references import before_hold_guard
    from tests.uniswap_v4_polygon_support import ROOT

    relative = f"strategies/uniswap_v4_polygon_{style}/strategy.py"
    captured, current = REVIEWED_SUCCESSORS[relative]
    strategy = StrategyDefinition.read(ROOT / relative)
    before = {
        hashlib.sha256(text.encode()).hexdigest() for text in before_hold_guard(strategy.source)
    }
    assert captured != current and captured in before
