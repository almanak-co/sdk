"""Branch coverage for find_strategy_dir.

Covers every search location: direct, src/, tiered, demo_ prefix mapping,
and the test_ prefix expansion across tests/ subdirectories. All against a
tmp_path CWD.
"""

from pathlib import Path

import pytest

from almanak.framework.cli.run import find_strategy_dir


@pytest.fixture(autouse=True)
def _isolated_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _mkdir(relative: str) -> Path:
    path = Path(relative)
    path.mkdir(parents=True)
    return path


def test_not_found_returns_none():
    assert find_strategy_dir("ghost") is None


def test_direct_strategies_path():
    expected = _mkdir("strategies/my_strat")
    assert find_strategy_dir("my_strat") == expected


def test_src_strategies_path():
    expected = _mkdir("src/strategies/my_strat")
    assert find_strategy_dir("my_strat") == expected


@pytest.mark.parametrize(
    "tier", ["poster_child", "production", "incubating", "demo", "alpha_team", "tests", "accounting"]
)
def test_tiered_paths(tier):
    expected = _mkdir(f"strategies/{tier}/my_strat")
    assert find_strategy_dir("my_strat") == expected


def test_demo_prefix_maps_to_demo_strategies():
    expected = _mkdir("almanak/demo_strategies/lp_rebalancer")
    assert find_strategy_dir("demo_lp_rebalancer") == expected


def test_demo_prefix_falls_back_to_plain_dir():
    expected = _mkdir("strategies/demo_lp_rebalancer")
    assert find_strategy_dir("demo_lp_rebalancer") == expected


def test_test_prefix_direct_tests_dir():
    expected = _mkdir("strategies/tests/momentum")
    assert find_strategy_dir("test_momentum") == expected


def test_test_prefix_searches_subdirectories_stripped():
    _mkdir("strategies/tests")
    expected = _mkdir("strategies/tests/lp/momentum")
    assert find_strategy_dir("test_momentum") == expected


def test_test_prefix_searches_subdirectories_with_prefix():
    _mkdir("strategies/tests")
    expected = _mkdir("strategies/tests/ta/test_momentum")
    assert find_strategy_dir("test_momentum") == expected


def test_test_prefix_skips_underscore_subdirectories():
    _mkdir("strategies/tests/_private/momentum")
    assert find_strategy_dir("test_momentum") is None


def test_file_with_matching_name_is_not_a_match():
    Path("strategies").mkdir()
    (Path("strategies") / "my_strat").write_text("not a directory")
    assert find_strategy_dir("my_strat") is None
