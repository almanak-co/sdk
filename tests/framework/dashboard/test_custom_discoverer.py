"""Tests for custom dashboard discovery."""

from pathlib import Path

from almanak.framework.dashboard.custom.discoverer import discover_custom_dashboards


def _write_dashboard(strategy_dir: Path, icon: str = "X") -> None:
    dashboard_dir = strategy_dir / "dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    (dashboard_dir / "ui.py").write_text("def render_custom_dashboard(*args, **kwargs):\n    return None\n")
    (dashboard_dir / "metadata.json").write_text(
        f'{{"display_name": "{strategy_dir.name}", "description": "test", "icon": "{icon}"}}'
    )


def test_discoverer_finds_nested_category_dashboards(tmp_path: Path) -> None:
    """Should discover dashboards under strategies/<category>/<strategy>/dashboard/ui.py."""
    strategies_dir = tmp_path / "strategies"
    _write_dashboard(strategies_dir / "demo" / "strat_a", icon="A")
    _write_dashboard(strategies_dir / "incubating" / "strat_b", icon="B")

    dashboards = discover_custom_dashboards(strategies_dir=strategies_dir)
    names = {d.strategy_name for d in dashboards}

    assert names == {"strat_a", "strat_b"}
    assert len(dashboards) == 2


def test_discoverer_finds_flat_strategy_dashboards(tmp_path: Path) -> None:
    """Should still discover flat strategies/<strategy>/dashboard/ui.py layout."""
    strategies_dir = tmp_path / "strategies"
    _write_dashboard(strategies_dir / "strat_flat", icon="F")

    dashboards = discover_custom_dashboards(strategies_dir=strategies_dir)

    assert len(dashboards) == 1
    assert dashboards[0].strategy_name == "strat_flat"
