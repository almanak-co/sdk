"""Tests for ``scripts/ci/check_strategy_taxonomy.py``.

Mirrors the ``importlib``-load pattern of the other ``tests/scripts/`` guards
so the script's internals are driven directly without re-shelling. Covers the
four flagship taxonomy predicates:

* FAIL on a committed config that sets a non-empty ``force_action``.
* PASS on the read-as-optional knob (the flagship-#5 false-positive guard).
* FAIL on a committed launch recipe using ``--once`` / ``--max-iterations`` /
  ``--teardown-after``.
* WARN (no fail) on a source mention of ``force_action``.
* No-op pass when ``strategies/flagship/`` does not exist yet (Cycle -1).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_strategy_taxonomy.py"
    spec = importlib.util.spec_from_file_location("check_strategy_taxonomy", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_strategy_taxonomy"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def module():
    return _load_module()


def _make_strategy(
    flagship: Path,
    name: str,
    *,
    config: dict | None = None,
    config_name: str = "config.json",
    source: str = "class S:\n    pass\n",
    readme: str | None = None,
    readme_name: str = "README.md",
) -> Path:
    d = flagship / name
    d.mkdir(parents=True)
    (d / "strategy.py").write_text(source, encoding="utf-8")
    if config is not None:
        (d / config_name).write_text(json.dumps(config), encoding="utf-8")
    if readme is not None:
        (d / readme_name).write_text(readme, encoding="utf-8")
    return d


def test_noop_pass_when_flagship_dir_absent(module, tmp_path):
    rc = module.main(["--strategies-dir", str(tmp_path / "flagship")])
    assert rc == 0


def test_clean_sound_strategy_passes(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "lst_basis",
        config={"chain": "ethereum", "protocol": "curve", "total_value_usd": 4.0},
        source="def run(self):\n    return self.config.get('chain')\n",
        readme="Run: `uv run almanak strat run --network anvil`\n",
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 0


def test_force_action_nonempty_config_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "pipe_demo",
        config={"chain": "ethereum", "force_action": "lifecycle"},
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_force_action_empty_string_passes(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "ok_default",
        config={"chain": "ethereum", "force_action": ""},
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 0


def test_read_as_optional_knob_passes_with_source_warn(module, tmp_path):
    """The flagship-#5 false-positive guard: source READS force_action as an
    optional knob (default empty) and the committed config does NOT set it."""
    flagship = tmp_path / "flagship"
    strat = _make_strategy(
        flagship,
        "hedged_lp_weth_usdc",
        config={"chain": "arbitrum", "protocol": "uniswap_v3"},
        source='force_action = self.gc("force_action", "")\n',
    )
    rc = module.main(["--strategies-dir", str(flagship), "--verbose"])
    assert rc == 0  # WARN does not fail the build

    reports = module.evaluate(flagship)
    report = next(r for r in reports if r.path == strat)
    assert report.ok
    assert report.warns  # the source mention is surfaced as a WARN


def test_forbidden_launch_flag_in_readme_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    for flag in ("--once", "--max-iterations 3", "--teardown-after 60"):
        name = "f_" + flag.strip("-").split()[0].replace("-", "_")
        _make_strategy(
            flagship,
            name,
            config={"chain": "ethereum"},
            readme=f"uv run almanak strat run {flag}\n",
        )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_forbidden_launch_flag_in_lowercase_readme_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "lowercase_readme",
        config={"chain": "ethereum"},
        readme="uv run almanak strat run --once\n",
        readme_name="readme.md",
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_unreadable_launch_recipe_fails(module, tmp_path, monkeypatch):
    flagship = tmp_path / "flagship"
    strat = _make_strategy(
        flagship,
        "unreadable_recipe",
        config={"chain": "ethereum"},
        readme="uv run almanak strat run --network anvil\n",
    )
    original_read_text = Path.read_text

    def _read_text(path: Path, *args, **kwargs):
        if path == strat / "README.md":
            raise OSError("permission denied")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _read_text)

    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_nested_force_action_under_params_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "nested",
        config={"chain": "ethereum", "params": {"force_action": "open"}},
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_nested_force_action_under_config_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    _make_strategy(
        flagship,
        "nested_config",
        config={"chain": "ethereum", "config": {"force_action": "close"}},
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_invalid_config_json_fails(module, tmp_path):
    flagship = tmp_path / "flagship"
    d = flagship / "broken"
    d.mkdir(parents=True)
    (d / "strategy.py").write_text("x = 1\n", encoding="utf-8")
    (d / "config.json").write_text("{ not valid json ", encoding="utf-8")
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1


def test_repoint_variant_config_also_scanned(module, tmp_path):
    """A re-point variant (config_*.json) that hardcodes force_action is also a
    pipe-exerciser and must FAIL."""
    flagship = tmp_path / "flagship"
    strat = _make_strategy(
        flagship,
        "variant",
        config={"chain": "ethereum"},  # clean default
    )
    (strat / "config_fluid_vault.json").write_text(
        json.dumps({"chain": "arbitrum", "force_action": "close_both"}), encoding="utf-8"
    )
    rc = module.main(["--strategies-dir", str(flagship)])
    assert rc == 1
