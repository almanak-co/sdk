"""`almanak support --references` and the wheel hook that ships reference author files."""

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

from almanak import references
from scripts.ci import verify_sdk_distribution

ROOT = Path(__file__).resolve().parents[1]
BAND = "uniswap_v3_robinhood_lp_band"
# A per-chain copy of a published family and a composed QA fixture: both stay in
# strategies/ for tests and proofs, and neither is listed or shipped.
HIDDEN = "uniswap_v3_robinhood_lp"
COMPOSED = "robinhood_morpho_borrow_and_trade_v3"


def publish(monkeypatch, *names):
    monkeypatch.setattr(references, "PUBLISHED", frozenset(names))


def run(monkeypatch, capsys, *argv):
    from almanak.cli import main

    monkeypatch.setattr("sys.argv", ["almanak", "support", *argv])
    code = main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_source_checkout_lists_only_published_references_from_strategies():
    listing = references.catalog()
    assert listing["directory"] == str((ROOT / "strategies").resolve())
    complete = {
        path.parent.name
        for path in (ROOT / "strategies").glob("*/strategy.py")
        if (path.parent / "config.yaml").is_file()
    }
    assert references.PUBLISHED <= complete
    listed = [entry["name"] for entry in listing["references"]]
    assert listed == sorted(references.PUBLISHED)
    assert HIDDEN in complete and HIDDEN not in listed
    assert references.reference(ROOT / "strategies" / HIDDEN) is None
    assert "does not establish support" in listing["note"]


TEMPLATES = {
    "aave_v3_arbitrum_managed_borrow",
    "arbitrum_aave_borrow_lp",
    "arbitrum_aave_lending_short",
    "arbitrum_aave_leverage_loop",
    "base_morpho_leverage_loop",
    "gmx_v2_arbitrum_price_stop",
}


def test_the_authoring_templates_are_published():
    assert len(references.PUBLISHED) == 46
    assert references.PUBLISHED >= TEMPLATES
    assert "morpho_blue_hyperevm_lending" not in references.PUBLISHED


def test_every_published_reference_has_a_title_and_one_line_summary():
    for entry in references.catalog()["references"]:
        assert entry["title"], entry["name"]
        assert entry["summary"] and "\n" not in entry["summary"], entry["name"]


def test_filters_select_references_with_one_matching_market(monkeypatch, capsys):
    code, out, _ = run(
        monkeypatch, capsys, "--references", "--connector", "uniswap_v3", "--chain", "eip155:4663"
    )
    assert code == 0
    listed = {entry["name"]: entry for entry in json.loads(out)["references"]}
    assert BAND in listed
    band = listed[BAND]
    assert band["markets"] == [{"connector": "uniswap_v3", "chain": "eip155:4663"}]
    assert band["files"] == ["README.md", "backtest-economic.yaml", "config.yaml", "strategy.py"]
    assert Path(band["path"]) == ROOT / "strategies" / BAND
    assert band["summary"] == references.summary(ROOT / "strategies" / BAND / "README.md")
    for entry in listed.values():
        assert {"connector": "uniswap_v3", "chain": "eip155:4663"} in entry["markets"]
    assert HIDDEN not in listed and COMPOSED not in listed


def test_a_composed_reference_matches_when_one_market_carries_both_filters(monkeypatch):
    publish(monkeypatch, COMPOSED)
    [entry] = references.catalog(connector="uniswap_v3", chain="eip155:4663")["references"]
    assert entry["name"] == COMPOSED
    assert entry["markets"] == [
        {"connector": "morpho_blue", "chain": "eip155:4663"},
        {"connector": "uniswap_v3", "chain": "eip155:4663"},
    ]
    assert references.catalog(connector="uniswap_v3", chain="eip155:1")["references"] == []


def test_evidence_and_production_variants_are_not_listed_files():
    listing = references.catalog(connector="aave_v4", chain="eip155:1")
    listed = {entry["name"]: entry["files"] for entry in listing["references"]}
    assert listed["aave_v4_weth_supply"] == [
        "README.md",
        "backtest-economic-vendor.yaml",
        "backtest-economic.yaml",
        "backtest.yaml",
        "config.yaml",
        "dashboard.py",
        "strategy.py",
    ]
    assert not references.is_author_file("observed-actions.json")
    assert not references.is_author_file("config.production.yaml")
    assert references.is_author_file("backtest-economic-vendor.yaml")


@pytest.mark.parametrize(
    ("argv", "field"),
    [
        (["--references", "--connector", "uniswap_v33"], "--connector"),
        (["--references", "--chain", "4663"], "--chain"),
    ],
)
def test_filters_are_validated_as_canonical_identities(monkeypatch, capsys, argv, field):
    code, _, err = run(monkeypatch, capsys, *argv)
    assert code == 1
    error = json.loads(err)
    assert (error["error"], error["field"]) == ("invalid_config", field)


def test_references_and_schema_are_exclusive(monkeypatch, capsys):
    with pytest.raises(SystemExit) as refused:
        run(monkeypatch, capsys, "--references", "--schema", "strategy")
    assert refused.value.code == 2


def test_listing_skips_incomplete_directories(monkeypatch, tmp_path):
    complete = tmp_path / "complete"
    complete.mkdir()
    (complete / "strategy.py").write_text("")
    (complete / "config.yaml").write_text(
        "markets:\n  a: {connector: curve, chain: 'eip155:10'}\n"
        "  b: {connector: curve, chain: 'eip155:10'}\n"
    )
    (complete / "README.md").write_text("No heading\n")
    (tmp_path / "unpublished").mkdir()
    (tmp_path / "unpublished" / "strategy.py").write_text("")
    (tmp_path / "unpublished" / "config.yaml").write_text("markets: {}\n")
    (tmp_path / "incomplete").mkdir()
    (tmp_path / "incomplete" / "strategy.py").write_text("")
    (tmp_path / "loose.txt").write_text("")
    monkeypatch.setattr(references, "directory", lambda: tmp_path)
    publish(monkeypatch, "complete", "incomplete")
    [entry] = references.catalog()["references"]
    assert entry["name"] == "complete"
    assert entry["title"] is None
    assert entry["summary"] is None
    assert entry["markets"] == [{"connector": "curve", "chain": "eip155:10"}]
    assert references.catalog(chain="eip155:1")["references"] == []
    monkeypatch.setattr(references, "directory", lambda: tmp_path / "absent")
    with pytest.raises(ValueError, match="no reference strategies"):
        references.catalog()


def test_the_bundled_copy_is_preferred_over_the_checkout(monkeypatch, tmp_path):
    package = tmp_path / "almanak" / "references"
    (package / "files").mkdir(parents=True)
    monkeypatch.setattr(references, "__file__", str(package / "__init__.py"))
    assert references.directory() == package / "files"


def load_hook(monkeypatch):
    # hatchling exists only in the isolated build environment.
    interface = types.ModuleType("hatchling.builders.hooks.plugin.interface")

    class BuildHookInterface:
        def __init__(self, root, target_name):
            self.root, self.target_name = root, target_name

    interface.BuildHookInterface = BuildHookInterface  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, interface.__name__, interface)
    spec = importlib.util.spec_from_file_location("hatch_build", ROOT / "scripts/hatch_build.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_build_hook_ships_what_the_release_verifier_expects(monkeypatch):
    hook = load_hook(monkeypatch)
    shipped = hook.reference_files(ROOT)
    verified = {
        name
        for name in verify_sdk_distribution.package_files(ROOT)
        if name.startswith(hook.DESTINATION + "/")
    }
    assert set(shipped.values()) == verified
    assert shipped[str(ROOT / "strategies" / BAND / "config.yaml")] == (
        f"almanak/references/files/{BAND}/config.yaml"
    )
    build_data: dict = {"force_include": {}}
    hook.ReferenceStrategies(str(ROOT), "sdist").initialize("3.0.0", build_data)
    assert build_data == {"force_include": {}}
    hook.ReferenceStrategies(str(ROOT), "wheel").initialize("3.0.0", build_data)
    assert build_data["force_include"] == shipped


def test_the_build_hook_refuses_a_missing_rule(monkeypatch, tmp_path):
    hook = load_hook(monkeypatch)
    monkeypatch.setattr(hook.importlib.util, "spec_from_file_location", lambda *_: None)
    with pytest.raises(RuntimeError, match="reference file rule"):
        hook.selection_rule(tmp_path)


def test_summary_is_the_one_line_quote_under_the_title(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text("# Title\n\n> Teaches one thing.\n\nBody.\n")
    assert references.summary(readme) == "Teaches one thing."
    readme.write_text("# Title\n\nBody first.\n\n> A later quote.\n")
    assert references.summary(readme) is None
    assert references.summary(tmp_path / "absent.md") is None


def write_reference(root: Path, name: str, config: str) -> Path:
    path = root / name
    path.mkdir()
    (path / "strategy.py").write_text("")
    (path / "config.yaml").write_text(config)
    return path


def test_unfiltered_listing_includes_a_reference_without_markets(tmp_path, monkeypatch):
    write_reference(tmp_path, "no_markets", "strategy: {}\n")
    monkeypatch.setattr(references, "directory", lambda: tmp_path)
    publish(monkeypatch, "no_markets")
    assert [entry["name"] for entry in references.catalog()["references"]] == ["no_markets"]
    assert references.catalog(connector="uniswap_v3")["references"] == []


@pytest.mark.parametrize(
    ("config", "field"),
    [
        (
            "markets:\n  pool: {connector: uniswap_v33, chain: 'eip155:1'}\n",
            "markets.pool.connector",
        ),
        ("markets:\n  pool: {connector: uniswap_v3, chain: '4663'}\n", "markets.pool.chain"),
        ("markets:\n  pool: {chain: 'eip155:1'}\n", "markets.pool.connector"),
        ("markets: [pool]\n", "markets"),
    ],
)
def test_invalid_reference_config_is_a_configuration_error(tmp_path, monkeypatch, config, field):
    from almanak.configuration.yaml import ConfigurationError

    write_reference(tmp_path, "broken", config)
    monkeypatch.setattr(references, "directory", lambda: tmp_path)
    publish(monkeypatch, "broken")
    with pytest.raises(ConfigurationError) as raised:
        references.catalog()
    assert raised.value.field == field


def test_reference_config_uses_the_author_loader_limits(tmp_path, monkeypatch):
    from almanak.configuration.yaml import ConfigurationError

    write_reference(
        tmp_path,
        "aliased",
        "markets:\n  a: &m {connector: uniswap_v3, chain: 'eip155:1'}\n  b: *m\n",
    )
    monkeypatch.setattr(references, "directory", lambda: tmp_path)
    publish(monkeypatch, "aliased")
    with pytest.raises(ConfigurationError, match="anchors and aliases"):
        references.catalog()


def test_empty_reference_config_reports_json_error(tmp_path, monkeypatch, capsys):
    write_reference(tmp_path, "empty", "")
    monkeypatch.setattr(references, "directory", lambda: tmp_path)
    publish(monkeypatch, "empty")
    code, out, err = run(monkeypatch, capsys, "--references")
    assert code != 0
    assert "Traceback" not in out + err


def test_backtest_file_rule_refuses_production_and_unnamed_variants():
    assert references.is_author_file("backtest.yaml")
    assert references.is_author_file("backtest-economic.yaml")
    assert not references.is_author_file("backtest.production.yaml")
    assert not references.is_author_file("backtest-economic.production.yaml")
    assert not references.is_author_file("backtestx.yaml")
