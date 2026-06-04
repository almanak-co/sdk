"""Tests for ``almanak strat check`` CLI.

These tests scaffold a strategy via ``strat new`` (so the fixtures stay in
sync with real output) and then run ``check`` against it. We cover:

- happy path: a freshly-scaffolded strategy is clean
- placeholder address corruption -> exit code 2, with file:line surfaced
- template-aware heuristic fires on perps template missing ``direction``

The tests invoke the command in-process via Click's ``CliRunner`` so they
run without spawning subprocesses.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from almanak.framework.cli.check import (
    CheckReport,
    Finding,
    Layer,
    Severity,
    check,
    run_checks,
)
from almanak.framework.cli.new_strategy import (
    StrategyTemplate,
    new_strategy,
)

# ---------------------------------------------------------------------------
# Scaffolding helper
# ---------------------------------------------------------------------------


def _scaffold(
    tmp_path: Path,
    template: StrategyTemplate,
    chain: str = "arbitrum",
    name: str = "check_fixture",
) -> Path:
    """Scaffold a fresh strategy into ``tmp_path`` and return the path."""
    target = tmp_path / name
    runner = CliRunner()
    # `env={"CI": ""}` mirrors what other scaffold tests do so auto-detect
    # doesn't redirect output into a strategies/incubating/ folder.
    result = runner.invoke(
        new_strategy,
        [
            "--name",
            name,
            "--template",
            template.value,
            "--chain",
            chain,
            "--output-dir",
            str(target),
        ],
        env={"CI": ""},
    )
    assert result.exit_code == 0, f"scaffold failed: {result.output}"
    assert (target / "strategy.py").exists()
    assert (target / "config.json").exists()
    return target


# ---------------------------------------------------------------------------
# Happy path: a scaffolded strategy becomes clean once obvious placeholders
# are filled in
# ---------------------------------------------------------------------------


def _clean_scaffolded_config(strategy_dir: Path) -> None:
    """Remove the scaffolded placeholders so ``check`` has nothing to flag.

    The ``strat new`` scaffold ships with placeholder ``0x000...`` addresses
    in its ``token_funding`` list (documented defaults). That's a real
    pre-flight finding, not a bug in ``check`` — so for the happy-path
    tests we strip it out before asserting a clean run.
    """
    config_path = strategy_dir / "config.json"
    config = json.loads(config_path.read_text())
    config.pop("token_funding", None)
    # VAULT_YIELD and similar templates also embed zero-address literals.
    for key in ("vault_address",):
        value = config.get(key)
        if isinstance(value, str) and value.startswith("0x0000"):
            config[key] = "0x1111111111111111111111111111111111111111"
    config_path.write_text(json.dumps(config, indent=4) + "\n")


@pytest.mark.parametrize(
    "template",
    [StrategyTemplate.TA_SWAP, StrategyTemplate.DYNAMIC_LP],
    ids=lambda t: t.value,
)
def test_check_clean_on_scaffolded_strategy(tmp_path: Path, template: StrategyTemplate) -> None:
    """After stripping scaffold placeholders, ``check`` reports no errors.

    We deliberately pick templates whose scaffolds have non-trivial teardown
    bodies so the AST warnings also clear. The exit code can still be 0 or 1
    depending on template-specific heuristic warnings, but no *errors* must
    remain.
    """
    strategy_dir = _scaffold(tmp_path, template)
    _clean_scaffolded_config(strategy_dir)
    report = run_checks(strategy_dir)
    assert not report.has_errors(), (
        f"scaffold should not error after placeholder cleanup: {[(f.code, f.message) for f in report.findings]}"
    )


def test_check_cli_clean_exit_code_on_ta_swap(tmp_path: Path) -> None:
    """CLI ``strat check`` on a cleaned TA_SWAP scaffold exits with no errors.

    We accept exit 0 or 1 — some template-aware advisories may still fire
    depending on default config shape. The contract is "no blocking errors".
    """
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.TA_SWAP, name="clean_ta")
    _clean_scaffolded_config(strategy_dir)

    runner = CliRunner()
    result = runner.invoke(check, ["--working-dir", str(strategy_dir)])
    assert result.exit_code in (0, 1), f"expected clean check, got exit {result.exit_code}:\n{result.output}"
    # Error section should not appear.
    assert "Errors (" not in result.output, f"unexpected error findings:\n{result.output}"


# ---------------------------------------------------------------------------
# Placeholder corruption -> exit 2 with file:line and field info
# ---------------------------------------------------------------------------


def test_check_flags_placeholder_address_in_source(tmp_path: Path) -> None:
    """A ``0x_SET_...`` address in strategy.py must be flagged as an error."""
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.BLANK, name="corrupted_src")

    # Inject a placeholder address into the strategy source.
    strategy_file = strategy_dir / "strategy.py"
    source = strategy_file.read_text()
    # Append a module-level constant so the line number is predictable at EOF.
    tainted_line = 'PLACEHOLDER_ADDR = "0x_SET_VAULT_ADDRESS"\n'
    strategy_file.write_text(source + tainted_line)

    report = run_checks(strategy_dir)
    placeholder_findings = [f for f in report.findings if f.code == "placeholder_address"]
    assert placeholder_findings, f"expected placeholder_address finding, got: {[f.code for f in report.findings]}"
    finding = placeholder_findings[0]
    assert finding.severity == Severity.ERROR
    assert finding.file == str(strategy_file)
    assert finding.line is not None, "placeholder finding must carry a line number"
    assert report.has_errors()


def test_check_cli_exit_2_on_placeholder(tmp_path: Path) -> None:
    """CLI exit code is 2 when a placeholder address is present."""
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.BLANK, name="corrupted_cli")
    strategy_file = strategy_dir / "strategy.py"
    strategy_file.write_text(strategy_file.read_text() + 'BAD_ADDR = "0x_SET_VAULT_ADDRESS"\n')

    runner = CliRunner()
    result = runner.invoke(check, ["--working-dir", str(strategy_dir)])
    assert result.exit_code == 2, f"expected exit 2, got {result.exit_code}:\n{result.output}"
    assert "placeholder_address" in result.output


def test_check_flags_placeholder_in_config_with_field_path(tmp_path: Path) -> None:
    """Placeholder address in config.json must carry the config field path."""
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.VAULT_YIELD, name="vault_corrupt")

    # The VAULT_YIELD scaffold already writes a zero vault_address; swap it
    # for an unambiguous 0x_SET_ placeholder so the test is clearly about
    # the scanner, not about whatever defaults the scaffold ships with.
    config_path = strategy_dir / "config.json"
    config = json.loads(config_path.read_text())
    config["vault_address"] = "0x_SET_VAULT_ADDRESS_HERE"
    config_path.write_text(json.dumps(config, indent=4) + "\n")

    report = run_checks(strategy_dir)
    config_findings = [f for f in report.findings if f.code == "placeholder_address" and f.field == "vault_address"]
    assert config_findings, (
        f"expected placeholder in config.vault_address, got: {[(f.code, f.field) for f in report.findings]}"
    )
    assert config_findings[0].file == str(config_path)


# ---------------------------------------------------------------------------
# Template heuristics
# ---------------------------------------------------------------------------


def test_check_warns_perps_missing_direction(tmp_path: Path) -> None:
    """Perps template without a ``direction`` config field -> warning.

    As of VIB-3174 the PERPS scaffold ships with ``direction`` pre-filled,
    so we strip it here to exercise the heuristic path. The test stays
    meaningful for hand-written perps strategies that omit the field.
    """
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.PERPS, name="perps_no_dir")

    # Remove the scaffold-provided direction so the heuristic can fire.
    config_path = strategy_dir / "config.json"
    config = json.loads(config_path.read_text())
    config.pop("direction", None)
    config_path.write_text(json.dumps(config, indent=4) + "\n")

    report = run_checks(strategy_dir)
    missing_dir = [f for f in report.findings if f.code == "template_perps_missing_direction"]
    assert missing_dir, f"expected perps missing-direction warning, got: {[f.code for f in report.findings]}"
    finding = missing_dir[0]
    assert finding.severity == Severity.WARNING
    assert finding.layer == Layer.TEMPLATE
    assert finding.field == "direction"


def test_check_perps_with_direction_has_no_template_warning(tmp_path: Path) -> None:
    """Adding ``direction`` to perps config silences the heuristic."""
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.PERPS, name="perps_with_dir")

    config_path = strategy_dir / "config.json"
    config = json.loads(config_path.read_text())
    config["direction"] = "long"
    config_path.write_text(json.dumps(config, indent=4) + "\n")

    report = run_checks(strategy_dir)
    missing_dir = [f for f in report.findings if f.code == "template_perps_missing_direction"]
    assert not missing_dir, "heuristic should not fire once direction is set"


def test_check_warns_lending_missing_health_factor(tmp_path: Path) -> None:
    """Lending-loop template sans min_health_factor -> warning.

    Note: the scaffold *does* include min_health_factor in its default
    config, so we strip it to exercise the heuristic path.
    """
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.LENDING_LOOP, name="lend_no_hf")
    config_path = strategy_dir / "config.json"
    config = json.loads(config_path.read_text())
    config.pop("min_health_factor", None)
    config_path.write_text(json.dumps(config, indent=4) + "\n")

    report = run_checks(strategy_dir)
    findings = [f for f in report.findings if f.code == "template_lending_missing_min_health_factor"]
    assert findings, f"expected lending heuristic warning, got: {[f.code for f in report.findings]}"


# ---------------------------------------------------------------------------
# AST-only behaviour: empty teardown, missing strategy.py, etc.
# ---------------------------------------------------------------------------


def test_check_missing_strategy_py_is_error(tmp_path: Path) -> None:
    """A directory without strategy.py should exit with an error."""
    bare_dir = tmp_path / "empty"
    bare_dir.mkdir()
    report = run_checks(bare_dir)
    assert report.has_errors()
    codes = [f.code for f in report.findings]
    assert "missing_strategy_py" in codes


def test_check_nonexistent_dir_is_error(tmp_path: Path) -> None:
    """Passing a path that does not exist surfaces an error."""
    report = run_checks(tmp_path / "does_not_exist")
    assert report.has_errors()
    codes = [f.code for f in report.findings]
    assert "dir_missing" in codes


def test_check_stateless_strategy_no_missing_teardown_warning(tmp_path: Path) -> None:
    """StatelessStrategy subclasses inherit a valid default teardown.

    A signal-only / alert-only strategy that extends ``StatelessStrategy``
    already gets a valid empty ``generate_teardown_intents`` from its base,
    so ``strat check`` must NOT warn about the missing override (and must
    not flip the exit code to warnings on an otherwise-clean strategy).
    """
    strategy_dir = tmp_path / "signal_strat"
    strategy_dir.mkdir()
    (strategy_dir / "strategy.py").write_text(
        '"""Signal-only stateless strategy."""\n'
        "from almanak.framework.strategies.stateless_strategy import StatelessStrategy\n\n"
        "class SignalStrategy(StatelessStrategy):\n"
        "    STRATEGY_NAME = 'signal_strat'\n"
        "    def decide(self, market):\n"
        "        from almanak.framework.intents import Intent\n"
        "        return Intent.hold(reason='signal only')\n",
        encoding="utf-8",
    )

    report = run_checks(strategy_dir)
    codes = [f.code for f in report.findings]
    assert "missing_teardown_intents" not in codes, (
        f"StatelessStrategy subclass should not trigger missing-teardown warning, got: {codes}"
    )
    assert "missing_get_open_positions" not in codes, (
        f"StatelessStrategy subclass should not trigger missing-get-positions warning, got: {codes}"
    )


def test_check_flags_empty_teardown(tmp_path: Path) -> None:
    """A strategy whose generate_teardown_intents() is trivial gets warned.

    BLANK template's teardown body is a literal ``return []`` (intentional —
    blank strategies have no positions to close). The AST scanner should
    surface a warning so operators know this scaffold needs attention.
    """
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.BLANK, name="blank_teardown")
    report = run_checks(strategy_dir)
    codes = [f.code for f in report.findings]
    assert "empty_teardown_intents" in codes, f"BLANK scaffold should warn about empty teardown, got: {codes}"


# ---------------------------------------------------------------------------
# JSON output contract
# ---------------------------------------------------------------------------


def test_check_json_output_is_valid(tmp_path: Path) -> None:
    """--json emits a parseable report object with the expected shape."""
    strategy_dir = _scaffold(tmp_path, StrategyTemplate.TA_SWAP, name="json_output")

    runner = CliRunner()
    result = runner.invoke(check, ["--working-dir", str(strategy_dir), "--json"])

    # Exit code can be 0 or 1 depending on warnings; we only require parseability.
    assert result.exit_code in (0, 1, 2), f"unexpected exit: {result.exit_code}\n{result.output}"
    data = json.loads(result.output)
    assert "findings" in data
    assert "summary" in data
    assert "strategy_dir" in data
    # Summary totals must match the findings list length.
    total = sum(data["summary"].values())
    assert total == len(data["findings"])


# ---------------------------------------------------------------------------
# Unit tests for internal helpers (fast, pure)
# ---------------------------------------------------------------------------


def test_finding_to_dict_flattens_enums() -> None:
    f = Finding(
        severity=Severity.ERROR,
        layer=Layer.AST,
        code="x",
        message="y",
        file="a.py",
        line=10,
        field="direction",
    )
    d = f.to_dict()
    assert d["severity"] == "error"
    assert d["layer"] == "ast"
    assert d["line"] == 10


def test_report_summary_counts_by_severity() -> None:
    r = CheckReport(strategy_dir=".")
    r.add(Finding(severity=Severity.ERROR, layer=Layer.LOAD, code="e", message="e"))
    r.add(Finding(severity=Severity.WARNING, layer=Layer.AST, code="w", message="w"))
    r.add(Finding(severity=Severity.WARNING, layer=Layer.TEMPLATE, code="w2", message="w2"))
    assert r.has_errors()
    assert r.has_warnings()
    d = r.to_dict()
    assert d["summary"] == {"errors": 1, "warnings": 2, "infos": 0}


def test_placeholder_detection_helpers() -> None:
    from almanak.framework.cli.check import _is_placeholder_value

    assert _is_placeholder_value("0x0000000000000000000000000000000000000000")
    assert _is_placeholder_value("0xDEADBEEF")
    assert _is_placeholder_value("0xdeadbeef")
    assert _is_placeholder_value("0x_SET_VAULT")
    assert _is_placeholder_value("REPLACE_ME")
    assert _is_placeholder_value("0x_SET_") == "0x_SET_"
    # Real addresses should NOT match.
    assert _is_placeholder_value("0xaf88d065e77c8cC2239327C5EDb3A432268e5831") is None
    assert _is_placeholder_value("") is None
    assert _is_placeholder_value("   ") is None


# ---------------------------------------------------------------------------
# Load + validate layer: ConfigValidationError + validate_config unavailable
# ---------------------------------------------------------------------------


def test_check_reports_config_validation_error(tmp_path: Path) -> None:
    """A strategy whose validate_config() raises ConfigValidationError must
    be surfaced as an error finding carrying the field + message.

    This is the happy-path for "invalid config" that PM / CI rely on to
    stop rollout; a regression here would silently downgrade the signal
    to an unhelpful ``instantiation_failed`` exception.
    """
    strategy_dir = tmp_path / "bad_config"
    strategy_dir.mkdir()
    (strategy_dir / "strategy.py").write_text(
        '"""Strategy that rejects its own config."""\n'
        "from almanak.framework.strategies.intent_strategy import IntentStrategy\n"
        "from almanak.framework.strategies.exceptions import ConfigValidationError\n\n"
        "class BadConfigStrategy(IntentStrategy):\n"
        "    STRATEGY_NAME = 'bad_config'\n"
        "    def validate_config(self) -> None:\n"
        "        raise ConfigValidationError('trade_size_usd must be > 0', field='trade_size_usd')\n"
        "    def decide(self, market):\n"
        "        from almanak.framework.intents import Intent\n"
        "        return Intent.hold(reason='never reached')\n"
        "    def get_open_positions(self):\n"
        "        from almanak.framework.teardown import TeardownPositionSummary\n"
        "        return TeardownPositionSummary.empty(self.deployment_id)\n"
        "    def generate_teardown_intents(self, mode=None, market=None):\n"
        "        return []\n",
        encoding="utf-8",
    )
    (strategy_dir / "config.json").write_text(json.dumps({"chain": "arbitrum", "trade_size_usd": 0}))

    report = run_checks(strategy_dir)
    findings = [f for f in report.findings if f.code == "config_validation_failed"]
    assert findings, f"expected config_validation_failed finding, got: {[f.code for f in report.findings]}"
    finding = findings[0]
    assert finding.severity == Severity.ERROR
    assert finding.field == "trade_size_usd"
    assert "trade_size_usd must be > 0" in finding.message
    assert report.has_errors()
    # Summary counts must reflect the error on the to_dict() contract.
    assert report.to_dict()["summary"]["errors"] >= 1


def test_check_handles_missing_validate_config_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the installed SDK lacks ``validate_config``, ``check`` must log
    an info-level ``validate_config_unavailable`` finding and keep going.

    We simulate the pre-T1 SDK by monkey-patching ``IntentStrategy.__init__``
    to raise ``AttributeError('validate_config')`` — mirroring the real
    rollout-window scenario that ``_instantiate_strategy`` explicitly
    accommodates.
    """
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    original_init = IntentStrategy.__init__

    def _fake_init(self: Any, *args: Any, **kwargs: Any) -> None:
        raise AttributeError(
            "'IntentStrategy' object has no attribute 'validate_config'"
        )

    monkeypatch.setattr(IntentStrategy, "__init__", _fake_init)

    try:
        strategy_dir = tmp_path / "no_hook"
        strategy_dir.mkdir()
        (strategy_dir / "strategy.py").write_text(
            '"""Strategy used to exercise the pre-T1 fallback path."""\n'
            "from almanak.framework.strategies.intent_strategy import IntentStrategy\n\n"
            "class LegacyStrategy(IntentStrategy):\n"
            "    STRATEGY_NAME = 'legacy'\n"
            "    def decide(self, market):\n"
            "        from almanak.framework.intents import Intent\n"
            "        return Intent.hold(reason='never reached')\n"
            "    def get_open_positions(self):\n"
            "        from almanak.framework.teardown import TeardownPositionSummary\n"
            "        return TeardownPositionSummary.empty(self.deployment_id or 'legacy')\n"
            "    def generate_teardown_intents(self, mode=None, market=None):\n"
            "        return []\n",
            encoding="utf-8",
        )

        report = run_checks(strategy_dir)
    finally:
        monkeypatch.setattr(IntentStrategy, "__init__", original_init)

    infos = [f for f in report.findings if f.code == "validate_config_unavailable"]
    assert infos, (
        f"expected validate_config_unavailable info, got: "
        f"{[(f.code, f.severity.value) for f in report.findings]}"
    )
    assert infos[0].severity == Severity.INFO
    # The scaffolded strategy is otherwise clean: the missing hook must not
    # flip the report into an error state.
    assert not report.has_errors(), (
        f"fallback path must not escalate to error: {[(f.code, f.message) for f in report.findings]}"
    )


# ---------------------------------------------------------------------------
# End-to-end CLI wiring: `almanak strat check` via the real top-level group
# ---------------------------------------------------------------------------


def test_cli_entrypoint_strat_check_wired(tmp_path: Path) -> None:
    """Invoke ``almanak strat check`` through the real top-level click group.

    Covers the wiring from ``almanak.cli.cli.strat`` -> ``check`` so a
    regression that drops the ``add_command`` call is caught. We reuse the
    same scaffold fixture the other tests use and only assert parseable
    output — the exit-code / finding semantics are already covered by the
    ``run_checks`` tests above.
    """
    from almanak.cli.cli import almanak as almanak_cli

    strategy_dir = _scaffold(tmp_path, StrategyTemplate.TA_SWAP, name="cli_wire")
    _clean_scaffolded_config(strategy_dir)

    runner = CliRunner()
    result = runner.invoke(
        almanak_cli,
        ["strat", "check", "--working-dir", str(strategy_dir), "--json"],
    )
    assert result.exit_code in (0, 1, 2), f"unexpected exit {result.exit_code}:\n{result.output}"
    # The JSON body may be preceded/followed by harmless framework log output
    # on some environments; locate the JSON object directly.
    start = result.output.find("{")
    end = result.output.rfind("}")
    assert start != -1 and end != -1, f"no JSON object in output:\n{result.output}"
    data = json.loads(result.output[start : end + 1])
    assert data["strategy_dir"] == str(strategy_dir)
    assert "findings" in data and "summary" in data
