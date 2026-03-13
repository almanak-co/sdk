import ast
import subprocess
import tempfile
from pathlib import Path

import pytest

from almanak._version import __version__
from almanak.framework.cli.new_strategy import (
    StrategyTemplate,
    SupportedChain,
    generate_config_json,
    generate_pyproject_toml,
    generate_strategy_file,
)

ALL_TEMPLATES = list(StrategyTemplate)


# ---------------------------------------------------------------------------
# pyproject.toml tests (existing)
# ---------------------------------------------------------------------------


def test_generate_pyproject_toml_uses_installed_version() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert 'name = "mean_reversion"' in pyproject
    assert f"almanak>={__version__}" in pyproject
    assert "interval = 60" in pyproject


def test_generate_pyproject_toml_has_no_build_system() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert "[build-system]" not in pyproject


def test_generate_pyproject_toml_has_no_framework_or_version_fields() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert 'framework = "v2"' not in pyproject
    assert f'version = "{__version__}"' not in pyproject
    # Only version that should appear is under [project] and in the dep spec
    assert 'version = "0.1.0"' in pyproject


def test_generate_pyproject_toml_has_run_section() -> None:
    pyproject = generate_pyproject_toml("Mean Reversion")

    assert "[tool.almanak.run]" in pyproject
    assert "interval = 60" in pyproject


# ---------------------------------------------------------------------------
# Smoke tests: every template generates valid, parseable, lintable Python
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_is_valid_python(template: StrategyTemplate) -> None:
    """Generated strategy.py must parse without syntax errors."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        # ast.parse will raise SyntaxError if code is invalid
        ast.parse(code)


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_passes_ruff(template: StrategyTemplate) -> None:
    """Generated strategy.py must pass ruff check (no lint errors)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        strategy_path = Path(tmpdir) / "strategy.py"
        strategy_path.write_text(code)

        result = subprocess.run(
            ["uv", "run", "ruff", "check", str(strategy_path), "--select", "E,W,F,I"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"ruff check failed for {template.value}:\n{result.stdout}\n{result.stderr}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_has_decide_method(template: StrategyTemplate) -> None:
    """Generated strategy must define a decide() method."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        assert len(class_defs) >= 1, "No class definition found"

        strategy_class = class_defs[0]
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        assert "decide" in method_names, f"decide() not found in {strategy_class.name}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_strategy_file_has_teardown_methods(template: StrategyTemplate) -> None:
    """Generated strategy must define teardown stubs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        strategy_class = class_defs[0]
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        for required in ("supports_teardown", "get_open_positions", "generate_teardown_intents"):
            assert required in method_names, f"{required}() missing from {strategy_class.name}"


@pytest.mark.parametrize("template", ALL_TEMPLATES, ids=lambda t: t.value)
def test_config_json_is_valid(template: StrategyTemplate) -> None:
    """Generated config.json must be valid JSON (no parse errors)."""
    import json

    config_str = generate_config_json(
        name="Smoke Test",
        template=template,
        chain=SupportedChain.ARBITRUM,
    )
    config = json.loads(config_str)
    assert isinstance(config, dict)


# ---------------------------------------------------------------------------
# Stateful templates must have on_intent_executed callback
# ---------------------------------------------------------------------------

STATEFUL_TEMPLATES = [
    StrategyTemplate.DYNAMIC_LP,
    StrategyTemplate.LENDING_LOOP,
    StrategyTemplate.BASIS_TRADE,
    StrategyTemplate.VAULT_YIELD,
    StrategyTemplate.PERPS,
    StrategyTemplate.MULTI_STEP,
    StrategyTemplate.STAKING,
]


@pytest.mark.parametrize("template", STATEFUL_TEMPLATES, ids=lambda t: t.value)
def test_stateful_templates_have_callbacks(template: StrategyTemplate) -> None:
    """Stateful templates must define on_intent_executed for state tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = generate_strategy_file(
            name="Smoke Test",
            template=template,
            chain=SupportedChain.ARBITRUM,
            output_dir=Path(tmpdir),
        )
        tree = ast.parse(code)
        class_defs = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        strategy_class = class_defs[0]
        method_names = [
            n.name for n in ast.walk(strategy_class) if isinstance(n, ast.FunctionDef)
        ]
        assert "on_intent_executed" in method_names, (
            f"on_intent_executed() missing from {template.value} template"
        )
