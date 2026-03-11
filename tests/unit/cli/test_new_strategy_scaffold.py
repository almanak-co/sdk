from almanak._version import __version__
from almanak.framework.cli.new_strategy import generate_pyproject_toml


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
