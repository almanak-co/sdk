"""CLI logging for short-lived commands (`ax`, `strat permissions`).

Library warnings (registry collisions, symbol-resolution deprecations, managed
gateway start-up notes) must stay OFF stderr by default — coding agents read
stderr and pay for every line — but must still be recorded in the log file, and
errors must still reach stderr.
"""

from __future__ import annotations

import logging
import warnings

import pytest

from almanak.framework.cli._cli_logging import cli_stderr_level, configure_cli_logging


@pytest.fixture
def clean_root(monkeypatch, tmp_path):
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    monkeypatch.setenv("ALMANAK_CLI_LOG_FILE", str(tmp_path / "cli.log"))
    monkeypatch.delenv("ALMANAK_CLI_LOG_LEVEL", raising=False)
    yield tmp_path / "cli.log"
    for handler in [h for h in root.handlers if h not in saved_handlers]:
        root.removeHandler(handler)
        handler.close()
    root.setLevel(saved_level)
    logging.captureWarnings(False)


def test_warnings_go_to_the_file_not_stderr(clean_root, capsys):
    configure_cli_logging()
    logging.getLogger("almanak.framework.data.tokens.resolver").warning("token_registry_address_collision x")
    logging.getLogger("almanak.gateway.server").error("gateway exploded")

    err = capsys.readouterr().err
    assert "token_registry_address_collision" not in err
    assert "gateway exploded" in err
    for handler in logging.getLogger().handlers:
        handler.flush()
    text = clean_root.read_text()
    assert "token_registry_address_collision x" in text
    assert "gateway exploded" in text


def test_python_warnings_are_captured_into_logging(clean_root, capsys):
    # Enable capture INSIDE a fresh catch_warnings so neither pytest's own
    # warning recorder nor another test's capture state decides the outcome.
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        logging.captureWarnings(False)
        configure_cli_logging()
        try:
            warnings.warn("symbol-based token reference 'USDC'", UserWarning, stacklevel=1)
        finally:
            logging.captureWarnings(False)

    assert "symbol-based token reference" not in capsys.readouterr().err
    for handler in logging.getLogger().handlers:
        handler.flush()
    assert "symbol-based token reference" in clean_root.read_text()


def test_verbose_shows_warnings_on_stderr(clean_root, capsys):
    configure_cli_logging(verbose=True)
    logging.getLogger("almanak.x").warning("visible now")

    assert "visible now" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("value", "expected"),
    [("warning", logging.WARNING), ("DEBUG", logging.DEBUG), ("bogus", logging.ERROR), ("", logging.ERROR)],
)
def test_env_sets_the_stderr_threshold(monkeypatch, value, expected):
    monkeypatch.setenv("ALMANAK_CLI_LOG_LEVEL", value)
    assert cli_stderr_level() == expected
    assert cli_stderr_level(verbose=True) == logging.INFO


def test_empty_log_file_setting_disables_the_file(clean_root, monkeypatch, capsys):
    monkeypatch.setenv("ALMANAK_CLI_LOG_FILE", "")
    configure_cli_logging()
    logging.getLogger("almanak.x").warning("dropped quietly")

    assert "dropped quietly" not in capsys.readouterr().err
    assert not clean_root.exists()


def test_reconfiguring_replaces_only_its_own_handlers(clean_root):
    root = logging.getLogger()
    foreign = logging.NullHandler()
    root.addHandler(foreign)
    try:
        configure_cli_logging()
        configure_cli_logging()
        ours = [h for h in root.handlers if h.get_name() == "almanak-cli"]
        assert len(ours) == 2  # one stderr + one file handler, not duplicated
        assert foreign in root.handlers
    finally:
        root.removeHandler(foreign)


def test_permissions_applies_log_settings_from_the_working_dir_env(tmp_path, monkeypatch, clean_root):
    """`strat permissions --working-dir` loads that directory's .env first; its
    ALMANAK_CLI_LOG_* values must reach the logging setup."""
    from click.testing import CliRunner

    from almanak.framework.cli.permissions import permissions

    target = tmp_path / "from-dotenv.log"
    work = tmp_path / "strategy"
    work.mkdir()
    (work / ".env").write_text(f"ALMANAK_CLI_LOG_FILE={target}\n")
    monkeypatch.delenv("ALMANAK_CLI_LOG_FILE", raising=False)
    try:
        CliRunner().invoke(permissions, ["--working-dir", str(work)])  # fails later: no strategy there
        files = [
            h.baseFilename
            for h in logging.getLogger().handlers
            if h.get_name() == "almanak-cli" and hasattr(h, "baseFilename")
        ]
        assert files == [str(target)]
    finally:
        import os

        os.environ.pop("ALMANAK_CLI_LOG_FILE", None)
