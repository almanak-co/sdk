"""Unit tests for ``almanak.config.cli_options`` (Phase 2, #2099).

Covers two surfaces:

1. :func:`warn_legacy_gateway_envvars` — emits a ``UserWarning``
   for each legacy unprefixed ``GATEWAY_*`` env var that is set without
   its canonical ``ALMANAK_GATEWAY_*`` equivalent. ``UserWarning`` is
   used (not ``DeprecationWarning``) so the notice survives Python's
   default warning filter for imported modules.
2. :func:`gateway_client_options` — Click decorator that exposes
   ``--gateway-host`` / ``--gateway-port`` with the canonical
   ``ALMANAK_GATEWAY_*`` envvar taking precedence over the legacy
   unprefixed name; an explicit CLI argument always wins.
"""

from __future__ import annotations

import warnings

import click
import pytest
from click.testing import CliRunner

from almanak.config.cli_options import (
    gateway_client_options,
    warn_legacy_gateway_envvars,
)

# Legacy / canonical pairs the warning iterates over. Mirrors the
# private constant in ``cli_options.py`` — kept in sync by both this
# parameterized test and the four-pair sweep below.
_LEGACY_PAIRS = (
    ("GATEWAY_HOST", "ALMANAK_GATEWAY_HOST"),
    ("GATEWAY_PORT", "ALMANAK_GATEWAY_PORT"),
    ("GATEWAY_AUTH_TOKEN", "ALMANAK_GATEWAY_AUTH_TOKEN"),
    ("GATEWAY_TIMEOUT", "ALMANAK_GATEWAY_TIMEOUT"),
)


@pytest.fixture
def cli_env_scrub(
    gateway_env_scrub: pytest.MonkeyPatch,
) -> pytest.MonkeyPatch:
    """Extend the shared scrub with the legacy unprefixed gateway names.

    The shared ``gateway_env_scrub`` fixture covers the canonical
    ``ALMANAK_GATEWAY_*`` envvars. Phase 2 adds the legacy bare-name
    siblings — scrub both so test outcomes are deterministic regardless
    of the developer's ``.env``.
    """
    for legacy, _ in _LEGACY_PAIRS:
        gateway_env_scrub.delenv(legacy, raising=False)
    return gateway_env_scrub


def test_warn_legacy_emits_for_unprefixed_only(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Setting only the legacy name fires the deprecation warning."""
    cli_env_scrub.setenv("GATEWAY_HOST", "legacy-host")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_legacy_gateway_envvars()
    matching = [
        w for w in caught if issubclass(w.category, UserWarning)
    ]
    assert len(matching) == 1, [str(w.message) for w in caught]
    msg = str(matching[0].message)
    assert "GATEWAY_HOST" in msg
    assert "ALMANAK_GATEWAY_HOST" in msg
    assert "deprecated" in msg


def test_warn_legacy_silent_when_canonical_set(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """When the canonical name is set the legacy warning is suppressed."""
    cli_env_scrub.setenv("ALMANAK_GATEWAY_HOST", "canonical")
    cli_env_scrub.setenv("GATEWAY_HOST", "legacy")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_legacy_gateway_envvars()
    matching = [
        w for w in caught if issubclass(w.category, UserWarning)
    ]
    assert matching == []


def test_warn_legacy_silent_when_neither_set(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Neither name set — warning never fires."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_legacy_gateway_envvars()
    matching = [
        w for w in caught if issubclass(w.category, UserWarning)
    ]
    assert matching == []


@pytest.mark.parametrize(("legacy", "canonical"), _LEGACY_PAIRS)
def test_warn_legacy_covers_all_four_pairs(
    legacy: str,
    canonical: str,
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Each of the four (HOST/PORT/AUTH_TOKEN/TIMEOUT) pairs warns independently."""
    cli_env_scrub.setenv(legacy, "x")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_legacy_gateway_envvars()
    matching = [
        w for w in caught if issubclass(w.category, UserWarning)
    ]
    assert len(matching) == 1
    msg = str(matching[0].message)
    assert legacy in msg
    assert canonical in msg


# Stub Click command — re-used across the decorator tests below.
@click.command()
@gateway_client_options
def _stub(gateway_host: str, gateway_port: int) -> None:
    click.echo(f"{gateway_host}:{gateway_port}")


def test_gateway_client_options_default_values(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """No env, no args — defaults are 127.0.0.1:50051 (matches GatewaySettings.host)."""
    runner = CliRunner()
    result = runner.invoke(_stub, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.strip() == "127.0.0.1:50051"


def test_gateway_client_options_canonical_envvar_wins(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Canonical ALMANAK_GATEWAY_* takes precedence over legacy GATEWAY_*."""
    cli_env_scrub.setenv("ALMANAK_GATEWAY_HOST", "foo")
    cli_env_scrub.setenv("GATEWAY_HOST", "bar")
    cli_env_scrub.setenv("ALMANAK_GATEWAY_PORT", "60051")
    cli_env_scrub.setenv("GATEWAY_PORT", "70051")
    runner = CliRunner()
    result = runner.invoke(_stub, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.strip() == "foo:60051"


def test_gateway_client_options_legacy_envvar_used_when_canonical_unset(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Legacy GATEWAY_* still works when canonical is unset."""
    cli_env_scrub.setenv("GATEWAY_HOST", "bar")
    cli_env_scrub.setenv("GATEWAY_PORT", "60052")
    runner = CliRunner()
    result = runner.invoke(_stub, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.strip() == "bar:60052"


def test_gateway_client_options_cli_arg_beats_env(
    cli_env_scrub: pytest.MonkeyPatch,
) -> None:
    """Explicit ``--gateway-host`` / ``--gateway-port`` override env."""
    cli_env_scrub.setenv("ALMANAK_GATEWAY_HOST", "from-env")
    cli_env_scrub.setenv("ALMANAK_GATEWAY_PORT", "11111")
    runner = CliRunner()
    result = runner.invoke(
        _stub,
        ["--gateway-host", "from-arg", "--gateway-port", "22222"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output.strip() == "from-arg:22222"
