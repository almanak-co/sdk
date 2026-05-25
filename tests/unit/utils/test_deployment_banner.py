"""Unit tests for the deployment-start banner.

The banner is the user-visible boundary between consecutive deployments in
the platform UI log viewer (and in raw ``kubectl logs``). Three things must
hold for the boundary to render correctly:

1. The ``ALMANAK_DEPLOYMENT_BANNER`` sentinel line must be parseable by the
   frontend's space-delimited ``(\\w+)=(\\S+)`` regex even when a value
   contains whitespace (e.g. ``"Momentum Strategy"``) — values get
   whitespace-collapsed.
2. The gateway banner is hosted-only — local-dev gateway boots must not
   emit it.
3. The CLI banner accepts a caller-supplied strategy name override (so the
   ``almanak strat run`` entrypoint can pass the working-dir basename
   before the strategy class is loaded).
"""

from __future__ import annotations

import logging
import re

import click
import click.testing
import pytest

from almanak.framework.utils.deployment_banner import (
    _sanitize_sentinel_value,
    emit_cli_banner,
    emit_gateway_banner,
)

_SENTINEL_KV = re.compile(r"(\w+)=(\S+)")


def _clear_env(monkeypatch):
    for var in (
        "ALMANAK_IS_HOSTED",
        "ALMANAK_DEPLOYMENT_ID",
        "ALMANAK_COMMIT_SHA",
        "ALMANAK_SDK_VERSION",
        "ALMANAK_STRATEGY_NAME",
        "ALMANAK_STRATEGY_VERSION",
    ):
        monkeypatch.delenv(var, raising=False)


def test_sanitize_collapses_inner_whitespace():
    assert _sanitize_sentinel_value("Momentum Strategy") == "Momentum_Strategy"
    assert _sanitize_sentinel_value("  leading and trailing  ") == "leading_and_trailing"


def test_sanitize_passes_through_safe_values():
    assert _sanitize_sentinel_value("dynamic_lp_vol_rebalance") == "dynamic_lp_vol_rebalance"
    assert _sanitize_sentinel_value("") == ""
    assert _sanitize_sentinel_value("v1.0.0-rc2") == "v1.0.0-rc2"


def test_gateway_banner_skipped_in_local_mode(monkeypatch, caplog):
    """Without ALMANAK_IS_HOSTED, the gateway banner is suppressed entirely.

    Local-dev gateway boots have no platform deployment id; the
    strategy-side banner fires instead and a gateway banner here would
    just be noise.
    """
    _clear_env(monkeypatch)
    logger = logging.getLogger("test_banner_gateway_local")
    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_gateway_banner(logger)
    assert "NEW DEPLOYMENT STARTED" not in caplog.text


def test_gateway_banner_emits_and_is_sentinel_parseable(monkeypatch, caplog):
    """Hosted mode: banner emits, and the sentinel survives a strategy name
    with whitespace by getting collapsed via underscores.
    """
    _clear_env(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "deploy-123")
    monkeypatch.setenv("ALMANAK_STRATEGY_NAME", "Momentum Strategy")  # has space
    monkeypatch.setenv("ALMANAK_STRATEGY_VERSION", "1.0.0")
    monkeypatch.setenv("ALMANAK_COMMIT_SHA", "abc1234")
    monkeypatch.setenv("ALMANAK_SDK_VERSION", "2.16.0")

    logger = logging.getLogger("test_banner_gateway_hosted")
    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_gateway_banner(logger)

    sentinel_line = next(line for line in caplog.text.splitlines() if "ALMANAK_DEPLOYMENT_BANNER" in line)
    fields = dict(_SENTINEL_KV.findall(sentinel_line))
    assert fields["deployment_id"] == "deploy-123"
    assert fields["strategy"] == "Momentum_Strategy"  # whitespace collapsed
    assert fields["strategy_version"] == "1.0.0"
    assert fields["commit_sha"] == "abc1234"
    assert fields["sdk_version"] == "2.16.0"


def test_cli_banner_uses_caller_override(monkeypatch, capsys):
    """The CLI banner uses caller-supplied strategy name + version when given.

    The ``almanak strat run`` entrypoint fires this before the strategy
    class is loaded; it derives a name from ``working_dir`` and passes it
    in. Env-injected values should be the fallback, not the override.
    """
    _clear_env(monkeypatch)

    runner = click.testing.CliRunner(mix_stderr=False)

    @click.command()
    def cmd():
        emit_cli_banner(strategy_name="my_local_strategy")

    result = runner.invoke(cmd)
    assert result.exit_code == 0
    sentinel_line = next(line for line in result.stdout.splitlines() if "ALMANAK_DEPLOYMENT_BANNER" in line)
    fields = dict(_SENTINEL_KV.findall(sentinel_line))
    assert fields["deployment_id"] == "local"
    assert fields["strategy"] == "my_local_strategy"
    assert fields["strategy_version"] == "unknown"


def test_cli_banner_prefers_env_when_no_override(monkeypatch):
    """When the caller does not override, env-injected values fill the banner."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "deploy-456")
    monkeypatch.setenv("ALMANAK_STRATEGY_NAME", "deployer_injected_name")
    monkeypatch.setenv("ALMANAK_STRATEGY_VERSION", "2.0.0")
    monkeypatch.setenv("ALMANAK_COMMIT_SHA", "deadbeef")
    monkeypatch.setenv("ALMANAK_SDK_VERSION", "2.16.1rc5")

    runner = click.testing.CliRunner(mix_stderr=False)

    @click.command()
    def cmd():
        emit_cli_banner()

    result = runner.invoke(cmd)
    assert result.exit_code == 0
    sentinel_line = next(line for line in result.stdout.splitlines() if "ALMANAK_DEPLOYMENT_BANNER" in line)
    fields = dict(_SENTINEL_KV.findall(sentinel_line))
    assert fields["deployment_id"] == "deploy-456"
    assert fields["strategy"] == "deployer_injected_name"
    assert fields["strategy_version"] == "2.0.0"
    assert fields["commit_sha"] == "deadbeef"
    assert fields["sdk_version"] == "2.16.1rc5"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
