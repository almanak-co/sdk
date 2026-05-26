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


def test_cli_banner_falls_back_to_caller_hint_in_local_mode(monkeypatch):
    """In local mode (no env), the CLI banner uses the caller's hint.

    The ``almanak strat run`` entrypoint passes the ``working_dir``
    basename so local users still get something more informative than
    ``unknown``.
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


def test_cli_banner_env_wins_over_caller_hint_in_hosted_mode(monkeypatch):
    """In hosted V2, the deployer-injected ALMANAK_STRATEGY_NAME wins.

    Regression test for v2.16.1-rc6 stage feedback: the CLI was passing
    ``Path(working_dir).resolve().name`` which is ``"src"`` for the
    hosted ``/app/src`` working dir. The deployer-injected env var is
    the authoritative strategy name in hosted mode and must beat the
    working-dir hint.
    """
    _clear_env(monkeypatch)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "deploy-456")
    monkeypatch.setenv("ALMANAK_STRATEGY_NAME", "dynamic_lp_vol_rebalance")
    monkeypatch.setenv("ALMANAK_STRATEGY_VERSION", "1.0.0")
    monkeypatch.setenv("ALMANAK_COMMIT_SHA", "deadbeef")
    monkeypatch.setenv("ALMANAK_SDK_VERSION", "2.16.1rc6")

    runner = click.testing.CliRunner(mix_stderr=False)

    @click.command()
    def cmd():
        # Simulate what cli.py does: pass the working-dir basename as a
        # local-mode hint. Env-injected values must still win.
        emit_cli_banner(strategy_name="src")

    result = runner.invoke(cmd)
    assert result.exit_code == 0
    sentinel_line = next(line for line in result.stdout.splitlines() if "ALMANAK_DEPLOYMENT_BANNER" in line)
    fields = dict(_SENTINEL_KV.findall(sentinel_line))
    assert fields["deployment_id"] == "deploy-456"
    assert fields["strategy"] == "dynamic_lp_vol_rebalance"  # env wins, not "src"
    assert fields["strategy_version"] == "1.0.0"
    assert fields["commit_sha"] == "deadbeef"
    assert fields["sdk_version"] == "2.16.1rc6"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
