"""Guard tests for ``ALMANAK_GATEWAY_NO_SPAWN`` (managed-environment flag).

In the AlmanakCode worker container a platform-provisioned data gateway is
expected to be listening on the default port. When it is not, ``ax``'s
silent auto-start fallback boots a keyless in-process gateway whose
data-lane failures ("provider unavailable", empty analytics fields) read
as platform capability gaps — the 2026-08-22 false "strategy not
deployable" verdict. The flag converts that fallback into a loud
environment-fault error at ``_start_managed_gateway``, the single choke
point every ax auto-start path funnels through.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import click
import pytest


class _FakeCtx:
    """Minimal stand-in for click.Context — exposes only ``obj``."""

    def __init__(self, data: dict) -> None:
        self.obj = data


@pytest.fixture
def base_ctx() -> dict:
    return {
        "gateway_host": "127.0.0.1",
        "gateway_port": 50051,
        "chain": "base",
        "wallet": "0xWALLET",
        "max_trade_usd": 1000.0,
        "network": None,
    }


def test_no_spawn_env_raises_environment_fault(base_ctx, monkeypatch) -> None:
    from almanak.framework.cli.ax import _start_managed_gateway

    monkeypatch.setenv("ALMANAK_GATEWAY_NO_SPAWN", "true")
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)

    with patch("almanak.gateway.managed.ManagedGateway") as fake_mg:
        with pytest.raises(click.ClickException) as excinfo:
            _start_managed_gateway(_FakeCtx(base_ctx), "127.0.0.1", 50051, "anvil")

    # The message must frame this as an environment fault, not data absence.
    assert "auto-start is disabled" in str(excinfo.value)
    assert "environment fault" in str(excinfo.value)
    fake_mg.assert_not_called()


def test_settings_field_reads_env(monkeypatch) -> None:
    from almanak.config.env import gateway_config_from_env

    monkeypatch.delenv("ALMANAK_GATEWAY_NO_SPAWN", raising=False)
    assert gateway_config_from_env().no_spawn is False

    monkeypatch.setenv("ALMANAK_GATEWAY_NO_SPAWN", "true")
    assert gateway_config_from_env().no_spawn is True


def test_default_behaviour_unchanged_without_flag(base_ctx, tmp_path, monkeypatch) -> None:
    """Negative control: without the flag, auto-start proceeds as before."""
    from almanak.framework.cli.ax import _start_managed_gateway

    monkeypatch.delenv("ALMANAK_GATEWAY_NO_SPAWN", raising=False)
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
    monkeypatch.chdir(tmp_path)  # clean, non-strategy cwd

    fake_managed = MagicMock()
    fake_managed.host = "127.0.0.1"
    fake_managed.port = 50051

    with (
        patch("almanak.gateway.managed.ManagedGateway", return_value=fake_managed),
        patch("almanak.gateway.managed.is_port_in_use", return_value=False),
        patch("almanak.framework.cli.ax._assert_signer_matches_intended_wallet"),
    ):
        managed = _start_managed_gateway(_FakeCtx(base_ctx), "127.0.0.1", 50051, "anvil")

    assert managed is fake_managed
    fake_managed.start.assert_called_once()
