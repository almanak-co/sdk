"""Branch-complete tests for the ``almanak ax`` managed gateway startup."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest

from almanak.framework.cli.ax import _start_managed_gateway


class _FakeCtx:
    def __init__(self, obj: dict) -> None:
        self.obj = obj


def _config(*, private_key: str = "") -> SimpleNamespace:
    return SimpleNamespace(gateway=SimpleNamespace(no_spawn=False, private_key=private_key))


def test_start_managed_gateway_preserves_port_auth_env_and_process_contract(capsys) -> None:
    ctx = _FakeCtx({"chain": "base", "wallet": "0xoperator"})
    settings = object()
    managed = MagicMock(host="127.0.0.1", port=50123)

    with (
        patch("almanak.config.load_config", return_value=_config(private_key="gateway-key")),
        patch("almanak.config.env.gateway_config_from_env", return_value=settings) as build_settings,
        patch("almanak.framework.local_paths.auto_detect_strategy_folder") as detect_folder,
        patch("almanak.config.runtime.private_key_from_env", return_value="operator-key"),
        patch("almanak.gateway.managed.is_port_in_use", return_value=True),
        patch("almanak.gateway.managed.find_available_gateway_port", return_value=50123) as find_port,
        patch("almanak.gateway.managed.ManagedGateway", return_value=managed) as managed_gateway,
        patch("almanak.framework.cli.ax._assert_signer_matches_intended_wallet") as assert_signer,
        patch("uuid.uuid4", return_value=SimpleNamespace(hex="session-token")),
        patch("atexit.register") as register_exit,
    ):
        result = _start_managed_gateway(ctx, "127.0.0.1", 50051, None)

    assert result is managed
    find_port.assert_called_once_with("127.0.0.1", 50052)
    detect_folder.assert_called_once_with(export_env=True)
    build_settings.assert_called_once_with(
        grpc_host="127.0.0.1",
        grpc_port=50123,
        network="mainnet",
        allow_insecure=False,
        chains=["base"],
        metrics_enabled=False,
        audit_enabled=False,
        standalone=True,
        auth_token="session-token",
        private_key="operator-key",
    )
    assert_signer.assert_called_once_with("operator-key", "0xoperator")
    managed_gateway.assert_called_once_with(settings=settings, anvil_chains=[], wallet_address="0xoperator")
    managed.start.assert_called_once_with(timeout=30.0)
    register_exit.assert_called_once_with(managed.stop)
    assert ctx.obj == {
        "chain": "base",
        "wallet": "0xoperator",
        "gateway_host": "127.0.0.1",
        "gateway_port": 50123,
        "gateway_auth_token": "session-token",
    }
    assert click.unstyle(capsys.readouterr().err) == (
        "Auto-starting gateway (mainnet) on 127.0.0.1:50123...\nGateway ready.\n"
    )


@pytest.mark.parametrize(
    ("network", "expected_suffix", "expected_auth_token", "expected_anvil_chains"),
    [
        (None, "", "session-token", []),
        ("anvil", " --network anvil", None, ["base"]),
    ],
)
def test_start_managed_gateway_preserves_exact_start_failure(
    network: str | None,
    expected_suffix: str,
    expected_auth_token: str | None,
    expected_anvil_chains: list[str],
) -> None:
    ctx = _FakeCtx({"chain": "base", "wallet": ""})
    managed = MagicMock()
    managed.start.side_effect = RuntimeError("boot failed")

    with (
        patch("almanak.config.load_config", return_value=_config()),
        patch("almanak.config.env.gateway_config_from_env", return_value=object()) as build_settings,
        patch("almanak.framework.local_paths.auto_detect_strategy_folder"),
        patch("almanak.config.runtime.private_key_from_env", return_value=""),
        patch("almanak.gateway.managed.is_port_in_use", return_value=False),
        patch("almanak.gateway.managed.ManagedGateway", return_value=managed) as managed_gateway,
        patch("almanak.framework.cli.ax._assert_signer_matches_intended_wallet"),
        patch("uuid.uuid4", return_value=SimpleNamespace(hex="session-token")),
        patch("atexit.register") as register_exit,
    ):
        with pytest.raises(click.ClickException) as exc_info:
            _start_managed_gateway(ctx, "localhost", 50051, network)

    expected_message = (
        f"Failed to auto-start gateway: boot failed\nStart one manually: almanak gateway{expected_suffix}"
    )
    assert str(exc_info.value) == expected_message
    assert build_settings.call_args.kwargs.get("auth_token") == expected_auth_token
    managed_gateway.assert_called_once_with(
        settings=build_settings.return_value,
        anvil_chains=expected_anvil_chains,
        wallet_address=None,
    )
    register_exit.assert_not_called()
