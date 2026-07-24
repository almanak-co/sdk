"""Unit tests for `almanak.framework.cli.status:_make_client`.

Covers every branch of the connect-or-exit helper shared by all five
`strat` monitoring commands:

- invalid gateway configuration (`from_env` raising) -> exit 1
- "localhost" host normalized to 127.0.0.1; other hosts passed through
- happy path: connect + health_check -> returns the connected client
- health_check False -> disconnect + exit 1 with the "Start it with" hint
- connect raising -> best-effort disconnect + exit 1
- disconnect raising during the failure path is swallowed (still exit 1)

No gateway, no network — GatewayClient / GatewayClientConfig are patched at
the status-module seam.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.cli import status as status_mod
from almanak.framework.cli.status import _make_client
from almanak.framework.gateway_client import GatewayClientConfig


def _patch_from_env(config: GatewayClientConfig):
    return patch.object(status_mod.GatewayClientConfig, "from_env", return_value=config)


def _make_gateway_client_cls(client: MagicMock):
    """Return a GatewayClient replacement class that records its config."""
    cls = MagicMock(return_value=client)
    return cls


def test_invalid_config_exits_1(capsys: pytest.CaptureFixture) -> None:
    with patch.object(
        status_mod.GatewayClientConfig,
        "from_env",
        side_effect=ValueError("bad token"),
    ):
        with pytest.raises(SystemExit) as excinfo:
            _make_client("localhost", 50051)

    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    assert "Invalid gateway configuration: bad token" in err


def test_happy_path_returns_client_and_normalizes_localhost() -> None:
    config = GatewayClientConfig()
    client = MagicMock()
    client.health_check.return_value = True
    client_cls = _make_gateway_client_cls(client)

    with _patch_from_env(config), patch.object(status_mod, "GatewayClient", client_cls):
        result = _make_client("localhost", 50099)

    assert result is client
    # "localhost" is normalized to 127.0.0.1 before being applied to config.
    assert config.host == "127.0.0.1"
    assert config.port == 50099
    client_cls.assert_called_once_with(config)
    client.connect.assert_called_once_with()
    client.health_check.assert_called_once_with()
    client.disconnect.assert_not_called()


def test_non_localhost_host_passed_through() -> None:
    config = GatewayClientConfig()
    client = MagicMock()
    client.health_check.return_value = True

    with _patch_from_env(config), patch.object(status_mod, "GatewayClient", MagicMock(return_value=client)):
        result = _make_client("10.1.2.3", 50051)

    assert result is client
    assert config.host == "10.1.2.3"


def test_health_check_failure_disconnects_and_exits(capsys: pytest.CaptureFixture) -> None:
    config = GatewayClientConfig()
    client = MagicMock()
    client.health_check.return_value = False

    with _patch_from_env(config), patch.object(status_mod, "GatewayClient", MagicMock(return_value=client)):
        with pytest.raises(SystemExit) as excinfo:
            _make_client("localhost", 50051)

    assert excinfo.value.code == 1
    client.disconnect.assert_called_once_with()
    err = capsys.readouterr().err
    assert "Cannot connect to gateway at 127.0.0.1:50051" in err
    assert "almanak gateway" in err


def test_connect_error_disconnects_and_exits(capsys: pytest.CaptureFixture) -> None:
    config = GatewayClientConfig()
    client = MagicMock()
    client.connect.side_effect = RuntimeError("connection refused")

    with _patch_from_env(config), patch.object(status_mod, "GatewayClient", MagicMock(return_value=client)):
        with pytest.raises(SystemExit) as excinfo:
            _make_client("gw.internal", 50052)

    assert excinfo.value.code == 1
    client.disconnect.assert_called_once_with()
    err = capsys.readouterr().err
    assert "Cannot connect to gateway at gw.internal:50052" in err


def test_disconnect_error_during_failure_is_swallowed(capsys: pytest.CaptureFixture) -> None:
    """A disconnect() raising while cleaning up must not mask the exit-1 path."""
    config = GatewayClientConfig()
    client = MagicMock()
    client.connect.side_effect = RuntimeError("boom")
    client.disconnect.side_effect = RuntimeError("also boom")

    with _patch_from_env(config), patch.object(status_mod, "GatewayClient", MagicMock(return_value=client)):
        with pytest.raises(SystemExit) as excinfo:
            _make_client("localhost", 50051)

    assert excinfo.value.code == 1
    assert "Cannot connect to gateway at 127.0.0.1:50051" in capsys.readouterr().err
