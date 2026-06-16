"""VIB-5163 / GH #2099 — `strat status/list/logs/pause/resume` honor canonical gateway env vars.

The five `status.py` commands previously bound `--gateway-host` / `--gateway-port`
to the legacy unprefixed `GATEWAY_HOST` / `GATEWAY_PORT` only (default ``localhost``),
so a user who exported the canonical ``ALMANAK_GATEWAY_HOST`` — the name the gateway
itself instructs them to set — had it silently ignored. These tests pin that all
five commands now use the single canonical ``gateway_client_options`` decorator:
``ALMANAK_GATEWAY_*`` first, then the legacy name, default ``127.0.0.1``.
"""

from __future__ import annotations

import pytest

from almanak.framework.cli.status import (
    list_strategies,
    strategy_logs,
    strategy_pause,
    strategy_resume,
    strategy_status,
)

_STATUS_COMMANDS = [list_strategies, strategy_status, strategy_logs, strategy_pause, strategy_resume]


def _param(command, name: str):
    for p in command.params:
        if getattr(p, "name", None) == name:
            return p
    raise AssertionError(f"{command.name!r} has no {name!r} option")


@pytest.mark.parametrize("command", _STATUS_COMMANDS, ids=lambda c: c.name)
def test_gateway_host_prefers_canonical_env(command) -> None:
    """--gateway-host reads ALMANAK_GATEWAY_HOST before the legacy GATEWAY_HOST."""
    host = _param(command, "gateway_host")
    assert host.envvar == ["ALMANAK_GATEWAY_HOST", "GATEWAY_HOST"]
    # Canonical default avoids the IPv6-loopback miss that ``localhost`` could cause.
    assert host.default == "127.0.0.1"


@pytest.mark.parametrize("command", _STATUS_COMMANDS, ids=lambda c: c.name)
def test_gateway_port_prefers_canonical_env(command) -> None:
    """--gateway-port reads ALMANAK_GATEWAY_PORT before the legacy GATEWAY_PORT."""
    port = _param(command, "gateway_port")
    assert port.envvar == ["ALMANAK_GATEWAY_PORT", "GATEWAY_PORT"]
    assert port.default == 50051


def test_no_local_gateway_option_helper_remains() -> None:
    """The status-local _gateway_options / _add_gateway_options helper is gone (converged on the canonical one)."""
    from almanak.framework.cli import status

    assert not hasattr(status, "_add_gateway_options")
    assert not hasattr(status, "_gateway_options")
