"""Session-token wiring in ``_start_managed_gateway_and_connect`` (VIB-4047).

The file helpers (`write_gateway_session_token` / `clear_gateway_session_token`)
are covered in ``tests/unit/local_paths/test_gateway_session_token.py``; this
suite covers the *wiring* — that the managed-gateway bootstrap persists a truthy
session token to the 0600 file and registers its clearer with ``atexit``, and
that it no-ops when no token is present (CodeRabbit).
"""

from __future__ import annotations

import atexit

import pytest

from almanak.framework.cli import _run_gateway


class _FakeManaged:
    def __init__(self, *_a, **_k) -> None:
        pass

    def start(self, *, timeout: float | None = None) -> None:  # noqa: ARG002
        pass

    def stop(self) -> None:
        pass


class _FakeClient:
    def __init__(self, config) -> None:
        self.config = config

    def connect(self) -> None:
        pass

    def wait_for_ready(self, *, timeout: float = 0.0, interval: float = 0.0) -> bool:  # noqa: ARG002
        return True

    def disconnect(self) -> None:
        pass


@pytest.fixture
def wired(monkeypatch):
    """Patch the heavy bootstrap deps; return recorders for the token wiring."""
    writes: list[str] = []
    registered: list = []

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", _FakeManaged)
    monkeypatch.setattr("almanak.framework.gateway_client.GatewayClient", _FakeClient)
    monkeypatch.setattr(
        "almanak.framework.local_paths.write_gateway_session_token",
        lambda token: writes.append(token) or None,
    )

    def _clear() -> None:  # sentinel identity checked below
        pass

    monkeypatch.setattr("almanak.framework.local_paths.clear_gateway_session_token", _clear)
    monkeypatch.setattr(atexit, "register", lambda fn, *a, **k: registered.append(fn))
    return writes, registered, _clear


def _call(token: str | None):
    return _run_gateway._start_managed_gateway_and_connect(
        gateway_settings=object(),
        anvil_chains=[],
        isolated_wallet_address=None,
        anvil_funding={},
        external_anvil_ports={},
        keep_anvil=False,
        effective_host="127.0.0.1",
        gateway_port=50051,
        gateway_network="anvil",
        session_auth_token=token,
    )


def test_managed_gateway_persists_and_clears_session_token(wired) -> None:
    writes, registered, clear_fn = wired
    client, managed = _call("tok-abc123")
    assert client is not None and managed is not None
    # Token persisted to the 0600 session file...
    assert writes == ["tok-abc123"]
    # ...and its clearer registered for shutdown.
    assert clear_fn in registered


def test_no_token_skips_session_file(wired) -> None:
    writes, registered, clear_fn = wired
    _call(None)
    assert writes == []
    assert clear_fn not in registered
