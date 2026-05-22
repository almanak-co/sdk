from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.teardown import (
    SQLiteTeardownStateAdapter,
    SQLiteTeardownStateManager,
    create_teardown_state_adapter_for_runtime,
    get_teardown_state_manager_for_runtime,
    reset_teardown_state_manager,
)
from almanak.framework.teardown.gateway_client import GatewayTeardownStateAdapter, GatewayTeardownStateManager


@pytest.fixture(autouse=True)
def _reset_manager() -> None:
    reset_teardown_state_manager()
    yield
    reset_teardown_state_manager()


def _fake_gateway_client(*, is_connected: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        teardown=object(),
        config=SimpleNamespace(timeout=1.0),
        is_connected=is_connected,
    )


def test_hosted_runtime_manager_uses_gateway_without_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-1")
    monkeypatch.delenv("ALMANAK_GATEWAY_DATABASE_URL", raising=False)

    manager = get_teardown_state_manager_for_runtime(gateway_client=_fake_gateway_client())

    assert isinstance(manager, GatewayTeardownStateManager)


def test_hosted_runtime_adapter_uses_gateway_without_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-1")
    monkeypatch.delenv("ALMANAK_GATEWAY_DATABASE_URL", raising=False)

    adapter = create_teardown_state_adapter_for_runtime(gateway_client=_fake_gateway_client())

    assert isinstance(adapter, GatewayTeardownStateAdapter)


def test_hosted_runtime_requires_gateway_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-1")

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        get_teardown_state_manager_for_runtime(gateway_client=None)

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        create_teardown_state_adapter_for_runtime(gateway_client=None)


def test_hosted_runtime_requires_connected_gateway_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-1")
    disconnected = _fake_gateway_client(is_connected=False)

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        get_teardown_state_manager_for_runtime(gateway_client=disconnected)

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        create_teardown_state_adapter_for_runtime(gateway_client=disconnected)


def test_local_runtime_manager_keeps_sqlite_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)

    manager = get_teardown_state_manager_for_runtime(
        gateway_client=_fake_gateway_client(),
        db_path=tmp_path / "state.db",
    )

    assert isinstance(manager, SQLiteTeardownStateManager)
    assert manager.db_path == tmp_path / "state.db"


def test_local_runtime_adapter_keeps_sqlite_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)

    adapter = create_teardown_state_adapter_for_runtime(
        gateway_client=_fake_gateway_client(),
        sqlite_path=tmp_path / "state.db",
    )

    assert isinstance(adapter, SQLiteTeardownStateAdapter)
    assert adapter.db_path == tmp_path / "state.db"
