"""Pytest configuration for gateway tests."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "docker: marks tests as requiring Docker (deselect with '-m \"not docker\"')",
    )


@pytest.fixture(autouse=True)
def _isolate_gateway_db_path(tmp_path, monkeypatch):
    """Per-test gateway DB path so the VIB-3761 flock never collides under
    pytest-xdist (or even sequentially when two GatewayServer fixtures run in
    the same worker). Without this isolation, every test in this directory
    races for the same default ``~/.local/share/almanak/utility/almanak_state.db``
    flock and a parallel worker hits ``BlockingIOError``.
    """
    db_path = tmp_path / "almanak_state.db"
    monkeypatch.setenv("ALMANAK_GATEWAY_DB_PATH", str(db_path))
    monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
    yield
