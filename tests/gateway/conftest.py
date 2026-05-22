"""Pytest configuration for gateway tests."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "docker: marks tests as requiring Docker (deselect with '-m \"not docker\"')",
    )


@pytest.fixture(autouse=True)
def _isolate_gateway_env(tmp_path, monkeypatch):
    """Isolate every gateway test from ambient deployment configuration.

    Two independent isolations, both autouse so no gateway test can forget:

    1. Per-test gateway DB path — the VIB-3761 flock would otherwise collide
       under pytest-xdist (or sequentially when two GatewayServer fixtures
       run in the same worker), racing for the default
       ``~/.local/share/almanak/utility/almanak_state.db`` flock and hitting
       ``BlockingIOError``.

    2. Deployment-mode env vars — ``almanak.config.env`` calls
       ``load_dotenv()`` at import, so a developer's ``.env`` leaks
       ``ALMANAK_GATEWAY_DATABASE_URL`` / ``AGENT_ID`` into the test
       process. With ``DATABASE_URL`` set but ``AGENT_ID`` unset, the
       gateway startup invariant aborts in
       ``validate_deployment_invariants``. CI never hits this because the CI
       checkout has no ``.env`` — clearing these makes local runs match CI.
       A test that exercises hosted mode sets ``AGENT_ID`` itself via its
       own monkeypatch, which runs after this fixture and wins.
    """
    db_path = tmp_path / "almanak_state.db"
    monkeypatch.setenv("ALMANAK_GATEWAY_DB_PATH", str(db_path))
    monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
    monkeypatch.delenv("ALMANAK_GATEWAY_DATABASE_URL", raising=False)
    monkeypatch.delenv("AGENT_ID", raising=False)
    yield
