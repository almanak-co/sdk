"""Regression guard for VIB-5542: ``almanak ax`` must run from any cwd.

``ax`` is an operator / AI-agent utility surface, not a strategy-scoped
runner, so it must be runnable from any directory. Its managed-gateway
autostart (`_start_managed_gateway`) handles two cases:

1. **Inside a strategy folder** — `auto_detect_strategy_folder(export_env=True)`
   pins that folder so the gateway uses the strategy's own DB (preserving the
   pre-fix strict-resolver behavior — e.g. `ax positions reconcile` must target
   the strategy it is standing in, not the shared utility DB).
2. **Anywhere else** — `standalone=True` lets the local SQLite path fall back to
   the per-user utility DB instead of raising `LocalPathError`. Before the fix
   the strict resolver hard-failed outside a strategy folder, breaking `ax` for
   AI-agent / operator use from arbitrary directories.

The downstream resolver behavior (`standalone=True` -> lenient resolution;
folder / `ALMANAK_STATE_DB` still win) is covered by
`tests/gateway/test_state_path_resolution.py`. These tests pin the *caller
contract*: that `ax` passes the flag, falls back cleanly with no strategy
folder, AND still pins a cwd strategy folder's DB when one is present. The
negative-control was verified manually (each behavioral assertion fails when
its half of the fix is reverted).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# Env vars that steer local DB-path resolution — cleared in every test so the
# fallback/auto-detect behavior under test is what actually runs, and so
# `ALMANAK_IS_HOSTED` in a developer's shell/.env can't turn resolution into the
# hosted-mode `LocalPathError`. `monkeypatch.delenv` here also guarantees these
# are restored on teardown even when production code re-exports
# `ALMANAK_STRATEGY_FOLDER` mid-test (no cross-test pollution).
_DB_PATH_ENV_VARS = (
    "ALMANAK_STATE_DB",
    "ALMANAK_STRATEGY_FOLDER",
    "ALMANAK_GATEWAY_DB_PATH",
    "ALMANAK_IS_HOSTED",
)


class _FakeCtx:
    """Minimal stand-in for click.Context — exposes only ``obj``."""

    def __init__(self, data: dict) -> None:
        self.obj = data


@pytest.fixture
def base_ctx() -> dict:
    return {
        "gateway_host": "127.0.0.1",
        "gateway_port": 50055,
        "chain": "base",
        "wallet": "0xWALLET",
        "max_trade_usd": 1000.0,
        "network": None,
    }


def _clear_db_path_env(monkeypatch) -> None:
    for var in _DB_PATH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _invoke_start_managed_gateway(ctx_obj: dict) -> dict:
    """Call ``ax._start_managed_gateway`` with the gateway boot mocked.

    Runs the real ``gateway_config_from_env`` (so the kwargs -> settings
    mapping is exercised) and the real ``auto_detect_strategy_folder`` (so the
    cwd-detection is exercised), but fakes ``ManagedGateway`` so no port is
    bound and no server starts. Returns the captured ``kwargs`` and ``settings``.

    Uses ``network="anvil"`` so the test-network path is taken (no auth-token
    generation, ``allow_insecure=True``) — orthogonal to the standalone flag.
    """
    from almanak.config.env import gateway_config_from_env as real_gcfe
    from almanak.framework.cli.ax import _start_managed_gateway

    captured: dict = {}

    def _spy_gcfe(**kwargs):
        captured["kwargs"] = kwargs
        settings = real_gcfe(**kwargs)
        captured["settings"] = settings
        return settings

    fake_managed = MagicMock()
    fake_managed.host = "127.0.0.1"
    fake_managed.port = 50055

    with (
        patch("almanak.config.env.gateway_config_from_env", side_effect=_spy_gcfe),
        patch("almanak.gateway.managed.ManagedGateway", return_value=fake_managed),
        patch("almanak.gateway.managed.is_port_in_use", return_value=False),
        # Signer/wallet matching is orthogonal to the standalone flag and reads
        # operator env; neutralize it so the test is hermetic.
        patch("almanak.framework.cli.ax._assert_signer_matches_intended_wallet"),
    ):
        _start_managed_gateway(_FakeCtx(ctx_obj), "127.0.0.1", 50055, "anvil")

    return captured


def test_ax_managed_gateway_requests_standalone(base_ctx, tmp_path, monkeypatch):
    """``ax`` must pass ``standalone=True`` so it works from any cwd (VIB-5542)."""
    _clear_db_path_env(monkeypatch)
    monkeypatch.chdir(tmp_path)  # clean, non-strategy cwd

    captured = _invoke_start_managed_gateway(base_ctx)

    # The caller contract: the flag is in the kwargs handed to the config
    # builder, and it survives into the resulting GatewaySettings.
    assert captured["kwargs"].get("standalone") is True
    assert captured["settings"].standalone is True


def test_ax_settings_resolve_outside_strategy_folder(base_ctx, tmp_path, monkeypatch):
    """No strategy folder present -> falls back to the utility DB, no crash.

    Behavioral regression guard: before the fix, ``standalone`` was ``False`` and
    this resolution raised ``LocalPathError``.
    """
    from almanak.gateway._server_start_helpers import resolve_gateway_local_db_path

    _clear_db_path_env(monkeypatch)
    monkeypatch.chdir(tmp_path)  # clean cwd with NO config.json / strategy.py

    captured = _invoke_start_managed_gateway(base_ctx)
    settings = captured["settings"]
    assert settings.standalone is True

    db_path = resolve_gateway_local_db_path(settings)
    assert "utility" in str(db_path)


def test_ax_inside_strategy_folder_pins_strategy_db(base_ctx, tmp_path, monkeypatch):
    """Run from inside a strategy folder -> gateway uses the strategy's DB.

    Guards the regression surfaced in audit (Codex / pr-auditor): ``standalone``
    alone would route an in-folder ``ax`` to the utility DB because the lenient
    resolver does not auto-detect cwd. ``ax`` must auto-detect+export the cwd
    strategy folder so e.g. ``ax positions reconcile`` targets the strategy it is
    standing in. This assertion fails if the ``auto_detect_strategy_folder`` call
    is removed.
    """
    from almanak.gateway._server_start_helpers import resolve_gateway_local_db_path

    _clear_db_path_env(monkeypatch)
    # Make tmp_path look like a strategy folder, then stand in it — with NO
    # ALMANAK_STRATEGY_FOLDER env set (the exact case that masked the bug).
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    captured = _invoke_start_managed_gateway(base_ctx)
    settings = captured["settings"]
    # Standalone is still requested (it only governs the no-folder fallback)...
    assert settings.standalone is True

    # ...but because we are inside a strategy folder, resolution pins the
    # folder's own DB, NOT the per-user utility DB.
    db_path = resolve_gateway_local_db_path(settings)
    assert db_path == tmp_path / "almanak_state.db"
    assert "utility" not in str(db_path)
