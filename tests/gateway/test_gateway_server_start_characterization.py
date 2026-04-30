"""Characterization tests for ``GatewayServer.start`` (Phase 8.3d).

These tests pin the observable behaviour of the gateway bootstrap sequence so
the upcoming extraction of phase helpers cannot regress it. Unlike
``tests/gateway/test_server.py`` (which spins up a real gRPC server on a real
port), these tests mock ``grpc.aio.server`` and every network-facing dependency
so they run fast, hermetically, and cover ALL branches of ``start`` including
several that previously had no regression coverage (audit interceptor
enablement, CoinGecko warning path, wallet-registry plugin discovery, stale
reconciliation, metrics port path, reflection service list, warmup guards).

The tests do not change production code. When ``start`` is decomposed into
helper functions, re-running this module must continue to pass without edits -
that is the definition of "characterization": a safety net.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from grpc_health.v1 import health_pb2

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.server import GatewayServer

TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


def _fake_create_task(coro, *, name: str | None = None) -> MagicMock:
    """Close the coroutine (to avoid "never awaited" warnings) and return a mock.

    ``start`` hands ``_heartbeat_ttl_loop()`` (a coroutine object) to
    ``asyncio.create_task``. Under test we do not want that loop to actually
    run; calling ``coro.close()`` releases its resources cleanly.
    """
    coro.close()
    task = MagicMock(name="heartbeat_task_mock")
    task.name = name
    task.done.return_value = False
    return task


# ---------------------------------------------------------------------------
# Default patch stack — every branch under test needs (almost) the same mocks.
# ---------------------------------------------------------------------------
def _install_bootstrap_patches(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch every external dependency ``start`` touches; return the mocks.

    Returns a dict of the most commonly inspected mocks. ``asyncio`` and
    ``await`` interactions (server.start / health_servicer.set) are handled
    separately per test.
    """
    fake_server = MagicMock()
    fake_server.start = AsyncMock()
    fake_server.add_insecure_port = MagicMock()

    fake_executor = MagicMock()

    # grpc.aio.server() factory
    grpc_server_factory = MagicMock(return_value=fake_server)
    monkeypatch.setattr("almanak.gateway.server.grpc.aio.server", grpc_server_factory)

    # ThreadPoolExecutor factory
    executor_factory = MagicMock(return_value=fake_executor)
    monkeypatch.setattr("almanak.gateway.server.futures.ThreadPoolExecutor", executor_factory)

    # Registration helpers (each a no-op MagicMock — we only care they were called)
    add_health = MagicMock()
    monkeypatch.setattr("almanak.gateway.server.health_pb2_grpc.add_HealthServicer_to_server", add_health)

    mock_pb2_grpc = MagicMock()
    monkeypatch.setattr("almanak.gateway.server.gateway_pb2_grpc", mock_pb2_grpc)

    reflection_enable = MagicMock()
    monkeypatch.setattr("almanak.gateway.server.reflection.enable_server_reflection", reflection_enable)

    # Storage / state singletons — instantiating these in-process is fine, but
    # they touch SQLite. Swap for explicit mocks so tests never hit the disk.
    # Registry factory is patched both on its source module (used by the
    # helper) and at the server path (used by ``_heartbeat_ttl_loop`` lazy
    # import).
    fake_registry = MagicMock()
    fake_registry.reconcile_stale_on_startup = MagicMock(return_value=0)
    fake_registry.enforce_heartbeat_ttl = MagicMock(return_value=0)
    registry_factory = MagicMock(return_value=fake_registry)
    monkeypatch.setattr("almanak.gateway.registry.get_instance_registry", registry_factory)

    # Timeline store: consumed by server.py (fed to helper as callable).
    timeline_factory = MagicMock()
    monkeypatch.setattr("almanak.gateway.server.get_timeline_store", timeline_factory)

    # Lifecycle store: consumed by the helper module.
    fake_lifecycle_store = MagicMock()
    lifecycle_factory = MagicMock(return_value=fake_lifecycle_store)
    monkeypatch.setattr("almanak.gateway._server_start_helpers.get_lifecycle_store", lifecycle_factory)

    # Schema-contract validator (VIB-3763): touches SQLite/Postgres in
    # production. Replace with an AsyncMock so boot tests never hit storage.
    schema_validator = AsyncMock()
    monkeypatch.setattr(
        "almanak.gateway.server.validate_state_schema_at_boot",
        schema_validator,
    )

    # Local-DB flock (VIB-3761): touches the filesystem in production.
    # Replace with a no-op so boot tests never write a real lock file.
    flock_acquire = MagicMock(return_value=None)
    monkeypatch.setattr(
        "almanak.gateway.server.acquire_local_db_flock",
        flock_acquire,
    )

    # Servicer classes — replace with MagicMock so construction never touches
    # the network (e.g. ExecutionServiceServicer spins up aiohttp sessions).
    for cls_name in (
        "ExecutionServiceServicer",
        "MarketServiceServicer",
        "StateServiceServicer",
        "ObserveServiceServicer",
        "RpcServiceServicer",
        "IntegrationServiceServicer",
        "DashboardServiceServicer",
        "FundingRateServiceServicer",
        "SimulationServiceServicer",
        "PolymarketServiceServicer",
        "EnsoServiceServicer",
        "TokenServiceServicer",
        "LifecycleServiceServicer",
    ):
        monkeypatch.setattr(f"almanak.gateway.server.{cls_name}", MagicMock())

    # MetricsServer — avoid binding port 9090.
    fake_metrics_server = MagicMock()
    metrics_factory = MagicMock(return_value=fake_metrics_server)
    monkeypatch.setattr("almanak.gateway.server.MetricsServer", metrics_factory)

    return {
        "server": fake_server,
        "executor": fake_executor,
        "grpc_server_factory": grpc_server_factory,
        "executor_factory": executor_factory,
        "add_health": add_health,
        "reflection_enable": reflection_enable,
        "registry": fake_registry,
        "registry_factory": registry_factory,
        "timeline_factory": timeline_factory,
        "lifecycle_factory": lifecycle_factory,
        "schema_validator": schema_validator,
        "metrics_factory": metrics_factory,
        "metrics_server": fake_metrics_server,
        "pb2_grpc": mock_pb2_grpc,
    }


def _make_settings(**overrides) -> GatewaySettings:
    """Build a lean GatewaySettings with auth + metrics + audit disabled."""
    defaults = dict(
        grpc_port=50099,
        grpc_host="127.0.0.1",
        metrics_enabled=False,
        audit_enabled=False,
        allow_insecure=True,
        network="anvil",
        chains=[],
    )
    defaults.update(overrides)
    return GatewaySettings(**defaults)


def _make_server(settings: GatewaySettings) -> GatewayServer:
    s = GatewayServer(settings)
    # Suppress the heartbeat loop scheduling — create_task is patched per test.
    return s


# ---------------------------------------------------------------------------
# Phase 1: interceptor chain
# ---------------------------------------------------------------------------
class TestInterceptorChain:
    @pytest.mark.asyncio
    async def test_insecure_on_anvil_no_auth_interceptor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """allow_insecure=True on anvil skips AuthInterceptor construction."""
        mocks = _install_bootstrap_patches(monkeypatch)

        with patch("almanak.gateway._server_start_helpers.AuthInterceptor") as auth_cls:
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                settings = _make_settings(allow_insecure=True, network="anvil")
                server = _make_server(settings)
                await server.start()

        assert auth_cls.call_count == 0
        # grpc.aio.server() called with interceptors=[] (no AuthInterceptor)
        kwargs = mocks["grpc_server_factory"].call_args.kwargs
        assert kwargs["interceptors"] == []

    @pytest.mark.asyncio
    async def test_insecure_with_auth_token_on_mainnet_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Contradictory config on production network hard-fails at start()."""
        _install_bootstrap_patches(monkeypatch)
        settings = _make_settings(
            allow_insecure=True,
            network="mainnet",
            auth_token="tok",  # noqa: S106
        )
        server = _make_server(settings)
        with pytest.raises(RuntimeError, match="conflicting configuration"):
            await server.start()

    @pytest.mark.asyncio
    async def test_insecure_on_mainnet_without_auth_token_is_warned(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """allow_insecure=True on mainnet with no auth_token logs a stern warning."""
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(allow_insecure=True, network="mainnet", auth_token=None)
            server = _make_server(settings)
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        # Pin the log-string prefix (observability grep relies on this literal).
        assert any("INSECURE MODE on network 'mainnet'" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_insecure_auth_token_on_anvil_logs_ignored_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Configured auth_token is ignored (with a log) on anvil insecure mode."""
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(
                allow_insecure=True,
                network="anvil",
                auth_token="tok",  # noqa: S106
            )
            server = _make_server(settings)
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        assert any("Configured auth token ignored" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_auth_token_adds_interceptor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """allow_insecure=False AND auth_token set -> AuthInterceptor appended."""
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway._server_start_helpers.AuthInterceptor") as auth_cls:
            auth_cls.return_value = MagicMock()
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                settings = _make_settings(
                    allow_insecure=False,
                    auth_token="tok",  # noqa: S106
                )
                server = _make_server(settings)
                await server.start()

        auth_cls.assert_called_once_with("tok")
        interceptors = mocks["grpc_server_factory"].call_args.kwargs["interceptors"]
        assert auth_cls.return_value in interceptors

    @pytest.mark.asyncio
    async def test_no_auth_token_no_insecure_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No auth_token and allow_insecure=False refuses to start."""
        _install_bootstrap_patches(monkeypatch)
        settings = _make_settings(allow_insecure=False, auth_token=None)
        server = _make_server(settings)
        with pytest.raises(RuntimeError, match="No auth_token configured"):
            await server.start()

    @pytest.mark.asyncio
    async def test_audit_interceptor_appended_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway._server_start_helpers.AuditInterceptor") as audit_cls:
            audit_cls.return_value = MagicMock()
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                settings = _make_settings(audit_enabled=True, audit_log_level="info")
                server = _make_server(settings)
                await server.start()
        audit_cls.assert_called_once_with(enabled=True, log_level="info")
        interceptors = mocks["grpc_server_factory"].call_args.kwargs["interceptors"]
        assert audit_cls.return_value in interceptors

    @pytest.mark.asyncio
    async def test_metrics_interceptor_appended_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway._server_start_helpers.MetricsInterceptor") as metrics_cls:
            metrics_cls.return_value = MagicMock()
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                settings = _make_settings(metrics_enabled=True, metrics_port=0)
                # Disable the actual HTTP metrics server
                mocks["metrics_factory"].reset_mock()
                server = _make_server(settings)
                await server.start()
        metrics_cls.assert_called_once_with()
        interceptors = mocks["grpc_server_factory"].call_args.kwargs["interceptors"]
        assert metrics_cls.return_value in interceptors
        # Metrics HTTP server started
        mocks["metrics_factory"].assert_called_once_with(port=0)
        mocks["metrics_server"].start.assert_called_once_with()


# ---------------------------------------------------------------------------
# Phase 2: grpc.aio.server + executor build
# ---------------------------------------------------------------------------
class TestServerAndExecutorBuild:
    @pytest.mark.asyncio
    async def test_executor_uses_grpc_max_workers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(grpc_max_workers=7)
            server = _make_server(settings)
            await server.start()
        mocks["executor_factory"].assert_called_once_with(max_workers=7)

    @pytest.mark.asyncio
    async def test_server_assigned_after_start(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        assert server.server is mocks["server"]
        assert server._executor is mocks["executor"]


# ---------------------------------------------------------------------------
# Phase 3: storage bootstrap
# ---------------------------------------------------------------------------
class TestStorageBootstrap:
    @pytest.mark.asyncio
    async def test_timeline_store_postgres_when_database_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # VIB-3760: hosted-mode (Postgres) requires AGENT_ID + auth_token.
        monkeypatch.setenv("AGENT_ID", "agent-test")
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(
                database_url="postgres://x/y",
                auth_token="tok",  # noqa: S106
                allow_insecure=False,
            )
            server = _make_server(settings)
            await server.start()
        mocks["timeline_factory"].assert_called_once_with(database_url="postgres://x/y")

    @pytest.mark.asyncio
    async def test_timeline_store_sqlite_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(database_url=None, gateway_db_path="/tmp/gw.db")
            server = _make_server(settings)
            await server.start()
        mocks["timeline_factory"].assert_called_once_with(db_path="/tmp/gw.db")

    @pytest.mark.asyncio
    async def test_timeline_db_path_override_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(database_url=None, gateway_db_path="/tmp/gw.db", timeline_db_path="/tmp/tl.db")
            server = _make_server(settings)
            await server.start()
        mocks["timeline_factory"].assert_called_once_with(db_path="/tmp/tl.db")

    @pytest.mark.asyncio
    async def test_registry_reconciliation_logs_on_ghosts(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        mocks["registry"].reconcile_stale_on_startup.return_value = 3
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        assert any("reconciled 3 ghost RUNNING instance(s)" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_registry_reconciliation_silent_when_zero(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        mocks["registry"].reconcile_stale_on_startup.return_value = 0
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        assert not any("ghost RUNNING instance" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_lifecycle_store_receives_database_url_and_sqlite_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # VIB-3760: hosted-mode (Postgres) requires AGENT_ID + auth_token.
        monkeypatch.setenv("AGENT_ID", "agent-test")
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(
                database_url="postgres://x/y",
                gateway_db_path="/tmp/gw.db",
                auth_token="tok",  # noqa: S106
                allow_insecure=False,
            )
            server = _make_server(settings)
            await server.start()
        mocks["lifecycle_factory"].assert_called_once_with(
            database_url="postgres://x/y", sqlite_path="/tmp/gw.db"
        )


# ---------------------------------------------------------------------------
# Phase 4: CoinGecko warning
# ---------------------------------------------------------------------------
class TestCoingeckoLog:
    @pytest.mark.asyncio
    async def test_coingecko_absent_logs_info(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Ambient env vars (developer .env) can inject a real key — clear them.
        monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", raising=False)
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(coingecko_api_key=None)
            # Force-null again in case model_validator repopulated from env.
            settings.coingecko_api_key = None
            server = _make_server(settings)
            # Capture both server + helper loggers.
            with caplog.at_level(logging.INFO, logger="almanak.gateway"):
                await server.start()
        assert any("on-chain pricing (Chainlink oracles)" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_coingecko_present_no_log(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(coingecko_api_key="sk-test")
            server = _make_server(settings)
            with caplog.at_level(logging.INFO, logger="almanak.gateway"):
                await server.start()
        assert not any("on-chain pricing" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Phase 6: wallet-registry plugin discovery
# ---------------------------------------------------------------------------
class TestWalletRegistryLoading:
    @pytest.mark.asyncio
    async def test_no_wallets_env_var_no_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        assert server._wallet_registry is None

    @pytest.mark.asyncio
    async def test_wallets_env_plugin_not_installed_logs_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _install_bootstrap_patches(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        # entry_points returns no ``registry`` entry
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([])
        monkeypatch.setattr("almanak.gateway._server_start_helpers.entry_points", MagicMock(return_value=ep_result))
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        assert server._wallet_registry is None
        assert any("wallet plugin is not installed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_wallets_env_plugin_loaded_sets_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = []
        fake_registry_cls = MagicMock(__name__="FakeRegistry")
        fake_registry_cls.from_env.return_value = fake_registry

        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = fake_registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr("almanak.gateway._server_start_helpers.entry_points", MagicMock(return_value=ep_result))

        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings(chains=["arbitrum"]))
            await server.start()

        fake_registry_cls.from_env.assert_called_once_with(default_chains=["arbitrum"])
        assert server._wallet_registry is fake_registry

    @pytest.mark.asyncio
    async def test_legacy_safe_env_warning_when_both_set(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _install_bootstrap_patches(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        monkeypatch.setenv("SAFE_WALLET_ADDRESS", "0xSafe")

        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = []
        fake_registry_cls = MagicMock(__name__="FakeRegistry")
        fake_registry_cls.from_env.return_value = fake_registry
        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = fake_registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr("almanak.gateway._server_start_helpers.entry_points", MagicMock(return_value=ep_result))

        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                await server.start()
        assert any("ALMANAK_GATEWAY_WALLETS takes precedence" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Phase 7: servicer registration (smoke, not exhaustive — each cls is mocked)
# ---------------------------------------------------------------------------
class TestServicerRegistration:
    @pytest.mark.asyncio
    async def test_all_servicers_added_and_attributes_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        # Every expected servicer was registered via gateway_pb2_grpc helper.
        for add_fn in (
            "add_ExecutionServiceServicer_to_server",
            "add_MarketServiceServicer_to_server",
            "add_HealthServicer_to_server",  # custom RegisterChains servicer
            "add_StateServiceServicer_to_server",
            "add_ObserveServiceServicer_to_server",
            "add_RpcServiceServicer_to_server",
            "add_IntegrationServiceServicer_to_server",
            "add_DashboardServiceServicer_to_server",
            "add_FundingRateServiceServicer_to_server",
            "add_SimulationServiceServicer_to_server",
            "add_PolymarketServiceServicer_to_server",
            "add_EnsoServiceServicer_to_server",
            "add_TokenServiceServicer_to_server",
            "add_LifecycleServiceServicer_to_server",
        ):
            assert getattr(mocks["pb2_grpc"], add_fn).call_count >= 1, (
                f"{add_fn} was not called"
            )

        # Execution + market cross-references wired.
        assert server._execution_servicer is not None
        assert server._market_servicer is not None
        # market reference exposed on execution for self-serve pricing
        assert server._execution_servicer.market_servicer is server._market_servicer

    @pytest.mark.asyncio
    async def test_execution_servicer_gets_wallet_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = []
        fake_registry_cls = MagicMock(__name__="FakeRegistry")
        fake_registry_cls.from_env.return_value = fake_registry
        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = fake_registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr("almanak.gateway._server_start_helpers.entry_points", MagicMock(return_value=ep_result))

        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        assert server._execution_servicer.wallet_registry is fake_registry
        assert server._market_servicer.wallet_registry is fake_registry


# ---------------------------------------------------------------------------
# Phase 8: reflection + port binding + NOT_SERVING
# ---------------------------------------------------------------------------
class TestReflectionAndPort:
    @pytest.mark.asyncio
    async def test_port_bound_via_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(grpc_host="0.0.0.0", grpc_port=12345)
            server = _make_server(settings)
            await server.start()
        mocks["server"].add_insecure_port.assert_called_once_with("0.0.0.0:12345")

    @pytest.mark.asyncio
    async def test_reflection_service_names_include_all_services(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mocks = _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        args, _kwargs = mocks["reflection_enable"].call_args
        service_names = args[0]
        # Every proto service that ships must appear so reflection clients can
        # discover them.  This list is load-bearing for observability.
        for expected in (
            "almanak.gateway.proto.MarketService",
            "almanak.gateway.proto.StateService",
            "almanak.gateway.proto.ExecutionService",
            "almanak.gateway.proto.ObserveService",
            "almanak.gateway.proto.RpcService",
            "almanak.gateway.proto.IntegrationService",
            "almanak.gateway.proto.DashboardService",
            "almanak.gateway.proto.FundingRateService",
            "almanak.gateway.proto.SimulationService",
            "almanak.gateway.proto.PolymarketService",
            "almanak.gateway.proto.EnsoService",
            "almanak.gateway.proto.TokenService",
            "almanak.gateway.proto.LifecycleService",
        ):
            assert expected in service_names

    @pytest.mark.asyncio
    async def test_not_serving_set_before_port_bind(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """NOT_SERVING must be set before bind so clients can't race with warmup."""
        mocks = _install_bootstrap_patches(monkeypatch)
        call_order: list[str] = []

        original_set = AsyncMock()

        async def track_set(service, status):
            if status == health_pb2.HealthCheckResponse.NOT_SERVING:
                call_order.append("NOT_SERVING")
            elif status == health_pb2.HealthCheckResponse.SERVING:
                call_order.append("SERVING")
            await original_set(service, status)

        def track_bind(addr):
            call_order.append("bind")

        mocks["server"].add_insecure_port.side_effect = track_bind

        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            server._health_servicer.set = track_set
            await server.start()

        # NOT_SERVING before bind before SERVING
        assert call_order.index("NOT_SERVING") < call_order.index("bind")
        assert call_order.index("bind") < call_order.index("SERVING")


# ---------------------------------------------------------------------------
# Phase 10-11-12: heartbeat task + warmup + prewarm
# ---------------------------------------------------------------------------
class TestWarmupAndPrewarm:
    @pytest.mark.asyncio
    async def test_heartbeat_task_created(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            await server.start()
        create_task.assert_called_once()
        assert server._heartbeat_ttl_task is not None
        # Task name preserved (operator tooling inspects this).
        assert create_task.call_args.kwargs.get("name") == "heartbeat-ttl-enforcer"

    @pytest.mark.asyncio
    async def test_market_warmup_skipped_when_no_chains(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings(chains=[]))
            await server.start()
        # warmup() on market_servicer never called
        server._market_servicer.warmup.assert_not_called()

    @pytest.mark.asyncio
    async def test_market_warmup_called_when_chains_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        # Make the MarketServiceServicer factory produce a mock with AsyncMock warmup
        fake_market = MagicMock()
        fake_market.warmup = AsyncMock()
        with patch("almanak.gateway.server.MarketServiceServicer", return_value=fake_market):
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                # _prewarm_chains is async and iterates settings.chains — stub it
                with patch.object(GatewayServer, "_prewarm_chains", new=AsyncMock()):
                    server = _make_server(
                        _make_settings(chains=["arbitrum"], private_key=TEST_PRIVATE_KEY)
                    )
                    await server.start()
        assert fake_market.warmup.await_count == 1

    @pytest.mark.asyncio
    async def test_market_warmup_swallows_exception(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warmup failures must not abort start (logged at warning, then continue)."""
        _install_bootstrap_patches(monkeypatch)
        market = MagicMock()
        market.warmup = AsyncMock(side_effect=RuntimeError("boom"))
        with patch("almanak.gateway.server.MarketServiceServicer", return_value=market):
            with patch("almanak.gateway.server.asyncio.create_task") as create_task:
                create_task.side_effect = _fake_create_task
                with patch.object(GatewayServer, "_prewarm_chains", new=AsyncMock()):
                    settings = _make_settings(chains=["arbitrum"], private_key=TEST_PRIVATE_KEY)
                    server = _make_server(settings)
                    with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                        await server.start()
        assert any("Market service warmup failed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_prewarm_chains_swallows_exception(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            settings = _make_settings(chains=["arbitrum"], private_key=TEST_PRIVATE_KEY)
            server = _make_server(settings)
            with patch.object(GatewayServer, "_prewarm_chains", side_effect=RuntimeError("bad")):
                with caplog.at_level(logging.WARNING, logger="almanak.gateway.server"):
                    await server.start()
        assert any("Chain pre-warm failed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_prewarm_skipped_with_no_chains_no_registry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_bootstrap_patches(monkeypatch)
        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            with patch.object(GatewayServer, "_prewarm_chains", new=AsyncMock()) as prewarm:
                server = _make_server(_make_settings(chains=[]))
                await server.start()
        prewarm.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 13: SERVING flip at end
# ---------------------------------------------------------------------------
class TestServingFlip:
    @pytest.mark.asyncio
    async def test_serving_set_after_warmup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_bootstrap_patches(monkeypatch)
        states: list[int] = []

        async def track_set(service, status):
            states.append(status)

        with patch("almanak.gateway.server.asyncio.create_task") as create_task:
            create_task.side_effect = _fake_create_task
            server = _make_server(_make_settings())
            server._health_servicer.set = track_set
            await server.start()

        assert health_pb2.HealthCheckResponse.NOT_SERVING in states
        assert health_pb2.HealthCheckResponse.SERVING in states
        # SERVING must be the LAST status set.
        assert states[-1] == health_pb2.HealthCheckResponse.SERVING
