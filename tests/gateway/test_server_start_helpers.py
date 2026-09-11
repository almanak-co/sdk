"""Unit tests for phase helpers extracted from ``GatewayServer.start`` (Phase 8.3d).

These tests exercise each helper in isolation with lightweight fakes. They
complement the RPC-level characterization tests in
``test_gateway_server_start_characterization.py`` by pinning helper-module
contracts directly, so later refactors of the start-up wiring cannot mask
bugs inside the helpers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.gateway._server_start_helpers import (
    acquire_local_db_flock,
    build_interceptors,
    build_reflection_service_names,
    initialize_instance_registry,
    initialize_lifecycle_store,
    initialize_timeline_store,
    load_wallet_registry,
    log_pricing_source_configuration,
    validate_deployment_invariants,
)
from almanak.gateway.core.settings import GatewaySettings


def _settings(**kwargs) -> GatewaySettings:
    defaults = {
        "metrics_enabled": False,
        "audit_enabled": False,
        "allow_insecure": True,
        "network": "anvil",
    }
    defaults.update(kwargs)
    return GatewaySettings(**defaults)


# ---------------------------------------------------------------------------
# build_interceptors
# ---------------------------------------------------------------------------
class TestBuildInterceptors:
    def test_insecure_anvil_no_interceptors(self) -> None:
        interceptors = build_interceptors(_settings(allow_insecure=True, network="anvil", auth_token=None))
        assert interceptors == []

    def test_insecure_mainnet_with_auth_token_raises(self) -> None:
        with pytest.raises(RuntimeError, match="conflicting configuration"):
            build_interceptors(
                _settings(allow_insecure=True, network="mainnet", auth_token="tok")  # noqa: S106
            )

    def test_insecure_mainnet_no_token_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            interceptors = build_interceptors(_settings(allow_insecure=True, network="mainnet", auth_token=None))
        assert interceptors == []
        assert any("INSECURE MODE on network 'mainnet'" in r.message for r in caplog.records)

    def test_insecure_anvil_with_auth_token_ignored(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            interceptors = build_interceptors(
                _settings(allow_insecure=True, network="anvil", auth_token="tok")  # noqa: S106
            )
        assert interceptors == []
        assert any("auth token ignored" in r.message for r in caplog.records)

    def test_no_auth_token_and_not_insecure_raises(self) -> None:
        with pytest.raises(RuntimeError, match="No auth_token configured"):
            build_interceptors(_settings(allow_insecure=False, auth_token=None))

    def test_auth_token_adds_auth_interceptor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_auth = MagicMock()
        fake_auth_cls = MagicMock(return_value=fake_auth)
        monkeypatch.setattr("almanak.gateway._server_start_helpers.AuthInterceptor", fake_auth_cls)
        interceptors = build_interceptors(
            _settings(allow_insecure=False, auth_token="tok")  # noqa: S106
        )
        fake_auth_cls.assert_called_once_with("tok")
        assert fake_auth in interceptors

    def test_audit_interceptor_appended(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_audit = MagicMock()
        fake_audit_cls = MagicMock(return_value=fake_audit)
        monkeypatch.setattr("almanak.gateway._server_start_helpers.AuditInterceptor", fake_audit_cls)
        interceptors = build_interceptors(
            _settings(audit_enabled=True, audit_log_level="debug", allow_insecure=True, network="anvil")
        )
        fake_audit_cls.assert_called_once_with(enabled=True, log_level="debug")
        assert fake_audit in interceptors

    def test_metrics_interceptor_appended(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_metrics = MagicMock()
        fake_metrics_cls = MagicMock(return_value=fake_metrics)
        monkeypatch.setattr("almanak.gateway._server_start_helpers.MetricsInterceptor", fake_metrics_cls)
        interceptors = build_interceptors(_settings(metrics_enabled=True, allow_insecure=True, network="anvil"))
        fake_metrics_cls.assert_called_once_with()
        assert fake_metrics in interceptors

    def test_interceptor_order_auth_audit_metrics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Auth must come first (rejects earliest); then audit; then metrics."""
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.AuthInterceptor",
            lambda _: "AUTH",
        )
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.AuditInterceptor",
            lambda **_: "AUDIT",
        )
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.MetricsInterceptor",
            lambda: "METRICS",
        )
        interceptors = build_interceptors(
            _settings(
                allow_insecure=False,
                auth_token="tok",  # noqa: S106
                audit_enabled=True,
                audit_log_level="info",
                metrics_enabled=True,
            )
        )
        assert interceptors == ["AUTH", "AUDIT", "METRICS"]


# ---------------------------------------------------------------------------
# initialize_timeline_store
# ---------------------------------------------------------------------------
class TestInitializeTimelineStore:
    def test_postgres_when_database_url_set(self) -> None:
        factory = MagicMock()
        initialize_timeline_store(
            _settings(database_url="postgres://x/y"),
            factory,
        )
        factory.assert_called_once_with(
            database_url="postgres://x/y",
            scope_deployment_id=None,
            startup_load_limit=10000,
        )

    def test_postgres_hosted_scopes_load_to_deployment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Hosted gateways must not slurp the platform-wide timeline table.

        The metrics DB is shared across every deployment; the startup load
        must be scoped to this pod's ALMANAK_DEPLOYMENT_ID (August 2026
        Cloud NAT incident: unscoped loads OOM-crashlooped 55 sidecars).
        """
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "dep-123")
        factory = MagicMock()
        initialize_timeline_store(
            _settings(database_url="postgres://x/y", timeline_startup_load_limit=500),
            factory,
        )
        factory.assert_called_once_with(
            database_url="postgres://x/y",
            scope_deployment_id="dep-123",
            startup_load_limit=500,
        )

    def test_sqlite_fallback_when_no_database_url(self) -> None:
        factory = MagicMock()
        initialize_timeline_store(
            _settings(database_url=None, gateway_db_path="/tmp/gw.db"),
            factory,
        )
        factory.assert_called_once_with(db_path="/tmp/gw.db")

    def test_timeline_db_path_override_wins(self) -> None:
        factory = MagicMock()
        initialize_timeline_store(
            _settings(
                database_url=None,
                gateway_db_path="/tmp/gw.db",
                timeline_db_path="/tmp/tl.db",
            ),
            factory,
        )
        factory.assert_called_once_with(db_path="/tmp/tl.db")

    @pytest.mark.parametrize("blank", ["", "   ", "\t"])
    def test_blank_timeline_override_falls_through_to_gateway_db(self, blank: str) -> None:
        factory = MagicMock()
        initialize_timeline_store(
            _settings(database_url=None, gateway_db_path="/tmp/gw.db", timeline_db_path=blank),
            factory,
        )
        factory.assert_called_once_with(db_path="/tmp/gw.db")


# ---------------------------------------------------------------------------
# initialize_instance_registry
# ---------------------------------------------------------------------------
class TestInitializeInstanceRegistry:
    def test_returns_registry_and_skips_log_when_no_stale(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        fake_registry = MagicMock()
        fake_registry.reconcile_stale_on_startup = MagicMock(return_value=0)
        monkeypatch.setattr(
            "almanak.gateway.registry.get_instance_registry",
            MagicMock(return_value=fake_registry),
        )
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            result = initialize_instance_registry(_settings(gateway_db_path="/tmp/gw.db"))
        assert result is fake_registry
        assert not any("ghost RUNNING instance" in r.message for r in caplog.records)

    def test_logs_when_stale_count_positive(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        fake_registry = MagicMock()
        fake_registry.reconcile_stale_on_startup = MagicMock(return_value=7)
        monkeypatch.setattr(
            "almanak.gateway.registry.get_instance_registry",
            MagicMock(return_value=fake_registry),
        )
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            initialize_instance_registry(_settings(gateway_db_path="/tmp/gw.db"))
        assert any("reconciled 7 ghost RUNNING instance(s)" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# initialize_lifecycle_store
# ---------------------------------------------------------------------------
class TestInitializeLifecycleStore:
    def test_passes_database_url_and_sqlite_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_store = MagicMock()
        factory = MagicMock(return_value=fake_store)
        monkeypatch.setattr("almanak.gateway._server_start_helpers.get_lifecycle_store", factory)
        result = initialize_lifecycle_store(_settings(database_url="postgres://x/y", gateway_db_path="/tmp/gw.db"))
        assert result is fake_store
        factory.assert_called_once_with(database_url="postgres://x/y", sqlite_path="/tmp/gw.db")


# ---------------------------------------------------------------------------
# log_pricing_source_configuration
# ---------------------------------------------------------------------------
class TestLogPricingSourceConfiguration:
    def test_logs_when_no_coingecko_key(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Ambient env vars (developer .env) can inject a real key — clear them.
        monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", raising=False)
        s = _settings(coingecko_api_key=None)
        s.coingecko_api_key = None
        with caplog.at_level(logging.INFO, logger="almanak.gateway._server_start_helpers"):
            log_pricing_source_configuration(s)
        assert any("Chainlink oracles" in r.message for r in caplog.records)

    def test_silent_when_coingecko_key_set(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger="almanak.gateway._server_start_helpers"):
            log_pricing_source_configuration(_settings(coingecko_api_key="sk-test"))
        assert not any("Chainlink oracles" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# load_wallet_registry
# ---------------------------------------------------------------------------
@dataclass
class _FakeResolved:
    account_address: str
    kind: str = "eoa"


class TestLoadWalletRegistry:
    def test_no_env_var_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        result = load_wallet_registry(_settings())
        assert result is None

    def test_plugin_not_installed_logs_and_returns_none(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([])
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.entry_points",
            MagicMock(return_value=ep_result),
        )
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            result = load_wallet_registry(_settings())
        assert result is None
        assert any("wallet plugin is not installed" in r.message for r in caplog.records)

    def test_plugin_loaded_returns_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        # No legacy Safe env var
        monkeypatch.delenv("SAFE_WALLET_ADDRESS", raising=False)
        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = ["arbitrum"]
        fake_registry.resolve.return_value = _FakeResolved(account_address="0x1234567890abcdef", kind="eoa")
        registry_cls = MagicMock(__name__="FakeRegistry")
        registry_cls.from_env.return_value = fake_registry
        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.entry_points",
            MagicMock(return_value=ep_result),
        )

        result = load_wallet_registry(_settings(chains=["arbitrum"]))
        assert result is fake_registry
        registry_cls.from_env.assert_called_once_with(default_chains=["arbitrum"])

    def test_plugin_loaded_with_short_address_does_not_slice(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Short addresses are logged as-is (no slice)."""
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        monkeypatch.delenv("SAFE_WALLET_ADDRESS", raising=False)
        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = ["arbitrum"]
        # <= 10 chars — exercises the else branch of the ternary.
        fake_registry.resolve.return_value = _FakeResolved(account_address="0xabc", kind="eoa")
        registry_cls = MagicMock(__name__="FakeRegistry")
        registry_cls.from_env.return_value = fake_registry
        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.entry_points",
            MagicMock(return_value=ep_result),
        )

        with caplog.at_level(logging.INFO, logger="almanak.gateway._server_start_helpers"):
            load_wallet_registry(_settings())
        # The short address must appear whole (no "..." suffix) in the log.
        wallet_log = [r.message for r in caplog.records if "Wallet config" in r.getMessage()]
        assert wallet_log, "expected a 'Wallet config' log line"
        assert "0xabc" in wallet_log[0]
        assert "..." not in wallet_log[0]

    def test_plugin_loaded_with_legacy_safe_env_warns(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
        monkeypatch.setenv("SAFE_WALLET_ADDRESS", "0xSafe")
        fake_registry = MagicMock()
        fake_registry.all_chains.return_value = []
        registry_cls = MagicMock(__name__="FakeRegistry")
        registry_cls.from_env.return_value = fake_registry
        fake_ep = MagicMock()
        fake_ep.name = "registry"
        fake_ep.load.return_value = registry_cls
        ep_result = MagicMock()
        ep_result.__iter__ = lambda self: iter([fake_ep])
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.entry_points",
            MagicMock(return_value=ep_result),
        )
        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            load_wallet_registry(_settings())
        assert any("takes precedence" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# build_reflection_service_names
# ---------------------------------------------------------------------------
class TestBuildReflectionServiceNames:
    def test_all_expected_services_present(self) -> None:
        names = build_reflection_service_names()
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
            "almanak.gateway.proto.PoolAnalyticsService",
            "almanak.gateway.proto.PoolHistoryService",
            "almanak.gateway.proto.EnsoService",
            "almanak.gateway.proto.TokenService",
            "almanak.gateway.proto.LifecycleService",
            "almanak.gateway.proto.TeardownService",
        ):
            assert expected in names

    def test_includes_grpc_health_and_reflection(self) -> None:
        names = build_reflection_service_names()
        assert "grpc.health.v1.Health" in names
        assert "grpc.reflection.v1alpha.ServerReflection" in names

    def test_reflection_covers_all_proto_services_except_exemptions(self) -> None:
        """Every gRPC service DEFINED in gateway.proto must be exposed via
        reflection OR appear in the documented exemption set.

        This is the *completeness* guard the older subset checks lacked. A
        service that is registered on the server but missing from
        ``build_reflection_service_names`` is invisible to operator tooling
        (``grpcurl list``, dashboards) — exactly how PoolHistoryService
        (VIB-4728 / POOL-2) shipped while its sibling PoolAnalyticsService was
        reflected. Forcing each proto service to be reflected-or-exempted turns
        that omission into a test failure instead of a runtime surprise.
        """
        from almanak.gateway.proto import gateway_pb2

        names = set(build_reflection_service_names())

        # Services intentionally NOT advertised via reflection. Each entry is a
        # deliberate, reviewable decision documented with a reason — NOT a place
        # to silence the guard for a service that simply forgot to register.
        reflection_exempt: dict[str, str] = {
            # VIB-4210: internal reconciliation control-plane RPC, not part of
            # the operator-facing surface.
            "almanak.gateway.proto.PositionService": "reconciliation control-plane (VIB-4210)",
            # The gateway's own Health service exists for the internal
            # RegisterChains pre-warming RPC; the operator-facing health
            # surface is the standard grpc.health.v1.Health (which IS reflected,
            # via build_reflection_service_names). Check/Watch here are
            # superseded by the standard health servicer.
            "almanak.gateway.proto.Health": "internal RegisterChains pre-warming; grpc.health.v1.Health is the operator surface",
        }

        proto_service_names = {svc.full_name for svc in gateway_pb2.DESCRIPTOR.services_by_name.values()}
        missing = proto_service_names - names - set(reflection_exempt)
        assert not missing, (
            "proto services missing from gRPC reflection — add to "
            "build_reflection_service_names(), or add to reflection_exempt with "
            f"a documented reason: {sorted(missing)}"
        )


# ---------------------------------------------------------------------------
# validate_deployment_invariants — VIB-3760, plan §A4
#
# Test IDs: T-3760-1..T-3760-10. These pin the boot-time invariants that
# Hosted-mode env and the gateway's deployment-shape settings must agree in
# both directions. Silent fallback is the bug we are removing.
# ---------------------------------------------------------------------------
def _hosted_settings(**overrides) -> GatewaySettings:
    """Settings shaped like a hosted deployment (DB url, auth, secure)."""
    base = {
        "metrics_enabled": False,
        "audit_enabled": False,
        "allow_insecure": False,
        "network": "mainnet",
        "database_url": "postgres://user:pass@host/db",  # noqa: S106
        "auth_token": "tok",  # noqa: S106
    }
    base.update(overrides)
    return GatewaySettings(**base)


def _local_settings(**overrides) -> GatewaySettings:
    """Settings shaped like a local deployment (no DB url, no auth)."""
    base = {
        "metrics_enabled": False,
        "audit_enabled": False,
        "allow_insecure": True,
        "network": "anvil",
        "database_url": None,
        "auth_token": None,
    }
    base.update(overrides)
    return GatewaySettings(**base)


@pytest.fixture(autouse=True)
def _clear_mode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test sets its own deployment-mode env explicitly."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)


class TestValidateDeploymentInvariants:
    """VIB-4722: hosted mode is signalled by ``ALMANAK_IS_HOSTED``."""

    # ---- T-3760-1: hosted, all consistent → passes ------------------------
    def test_t_3760_1_hosted_all_consistent_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-1")
        # Should not raise.
        validate_deployment_invariants(_hosted_settings())

    # ---- T-3760-2: hosted + DATABASE_URL unset → refuse -------------------
    def test_t_3760_2_hosted_missing_database_url_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-2")
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_hosted_settings(database_url=None))
        msg = str(exc.value)
        assert "ALMANAK_IS_HOSTED is set (hosted mode)" in msg
        assert "ALMANAK_GATEWAY_DATABASE_URL is unset" in msg

    # ---- T-3760-3: hosted + allow_insecure=True → refuse ------------------
    def test_t_3760_3_hosted_allow_insecure_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-3")
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_hosted_settings(allow_insecure=True))
        msg = str(exc.value)
        assert "ALMANAK_GATEWAY_ALLOW_INSECURE=true" in msg
        assert "Hosted mode forbids insecure" in msg

    # ---- T-3760-4: hosted + AUTH_TOKEN unset → refuse ---------------------
    def test_t_3760_4_hosted_missing_auth_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-4")
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_hosted_settings(auth_token=None))
        msg = str(exc.value)
        assert "ALMANAK_GATEWAY_AUTH_TOKEN is unset" in msg
        assert "Hosted mode requires an auth token" in msg

    # ---- VIB-4722 review F1: hosted + ALMANAK_DEPLOYMENT_ID blank → refuse -
    def test_hosted_missing_deployment_id_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # ALMANAK_DEPLOYMENT_ID is left unset by the _clear_mode_env fixture.
        # The joint invariant (blueprint 29 §2.3) must be enforced at this
        # boot guard — a hosted pod with no id cannot stamp deployment-scoped
        # rows, and a read-only dashboard pod never reaches the lazy check
        # inside mode.deployment_id().
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_hosted_settings())
        msg = str(exc.value)
        assert "ALMANAK_DEPLOYMENT_ID is blank" in msg

    # ---- T-3760-5: local + DATABASE_URL set → refuse ----------------------
    def test_t_3760_5_local_with_database_url_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_local_settings(database_url="postgres://x"))
        msg = str(exc.value)
        assert "ALMANAK_GATEWAY_DATABASE_URL is set but ALMANAK_IS_HOSTED is not" in msg
        assert "Silent fallback removed" in msg

    # ---- T-3760-6: local default → passes ---------------------------------
    def test_t_3760_6_local_default_passes(self) -> None:
        # Should not raise.
        validate_deployment_invariants(_local_settings())

    # ---- T-3760-7: falsey ALMANAK_IS_HOSTED is treated as local -----------
    def test_t_3760_7_falsey_is_hosted_is_local(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A falsey/empty ALMANAK_IS_HOSTED must NOT trigger hosted-mode checks.

        If the helper treated mere presence as hosted, the local-mode
        DATABASE_URL check would not fire and we'd silently proceed in a
        half-hosted state.
        """
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "")
        with pytest.raises(RuntimeError, match=r"Gateway startup aborted") as exc:
            validate_deployment_invariants(_local_settings(database_url="postgres://x"))
        msg = str(exc.value)
        assert "ALMANAK_GATEWAY_DATABASE_URL is set but ALMANAK_IS_HOSTED is not" in msg

    # ---- T-3760-8: whitespace ALMANAK_IS_HOSTED is treated as local -------
    def test_t_3760_8_whitespace_is_hosted_is_local(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "   ")
        validate_deployment_invariants(_local_settings())

    # ---- T-3760-9: multiple mismatches → all reported in one error -------
    def test_t_3760_9_multiple_mismatches_reported_together(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Operator gets every issue in one pass — not N restart cycles."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "agent-9")
        with pytest.raises(RuntimeError) as exc:
            validate_deployment_invariants(_hosted_settings(database_url=None, allow_insecure=True, auth_token=None))
        msg = str(exc.value)
        assert "multiple deployment-config mismatches detected" in msg
        assert "ALMANAK_GATEWAY_DATABASE_URL is unset" in msg
        assert "ALMANAK_GATEWAY_ALLOW_INSECURE=true" in msg
        assert "ALMANAK_GATEWAY_AUTH_TOKEN is unset" in msg

    # ---- T-3760-10: invariants check runs BEFORE build_interceptors -------
    def test_t_3760_10_runs_before_build_interceptors_in_start(self) -> None:
        """Phase-0 invariants must execute before Phase-1 interceptors.

        Anti-gaming guard: a future refactor that reorders the bootstrap
        could move the invariant check after port bind / storage init,
        defeating its purpose. This test pins call order in the start()
        source so a regression is caught at PR-review time.
        """
        import inspect

        from almanak.gateway import server as server_mod

        src = inspect.getsource(server_mod.GatewayServer.start)
        invariant_pos = src.find("validate_deployment_invariants(")
        interceptors_pos = src.find("build_interceptors(")
        assert invariant_pos != -1, "start() no longer calls validate_deployment_invariants"
        assert interceptors_pos != -1, "start() no longer calls build_interceptors"
        assert invariant_pos < interceptors_pos, (
            "validate_deployment_invariants must run BEFORE build_interceptors. "
            f"Found invariant at offset {invariant_pos}, interceptors at {interceptors_pos}."
        )


# ---------------------------------------------------------------------------
# acquire_local_db_flock — VIB-5550 single-writer flock + utility fallback
#
# The RPC-level characterization test patches this helper out wholesale, so
# its branch selection (hosted no-op / strategy-pinned plain lock / standalone
# utility-fallback) and the PR #3213 review fix — propagating a fallback
# session DB into ``settings.gateway_db_path`` so the operational stores
# (timeline / registry / lifecycle) follow the state backend onto the session
# DB — are only covered here.
# ---------------------------------------------------------------------------
class TestAcquireLocalDbFlock:
    def _patch_lock_helpers(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        resolved,
        plain=None,
        fallback=None,
        hosted: bool = False,
    ) -> None:
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: hosted)
        monkeypatch.setattr(
            "almanak.framework.local_paths.warn_if_legacy_cwd_db_exists",
            MagicMock(),
        )
        # Never probe the developer's real legacy DB: it makes the suite
        # environment-dependent and injects a warning into caplog scopes.
        monkeypatch.setattr(
            "almanak.gateway.core.settings.DEFAULT_GATEWAY_DB_PATH",
            str(Path(resolved).parent / "absent-legacy-gateway.db"),
        )
        monkeypatch.setattr(
            "almanak.gateway._server_start_helpers.resolve_gateway_local_db_path",
            lambda _settings: resolved,
        )
        if plain is not None:
            monkeypatch.setattr("almanak.framework.local_paths.acquire_local_db_lock", plain)
        if fallback is not None:
            monkeypatch.setattr(
                "almanak.framework.local_paths.acquire_local_db_lock_with_utility_fallback",
                fallback,
            )

    def test_hosted_mode_is_noop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: True)
        assert acquire_local_db_flock(_settings(standalone=True)) is None

    def test_strategy_pinned_uses_plain_lock(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        resolved = tmp_path / "strat" / "almanak_state.db"
        plain = MagicMock(return_value=4242)
        fallback = MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        s = _settings(standalone=False)

        handle = acquire_local_db_flock(s)

        assert handle == 4242
        plain.assert_called_once_with(resolved)
        fallback.assert_not_called()
        assert s.gateway_db_path == str(resolved)

    def test_standalone_uncontended_pins_gateway_db_path(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        resolved = tmp_path / "utility" / "almanak_state.db"
        # No contention → the fallback returns the SAME (canonical) path.
        fallback = MagicMock(return_value=(777, resolved))
        plain = MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        s = _settings(standalone=True)

        handle = acquire_local_db_flock(s)

        assert handle == 777
        plain.assert_not_called()
        assert fallback.call_args.args[0] == resolved
        assert s.gateway_db_path == str(resolved)

    def test_standalone_fallback_pins_session_db(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        resolved = tmp_path / "utility" / "almanak_state.db"
        session = tmp_path / "utility" / "sessions" / "gw-1-abcd" / "almanak_state.db"
        fallback = MagicMock(return_value=(999, session))
        self._patch_lock_helpers(monkeypatch, resolved=resolved, fallback=fallback)
        s = _settings(standalone=True)

        handle = acquire_local_db_flock(s)

        assert handle == 999
        # Fallback fired (effective != resolved) ⇒ operational stores follow
        # the state backend onto the private session DB.
        assert s.gateway_db_path == str(session)

    @pytest.mark.parametrize("field", ["gateway_db_path", "timeline_db_path"])
    def test_explicit_mismatch_refused_before_lock(self, monkeypatch, tmp_path, field):
        resolved = tmp_path / "state.db"
        plain, fallback = MagicMock(), MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        settings = _settings(standalone=True, **{field: str(tmp_path / "other.db")})
        with pytest.raises(RuntimeError, match="must match the canonical state DB"):
            acquire_local_db_flock(settings)
        plain.assert_not_called()
        fallback.assert_not_called()
        assert not (tmp_path / "other.db").exists()

    @pytest.mark.parametrize("field", ["gateway_db_path", "timeline_db_path"])
    def test_explicit_matching_path_pins_utility_ownership(self, monkeypatch, tmp_path, field):
        resolved = tmp_path / "state.db"
        plain, fallback = MagicMock(return_value=42), MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        settings = _settings(standalone=True, **{field: str(resolved)})
        assert acquire_local_db_flock(settings) == 42
        plain.assert_called_once_with(resolved)
        fallback.assert_not_called()
        assert settings.gateway_db_path == str(resolved)

    def test_env_explicit_default_path_is_not_implicit(self, monkeypatch, tmp_path):
        from almanak.gateway.core.settings import DEFAULT_GATEWAY_DB_PATH

        monkeypatch.setenv("ALMANAK_GATEWAY_GATEWAY_DB_PATH", DEFAULT_GATEWAY_DB_PATH)
        settings = _settings(standalone=True)
        self._patch_lock_helpers(monkeypatch, resolved=tmp_path / "state.db")
        with pytest.raises(RuntimeError, match="gateway_db_path must match"):
            acquire_local_db_flock(settings)

    def test_blank_timeline_override_is_treated_as_unset(self, monkeypatch, tmp_path):
        """Whitespace is unset: flock must not pin it, and the store must follow the lock."""
        resolved = tmp_path / "state.db"
        plain, fallback = MagicMock(), MagicMock(return_value=(5, resolved))
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        settings = _settings(standalone=True, timeline_db_path="   ")

        assert acquire_local_db_flock(settings) == 5

        fallback.assert_called_once()
        plain.assert_not_called()
        # Cleared, not filled in: a rewritten value reads as an operator pin
        # on the next boot, and leftover whitespace is a truthy second path.
        assert settings.timeline_db_path is None

        factory = MagicMock()
        initialize_timeline_store(settings, factory)
        factory.assert_called_once_with(db_path=str(resolved))

        assert acquire_local_db_flock(settings) == 5

        assert fallback.call_count == 2
        plain.assert_not_called()

    def test_second_boot_on_the_same_settings_keeps_the_utility_fallback(self, monkeypatch, tmp_path):
        """The path write-back must not read back as an operator pin.

        Plain attribute assignment adds the field to ``model_fields_set``, which
        is the same signal the guard uses to detect an operator-pinned path.
        """
        resolved = tmp_path / "utility" / "almanak_state.db"
        fallback = MagicMock(return_value=(11, resolved))
        plain = MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        settings = _settings(standalone=True)

        acquire_local_db_flock(settings)
        acquire_local_db_flock(settings)

        assert fallback.call_count == 2
        plain.assert_not_called()

    def test_populated_legacy_gateway_db_is_named_at_boot(self, monkeypatch, tmp_path, caplog):
        """Orphaned operational history must be announced, not silently dropped."""
        import sqlite3

        resolved = tmp_path / "state.db"
        self._patch_lock_helpers(monkeypatch, resolved=resolved, fallback=MagicMock(return_value=(3, resolved)))
        legacy = tmp_path / "legacy-gateway.db"
        with sqlite3.connect(legacy) as conn:
            conn.execute("CREATE TABLE strategy_instances (deployment_id TEXT)")
            conn.execute("INSERT INTO strategy_instances VALUES ('deployment:old')")
            conn.commit()
        monkeypatch.setattr("almanak.gateway.core.settings.DEFAULT_GATEWAY_DB_PATH", str(legacy))

        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            acquire_local_db_flock(_settings(standalone=True))

        assert str(legacy) in caplog.text
        assert "no longer served" in caplog.text

    def test_empty_legacy_gateway_db_is_not_announced(self, monkeypatch, tmp_path, caplog):
        """An empty legacy file is not orphaned history; warning on it is noise."""
        import sqlite3

        resolved = tmp_path / "state.db"
        self._patch_lock_helpers(monkeypatch, resolved=resolved, fallback=MagicMock(return_value=(3, resolved)))
        legacy = tmp_path / "legacy-gateway.db"
        with sqlite3.connect(legacy) as conn:
            conn.execute("CREATE TABLE strategy_instances (deployment_id TEXT)")
            conn.commit()
        monkeypatch.setattr("almanak.gateway.core.settings.DEFAULT_GATEWAY_DB_PATH", str(legacy))

        with caplog.at_level(logging.WARNING, logger="almanak.gateway._server_start_helpers"):
            acquire_local_db_flock(_settings(standalone=True))

        assert "no longer served" not in caplog.text

    def test_sequential_gateways_bind_their_own_databases(self, monkeypatch, tmp_path):
        """Once released, the initializers rebind to the new gateway's path.

        This is the half of the contract that ``stop()`` depends on; that
        ``stop()`` performs the release is covered separately.
        """
        from almanak.gateway.registry import reset_instance_registry
        from almanak.gateway.timeline.store import reset_timeline_store

        first, second = tmp_path / "a.db", tmp_path / "b.db"
        reset_instance_registry()
        reset_timeline_store()
        try:
            settings_a = _settings()
            settings_a.__dict__["gateway_db_path"] = str(first)
            assert initialize_instance_registry(settings_a).db_path == first

            reset_instance_registry()
            reset_timeline_store()

            settings_b = _settings()
            settings_b.__dict__["gateway_db_path"] = str(second)
            assert initialize_instance_registry(settings_b).db_path == second
        finally:
            reset_instance_registry()
            reset_timeline_store()

    @pytest.mark.asyncio
    async def test_stop_releases_the_pinned_store_singletons(self, monkeypatch):
        """``stop()`` is the only place those singletons can be released."""
        from almanak.gateway import server as server_module

        called = []
        for name in ("reset_lifecycle_store", "reset_instance_registry", "reset_timeline_store"):
            monkeypatch.setattr(server_module, name, lambda n=name: called.append(n))

        gateway = server_module.GatewayServer(_settings())

        await gateway.stop()

        assert called == ["reset_lifecycle_store", "reset_instance_registry", "reset_timeline_store"]

    @pytest.mark.asyncio
    async def test_stop_actually_clears_the_pinned_store_singletons(self, tmp_path):
        """Run the real resets, not stand-ins.

        Asserting only that ``stop()`` calls three names would still pass if a
        reset were a broken no-op, and it is the cleared singleton -- not the
        call -- that stops the next gateway inheriting this strategy's stores.
        """
        from almanak.gateway import registry as registry_module
        from almanak.gateway import server as server_module
        from almanak.gateway.registry import store as registry_store
        from almanak.gateway.timeline import store as timeline_store

        registry_module.reset_instance_registry()
        timeline_store.reset_timeline_store()
        try:
            registry_module.get_instance_registry(db_path=tmp_path / "a.db")
            timeline_store.get_timeline_store(db_path=tmp_path / "a.db")
            assert registry_store._instance_registry is not None
            assert timeline_store._timeline_store is not None

            await server_module.GatewayServer(_settings()).stop()

            assert registry_store._instance_registry is None
            assert timeline_store._timeline_store is None

            # And the next gateway's initializer binds its own file.
            second = _settings()
            second.__dict__["gateway_db_path"] = str(tmp_path / "b.db")
            assert initialize_instance_registry(second).db_path == tmp_path / "b.db"
        finally:
            registry_module.reset_instance_registry()
            timeline_store.reset_timeline_store()

    @pytest.mark.asyncio
    async def test_stop_releases_the_flock_even_when_a_reset_raises(self, monkeypatch):
        """A stranded flock locks the next gateway out for the life of the process."""
        from almanak.gateway import server as server_module

        called = []
        failing = MagicMock(side_effect=RuntimeError("store close failed"))
        monkeypatch.setattr(server_module, "reset_lifecycle_store", failing)
        monkeypatch.setattr(server_module, "reset_instance_registry", lambda: called.append("registry"))
        monkeypatch.setattr(server_module, "reset_timeline_store", lambda: called.append("timeline"))
        released = MagicMock()
        monkeypatch.setattr("almanak.framework.local_paths.release_local_db_lock", released)

        gateway = server_module.GatewayServer(_settings())
        gateway._local_db_lock = 4242

        await gateway.stop()

        # The raising reset was attempted, not skipped over.
        failing.assert_called_once_with()
        assert called == ["registry", "timeline"]
        released.assert_called_once_with(4242)
        assert gateway._local_db_lock is None

    def test_hosted_explicit_paths_unchanged(self, monkeypatch):
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: True)
        settings = _settings(gateway_db_path="/a.db", timeline_db_path="/b.db")
        assert acquire_local_db_flock(settings) is None
        assert settings.gateway_db_path == "/a.db"
        assert settings.timeline_db_path == "/b.db"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("standalone", [False, True])
    async def test_real_registry_startup_cannot_reconcile_other_database(self, monkeypatch, tmp_path, standalone):
        import sqlite3
        from datetime import UTC, datetime

        from almanak.framework.local_paths import release_local_db_lock
        from almanak.gateway.registry.store import InstanceRegistry, StrategyInstance

        other = tmp_path / "legacy-gateway.db"
        owned = tmp_path / "owned.db"
        registry = InstanceRegistry(other)
        now = datetime.now(UTC)
        registry.register(
            StrategyInstance(
                deployment_id="deployment:other",
                strategy_name="other",
                template_name="Other",
                chain="base",
                protocol="uniswap_v3",
                wallet_address="0x0000000000000000000000000000000000000001",
                config_json="{}",
                chains="base",
                chain_wallets="{}",
                status="RUNNING",
                archived=False,
                created_at=now,
                updated_at=now,
                last_heartbeat_at=now,
                version="test",
            )
        )
        with sqlite3.connect(other) as conn:
            before = conn.execute("SELECT * FROM strategy_instances").fetchall()
        monkeypatch.setenv("ALMANAK_STATE_DB", str(owned))
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: False)
        monkeypatch.setattr("almanak.gateway.registry.get_instance_registry", lambda db_path: InstanceRegistry(db_path))
        settings = _settings(standalone=standalone)
        # Stand in for the legacy default without ever opening a user's real DB.
        settings.__dict__["gateway_db_path"] = str(other)
        handle = acquire_local_db_flock(settings)
        try:
            from almanak.gateway.lifecycle.sqlite_store import SQLiteLifecycleStore
            from almanak.gateway.timeline.store import TimelineStore

            monkeypatch.setattr(
                "almanak.gateway._server_start_helpers.get_lifecycle_store",
                lambda **kwargs: SQLiteLifecycleStore(db_path=kwargs["sqlite_path"]),
            )
            lifecycle = initialize_lifecycle_store(settings)
            lifecycle.initialize()
            lifecycle.close()
            from almanak.gateway._server_start_helpers import validate_state_schema_at_boot
            from almanak.gateway.timeline.store import TimelineEvent

            # Build the store from the path the helper hands the factory, so the
            # assertions below fail if routing regresses to the legacy DB.
            built: dict[str, TimelineStore] = {}

            def _timeline_factory(**kwargs):
                built["store"] = TimelineStore(db_path=Path(kwargs["db_path"]))
                built["store"].initialize()

            initialize_timeline_store(settings, _timeline_factory)
            timeline = built["store"]
            assert timeline._db_path == owned
            timeline.add_event(TimelineEvent("first", "deployment:owned", now, "STATE_CHANGE", "running"))
            for _ in range(2):
                await validate_state_schema_at_boot(settings)
                with sqlite3.connect(owned) as conn:
                    assert conn.execute("SELECT description FROM timeline_events").fetchall() == [("running",)]
            timeline.add_event(TimelineEvent("second", "deployment:owned", now, "STATE_CHANGE", "still running"))
            actual = initialize_instance_registry(settings)
            with sqlite3.connect(owned) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                assert "timeline_events" in tables
                assert "strategy_instances" in tables
            assert actual.db_path == owned
            assert actual.list_all() == []
            with sqlite3.connect(other) as conn:
                assert conn.execute("SELECT * FROM strategy_instances").fetchall() == before
        finally:
            release_local_db_lock(handle)

    def test_real_utility_contention_falls_back_without_touching_owner(self, monkeypatch, tmp_path):
        from almanak.framework import local_paths

        for name in ("ALMANAK_STATE_DB", "ALMANAK_STRATEGY_FOLDER", "ALMANAK_GATEWAY_DB_PATH"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setattr(local_paths, "_utility_data_dir", lambda: tmp_path / "utility")
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: False)
        owned = local_paths.local_db_path()
        first = local_paths.acquire_local_db_lock(owned)
        fallback = local_paths.acquire_local_db_lock_with_utility_fallback
        monkeypatch.setattr(
            local_paths,
            "acquire_local_db_lock_with_utility_fallback",
            lambda path, logger: fallback(path, logger, retry_budget_s=0),
        )
        settings = _settings(standalone=True)
        second = None
        try:
            second = acquire_local_db_flock(settings)
            assert settings.gateway_db_path != str(owned)
            assert settings.gateway_db_path == str(local_paths.local_db_path())
            with pytest.raises(local_paths.LocalDbLockError):
                local_paths.acquire_local_db_lock(owned)
            with pytest.raises(local_paths.LocalDbLockError):
                local_paths.acquire_local_db_lock(local_paths.local_db_path())
        finally:
            local_paths.release_local_db_lock(second)
            local_paths.release_local_db_lock(first)

    def test_symlinked_utility_dir_still_falls_back(self, monkeypatch, tmp_path):
        """The utility fallback keys on the resolver's own unresolved path form.

        Collapsing symlinks before the lock call makes the path stop matching
        ``utility_db_path()``, which reads as an operator-pinned path and
        hard-fails every concurrent standalone session instead of falling back.
        """
        from almanak.framework import local_paths

        for name in ("ALMANAK_STATE_DB", "ALMANAK_STRATEGY_FOLDER", "ALMANAK_GATEWAY_DB_PATH"):
            monkeypatch.delenv(name, raising=False)
        real = tmp_path / "real"
        real.mkdir()
        (tmp_path / "linked").symlink_to(real)
        monkeypatch.setattr(local_paths, "_utility_data_dir", lambda: tmp_path / "linked" / "utility")
        monkeypatch.setattr("almanak.framework.deployment.is_hosted", lambda: False)
        owned = local_paths.local_db_path()
        assert owned.resolve() != owned
        first = local_paths.acquire_local_db_lock(owned)
        fallback = local_paths.acquire_local_db_lock_with_utility_fallback
        monkeypatch.setattr(
            local_paths,
            "acquire_local_db_lock_with_utility_fallback",
            lambda path, logger: fallback(path, logger, retry_budget_s=0),
        )
        second = None
        try:
            second = acquire_local_db_flock(_settings(standalone=True))
            assert second is not None
        finally:
            local_paths.release_local_db_lock(second)
            local_paths.release_local_db_lock(first)

    def test_explicit_symlinked_path_matching_canonical_is_accepted(self, monkeypatch, tmp_path):
        """Path comparison collapses symlinks, so the same file spelled two ways matches."""
        real = tmp_path / "real"
        real.mkdir()
        (tmp_path / "linked").symlink_to(real)
        resolved = tmp_path / "linked" / "state.db"
        plain, fallback = MagicMock(return_value=7), MagicMock()
        self._patch_lock_helpers(monkeypatch, resolved=resolved, plain=plain, fallback=fallback)
        settings = _settings(standalone=True, gateway_db_path=str(real / "state.db"))

        assert acquire_local_db_flock(settings) == 7

        plain.assert_called_once_with(resolved)
        fallback.assert_not_called()
        assert settings.gateway_db_path == str(resolved)


# ---------------------------------------------------------------------------
# timeline_startup_load_limit validation (PR 3560 review, P2)
# ---------------------------------------------------------------------------
class TestTimelineStartupLoadLimitValidation:
    """Non-positive or malformed values fall back to the default (10000),
    matching the history-cache cap validators: a typo must not boot a
    gateway with an empty timeline cache or a Postgres-rejected query."""

    def test_default(self) -> None:
        assert _settings().timeline_startup_load_limit == 10000

    def test_valid_override(self) -> None:
        assert _settings(timeline_startup_load_limit=500).timeline_startup_load_limit == 500

    def test_zero_falls_back_to_default(self) -> None:
        assert _settings(timeline_startup_load_limit=0).timeline_startup_load_limit == 10000

    def test_negative_falls_back_to_default(self) -> None:
        assert _settings(timeline_startup_load_limit=-5).timeline_startup_load_limit == 10000

    def test_malformed_falls_back_to_default(self) -> None:
        assert _settings(timeline_startup_load_limit="junk").timeline_startup_load_limit == 10000

    def test_env_var_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_GATEWAY_TIMELINE_STARTUP_LOAD_LIMIT", "0")
        assert _settings().timeline_startup_load_limit == 10000
        monkeypatch.setenv("ALMANAK_GATEWAY_TIMELINE_STARTUP_LOAD_LIMIT", "2500")
        assert _settings().timeline_startup_load_limit == 2500
