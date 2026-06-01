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
from unittest.mock import MagicMock

import pytest

from almanak.gateway._server_start_helpers import (
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
        factory.assert_called_once_with(database_url="postgres://x/y")

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
