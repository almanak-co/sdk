"""Unit tests for the Phase 4b helpers in `almanak/framework/cli/run_helpers.py`.

Covers:
    - _setup_gateway
    - _wire_token_resolver
    - _resolve_identity
    - _detect_state_resume

Pattern follows `test_run_helpers_setup.py` (Phase 4a): CliRunner + monkeypatch,
with on-disk SQLite for the identity/resume helpers. We stub ManagedGateway,
GatewayClient, and the identity-resolution utilities so tests never start a
real gateway or RPC connection.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from almanak.framework.cli._run_context import IdentityInfo, ResumeInfo

# ---------------------------------------------------------------------------
# Fakes for gateway tests
# ---------------------------------------------------------------------------


class _FakeChannel:
    """Stand-in for a gRPC channel."""


class _FakeGatewayClient:
    last: _FakeGatewayClient | None = None

    def __init__(self, config: Any) -> None:
        self.config = config
        self.connected = False
        self.disconnected = False
        self.channel = _FakeChannel()
        self.wait_for_ready_return = True
        _FakeGatewayClient.last = self

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.disconnected = True

    def wait_for_ready(self, timeout: float, interval: float) -> bool:
        return self.wait_for_ready_return


class _FakeManagedGateway:
    last: _FakeManagedGateway | None = None
    start_should_fail = False
    # Match the real ManagedGateway class attributes — the _anvil_timeout
    # helper sources the slow-chain set from the gateway, so the fake must
    # expose it for TestArchiveChainStartupTimeout below to keep working.
    #
    # VIB-5869 split these apart: ARCHIVE_RPC_REQUIRED_CHAINS is now the
    # safety gate (which chains refuse to fork without an archive RPC) while
    # COLD_START_SLOW_CHAINS drives the startup budget. The budget tests below
    # follow the latter.
    ARCHIVE_RPC_REQUIRED_CHAINS = frozenset({"polygon", "ethereum", "avalanche"})
    COLD_START_SLOW_CHAINS = frozenset({"polygon", "ethereum", "avalanche"})

    def __init__(
        self,
        settings: Any,
        anvil_chains: list[str],
        wallet_address: str | None,
        anvil_funding: dict[str, Any],
        external_anvil_ports: dict[str, int],
        keep_anvil: bool,
    ) -> None:
        self.settings = settings
        self.anvil_chains = anvil_chains
        self.wallet_address = wallet_address
        self.anvil_funding = anvil_funding
        self.external_anvil_ports = external_anvil_ports
        self.keep_anvil = keep_anvil
        self.started = False
        self.stopped = False
        _FakeManagedGateway.last = self

    def start(self, timeout: float) -> None:
        if _FakeManagedGateway.start_should_fail:
            raise RuntimeError("fake start failure")
        self.started = True
        self.start_timeout = timeout

    def stop(self) -> None:
        self.stopped = True


def _install_gateway_fakes(monkeypatch: pytest.MonkeyPatch, *, port: int = 50051) -> None:
    """Monkeypatch the gateway-facing imports used by `_setup_gateway`.

    This intercepts the deferred imports inside the helper so tests do not
    start real subprocesses or open real gRPC channels.

    Phase 1 (config-service plan): the helper now constructs gateway settings
    via :func:`almanak.config.env.gateway_config_from_env` instead of
    ``GatewaySettings(...)`` directly, so the fake replaces the boundary
    helper rather than the pydantic class.
    """
    from almanak.config import env as cfg_env
    from almanak.framework import gateway_client as gw_client_pkg
    from almanak.gateway import managed as gw_managed

    class _FakeSettings:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            # Mirror pydantic-settings + ``_apply_gateway_env_fallbacks`` for the
            # fields the production helpers read off ``GatewayConfig``. Each
            # attribute is presence-checked first (so an explicit empty-string
            # kwarg is preserved verbatim, matching the documented force-empty
            # override pattern), otherwise sourced from the corresponding env var.
            if "private_key" in kwargs and kwargs["private_key"] is not None:
                self.private_key = kwargs["private_key"]
            else:
                self.private_key = os.environ.get("ALMANAK_PRIVATE_KEY", "")
            # ``auth_token`` is read by ``_attach_external_gateway`` (PR
            # #2152 narrowed it to ``gateway_config_from_env`` so a malformed
            # unrelated submodel doesn't block ``--no-gateway`` startup).
            # The pydantic prefix is ``ALMANAK_GATEWAY_AUTH_TOKEN``.
            if "auth_token" in kwargs and kwargs["auth_token"] is not None:
                self.auth_token = kwargs["auth_token"]
            else:
                self.auth_token = os.environ.get("ALMANAK_GATEWAY_AUTH_TOKEN")
            # ``solana_private_key`` is read by ``_resolve_effective_signing_key``
            # for Solana strategies. Pydantic prefix gives
            # ``ALMANAK_GATEWAY_SOLANA_PRIVATE_KEY``; the unprefixed
            # ``SOLANA_PRIVATE_KEY`` fallback is applied by
            # ``_apply_gateway_env_fallbacks``.
            if "solana_private_key" in kwargs and kwargs["solana_private_key"] is not None:
                self.solana_private_key = kwargs["solana_private_key"]
            else:
                self.solana_private_key = os.environ.get("SOLANA_PRIVATE_KEY", "")

    monkeypatch.setattr(gw_managed, "ManagedGateway", _FakeManagedGateway)
    monkeypatch.setattr(gw_managed, "find_available_gateway_port", lambda host, port: port)
    monkeypatch.setattr(cfg_env, "gateway_config_from_env", _FakeSettings)

    monkeypatch.setattr(gw_client_pkg, "GatewayClient", _FakeGatewayClient)

    _FakeGatewayClient.last = None
    _FakeManagedGateway.last = None
    _FakeManagedGateway.start_should_fail = False


# ---------------------------------------------------------------------------
# _setup_gateway
# ---------------------------------------------------------------------------


class TestSetupGatewayExternal:
    """`--no-gateway` path: connect to an existing gateway, no ManagedGateway."""

    def test_external_happy_path_returns_none_managed_gateway(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        runner = CliRunner()
        with runner.isolation():
            (
                gw_client,
                managed,
                effective_host,
                gw_port,
                gw_network,
                token,
                isolated_wallet,
                early_cls,
            ) = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="localhost",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert managed is None
        assert gw_client is _FakeGatewayClient.last
        assert gw_client.connected is True
        # "localhost" is normalized to 127.0.0.1.
        assert effective_host == "127.0.0.1"
        assert gw_port == 50051
        assert gw_network == "mainnet"
        # No session auth token, isolated wallet, or derived key in external mode.
        assert token is None
        assert isolated_wallet is None
        # No derived key plumbed when no_gateway + no caller-plumbed key.
        assert run_helpers._runtime_private_key_override.get() is None
        assert early_cls is None

    def test_external_reads_auth_token_from_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _install_gateway_fakes(monkeypatch)
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "primary-token")
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "fallback-token")
        runner = CliRunner()
        with runner.isolation():
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert _FakeGatewayClient.last is not None
        # Primary env var wins over fallback.
        assert _FakeGatewayClient.last.config.auth_token == "primary-token"

    def test_external_falls_back_to_gateway_auth_token(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _install_gateway_fakes(monkeypatch)
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "legacy-token")
        runner = CliRunner()
        with runner.isolation():
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert _FakeGatewayClient.last is not None
        assert _FakeGatewayClient.last.config.auth_token == "legacy-token"

    def test_external_health_check_failure_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _install_gateway_fakes(monkeypatch)

        # Patch wait_for_ready on the fake class BEFORE instantiation.
        original_wait = _FakeGatewayClient.wait_for_ready

        def always_false(self: _FakeGatewayClient, timeout: float, interval: float) -> bool:
            return False

        monkeypatch.setattr(_FakeGatewayClient, "wait_for_ready", always_false)
        try:
            runner = CliRunner()
            with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
                run_helpers._setup_gateway(
                    working_dir=str(tmp_path),
                    config_file=None,
                    network=None,
                    gateway_host="127.0.0.1",
                    gateway_port=50051,
                    no_gateway=True,
                    anvil_ports=(),
                    wallet="default",
                    keep_anvil=False,
                    reset_fork=False,
                    once=False,
                )
            assert "Gateway not available" in exc_info.value.message
        finally:
            monkeypatch.setattr(_FakeGatewayClient, "wait_for_ready", original_wait)

    def test_external_rejects_anvil_port(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _install_gateway_fakes(monkeypatch)
        with pytest.raises(click.ClickException, match="--anvil-port requires a managed gateway"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=("arbitrum=8545",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_external_rejects_keep_anvil(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _install_gateway_fakes(monkeypatch)
        with pytest.raises(click.ClickException, match="--keep-anvil requires a managed gateway"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=(),
                wallet="default",
                keep_anvil=True,
                reset_fork=False,
                once=False,
            )

    def test_external_rejects_isolated_wallet(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _install_gateway_fakes(monkeypatch)
        with pytest.raises(click.ClickException, match="--wallet isolated requires a managed gateway"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=True,
                anvil_ports=(),
                wallet="isolated",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )


class TestSetupGatewayManaged:
    """Managed gateway path (default, no_gateway=False)."""

    def _write_config(self, working_dir: Path, chain: str) -> None:
        (working_dir / "config.json").write_text(json.dumps({"chain": chain}))

    def test_managed_happy_path_starts_and_registers_atexit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        atexit_calls: list[Any] = []

        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda fn, *a, **kw: atexit_calls.append((fn, a, kw)))
        runner = CliRunner()
        with runner.isolation():
            (
                gw_client,
                managed,
                effective_host,
                gw_port,
                gw_network,
                token,
                isolated_wallet,
                early_cls,
            ) = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="localhost",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert managed is _FakeManagedGateway.last
        assert managed is not None
        assert managed.started is True
        assert effective_host == "127.0.0.1"
        assert gw_network == "mainnet"
        # Mainnet gets a session auth token (VIB-520); test networks do not.
        assert token is not None and len(token) == 32
        # atexit registered managed_gateway.stop.
        assert any(call[0] == managed.stop for call in atexit_calls)

    def test_managed_anvil_network_uses_no_auth_token(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        self._write_config(tmp_path, "arbitrum")
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        runner = CliRunner()
        with runner.isolation():
            _, _, _, _, gw_network, token, _, _ = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert gw_network == "anvil"
        # Test networks get no session token.
        assert token is None

    def test_anvil_ports_without_network_flag_switches_to_anvil(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        runner = CliRunner()
        with runner.isolation():
            _, _, _, _, gw_network, _, _, _ = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("arbitrum=8545",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert gw_network == "anvil"
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.external_anvil_ports == {"arbitrum": 8545}

    def test_anvil_ports_invalid_format_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="Invalid --anvil-port format"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("no-equals",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_anvil_ports_out_of_range_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="Expected 1-65535"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("arbitrum=99999",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_anvil_ports_duplicate_chain_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="Duplicate --anvil-port"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("arbitrum=8545", "arbitrum=8546"),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_isolated_wallet_requires_anvil_network(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0x" + "11" * 32)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="only supported with --network anvil"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="mainnet",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="isolated",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_isolated_wallet_derives_unique_key_per_strategy(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        from almanak.gateway import managed as gw_managed

        captured_seeds: list[str] = []

        def fake_derive(master_key: str, seed: str) -> tuple[str, str]:
            captured_seeds.append(seed)
            return ("0x" + "ab" * 32, "0x" + "cd" * 20)

        monkeypatch.setattr(gw_managed, "derive_isolated_wallet", fake_derive)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0x" + "11" * 32)
        self._write_config(tmp_path, "arbitrum")
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        runner = CliRunner()
        strategy_dir = tmp_path / "my_strategy"
        strategy_dir.mkdir()
        self._write_config(strategy_dir, "arbitrum")
        # Reset the contextvar so a stale value from a prior test cannot
        # masquerade as the derived key. Belt-and-braces with the autouse
        # conftest fixture: explicit cleanup at the end of this test means
        # an assertion failure mid-test still leaves the contextvar in a
        # known state, independent of fixture teardown order.
        run_helpers._runtime_private_key_override.set(None)
        try:
            with runner.isolation():
                (
                    _,
                    _,
                    _,
                    _,
                    _,
                    _,
                    isolated_wallet,
                    _,
                ) = run_helpers._setup_gateway(
                    working_dir=str(strategy_dir),
                    config_file=None,
                    network="anvil",
                    gateway_host="127.0.0.1",
                    gateway_port=50051,
                    no_gateway=False,
                    anvil_ports=(),
                    wallet="isolated",
                    keep_anvil=False,
                    reset_fork=False,
                    once=False,
                )
            assert isolated_wallet == "0x" + "cd" * 20
            # Seed is the strategy directory name.
            assert captured_seeds == ["my_strategy"]
            # The derived key is plumbed downstream via the ContextVar so
            # `_build_runtime_config` signs from the same identity (#2100).
            assert run_helpers._runtime_private_key_override.get() == "0x" + "ab" * 32
            # ALMANAK_PRIVATE_KEY remains the master key — unchanged by _setup_gateway.
            import os

            assert os.environ["ALMANAK_PRIVATE_KEY"] == "0x" + "11" * 32
        finally:
            run_helpers._runtime_private_key_override.set(None)

    def test_isolated_wallet_without_master_key_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="--wallet isolated requires a private key"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="isolated",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_reset_fork_requires_anvil_network(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="--reset-fork is only supported"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="mainnet",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=True,
                once=False,
            )

    def test_start_failure_raises_click_exception_and_does_not_register_atexit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        _FakeManagedGateway.start_should_fail = True
        atexit_calls: list[Any] = []
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda fn, *a, **kw: atexit_calls.append(fn))
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException, match="startup failed"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        # Nothing was registered because start failed before atexit.register.
        assert atexit_calls == []


class TestSetupGatewayQuickConfigRobustness:
    """Quick-load config handling at gateway boot (#2101).

    `_setup_gateway` reads the strategy's config file early to discover which
    chains need Anvil forks. Since #2101 this early probe shares the SINGLE
    validated parse (`parse_strategy_config_file`) used by the canonical runner
    loader, so a malformed or schema-invalid config now FAILS FAST with a
    `click.ClickException` naming the file — the same error the runner would
    raise moments later — instead of being silently swallowed into a misleading
    "no chain found" warning. An empty config (empty YAML -> None -> {}) is
    valid and still boots with no Anvil forks. These tests pin that contract.
    """

    def test_empty_yaml_config_does_not_crash(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """yaml.safe_load('') -> None; must not raise AttributeError on .get()."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.yaml").write_text("")  # empty file -> yaml.safe_load returns None
        runner = CliRunner()
        with runner.isolation():
            _, _, _, _, gw_network, _, _, _ = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert gw_network == "anvil"
        # No chain discovered -> ManagedGateway gets an empty anvil_chains list.
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.anvil_chains == []

    def test_chains_as_single_string_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """`chains: arbitrum` (a string scalar) is a schema violation — fail fast, don't iterate chars.

        Since #2101 the probe shares the canonical validated parse, so a bare
        string for the list-typed `chains` field raises the same schema error
        the runner would — early and naming the file — rather than being
        silently wrapped only at the probe while the runner rejects it.
        """
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": "arbitrum"}))
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert "config.json" in exc_info.value.message
        assert "schema validation" in exc_info.value.message

    def test_malformed_json_config_fails_fast(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Malformed JSON fails fast with a file-naming error, not a swallowed empty parse (#2101)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        # Truncated JSON - json.load will raise.
        (tmp_path / "config.json").write_text("{chain:")
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert "config.json" in exc_info.value.message
        assert "Failed to read strategy config" in exc_info.value.message

    def test_chains_as_int_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A numeric `chains` value is a schema violation — fail fast, don't silently drop it."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": 5}))
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert "schema validation" in exc_info.value.message

    def test_chains_as_dict_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A dict `chains` is a schema violation — fail fast, never coerce keys (the list({"base":1}) footgun)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": {"base": 1}}))
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        # Critical: the dict is REJECTED, never coerced to ["base"].
        assert "schema validation" in exc_info.value.message

    def test_chains_as_list_is_preserved(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Happy-path regression: list `chains` values flow through unchanged."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": ["arbitrum", "base"]}))
        runner = CliRunner()
        with runner.isolation():
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.anvil_chains == ["arbitrum", "base"]

    def test_mainnet_probe_malformed_json_fails_fast(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """The second probe (mainnet chain detection) also fails fast on a malformed config (#2101).

        The second probe runs when `anvil_chains` is empty (typically on mainnet)
        to seed `gateway_chains`. Like the Anvil probe it shares the validated
        parse, so a malformed config raises a file-naming error here too rather
        than silently seeding an empty chain list.
        """
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text("{chain:")
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,  # -> mainnet, exercises probe 2
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert "Failed to read strategy config" in exc_info.value.message

    def test_mainnet_probe_chains_as_dict_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Second probe also rejects a dict `chains` via schema validation rather than coercing keys."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": {"base": 1}}))
        runner = CliRunner()
        with runner.isolation(), pytest.raises(click.ClickException) as exc_info:
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,  # mainnet -> probe 2
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert "schema validation" in exc_info.value.message


class TestNormalizeQuickChains:
    """Unit tests for the `_normalize_quick_chains` helper itself."""

    def test_string_wraps_into_single_element_list(self) -> None:
        assert run_helpers._normalize_quick_chains("arbitrum") == ["arbitrum"]

    def test_list_of_strings_is_returned_as_list(self) -> None:
        assert run_helpers._normalize_quick_chains(["arbitrum", "base"]) == ["arbitrum", "base"]

    def test_list_with_non_strings_is_stringified(self) -> None:
        # Defensive stringification: YAML can parse unquoted values as ints.
        assert run_helpers._normalize_quick_chains([1, "base"]) == ["1", "base"]

    def test_none_returns_empty_list(self) -> None:
        assert run_helpers._normalize_quick_chains(None) == []

    def test_int_returns_empty_list(self) -> None:
        assert run_helpers._normalize_quick_chains(5) == []

    def test_dict_returns_empty_list_not_keys(self) -> None:
        # This is the key bug guard: list({"base": 1}) == ["base"] would be wrong.
        assert run_helpers._normalize_quick_chains({"base": 1}) == []


# ---------------------------------------------------------------------------
# _wire_token_resolver
# ---------------------------------------------------------------------------


class TestWireTokenResolver:
    def test_wires_channel_into_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        class _FakeResolver:
            def set_gateway_channel(self, channel: Any) -> None:
                captured["channel"] = channel

        fake_resolver = _FakeResolver()
        from almanak.framework.data import tokens

        monkeypatch.setattr(tokens, "get_token_resolver", lambda: fake_resolver)

        channel_sentinel = _FakeChannel()
        fake_client = type("X", (), {"channel": channel_sentinel})()
        run_helpers._wire_token_resolver(fake_client)

        assert captured["channel"] is channel_sentinel


# ---------------------------------------------------------------------------
# _detect_state_resume
# ---------------------------------------------------------------------------


def _make_state_db(path: Path) -> None:
    # strategy_state keys on the canonical deployment_id column (blueprint 29).
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE strategy_state (
                deployment_id TEXT PRIMARY KEY,
                version INTEGER,
                state_data TEXT
            )
            """
        )


class TestDetectStateResume:
    def test_missing_db_file_returns_fresh(self, tmp_path: Path) -> None:
        info = run_helpers._detect_state_resume(tmp_path / "does-not-exist.db", "strat-1")
        assert info == ResumeInfo(is_resume=False, version=None, state_keys=[])

    def test_missing_row_returns_fresh(self, tmp_path: Path) -> None:
        db = tmp_path / "state.db"
        _make_state_db(db)
        info = run_helpers._detect_state_resume(db, "strat-1")
        assert info.is_resume is False
        assert info.version is None
        assert info.state_keys == []

    def test_existing_row_populates_state_keys(self, tmp_path: Path) -> None:
        db = tmp_path / "state.db"
        _make_state_db(db)
        with sqlite3.connect(str(db)) as conn:
            conn.execute(
                "INSERT INTO strategy_state (deployment_id, version, state_data) VALUES (?, ?, ?)",
                ("strat-1", 3, json.dumps({"last_trade": "2024-01-01", "position_id": 42})),
            )
        info = run_helpers._detect_state_resume(db, "strat-1")
        assert info.is_resume is True
        assert info.version == 3
        assert set(info.state_keys) == {"last_trade", "position_id"}

    def test_empty_state_data_returns_empty_keys(self, tmp_path: Path) -> None:
        db = tmp_path / "state.db"
        _make_state_db(db)
        with sqlite3.connect(str(db)) as conn:
            conn.execute(
                "INSERT INTO strategy_state (deployment_id, version, state_data) VALUES (?, ?, ?)",
                ("strat-1", 1, ""),
            )
        info = run_helpers._detect_state_resume(db, "strat-1")
        assert info.is_resume is True
        assert info.version == 1
        assert info.state_keys == []

    def test_corrupt_json_swallowed_still_reports_resume(self, tmp_path: Path) -> None:
        db = tmp_path / "state.db"
        _make_state_db(db)
        with sqlite3.connect(str(db)) as conn:
            conn.execute(
                "INSERT INTO strategy_state (deployment_id, version, state_data) VALUES (?, ?, ?)",
                ("strat-1", 7, "not-valid-json"),
            )
        info = run_helpers._detect_state_resume(db, "strat-1")
        assert info.is_resume is True
        assert info.version == 7
        assert info.state_keys == []

    def test_corrupt_schema_swallows_and_returns_fresh(self, tmp_path: Path) -> None:
        db = tmp_path / "state.db"
        # Create a DB with the wrong schema — helper should not raise.
        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE strategy_state (id INTEGER)")
        info = run_helpers._detect_state_resume(db, "strat-1")
        assert info.is_resume is False


# ---------------------------------------------------------------------------
# _resolve_identity
# ---------------------------------------------------------------------------


def _install_identity_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    deployment_id: str,
    run_id: str = "run-xyz",
) -> list[dict[str, Any]]:
    """Stub out the identity resolver + run_id generator.

    VIB-4722: ``resolve_deployment_id`` now takes only ``wallet_address`` and
    ``chain`` (no ``strategy_name``, no ``cli_id``).
    """
    from almanak.framework.runner import identity as identity_mod

    captured: list[dict[str, Any]] = []

    def fake_resolve(**kwargs: Any) -> str:
        captured.append(kwargs)
        return deployment_id

    monkeypatch.setattr(identity_mod, "resolve_deployment_id", fake_resolve)
    monkeypatch.setattr(identity_mod, "generate_run_id", lambda: run_id)
    return captured


class TestResolveIdentity:
    def test_single_chain_uses_config_chain(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="deployment:hash")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        info = run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="my_strat",
            gateway_network="mainnet",
        )
        assert info.deployment_id == "deployment:hash"
        assert info.run_id == "run-xyz"
        assert strategy_config["deployment_id"] == "deployment:hash"
        assert strategy_config["run_id"] == "run-xyz"
        assert captured[0]["chain"] == "arbitrum"
        assert captured[0]["wallet_address"] == "0xabc"

    def test_multi_chain_hashes_all_chains(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="deployment:multi")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=True,
            strategy_chains=["base", "Arbitrum", "optimism"],
            config_display_name="my_strat",
            gateway_network="mainnet",
        )
        # Chains are lowercased and sorted before joining.
        assert captured[0]["chain"] == "arbitrum,base,optimism"

    def test_no_cli_id_kwarg_passed_to_resolver(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """VIB-4722 removed --id: the resolver gets only wallet + chain."""
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="deployment:hash")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="my_strat",
            gateway_network="mainnet",
        )
        assert set(captured[0].keys()) == {"wallet_address", "chain"}

    def test_fresh_mainnet_deletes_only_current_strategy_rows(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="deployment:hash")
        # Create a state DB with one row for the target deployment and one for another.
        db_path = tmp_path / "almanak_state.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (deployment_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
            conn.execute("INSERT INTO strategy_state VALUES (?, ?, ?)", ("deployment:hash", 1, "{}"))
            conn.execute("INSERT INTO strategy_state VALUES (?, ?, ?)", ("deployment:other", 1, "{}"))
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        runner = CliRunner()
        with runner.isolation():
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                gateway_network="mainnet",
            )
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT deployment_id FROM strategy_state").fetchall()
        assert [r[0] for r in rows] == ["deployment:other"]

    def test_fresh_anvil_deletes_all_rows(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="deployment:hash")
        db_path = tmp_path / "almanak_state.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (deployment_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
            conn.execute("INSERT INTO strategy_state VALUES (?, ?, ?)", ("deployment:hash", 1, "{}"))
            conn.execute("INSERT INTO strategy_state VALUES (?, ?, ?)", ("deployment:other", 1, "{}"))
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        runner = CliRunner()
        with runner.isolation():
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                gateway_network="anvil",
            )
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT deployment_id FROM strategy_state").fetchall()
        assert rows == []

    def test_fresh_without_state_db_emits_message(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="deployment:hash")
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "missing.db"))
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                gateway_network="mainnet",
            )
            out_stream.seek(0)
            output = out_stream.read().decode()
        assert "No existing state to clear (--fresh flag)" in output

    def test_hosted_mode_rejects_fresh_flag(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """--fresh is a SQLite-only operation. In hosted mode there is no
        local DB to clear; the platform recreates the agent if a clean state
        is required. Surface this as a clear ClickException."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "platform-agent-id")
        _install_identity_fakes(monkeypatch, deployment_id="platform-agent-id")

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        with pytest.raises(click.ClickException) as exc_info:
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                gateway_network="mainnet",
            )
        assert "--fresh is not supported in hosted mode" in exc_info.value.message


# ---------------------------------------------------------------------------
# Sanity: the typed IdentityInfo dataclass is frozen + ordered like documented.
# ---------------------------------------------------------------------------


class TestIdentityInfoShape:
    def test_identity_info_is_frozen(self) -> None:
        info = IdentityInfo(deployment_id="d", run_id="r", strategy_name="s")
        with pytest.raises(Exception):
            info.deployment_id = "changed"  # type: ignore[misc]

    def test_resume_info_is_frozen(self) -> None:
        info = ResumeInfo(is_resume=True, version=1, state_keys=["a"])
        with pytest.raises(Exception):
            info.is_resume = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Archive-chain startup timeout derivation (VIB-2902 fix)
# ---------------------------------------------------------------------------


class TestArchiveChainStartupTimeout:
    """_setup_gateway derives ManagedGateway.start(timeout=...) from per-fork budgets.

    Archive chains (avalanche, ethereum, polygon) get 90s each; non-archive
    chains get 30s each; plus 30s warmup headroom.  Aliases such as "avax"
    must resolve to the canonical "avalanche" so they also get the longer budget.
    """

    def _run_setup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        chain: str | None = None,
        network: str = "anvil",
    ) -> float:
        """Run _setup_gateway with the given chain config and return the timeout used."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        if chain is not None:
            (tmp_path / "config.json").write_text(json.dumps({"chain": chain}))

        runner = CliRunner()
        with runner.isolation():
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=network,
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        assert _FakeManagedGateway.last is not None
        return _FakeManagedGateway.last.start_timeout

    def test_archive_single_chain_avalanche_gets_120s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Single archive chain: 90s Anvil + 30s warmup = 120s."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="avalanche")
        assert timeout == 120.0

    def test_archive_single_chain_ethereum_gets_120s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="ethereum")
        assert timeout == 120.0

    def test_archive_single_chain_polygon_gets_120s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="polygon")
        assert timeout == 120.0

    def test_non_archive_chain_arbitrum_gets_60s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Non-archive chain: 30s Anvil + 30s warmup = 60s."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="arbitrum")
        assert timeout == 60.0

    def test_non_archive_chain_base_gets_60s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="base")
        assert timeout == 60.0

    def test_alias_avax_resolves_to_archive_budget(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Chain alias 'avax' must canonicalize to 'avalanche' and get 90s budget."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="avax")
        assert timeout == 120.0

    def test_alias_eth_resolves_to_archive_budget(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Chain alias 'eth' must canonicalize to 'ethereum' and get 90s budget."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="eth")
        assert timeout == 120.0

    def test_no_chains_mainnet_gets_10s(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """No anvil_chains (mainnet mode): short 10s health-check timeout."""
        # No network=anvil → anvil_chains stays empty → 10s timeout.
        timeout = self._run_setup(monkeypatch, tmp_path, chain=None, network=None)
        assert timeout == 10.0
