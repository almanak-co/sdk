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
    last: "_FakeGatewayClient | None" = None

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
    last: "_FakeManagedGateway | None" = None
    start_should_fail = False
    # Match the real ManagedGateway class attribute (managed.py:196) — the
    # _anvil_timeout helper sources the slow-chain set from this attribute,
    # so the fake must expose it for the existing timeout-budget tests
    # below (TestArchiveChainStartupTimeout) to keep working.
    ARCHIVE_RPC_REQUIRED_CHAINS = frozenset({"polygon", "ethereum", "avalanche"})

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
    """
    from almanak.gateway import managed as gw_managed
    from almanak.gateway.core import settings as gw_settings

    from almanak.framework import gateway_client as gw_client_pkg

    class _FakeSettings:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    monkeypatch.setattr(gw_managed, "ManagedGateway", _FakeManagedGateway)
    monkeypatch.setattr(gw_managed, "find_available_gateway_port", lambda host, port: port)
    monkeypatch.setattr(gw_settings, "GatewaySettings", _FakeSettings)

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
        # No session auth token or isolated wallet in external mode.
        assert token is None
        assert isolated_wallet is None
        assert early_cls is None

    def test_external_reads_auth_token_from_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

    def test_external_falls_back_to_gateway_auth_token(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

    def test_external_rejects_anvil_port(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

    def test_external_rejects_keep_anvil(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

    def test_external_rejects_isolated_wallet(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
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

        monkeypatch.setattr(
            real_atexit, "register", lambda fn, *a, **kw: atexit_calls.append((fn, a, kw))
        )
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
        with runner.isolation(), pytest.raises(
            click.ClickException, match="only supported with --network anvil"
        ):
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
        with runner.isolation():
            _, _, _, _, _, _, isolated_wallet, _ = run_helpers._setup_gateway(
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
        # ALMANAK_PRIVATE_KEY was overwritten with the derived key.
        import os

        assert os.environ["ALMANAK_PRIVATE_KEY"] == "0x" + "ab" * 32

    def test_isolated_wallet_without_master_key_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        _install_gateway_fakes(monkeypatch)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        runner = CliRunner()
        with runner.isolation(), pytest.raises(
            click.ClickException, match="ALMANAK_PRIVATE_KEY to be set"
        ):
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
    """Defensive handling of malformed Anvil quick-load configs.

    `_setup_gateway` reads the strategy's config file early to discover which
    chains need Anvil forks. The parse can return non-dict values (empty YAML
    -> None, a YAML list -> list) and the `chains` field may be a bare string
    in user-authored configs. These tests pin the hardened behavior.
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

    def test_chains_as_single_string_is_normalized_to_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """`chains: arbitrum` (a string scalar) must not iterate as characters."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        # Use JSON so we deterministically get a string scalar for chains.
        (tmp_path / "config.json").write_text(json.dumps({"chains": "arbitrum"}))
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
        # "arbitrum" wrapped into a single-element list, NOT iterated as chars.
        assert _FakeManagedGateway.last.anvil_chains == ["arbitrum"]

    def test_malformed_json_config_does_not_crash(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Malformed JSON must be swallowed, not crash gateway startup."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        # Truncated JSON - json.load will raise.
        (tmp_path / "config.json").write_text("{chain:")
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
        assert _FakeManagedGateway.last is not None
        # Parse failed -> fell through to empty list (no decorator fallback either).
        assert _FakeManagedGateway.last.anvil_chains == []

    def test_chains_as_int_is_ignored(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A numeric `chains` value is nonsensical and must be dropped, not coerced."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": 5}))
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
        assert _FakeManagedGateway.last.anvil_chains == []

    def test_chains_as_dict_does_not_use_keys_as_chains(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Guard the `list({"base": 1}) == ["base"]` footgun: dict values must be ignored."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        # A dict value would silently iterate its keys under a naive list() coercion.
        (tmp_path / "config.json").write_text(json.dumps({"chains": {"base": 1}}))
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
        # Critical: NOT ["base"]. The dict must be ignored entirely.
        assert _FakeManagedGateway.last.anvil_chains == []

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

    def test_mainnet_probe_malformed_json_does_not_crash(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """The second probe (mainnet chain detection) must also swallow parse errors.

        The second probe runs when `anvil_chains` is empty (typically on mainnet)
        to seed `gateway_chains` for the managed GatewaySettings. A malformed
        config must not crash startup here either.
        """
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text("{chain:")
        runner = CliRunner()
        with runner.isolation():
            _, managed, _, _, gw_network, _, _, _ = run_helpers._setup_gateway(
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
        assert gw_network == "mainnet"
        assert managed is not None
        # `gateway_chains` is passed through GatewaySettings; our _FakeSettings
        # stores kwargs unchanged, so we can assert the final value.
        assert managed.settings.kwargs["chains"] == []

    def test_mainnet_probe_chains_as_dict_is_ignored(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Second probe must also reject dict `chains` rather than coerce keys."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": {"base": 1}}))
        runner = CliRunner()
        with runner.isolation():
            _, managed, _, _, _, _, _, _ = run_helpers._setup_gateway(
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
        assert managed is not None
        assert managed.settings.kwargs["chains"] == []


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
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE strategy_state (
                strategy_id TEXT PRIMARY KEY,
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
                "INSERT INTO strategy_state (strategy_id, version, state_data) VALUES (?, ?, ?)",
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
                "INSERT INTO strategy_state (strategy_id, version, state_data) VALUES (?, ?, ?)",
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
                "INSERT INTO strategy_state (strategy_id, version, state_data) VALUES (?, ?, ?)",
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
    """Stub out the identity resolver + run_id generator."""
    from almanak.framework.runner import identity as identity_mod

    captured: list[dict[str, Any]] = []

    def fake_resolve(**kwargs: Any) -> str:
        captured.append(kwargs)
        return deployment_id

    monkeypatch.setattr(identity_mod, "resolve_deployment_id", fake_resolve)
    monkeypatch.setattr(identity_mod, "generate_run_id", lambda: run_id)
    return captured


class TestResolveIdentity:
    def test_single_chain_uses_config_chain(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="name:hash")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        info = run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="my_strat",
            cli_id_override=None,
            gateway_network="mainnet",
        )
        assert info.deployment_id == "name:hash"
        assert info.run_id == "run-xyz"
        assert strategy_config["strategy_id"] == "name:hash"
        assert strategy_config["run_id"] == "run-xyz"
        assert captured[0]["chain"] == "arbitrum"
        assert captured[0]["wallet_address"] == "0xabc"

    def test_multi_chain_hashes_all_chains(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="name:multi")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=True,
            strategy_chains=["base", "Arbitrum", "optimism"],
            config_display_name="my_strat",
            cli_id_override=None,
            gateway_network="mainnet",
        )
        # Chains are lowercased and sorted before joining.
        assert captured[0]["chain"] == "arbitrum,base,optimism"

    def test_cli_override_takes_precedence(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        captured = _install_identity_fakes(monkeypatch, deployment_id="cli-override")
        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="my_strat",
            cli_id_override="user-requested-id",
            gateway_network="mainnet",
        )
        assert captured[0]["cli_id"] == "user-requested-id"

    def test_fresh_mainnet_deletes_only_current_strategy_rows(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        # Create a state DB with one row for the target strategy and one for another.
        db_path = tmp_path / "almanak_state.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (strategy_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("strat-1:hash", 1, "{}")
            )
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("other-strat", 1, "{}")
            )
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
                cli_id_override=None,
                gateway_network="mainnet",
            )
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT strategy_id FROM strategy_state").fetchall()
        assert [r[0] for r in rows] == ["other-strat"]

    def test_fresh_anvil_deletes_all_rows(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        db_path = tmp_path / "almanak_state.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (strategy_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("strat-1:hash", 1, "{}")
            )
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("other-strat", 1, "{}")
            )
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
                cli_id_override=None,
                gateway_network="anvil",
            )
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT strategy_id FROM strategy_state").fetchall()
        assert rows == []

    def test_fresh_without_state_db_emits_message(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
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
                cli_id_override=None,
                gateway_network="mainnet",
            )
            out_stream.seek(0)
            output = out_stream.read().decode()
        assert "No existing state to clear (--fresh flag)" in output

    def test_backfill_migrates_when_deployment_id_differs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")

        # Fake SQLiteStore that reports migrated rows.
        migrations: list[tuple[str, str]] = []

        class _FakeStore:
            def __init__(self, config: Any) -> None:
                self.config = config

            async def backfill_deployment_id(self, old: str, new: str) -> int:
                migrations.append((old, new))
                return 4

            async def close(self) -> None:
                return None

        from almanak.framework.state.backends import sqlite as sqlite_backend

        monkeypatch.setattr(sqlite_backend, "SQLiteStore", _FakeStore)
        monkeypatch.setattr(sqlite_backend, "SQLiteConfig", lambda db_path: {"db_path": db_path})

        db_path = tmp_path / "almanak_state.db"
        db_path.write_text("")  # just needs to exist
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        runner = CliRunner()
        with runner.isolation() as (out_stream, _err):
            info = run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=False,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                cli_id_override=None,
                gateway_network="mainnet",
            )
            out_stream.seek(0)
            output = out_stream.read().decode()
        assert migrations == [("strat-1", "strat-1:hash")]
        assert info.migrated is True
        assert "Migrated 4 rows from 'strat-1' to 'strat-1:hash'" in output

    def test_backfill_exception_is_swallowed(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")

        class _BrokenStore:
            def __init__(self, config: Any) -> None:
                raise RuntimeError("simulated backfill failure")

        from almanak.framework.state.backends import sqlite as sqlite_backend

        monkeypatch.setattr(sqlite_backend, "SQLiteStore", _BrokenStore)
        monkeypatch.setattr(sqlite_backend, "SQLiteConfig", lambda db_path: {"db_path": db_path})

        db_path = tmp_path / "almanak_state.db"
        db_path.write_text("")
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        runner = CliRunner()
        with runner.isolation():
            info = run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=False,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                cli_id_override=None,
                gateway_network="mainnet",
            )
        # Startup proceeds even though backfill crashed.
        assert info.migrated is False
        assert info.deployment_id == "strat-1:hash"

    def test_backfill_skipped_when_names_match(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """If deployment_id already equals display name, no backfill runs."""
        monkeypatch.chdir(tmp_path)
        # deployment_id equals config_display_name to skip backfill.
        _install_identity_fakes(monkeypatch, deployment_id="strat-1")

        from almanak.framework.state.backends import sqlite as sqlite_backend

        class _Tripwire:
            def __init__(self, config: Any) -> None:
                raise AssertionError("backfill should not have been called")

        monkeypatch.setattr(sqlite_backend, "SQLiteStore", _Tripwire)

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        info = run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="strat-1",
            cli_id_override=None,
            gateway_network="mainnet",
        )
        assert info.migrated is False

    def test_hosted_mode_skips_sqlite_backfill(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Hosted mode (AGENT_ID set) keeps state in Postgres — the
        bare-name → deployment_id SQLite backfill must not even attempt to
        resolve a local DB path. Calling local_db_path in hosted mode raises
        LocalPathError by design (see local_paths._ensure_local), so any
        unguarded call here would crash the entire deploy at startup.
        Regression guard for VIB-3879 (rc8 stage deploy crash)."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("AGENT_ID", "test-agent-id")
        # deployment_id intentionally differs from display_name so we hit the
        # backfill branch — without the is_local() guard this would raise.
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")

        from almanak.framework.state.backends import sqlite as sqlite_backend

        class _Tripwire:
            def __init__(self, config: Any) -> None:
                raise AssertionError(
                    "hosted mode must not construct SQLiteStore for backfill"
                )

        monkeypatch.setattr(sqlite_backend, "SQLiteStore", _Tripwire)

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        info = run_helpers._resolve_identity(
            strategy_config=strategy_config,
            fresh=False,
            multi_chain=False,
            strategy_chains=[],
            config_display_name="strat-1",
            cli_id_override=None,
            gateway_network="mainnet",
        )
        # Identity still resolves; backfill skipped; nothing crashed.
        assert info.deployment_id == "strat-1:hash"
        assert info.migrated is False

    def test_hosted_mode_rejects_fresh_flag(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--fresh is a SQLite-only operation. In hosted mode there is no
        local DB to clear; the platform recreates the agent if a clean state
        is required. Surface this as a clear ClickException rather than
        letting local_db_path raise LocalPathError mid-flight."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("AGENT_ID", "test-agent-id")
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        with pytest.raises(click.ClickException) as exc_info:
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                cli_id_override=None,
                gateway_network="mainnet",
            )
        assert "--fresh is not supported in hosted mode" in exc_info.value.message


# ---------------------------------------------------------------------------
# Sanity: the typed IdentityInfo dataclass is frozen + ordered like documented.
# ---------------------------------------------------------------------------


class TestIdentityInfoShape:
    def test_identity_info_is_frozen(self) -> None:
        info = IdentityInfo(
            deployment_id="d", run_id="r", strategy_name="s", migrated=False
        )
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

    def test_archive_single_chain_avalanche_gets_120s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Single archive chain: 90s Anvil + 30s warmup = 120s."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="avalanche")
        assert timeout == 120.0

    def test_archive_single_chain_ethereum_gets_120s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="ethereum")
        assert timeout == 120.0

    def test_archive_single_chain_polygon_gets_120s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="polygon")
        assert timeout == 120.0

    def test_non_archive_chain_arbitrum_gets_60s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Non-archive chain: 30s Anvil + 30s warmup = 60s."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="arbitrum")
        assert timeout == 60.0

    def test_non_archive_chain_base_gets_60s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        timeout = self._run_setup(monkeypatch, tmp_path, chain="base")
        assert timeout == 60.0

    def test_alias_avax_resolves_to_archive_budget(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Chain alias 'avax' must canonicalize to 'avalanche' and get 90s budget."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="avax")
        assert timeout == 120.0

    def test_alias_eth_resolves_to_archive_budget(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Chain alias 'eth' must canonicalize to 'ethereum' and get 90s budget."""
        timeout = self._run_setup(monkeypatch, tmp_path, chain="eth")
        assert timeout == 120.0

    def test_no_chains_mainnet_gets_10s(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """No anvil_chains (mainnet mode): short 10s health-check timeout."""
        # No network=anvil → anvil_chains stays empty → 10s timeout.
        timeout = self._run_setup(monkeypatch, tmp_path, chain=None, network=None)
        assert timeout == 10.0
