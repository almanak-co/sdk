"""Extended unit tests for Phase 4b helpers in `run_helpers.py`.

Focuses on gaps left by `test_run_helpers_gateway.py`:
    - _setup_gateway:
        * managed-gateway health-check failure path (stops ManagedGateway)
        * `--keep-anvil` without `--network anvil` warning
        * `--reset-fork --once` informational note
        * `find_available_gateway_port` failure -> ClickException
        * Decorator-chain fallback (both Anvil and mainnet probes)
        * External-auth insecure flag allowed on anvil
        * isolated wallet threads derived key into GatewaySettings
    - _resolve_identity:
        * fresh mainnet clears teardown_requests alongside strategy_state
        * fresh mainnet handles missing teardown_requests table gracefully
        * fresh prints "No existing state" when nothing to clear
        * sqlite3.Error during clear is reported on stderr
        * Backfill "0 migrated rows" does not set migrated=True
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

# ---------------------------------------------------------------------------
# Reuse the fakes from the base gateway test file by importing them.
# ---------------------------------------------------------------------------
from tests.unit.cli.test_run_helpers_gateway import (
    _FakeGatewayClient,
    _FakeManagedGateway,
    _install_gateway_fakes,
    _install_identity_fakes,
)

# ---------------------------------------------------------------------------
# _setup_gateway — additional paths
# ---------------------------------------------------------------------------


class TestSetupGatewayManagedErrors:
    def test_managed_health_check_failure_stops_gateway(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When wait_for_ready=False on the managed path, ManagedGateway.stop() is called."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        # Force the fake client to fail health check.
        monkeypatch.setattr(_FakeGatewayClient, "wait_for_ready", lambda *_a, **_kw: False)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="health check failed"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,  # mainnet
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        # ManagedGateway.stop() was called even though health check failed.
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.stopped is True

    def test_keep_anvil_without_anvil_network_prints_warning(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--keep-anvil with mainnet prints a warning (line 768)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,  # mainnet
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=True,
                reset_fork=False,
                once=False,
            )
        text = out.getvalue().decode()
        assert "--keep-anvil has no effect without --network anvil" in text

    def test_reset_fork_with_once_prints_note(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--reset-fork + --once prints the 'has no effect' note (line 874)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        cli = CliRunner()
        with cli.isolation() as (out, _err):
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
                reset_fork=True,
                once=True,
            )
        text = out.getvalue().decode()
        assert "--reset-fork has no effect with --once" in text

    def test_find_available_port_failure_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """RuntimeError from find_available_gateway_port -> ClickException with help (733-745)."""
        _install_gateway_fakes(monkeypatch)
        from almanak.gateway import managed as gw_managed

        def _boom(_host: str, _port: int) -> int:
            raise RuntimeError("all ports in use")

        monkeypatch.setattr(gw_managed, "find_available_gateway_port", _boom)
        cli = CliRunner()
        with cli.isolation() as (out, _err), pytest.raises(click.ClickException, match="all ports in use"):
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
        text = out.getvalue().decode()
        assert "Set a specific port" in text

    def test_anvil_port_missing_chain_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--anvil-port '=8545' (empty chain) raises ClickException."""
        _install_gateway_fakes(monkeypatch)
        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="Chain name cannot be empty"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("=8545",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_anvil_port_non_integer_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """`--anvil-port arbitrum=abc` raises ClickException on int() failure."""
        _install_gateway_fakes(monkeypatch)
        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="Invalid port"):
            run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network="anvil",
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=("arbitrum=abc",),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )

    def test_decorator_chain_fallback_anvil(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When config has no `chain`, decorator metadata seeds Anvil chains (826-829)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        # Create a strategy.py to trigger early_strategy_class load.
        (tmp_path / "strategy.py").write_text("# marker")

        # Stub load_strategy_from_file to return a sentinel class.
        from almanak.framework.cli import intent_debug

        sentinel_cls = type("Sentinel", (), {})
        monkeypatch.setattr(
            intent_debug,
            "load_strategy_from_file",
            lambda path: (sentinel_cls, None),
        )
        # Seed get_default_chain to return a decorator chain.
        from almanak.framework.cli import run as run_mod

        monkeypatch.setattr(run_mod, "get_default_chain", lambda _cls: "optimism")

        cli = CliRunner()
        with cli.isolation():
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
        assert _FakeManagedGateway.last.anvil_chains == ["optimism"]

    def test_decorator_chain_fallback_mainnet_probe(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Second probe (mainnet path) also falls back to decorator chain (918-921)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        (tmp_path / "strategy.py").write_text("# marker")
        from almanak.framework.cli import intent_debug
        from almanak.framework.cli import run as run_mod

        sentinel_cls = type("Sentinel", (), {})
        monkeypatch.setattr(
            intent_debug,
            "load_strategy_from_file",
            lambda path: (sentinel_cls, None),
        )
        monkeypatch.setattr(run_mod, "get_default_chain", lambda _cls: "base")

        cli = CliRunner()
        with cli.isolation():
            _, managed, _, _, _, _, _, _ = run_helpers._setup_gateway(
                working_dir=str(tmp_path),
                config_file=None,
                network=None,  # mainnet -> second probe
                gateway_host="127.0.0.1",
                gateway_port=50051,
                no_gateway=False,
                anvil_ports=(),
                wallet="default",
                keep_anvil=False,
                reset_fork=False,
                once=False,
            )
        # gateway_chains passed to GatewaySettings contains the decorator chain.
        assert managed.settings.kwargs["chains"] == ["base"]

    def test_solana_chain_filtered_from_anvil_chains(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Solana is handled by solana-test-validator, filtered from ManagedGateway's anvil_chains."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chains": ["solana", "arbitrum"]}))

        cli = CliRunner()
        with cli.isolation():
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
        # Solana filtered out.
        assert _FakeManagedGateway.last.anvil_chains == ["arbitrum"]

    def test_only_solana_chain_warns_no_forks(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Only Solana configured on anvil -> no EVM anvil forks (no warning since solana_anvil=True)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        (tmp_path / "config.json").write_text(json.dumps({"chain": "solana"}))

        cli = CliRunner()
        with cli.isolation() as (out, _err):
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
        # No evm anvil chains, but solana_anvil=True -> NO warning emitted.
        assert "Gateway will start without Anvil forks" not in out.getvalue().decode()
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.anvil_chains == []

    def test_anvil_without_any_chain_prints_warning(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--network anvil but no chain info anywhere -> WARNING (844-848)."""
        _install_gateway_fakes(monkeypatch)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)
        # No config, no strategy.py -> no chain discovered.
        cli = CliRunner()
        with cli.isolation() as (out, _err):
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
        assert "Gateway will start without Anvil forks" in out.getvalue().decode()

    def test_isolated_wallet_threads_derived_key_into_gateway_settings(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Isolated wallet passes derived private key via GatewaySettings kwargs."""
        _install_gateway_fakes(monkeypatch)
        from almanak.gateway import managed as gw_managed

        monkeypatch.setattr(
            gw_managed,
            "derive_isolated_wallet",
            lambda master, seed: ("0x" + "ee" * 32, "0x" + "ff" * 20),
        )
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0x" + "11" * 32)
        (tmp_path / "config.json").write_text(json.dumps({"chain": "arbitrum"}))
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        cli = CliRunner()
        strat_dir = tmp_path / "mystrat"
        strat_dir.mkdir()
        (strat_dir / "config.json").write_text(json.dumps({"chain": "arbitrum"}))
        with cli.isolation():
            run_helpers._setup_gateway(
                working_dir=str(strat_dir),
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
        # GatewaySettings received the derived private_key kwarg.
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.settings.kwargs["private_key"] == "0x" + "ee" * 32


# ---------------------------------------------------------------------------
# _resolve_identity — additional paths
# ---------------------------------------------------------------------------


def _make_state_db_with_teardown(path: Path) -> None:
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
        conn.execute(
            """
            CREATE TABLE teardown_requests (
                id INTEGER PRIMARY KEY,
                strategy_id TEXT
            )
            """
        )


class TestResolveIdentityFreshExtended:
    def test_fresh_mainnet_clears_teardown_requests(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--fresh on mainnet deletes teardown_requests for the current strategy only."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        db_path = tmp_path / "almanak_state.db"
        _make_state_db_with_teardown(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("strat-1:hash", 1, "{}")
            )
            conn.execute(
                "INSERT INTO teardown_requests (strategy_id) VALUES (?)", ("strat-1:hash",)
            )
            conn.execute(
                "INSERT INTO teardown_requests (strategy_id) VALUES (?)", ("other-strat",)
            )
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner()
        with cli.isolation() as (out, _err):
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
            remaining = conn.execute("SELECT strategy_id FROM teardown_requests").fetchall()
        assert [r[0] for r in remaining] == ["other-strat"]
        text = out.getvalue().decode()
        assert "teardown requests" in text.lower() or "teardown" in text.lower()

    def test_fresh_anvil_clears_all_teardown_requests(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--fresh on anvil nukes every teardown_requests row, not just the current strategy."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        db_path = tmp_path / "almanak_state.db"
        _make_state_db_with_teardown(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("strat-1:hash", 1, "{}")
            )
            conn.execute(
                "INSERT INTO teardown_requests (strategy_id) VALUES (?)", ("strat-1:hash",)
            )
            conn.execute(
                "INSERT INTO teardown_requests (strategy_id) VALUES (?)", ("other-strat",)
            )
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner()
        with cli.isolation():
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
            remaining = conn.execute("SELECT strategy_id FROM teardown_requests").fetchall()
        assert remaining == []

    def test_fresh_with_missing_teardown_table_does_not_crash(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """If teardown_requests table does not exist, OperationalError is swallowed."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        db_path = tmp_path / "almanak_state.db"
        # Only strategy_state table — teardown_requests is absent.
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (strategy_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
            conn.execute(
                "INSERT INTO strategy_state VALUES (?, ?, ?)", ("strat-1:hash", 1, "{}")
            )
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner()
        with cli.isolation():
            # Must not raise.
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
        assert rows == []

    def test_fresh_empty_db_prints_no_existing_state_message(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--fresh with a db that exists but has no matching rows prints a no-state message (583)."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        db_path = tmp_path / "almanak_state.db"
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "CREATE TABLE strategy_state (strategy_id TEXT PRIMARY KEY, version INTEGER, state_data TEXT)"
            )
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                cli_id_override=None,
                gateway_network="mainnet",
            )
        assert "No existing state for strategy" in out.getvalue().decode()

    def test_fresh_reports_sqlite_error_on_stderr(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A malformed sqlite DB triggers the sqlite3.Error branch (584-585)."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")
        # Write a non-sqlite file but non-empty so .exists() passes and sqlite3 bails.
        db_path = tmp_path / "almanak_state.db"
        db_path.write_bytes(b"\x00\x01\x02 not-a-db")
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err):
            run_helpers._resolve_identity(
                strategy_config=strategy_config,
                fresh=True,
                multi_chain=False,
                strategy_chains=[],
                config_display_name="strat-1",
                cli_id_override=None,
                gateway_network="mainnet",
            )
        assert "Failed to clear strategy state" in err.getvalue().decode()

    def test_backfill_with_zero_rows_migrated_does_not_flag_migrated(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Backfill returning 0 means nothing migrated (info.migrated stays False)."""
        monkeypatch.chdir(tmp_path)
        _install_identity_fakes(monkeypatch, deployment_id="strat-1:hash")

        class _ZeroStore:
            def __init__(self, config: Any) -> None:
                self.config = config

            async def backfill_deployment_id(self, _old: str, _new: str) -> int:
                return 0

            async def close(self) -> None:
                return None

        from almanak.framework.state.backends import sqlite as sqlite_backend

        monkeypatch.setattr(sqlite_backend, "SQLiteStore", _ZeroStore)
        monkeypatch.setattr(sqlite_backend, "SQLiteConfig", lambda db_path: {"db_path": db_path})

        db_path = tmp_path / "almanak_state.db"
        db_path.write_text("")
        monkeypatch.setenv("ALMANAK_STATE_DB", str(db_path))

        strategy_config: dict[str, Any] = {"chain": "arbitrum", "wallet_address": "0xabc"}
        cli = CliRunner()
        with cli.isolation():
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
