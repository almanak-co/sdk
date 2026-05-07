"""Parity tests for Phase 4b — private-key kwarg replaces env mutation (#2100).

The four ``os.environ["ALMANAK_PRIVATE_KEY"] = ...`` mutation sites in
``almanak/cli/cli.py`` and ``almanak/framework/cli/run_helpers.py`` were
replaced with a typed ``private_key`` kwarg threaded through
``runtime_config_from_env`` (Phase 5a-2 — formerly
``LocalRuntimeConfig.from_env`` / ``MultiChainRuntimeConfig.from_env``).
These tests pin the contract: same observable behaviour, but ``os.environ``
is never mutated mid-run.

Each test exercises one of the original mutation sites:

1. ``_build_runtime_config`` Anvil fallback (the two ``run_helpers.py``
   single- and multi-chain retry sites).
2. ``--wallet isolated`` derived-key plumbing (the third ``run_helpers.py``
   site).
3. ``almanak strat test`` Anvil-default fallback (the ``cli.py`` site).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers


class _FakeMissingEnvErr(Exception):
    """Stand-in for ``MissingEnvironmentVariableError`` (var_name attribute)."""

    def __init__(self, var_name: str) -> None:
        super().__init__(var_name)
        self.var_name = var_name


def _patch_local_factory(monkeypatch: pytest.MonkeyPatch, factory: Any) -> None:
    """Patch ``runtime_config_from_env`` in :mod:`almanak.config.runtime` so
    the helper exercises our fake.

    Phase 5a-2: the legacy ``LocalRuntimeConfig.from_env`` classmethod is
    gone. Tests inject behaviour by faking the env-reading factory; the
    production helper's MissingEnv → Anvil-default retry loop runs
    unmodified, then ``LocalRuntimeConfig.from_runtime_config(rc)`` runs on
    the fake's output (we monkeypatch ``from_runtime_config`` to be a
    pass-through identity that pulls the dataclass fake out of a stub
    runtime-config wrapper).
    """
    from almanak.config import runtime as cfg_runtime
    from almanak.framework.execution import config as execution_config

    class _RuntimeConfigStub:
        def __init__(self, dataclass: Any) -> None:
            self._test_dataclass = dataclass
            self.single_chain = True
            # ``chains`` populated so the stub satisfies any incidental
            # access — the production helper only reaches into it via
            # ``from_runtime_config``, which is patched below.
            self.chains = [getattr(dataclass, "chain", "arbitrum")]

    def _fake_runtime_config_from_env(
        *,
        chain: str | None = None,
        chains: list[str] | None = None,
        protocols: dict[str, list[str]] | None = None,
        network: str = "mainnet",
        dotenv_path: str | None = None,
        prefix: str = "ALMANAK_",
        private_key: str | None = None,
    ) -> Any:
        # Tests only need the local lane here; the legacy fakes have a
        # ``(chain, network, private_key)`` signature so we forward those.
        dc = factory(chain, network, private_key)
        return _RuntimeConfigStub(dc)

    monkeypatch.setattr(cfg_runtime, "runtime_config_from_env", _fake_runtime_config_from_env)
    monkeypatch.setattr(
        execution_config.LocalRuntimeConfig,
        "from_runtime_config",
        classmethod(lambda cls, rc: rc._test_dataclass),
    )
    monkeypatch.setattr(cfg_runtime, "MissingEnvironmentVariableError", _FakeMissingEnvErr)
    monkeypatch.setattr(execution_config, "MissingEnvironmentVariableError", _FakeMissingEnvErr)


def _make_fake_local_config(chain: str = "arbitrum") -> Any:
    from almanak.framework.execution.config import LocalRuntimeConfig

    return LocalRuntimeConfig(
        chain=chain,
        rpc_url="https://rpc.test",
        private_key="0x" + "11" * 32,
    )


class TestNoEnvMutationOnAnvilFallback:
    """The Anvil-default-key fallback in ``_build_runtime_config`` plumbs the
    key via the typed kwarg, never via ``os.environ``."""

    def test_anvil_fallback_does_not_mutate_environ(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """First call raises MissingEnv(PRIVATE_KEY); retry receives
        ``private_key=ANVIL_DEFAULT_PRIVATE_KEY`` via kwarg, env stays unset."""
        captured: dict[str, Any] = {"calls": 0, "retry_kwargs": None}

        def _from_env(chain: str, network: str, private_key: str | None = None) -> Any:
            captured["calls"] += 1
            if captured["calls"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            captured["retry_kwargs"] = {
                "chain": chain,
                "network": network,
                "private_key": private_key,
            }
            return _make_fake_local_config(chain or "arbitrum")

        _patch_local_factory(monkeypatch, _from_env)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        # Drive the non-interactive branch of the prompt.
        import sys as _sys

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False)

        cli = CliRunner()
        with cli.isolation() as (out_buf, _err):
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="anvil",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
            stdout_text = out_buf.getvalue().decode()

        from almanak.framework.cli.run import ANVIL_DEFAULT_ADDRESS, ANVIL_DEFAULT_PRIVATE_KEY

        # Contract 1: retry was driven by the kwarg, not by env mutation.
        assert captured["calls"] == 2
        assert captured["retry_kwargs"]["private_key"] == ANVIL_DEFAULT_PRIVATE_KEY
        # Contract 2: os.environ is NOT mutated.
        import os as _os

        assert _os.environ.get("ALMANAK_PRIVATE_KEY") is None
        # Contract 3: existing user-facing echoes still fire (unchanged UX).
        assert "No ALMANAK_PRIVATE_KEY set" in stdout_text
        assert ANVIL_DEFAULT_ADDRESS in stdout_text
        assert "(non-interactive, accepting default Anvil wallet)" in stdout_text


class TestNoEnvMutationOnIsolatedWallet:
    """``--wallet isolated`` derives a key and plumbs it via the runtime-config
    kwarg + gateway ``private_key`` argument; ``os.environ["ALMANAK_PRIVATE_KEY"]``
    keeps holding the master key (never overwritten)."""

    def test_isolated_wallet_does_not_mutate_environ(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        from almanak.gateway import managed as gw_managed

        # Reuse the gateway fakes harness so _setup_gateway can run end-to-end
        # without spawning a real ManagedGateway.
        from tests.unit.cli.test_run_helpers_gateway import (
            _FakeManagedGateway,
            _install_gateway_fakes,
        )

        _install_gateway_fakes(monkeypatch)
        master_key = "0x" + "11" * 32
        derived_key = "0x" + "ee" * 32
        derived_address = "0x" + "ff" * 20
        monkeypatch.setattr(
            gw_managed,
            "derive_isolated_wallet",
            lambda master, seed: (derived_key, derived_address),
        )
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", master_key)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        import json as _json

        strat_dir = tmp_path / "mystrat"
        strat_dir.mkdir()
        (strat_dir / "config.json").write_text(_json.dumps({"chain": "arbitrum"}))

        # Reset the contextvar before the test so a stale value cannot
        # masquerade as the derived key. Belt-and-braces with the autouse
        # conftest fixture: explicit reset in finally means an assertion
        # failure mid-test still leaves the contextvar in a known state,
        # independent of fixture teardown order.
        run_helpers._runtime_private_key_override.set(None)
        cli = CliRunner()
        try:
            with cli.isolation():
                (
                    _,
                    _,
                    _,
                    _,
                    _,
                    _,
                    isolated_wallet_address,
                    _,
                ) = run_helpers._setup_gateway(
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

            # Contract 1: isolated wallet derivation returned the expected address;
            # the derived key is plumbed downstream via the ContextVar so
            # `_build_runtime_config` signs through the same identity (#2100).
            assert isolated_wallet_address == derived_address
            assert run_helpers._runtime_private_key_override.get() == derived_key
            # Contract 2: ALMANAK_PRIVATE_KEY in os.environ is still the MASTER key
            # (the derived key never leaks back into the process env).
            import os as _os

            assert _os.environ.get("ALMANAK_PRIVATE_KEY") == master_key
            # Contract 3: the gateway received the derived key via the explicit kwarg.
            assert _FakeManagedGateway.last is not None
            assert _FakeManagedGateway.last.settings.kwargs["private_key"] == derived_key
        finally:
            run_helpers._runtime_private_key_override.set(None)


class TestSidecarDispatchHonoursKwargPrecedence:
    """Sidecar-vs-local dispatch in ``_build_runtime_config`` reads the same
    kwarg-over-env precedence the downstream ``from_env`` calls use (#2100,
    CodeRabbit follow-up). Without this, an explicit ``runtime_private_key``
    can't actually flip dispatch — the kwarg-precedence story would only be
    half-true.
    """

    def test_kwarg_key_with_env_unset_does_not_enter_sidecar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``runtime_private_key="<key>"`` + env unset + ``--no-gateway`` must
        dispatch to the local-runtime path (Local.from_env is called), not the
        sidecar branch — the kwarg supplied a key, env emptiness should not
        override that.
        """
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        captured: dict[str, Any] = {"local_called": False}

        def _from_env(chain: str, network: str, private_key: str | None = None) -> Any:
            captured["local_called"] = True
            return _make_fake_local_config(chain or "arbitrum")

        _patch_local_factory(monkeypatch, _from_env)

        from almanak.framework.execution.config import GatewayRuntimeConfig, LocalRuntimeConfig

        cli = CliRunner()
        with cli.isolation():
            runtime_config, _ = run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="anvil",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
                runtime_private_key="0x" + "ab" * 32,
            )

        assert captured["local_called"], (
            "kwarg supplied a key — must take the local-runtime path"
        )
        assert isinstance(runtime_config, LocalRuntimeConfig), (
            f"sidecar branch was taken despite the kwarg supplying a key — "
            f"got {type(runtime_config).__name__} instead of LocalRuntimeConfig"
        )
        assert not isinstance(runtime_config, GatewayRuntimeConfig)

    def test_explicit_empty_kwarg_with_env_set_forces_sidecar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``runtime_private_key=""`` + env-key set + ``--no-gateway`` must
        force the sidecar branch — explicit empty-string is the documented
        "no local key" override and should beat an ambient env value.
        """
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0x" + "cc" * 32)
        monkeypatch.setenv("ALMANAK_EOA_ADDRESS", "0x" + "11" * 20)
        monkeypatch.delenv("ALMANAK_SAFE_ADDRESS", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        captured: dict[str, Any] = {"local_called": False}

        def _from_env(chain: str, network: str, private_key: str | None = None) -> Any:
            captured["local_called"] = True
            return _make_fake_local_config(chain or "arbitrum")

        _patch_local_factory(monkeypatch, _from_env)

        from almanak.framework.execution.config import GatewayRuntimeConfig

        cli = CliRunner()
        with cli.isolation():
            runtime_config, _ = run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
                runtime_private_key="",
            )

        assert isinstance(runtime_config, GatewayRuntimeConfig), (
            f'explicit runtime_private_key="" did not force the sidecar branch — '
            f"got {type(runtime_config).__name__} instead of GatewayRuntimeConfig"
        )
        assert not captured["local_called"], (
            "local-runtime branch fired despite the explicit empty-string override"
        )

    def test_solana_chain_with_only_solana_private_key_does_not_enter_sidecar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-chain Solana with ``--no-gateway``, ``SOLANA_PRIVATE_KEY``
        set, ``ALMANAK_PRIVATE_KEY`` unset must dispatch to the local-runtime
        path — Solana uses ``SOLANA_PRIVATE_KEY`` (base58 Ed25519) as its
        canonical env var, mirroring the rule that
        ``LocalRuntimeConfig.from_env`` (and
        ``execution.config._resolve_private_key_from_env``) already follow.

        Regression test for CodeRabbit's finding on
        ``_resolve_effective_signing_key``: the helper used to look at
        ``ALMANAK_PRIVATE_KEY`` only, so on this configuration the sidecar
        branch fired even though the local config could have loaded.
        """
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.setenv("SOLANA_PRIVATE_KEY", "11" * 32)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        captured: dict[str, Any] = {"local_called": False, "from_env_chain": None}

        def _from_env(chain: str, network: str, private_key: str | None = None) -> Any:
            captured["local_called"] = True
            captured["from_env_chain"] = chain
            # Return an arbitrum config because LocalRuntimeConfig validates
            # the (chain, private_key) pair and our hex-secp256k1 fake key
            # would fail Solana's base58-Ed25519 check. The dispatch decision
            # under test was already made before this fake is reached, so
            # the runtime_config's chain field is incidental here.
            return _make_fake_local_config("arbitrum")

        _patch_local_factory(monkeypatch, _from_env)

        from almanak.framework.execution.config import GatewayRuntimeConfig, LocalRuntimeConfig

        cli = CliRunner()
        with cli.isolation():
            runtime_config, _ = run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="solana",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )

        assert captured["local_called"], (
            "SOLANA_PRIVATE_KEY satisfies LocalRuntimeConfig.from_env on a "
            "Solana chain — sidecar dispatch must not fire"
        )
        # The dispatch helper forwarded ``config_chain="solana"`` through to
        # the from_env call, confirming the local-runtime branch was taken.
        assert captured["from_env_chain"] == "solana"
        assert isinstance(runtime_config, LocalRuntimeConfig), (
            f"sidecar branch fired for Solana with SOLANA_PRIVATE_KEY set — "
            f"got {type(runtime_config).__name__} instead of LocalRuntimeConfig"
        )
        assert not isinstance(runtime_config, GatewayRuntimeConfig)


class TestNoEnvMutationOnStratTestGateway:
    """The CLI ``runtime_private_key`` kwarg also feeds the managed gateway
    (Codex P1 follow-up to #2100): when ``almanak strat test`` runs without
    ``ALMANAK_PRIVATE_KEY`` set, the Anvil-default fallback key must reach
    the gateway *and* the runtime config — a runtime-only plumb leaves the
    gateway with ``private_key=None`` and Anvil funding silently skips the
    strategy wallet.
    """

    def test_runtime_private_key_kwarg_reaches_gateway_in_default_wallet_mode(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        from tests.unit.cli.test_run_helpers_gateway import (
            _FakeManagedGateway,
            _install_gateway_fakes,
        )

        _install_gateway_fakes(monkeypatch)
        # No ALMANAK_PRIVATE_KEY in env — mirrors `strat test` precondition
        # that triggers the fallback to ANVIL_DEFAULT_PRIVATE_KEY.
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        import atexit as real_atexit

        monkeypatch.setattr(real_atexit, "register", lambda *a, **kw: None)

        import json as _json

        strat_dir = tmp_path / "mystrat"
        strat_dir.mkdir()
        (strat_dir / "config.json").write_text(_json.dumps({"chain": "arbitrum"}))

        anvil_default_key = "0x" + "ac" * 32

        cli = CliRunner()
        with cli.isolation():
            run_helpers._setup_gateway(
                working_dir=str(strat_dir),
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
                runtime_private_key=anvil_default_key,
            )

        # Contract: the managed gateway received the caller-plumbed key via
        # the explicit ``private_key`` kwarg. Without this the gateway would
        # boot with ``private_key=None`` and Anvil funding would skip the
        # strategy wallet (Codex P1 finding on #2100).
        assert _FakeManagedGateway.last is not None
        assert _FakeManagedGateway.last.settings.kwargs.get("private_key") == anvil_default_key
        # Contract: env is still NOT mutated by _setup_gateway.
        import os as _os

        assert _os.environ.get("ALMANAK_PRIVATE_KEY") is None


class TestNoEnvMutationOnStratTest:
    """``almanak strat test`` plumbs ANVIL_DEFAULT_PRIVATE_KEY via the
    ``_runtime_private_key_override`` ContextVar instead of mutating env or
    growing ``framework_run_cmd``'s parameter list (#2100).
    """

    def test_strat_test_does_not_mutate_environ(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        """Invoke `strat test` without ALMANAK_PRIVATE_KEY; the framework_run_cmd
        callback observes ``_runtime_private_key_override`` set to
        ``ANVIL_DEFAULT_PRIVATE_KEY``; env is never assigned and the contextvar
        is reset after the invocation finishes."""
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        # Make a minimal strategy folder so the SKIP path of strat_test_skip_reason
        # is not triggered and load_dotenv has nothing to do.
        strat_dir = tmp_path / "mystrat"
        strat_dir.mkdir()

        captured: dict[str, Any] = {"contextvar_during_invoke": object()}

        # Patch framework_run_cmd's invoke target to capture the contextvar
        # value while the click callback is executing — the CLI sets the
        # contextvar around `ctx.invoke` and resets it in the finally block,
        # so reading it AFTER the invocation would always show None.
        from almanak.framework.cli import run as run_mod

        def _capture_callback(*args: Any, **kwargs: Any) -> None:
            captured["contextvar_during_invoke"] = (
                run_helpers._runtime_private_key_override.get()
            )
            # Don't actually invoke the heavy runner — raise SystemExit(0) instead.
            import sys as _sys

            _sys.exit(0)

        monkeypatch.setattr(run_mod.run, "callback", _capture_callback)

        # Bypass the SKIP-detection probe so strat test reaches the ctx.invoke().
        from almanak.cli import cli as cli_mod

        monkeypatch.setattr(cli_mod, "_strat_test_skip_reason", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli_mod, "install_redaction", lambda: None)

        # Reset the contextvar before the test so a stale value from a prior
        # test cannot satisfy the contract assertion below.
        run_helpers._runtime_private_key_override.set(None)

        runner = CliRunner()
        result = runner.invoke(
            cli_mod.almanak,
            [
                "strat",
                "test",
                "-d",
                str(strat_dir),
                "--actions",
                "supply",
                "--gateway-port",
                "50051",
            ],
        )

        from almanak.framework.cli.run import ANVIL_DEFAULT_PRIVATE_KEY

        # Contract 1: framework_run_cmd ran with the Anvil-default key visible
        # through the ContextVar (the new plumbing channel).
        assert captured["contextvar_during_invoke"] == ANVIL_DEFAULT_PRIVATE_KEY, (
            f"strat test exit_code={result.exit_code} stdout={result.output!r}"
        )
        # Contract 2: os.environ was NOT mutated.
        import os as _os

        assert _os.environ.get("ALMANAK_PRIVATE_KEY") is None
        # Contract 3: the contextvar is reset after the CLI invocation so the
        # value never leaks into a subsequent `almanak strat test` call within
        # the same Python process.
        assert run_helpers._runtime_private_key_override.get() is None
