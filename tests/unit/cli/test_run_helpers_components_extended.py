"""Extended unit tests for Phase 4c helpers in `run_helpers.py`.

Closes gaps in:
    - _instantiate_strategy: Decimal coercion exception + introspection fallback
    - _build_runtime_config: multi-chain error branches, env override, gateway wallets failure
    - _build_components (_build_orchestrator_and_providers):
        * multi-chain missing wallet -> ClickException
        * rate monitor / funding rate provider (both single and multi-chain)
        * set_multi_chain_providers path
        * Solana fork manager init on anvil
        * prediction provider chain gating
    - _init_copy_trading: copy-trading v2 replay path, non-dict ct_raw, price_fn
    - _maybe_auto_deploy_vault: vault state backfill / persist callback

Pattern matches `test_run_helpers_components.py`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from almanak.framework.cli._run_context import ComponentBundle
from tests.unit.cli.test_run_helpers_components import (
    _FakeMissingEnvErr,
    _make_fake_local_config,
    _make_fake_multichain_config,
    _make_intent_strategy_stub,
    _make_strategy_instance,
    _patch_component_factories,
    _patch_runtime_config_imports,
)

# ---------------------------------------------------------------------------
# _instantiate_strategy — Decimal coercion edge + introspection fallback
# ---------------------------------------------------------------------------


class TestInstantiateStrategyExtras:
    def test_decimal_coercion_exception_keeps_original_value(self) -> None:
        """An un-coerceable value for a Decimal field is kept as-is (1148-1149)."""
        strategy_cls = _make_intent_strategy_stub()
        runtime_config = MagicMock()
        runtime_config.chain = "arbitrum"
        runtime_config.execution_address = "0xabc"

        # `object()` can't be stringified into a Decimal -> triggers except branch.
        # But isinstance check rejects it first. Use a string with invalid chars.
        cli = CliRunner()
        with cli.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={
                    "threshold": "not-a-number",  # hits Decimal(str()) ValueError path
                    "chain": "arbitrum",
                    "wallet_address": "0xabc",
                },
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        # Fell back to the raw string.
        assert instance.config.threshold == "not-a-number"

    def test_unknown_fields_emit_ignored_message(self) -> None:
        """Unknown fields trigger the 'ignored' branch (1158-1161)."""
        strategy_cls = _make_intent_strategy_stub()
        runtime_config = MagicMock()
        runtime_config.chain = "arbitrum"
        runtime_config.execution_address = "0xabc"

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={
                    "threshold": "0.5",
                    "label": "x",
                    "unknown_param_one": "foo",  # not a dataclass field
                    "bogus": 42,  # also not a field
                    "chain": "arbitrum",
                    "wallet_address": "0xabc",
                },
                runtime_config=runtime_config,
                multi_chain=False,
                strategy_chains=[],
                chain_wallets={},
            )
        text = out.getvalue().decode()
        # The ignored fields are mentioned in the echo.
        assert "ignored:" in text
        assert "unknown_param_one" in text or "bogus" in text

    def test_introspection_failure_falls_back_to_base_kwargs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When inspect.signature raises, helper uses base_kwargs only (1204-1208)."""
        strategy_cls = _make_intent_strategy_stub()
        runtime_config = MagicMock()
        runtime_config.chain = "arbitrum"
        runtime_config.execution_address = "0xabc"

        # Patch inspect.signature in the helper module to raise.
        import inspect as _inspect

        def _raise(*_a: Any, **_kw: Any) -> Any:
            raise ValueError("cannot introspect this class")

        monkeypatch.setattr(run_helpers, "inspect", MagicMock(signature=_raise, Parameter=_inspect.Parameter))

        cli = CliRunner()
        with cli.isolation():
            instance = run_helpers._instantiate_strategy(
                strategy_class=strategy_cls,
                strategy_config={
                    "threshold": "0.5",
                    "chain": "arbitrum",
                    "wallet_address": "0xabc",
                },
                runtime_config=runtime_config,
                multi_chain=True,
                strategy_chains=["arbitrum", "base"],
                chain_wallets={"arbitrum": "0x111", "base": "0x222"},
            )
        # Even with chain_wallets present, fallback means no chains/chain_wallets kwargs
        # were passed (validates the base_kwargs-only path).
        assert instance.chain == "arbitrum"


# ---------------------------------------------------------------------------
# _build_runtime_config — uncovered branches
# ---------------------------------------------------------------------------


class TestBuildRuntimeConfigErrors:
    def test_sidecar_missing_chain_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sidecar mode requires a chain from config or decorator (1298)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="Chain must be specified"):
            run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain=None,  # no chain!
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )

    def test_sidecar_missing_wallet_and_no_registry_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sidecar mode requires SAFE/EOA/GATEWAY_WALLETS (1305)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.delenv("ALMANAK_SAFE_ADDRESS", raising=False)
        monkeypatch.delenv("ALMANAK_EOA_ADDRESS", raising=False)

        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="Sidecar mode"):
            run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )

    def test_multi_chain_anvil_fallback_to_default_private_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain anvil falls back to default private key on MissingEnv (1330-1348)."""
        call_count: dict[str, Any] = {"n": 0, "retry_kwargs": None}

        def _from_env(
            chains: list[str], protocols: Any, network: str, private_key: str | None = None
        ) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            call_count["retry_kwargs"] = {
                "chains": chains,
                "network": network,
                "private_key": private_key,
            }
            return _make_fake_multichain_config(chains)

        _patch_runtime_config_imports(monkeypatch, multi_factory=_from_env)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        import sys as _sys

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False)

        cli = CliRunner()
        strategy_config: dict[str, Any] = {}
        with cli.isolation():
            rt, _ = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="anvil",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        assert call_count["n"] == 2  # retried with the kwarg
        import os as _os

        from almanak.framework.cli.run import ANVIL_DEFAULT_PRIVATE_KEY

        # Retry plumbed the Anvil-default key via the kwarg; env was NOT mutated.
        assert call_count["retry_kwargs"]["private_key"] == ANVIL_DEFAULT_PRIVATE_KEY
        assert _os.environ.get("ALMANAK_PRIVATE_KEY") is None

    def test_multi_chain_sidecar_no_private_key_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain sidecar mode without registry raises exit-1 guidance (1349-1358)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")

        _patch_runtime_config_imports(monkeypatch, multi_factory=_raises)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=True,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        assert "Multi-chain sidecar" in err.getvalue().decode()

    def test_multi_chain_mainnet_missing_private_key_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain mainnet without private key: ALMANAK_PRIVATE_KEY guidance (1360-1361)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")

        _patch_runtime_config_imports(monkeypatch, multi_factory=_raises)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        assert "ALMANAK_PRIVATE_KEY is required" in err.getvalue().decode()

    def test_multi_chain_missing_rpc_env_exits_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-chain missing non-PRIVATE_KEY env var prints RPC guidance (1364-1374)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise _FakeMissingEnvErr("ALMANAK_BASE_RPC_URL")

        _patch_runtime_config_imports(monkeypatch, multi_factory=_raises)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        out_text = err.getvalue().decode()
        assert "RPC" in out_text
        assert "ALMANAK_BASE_RPC_URL" in out_text

    def test_multi_chain_unknown_exception_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unexpected exception from MultiChainRuntimeConfig.from_env -> exit 1 (1375-1386)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise RuntimeError("unexpected explosion")

        _patch_runtime_config_imports(monkeypatch, multi_factory=_raises)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        assert "unexpected explosion" in err.getvalue().decode()

    def test_single_chain_mainnet_missing_private_key_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-chain mainnet without key -> exit 1 (1406-1408)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")

        _patch_runtime_config_imports(monkeypatch, local_factory=_raises)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        assert "required for mainnet" in err.getvalue().decode()

    def test_single_chain_missing_other_env_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-PRIVATE_KEY env missing -> RPC guidance (1409-1424)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise _FakeMissingEnvErr("ALMANAK_ARBITRUM_RPC_URL")

        _patch_runtime_config_imports(monkeypatch, local_factory=_raises)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        text = err.getvalue().decode()
        assert "ALMANAK_ARBITRUM_RPC_URL" in text

    def test_single_chain_unknown_exception_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-chain arbitrary exception -> exit 1 with help (1426-1442)."""

        def _raises(*_a: Any, **_kw: Any) -> Any:
            raise RuntimeError("config explosion")

        _patch_runtime_config_imports(monkeypatch, local_factory=_raises)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        text = err.getvalue().decode()
        assert "config explosion" in text

    def test_anvil_single_chain_retry_fails_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If retry after default-key fallback still fails, exit 1 (1402-1404)."""
        call_count = {"n": 0}

        def _fail_twice(chain: str, network: str, private_key: str | None = None) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            raise RuntimeError("retry also broken")

        _patch_runtime_config_imports(monkeypatch, local_factory=_fail_twice)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        import sys as _sys

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
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
        assert exc_info.value.code == 1
        assert "after setting default key" in err.getvalue().decode()

    def test_anvil_user_declines_default_wallet_exits_0(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On TTY, `confirm(...)` False -> sys.exit(0) (1395-1396)."""
        call_count = {"n": 0}

        def _fail_once(chain: str, network: str, private_key: str | None = None) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            return _make_fake_local_config(chain or "arbitrum")

        _patch_runtime_config_imports(monkeypatch, local_factory=_fail_once)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        # Skip CliRunner.isolation() (which forces isatty=False via its stdin
        # replacement). Patch sys.stdin.isatty to return True and click.confirm
        # to return False so the helper takes the decline branch.
        import sys as _sys

        import click as _click_pkg

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(_click_pkg, "confirm", lambda *_a, **_kw: False)

        with pytest.raises(SystemExit) as exc_info:
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
        assert exc_info.value.code == 0

    def test_register_chains_exception_logs_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When register_chains fails, helper logs warning and continues (1484-1487)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", '{"arbitrum":"0xaaa"}')

        gateway_client = MagicMock()
        gateway_client.register_chains.side_effect = RuntimeError("registry down")

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err):
            rt, chain_wallets = run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=gateway_client,
                strategy_config={},
            )
        assert chain_wallets == {}
        err_text = err.getvalue().decode()
        assert "register_chains" in err_text
        assert "registry down" in err_text

    def test_env_chain_override_rewrites_strategy_config_chain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ALMANAK_CHAIN env override rewrites a pre-existing config chain (1501-1505)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")
        monkeypatch.delenv("ALMANAK_GATEWAY_WALLETS", raising=False)
        monkeypatch.setenv("ALMANAK_CHAIN", "base")

        cli = CliRunner()
        strategy_config: dict[str, Any] = {"chain": "arbitrum"}  # pre-existing
        with cli.isolation():
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=False,
                resolved_network="mainnet",
                config_chain="arbitrum",
                strategy_chains=[],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config=strategy_config,
            )
        assert strategy_config["chain"] == "base"

    def test_non_uniform_gateway_wallets_prints_per_chain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-uniform wallet map -> prints per-chain mapping (1481-1483)."""
        _patch_runtime_config_imports(monkeypatch)
        monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xkey")
        monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")

        gateway_client = MagicMock()
        gateway_client.register_chains.return_value = {
            "arbitrum": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "base": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        }

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="mainnet",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=gateway_client,
                strategy_config={},
            )
        text = out.getvalue().decode()
        assert "non-uniform" in text
        assert "0xaaaaaaaaaaaaaaaa".lower() in text.lower()


# ---------------------------------------------------------------------------
# _build_orchestrator_and_providers — rate monitor / funding rate / multi-chain setters
# ---------------------------------------------------------------------------


class TestBuildOrchestratorExtras:
    def test_multi_chain_missing_wallet_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain with empty wallet in runtime_config and no chain_wallets -> ClickException (1562-1566)."""
        _patch_component_factories(monkeypatch)

        # Craft a multi-chain runtime_config where execution_address is ""
        class _NoWallet:
            chain = "arbitrum"
            chains = ["arbitrum", "base"]
            execution_address = ""
            wallet_address = ""
            max_gas_price_gwei = 50
            rpc_urls = {"arbitrum": "https://rpc", "base": "https://rpc"}

            @property
            def is_safe_mode(self) -> bool:
                return False

        strategy_instance = _make_strategy_instance()
        cli = CliRunner()
        with cli.isolation(), pytest.raises(click.ClickException, match="No wallet address resolved"):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum"},
                runtime_config=_NoWallet(),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="err-wallet",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )

    def test_multi_chain_with_set_multi_chain_providers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain calls set_multi_chain_providers on strategies that expose it (1592-1596)."""
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()
        strategy_instance.set_multi_chain_providers = MagicMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_multichain_config(["arbitrum", "base"]),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={"arbitrum": "0xchain1", "base": "0xchain2"},
                interval=60,
                effective_dry_run=False,
                deployment_id="mc-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        strategy_instance.set_multi_chain_providers.assert_called_once()
        assert "Multi-chain providers set" in out.getvalue().decode()

    def test_multi_chain_rate_monitor_wired(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-chain path attaches a rate monitor when strategy declares _rate_monitor (1612-1622)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._rate_monitor = None

        from almanak.framework.data import rates as rates_mod

        rm_calls: list[Any] = []

        class _FakeRateMonitor:
            def __init__(self, chain: str, rpc_url: Any) -> None:
                rm_calls.append((chain, rpc_url))

        monkeypatch.setattr(rates_mod, "RateMonitor", _FakeRateMonitor)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_multichain_config(["arbitrum", "base"]),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="rm-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        assert rm_calls and rm_calls[0][0] == "arbitrum"
        assert isinstance(strategy_instance._rate_monitor, _FakeRateMonitor)

    def test_multi_chain_rate_monitor_init_failure_is_logged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """RateMonitor init failure is caught at DEBUG, does not abort startup (1621-1622)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._rate_monitor = None

        from almanak.framework.data import rates as rates_mod

        def _boom(*_a: Any, **_kw: Any) -> None:
            raise RuntimeError("rpc not reachable")

        monkeypatch.setattr(rates_mod, "RateMonitor", _boom)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_multichain_config(["arbitrum", "base"]),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="rm-err",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        # Strategy still instantiated, attribute stays None.
        assert strategy_instance._rate_monitor is None

    def test_multi_chain_funding_rate_wired(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-chain funding rate provider wired on strategies that declare it (1625-1632)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._funding_rate_provider = None

        from almanak.framework.data import funding as funding_mod

        class _FakeFunding:
            def __init__(self, gateway_client: Any, chain: str) -> None:
                self.chain = chain

        monkeypatch.setattr(funding_mod, "GatewayFundingRateProvider", _FakeFunding)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_multichain_config(["arbitrum", "base"]),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="funding-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        assert isinstance(strategy_instance._funding_rate_provider, _FakeFunding)

    def test_multi_chain_funding_rate_init_failure_warns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Funding provider init failure is warned, not fatal (1633-1639)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._funding_rate_provider = None

        from almanak.framework.data import funding as funding_mod

        def _boom(*_a: Any, **_kw: Any) -> None:
            raise ValueError("bad chain")

        monkeypatch.setattr(funding_mod, "GatewayFundingRateProvider", _boom)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_multichain_config(["arbitrum", "base"]),
                strategy_chains=["arbitrum", "base"],
                multi_chain=True,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="funding-fail",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain=None,
            )
        # stays None after failure
        assert strategy_instance._funding_rate_provider is None

    def test_single_chain_prediction_provider_wired(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_init_prediction_provider is called when strategy declares _prediction_provider (1732-1733)."""
        mocks = _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._prediction_provider = None

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="pred-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        mocks["_init_prediction_provider"].assert_called_once()

    def test_single_chain_rate_monitor_wired(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Single-chain rate monitor wired for strategies declaring _rate_monitor (1736-1745)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._rate_monitor = None

        from almanak.framework.data import rates as rates_mod

        rm_calls: list[Any] = []

        class _FakeRateMonitor:
            def __init__(self, chain: str, rpc_url: Any) -> None:
                rm_calls.append((chain, rpc_url))

        monkeypatch.setattr(rates_mod, "RateMonitor", _FakeRateMonitor)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="sc-rm",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert rm_calls and rm_calls[0][0] == "arbitrum"

    def test_single_chain_funding_provider_wired(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-chain funding provider wired (1748-1754)."""
        _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()
        strategy_instance._funding_rate_provider = None

        from almanak.framework.data import funding as funding_mod

        class _Fake:
            def __init__(self, gateway_client: Any, chain: str) -> None:
                self.chain = chain

        monkeypatch.setattr(funding_mod, "GatewayFundingRateProvider", _Fake)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="sc-fund",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert isinstance(strategy_instance._funding_rate_provider, _Fake)

    def test_single_chain_chain_wallets_override_wallet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """sc_effective_wallet = chain_wallets.get(runtime_config.chain, default) (1652)."""
        mocks = _patch_component_factories(monkeypatch)

        strategy_instance = _make_strategy_instance()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "arbitrum", "wallet_address": "0xwallet"},
                runtime_config=_make_fake_local_config("arbitrum"),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={"arbitrum": "0xmappedwallet"},
                interval=60,
                effective_dry_run=False,
                deployment_id="chain-wallet-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # GatewayBalanceProvider constructed with the mapped wallet.
        call = mocks["GatewayBalanceProvider"].call_args
        assert call.kwargs["wallet_address"] == "0xmappedwallet"


# ---------------------------------------------------------------------------
# _init_copy_trading — missing branches
# ---------------------------------------------------------------------------


class TestInitCopyTradingExtras:
    def test_non_dict_copy_trading_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-dict copy_trading raises ClickException from _build_components (1817-1818)."""
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        cli = CliRunner()
        with cli.isolation(), pytest.raises((click.ClickException, SystemExit)):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "copy_trading": "bogus-string-not-dict",
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="ct-baddict",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )

    def test_copy_trading_price_fn_handles_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """copy_price_fn returns None when underlying sync_price raises (1873-1876)."""
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        # Stub sync_price to raise.
        from almanak.framework.cli import run as run_mod

        def _boom_sync_price(_oracle: Any) -> Any:
            def _sync(*_a: Any, **_kw: Any) -> Any:
                raise RuntimeError("network down")

            return _sync

        monkeypatch.setattr(run_mod, "create_sync_price_oracle_func", _boom_sync_price)

        # Stub the copy-trading factories so _init_copy_trading can progress.
        from almanak.framework.services import copy_signal_engine as cse_mod
        from almanak.framework.services import copy_trading_models as ctm_mod
        from almanak.framework.services import wallet_monitor as wm_mod

        fake_v1 = MagicMock(leaders=[{"address": "0xleader", "chain": "arbitrum"}], monitoring={})
        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _r: fake_v1))

        def _v2_boom(_r: Any) -> Any:
            raise ctm_mod.CopyTradingConfigError("unsupported")

        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(_v2_boom))

        captured_price_fn: list[Any] = []

        class _CapturedEngine:
            def __init__(self, **kwargs: Any) -> None:
                captured_price_fn.append(kwargs.get("price_fn"))

        monkeypatch.setattr(cse_mod, "CopySignalEngine", _CapturedEngine)
        monkeypatch.setattr(wm_mod, "WalletMonitor", MagicMock())

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "copy_trading": {"leaders": [{"address": "0xleader", "chain": "arbitrum"}]},
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="ct-pricefn",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # The captured price_fn swallows the underlying exception and returns None.
        price_fn = captured_price_fn[0]
        assert price_fn is not None
        assert price_fn("ETH", "arbitrum") is None

    def test_copy_trading_v2_replay_loads_signals(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """copy_mode=replay loads signals via CopyReplayRunner (1919-1932)."""
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        from almanak.framework.services import (
            copy_circuit_breaker as ccb_mod,
        )
        from almanak.framework.services import (
            copy_intent_builder as cib_mod,
        )
        from almanak.framework.services import (
            copy_ledger as cl_mod,
        )
        from almanak.framework.services import (
            copy_policy_engine as cpe_mod,
        )
        from almanak.framework.services import (
            copy_signal_engine as cse_mod,
        )
        from almanak.framework.services import (
            copy_trading_models as ctm_mod,
        )
        from almanak.framework.services import (
            wallet_monitor as wm_mod,
        )
        from almanak.framework.testing import copy_replay as cr_mod

        fake_v2 = MagicMock()
        fake_v2.leaders = [MagicMock(chain="arbitrum", address="0xleader")]
        fake_v2.monitoring = MagicMock(
            poll_interval_seconds=10,
            lookback_blocks=30,
            confirmation_depth=1,
            max_signal_age_seconds=200,
        )
        fake_v2.execution_policy = MagicMock(copy_mode="live", replay_file=None)

        fake_v1 = MagicMock(leaders=[{"address": "0xleader", "chain": "arbitrum"}], monitoring={})
        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _r: fake_v1))
        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(lambda _r: fake_v2))
        monkeypatch.setattr(cse_mod, "CopySignalEngine", MagicMock())
        monkeypatch.setattr(cpe_mod, "CopyPolicyEngine", MagicMock())
        monkeypatch.setattr(cib_mod, "CopyIntentBuilder", MagicMock())
        monkeypatch.setattr(ccb_mod, "CopyCircuitBreaker", MagicMock())
        monkeypatch.setattr(cl_mod, "CopyLedger", MagicMock())
        monkeypatch.setattr(wm_mod, "WalletMonitor", MagicMock())

        # Replay runner returns 3 signals.
        # Replace activity_provider.inject_signals so we don't need real signal objects.
        from almanak.framework.data import wallet_activity as wa_mod

        inject_called: dict[str, Any] = {}

        def _fake_inject(self, signals: list[Any]) -> None:
            inject_called["signals"] = list(signals)

        monkeypatch.setattr(wa_mod.WalletActivityProvider, "inject_signals", _fake_inject)

        class _FakeRunner:
            def __init__(self, config: Any) -> None:
                pass

            def load_signals(self, path: str) -> list[Any]:
                return [f"sig{i}" for i in range(3)]

        monkeypatch.setattr(cr_mod, "CopyReplayRunner", _FakeRunner)

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "copy_trading": {"leaders": [{"address": "0xleader", "chain": "arbitrum"}]},
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="ct-replay",
                normalized_copy_mode=None,
                copy_replay_file="/tmp/signals.jsonl",  # triggers replay branch
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert strategy_instance._copy_mode == "replay"
        assert strategy_instance._copy_replay_file == "/tmp/signals.jsonl"
        assert inject_called.get("signals") == ["sig0", "sig1", "sig2"]
        text = out.getvalue().decode()
        assert "Copy replay loaded: 3 signal(s)" in text

    def test_copy_trading_v2_shadow_overrides_copy_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """copy_shadow=True sets copy_mode to 'shadow' on the strategy (1922-1923)."""
        _patch_component_factories(monkeypatch)
        strategy_instance = _make_strategy_instance()

        from almanak.framework.services import (
            copy_circuit_breaker as ccb_mod,
        )
        from almanak.framework.services import (
            copy_intent_builder as cib_mod,
        )
        from almanak.framework.services import (
            copy_ledger as cl_mod,
        )
        from almanak.framework.services import (
            copy_policy_engine as cpe_mod,
        )
        from almanak.framework.services import (
            copy_signal_engine as cse_mod,
        )
        from almanak.framework.services import (
            copy_trading_models as ctm_mod,
        )
        from almanak.framework.services import (
            wallet_monitor as wm_mod,
        )

        fake_v2 = MagicMock()
        fake_v2.leaders = [MagicMock(chain="arbitrum", address="0xleader")]
        fake_v2.monitoring = MagicMock(
            poll_interval_seconds=10,
            lookback_blocks=30,
            confirmation_depth=1,
            max_signal_age_seconds=200,
        )
        fake_v2.execution_policy = MagicMock(copy_mode="live", replay_file=None)
        fake_v1 = MagicMock(leaders=[{"address": "0xleader", "chain": "arbitrum"}], monitoring={})
        monkeypatch.setattr(ctm_mod.CopyTradingConfig, "from_config", staticmethod(lambda _r: fake_v1))
        monkeypatch.setattr(ctm_mod.CopyTradingConfigV2, "from_config", staticmethod(lambda _r: fake_v2))
        monkeypatch.setattr(cse_mod, "CopySignalEngine", MagicMock())
        monkeypatch.setattr(cpe_mod, "CopyPolicyEngine", MagicMock())
        monkeypatch.setattr(cib_mod, "CopyIntentBuilder", MagicMock())
        monkeypatch.setattr(ccb_mod, "CopyCircuitBreaker", MagicMock())
        monkeypatch.setattr(cl_mod, "CopyLedger", MagicMock())
        monkeypatch.setattr(wm_mod, "WalletMonitor", MagicMock())

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "copy_trading": {"leaders": [{"address": "0xleader", "chain": "arbitrum"}]},
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=True,
                deployment_id="ct-shadow",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=True,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert strategy_instance._copy_mode == "shadow"


# ---------------------------------------------------------------------------
# _maybe_auto_deploy_vault — persistence callback and initial-state load
# ---------------------------------------------------------------------------


class TestVaultLifecycleExtras:
    def test_vault_initial_state_loaded_from_persistent_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Vault initial_state falls back to strategy.persistent_state (2013-2018)."""
        mocks = _patch_component_factories(monkeypatch)
        mocks["_has_placeholder_vault_address"].return_value = False

        strategy_instance = _make_strategy_instance()
        # persistent_state has a vault state key.
        from almanak.framework.vault.lifecycle import VAULT_STATE_KEY

        strategy_instance.persistent_state = {VAULT_STATE_KEY: {"cached": True}}
        strategy_instance.state = None  # only persistent_state should be consulted

        # state manager returns no persisted state -> fallback to persistent_state.
        class _FakeStateMgr:
            async def load_state(self, _sid: str) -> Any:
                return None

        mocks["GatewayStateManager"].return_value = _FakeStateMgr()

        # Capture VaultLifecycleManager kwargs to assert initial_vault_state was passed.
        captured: dict[str, Any] = {}
        from almanak.connectors import lagoon as lagoon_mod
        from almanak.framework.vault import lifecycle as vlc_mod

        def _capture(**kwargs: Any) -> Any:
            captured.update(kwargs)
            return MagicMock()

        monkeypatch.setattr(vlc_mod, "VaultLifecycleManager", _capture)
        monkeypatch.setattr(lagoon_mod, "LagoonVaultSDK", MagicMock())
        monkeypatch.setattr(lagoon_mod, "LagoonVaultAdapter", MagicMock())

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "vault": {
                        "vault_address": "0x" + "a" * 40,
                        "valuator_address": "0x" + "b" * 40,
                        "underlying_token": "USDC",
                        "settlement_interval_minutes": 60,
                    },
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="vault-init",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert captured["initial_vault_state"] == {"cached": True}

    def test_vault_persist_callback_writes_to_strategy_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Persist callback writes vault state into strategy.persistent_state + calls save_state (2022-2029)."""
        mocks = _patch_component_factories(monkeypatch)
        mocks["_has_placeholder_vault_address"].return_value = False

        strategy_instance = _make_strategy_instance()
        strategy_instance.persistent_state = {}
        strategy_instance.save_state = MagicMock()

        class _FakeStateMgr:
            async def load_state(self, _sid: str) -> Any:
                return None

        mocks["GatewayStateManager"].return_value = _FakeStateMgr()

        captured_callback: list[Any] = []
        from almanak.connectors import lagoon as lagoon_mod
        from almanak.framework.vault import lifecycle as vlc_mod

        def _capture(**kwargs: Any) -> Any:
            captured_callback.append(kwargs["persistence_callback"])
            return MagicMock()

        monkeypatch.setattr(vlc_mod, "VaultLifecycleManager", _capture)
        monkeypatch.setattr(lagoon_mod, "LagoonVaultSDK", MagicMock())
        monkeypatch.setattr(lagoon_mod, "LagoonVaultAdapter", MagicMock())

        cli = CliRunner()
        with cli.isolation():
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "vault": {
                        "vault_address": "0x" + "a" * 40,
                        "valuator_address": "0x" + "b" * 40,
                        "underlying_token": "USDC",
                        "settlement_interval_minutes": 60,
                    },
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="vault-persist",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        # Invoke the captured callback
        persist = captured_callback[0]
        persist({"foo": "bar"})
        from almanak.framework.vault.lifecycle import VAULT_STATE_KEY

        assert strategy_instance.persistent_state[VAULT_STATE_KEY] == {"foo": "bar"}
        strategy_instance.save_state.assert_called_once()

    def test_vault_state_load_exception_is_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """State manager load_state raising is caught -> initial_vault_state=None (2009-2010)."""
        mocks = _patch_component_factories(monkeypatch)
        mocks["_has_placeholder_vault_address"].return_value = False

        strategy_instance = _make_strategy_instance()

        class _FailingStateMgr:
            async def load_state(self, _sid: str) -> Any:
                raise RuntimeError("load_state blew up")

        mocks["GatewayStateManager"].return_value = _FailingStateMgr()

        captured: dict[str, Any] = {}
        from almanak.connectors import lagoon as lagoon_mod
        from almanak.framework.vault import lifecycle as vlc_mod

        def _capture(**kwargs: Any) -> Any:
            captured.update(kwargs)
            return MagicMock()

        monkeypatch.setattr(vlc_mod, "VaultLifecycleManager", _capture)
        monkeypatch.setattr(lagoon_mod, "LagoonVaultSDK", MagicMock())
        monkeypatch.setattr(lagoon_mod, "LagoonVaultAdapter", MagicMock())

        cli = CliRunner()
        with cli.isolation():
            # Must not raise.
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "arbitrum",
                    "wallet_address": "0xwallet",
                    "vault": {
                        "vault_address": "0x" + "a" * 40,
                        "valuator_address": "0x" + "b" * 40,
                        "underlying_token": "USDC",
                        "settlement_interval_minutes": 60,
                    },
                },
                runtime_config=_make_fake_local_config(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="mainnet",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="vault-exc",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="arbitrum",
            )
        assert captured["initial_vault_state"] is None


# ---------------------------------------------------------------------------
# _build_runtime_config — multi-chain anvil retry failure (1345-1347)
# ---------------------------------------------------------------------------


class TestBuildRuntimeConfigMultiChainAnvil:
    def test_multi_chain_anvil_retry_failure_exits_1(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multi-chain anvil: first call raises MissingEnv, retry raises RuntimeError -> exit 1."""
        call_count = {"n": 0}

        def _factory(
            chains: list[str], protocols: Any, network: str, private_key: str | None = None
        ) -> Any:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _FakeMissingEnvErr("ALMANAK_PRIVATE_KEY")
            raise RuntimeError("retry boom")

        _patch_runtime_config_imports(monkeypatch, multi_factory=_factory)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
        import sys as _sys

        monkeypatch.setattr(_sys.stdin, "isatty", lambda: False)

        cli = CliRunner(mix_stderr=False)
        with cli.isolation() as (_out, err), pytest.raises(SystemExit) as exc_info:
            run_helpers._build_runtime_config(
                no_gateway=False,
                multi_chain=True,
                resolved_network="anvil",
                config_chain=None,
                strategy_chains=["arbitrum", "base"],
                strategy_protocols=[],
                gateway_client=MagicMock(),
                strategy_config={},
            )
        assert exc_info.value.code == 1
        assert "after setting default key" in err.getvalue().decode()


# ---------------------------------------------------------------------------
# _build_orchestrator_and_providers — Solana fork init (1664-1709)
# ---------------------------------------------------------------------------


class TestSolanaForkInit:
    """Solana-specific branch of `_build_orchestrator_and_providers`.

    The helper calls `asyncio.get_event_loop()`, which raises on Python 3.12
    when no loop is active. We install a loop before invoking and tear it
    down at the end so the assertion holds in both xdist and sequential modes.
    """

    @pytest.fixture(autouse=True)
    def _ensure_event_loop(self) -> Any:
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            yield loop
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    def test_solana_anvil_starts_fork_manager_and_funds_wallet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Solana + --network anvil boots SolanaForkManager, funds wallet, attaches to bundle."""
        _patch_component_factories(monkeypatch)

        # Fake SolanaForkManager with trackable async methods.
        class _FakeSolanaForkManager:
            def __init__(self, rpc_url: str, validator_port: int, clone_accounts: list[str]) -> None:
                self.rpc_url = rpc_url
                self.validator_port = validator_port
                self.clone_accounts = clone_accounts
                self.started = False
                self.funded_wallet: tuple[str, Decimal] | None = None
                self.funded_tokens: tuple[str, dict[str, Decimal]] | None = None

            async def start(self) -> bool:
                self.started = True
                return True

            def get_rpc_url(self) -> str:
                return f"http://localhost:{self.validator_port}"

            async def fund_wallet(self, wallet: str, amount: Decimal) -> None:
                self.funded_wallet = (wallet, amount)

            async def fund_tokens(self, wallet: str, tokens: dict[str, Decimal]) -> None:
                self.funded_tokens = (wallet, tokens)

        from almanak.framework.anvil import solana_fork_manager as sfm_mod

        monkeypatch.setattr(sfm_mod, "SolanaForkManager", _FakeSolanaForkManager)

        # Solana uses runtime_config.chain == "solana"
        class _SolanaConfig:
            chain = "solana"
            execution_address = "SolanaWallet11111111111111111111111"
            wallet_address = "SolanaWallet11111111111111111111111"
            max_gas_price_gwei = 0
            rpc_url = None
            is_safe_mode = False

        strategy_instance = _make_strategy_instance()

        # Monkey-patch LocalRuntimeConfig isinstance test by swapping its base.
        from almanak.framework.execution import config as exec_config

        monkeypatch.setattr(
            exec_config, "LocalRuntimeConfig", _SolanaConfig, raising=True
        )
        monkeypatch.setattr(
            exec_config, "GatewayRuntimeConfig", _SolanaConfig, raising=True
        )

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            bundle = run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={
                    "chain": "solana",
                    "wallet_address": "SolanaWallet11111111111111111111111",
                    "pool_address": "Pool111",
                    "pool_a_address": "PoolA111",
                },
                runtime_config=_SolanaConfig(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="anvil",  # key: triggers Solana block
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="solana-test",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="solana",
            )
        assert isinstance(bundle.solana_fork_mgr, _FakeSolanaForkManager)
        assert bundle.solana_fork_mgr.started is True
        assert bundle.solana_fork_mgr.funded_wallet is not None
        assert bundle.solana_fork_mgr.funded_wallet[1] == Decimal("100")
        assert bundle.solana_fork_mgr.funded_tokens is not None
        assert bundle.solana_fork_mgr.funded_tokens[1] == {
            "USDC": Decimal("10000"),
            "USDT": Decimal("10000"),
        }
        out_text = out.getvalue().decode()
        assert "solana-test-validator" in out_text

    def test_solana_fork_start_failure_raises_click_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SolanaForkManager.start() returning False -> ClickException (1690-1695)."""
        _patch_component_factories(monkeypatch)

        class _FailingForkMgr:
            def __init__(self, **_kw: Any) -> None:
                pass

            async def start(self) -> bool:
                return False

            def get_rpc_url(self) -> str:
                return ""

            async def fund_wallet(self, *a: Any, **kw: Any) -> None:
                pass

            async def fund_tokens(self, *a: Any, **kw: Any) -> None:
                pass

        from almanak.framework.anvil import solana_fork_manager as sfm_mod

        monkeypatch.setattr(sfm_mod, "SolanaForkManager", _FailingForkMgr)

        class _SolanaConfig:
            chain = "solana"
            execution_address = "Wallet111"
            wallet_address = "Wallet111"
            max_gas_price_gwei = 0
            rpc_url = None
            is_safe_mode = False

        from almanak.framework.execution import config as exec_config

        monkeypatch.setattr(exec_config, "LocalRuntimeConfig", _SolanaConfig, raising=True)
        monkeypatch.setattr(exec_config, "GatewayRuntimeConfig", _SolanaConfig, raising=True)

        strategy_instance = _make_strategy_instance()

        cli = CliRunner()
        # The outer _build_components catches non-ClickException -> SystemExit(1).
        # ClickException propagates through intact.
        with cli.isolation(), pytest.raises((click.ClickException, SystemExit)):
            run_helpers._build_components(
                strategy_instance=strategy_instance,
                strategy_config={"chain": "solana", "wallet_address": "Wallet111"},
                runtime_config=_SolanaConfig(),
                strategy_chains=[],
                multi_chain=False,
                resolved_network="anvil",
                gateway_client=MagicMock(),
                chain_wallets={},
                interval=60,
                effective_dry_run=False,
                deployment_id="solana-fail",
                normalized_copy_mode=None,
                copy_replay_file=None,
                copy_shadow=False,
                copy_strict=False,
                config_chain="solana",
            )


# ---------------------------------------------------------------------------
# _build_cleanup_fn — exception paths
# ---------------------------------------------------------------------------


class TestBuildCleanupFnExceptions:
    def test_solana_fork_stop_exception_is_swallowed(self) -> None:
        """solana_fork_mgr.stop() raising is caught and logged (1049-1050)."""
        import asyncio

        fork_mgr = MagicMock()
        fork_mgr.stop = AsyncMock(side_effect=RuntimeError("validator refused to stop"))
        components = ComponentBundle(solana_fork_mgr=fork_mgr)
        cleanup = run_helpers._build_cleanup_fn(
            gateway_client=None,
            managed_gateway=None,
            keep_anvil=False,
            components=components,
        )
        # Must not raise.
        cli = CliRunner()
        with cli.isolation():
            asyncio.run(cleanup())
        fork_mgr.stop.assert_awaited_once()
