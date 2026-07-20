"""VIB-5920 — the config ``network`` key must not be decorative.

``uv run almanak strat run -c config-anvil.json`` (no ``--network``) used to
boot **mainnet** while the config assumed an Anvil fork — a real-money footgun,
and a direct contradiction of the ``--network`` help text ("Overrides
config.json 'network' field").

These tests pin:

* the precedence ladder of ``_network_resolution.resolve_network``;
* loud failure on a malformed / typo'd config value (never a silent mainnet);
* hosted mode ignoring the key (the platform owns the network);
* single-sourcing — the three CLI sites must not re-grow their own
  ``network or "mainnet"`` default (static source guard).
"""

from __future__ import annotations

import ast
from pathlib import Path

import click
import pytest

from almanak.framework.cli._network_resolution import ResolvedNetwork, resolve_network

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "almanak" / "framework" / "cli"
RUN_GATEWAY = CLI / "_run_gateway.py"
RUN_MODES = CLI / "_run_modes.py"
TEARDOWN_HELPERS = CLI / "teardown_helpers.py"
# The wider CLI boundary (coderabbit review): these files orchestrate the three
# resolution sites — a hand-rolled default sneaking into any of them is the same
# split-brain regression, so the no-local-default scan covers them too.
CLI_BOUNDARY_FILES = (
    RUN_GATEWAY,
    RUN_MODES,
    TEARDOWN_HELPERS,
    CLI / "run.py",
    CLI / "run_helpers.py",
    CLI / "teardown.py",
)


@pytest.fixture(autouse=True)
def _local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test to LOCAL mode; a stray shell/.env var must not leak in."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)


class TestPrecedence:
    def test_flag_wins_over_config(self) -> None:
        resolved = resolve_network(flag_network="mainnet", strategy_config={"network": "anvil"})
        assert resolved == ResolvedNetwork(network="mainnet", source="flag")

    def test_flag_is_normalized(self) -> None:
        assert resolve_network(flag_network="ANVIL", strategy_config=None).network == "anvil"

    def test_config_wins_over_default(self) -> None:
        resolved = resolve_network(flag_network=None, strategy_config={"network": "anvil"})
        assert resolved == ResolvedNetwork(network="anvil", source="config")
        assert resolved.from_config is True

    def test_config_mainnet_is_reported_as_config_sourced(self) -> None:
        resolved = resolve_network(flag_network=None, strategy_config={"network": "MAINNET "})
        assert resolved == ResolvedNetwork(network="mainnet", source="config")

    def test_default_is_mainnet_when_nothing_set(self) -> None:
        resolved = resolve_network(flag_network=None, strategy_config={})
        assert resolved == ResolvedNetwork(network="mainnet", source="default")
        assert resolved.from_config is False

    def test_no_config_at_all_defaults_to_mainnet(self) -> None:
        assert resolve_network(flag_network=None, strategy_config=None).network == "mainnet"

    def test_anvil_ports_inference_beats_config(self) -> None:
        resolved = resolve_network(
            flag_network=None,
            anvil_ports_present=True,
            strategy_config={"network": "mainnet"},
        )
        assert resolved == ResolvedNetwork(network="anvil", source="anvil-ports")

    def test_anvil_ports_inference_loses_to_flag(self) -> None:
        resolved = resolve_network(
            flag_network="mainnet",
            anvil_ports_present=True,
            strategy_config={"network": "anvil"},
        )
        assert resolved == ResolvedNetwork(network="mainnet", source="flag")

    def test_anvil_ports_inference_disabled_under_no_gateway(self) -> None:
        """`--no-gateway` owns the gateway; the anvil-port shortcut must not fire.

        (Pre-existing `_setup_gateway` behaviour, preserved verbatim.)
        """
        resolved = resolve_network(
            flag_network=None,
            anvil_ports_present=True,
            no_gateway=True,
            strategy_config={},
        )
        assert resolved == ResolvedNetwork(network="mainnet", source="default")

    def test_no_gateway_still_honours_config(self) -> None:
        resolved = resolve_network(
            flag_network=None,
            anvil_ports_present=True,
            no_gateway=True,
            strategy_config={"network": "anvil"},
        )
        assert resolved == ResolvedNetwork(network="anvil", source="config")


class TestConfigValueHandling:
    def test_empty_string_is_treated_as_unset(self) -> None:
        assert resolve_network(flag_network=None, strategy_config={"network": ""}).source == "default"

    def test_whitespace_is_treated_as_unset(self) -> None:
        assert resolve_network(flag_network=None, strategy_config={"network": "   "}).source == "default"

    def test_explicit_none_is_treated_as_unset(self) -> None:
        assert resolve_network(flag_network=None, strategy_config={"network": None}).source == "default"

    def test_typo_raises_instead_of_silently_booting_mainnet(self) -> None:
        with pytest.raises(click.ClickException) as exc:
            resolve_network(flag_network=None, strategy_config={"network": "anvi"})
        assert "anvi" in str(exc.value)
        assert "anvil" in str(exc.value)
        assert "mainnet" in str(exc.value)

    def test_non_string_value_raises(self) -> None:
        with pytest.raises(click.ClickException) as exc:
            resolve_network(flag_network=None, strategy_config={"network": {"chain": "anvil"}})
        assert "must be a string" in str(exc.value)

    def test_flag_overrides_even_an_invalid_config_value(self) -> None:
        """An operator with an explicit flag must not be blocked by a bad key."""
        assert resolve_network(flag_network="anvil", strategy_config={"network": "anvi"}).network == "anvil"


class TestLazyConfigLoader:
    def test_loader_not_invoked_when_flag_decides(self) -> None:
        calls: list[int] = []

        def _loader() -> dict[str, str]:
            calls.append(1)
            return {"network": "anvil"}

        assert resolve_network(flag_network="mainnet", config_loader=_loader).network == "mainnet"
        assert calls == []

    def test_loader_not_invoked_when_anvil_ports_decide(self) -> None:
        calls: list[int] = []

        def _loader() -> dict[str, str]:
            calls.append(1)
            return {"network": "mainnet"}

        assert resolve_network(flag_network=None, anvil_ports_present=True, config_loader=_loader).network == "anvil"
        assert calls == []

    def test_loader_used_when_nothing_else_decides(self) -> None:
        resolved = resolve_network(flag_network=None, config_loader=lambda: {"network": "anvil"})
        assert resolved == ResolvedNetwork(network="anvil", source="config")

    def test_loader_returning_none_falls_through_to_default(self) -> None:
        assert resolve_network(flag_network=None, config_loader=lambda: None).source == "default"


class TestHostedMode:
    def test_hosted_ignores_config_and_warns(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        resolved = resolve_network(flag_network=None, strategy_config={"network": "anvil"})
        assert resolved == ResolvedNetwork(network="mainnet", source="default")
        out = capsys.readouterr().out
        assert "HOSTED" in out
        assert "ignored" in out

    def test_hosted_does_not_validate_config_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A decorative typo must never fail a hosted boot — the key is ignored."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "1")
        assert resolve_network(flag_network=None, strategy_config={"network": "anvi"}).network == "mainnet"

    def test_hosted_never_invokes_the_config_loader(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "yes")
        calls: list[int] = []

        def _loader() -> dict[str, str]:
            calls.append(1)
            return {"network": "anvil"}

        assert resolve_network(flag_network=None, config_loader=_loader).network == "mainnet"
        assert calls == []

    def test_hosted_still_honours_an_explicit_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "on")
        assert resolve_network(flag_network="anvil", strategy_config=None).network == "anvil"

    def test_hosted_mainnet_config_emits_no_notice(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        resolve_network(flag_network=None, strategy_config={"network": "mainnet"})
        assert capsys.readouterr().out == ""


class TestSingleSourcingGuard:
    """Static guards — the three CLI sites must keep delegating to the resolver.

    Mirrors the source-inspection idiom of
    ``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``.
    """

    @pytest.mark.parametrize("path", CLI_BOUNDARY_FILES)
    def test_no_local_mainnet_default_remains(self, path: Path) -> None:
        # RUN_MODES no longer resolves at all (it consumes gateway_network), but
        # it stays in the scan: a re-introduced local default there would be the
        # split-brain regression this guard exists for. Both quote styles are
        # checked — a single-quoted `or 'mainnet'` is the same landmine
        # (review: gemini medium / coderabbit minor).
        src = path.read_text(encoding="utf-8")
        offenders = [
            f"{path.name}:{i}"
            for i, line in enumerate(src.splitlines(), 1)
            if 'or "mainnet"' in line.split("#")[0] or "or 'mainnet'" in line.split("#")[0]
        ]
        assert not offenders, (
            f'VIB-5920: re-introduced a local `or "mainnet"` network default at {offenders}. '
            "Route the resolution through almanak.framework.cli._network_resolution.resolve_network."
        )

    @staticmethod
    def _calls_in(path: Path, func: str) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        fn = next(
            (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef) and n.name == func),
            None,
        )
        assert fn is not None, f"{func} not found in {path.name}"
        return {node.func.id for node in ast.walk(fn) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}

    @pytest.mark.parametrize(
        ("path", "func"),
        [
            (RUN_GATEWAY, "_resolve_gateway_network"),
            (TEARDOWN_HELPERS, "setup_gateway"),
        ],
    )
    def test_site_calls_the_shared_resolver(self, path: Path, func: str) -> None:
        assert "resolve_network" in self._calls_in(path, func), (
            f"VIB-5920: {path.name}:{func} no longer calls resolve_network — the CLI "
            "network-resolution sites must stay single-sourced or they drift apart again."
        )

    def test_run_lane_resolves_exactly_once(self) -> None:
        """The runtime bootstrap must CONSUME the gateway's resolution, not redo it.

        A second resolution in ``_run_modes`` can legitimately disagree: the
        runtime's config load falls back to ``load_strategy_config(<ClassName>)``
        → ``find_strategy_dir``, which may resolve a DIFFERENT directory's
        config.json than the gateway's pre-boot peek ever saw.
        """
        src = RUN_MODES.read_text(encoding="utf-8")
        assert "resolve_network(" not in src, (
            "VIB-5920: _run_modes re-resolves the network. It must consume "
            "`gateway_network` produced by _setup_gateway (one resolution per process)."
        )
        assert "_echo_runtime_network" in src


class TestQuickConfigEnvOverrideParity:
    """VIB-5920 codex P1: the gateway-setup pre-boot peek must apply the same
    ``ALMANAK_STRATEGY_CONFIG`` deep-merge the canonical loader applies
    unconditionally (``run.py:_apply_env_strategy_config_override``). A peek
    that read only the on-disk file would resolve the gateway network (and the
    Anvil chain/funding probes) from a DIFFERENT config than the runtime
    bootstrap — a local run with the env override set could boot the gateway on
    mainnet while the runtime believes it is on an Anvil fork.
    """

    @staticmethod
    def _write_config(tmp_path, extra: str = "") -> None:
        (tmp_path / "config.json").write_text(
            f'{{"deployment_id": "t", "strategy_name": "t", "chain": "ethereum"{extra}}}'
        )

    def test_env_override_reaches_quick_config(self, tmp_path, monkeypatch, capsys) -> None:
        from almanak.framework.cli._run_gateway import _load_quick_config

        self._write_config(tmp_path)
        monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", '{"network": "anvil"}')
        quick = _load_quick_config(str(tmp_path), None)
        assert quick is not None
        assert quick.get("network") == "anvil", (
            "the quick-config peek must see the env-merged config, or the gateway "
            "resolves a different network than the runtime bootstrap"
        )
        # The visible notice belongs to the canonical load only (echo=False here).
        assert "Applied" not in capsys.readouterr().out

    def test_gateway_network_resolves_from_env_override(self, tmp_path, monkeypatch) -> None:
        from almanak.framework.cli._run_gateway import _quick_config_loader, _resolve_gateway_network

        self._write_config(tmp_path)
        monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", '{"network": "anvil"}')
        resolved = _resolve_gateway_network(
            network=None,
            anvil_ports=(),
            no_gateway=False,
            load_quick_config=_quick_config_loader(str(tmp_path), None),
        )
        assert resolved.network == "anvil"
        assert resolved.source == "config"

    def test_disk_config_still_wins_shape_without_env(self, tmp_path, monkeypatch) -> None:
        from almanak.framework.cli._run_gateway import _load_quick_config

        self._write_config(tmp_path, ', "network": "anvil"')
        monkeypatch.delenv("ALMANAK_STRATEGY_CONFIG", raising=False)
        quick = _load_quick_config(str(tmp_path), None)
        assert quick is not None
        assert quick.get("network") == "anvil"


class TestChainConfusionHint:
    """A `network` value that is really a CHAIN name gets targeted guidance."""

    def test_chain_name_gets_targeted_message(self) -> None:
        with pytest.raises(click.ClickException) as exc:
            resolve_network(flag_network=None, strategy_config={"network": "base"})
        message = str(exc.value)
        assert "'chain' / 'chains'" in message
        assert "mainnet vs a local fork" in message

    def test_plain_typo_gets_no_chain_hint(self) -> None:
        with pytest.raises(click.ClickException) as exc:
            resolve_network(flag_network=None, strategy_config={"network": "anvi"})
        assert "'chain' / 'chains'" not in str(exc.value)


class TestGatewayAuthPosture:
    """VIB-5920 audit blocker: a config file must never disarm gateway auth.

    `_build_gateway_settings` drops the AuthInterceptor (`allow_insecure=True`,
    `auth_token=None`) on test networks for local-dev convenience. That posture
    is safe only when a human asked for a fork on THIS invocation. A copied /
    committed `"network": "anvil"` reaching the same branch would silently run
    an unauthenticated gateway that may hold the real ALMANAK_PRIVATE_KEY.
    """

    @staticmethod
    def _settings(*, network: str, operator_signalled: bool):
        from almanak.framework.cli._run_gateway import _build_gateway_settings

        return _build_gateway_settings(
            effective_host="127.0.0.1",
            gateway_port=50099,
            gateway_network=network,
            gateway_chains=["arbitrum"],
            gateway_private_key=None,
            operator_signalled_network=operator_signalled,
        )

    def test_config_sourced_anvil_keeps_auth(self) -> None:
        settings, token = self._settings(network="anvil", operator_signalled=False)
        assert token, "config-sourced anvil must still mint a session auth token"
        assert settings.auth_token == token
        assert settings.allow_insecure is False

    def test_flag_sourced_anvil_keeps_legacy_insecure_posture(self) -> None:
        settings, token = self._settings(network="anvil", operator_signalled=True)
        assert token is None
        assert settings.auth_token is None
        assert settings.allow_insecure is True

    def test_mainnet_always_authenticated(self) -> None:
        for signalled in (True, False):
            settings, token = self._settings(network="mainnet", operator_signalled=signalled)
            assert token and settings.auth_token == token
            assert settings.allow_insecure is False

    def test_resolved_network_exposes_operator_signal(self) -> None:
        assert resolve_network(flag_network="anvil", strategy_config=None).operator_signalled is True
        assert (
            resolve_network(flag_network=None, anvil_ports_present=True, strategy_config=None).operator_signalled
            is True
        )
        assert resolve_network(flag_network=None, strategy_config={"network": "anvil"}).operator_signalled is False
        assert resolve_network(flag_network=None, strategy_config={}).operator_signalled is False


class TestTeardownAuthPosture:
    """Same blocker, teardown lane (`teardown_helpers.setup_gateway`)."""

    def test_source_gates_allow_insecure(self) -> None:
        src = TEARDOWN_HELPERS.read_text(encoding="utf-8")
        assert 'allow_insecure = resolved_network == "anvil" and resolved.operator_signalled' in src, (
            "teardown must gate allow_insecure on an operator-typed --network anvil, not merely on the resolved network"
        )
        assert "allow_insecure=allow_insecure," in src

    def test_config_sourced_anvil_is_not_operator_signalled(self) -> None:
        # The exact call teardown makes: flag absent, config declares anvil.
        resolved = resolve_network(flag_network=None, strategy_config={"network": "anvil"})
        assert resolved.network == "anvil"
        assert resolved.operator_signalled is False

    def test_flag_sourced_anvil_is_operator_signalled(self) -> None:
        resolved = resolve_network(flag_network="anvil", strategy_config={"network": "anvil"})
        assert resolved.operator_signalled is True


class TestGatewayRuntimeAgreement:
    """The gateway network and the runtime network must be the same value.

    Complements TestQuickConfigEnvOverrideParity (env-override case) with the
    plain on-disk config.json case, end to end through both helpers.
    """

    @staticmethod
    def _write(tmp_path, network: str) -> None:
        (tmp_path / "config.json").write_text(
            f'{{"deployment_id": "t", "strategy_name": "t", "chain": "arbitrum", "network": "{network}"}}'
        )

    @pytest.mark.parametrize("network", ["anvil", "mainnet"])
    def test_runtime_consumes_the_gateway_resolution(self, tmp_path, monkeypatch, network: str) -> None:
        from almanak.framework.cli._run_gateway import _quick_config_loader, _resolve_gateway_network
        from almanak.framework.cli._run_modes import _echo_runtime_network

        monkeypatch.delenv("ALMANAK_STRATEGY_CONFIG", raising=False)
        self._write(tmp_path, network)
        gateway_network = _resolve_gateway_network(
            network=None,
            anvil_ports=(),
            no_gateway=False,
            load_quick_config=_quick_config_loader(str(tmp_path), None),
        ).network
        assert gateway_network == network
        # The runtime path is now a pure echo of the same value.
        assert _echo_runtime_network(resolved_network=gateway_network, config_chain="arbitrum") == network

    def test_anvil_banner_is_byte_identical_regardless_of_source(self, capsys) -> None:
        from almanak.framework.cli._run_modes import _echo_runtime_network

        _echo_runtime_network(resolved_network="anvil", config_chain="arbitrum")
        out = capsys.readouterr().out
        assert out.startswith("Network: ANVIL (local fork at http://127.0.0.1:")
        assert "[from config.json" not in out, "source attribution belongs to the pre-boot gateway echo"


class TestPreBootEcho:
    """Implicit resolutions are announced BEFORE the gateway/fork start."""

    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            ("config", "Network: ANVIL (resolved from config.json 'network')"),
            ("anvil-ports", "Network: ANVIL (inferred from --anvil-port)"),
        ],
    )
    def test_implicit_sources_are_announced(self, capsys, source: str, expected: str) -> None:
        from almanak.framework.cli._run_gateway import _echo_resolved_network

        _echo_resolved_network(ResolvedNetwork(network="anvil", source=source))
        assert capsys.readouterr().out.strip() == expected

    @pytest.mark.parametrize("source", ["flag", "default"])
    def test_explicit_sources_stay_silent(self, capsys, source: str) -> None:
        from almanak.framework.cli._run_gateway import _echo_resolved_network

        _echo_resolved_network(ResolvedNetwork(network="anvil", source=source))
        assert capsys.readouterr().out == ""
