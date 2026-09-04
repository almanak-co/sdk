"""Branch and boundary tests for ``almanak strat permissions``."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner

from almanak.core.chains import DEFAULT_CHAIN
from almanak.core.intent_types import IntentType
from almanak.framework.cli import permissions as permissions_module
from almanak.framework.cli.permissions import (
    _discover_manifests,
    _render_manifests,
    _resolve_cli_inputs,
    _resolve_strategy_chains,
    _select_output_chains,
    _StrategyInputs,
    permissions,
)
from almanak.framework.permissions.models import ContractPermission, FunctionPermission, PermissionManifest


def _metadata(**overrides: Any) -> SimpleNamespace:
    values = {
        "name": "test_strategy",
        "supported_protocols": ["spark"],
        "intent_types": [IntentType.SUPPLY],
        "supported_chains": ["base"],
        "default_chain": "base",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _strategy_class(metadata: SimpleNamespace | None) -> type:
    return type("TestStrategy", (), {"STRATEGY_METADATA": metadata})


def _manifest(chain: str, *, warning: str | None = None) -> PermissionManifest:
    return PermissionManifest(
        version="1.0",
        chain=chain,
        strategy="test_strategy",
        generated_at="2026-01-01T00:00:00+00:00",
        warnings=[warning] if warning else [],
        permissions=[
            ContractPermission(
                target="0x0000000000000000000000000000000000000001",
                label="Test",
                function_selectors=[FunctionPermission(selector="0x12345678", label="test()")],
            )
        ],
    )


def _patch_successful_cli(
    monkeypatch: pytest.MonkeyPatch,
    metadata: SimpleNamespace,
    *,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    import almanak.framework.permissions.generator as generator

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(permissions_module, "_load_dotenv", lambda _path: None)
    monkeypatch.setattr(
        permissions_module,
        "load_strategy_from_file",
        lambda _path: (_strategy_class(metadata), None),
    )
    monkeypatch.setattr(generator, "load_strategy_config", lambda _path: config or {})
    monkeypatch.setattr(generator, "discover_teardown_protocols", lambda *_args, **_kwargs: (set(), []))

    def generate_manifest(**kwargs: Any) -> PermissionManifest:
        calls.append(kwargs)
        return _manifest(kwargs["chain"])

    monkeypatch.setattr(generator, "generate_manifest", generate_manifest)
    monkeypatch.setattr(permissions_module, "_resolve_rpc_url", lambda explicit, _chain: explicit)
    return calls


def test_resolve_cli_inputs_resolves_working_path_and_preserves_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loaded_from: list[Path] = []
    monkeypatch.setattr(permissions_module, "_load_dotenv", loaded_from.append)

    options = _resolve_cli_inputs(
        str(tmp_path / ".." / tmp_path.name),
        "base",
        "manifest.json",
        "manifest",
        "https://rpc.example",
    )

    assert options.working_path == tmp_path.resolve()
    assert loaded_from == [tmp_path.resolve()]
    assert (options.chain, options.output, options.output_format, options.rpc_url) == (
        "base",
        "manifest.json",
        "manifest",
        "https://rpc.example",
    )


@pytest.mark.parametrize(
    ("explicit", "supported", "default", "expected"),
    [
        ("optimism", ["base", "arbitrum"], "ethereum", ["optimism"]),
        (None, ["base", "arbitrum"], "ethereum", ["base", "arbitrum"]),
        (None, [], "ethereum", ["ethereum"]),
        (None, [], "", [DEFAULT_CHAIN]),
    ],
)
def test_resolve_strategy_chains_preserves_precedence(
    explicit: str | None, supported: list[str], default: str, expected: list[str]
) -> None:
    assert (
        _resolve_strategy_chains(
            explicit,
            _metadata(supported_chains=supported, default_chain=default),
        )
        == expected
    )


def test_select_output_chains_filters_zodiac_fail_closed(capsys: pytest.CaptureFixture[str]) -> None:
    chains = ["solana", "unknown-chain", "base"]

    assert _select_output_chains(chains, "manifest") is chains
    assert _select_output_chains(chains, "zodiac") == ["base"]
    assert capsys.readouterr().err == (
        "  Skipping solana (non-EVM, Zodiac not applicable)\n"
        "  Skipping unknown-chain (unknown chain, cannot verify EVM)\n"
    )


@pytest.mark.parametrize(
    ("output_format", "chains", "expected"),
    [
        (
            "zodiac",
            ["base"],
            [
                {
                    "address": "0x0000000000000000000000000000000000000001",
                    "clearance": 2,
                    "executionOptions": 0,
                    "functions": [{"selector": "0x12345678", "wildcarded": True}],
                }
            ],
        ),
        (
            "zodiac",
            ["base", "arbitrum"],
            {
                "base": [
                    {
                        "address": "0x0000000000000000000000000000000000000001",
                        "clearance": 2,
                        "executionOptions": 0,
                        "functions": [{"selector": "0x12345678", "wildcarded": True}],
                    }
                ],
                "arbitrum": [
                    {
                        "address": "0x0000000000000000000000000000000000000001",
                        "clearance": 2,
                        "executionOptions": 0,
                        "functions": [{"selector": "0x12345678", "wildcarded": True}],
                    }
                ],
            },
        ),
        ("manifest", ["base"], _manifest("base").to_dict()),
        ("manifest", ["base", "arbitrum"], [_manifest("base").to_dict(), _manifest("arbitrum").to_dict()]),
    ],
)
def test_render_manifests_stdout_table(output_format: str, chains: list[str], expected: object) -> None:
    @click.command()
    def render() -> None:
        _render_manifests([_manifest(chain) for chain in chains], output_format, None)

    result = CliRunner().invoke(render)

    assert result.exit_code == 0
    assert json.loads(result.output) == expected


@pytest.mark.parametrize(
    ("output_format", "expected_message"),
    [("zodiac", "Zodiac targets written to"), ("manifest", "Manifest written to")],
)
def test_render_manifests_file_table(tmp_path: Path, output_format: str, expected_message: str) -> None:
    output_path = tmp_path / "permissions.json"

    @click.command()
    def render() -> None:
        _render_manifests([_manifest("base")], output_format, str(output_path))

    result = CliRunner().invoke(render)

    assert result.exit_code == 0
    assert expected_message in result.output
    expected = _manifest("base").to_zodiac_targets() if output_format == "zodiac" else _manifest("base").to_dict()
    assert json.loads(output_path.read_text()) == expected


@pytest.mark.parametrize(
    ("loader_result", "expected"),
    [
        ((None, "could not import"), "Error loading strategy: could not import"),
        ((_strategy_class(None), None), "Error: Strategy has no STRATEGY_METADATA"),
    ],
)
def test_cli_strategy_loading_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    loader_result: tuple[type | None, str | None],
    expected: str,
) -> None:
    (tmp_path / "strategy.py").write_text("# stub\n")
    monkeypatch.setattr(permissions_module, "_load_dotenv", lambda _path: None)
    monkeypatch.setattr(permissions_module, "load_strategy_from_file", lambda _path: loader_result)

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path)])

    assert result.exit_code == 1
    assert expected in result.output


def test_cli_missing_strategy_file_exits_before_loading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(permissions_module, "_load_dotenv", lambda _path: None)

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path)])

    assert result.exit_code == 1
    assert result.output == f"Error: No strategy.py found in {tmp_path.resolve()}\n"


def test_config_loading_failure_precedes_zodiac_chain_filtering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import almanak.framework.permissions.generator as generator

    (tmp_path / "strategy.py").write_text("# stub\n")
    monkeypatch.setattr(permissions_module, "_load_dotenv", lambda _path: None)
    monkeypatch.setattr(
        permissions_module,
        "load_strategy_from_file",
        lambda _path: (_strategy_class(_metadata(supported_chains=["solana"])), None),
    )

    def fail_config(_path: Path) -> dict[str, Any]:
        raise RuntimeError("config failure")

    monkeypatch.setattr(generator, "load_strategy_config", fail_config)

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "config failure"
    assert "Skipping solana" not in result.output


def test_cli_preserves_empty_metadata_warnings_and_strategy_name_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "strategy.py").write_text("# stub\n")
    calls = _patch_successful_cli(
        monkeypatch,
        _metadata(name="", supported_protocols=[], intent_types=[]),
    )

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path), "--format", "manifest"])

    assert result.exit_code == 0
    assert calls[0]["strategy_name"] == "TestStrategy"
    assert result.output.index("Warning: No supported_protocols") < result.output.index("Warning: No intent_types")
    assert result.output.index("Warning: No intent_types") < result.output.index("Generating permissions")


@pytest.mark.parametrize("use_output_file", [False, True])
def test_cli_empty_zodiac_result_for_non_evm_strategy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, use_output_file: bool
) -> None:
    (tmp_path / "strategy.py").write_text("# stub\n")
    calls = _patch_successful_cli(monkeypatch, _metadata(supported_chains=["solana"], default_chain="solana"))
    output_path = tmp_path / "targets.json"
    args = ["-d", str(tmp_path)]
    if use_output_file:
        args.extend(["--output", str(output_path)])

    result = CliRunner().invoke(permissions, args)

    assert result.exit_code == 0
    assert calls == []
    assert "Skipping solana (non-EVM, Zodiac not applicable)" in result.output
    assert "No EVM chains to generate permissions for." in result.output
    if use_output_file:
        assert output_path.read_text() == "[]"
        assert f"Empty zodiac targets written to {output_path}" in result.output
    else:
        assert result.output.endswith("[]\n")


@pytest.mark.parametrize(
    ("chains", "expected_exit", "expected_calls"),
    [
        (["base", "arbitrum"], 1, 0),
        (["base", "solana"], 0, 1),
    ],
)
def test_explicit_rpc_validation_runs_after_zodiac_chain_filtering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    chains: list[str],
    expected_exit: int,
    expected_calls: int,
) -> None:
    (tmp_path / "strategy.py").write_text("# stub\n")
    calls = _patch_successful_cli(monkeypatch, _metadata(supported_chains=chains))

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path), "--rpc-url", "https://rpc.example"])

    assert result.exit_code == expected_exit
    assert len(calls) == expected_calls
    if expected_exit:
        assert "--rpc-url cannot be used with multiple chains (base, arbitrum)" in result.output


def test_discovery_is_chain_scoped_and_reports_teardown_and_manifest_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import almanak.framework.permissions.generator as generator

    generated: list[dict[str, Any]] = []
    inputs = _StrategyInputs(
        strategy_class=_strategy_class(_metadata()),
        strategy_name="test_strategy",
        protocols=["Spark"],
        declared_protocols_lower={"spark"},
        intent_types=["SUPPLY"],
        chains=["base", "arbitrum"],
        config={"token": "USDC"},
    )

    def teardown(_strategy: type, chain: str, *, config: dict[str, Any]) -> tuple[set[str], list[str]]:
        assert config == inputs.config
        if chain == "base":
            return {"enso"}, ["teardown warning"]
        return set(), []

    def generate(**kwargs: Any) -> PermissionManifest:
        generated.append(kwargs)
        return _manifest(kwargs["chain"], warning="manifest warning")

    monkeypatch.setattr(generator, "discover_teardown_protocols", teardown)
    monkeypatch.setattr(generator, "generate_manifest", generate)
    monkeypatch.setattr(
        permissions_module,
        "_resolve_rpc_url",
        lambda _explicit, chain: f"https://{chain}.rpc",
    )

    @click.command()
    def discover() -> None:
        manifests = _discover_manifests(inputs, inputs.chains, None)
        click.echo(",".join(manifest.chain for manifest in manifests))

    result = CliRunner().invoke(discover)

    assert result.exit_code == 0
    assert result.output.endswith("base,arbitrum\n")
    assert [call["chain"] for call in generated] == ["base", "arbitrum"]
    assert set(generated[0]["supported_protocols"]) == {"Spark", "enso"}
    assert generated[1]["supported_protocols"] == ["Spark"]
    assert [call["rpc_url"] for call in generated] == ["https://base.rpc", "https://arbitrum.rpc"]
    assert result.output.index("teardown warning") < result.output.index("Teardown on base")
    assert result.output.count("manifest warning") == 2
    assert result.output.count("Found 1 contract permissions with 1 selectors") == 2


def test_malformed_config_fallback_remains_compatible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import almanak.framework.permissions.generator as generator

    (tmp_path / "strategy.py").write_text("# stub\n")
    (tmp_path / "config.json").write_text("{not-json")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(permissions_module, "_load_dotenv", lambda _path: None)
    monkeypatch.setattr(
        permissions_module,
        "load_strategy_from_file",
        lambda _path: (_strategy_class(_metadata()), None),
    )
    monkeypatch.setattr(generator, "discover_teardown_protocols", lambda *_args, **_kwargs: (set(), []))

    def generate_manifest(**kwargs: Any) -> PermissionManifest:
        calls.append(kwargs)
        return _manifest(kwargs["chain"])

    monkeypatch.setattr(generator, "generate_manifest", generate_manifest)
    monkeypatch.setattr(permissions_module, "_resolve_rpc_url", lambda _explicit, _chain: None)

    result = CliRunner().invoke(permissions, ["-d", str(tmp_path)])

    assert result.exit_code == 0
    assert calls[0]["config"] == {}
    assert "Failed to read" in caplog.text
