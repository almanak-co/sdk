"""Tests for ``load_strategy_config`` (Phase 3, #2098 / #2101).

The loader lives at ``almanak/framework/cli/run.py``. These tests cover the
Phase 3 contract:

* Valid JSON / YAML loads as a dict (Pydantic-validated).
* Read failures (``OSError`` / ``UnicodeDecodeError``), parse errors
  (``json.JSONDecodeError`` / ``yaml.YAMLError``), and schema violations all
  surface as ``click.ClickException`` naming the file path — no opaque stack
  traces, no silent swallow further upstream in ``_setup_gateway``.
"""

from __future__ import annotations

import json
from pathlib import Path

import click
import pytest

from almanak.framework.cli.run import load_strategy_config


def test_load_valid_json(tmp_path: Path) -> None:
    """A well-formed JSON config loads as a dict."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "max_slippage": "0.005"}))
    config = load_strategy_config("any", str(cfg_file))
    assert isinstance(config, dict)
    assert config["chain"] == "arbitrum"
    # Stringly-typed numeric coerces to Decimal via the schema.
    from decimal import Decimal as _Decimal
    assert config["max_slippage"] == _Decimal("0.005")


def test_load_invalid_json_reports_path(tmp_path: Path) -> None:
    """Malformed JSON surfaces as ClickException naming the file path."""
    cfg_file = tmp_path / "broken.json"
    # Trailing comma is invalid JSON.
    cfg_file.write_text('{"chain": "arbitrum",}')
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    msg = exc_info.value.message
    assert str(cfg_file) in msg
    assert "Failed to read" in msg


def test_load_invalid_yaml_reports_path(tmp_path: Path) -> None:
    """Malformed YAML surfaces as ClickException naming the file path."""
    cfg_file = tmp_path / "broken.yaml"
    # Unbalanced quote -> YAML parse error.
    cfg_file.write_text('chain: "arbitrum\nfoo: bar')
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    msg = exc_info.value.message
    assert str(cfg_file) in msg
    assert "Failed to read" in msg


def test_load_non_object_top_level_rejected(tmp_path: Path) -> None:
    """A top-level JSON list (not an object) is rejected with a clear error."""
    cfg_file = tmp_path / "list.json"
    cfg_file.write_text(json.dumps([1, 2, 3]))
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    msg = exc_info.value.message
    assert str(cfg_file) in msg
    assert "must be a JSON/YAML object" in msg


def test_load_undecodable_bytes_reports_path(tmp_path: Path) -> None:
    """A binary file masquerading as JSON surfaces as ClickException, not raw UnicodeDecodeError."""
    cfg_file = tmp_path / "binary.json"
    # Bytes that aren't valid UTF-8.
    cfg_file.write_bytes(b"\xff\xfe\x00\x00bad")
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    msg = exc_info.value.message
    assert str(cfg_file) in msg
    assert "Failed to read" in msg


def test_load_validation_error_reports_path(tmp_path: Path) -> None:
    """Schema-validation failures cite the file and the Pydantic message."""
    cfg_file = tmp_path / "exclusive.json"
    # Both chain and chains set -> mutually-exclusive validator fails.
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "chains": ["arbitrum"]}))
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    msg = exc_info.value.message
    assert str(cfg_file) in msg
    assert "schema validation" in msg


# --- ALMANAK_STRATEGY_CONFIG env override (hosted V2 platform) -------------
#
# Hosted V2 deployments inject the user's UI-edited config as the
# ``ALMANAK_STRATEGY_CONFIG`` env var on the strategy container. Without the
# overlay applied here, the strategy silently runs the in-repo config.json
# and the UI edits only surface on the dashboard.


def test_env_override_deep_merges_over_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Top-level scalars from env replace; nested dicts merge key-wise."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "chain": "arbitrum",
        "max_slippage": "0.005",
        "risk": {"max_leverage": 3, "daily_loss_limit_usd": 100},
    }))
    monkeypatch.setenv(
        "ALMANAK_STRATEGY_CONFIG",
        json.dumps({
            "max_slippage": "0.01",
            "risk": {"max_leverage": 5},
        }),
    )

    config = load_strategy_config("any", str(cfg_file))

    from decimal import Decimal as _Decimal
    assert config["chain"] == "arbitrum"  # untouched
    assert config["max_slippage"] == _Decimal("0.01")  # env replaced + coerced
    # Nested risk dict: max_leverage replaced, daily_loss_limit_usd preserved
    assert config["risk"]["max_leverage"] == 5
    assert config["risk"]["daily_loss_limit_usd"] == _Decimal("100")


def test_env_override_with_no_file_loads_on_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Env override applies even when no on-disk config is found (hosted minimal default path)."""
    # Pin find_strategy_dir to None so this test is deterministic regardless of
    # whether some future strategy fixture happens to be named the same.
    monkeypatch.setattr("almanak.framework.cli.run.find_strategy_dir", lambda _strategy_name: None)
    monkeypatch.setenv(
        "ALMANAK_STRATEGY_CONFIG",
        json.dumps({"chain": "base", "max_slippage": "0.003"}),
    )
    config = load_strategy_config("strategy_with_no_config_file_anywhere")
    from decimal import Decimal as _Decimal
    assert config["chain"] == "base"
    assert config["max_slippage"] == _Decimal("0.003")
    # Minimal default still in place where env did not provide one
    assert "deployment_id" in config


def test_env_override_unset_is_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unset / empty env var leaves the loaded config untouched."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum"}))
    monkeypatch.delenv("ALMANAK_STRATEGY_CONFIG", raising=False)
    config = load_strategy_config("any", str(cfg_file))
    assert config["chain"] == "arbitrum"

    monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", "   ")
    config = load_strategy_config("any", str(cfg_file))
    assert config["chain"] == "arbitrum"


def test_env_override_invalid_json_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Malformed JSON in env var surfaces as ClickException naming the env var."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum"}))
    monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", '{"chain":')  # truncated
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    assert "ALMANAK_STRATEGY_CONFIG" in exc_info.value.message
    assert "not valid JSON" in exc_info.value.message


def test_env_override_non_object_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A JSON list / scalar in the env var is rejected with a clear error."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum"}))
    monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", json.dumps([1, 2, 3]))
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    assert "ALMANAK_STRATEGY_CONFIG" in exc_info.value.message
    assert "must encode a JSON object" in exc_info.value.message


def test_env_override_schema_validation_error_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Env override that breaks StrategyConfig invariants is rejected post-merge."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum"}))
    # chain + chains together is the mutually-exclusive guard.
    monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", json.dumps({"chains": ["arbitrum"]}))
    with pytest.raises(click.ClickException) as exc_info:
        load_strategy_config("any", str(cfg_file))
    assert "ALMANAK_STRATEGY_CONFIG" in exc_info.value.message
    assert "schema validation" in exc_info.value.message


def test_env_override_empty_object_is_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty-object env var override leaves the loaded config untouched."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum"}))
    monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", "{}")
    config = load_strategy_config("any", str(cfg_file))
    assert config["chain"] == "arbitrum"


# ---------------------------------------------------------------------------
# #2098 / VIB-2867 — warn on unrecognized config keys that look like typos.
# ---------------------------------------------------------------------------


def test_typo_key_warns_with_suggestion(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A near-miss unknown key warns with a 'did you mean' suggestion (the money-bug guard)."""
    cfg_file = tmp_path / "config.json"
    # ``chian`` -> ``chain`` (dist 2); ``totl_value_usd`` -> ``total_value_usd`` (dist 1).
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "chian": "ethereum", "totl_value_usd": "4.0"}))
    with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
        config = load_strategy_config("any", str(cfg_file))
    # extra="allow" — the typo is retained (not dropped), but it is surfaced.
    assert config["chian"] == "ethereum"
    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert any("'chian'" in m and "did you mean 'chain'" in m for m in warnings)
    assert any("'totl_value_usd'" in m and "did you mean 'total_value_usd'" in m for m in warnings)


def test_legit_custom_field_does_not_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A legitimate per-strategy extension field (not near a known field) does NOT warn — no noise."""
    cfg_file = tmp_path / "config.json"
    # ``lp1_range_width_pct`` shares a substring with ``range_width_pct`` but is dist 4 — not a typo.
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "lp1_range_width_pct": "0.1"}))
    with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
        load_strategy_config("any", str(cfg_file))
    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert not any("lp1_range_width_pct" in m for m in warnings)


@pytest.mark.parametrize(
    "extension_key",
    # Real per-strategy extension keys that a plain-Levenshtein<=2 heuristic
    # false-flagged (traderjoe_fee_rotator's pool_a/pool_b vs `pool`,
    # uniswap_v4_hooks' hook_address vs `pool_address`). Each differs from the
    # nearest known field by a whole token (OSA distance >= 2), so the
    # transposition-aware threshold-1 heuristic must stay silent.
    ["pool_a", "pool_b", "hook_address"],
)
def test_real_extension_keys_do_not_warn(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, extension_key: str
) -> None:
    """Whole-token extension keys must not trip the typo warning (regression guard)."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum", extension_key: "x"}))
    with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
        load_strategy_config("any", str(cfg_file))
    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert not any(extension_key in m for m in warnings)


def test_transposition_typo_is_flagged(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """An adjacent-swap typo (OSA distance 1) is still caught under the tighter threshold."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chian": "arbitrum"}))  # chian <-> chain, one transposition
    with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
        load_strategy_config("any", str(cfg_file))
    warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert any("'chian'" in m and "did you mean 'chain'" in m for m in warnings)


def test_probe_parse_suppresses_typo_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """The pre-boot probe parse (warn_unknown_keys=False) stays silent so the warning fires once."""
    from almanak.framework.cli.run import parse_strategy_config_file

    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "chian": "ethereum"}))
    with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
        parse_strategy_config_file(cfg_file, warn_unknown_keys=False)
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


# ---------------------------------------------------------------------------
# #2101 / VIB-5164 — the gateway-boot quick probe shares the single validated
# parse; a malformed config fails fast with the file error instead of being
# swallowed into a misleading "no chain found" warning.
# ---------------------------------------------------------------------------


def test_anvil_probe_fails_fast_on_malformed_config(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """A malformed config raises the file-naming error FIRST — before any 'no chain found' warning."""
    from almanak.framework.cli._run_gateway import _resolve_anvil_chains_and_funding

    cfg_file = tmp_path / "config.json"
    cfg_file.write_text('{"chain": "arbitrum",,}')  # stray double comma
    with pytest.raises(click.ClickException) as exc_info:
        _resolve_anvil_chains_and_funding(
            working_dir=str(tmp_path),
            config_file=str(cfg_file),
            early_strategy_class=None,
            external_anvil_ports={},
        )
    assert str(cfg_file) in exc_info.value.message
    # The misleading warning must NOT have been printed before the real error.
    assert "no chain found" not in capsys.readouterr().out


def test_anvil_probe_reads_chain_from_valid_config(tmp_path: Path) -> None:
    """The shared validated parse still extracts chain + anvil_funding for a valid config."""
    from almanak.framework.cli._run_gateway import _resolve_anvil_chains_and_funding

    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"chain": "arbitrum", "anvil_funding": {"WETH": "1.5"}}))
    chains, funding = _resolve_anvil_chains_and_funding(
        working_dir=str(tmp_path),
        config_file=str(cfg_file),
        early_strategy_class=None,
        external_anvil_ports={},
    )
    assert chains == ["arbitrum"]
    assert funding == {"WETH": "1.5"}
