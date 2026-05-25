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
