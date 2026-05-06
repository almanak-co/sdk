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
