"""Unit tests for ``_strat_test_skip_reason`` in ``almanak/cli/cli.py``.

The helper decides whether ``almanak strat test`` should skip because the
strategy's configured chain is not Anvil-forkable (non-EVM or unknown). It
must honor the same config formats ``strat run`` does: an explicit
``--config-file`` path or a discovered ``config.json`` / ``config.yaml`` /
``config.yml`` in the working dir, with chains declared as a scalar
``chains`` string, a ``chains`` list, or a singular ``chain`` key.

These are pure filesystem/parsing tests — no network, no subprocesses, no
CliRunner. The function is called directly with tmp_path-backed configs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from almanak.cli.cli import _strat_test_skip_reason


def _write_json_config(directory: Path, payload: dict, name: str = "config.json") -> Path:
    path = directory / name
    path.write_text(json.dumps(payload))
    return path


# ---------------------------------------------------------------------------
# Config discovery / load-failure branches
# ---------------------------------------------------------------------------


def test_explicit_config_file_missing_returns_none(tmp_path):
    """An explicit --config-file pointing at a missing path is not a skip."""
    missing = tmp_path / "does-not-exist.json"
    assert _strat_test_skip_reason(str(tmp_path), str(missing)) is None


def test_no_config_in_working_dir_returns_none(tmp_path):
    """A working dir with no config.{json,yaml,yml} yields no skip reason."""
    assert _strat_test_skip_reason(str(tmp_path), None) is None


def test_config_json_discovered_in_working_dir(tmp_path):
    """config.json is discovered without an explicit --config-file."""
    _write_json_config(tmp_path, {"chains": ["solana"]})
    reason = _strat_test_skip_reason(str(tmp_path), None)
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


@pytest.mark.parametrize("name", ["config.yaml", "config.yml"])
def test_yaml_config_discovered_in_working_dir(tmp_path, name):
    """YAML configs are discovered and parsed like strat run does."""
    (tmp_path / name).write_text("chains:\n  - solana\n")
    reason = _strat_test_skip_reason(str(tmp_path), None)
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


def test_explicit_config_file_takes_priority_over_discovery(tmp_path):
    """When --config-file is given, the working-dir configs are not consulted."""
    # Working dir declares an EVM chain; the explicit file declares solana.
    _write_json_config(tmp_path, {"chains": ["arbitrum"]})
    explicit = _write_json_config(tmp_path, {"chains": ["solana"]}, name="other.json")
    reason = _strat_test_skip_reason(str(tmp_path), str(explicit))
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


def test_malformed_config_returns_none(tmp_path):
    """A config that fails to parse is treated as no-skip, not an error."""
    (tmp_path / "config.json").write_text("{not valid json")
    assert _strat_test_skip_reason(str(tmp_path), None) is None


# ---------------------------------------------------------------------------
# Chain-declaration format branches
# ---------------------------------------------------------------------------


def test_scalar_chains_string_evm_returns_none(tmp_path):
    cfg = _write_json_config(tmp_path, {"chains": "arbitrum"})
    assert _strat_test_skip_reason(str(tmp_path), str(cfg)) is None


def test_scalar_chains_string_non_evm_returns_reason(tmp_path):
    cfg = _write_json_config(tmp_path, {"chains": "solana"})
    reason = _strat_test_skip_reason(str(tmp_path), str(cfg))
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


def test_chains_list_all_evm_returns_none(tmp_path):
    cfg = _write_json_config(tmp_path, {"chains": ["arbitrum", "base", "ethereum"]})
    assert _strat_test_skip_reason(str(tmp_path), str(cfg)) is None


def test_singular_chain_key_evm_returns_none(tmp_path):
    cfg = _write_json_config(tmp_path, {"chain": "base"})
    assert _strat_test_skip_reason(str(tmp_path), str(cfg)) is None


def test_singular_chain_key_non_evm_returns_reason(tmp_path):
    cfg = _write_json_config(tmp_path, {"chain": "solana"})
    reason = _strat_test_skip_reason(str(tmp_path), str(cfg))
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"chains": None},
        {"chains": 42},
        {"chain": ""},
        {"unrelated": "value"},
    ],
)
def test_no_usable_chain_declaration_returns_none(tmp_path, payload):
    """Empty / unsupported chain declarations yield an empty chain list → no skip."""
    cfg = _write_json_config(tmp_path, payload)
    assert _strat_test_skip_reason(str(tmp_path), str(cfg)) is None


# ---------------------------------------------------------------------------
# Chain normalization branches
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("alias", ["eth", "avax", "bnb"])
def test_chain_alias_normalizes_to_evm_returns_none(tmp_path, alias):
    """Aliases resolve through resolve_chain_name and are not falsely skipped."""
    cfg = _write_json_config(tmp_path, {"chains": [alias]})
    assert _strat_test_skip_reason(str(tmp_path), str(cfg)) is None


def test_unknown_chain_falls_back_to_lowercase_and_returns_reason(tmp_path):
    """resolve_chain_name raising ValueError falls back to chain.lower()."""
    cfg = _write_json_config(tmp_path, {"chains": ["NotAChain"]})
    reason = _strat_test_skip_reason(str(tmp_path), str(cfg))
    assert reason == "chain 'NotAChain' is not Anvil-forkable (not in CHAIN_IDS)"


def test_mixed_list_reports_first_non_forkable_chain(tmp_path):
    """An EVM chain earlier in the list does not mask a later non-forkable one."""
    cfg = _write_json_config(tmp_path, {"chains": ["arbitrum", "solana"]})
    reason = _strat_test_skip_reason(str(tmp_path), str(cfg))
    assert reason == "chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)"


def test_chains_list_coerces_non_string_entries(tmp_path):
    """List entries are str()-coerced before normalization; unknowns skip."""
    cfg = _write_json_config(tmp_path, {"chains": [12345]})
    reason = _strat_test_skip_reason(str(tmp_path), str(cfg))
    assert reason == "chain '12345' is not Anvil-forkable (not in CHAIN_IDS)"
