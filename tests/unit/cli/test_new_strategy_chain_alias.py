"""``strat new -c`` chain handling: canonical choices, alias-tolerant input.

VIB-5293: ``almanak strat new -c``
historically advertised the ChainRegistry canonical names via ``click.Choice``
but hard-rejected registered aliases — a user scaffolding with ``bnb`` (the
vocabulary the connector manifests used at the time) was refused, while a user
scaffolding with ``bsc`` produced a config other surfaces then disagreed on.

The contract pinned here:

* choices come FROM :class:`ChainRegistry` (canonical names);
* any registered alias (``bnb``, ``eth``, ``avax``, mixed case) is accepted
  and CONVERTED to its canonical name, so the scaffolded ``config.json``
  always carries a chain the runtime resolves to itself (idempotent under
  ``resolve_chain_name``);
* an unknown chain fails with a message that lists the canonical choices.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.core.constants import resolve_chain_name
from almanak.framework.cli.chain_params import ChainChoice
from almanak.framework.cli.chain_resolution import cli_chain_choices
from almanak.framework.cli.new_strategy import new_strategy


def _scaffold(runner: CliRunner, tmp_path: Path, chain: str, name: str = "alias_probe"):
    out_dir = tmp_path / name
    result = runner.invoke(
        new_strategy,
        ["--template", "blank", "--name", name, "--chain", chain, "--output-dir", str(out_dir)],
    )
    return result, out_dir


@pytest.mark.parametrize("alias", ["bnb", "bsc", "BNB", "binance"])
def test_scaffold_accepts_bnb_bsc_vocabulary_and_emits_canonical(tmp_path: Path, alias: str) -> None:
    runner = CliRunner()
    result, out_dir = _scaffold(runner, tmp_path, alias, name=f"probe_{alias.lower()}")
    assert result.exit_code == 0, result.output

    config = json.loads((out_dir / "config.json").read_text())
    # The scaffolded config carries the canonical name...
    assert config["chain"] == "bsc"
    # ...and the runtime resolver maps it to itself (idempotent — the config
    # the scaffold writes is the vocabulary every runtime seam expects).
    assert resolve_chain_name(config["chain"]) == "bsc"


def test_scaffold_accepts_eth_alias(tmp_path: Path) -> None:
    runner = CliRunner()
    result, out_dir = _scaffold(runner, tmp_path, "eth")
    assert result.exit_code == 0, result.output
    config = json.loads((out_dir / "config.json").read_text())
    assert config["chain"] == "ethereum"


def test_scaffold_unknown_chain_error_lists_canonical_choices(tmp_path: Path) -> None:
    runner = CliRunner()
    result, out_dir = _scaffold(runner, tmp_path, "notachain")
    assert result.exit_code != 0
    assert not out_dir.exists()
    # The error names the canonical vocabulary, not click's generic message.
    assert "not a supported chain" in result.output
    for canonical in ("arbitrum", "bsc", "ethereum"):
        assert canonical in result.output
    # The alias must not be advertised as a choice.
    choices_line = result.output
    assert "'notachain'" in choices_line


def test_chain_choice_choices_are_registry_canonical() -> None:
    # Single source of truth: the advertised choices ARE the registry's
    # canonical names — "bsc" is offered, its aliases are not.
    choices = ChainChoice().choices
    assert choices == cli_chain_choices()
    assert "bsc" in choices
    assert "bnb" not in choices


def test_chain_choice_evm_only_rejects_solana_and_its_alias() -> None:
    param = ChainChoice(evm_only=True)
    assert "solana" not in param.choices
    for value in ("solana", "sol"):
        with pytest.raises(Exception) as exc:  # click.exceptions.UsageError subclass
            param.convert(value, None, None)
        assert "not a supported chain" in str(exc.value)


def test_chain_choice_converts_alias_to_canonical() -> None:
    param = ChainChoice()
    assert param.convert("bnb", None, None) == "bsc"
    assert param.convert("bsc", None, None) == "bsc"
    assert param.convert(" Avax ", None, None) == "avalanche"


def test_chain_choice_convert_passes_none_through() -> None:
    # An optional --chain left unset must stay unset, not fail as an unknown
    # chain (convert robustness).
    assert ChainChoice().convert(None, None, None) is None


def test_chain_choice_get_metavar_accepts_ctx() -> None:
    # Click 8.2+ calls get_metavar(param, ctx); 8.1 calls get_metavar(param).
    # The override must tolerate both so any --help path renders.
    # Exercise both call shapes and assert the metavar is the
    # canonical [a|b|c] form.
    param = ChainChoice()
    metavar_no_ctx = param.get_metavar(None)
    metavar_with_ctx = param.get_metavar(None, None)
    assert metavar_no_ctx == metavar_with_ctx
    assert metavar_with_ctx.startswith("[") and metavar_with_ctx.endswith("]")
    assert "bsc" in metavar_with_ctx


def test_chain_choice_shell_complete_returns_canonical_names() -> None:
    # Leaving click.Choice for a custom ParamType dropped --chain shell
    # completion; shell_complete restores it, offering canonical names only.
    param = ChainChoice()
    completions = [c.value for c in param.shell_complete(None, None, "a")]
    # Prefix-filtered, canonical-only: "arbitrum"/"avalanche" match "a",
    # aliases ("avax") never surface, and non-"a" chains are excluded.
    assert "arbitrum" in completions
    assert "avax" not in completions
    assert all(c.startswith("a") for c in completions)
    assert "bsc" not in completions

    # Empty incomplete offers the full canonical set (== choices).
    all_completions = {c.value for c in param.shell_complete(None, None, "")}
    assert all_completions == set(cli_chain_choices())

    # Case-insensitive prefix match.
    upper = {c.value for c in param.shell_complete(None, None, "A")}
    assert "arbitrum" in upper
