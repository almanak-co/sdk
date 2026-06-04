"""Smoke coverage for the ``almanak new-protocol`` connector scaffold codegen.

``new_protocol.py`` had no test module before VIB-4851 C2 deleted its
hand-maintained ``SupportedChain`` StrEnum and switched the ``--chain`` choice
to the registry-derived :func:`cli_chain_choices`. That change rewrote every
``{chain.value}`` codegen site to ``{chain}`` (chain is now a plain ``str``).

These tests are deliberately lightweight: they call the public generators (and
the Click command) for the historical default chain plus a chain that the old
6-member enum did NOT allow (``linea``), then assert the emitted ``.py`` text
``ast.parse``-es and embeds the chain string with no leaked ``.value`` /
``SupportedChain`` token. No subprocess, no network.
"""

import ast
import json
from pathlib import Path

from click.testing import CliRunner

from almanak.framework.cli.new_protocol import (
    ProtocolType,
    generate_adapter_file,
    generate_readme_file,
    generate_test_file,
    new_protocol,
)

# "arbitrum" was always allowed; "linea" is newly reachable now that the choice
# is registry-derived (the old enum stopped at avalanche). Exercising both
# proves the widening landed and the codegen is chain-agnostic.
_CHAINS = ["arbitrum", "linea"]


def _assert_no_chain_enum_leak(text: str) -> None:
    """The chain must be a bare string in generated code — no enum residue."""
    assert "chain.value" not in text
    assert "chains[0].value" not in text
    assert "SupportedChain" not in text


def test_generated_adapter_is_valid_python_with_chain() -> None:
    for chain in _CHAINS:
        adapter = generate_adapter_file("foo_dex", ProtocolType.DEX, [chain])
        ast.parse(adapter)  # generated adapter.py must be importable Python
        # The chain becomes a key in the *_ADDRESSES dict literal.
        assert f'"{chain}"' in adapter
        _assert_no_chain_enum_leak(adapter)


def test_generated_adapter_emits_each_requested_chain() -> None:
    chains = ["arbitrum", "base", "linea"]
    adapter = generate_adapter_file("foo_dex", ProtocolType.DEX, chains)
    ast.parse(adapter)
    for chain in chains:
        assert f'"{chain}"' in adapter
    _assert_no_chain_enum_leak(adapter)


def test_generated_adapter_tests_are_valid_python_with_chain() -> None:
    for chain in _CHAINS:
        test_src = generate_test_file("foo_dex", ProtocolType.DEX, [chain])
        ast.parse(test_src)  # generated tests/test_adapter.py must parse
        assert chain in test_src
        _assert_no_chain_enum_leak(test_src)


def test_generated_readme_titlecases_each_chain() -> None:
    for chain in _CHAINS:
        readme = generate_readme_file("foo_dex", ProtocolType.LENDING, [chain])
        # README bullets render the chain title-cased (e.g. "- Linea").
        assert chain.title() in readme
        # README is prose, not Python, but the .value leak guard still applies.
        assert "chain.value" not in readme
        assert "SupportedChain" not in readme


def test_protocol_type_value_is_preserved_in_codegen() -> None:
    """The non-chain ``protocol_type.value`` codegen must survive untouched."""
    adapter = generate_adapter_file("foo_dex", ProtocolType.PERPS, ["arbitrum"])
    # ProtocolType is still an enum; its .value ("perps") must appear verbatim.
    assert "perps" in adapter


def test_new_protocol_command_scaffolds_new_chain(tmp_path: Path) -> None:
    """End-to-end: the CLI accepts a registry-only chain and writes a tree."""
    target = tmp_path / "foo_dex"  # must not pre-exist (command refuses existing dirs)
    result = CliRunner().invoke(
        new_protocol,
        ["--name", "foo_dex", "--type", "dex", "--chain", "linea", "--chain", "base", "--output-dir", str(target)],
    )
    assert result.exit_code == 0, result.output

    adapter = (target / "adapter.py").read_text()
    ast.parse(adapter)
    assert '"linea"' in adapter and '"base"' in adapter
    _assert_no_chain_enum_leak(adapter)

    test_src = (target / "tests" / "test_adapter.py").read_text()
    ast.parse(test_src)
    _assert_no_chain_enum_leak(test_src)

    # __init__.py / sdk.py / receipt_parser.py must all parse too.
    for fname in ("__init__.py", "sdk.py", "receipt_parser.py"):
        ast.parse((target / fname).read_text())

    readme = (target / "README.md").read_text()
    assert "Linea" in readme and "Base" in readme


def test_duplicate_chain_flags_are_deduped(tmp_path: Path) -> None:
    """Duplicate ``--chain`` flags must not emit duplicate keys in adapter.py.

    The command dedupes via ``dict.fromkeys`` (order-preserving) so a slip like
    ``--chain ethereum --chain ethereum`` yields a single ADDRESSES key
    (VIB-4851 C2 review).
    """
    target = tmp_path / "dup_dex"
    result = CliRunner().invoke(
        new_protocol,
        [
            "--name",
            "dup_dex",
            "--type",
            "dex",
            "--chain",
            "ethereum",
            "--chain",
            "ethereum",
            "--output-dir",
            str(target),
        ],
    )
    assert result.exit_code == 0, result.output
    adapter = (target / "adapter.py").read_text()
    ast.parse(adapter)
    # Exactly one "ethereum": ADDRESSES key despite the duplicate flag.
    assert adapter.count('"ethereum":') == 1, adapter


def test_new_protocol_command_rejects_existing_directory(tmp_path: Path) -> None:
    """Regression guard for the directory-already-exists abort path."""
    target = tmp_path / "already_here"
    target.mkdir()
    result = CliRunner().invoke(
        new_protocol,
        ["--name", "already_here", "--type", "dex", "--chain", "arbitrum", "--output-dir", str(target)],
    )
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_default_chain_is_arbitrum(tmp_path: Path) -> None:
    """Omitting --chain falls back to the historical arbitrum default."""
    target = tmp_path / "def_dex"
    result = CliRunner().invoke(
        new_protocol,
        ["--name", "def_dex", "--type", "dex", "--output-dir", str(target)],
    )
    assert result.exit_code == 0, result.output
    adapter = (target / "adapter.py").read_text()
    assert '"arbitrum"' in adapter
    # The echoed summary names the default chain.
    assert "arbitrum" in result.output


def test_generated_config_is_valid_json_smoke(tmp_path: Path) -> None:
    """The new-strategy sibling check lives elsewhere; here we just confirm the
    connector scaffold does not emit a config.json (it has no runtime config),
    so this asserts the expected file set instead."""
    target = tmp_path / "set_dex"
    result = CliRunner().invoke(
        new_protocol,
        ["--name", "set_dex", "--type", "dex", "--chain", "arbitrum", "--output-dir", str(target)],
    )
    assert result.exit_code == 0, result.output
    produced = {p.name for p in target.iterdir()}
    assert {"adapter.py", "sdk.py", "receipt_parser.py", "__init__.py", "README.md"} <= produced
    assert "config.json" not in produced, "new-protocol scaffold must not emit config.json"
    # sanity: any JSON the scaffold did emit must be loadable
    for jf in target.rglob("*.json"):
        json.loads(jf.read_text())


def test_new_protocol_rejects_non_evm_chain(tmp_path: Path) -> None:
    """new-protocol is EVM-only: the generated scaffold assumes 0x/42-char
    addresses + hex calldata, so a non-EVM chain (solana) is not an accepted
    --chain choice (VIB-4851 C2 / CodeRabbit review)."""
    result = CliRunner().invoke(
        new_protocol,
        ["--name", "sol_dex", "--type", "dex", "--chain", "solana", "--output-dir", str(tmp_path / "sol_dex")],
    )
    assert result.exit_code != 0
    assert "solana" in result.output.lower()  # click reports the rejected choice
