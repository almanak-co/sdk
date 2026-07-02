"""Tests for ``scripts/ci/check_connector_chains.py`` (VIB-4802).

Mirrors the pattern of ``tests/scripts/test_check_config_boundary.py`` —
the script is loaded via ``importlib`` so we can drive its internals from
unit tests without re-shelling.

Coverage:

* String and int-keyed connector address dicts (enum keys are rejected).
* The heuristic that distinguishes chain-keyed from non-chain-keyed
  dicts (the false-positive guard the ticket calls out).
* Strategy ``supported_chains=[...]`` decorator lists.
* Real-codebase sweep — the live ``almanak/framework/connectors`` tree
  must pass at the time of merge.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _load_module():
    """Load the script as ``check_connector_chains`` so its globals are stable."""
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_connector_chains.py"
    spec = importlib.util.spec_from_file_location("check_connector_chains", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_connector_chains"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def module():
    return _load_module()


def _make_connector(root: Path, name: str, body: str) -> Path:
    """Write a synthetic connector dir at ``root/almanak/framework/connectors/<name>/``."""
    pkg = root / "almanak" / "framework" / "connectors" / name
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("")
    f = pkg / "sdk.py"
    f.write_text(body)
    return f


def _redirect_to_tmp(module, tmp_path: Path) -> None:
    """Point the validator at a synthetic repo rooted at ``tmp_path``."""
    module.CONNECTORS_DIR = tmp_path / "almanak" / "framework" / "connectors"
    module.DEMO_STRATEGIES_DIR = tmp_path / "almanak" / "demo_strategies"
    module.STRATEGIES_DIR = tmp_path / "strategies"
    module.ALLOWLIST_PATH = tmp_path / "scripts" / "ci" / "connector-chain-allowlist.yml"
    module._REPO_ROOT = tmp_path
    module.CONNECTORS_DIR.mkdir(parents=True, exist_ok=True)
    module.DEMO_STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)
    module.STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)
    module.ALLOWLIST_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Pass cases
# ---------------------------------------------------------------------------


def test_pass_string_keyed_addresses(module, tmp_path: Path) -> None:
    """All-valid string-keyed FACTORY_ADDRESSES → exit 0."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {\n'
        '    "arbitrum": "0x1111111111111111111111111111111111111111",\n'
        '    "ethereum": "0x2222222222222222222222222222222222222222",\n'
        '    "base": "0x3333333333333333333333333333333333333333",\n'
        "}\n",
    )
    violations, inspection = module.run()
    assert violations == [], violations
    assert inspection.address_dicts_validated == 1
    assert inspection.connectors_checked == 1


def test_pass_int_keyed_addresses(module, tmp_path: Path) -> None:
    """All-valid int-keyed (chain_id) ROUTER_ADDRESSES → exit 0."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakebridge",
        'ROUTER_ADDRESSES: dict[int, str] = {\n'
        '    1: "0x1111111111111111111111111111111111111111",\n'
        '    42161: "0x2222222222222222222222222222222222222222",\n'
        '    8453: "0x3333333333333333333333333333333333333333",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert violations == [], violations


def test_fail_enum_keyed_addresses(module, tmp_path: Path) -> None:
    """Chain.X-keyed dicts are rejected: the Chain enum was removed (VIB-4851).

    Any surviving enum-shaped key is a migration miss — the structured CI
    message points the author at canonical chain strings.
    """
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeenum",
        "FACTORY_ADDRESSES = {\n"
        '    Chain.ARBITRUM: "0x1111111111111111111111111111111111111111",\n'
        '    Chain.BASE: "0x2222222222222222222222222222222222222222",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert len(violations) == 2
    assert all("canonical chain string" in v.message for v in violations)


# ---------------------------------------------------------------------------
# Fail cases
# ---------------------------------------------------------------------------


def test_fail_string_key(module, tmp_path: Path) -> None:
    """Unknown string chain key surfaces in error message."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {\n'
        '    "arbitrum": "0x1111111111111111111111111111111111111111",\n'
        '    "unobtanium": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.key == "unobtanium"
    assert "FACTORY_ADDRESSES" in v.symbol
    assert "not registered" in v.message


def test_fail_int_key_outside_chain_ids(module, tmp_path: Path) -> None:
    """Unknown int chain_id surfaces in error message + hints at allowlist."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakebridge",
        'ROUTER_ADDRESSES: dict[int, str] = {\n'
        '    1: "0x1111111111111111111111111111111111111111",\n'
        '    999999999: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.key == "999999999"
    assert "allowlist" in v.message.lower()


def test_fail_enum_key_unknown_attr(module, tmp_path: Path) -> None:
    """Any ``Chain.X``-shaped key is flagged, member-like or not (the enum
    is gone — VIB-4851)."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeenum",
        "FACTORY_ADDRESSES = {\n"
        '    Chain.NOT_A_CHAIN: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert len(violations) == 1
    assert "Chain.NOT_A_CHAIN" in violations[0].key


def test_fail_string_key_whitespace(module, tmp_path: Path) -> None:
    """A chain literal with trailing whitespace is rejected.

    ``resolve_chain_name`` calls ``.lower().strip()`` internally, so a
    literal like ``"arbitrum "`` would resolve at validation time but
    break every caller that looks up the dict by the exact key string.
    """
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {\n'
        '    "arbitrum": "0x1111111111111111111111111111111111111111",\n'
        '    "ethereum ": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",\n'
        "}\n",
    )
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.key == "ethereum "
    assert "whitespace" in v.message


def test_fail_strategy_supported_chains_whitespace(module, tmp_path: Path) -> None:
    """``supported_chains=['arbitrum ']`` is rejected on the same grounds."""
    _redirect_to_tmp(module, tmp_path)
    strat_dir = tmp_path / "strategies" / "incubating" / "wsstrat"
    strat_dir.mkdir(parents=True)
    (strat_dir / "strategy.py").write_text(
        "from almanak.framework.strategies import almanak_strategy, IntentStrategy\n"
        "@almanak_strategy(name='ws', supported_chains=['arbitrum '])\n"
        "class WsStrategy(IntentStrategy):\n"
        "    def decide(self, market):\n"
        "        return None\n"
    )
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "strategy"
    assert v.key == "arbitrum "
    assert "whitespace" in v.message


def test_pass_strategy_supported_chains_tuple(module, tmp_path: Path) -> None:
    """``supported_chains=('arbitrum',)`` (tuple form) is validated like a list."""
    _redirect_to_tmp(module, tmp_path)
    strat_dir = tmp_path / "strategies" / "incubating" / "tupstrat"
    strat_dir.mkdir(parents=True)
    (strat_dir / "strategy.py").write_text(
        "from almanak.framework.strategies import almanak_strategy, IntentStrategy\n"
        "@almanak_strategy(name='tup', supported_chains=('arbitrum',))\n"
        "class TupStrategy(IntentStrategy):\n"
        "    def decide(self, market):\n"
        "        return None\n"
    )
    violations, inspection = module.run()
    assert violations == []
    assert inspection.strategies_checked == 1


def test_fail_strategy_supported_chains_tuple_unknown(module, tmp_path: Path) -> None:
    """Unknown chain in a tuple-form ``supported_chains`` is flagged."""
    _redirect_to_tmp(module, tmp_path)
    strat_dir = tmp_path / "strategies" / "incubating" / "tupbadstrat"
    strat_dir.mkdir(parents=True)
    (strat_dir / "strategy.py").write_text(
        "from almanak.framework.strategies import almanak_strategy, IntentStrategy\n"
        "@almanak_strategy(name='tupbad', supported_chains=('arbitrum', 'fake-chain'))\n"
        "class TupBadStrategy(IntentStrategy):\n"
        "    def decide(self, market):\n"
        "        return None\n"
    )
    violations, _ = module.run()
    assert len(violations) == 1
    assert violations[0].key == "fake-chain"


def test_fail_strategy_supported_chains(module, tmp_path: Path) -> None:
    """A strategy with ``supported_chains=['fake-chain']`` fails."""
    _redirect_to_tmp(module, tmp_path)
    strat_dir = tmp_path / "strategies" / "incubating" / "fake_strat"
    strat_dir.mkdir(parents=True)
    (strat_dir / "strategy.py").write_text(
        "from almanak.framework.strategies import almanak_strategy, IntentStrategy\n"
        "@almanak_strategy(name='fake', supported_chains=['arbitrum', 'fake-chain'])\n"
        "class FakeStrategy(IntentStrategy):\n"
        "    def decide(self, market):\n"
        "        return None\n"
    )
    violations, inspection = module.run()
    assert inspection.strategies_checked == 1
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "strategy"
    assert v.key == "fake-chain"
    assert "name='fake'" in v.symbol


# ---------------------------------------------------------------------------
# Heuristic correctness — no false positives on non-chain-keyed dicts
# ---------------------------------------------------------------------------


def test_heuristic_skips_token_addresses(module, tmp_path: Path) -> None:
    """A dict named like an address table but keyed by token symbol must NOT be flagged."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "tokenmap",
        # Note the name matches ``*_ADDRESSES`` but the keys are token
        # symbols, not chain names — the heuristic must skip cleanly.
        'TOKEN_ADDRESSES: dict[str, str] = {\n'
        '    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",\n'
        '    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",\n'
        '    "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",\n'
        "}\n",
    )
    violations, inspection = module.run()
    assert violations == [], violations
    assert inspection.address_dicts_validated == 0


def test_heuristic_skips_non_chain_int_keys(module, tmp_path: Path) -> None:
    """A dict whose ints clearly aren't chain ids is still routed to int-validation.

    This is the explicit conservative trade-off: any int-keyed
    ``*_ADDRESSES`` dict is presumed chain-keyed, because the universe
    of "addresses keyed by integer that isn't a chain id" is essentially
    empty in this codebase. Validating against ``CHAIN_IDS.values()``
    AND the allowlist catches the real cases without manufacturing
    false positives in practice.
    """
    _redirect_to_tmp(module, tmp_path)
    # All valid chain ids — heuristic should pass, validator finds nothing.
    _make_connector(
        tmp_path,
        "intkeyed",
        'POOL_ADDRESSES: dict[int, str] = {1: "0x" + "11" * 20, 42161: "0x" + "22" * 20}\n',
    )
    violations, _ = module.run()
    assert violations == []


def test_skipped_dict_built_from_comprehension(module, tmp_path: Path) -> None:
    """Dicts built via comprehension or function call are skipped (out of AST scope)."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "comprehension",
        "_SOURCE = {'arbitrum': '0x' + '11' * 20}\n"
        'FACTORY_ADDRESSES: dict[str, str] = {chain: addr for chain, addr in _SOURCE.items()}\n',
    )
    violations, inspection = module.run()
    # Not validated (skipped) and not violated.
    assert violations == []
    assert inspection.address_dicts_validated == 0


# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------


def test_allowlist_suppresses_int_chain_id_violation(module, tmp_path: Path) -> None:
    """An int chain_id in the allowlist must not be reported."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakebridge",
        'ROUTER_ADDRESSES: dict[int, str] = {1: "0x" + "11" * 20, 324: "0x" + "22" * 20}\n',
    )
    module.ALLOWLIST_PATH.write_text(
        "symbols:\n"
        "  almanak/framework/connectors/fakebridge/sdk.py::ROUTER_ADDRESSES:\n"
        "    - 324   # zkSync — bridge target only. TEST-TICKET-1.\n"
    )
    violations, _ = module.run()
    assert violations == []


def test_allowlist_does_not_apply_to_other_symbols(module, tmp_path: Path) -> None:
    """Allowlist entries are scoped to a single ``file::symbol`` key."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakebridge",
        'ROUTER_ADDRESSES: dict[int, str] = {1: "0x" + "11" * 20}\n'
        'DELEGATE_ADDRESSES: dict[int, str] = {324: "0x" + "22" * 20}\n',
    )
    module.ALLOWLIST_PATH.write_text(
        "symbols:\n"
        "  almanak/framework/connectors/fakebridge/sdk.py::ROUTER_ADDRESSES:\n"
        "    - 324\n"
    )
    violations, _ = module.run()
    assert len(violations) == 1
    assert violations[0].symbol == "DELEGATE_ADDRESSES"


# ---------------------------------------------------------------------------
# Real-codebase sweep — regression guard for "all current connectors pass"
# ---------------------------------------------------------------------------


def test_real_codebase_passes() -> None:
    """The live ``almanak/framework/connectors`` tree must validate cleanly.

    If this test fails, either (a) a new violation was introduced or
    (b) the allowlist file is out of sync. The script's stderr message
    points at the fix path.
    """
    # Reload fresh — earlier tests mutate module globals.
    module = _load_module()
    violations, inspection = module.run()
    # We don't pin a specific number — connectors come and go — but we
    # do pin "no violations at the moment this gate landed".
    assert violations == [], "\n".join(v.format() for v in violations)
    # Sanity bounds: there ARE connectors and strategies on disk.
    assert inspection.connectors_checked > 0
    assert inspection.strategies_checked > 0
    assert inspection.address_dicts_validated > 0


# ---------------------------------------------------------------------------
# main() exit codes
# ---------------------------------------------------------------------------


def test_main_exit_code_pass(module, tmp_path: Path, capsys) -> None:
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {"arbitrum": "0x" + "11" * 20}\n',
    )
    assert module.main([]) == 0
    captured = capsys.readouterr()
    assert "OK:" in captured.out


def test_main_exit_code_fail(module, tmp_path: Path, capsys) -> None:
    _redirect_to_tmp(module, tmp_path)
    # Single all-lowercase key that does not resolve — lowercase shape
    # triggers chain-keyed detection (token symbols are uppercase by
    # convention and would not match), so the typo is caught.
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {"unobtanium": "0x" + "11" * 20}\n',
    )
    assert module.main([]) == 1
    captured = capsys.readouterr()
    # Errors go to stderr.
    assert "unobtanium" in captured.err


def test_main_json_success_is_pure_json(module, tmp_path: Path, capsys) -> None:
    """``--json`` mode must emit ONLY a parseable JSON payload on stdout.

    Regression guard for the bug where the trailing human-readable
    ``OK: ...`` summary was printed alongside the JSON payload and
    broke machine consumers calling ``json.loads(captured.out)``.
    """
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {"arbitrum": "0x" + "11" * 20}\n',
    )
    assert module.main(["--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)  # must not raise
    assert payload["ok"] is True
    assert payload["violations"] == []
    # Nothing past the JSON document beyond a single trailing newline.
    assert captured.out.rstrip("\n") == json.dumps(payload, indent=2)


def test_parse_failure_surfaces_as_violation(module, tmp_path: Path) -> None:
    """A connector file with a syntax error must NOT be silently skipped.

    Returning ``None`` on parse failure (the prior behaviour) created a
    fail-open hole — a broken connector evaded validation entirely.
    The contract is fail-closed: surface as a violation so CI blocks.
    """
    _redirect_to_tmp(module, tmp_path)
    # Garbage file under connectors/ — unparseable Python.
    pkg = tmp_path / "almanak" / "framework" / "connectors" / "broken"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "sdk.py").write_text("def oops(:\n")  # syntax error
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "connector"
    assert v.symbol == "<module>"
    assert "parse" in v.message.lower() or "AST" in v.message


def test_parse_failure_in_strategy_surfaces_as_violation(module, tmp_path: Path) -> None:
    """A strategy file with a syntax error surfaces as a strategy violation."""
    _redirect_to_tmp(module, tmp_path)
    strat_dir = tmp_path / "strategies" / "incubating" / "broken"
    strat_dir.mkdir(parents=True)
    (strat_dir / "strategy.py").write_text("class Broken(\n")  # syntax error
    violations, _ = module.run()
    assert len(violations) == 1
    v = violations[0]
    assert v.kind == "strategy"
    assert v.symbol == "<module>"


def test_main_json_failure_payload(module, tmp_path: Path, capsys) -> None:
    """``--json`` mode on failure still emits parseable JSON on stdout."""
    _redirect_to_tmp(module, tmp_path)
    _make_connector(
        tmp_path,
        "fakeswap",
        'FACTORY_ADDRESSES: dict[str, str] = {"unobtanium": "0x" + "11" * 20}\n',
    )
    assert module.main(["--json"]) == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert any(v["key"] == "unobtanium" for v in payload["violations"])
