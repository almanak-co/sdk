"""Unit tests for the manifest-derived capability matrix (`almanak info capabilities`).

VIB-5112 phase 1 — advisory view. Tests cover:

* per-capability derivation helpers against minimal fake connectors,
* applicability (rate/valuation/safety-floor only emitted for relevant intents),
* the demo-coverage index (supported / quarantined / unknown),
* off-chain venues → unsupported-explicit,
* real-connector derivation (a lending connector with a rate provider vs a vault
  connector without), per the §D3 spec,
* the CLI command (table, --json, filters),
* the keystone advisory guarantee: ``unknown`` never raises / fails.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from click.testing import CliRunner

from almanak.framework.cli.capability_matrix import (
    CAP_ACCOUNTING,
    CAP_COMPILE,
    CAP_DEMO_COVERAGE,
    CAP_EXECUTE,
    CAP_RATE,
    CAP_SAFETY_FLOOR,
    CAP_VALUATION,
    CAPABILITIES,
    STATE_QUARANTINED,
    STATE_SUPPORTED,
    STATE_UNKNOWN,
    STATE_UNSUPPORTED,
    CapabilityMatrix,
    _accounting_state,
    _build_demo_coverage,
    _capability_applies,
    _cells_for_connector,
    _demo_coverage_state,
    _DemoCoverage,
    _intent_category,
    _rate_state,
    _safety_floor_state,
    _valuation_state,
    build_capability_matrix,
    capability_matrix_command,
)


# =============================================================================
# Fakes
# =============================================================================
@dataclass
class _FakeLendingRead:
    rate_history_chains: tuple[str, ...] = ()


@dataclass
class _FakeConnector:
    """Minimal stand-in exposing only the manifest fields the view reads."""

    name: str = "fake"
    aliases: tuple[str, ...] = ()
    strategy_intents: tuple[str, ...] | None = None
    strategy_chains: tuple[str, ...] | None = None
    lending_read: object | None = None
    receipt_parser_connector: object | None = None
    accounting_treatment: object | None = None
    primitive: object | None = None
    swap_quote_connector: object | None = None


@dataclass
class _FakeDemoSpec:
    name: str
    supported_protocols: list[str]
    supported_chains: list[str]


class _FakeQuarantine:
    """Quarantine stub mirroring the real ``Quarantine.find`` semantics.

    ``entries`` maps demo name -> the set of chains that demo is quarantined on.
    The real ``demo-quarantine.yml`` schema requires a chain per entry, so there
    is no chain-less "all chains" entry; ``find(demo, chain)`` matches only that
    exact chain (``find(demo, None)`` returns any entry, like the real helper).
    """

    def __init__(self, entries: dict[str, set[str]]):
        self._entries = entries

    def find(self, demo: str, chain: str | None = None):
        chains = self._entries.get(demo)
        if not chains:
            return None
        if chain is None:
            return object()
        return object() if chain in chains else None


_EMPTY_COVERAGE = _DemoCoverage(by_protocol_chain={}, quarantined={})


# =============================================================================
# Applicability
# =============================================================================
def test_intent_category_maps_known_verbs() -> None:
    assert _intent_category("SUPPLY") == "lending"
    assert _intent_category("LP_OPEN") == "lp"
    assert _intent_category("VAULT_DEPOSIT") == "yield"
    assert _intent_category("SWAP") == "swap"
    assert _intent_category("PERP_OPEN") == "perp"
    assert _intent_category("BRIDGE") == "other"


def test_compile_execute_accounting_demo_apply_to_all_categories() -> None:
    for cap in (CAP_COMPILE, CAP_EXECUTE, CAP_ACCOUNTING, CAP_DEMO_COVERAGE):
        for cat in ("lending", "lp", "yield", "perp", "swap", "other"):
            assert _capability_applies(cap, cat) is True


def test_rate_only_applies_to_lending_and_yield() -> None:
    assert _capability_applies(CAP_RATE, "lending") is True
    assert _capability_applies(CAP_RATE, "yield") is True
    assert _capability_applies(CAP_RATE, "swap") is False
    assert _capability_applies(CAP_RATE, "lp") is False


def test_valuation_applies_to_position_bearing_only() -> None:
    assert _capability_applies(CAP_VALUATION, "lending") is True
    assert _capability_applies(CAP_VALUATION, "lp") is True
    assert _capability_applies(CAP_VALUATION, "perp") is True
    assert _capability_applies(CAP_VALUATION, "swap") is False


def test_safety_floor_applies_to_swap_and_lp_only() -> None:
    assert _capability_applies(CAP_SAFETY_FLOOR, "swap") is True
    assert _capability_applies(CAP_SAFETY_FLOOR, "lp") is True
    assert _capability_applies(CAP_SAFETY_FLOOR, "lending") is False


# =============================================================================
# Per-capability derivation
# =============================================================================
def test_rate_supported_when_chain_in_rate_history() -> None:
    conn = _FakeConnector(lending_read=_FakeLendingRead(rate_history_chains=("base", "ethereum")))
    state, reason = _rate_state(conn, "base")
    assert state == STATE_SUPPORTED
    assert "rate_history_chains" in reason


def test_rate_unknown_when_no_lending_read() -> None:
    state, _ = _rate_state(_FakeConnector(), "base")
    assert state == STATE_UNKNOWN


def test_rate_unknown_when_chain_omitted_from_rate_history() -> None:
    conn = _FakeConnector(lending_read=_FakeLendingRead(rate_history_chains=("ethereum",)))
    state, reason = _rate_state(conn, "base")
    assert state == STATE_UNKNOWN
    assert "omits this chain" in reason


def test_valuation_supported_for_lending_with_lending_read() -> None:
    conn = _FakeConnector(lending_read=_FakeLendingRead())
    state, _ = _valuation_state(conn, "lending")
    assert state == STATE_SUPPORTED


def test_valuation_unknown_for_lp_no_position_read() -> None:
    state, reason = _valuation_state(_FakeConnector(), "lp")
    assert state == STATE_UNKNOWN
    assert "position_read" in reason


def test_accounting_supported_via_receipt_parser() -> None:
    conn = _FakeConnector(receipt_parser_connector=object())
    state, reason = _accounting_state(conn)
    assert state == STATE_SUPPORTED
    assert "receipt_parser_connector" in reason


def test_accounting_supported_via_lending_read() -> None:
    state, _ = _accounting_state(_FakeConnector(lending_read=_FakeLendingRead()))
    assert state == STATE_SUPPORTED


def test_accounting_unknown_when_no_decl() -> None:
    state, _ = _accounting_state(_FakeConnector())
    assert state == STATE_UNKNOWN


def test_safety_floor_supported_for_swap_with_quote_connector() -> None:
    conn = _FakeConnector(swap_quote_connector=object())
    state, _ = _safety_floor_state(conn, "swap")
    assert state == STATE_SUPPORTED


def test_safety_floor_unknown_for_lp() -> None:
    state, _ = _safety_floor_state(_FakeConnector(), "lp")
    assert state == STATE_UNKNOWN


# =============================================================================
# Demo coverage index
# =============================================================================
def test_build_demo_coverage_indexes_protocol_chain() -> None:
    catalog = type("Cat", (), {"specs": [_FakeDemoSpec("d1", ["aave_v3"], ["base", "arbitrum"])]})()
    cov = _build_demo_coverage(catalog, None)
    assert cov.covering_demos(["aave_v3"], "base") == ("d1",)
    assert cov.covering_demos(["aave_v3"], "polygon") == ()


def test_demo_coverage_state_supported() -> None:
    cov = _DemoCoverage(by_protocol_chain={("aave_v3", "base"): ("d1",)}, quarantined={})
    state, reason = _demo_coverage_state(["aave_v3"], "base", cov)
    assert state == STATE_SUPPORTED
    assert "d1" in reason


def test_demo_coverage_state_unknown_when_no_demo() -> None:
    state, _ = _demo_coverage_state(["aave_v3"], "base", _EMPTY_COVERAGE)
    assert state == STATE_UNKNOWN


def test_demo_coverage_state_quarantined_when_only_quarantined_demo() -> None:
    catalog = type("Cat", (), {"specs": [_FakeDemoSpec("d1", ["uniswap_v4"], ["base"])]})()
    quarantine = _FakeQuarantine({"d1": {"base"}})
    cov = _build_demo_coverage(catalog, quarantine)
    state, _ = _demo_coverage_state(["uniswap_v4"], "base", cov)
    assert state == STATE_QUARANTINED


def test_chain_specific_quarantine_does_not_shadow_other_chains() -> None:
    # Regression: a quarantine on (d1, base) must NOT mark d1's arbitrum cell
    # quarantined. The real Quarantine.matches(demo, None) returns True for any
    # entry, so a blanket find(demo, None) check would wrongly promote a
    # chain-specific quarantine to all chains. (CodeRabbit review on PR #3094.)
    catalog = type("Cat", (), {"specs": [_FakeDemoSpec("d1", ["p"], ["base", "arbitrum"])]})()
    quarantine = _FakeQuarantine({"d1": {"base"}})
    cov = _build_demo_coverage(catalog, quarantine)
    assert _demo_coverage_state(["p"], "base", cov)[0] == STATE_QUARANTINED
    assert _demo_coverage_state(["p"], "arbitrum", cov)[0] == STATE_SUPPORTED


# =============================================================================
# Connector cell assembly
# =============================================================================
def test_lending_connector_emits_rate_valuation_accounting() -> None:
    conn = _FakeConnector(
        name="lender",
        strategy_intents=("SUPPLY",),
        strategy_chains=("base",),
        lending_read=_FakeLendingRead(rate_history_chains=("base",)),
        receipt_parser_connector=object(),
    )
    cells = _cells_for_connector(conn, _EMPTY_COVERAGE)
    by_cap = {c.capability: c.state for c in cells}
    assert by_cap[CAP_COMPILE] == STATE_SUPPORTED
    assert by_cap[CAP_EXECUTE] == STATE_SUPPORTED
    assert by_cap[CAP_RATE] == STATE_SUPPORTED
    assert by_cap[CAP_VALUATION] == STATE_SUPPORTED
    assert by_cap[CAP_ACCOUNTING] == STATE_SUPPORTED
    # SUPPLY is lending → safety-floor not applicable → not emitted.
    assert CAP_SAFETY_FLOOR not in by_cap


def test_swap_connector_emits_safety_floor_not_rate() -> None:
    conn = _FakeConnector(
        name="dex",
        strategy_intents=("SWAP",),
        strategy_chains=("base",),
        swap_quote_connector=object(),
    )
    caps = {c.capability for c in _cells_for_connector(conn, _EMPTY_COVERAGE)}
    assert CAP_SAFETY_FLOOR in caps
    assert CAP_RATE not in caps  # swap has no rate cell
    assert CAP_VALUATION not in caps  # swap has no valuation cell


def test_offchain_venue_marked_unsupported_explicit() -> None:
    conn = _FakeConnector(name="kraken", strategy_intents=("SWAP",), strategy_chains=None)
    cells = _cells_for_connector(conn, _EMPTY_COVERAGE)
    assert cells
    assert all(c.state == STATE_UNSUPPORTED for c in cells)
    assert all(c.chain == "(off-chain)" for c in cells)


def test_connector_without_intents_emits_nothing() -> None:
    assert _cells_for_connector(_FakeConnector(strategy_intents=None), _EMPTY_COVERAGE) == []


def test_bnb_chain_normalised_to_bsc() -> None:
    conn = _FakeConnector(name="x", strategy_intents=("SWAP",), strategy_chains=("bnb",))
    chains = {c.chain for c in _cells_for_connector(conn, _EMPTY_COVERAGE)}
    assert "bsc" in chains
    assert "bnb" not in chains


# =============================================================================
# build_capability_matrix (injected) + determinism
# =============================================================================
def test_build_matrix_with_injected_connectors_is_sorted_and_deterministic() -> None:
    conns = [
        _FakeConnector(name="zeta", strategy_intents=("SWAP",), strategy_chains=("base",)),
        _FakeConnector(name="alpha", strategy_intents=("SWAP",), strategy_chains=("base",)),
    ]
    m1 = build_capability_matrix(connectors=conns, demo_coverage=_EMPTY_COVERAGE)
    m2 = build_capability_matrix(connectors=list(conns), demo_coverage=_EMPTY_COVERAGE)
    assert m1.cells == m2.cells  # deterministic
    protocols = [c.protocol for c in m1.cells]
    assert protocols == sorted(protocols)  # sorted by protocol first


def test_counts_by_state_tallies_all_states() -> None:
    conns = [_FakeConnector(name="x", strategy_intents=("VAULT_DEPOSIT",), strategy_chains=("base",))]
    m = build_capability_matrix(connectors=conns, demo_coverage=_EMPTY_COVERAGE)
    counts = m.counts_by_state()
    # compile+execute supported; rate/valuation/demo unknown; accounting unknown.
    assert counts[STATE_SUPPORTED] == 2
    assert counts[STATE_UNKNOWN] >= 3


# =============================================================================
# Real-connector derivation (the §D3 contrast: rate provider vs none)
# =============================================================================
@pytest.fixture(scope="module")
def real_matrix() -> CapabilityMatrix:
    return build_capability_matrix()


def _states(
    matrix: CapabilityMatrix,
    protocol: str,
    intent: str,
    capability: str,
    chain: str | None = None,
) -> set[str]:
    return {
        c.state
        for c in matrix.cells
        if c.protocol == protocol
        and c.intent == intent
        and c.capability == capability
        and (chain is None or c.chain == chain)
    }


def test_real_matrix_is_nonempty(real_matrix: CapabilityMatrix) -> None:
    assert len(real_matrix.cells) > 100
    assert {c.protocol for c in real_matrix.cells}  # multiple connectors present


def test_aave_v3_lending_has_rate_valuation_accounting(real_matrix: CapabilityMatrix) -> None:
    # aave_v3 declares lending_read with rate_history_chains incl. ethereum →
    # rate is supported there. valuation/accounting derive from lending_read /
    # receipt_parser regardless of chain.
    assert _states(real_matrix, "aave_v3", "SUPPLY", CAP_RATE, chain="ethereum") == {STATE_SUPPORTED}
    assert _states(real_matrix, "aave_v3", "SUPPLY", CAP_VALUATION) == {STATE_SUPPORTED}
    assert _states(real_matrix, "aave_v3", "SUPPLY", CAP_ACCOUNTING) == {STATE_SUPPORTED}
    # And on a chain absent from rate_history_chains, rate is honestly unknown
    # (the §D3 "discovered-at-runtime" gap the advisory view exists to surface).
    assert _states(real_matrix, "aave_v3", "SUPPLY", CAP_RATE, chain="mantle") == {STATE_UNKNOWN}


def test_morpho_vault_has_accounting_but_rate_and_valuation_unknown(real_matrix: CapabilityMatrix) -> None:
    # morpho_vault is a vault (no lending_read) → the §D3 "rate/valuation
    # discovered-at-runtime" gap is surfaced as unknown, accounting present.
    assert _states(real_matrix, "morpho_vault", "VAULT_DEPOSIT", CAP_RATE) == {STATE_UNKNOWN}
    assert _states(real_matrix, "morpho_vault", "VAULT_DEPOSIT", CAP_VALUATION) == {STATE_UNKNOWN}
    assert _states(real_matrix, "morpho_vault", "VAULT_DEPOSIT", CAP_ACCOUNTING) == {STATE_SUPPORTED}


def test_every_real_cell_has_a_valid_state(real_matrix: CapabilityMatrix) -> None:
    valid = {STATE_SUPPORTED, STATE_UNSUPPORTED, STATE_QUARANTINED, STATE_UNKNOWN}
    assert all(c.state in valid for c in real_matrix.cells)
    assert all(c.capability in CAPABILITIES for c in real_matrix.cells)


# =============================================================================
# CLI
# =============================================================================
@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_renders_table(runner: CliRunner) -> None:
    result = runner.invoke(capability_matrix_command, [])
    assert result.exit_code == 0, result.output
    assert "ADVISORY VIEW" in result.output
    assert "Legend:" in result.output


def test_cli_json_is_valid(runner: CliRunner) -> None:
    result = runner.invoke(capability_matrix_command, ["--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["capabilities"] == list(CAPABILITIES)
    assert "cells" in payload and payload["cells"]
    assert "summary" in payload


def test_cli_state_filter_unknown(runner: CliRunner) -> None:
    result = runner.invoke(capability_matrix_command, ["--json", "--state", "unknown"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert all(c["state"] == "unknown" for c in payload["cells"])


def test_cli_protocol_filter(runner: CliRunner) -> None:
    result = runner.invoke(capability_matrix_command, ["--json", "-p", "aave_v3"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert all("aave_v3" in c["protocol"] for c in payload["cells"])


def test_cli_no_match_prints_message(runner: CliRunner) -> None:
    result = runner.invoke(capability_matrix_command, ["-p", "does-not-exist-zzz"])
    assert result.exit_code == 0
    assert "No capability cells match" in result.output


def test_cli_json_empty_result_is_valid_json(runner: CliRunner) -> None:
    # Regression: --json with no matches must emit valid JSON (empty cells),
    # not a human-readable error — a consumer piping to a parser must not break.
    # (CodeRabbit review on PR #3094.)
    result = runner.invoke(capability_matrix_command, ["--json", "-p", "does-not-exist-zzz"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)  # would raise on the old human-text path
    assert payload["cells"] == []
    assert payload["capabilities"] == list(CAPABILITIES)


def test_cli_chain_filter_normalises_alias(runner: CliRunner) -> None:
    # Regression: --chain must normalise the request the same way rows are
    # normalised, so the bnb alias matches the rendered bsc rows.
    # (CodeRabbit review on PR #3094.)
    result = runner.invoke(capability_matrix_command, ["--json", "--chain", "bnb"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cells"], "bnb should match normalised bsc rows"
    assert all(c["chain"] == "bsc" for c in payload["cells"])


def test_cli_advisory_never_errors_on_unknown(runner: CliRunner) -> None:
    # The keystone guarantee for phase 1: unknown cells are reported, the
    # command exits 0 — there is no CI-failing behaviour here.
    result = runner.invoke(capability_matrix_command, [])
    assert result.exit_code == 0
    assert "unknown" in result.output.lower()
