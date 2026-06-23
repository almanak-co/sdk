"""Tests for nightly-test-builds/nightly_classify.py — the shared classifier,
root-cause map, fingerprinting, and (protocol, chain, intent) manifest preflight
that hardens the nightly tester against false-urgent tickets (VIB-5373).

The four defects under test:

(a) scope-aware auto-close — the in-scope fingerprint set is the allowlist;
(b) (strategy, chain) dedup IGNORING failure_kind — twin failure types collapse
    to ONE fingerprint;
(c) root-cause (not symptom) categorizer — the absl line maps to
    ``anvil_fork_start_failure``, not the raw symptom / ``gateway_error``;
(d) (protocol, chain, intent) manifest preflight — a structurally-unsupported
    combo (PancakeSwap V3 on Optimism) is skipped, a real one is not, and the
    check fails OPEN on anything ambiguous.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_NIGHTLY_DIR = Path(__file__).resolve().parents[2] / "nightly-test-builds"


def _load_nightly_classify():
    if str(_NIGHTLY_DIR) not in sys.path:
        sys.path.insert(0, str(_NIGHTLY_DIR))
    script_path = _NIGHTLY_DIR / "nightly_classify.py"
    spec = importlib.util.spec_from_file_location("nightly_classify", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load nightly_classify from {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    sys.modules["nightly_classify"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def nc():
    return _load_nightly_classify()


# ──────────────────────────────────────────────────────────────────────────
# (b) (strategy, chain) dedup ignoring failure_kind
# ──────────────────────────────────────────────────────────────────────────


def test_twin_failure_types_collapse_to_one_fingerprint(nc):
    """The SAME (strategy, chain) revert classified two different ways must
    produce ONE dedup key. This is the core VIB-5373(b) regression guard:
    EXECUTION_REVERTED (typed) vs UNKNOWN ("other") for the same run filed two
    Urgent tickets before this fix."""
    typed = "Transaction reverted: tx_hash=abc execution reverted"
    untyped = "some opaque failure with no recognizable marker"

    ft_typed = nc.classify_failure_type(typed, outcome="ERROR")
    ft_untyped = nc.classify_failure_type(untyped, outcome="ERROR")
    # The two runs classify differently — that's the real-world twinning.
    assert ft_typed == "execution_revert"
    assert ft_untyped == "other"

    # …but the fingerprint (the dedup key) is identical, so downstream files ONE
    # ticket, not two.
    fp_typed = nc.result_fingerprint("euler_borrow_arbitrum", "arbitrum")
    fp_untyped = nc.result_fingerprint("euler_borrow_arbitrum", "arbitrum")
    assert fp_typed == fp_untyped == "euler_borrow_arbitrum:arbitrum"


def test_fingerprint_excludes_failure_kind_and_normalizes_chain(nc):
    assert nc.result_fingerprint("foo", "Arbitrum") == nc.result_fingerprint("foo", "arbitrum")
    assert "execution_revert" not in nc.result_fingerprint("foo", "arbitrum")
    assert nc.result_fingerprint(" foo ", " ARBITRUM ") == "foo:arbitrum"


# ──────────────────────────────────────────────────────────────────────────
# (c) root-cause (not symptom) categorizer
# ──────────────────────────────────────────────────────────────────────────


def test_absl_symptom_maps_to_anvil_fork_start_failure(nc):
    """The absl::InitializeLog line is a documented DOWNSTREAM symptom of an
    Anvil fork-start failure (almanak/gateway/managed.py). The categorizer must
    report the cause, not the symptom — and must NOT bucket it as the
    ``gateway_error`` it superficially resembles."""
    combined = (
        "E0101 absl::InitializeLog() called multiple times. "
        "This warning ... gRPC cleanup race ..."
    )
    assert nc.classify_failure_type(combined, outcome="ERROR") == "anvil_fork_start_failure"
    cause = nc.root_cause_for(combined)
    assert cause is not None
    assert "Anvil fork failed to start" in cause


def test_root_cause_takes_precedence_over_generic_markers(nc):
    """When a line contains BOTH a generic gateway marker and the absl
    symptom, the root-cause rule wins so the ticket is labelled by cause."""
    combined = (
        "StatusCode.UNAVAILABLE failed to connect to all addresses; "
        "absl::InitializeLog() called multiple times"
    )
    assert nc.classify_failure_type(combined, outcome="ERROR") == "anvil_fork_start_failure"


def test_plain_revert_and_unknown_unchanged(nc):
    assert nc.classify_failure_type("Transaction reverted") == "execution_revert"
    assert nc.classify_failure_type("weird") == "other"
    assert nc.root_cause_for("weird") is None


def test_outcome_buckets(nc):
    assert nc.classify_failure_type("", outcome="SKIPPED") == "funding_failure"
    assert nc.classify_failure_type("", outcome="PARTIAL") == "stranded_funds"
    assert nc.classify_failure_type("", outcome="TIMEOUT") == "timeout"
    assert nc.classify_failure_type("", config_error=True) == "config_error"


# ──────────────────────────────────────────────────────────────────────────
# (d) (protocol, chain, intent) manifest preflight
# ──────────────────────────────────────────────────────────────────────────


def _require_manifest(nc, protocol: str) -> None:
    """Skip when the connector registry / a required manifest is unavailable.

    These tests assert concrete (protocol, chain) manifest facts. In a partial
    checkout the registry is exactly what the production code fails OPEN for, so
    the test must skip rather than hard-fail (mirrors the guard at
    ``test_parse_strategy_metadata_reads_decorator_and_config``)."""
    registry = nc._load_registry()
    if registry is None or registry.get(protocol) is None:
        pytest.skip(f"connector registry / {protocol} manifest unavailable")


def test_unsupported_protocol_chain_is_skipped(nc):
    """PancakeSwap V3 is NOT deployed on Optimism per the connector manifest —
    a real combo from the backlog (VIB-2271). The preflight must flag it so the
    tester skips the run instead of filing a false bug."""
    _require_manifest(nc, "pancakeswap_v3")
    meta = nc.StrategyMetadata(
        name="pancakeswap_v3_swap_optimism",
        chain="optimism",
        protocols=("pancakeswap_v3",),
        intents=("SWAP", "HOLD"),
    )
    reason = nc.preflight_unsupported(meta)
    assert reason is not None
    assert "pancakeswap_v3" in reason
    assert "optimism" in reason


def test_supported_protocol_chain_is_not_skipped(nc):
    """A protocol that IS deployed on the chain must NOT be skipped."""
    _require_manifest(nc, "uniswap_v3")
    meta = nc.StrategyMetadata(
        name="uniswap_rsi_arbitrum",
        chain="arbitrum",
        protocols=("uniswap_v3",),
        intents=("SWAP", "HOLD"),
    )
    assert nc.preflight_unsupported(meta) is None


def test_preflight_fails_open_on_unknown_protocol(nc):
    """An unresolvable protocol name (paper-trade / off-registry venue) must
    NEVER trigger a skip — we cannot prove it unsupported."""
    meta = nc.StrategyMetadata(
        name="mystery_strategy",
        chain="optimism",
        protocols=("not_a_real_connector",),
        intents=("SWAP",),
    )
    assert nc.preflight_unsupported(meta) is None


def test_preflight_fails_open_on_unknown_chain_alias(nc):
    """A chain the registry doesn't model (sonic/berachain) must fail open —
    absence from a manifest's chains list is not proof of non-deployment when
    the chain isn't a KNOWN_VENUE at all."""
    meta = nc.StrategyMetadata(
        name="uniswap_swap_sonic",
        chain="sonic",
        protocols=("uniswap_v3",),
        intents=("SWAP",),
    )
    assert nc.preflight_unsupported(meta) is None


def test_preflight_resolves_chain_alias_bsc_to_bnb(nc):
    """A strategy on `bsc` against a protocol deployed on `bnb` must NOT be
    skipped — the alias resolves to a supported chain."""
    meta = nc.StrategyMetadata(
        name="pancakeswap_v3_swap_bsc",
        chain="bsc",
        protocols=("pancakeswap_v3",),
        intents=("SWAP",),
    )
    assert nc.preflight_unsupported(meta) is None


def test_preflight_resolves_protocol_alias(nc):
    """`balancer` -> `balancer_v2`: aliasing must resolve so a balancer strategy
    on a supported chain is NOT wrongly skipped."""
    meta = nc.StrategyMetadata(
        name="balancer_lp_ethereum",
        chain="ethereum",
        protocols=("balancer",),
        intents=("FLASH_LOAN",),
    )
    assert nc.preflight_unsupported(meta) is None


def test_multi_protocol_strategy_fails_open(nc):
    """A strategy declaring several protocols is supported if ANY one services
    the chain; an unresolvable member fails the whole check open."""
    meta = nc.StrategyMetadata(
        name="cross_dex_arb",
        chain="ethereum",
        protocols=("uniswap_v3", "curve", "not_a_connector"),
        intents=("SWAP",),
    )
    assert nc.preflight_unsupported(meta) is None


def test_preflight_no_protocols_or_chain_fails_open(nc):
    assert nc.preflight_unsupported(
        nc.StrategyMetadata(name="x", chain="optimism", protocols=(), intents=("SWAP",))
    ) is None
    assert nc.preflight_unsupported(
        nc.StrategyMetadata(name="x", chain=None, protocols=("uniswap_v3",), intents=("SWAP",))
    ) is None


# ──────────────────────────────────────────────────────────────────────────
# Strategy-metadata parsing (drives the preflight) — against the real repo
# ──────────────────────────────────────────────────────────────────────────


def test_parse_strategy_metadata_reads_decorator_and_config(nc):
    repo_root = Path(__file__).resolve().parents[2]
    strat = repo_root / "strategies" / "incubating" / "pancakeswap_v3_swap_optimism"
    if not strat.is_dir():
        pytest.skip("reference strategy not present in this checkout")
    meta = nc.parse_strategy_metadata(strat)
    assert meta.name == "pancakeswap_v3_swap_optimism"
    assert meta.chain == "optimism"
    assert "pancakeswap_v3" in meta.protocols
    assert "SWAP" in meta.intents


def test_parse_resolves_module_constant_supported_protocols(nc):
    """Five accounting strategies declare ``supported_protocols=[_PROTOCOL]``
    where ``_PROTOCOL`` is a module-level string constant. The parser must
    resolve the constant's VALUE, not the literal token ``_protocol`` — else the
    manifest preflight silently fails open for every such strategy (CodeRabbit
    VIB-5373 review)."""
    repo_root = Path(__file__).resolve().parents[2]
    strat = repo_root / "strategies" / "accounting" / "lp_v4"
    if not strat.is_dir():
        pytest.skip("reference strategy not present in this checkout")
    meta = nc.parse_strategy_metadata(strat)
    assert "uniswap_v4" in meta.protocols, meta.protocols
    assert "_protocol" not in meta.protocols


def test_resolve_module_constant_helper(nc):
    """Unit-level: a top-level ``NAME = "value"`` is resolved; a non-literal
    assignment (call / f-string) is not (returns None -> token dropped)."""
    text = (
        '_PROTOCOL = "aerodrome_slipstream"\n'
        "_DYNAMIC = some_call()\n"
        '_OTHER = f"{x}_v3"\n'
    )
    assert nc._resolve_module_constant("_PROTOCOL", text) == "aerodrome_slipstream"
    assert nc._resolve_module_constant("_DYNAMIC", text) is None
    assert nc._resolve_module_constant("_OTHER", text) is None
    assert nc._resolve_module_constant("_MISSING", text) is None
    # Mixed literal + identifier list resolves both.
    assert nc._split_list_literal('"uniswap_v3", _PROTOCOL', text) == [
        "uniswap_v3",
        "aerodrome_slipstream",
    ]
