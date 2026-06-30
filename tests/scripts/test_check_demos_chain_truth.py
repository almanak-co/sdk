"""Tests for ``scripts/ci/check_demos.py`` Gate 7 — chain truth (VIB-5327).

Gate 7 asserts ``demo.supported_chains ⊆ covering connector.strategy_chains``.
Loaded via ``importlib`` (mirrors ``test_check_connector_chains.py``) so the
script's internals can be driven directly without re-shelling.

The reconciliation core is exercised with a SYNTHETIC manifest registry so
the tests are independent of the live connector corpus:

* over-advertising (chain no manifest supports) → FAIL
* under-advertising (manifest supports chains the demo omits) → WARN, not FAIL
  (intentional single-chain scoping is legitimate; see the gate's docstring)
* exact match → PASS (no failures, no warnings)
* documented + ticketed exception → PASS (failure downgraded to a WARN)
* fork / off-registry protocol the SSOT does not model → SKIP with a WARN
* expired exception → hard error
* real-corpus sweep — the live demo catalog must reconcile clean (no failures).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_demos.py"
    spec = importlib.util.spec_from_file_location("check_demos", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_demos"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def module():
    return _load_module()


def _manifest(name: str, chains, intents):
    """A duck-typed stand-in for ConnectorManifest (chains + named intents)."""
    return SimpleNamespace(
        name=name,
        chains=tuple(chains),
        intents=tuple(SimpleNamespace(name=i) for i in intents),
    )


@pytest.fixture
def index(module):
    """Synthetic index: uniswap_v3 on eth/arb/base/polygon, curve on eth only."""
    manifests = {
        "uniswap_v3": _manifest(
            "uniswap_v3", ("ethereum", "arbitrum", "base", "polygon"), ("SWAP", "LP_OPEN", "LP_CLOSE")
        ),
        "curve": _manifest("curve", ("ethereum",), ("SWAP", "LP_OPEN", "LP_CLOSE")),
        "aerodrome": _manifest("aerodrome", ("base", "optimism"), ("SWAP", "LP_OPEN", "LP_CLOSE")),
    }
    return module.ChainTruthIndex(manifests=manifests, descriptor_aliases={"agni_finance": "uniswap_v3"})


def _reconcile(module, index, *, name, chains, protocols, intents, exceptions=None):
    return module.reconcile_demo_chains(
        name=name,
        supported_chains=chains,
        supported_protocols=protocols,
        intent_types=intents,
        index=index,
        exceptions=exceptions or [],
    )


def test_over_advertising_fails(module, index):
    """A demo claiming a chain no covering manifest supports → FAIL."""
    failures, _ = _reconcile(
        module,
        index,
        name="demo_x",
        chains=["ethereum", "avalanche"],
        protocols=["uniswap_v3"],
        intents=["SWAP", "HOLD"],
    )
    assert len(failures) == 1
    assert "avalanche" in failures[0]
    assert "demo_x" in failures[0]


def test_under_advertising_warns_does_not_fail(module, index):
    """Omitting chains the manifest supports → WARN, never a hard FAIL."""
    failures, warnings = _reconcile(
        module,
        index,
        name="demo_y",
        chains=["ethereum"],
        protocols=["uniswap_v3"],
        intents=["SWAP", "HOLD"],
    )
    assert failures == []
    assert any("under-advertising" in w and "demo_y" in w for w in warnings)


def test_exact_match_passes_clean(module, index):
    """supported_chains == manifest chains → no failures, no warnings."""
    failures, warnings = _reconcile(
        module,
        index,
        name="demo_z",
        chains=["ethereum", "arbitrum", "base", "polygon"],
        protocols=["uniswap_v3"],
        intents=["SWAP", "LP_OPEN", "LP_CLOSE", "HOLD"],
    )
    assert failures == []
    assert warnings == []


def test_documented_exception_downgrades_failure(module, index):
    """A ticketed, unexpired exception turns an over-advertising FAIL into a WARN."""
    exc = module.ChainException(
        demo="demo_x",
        chain="avalanche",
        ticket="VIB-9999",
        until=date.today() + timedelta(days=30),
        reason="manifest extension in flight",
    )
    failures, warnings = _reconcile(
        module,
        index,
        name="demo_x",
        chains=["ethereum", "avalanche"],
        protocols=["uniswap_v3"],
        intents=["SWAP", "HOLD"],
        exceptions=[exc],
    )
    assert failures == []
    assert any("WAIVED" in w and "VIB-9999" in w for w in warnings)


def test_fork_or_offregistry_protocol_is_skipped_not_failed(module, index):
    """A protocol the manifest SSOT does not model → SKIP with a WARN, no FAIL.

    ``agni`` on mantle normalises to ``agni_finance`` (a uniswap_v3 fork). The
    canonical has no manifest, so the gate must NOT fall through to the
    descriptor alias of the raw brand and falsely assert against uniswap_v3.
    """
    failures, warnings = _reconcile(
        module,
        index,
        name="agni_demo",
        chains=["mantle"],
        protocols=["agni"],
        intents=["SWAP", "HOLD"],
    )
    assert failures == []
    assert any("fork / off-registry" in w for w in warnings)


def test_multi_protocol_chain_covered_by_any(module, index):
    """A chain only one of several protocols supports is still covered."""
    # base ∈ aerodrome and uniswap_v3; optimism ∈ aerodrome only.
    failures, _ = _reconcile(
        module,
        index,
        name="multi",
        chains=["base", "optimism"],
        protocols=["uniswap_v3", "aerodrome"],
        intents=["SWAP", "HOLD"],
    )
    assert failures == []


def test_normalize_remaps_brand_to_manifest(module, index):
    """``velodrome`` on optimism normalises to ``aerodrome`` and resolves."""
    m = index.resolve("velodrome", "optimism")
    assert m is not None and m.name == "aerodrome"


def test_resolve_skips_fork_without_manifest(module, index):
    """``agni`` → ``agni_finance`` (no manifest) → None, not uniswap_v3."""
    assert index.resolve("agni", "mantle") is None


def test_load_chain_exceptions_empty(module, tmp_path):
    path = tmp_path / "exc.yml"
    path.write_text("exceptions: []\n")
    assert module._load_chain_exceptions(path) == []


def test_load_chain_exceptions_null_value_is_empty(module, tmp_path):
    # `exceptions:` with no value parses to None — treated as no exceptions.
    path = tmp_path / "exc.yml"
    path.write_text("exceptions:\n")
    assert module._load_chain_exceptions(path) == []


def test_load_chain_exceptions_falsy_non_list_raises(module, tmp_path):
    # A falsy-but-non-list value must fail schema validation, not coerce to [].
    for bad in ("exceptions: {}\n", "exceptions: false\n", "exceptions: ''\n"):
        path = tmp_path / "exc.yml"
        path.write_text(bad)
        with pytest.raises(ValueError):
            module._load_chain_exceptions(path)


def test_load_chain_exceptions_non_dict_root_raises(module, tmp_path):
    path = tmp_path / "exc.yml"
    path.write_text("- just\n- a\n- list\n")
    with pytest.raises(ValueError):
        module._load_chain_exceptions(path)


def test_load_chain_exceptions_missing_file(module, tmp_path):
    assert module._load_chain_exceptions(tmp_path / "nope.yml") == []


def test_load_chain_exceptions_expired_raises(module, tmp_path):
    path = tmp_path / "exc.yml"
    path.write_text(
        "exceptions:\n  - demo: d\n    chain: polygon\n    ticket: VIB-1\n    until: 2000-01-01\n    reason: stale\n"
    )
    with pytest.raises(module.ChainExceptionExpiredError):
        module._load_chain_exceptions(path)


def test_load_chain_exceptions_missing_field_raises(module, tmp_path):
    path = tmp_path / "exc.yml"
    path.write_text("exceptions:\n  - demo: d\n    chain: polygon\n")
    with pytest.raises(ValueError):
        module._load_chain_exceptions(path)


def test_load_chain_exceptions_duplicate_raises(module, tmp_path):
    path = tmp_path / "exc.yml"
    row = "  - demo: d\n    chain: polygon\n    ticket: VIB-1\n    until: 2999-01-01\n    reason: r\n"
    path.write_text("exceptions:\n" + row + row)
    with pytest.raises(ValueError):
        module._load_chain_exceptions(path)


def test_real_corpus_reconciles_clean(module):
    """The live demo catalog must have zero over-advertising failures."""
    from almanak.framework.demos import DemoCatalog

    catalog = DemoCatalog.discover()
    result = module.gate_chain_truth(catalog)
    assert result.failures == [], f"unexpected chain-truth failures: {result.failures}"
