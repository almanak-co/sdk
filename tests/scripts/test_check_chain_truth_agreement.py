"""Tests for ``scripts/ci/check_chain_truth_agreement.py`` — the config-only blind spot.

The gate compares the published catalogue (``almanak info matrix --json``)
against the config gate (``SUPPORTED_PROTOCOLS``) pair by pair. Its two
directions are asymmetric, and that asymmetry is where it can go quietly blind:

* *matrix-only* (published, config rejects) is found by iterating ``published``;
* *unmapped* (published, no config entry at all) is counted separately;
* *config-only* (config accepts, nothing published) is the only direction that
  has to be found by iterating ``SUPPORTED_PROTOCOLS`` — so a filter on that
  loop is the one place a whole class of drift can disappear without a trace.

Dropping every config key absent from the catalogue is exactly such a filter:
it hides descriptor aliases (intended) and a canonical protocol the matrix
builder forgot (not intended) with no way to tell the two apart. These tests
pin that a rowless config key is excused only for a reason a descriptor
actually declares.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    """Load the gate as ``check_chain_truth_agreement`` so its globals are stable."""
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "check_chain_truth_agreement.py"
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    spec = importlib.util.spec_from_file_location("check_chain_truth_agreement", script_path)
    if spec is None or spec.loader is None:  # pragma: no cover - import plumbing
        raise RuntimeError(f"Failed to load {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_chain_truth_agreement"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _load_module()


def test_live_repo_agrees(gate) -> None:
    """The gate must be green on the checkout it ships with."""
    disagreements, _unmapped, pairs = gate.find_disagreements()
    assert pairs > 0, "a gate that compared nothing looks identical to a gate that passed"
    assert disagreements == []


def test_every_rowless_config_key_has_a_declared_reason(gate) -> None:
    """The exception set is live, and each member names why it has no row."""
    published = gate._published_chains_by_protocol()
    rowless = gate._rowless_by_design(published)

    # Vacuous truth would make the next assertion meaningless.
    assert rowless, "no config key is currently rowless — this test can no longer detect over-excusing"
    for key, reason in rowless.items():
        assert key not in published, key
        assert reason, key


def test_a_canonical_protocol_missing_from_the_catalogue_is_reported(gate, monkeypatch) -> None:
    """The class the blanket ``protocol in published`` filter used to swallow.

    ``aave_v3`` is published today, so removing it from the catalogue
    simulates exactly one defect: ``_build_matrix()`` dropping a canonical row
    the config gate still accepts. Nothing on the descriptor declares a reason
    for that absence, so it must surface as config-only drift — the user reads
    the catalogue, does not find the protocol, and cannot tell the SDK would
    have run it.
    """
    published = gate._published_chains_by_protocol()
    victim = "aave_v3"
    assert victim in published, "fixture assumption broke: pick another published canonical protocol"

    pruned = {name: chains for name, chains in published.items() if name != victim}
    monkeypatch.setattr(gate, "_published_chains_by_protocol", lambda: pruned)

    disagreements, _unmapped, _pairs = gate.find_disagreements()
    reported = {d.chain for d in disagreements if d.protocol == victim and d.direction == gate.CONFIG_ONLY}
    assert reported, f"{victim} vanished from the catalogue and the gate stayed clean"


def test_a_rowless_by_design_key_is_still_excused(gate, monkeypatch) -> None:
    """Excusing must stay keyed to the declared reason, not to absence itself.

    A descriptor alias covered by its parent's published row is not drift, so
    the narrowed filter must not start reporting it — that would trade a blind
    spot for noise and the gate would be waived back into uselessness.
    """
    published = gate._published_chains_by_protocol()
    rowless = gate._rowless_by_design(published)
    disagreements, _unmapped, _pairs = gate.find_disagreements()

    assert not [d for d in disagreements if d.protocol in rowless]
