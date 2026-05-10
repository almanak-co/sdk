"""Hand-edit guard for the frozen dispatch truth table (VIB-4163 origin,
VIB-4166 contract update).

The fixture ``tests/fixtures/accounting/legacy_dispatch_truth_table.json``
captures the legacy if-ladder dispatcher's output for the parity test
``test_dispatch_parity_against_legacy_truth_table``. The intent of THIS
file is to catch hand-edits to the fixture — i.e. someone tweaking
expected payloads to make a parity failure go away instead of fixing the
underlying dispatcher.

Contract evolution:
    * **VIB-4163 (origin)**: the guard asserted "exactly 1 commit ever
      touched the file" because at T3 there was a single precursor freeze
      commit and the discipline was "freeze it and never touch it".
    * **VIB-4166 (T6 of VIB-4160)**: T6 added ``primitive_version`` to
      every event's ``to_payload_json`` output. The fixture's
      ``expected_payload`` blocks had to mirror this additive change
      (otherwise the parity test would fail, and rightly so). The
      "exactly 1 commit" rule was thus mathematically incompatible with
      ANY legitimate payload-shape evolution of the legacy dispatcher's
      output. The contract is now stricter AND more precise: the
      committed file must be byte-identical to what the generator
      (``_generate_legacy_dispatch_truth_table.build_truth_table_json_text``)
      would produce TODAY. Hand-edits diverge from generator output;
      legitimate regenerations don't.

The runtime contract (legacy ladder vs registry parity) is still
enforced by ``test_dispatch_parity_against_legacy_truth_table`` in
``test_category_handler_registry.py`` and
``test_classifier_parity_against_frozen_truth_table`` in
``test_classifier_taxonomy_parity.py``. The companion freeze-discipline
guard ``test_truth_table_precursor_is_not_the_t3_commit`` was removed
during T4 (VIB-4164) — see the comment block below for the rationale.
"""

from __future__ import annotations

from pathlib import Path

from tests.fixtures.accounting._generate_legacy_dispatch_truth_table import (
    build_truth_table_json_text,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TRUTH_TABLE_PATH = _REPO_ROOT / "tests/fixtures/accounting/legacy_dispatch_truth_table.json"


def test_truth_table_matches_generator_output() -> None:
    """The committed truth-table must be byte-identical to what the
    regenerator script produces in-process today.

    Catches hand-edits to ``legacy_dispatch_truth_table.json`` — the
    original VIB-4163 concern — without falsely failing when a legitimate
    payload-shape evolution (e.g. VIB-4166's additive ``primitive_version``
    stamp) is regenerated through the canonical generator. If THIS test
    fails, EITHER the file was hand-edited (re-run the generator and
    commit) OR the dispatcher's output drifted without the fixture being
    refreshed (re-run the generator and commit).

    Re-run with::

        uv run python tests/fixtures/accounting/_generate_legacy_dispatch_truth_table.py
    """
    expected_bytes = build_truth_table_json_text().encode("utf-8")
    actual_bytes = _TRUTH_TABLE_PATH.read_bytes()
    assert actual_bytes == expected_bytes, (
        f"{_TRUTH_TABLE_PATH.relative_to(_REPO_ROOT)} content diverges from generator output. "
        f"Either the file was hand-edited (re-run "
        f"`uv run python tests/fixtures/accounting/_generate_legacy_dispatch_truth_table.py` "
        f"and commit) OR the legacy dispatcher's output changed without the fixture being "
        f"refreshed (same fix). Lengths: actual={len(actual_bytes)} bytes, "
        f"expected={len(expected_bytes)} bytes."
    )


# test_truth_table_committed_exactly_once — REPLACED by the content-based
# guard above (VIB-4166, T6 of VIB-4160).
#
# The original "exactly 1 commit ever touched this file" check was a
# heuristic for "didn't hand-edit", but it's mathematically incompatible
# with ANY legitimate evolution of the legacy dispatcher's `to_payload_json`
# output: when the shape changes (additively or otherwise), the fixture
# MUST be regenerated for the parity test to pass, which then trips the
# commit-count rule. The new content-based check is strictly stronger:
# (a) catches hand-edits (the original concern) by detecting any
#     divergence from generator output, including subtle ones the
#     commit-count check would have missed (e.g. a "fix-up" commit that
#     introduced a tiny wrong value), and
# (b) allows legitimate regenerations under any future payload-shape
#     change without locking the codebase out of evolution.
#
# test_truth_table_precursor_is_not_the_t3_commit — REMOVED (VIB-4164, T4).
#
# The test was a pre-merge review-discipline guard for T3 (VIB-4163): "precursor
# freeze and T3 dispatcher rewrite must be in different commits". After T3's
# squash-merge to main (commit 325e3d3ca), both files share that commit by
# construction — the discipline window has closed. The runtime contract (legacy
# ladder vs registry parity) is enforced by `test_classifier_parity_against_frozen_truth_table`
# which remains in `test_classifier_taxonomy_parity.py`. Removing this stale
# guard alongside the analogous `test_precursor_files_committed_separately`
# (in `test_no_scoring_drift.py`) which T4 also drops for the same reason.
