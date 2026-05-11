"""VIB-4278 — augmentation chokepoint reads position_registry.

Covers the new ``registry_lookup`` parameter on
:func:`augment_accounting_payload` and the new
:func:`build_registry_position_reference` helper. Mirrors the layout of
``test_position_reference_shape.py`` so a reviewer can compare the
registry vs legacy contracts side by side.

UAT card: ``docs/internal/uat-cards/VIB-4278.md``.
"""

from __future__ import annotations

import json
import logging

import pytest

from almanak.framework.accounting.position_reference import (
    POSITION_REFERENCE_SOURCES,
    PositionReference,
    build_legacy_position_reference,
    build_registry_position_reference,
)
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.state.exceptions import AccountingPersistenceError

# A realistic position_registry row payload (subset — only the columns the
# augment chokepoint reads). Mirrors the lp_dual narrow-leg row shape from
# ``strategies/accounting/lp_dual/strategy.py``.
SAMPLE_REGISTRY_ROW: dict = {
    "physical_identity_hash": "0xnarrow-leg-hash-deadbeef" + "0" * 24,
    "semantic_grouping_key": "arbitrum:0xpool-usdc-weth-500",
    "grouping_policy_version": "univ3_lp@v1",
    "handle": "leg_narrow",
    "matching_policy_version": 3,
    "status": "open",
    "accounting_category": "lp",
}


# ---------------------------------------------------------------------------
# D1 — build_registry_position_reference helper unit
# ---------------------------------------------------------------------------
def test_build_registry_position_reference_from_row_dict() -> None:
    """The helper builds a frozen PositionReference with all 5 fields stamped."""
    ref = build_registry_position_reference(
        record_for("LP_OPEN"),
        registry_row=SAMPLE_REGISTRY_ROW,
    )
    assert ref.source == "registry"
    assert ref.primitive == "lp"
    assert ref.accounting_category == "lp"
    assert ref.physical_identity_hash == SAMPLE_REGISTRY_ROW["physical_identity_hash"]
    assert ref.semantic_grouping_key == SAMPLE_REGISTRY_ROW["semantic_grouping_key"]
    assert ref.registry_handle == "leg_narrow"
    assert ref.grouping_policy_version == "univ3_lp@v1"
    assert ref.matching_policy_version == 3


def test_build_registry_position_reference_handle_can_be_null() -> None:
    """Auto-mode registry rows have handle=NULL; reference must accept that."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["handle"] = None
    ref = build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)
    assert ref.source == "registry"
    assert ref.registry_handle is None
    # Identity fields still required.
    assert ref.physical_identity_hash is not None


def test_build_registry_rejects_non_open_close() -> None:
    """Only OPEN/CLOSE rows carry a position_reference (mirrors legacy helper)."""
    with pytest.raises(ValueError, match="not OPEN or CLOSE"):
        build_registry_position_reference(
            record_for("SWAP"),
            registry_row=SAMPLE_REGISTRY_ROW,
        )


def test_build_registry_rejects_empty_hash_in_row() -> None:
    """Empty ≠ Zero — an empty string in the registry row is a parser bug."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = ""
    with pytest.raises(ValueError, match="physical_identity_hash"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_none_hash_in_row() -> None:
    """A registry row reaching the helper with hash=None is a write bug.

    Round 2 of PR #2236 tightened the contract: previously the writer
    short-circuited on None hash with a WARN + legacy fallback even in
    live mode; per CodeRabbit this lost the L5_22 join key silently and
    was reclassified as a registry-write bug that must fail loud.
    """
    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = None
    with pytest.raises(ValueError, match="physical_identity_hash"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_none_semantic_grouping_key() -> None:
    """semantic_grouping_key is registry NOT NULL; None reaching here is a write bug."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["semantic_grouping_key"] = None
    with pytest.raises(ValueError, match="semantic_grouping_key"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_empty_semantic_grouping_key() -> None:
    row = dict(SAMPLE_REGISTRY_ROW)
    row["semantic_grouping_key"] = "   "  # whitespace-only
    with pytest.raises(ValueError, match="semantic_grouping_key"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_none_grouping_policy_version() -> None:
    """grouping_policy_version is registry NOT NULL; None reaching here is a write bug."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["grouping_policy_version"] = None
    with pytest.raises(ValueError, match="grouping_policy_version"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_non_string_grouping_policy_version() -> None:
    row = dict(SAMPLE_REGISTRY_ROW)
    row["grouping_policy_version"] = 123  # non-string
    with pytest.raises(ValueError, match="grouping_policy_version"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_bool_matching_policy_version() -> None:
    """bool is an int subclass in Python; explicit reject."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["matching_policy_version"] = True
    with pytest.raises(ValueError, match="matching_policy_version"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_rejects_str_matching_policy_version() -> None:
    row = dict(SAMPLE_REGISTRY_ROW)
    row["matching_policy_version"] = "3"
    with pytest.raises(ValueError, match="matching_policy_version"):
        build_registry_position_reference(record_for("LP_OPEN"), registry_row=row)


def test_build_registry_emits_canonical_source() -> None:
    """The 3 documented sources are stable; registry helper must use 'registry'."""
    ref = build_registry_position_reference(
        record_for("LP_OPEN"),
        registry_row=SAMPLE_REGISTRY_ROW,
    )
    assert ref.source in POSITION_REFERENCE_SOURCES
    assert ref.source == "registry"


# ---------------------------------------------------------------------------
# D1 — augment chokepoint behaviour with registry_lookup hook
# ---------------------------------------------------------------------------
def test_augment_with_registry_lookup_stamps_source_registry() -> None:
    """Lookup callable returns a row → result decodes source='registry'."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return SAMPLE_REGISTRY_ROW

    payload = json.dumps({"event_type": "LP_OPEN"})
    out = json.loads(
        augment_accounting_payload(payload, is_live=True, registry_lookup=lookup)
    )
    pr = out["position_reference"]
    assert pr["source"] == "registry"
    assert pr["physical_identity_hash"] == SAMPLE_REGISTRY_ROW["physical_identity_hash"]
    assert pr["semantic_grouping_key"] == SAMPLE_REGISTRY_ROW["semantic_grouping_key"]
    assert pr["registry_handle"] == "leg_narrow"
    assert pr["grouping_policy_version"] == "univ3_lp@v1"
    assert pr["matching_policy_version"] == 3
    assert calls == [("lp", "open")], (
        f"lookup should be called once with (primitive='lp', event_kind='open'); got {calls}"
    )


def test_augment_with_registry_lookup_on_lp_close() -> None:
    """LP_CLOSE invokes the lookup with event_kind='close'."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return SAMPLE_REGISTRY_ROW

    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "LP_CLOSE"}),
            is_live=True,
            registry_lookup=lookup,
        )
    )
    assert out["position_reference"]["source"] == "registry"
    assert calls == [("lp", "close")]


def test_augment_without_lookup_stamps_legacy() -> None:
    """Default signature (no registry_lookup) keeps emitting source='legacy'."""
    out = json.loads(
        augment_accounting_payload(json.dumps({"event_type": "LP_OPEN"}), is_live=True)
    )
    assert out["position_reference"]["source"] == "legacy"
    assert out["position_reference"]["physical_identity_hash"] is None


def test_augment_lookup_returns_none_falls_back_to_legacy() -> None:
    """F1 — registry has no row for this event → legacy fallback, no error."""
    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "LP_OPEN"}),
            is_live=True,
            registry_lookup=lambda p, k, c: None,
        )
    )
    pr = out["position_reference"]
    assert pr["source"] == "legacy"
    assert pr["physical_identity_hash"] is None
    assert pr["registry_handle"] is None


def test_augment_lookup_not_called_for_swap() -> None:
    """SWAP is event_kind=NONE — chokepoint MUST NOT invoke the lookup."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return SAMPLE_REGISTRY_ROW  # would smuggle if called

    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "SWAP"}),
            is_live=True,
            registry_lookup=lookup,
        )
    )
    assert "position_reference" not in out
    assert calls == [], "lookup must not run on non-OPEN/CLOSE events"


def test_augment_lookup_not_called_for_collect_fees() -> None:
    """LP_COLLECT_FEES is event_kind=COLLECT — chokepoint MUST NOT invoke the lookup."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return SAMPLE_REGISTRY_ROW

    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "LP_COLLECT_FEES"}),
            is_live=True,
            registry_lookup=lookup,
        )
    )
    assert "position_reference" not in out
    assert calls == []


def test_augment_lookup_not_called_on_unknown_event_type_in_paper_mode() -> None:
    """Paper-mode unknown event_type falls back to UTILITY — no lookup, no reference."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return SAMPLE_REGISTRY_ROW

    out = json.loads(
        augment_accounting_payload(
            json.dumps({"event_type": "FROBNICATE"}),
            is_live=False,
            registry_lookup=lookup,
        )
    )
    assert "position_reference" not in out
    assert calls == [], "lookup must not run on unknown-event_type fallback path"


# ---------------------------------------------------------------------------
# D2 — variance / scalability
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("event_type", "expected_primitive", "expected_kind"),
    [
        ("LP_OPEN", "lp", "open"),
        ("LP_CLOSE", "lp", "close"),
        ("PERP_OPEN", "perp", "open"),
        ("PERP_CLOSE", "perp", "close"),
        ("PENDLE_LP_OPEN", "lp", "open"),
        ("PENDLE_LP_CLOSE", "lp", "close"),
        ("VAULT_DEPOSIT", "vault", "open"),
        ("VAULT_WITHDRAW", "vault", "close"),
    ],
)
def test_augment_lookup_invoked_for_every_open_close_primitive(
    event_type: str, expected_primitive: str, expected_kind: str
) -> None:
    """Sweep — the lookup hook fires for every OPEN/CLOSE primitive uniformly."""
    calls: list[tuple[str, str]] = []

    def lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        calls.append((primitive, event_kind))
        return None  # fall back to legacy

    augment_accounting_payload(
        json.dumps({"event_type": event_type}),
        is_live=True,
        registry_lookup=lookup,
    )
    assert calls == [(expected_primitive, expected_kind)], (
        f"event_type={event_type!r}: lookup called with {calls!r}, "
        f"expected {[(expected_primitive, expected_kind)]!r}"
    )


def test_legacy_call_signature_byte_identical_to_pre_vib_4278() -> None:
    """F3 — default signature is bit-for-bit compatible with pre-VIB-4278."""
    payload = json.dumps({"event_type": "LP_OPEN"})
    # The output is sorted-key JSON; identical inputs ⇒ identical bytes.
    out_a = augment_accounting_payload(payload, is_live=True)
    out_b = augment_accounting_payload(payload, is_live=True, registry_lookup=None)
    assert out_a == out_b


# ---------------------------------------------------------------------------
# D3 — Robustness (F1–F8)
# ---------------------------------------------------------------------------
def test_augment_lookup_raises_propagates_in_live() -> None:
    """F2 — DB error during lookup raises AccountingPersistenceError in live mode."""

    def raising(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        raise RuntimeError("simulated DB error")

    with pytest.raises(AccountingPersistenceError) as excinfo:
        augment_accounting_payload(
            json.dumps({"event_type": "LP_OPEN"}),
            is_live=True,
            registry_lookup=raising,
        )
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_augment_lookup_raises_falls_back_in_paper(caplog: pytest.LogCaptureFixture) -> None:
    """F2 — paper mode logs ERROR and falls back to legacy on lookup exception."""

    def raising(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
        raise RuntimeError("simulated DB error")

    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        out = json.loads(
            augment_accounting_payload(
                json.dumps({"event_type": "LP_OPEN"}),
                is_live=False,
                registry_lookup=raising,
            )
        )
    assert out["position_reference"]["source"] == "legacy"
    assert out["position_reference"]["physical_identity_hash"] is None
    assert any("registry_lookup raised" in r.message for r in caplog.records)


def test_augment_registry_row_with_null_hash_raises_in_live(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F7 — physical_identity_hash=None in live mode raises (round 2 tightening).

    Per CodeRabbit PR #2236 round 2: a registry row reaching the augment
    chokepoint with a None / empty identity field is a registry-write
    bug, not a value. In live mode the writer raises
    AccountingPersistenceError so the runner halts on the books-corruption
    signal rather than silently emitting source="legacy" and losing the
    L5_22 join key. Paper / dry_run still falls back to legacy + ERROR
    log — see ``test_augment_registry_row_with_null_hash_paper_falls_back``.
    """
    from almanak.framework.state.exceptions import AccountingPersistenceError

    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = None
    with pytest.raises(AccountingPersistenceError, match="physical_identity_hash"):
        augment_accounting_payload(
            json.dumps({"event_type": "LP_OPEN"}),
            is_live=True,
            registry_lookup=lambda p, k, c: row,
        )


def test_augment_registry_row_with_null_hash_paper_falls_back(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F7 — physical_identity_hash=None in paper mode logs ERROR + falls back to legacy.

    Same registry-write-bug signal as the live test above, but paper
    mode never halts the loop; it logs ERROR and emits the legacy
    reference so dry_run / backtest keeps moving.
    """
    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = None
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        out = json.loads(
            augment_accounting_payload(
                json.dumps({"event_type": "LP_OPEN"}),
                is_live=False,
                registry_lookup=lambda p, k, c: row,
            )
        )
    assert out["position_reference"]["source"] == "legacy"
    assert any("physical_identity_hash" in r.message for r in caplog.records)


def test_augment_registry_row_with_empty_hash_paper_falls_back(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F4 — paper mode logs ERROR + falls back to legacy on empty hash."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = ""
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        out = json.loads(
            augment_accounting_payload(
                json.dumps({"event_type": "LP_OPEN"}),
                is_live=False,
                registry_lookup=lambda p, k, c: row,
            )
        )
    assert out["position_reference"]["source"] == "legacy"
    assert any(
        "Empty" in r.message or "non-empty" in r.message or "shape check" in r.message
        for r in caplog.records
    )


def test_augment_registry_row_with_empty_hash_live_raises() -> None:
    """F4 — live mode raises AccountingPersistenceError on empty hash."""
    row = dict(SAMPLE_REGISTRY_ROW)
    row["physical_identity_hash"] = ""
    with pytest.raises(AccountingPersistenceError):
        augment_accounting_payload(
            json.dumps({"event_type": "LP_OPEN"}),
            is_live=True,
            registry_lookup=lambda p, k, c: row,
        )


def test_augment_idempotent_under_double_invocation_with_registry_lookup() -> None:
    """F5 — calling augment twice with the same lookup produces bit-identical output."""
    payload = json.dumps({"event_type": "LP_OPEN"})
    out1 = augment_accounting_payload(
        payload, is_live=True, registry_lookup=lambda p, k, c: SAMPLE_REGISTRY_ROW
    )
    # Second pass: feed augmented bytes back through with same lookup
    # (simulating GatewayStateManager → SQLiteStore double-augment path).
    out2 = augment_accounting_payload(
        out1, is_live=True, registry_lookup=lambda p, k, c: SAMPLE_REGISTRY_ROW
    )
    assert out1 == out2, "Augment must be idempotent under same-input double invocation"


def test_augment_double_invocation_legacy_then_registry_yields_registry() -> None:
    """F5 — strategy-side legacy augment then sidecar registry augment lands registry."""
    payload = json.dumps({"event_type": "LP_OPEN"})
    # First augment: no lookup (strategy-side, GatewayStateManager).
    pass1 = augment_accounting_payload(payload, is_live=True, registry_lookup=None)
    assert json.loads(pass1)["position_reference"]["source"] == "legacy"
    # Second augment: with lookup (gateway sidecar, SQLiteStore).
    pass2 = augment_accounting_payload(
        pass1, is_live=True, registry_lookup=lambda p, k, c: SAMPLE_REGISTRY_ROW
    )
    final = json.loads(pass2)
    assert final["position_reference"]["source"] == "registry"
    assert final["position_reference"]["physical_identity_hash"] == SAMPLE_REGISTRY_ROW[
        "physical_identity_hash"
    ]


def test_augment_strips_smuggled_position_reference_then_stamps_registry() -> None:
    """Anti-smuggling under registry mode: caller's fake reference is dropped first,
    then the registry reference is constructed fresh.
    """
    smuggled = {
        "source": "receipt",
        "primitive": "perp",
        "accounting_category": "perp",
        "physical_identity_hash": "0xfakeforgery",
        "semantic_grouping_key": None,
        "registry_handle": None,
        "grouping_policy_version": None,
        "matching_policy_version": None,
    }
    payload = json.dumps({"event_type": "LP_OPEN", "position_reference": smuggled})
    out = json.loads(
        augment_accounting_payload(
            payload, is_live=True, registry_lookup=lambda p, k, c: SAMPLE_REGISTRY_ROW
        )
    )
    pr = out["position_reference"]
    # The smuggled receipt+perp pairing is wiped; the registry row wins.
    assert pr["source"] == "registry"
    assert pr["primitive"] == "lp"
    assert pr["physical_identity_hash"] == SAMPLE_REGISTRY_ROW["physical_identity_hash"]


def test_position_reference_frozen_post_construction() -> None:
    """Sanity — registry references are frozen just like legacy."""
    ref = build_registry_position_reference(
        record_for("LP_OPEN"),
        registry_row=SAMPLE_REGISTRY_ROW,
    )
    import dataclasses as _dc

    with pytest.raises(_dc.FrozenInstanceError):
        ref.source = "legacy"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# D3.F6 — silent-error backstop: if a state backend forgets to pass the
# lookup, the chokepoint must NOT silently emit ``source="registry"`` from
# stale data. The default ``registry_lookup=None`` always lands legacy.
# ---------------------------------------------------------------------------
def test_default_signature_never_emits_source_registry() -> None:
    """Backstop — every default call (no lookup) lands source='legacy'."""
    for et in ("LP_OPEN", "LP_CLOSE", "PERP_OPEN", "PERP_CLOSE"):
        out = json.loads(
            augment_accounting_payload(json.dumps({"event_type": et}), is_live=True)
        )
        assert out["position_reference"]["source"] == "legacy", (
            f"{et}: default signature must produce source='legacy', "
            f"got {out['position_reference']['source']!r}"
        )


# ---------------------------------------------------------------------------
# Helper: ensure new helper is exported
# ---------------------------------------------------------------------------
def test_helper_exported() -> None:
    from almanak.framework.accounting import position_reference as pr_mod

    assert "build_registry_position_reference" in pr_mod.__all__


def test_position_reference_class_is_unchanged() -> None:
    """Sanity — VIB-4278 doesn't reshape the dataclass."""
    ref = PositionReference(
        source="registry",
        primitive="lp",
        accounting_category="lp",
        physical_identity_hash="0xabc",
        semantic_grouping_key="key",
        registry_handle="h",
        grouping_policy_version="v1",
        matching_policy_version=3,
    )
    # All fields present in to_dict.
    d = ref.to_dict()
    assert set(d) == {
        "source",
        "primitive",
        "accounting_category",
        "physical_identity_hash",
        "semantic_grouping_key",
        "registry_handle",
        "grouping_policy_version",
        "matching_policy_version",
    }


# ---------------------------------------------------------------------------
# Cross-check — legacy helper output unchanged
# ---------------------------------------------------------------------------
def test_legacy_helper_output_unchanged() -> None:
    legacy = build_legacy_position_reference(record_for("LP_OPEN"))
    assert legacy.source == "legacy"
    assert legacy.physical_identity_hash is None
    assert legacy.semantic_grouping_key is None
    assert legacy.registry_handle is None
    assert legacy.grouping_policy_version is None
    assert legacy.matching_policy_version is None
