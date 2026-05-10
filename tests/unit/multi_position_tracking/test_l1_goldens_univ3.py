"""Shape-contract tests for the L1 offline UniV3 goldens (VIB-4194 / T08).

These tests enforce every invariant the goldens advertise. T11/T12 will consume
the goldens to validate atomic-commit behavior and per-primitive cutover; this
file's job is to guarantee that what they consume is well-formed BEFORE they
consume it.

The loader functions defined at the top of the file are the structural backstop
for the silent-failure-class regressions a careless future edit would introduce
(missing column, typo, invented enum, accidental zero-substitution). Every
adversarial input in the D3 section MUST raise — never return None, never
return a partially-populated dict, never silently coerce.

Reference: ``docs/internal/uat-cards/VIB-4194.md``.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.accounting.payload_schemas import (
    MATCHING_POLICY_VERSIONS,
    validate_payload,
)
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import AccountingCategory, Primitive

# =============================================================================
# Constants & fixture roots
# =============================================================================

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE_ROOT = _REPO_ROOT / "tests" / "fixtures" / "multi-position-tracking" / "univ3-arbitrum"

_FIXTURE_NAMES = ("swap", "lp_open", "lp_close")
_LP_FIXTURE_NAMES = ("lp_open", "lp_close")
_REQUIRED_FILES = (
    "receipt.json",
    "pre_state.json",
    "oracle_snapshot.json",
    "expected_ledger_row.json",
    "expected_registry_row.json",
    "expected_accounting_event.json",
)

# 16 columns per PRD §Registry Data Shape (multi-position-tracking.md line 119).
_REGISTRY_COLUMNS_16 = frozenset(
    {
        "deployment_id",
        "chain",
        "primitive",
        "accounting_category",
        "physical_identity_hash",
        "semantic_grouping_key",
        "grouping_policy_version",
        "handle",
        "status",
        "payload",
        "opened_at_block",
        "opened_tx",
        "closed_at_block",
        "closed_tx",
        "last_reconciled_at_block",
        "matching_policy_version",
    }
)

# Per PRD §Position Reference Shape (line 501) — sources canonical at Day 1.
_VALID_POSITION_REFERENCE_SOURCES = frozenset({"receipt", "registry", "legacy"})

# Canonical UniV3 NFT manager on Arbitrum (per parser POSITION_MANAGER_ADDRESSES).
_NFT_MANAGER_ARBITRUM = "0xc36442b4a4522e871399cd717abdd847ab11fe88"

# IncreaseLiquidity event signature topic (per receipt_parser EVENT_TOPICS).
_TOPIC_INCREASE_LIQUIDITY = "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f"
# DecreaseLiquidity event signature topic — keccak256("DecreaseLiquidity(uint256,uint128,uint256,uint256)")
_TOPIC_DECREASE_LIQUIDITY = "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4"


# =============================================================================
# Shape-contract loaders (this file is the loader; no production loader exists)
# =============================================================================


def _read_fixture_json(name: str, file: str) -> dict[str, Any]:
    """Read a fixture file and assert it parses to a dict.

    Strict: the top-level MUST be a dict. A JSON list at the top level is a
    common silent-failure mode (a future agent saving a bare event log instead
    of the wrapper object). We raise rather than coerce.
    """
    path = _FIXTURE_ROOT / name / file
    if not path.is_file():
        raise FileNotFoundError(f"Missing fixture file: {path}")
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise TypeError(
            f"{path}: top-level JSON MUST be a dict, got {type(data).__name__}. "
            "Bare lists are silent-failure-prone — wrap the payload in a dict."
        )
    return data


def _load_registry_row(name: str) -> dict[str, Any]:
    """Load and validate the expected_registry_row.json for a fixture set.

    Strict invariants enforced here (every silent-failure-class issue D3 calls
    out is enforced at this single chokepoint):

    - The 16 PRD columns are ALL present (no missing field).
    - No extra unknown columns (no typo can sneak through).
    - ``primitive`` is a valid ``Primitive`` enum value (raises ValueError on
      invented strings like ``'lpv3'``).
    - ``accounting_category`` is a valid ``AccountingCategory`` enum value.
    - ``physical_identity_hash`` is a non-empty, non-whitespace string.
    - ``status`` is one of the canonical three values (per SQLite CHECK).

    The SWAP fixture's expected_registry_row.json carries the explicit
    "_no_registry_row" sentinel and is short-circuited by the caller — it does
    NOT pass through this validator. SWAP is not a position-establishing intent.
    """
    data = _read_fixture_json(name, "expected_registry_row.json")
    # Strip _comment / _* keys (documentation, not data).
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    actual = set(clean)
    missing = _REGISTRY_COLUMNS_16 - actual
    extra = actual - _REGISTRY_COLUMNS_16
    if missing:
        raise KeyError(
            f"{name}/expected_registry_row.json is missing required columns: "
            f"{sorted(missing)}. The 16-column PRD shape is mandatory."
        )
    if extra:
        raise KeyError(
            f"{name}/expected_registry_row.json has unknown columns: "
            f"{sorted(extra)}. A typo here would silently land in T11's INSERT — "
            "every key must match the canonical PRD shape exactly."
        )
    # primitive / accounting_category MUST be canonical enum values.
    Primitive(clean["primitive"])  # raises ValueError on invented strings
    AccountingCategory(clean["accounting_category"])
    # physical_identity_hash non-empty.
    pih = clean["physical_identity_hash"]
    if not isinstance(pih, str) or not pih.strip():
        raise ValueError(
            f"{name}/expected_registry_row.json: physical_identity_hash must be "
            f"a non-empty, non-whitespace string (got {pih!r})."
        )
    # status pinned to the SQLite CHECK values.
    if clean["status"] not in {"open", "closed", "reorg_invalidated"}:
        raise ValueError(
            f"{name}/expected_registry_row.json: status must be one of "
            f"'open' | 'closed' | 'reorg_invalidated' (got {clean['status']!r})."
        )
    return clean


def _load_accounting_event(name: str) -> dict[str, Any]:
    """Load and validate expected_accounting_event.json.

    For position-establishing events (LP_OPEN/LP_CLOSE), ``position_reference``
    MUST be present and shape-valid. For SWAP, ``position_reference`` MUST
    be absent or null (SWAP has no position).

    The ``payload`` is also validated against the production Pydantic models
    in ``almanak.framework.accounting.payload_schemas`` via ``validate_payload``.
    This is the load-bearing contract that prevents fixture drift from passing
    silently — if the production schema gains a required field or renames one,
    the goldens MUST be updated in lockstep, and this loader will surface the
    mismatch as a hard failure rather than letting T11/T12 ship on stale shapes.
    """
    data = _read_fixture_json(name, "expected_accounting_event.json")
    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    # Validate payload against the production v1 Pydantic model. Returns None
    # for non-v1 event_types (PENDLE, POLYMARKET) — those are out of v1 scope
    # per AttemptNo17 §8.5; raise on v1 mismatch (contract drift). Only run
    # when both keys are present so the position_reference adversarial test
    # cases (which intentionally omit ``payload``) still surface their own
    # specific failure mode rather than getting short-circuited here.
    if "event_type" in clean and "payload" in clean:
        validate_payload(clean["event_type"], clean["payload"])
    pr = clean.get("position_reference")
    if pr is not None:
        if not isinstance(pr, dict):
            raise TypeError(
                f"{name}: position_reference MUST be a dict or null/absent "
                f"(got {type(pr).__name__})."
            )
        # source enum
        src = pr.get("source")
        if src not in _VALID_POSITION_REFERENCE_SOURCES:
            raise ValueError(
                f"{name}: position_reference.source must be one of "
                f"{sorted(_VALID_POSITION_REFERENCE_SOURCES)} (got {src!r})."
            )
        # primitive / category enums
        Primitive(pr["primitive"])
        AccountingCategory(pr["accounting_category"])
        # hash non-empty
        if not isinstance(pr.get("physical_identity_hash"), str) or not pr[
            "physical_identity_hash"
        ].strip():
            raise ValueError(
                f"{name}: position_reference.physical_identity_hash must be a "
                "non-empty, non-whitespace string."
            )
    return clean


def _compute_univ3_arbitrum_pih(token_id: int) -> str:
    """Recompute physical_identity_hash from the canonical receipt-derivable inputs.

    Per ``docs/internal/qa/parser-coverage-audit-tier1-20260508.md``, UniV3's
    identity tuple is ``(chain, nft_manager_addr, token_id)``. The hash is
    SHA-256 over the canonical seed string. The README at the fixture root
    documents the exact recipe.
    """
    seed = f"arbitrum:{_NFT_MANAGER_ARBITRUM}:{token_id}"
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


def _extract_token_id_from_receipt(
    receipt: dict[str, Any], topic: str = _TOPIC_INCREASE_LIQUIDITY
) -> int:
    """Pull ``tokenId`` from a receipt log keyed by event topic.

    Per parser source ``receipt_parser.py:1611-1619``, on LP_OPEN ``tokenId`` is
    ``IncreaseLiquidity.topics[1]`` decoded as a uint256 hex string emitted by the
    NPM. The mirror on LP_CLOSE is ``DecreaseLiquidity.topics[1]`` from the same
    NPM. The log emitter address MUST be the chain's NFT manager; we
    receipt-validate that property here so the recompute can't silently use the
    wrong log.
    """
    for log in receipt.get("logs", []):
        topics = log.get("topics", [])
        if len(topics) < 2:
            continue
        if topics[0].lower() != topic:
            continue
        emitter = (log.get("address") or "").lower()
        if emitter != _NFT_MANAGER_ARBITRUM:
            continue
        return int(topics[1], 16)
    raise LookupError(
        f"No log with topic {topic} from the canonical UniV3 NFT manager"
    )


# =============================================================================
# D1 — Correctness
# =============================================================================


@pytest.mark.parametrize("name", _FIXTURE_NAMES)
def test_fixture_directory_exists(name: str) -> None:
    """D1.S1 — every fixture set has all six required files."""
    fixture_dir = _FIXTURE_ROOT / name
    assert fixture_dir.is_dir(), f"Missing fixture directory: {fixture_dir}"
    for fname in _REQUIRED_FILES:
        path = fixture_dir / fname
        assert path.is_file(), f"Missing fixture file: {path}"


def test_readme_exists() -> None:
    """README.md exists at the fixture root and cites the source tx hashes."""
    readme = _FIXTURE_ROOT / "README.md"
    assert readme.is_file(), f"Missing README at {readme}"


@pytest.mark.parametrize("name,fname", [(n, f) for n in _FIXTURE_NAMES for f in _REQUIRED_FILES])
def test_every_fixture_file_is_valid_json_dict(name: str, fname: str) -> None:
    """D1.S2 — every file parses to a dict (not a list, not a scalar)."""
    data = _read_fixture_json(name, fname)
    assert isinstance(data, dict)


def test_lp_open_registry_row_has_all_16_columns() -> None:
    """D1.S3 — LP_OPEN registry row carries every PRD column."""
    row = _load_registry_row("lp_open")
    actual = set(row)
    assert actual == _REGISTRY_COLUMNS_16, (
        f"LP_OPEN row column-set mismatch:\n"
        f"  missing: {sorted(_REGISTRY_COLUMNS_16 - actual)}\n"
        f"  extra:   {sorted(actual - _REGISTRY_COLUMNS_16)}"
    )


def test_lp_close_registry_row_has_all_16_columns() -> None:
    """D1.S3 — LP_CLOSE registry row carries every PRD column."""
    row = _load_registry_row("lp_close")
    actual = set(row)
    assert actual == _REGISTRY_COLUMNS_16


def test_swap_registry_row_is_explicit_sentinel() -> None:
    """D1.S3 — SWAP fixture's expected_registry_row.json is the documented sentinel.

    SWAP is not a position-establishing intent (record_for('SWAP').position_type
    is None). Per PRD §Forbidden Patterns line 261, registry rows MUST NOT be
    seeded for these intents. The sentinel exists for symmetry so consumers can
    iterate the three sets uniformly.
    """
    sentinel = _read_fixture_json("swap", "expected_registry_row.json")
    assert sentinel.get("_no_registry_row") is True, (
        "SWAP/expected_registry_row.json must carry _no_registry_row=true sentinel; "
        f"got {sentinel!r}. Per PRD line 261 SWAP must not seed a registry row."
    )
    assert "reason" in sentinel and isinstance(sentinel["reason"], str), (
        "Sentinel must include a human-readable reason string."
    )
    # And taxonomy CONFIRMS this is a no-position intent.
    assert record_for("SWAP").position_type is None


def test_registry_rows_use_canonical_taxonomy() -> None:
    """D1.S4 — primitive + accounting_category match record_for(intent_type) per row.

    Each LP fixture's registry row is checked against ``record_for`` with its own
    intent_type, not the OPEN type for both — this guards against a future schema
    change where LP_OPEN/LP_CLOSE diverge on category/primitive. They MUST agree
    today, and the test asserts the agreement explicitly.
    """
    intent_for_fixture = {"lp_open": "LP_OPEN", "lp_close": "LP_CLOSE"}
    for fix in _LP_FIXTURE_NAMES:
        record = record_for(intent_for_fixture[fix])
        expected_primitive = record.primitive.value
        expected_category = record.accounting_category.value
        row = _load_registry_row(fix)
        assert row["primitive"] == expected_primitive, (
            f"{fix}: registry row primitive={row['primitive']!r}, "
            f"expected {expected_primitive!r} "
            f"(record_for({intent_for_fixture[fix]!r}).primitive.value)"
        )
        assert row["accounting_category"] == expected_category, (
            f"{fix}: registry row accounting_category={row['accounting_category']!r}, "
            f"expected {expected_category!r}"
        )
        # Stronger structural check — the values match the LP enum constants exactly.
        assert row["primitive"] == Primitive.LP.value
        assert row["accounting_category"] == AccountingCategory.LP.value
    # And the OPEN/CLOSE pair MUST agree on these values today (else lifecycle
    # matching breaks).
    assert record_for("LP_OPEN").primitive == record_for("LP_CLOSE").primitive
    assert (
        record_for("LP_OPEN").accounting_category
        == record_for("LP_CLOSE").accounting_category
    )


def test_lp_open_close_same_physical_identity_hash() -> None:
    """D1.S5 — LP_OPEN and LP_CLOSE point at the SAME NFT (same hash).

    Load-bearing: T11/T12's lifecycle-matching tests rely on byte-identical
    hashes across the open/close pair. A hash mismatch here would silently
    corrupt every downstream identity-matching test.
    """
    open_row = _load_registry_row("lp_open")
    close_row = _load_registry_row("lp_close")
    assert open_row["physical_identity_hash"] == close_row["physical_identity_hash"], (
        "LP_OPEN and LP_CLOSE physical_identity_hash MUST be equal "
        "(same NFT) — they are not. Goldens are inconsistent.\n"
        f"  open : {open_row['physical_identity_hash']}\n"
        f"  close: {close_row['physical_identity_hash']}"
    )
    # Same for semantic_grouping_key (same pool).
    assert open_row["semantic_grouping_key"] == close_row["semantic_grouping_key"]


def test_physical_identity_hash_derivable_from_receipt() -> None:
    """D1.S5 — the hash recomputes from receipt facts only.

    Per PRD §Hard Gates Gate 1 + parser-coverage-audit, UniV3's identity-tuple
    inputs are ``(chain, nft_manager_addr, token_id)``. ``token_id`` is read
    from the receipt's IncreaseLiquidity log topic[1]; ``nft_manager_addr``
    is asserted to be the log emitter (receipt-validated), which matches the
    parser's chain-keyed config constant. The recompute MUST agree with the
    hash stored in the LP_OPEN golden.
    """
    receipt = _read_fixture_json("lp_open", "receipt.json")
    token_id = _extract_token_id_from_receipt(receipt)
    # 5467895 is the documented tokenId per the run report.
    assert token_id == 5467895
    expected = _compute_univ3_arbitrum_pih(token_id)
    row = _load_registry_row("lp_open")
    assert row["physical_identity_hash"] == expected, (
        f"physical_identity_hash recompute mismatch.\n"
        f"  receipt-derived: {expected}\n"
        f"  golden value   : {row['physical_identity_hash']}\n"
        "Either the golden was hand-edited away from receipt facts, or the "
        "recompute recipe drifted. Check the README's hash recipe section."
    )


def test_lp_close_physical_identity_hash_derivable_from_receipt() -> None:
    """D1.S5 mirror — LP_CLOSE hash recomputes from its OWN receipt's
    DecreaseLiquidity log (NPM-emitted). Without this, the close-side hash
    would only be checked by hand-asserted equality with the open-side row,
    leaving room for a future agent to silently change the close payload's
    tokenId (Codex P1 #2). This guarantees end-to-end receipt-derivability
    on both sides of the lifecycle.
    """
    receipt = _read_fixture_json("lp_close", "receipt.json")
    token_id = _extract_token_id_from_receipt(receipt, _TOPIC_DECREASE_LIQUIDITY)
    assert token_id == 5467895, (
        f"LP_CLOSE receipt's DecreaseLiquidity topic[1] decoded to tokenId "
        f"{token_id}, expected 5467895 (the same NFT as LP_OPEN)."
    )
    expected = _compute_univ3_arbitrum_pih(token_id)
    row = _load_registry_row("lp_close")
    assert row["physical_identity_hash"] == expected, (
        "LP_CLOSE physical_identity_hash recompute mismatch.\n"
        f"  receipt-derived: {expected}\n"
        f"  golden value   : {row['physical_identity_hash']}"
    )


def test_registry_payload_carries_univ3_identity_bag() -> None:
    """D1.S5b — payload carries token_id, pool_address, ticks across both rows."""
    open_row = _load_registry_row("lp_open")
    close_row = _load_registry_row("lp_close")
    for fix, row in (("lp_open", open_row), ("lp_close", close_row)):
        payload = row["payload"]
        assert isinstance(payload, dict), f"{fix}: payload must be a dict"
        for key in ("token_id", "pool_address", "tick_lower", "tick_upper"):
            assert key in payload, f"{fix}: payload missing required key {key!r}"
        # Field-swap canary.
        assert payload["tick_lower"] < payload["tick_upper"], (
            f"{fix}: tick_lower ({payload['tick_lower']}) must be < tick_upper "
            f"({payload['tick_upper']}). Field-swap suspected."
        )
    # Same NFT: token_id matches across the pair.
    assert open_row["payload"]["token_id"] == close_row["payload"]["token_id"]


def test_matching_policy_version_matches_runtime_constant() -> None:
    """D1.S6 — goldens stamp MATCHING_POLICY_VERSIONS[Primitive.LP] (== 3)."""
    expected = MATCHING_POLICY_VERSIONS[Primitive.LP]
    for fix in _LP_FIXTURE_NAMES:
        row = _load_registry_row(fix)
        assert row["matching_policy_version"] == expected, (
            f"{fix}: matching_policy_version={row['matching_policy_version']}, "
            f"runtime expects {expected}"
        )
        # Accounting event stamps it too.
        evt = _load_accounting_event(fix)
        assert evt["matching_policy_version"] == expected


def test_position_reference_shape_for_lp() -> None:
    """D1.S7 — LP_OPEN/LP_CLOSE accounting events carry the PRD shape."""
    open_row = _load_registry_row("lp_open")
    close_row = _load_registry_row("lp_close")

    for fix, reg_row in (("lp_open", open_row), ("lp_close", close_row)):
        evt = _load_accounting_event(fix)
        pr = evt.get("position_reference")
        assert pr is not None, f"{fix}: position_reference must be present for LP events"
        assert pr["source"] == "receipt", (
            f"{fix}: position_reference.source must be 'receipt' (Day-1 mode); "
            f"got {pr['source']!r}"
        )
        assert pr["primitive"] == "lp"
        assert pr["accounting_category"] == "lp"
        # Hash agreement with the matching registry row.
        assert pr["physical_identity_hash"] == reg_row["physical_identity_hash"], (
            f"{fix}: accounting-event hash != registry-row hash. The two "
            "consumers would disagree on identity — that's the bug-#2130 class."
        )


def test_swap_accounting_event_no_position_reference() -> None:
    """D1.S8 — SWAP accounting event has no position_reference."""
    evt = _load_accounting_event("swap")
    pr = evt.get("position_reference")
    assert pr is None, (
        "SWAP accounting event must NOT carry position_reference (no position). "
        f"Got: {pr!r}"
    )
    assert evt["category"] == "swap"
    assert evt["primitive"] == "swap"


@pytest.mark.parametrize("name", _FIXTURE_NAMES)
def test_accounting_event_payload_validates_against_production_pydantic(name: str) -> None:
    """D1.S8b — every payload survives the production Pydantic v1 model.

    This is a sentinel guarding against the silent-fail pattern Codex P1 / Gemini
    flagged on the first review pass: handcrafted golden payloads naming fields
    the production schema doesn't know (``amount0_principal``, ``amount_in_decimal``,
    ``fees_collected_usd``). ``validate_payload`` raises ``ValueError`` on contract
    drift; if a future edit to the schema adds a required field or renames one,
    this test surfaces the mismatch loudly rather than letting T11/T12's L2 tests
    ship on stale fixtures.
    """
    data = _read_fixture_json(name, "expected_accounting_event.json")
    payload = {k: v for k, v in data.items() if not k.startswith("_")}["payload"]
    event_type = data["event_type"]
    # validate_payload returns the parsed model on success; None for out-of-v1 types
    # (PENDLE/POLYMARKET — none of our three SWAP/LP_OPEN/LP_CLOSE fall in that bucket);
    # raises ValueError on v1 contract drift.
    result = validate_payload(event_type, payload)
    assert result is not None, (
        f"{name}: event_type={event_type!r} is unexpectedly outside the v1 surface "
        "— SWAP/LP_OPEN/LP_CLOSE must all validate via the v1 rail."
    )


# =============================================================================
# D2 — Scalability
# =============================================================================


def test_all_three_intent_types_covered() -> None:
    """D2.M1 — three intent types: SWAP, LP_OPEN, LP_CLOSE."""
    expected_intent = {
        "swap": "SWAP",
        "lp_open": "LP_OPEN",
        "lp_close": "LP_CLOSE",
    }
    for fix, intent in expected_intent.items():
        led = _read_fixture_json(fix, "expected_ledger_row.json")
        assert led["intent_type"] == intent, (
            f"{fix}/expected_ledger_row.json: intent_type={led['intent_type']!r}, "
            f"expected {intent!r}"
        )


def test_registry_consumer_can_construct_RegistryRow() -> None:
    """D2.M2 — registry consumers (T11/T12) can construct RegistryRow from goldens.

    This is the structural "shape contract" with the production consumer:
    every column the JSON declares MUST be a valid kwarg on the dataclass.
    """
    for fix in _LP_FIXTURE_NAMES:
        row_dict = _load_registry_row(fix)
        # RegistryRow accepts the canonical 16-column shape directly. handle is
        # a kwarg with default None so the JSON null is fine. The ``.payload``
        # field is dict-typed.
        rr = RegistryRow(**row_dict)
        # Round-trip the canonical-string accessor — they validate the StrEnum.
        assert rr.primitive_value() == Primitive.LP.value
        assert rr.accounting_category_value() == AccountingCategory.LP.value
        # payload_json must be valid JSON (no Decimal / datetime / etc.).
        assert json.loads(rr.payload_json()) == row_dict["payload"]


def test_accounting_consumer_can_read_ledger_and_event() -> None:
    """D2.M2 — accounting consumers can read ledger + event with intent_type agreement."""
    for fix in _FIXTURE_NAMES:
        led = _read_fixture_json(fix, "expected_ledger_row.json")
        evt = _load_accounting_event(fix)
        # The consumer joins on intent_type / event_type — must agree.
        assert led["intent_type"] == evt["event_type"], (
            f"{fix}: ledger.intent_type={led['intent_type']!r} vs "
            f"accounting_event.event_type={evt['event_type']!r}"
        )
        # tx_hash agreement (same on-chain action, two views).
        assert led["tx_hash"] == evt["tx_hash"]


def test_readme_cites_real_tx_hashes_matching_receipts() -> None:
    """D2.M3 — README cites tx hashes that are actually present in receipts.

    The contract: a third party reading the README sees a tx hash, pastes it
    into arbiscan, and sees the same logs as the receipt fixture. README and
    receipt agreeing is the only thing that makes that a real audit trail.
    """
    readme_text = (_FIXTURE_ROOT / "README.md").read_text()
    cited_hashes = {
        m.group(0).lower() for m in re.finditer(r"0x[0-9a-fA-F]{64}", readme_text)
    }
    for fix in _FIXTURE_NAMES:
        receipt = _read_fixture_json(fix, "receipt.json")
        rec_hash = (receipt.get("transactionHash") or "").lower()
        assert rec_hash, f"{fix}/receipt.json missing transactionHash"
        assert rec_hash in cited_hashes, (
            f"{fix}/receipt.json transactionHash={rec_hash} is NOT cited "
            "in README. Add a citation row so the audit trail is reproducible."
        )


# =============================================================================
# D3 — Robustness (no silent failure)
# =============================================================================


def test_loader_rejects_unknown_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """D3.F1 — extra unknown column is rejected by the registry-row loader."""
    bogus = {
        "deployment_id": "X",
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "lp",
        "physical_identity_hash": "0xabc",
        "semantic_grouping_key": "X:Y",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload": {},
        "opened_at_block": 1,
        "opened_tx": "0x" + "1" * 64,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
        "bogus_column": "smuggled",  # extra
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_registry_row.json").write_text(json.dumps(bogus))
    # Re-point the loader's root at the temp dir.
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(KeyError, match="unknown columns"):
        _load_registry_row("lp_open")


def test_loader_rejects_missing_required_columns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3.F1 — missing required column is rejected."""
    bogus = {
        # missing 'physical_identity_hash'
        "deployment_id": "X",
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "lp",
        "semantic_grouping_key": "X:Y",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload": {},
        "opened_at_block": 1,
        "opened_tx": "0x" + "1" * 64,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_registry_row.json").write_text(json.dumps(bogus))
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(KeyError, match="missing required columns"):
        _load_registry_row("lp_open")


def test_loader_rejects_invalid_primitive_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3.F2 — invented primitive string is rejected, not silently coerced."""
    bogus = {
        "deployment_id": "X",
        "chain": "arbitrum",
        "primitive": "lpv3",  # invented
        "accounting_category": "lp",
        "physical_identity_hash": "0xabc",
        "semantic_grouping_key": "X:Y",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload": {},
        "opened_at_block": 1,
        "opened_tx": "0x" + "1" * 64,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_registry_row.json").write_text(json.dumps(bogus))
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(ValueError):
        _load_registry_row("lp_open")


def test_loader_rejects_invalid_accounting_category_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3.F2 — invented category string is rejected."""
    bogus = {
        "deployment_id": "X",
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "univ3lp",  # invented
        "physical_identity_hash": "0xabc",
        "semantic_grouping_key": "X:Y",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload": {},
        "opened_at_block": 1,
        "opened_tx": "0x" + "1" * 64,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_registry_row.json").write_text(json.dumps(bogus))
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(ValueError):
        _load_registry_row("lp_open")


@pytest.mark.parametrize("bad_pih", ["", "   ", "\t\n"])
def test_loader_rejects_empty_physical_identity_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_pih: str
) -> None:
    """D3.F3 — empty / whitespace-only physical_identity_hash is rejected."""
    bogus = {
        "deployment_id": "X",
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": "lp",
        "physical_identity_hash": bad_pih,
        "semantic_grouping_key": "X:Y",
        "grouping_policy_version": "univ3_lp@v1",
        "handle": None,
        "status": "open",
        "payload": {},
        "opened_at_block": 1,
        "opened_tx": "0x" + "1" * 64,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": None,
        "matching_policy_version": 3,
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_registry_row.json").write_text(json.dumps(bogus))
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(ValueError, match="non-empty"):
        _load_registry_row("lp_open")


def test_loader_rejects_invalid_position_reference_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3.F4 — position_reference.source outside {receipt, registry, legacy} rejected."""
    bogus = {
        "category": "lp",
        "primitive": "lp",
        "event_type": "LP_OPEN",
        "position_reference": {
            "source": "oracle",  # invalid
            "primitive": "lp",
            "accounting_category": "lp",
            "physical_identity_hash": "0xabc",
        },
    }
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    (fake_dir / "expected_accounting_event.json").write_text(json.dumps(bogus))
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )
    with pytest.raises(ValueError, match="source must be one of"):
        _load_accounting_event("lp_open")


def test_lp_open_close_anchors_are_null_not_zero() -> None:
    """D3.F5 — Empty ≠ zero. closed_* on LP_OPEN is JSON null, not 0/"".

    CLAUDE.md: ``Decimal("0")`` = measured zero. ``None`` = unmeasured. The
    LP_OPEN row has not been closed; ``closed_at_block`` and ``closed_tx``
    are unmeasured, NOT zero. Substituting 0 here would silently corrupt
    every "is this open?" check downstream.
    """
    open_row = _load_registry_row("lp_open")
    assert open_row["closed_at_block"] is None, (
        f"LP_OPEN closed_at_block must be JSON null (unmeasured), "
        f"got {open_row['closed_at_block']!r}"
    )
    assert open_row["closed_tx"] is None, (
        f"LP_OPEN closed_tx must be JSON null, got {open_row['closed_tx']!r}"
    )
    # last_reconciled_at_block is also null at open time.
    assert open_row["last_reconciled_at_block"] is None

    close_row = _load_registry_row("lp_close")
    assert isinstance(close_row["closed_at_block"], int)
    assert close_row["closed_at_block"] > 0
    assert isinstance(close_row["closed_tx"], str)
    assert close_row["closed_tx"].startswith("0x")
    # opened anchors are PRESERVED on close — open_row's opened_at_block ==
    # close_row's opened_at_block.
    assert close_row["opened_at_block"] == open_row["opened_at_block"]
    assert close_row["opened_tx"] == open_row["opened_tx"]


def test_loader_no_silent_failure_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """D3.F6 — every adversarial input raises; loader never returns partial data.

    Seven scenarios; each MUST raise (any of ValueError / TypeError / KeyError /
    JSONDecodeError / FileNotFoundError is acceptable). A None / partial-dict
    return is a hard fail.
    """
    fake_dir = tmp_path / "univ3-arbitrum" / "lp_open"
    fake_dir.mkdir(parents=True)
    target = fake_dir / "expected_registry_row.json"
    monkeypatch.setattr(
        "tests.unit.multi_position_tracking.test_l1_goldens_univ3._FIXTURE_ROOT",
        fake_dir.parent,
    )

    # Scenario 1: truncated JSON.
    target.write_text("{bogus")
    with pytest.raises(json.JSONDecodeError):
        _load_registry_row("lp_open")

    # Scenario 2: list at top level.
    target.write_text("[1, 2, 3]")
    with pytest.raises(TypeError, match="MUST be a dict"):
        _load_registry_row("lp_open")

    # Scenario 3: scalar at top level.
    target.write_text("42")
    with pytest.raises(TypeError):
        _load_registry_row("lp_open")

    # Scenario 4: empty file.
    target.write_text("")
    with pytest.raises(json.JSONDecodeError):
        _load_registry_row("lp_open")

    # Scenario 5: missing required column.
    target.write_text(json.dumps({"deployment_id": "X"}))
    with pytest.raises(KeyError):
        _load_registry_row("lp_open")

    # Scenario 6: unknown extra column.
    bad6 = {col: None for col in _REGISTRY_COLUMNS_16}
    bad6.update(
        {
            "primitive": "lp",
            "accounting_category": "lp",
            "physical_identity_hash": "0xabc",
            "status": "open",
            "payload": {},
            "matching_policy_version": 3,
            "smuggled": True,
        }
    )
    target.write_text(json.dumps(bad6))
    with pytest.raises(KeyError, match="unknown"):
        _load_registry_row("lp_open")

    # Scenario 7: bad status enum.
    bad7 = {col: None for col in _REGISTRY_COLUMNS_16}
    bad7.update(
        {
            "primitive": "lp",
            "accounting_category": "lp",
            "physical_identity_hash": "0xabc",
            "status": "OPEN",  # uppercase typo
            "payload": {},
            "matching_policy_version": 3,
        }
    )
    target.write_text(json.dumps(bad7))
    with pytest.raises(ValueError, match="status must be one of"):
        _load_registry_row("lp_open")


# =============================================================================
# D4 — Audit reproducibility
# =============================================================================


def test_readme_cites_arbitrum_plausible_block_numbers() -> None:
    """D4.A2 — README cites Arbitrum-plausible (post-Nitro) block numbers.

    Per UAT card §D4.A2 the floor is three citations (one per fixture set).
    Mainnet cross-check rows may add more. Regex tolerates the comma-separated
    inline form ``block 459393901`` AND the colon form ``block: 459393901`` so
    documentation tweaks don't accidentally drop matches.
    """
    readme = (_FIXTURE_ROOT / "README.md").read_text()
    blocks = [int(m.group(1)) for m in re.finditer(r"block[:\s]+(\d{6,})", readme, re.I)]
    assert len(blocks) >= 3, (
        f"README must cite at least three block numbers (one per fixture set), "
        f"got: {blocks}"
    )
    for b in blocks:
        # Arbitrum post-Nitro ranges are well above 22M.
        assert 22_000_000 < b < 1_000_000_000, (
            f"block {b} not Arbitrum-plausible (post-Nitro)"
        )


def test_handle_column_is_explicit_null() -> None:
    """D4 audit — handle is JSON null (explicit auto-mode), not absent.

    Per CLAUDE.md "Empty ≠ zero": handle=None means "auto-mode, no alias".
    Absence of the key would silently coerce to a different default in JSON
    deserializers. We pin the explicit null.
    """
    for fix in _LP_FIXTURE_NAMES:
        # _load_registry_row guarantees the key is present (16-column check),
        # so we just assert the value is None.
        row = _load_registry_row(fix)
        assert row["handle"] is None, (
            f"{fix}: handle must be JSON null in goldens (auto-mode); "
            f"got {row['handle']!r}"
        )


def test_grouping_policy_version_is_canonical_string() -> None:
    """D4 audit — grouping_policy_version stamps the PRD-canonical version."""
    for fix in _LP_FIXTURE_NAMES:
        row = _load_registry_row(fix)
        # PRD §Registry Data Shape line 126 example: 'univ3_lp@v1'.
        assert row["grouping_policy_version"] == "univ3_lp@v1"
