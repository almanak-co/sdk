from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

from almanak.core.capability_obligations import ObligationId, Satisfied, Unsupported
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import ReportedObligationState
from scripts.ci.production_claim_universe import build_production_core_execution_matrix

_REPO_ROOT = Path(__file__).resolve().parents[4]


def _gmx_cells():
    matrix = build_production_core_execution_matrix()
    return tuple(cell for cell in matrix.cells if cell.key.protocol == "gmx_v2")


def test_production_gmx_projection_is_exact_and_has_zero_undeclared() -> None:
    cells = _gmx_cells()
    assert Counter((cell.key.chain, cell.key.intent) for cell in cells) == {
        (chain, intent): 1
        for chain in ("arbitrum", "avalanche")
        for intent in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER)
    }
    assert len(cells) == 6
    assert sum(len(cell.obligations) for cell in cells) == 42
    assert Counter(row.state for cell in cells for row in cell.obligations) == {
        ReportedObligationState.SATISFIED: 30,
        ReportedObligationState.UNSUPPORTED: 12,
    }
    assert not [row for cell in cells for row in cell.obligations if row.state is ReportedObligationState.UNDECLARED]
    assert not any(cell.claim_satisfied for cell in cells)


def test_unsupported_gmx_rows_are_only_the_exact_reviewed_gaps() -> None:
    unsupported = {
        (cell.key.chain, cell.key.intent, row.audited.obligation): row.audited.disposition
        for cell in _gmx_cells()
        for row in cell.obligations
        if row.state is ReportedObligationState.UNSUPPORTED
    }
    expected = (
        {
            (chain, intent, ObligationId.MONEY_LEGS)
            for chain in ("arbitrum", "avalanche")
            for intent in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER)
        }
        | {
            (chain, IntentType.PERP_CANCEL_ORDER, obligation)
            for chain in ("arbitrum", "avalanche")
            for obligation in (ObligationId.ASSET_RESOLUTION, ObligationId.AMOUNT_PROTECTION)
        }
        | {(chain, IntentType.PERP_OPEN, ObligationId.RECEIPT_EVIDENCE) for chain in ("arbitrum", "avalanche")}
    )
    assert set(unsupported) == expected
    for identity, disposition in unsupported.items():
        assert type(disposition) is Unsupported
        assert disposition.owner == "SDK Capability Audit"
        assert disposition.review_by == date(2026, 10, 15)
        expected_ref = {
            ObligationId.MONEY_LEGS: "VIB-6664",
            ObligationId.ASSET_RESOLUTION: "VIB-6663",
            ObligationId.AMOUNT_PROTECTION: "VIB-6663",
            ObligationId.RECEIPT_EVIDENCE: "VIB-6152",
        }[identity[-1]]
        assert disposition.tracking_ref == expected_ref


def test_gmx_open_receipt_rows_do_not_promote_submission_to_terminal_proof() -> None:
    for cell in _gmx_cells():
        if cell.key.intent is not IntentType.PERP_OPEN:
            continue
        receipt = next(row for row in cell.obligations if row.audited.obligation is ObligationId.RECEIPT_EVIDENCE)
        disposition = receipt.audited.disposition
        assert type(disposition) is Unsupported
        assert disposition.tracking_ref == "VIB-6152"


def test_every_satisfied_gmx_evidence_ref_resolves_in_the_repository() -> None:
    for cell in _gmx_cells():
        for row in cell.obligations:
            disposition = row.audited.disposition
            if type(disposition) is not Satisfied:
                continue
            for evidence in disposition.test_evidence:
                assert (_REPO_ROOT / evidence.ref).is_file(), (
                    f"missing evidence for {cell.key.sort_key()!r}/{row.audited.obligation.value}: {evidence.ref}"
                )
