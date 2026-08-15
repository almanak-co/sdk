from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

from almanak.core.capability_obligations import NotApplicable, Satisfied, Unsupported
from almanak.framework.capabilities.obligation_profiles import ReportedObligationState
from scripts.ci.production_claim_universe import (
    build_production_core_execution_matrix,
    load_production_claim_universe,
)

_REPO_ROOT = Path(__file__).resolve().parents[4]
_REVIEW_BY = date(2026, 10, 15)


def test_p1b_production_matrix_has_exact_zero_undeclared_exit_shape() -> None:
    universe = load_production_claim_universe()
    matrix = build_production_core_execution_matrix()

    assert len(universe.protocol_chain_pairs) == 46
    assert len({cell.key.protocol for cell in matrix.cells}) == 12
    assert len(matrix.cells) == 174
    assert sum(len(cell.obligations) for cell in matrix.cells) == 1_218
    assert Counter(row.state for cell in matrix.cells for row in cell.obligations) == {
        ReportedObligationState.SATISFIED: 838,
        ReportedObligationState.UNSUPPORTED: 380,
    }
    assert sum(cell.claim_satisfied for cell in matrix.cells) == 11
    assert matrix.to_json() == build_production_core_execution_matrix().to_json()


def test_p1b_production_matrix_has_only_reviewed_typed_dispositions() -> None:
    matrix = build_production_core_execution_matrix()
    tracking_refs: Counter[str] = Counter()
    identities: set[tuple[tuple[object, ...], str]] = set()

    for cell in matrix.cells:
        assert len(cell.obligations) == 7
        for row in cell.obligations:
            identity = (cell.key.sort_key(), row.audited.obligation.value)
            assert identity not in identities
            identities.add(identity)
            disposition = row.audited.disposition
            assert type(disposition) in {Satisfied, NotApplicable, Unsupported}
            if type(disposition) is Unsupported:
                assert disposition.owner
                assert disposition.review_by == _REVIEW_BY
                assert not disposition.is_review_due(as_of=date(2026, 8, 15))
                tracking_refs[disposition.tracking_ref] += 1

    assert tracking_refs == {
        "ALM-3041": 9,
        "VIB-5968": 112,
        "VIB-5974": 14,
        "VIB-6016": 1,
        "VIB-6152": 2,
        "VIB-6220": 17,
        "VIB-6223": 4,
        "VIB-6226": 2,
        "VIB-6235": 1,
        "VIB-6660": 48,
        "VIB-6661": 21,
        "VIB-6662": 73,
        "VIB-6663": 6,
        "VIB-6664": 6,
        "VIB-6666": 7,
        "VIB-6667": 42,
        "VIB-6668": 7,
        "VIB-6669": 2,
        "https://github.com/almanak-co/almanak-sdk-private/issues/3400": 6,
    }


def test_p1b_claim_satisfaction_is_exactly_the_reviewed_eleven_cells() -> None:
    satisfied = [cell.key.sort_key() for cell in build_production_core_execution_matrix().cells if cell.claim_satisfied]
    assert satisfied == [
        ("curve", "ethereum", "LP_OPEN", "core_execution", ""),
        ("morpho_blue", "arbitrum", "BORROW", "core_execution", ""),
        ("morpho_blue", "arbitrum", "REPAY", "core_execution", ""),
        ("morpho_blue", "base", "BORROW", "core_execution", ""),
        ("morpho_blue", "base", "REPAY", "core_execution", ""),
        ("morpho_blue", "ethereum", "BORROW", "core_execution", ""),
        ("morpho_blue", "ethereum", "REPAY", "core_execution", ""),
        ("morpho_blue", "polygon", "BORROW", "core_execution", ""),
        ("morpho_blue", "polygon", "REPAY", "core_execution", ""),
        ("spark", "ethereum", "BORROW", "core_execution", ""),
        ("spark", "ethereum", "SUPPLY", "core_execution", ""),
    ]


def test_p1b_satisfied_evidence_refs_resolve_in_the_repository() -> None:
    for cell in build_production_core_execution_matrix().cells:
        for row in cell.obligations:
            disposition = row.audited.disposition
            if type(disposition) is not Satisfied:
                continue
            for evidence in disposition.test_evidence:
                assert (_REPO_ROOT / evidence.ref).is_file(), (
                    f"missing evidence for {cell.key.sort_key()!r}/{row.audited.obligation.value}: {evidence.ref}"
                )
