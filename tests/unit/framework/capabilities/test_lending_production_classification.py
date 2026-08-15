from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

from almanak.core.capability_obligations import ObligationId, Satisfied, Unsupported
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import ReportedObligationState
from scripts.ci.production_claim_universe import build_production_core_execution_matrix

_LENDING_PROTOCOLS = frozenset({"aave_v3", "compound_v3", "morpho_blue", "spark"})
_POLYGON_WIND_DOWN = "https://github.com/almanak-co/almanak-sdk-private/issues/3400"
_REPO_ROOT = Path(__file__).resolve().parents[4]


def _lending_cells():
    matrix = build_production_core_execution_matrix()
    return tuple(cell for cell in matrix.cells if cell.key.protocol in _LENDING_PROTOCOLS)


def test_production_lending_projection_is_exact_and_has_zero_undeclared() -> None:
    cells = _lending_cells()
    assert Counter(cell.key.protocol for cell in cells) == {
        "aave_v3": 28,
        "compound_v3": 20,
        "morpho_blue": 16,
        "spark": 4,
    }
    assert len(cells) == 68
    assert sum(len(cell.obligations) for cell in cells) == 476
    assert Counter(row.state for cell in cells for row in cell.obligations) == {
        ReportedObligationState.SATISFIED: 401,
        ReportedObligationState.UNSUPPORTED: 75,
    }
    assert not [row for cell in cells for row in cell.obligations if row.state is ReportedObligationState.UNDECLARED]
    assert sum(cell.claim_satisfied for cell in cells) == 10


def test_unsupported_lending_rows_are_only_the_reviewed_owned_gaps() -> None:
    cells = _lending_cells()
    unsupported = {
        (cell.key.protocol, cell.key.chain, cell.key.intent, row.audited.obligation): row.audited.disposition
        for cell in cells
        for row in cell.obligations
        if row.state is ReportedObligationState.UNSUPPORTED
    }
    expected_money_leg_gaps = {
        (cell.key.protocol, cell.key.chain, cell.key.intent, ObligationId.MONEY_LEGS)
        for cell in cells
        if cell.key.protocol in {"aave_v3", "compound_v3"}
    }
    expected_wind_down_gaps = {
        ("compound_v3", "polygon", intent, obligation)
        for intent in (IntentType.BORROW, IntentType.REPAY)
        for obligation in (
            ObligationId.ASSET_RESOLUTION,
            ObligationId.VENUE_RESOLUTION,
            ObligationId.AMOUNT_PROTECTION,
            ObligationId.COMPILER,
            ObligationId.RECEIPT_EVIDENCE,
            ObligationId.PERMISSION_PLAN,
        )
    }
    expected_aave_receipt_gaps = {
        ("aave_v3", "avalanche", IntentType.WITHDRAW, ObligationId.RECEIPT_EVIDENCE),
        ("aave_v3", "avalanche", IntentType.REPAY, ObligationId.RECEIPT_EVIDENCE),
        ("aave_v3", "polygon", IntentType.REPAY, ObligationId.RECEIPT_EVIDENCE),
    }
    expected_morpho_gaps = {
        ("morpho_blue", chain, intent, ObligationId.MONEY_LEGS)
        for chain in ("arbitrum", "base", "ethereum", "polygon")
        for intent in (IntentType.SUPPLY, IntentType.WITHDRAW)
    } | {
        ("morpho_blue", "ethereum", IntentType.SUPPLY, obligation)
        for obligation in (ObligationId.AMOUNT_PROTECTION, ObligationId.RECEIPT_EVIDENCE)
    }
    expected_spark_gaps = {
        ("spark", "ethereum", intent, ObligationId.MONEY_LEGS) for intent in (IntentType.WITHDRAW, IntentType.REPAY)
    }
    assert set(unsupported) == (
        expected_money_leg_gaps
        | expected_wind_down_gaps
        | expected_aave_receipt_gaps
        | expected_morpho_gaps
        | expected_spark_gaps
    )
    for identity, disposition in unsupported.items():
        assert type(disposition) is Unsupported
        assert disposition.review_by == date(2026, 10, 15)
        if identity[0] in {"aave_v3", "compound_v3"} and identity[-1] is ObligationId.MONEY_LEGS:
            assert disposition.tracking_ref == "VIB-6660"
            assert disposition.owner == "SDK Accounting"
        elif identity[:3] == ("compound_v3", "polygon", IntentType.BORROW):
            assert disposition.tracking_ref == _POLYGON_WIND_DOWN
            assert disposition.owner == "Connector - Compound V3"
        else:
            assert disposition.tracking_ref == "VIB-6661"
            assert disposition.owner == "SDK Capability Audit"


def test_every_satisfied_lending_evidence_ref_resolves_in_the_repository() -> None:
    for cell in _lending_cells():
        for row in cell.obligations:
            disposition = row.audited.disposition
            if type(disposition) is not Satisfied:
                continue
            for evidence in disposition.test_evidence:
                assert (_REPO_ROOT / evidence.ref).is_file(), (
                    f"missing evidence for {cell.key.sort_key()!r}/{row.audited.obligation.value}: {evidence.ref}"
                )


def test_morpho_gap_provenance_does_not_claim_unsupported_money_leg_evidence() -> None:
    from almanak.connectors.morpho_blue.connector import CONNECTOR

    unsupported_money_legs = tuple(
        item
        for item in CONNECTOR.lifecycle_declarations
        if item.declaration.obligation is ObligationId.MONEY_LEGS and type(item.declaration.disposition) is Unsupported
    )
    assert unsupported_money_legs
    assert all(
        item.source_detail
        == "Exact-chain real-fork execution; typed money-leg evidence is attached only where declared SATISFIED."
        for item in unsupported_money_legs
    )


def test_registered_nonproduction_lending_cells_do_not_inherit_production_declarations() -> None:
    from almanak.connectors.aave_v3.connector import CONNECTOR
    from almanak.framework.capabilities.effective_matrix import build_effective_capability_matrix

    matrix = build_effective_capability_matrix((CONNECTOR,))
    linea_supply = next(
        cell
        for cell in matrix.cells
        if cell.key.protocol == "aave_v3"
        and cell.key.chain == "linea"
        and cell.key.intent.value == "SUPPLY"
        and cell.key.claim.value == "core_execution"
    )
    assert linea_supply.obligations
    assert all(row.state is ReportedObligationState.UNDECLARED for row in linea_supply.obligations)
