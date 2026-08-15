from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

import pytest

from almanak.connectors._amm_lifecycle_declaration import AmmCoreExecutionCell
from almanak.connectors.uniswap_v4.receipt_parser import (
    TRANSFER_EVENT_TOPIC,
    ParseResult,
    TransferEventData,
)
from almanak.core.capability_obligations import ObligationId, Satisfied, Unsupported
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import ReportedObligationState
from scripts.ci.production_claim_universe import build_production_core_execution_matrix
from tests.intents.arbitrum.test_uniswap_v4_lp_open import _assert_parsed_position_manager_mint
from tests.intents.arbitrum.test_uniswap_v4_lp_open import (
    _assert_v4_open_position_hash as _assert_arbitrum_v4_open_position_hash,
)
from tests.intents.base.test_uniswap_v4_lp_close import (
    _assert_v4_open_position_hash as _assert_base_v4_close_basis_position_hash,
)
from tests.intents.base.test_uniswap_v4_lp_open import (
    _assert_v4_open_position_hash as _assert_base_v4_open_position_hash,
)

_REPO_ROOT = Path(__file__).resolve().parents[4]
_AMM_PROTOCOLS = {
    "aerodrome",
    "aerodrome_slipstream",
    "curve",
    "pancakeswap_v3",
    "sushiswap_v3",
    "uniswap_v3",
    "uniswap_v4",
}


@pytest.mark.parametrize(
    "assert_position_hash",
    (
        _assert_arbitrum_v4_open_position_hash,
        _assert_base_v4_open_position_hash,
        _assert_base_v4_close_basis_position_hash,
    ),
)
def test_revalidated_v4_position_hash_rejects_non_hex_bytes(assert_position_hash) -> None:
    with pytest.raises(AssertionError, match="32 hex bytes"):
        assert_position_hash({"position_hash": "0x" + "z" * 64})


def test_v4_parser_mint_filter_rejects_unrelated_transfer_events() -> None:
    zero = "0x0000000000000000000000000000000000000000"
    position_manager = "0x1111111111111111111111111111111111111111"
    parsed = ParseResult(
        transfer_events=[
            TransferEventData(
                token="0x2222222222222222222222222222222222222222",
                from_address=zero,
                to_address="0x3333333333333333333333333333333333333333",
                amount=7,
            ),
            TransferEventData(
                token=position_manager,
                from_address="0x4444444444444444444444444444444444444444",
                to_address="0x3333333333333333333333333333333333333333",
                amount=8,
            ),
        ]
    )
    receipt = {
        "logs": [
            {
                "address": position_manager,
                "topics": [
                    TRANSFER_EVENT_TOPIC,
                    "0x" + "0" * 64,
                    "0x" + "0" * 24 + "3" * 40,
                    "0x" + "0" * 63 + "9",
                ],
                "data": "0x",
            }
        ]
    }
    with pytest.raises(AssertionError, match="surface exactly the PositionManager"):
        _assert_parsed_position_manager_mint(parsed, receipt, position_manager, 9)

    parsed.transfer_events.append(
        TransferEventData(
            token=position_manager,
            from_address=zero,
            to_address="0x3333333333333333333333333333333333333333",
            amount=0,
        )
    )
    _assert_parsed_position_manager_mint(parsed, receipt, position_manager, 9)

    with pytest.raises(AssertionError, match="matching the extracted position_id"):
        _assert_parsed_position_manager_mint(parsed, receipt, position_manager, 10)

    receipt["logs"].append(
        {
            "address": position_manager,
            "topics": [
                TRANSFER_EVENT_TOPIC,
                "0x" + "0" * 64,
                "0x" + "0" * 24 + "3" * 40,
                "0x" + "0" * 63 + "8",
            ],
            "data": "0x",
        }
    )
    with pytest.raises(AssertionError, match="exactly one PositionManager"):
        _assert_parsed_position_manager_mint(parsed, receipt, position_manager, 9)


def _amm_cells():
    matrix = build_production_core_execution_matrix()
    return tuple(cell for cell in matrix.cells if cell.key.protocol in _AMM_PROTOCOLS)


def test_production_amm_projection_is_exact_and_has_zero_undeclared() -> None:
    cells = _amm_cells()
    assert Counter(cell.key.protocol for cell in cells) == {
        "aerodrome": 6,
        "aerodrome_slipstream": 3,
        "curve": 15,
        "pancakeswap_v3": 16,
        "sushiswap_v3": 24,
        "uniswap_v3": 28,
        "uniswap_v4": 8,
    }
    assert len(cells) == 100
    assert sum(len(cell.obligations) for cell in cells) == 700
    assert Counter(row.state for cell in cells for row in cell.obligations) == {
        ReportedObligationState.SATISFIED: 407,
        ReportedObligationState.UNSUPPORTED: 293,
    }
    assert not [row for cell in cells for row in cell.obligations if row.state is ReportedObligationState.UNDECLARED]
    assert [cell.key.sort_key() for cell in cells if cell.claim_satisfied] == [
        ("curve", "ethereum", "LP_OPEN", "core_execution", "")
    ]


def test_amm_unsupported_rows_preserve_exact_owned_gap_taxonomy() -> None:
    unsupported = [
        row.audited.disposition
        for cell in _amm_cells()
        for row in cell.obligations
        if row.state is ReportedObligationState.UNSUPPORTED
    ]
    assert Counter(disposition.tracking_ref for disposition in unsupported if type(disposition) is Unsupported) == {
        "ALM-3041": 9,
        "VIB-5968": 112,
        "VIB-5974": 14,
        "VIB-6016": 1,
        "VIB-6220": 17,
        "VIB-6223": 4,
        "VIB-6226": 2,
        "VIB-6235": 1,
        "VIB-6662": 73,
        "VIB-6663": 2,
        "VIB-6666": 7,
        "VIB-6667": 42,
        "VIB-6668": 7,
        "VIB-6669": 2,
    }
    for disposition in unsupported:
        assert type(disposition) is Unsupported
        assert disposition.owner == "SDK Capability Audit"
        assert disposition.review_by == date(2026, 10, 15)


def test_all_missing_positive_lanes_fail_the_entire_core_claim_closed() -> None:
    lane_gaps = [
        cell
        for cell in _amm_cells()
        if all(row.state is ReportedObligationState.UNSUPPORTED for row in cell.obligations)
    ]
    assert Counter(cell.key.protocol for cell in lane_gaps) == {
        "curve": 1,
        "pancakeswap_v3": 6,
        "sushiswap_v3": 6,
        "uniswap_v3": 12,
        "uniswap_v4": 1,
    }


def test_revalidated_uniswap_v4_lp_lanes_keep_only_exact_obligation_gaps() -> None:
    expected_gaps = {
        ("arbitrum", "LP_OPEN"): {
            ObligationId.AMOUNT_PROTECTION: "VIB-6669",
            ObligationId.MONEY_LEGS: "VIB-6662",
        },
        ("base", "LP_OPEN"): {
            ObligationId.AMOUNT_PROTECTION: "VIB-6669",
            ObligationId.MONEY_LEGS: "VIB-6662",
        },
        ("base", "LP_CLOSE"): {
            ObligationId.AMOUNT_PROTECTION: "VIB-6226",
            ObligationId.MONEY_LEGS: "VIB-6662",
        },
    }
    cells = {
        (cell.key.chain, cell.key.intent.value): cell
        for cell in _amm_cells()
        if cell.key.protocol == "uniswap_v4" and (cell.key.chain, cell.key.intent.value) in expected_gaps
    }
    assert set(cells) == set(expected_gaps)
    for key, cell in cells.items():
        unsupported = {
            row.audited.obligation: row.audited.disposition.tracking_ref
            for row in cell.obligations
            if type(row.audited.disposition) is Unsupported
        }
        assert unsupported == expected_gaps[key]
        assert Counter(row.state for row in cell.obligations) == {
            ReportedObligationState.SATISFIED: 5,
            ReportedObligationState.UNSUPPORTED: 2,
        }


def test_alm_3041_owns_only_the_nine_exact_v3_swap_amount_rows() -> None:
    rows = [
        (cell.key.protocol, cell.key.chain, cell.key.intent.value, row.audited.obligation.value)
        for cell in _amm_cells()
        for row in cell.obligations
        if type(row.audited.disposition) is Unsupported and row.audited.disposition.tracking_ref == "ALM-3041"
    ]
    assert rows == [
        ("pancakeswap_v3", "base", "SWAP", "amount_protection"),
        ("pancakeswap_v3", "bsc", "SWAP", "amount_protection"),
        ("sushiswap_v3", "arbitrum", "SWAP", "amount_protection"),
        ("sushiswap_v3", "base", "SWAP", "amount_protection"),
        ("sushiswap_v3", "bsc", "SWAP", "amount_protection"),
        ("sushiswap_v3", "ethereum", "SWAP", "amount_protection"),
        ("sushiswap_v3", "optimism", "SWAP", "amount_protection"),
        ("sushiswap_v3", "polygon", "SWAP", "amount_protection"),
        ("uniswap_v3", "base", "SWAP", "amount_protection"),
    ]


def test_slipstream_declarations_name_its_exact_parser_and_permission_providers() -> None:
    expected = {
        ObligationId.RECEIPT_EVIDENCE: ("almanak.connectors.aerodrome.receipt_parser:AerodromeSlipstreamReceiptParser"),
        ObligationId.PERMISSION_PLAN: ("almanak.connectors.aerodrome.permission_hints:PERMISSION_HINTS_SLIPSTREAM"),
    }
    for cell in _amm_cells():
        if cell.key.protocol != "aerodrome_slipstream":
            continue
        for row in cell.obligations:
            if row.audited.obligation not in expected:
                continue
            disposition = row.audited.disposition
            assert type(disposition) is Satisfied
            assert disposition.provider_ref == expected[row.audited.obligation]


def test_every_satisfied_amm_evidence_ref_resolves_in_the_repository() -> None:
    for cell in _amm_cells():
        for row in cell.obligations:
            disposition = row.audited.disposition
            if type(disposition) is not Satisfied:
                continue
            for evidence in disposition.test_evidence:
                assert (_REPO_ROOT / evidence.ref).is_file(), (
                    f"missing evidence for {cell.key.sort_key()!r}/{row.audited.obligation.value}: {evidence.ref}"
                )


@pytest.mark.parametrize(
    ("real_fork_ref", "lane_gap_ref"),
    ((None, None), ("tests/intents/base/test_aerodrome_swap.py", "VIB-1")),
)
def test_amm_evidence_cell_requires_exactly_one_lane_disposition(
    real_fork_ref: str | None,
    lane_gap_ref: str | None,
) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=IntentType.SWAP,
            real_fork_ref=real_fork_ref,
            lane_gap_ref=lane_gap_ref,
        )


def test_amm_lane_gap_rejects_partial_positive_evidence() -> None:
    with pytest.raises(ValueError, match="lane gaps cannot carry"):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=IntentType.LP_CLOSE,
            lane_gap_ref="VIB-1",
            obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "VIB-2"),),
        )


@pytest.mark.parametrize("intent", ("SWAP", object()))
def test_amm_evidence_cell_rejects_untyped_intent(intent: object) -> None:
    with pytest.raises(TypeError, match="intent must be an IntentType"):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=intent,  # type: ignore[arg-type]
            real_fork_ref="tests/intents/base/test_aerodrome_swap.py",
        )


def test_amm_evidence_cell_rejects_untyped_chain() -> None:
    with pytest.raises(TypeError, match="chain must be a ChainDescriptor"):
        AmmCoreExecutionCell(
            chain="base",  # type: ignore[arg-type]
            intent=IntentType.SWAP,
            real_fork_ref="tests/intents/base/test_aerodrome_swap.py",
        )


def test_amm_evidence_cell_rejects_non_amm_intent() -> None:
    with pytest.raises(ValueError, match="intent must be an AMM execution intent"):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=IntentType.HOLD,
            real_fork_ref="tests/intents/base/test_aerodrome_swap.py",
        )


@pytest.mark.parametrize(
    ("gap_refs", "error_type", "message"),
    (
        ([], TypeError, "obligation_gap_refs must be a tuple"),
        ((ObligationId.MONEY_LEGS,), TypeError, "entries must be"),
        ((("money_legs", "VIB-1"),), TypeError, "must use CORE_EXECUTION"),
        (((ObligationId.MONEY_LEGS, ""),), ValueError, "must be non-empty"),
        (
            ((ObligationId.MONEY_LEGS, "VIB-1"), (ObligationId.MONEY_LEGS, "VIB-2")),
            ValueError,
            "duplicate obligations",
        ),
    ),
)
def test_amm_evidence_cell_rejects_malformed_obligation_gaps(
    gap_refs: object,
    error_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error_type, match=message):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=IntentType.LP_OPEN,
            real_fork_ref="tests/intents/base/test_curve_lp_base.py",
            obligation_gap_refs=gap_refs,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (("real_fork_ref", ""), ("lane_gap_ref", " "), ("money_leg_evidence_ref", 1)),
)
def test_amm_evidence_cell_rejects_invalid_optional_refs(field_name: str, value: object) -> None:
    kwargs: dict[str, object] = {
        "chain": BASE,
        "intent": IntentType.SWAP,
        "real_fork_ref": "tests/intents/base/test_aerodrome_swap.py",
    }
    if field_name == "lane_gap_ref":
        kwargs["real_fork_ref"] = None
    kwargs[field_name] = value
    with pytest.raises(ValueError, match=field_name):
        AmmCoreExecutionCell(**kwargs)  # type: ignore[arg-type]


def test_amm_evidence_cell_rejects_money_leg_proof_and_gap_together() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        AmmCoreExecutionCell(
            chain=BASE,
            intent=IntentType.LP_OPEN,
            real_fork_ref="tests/intents/base/test_curve_lp_base.py",
            obligation_gap_refs=((ObligationId.MONEY_LEGS, "VIB-1"),),
            money_leg_evidence_ref="tests/unit/execution/test_result_enricher_curve_lp_open_legs.py",
        )
