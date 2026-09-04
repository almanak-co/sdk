"""The lane-gap claim helper answers every non-core claim of an AMM cell as tracked debt."""

from __future__ import annotations

from almanak.connectors._amm_lifecycle_declaration import build_amm_lane_gap_claim_declarations
from almanak.connectors.uniswap_v4.connector import CONNECTOR
from almanak.core.capability_obligations import ExactTargetFeature, ObligationState, SupportClaim, Unsupported
from almanak.core.chains.robinhood import DESCRIPTOR as ROBINHOOD
from almanak.core.intent_types import IntentType
from almanak.framework.primitives.types import Primitive

_INTENTS = (IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES)
_TRACKING_REF = "ALM-9996"


def _declarations():
    return build_amm_lane_gap_claim_declarations(
        protocol="uniswap_v4",
        chain=ROBINHOOD,
        intents=_INTENTS,
        tracking_ref=_TRACKING_REF,
        lp_primitive=Primitive.LP_V4,
        quote_feature=True,
    )


def test_claim_cells_follow_intent_semantics() -> None:
    cells = {(leaf.intent, leaf.claim, leaf.exact_target_feature) for leaf in _declarations()}
    assert cells == {
        (IntentType.SWAP, SupportClaim.EXACT_TARGET_DATA, ExactTargetFeature.QUOTE),
        (IntentType.SWAP, SupportClaim.MANAGED_ANVIL_TESTABLE, None),
        (IntentType.LP_OPEN, SupportClaim.POSITION_OPEN, None),
        (IntentType.LP_OPEN, SupportClaim.FULL_LIFECYCLE_CERTIFICATION, None),
        (IntentType.LP_OPEN, SupportClaim.VALUATION_READY, None),
        (IntentType.LP_OPEN, SupportClaim.MANAGED_ANVIL_TESTABLE, None),
        (IntentType.LP_CLOSE, SupportClaim.POSITION_CLOSE, None),
        (IntentType.LP_CLOSE, SupportClaim.FULL_LIFECYCLE_CERTIFICATION, None),
        (IntentType.LP_CLOSE, SupportClaim.VALUATION_READY, None),
        (IntentType.LP_CLOSE, SupportClaim.MANAGED_ANVIL_TESTABLE, None),
        (IntentType.LP_COLLECT_FEES, SupportClaim.FULL_LIFECYCLE_CERTIFICATION, None),
        (IntentType.LP_COLLECT_FEES, SupportClaim.VALUATION_READY, None),
        (IntentType.LP_COLLECT_FEES, SupportClaim.MANAGED_ANVIL_TESTABLE, None),
    }
    assert all(leaf.protocol == "uniswap_v4" and leaf.chain == ROBINHOOD.name for leaf in _declarations())


def test_core_execution_is_left_to_the_audited_cells() -> None:
    assert not any(leaf.claim is SupportClaim.CORE_EXECUTION for leaf in _declarations())


def test_every_obligation_is_unsupported_under_the_tracking_ref() -> None:
    leaves = _declarations()
    assert leaves
    for leaf in leaves:
        disposition = leaf.declaration.disposition
        assert isinstance(disposition, Unsupported), (leaf.claim, leaf.declaration.obligation)
        assert disposition.state is ObligationState.UNSUPPORTED
        assert disposition.tracking_ref == _TRACKING_REF


def test_quote_feature_off_drops_the_swap_quote_claim() -> None:
    leaves = build_amm_lane_gap_claim_declarations(
        protocol="uniswap_v4",
        chain=ROBINHOOD,
        intents=(IntentType.SWAP,),
        tracking_ref=_TRACKING_REF,
        lp_primitive=Primitive.LP_V4,
        quote_feature=False,
    )
    assert {leaf.claim for leaf in leaves} == {SupportClaim.MANAGED_ANVIL_TESTABLE}


def test_connector_carries_the_robinhood_lane_gap_rows() -> None:
    robinhood = [
        leaf
        for leaf in CONNECTOR.lifecycle_declarations
        if leaf.chain == ROBINHOOD.name and leaf.claim is not SupportClaim.CORE_EXECUTION
    ]
    assert {(leaf.intent, leaf.claim) for leaf in robinhood} == {(leaf.intent, leaf.claim) for leaf in _declarations()}
    assert all(leaf.declaration.disposition.tracking_ref == _TRACKING_REF for leaf in robinhood)
