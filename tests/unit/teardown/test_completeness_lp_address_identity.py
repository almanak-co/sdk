"""Completeness: an address-bound LP_CLOSE covers a position labelled symbolically.

A strategy that names its teardown close by the exact pool address (the
exact-venue lanes) while its position summary still labels the pool
"WETH/USDC/15" used to fail the completeness gate — the matcher compared the
two strings literally and reported the position uncovered even though the
same pair was closed on-chain. The summary records the pool contract under an
address-shaped detail key; that is the identity the matcher now honours.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.models import PositionInfo, PositionType, TeardownPositionSummary

PAIR = "0x69f1216cB2905bf0852f74624D5Fa7b5FC4dA710"
OTHER_PAIR = "0x" + "ab" * 20


def _summary(positions: list[PositionInfo]) -> TeardownPositionSummary:
    return TeardownPositionSummary(deployment_id="dep", timestamp=datetime.now(UTC), positions=positions)


def _lp(details: dict, *, position_id: str = "traderjoe-lp-WETH/USDC/15-arbitrum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain="arbitrum",
        protocol="traderjoe_v2",
        value_usd=Decimal("100"),
        details=details,
    )


def _close(pool: str) -> dict:
    return {"intent_type": "LP_CLOSE", "pool": pool, "position_id": "WETH/USDC/15", "chain": "arbitrum"}


def test_address_close_covers_symbolic_position_that_records_the_pool_address() -> None:
    position = _lp({"pool": "WETH/USDC/15", "pool_address": PAIR})
    assert check_intent_coverage(_summary([position]), [_close(PAIR)]).complete


def test_match_is_case_insensitive() -> None:
    position = _lp({"pool": "WETH/USDC/15", "pool_address": PAIR.lower()})
    assert check_intent_coverage(_summary([position]), [_close(PAIR.upper().replace("0X", "0x"))]).complete


def test_address_close_for_a_different_pair_does_not_cover() -> None:
    position = _lp({"pool": "WETH/USDC/15", "pool_address": PAIR})
    report = check_intent_coverage(_summary([position]), [_close(OTHER_PAIR)])
    assert not report.complete
    assert [p.position_id for p in report.uncovered] == [position.position_id]


def test_symbolic_only_summary_is_still_uncovered_by_an_address_close() -> None:
    """No address on the position side means no resolution is attempted — never a guess."""
    position = _lp({"pool": "WETH/USDC/15"})
    assert not check_intent_coverage(_summary([position]), [_close(PAIR)]).complete


def test_symbolic_close_still_matches_symbolic_label() -> None:
    position = _lp({"pool": "WETH/USDC/15", "pool_address": PAIR})
    assert check_intent_coverage(_summary([position]), [_close("WETH/USDC/15")]).complete
