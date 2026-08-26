"""Permanent ownership-boundary counterexamples for ALM-3251."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents import LPOpenIntent

CURVE_POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"


@pytest.mark.parametrize(
    "invented_field",
    ["curve_pool_binding", "token_fingerprint", "pool_fingerprint"],
)
def test_alm_3251_edge_admission_fingerprints_are_not_sdk_runtime_intent_facts(invented_field: str) -> None:
    """Generated Edge attestations cannot masquerade as chain-observable facts."""
    kwargs = {
        "pool": CURVE_POOL,
        "amount0": Decimal("1"),
        "amount1": Decimal("1"),
        "range_lower": Decimal("0.9"),
        "range_upper": Decimal("1.1"),
        "protocol": "curve",
        invented_field: "producer-declared-value",
    }

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        LPOpenIntent(**kwargs)

