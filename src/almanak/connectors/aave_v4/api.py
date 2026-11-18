"""Public lending authoring interface."""

from almanak.connectors.aave_v4.authority import borrow_capability, supply_capability
from almanak.connectors.aave_v4.models import (
    AaveCapabilities,
    Decision,
    DecisionInput,
    LendingParameters,
    Memory,
    OutcomeInput,
)
from almanak.lending_intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent

__all__ = [
    "AaveCapabilities",
    "BorrowIntent",
    "Decision",
    "DecisionInput",
    "LendingParameters",
    "Memory",
    "OutcomeInput",
    "RepayIntent",
    "SupplyIntent",
    "WithdrawIntent",
    "borrow_capability",
    "supply_capability",
    "obligations",
]


def obligations(observed):
    if observed is None:
        return "unknown"
    present = (
        observed.position.total_debt,
        observed.drawn_shares,
        observed.premium_shares,
        observed.premium_offset_ray,
        observed.allowance,
        any(c.enabled for c in observed.collateral),
    )
    return "remaining" if any(present) else "clear"
