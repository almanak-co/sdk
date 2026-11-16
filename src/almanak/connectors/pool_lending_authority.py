"""Scoped economic admission for concrete Pool capability types.

A capability names what an allocation may do with its reserve, never how much:
amounts are the strategy's decision, bounded by inventory, the protocol and the
gateway's risk checks.
"""

from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)


def admit(capabilities, intent) -> None:
    matches = [c for c in capabilities.requirements if c.target == intent.target]
    if len(matches) != 1:
        raise ValueError("intent requires exactly one retained reserve capability")
    capability = matches[0]
    if isinstance(intent, CollateralIntent):
        if intent.enabled and not capability.collateral:
            raise ValueError("collateral action is outside its retained capability")
        return
    if isinstance(intent, (BorrowIntent, RepayIntent, RepayAmountIntent)):
        # Repayment is scoped to the fund's own debt, not to an amount. Planning
        # refuses a partial repayment above the observed debt.
        if not capability.borrow:
            raise ValueError("borrowing and repayment require a borrowing allocation")
        return
    # Supply and withdrawal are always within an allocation's own reserve. The gateway
    # separately verifies a withdrawal against the fund's owned shares.
    if not isinstance(intent, (SupplyIntent, WithdrawIntent, WithdrawAmountIntent)):
        raise ValueError("action is outside the Pool lending economic capability")
