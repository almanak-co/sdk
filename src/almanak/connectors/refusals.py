"""Execution policy refusals that name the values an author can act on.

Balances and minima are policy values and chain observations, never secrets,
so these refusals reach the coordinator, its status and the author unchanged.
"""

from almanak.gateway.ethereum import DataError


def below_entry_minimum(balance, minimum):
    """New exposure refused because the gas payer holds less than the allocation's minimum."""
    held = "an unobserved balance" if balance is None else f"{balance.amount} {balance.asset}"
    return DataError(
        "gas_budget",
        f"gas payer is below the configured entry minimum: it holds {held}, "
        f"minimum_native_balance is {minimum} (base units)",
    )
