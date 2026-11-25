"""SparkLend direct operator actions on one reserve of the Pool."""

from functools import partial

from almanak.connectors import pool_lending_direct
from almanak.connectors.direct import DirectActions

DIRECT = DirectActions(
    actions=("supply", "withdraw", "borrow", "repay", "collateral"),
    positions=partial(pool_lending_direct.positions, "sparklend"),
    intent=pool_lending_direct.intent,
    position=pool_lending_direct.position,
    steps=pool_lending_direct.steps,
)
