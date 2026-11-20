"""Aave V3 direct operator actions: supply to one reserve and withdraw it in full."""

from functools import partial

from almanak.connectors import pool_lending_direct
from almanak.connectors.direct import DirectActions

DIRECT = DirectActions(
    actions=("supply", "withdraw"),
    positions=partial(pool_lending_direct.positions, "aave_v3"),
    intent=pool_lending_direct.intent,
    position=pool_lending_direct.position,
)
