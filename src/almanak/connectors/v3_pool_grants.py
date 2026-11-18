"""Tick conditions for V3-family mint grants: the protocol's whole tick domain."""

from almanak.connectors.v3_pool_domain import MAX_TICK, MIN_TICK
from almanak.gateway.roles_conditions import signed_at_least, signed_at_most


def tick_conditions():
    """The two `int24` tick conditions of a mint grant, in tickLower, tickUpper order.

    Ticks are not scope (blueprint 27 section 4.3): a fixed range or a band is an
    economic choice the gateway enforces before submission, so a changed band needs no
    new permissions. The grant admits every tick the pool itself accepts; the pool
    reverts outside [MIN_TICK, MAX_TICK], so these bounds refuse nothing it would admit.
    """
    return signed_at_least(MIN_TICK), signed_at_most(MAX_TICK)
