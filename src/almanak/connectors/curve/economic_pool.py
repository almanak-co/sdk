"""Family math for hypothetical shares layered on fixed external pool history."""

from dataclasses import dataclass, replace

from almanak.connectors.curve import stable_math as stable
from almanak.connectors.curve import tricrypto_transitions as tri
from almanak.connectors.curve import twocrypto_transitions as crypto
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.twocrypto_liquidity import proportional


@dataclass(frozen=True)
class CryptoState:
    pool: CryptoObservation | TriCryptoObservation
    timestamp: int


def virtual_pool(pool, owned, time):
    if time < pool.block.timestamp:
        raise ValueError("Curve hypothetical pool cannot consume a future observation")
    shares = owned.position.shares.amount if owned.position else 0
    if isinstance(pool, (CryptoObservation, TriCryptoObservation)):
        return CryptoState(crypto_overlay(pool, shares), int(time))
    state = pool.math_state(timestamp=time)
    if not shares:
        return state
    return replace(
        state,
        balances=expanded_balances(state.balances, shares, state.total_supply),
        total_supply=stable.uint(state.total_supply + shares),
    )


def expanded_balances(balances, shares, supply):
    return tuple(
        stable.uint(value + stable.divide(stable.product(value, shares), supply))
        for value in balances
    )


def crypto_overlay(pool, shares):
    if not shares:
        return pool

    # Scale extensive pool quantities together so historical per-share claims
    # and admin entitlements survive the virtual LP overlay. Own transaction
    # impact and fees never feed subsequent external samples.
    def expanded(value):
        return stable.uint(value + stable.divide(stable.product(value, shares), pool.total_supply))

    updates = {
        "balances": expanded_balances(pool.balances, shares, pool.total_supply),
        "total_supply": stable.uint(pool.total_supply + shares),
        "invariant": expanded(pool.invariant),
    }
    if isinstance(pool, TriCryptoObservation):
        updates["actual_balances"] = expanded_balances(
            pool.actual_balances, shares, pool.total_supply
        )
        if pool.token_accounting is not None:
            updates["token_accounting"] = pool.token_accounting.model_copy(
                update={
                    name: expanded(getattr(pool.token_accounting, name))
                    for name in ("virtual_admin_shares", "cached_xcp_oracle", "last_xcp")
                }
            )
    else:
        updates["accounting"] = pool.accounting.model_copy(
            update={"virtual_shares": expanded(pool.accounting.virtual_shares)}
        )
    return pool.model_copy(update=updates)


def crypto_module(pool):
    return tri if isinstance(pool, TriCryptoObservation) else crypto


def swap(state, first, second, amount):
    if isinstance(state, CryptoState):
        return (
            crypto_module(state.pool)
            .exchange(state.pool, first, second, amount, timestamp=state.timestamp)
            .amounts[second]
        )
    return stable.swap(state, first, second, amount).output


def minted(state, amounts):
    if isinstance(state, CryptoState):
        result = crypto_module(state.pool).add_liquidity(
            state.pool, amounts, timestamp=state.timestamp
        )
        return (
            result.state.total_supply
            - state.pool.total_supply
            - (result.admin_shares if isinstance(state.pool, TriCryptoObservation) else 0)
        )
    return stable.addition(state, amounts).minted


def removed(state, shares, index=None):
    if isinstance(state, CryptoState):
        if index is None and isinstance(state.pool, TriCryptoObservation):
            return tri.withdraw_proportional(state.pool, shares, timestamp=state.timestamp).amounts
        if index is None:
            return proportional(state.pool.math_state(timestamp=state.timestamp), shares)
        return (
            crypto_module(state.pool)
            .withdraw_one(state.pool, index, shares, timestamp=state.timestamp)
            .amounts
        )
    if index is None:
        return stable.removal_proportional(state, shares)
    output = stable.removal_one(state, index, shares).output
    return tuple(output if coin == index else 0 for coin in range(len(state.balances)))
