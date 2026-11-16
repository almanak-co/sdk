"""Liquidity bounds use the retained pool revision's native transition semantics."""

from almanak.connectors.curve import stable_math, tricrypto_transitions, twocrypto_transitions
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.twocrypto_liquidity import proportional


def minted(pool, amounts):
    if isinstance(pool, TriCryptoObservation):
        transition = tricrypto_transitions.add_liquidity(
            pool, amounts, timestamp=pool.block.timestamp
        )
        return transition.state.total_supply - pool.total_supply - transition.admin_shares
    if isinstance(pool, CryptoObservation):
        transition = twocrypto_transitions.add_liquidity(
            pool, amounts, timestamp=pool.block.timestamp
        )
        return transition.state.total_supply - pool.total_supply
    return stable_math.addition(pool.math_state(), amounts).minted


def removed(pool, shares, index=None):
    if isinstance(pool, TriCryptoObservation):
        if index is None:
            return tricrypto_transitions.withdraw_proportional(
                pool, shares, timestamp=pool.block.timestamp
            ).amounts
        return tricrypto_transitions.withdraw_one(
            pool, index, shares, timestamp=pool.block.timestamp
        ).amounts
    if isinstance(pool, CryptoObservation):
        if index is None:
            return proportional(pool.math_state(), shares)
        return twocrypto_transitions.withdraw_one(
            pool, index, shares, timestamp=pool.block.timestamp
        ).amounts
    if index is None:
        return stable_math.removal_proportional(pool.math_state(), shares)
    output = stable_math.removal_one(pool.math_state(), index, shares).output
    return tuple(output if coin == index else 0 for coin in range(len(pool.source.coins)))
