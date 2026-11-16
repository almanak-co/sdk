"""Curve pool funding and the state `almanak ax pool` reports beside its prices.

A pool is funded when every coin balance and the LP supply are positive. Prepared
backtest samples, `ax pool`, `ax dex-pools` and `almanak check --online` share this
one rule: an unfunded pool has no price, a swap into it cannot fill and a first
deposit would set its price, so Curve execution requires an existing funded pool.

Reported amounts are raw integer strings beside their asset and decimals. Fees are
Curve's 1e10-scaled integers with their basis-point value; amplification is the
pool's `A` at the pinned block (StableSwap `A_precise / 100`, crypto `A / 10_000`).
"""

from decimal import Decimal

from almanak.connectors.curve import stable_math
from almanak.connectors.curve.deployment import scalar
from almanak.connectors.curve.reader import quantities
from almanak.gateway.ethereum import DataError

FUNDED_POOL = "Curve execution requires an existing funded pool"
UNFUNDED_POOL = "unfunded_pool"
FEE_DENOMINATOR = 10**10
# A crypto pool's stored `A` carries this multiplier over the published A.
CRYPTO_A_MULTIPLIER = 10_000


def funded(balances, total_supply) -> bool:
    return min(*balances, total_supply) > 0


def unfunded_reason(coins, balances, total_supply) -> str | None:
    """Why the pool is unfunded, naming each empty coin balance, or None when funded."""
    if funded(balances, total_supply):
        return None
    empty = [str(coin) for coin, value in zip(coins, balances, strict=True) if not value]
    parts = ([f"no balance of {', '.join(empty)}"] if empty else []) + (
        [] if total_supply else ["no LP supply"]
    )
    return f"{FUNDED_POOL}; the pool holds {' and '.join(parts)}"


def read_funding(reader, source, block) -> tuple[tuple[int, ...], int]:
    """Coin balances and LP supply, read the same way by every Curve family."""
    supply = scalar(reader, source.pool, "totalSupply()", block, outputs=("uint256",))
    return quantities(reader, source, block, "balances"), supply


def amount(asset, value: int) -> dict:
    return {"asset": str(asset.identifier), "amount": str(value), "decimals": asset.decimals}


def holdings(coins, share, balances, total_supply) -> dict:
    return {
        "balances": [amount(coin, value) for coin, value in zip(coins, balances, strict=True)],
        "total_supply": amount(share, total_supply),
    }


def unfunded_report(reader, resolved, block) -> dict | None:
    """The `ax` report of an unfunded pool, read before its full state; None when funded.

    A crypto pool's observation requires positive balances, so its funding is read first.
    """
    balances, supply = read_funding(reader, resolved.pool, block)
    reason = unfunded_reason(resolved.pool.coins, balances, supply)
    if reason is None:
        return None
    return {
        "status": UNFUNDED_POOL,
        "reason": reason,
        "prices": None,
        "pool_state": holdings(resolved.coins, resolved.share, balances, supply),
    }


def bps(fee: int) -> str:
    return str(Decimal(fee) * 10_000 / FEE_DENOMINATOR)


def ratio(value: int, scale: int) -> str:
    return str(Decimal(value) / scale)


def pool_state(observation) -> dict:
    state = holdings(
        observation.currencies, observation.shares, observation.balances, observation.total_supply
    )
    if observation.source.family == "stableswap_ng":
        return {**state, **stable_state(observation)}
    return {**state, **crypto_state(observation)}


def stable_state(observation) -> dict:
    math = observation.math_state()
    invariant = math.invariant(math.normalized)
    return {
        "fee": {"raw": str(observation.fee), "bps": bps(observation.fee)},
        "offpeg_fee_multiplier": ratio(observation.offpeg_multiplier, FEE_DENOMINATOR),
        "amplification": ratio(math.amplification, stable_math.AMPLIFICATION_PRECISION),
        # The pool's own get_virtual_price: D per LP share, 1e18-scaled.
        "virtual_price": str(invariant * stable_math.PRECISION // observation.total_supply)
        if observation.total_supply
        else None,
    }


def crypto_state(observation) -> dict:
    amplification, gamma = observation.ramp.at(observation.block.timestamp)
    mid, out, fee_gamma = (
        (observation.packed_fees >> shift) & (2**64 - 1) for shift in (128, 64, 0)
    )
    return {
        # The swap fee moves from mid_fee (balanced) towards out_fee (imbalanced).
        "fee": {
            "mid_fee": {"raw": str(mid), "bps": bps(mid)},
            "out_fee": {"raw": str(out), "bps": bps(out)},
            "fee_gamma": str(fee_gamma),
        },
        "amplification": ratio(amplification, CRYPTO_A_MULTIPLIER),
        "gamma": str(gamma),
        "virtual_price": str(observation.accounting.virtual_price),
    }


class UnfundedPool(DataError):
    """A strategy market's Curve pool is unfunded at the block the check read."""

    check_code = UNFUNDED_POOL
    remedy = (
        "Choose a Curve pool that holds every coin and LP supply (`almanak ax pool` "
        "reports its balances), or check again once this pool is funded."
    )

    def __init__(self, market: str, pool: str, reason: str):
        super().__init__(UNFUNDED_POOL, f"market {market}: Curve pool {pool}: {reason}")


def require_funded(reader, name: str, resolved, block) -> None:
    """Refuse a strategy market whose pool is unfunded at `block`."""
    balances, supply = read_funding(reader, resolved.pool, block)
    reason = unfunded_reason(resolved.pool.coins, balances, supply)
    if reason is not None:
        raise UnfundedPool(name, resolved.pool.pool, reason)
