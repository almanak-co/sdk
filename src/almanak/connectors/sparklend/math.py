"""SparkLend Pool revision 4 and token revision 1 integer accounting."""

from almanak.connectors.pool_lending_math import RAY as RAY
from almanak.connectors.pool_lending_math import SECONDS_PER_YEAR as SECONDS_PER_YEAR
from almanak.connectors.pool_lending_math import UINT256_MAX as UINT256_MAX
from almanak.connectors.pool_lending_math import linear_interest as linear_interest
from almanak.connectors.pool_lending_math import ray_mul as ray_mul
from almanak.connectors.pool_lending_math import uint as uint


def ray_div(a: int, b: int) -> int:
    if not uint(b):
        raise ValueError("SparkLend arithmetic cannot divide by zero")
    return uint(uint(a) * RAY + b // 2) // b


def compounded_interest(rate_ray: int, elapsed_seconds: int) -> int:
    uint(rate_ray, 128)
    elapsed = uint(elapsed_seconds, 40)
    if elapsed == 0:
        return RAY
    power_two = ray_mul(rate_ray, rate_ray) // SECONDS_PER_YEAR**2
    power_three = ray_mul(power_two, rate_ray) // SECONDS_PER_YEAR
    second = uint(elapsed * (elapsed - 1) * power_two) // 2
    third = uint(elapsed * (elapsed - 1) * max(elapsed - 2, 0) * power_three) // 6
    return uint(RAY + rate_ray * elapsed // SECONDS_PER_YEAR + second + third)


def normalized_indices(reserve, timestamp: int) -> tuple[int, int]:
    elapsed = uint(timestamp, 40) - reserve.last_update_timestamp
    return (
        ray_mul(reserve.liquidity_index_ray, linear_interest(reserve.liquidity_rate_ray, elapsed)),
        ray_mul(
            reserve.variable_borrow_index_ray,
            compounded_interest(reserve.variable_borrow_rate_ray, elapsed),
        ),
    )


def transition_indices(reserve, timestamp: int) -> tuple[int, int]:
    supply_index, borrow_index = normalized_indices(reserve, timestamp)
    if not reserve.total_scaled_debt:
        borrow_index = reserve.variable_borrow_index_ray
    return uint(supply_index, 128), uint(borrow_index, 128)


def treasury_shares(reserve, timestamp: int) -> int:
    if reserve.total_stable_debt:
        raise ValueError("SparkLend treasury accounting does not model stable-rate debt")
    supply_index, borrow_index = transition_indices(reserve, timestamp)
    before = ray_mul(reserve.total_scaled_debt, reserve.variable_borrow_index_ray)
    after = ray_mul(reserve.total_scaled_debt, borrow_index)
    amount = uint((after - before) * reserve.configuration.reserve_factor_bps + 5000) // 10_000
    return uint(reserve.accrued_to_treasury_scaled + ray_div(amount, supply_index), 128)


def supply_shares(amount: int, index: int) -> int:
    return ray_div(amount, index)


def withdraw_shares(amount: int, index: int) -> int:
    return ray_div(amount, index)


def supplied_assets(shares: int, index: int) -> int:
    return ray_mul(shares, index)


def borrow_shares(amount: int, index: int) -> int:
    return ray_div(amount, index)


def repay_shares(amount: int, index: int) -> int:
    return ray_div(amount, index)


def debt_assets(shares: int, index: int) -> int:
    return ray_mul(shares, index)


def percent_mul(value: int, percentage: int) -> int:
    return uint(uint(value) * uint(percentage) + 10_000 // 2) // 10_000


def percent_div(value: int, percentage: int) -> int:
    if not uint(percentage):
        raise ValueError("SparkLend percentage division cannot divide by zero")
    return uint(uint(value) * 10_000 + percentage // 2) // percentage
