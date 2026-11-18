"""Integer accounting for the verified V3 revision, with protocol rounding directions."""

from almanak.connectors.pool_lending_math import RAY as RAY
from almanak.connectors.pool_lending_math import SECONDS_PER_YEAR as SECONDS_PER_YEAR
from almanak.connectors.pool_lending_math import UINT256_MAX as UINT256_MAX
from almanak.connectors.pool_lending_math import linear_interest as linear_interest
from almanak.connectors.pool_lending_math import ray_mul as ray_mul
from almanak.connectors.pool_lending_math import uint as uint


def mul_div(a: int, b: int, denominator: int, *, ceiling: bool = False) -> int:
    product = uint(uint(a) * uint(b))
    if not uint(denominator):
        raise ValueError("Aave V3 arithmetic cannot divide by zero")
    quotient, remainder = divmod(product, denominator)
    return quotient + int(ceiling and remainder != 0)


def compounded_interest(rate_ray: int, elapsed_seconds: int) -> int:
    uint(rate_ray, 128)
    uint(elapsed_seconds, 40)
    x = rate_ray * elapsed_seconds // SECONDS_PER_YEAR
    return RAY + x + ray_mul(x, x // 2 + ray_mul(x, x // 6))


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
    # getNormalizedDebt is a view. updateState deliberately keeps the stored
    # borrow index while there is no scaled debt, including the first new borrow.
    if not reserve.total_scaled_debt:
        borrow_index = reserve.variable_borrow_index_ray
    return uint(supply_index, 128), uint(borrow_index, 128)


def treasury_shares(reserve, timestamp: int) -> int:
    supply_index, borrow_index = transition_indices(reserve, timestamp)
    accrued_debt = mul_div(
        reserve.total_scaled_debt, borrow_index - reserve.variable_borrow_index_ray, RAY
    )
    amount = uint(accrued_debt * reserve.configuration.reserve_factor_bps + 5000) // 10_000
    return uint(reserve.accrued_to_treasury_scaled + supply_shares(amount, supply_index), 128)


def supply_shares(amount: int, index: int) -> int:
    return mul_div(amount, RAY, index)


def withdraw_shares(amount: int, index: int) -> int:
    return mul_div(amount, RAY, index, ceiling=True)


def supplied_assets(shares: int, index: int) -> int:
    return mul_div(shares, index, RAY)


def borrow_shares(amount: int, index: int) -> int:
    return mul_div(amount, RAY, index, ceiling=True)


def repay_shares(amount: int, index: int) -> int:
    return mul_div(amount, RAY, index)


def debt_assets(shares: int, index: int) -> int:
    return mul_div(shares, index, RAY, ceiling=True)
