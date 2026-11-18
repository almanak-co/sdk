"""Checked integer arithmetic for the verified Aerodrome Pool revision."""

ONE = 10**18
MAX_UINT256 = 2**256 - 1


def uint(value: int) -> int:
    if not 0 <= value <= MAX_UINT256:
        raise ValueError("uint256 arithmetic overflow or underflow")
    return value


def mul(a: int, b: int) -> int:
    return uint(a * b)


def add(a: int, b: int) -> int:
    return uint(a + b)


def sub(a: int, b: int) -> int:
    return uint(a - b)


def div(a: int, b: int) -> int:
    if not b:
        raise ValueError("division by zero")
    return a // b


def f(x: int, y: int) -> int:
    product = div(mul(x, y), ONE)
    squares = add(div(mul(x, x), ONE), div(mul(y, y), ONE))
    return div(mul(product, squares), ONE)


def derivative(x: int, y: int) -> int:
    first = div(mul(mul(3, x), div(mul(y, y), ONE)), ONE)
    second = div(mul(div(mul(x, x), ONE), x), ONE)
    return add(first, second)


def invariant(stable: bool, d0: int, d1: int, x: int, y: int) -> int:
    if not stable:
        return mul(x, y)
    return f(div(mul(x, ONE), d0), div(mul(y, ONE), d1))


def ascending_step(x: int, target: int, y: int, k: int, d0: int, d1: int):
    delta = div(mul(sub(target, k), ONE), derivative(x, y))
    if not delta:
        # The deployed revision applies decimal normalization again in this branch.
        # Replacing this with f(x, y + 1) changes observable integer rounding.
        if invariant(True, d0, d1, x, add(y, 1)) > target:
            return add(y, 1), True
        delta = 1
    return add(y, delta), False


def descending_step(x: int, target: int, y: int, k: int):
    delta = div(mul(sub(k, target), ONE), derivative(x, y))
    if not delta:
        if k == target or f(x, sub(y, 1)) < target:
            return y, True
        delta = 1
    return sub(y, delta), False


def solve(x: int, target: int, y: int, d0: int, d1: int) -> int:
    for _ in range(255):
        k = f(x, y)
        if k < target:
            y, done = ascending_step(x, target, y, k, d0, d1)
        else:
            y, done = descending_step(x, target, y, k)
        if done:
            return y
    raise ValueError("stable root did not converge")


def stable_output(d0: int, d1: int, r0: int, r1: int, amount: int, zero_for_one: bool) -> int:
    k = invariant(True, d0, d1, r0, r1)
    r0, r1 = div(mul(r0, ONE), d0), div(mul(r1, ONE), d1)
    reserve_in, reserve_out = (r0, r1) if zero_for_one else (r1, r0)
    amount = div(mul(amount, ONE), d0 if zero_for_one else d1)
    output = sub(reserve_out, solve(add(amount, reserve_in), k, reserve_out, d0, d1))
    return div(mul(output, d1 if zero_for_one else d0), ONE)


def quote(
    stable: bool, d0: int, d1: int, r0: int, r1: int, amount: int, fee: int, zero_for_one: bool
) -> int:
    for value in (d0, d1, r0, r1, amount, fee):
        uint(value)
    amount = sub(amount, div(mul(amount, fee), 10000))
    if stable:
        return stable_output(d0, d1, r0, r1, amount, zero_for_one)
    reserve_in, reserve_out = (r0, r1) if zero_for_one else (r1, r0)
    return div(mul(amount, reserve_out), add(reserve_in, amount))


def accrued(shares: int, global_index: int, supply_index: int, claimable: int) -> int:
    """Fees remain a separate fund claim, including after its last LP share is burned."""
    for value in (shares, global_index, supply_index, claimable):
        uint(value)
    if not shares:
        return claimable
    delta = sub(global_index, supply_index)
    return add(claimable, div(mul(shares, delta), ONE))


def addition(r0: int, r1: int, supply: int, maximum0: int, maximum1: int):
    """Router proportional budgets for an existing pool with synchronized balances."""
    if min(r0, r1, supply, maximum0, maximum1) <= 0:
        raise ValueError("liquidity addition requires an existing pool and positive budgets")
    optimal1 = div(mul(maximum0, r1), r0)
    if optimal1 <= maximum1:
        amount0, amount1 = maximum0, optimal1
    else:
        amount0, amount1 = div(mul(maximum1, r0), r1), maximum1
    shares = min(div(mul(amount0, supply), r0), div(mul(amount1, supply), r1))
    if not shares:
        raise ValueError("insufficient liquidity minted")
    return amount0, amount1, shares


def removal(balance0: int, balance1: int, supply: int, shares: int):
    """Burn uses actual pool balances, which need not equal stored reserves."""
    if not 0 < shares <= supply:
        raise ValueError("liquidity removal requires positive shares within total supply")
    amount0 = div(mul(shares, balance0), supply)
    amount1 = div(mul(shares, balance1), supply)
    if not min(amount0, amount1):
        raise ValueError("insufficient liquidity burned")
    return amount0, amount1
