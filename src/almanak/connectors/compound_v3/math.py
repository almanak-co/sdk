"""Checked Comet integer arithmetic with the deployed principal rounding rules."""

INDEX_SCALE = 10**15
FACTOR_SCALE = 10**18
PRICE_SCALE = 10**8
UINT256_MAX = 2**256 - 1


def uint(value: int, bits: int = 256) -> int:
    if type(value) is not int or not 0 <= value < 2**bits:
        raise ValueError(f"Compound arithmetic requires uint{bits}")
    return value


def signed(value: int, bits: int = 256) -> int:
    if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
        raise ValueError(f"Compound arithmetic requires int{bits}")
    return value


def mul_div(left: int, right: int, scale: int) -> int:
    product = uint(uint(left) * uint(right))
    if not uint(scale):
        raise ValueError("Compound arithmetic cannot divide by zero")
    return product // scale


def present_supply(principal: int, index: int) -> int:
    return mul_div(uint(principal, 104), uint(index, 64), INDEX_SCALE)


def present_borrow(principal: int, index: int) -> int:
    return mul_div(uint(principal, 104), uint(index, 64), INDEX_SCALE)


def present_value(principal: int, supply_index: int, borrow_index: int) -> int:
    signed(principal, 104)
    if principal >= 0:
        return signed(present_supply(principal, supply_index))
    return -signed(present_borrow(signed(-principal, 104), borrow_index))


def principal_value(balance: int, supply_index: int, borrow_index: int) -> int:
    signed(balance)
    if balance >= 0:
        return signed(mul_div(balance, INDEX_SCALE, uint(supply_index, 64)), 104)
    index = uint(borrow_index, 64)
    if not index:
        raise ValueError("Compound borrow principal requires a positive index")
    numerator = uint(uint(signed(-balance) * INDEX_SCALE) + index - 1)
    return -signed(uint(numerator // index, 104), 104)


def utilization(supply_principal: int, borrow_principal: int, indices: tuple[int, int]) -> int:
    supply = present_supply(supply_principal, indices[0])
    borrow = present_borrow(borrow_principal, indices[1])
    return mul_div(borrow, FACTOR_SCALE, supply) if supply else 0


def kink_rate(utilization: int, kink: int, base: int, slope_low: int, slope_high: int) -> int:
    uint(utilization)
    uint(kink)
    low = mul_div(slope_low, min(utilization, kink), FACTOR_SCALE)
    high = mul_div(slope_high, max(0, utilization - kink), FACTOR_SCALE)
    return uint(uint(uint(base) + low) + high, 64)


def accrue_index(index: int, rate: int, elapsed_seconds: int) -> int:
    uint(index, 64)
    interest = mul_div(index, uint(uint(rate, 64) * uint(elapsed_seconds, 40)), FACTOR_SCALE)
    return uint(index + uint(interest, 64), 64)


def repay_and_supply(old: int, new: int) -> tuple[int, int]:
    signed(old, 104)
    signed(new, 104)
    if new < old:
        return 0, 0
    if new <= 0:
        return uint(signed(new - old, 104), 104), 0
    if old >= 0:
        return 0, uint(signed(new - old, 104), 104)
    return uint(signed(-old, 104), 104), uint(new, 104)


def withdraw_and_borrow(old: int, new: int) -> tuple[int, int]:
    signed(old, 104)
    signed(new, 104)
    if new > old:
        return 0, 0
    if new >= 0:
        return uint(signed(old - new, 104), 104), 0
    if old <= 0:
        return 0, uint(signed(old - new, 104), 104)
    return uint(old, 104), uint(signed(-new, 104), 104)


def signed_price(balance: int, price: int, asset_scale: int) -> int:
    signed(balance)
    product = signed(balance * signed(uint(price)))
    if not uint(asset_scale, 64):
        raise ValueError("Compound price conversion requires a positive asset scale")
    value = abs(product) // asset_scale
    return -value if product < 0 else value
