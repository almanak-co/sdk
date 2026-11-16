"""Integer StableSwap NG calculations retain the deployed division ordering.

Contract comparison source: curvefi/stableswap-ng revision 9228406a8ca0cee11fbe0510b269354767599e07.
Its retained license is third_party/Curve-StableSwap-NG-20231115-LICENSE.txt.
All vectors here are protocol-native units inside the connector boundary.
"""

from dataclasses import dataclass
from typing import Literal

StableRevision = Literal["stableswap-ng-2023-11-15", "stableswap-ng-2023-12-12"]
NOVEMBER: StableRevision = "stableswap-ng-2023-11-15"
DECEMBER: StableRevision = "stableswap-ng-2023-12-12"

PRECISION = 10**18
FEE_DENOMINATOR = 10**10
AMPLIFICATION_PRECISION = 100
ADMIN_FEE = 5 * 10**9
UINT256_MAX = 2**256 - 1


def uint(value):
    if type(value) is not int or not 0 <= value <= UINT256_MAX:
        raise ValueError("StableSwap arithmetic requires a supported uint256")
    return value


def product(*values):
    result = 1
    for value in values:
        result = uint(result * uint(value))
    return result


def divide(numerator, denominator):
    if denominator <= 0:
        raise ValueError("StableSwap requires positive divisors and funded reserves")
    return uint(numerator) // uint(denominator)


@dataclass(frozen=True)
class StableState:
    balances: tuple[int, ...]
    rates: tuple[int, ...]
    amplification: int
    fee: int
    offpeg_multiplier: int
    total_supply: int
    revision: StableRevision = NOVEMBER

    def __post_init__(self):
        validate_revision(self.revision)
        if not 2 <= len(self.balances) <= 8 or len(self.rates) != len(self.balances):
            raise ValueError("StableSwap requires two to eight ordered balances and rates")
        for value in (*self.balances, *self.rates, self.total_supply, self.offpeg_multiplier):
            uint(value)
        uint(self.amplification)
        uint(self.fee)
        if any(rate == 0 for rate in self.rates):
            raise ValueError("StableSwap rates must be positive")
        if not 0 < self.amplification <= 10**8 or not 0 <= self.fee <= FEE_DENOMINATOR:
            raise ValueError("StableSwap amplification or fee is outside the supported domain")

    @property
    def normalized(self):
        return normalize(self.balances, self.rates)

    def invariant(self, values):
        return invariant(values, self.amplification, revision=self.revision)


def validate_revision(revision):
    if revision not in (NOVEMBER, DECEMBER):
        raise ValueError("unsupported StableSwap integer revision")


def normalize(balances, rates):
    return tuple(
        divide(product(balance, rate), PRECISION)
        for balance, rate in zip(balances, rates, strict=True)
    )


def invariant(values, amplification, *, revision: StableRevision = NOVEMBER):
    validate_revision(revision)
    total = uint(sum(values))
    if total == 0:
        return 0
    count = len(values)
    ann = product(amplification, count)
    current = total
    for _ in range(255):
        term = invariant_product(values, current, revision)
        previous = current
        numerator = product(
            divide(product(ann, total), AMPLIFICATION_PRECISION) + product(term, count), current
        )
        denominator = divide(
            product(uint(ann - AMPLIFICATION_PRECISION), current), AMPLIFICATION_PRECISION
        ) + product(count + 1, term)
        current = divide(numerator, denominator)
        if abs(current - previous) <= 1:
            return current
    raise ValueError("StableSwap invariant did not converge")


def invariant_product(values, current, revision):
    # December defers the coin-count division until after the product loop.
    # Reordering these integer divisions can change LP issuance and swap output.
    count = len(values)
    divisor = 1 if revision == DECEMBER else count
    term = current
    for value in values:
        term = divide(product(term, current), product(value, divisor))
    return divide(term, count**count) if revision == DECEMBER else term


def solve_balance(values, amplification, excluded, invariant_value):
    count = len(values)
    ann = product(amplification, count)
    coefficient, subtotal = invariant_value, 0
    for index, value in enumerate(values):
        if index == excluded:
            continue
        subtotal = uint(subtotal + value)
        coefficient = divide(product(coefficient, invariant_value), product(value, count))
    coefficient = divide(
        product(coefficient, invariant_value, AMPLIFICATION_PRECISION), product(ann, count)
    )
    offset = uint(subtotal + divide(product(invariant_value, AMPLIFICATION_PRECISION), ann))
    current = invariant_value
    for _ in range(255):
        previous = current
        current = divide(
            uint(product(current, current) + coefficient),
            uint(2 * current + offset - invariant_value),
        )
        if abs(current - previous) <= 1:
            return current
    raise ValueError("StableSwap output balance did not converge")


def dynamic_fee(state, first, second, base_fee):
    multiplier = state.offpeg_multiplier
    if multiplier <= FEE_DENOMINATOR:
        return base_fee
    square = product(first + second, first + second)
    denominator = (
        divide(product(multiplier - FEE_DENOMINATOR, 4, first, second), square) + FEE_DENOMINATOR
    )
    return divide(product(multiplier, base_fee), denominator)


def coin_index(state, index):
    if type(index) is not int or not 0 <= index < len(state.balances):
        raise ValueError("StableSwap coin index is outside the configured pool")


@dataclass(frozen=True)
class SwapResult:
    output: int
    fee: int
    admin_fee: int


def swap(state, first, second, amount):
    coin_index(state, first)
    coin_index(state, second)
    if first == second or uint(amount) == 0:
        raise ValueError("StableSwap swaps require different coins and positive input")
    values = state.normalized
    changed = list(values)
    changed[first] = uint(values[first] + divide(product(amount, state.rates[first]), PRECISION))
    d = state.invariant(values)
    output_balance = solve_balance(changed, state.amplification, second, d)
    gross = uint(values[second] - output_balance - 1)
    rate = dynamic_fee(
        state,
        (values[first] + changed[first]) // 2,
        (values[second] + output_balance) // 2,
        state.fee,
    )
    fee = divide(product(gross, rate), FEE_DENOMINATOR)
    output = divide(product(gross - fee, PRECISION), state.rates[second])
    fee_units = divide(product(fee, PRECISION), state.rates[second])
    admin = divide(
        product(divide(product(fee, ADMIN_FEE), FEE_DENOMINATOR), PRECISION), state.rates[second]
    )
    return SwapResult(output=output, fee=fee_units, admin_fee=admin)


@dataclass(frozen=True)
class AdditionResult:
    minted: int
    fees: tuple[int, ...]
    admin_fees: tuple[int, ...]


def addition(state, amounts):
    if len(amounts) != len(state.balances) or not any(amounts):
        raise ValueError("StableSwap addition requires ordered positive inventory")
    if state.total_supply == 0 and not all(amounts):
        raise ValueError("initial StableSwap liquidity requires every coin")
    new = tuple(
        uint(balance + uint(amount))
        for balance, amount in zip(state.balances, amounts, strict=True)
    )
    initial = state.invariant(state.normalized)
    expanded = state.invariant(normalize(new, state.rates))
    if expanded <= initial:
        raise ValueError("StableSwap addition did not increase the invariant")
    if state.total_supply == 0:
        return AdditionResult(expanded, (0,) * len(new), (0,) * len(new))
    fees = addition_fees(state, new, initial, expanded)
    adjusted = tuple(uint(balance - fee) for balance, fee in zip(new, fees, strict=True))
    after_fees = state.invariant(normalize(adjusted, state.rates))
    minted = divide(product(state.total_supply, uint(after_fees - initial)), initial)
    admin = tuple(divide(product(fee, ADMIN_FEE), FEE_DENOMINATOR) for fee in fees)
    return AdditionResult(minted, fees, admin)


def addition_fees(state, balances, initial, expanded):
    count = len(balances)
    base = divide(product(state.fee, count), 4 * (count - 1))
    average = (initial + expanded) // count
    fees = []
    for old, new, rate in zip(state.balances, balances, state.rates, strict=True):
        ideal = divide(product(expanded, old), initial)
        combined = divide(product(rate, old + new), PRECISION)
        fee = dynamic_fee(state, combined, average, base)
        fees.append(divide(product(fee, abs(ideal - new)), FEE_DENOMINATOR))
    return tuple(fees)


@dataclass(frozen=True)
class RemovalResult:
    output: int
    fee: int
    admin_fee: int


def removal_one(state, index, shares):
    coin_index(state, index)
    if not 0 < uint(shares) < state.total_supply:
        raise ValueError("single-coin removal requires less than the pool's entire LP supply")
    values = state.normalized
    initial = state.invariant(values)
    reduced_d = uint(initial - divide(product(shares, initial), state.total_supply))
    new_y = solve_balance(values, state.amplification, index, reduced_d)
    reduced = removal_balances(state, index, initial, reduced_d, new_y)
    remaining = solve_balance(reduced, state.amplification, index, reduced_d)
    output = divide(product(uint(reduced[index] - remaining - 1), PRECISION), state.rates[index])
    gross = divide(product(uint(values[index] - new_y), PRECISION), state.rates[index])
    fee = uint(gross - output)
    return RemovalResult(output, fee, divide(product(fee, ADMIN_FEE), FEE_DENOMINATOR))


def removal_balances(state, index, initial, reduced_d, new_y):
    values = state.normalized
    count = len(values)
    base = divide(product(state.fee, count), 4 * (count - 1))
    average = (initial + reduced_d) // (2 * count)
    reduced = []
    for coin, value in enumerate(values):
        proportional = divide(product(value, reduced_d), initial)
        expected = uint(proportional - new_y) if coin == index else uint(value - proportional)
        observed_average = (value + new_y) // 2 if coin == index else value
        fee = dynamic_fee(state, observed_average, average, base)
        reduced.append(uint(value - divide(product(fee, expected), FEE_DENOMINATOR)))
    return tuple(reduced)


def removal_proportional(state, shares):
    if not 0 < uint(shares) <= state.total_supply:
        raise ValueError("proportional removal requires owned shares within the pool supply")
    return tuple(divide(product(balance, shares), state.total_supply) for balance in state.balances)
