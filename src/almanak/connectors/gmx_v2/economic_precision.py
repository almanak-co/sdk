"""GMX decimal conversions and the deployed Unlicense PRB fixed-point power algorithm."""

FLOAT = 10**30
SCALE = 10**18
MAX_UINT = 2**256 - 1

# Binary roots retained from deployed compiler input a8f08a50b1857f459bad4d5d99a5e97c.
EXP2_ROOTS = (
    0x16A09E667F3BCC909,
    0x1306FE0A31B7152DF,
    0x1172B83C7D517ADCE,
    0x10B5586CF9890F62A,
    0x1059B0D31585743AE,
    0x102C9A3E778060EE7,
    0x10163DA9FB33356D8,
    0x100B1AFA5ABCBED61,
    0x10058C86DA1C09EA2,
    0x1002C605E2E8CEC50,
    0x100162F3904051FA1,
    0x1000B175EFFDC76BA,
    0x100058BA01FB9F96D,
    0x10002C5CC37DA9492,
    0x1000162E525EE0547,
    0x10000B17255775C04,
    0x1000058B91B5BC9AE,
    0x100002C5C89D5EC6D,
    0x10000162E43F4F831,
    0x100000B1721BCFC9A,
    0x10000058B90CF1E6E,
    0x1000002C5C863B73F,
    0x100000162E430E5A2,
    0x1000000B172183551,
    0x100000058B90C0B49,
    0x10000002C5C8601CC,
    0x1000000162E42FFF0,
    0x10000000B17217FBB,
    0x1000000058B90BFCE,
    0x100000002C5C85FE3,
    0x10000000162E42FF1,
    0x100000000B17217F8,
    0x10000000058B90BFC,
    0x1000000002C5C85FE,
    0x100000000162E42FF,
    0x1000000000B17217F,
    0x100000000058B90C0,
    0x10000000002C5C860,
    0x1000000000162E430,
    0x10000000000B17218,
    0x1000000000058B90C,
    0x100000000002C5C86,
    0x10000000000162E43,
    0x100000000000B1721,
    0x10000000000058B91,
    0x1000000000002C5C8,
    0x100000000000162E4,
    0x1000000000000B172,
    0x100000000000058B9,
    0x10000000000002C5D,
    0x1000000000000162E,
    0x10000000000000B17,
    0x1000000000000058C,
    0x100000000000002C6,
    0x10000000000000163,
    0x100000000000000B1,
    0x10000000000000059,
    0x1000000000000002C,
    0x10000000000000016,
    0x1000000000000000B,
    0x10000000000000006,
    0x10000000000000003,
    0x10000000000000001,
    0x10000000000000001,
)


def uint(value: int) -> int:
    if not 0 <= value <= MAX_UINT:
        raise ValueError("GMX integer calculation exceeds uint256")
    return value


def logarithm2(value: int) -> int:
    uint(value)
    if value < SCALE:
        raise ValueError("GMX PRB logarithm requires a value at least one")
    integer = (value // SCALE).bit_length() - 1
    result, reduced = integer * SCALE, value >> integer
    delta = SCALE // 2
    while delta:
        reduced = reduced * reduced // SCALE
        if reduced >= 2 * SCALE:
            result += delta
            reduced >>= 1
        delta >>= 1
    return result


def exponential2(value: int) -> int:
    if not 0 <= value < 192 * SCALE:
        raise ValueError("GMX PRB exponent exceeds its supported range")
    binary = (value << 64) // SCALE
    result = 1 << 191
    for index, root in enumerate(EXP2_ROOTS):
        if binary & (1 << (63 - index)):
            result = result * root >> 64
    return uint((result * SCALE) >> (191 - (binary >> 64)))


def multiply_fixed(first: int, second: int) -> int:
    product = uint(first) * uint(second)
    if product >> 256 >= SCALE:
        raise ValueError("GMX PRB fixed-point product overflows")
    # The deployed PRB multiplication rounds half up before exponentiation.
    return uint((product + SCALE // 2) // SCALE)


def apply_exponent(value: int, exponent: int) -> int:
    uint(value)
    uint(exponent)
    if value < FLOAT:
        return 0
    if exponent == FLOAT:
        return value
    power = multiply_fixed(logarithm2(value // 10**12), exponent // 10**12)
    return uint(exponential2(power) * 10**12)


def apply_factor(value: int, factor: int) -> int:
    return uint(uint(value) * uint(factor) // FLOAT)


def ceil_div(value: int, divisor: int) -> int:
    if divisor <= 0:
        raise ValueError("GMX integer division requires a positive divisor")
    return (uint(value) + divisor - 1) // divisor
