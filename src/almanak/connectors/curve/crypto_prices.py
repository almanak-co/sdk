"""Crypto NG integer exponential and EMA, compared for both deployed revisions."""

from almanak.connectors.curve.crypto_integer import PRECISION, divide, unsigned


def exponential(value):
    if type(value) is not int or not -(2**255) <= value < 2**255:
        raise ValueError("Crypto NG exponential requires an int256")
    if value <= -42139678854452767551:
        return 0
    if value >= 135305999368893231589:
        raise ValueError("Crypto NG exponential overflows int256")
    reduced = divide(value << 78, 5**18)
    power = (divide(reduced << 96, 54916777467707473351141471128) + 2**95) >> 96
    reduced -= power * 54916777467707473351141471128
    numerator, denominator = exponential_polynomials(reduced)
    ratio = divide(numerator, denominator)
    scaled = (ratio * 3822833074963236453042738258902158003155416615667) % 2**256
    return scaled >> (195 - power)


def exponential_polynomials(value):
    intermediate = (value + 1346386616545796478920950773328) * value >> 96
    intermediate += 57155421227552351082224309758442
    numerator = ((intermediate + value - 94201549194550492254356042504812) * intermediate) >> 96
    numerator = (numerator + 28719021644029726153956944680412240) * value
    numerator += 4385272521454847904659076985693276 << 96
    denominator = ((value - 2855989394907223263936484059900) * value) >> 96
    denominator += 50020603652535783019961831881945
    denominator = (denominator * value >> 96) - 533845033583426703283633433725380
    denominator = (denominator * value >> 96) + 3604857256930695427073651918091429
    denominator = (denominator * value >> 96) - 14423608567350463180887372962807573
    denominator = (denominator * value >> 96) + 26449188498355588339934803723976023
    return numerator, denominator


def oracle(*, cached, last_price, scale, last_timestamp, timestamp, duration):
    for value in (cached, last_price, scale, duration):
        if unsigned(value) == 0:
            raise ValueError("Crypto NG EMA requires positive price inputs and duration")
    unsigned(timestamp)
    unsigned(last_timestamp)
    if timestamp < last_timestamp:
        raise ValueError("Crypto NG EMA cannot use a future price")
    if timestamp == last_timestamp:
        return cached
    decay = exponential(-(unsigned((timestamp - last_timestamp) * PRECISION) // duration))
    weighted = unsigned(min(last_price, unsigned(2 * scale)) * (PRECISION - decay))
    return unsigned(weighted + unsigned(cached * decay)) // PRECISION
