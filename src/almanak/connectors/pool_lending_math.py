"""Integer primitives shared by verified Aave V3 and SparkLend pool revisions."""

RAY = 10**27
SECONDS_PER_YEAR = 365 * 24 * 60 * 60
UINT256_MAX = 2**256 - 1


def uint(value: int, bits: int = 256) -> int:
    if type(value) is not int or not 0 <= value < 2**bits:
        raise ValueError(f"pool lending arithmetic requires a uint{bits}")
    return value


def ray_mul(a: int, b: int) -> int:
    return uint(uint(a) * uint(b) + RAY // 2) // RAY


def linear_interest(rate_ray: int, elapsed_seconds: int) -> int:
    uint(rate_ray, 128)
    uint(elapsed_seconds, 40)
    return RAY + rate_ray * elapsed_seconds // SECONDS_PER_YEAR
