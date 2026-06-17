import pytest

from almanak.core.enums import SwapSide
from almanak.core.models.params import SwapParams


def _swap_params(**overrides) -> SwapParams:
    params = {
        "tokenIn": "USDC",
        "tokenOut": "WETH",
        "recipient": "0x0000000000000000000000000000000000000001",
        "amount": 100,
    }
    params.update(overrides)
    return SwapParams(**params)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"amountOutMinimum": -1},
            "amountOutMinimum must be non-negative if provided",
        ),
        (
            {"amountInMaximum": -1},
            "amountInMaximum must be non-negative if provided",
        ),
        (
            {"sqrtPriceLimitX96": -1},
            "sqrtPriceLimitX96 must be non-negative if provided",
        ),
        (
            {"side": SwapSide.SELL, "amountInMaximum": 1},
            "amountInMaximum should not be provided for sell side",
        ),
        (
            {"side": SwapSide.BUY, "amountOutMinimum": 1},
            "amountOutMinimum should not be provided for buy side",
        ),
        (
            {"amountOutMinimum": 1, "slippage": 0.01},
            "Only one of amountOutMinimum or slippage should be provided, not both",
        ),
        (
            {"amountInMaximum": 1, "slippage": 0.01},
            "Only one of amountInMaximum or slippage should be provided, not both",
        ),
        (
            {"side": SwapSide.SELL},
            "Either amountOutMinimum or slippage must be provided",
        ),
        (
            {"side": SwapSide.BUY},
            "Either amountInMaximum or slippage must be provided",
        ),
    ],
)
def test_swap_params_validate_params_rejects_invalid_protection(overrides, message) -> None:
    params = _swap_params(**overrides)

    with pytest.raises(ValueError) as exc_info:
        params.validate_params()

    assert str(exc_info.value) == message


@pytest.mark.parametrize(
    "overrides",
    [
        {"side": SwapSide.SELL, "amountOutMinimum": 0},
        {"side": SwapSide.SELL, "slippage": 0.01},
        {"side": SwapSide.BUY, "amountInMaximum": 0},
        {"side": SwapSide.BUY, "slippage": 0.01},
        {"side": None},
        {"side": None, "amountOutMinimum": 1, "amountInMaximum": 2},
    ],
)
def test_swap_params_validate_params_preserves_legacy_valid_shapes(overrides) -> None:
    _swap_params(**overrides).validate_params()


def test_swap_params_validate_params_preserves_first_error_order() -> None:
    params = _swap_params(
        side=SwapSide.BUY,
        amountOutMinimum=-1,
        amountInMaximum=-2,
        slippage=0.01,
        sqrtPriceLimitX96=-3,
    )

    with pytest.raises(ValueError) as exc_info:
        params.validate_params()

    assert str(exc_info.value) == "amountOutMinimum must be non-negative if provided"
