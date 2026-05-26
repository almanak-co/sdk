"""Tests for AerodromeAdapter.add_cl_liquidity wei-overload guards (VIB-3737).

The wei-overload path on ``add_cl_liquidity`` carries pool-aligned amounts
and pre-computed mins straight from the IntentCompiler to the on-chain
mint calldata. Money-critical input invariants must be enforced at the
adapter boundary so an upstream bug (negative wei, min > desired) fails
loudly at compile time instead of producing malformed calldata or a
guaranteed on-chain revert.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
)
from almanak.framework.data.tokens.models import ResolvedToken

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"


@pytest.fixture
def adapter():
    """Construct an AerodromeAdapter wired to a mock TokenResolver.

    USDC and WETH are pre-resolved so the wei-overload guard tests don't
    need to exercise on-chain token lookup.
    """
    resolver = MagicMock()

    def _resolve(symbol_or_addr: str, *args: object, **kwargs: object) -> ResolvedToken:
        if symbol_or_addr in ("USDC", USDC_ADDRESS):
            return ResolvedToken(
                symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453,
            )
        if symbol_or_addr in ("WETH", WETH_ADDRESS):
            return ResolvedToken(
                symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453,
            )
        raise AssertionError(f"Unexpected token in test: {symbol_or_addr}")

    resolver.resolve.side_effect = _resolve
    config = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
    return AerodromeAdapter(config, token_resolver=resolver)


class TestWeiOverloadAllOrNone:
    """Wei-overload kwargs must be supplied as a complete 4-tuple, or none at all."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            # Only one wei kwarg supplied
            {"amount_a_wei": 100},
            {"amount_b_wei": 200},
            {"amount_a_min_wei": 50},
            {"amount_b_min_wei": 60},
            # Three of four supplied (missing min)
            {"amount_a_wei": 100, "amount_b_wei": 200, "amount_a_min_wei": 50},
            # Three of four supplied (missing amount)
            {"amount_a_wei": 100, "amount_b_wei": 200, "amount_b_min_wei": 60},
        ],
    )
    def test_partial_wei_overload_rejected(self, adapter: AerodromeAdapter, kwargs: dict) -> None:
        """Any partial wei-overload combination must short-circuit with a clear error."""
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-200,
            tick_upper=200,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            **kwargs,
        )
        assert result.success is False
        assert "Wei-overload requires all of" in (result.error or "")


class TestWeiOverloadInvariantGuards:
    """Money-critical invariants on wei-overload values."""

    @pytest.mark.parametrize(
        "amounts",
        [
            (-1, 200, 50, 60),      # negative amount_a
            (100, -200, 50, 60),    # negative amount_b
            (100, 200, -1, 60),     # negative min_a
            (100, 200, 50, -1),     # negative min_b
        ],
    )
    def test_negative_wei_rejected(self, adapter: AerodromeAdapter, amounts: tuple[int, int, int, int]) -> None:
        """Negative wei amounts/mins fail at compile time, not on-chain."""
        a_wei, b_wei, a_min, b_min = amounts
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-200,
            tick_upper=200,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=a_wei,
            amount_b_wei=b_wei,
            amount_a_min_wei=a_min,
            amount_b_min_wei=b_min,
        )
        assert result.success is False
        assert "non-negative" in (result.error or "").lower()

    @pytest.mark.parametrize(
        "amounts,which",
        [
            ((100, 200, 101, 60), "a"),  # min_a > amount_a
            ((100, 200, 50, 201), "b"),  # min_b > amount_b
        ],
    )
    def test_min_above_desired_rejected(
        self,
        adapter: AerodromeAdapter,
        amounts: tuple[int, int, int, int],
        which: str,
    ) -> None:
        """``min > desired`` is a guaranteed on-chain revert; reject early."""
        a_wei, b_wei, a_min, b_min = amounts
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-200,
            tick_upper=200,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=a_wei,
            amount_b_wei=b_wei,
            amount_a_min_wei=a_min,
            amount_b_min_wei=b_min,
        )
        assert result.success is False
        assert "<= desired" in (result.error or "")

    def test_zero_amounts_and_mins_accepted(self, adapter: AerodromeAdapter) -> None:
        """``0`` is the boundary value; both desired and min may be 0."""
        # Zero amounts won't yield a useful position but must NOT trip the
        # non-negativity guard. The mint downstream may fail for other
        # reasons (zero liquidity); we only test the guard here.
        # NOTE: reaching the SDK requires a working web3 + sdk; we only
        # need to assert the guard does NOT short-circuit on zeros, so we
        # patch the SDK call to a benign success.
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "value": 0, "data": b"\x00" * 4},
        )
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-200,
            tick_upper=200,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=0,
            amount_b_wei=0,
            amount_a_min_wei=0,
            amount_b_min_wei=0,
        )
        assert result.success is True
