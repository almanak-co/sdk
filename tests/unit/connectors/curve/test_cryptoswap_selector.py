"""Tests for Curve CryptoSwap/Tricrypto exchange selector fix (VIB-583).

CryptoSwap and Tricrypto pools use a different exchange function signature
than StableSwap pools:
- StableSwap:  exchange(int128 i, int128 j, uint256 dx, uint256 min_dy) = 0x3df02124
- CryptoSwap:  exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy) = 0x5b41b908
- Tricrypto:   exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy) = 0x5b41b908

Using the wrong selector causes a silent no-op or revert on CryptoSwap/Tricrypto pools.
"""

import pytest

from almanak.framework.connectors.curve.adapter import (
    EXCHANGE_SELECTOR,
    EXCHANGE_UINT256_SELECTOR,
    CurveAdapter,
    CurveConfig,
    PoolType,
)


@pytest.fixture
def adapter() -> CurveAdapter:
    """Create Curve adapter for Ethereum."""
    config = CurveConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return CurveAdapter(config)


class TestExchangeSelector:
    """Tests that the correct exchange selector is used for each pool type."""

    @pytest.mark.parametrize(
        ("pool_type", "expected_selector"),
        [
            (PoolType.STABLESWAP, EXCHANGE_SELECTOR),
            (PoolType.CRYPTOSWAP, EXCHANGE_UINT256_SELECTOR),
            (PoolType.TRICRYPTO, EXCHANGE_UINT256_SELECTOR),
        ],
    )
    def test_exchange_selector_for_pool_type(
        self, adapter: CurveAdapter, pool_type: PoolType, expected_selector: str
    ) -> None:
        """Each pool type should use its corresponding exchange selector."""
        tx = adapter._build_exchange_tx(
            pool_address="0x" + "A" * 40,
            i=0,
            j=1,
            amount_in=1000000,
            min_amount_out=990000,
            pool_type=pool_type,
        )
        assert tx.data.startswith(expected_selector)

    def test_default_pool_type_is_stableswap(self, adapter: CurveAdapter) -> None:
        """Default pool_type should be STABLESWAP for backward compatibility."""
        tx = adapter._build_exchange_tx(
            pool_address="0x" + "A" * 40,
            i=0,
            j=1,
            amount_in=1000000,
            min_amount_out=990000,
        )
        assert tx.data.startswith(EXCHANGE_SELECTOR)


class TestSelectorValues:
    """Verify selector constants match expected keccak256 values."""

    def test_stableswap_selector_value(self) -> None:
        """EXCHANGE_SELECTOR should be keccak256('exchange(int128,int128,uint256,uint256)')[:4]."""
        assert EXCHANGE_SELECTOR == "0x3df02124"

    def test_cryptoswap_selector_value(self) -> None:
        """EXCHANGE_UINT256_SELECTOR should be keccak256('exchange(uint256,uint256,uint256,uint256)')[:4]."""
        assert EXCHANGE_UINT256_SELECTOR == "0x5b41b908"


class TestCalldataEncoding:
    """Verify calldata encoding for different pool types."""

    @pytest.mark.parametrize(
        "pool_type",
        [
            PoolType.STABLESWAP,
            PoolType.CRYPTOSWAP,
        ],
    )
    def test_calldata_length(self, adapter: CurveAdapter, pool_type: PoolType) -> None:
        """Calldata should be selector(10) + 4 words(256) = 266 hex chars."""
        tx = adapter._build_exchange_tx(
            pool_address="0x" + "A" * 40,
            i=0,
            j=1,
            amount_in=1000000,
            min_amount_out=990000,
            pool_type=pool_type,
        )
        # 0x + 8 selector + 4*64 words = 2 + 8 + 256 = 266
        assert len(tx.data) == 266

    def test_tricrypto_swap_passes_pool_type(self, adapter: CurveAdapter) -> None:
        """Full swap() on a tricrypto pool should use the uint256 selector."""
        result = adapter.swap(
            pool_address="0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
            token_in="USDT",
            token_out="WBTC",
            amount_in=1000,
        )
        assert result.success is True
        # Find the swap tx (last one, after approve)
        swap_tx = result.transactions[-1]
        assert swap_tx.data.startswith(EXCHANGE_UINT256_SELECTOR)
