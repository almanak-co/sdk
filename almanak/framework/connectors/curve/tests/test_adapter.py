"""Tests for Curve Finance Adapter.

This module tests the CurveAdapter class functionality including:
- Configuration validation
- Swap transaction building
- Add liquidity (LP_OPEN) transaction building
- Remove liquidity (LP_CLOSE) transaction building
- Token resolution and pool info lookup
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.curve.adapter import (
    ADD_LIQUIDITY_3_SELECTOR,
    CURVE_ADDRESSES,
    CURVE_GAS_ESTIMATES,
    CURVE_POOLS,
    EXCHANGE_SELECTOR,
    REMOVE_LIQUIDITY_3_SELECTOR,
    REMOVE_LIQUIDITY_ONE_SELECTOR,
    CurveAdapter,
    CurveConfig,
    LiquidityResult,
    PoolInfo,
    PoolType,
    SwapResult,
    TransactionData,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def ethereum_config() -> CurveConfig:
    """Create an Ethereum config for testing."""
    return CurveConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        default_slippage_bps=50,
    )


@pytest.fixture
def arbitrum_config() -> CurveConfig:
    """Create an Arbitrum config for testing."""
    return CurveConfig(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        default_slippage_bps=100,
    )


@pytest.fixture
def adapter(ethereum_config: CurveConfig) -> CurveAdapter:
    """Create a CurveAdapter instance for testing."""
    return CurveAdapter(ethereum_config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestCurveConfig:
    """Tests for CurveConfig validation."""

    def test_valid_ethereum_config(self) -> None:
        """Test valid Ethereum configuration."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.default_slippage_bps == 50

    def test_valid_arbitrum_config(self) -> None:
        """Test valid Arbitrum configuration."""
        config = CurveConfig(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "arbitrum"

    def test_invalid_chain(self) -> None:
        """Test invalid chain raises error."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            CurveConfig(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_slippage_negative(self) -> None:
        """Test negative slippage raises error."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            CurveConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_too_high(self) -> None:
        """Test slippage > 100% raises error."""
        with pytest.raises(ValueError, match="Slippage must be between"):
            CurveConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
            )

    def test_config_to_dict(self, ethereum_config: CurveConfig) -> None:
        """Test config serialization."""
        config_dict = ethereum_config.to_dict()
        assert config_dict["chain"] == "ethereum"
        assert config_dict["default_slippage_bps"] == 50


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestAdapterInitialization:
    """Tests for CurveAdapter initialization."""

    def test_adapter_init(self, ethereum_config: CurveConfig) -> None:
        """Test adapter initializes correctly."""
        adapter = CurveAdapter(ethereum_config)
        assert adapter.chain == "ethereum"
        assert adapter.addresses == CURVE_ADDRESSES["ethereum"]

    def test_adapter_init_arbitrum(self, arbitrum_config: CurveConfig) -> None:
        """Test adapter initializes for Arbitrum."""
        adapter = CurveAdapter(arbitrum_config)
        assert adapter.chain == "arbitrum"
        assert adapter.addresses == CURVE_ADDRESSES["arbitrum"]

    def test_adapter_has_pools(self, adapter: CurveAdapter) -> None:
        """Test adapter has pool data."""
        assert adapter.pools is not None
        assert "3pool" in adapter.pools

    def test_adapter_has_tokens(self, adapter: CurveAdapter) -> None:
        """Test adapter has token data."""
        assert adapter.tokens is not None
        assert "USDC" in adapter.tokens


# =============================================================================
# Pool Info Tests
# =============================================================================


class TestPoolInfo:
    """Tests for pool information lookup."""

    def test_get_pool_info_by_address(self, adapter: CurveAdapter) -> None:
        """Test getting pool info by address."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        pool_info = adapter.get_pool_info(pool_address)

        assert pool_info is not None
        assert pool_info.name == "3pool"
        assert pool_info.n_coins == 3
        assert pool_info.pool_type == PoolType.STABLESWAP

    def test_get_pool_info_by_name(self, adapter: CurveAdapter) -> None:
        """Test getting pool info by name."""
        pool_info = adapter.get_pool_by_name("3pool")

        assert pool_info is not None
        assert pool_info.n_coins == 3
        assert "DAI" in pool_info.coins
        assert "USDC" in pool_info.coins
        assert "USDT" in pool_info.coins

    def test_get_pool_info_unknown(self, adapter: CurveAdapter) -> None:
        """Test getting unknown pool returns None."""
        pool_info = adapter.get_pool_info("0x0000000000000000000000000000000000000000")
        assert pool_info is None

    def test_pool_info_get_coin_index(self) -> None:
        """Test getting coin index from PoolInfo."""
        pool_info = PoolInfo(
            address="0x1234",
            lp_token="0x5678",
            coins=["DAI", "USDC", "USDT"],
            coin_addresses=["0xaaa", "0xbbb", "0xccc"],
            pool_type=PoolType.STABLESWAP,
            n_coins=3,
        )

        assert pool_info.get_coin_index("DAI") == 0
        assert pool_info.get_coin_index("USDC") == 1
        assert pool_info.get_coin_index("USDT") == 2
        assert pool_info.get_coin_index("usdc") == 1  # Case insensitive

    def test_pool_info_get_coin_index_by_address(self) -> None:
        """Test getting coin index by address."""
        pool_info = PoolInfo(
            address="0x1234",
            lp_token="0x5678",
            coins=["DAI", "USDC", "USDT"],
            coin_addresses=["0xaaa", "0xbbb", "0xccc"],
            pool_type=PoolType.STABLESWAP,
            n_coins=3,
        )

        assert pool_info.get_coin_index("0xbbb") == 1

    def test_pool_info_get_coin_index_invalid(self) -> None:
        """Test getting invalid coin raises error."""
        pool_info = PoolInfo(
            address="0x1234",
            lp_token="0x5678",
            coins=["DAI", "USDC", "USDT"],
            coin_addresses=["0xaaa", "0xbbb", "0xccc"],
            pool_type=PoolType.STABLESWAP,
            n_coins=3,
        )

        with pytest.raises(ValueError, match="not found in pool"):
            pool_info.get_coin_index("WETH")


# =============================================================================
# Swap Tests
# =============================================================================


class TestSwap:
    """Tests for swap transaction building."""

    def test_swap_success(self, adapter: CurveAdapter) -> None:
        """Test successful swap transaction building."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        assert len(result.transactions) >= 1  # At least swap tx
        assert result.pool_address == pool_address
        assert result.amount_in > 0

    def test_swap_with_approve(self, adapter: CurveAdapter) -> None:
        """Test swap includes approve transaction."""
        adapter.clear_allowance_cache()  # Ensure no cached allowance
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        assert len(result.transactions) == 2  # approve + swap
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"

    def test_swap_skips_approve_when_cached(self, adapter: CurveAdapter) -> None:
        """Test swap skips approve when allowance is cached."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        token_address = CURVE_POOLS["ethereum"]["3pool"]["coin_addresses"][1]  # USDC

        # Pre-set allowance
        adapter.set_allowance(token_address, pool_address, 10**18)

        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        assert len(result.transactions) == 1  # Only swap, no approve
        assert result.transactions[0].tx_type == "swap"

    def test_swap_unknown_pool(self, adapter: CurveAdapter) -> None:
        """Test swap with unknown pool returns error."""
        result = adapter.swap(
            pool_address="0x0000000000000000000000000000000000000000",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is False
        assert "Unknown pool" in (result.error or "")

    def test_swap_unknown_token(self, adapter: CurveAdapter) -> None:
        """Test swap with unknown token returns error."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.swap(
            pool_address=pool_address,
            token_in="WETH",  # Not in 3pool
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is False
        assert "not found in pool" in (result.error or "")

    def test_swap_calldata_format(self, adapter: CurveAdapter) -> None:
        """Test swap calldata has correct format."""
        adapter.clear_allowance_cache()
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]

        # Set allowance to skip approve tx
        token_address = CURVE_POOLS["ethereum"]["3pool"]["coin_addresses"][1]  # USDC
        adapter.set_allowance(token_address, pool_address, 10**18)

        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        swap_tx = result.transactions[0]
        assert swap_tx.data.startswith(EXCHANGE_SELECTOR)
        assert swap_tx.to.lower() == pool_address.lower()

    def test_swap_gas_estimate(self, adapter: CurveAdapter) -> None:
        """Test swap gas estimate is reasonable."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )

        assert result.success is True
        assert result.gas_estimate > 0
        assert result.gas_estimate <= sum([CURVE_GAS_ESTIMATES["approve"], CURVE_GAS_ESTIMATES["exchange"]])


# =============================================================================
# Add Liquidity Tests
# =============================================================================


class TestAddLiquidity:
    """Tests for add_liquidity (LP_OPEN) transaction building."""

    def test_add_liquidity_success(self, adapter: CurveAdapter) -> None:
        """Test successful add_liquidity transaction building."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.add_liquidity(
            pool_address=pool_address,
            amounts=[Decimal("1000"), Decimal("1000"), Decimal("1000")],
        )

        assert result.success is True
        assert result.operation == "add_liquidity"
        assert len(result.amounts) == 3

    def test_add_liquidity_wrong_amount_count(self, adapter: CurveAdapter) -> None:
        """Test add_liquidity with wrong number of amounts."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.add_liquidity(
            pool_address=pool_address,
            amounts=[Decimal("1000"), Decimal("1000")],  # Wrong count
        )

        assert result.success is False
        assert "Expected 3 amounts" in (result.error or "")

    def test_add_liquidity_calldata_format(self, adapter: CurveAdapter) -> None:
        """Test add_liquidity calldata has correct format."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.add_liquidity(
            pool_address=pool_address,
            amounts=[Decimal("1000"), Decimal("1000"), Decimal("1000")],
        )

        assert result.success is True
        # Find the add_liquidity tx (skip approves)
        add_liq_tx = next(tx for tx in result.transactions if tx.tx_type == "add_liquidity")
        assert add_liq_tx.data.startswith(ADD_LIQUIDITY_3_SELECTOR)


# =============================================================================
# Remove Liquidity Tests
# =============================================================================


class TestRemoveLiquidity:
    """Tests for remove_liquidity (LP_CLOSE) transaction building."""

    def test_remove_liquidity_success(self, adapter: CurveAdapter) -> None:
        """Test successful remove_liquidity transaction building."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
        )

        assert result.success is True
        assert result.operation == "remove_liquidity"
        assert len(result.amounts) == 3

    def test_remove_liquidity_calldata_format(self, adapter: CurveAdapter) -> None:
        """Test remove_liquidity calldata has correct format."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
        )

        assert result.success is True
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_3_SELECTOR)

    def test_remove_liquidity_one_coin(self, adapter: CurveAdapter) -> None:
        """Test remove_liquidity_one_coin transaction building."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity_one_coin(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
            coin_index=1,  # USDC
        )

        assert result.success is True
        assert result.operation == "remove_liquidity_one_coin"

    def test_remove_liquidity_one_coin_invalid_index(self, adapter: CurveAdapter) -> None:
        """Test remove_liquidity_one_coin with invalid index."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity_one_coin(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
            coin_index=5,  # Invalid
        )

        assert result.success is False
        assert "Invalid coin index" in (result.error or "")

    def test_remove_liquidity_one_calldata_format(self, adapter: CurveAdapter) -> None:
        """Test remove_liquidity_one_coin calldata has correct format."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity_one_coin(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
            coin_index=0,
        )

        assert result.success is True
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_ONE_SELECTOR)


# =============================================================================
# Swap Output Estimation Tests (VIB-1417)
# =============================================================================


class TestEstimateSwapOutput:
    """Tests for _estimate_swap_output slippage protection.

    VIB-1417: CryptoSwap pools must use price_ratio for accurate min_amount_out.
    Without price_ratio, cross-decimal swaps (e.g., USDT->WETH) had zero protection.
    """

    def _make_pool(self, pool_type: PoolType, coins: list[str], coin_addresses: list[str]) -> PoolInfo:
        return PoolInfo(
            address="0x1234",
            lp_token="0x5678",
            coins=coins,
            coin_addresses=coin_addresses,
            pool_type=pool_type,
            n_coins=len(coins),
        )

    def test_stableswap_same_decimals(self, adapter: CurveAdapter) -> None:
        """StableSwap with same decimals returns 1:1."""
        pool = self._make_pool(PoolType.STABLESWAP, ["DAI", "USDC"], ["0xaaa", "0xbbb"])
        # Mock: pretend both have 18 decimals (DAI)
        # DAI=18, USDC=6 in real life, but we test same-decimal case
        result = adapter._estimate_swap_output(pool, 0, 0, 1000000000000000000)
        assert result == 1000000000000000000

    def test_stableswap_different_decimals(self, adapter: CurveAdapter) -> None:
        """StableSwap adjusts for decimal difference (USDC 6 -> DAI 18)."""
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        pool = adapter.get_pool_info(pool_address)
        assert pool is not None
        # USDC (index 1, 6 decimals) -> DAI (index 0, 18 decimals)
        # 1000 USDC = 1000_000000 in 6 decimals
        amount_in = 1000_000000
        result = adapter._estimate_swap_output(pool, 1, 0, amount_in)
        # Should scale up by 10^12 (18-6)
        assert result == 1000_000000_000000_000000

    def test_cryptoswap_with_price_ratio(self, adapter: CurveAdapter) -> None:
        """CryptoSwap with price_ratio gives accurate estimate.

        The original bug: USDT->WETH returned amount_in=100_000000 (100 USDT in 6 decimals)
        as min_amount_out for WETH (18 decimals), providing zero protection.
        With price_ratio, it should return ~0.04 WETH in 18 decimals.
        """
        pool_address = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        pool = adapter.get_pool_info(pool_address)
        assert pool is not None
        # USDT (index 0, 6 decimals) -> WETH (index 2, 18 decimals)
        # 100 USDT at $1, WETH at $2500 -> price_ratio = 1/2500 = 0.0004
        amount_in = 100_000000  # 100 USDT in 6 decimals
        price_ratio = Decimal("1") / Decimal("2500")

        result = adapter._estimate_swap_output(pool, 0, 2, amount_in, price_ratio=price_ratio)

        # Expected: 100 * 0.0004 = 0.04 WETH = 40_000_000_000_000_000 wei (4e16)
        # = amount_in * price_ratio * 10^(18-6) = 100_000000 * 0.0004 * 10^12
        expected = int(Decimal("100000000") * price_ratio * Decimal("1000000000000"))
        assert result == expected
        # Sanity: ~0.04 WETH
        assert 3 * 10**16 < result < 5 * 10**16

    def test_cryptoswap_without_price_ratio_raises(self, adapter: CurveAdapter) -> None:
        """CryptoSwap without price_ratio raises ValueError (fail closed).

        Decimal-only adjustment for volatile pairs is mathematically wrong:
        USDT(6 dec)->WETH(18 dec) would produce min_amount_out = 100*10^12 wei
        (~100 billion WETH), guaranteeing a revert. Fail closed is safer.
        """
        import pytest

        pool_address = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        pool = adapter.get_pool_info(pool_address)
        assert pool is not None
        amount_in = 100_000000  # 100 USDT in 6 decimals
        with pytest.raises(ValueError, match="price_ratio is required"):
            adapter._estimate_swap_output(pool, 0, 2, amount_in)

    def test_cryptoswap_weth_to_usdt_with_price_ratio(self, adapter: CurveAdapter) -> None:
        """CryptoSwap reverse direction: WETH->USDT with price_ratio."""
        pool_address = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        pool = adapter.get_pool_info(pool_address)
        assert pool is not None
        # WETH (index 2, 18 decimals) -> USDT (index 0, 6 decimals)
        # 0.04 WETH at $2500, USDT at $1 -> price_ratio = 2500/1 = 2500
        amount_in = 40_000_000_000_000_000  # 0.04 WETH
        price_ratio = Decimal("2500")

        result = adapter._estimate_swap_output(pool, 2, 0, amount_in, price_ratio=price_ratio)

        # Expected: 0.04 WETH * 2500 = 100 USDT = 100_000000 in 6 decimals
        # = 40000000000000000 * 2500 / 10^12 = 100_000000
        assert 99_000000 < result < 101_000000

    def test_swap_with_price_ratio_produces_protected_min(self, adapter: CurveAdapter) -> None:
        """End-to-end: swap() with price_ratio gives meaningful min_amount_out."""
        pool_address = CURVE_POOLS["ethereum"]["tricrypto2"]["address"]
        price_ratio = Decimal("1") / Decimal("2500")

        result = adapter.swap(
            pool_address=pool_address,
            token_in="USDT",
            token_out="WETH",
            amount_in=Decimal("100"),
            slippage_bps=50,
            price_ratio=price_ratio,
        )

        assert result.success is True
        # Compute exact expected min_amount_out:
        # estimate = 100_000000 * (1/2500) * 10^12 = 4e16 (0.04 WETH in wei)
        # min = max(1, int(estimate * (10000 - 50) // 10000)) = int(4e16 * 9950 / 10000)
        amount_in_wei = 100_000000
        estimate = int(Decimal(amount_in_wei) * price_ratio * Decimal(10**12))
        expected_min = max(1, int(estimate * (10000 - 50) // 10000))
        assert result.amount_out_minimum == expected_min


# =============================================================================
# Data Class Tests
# =============================================================================


class TestDataClasses:
    """Tests for data class serialization."""

    def test_swap_result_to_dict(self) -> None:
        """Test SwapResult serialization."""
        result = SwapResult(
            success=True,
            pool_address="0x1234",
            amount_in=1000,
            amount_out_minimum=990,
        )
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["pool_address"] == "0x1234"
        assert result_dict["amount_in"] == "1000"

    def test_liquidity_result_to_dict(self) -> None:
        """Test LiquidityResult serialization."""
        result = LiquidityResult(
            success=True,
            pool_address="0x1234",
            operation="add_liquidity",
            amounts=[1000, 1000, 1000],
            lp_amount=3000,
        )
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["operation"] == "add_liquidity"
        assert result_dict["amounts"] == ["1000", "1000", "1000"]

    def test_transaction_data_to_dict(self) -> None:
        """Test TransactionData serialization."""
        tx = TransactionData(
            to="0x1234",
            value=0,
            data="0xabcd",
            gas_estimate=200000,
            description="Test swap",
            tx_type="swap",
        )
        tx_dict = tx.to_dict()

        assert tx_dict["to"] == "0x1234"
        assert tx_dict["value"] == "0"
        assert tx_dict["data"] == "0xabcd"

    def test_pool_info_to_dict(self) -> None:
        """Test PoolInfo serialization."""
        pool_info = PoolInfo(
            address="0x1234",
            lp_token="0x5678",
            coins=["DAI", "USDC"],
            coin_addresses=["0xaaa", "0xbbb"],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            name="test_pool",
        )
        pool_dict = pool_info.to_dict()

        assert pool_dict["address"] == "0x1234"
        assert pool_dict["pool_type"] == "stableswap"
        assert pool_dict["n_coins"] == 2
