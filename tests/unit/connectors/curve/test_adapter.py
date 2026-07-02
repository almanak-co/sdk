"""Tests for Curve Finance Adapter.

This module tests the CurveAdapter class functionality including:
- Configuration validation
- Swap transaction building
- Add liquidity (LP_OPEN) transaction building
- Remove liquidity (LP_CLOSE) transaction building
- Token resolution and pool info lookup
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.curve.adapter import (
    ADD_LIQUIDITY_3_SELECTOR,
    CURVE_ADDRESSES,
    CURVE_GAS_ESTIMATE_BUFFER,
    CURVE_GAS_ESTIMATES,
    CURVE_POOLS,
    EXCHANGE_SELECTOR,
    EXCHANGE_UNDERLYING_SELECTOR,
    REMOVE_LIQUIDITY_3_SELECTOR,
    REMOVE_LIQUIDITY_ONE_SELECTOR,
    ZAP_ADD_LIQUIDITY_4_SELECTOR,
    ZAP_REMOVE_LIQUIDITY_4_SELECTOR,
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
        """Test adapter resolves common tokens via the shared TokenResolver.

        Post-refactor: per-adapter ``self.tokens`` dicts were removed when
        connectors migrated to the shared ``TokenResolver``. The adapter
        no longer keeps a token map; it queries the resolver on demand.
        """
        # USDC must resolve to a fully-formed token on the configured chain.
        resolved = adapter._token_resolver.resolve("USDC", adapter.chain)
        assert resolved.symbol == "USDC"
        # resolved.chain may be a Chain enum or str depending on the resolver path;
        # compare the lowered string value.
        assert str(resolved.chain).lower().endswith(str(adapter.chain).lower())
        assert resolved.decimals == 6
        # Address must be a checksummable 20-byte hex string.
        assert resolved.address.startswith("0x")
        assert len(resolved.address) == 42


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
        # No cached/confirmed allowance → swap is last and every preceding tx is an
        # approve (no-transport adapter resets-to-be-safe before the approve).
        assert result.transactions[-1].tx_type == "swap"
        assert result.transactions[:-1] and all(t.tx_type == "approve" for t in result.transactions[:-1])

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
        # The swap is always included; up to a reset + approve may precede it.
        assert result.gas_estimate >= CURVE_GAS_ESTIMATES["exchange"]
        assert result.gas_estimate <= sum([2 * CURVE_GAS_ESTIMATES["approve"], CURVE_GAS_ESTIMATES["exchange"]])


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

    def test_remove_liquidity_fails_without_rpc_url(self, adapter: CurveAdapter) -> None:
        """remove_liquidity must fail closed when rpc_url is not configured.

        Without rpc_url, _estimate_remove_liquidity returns [0,...,0] (no slippage
        protection). Rather than warn-and-proceed (sandwich-extractable), the adapter
        must return a failure result so the caller can handle it explicitly.
        """
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity(
            pool_address=pool_address,
            lp_amount=Decimal("1000"),
        )

        assert result.success is False
        assert "cannot compute slippage protection" in result.error
        assert "rpc_url not configured" in result.error

    def test_remove_liquidity_success_with_rpc_url(self) -> None:
        """remove_liquidity succeeds when rpc_url is set and RPC responds."""
        from unittest.mock import MagicMock, patch

        total_supply = 100_000_000 * 10**18
        lp_amount = Decimal("1000")
        dai_balance = 10_000_000 * 10**18
        usdc_balance = 56_000_000 * 10**6
        usdt_balance = 37_000_000 * 10**6

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]

        def _hex(v: int) -> str:
            return "0x" + hex(v)[2:].zfill(64)

        def mock_resp(v: int) -> MagicMock:
            m = MagicMock()
            m.raise_for_status = MagicMock()
            m.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": _hex(v)}
            return m

        rpc_responses = [
            mock_resp(total_supply),
            mock_resp(dai_balance),
            mock_resp(usdc_balance),
            mock_resp(usdt_balance),
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            result = adapter.remove_liquidity(pool_address=pool_address, lp_amount=lp_amount)

        assert result.success is True
        assert result.operation == "remove_liquidity"
        assert len(result.amounts) == 3
        assert all(a > 0 for a in result.amounts)

    def test_remove_liquidity_calldata_format(self) -> None:
        """Test remove_liquidity calldata has correct format (requires rpc_url)."""
        from unittest.mock import MagicMock, patch

        total_supply = 100_000_000 * 10**18
        dai_balance = 10_000_000 * 10**18
        usdc_balance = 56_000_000 * 10**6
        usdt_balance = 37_000_000 * 10**6

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]

        def _hex(v: int) -> str:
            return "0x" + hex(v)[2:].zfill(64)

        def mock_resp(v: int) -> MagicMock:
            m = MagicMock()
            m.raise_for_status = MagicMock()
            m.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": _hex(v)}
            return m

        rpc_responses = [
            mock_resp(total_supply),
            mock_resp(dai_balance),
            mock_resp(usdc_balance),
            mock_resp(usdt_balance),
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            result = adapter.remove_liquidity(pool_address=pool_address, lp_amount=Decimal("1000"))

        assert result.success is True
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        assert remove_tx.data.startswith(REMOVE_LIQUIDITY_3_SELECTOR)

    @staticmethod
    def _rpc_adapter() -> CurveAdapter:
        """Adapter with an rpc_url so the on-chain calc_withdraw_one_coin read
        (mocked at eth_call_uint256) is reached instead of the fail-closed guard."""
        return CurveAdapter(
            CurveConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=50,
                rpc_url="http://localhost:8545",
            )
        )

    def test_remove_liquidity_one_coin(self) -> None:
        """Test remove_liquidity_one_coin transaction building."""
        adapter = self._rpc_adapter()
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        # min-out is now sourced from on-chain calc_withdraw_one_coin (VIB-5437);
        # mock a sane positive quote (USDC has 6 decimals).
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=1000 * 10**6):
            result = adapter.remove_liquidity_one_coin(
                pool_address=pool_address,
                lp_amount=Decimal("1000"),
                coin_index=1,  # USDC
            )

        assert result.success is True
        assert result.operation == "remove_liquidity_one_coin"
        # Floor must be non-zero and below the gross quote (slippage applied).
        assert 0 < result.amounts[1] < 1000 * 10**6

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

    def test_remove_liquidity_one_calldata_format(self) -> None:
        """Test remove_liquidity_one_coin calldata has correct format."""
        adapter = self._rpc_adapter()
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=1000 * 10**18):
            result = adapter.remove_liquidity_one_coin(
                pool_address=pool_address,
                lp_amount=Decimal("1000"),
                coin_index=0,
            )

        assert result.success is True
        remove_tx = next(tx for tx in result.transactions if tx.tx_type == "remove_liquidity")
        # 3pool is StableSwap → int128-form selector.
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


# =============================================================================
# Slippage Protection Tests: remove_liquidity (VIB-1510)
# =============================================================================


def _make_3pool_info() -> PoolInfo:
    """Build a PoolInfo for Ethereum 3pool for testing."""
    return PoolInfo(
        address=CURVE_POOLS["ethereum"]["3pool"]["address"],
        lp_token=CURVE_POOLS["ethereum"]["3pool"]["lp_token"],
        coins=CURVE_POOLS["ethereum"]["3pool"]["coins"],
        coin_addresses=CURVE_POOLS["ethereum"]["3pool"]["coin_addresses"],
        pool_type=PoolType.STABLESWAP,
        n_coins=3,
        name="3pool",
        virtual_price=Decimal("1.04"),
    )


def _make_mock_rpc_response(result_hex: str) -> MagicMock:
    """Build a mock httpx response returning result_hex."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    return mock_resp


def _hex_uint256(value: int) -> str:
    """Encode an integer as a 0x-prefixed 64-char hex string (uint256)."""
    return "0x" + hex(value)[2:].zfill(64)


class TestEstimateRemoveLiquiditySlippage:
    """Tests for _estimate_remove_liquidity on-chain slippage estimation (VIB-1510)."""

    def test_returns_zeros_when_no_rpc_url(self, adapter: CurveAdapter) -> None:
        """Without rpc_url, _estimate_remove_liquidity returns [0, 0, 0]."""
        pool_info = _make_3pool_info()
        result = adapter._estimate_remove_liquidity(pool_info, lp_amount=10**18)
        assert result == [0, 0, 0]

    def test_config_rpc_url_is_stored(self) -> None:
        """CurveConfig.rpc_url should be stored on adapter._rpc_url."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        assert adapter._rpc_url == "http://localhost:8545"

    def test_rpc_url_defaults_to_none(self) -> None:
        """CurveConfig.rpc_url should default to None."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.rpc_url is None

    def test_query_proportional_amounts_onchain(self) -> None:
        """_query_proportional_amounts_onchain returns proportional amounts from pool balances."""
        # Pool has 10M DAI + 56M USDC (6 dec) + 37M USDT (6 dec), total supply 100M 3Crv
        # Burning 1M 3Crv (1% of supply) should yield: 100k DAI, 560k USDC, 370k USDT
        total_supply = 100_000_000 * 10**18  # 100M LP tokens
        lp_amount = 1_000_000 * 10**18  # 1M LP tokens (1%)
        dai_balance = 10_000_000 * 10**18  # 10M DAI (18 dec)
        usdc_balance = 56_000_000 * 10**6  # 56M USDC (6 dec)
        usdt_balance = 37_000_000 * 10**6  # 37M USDT (6 dec)

        # Expected: proportional = balance * lp_amount / total_supply
        expected_dai = dai_balance * lp_amount // total_supply  # 100k DAI
        expected_usdc = usdc_balance * lp_amount // total_supply  # 560k USDC
        expected_usdt = usdt_balance * lp_amount // total_supply  # 370k USDT

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_info = _make_3pool_info()

        # Mock httpx.post to return: totalSupply, then balances(0), balances(1), balances(2)
        rpc_responses = [
            _make_mock_rpc_response(_hex_uint256(total_supply)),  # totalSupply()
            _make_mock_rpc_response(_hex_uint256(dai_balance)),  # balances(0)
            _make_mock_rpc_response(_hex_uint256(usdc_balance)),  # balances(1)
            _make_mock_rpc_response(_hex_uint256(usdt_balance)),  # balances(2)
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            amounts = adapter._query_proportional_amounts_onchain(pool_info, lp_amount)

        assert amounts == [expected_dai, expected_usdc, expected_usdt]
        assert amounts[0] == 100_000 * 10**18  # 100k DAI
        assert amounts[1] == 560_000 * 10**6  # 560k USDC
        assert amounts[2] == 370_000 * 10**6  # 370k USDT

    def test_estimate_remove_liquidity_with_rpc(self) -> None:
        """_estimate_remove_liquidity returns on-chain amounts when rpc_url configured."""
        total_supply = 100_000_000 * 10**18
        lp_amount = 1_000_000 * 10**18
        dai_balance = 10_000_000 * 10**18
        usdc_balance = 56_000_000 * 10**6
        usdt_balance = 37_000_000 * 10**6

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_info = _make_3pool_info()

        rpc_responses = [
            _make_mock_rpc_response(_hex_uint256(total_supply)),
            _make_mock_rpc_response(_hex_uint256(dai_balance)),
            _make_mock_rpc_response(_hex_uint256(usdc_balance)),
            _make_mock_rpc_response(_hex_uint256(usdt_balance)),
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            amounts = adapter._estimate_remove_liquidity(pool_info, lp_amount)

        # Should return non-zero proportional amounts
        assert len(amounts) == 3
        assert all(a > 0 for a in amounts)
        assert amounts[0] == 100_000 * 10**18  # DAI
        assert amounts[1] == 560_000 * 10**6  # USDC
        assert amounts[2] == 370_000 * 10**6  # USDT

    def test_estimate_remove_liquidity_rpc_failure_falls_back_to_zeros(self) -> None:
        """When RPC call fails, _estimate_remove_liquidity returns [0, 0, 0] with warning."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_info = _make_3pool_info()

        with patch("httpx.post", side_effect=Exception("Connection refused")):
            amounts = adapter._estimate_remove_liquidity(pool_info, lp_amount=10**18)

        assert amounts == [0, 0, 0]

    def test_remove_liquidity_with_rpc_produces_slippage_protected_amounts(self) -> None:
        """remove_liquidity with rpc_url produces non-zero slippage-protected min_amounts."""
        total_supply = 100_000_000 * 10**18
        lp_amount_dec = Decimal("1000000")  # 1M LP tokens
        lp_amount_wei = int(lp_amount_dec * 10**18)
        dai_balance = 10_000_000 * 10**18
        usdc_balance = 56_000_000 * 10**6
        usdt_balance = 37_000_000 * 10**6

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
            default_slippage_bps=50,  # 0.5%
        )
        adapter = CurveAdapter(config)
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]

        rpc_responses = [
            _make_mock_rpc_response(_hex_uint256(total_supply)),
            _make_mock_rpc_response(_hex_uint256(dai_balance)),
            _make_mock_rpc_response(_hex_uint256(usdc_balance)),
            _make_mock_rpc_response(_hex_uint256(usdt_balance)),
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            result = adapter.remove_liquidity(pool_address=pool_address, lp_amount=lp_amount_dec)

        assert result.success is True
        assert len(result.amounts) == 3
        # With slippage 0.5%, min_amounts = expected * 9950 / 10000
        assert result.amounts[0] > 0, "DAI min_amount should be positive"
        assert result.amounts[1] > 0, "USDC min_amount should be positive"
        assert result.amounts[2] > 0, "USDT min_amount should be positive"
        # Verify slippage was applied: amounts should be 99.5% of raw estimates
        expected_dai = dai_balance * lp_amount_wei // total_supply
        assert result.amounts[0] == expected_dai * 9950 // 10000

    def test_rpc_error_response_falls_back_to_zeros(self) -> None:
        """RPC error response causes fallback to [0, ..., 0]."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_info = _make_3pool_info()

        # totalSupply returns RPC error
        mock_error_resp = MagicMock()
        mock_error_resp.raise_for_status = MagicMock()
        mock_error_resp.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": "execution reverted"},
        }

        with patch("httpx.post", return_value=mock_error_resp):
            amounts = adapter._estimate_remove_liquidity(pool_info, lp_amount=10**18)

        assert amounts == [0, 0, 0]

    def test_zero_total_supply_raises(self) -> None:
        """_query_proportional_amounts_onchain raises when totalSupply is zero."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)
        pool_info = _make_3pool_info()

        # Return 0 for totalSupply
        with patch("httpx.post", return_value=_make_mock_rpc_response(_hex_uint256(0))):
            with pytest.raises(ValueError, match="totalSupply is zero"):
                adapter._query_proportional_amounts_onchain(pool_info, lp_amount=10**18)


# =============================================================================
# StableSwap NG calc_token_amount on-chain query (VIB-4836)
# =============================================================================


def _make_ng_2pool_info() -> PoolInfo:
    """Build a PoolInfo for the Optimism crvUSD/USDC StableSwap NG pool."""
    return PoolInfo(
        address="0x03771e24b7C9172d163Bf447490B142a15be3485",
        lp_token="0x03771e24b7C9172d163Bf447490B142a15be3485",
        coins=["crvUSD", "USDC"],
        coin_addresses=[
            "0xC52D7F23a2e460248Db6eE192Cb23dD12bDDCbf6",
            "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        ],
        pool_type=PoolType.STABLESWAP,
        n_coins=2,
        name="crvusd_usdc",
        virtual_price=Decimal("1.0"),
        is_ng=True,
    )


class TestQueryCalcTokenAmountNGOnchain:
    """Tests for _query_calc_token_amount_ng_onchain (VIB-4836).

    Routing mirrors the existing `_eth_call` helper used by
    `_estimate_remove_liquidity_proportional`: gateway-first when a
    `GatewayRpcClient` is wired, with an `rpc_url` httpx fallback for
    intent-test / ad-hoc adapter constructions that don't go through the
    gateway. Tests cover both paths plus the relevant error branches and
    the caller's gating + naive-estimator fallback.
    """

    def _build_adapter_with_gateway(self) -> tuple["CurveAdapter", MagicMock]:
        """Construct an adapter with a mocked gateway client attached."""
        config = CurveConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = CurveAdapter(config)
        adapter._gateway_client = MagicMock()
        return adapter, adapter._gateway_client

    def _build_adapter_with_rpc_only(self) -> "CurveAdapter":
        """Construct an adapter with rpc_url only (no gateway client)."""
        config = CurveConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        return CurveAdapter(config)

    def test_gateway_path_returns_int(self) -> None:
        """Gateway path: hex result string is decoded to int."""
        adapter, client = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()
        expected = 19_525_895_236_381_251_599  # mainnet quote at probe time

        gateway_resp = MagicMock()
        gateway_resp.success = True
        # The adapter calls _json.loads(response.result) so the gateway must
        # return a JSON-encoded string, mirroring how proto wraps RPC results.
        gateway_resp.result = f'"{_hex_uint256(expected)}"'
        client.rpc.Call.return_value = gateway_resp

        got = adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[10**18, 10**6])
        assert got == expected
        rpc_request = client.rpc.Call.call_args.args[0]
        assert rpc_request.chain == "optimism"
        assert rpc_request.method == "eth_call"

    def test_gateway_path_emits_correct_calldata(self) -> None:
        """Calldata is selector + offset(0x40) + is_deposit(1) + len + amounts."""
        adapter, client = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()

        gateway_resp = MagicMock()
        gateway_resp.success = True
        gateway_resp.result = f'"{_hex_uint256(1)}"'
        client.rpc.Call.return_value = gateway_resp

        adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[10 * 10**18, 10 * 10**6])

        rpc_request = client.rpc.Call.call_args.args[0]
        params = json.loads(rpc_request.params)
        to = params[0]["to"]
        data = params[0]["data"]
        assert to == pool_info.address
        # selector + 4 head words (offset, is_deposit, length, amount0, amount1)
        # = 10 + 5*64 = 330 hex chars
        assert data.startswith("0x3db06dd8")
        assert len(data) == 2 + 8 + 5 * 64
        assert data[10:74] == "0" * 62 + "40"  # offset to amounts array
        assert data[74:138] == "0" * 63 + "1"  # is_deposit
        assert data[138:202] == "0" * 63 + "2"  # array length
        assert int(data[202:266], 16) == 10 * 10**18  # amount0
        assert int(data[266:330], 16) == 10 * 10**6  # amount1

    def test_gateway_failure_raises(self) -> None:
        """Gateway returning success=False raises ValueError."""
        adapter, client = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()

        bad = MagicMock()
        bad.success = False
        bad.error = "node connection refused"
        client.rpc.Call.return_value = bad

        with pytest.raises(ValueError, match="calc_token_amount"):
            adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[1, 1])

    def test_gateway_empty_result_raises(self) -> None:
        """Gateway returning empty result raises ValueError."""
        adapter, client = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()

        empty = MagicMock()
        empty.success = True
        empty.result = ""  # falsy → _json.loads is skipped, default "0x"
        client.rpc.Call.return_value = empty

        with pytest.raises(ValueError, match="empty result"):
            adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[1, 1])

    def test_rpc_path_returns_int(self) -> None:
        """rpc_url fallback: hex result body is decoded to int."""
        adapter = self._build_adapter_with_rpc_only()
        pool_info = _make_ng_2pool_info()
        expected = 19_525_895_236_381_251_599

        with patch(
            "httpx.post",
            return_value=_make_mock_rpc_response(_hex_uint256(expected)),
        ):
            got = adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[10**18, 10**6])
        assert got == expected

    def test_rpc_path_emits_correct_calldata(self) -> None:
        """rpc_url fallback emits the same selector/head/tail calldata layout."""
        adapter = self._build_adapter_with_rpc_only()
        pool_info = _make_ng_2pool_info()

        captured: dict = {}

        def fake_post(url, json, timeout):
            captured["url"] = url
            captured["json"] = json
            return _make_mock_rpc_response(_hex_uint256(1))

        with patch("httpx.post", side_effect=fake_post):
            adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[10 * 10**18, 10 * 10**6])

        assert captured["url"] == "http://localhost:8545"
        params = captured["json"]["params"]
        data = params[0]["data"]
        assert params[0]["to"] == pool_info.address
        assert data.startswith("0x3db06dd8")
        assert len(data) == 2 + 8 + 5 * 64
        assert int(data[202:266], 16) == 10 * 10**18  # amount0
        assert int(data[266:330], 16) == 10 * 10**6  # amount1

    def test_rpc_error_response_raises(self) -> None:
        """rpc_url body containing `error` raises ValueError."""
        adapter = self._build_adapter_with_rpc_only()
        pool_info = _make_ng_2pool_info()

        bad_resp = MagicMock()
        bad_resp.raise_for_status = MagicMock()
        bad_resp.json.return_value = {"error": {"message": "execution reverted"}}

        with patch("httpx.post", return_value=bad_resp):
            with pytest.raises(ValueError, match="calc_token_amount"):
                adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[1, 1])

    def test_rpc_empty_result_raises(self) -> None:
        """rpc_url returning '0x' raises ValueError."""
        adapter = self._build_adapter_with_rpc_only()
        pool_info = _make_ng_2pool_info()

        with patch("httpx.post", return_value=_make_mock_rpc_response("0x")):
            with pytest.raises(ValueError, match="empty result"):
                adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[1, 1])

    def test_missing_gateway_and_rpc_raises(self) -> None:
        """Without gateway client AND without rpc_url, the helper raises."""
        config = CurveConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
            # no rpc_url, no gateway client
        )
        adapter = CurveAdapter(config)
        pool_info = _make_ng_2pool_info()

        with pytest.raises(AssertionError, match="gateway client or rpc_url"):
            adapter._query_calc_token_amount_ng_onchain(pool_info, amounts=[1, 1])

    def test_estimate_add_liquidity_uses_ng_query_with_gateway(self) -> None:
        """_estimate_add_liquidity for NG pools routes through the on-chain helper."""
        adapter, _ = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()
        expected = 19_525_895_236_381_251_599

        with patch.object(
            adapter,
            "_query_calc_token_amount_ng_onchain",
            return_value=expected,
        ) as mocked:
            got = adapter._estimate_add_liquidity(pool_info, amounts=[10 * 10**18, 10 * 10**6])

        assert got == expected
        mocked.assert_called_once()

    def test_estimate_add_liquidity_uses_ng_query_with_rpc_only(self) -> None:
        """rpc_url alone is sufficient to trigger the NG on-chain quote.

        Intent tests construct adapters with `rpc_url` and no gateway client;
        without this gating, NG pools would fall back to the naive estimator
        and revert with "Slippage screwed you" (VIB-4836)."""
        adapter = self._build_adapter_with_rpc_only()
        pool_info = _make_ng_2pool_info()
        expected = 19_525_895_236_381_251_599

        with patch.object(
            adapter,
            "_query_calc_token_amount_ng_onchain",
            return_value=expected,
        ) as mocked:
            got = adapter._estimate_add_liquidity(pool_info, amounts=[10 * 10**18, 10 * 10**6])

        assert got == expected
        mocked.assert_called_once()

    def test_estimate_add_liquidity_falls_back_when_ng_query_raises(self) -> None:
        """If the on-chain NG query fails, fall back to the naive estimator."""
        adapter, _ = self._build_adapter_with_gateway()
        pool_info = _make_ng_2pool_info()

        # Mock the token decimals lookup to avoid token registry calls.
        with patch.object(adapter, "_get_token_decimals", side_effect=[18, 6]):
            with patch.object(
                adapter,
                "_query_calc_token_amount_ng_onchain",
                side_effect=ValueError("RPC down"),
            ):
                got = adapter._estimate_add_liquidity(pool_info, amounts=[10 * 10**18, 10 * 10**6])

        # Naive: (10e18 + 10e6 * 10^12) / 1.0 = 20e18
        assert got == 20 * 10**18

    def test_estimate_add_liquidity_skips_ng_query_without_rpc_or_gateway(self) -> None:
        """Without gateway client and without rpc_url, the NG on-chain query
        is never invoked (the caller falls back to the naive estimator)."""
        config = CurveConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
            # no rpc_url, no gateway_client
        )
        adapter = CurveAdapter(config)
        pool_info = _make_ng_2pool_info()

        with patch.object(adapter, "_get_token_decimals", side_effect=[18, 6]):
            with patch.object(
                adapter,
                "_query_calc_token_amount_ng_onchain",
            ) as mocked:
                adapter._estimate_add_liquidity(pool_info, amounts=[10 * 10**18, 10 * 10**6])
        mocked.assert_not_called()


# =============================================================================
# Metapool Tier B tests (VIB-5419)
# =============================================================================

# FRAX/3CRV factory metapool. Native coins: FRAX(0), 3CRV(1). Combined coin
# space: FRAX(0), DAI(1), USDC(2), USDT(3). The metapool IS its own LP token.
_META = "frax_3crv"
_META_ADDR = "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B"
_ZAP_ADDR = "0xA79828DF1850E8a3A3064576f380D90aECDD3359"
_DAI_ADDR = "0x6B175474E89094C44Da98b954EedeAC495271d0F"


def _word(calldata: str, index: int) -> int:
    """Decode the ``index``-th 32-byte word of calldata (after the 4-byte selector)."""
    body = calldata[10:]  # strip "0x" + 8 hex selector chars
    start = index * 64
    return int(body[start : start + 64], 16)


class TestUnderlyingCoinIndex:
    """PoolInfo.underlying_coin_index + _match_coin (metapool combined space)."""

    def test_meta_coin_index_zero(self, adapter: CurveAdapter) -> None:
        pool = adapter.get_pool_by_name(_META)
        assert pool is not None
        assert pool.underlying_coin_index("FRAX") == 0
        # also resolvable by the meta coin's address
        assert pool.underlying_coin_index(pool.coin_addresses[0]) == 0

    def test_base_coin_indices(self, adapter: CurveAdapter) -> None:
        pool = adapter.get_pool_by_name(_META)
        assert pool is not None
        assert pool.underlying_coin_index("DAI") == 1
        assert pool.underlying_coin_index("USDC") == 2
        assert pool.underlying_coin_index("USDT") == 3
        # by address (DAI is combined index 1)
        assert pool.underlying_coin_index(_DAI_ADDR) == 1

    def test_base_lp_token_excluded(self, adapter: CurveAdapter) -> None:
        """3CRV is the native coins[1] base-LP token, NOT a combined-space coin."""
        pool = adapter.get_pool_by_name(_META)
        assert pool is not None
        assert pool.underlying_coin_index("3CRV") is None

    def test_absent_coin_returns_none(self, adapter: CurveAdapter) -> None:
        pool = adapter.get_pool_by_name(_META)
        assert pool is not None
        assert pool.underlying_coin_index("WETH") is None

    def test_non_metapool_returns_none(self, adapter: CurveAdapter) -> None:
        pool = adapter.get_pool_by_name("3pool")
        assert pool is not None
        assert pool.underlying_coin_index("DAI") is None


class TestSwapUnderlying:
    """CurveAdapter.swap_underlying (metapool exchange_underlying)."""

    def test_swap_underlying_success(self, adapter: CurveAdapter) -> None:
        adapter.clear_allowance_cache()
        result = adapter.swap_underlying(
            pool_address=_META_ADDR,
            token_in="FRAX",
            token_out="USDT",
            amount_in=Decimal("50"),
        )
        assert result.success is True, result.error
        swap_tx = result.transactions[-1]
        assert swap_tx.tx_type == "swap"
        assert result.transactions[:-1] and all(t.tx_type == "approve" for t in result.transactions[:-1])
        assert swap_tx.to == _META_ADDR
        assert swap_tx.data.startswith(EXCHANGE_UNDERLYING_SELECTOR)
        # combined indices: i=0 (FRAX), j=3 (USDT)
        assert _word(swap_tx.data, 0) == 0
        assert _word(swap_tx.data, 1) == 3

    def test_swap_underlying_non_metapool_error(self, adapter: CurveAdapter) -> None:
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.swap_underlying(
            pool_address=pool_address, token_in="DAI", token_out="USDC", amount_in=Decimal("10")
        )
        assert result.success is False
        assert "requires a metapool" in (result.error or "")

    def test_swap_underlying_coin_not_on_combined_space(self, adapter: CurveAdapter) -> None:
        result = adapter.swap_underlying(
            pool_address=_META_ADDR, token_in="FRAX", token_out="WETH", amount_in=Decimal("10")
        )
        assert result.success is False
        assert "not on metapool" in (result.error or "")

    def test_swap_underlying_same_coin_error(self, adapter: CurveAdapter) -> None:
        result = adapter.swap_underlying(
            pool_address=_META_ADDR, token_in="DAI", token_out="DAI", amount_in=Decimal("10")
        )
        assert result.success is False
        assert "same coin" in (result.error or "")

    def test_swap_underlying_estimate_offline(self, adapter: CurveAdapter) -> None:
        """No rpc/gateway -> 1:1 decimal-adjusted stable estimate (FRAX 18 -> USDT 6)."""
        adapter.clear_allowance_cache()
        result = adapter.swap_underlying(
            pool_address=_META_ADDR, token_in="FRAX", token_out="USDT", amount_in=Decimal("50")
        )
        assert result.success is True
        amount_in_wei = int(Decimal("50") * Decimal(10**18))
        # 18 -> 6 decimals: divide by 10**12
        assert result.amount_out_estimate == amount_in_wei // (10**12)
        assert result.amount_out_minimum > 0


class TestAddLiquidityUnderlying:
    """CurveAdapter.add_liquidity_underlying (generic zap)."""

    def test_add_liquidity_underlying_success(self, adapter: CurveAdapter) -> None:
        adapter.clear_allowance_cache()
        result = adapter.add_liquidity_underlying(
            pool_address=_META_ADDR,
            underlying_amounts=[Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0")],
        )
        assert result.success is True, result.error
        assert result.operation == "add_liquidity_underlying"
        zap_txs = [tx for tx in result.transactions if tx.to == _ZAP_ADDR]
        assert len(zap_txs) == 1
        zap_tx = zap_txs[0]
        assert zap_tx.data.startswith(ZAP_ADD_LIQUIDITY_4_SELECTOR)
        # first arg is the POOL address (generic zap signature)
        assert _word(zap_tx.data, 0) == int(_META_ADDR, 16)

    def test_add_liquidity_underlying_wrong_length(self, adapter: CurveAdapter) -> None:
        result = adapter.add_liquidity_underlying(
            pool_address=_META_ADDR,
            underlying_amounts=[Decimal("100"), Decimal("100"), Decimal("0")],  # 3 != combined 4
        )
        assert result.success is False
        assert "combined coin space has 4" in (result.error or "")

    def test_add_liquidity_underlying_non_metapool(self, adapter: CurveAdapter) -> None:
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.add_liquidity_underlying(
            pool_address=pool_address, underlying_amounts=[Decimal("1"), Decimal("1")]
        )
        assert result.success is False
        assert "is not a metapool" in (result.error or "")

    def test_add_liquidity_underlying_min_lp_offline(self, adapter: CurveAdapter) -> None:
        """No rpc -> sum/virtual_price fallback yields a positive min-LP estimate."""
        adapter.clear_allowance_cache()
        result = adapter.add_liquidity_underlying(
            pool_address=_META_ADDR,
            underlying_amounts=[Decimal("100"), Decimal("100"), Decimal("0"), Decimal("0")],
        )
        assert result.success is True
        assert result.lp_amount > 0


class TestRemoveLiquidityUnderlying:
    """CurveAdapter.remove_liquidity_underlying (generic zap)."""

    def test_remove_liquidity_underlying_fail_closed_no_rpc(self, adapter: CurveAdapter) -> None:
        """No rpc/gateway -> all-zero min_amounts -> fail closed."""
        result = adapter.remove_liquidity_underlying(pool_address=_META_ADDR, lp_amount=Decimal("1000"))
        assert result.success is False
        assert "cannot compute slippage protection" in (result.error or "")
        assert "not configured" in (result.error or "")

    def test_remove_liquidity_underlying_non_metapool(self, adapter: CurveAdapter) -> None:
        pool_address = CURVE_POOLS["ethereum"]["3pool"]["address"]
        result = adapter.remove_liquidity_underlying(pool_address=pool_address, lp_amount=Decimal("1000"))
        assert result.success is False
        assert "is not a metapool" in (result.error or "")

    def test_remove_liquidity_underlying_success_with_rpc(self) -> None:
        """rpc-configured: metapool native split + base-pool decomposition -> 4 combined amounts.

        Read order in _estimate_remove_liquidity_underlying:
          1. _query_proportional_amounts_onchain(metapool): totalSupply, balances(0)=FRAX, balances(1)=3CRV
          2. _query_base_pool_underlying_amounts -> _query_proportional_amounts_onchain(3pool):
             totalSupply, balances(0..2) = DAI/USDC/USDT
        = 7 mocked eth_calls in order.
        """
        from unittest.mock import MagicMock, patch

        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            rpc_url="http://localhost:8545",
        )
        adapter = CurveAdapter(config)

        meta_total_supply = 575_000 * 10**18
        meta_frax_balance = 531_000 * 10**18
        meta_3crv_balance = 53_000 * 10**18
        base_total_supply = 100_000_000 * 10**18
        base_dai = 10_000_000 * 10**18
        base_usdc = 56_000_000 * 10**6
        base_usdt = 37_000_000 * 10**6

        def _hex(v: int) -> str:
            return "0x" + hex(v)[2:].zfill(64)

        def mock_resp(v: int) -> MagicMock:
            m = MagicMock()
            m.raise_for_status = MagicMock()
            m.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": _hex(v)}
            return m

        rpc_responses = [
            mock_resp(meta_total_supply),
            mock_resp(meta_frax_balance),
            mock_resp(meta_3crv_balance),
            mock_resp(base_total_supply),
            mock_resp(base_dai),
            mock_resp(base_usdc),
            mock_resp(base_usdt),
        ]

        with patch("httpx.post", side_effect=rpc_responses):
            result = adapter.remove_liquidity_underlying(pool_address=_META_ADDR, lp_amount=Decimal("1000"))

        assert result.success is True, result.error
        assert result.operation == "remove_liquidity_underlying"
        zap_txs = [tx for tx in result.transactions if tx.to == _ZAP_ADDR]
        assert len(zap_txs) == 1
        assert zap_txs[0].data.startswith(ZAP_REMOVE_LIQUIDITY_4_SELECTOR)
        # first arg is the POOL address
        assert _word(zap_txs[0].data, 0) == int(_META_ADDR, 16)
        # combined min-amounts vector: [meta, DAI, USDC, USDT] all > 0
        assert len(result.amounts) == 4
        assert all(a > 0 for a in result.amounts)


# =============================================================================
# Gas estimation — live eth_estimateGas × buffer, conservative static floor (VIB-5440)
# =============================================================================


def _connected_gateway(estimate: int | None) -> MagicMock:
    """A mock GatewayClient that reports connected and returns ``estimate`` gas."""
    gw = MagicMock()
    gw.is_connected = True
    gw.estimate_gas.return_value = estimate
    return gw


class TestResolveGas:
    """``CurveAdapter._resolve_gas`` — the live-estimate + buffer + static-floor seam."""

    def _adapter_with_gateway(self, estimate: int | None) -> CurveAdapter:
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            gateway_client=_connected_gateway(estimate),
        )
        return CurveAdapter(config)

    def test_live_estimate_buffered_when_above_floor(self) -> None:
        """A live estimate whose buffered value exceeds the floor governs the limit."""
        adapter = self._adapter_with_gateway(1_000_000)
        static_floor = 500_000
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=static_floor)
        assert gas == round(1_000_000 * CURVE_GAS_ESTIMATE_BUFFER)
        assert gas > static_floor

    def test_buffer_rounds_not_truncates(self) -> None:
        """The buffered estimate is ROUNDED, not truncated (int() would 1-gas
        under-estimate a fractional product, e.g. 500_003 * 1.20 = 600003.6)."""
        adapter = self._adapter_with_gateway(500_003)
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=300_000)
        assert gas == round(500_003 * CURVE_GAS_ESTIMATE_BUFFER) == 600_004
        assert gas == 600_004 and int(500_003 * CURVE_GAS_ESTIMATE_BUFFER) == 600_003

    def test_estimate_clamped_up_to_static_floor(self) -> None:
        """A low estimate may only RAISE the floor, never lower it below the static."""
        adapter = self._adapter_with_gateway(100_000)
        static_floor = 500_000
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=static_floor)
        # buffered 120_000 < 500_000 floor -> floor wins.
        assert gas == static_floor

    def test_unavailable_estimate_falls_back_to_static(self) -> None:
        """Empty≠Zero: a None estimate (revert / miss) falls back to the static floor, not 0."""
        adapter = self._adapter_with_gateway(None)
        static_floor = 450_000
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=static_floor)
        assert gas == static_floor

    def test_zero_estimate_falls_back_to_static(self) -> None:
        """A non-positive estimate is treated as unmeasured -> static floor."""
        adapter = self._adapter_with_gateway(0)
        static_floor = 350_000
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=static_floor)
        assert gas == static_floor

    def test_no_transport_returns_static(self) -> None:
        """With no gateway and no rpc_url, the static floor is used unchanged."""
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = CurveAdapter(config)
        assert adapter._has_read_transport() is False
        gas = adapter._resolve_gas(to="0xpool", data="0xdata", value=0, static_gas=600_000)
        assert gas == 600_000

    def test_estimate_passes_wallet_and_value(self) -> None:
        """The estimate is issued from the execution wallet and carries the native value."""
        gw = _connected_gateway(700_000)
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            gateway_client=gw,
        )
        adapter = CurveAdapter(config)
        adapter._resolve_gas(to="0xpool", data="0xdata", value=10**18, static_gas=250_000)
        gw.estimate_gas.assert_called_once_with(
            "ethereum",
            "0xpool",
            "0xdata",
            from_address="0x1234567890123456789012345678901234567890",
            value=10**18,
        )


class TestStaticGasFloors:
    """The raised conservative static floors under-sized 4-coin / aave paths before (VIB-5440)."""

    def test_remove_liquidity_floor_scales_with_coin_count(self) -> None:
        assert (
            CURVE_GAS_ESTIMATES["remove_liquidity_2"]
            <= CURVE_GAS_ESTIMATES["remove_liquidity_3"]
            < CURVE_GAS_ESTIMATES["remove_liquidity_4"]
        )

    def test_aave_exchange_underlying_not_below_plain_exchange(self) -> None:
        # Aave-type exchange_underlying wraps/unwraps aTokens -> must be >= plain exchange.
        assert CURVE_GAS_ESTIMATES["exchange_underlying"] >= CURVE_GAS_ESTIMATES["exchange"]

    def test_four_coin_add_floor_raised(self) -> None:
        assert CURVE_GAS_ESTIMATES["add_liquidity_4"] >= 600_000
