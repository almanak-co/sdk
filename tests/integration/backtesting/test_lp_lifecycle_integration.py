"""Integration tests for LP position lifecycle with Anvil fork.

These tests validate the complete LP lifecycle:
- Open LP position → Fee accrual → IL calculation → Close position

Requirements:
    - Anvil running with Arbitrum mainnet fork on port 8546
    - ALCHEMY_API_KEY for price data (optional, uses fallback)

To run:
    1. Start Anvil with Arbitrum mainnet fork:
       anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --chain-id 42161 --port 8546

    2. Run tests:
       uv run pytest tests/integration/backtesting/test_lp_lifecycle_integration.py -v -s
"""

import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest


def _require_cast() -> None:
    """Skip test if Foundry 'cast' CLI is not available."""
    if shutil.which("cast") is None:
        pytest.skip("Foundry 'cast' CLI not installed; required for Anvil funding helpers.")
from web3 import Web3

from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
    RangeStatus,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)

# =============================================================================
# Constants
# =============================================================================

# Default test wallet (Anvil's first account)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Anvil RPC for Paper Trader (using port 8546 per CLAUDE.md guidance)
ANVIL_RPC = "http://localhost:8546"

# Token addresses (Arbitrum mainnet)
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC on Arbitrum

# Minimal ERC20 ABI for balance checks
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


# =============================================================================
# Helper Functions
# =============================================================================


def is_anvil_running(rpc_url: str = ANVIL_RPC) -> bool:
    """Check if Anvil is running and responding."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
        return web3.is_connected()
    except Exception:
        return False


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str = ANVIL_RPC) -> None:
    """Fund a wallet with ETH using cast."""
    _require_cast()
    amount_hex = hex(amount_wei)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


def fund_erc20_token(
    wallet: str,
    token_address: str,
    amount_wei: int,
    rpc_url: str = ANVIL_RPC,
    decimals: int = 6,
) -> None:
    """Fund a wallet with ERC20 tokens using cast index.

    Uses the cast index technique to find storage slots and set balances directly.
    """
    _require_cast()
    slot_mappings = {
        USDC_ADDRESS.lower(): "0x33",  # USDC storage slot on Arbitrum
        WETH_ADDRESS.lower(): "0x3",  # WETH storage slot on Arbitrum
    }

    slot_base = slot_mappings.get(token_address.lower())
    if not slot_base:
        slot_base = "0x0"

    result = subprocess.run(
        ["cast", "index", "address", wallet, slot_base],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    subprocess.run(
        [
            "cast",
            "rpc",
            "anvil_setStorageAt",
            token_address,
            storage_slot,
            f"0x{amount_wei:064x}",
            "--rpc-url",
            rpc_url,
        ],
        capture_output=True,
        check=True,
    )


def get_token_balance(web3: Web3, token_address: str, wallet: str) -> int:
    """Get ERC20 token balance for a wallet."""
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()


# =============================================================================
# Mock Classes for LP Adapter Testing
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state with timestamp for LP lifecycle testing."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    available_tokens: set[str] = field(default_factory=set)

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        if token not in self.prices:
            raise KeyError(f"Price not found for {token}")
        return self.prices.get(token)


@dataclass
class MockPortfolio:
    """Mock portfolio for LP lifecycle testing."""

    cash_balance: Decimal = Decimal("100000")
    positions: list[SimulatedPosition] = field(default_factory=list)


def create_lp_position(
    token0: str = "WETH",
    token1: str = "USDC",
    tick_lower: int = -887272,
    tick_upper: int = 887272,
    entry_price: Decimal = Decimal("2000"),
    liquidity: Decimal = Decimal("4000"),
    fee_tier: Decimal = Decimal("0.003"),
    amounts: dict[str, Decimal] | None = None,
) -> SimulatedPosition:
    """Create a mock LP position for testing."""
    if amounts is None:
        amounts = {token0: Decimal("1"), token1: Decimal("2000")}

    position = SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=[token0, token1],
        amounts=amounts,
        entry_price=entry_price,
        entry_time=datetime.now(UTC),
        tick_lower=tick_lower,
        tick_upper=tick_upper,
        liquidity=liquidity,
        fee_tier=fee_tier,
    )
    # Store entry amounts in metadata for close calculations
    position.metadata["entry_amounts"] = {k: str(v) for k, v in amounts.items()}
    return position


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def web3() -> Web3:
    """Get Web3 instance connected to Anvil.

    Skips tests if Anvil is not running or not on Arbitrum fork.
    """
    if not is_anvil_running(ANVIL_RPC):
        pytest.skip(
            "Anvil is not running. Start with: "
            "anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY "
            "--chain-id 42161 --port 8546"
        )

    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))

    chain_id = w3.eth.chain_id
    if chain_id != 42161:
        pytest.skip(
            f"Anvil must be forked from Arbitrum mainnet (chain ID 42161). "
            f"Current chain ID: {chain_id}. Start with: "
            "anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY "
            "--chain-id 42161 --port 8546"
        )

    return w3


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3) -> str:
    """Fund the test wallet with ETH and USDC.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH for gas and testing
    eth_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, ANVIL_RPC)

    # Verify ETH funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= eth_amount, f"Wallet not funded with ETH: {balance}"

    # Fund with 10,000 USDC (6 decimals)
    usdc_amount = 10000 * 10**6
    fund_erc20_token(TEST_WALLET, USDC_ADDRESS, usdc_amount, ANVIL_RPC, decimals=6)

    # Verify USDC funding
    usdc_balance = get_token_balance(web3, USDC_ADDRESS, TEST_WALLET)
    assert usdc_balance >= usdc_amount, f"Wallet not funded with USDC: {usdc_balance}"

    return TEST_WALLET


@pytest.fixture
def lp_adapter() -> LPBacktestAdapter:
    """Create LP adapter with fee tracking enabled."""
    config = LPBacktestConfig(
        strategy_type="lp",
        fee_tracking_enabled=True,
        il_calculation_method="standard",
        rebalance_on_out_of_range=True,
        volume_multiplier=Decimal("10"),
        base_liquidity=Decimal("1000000"),
    )
    return LPBacktestAdapter(config)


# =============================================================================
# Integration Tests - LP Lifecycle
# =============================================================================


class TestLPLifecycleIntegration:
    """Integration tests for complete LP position lifecycle.

    Tests the full flow: Open → Fee accrual → IL calculation → Close
    """

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_lp_lifecycle_open_to_close(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test complete LP lifecycle: open → fee accrual → IL calculation → close."""
        from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

        # Step 1: Open LP position
        open_intent = LPOpenIntent(
            pool="WETH/USDC",
            amount0=Decimal("1"),  # 1 WETH
            amount1=Decimal("2000"),  # 2000 USDC
            range_lower=Decimal("0.5"),  # Wide range
            range_upper=Decimal("2.0"),
            protocol="uniswap_v3",
        )

        initial_market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )
        portfolio = MockPortfolio()

        open_fill = lp_adapter.execute_intent(open_intent, portfolio, initial_market)

        assert open_fill is not None
        assert open_fill.success is True
        assert open_fill.position_delta is not None
        assert open_fill.position_delta.is_lp is True
        assert open_fill.tokens == ["WETH", "USDC"]
        assert open_fill.amount_usd == Decimal("4000")  # 1*$2000 + 2000*$1

        # Create the position from the fill
        position = create_lp_position(
            token0="WETH",
            token1="USDC",
            tick_lower=open_fill.position_delta.tick_lower,
            tick_upper=open_fill.position_delta.tick_upper,
            entry_price=Decimal("2000"),
            liquidity=open_fill.position_delta.liquidity,
            amounts={"WETH": Decimal("1"), "USDC": Decimal("2000")},
        )

        # Step 2: Simulate time passage and fee accrual
        elapsed_seconds = 86400 * 7  # 7 days
        initial_fees = position.accumulated_fees_usd

        mid_market = MockMarketState(
            prices={"WETH": Decimal("2100"), "USDC": Decimal("1")},  # 5% price increase
            timestamp=datetime.now(UTC) + timedelta(days=7),
        )

        lp_adapter.update_position(position, mid_market, elapsed_seconds=elapsed_seconds)

        # Verify fee accrual is non-zero
        assert position.accumulated_fees_usd > initial_fees
        assert position.accumulated_fees_usd > Decimal("0")

        # Step 3: Close LP position
        portfolio.positions = [position]

        close_intent = LPCloseIntent(
            position_id=position.position_id,
            pool="WETH/USDC",
            collect_fees=True,
            protocol="uniswap_v3",
        )

        final_market = MockMarketState(
            prices={"WETH": Decimal("2200"), "USDC": Decimal("1")},  # 10% total increase
            timestamp=datetime.now(UTC) + timedelta(days=14),
        )

        close_fill = lp_adapter.execute_intent(close_intent, portfolio, final_market)

        assert close_fill is not None
        assert close_fill.success is True
        assert close_fill.position_close_id == position.position_id

        # Verify position close returns expected tokens
        assert "WETH" in close_fill.tokens_in
        assert "USDC" in close_fill.tokens_in
        assert close_fill.tokens_in["WETH"] > Decimal("0")
        assert close_fill.tokens_in["USDC"] > Decimal("0")

        # Verify metadata contains expected LP metrics
        assert "il_percentage" in close_fill.metadata
        assert "il_loss_usd" in close_fill.metadata
        assert "fees_earned_usd" in close_fill.metadata
        assert "net_lp_pnl_usd" in close_fill.metadata

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_fee_accrual_is_non_zero(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that LP fee accrual produces non-zero fees over time."""
        position = create_lp_position()
        initial_fees = position.accumulated_fees_usd

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        # Simulate 30 days of fee accrual
        lp_adapter.update_position(position, market, elapsed_seconds=86400 * 30)

        assert position.accumulated_fees_usd > initial_fees
        assert position.accumulated_fees_usd > Decimal("0")
        # Reasonable fee range for 30 days on a $4000 position
        assert position.accumulated_fees_usd < Decimal("1000")  # Upper bound sanity check

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_il_calculation_is_reasonable(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that IL calculation produces reasonable values."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        # Create position at entry price 2000
        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        close_intent = LPCloseIntent(
            position_id=position.position_id,
            protocol="uniswap_v3",
        )

        # Scenario 1: 50% price increase (WETH from $2000 to $3000)
        market_up = MockMarketState(
            prices={"WETH": Decimal("3000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill_up = lp_adapter.execute_intent(close_intent, portfolio, market_up)
        il_pct_up = Decimal(fill_up.metadata["il_percentage"])

        # IL should be non-zero and reasonable for 50% price change
        # Expected IL for 50% increase ≈ 2% for full range position
        assert il_pct_up > Decimal("0")
        assert il_pct_up < Decimal("50")  # IL shouldn't exceed 50% for typical price moves

        # Scenario 2: 50% price decrease (WETH from $2000 to $1000)
        portfolio.positions = [create_lp_position(entry_price=Decimal("2000"), liquidity=Decimal("4000"))]
        position_down = portfolio.positions[0]

        close_intent_down = LPCloseIntent(
            position_id=position_down.position_id,
            protocol="uniswap_v3",
        )

        market_down = MockMarketState(
            prices={"WETH": Decimal("1000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill_down = lp_adapter.execute_intent(close_intent_down, portfolio, market_down)
        il_pct_down = Decimal(fill_down.metadata["il_percentage"])

        # IL should be non-zero for price decrease
        assert il_pct_down > Decimal("0")
        assert il_pct_down < Decimal("50")

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_position_close_returns_expected_tokens(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that closing LP position returns both tokens."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        position = create_lp_position(
            amounts={"WETH": Decimal("1"), "USDC": Decimal("2000")},
        )
        position.accumulated_fees_usd = Decimal("50")  # Some fees

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        close_intent = LPCloseIntent(
            position_id=position.position_id,
            pool="WETH/USDC",
            collect_fees=True,
            protocol="uniswap_v3",
        )

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill = lp_adapter.execute_intent(close_intent, portfolio, market)

        assert fill is not None
        assert fill.success is True

        # Both tokens should be returned
        assert "WETH" in fill.tokens_in
        assert "USDC" in fill.tokens_in
        assert fill.tokens_in["WETH"] > Decimal("0")
        assert fill.tokens_in["USDC"] > Decimal("0")

        # Verify fees were collected
        assert fill.metadata.get("fees_earned_usd") == "50"
        assert fill.metadata.get("collect_fees") is True

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_range_status_tracking(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that range status is correctly tracked throughout lifecycle."""
        # Create narrow range position around price ratio 1
        position = create_lp_position(
            tick_lower=-1000,  # price ≈ 0.905
            tick_upper=1000,  # price ≈ 1.105
        )

        # Test in-range
        in_range_market = MockMarketState(
            prices={"WETH": Decimal("1"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )
        result_in = lp_adapter.get_range_status(position, in_range_market)
        assert result_in is not None
        assert result_in.status == RangeStatus.IN_RANGE
        assert not result_in.is_out_of_range

        # Test below range
        below_range_market = MockMarketState(
            prices={"WETH": Decimal("0.5"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )
        result_below = lp_adapter.get_range_status(position, below_range_market)
        assert result_below is not None
        assert result_below.status == RangeStatus.BELOW_RANGE
        assert result_below.is_out_of_range

        # Test above range
        above_range_market = MockMarketState(
            prices={"WETH": Decimal("2"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )
        result_above = lp_adapter.get_range_status(position, above_range_market)
        assert result_above is not None
        assert result_above.status == RangeStatus.ABOVE_RANGE
        assert result_above.is_out_of_range

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_fee_tracking_disabled(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that fees are not tracked when disabled."""
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=False,
        )
        adapter = LPBacktestAdapter(config)

        position = create_lp_position()
        initial_fees = position.accumulated_fees_usd

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        adapter.update_position(position, market, elapsed_seconds=86400 * 7)

        # Fees should not have changed when tracking is disabled
        assert position.accumulated_fees_usd == initial_fees

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_lp_with_price_stable_no_il(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that stable prices result in minimal IL but positive fees."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        position = create_lp_position(
            entry_price=Decimal("2000"),
            liquidity=Decimal("4000"),
        )

        # Accrue fees over 30 days
        stable_market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )
        lp_adapter.update_position(position, stable_market, elapsed_seconds=86400 * 30)

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        close_intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=True,
            protocol="uniswap_v3",
        )

        fill = lp_adapter.execute_intent(close_intent, portfolio, stable_market)

        assert fill is not None
        assert fill.success is True

        # With stable prices, IL should be zero or very small
        il_pct = Decimal(fill.metadata["il_percentage"])
        assert il_pct < Decimal("0.1")  # Less than 0.1%

        # But fees should still be positive
        fees = Decimal(fill.metadata["fees_earned_usd"])
        assert fees > Decimal("0")

        # Net PnL should be positive (fees earned, no IL)
        net_pnl = Decimal(fill.metadata["net_lp_pnl_usd"])
        assert net_pnl > Decimal("0")


# =============================================================================
# Integration Tests - Multiple Positions
# =============================================================================


class TestMultipleLPPositions:
    """Integration tests for managing multiple LP positions."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_multiple_positions_independent_fee_accrual(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that multiple positions accrue fees independently."""
        # Create two positions with different parameters
        position1 = create_lp_position(
            tick_lower=-887272,
            tick_upper=887272,
            liquidity=Decimal("4000"),
        )

        position2 = create_lp_position(
            tick_lower=-1000,
            tick_upper=1000,
            liquidity=Decimal("8000"),  # Higher liquidity
        )

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        # Update both positions
        lp_adapter.update_position(position1, market, elapsed_seconds=86400 * 7)
        lp_adapter.update_position(position2, market, elapsed_seconds=86400 * 7)

        # Both should have accrued fees
        assert position1.accumulated_fees_usd > Decimal("0")
        assert position2.accumulated_fees_usd > Decimal("0")

        # Higher liquidity position may accrue different fees
        # (depends on fee model and range)
        assert position1.accumulated_fees_usd != position2.accumulated_fees_usd

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_close_specific_position(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test closing a specific position from multiple positions."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        position1 = create_lp_position()
        position2 = create_lp_position()

        portfolio = MockPortfolio()
        portfolio.positions = [position1, position2]

        # Close only position1
        close_intent = LPCloseIntent(
            position_id=position1.position_id,
            protocol="uniswap_v3",
        )

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill = lp_adapter.execute_intent(close_intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        assert fill.position_close_id == position1.position_id
        # Position2 should still be in portfolio (handled by portfolio manager)
        assert len(portfolio.positions) == 2


# =============================================================================
# Integration Tests - Edge Cases
# =============================================================================


class TestLPEdgeCases:
    """Integration tests for LP edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_position_close_without_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test closing position without collecting fees."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        position = create_lp_position()
        position.accumulated_fees_usd = Decimal("100")  # Has fees

        portfolio = MockPortfolio()
        portfolio.positions = [position]

        close_intent = LPCloseIntent(
            position_id=position.position_id,
            collect_fees=False,  # Don't collect fees
            protocol="uniswap_v3",
        )

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill = lp_adapter.execute_intent(close_intent, portfolio, market)

        assert fill is not None
        assert fill.success is True
        assert fill.metadata.get("collect_fees") is False

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_full_range_position_always_in_range(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that full-range position is always in range."""
        # Full range position (like Uniswap V2)
        position = create_lp_position(
            tick_lower=-887272,
            tick_upper=887272,
        )

        # Test various extreme price points
        for price in [Decimal("0.001"), Decimal("1"), Decimal("1000"), Decimal("1000000")]:
            market = MockMarketState(
                prices={"WETH": price, "USDC": Decimal("1")},
                timestamp=datetime.now(UTC),
            )
            result = lp_adapter.get_range_status(position, market)

            assert result is not None
            assert result.status == RangeStatus.IN_RANGE
            assert not result.is_out_of_range

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_position_not_found_returns_failure(
        self,
        web3: Web3,
        funded_wallet: str,
        lp_adapter: LPBacktestAdapter,
    ) -> None:
        """Test that closing non-existent position returns failure."""
        from almanak.framework.intents.vocabulary import LPCloseIntent

        portfolio = MockPortfolio()
        portfolio.positions = []  # Empty portfolio

        close_intent = LPCloseIntent(
            position_id="nonexistent_id",
            protocol="uniswap_v3",
        )

        market = MockMarketState(
            prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
            timestamp=datetime.now(UTC),
        )

        fill = lp_adapter.execute_intent(close_intent, portfolio, market)

        assert fill is not None
        assert fill.success is False
        assert "not found" in fill.metadata.get("failure_reason", "").lower()
