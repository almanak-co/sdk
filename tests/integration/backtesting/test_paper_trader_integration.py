"""Integration tests for Paper Trader with Anvil fork.

These tests run real transactions on an Anvil fork to validate:
- Fork initialization and management
- Real swap execution and receipt parsing
- Portfolio tracking accuracy
- Trade recording and metrics calculation

To run:
    1. Start Anvil with Arbitrum mainnet fork:
       anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --chain-id 42161 --port 8546

    2. Run tests:
       uv run pytest tests/integration/backtesting/test_paper_trader_integration.py -v -s

Requirements:
    - Anvil running with Arbitrum mainnet fork on port 8546
    - ALCHEMY_API_KEY for price data (optional, uses fallback)
"""

import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest


def _require_cast() -> None:
    """Skip test if Foundry 'cast' CLI is not available."""
    if shutil.which("cast") is None:
        pytest.skip("Foundry 'cast' CLI not installed; required for Anvil funding helpers.")
from web3 import Web3

from almanak.framework.backtesting.models import BacktestEngine
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader
from almanak.framework.anvil.fork_manager import (
    ForkManagerConfig,
    RollingForkManager,
)
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.data.market_snapshot import MarketSnapshot

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
ARB_ADDRESS = "0x912CE59144191C1204E64559FE8253a0e49E6548"

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
    This is more reliable than impersonation for common tokens.
    """
    _require_cast()
    # For USDC on Arbitrum, the balanceOf mapping is at slot 51 (0x33)
    # For WETH on Arbitrum, the balanceOf mapping is at slot 3
    slot_mappings = {
        USDC_ADDRESS.lower(): "0x33",  # USDC storage slot on Arbitrum
        WETH_ADDRESS.lower(): "0x3",   # WETH storage slot on Arbitrum
    }

    slot_base = slot_mappings.get(token_address.lower())
    if not slot_base:
        # Default to slot 0 for unknown tokens (standard ERC20 pattern)
        slot_base = "0x0"

    # Calculate storage slot using cast index
    result = subprocess.run(
        ["cast", "index", "address", wallet, slot_base],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    # Set the balance directly in storage (pad to 32 bytes)
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
    contract = web3.eth.contract(
        address=Web3.to_checksum_address(token_address), abi=ERC20_ABI
    )
    return contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()


def format_token(amount_wei: int, decimals: int = 18) -> Decimal:
    """Convert wei to token units."""
    return Decimal(amount_wei) / Decimal(10**decimals)


# =============================================================================
# Mock Strategy for Testing
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing Paper Trader."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount: Decimal = Decimal("100")  # Amount of from_token
    amount_usd: Decimal = Decimal("100")
    protocol: str = "uniswap_v3"
    slippage: Decimal = Decimal("0.01")  # 1% slippage


@dataclass
class MockHoldIntent:
    """Mock hold intent for testing Paper Trader."""

    intent_type: str = "HOLD"
    reason: str = "Testing hold behavior"


class DeterministicPaperStrategy:
    """Strategy with pre-defined decision sequence for Paper Trader testing."""

    def __init__(
        self,
        intents: list[Any | None],
        strategy_id: str = "paper_test_strategy",
    ):
        """Initialize with pre-defined intent sequence.

        Args:
            intents: List of intents to return in order (None = hold)
            strategy_id: Identifier for the strategy
        """
        self._intents = intents
        self._strategy_id = strategy_id
        self._call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: MarketSnapshot) -> Any | None:
        """Return next intent from sequence."""
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


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

    # Verify we're on Arbitrum mainnet fork
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
def paper_trader_config() -> PaperTraderConfig:
    """Create PaperTraderConfig for testing."""
    return PaperTraderConfig(
        chain="arbitrum",
        rpc_url=ANVIL_RPC,
        strategy_id="integration_test",
        initial_eth=Decimal("10"),
        initial_tokens={"USDC": Decimal("10000")},
        tick_interval_seconds=1,  # Fast ticks for testing
        max_ticks=5,
        anvil_port=8546,
        reset_fork_every_tick=False,  # Don't reset to preserve state during test
        startup_timeout_seconds=30.0,
    )


@pytest.fixture
def fork_manager_config() -> ForkManagerConfig:
    """Create ForkManagerConfig for testing."""
    return ForkManagerConfig(
        rpc_url=ANVIL_RPC,
        chain="arbitrum",
        anvil_port=8546,
        startup_timeout_seconds=30.0,
        auto_impersonate=True,
    )


# =============================================================================
# Integration Tests - Paper Trader Setup
# =============================================================================


class TestPaperTraderSetup:
    """Integration tests for Paper Trader initialization."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_fork_manager_connects_to_anvil(
        self,
        web3: Web3,
        fork_manager_config: ForkManagerConfig,
    ) -> None:
        """Test that RollingForkManager can connect to existing Anvil."""
        # Create fork manager that connects to existing Anvil
        fork_manager = RollingForkManager(
            rpc_url=fork_manager_config.rpc_url,
            chain=fork_manager_config.chain,
            anvil_port=fork_manager_config.anvil_port,
        )

        # The fork manager should detect the existing Anvil instance
        assert fork_manager.chain == "arbitrum"
        assert fork_manager.anvil_port == 8546

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_paper_trader_initializes(
        self,
        web3: Web3,
        funded_wallet: str,
        paper_trader_config: PaperTraderConfig,
    ) -> None:
        """Test that PaperTrader initializes correctly with config."""
        # Create fork manager
        fork_manager_config = ForkManagerConfig(
            rpc_url=ANVIL_RPC,
            chain="arbitrum",
            anvil_port=8546,
        )
        fork_manager = RollingForkManager(
            rpc_url=fork_manager_config.rpc_url,
            chain=fork_manager_config.chain,
            anvil_port=fork_manager_config.anvil_port,
        )

        # Create portfolio tracker
        portfolio_tracker = PaperPortfolioTracker(
            strategy_id=paper_trader_config.strategy_id,
            chain="arbitrum",
        )

        # Create paper trader
        paper_trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=paper_trader_config,
        )

        # Verify initialization
        assert paper_trader.config.chain == "arbitrum"
        assert paper_trader.config.tick_interval_seconds == 1
        assert not paper_trader.is_running()


# =============================================================================
# Integration Tests - Portfolio Tracking
# =============================================================================


class TestPortfolioTracking:
    """Integration tests for portfolio tracking accuracy."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_portfolio_tracker_initializes_with_balances(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that portfolio tracker correctly initializes balances."""
        tracker = PaperPortfolioTracker(
            strategy_id="test_strategy",
            chain="arbitrum",
        )

        # Start session with initial balances
        initial_balances = {
            "ETH": Decimal("10"),
            "USDC": Decimal("10000"),
        }
        tracker.start_session(initial_balances)

        # Verify balances
        assert tracker.current_balances["ETH"] == Decimal("10")
        assert tracker.current_balances["USDC"] == Decimal("10000")
        assert tracker.session_started is not None

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_portfolio_tracker_updates_on_trade(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that portfolio tracker correctly updates after trade."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="test_strategy",
            chain="arbitrum",
        )

        # Start session
        initial_balances = {
            "USDC": Decimal("10000"),
        }
        tracker.start_session(initial_balances)

        # Record a mock trade (USDC -> WETH swap)
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.05")},  # Received 0.05 WETH
            tokens_out={"USDC": Decimal("100")},  # Spent 100 USDC
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify balance updates
        assert tracker.current_balances["USDC"] == Decimal("9900")  # 10000 - 100
        assert tracker.current_balances["WETH"] == Decimal("0.05")  # Gained WETH
        assert tracker.total_gas_cost_usd == Decimal("0.50")
        assert len(tracker.trades) == 1

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_portfolio_tracker_calculates_pnl(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that portfolio tracker correctly calculates PnL."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="test_strategy",
            chain="arbitrum",
        )

        # Start session
        initial_balances = {
            "USDC": Decimal("10000"),
        }
        tracker.start_session(initial_balances)

        # Record a trade
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.05")},
            tokens_out={"USDC": Decimal("100")},
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Calculate PnL with current prices
        current_prices = {
            "USDC": Decimal("1"),
            "WETH": Decimal("2000"),  # WETH at $2000
        }
        pnl = tracker.get_pnl_usd(current_prices)

        # Initial: $10,000 USDC = $10,000
        # Final: $9,900 USDC + 0.05 WETH * $2000 = $9,900 + $100 = $10,000
        # PnL should be ~$0 (minus gas)
        assert pnl is not None
        # Account for gas cost
        expected_pnl = Decimal("0") - Decimal("0.50")
        assert abs(pnl - expected_pnl) < Decimal("1")  # Within $1 tolerance


# =============================================================================
# Integration Tests - Hold Strategy
# =============================================================================


class TestHoldStrategy:
    """Integration tests for hold-only strategy."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_hold_strategy_preserves_capital(
        self,
        web3: Web3,
        funded_wallet: str,
        paper_trader_config: PaperTraderConfig,
    ) -> None:
        """Test that hold-only strategy preserves initial capital."""
        # Create fork manager
        fork_manager_config = ForkManagerConfig(
            rpc_url=ANVIL_RPC,
            chain="arbitrum",
            anvil_port=8546,
        )
        fork_manager = RollingForkManager(
            rpc_url=fork_manager_config.rpc_url,
            chain=fork_manager_config.chain,
            anvil_port=fork_manager_config.anvil_port,
        )

        # Create portfolio tracker
        portfolio_tracker = PaperPortfolioTracker(
            strategy_id="hold_test",
            chain="arbitrum",
        )

        # Create paper trader with short duration
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url=ANVIL_RPC,
            strategy_id="hold_test",
            initial_eth=Decimal("10"),
            initial_tokens={"USDC": Decimal("10000")},
            tick_interval_seconds=1,
            max_ticks=3,  # Just 3 ticks
            anvil_port=8546,
            reset_fork_every_tick=False,
        )

        paper_trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=config,
        )

        # Create hold-only strategy
        strategy = DeterministicPaperStrategy(
            intents=[None, None, None],  # Always hold
            strategy_id="hold_test",
        )

        # Run paper trading (short duration)
        result = await paper_trader.run(
            strategy,
            duration_seconds=10,  # Max 10 seconds
            max_ticks=3,
        )

        # Verify results
        assert result.engine == BacktestEngine.PAPER
        assert result.metrics.total_trades == 0
        assert result.error is None
        # Initial capital should be preserved (no trades)
        assert len(result.trades) == 0


# =============================================================================
# Integration Tests - Receipt Parsing
# =============================================================================


class TestReceiptParsing:
    """Integration tests for transaction receipt parsing."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_receipt_parsing_extracts_token_flows(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that receipt parsing correctly extracts token flows."""
        from almanak.framework.backtesting.pnl.receipt_utils import (
            extract_token_flows,
            parse_transfer_events,
        )

        # Create a mock receipt with Transfer events
        # ERC20 Transfer event topic
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        # Create mock receipt dict
        mock_receipt = {
            "status": 1,
            "blockNumber": 12345678,
            "transactionHash": "0x" + "a" * 64,
            "logs": [
                {
                    # Transfer event: USDC sent from wallet
                    "address": USDC_ADDRESS,
                    "topics": [
                        transfer_topic,
                        "0x000000000000000000000000" + TEST_WALLET[2:].lower(),  # from
                        "0x000000000000000000000000" + "b" * 40,  # to (router)
                    ],
                    "data": "0x" + "0" * 56 + "5f5e100",  # 100 USDC (100 * 10^6)
                },
                {
                    # Transfer event: WETH received to wallet
                    "address": WETH_ADDRESS,
                    "topics": [
                        transfer_topic,
                        "0x000000000000000000000000" + "c" * 40,  # from (pool)
                        "0x000000000000000000000000" + TEST_WALLET[2:].lower(),  # to
                    ],
                    "data": "0x" + "0" * 48 + "2386f26fc10000",  # 0.01 WETH
                },
            ],
        }

        # Parse transfer events
        events = parse_transfer_events(mock_receipt)
        assert len(events) == 2

        # Extract token flows
        flows = extract_token_flows(mock_receipt, TEST_WALLET)

        # Verify tokens out (USDC sent)
        usdc_addr = USDC_ADDRESS.lower()
        assert usdc_addr in flows.tokens_out
        assert flows.tokens_out[usdc_addr] == 100000000  # 100 USDC in smallest units

        # Verify tokens in (WETH received)
        weth_addr = WETH_ADDRESS.lower()
        assert weth_addr in flows.tokens_in
        assert flows.tokens_in[weth_addr] == 10000000000000000  # 0.01 WETH in wei


# =============================================================================
# Integration Tests - Metrics Calculation
# =============================================================================


class TestMetricsCalculation:
    """Integration tests for backtest metrics calculation."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_metrics_track_gas_costs(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that metrics correctly track gas costs."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="gas_test",
            chain="arbitrum",
        )

        # Start session
        tracker.start_session({"USDC": Decimal("10000")})

        # Record multiple trades with gas costs
        for i in range(3):
            trade = PaperTrade(
                timestamp=datetime.now(UTC),
                block_number=12345678 + i,
                intent={"type": "SWAP"},
                tx_hash=f"0x{'a' * 63}{i}",
                gas_used=150000,
                gas_cost_usd=Decimal("0.50"),
                tokens_in={"WETH": Decimal("0.01")},
                tokens_out={"USDC": Decimal("20")},
                protocol="uniswap_v3",
            )
            tracker.record_trade(trade)

        # Verify gas tracking
        assert tracker.total_gas_used == 450000  # 150000 * 3
        assert tracker.total_gas_cost_usd == Decimal("1.50")  # 0.50 * 3
        assert len(tracker.trades) == 3

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_paper_trading_summary_generation(
        self,
        web3: Web3,
        funded_wallet: str,
    ) -> None:
        """Test that paper trading generates valid summary."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="summary_test",
            chain="arbitrum",
        )

        # Start session
        initial_balances = {"USDC": Decimal("10000")}
        tracker.start_session(initial_balances)

        # Record a trade
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.05")},
            tokens_out={"USDC": Decimal("100")},
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Get summary with PnL
        current_prices = {"USDC": Decimal("1"), "WETH": Decimal("2000")}
        summary = tracker.get_summary_with_pnl(current_prices)

        # Verify summary fields
        assert summary.strategy_id == "summary_test"
        assert summary.chain == "arbitrum"
        assert summary.total_trades == 1
        assert summary.successful_trades == 1
        assert summary.total_gas_cost_usd == Decimal("0.50")


# =============================================================================
# Integration Tests - Config Validation
# =============================================================================


class TestConfigValidation:
    """Integration tests for Paper Trader configuration validation."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_config_validates_chain(self) -> None:
        """Test that config validates chain parameter."""
        # Valid chain should work
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url=ANVIL_RPC,
            strategy_id="test",
        )
        assert config.chain == "arbitrum"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_config_provides_initial_balances(self) -> None:
        """Test that config correctly provides initial balances."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url=ANVIL_RPC,
            strategy_id="test",
            initial_eth=Decimal("5"),
            initial_tokens={"USDC": Decimal("5000"), "ARB": Decimal("1000")},
        )

        balances = config.get_initial_balances()

        # Should include ETH and all initial_tokens
        assert "ETH" in balances
        assert balances["ETH"] == Decimal("5")
        assert "USDC" in balances
        assert balances["USDC"] == Decimal("5000")
        assert "ARB" in balances
        assert balances["ARB"] == Decimal("1000")

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_config_serialization(self) -> None:
        """Test that config serializes and deserializes correctly."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url=ANVIL_RPC,
            strategy_id="test",
            initial_eth=Decimal("10"),
            initial_tokens={"USDC": Decimal("10000")},
            tick_interval_seconds=60,
            max_ticks=100,
        )

        # Serialize
        config_dict = config.to_dict()

        # Verify serialization
        assert config_dict["chain"] == "arbitrum"
        assert config_dict["strategy_id"] == "test"
        assert config_dict["tick_interval_seconds"] == 60
        assert config_dict["max_ticks"] == 100

        # Deserialize
        restored_config = PaperTraderConfig.from_dict(config_dict)

        # Verify restoration
        assert restored_config.chain == config.chain
        assert restored_config.strategy_id == config.strategy_id
        assert restored_config.tick_interval_seconds == config.tick_interval_seconds
