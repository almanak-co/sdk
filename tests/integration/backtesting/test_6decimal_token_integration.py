"""Integration tests for 6-decimal token handling (USDC/USDT).

These tests validate correct decimal handling for stablecoin swaps:
- USDC (6 decimals) swap execution and balance tracking
- USDT (6 decimals) swap execution and balance tracking
- Portfolio balance uses correct decimals (not 1,000,000x off)

Requirements:
    - Anvil running with Arbitrum mainnet fork on port 8546
    - ALCHEMY_API_KEY for price data (optional, uses fallback)

To run:
    1. Start Anvil with Arbitrum mainnet fork:
       anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --chain-id 42161 --port 8546

    2. Run tests:
       uv run pytest tests/integration/backtesting/test_6decimal_token_integration.py -v -s
"""

import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import pytest


def _require_cast() -> None:
    """Skip test if Foundry 'cast' CLI is not available."""
    if shutil.which("cast") is None:
        pytest.skip("Foundry 'cast' CLI not installed; required for Anvil funding helpers.")
from web3 import Web3

from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.backtesting.paper.token_registry import (
    CHAIN_ID_ARBITRUM,
    get_token_decimals,
    get_token_info,
    get_token_symbol,
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
USDT_ADDRESS = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"  # USDT on Arbitrum

# Minimal ERC20 ABI for balance and decimals checks
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
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
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
) -> None:
    """Fund a wallet with ERC20 tokens using cast index.

    Uses the cast index technique to find storage slots and set balances directly.
    """
    _require_cast()
    # Storage slot mappings for common tokens on Arbitrum
    slot_mappings = {
        USDC_ADDRESS.lower(): "0x33",  # USDC storage slot on Arbitrum
        USDT_ADDRESS.lower(): "0x33",  # USDT storage slot on Arbitrum (standard ERC20 pattern)
        WETH_ADDRESS.lower(): "0x3",  # WETH storage slot on Arbitrum
    }

    slot_base = slot_mappings.get(token_address.lower())
    if not slot_base:
        slot_base = "0x0"  # Default for standard ERC20

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


def get_onchain_decimals(web3: Web3, token_address: str) -> int:
    """Get ERC20 token decimals from on-chain."""
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.decimals().call()


def get_onchain_symbol(web3: Web3, token_address: str) -> str:
    """Get ERC20 token symbol from on-chain."""
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.symbol().call()


def wei_to_human(amount_wei: int, decimals: int) -> Decimal:
    """Convert wei (smallest unit) to human-readable amount."""
    return Decimal(amount_wei) / Decimal(10**decimals)


def human_to_wei(amount: Decimal, decimals: int) -> int:
    """Convert human-readable amount to wei (smallest unit)."""
    return int(amount * Decimal(10**decimals))


# =============================================================================
# Mock Classes for Testing
# =============================================================================


@dataclass
class MockMarketState:
    """Mock market state for decimal handling tests."""

    prices: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    available_tokens: set[str] = field(default_factory=set)

    def get_price(self, token: str) -> Decimal | None:
        """Get price for a token."""
        return self.prices.get(token)


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
def funded_wallet_usdc(web3: Web3) -> str:
    """Fund the test wallet with ETH and USDC.

    Returns the wallet address after funding.
    """
    # Fund with 10 ETH for gas
    eth_amount = 10 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, ANVIL_RPC)

    # Fund with 10,000 USDC (6 decimals)
    usdc_amount = human_to_wei(Decimal("10000"), 6)
    fund_erc20_token(TEST_WALLET, USDC_ADDRESS, usdc_amount, ANVIL_RPC)

    # Verify USDC funding
    usdc_balance = get_token_balance(web3, USDC_ADDRESS, TEST_WALLET)
    assert usdc_balance >= usdc_amount, f"Wallet not funded with USDC: {usdc_balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def funded_wallet_usdt(web3: Web3) -> str:
    """Fund the test wallet with ETH and USDT.

    Returns the wallet address after funding.
    """
    # Fund with 10 ETH for gas
    eth_amount = 10 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, ANVIL_RPC)

    # Fund with 10,000 USDT (6 decimals)
    usdt_amount = human_to_wei(Decimal("10000"), 6)
    fund_erc20_token(TEST_WALLET, USDT_ADDRESS, usdt_amount, ANVIL_RPC)

    # Verify USDT funding
    usdt_balance = get_token_balance(web3, USDT_ADDRESS, TEST_WALLET)
    assert usdt_balance >= usdt_amount, f"Wallet not funded with USDT: {usdt_balance}"

    return TEST_WALLET


# =============================================================================
# Integration Tests - USDC 6-Decimal Handling
# =============================================================================


class TestUSDC6DecimalHandling:
    """Integration tests for USDC 6-decimal token handling."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_decimals_are_6(
        self,
        web3: Web3,
    ) -> None:
        """Test that USDC on Arbitrum has 6 decimals."""
        # Verify on-chain
        onchain_decimals = get_onchain_decimals(web3, USDC_ADDRESS)
        assert onchain_decimals == 6, f"USDC should have 6 decimals, got {onchain_decimals}"

        # Verify registry matches
        registry_decimals = get_token_decimals(CHAIN_ID_ARBITRUM, USDC_ADDRESS)
        assert registry_decimals == 6, f"Registry should have 6 decimals for USDC, got {registry_decimals}"

        # Verify they match
        assert onchain_decimals == registry_decimals

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_symbol_resolved_correctly(
        self,
        web3: Web3,
    ) -> None:
        """Test that USDC symbol is resolved correctly from registry."""
        # Get on-chain symbol
        onchain_symbol = get_onchain_symbol(web3, USDC_ADDRESS)
        assert "USDC" in onchain_symbol, f"Expected USDC symbol, got {onchain_symbol}"

        # Get registry symbol
        registry_symbol = get_token_symbol(CHAIN_ID_ARBITRUM, USDC_ADDRESS)
        assert registry_symbol == "USDC", f"Registry should return USDC, got {registry_symbol}"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_balance_not_1million_off(
        self,
        web3: Web3,
        funded_wallet_usdc: str,
    ) -> None:
        """Test that USDC balance is NOT 1,000,000x off due to decimal handling."""
        # Get raw balance in smallest units
        raw_balance = get_token_balance(web3, USDC_ADDRESS, TEST_WALLET)

        # Convert to human-readable using 6 decimals
        human_balance = wei_to_human(raw_balance, 6)

        # We funded 10,000 USDC, so balance should be >= 10,000 (not 10,000,000,000)
        assert human_balance >= Decimal("10000"), f"Balance too low: {human_balance}"
        assert human_balance < Decimal("100000"), f"Balance too high (possible decimal error): {human_balance}"

        # Verify it's NOT 1,000,000x off (wrong decimals)
        wrong_balance_18 = wei_to_human(raw_balance, 18)
        assert wrong_balance_18 < Decimal("1"), f"If using 18 decimals, should be tiny: {wrong_balance_18}"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_portfolio_balance_correct(
        self,
        web3: Web3,
        funded_wallet_usdc: str,
    ) -> None:
        """Test that portfolio tracker handles USDC decimals correctly."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="usdc_decimal_test",
            chain="arbitrum",
        )

        # Start session with USDC balance (human-readable)
        initial_balances = {"USDC": Decimal("10000")}
        tracker.start_session(initial_balances)

        # Verify initial balance is correct
        assert tracker.current_balances["USDC"] == Decimal("10000")

        # Record a trade: swap 100 USDC for 0.05 WETH
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345678,
            intent={"type": "SWAP", "from_token": "USDC", "to_token": "WETH"},
            tx_hash="0x" + "a" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.05")},  # Received 0.05 WETH
            tokens_out={"USDC": Decimal("100")},  # Spent 100 USDC
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify balances after trade
        assert tracker.current_balances["USDC"] == Decimal("9900"), (
            f"Expected 9900 USDC, got {tracker.current_balances['USDC']}"
        )
        assert tracker.current_balances["WETH"] == Decimal("0.05"), (
            f"Expected 0.05 WETH, got {tracker.current_balances.get('WETH')}"
        )

        # Verify PnL calculation uses correct decimals
        current_prices = {"USDC": Decimal("1"), "WETH": Decimal("2000")}
        pnl = tracker.get_pnl_usd(current_prices)

        # Initial: $10,000 USDC
        # Final: $9,900 USDC + 0.05 WETH * $2000 = $9,900 + $100 = $10,000
        # PnL should be ~$0 (minus gas $0.50)
        expected_pnl = Decimal("-0.50")
        assert abs(pnl - expected_pnl) < Decimal("1"), f"Expected PnL ~{expected_pnl}, got {pnl}"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_amount_precision_preserved(
        self,
        web3: Web3,
        funded_wallet_usdc: str,
    ) -> None:
        """Test that USDC fractional amounts are handled correctly."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="usdc_precision_test",
            chain="arbitrum",
        )

        # Start with fractional USDC amount
        initial_balances = {"USDC": Decimal("100.123456")}  # Max 6 decimal places
        tracker.start_session(initial_balances)

        # Verify fractional balance preserved
        assert tracker.current_balances["USDC"] == Decimal("100.123456")

        # Record trade with fractional amount
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0x" + "b" * 64,
            gas_used=100000,
            gas_cost_usd=Decimal("0.25"),
            tokens_in={"WETH": Decimal("0.000025")},  # ~$0.05 worth
            tokens_out={"USDC": Decimal("0.050001")},
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify precision preserved
        expected = Decimal("100.123456") - Decimal("0.050001")
        assert tracker.current_balances["USDC"] == expected, (
            f"Expected {expected}, got {tracker.current_balances['USDC']}"
        )


# =============================================================================
# Integration Tests - USDT 6-Decimal Handling
# =============================================================================


class TestUSDT6DecimalHandling:
    """Integration tests for USDT 6-decimal token handling."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdt_decimals_are_6(
        self,
        web3: Web3,
    ) -> None:
        """Test that USDT on Arbitrum has 6 decimals."""
        # Verify on-chain
        onchain_decimals = get_onchain_decimals(web3, USDT_ADDRESS)
        assert onchain_decimals == 6, f"USDT should have 6 decimals, got {onchain_decimals}"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdt_balance_not_1million_off(
        self,
        web3: Web3,
        funded_wallet_usdt: str,
    ) -> None:
        """Test that USDT balance is NOT 1,000,000x off due to decimal handling."""
        # Get raw balance in smallest units
        raw_balance = get_token_balance(web3, USDT_ADDRESS, TEST_WALLET)

        # Convert to human-readable using 6 decimals
        human_balance = wei_to_human(raw_balance, 6)

        # We funded 10,000 USDT, so balance should be >= 10,000 (not 10,000,000,000)
        assert human_balance >= Decimal("10000"), f"Balance too low: {human_balance}"
        assert human_balance < Decimal("100000"), f"Balance too high (possible decimal error): {human_balance}"

        # Verify it's NOT 1,000,000x off (wrong decimals)
        wrong_balance_18 = wei_to_human(raw_balance, 18)
        assert wrong_balance_18 < Decimal("1"), f"If using 18 decimals, should be tiny: {wrong_balance_18}"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdt_portfolio_balance_correct(
        self,
        web3: Web3,
        funded_wallet_usdt: str,
    ) -> None:
        """Test that portfolio tracker handles USDT decimals correctly."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="usdt_decimal_test",
            chain="arbitrum",
        )

        # Start session with USDT balance (human-readable)
        initial_balances = {"USDT": Decimal("10000")}
        tracker.start_session(initial_balances)

        # Verify initial balance is correct
        assert tracker.current_balances["USDT"] == Decimal("10000")

        # Record a trade: swap 100 USDT for 0.05 WETH
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345679,
            intent={"type": "SWAP", "from_token": "USDT", "to_token": "WETH"},
            tx_hash="0x" + "c" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"WETH": Decimal("0.05")},  # Received 0.05 WETH
            tokens_out={"USDT": Decimal("100")},  # Spent 100 USDT
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify balances after trade
        assert tracker.current_balances["USDT"] == Decimal("9900"), (
            f"Expected 9900 USDT, got {tracker.current_balances['USDT']}"
        )
        assert tracker.current_balances["WETH"] == Decimal("0.05"), (
            f"Expected 0.05 WETH, got {tracker.current_balances.get('WETH')}"
        )


# =============================================================================
# Integration Tests - Cross-Token Decimal Comparison
# =============================================================================


class TestCrossTokenDecimals:
    """Integration tests comparing decimal handling across tokens."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_usdc_usdt_weth_decimal_consistency(
        self,
        web3: Web3,
    ) -> None:
        """Test that decimal handling is consistent across USDC, USDT, and WETH."""
        # Get decimals for each token
        usdc_decimals = get_onchain_decimals(web3, USDC_ADDRESS)
        usdt_decimals = get_onchain_decimals(web3, USDT_ADDRESS)
        weth_decimals = get_onchain_decimals(web3, WETH_ADDRESS)

        # Verify expected decimals
        assert usdc_decimals == 6, f"USDC should have 6 decimals, got {usdc_decimals}"
        assert usdt_decimals == 6, f"USDT should have 6 decimals, got {usdt_decimals}"
        assert weth_decimals == 18, f"WETH should have 18 decimals, got {weth_decimals}"

        # Verify USDC and USDT have same decimals
        assert usdc_decimals == usdt_decimals, "USDC and USDT should have same decimals"

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_portfolio_mixed_decimal_tokens(
        self,
        web3: Web3,
        funded_wallet_usdc: str,
    ) -> None:
        """Test portfolio with mixed decimal tokens (6 and 18 decimals)."""
        from almanak.framework.backtesting.paper.models import PaperTrade

        tracker = PaperPortfolioTracker(
            strategy_id="mixed_decimal_test",
            chain="arbitrum",
        )

        # Start with mixed tokens
        initial_balances = {
            "USDC": Decimal("10000"),  # 6 decimals
            "WETH": Decimal("5"),  # 18 decimals
        }
        tracker.start_session(initial_balances)

        # Record swap: 1 WETH -> 2000 USDC
        trade = PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=12345680,
            intent={"type": "SWAP", "from_token": "WETH", "to_token": "USDC"},
            tx_hash="0x" + "d" * 64,
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("2000")},  # Received 2000 USDC
            tokens_out={"WETH": Decimal("1")},  # Spent 1 WETH
            protocol="uniswap_v3",
        )
        tracker.record_trade(trade)

        # Verify balances
        assert tracker.current_balances["USDC"] == Decimal("12000"), (
            f"Expected 12000 USDC, got {tracker.current_balances['USDC']}"
        )
        assert tracker.current_balances["WETH"] == Decimal("4"), (
            f"Expected 4 WETH, got {tracker.current_balances['WETH']}"
        )

        # Verify PnL with mixed decimals
        current_prices = {"USDC": Decimal("1"), "WETH": Decimal("2000")}
        pnl = tracker.get_pnl_usd(current_prices)

        # Initial: $10,000 USDC + 5 WETH * $2000 = $20,000
        # Final: $12,000 USDC + 4 WETH * $2000 = $20,000 (minus gas $0.50)
        expected_pnl = Decimal("-0.50")
        assert abs(pnl - expected_pnl) < Decimal("1"), f"Expected PnL ~{expected_pnl}, got {pnl}"


# =============================================================================
# Integration Tests - Token Registry Accuracy
# =============================================================================


class TestTokenRegistryAccuracy:
    """Integration tests verifying token registry matches on-chain data."""

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_registry_usdc_matches_onchain(
        self,
        web3: Web3,
    ) -> None:
        """Test that registry USDC info matches on-chain data."""
        # Get on-chain data
        onchain_decimals = get_onchain_decimals(web3, USDC_ADDRESS)
        onchain_symbol = get_onchain_symbol(web3, USDC_ADDRESS)

        # Get registry data
        info = get_token_info(CHAIN_ID_ARBITRUM, USDC_ADDRESS)

        assert info is not None, "USDC should be in registry"
        assert info.decimals == onchain_decimals, (
            f"Registry decimals {info.decimals} != on-chain {onchain_decimals}"
        )
        assert "USDC" in onchain_symbol and info.symbol == "USDC", (
            f"Registry symbol {info.symbol} doesn't match on-chain {onchain_symbol}"
        )

    @pytest.mark.asyncio
    @pytest.mark.anvil
    async def test_registry_weth_matches_onchain(
        self,
        web3: Web3,
    ) -> None:
        """Test that registry WETH info matches on-chain data."""
        # Get on-chain data
        onchain_decimals = get_onchain_decimals(web3, WETH_ADDRESS)
        onchain_symbol = get_onchain_symbol(web3, WETH_ADDRESS)

        # Get registry data
        info = get_token_info(CHAIN_ID_ARBITRUM, WETH_ADDRESS)

        assert info is not None, "WETH should be in registry"
        assert info.decimals == onchain_decimals, (
            f"Registry decimals {info.decimals} != on-chain {onchain_decimals}"
        )
        assert info.symbol == onchain_symbol, (
            f"Registry symbol {info.symbol} != on-chain {onchain_symbol}"
        )
