"""On-chain integration tests for Ethena adapter.

Tests verify actual on-chain behavior using Anvil fork of Ethereum mainnet.

To run:
    uv run pytest tests/integration/connectors/test_ethena_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set

Note: Ethena's StakedUSDeV2 does NOT emit a custom CooldownStarted event.
When cooldownAssets() is called, it emits standard ERC4626 Withdraw event
with the receiver being the USDeSilo contract.
"""

import subprocess
from decimal import Decimal

import pytest
from web3 import Web3

from tests.conftest_gateway import AnvilFixture

# Import fixture for pytest to discover (re-exported from conftest_gateway)
pytest_plugins = ["tests.conftest_gateway"]

# =============================================================================
# Constants
# =============================================================================

# Default test wallet (Anvil's first account)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Ethena contract addresses (Ethereum mainnet)
USDE_ADDRESS = "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"
SUSDE_ADDRESS = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"

# USDe balance storage slot (slot 2 for this contract)
# Computed using: cast index address <wallet> 2
# Note: USDe is an upgradeable contract, balances mapping is at slot 2
USDE_BALANCE_SLOT_BASE = 2

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
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function",
    },
]

# sUSDe ABI for staking/unstaking
SUSDE_ABI = [
    *ERC20_ABI,
    # ERC4626 deposit (stake)
    {
        "inputs": [
            {"name": "assets", "type": "uint256"},
            {"name": "receiver", "type": "address"},
        ],
        "name": "deposit",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    # Ethena cooldownAssets (initiate unstake)
    {
        "inputs": [{"name": "assets", "type": "uint256"}],
        "name": "cooldownAssets",
        "outputs": [{"name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    # Check cooldown duration
    {
        "inputs": [],
        "name": "cooldownDuration",
        "outputs": [{"name": "", "type": "uint24"}],
        "stateMutability": "view",
        "type": "function",
    },
    # Check user's cooldown state
    {
        "inputs": [{"name": "user", "type": "address"}],
        "name": "cooldowns",
        "outputs": [
            {"name": "cooldownEnd", "type": "uint104"},
            {"name": "underlyingAmount", "type": "uint152"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Helper Functions
# =============================================================================


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with ETH."""
    amount_hex = hex(amount_wei)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


def fund_usde(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with USDe using storage slot manipulation.

    USDe uses standard ERC20 storage layout where balanceOf mapping is at slot 0.
    We compute the storage slot for the wallet's balance using cast index.
    """
    # Compute storage slot: keccak256(wallet . slot_number)
    # cast index computes this for us
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(USDE_BALANCE_SLOT_BASE)],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    # Set the storage value
    amount_hex = hex(amount_wei)
    subprocess.run(
        [
            "cast",
            "rpc",
            "anvil_setStorageAt",
            USDE_ADDRESS,
            storage_slot,
            # Pad the amount to 32 bytes
            "0x" + amount_hex[2:].zfill(64),
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


def format_token(amount_wei: int, decimals: int = 18) -> Decimal:
    """Convert wei to token units."""
    return Decimal(amount_wei) / Decimal(10**decimals)


def send_signed_transaction(
    web3: Web3,
    tx_dict: dict,
    private_key: str,
) -> dict:
    """Sign and send a transaction, return receipt."""
    # Add missing tx fields
    tx_dict["chainId"] = web3.eth.chain_id
    tx_dict["nonce"] = web3.eth.get_transaction_count(Web3.to_checksum_address(tx_dict.get("from", TEST_WALLET)))
    if "gas" not in tx_dict:
        tx_dict["gas"] = 500000
    if "gasPrice" not in tx_dict:
        tx_dict["gasPrice"] = web3.eth.gas_price

    # Sign and send
    signed_tx = web3.eth.account.sign_transaction(tx_dict, private_key)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    return dict(receipt)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_ethereum: AnvilFixture) -> str:
    """Get the RPC URL for the Ethereum Anvil fork."""
    return anvil_ethereum.get_rpc_url()


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Get Web3 instance connected to Anvil.

    The anvil_ethereum fixture guarantees Anvil is running with Ethereum mainnet fork.
    """
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url))

    # Verify we're on Ethereum mainnet fork
    chain_id = w3.eth.chain_id
    if chain_id != 1:
        pytest.skip(
            f"Anvil must be forked from Ethereum mainnet (chain ID 1). Current chain ID: {chain_id}."
        )

    return w3


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with ETH and USDe.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH for gas
    eth_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, anvil_rpc_url)

    # Fund with 10,000 USDe for testing
    usde_amount = 10000 * 10**18
    fund_usde(TEST_WALLET, usde_amount, anvil_rpc_url)

    # Verify ETH funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= eth_amount, f"Wallet not funded with ETH: {balance}"

    # Verify USDe funding
    usde_balance = get_token_balance(web3, USDE_ADDRESS, TEST_WALLET)
    assert usde_balance >= usde_amount, f"Wallet not funded with USDe: {usde_balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def usde_contract(web3: Web3):
    """Get USDe contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(USDE_ADDRESS),
        abi=ERC20_ABI,
    )


@pytest.fixture(scope="module")
def susde_contract(web3: Web3):
    """Get sUSDe contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(SUSDE_ADDRESS),
        abi=SUSDE_ABI,
    )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.ethena
class TestEthenaOnChain:
    """On-chain integration tests for Ethena adapter.

    Tests run sequentially and share state (USDe/sUSDe balances).
    """

    def test_stake_usde_for_susde(
        self,
        web3: Web3,
        funded_wallet: str,
        usde_contract,
        susde_contract,
    ):
        """
        Test: Stake 1000 USDe to receive sUSDe.

        Validates:
        1. Approve transaction succeeds
        2. Deposit transaction succeeds (status=1)
        3. USDe balance decreases by stake amount
        4. sUSDe balance increases
        5. Deposit event is emitted with correct data
        """
        from almanak.framework.connectors.ethena import EthenaAdapter, EthenaConfig
        from almanak.framework.connectors.ethena.receipt_parser import (
            EthenaReceiptParser,
        )

        # Get initial balances
        usde_before = usde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        susde_before = susde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Stake USDe for sUSDe ===")
        print(f"USDe before: {format_token(usde_before)}")
        print(f"sUSDe before: {format_token(susde_before)}")

        # Create adapter and build stake transaction
        config = EthenaConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = EthenaAdapter(config)

        stake_amount = Decimal("1000.0")
        stake_amount_wei = int(stake_amount * Decimal(10**18))

        # First, approve sUSDe contract to spend USDe
        approve_tx = usde_contract.functions.approve(
            Web3.to_checksum_address(SUSDE_ADDRESS),
            stake_amount_wei,
        ).build_transaction(
            {
                "from": funded_wallet,
                "gas": 100000,
                "gasPrice": web3.eth.gas_price,
                "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(funded_wallet)),
                "chainId": web3.eth.chain_id,
            }
        )
        approve_receipt = send_signed_transaction(web3, approve_tx, TEST_PRIVATE_KEY)
        assert approve_receipt["status"] == 1, f"Approve failed: {approve_receipt}"

        # Build stake transaction using adapter
        result = adapter.stake_usde(stake_amount)
        assert result.success, f"Stake transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the stake transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Stake transaction failed: {receipt}"

        # Verify balances after
        usde_after = usde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        susde_after = susde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"USDe after: {format_token(usde_after)}")
        print(f"sUSDe after: {format_token(susde_after)}")

        # USDe should decrease by stake amount
        usde_decrease = usde_before - usde_after
        assert usde_decrease == stake_amount_wei, (
            f"USDe decrease ({usde_decrease}) should equal stake amount ({stake_amount_wei})"
        )

        # sUSDe should increase
        susde_increase = susde_after - susde_before
        assert susde_increase > 0, "sUSDe should increase after staking"

        # Parse receipt to verify Deposit event
        parser = EthenaReceiptParser(chain="ethereum")
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        assert len(parse_result.stakes) == 1, f"Expected 1 stake event, got {len(parse_result.stakes)}"

        stake_event = parse_result.stakes[0]
        print(f"\nDeposit event: {stake_event.to_dict()}")

        # Verify event data
        assert stake_event.assets == stake_amount, (
            f"Event assets ({stake_event.assets}) should match stake amount ({stake_amount})"
        )
        assert stake_event.sender.lower() == funded_wallet.lower(), "Event sender mismatch"
        assert stake_event.owner.lower() == funded_wallet.lower(), "Event owner mismatch"

        print(f"\nSuccessfully staked {stake_amount} USDe for ~{format_token(susde_increase)} sUSDe")

    def test_initiate_cooldown(
        self,
        web3: Web3,
        funded_wallet: str,
        usde_contract,
        susde_contract,
    ):
        """
        Test: Initiate cooldown for unstaking sUSDe.

        Note: Ethena's StakedUSDeV2 does NOT emit a custom CooldownStarted event.
        When cooldownAssets() is called, it emits the standard ERC4626 Withdraw event
        with the receiver being the USDeSilo contract.

        Prerequisites: Must have sUSDe from previous test.

        Validates:
        1. Transaction executes successfully (status=1)
        2. ERC4626 Withdraw event is emitted (this is the cooldown initiation)
        3. sUSDe balance decreases
        4. Cooldown state is tracked on-chain via cooldowns mapping
        """
        from almanak.framework.connectors.ethena import EthenaAdapter, EthenaConfig
        from almanak.framework.connectors.ethena.receipt_parser import EthenaReceiptParser

        # Get initial balances
        usde_before = usde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        susde_before = susde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Initiate Cooldown for Unstaking ===")
        print(f"USDe before: {format_token(usde_before)}")
        print(f"sUSDe before: {format_token(susde_before)}")

        if susde_before == 0:
            pytest.skip("No sUSDe balance - run test_stake_usde_for_susde first")

        # Use half of sUSDe balance for cooldown
        cooldown_amount_wei = susde_before // 2
        cooldown_amount = Decimal(cooldown_amount_wei) / Decimal(10**18)

        # Create adapter and build unstake (cooldown) transaction
        config = EthenaConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = EthenaAdapter(config)

        result = adapter.unstake_susde(cooldown_amount)
        assert result.success, f"Unstake transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the cooldown transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Cooldown transaction failed: {receipt}"

        # Verify balances after
        usde_after = usde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        susde_after = susde_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"USDe after: {format_token(usde_after)}")
        print(f"sUSDe after: {format_token(susde_after)}")

        # sUSDe should decrease after cooldown (shares are burned)
        susde_decrease = susde_before - susde_after
        assert susde_decrease > 0, "sUSDe should decrease after cooldown initiation"

        # Parse receipt - should have Withdraw event (this IS the cooldown event)
        parser = EthenaReceiptParser(chain="ethereum")
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

        # Ethena emits Withdraw event when cooldownAssets is called
        # The receiver will be the USDeSilo contract (not the user)
        print(f"\nWithdraw events: {len(parse_result.withdraws)}")
        for i, withdraw in enumerate(parse_result.withdraws):
            print(f"  [{i}] {withdraw.to_dict()}")

        assert len(parse_result.withdraws) >= 1, (
            f"Expected at least 1 Withdraw event for cooldown, got {len(parse_result.withdraws)}"
        )

        # The Withdraw event indicates assets are being moved to the silo
        withdraw_event = parse_result.withdraws[0]

        # Verify the event data
        assert withdraw_event.sender.lower() == funded_wallet.lower(), "Event sender mismatch"
        assert withdraw_event.owner.lower() == funded_wallet.lower(), "Event owner mismatch"
        assert withdraw_event.assets > 0, "Event should have non-zero assets"
        assert withdraw_event.shares > 0, "Event should have non-zero shares"

        # Check cooldown state on-chain
        cooldown_info = susde_contract.functions.cooldowns(Web3.to_checksum_address(funded_wallet)).call()
        cooldown_end, underlying_amount = cooldown_info

        print("\nCooldown state:")
        print(f"  Cooldown end timestamp: {cooldown_end}")
        print(f"  Underlying amount: {format_token(underlying_amount)} USDe")

        # Verify cooldown is active
        assert cooldown_end > 0, "Cooldown end timestamp should be set"
        assert underlying_amount > 0, "Underlying amount should be set"

        print(f"\nSuccessfully initiated cooldown for ~{format_token(susde_decrease)} sUSDe worth of assets")
        print(f"USDe can be withdrawn after cooldown expires (timestamp: {cooldown_end})")


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
