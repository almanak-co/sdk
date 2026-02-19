"""On-chain integration tests for Lido adapter.

Tests verify actual on-chain behavior using Anvil fork of Ethereum mainnet.

To run:
    uv run pytest tests/integration/connectors/test_lido_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set
"""

import subprocess
from decimal import Decimal

import pytest
from web3 import Web3

from tests.conftest_gateway import AnvilFixture

# Import fixtures for pytest discovery
pytest_plugins = ["tests.conftest_gateway"]

# =============================================================================
# Constants
# =============================================================================

# Default test wallet (Anvil's first account)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Lido contract addresses (Ethereum mainnet)
STETH_ADDRESS = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
WSTETH_ADDRESS = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

# stETH balance storage slot
STETH_BALANCE_SLOT = 0

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

# wstETH ABI for wrap/unwrap
WSTETH_ABI = [
    *ERC20_ABI,
    {
        "inputs": [{"name": "_stETHAmount", "type": "uint256"}],
        "name": "wrap",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "_wstETHAmount", "type": "uint256"}],
        "name": "unwrap",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# stETH ABI for submit (staking)
STETH_ABI = [
    *ERC20_ABI,
    {
        "inputs": [{"name": "_referral", "type": "address"}],
        "name": "submit",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": True,
        "stateMutability": "payable",
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


def get_token_balance(web3: Web3, token_address: str, wallet: str) -> int:
    """Get ERC20 token balance for a wallet."""
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()


def format_ether(amount_wei: int) -> Decimal:
    """Convert wei to ether."""
    return Decimal(amount_wei) / Decimal(10**18)


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
    """Fund the test wallet with ETH.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH
    eth_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, anvil_rpc_url)

    # Verify funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= eth_amount, f"Wallet not funded: {balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def steth_contract(web3: Web3):
    """Get stETH contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(STETH_ADDRESS),
        abi=STETH_ABI,
    )


@pytest.fixture(scope="module")
def wsteth_contract(web3: Web3):
    """Get wstETH contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(WSTETH_ADDRESS),
        abi=WSTETH_ABI,
    )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.ethereum
class TestLidoOnChain:
    """On-chain integration tests for Lido adapter.

    Tests run sequentially and share state (stETH/wstETH balances).
    """

    def test_stake_eth_for_steth(
        self,
        web3: Web3,
        funded_wallet: str,
        steth_contract,
    ):
        """
        Test: Stake 1 ETH to receive stETH.

        Validates:
        1. Transaction executes successfully (status=1)
        2. ETH balance decreases by stake amount
        3. stETH balance increases by approximately stake amount
        """
        from almanak.framework.connectors.lido import LidoAdapter, LidoConfig

        # Get initial balances
        eth_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_before = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Stake ETH for stETH ===")
        print(f"ETH before: {format_ether(eth_before)}")
        print(f"stETH before: {format_ether(steth_before)}")

        # Create adapter and build stake transaction
        config = LidoConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = LidoAdapter(config)

        stake_amount = Decimal("1.0")
        result = adapter.stake(stake_amount)

        assert result.success, f"Stake transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Transaction failed: {receipt}"

        # Verify balances after
        eth_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        steth_after = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"ETH after: {format_ether(eth_after)}")
        print(f"stETH after: {format_ether(steth_after)}")

        # ETH should decrease by approximately stake amount (plus gas)
        stake_amount_wei = int(stake_amount * Decimal(10**18))
        eth_decrease = eth_before - eth_after
        assert eth_decrease >= stake_amount_wei, (
            f"ETH decrease ({eth_decrease}) should be >= stake amount ({stake_amount_wei})"
        )

        # stETH should increase by approximately stake amount
        steth_increase = steth_after - steth_before
        # Allow for small variance due to rebasing
        expected_min = stake_amount_wei * 99 // 100  # Allow 1% variance
        assert steth_increase >= expected_min, f"stETH increase ({steth_increase}) should be >= {expected_min}"

        print(f"\nSuccessfully staked {stake_amount} ETH for ~{format_ether(steth_increase)} stETH")

    def test_wrap_steth_to_wsteth(
        self,
        web3: Web3,
        funded_wallet: str,
        steth_contract,
        wsteth_contract,
    ):
        """
        Test: Wrap stETH to wstETH.

        Prerequisites: Must have stETH from previous test.

        Validates:
        1. Approve wstETH contract to spend stETH
        2. Transaction executes successfully (status=1)
        3. stETH balance decreases
        4. wstETH balance increases
        """
        from almanak.framework.connectors.lido import LidoAdapter, LidoConfig

        # Get initial balances
        steth_before = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        wsteth_before = wsteth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Wrap stETH to wstETH ===")
        print(f"stETH before: {format_ether(steth_before)}")
        print(f"wstETH before: {format_ether(wsteth_before)}")

        if steth_before == 0:
            pytest.skip("No stETH balance - run test_stake_eth_for_steth first")

        # Use half of stETH balance for wrapping
        wrap_amount_wei = steth_before // 2
        wrap_amount = Decimal(wrap_amount_wei) / Decimal(10**18)

        # First, approve wstETH contract to spend stETH
        approve_tx = steth_contract.functions.approve(
            Web3.to_checksum_address(WSTETH_ADDRESS),
            wrap_amount_wei,
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

        # Build wrap transaction using adapter
        config = LidoConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = LidoAdapter(config)

        result = adapter.wrap(wrap_amount)
        assert result.success, f"Wrap transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the wrap transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Wrap transaction failed: {receipt}"

        # Verify balances after
        steth_after = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        wsteth_after = wsteth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"stETH after: {format_ether(steth_after)}")
        print(f"wstETH after: {format_ether(wsteth_after)}")

        # stETH should decrease
        steth_decrease = steth_before - steth_after
        assert steth_decrease > 0, "stETH should decrease after wrapping"

        # wstETH should increase
        wsteth_increase = wsteth_after - wsteth_before
        assert wsteth_increase > 0, "wstETH should increase after wrapping"

        print(f"\nSuccessfully wrapped {format_ether(steth_decrease)} stETH to {format_ether(wsteth_increase)} wstETH")

    def test_unwrap_wsteth_to_steth(
        self,
        web3: Web3,
        funded_wallet: str,
        steth_contract,
        wsteth_contract,
    ):
        """
        Test: Unwrap wstETH back to stETH.

        Prerequisites: Must have wstETH from previous test.

        Validates:
        1. Transaction executes successfully (status=1)
        2. wstETH balance decreases
        3. stETH balance increases
        """
        from almanak.framework.connectors.lido import LidoAdapter, LidoConfig

        # Get initial balances
        steth_before = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        wsteth_before = wsteth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Unwrap wstETH to stETH ===")
        print(f"stETH before: {format_ether(steth_before)}")
        print(f"wstETH before: {format_ether(wsteth_before)}")

        if wsteth_before == 0:
            pytest.skip("No wstETH balance - run test_wrap_steth_to_wsteth first")

        # Unwrap all wstETH
        unwrap_amount = Decimal(wsteth_before) / Decimal(10**18)

        # Build unwrap transaction using adapter
        config = LidoConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = LidoAdapter(config)

        result = adapter.unwrap(unwrap_amount)
        assert result.success, f"Unwrap transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the unwrap transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Unwrap transaction failed: {receipt}"

        # Verify balances after
        steth_after = steth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        wsteth_after = wsteth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"stETH after: {format_ether(steth_after)}")
        print(f"wstETH after: {format_ether(wsteth_after)}")

        # wstETH should decrease to approximately zero
        assert wsteth_after < wsteth_before, "wstETH should decrease after unwrapping"

        # stETH should increase
        steth_increase = steth_after - steth_before
        assert steth_increase > 0, "stETH should increase after unwrapping"

        print(f"\nSuccessfully unwrapped {format_ether(wsteth_before)} wstETH to {format_ether(steth_increase)} stETH")


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
