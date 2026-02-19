"""On-chain integration tests for Spark adapter.

Tests verify actual on-chain behavior using Anvil fork of Ethereum mainnet.

To run:
    uv run pytest tests/integration/connectors/test_spark_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set

Note: Spark is an Aave V3 fork that uses the same event signatures.
The Spark Pool on Ethereum mainnet is at 0xC13e21B648A5Ee794902342038FF3aDAB66BE987.
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

# Spark contract addresses (Ethereum mainnet)
SPARK_POOL_ADDRESS = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

# DAI token address (Ethereum mainnet)
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# spDAI (Spark's DAI aToken equivalent) address
# Note: spTokens are minted when supplying to Spark
SPDAI_ADDRESS = "0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B"

# DAI balance storage slot (slot 2 for MakerDAO's DAI)
DAI_BALANCE_SLOT_BASE = 2

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


def fund_dai(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with DAI using storage slot manipulation.

    DAI uses slot 2 for the balanceOf mapping (standard MakerDAO layout).
    We compute the storage slot for the wallet's balance using cast index.
    """
    # Compute storage slot: keccak256(wallet . slot_number)
    # cast index computes this for us
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(DAI_BALANCE_SLOT_BASE)],
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
            DAI_ADDRESS,
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
    """Fund the test wallet with ETH and DAI.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH for gas
    eth_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, anvil_rpc_url)

    # Fund with 10,000 DAI for testing
    dai_amount = 10000 * 10**18
    fund_dai(TEST_WALLET, dai_amount, anvil_rpc_url)

    # Verify ETH funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= eth_amount, f"Wallet not funded with ETH: {balance}"

    # Verify DAI funding
    dai_balance = get_token_balance(web3, DAI_ADDRESS, TEST_WALLET)
    assert dai_balance >= dai_amount, f"Wallet not funded with DAI: {dai_balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def dai_contract(web3: Web3):
    """Get DAI contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(DAI_ADDRESS),
        abi=ERC20_ABI,
    )


@pytest.fixture(scope="module")
def spdai_contract(web3: Web3):
    """Get spDAI contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(SPDAI_ADDRESS),
        abi=ERC20_ABI,
    )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.spark
class TestSparkOnChain:
    """On-chain integration tests for Spark adapter.

    Tests run sequentially and share state (DAI/spDAI balances).
    """

    def test_supply_dai(
        self,
        web3: Web3,
        funded_wallet: str,
        dai_contract,
        spdai_contract,
    ):
        """
        Test: Supply 1000 DAI to Spark to receive spDAI.

        Validates:
        1. Approve transaction succeeds
        2. Supply transaction succeeds (status=1)
        3. DAI balance decreases by supply amount
        4. spDAI balance increases
        5. Supply event is emitted with correct data
        """
        from almanak.framework.connectors.spark import SparkAdapter, SparkConfig
        from almanak.framework.connectors.spark.receipt_parser import SparkReceiptParser

        # Get initial balances
        dai_before = dai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        spdai_before = spdai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Supply DAI to Spark ===")
        print(f"DAI before: {format_token(dai_before)}")
        print(f"spDAI before: {format_token(spdai_before)}")

        # Create adapter and build supply transaction
        config = SparkConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = SparkAdapter(config)

        supply_amount = Decimal("1000.0")
        supply_amount_wei = int(supply_amount * Decimal(10**18))

        # First, approve Spark Pool to spend DAI
        approve_tx = dai_contract.functions.approve(
            Web3.to_checksum_address(SPARK_POOL_ADDRESS),
            supply_amount_wei,
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

        # Build supply transaction using adapter
        result = adapter.supply("DAI", supply_amount)
        assert result.success, f"Supply transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the supply transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Supply transaction failed: {receipt}"

        # Verify balances after
        dai_after = dai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        spdai_after = spdai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"DAI after: {format_token(dai_after)}")
        print(f"spDAI after: {format_token(spdai_after)}")

        # DAI should decrease by supply amount
        dai_decrease = dai_before - dai_after
        assert dai_decrease == supply_amount_wei, (
            f"DAI decrease ({dai_decrease}) should equal supply amount ({supply_amount_wei})"
        )

        # spDAI should increase (1:1 ratio at first deposit)
        spdai_increase = spdai_after - spdai_before
        assert spdai_increase > 0, "spDAI should increase after supply"

        # Parse receipt to verify Supply event
        parser = SparkReceiptParser()
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        assert len(parse_result.supplies) == 1, f"Expected 1 supply event, got {len(parse_result.supplies)}"

        supply_event = parse_result.supplies[0]
        print(f"\nSupply event: {supply_event.to_dict()}")

        # Verify event data
        assert supply_event.user.lower() == funded_wallet.lower(), "Event user mismatch"
        assert supply_event.on_behalf_of.lower() == funded_wallet.lower(), "Event on_behalf_of mismatch"

        print(f"\nSuccessfully supplied {supply_amount} DAI for ~{format_token(spdai_increase)} spDAI")

    def test_withdraw_dai(
        self,
        web3: Web3,
        funded_wallet: str,
        dai_contract,
        spdai_contract,
    ):
        """
        Test: Withdraw DAI from Spark.

        Prerequisites: Must have spDAI from previous test.

        Validates:
        1. Transaction executes successfully (status=1)
        2. DAI balance increases
        3. spDAI balance decreases
        4. Withdraw event is emitted with correct data
        """
        from almanak.framework.connectors.spark import SparkAdapter, SparkConfig
        from almanak.framework.connectors.spark.receipt_parser import SparkReceiptParser

        # Get initial balances
        dai_before = dai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        spdai_before = spdai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print("\n=== Withdraw DAI from Spark ===")
        print(f"DAI before: {format_token(dai_before)}")
        print(f"spDAI before: {format_token(spdai_before)}")

        if spdai_before == 0:
            pytest.skip("No spDAI balance - run test_supply_dai first")

        # Withdraw half of the supplied amount
        withdraw_amount_wei = spdai_before // 2
        withdraw_amount = Decimal(withdraw_amount_wei) / Decimal(10**18)

        # Create adapter and build withdraw transaction
        config = SparkConfig(chain="ethereum", wallet_address=funded_wallet)
        adapter = SparkAdapter(config)

        result = adapter.withdraw("DAI", withdraw_amount)
        assert result.success, f"Withdraw transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the withdraw transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Withdraw transaction failed: {receipt}"

        # Verify balances after
        dai_after = dai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        spdai_after = spdai_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"DAI after: {format_token(dai_after)}")
        print(f"spDAI after: {format_token(spdai_after)}")

        # DAI should increase
        dai_increase = dai_after - dai_before
        assert dai_increase > 0, "DAI should increase after withdrawal"

        # spDAI should decrease
        spdai_decrease = spdai_before - spdai_after
        assert spdai_decrease > 0, "spDAI should decrease after withdrawal"

        # Parse receipt to verify Withdraw event
        parser = SparkReceiptParser()
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
        assert len(parse_result.withdraws) == 1, f"Expected 1 withdraw event, got {len(parse_result.withdraws)}"

        withdraw_event = parse_result.withdraws[0]
        print(f"\nWithdraw event: {withdraw_event.to_dict()}")

        # Verify event data
        assert withdraw_event.user.lower() == funded_wallet.lower(), "Event user mismatch"
        assert withdraw_event.to.lower() == funded_wallet.lower(), "Event to mismatch"

        print(
            f"\nSuccessfully withdrew {format_token(dai_increase)} DAI (burned ~{format_token(spdai_decrease)} spDAI)"
        )


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
