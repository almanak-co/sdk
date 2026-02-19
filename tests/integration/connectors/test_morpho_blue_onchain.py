"""On-chain integration tests for Morpho Blue adapter.

Tests verify actual on-chain behavior using Anvil fork of Ethereum mainnet.

To run:
    uv run pytest tests/integration/connectors/test_morpho_blue_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set

Note: Morpho Blue is deployed at 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb on Ethereum.
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

# Morpho Blue contract address (same on all chains)
MORPHO_BLUE_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

# wstETH/USDC market on Ethereum (86% LLTV)
WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# Token addresses (Ethereum mainnet)
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
WSTETH_ADDRESS = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

# Token balance storage slots
# USDC uses slot 9 for balances (verified via cast storage)
USDC_BALANCE_SLOT = 9
# wstETH uses slot 0 for balances
WSTETH_BALANCE_SLOT = 0

# Minimal ERC20 ABI
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


def fund_erc20(
    wallet: str,
    token_address: str,
    amount_wei: int,
    balance_slot: int,
    rpc_url: str,
) -> None:
    """Fund a wallet with ERC20 tokens using storage slot manipulation.

    Uses cast index to compute the storage slot for the wallet's balance.
    """
    # Compute storage slot: keccak256(wallet . slot_number)
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(balance_slot)],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    # Set the storage value (pad to 32 bytes)
    amount_hex = "0x" + hex(amount_wei)[2:].zfill(64)
    subprocess.run(
        [
            "cast",
            "rpc",
            "anvil_setStorageAt",
            token_address,
            storage_slot,
            amount_hex,
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
    tx_dict["chainId"] = web3.eth.chain_id
    tx_dict["nonce"] = web3.eth.get_transaction_count(Web3.to_checksum_address(tx_dict.get("from", TEST_WALLET)))
    if "gas" not in tx_dict:
        tx_dict["gas"] = 500000
    if "gasPrice" not in tx_dict:
        tx_dict["gasPrice"] = web3.eth.gas_price

    signed_tx = web3.eth.account.sign_transaction(tx_dict, private_key)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    return dict(receipt)


def approve_token(
    web3: Web3,
    token_address: str,
    spender: str,
    amount: int,
    wallet: str = TEST_WALLET,
    private_key: str = TEST_PRIVATE_KEY,
) -> dict:
    """Approve token spending."""
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    tx = contract.functions.approve(Web3.to_checksum_address(spender), amount).build_transaction(
        {
            "from": Web3.to_checksum_address(wallet),
            "gas": 100000,
            "gasPrice": web3.eth.gas_price,
            "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(wallet)),
            "chainId": web3.eth.chain_id,
        }
    )
    return send_signed_transaction(web3, tx, private_key)


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
            f"Anvil must be forked from Ethereum mainnet (chain ID 1). "
            f"Current chain ID: {chain_id}."
        )

    return w3


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with ETH, USDC, and wstETH.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH for gas
    fund_native_token(TEST_WALLET, 100 * 10**18, anvil_rpc_url)

    # Fund with 100,000 USDC (6 decimals)
    fund_erc20(TEST_WALLET, USDC_ADDRESS, 100_000 * 10**6, USDC_BALANCE_SLOT, anvil_rpc_url)

    # Fund with 100 wstETH (18 decimals)
    fund_erc20(TEST_WALLET, WSTETH_ADDRESS, 100 * 10**18, WSTETH_BALANCE_SLOT, anvil_rpc_url)

    return TEST_WALLET


# =============================================================================
# SDK Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.morpho
class TestMorphoBlueSDKOnChain:
    """On-chain tests for Morpho Blue SDK."""

    def test_sdk_connection(self, web3: Web3, anvil_rpc_url: str) -> None:
        """Test SDK can connect to the chain."""
        from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

        sdk = MorphoBlueSDK(chain="ethereum", rpc_url=anvil_rpc_url)
        assert sdk.is_connected()
        assert sdk.get_chain_id() == 1

    def test_get_market_params(self, web3: Web3, anvil_rpc_url: str) -> None:
        """Test getting market parameters from on-chain."""
        from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

        sdk = MorphoBlueSDK(chain="ethereum", rpc_url=anvil_rpc_url)
        params = sdk.get_market_params(WSTETH_USDC_MARKET_ID)

        assert params.loan_token.lower() == USDC_ADDRESS.lower()
        assert params.collateral_token.lower() == WSTETH_ADDRESS.lower()
        assert params.lltv == 860000000000000000  # 86% LLTV

    def test_get_market_state(self, web3: Web3, anvil_rpc_url: str) -> None:
        """Test getting market state from on-chain."""
        from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

        sdk = MorphoBlueSDK(chain="ethereum", rpc_url=anvil_rpc_url)
        state = sdk.get_market_state(WSTETH_USDC_MARKET_ID)

        # Market should exist and have some activity
        assert state.market_id == WSTETH_USDC_MARKET_ID.lower()
        # Don't assert on exact values since they change

    def test_get_position_empty(self, web3: Web3, anvil_rpc_url: str) -> None:
        """Test getting position for user with no position."""
        from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

        sdk = MorphoBlueSDK(chain="ethereum", rpc_url=anvil_rpc_url)
        position = sdk.get_position(WSTETH_USDC_MARKET_ID, TEST_WALLET)

        # Test wallet should have no position initially
        assert position.is_empty
        assert position.supply_shares == 0
        assert position.borrow_shares == 0
        assert position.collateral == 0

    def test_discover_markets(self, web3: Web3, anvil_rpc_url: str) -> None:
        """Test discovering markets from on-chain events."""
        from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

        sdk = MorphoBlueSDK(chain="ethereum", rpc_url=anvil_rpc_url)
        markets = sdk.discover_markets()

        # Should find multiple markets
        assert len(markets) > 0
        # Our test market should be in the list
        assert WSTETH_USDC_MARKET_ID.lower() in [m.lower() for m in markets]


# =============================================================================
# Adapter Transaction Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.morpho
class TestMorphoBlueAdapterOnChain:
    """On-chain tests for Morpho Blue adapter transactions."""

    def test_supply_collateral(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test supplying collateral to Morpho Blue."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )
        from almanak.framework.connectors.morpho_blue.receipt_parser import (
            MorphoBlueReceiptParser,
        )

        # Get initial balances
        wsteth_before = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        assert wsteth_before > 0, "Wallet should be funded with wstETH"

        # Create adapter
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Approve wstETH for Morpho Blue
        supply_amount = Decimal("1.0")  # 1 wstETH
        supply_amount_wei = int(supply_amount * Decimal(10**18))
        approve_receipt = approve_token(web3, WSTETH_ADDRESS, MORPHO_BLUE_ADDRESS, supply_amount_wei * 2)
        assert approve_receipt["status"] == 1, "Approval failed"

        # Build supply collateral transaction
        result = adapter.supply_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=supply_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"
        assert result.tx_data is not None

        # Execute transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,  # Add buffer
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        # Verify transaction succeeded
        assert receipt["status"] == 1, f"Transaction failed: {receipt}"

        # Verify wstETH balance decreased
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        assert wsteth_before - wsteth_after == supply_amount_wei, "Balance mismatch"

        # Parse receipt and verify event
        parser = MorphoBlueReceiptParser()
        parse_result = parser.parse_receipt(receipt)
        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

        # Find SupplyCollateral event
        supply_events = [e for e in parse_result.events if e.event_name == "SupplyCollateral"]
        assert len(supply_events) >= 1, "Expected SupplyCollateral event"

        # Verify position using SDK
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        assert position.collateral == Decimal(supply_amount_wei), "Position collateral mismatch"

    def test_borrow_after_collateral(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test borrowing USDC after supplying collateral."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )

        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Check we have collateral from previous test
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        if position.collateral == 0:
            pytest.skip("No collateral from previous test")

        # Get initial USDC balance
        usdc_before = get_token_balance(web3, USDC_ADDRESS, funded_wallet)

        # Borrow a small amount of USDC (well under collateral value)
        borrow_amount = Decimal("100")  # 100 USDC
        borrow_amount_wei = int(borrow_amount * Decimal(10**6))

        result = adapter.borrow(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=borrow_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"

        # Execute borrow
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1, f"Borrow failed: {receipt}"

        # Verify USDC balance increased
        usdc_after = get_token_balance(web3, USDC_ADDRESS, funded_wallet)
        assert usdc_after - usdc_before == borrow_amount_wei, "USDC balance mismatch"

        # Verify position has borrow
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        assert position.borrow_shares > 0, "Position should have borrow shares"

    def test_repay_debt(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test repaying borrowed USDC."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )

        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Check we have borrow from previous test
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        if position.borrow_shares == 0:
            pytest.skip("No borrow from previous test")

        # Approve USDC for repayment
        repay_amount = Decimal("50")  # Repay 50 USDC
        repay_amount_wei = int(repay_amount * Decimal(10**6))
        approve_receipt = approve_token(web3, USDC_ADDRESS, MORPHO_BLUE_ADDRESS, repay_amount_wei * 2)
        assert approve_receipt["status"] == 1

        # Build repay transaction
        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=repay_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"

        # Execute repay
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1, f"Repay failed: {receipt}"

    def test_withdraw_collateral(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test withdrawing collateral (if no remaining debt)."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )

        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Check position
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        if position.collateral == 0:
            pytest.skip("No collateral to withdraw")

        # Get initial wstETH balance
        wsteth_before = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)

        # Withdraw a small amount of collateral
        withdraw_amount = Decimal("0.1")  # 0.1 wstETH
        withdraw_amount_wei = int(withdraw_amount * Decimal(10**18))

        result = adapter.withdraw_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=withdraw_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"

        # Execute withdraw
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1, f"Withdraw failed: {receipt}"

        # Verify wstETH balance increased
        wsteth_after = get_token_balance(web3, WSTETH_ADDRESS, funded_wallet)
        assert wsteth_after - wsteth_before == withdraw_amount_wei


# =============================================================================
# Supply (Lending) Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.morpho
class TestMorphoBlueSupplyOnChain:
    """On-chain tests for Morpho Blue supply (lending) operations."""

    def test_supply_lending(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test supplying USDC for lending yield."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )

        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Approve USDC
        supply_amount = Decimal("1000")  # 1000 USDC
        supply_amount_wei = int(supply_amount * Decimal(10**6))
        approve_receipt = approve_token(web3, USDC_ADDRESS, MORPHO_BLUE_ADDRESS, supply_amount_wei * 2)
        assert approve_receipt["status"] == 1

        # Build supply transaction
        result = adapter.supply(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=supply_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"

        # Execute supply
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1, f"Supply failed: {receipt}"

        # Verify position has supply shares
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        assert position.supply_shares > 0, "Position should have supply shares"

    def test_withdraw_lending(self, web3: Web3, funded_wallet: str, anvil_rpc_url: str) -> None:
        """Test withdrawing supplied USDC."""
        from almanak.framework.connectors.morpho_blue import (
            MorphoBlueAdapter,
            MorphoBlueConfig,
        )

        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            allow_placeholder_prices=True,
        )
        adapter = MorphoBlueAdapter(config)

        # Check we have supply from previous test
        position = adapter.get_position_on_chain(WSTETH_USDC_MARKET_ID)
        if position.supply_shares == 0:
            pytest.skip("No supply from previous test")

        # Withdraw some supplied USDC
        withdraw_amount = Decimal("500")  # 500 USDC
        result = adapter.withdraw(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=withdraw_amount,
        )
        assert result.success, f"Transaction build failed: {result.error}"

        # Execute withdraw
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
            "gas": result.gas_estimate + 50000,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1, f"Withdraw failed: {receipt}"
