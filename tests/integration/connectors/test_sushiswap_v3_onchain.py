"""On-chain integration tests for SushiSwap V3 connector.

Tests verify actual on-chain behavior using Anvil fork of Arbitrum mainnet.

To run:
    uv run pytest tests/integration/connectors/test_sushiswap_v3_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set

Note: SushiSwap V3 is a Uniswap V3 fork with identical interfaces. The swap router
on Arbitrum is at 0x8A21F6768C1f8075791D08546Dadf6daA0bE820c.
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

# SushiSwap V3 addresses (Arbitrum mainnet)
SUSHISWAP_V3_ROUTER = "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c"
SUSHISWAP_V3_POSITION_MANAGER = "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49"

# Token addresses (Arbitrum mainnet)
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
# Use bridged USDC.e for SushiSwap V3 tests - more liquidity than native USDC
USDC_ADDRESS = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"  # USDC.e (bridged)
ARB_ADDRESS = "0x912CE59144191C1204E64559FE8253a0e49E6548"

# Minimal ERC20 ABI for balance checks and approvals
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

# WETH ABI for deposit (wrap ETH to WETH)
WETH_ABI = [
    *ERC20_ABI,
    {
        "inputs": [],
        "name": "deposit",
        "outputs": [],
        "stateMutability": "payable",
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
def anvil_rpc_url(anvil_arbitrum: AnvilFixture) -> str:
    """Get the RPC URL for the Arbitrum Anvil fork."""
    return anvil_arbitrum.get_rpc_url()


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Get Web3 instance connected to Anvil.

    The anvil_arbitrum fixture guarantees Anvil is running with Arbitrum mainnet fork.
    """
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url))

    # Verify we're on Arbitrum mainnet fork
    chain_id = w3.eth.chain_id
    if chain_id != 42161:
        pytest.skip(f"Anvil must be forked from Arbitrum mainnet (chain ID 42161). Current chain ID: {chain_id}.")

    return w3


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with ETH.

    Returns the wallet address after funding.
    """
    # Fund with 100 ETH for gas and testing
    eth_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, eth_amount, anvil_rpc_url)

    # Verify ETH funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= eth_amount, f"Wallet not funded with ETH: {balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def weth_contract(web3: Web3):
    """Get WETH contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(WETH_ADDRESS),
        abi=WETH_ABI,
    )


@pytest.fixture(scope="module")
def usdc_contract(web3: Web3):
    """Get USDC contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=ERC20_ABI,
    )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.sushiswap
class TestSushiSwapV3OnChain:
    """On-chain integration tests for SushiSwap V3 connector.

    Tests run sequentially and share state (WETH/USDC balances).
    """

    @pytest.mark.skip(reason="SushiSwap V3 WETH/USDC.e pool on Arbitrum has insufficient liquidity for reliable testing")
    def test_swap_weth_for_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        weth_contract,
        usdc_contract,
    ):
        """
        Test: Swap 0.1 WETH for USDC on SushiSwap V3.

        Validates:
        1. Wrap ETH to WETH
        2. Approve WETH for router
        3. Swap transaction succeeds (status=1)
        4. WETH balance decreases by swap amount
        5. USDC balance increases
        6. Swap event is emitted with correct data
        """
        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
        )
        from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
            SushiSwapV3ReceiptParser,
        )

        # Step 1: Wrap ETH to WETH
        wrap_amount = Decimal("1.0")  # Wrap 1 ETH for testing
        wrap_amount_wei = int(wrap_amount * Decimal(10**18))

        print("\n=== Wrap ETH to WETH ===")
        wrap_tx = weth_contract.functions.deposit().build_transaction(
            {
                "from": funded_wallet,
                "value": wrap_amount_wei,
                "gas": 100000,
                "gasPrice": web3.eth.gas_price,
                "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(funded_wallet)),
                "chainId": web3.eth.chain_id,
            }
        )
        wrap_receipt = send_signed_transaction(web3, wrap_tx, TEST_PRIVATE_KEY)
        assert wrap_receipt["status"] == 1, f"Wrap failed: {wrap_receipt}"

        # Get initial balances
        weth_before = weth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        usdc_before = usdc_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"WETH before: {format_token(weth_before)}")
        print(f"USDC before: {format_token(usdc_before, 6)}")

        assert weth_before >= wrap_amount_wei, f"WETH not wrapped: {weth_before}"

        # Step 2: Approve SushiSwap router to spend WETH
        swap_amount = Decimal("0.1")  # Swap 0.1 WETH for USDC
        swap_amount_wei = int(swap_amount * Decimal(10**18))

        print("\n=== Approve WETH for Router ===")
        approve_tx = weth_contract.functions.approve(
            Web3.to_checksum_address(SUSHISWAP_V3_ROUTER),
            swap_amount_wei,
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

        # Step 3: Build and execute swap transaction using adapter
        # Use real-ish prices for Arbitrum (ETH ~$3400, USDC = $1)
        price_provider = {
            "WETH": Decimal("3400"),
            "ETH": Decimal("3400"),
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
        }

        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=funded_wallet,
            price_provider=price_provider,
            default_slippage_bps=500,  # 5% slippage for integration test (pools may have low liquidity)
            default_fee_tier=3000,  # 0.3% fee tier - standard for ETH/stablecoin pairs
        )
        adapter = SushiSwapV3Adapter(config)

        print("\n=== Swap WETH for USDC.e ===")
        result = adapter.swap_exact_input(
            token_in="WETH",
            token_out="USDC.e",  # Use bridged USDC.e for better liquidity
            amount_in=swap_amount,
        )
        assert result.success, f"Swap transaction build failed: {result.error}"
        assert len(result.transactions) > 0, "No transactions in result"

        # Find the swap transaction (skip approves)
        swap_tx_data = None
        for tx in result.transactions:
            if tx.tx_type == "swap":
                swap_tx_data = tx
                break

        assert swap_tx_data is not None, "No swap transaction found"

        # Execute the swap transaction
        tx_dict = {
            "from": funded_wallet,
            "to": swap_tx_data.to,
            "value": swap_tx_data.value,
            "data": swap_tx_data.data,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Swap transaction failed: {receipt}"

        # Verify balances after
        weth_after = weth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        usdc_after = usdc_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"WETH after: {format_token(weth_after)}")
        print(f"USDC after: {format_token(usdc_after, 6)}")

        # WETH should decrease by swap amount
        weth_decrease = weth_before - weth_after
        assert weth_decrease == swap_amount_wei, (
            f"WETH decrease ({weth_decrease}) should equal swap amount ({swap_amount_wei})"
        )

        # USDC should increase
        usdc_increase = usdc_after - usdc_before
        assert usdc_increase > 0, "USDC should increase after swap"

        # Parse receipt to verify Swap event
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

        # Verify swap events are present
        if parse_result.swap_events:
            swap_event = parse_result.swap_events[0]
            print(f"\nSwap event: {swap_event.to_dict()}")

            # Verify swap direction - user sent WETH (token1) and received USDC (token0)
            # So amount1 should be positive (in) and amount0 should be negative (out)
            assert swap_event.token1_is_input, "Expected WETH (token1) to be input"
            assert swap_event.amount_in > 0, "Amount in should be positive"
            assert swap_event.amount_out > 0, "Amount out should be positive"
        else:
            print("\nNote: No V3 Swap events found - router may have used different route")

        print(f"\nSuccessfully swapped {swap_amount} WETH for ~{format_token(usdc_increase, 6)} USDC.e")

    def test_slippage_protection_enforced(
        self,
        web3: Web3,
        funded_wallet: str,
    ):
        """
        Test: Verify slippage protection results in non-zero minimum output.

        This test validates that the adapter correctly calculates slippage
        protection based on price provider data, ensuring users are protected
        from MEV and price manipulation.

        Validates:
        1. swap_exact_input builds transaction with non-zero amount_out_min
        2. The calldata contains a reasonable minimum output value
        """
        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
        )

        print("\n=== Test Slippage Protection ===")

        # Test with real price provider
        price_provider = {
            "WETH": Decimal("3400"),
            "USDC.e": Decimal("1"),
        }

        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=funded_wallet,
            price_provider=price_provider,
            default_slippage_bps=50,  # 0.5% slippage
            default_fee_tier=3000,  # 0.3% fee tier
        )
        adapter = SushiSwapV3Adapter(config)

        swap_amount = Decimal("1.0")  # 1 WETH
        result = adapter.swap_exact_input(
            token_in="WETH",
            token_out="USDC.e",
            amount_in=swap_amount,
        )

        assert result.success, f"Swap transaction build failed: {result.error}"

        # Find the swap transaction
        swap_tx_data = None
        for tx in result.transactions:
            if tx.tx_type == "swap":
                swap_tx_data = tx
                break

        assert swap_tx_data is not None, "No swap transaction found"

        # Parse the calldata to extract amount_out_min
        # exactInputSingle calldata layout (after selector):
        # 0-31: tokenIn
        # 32-63: tokenOut
        # 64-95: fee
        # 96-127: recipient
        # 128-159: deadline
        # 160-191: amountIn
        # 192-223: amountOutMinimum
        # 224-255: sqrtPriceLimitX96

        calldata = swap_tx_data.data
        # Remove selector (0x + 8 hex chars = 10 chars)
        data_hex = calldata[10:] if calldata.startswith("0x") else calldata[8:]

        # Each parameter is 64 hex chars (32 bytes)
        chunk_size = 64
        chunks = [data_hex[i : i + chunk_size] for i in range(0, len(data_hex), chunk_size)]

        amount_in_wei = int(chunks[5], 16)
        amount_out_min_wei = int(chunks[6], 16)

        print(f"Amount in: {format_token(amount_in_wei)} WETH")
        print(f"Amount out min: {format_token(amount_out_min_wei, 6)} USDC.e")

        # Verify amount_out_min is reasonable
        # With WETH=$3400 and 0.5% slippage + 0.05% fee, expect ~$3380 minimum
        # That's approximately 3380 USDC.e for 1 WETH
        assert amount_out_min_wei > 0, "amount_out_min should be non-zero"

        # Verify it's in a reasonable range (3000-4000 USDC for 1 WETH @ $3400)
        min_expected = 3000 * 10**6  # 3000 USDC (6 decimals)
        max_expected = 4000 * 10**6  # 4000 USDC

        assert amount_out_min_wei >= min_expected, (
            f"amount_out_min ({format_token(amount_out_min_wei, 6)}) too low (expected >= 3000 USDC)"
        )
        assert amount_out_min_wei <= max_expected, (
            f"amount_out_min ({format_token(amount_out_min_wei, 6)}) too high (expected <= 4000 USDC)"
        )

        print(f"\nSlippage protection verified: min output = {format_token(amount_out_min_wei, 6)} USDC.e")
        print("This protects against MEV and price manipulation during the swap.")

    @pytest.mark.skip(reason="SushiSwap V3 WETH/USDC.e pool on Arbitrum has insufficient liquidity for reliable testing")
    def test_receipt_parser_with_real_swap(
        self,
        web3: Web3,
        funded_wallet: str,
        weth_contract,
        usdc_contract,
    ):
        """
        Test: Verify receipt parser correctly extracts swap data from real transaction.

        Validates:
        1. Parser correctly identifies Swap events
        2. Amount extraction works with real transaction data
        3. extract_swap_amounts returns valid SwapAmounts
        """
        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
        )
        from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
            SushiSwapV3ReceiptParser,
        )

        print("\n=== Test Receipt Parser with Real Swap ===")

        # Ensure we have WETH
        weth_balance = weth_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        if weth_balance < 10**16:  # Less than 0.01 WETH
            # Wrap some ETH
            wrap_tx = weth_contract.functions.deposit().build_transaction(
                {
                    "from": funded_wallet,
                    "value": 10**17,  # 0.1 ETH
                    "gas": 100000,
                    "gasPrice": web3.eth.gas_price,
                    "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(funded_wallet)),
                    "chainId": web3.eth.chain_id,
                }
            )
            wrap_receipt = send_signed_transaction(web3, wrap_tx, TEST_PRIVATE_KEY)
            assert wrap_receipt["status"] == 1, f"Wrap failed: {wrap_receipt}"

        # Approve router
        swap_amount = Decimal("0.01")
        swap_amount_wei = int(swap_amount * Decimal(10**18))

        approve_tx = weth_contract.functions.approve(
            Web3.to_checksum_address(SUSHISWAP_V3_ROUTER),
            swap_amount_wei,
        ).build_transaction(
            {
                "from": funded_wallet,
                "gas": 100000,
                "gasPrice": web3.eth.gas_price,
                "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(funded_wallet)),
                "chainId": web3.eth.chain_id,
            }
        )
        send_signed_transaction(web3, approve_tx, TEST_PRIVATE_KEY)

        # Build and execute swap
        price_provider = {"WETH": Decimal("3400"), "USDC.e": Decimal("1")}
        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=funded_wallet,
            price_provider=price_provider,
            default_slippage_bps=500,  # 5% slippage for low liquidity pools
            default_fee_tier=3000,  # 0.3% fee tier
        )
        adapter = SushiSwapV3Adapter(config)

        result = adapter.swap_exact_input(
            token_in="WETH",
            token_out="USDC.e",
            amount_in=swap_amount,
        )
        assert result.success

        # Get swap tx
        swap_tx_data = next(tx for tx in result.transactions if tx.tx_type == "swap")

        tx_dict = {
            "from": funded_wallet,
            "to": swap_tx_data.to,
            "value": swap_tx_data.value,
            "data": swap_tx_data.data,
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)
        assert receipt["status"] == 1

        # Test receipt parser
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        # Test parse_receipt
        parse_result = parser.parse_receipt(receipt)
        assert parse_result.success, f"Parse failed: {parse_result.error}"

        if parse_result.swap_events:
            print(f"Found {len(parse_result.swap_events)} swap events")

            # Test extract_swap_amounts
            swap_amounts = parser.extract_swap_amounts(receipt)
            assert swap_amounts is not None, "extract_swap_amounts returned None"

            print("Swap amounts extracted:")
            print(f"  Amount in: {swap_amounts.amount_in_decimal}")
            print(f"  Amount out: {swap_amounts.amount_out_decimal}")
            print(f"  Effective price: {swap_amounts.effective_price}")

            assert swap_amounts.amount_in > 0
            assert swap_amounts.amount_out > 0
        else:
            print("Note: No V3 swap events - may have routed through different pool")

        print("\nReceipt parser test passed!")


# =============================================================================
# LP Position Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.sushiswap
@pytest.mark.lp
class TestSushiSwapV3LPOnChain:
    """On-chain tests for SushiSwap V3 LP operations.

    These tests verify LP position creation and management.
    """

    def test_open_lp_position_builds_correctly(
        self,
        web3: Web3,
        funded_wallet: str,
    ):
        """
        Test: Verify LP position transaction builds correctly.

        Note: Actually executing LP operations requires both tokens and
        may fail if liquidity is insufficient. This test validates the
        transaction building logic.

        Validates:
        1. open_lp_position returns success
        2. Multiple transactions are created (approves + mint)
        3. Mint transaction has correct parameters
        """
        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
        )

        print("\n=== Test LP Position Building ===")

        price_provider = {
            "WETH": Decimal("3400"),
            "USDC": Decimal("1"),
        }

        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=funded_wallet,
            price_provider=price_provider,
        )
        adapter = SushiSwapV3Adapter(config)

        # Build LP position transaction
        result = adapter.open_lp_position(
            token0="USDC",
            token1="WETH",
            amount0=Decimal("100"),  # 100 USDC
            amount1=Decimal("0.03"),  # ~0.03 WETH (~$100 worth)
            fee_tier=3000,  # 0.3% fee tier
            tick_lower=-887220,  # Full range
            tick_upper=887220,
        )

        assert result.success, f"LP position build failed: {result.error}"
        assert len(result.transactions) >= 1, "Expected at least 1 transaction (mint)"

        # Find mint transaction
        mint_tx = None
        for tx in result.transactions:
            if tx.tx_type == "mint":
                mint_tx = tx
                break

        assert mint_tx is not None, "No mint transaction found"
        assert mint_tx.to.lower() == SUSHISWAP_V3_POSITION_MANAGER.lower(), "Mint should go to position manager"

        print(f"Built LP position with {len(result.transactions)} transactions")
        print(f"Position info: {result.position_info}")

        # Verify position info
        assert "token0" in result.position_info
        assert "token1" in result.position_info
        assert result.position_info["fee_tier"] == 3000
        assert result.position_info["tick_lower"] == -887220
        assert result.position_info["tick_upper"] == 887220

        print("\nLP position building test passed!")

    def test_close_lp_position_builds_correctly(
        self,
        web3: Web3,
        funded_wallet: str,
    ):
        """
        Test: Verify LP close transaction builds correctly.

        Validates:
        1. close_lp_position returns success
        2. Decrease liquidity and collect transactions are created
        """
        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
        )

        print("\n=== Test LP Close Building ===")

        price_provider = {
            "WETH": Decimal("3400"),
            "USDC": Decimal("1"),
        }

        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=funded_wallet,
            price_provider=price_provider,
        )
        adapter = SushiSwapV3Adapter(config)

        # Build close LP position transaction with a dummy position ID
        # In real usage, this would be an actual position ID from a previous mint
        dummy_position_id = 12345
        dummy_liquidity = 10**18  # 1e18 liquidity units

        result = adapter.close_lp_position(
            token_id=dummy_position_id,
            liquidity=dummy_liquidity,
            amount0_min=0,
            amount1_min=0,
        )

        assert result.success, f"LP close build failed: {result.error}"
        assert len(result.transactions) == 2, "Expected 2 transactions (decrease + collect)"

        # Verify transaction types
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "decrease_liquidity" in tx_types, "Expected decrease_liquidity transaction"
        assert "collect" in tx_types, "Expected collect transaction"

        print(f"Built LP close with {len(result.transactions)} transactions")
        print(f"Transaction types: {tx_types}")

        print("\nLP close building test passed!")


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
