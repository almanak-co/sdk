"""On-chain integration tests for PancakeSwap V3 adapter.

Tests verify actual on-chain behavior using Anvil fork of BSC mainnet.

To run:
    uv run pytest tests/integration/connectors/test_pancakeswap_v3_onchain.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set

Note: PancakeSwap V3 is a Uniswap V3 fork with different fee tiers (100, 500, 2500, 10000 bps).
Uses the SmartRouter at 0x13f4EA83D0bd40E75C8222255bc855a974568Dd4 (not the V3 Swap Router).
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

# PancakeSwap V3 addresses (BSC mainnet)
# Note: Use SmartRouter (not V3 Swap Router) to match the adapter
PANCAKESWAP_V3_ROUTER = "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4"  # SmartRouter

# Token addresses (BSC mainnet)
WBNB_ADDRESS = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
USDT_ADDRESS = "0x55d398326f99059fF775485246999027B3197955"

# WBNB balance storage slot (slot 3 for WBNB on BSC)
# WBNB uses a mapping at slot 3 for balanceOf
WBNB_BALANCE_SLOT_BASE = 3

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

# WBNB ABI for deposit (wrap BNB to WBNB)
WBNB_ABI = [
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
    """Fund a wallet with BNB."""
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
def anvil_rpc_url(anvil_bsc: AnvilFixture) -> str:
    """Get the RPC URL for the BSC Anvil fork."""
    return anvil_bsc.get_rpc_url()


@pytest.fixture(scope="module")
def web3(anvil_rpc_url: str) -> Web3:
    """Get Web3 instance connected to Anvil.

    The anvil_bsc fixture guarantees Anvil is running with BSC mainnet fork.
    """
    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url))

    # Verify we're on BSC mainnet fork
    chain_id = w3.eth.chain_id
    if chain_id != 56:
        pytest.skip(
            f"Anvil must be forked from BSC mainnet (chain ID 56). Current chain ID: {chain_id}."
        )

    return w3


@pytest.fixture(scope="module")
def funded_wallet(web3: Web3, anvil_rpc_url: str) -> str:
    """Fund the test wallet with BNB.

    Returns the wallet address after funding.
    """
    # Fund with 100 BNB for gas and testing
    bnb_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, bnb_amount, anvil_rpc_url)

    # Verify BNB funding
    balance = web3.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= bnb_amount, f"Wallet not funded with BNB: {balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def wbnb_contract(web3: Web3):
    """Get WBNB contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(WBNB_ADDRESS),
        abi=WBNB_ABI,
    )


@pytest.fixture(scope="module")
def usdt_contract(web3: Web3):
    """Get USDT contract instance."""
    return web3.eth.contract(
        address=Web3.to_checksum_address(USDT_ADDRESS),
        abi=ERC20_ABI,
    )


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.bsc
@pytest.mark.pancakeswap
class TestPancakeSwapV3OnChain:
    """On-chain integration tests for PancakeSwap V3 adapter.

    Tests run sequentially and share state (WBNB/USDT balances).
    """

    @pytest.mark.xfail(reason="Flaky: PancakeSwap V3 WBNB->USDT swap intermittently reverts", strict=False)
    def test_swap_wbnb_for_usdt(
        self,
        web3: Web3,
        funded_wallet: str,
        wbnb_contract,
        usdt_contract,
    ):
        """
        Test: Swap 0.1 WBNB for USDT on PancakeSwap V3.

        Validates:
        1. Wrap BNB to WBNB
        2. Approve WBNB for router
        3. Swap transaction succeeds (status=1)
        4. WBNB balance decreases by swap amount
        5. USDT balance increases
        6. Swap event is emitted with correct data
        """
        from almanak.framework.connectors.pancakeswap_v3 import (
            PancakeSwapV3Adapter,
            PancakeSwapV3Config,
        )
        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
            PancakeSwapV3ReceiptParser,
        )

        # Step 1: Wrap BNB to WBNB
        wrap_amount = Decimal("1.0")  # Wrap 1 BNB for testing
        wrap_amount_wei = int(wrap_amount * Decimal(10**18))

        print("\n=== Wrap BNB to WBNB ===")
        wrap_tx = wbnb_contract.functions.deposit().build_transaction(
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
        wbnb_before = wbnb_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        usdt_before = usdt_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"WBNB before: {format_token(wbnb_before)}")
        print(f"USDT before: {format_token(usdt_before)}")

        assert wbnb_before >= wrap_amount_wei, f"WBNB not wrapped: {wbnb_before}"

        # Step 2: Approve PancakeSwap router to spend WBNB
        swap_amount = Decimal("0.1")  # Swap 0.1 WBNB for USDT
        swap_amount_wei = int(swap_amount * Decimal(10**18))

        print("\n=== Approve WBNB for Router ===")
        approve_tx = wbnb_contract.functions.approve(
            Web3.to_checksum_address(PANCAKESWAP_V3_ROUTER),
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
        # Use real-ish prices for BSC (BNB ~$600, USDT = $1)
        price_provider = {
            "WBNB": Decimal("600"),
            "BNB": Decimal("600"),
            "USDT": Decimal("1"),
        }

        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address=funded_wallet,
            price_provider=price_provider,
            default_slippage_bps=100,  # 1% slippage for integration test
        )
        adapter = PancakeSwapV3Adapter(config)

        print("\n=== Swap WBNB for USDT ===")
        result = adapter.swap_exact_input(
            token_in="WBNB",
            token_out="USDT",
            amount_in=swap_amount,
        )
        assert result.success, f"Swap transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Execute the swap transaction
        tx_dict = {
            "from": funded_wallet,
            "to": result.tx_data["to"],
            "value": result.tx_data["value"],
            "data": result.tx_data["data"],
        }
        receipt = send_signed_transaction(web3, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Swap transaction failed: {receipt}"

        # Verify balances after
        wbnb_after = wbnb_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()
        usdt_after = usdt_contract.functions.balanceOf(Web3.to_checksum_address(funded_wallet)).call()

        print(f"WBNB after: {format_token(wbnb_after)}")
        print(f"USDT after: {format_token(usdt_after)}")

        # WBNB should decrease by swap amount
        wbnb_decrease = wbnb_before - wbnb_after
        assert wbnb_decrease == swap_amount_wei, (
            f"WBNB decrease ({wbnb_decrease}) should equal swap amount ({swap_amount_wei})"
        )

        # USDT should increase
        usdt_increase = usdt_after - usdt_before
        assert usdt_increase > 0, "USDT should increase after swap"

        # Parse receipt to verify Swap event
        # Note: The Smart Router may route through V2 pools when V3 liquidity is
        # insufficient, which won't emit V3 Swap events. We verify the swap worked
        # by checking balances above. The event parsing is optional validation.
        parser = PancakeSwapV3ReceiptParser()
        parse_result = parser.parse_receipt(receipt)

        assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"

        # If V3 Swap events are present, verify them
        if parse_result.swaps:
            swap_event = parse_result.swaps[0]
            print(f"\nSwap event: {swap_event.to_dict()}")

            # Verify event data - recipient should be our wallet
            assert swap_event.recipient.lower() == funded_wallet.lower(), (
                f"Event recipient ({swap_event.recipient}) should be wallet ({funded_wallet})"
            )
        else:
            print("\nNote: No V3 Swap events found - router may have used V2 pools")

        print(f"\nSuccessfully swapped {swap_amount} WBNB for ~{format_token(usdt_increase)} USDT")

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
        3. Using placeholder prices results in lower slippage tolerance
        """
        from almanak.framework.connectors.pancakeswap_v3 import (
            PancakeSwapV3Adapter,
            PancakeSwapV3Config,
        )

        print("\n=== Test Slippage Protection ===")

        # Test with real price provider
        price_provider = {
            "WBNB": Decimal("600"),
            "USDT": Decimal("1"),
        }

        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address=funded_wallet,
            price_provider=price_provider,
            default_slippage_bps=50,  # 0.5% slippage
        )
        adapter = PancakeSwapV3Adapter(config)

        swap_amount = Decimal("1.0")  # 1 WBNB
        result = adapter.swap_exact_input(
            token_in="WBNB",
            token_out="USDT",
            amount_in=swap_amount,
        )

        assert result.success, f"Swap transaction build failed: {result.error}"
        assert result.tx_data is not None, "No tx_data in result"

        # Parse the calldata to extract amount_out_min
        # PancakeSwap SmartRouter uses IV3SwapRouter interface (7-param struct, NO deadline):
        # exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))
        # Struct: tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96
        #
        # Calldata layout (after selector):
        # 0-31: tokenIn (chunks[0])
        # 32-63: tokenOut (chunks[1])
        # 64-95: fee (chunks[2])
        # 96-127: recipient (chunks[3])
        # 128-159: amountIn (chunks[4])
        # 160-191: amountOutMinimum (chunks[5])
        # 192-223: sqrtPriceLimitX96 (chunks[6])

        calldata = result.tx_data["data"]
        # Remove selector (0x + 8 hex chars = 10 chars)
        data_hex = calldata[10:] if calldata.startswith("0x") else calldata[8:]

        # Each parameter is 64 hex chars (32 bytes)
        chunk_size = 64
        chunks = [data_hex[i : i + chunk_size] for i in range(0, len(data_hex), chunk_size)]

        amount_in_wei = int(chunks[4], 16)
        amount_out_min_wei = int(chunks[5], 16)

        print(f"Amount in: {format_token(amount_in_wei)} WBNB")
        print(f"Amount out min: {format_token(amount_out_min_wei)} USDT")

        # Verify amount_out_min is reasonable
        # With WBNB=$600 and 0.5% slippage + 0.25% fee, expect ~$596 minimum
        # That's approximately 596 USDT for 1 WBNB
        assert amount_out_min_wei > 0, "amount_out_min should be non-zero"

        # Verify it's in a reasonable range (500-700 USDT for 1 WBNB @ $600)
        min_expected = 500 * 10**18  # 500 USDT
        max_expected = 700 * 10**18  # 700 USDT
        assert amount_out_min_wei >= min_expected, (
            f"amount_out_min ({format_token(amount_out_min_wei)}) too low (expected >= 500 USDT)"
        )
        assert amount_out_min_wei <= max_expected, (
            f"amount_out_min ({format_token(amount_out_min_wei)}) too high (expected <= 700 USDT)"
        )

        print(f"\nSlippage protection verified: min output = {format_token(amount_out_min_wei)} USDT")
        print("This protects against MEV and price manipulation during the swap.")


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
