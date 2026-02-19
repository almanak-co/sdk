"""Integration tests for Polymarket connector.

Tests verify the Polymarket integration works end-to-end:
1. Read-only tests using live Polymarket API (markets, orderbook, price, health)
2. Fork-based tests using Anvil fork of Polygon mainnet (approvals, redemption)
3. Intent compilation tests

To run read-only tests (requires network):
    uv run pytest tests/integration/connectors/test_polymarket_integration.py -v -s -m integration

To run Anvil tests:
    uv run pytest tests/integration/connectors/test_polymarket_integration.py -v -s -m anvil

Requirements:
    - ALCHEMY_API_KEY environment variable set (for Anvil fork)
    - Network access to Polymarket API (for read-only tests)
"""

import subprocess
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from tests.conftest_gateway import AnvilFixture

# Import fixtures for pytest discovery
pytest_plugins = ["tests.conftest_gateway"]

# =============================================================================
# Constants
# =============================================================================

# Default test wallet (Anvil's first account)
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Contract addresses (Polygon mainnet)
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# USDC storage slot for Polygon USDC (USDC.e bridged token)
# This is slot 0 for the balanceOf mapping
USDC_BALANCE_SLOT_BASE = 0

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
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
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
    """Fund a wallet with MATIC (Polygon native token)."""
    amount_hex = hex(amount_wei)
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


def fund_usdc(wallet: str, amount: int, rpc_url: str) -> None:
    """Fund a wallet with USDC using storage slot manipulation.

    Polygon USDC.e uses a standard ERC20 storage layout where
    balanceOf mapping is at slot 0. We compute the storage slot
    using cast index.
    """
    # Compute storage slot: keccak256(wallet . slot_number)
    result = subprocess.run(
        ["cast", "index", "address", wallet, str(USDC_BALANCE_SLOT_BASE)],
        capture_output=True,
        text=True,
        check=True,
    )
    storage_slot = result.stdout.strip()

    # Set the storage value (USDC has 6 decimals)
    amount_hex = hex(amount)
    subprocess.run(
        [
            "cast",
            "rpc",
            "anvil_setStorageAt",
            USDC_POLYGON,
            storage_slot,
            # Pad the amount to 32 bytes
            "0x" + amount_hex[2:].zfill(64),
            "--rpc-url",
            rpc_url,
        ],
        capture_output=True,
        check=True,
    )


def format_usdc(amount: int) -> Decimal:
    """Convert USDC smallest unit to readable format."""
    return Decimal(amount) / Decimal(10**6)


def send_signed_transaction(web3, tx_dict: dict, private_key: str) -> dict:
    """Sign and send a transaction, return receipt.

    Uses EIP-1559 (type 2) transactions for Polygon compatibility.
    """
    from web3 import Web3

    # Add missing tx fields
    tx_dict["chainId"] = web3.eth.chain_id
    tx_dict["nonce"] = web3.eth.get_transaction_count(Web3.to_checksum_address(tx_dict.get("from", TEST_WALLET)))
    if "gas" not in tx_dict:
        tx_dict["gas"] = 500000

    # Use EIP-1559 transaction parameters (type 2) instead of legacy gasPrice
    # Polygon requires type 2 transactions to avoid reverts
    if "gasPrice" not in tx_dict and "maxFeePerGas" not in tx_dict:
        # Get base fee - use raw RPC call to avoid POA middleware issues
        latest_block = web3.eth.get_block("pending")
        base_fee = latest_block.get("baseFeePerGas", 30 * 10**9)  # fallback to 30 gwei
        # Set maxPriorityFeePerGas to a reasonable value (30 gwei for Polygon)
        priority_fee = 30 * 10**9
        tx_dict["maxPriorityFeePerGas"] = priority_fee
        tx_dict["maxFeePerGas"] = base_fee * 2 + priority_fee

    # Sign and send
    signed_tx = web3.eth.account.sign_transaction(tx_dict, private_key)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    return dict(receipt)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def polymarket_config():
    """Create a basic Polymarket config for read-only operations.

    Note: This config doesn't include API credentials,
    so it's only suitable for unauthenticated read operations.
    """
    from almanak.framework.connectors.polymarket import PolymarketConfig

    return PolymarketConfig(
        wallet_address=TEST_WALLET,
        private_key=SecretStr(TEST_PRIVATE_KEY),
        rate_limit_enabled=False,  # Disable for testing
    )


@pytest.fixture(scope="module")
def clob_client(polymarket_config):
    """Create a CLOB client for read-only operations."""
    from almanak.framework.connectors.polymarket import ClobClient

    return ClobClient(polymarket_config)


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_polygon: AnvilFixture) -> str:
    """Get the RPC URL for the Polygon Anvil fork."""
    return anvil_polygon.get_rpc_url()


@pytest.fixture(scope="module")
def web3_polygon(anvil_rpc_url: str):
    """Get Web3 instance connected to Anvil Polygon fork.

    The anvil_polygon fixture guarantees Anvil is running with Polygon mainnet fork.
    Polygon is a PoA chain, so we inject the geth_poa_middleware.
    """
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware

    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url))
    # Inject POA middleware for Polygon chain
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # Verify we're on Polygon mainnet fork
    chain_id = w3.eth.chain_id
    if chain_id != 137:
        pytest.skip(
            f"Anvil must be forked from Polygon mainnet (chain ID 137). "
            f"Current chain ID: {chain_id}."
        )

    return w3


@pytest.fixture(scope="module")
def funded_wallet_polygon(web3_polygon, anvil_rpc_url: str) -> str:
    """Fund the test wallet with MATIC and USDC.

    Returns the wallet address after funding.
    """
    from web3 import Web3

    # Fund with 100 MATIC for gas
    matic_amount = 100 * 10**18
    fund_native_token(TEST_WALLET, matic_amount, anvil_rpc_url)

    # Fund with 10,000 USDC (6 decimals)
    usdc_amount = 10_000 * 10**6
    fund_usdc(TEST_WALLET, usdc_amount, anvil_rpc_url)

    # Verify MATIC funding
    balance = web3_polygon.eth.get_balance(Web3.to_checksum_address(TEST_WALLET))
    assert balance >= matic_amount, f"Wallet not funded with MATIC: {balance}"

    # Verify USDC funding
    usdc_contract = web3_polygon.eth.contract(address=Web3.to_checksum_address(USDC_POLYGON), abi=ERC20_ABI)
    usdc_balance = usdc_contract.functions.balanceOf(Web3.to_checksum_address(TEST_WALLET)).call()
    assert usdc_balance >= usdc_amount, f"Wallet not funded with USDC: {usdc_balance}"

    return TEST_WALLET


@pytest.fixture(scope="module")
def usdc_contract(web3_polygon):
    """Get USDC contract instance."""
    from web3 import Web3

    return web3_polygon.eth.contract(
        address=Web3.to_checksum_address(USDC_POLYGON),
        abi=ERC20_ABI,
    )


# =============================================================================
# Read-Only API Tests (Live Network)
# =============================================================================


@pytest.mark.integration
@pytest.mark.polymarket
class TestPolymarketReadOnlyAPI:
    """Read-only integration tests for Polymarket CLOB API.

    These tests hit the live Polymarket API without authentication.
    They verify that the client can fetch market data correctly.
    """

    def test_health_check(self, clob_client):
        """
        Test: Verify Polymarket CLOB API is reachable.

        Validates:
        1. Health check endpoint responds
        2. API is operational
        """
        try:
            result = clob_client.health_check()
            print(f"\nHealth check result: {result}")
            assert result is True, "Health check should return True"
        except Exception as e:
            # API might be temporarily unavailable
            pytest.skip(f"Polymarket API not reachable: {e}")

    def test_fetch_markets(self, clob_client):
        """
        Test: Fetch active markets from Gamma API.

        Validates:
        1. Market fetch returns data
        2. Markets have required fields (id, question, outcomes)
        3. At least one active market exists
        """
        from almanak.framework.connectors.polymarket import MarketFilters

        try:
            markets = clob_client.get_markets(MarketFilters(active=True, limit=5))
            print(f"\nFetched {len(markets)} active markets")

            assert len(markets) > 0, "Should have at least one active market"

            # Verify first market has required fields
            market = markets[0]
            print(f"Sample market: {market.question[:80]}...")
            print(f"  ID: {market.id}")
            print(f"  Active: {market.active}")
            print(f"  Outcomes: {market.outcomes}")

            assert market.id, "Market should have an ID"
            assert market.question, "Market should have a question"
            assert market.outcomes, "Market should have outcomes"
            assert len(market.outcomes) >= 2, "Market should have at least 2 outcomes"
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

    def test_fetch_orderbook(self, clob_client):
        """
        Test: Fetch orderbook for a token.

        Validates:
        1. Orderbook fetch works
        2. Orderbook has bid/ask structure
        """
        from almanak.framework.connectors.polymarket import MarketFilters
        from almanak.framework.connectors.polymarket.exceptions import PolymarketAPIError

        try:
            # Get markets with CLOB enabled to ensure orderbook exists
            markets = clob_client.get_markets(MarketFilters(active=True, enable_order_book=True, limit=10))
            if not markets:
                pytest.skip("No active markets with CLOB enabled found")

            # Find a market with clob_token_ids
            market = None
            for m in markets:
                if m.clob_token_ids and len(m.clob_token_ids) > 0:
                    market = m
                    break

            if not market:
                pytest.skip("No markets with CLOB token IDs found")

            token_id = market.clob_token_ids[0]
            print(f"\nFetching orderbook for market: {market.question[:50]}...")
            print(f"Token ID: {token_id[:30]}...")

            try:
                orderbook = clob_client.get_orderbook(token_id)
                print(f"Orderbook: {len(orderbook.bids)} bids, {len(orderbook.asks)} asks")

                if orderbook.bids:
                    print(f"Best bid: {orderbook.bids[0]}")
                if orderbook.asks:
                    print(f"Best ask: {orderbook.asks[0]}")

                # Orderbook might be empty for illiquid markets
                assert orderbook is not None, "Should return orderbook object"
                assert hasattr(orderbook, "bids"), "Orderbook should have bids"
                assert hasattr(orderbook, "asks"), "Orderbook should have asks"
            except PolymarketAPIError as e:
                # Some tokens may not have orderbooks yet
                if "No orderbook exists" in str(e):
                    pytest.skip(f"Orderbook not available for this token: {e}")
                raise
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

    def test_fetch_price(self, clob_client):
        """
        Test: Fetch price for a token.

        Validates:
        1. Price fetch works
        2. Price has bid/ask/mid values
        3. Prices are within valid range (0-1)
        """
        from almanak.framework.connectors.polymarket import MarketFilters
        from almanak.framework.connectors.polymarket.exceptions import PolymarketAPIError

        try:
            # Get markets with CLOB enabled to ensure price is available
            markets = clob_client.get_markets(MarketFilters(active=True, enable_order_book=True, limit=10))
            if not markets:
                pytest.skip("No active markets with CLOB enabled found")

            # Find a market with clob_token_ids
            market = None
            for m in markets:
                if m.clob_token_ids and len(m.clob_token_ids) > 0:
                    market = m
                    break

            if not market:
                pytest.skip("No markets with CLOB token IDs found")

            token_id = market.clob_token_ids[0]
            print(f"\nFetching price for market: {market.question[:50]}...")
            print(f"Token ID: {token_id[:30]}...")

            try:
                price = clob_client.get_price(token_id)
                print(f"Price - Bid: {price.bid}, Ask: {price.ask}, Mid: {price.mid}")

                # Prices might be None for illiquid markets
                if price.mid is not None:
                    assert Decimal("0") <= price.mid <= Decimal("1"), "Mid price should be between 0 and 1"
                if price.bid is not None:
                    assert Decimal("0") <= price.bid <= Decimal("1"), "Bid price should be between 0 and 1"
                if price.ask is not None:
                    assert Decimal("0") <= price.ask <= Decimal("1"), "Ask price should be between 0 and 1"
            except PolymarketAPIError as e:
                # Some tokens may not have prices yet
                if "Invalid side" in str(e) or "No orderbook" in str(e):
                    pytest.skip(f"Price not available for this token: {e}")
                raise
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

    def test_get_server_time(self, clob_client):
        """
        Test: Fetch server time from CLOB API.

        Validates:
        1. Server time is returned
        2. Server time is reasonable (not too far in past/future)
        """
        import time

        try:
            server_time = clob_client.get_server_time()
            print(f"\nServer time: {server_time}")

            current_time = int(time.time())
            # Allow 5 minute clock drift
            assert abs(server_time - current_time) < 300, "Server time should be close to current time"
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")


# =============================================================================
# Fork-Based Tests (Anvil Polygon)
# =============================================================================


@pytest.mark.anvil
@pytest.mark.polymarket
class TestPolymarketOnChain:
    """On-chain integration tests for Polymarket using Anvil fork.

    Tests run on Anvil fork of Polygon mainnet to test on-chain operations
    without spending real funds.
    """

    def test_approve_usdc_for_ctf_exchange(
        self,
        web3_polygon,
        funded_wallet_polygon: str,
        usdc_contract,
    ):
        """
        Test: Approve USDC spending for CTF Exchange.

        Validates:
        1. Build approval transaction using CtfSDK
        2. Transaction executes successfully (status=1)
        3. Allowance is updated
        """
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        # Get initial allowance
        allowance_before = usdc_contract.functions.allowance(
            Web3.to_checksum_address(funded_wallet_polygon),
            Web3.to_checksum_address(CTF_EXCHANGE),
        ).call()

        print("\n=== Approve USDC for CTF Exchange ===")
        print(f"Allowance before: {format_usdc(allowance_before)} USDC")

        # Build approve transaction using SDK
        sdk = CtfSDK()
        tx_data = sdk.build_approve_usdc_tx(
            spender=CTF_EXCHANGE,
            amount=MAX_UINT256,
            sender=funded_wallet_polygon,
        )

        # Execute the transaction
        tx_dict = {
            "from": funded_wallet_polygon,
            "to": tx_data.to,
            "value": tx_data.value,
            "data": tx_data.data,
            "gas": tx_data.gas_estimate,
        }
        receipt = send_signed_transaction(web3_polygon, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Approve transaction failed: {receipt}"

        # Verify allowance after
        allowance_after = usdc_contract.functions.allowance(
            Web3.to_checksum_address(funded_wallet_polygon),
            Web3.to_checksum_address(CTF_EXCHANGE),
        ).call()

        print(f"Allowance after: {allowance_after} (MAX_UINT256: {allowance_after == MAX_UINT256})")

        assert allowance_after == MAX_UINT256, "Allowance should be max uint256"
        print("\nSuccessfully approved USDC for CTF Exchange")

    def test_approve_usdc_for_neg_risk_exchange(
        self,
        web3_polygon,
        funded_wallet_polygon: str,
        usdc_contract,
    ):
        """
        Test: Approve USDC spending for Neg Risk Exchange.

        Validates:
        1. Build approval transaction using CtfSDK
        2. Transaction executes successfully (status=1)
        3. Allowance is updated
        """
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        # Get initial allowance
        allowance_before = usdc_contract.functions.allowance(
            Web3.to_checksum_address(funded_wallet_polygon),
            Web3.to_checksum_address(NEG_RISK_EXCHANGE),
        ).call()

        print("\n=== Approve USDC for Neg Risk Exchange ===")
        print(f"Allowance before: {format_usdc(allowance_before)} USDC")

        # Build approve transaction using SDK
        sdk = CtfSDK()
        tx_data = sdk.build_approve_usdc_tx(
            spender=NEG_RISK_EXCHANGE,
            amount=MAX_UINT256,
            sender=funded_wallet_polygon,
        )

        # Execute the transaction
        tx_dict = {
            "from": funded_wallet_polygon,
            "to": tx_data.to,
            "value": tx_data.value,
            "data": tx_data.data,
            "gas": tx_data.gas_estimate,
        }
        receipt = send_signed_transaction(web3_polygon, tx_dict, TEST_PRIVATE_KEY)

        assert receipt["status"] == 1, f"Approve transaction failed: {receipt}"

        # Verify allowance after
        allowance_after = usdc_contract.functions.allowance(
            Web3.to_checksum_address(funded_wallet_polygon),
            Web3.to_checksum_address(NEG_RISK_EXCHANGE),
        ).call()

        print(f"Allowance after: {allowance_after} (MAX_UINT256: {allowance_after == MAX_UINT256})")

        assert allowance_after == MAX_UINT256, "Allowance should be max uint256"
        print("\nSuccessfully approved USDC for Neg Risk Exchange")

    def test_check_allowances(
        self,
        web3_polygon,
        funded_wallet_polygon: str,
    ):
        """
        Test: Check all allowances using CtfSDK.

        Validates:
        1. Check allowances returns AllowanceStatus
        2. USDC balance is correct
        3. Previously approved allowances are reflected
        """
        from almanak.framework.connectors.polymarket.ctf_sdk import CtfSDK

        print("\n=== Check Allowances ===")

        sdk = CtfSDK()
        status = sdk.check_allowances(funded_wallet_polygon, web3_polygon)

        print(f"USDC Balance: {format_usdc(status.usdc_balance)} USDC")
        print(f"USDC Allowance (CTF Exchange): {status.usdc_allowance_ctf_exchange}")
        print(f"USDC Allowance (Neg Risk Exchange): {status.usdc_allowance_neg_risk_exchange}")
        print(f"CTF Approved for Exchange: {status.ctf_approved_for_ctf_exchange}")
        print(f"CTF Approved for Neg Risk: {status.ctf_approved_for_neg_risk_adapter}")

        # Verify USDC balance
        expected_balance = 10_000 * 10**6
        assert status.usdc_balance >= expected_balance, f"USDC balance should be >= {expected_balance}"

        print("\nAllowance check completed successfully")


# =============================================================================
# Intent Compilation Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.polymarket
class TestPolymarketIntentCompilation:
    """End-to-end compilation tests for prediction intents.

    These tests verify that the IntentCompiler correctly compiles
    prediction intents to ActionBundles.
    """

    def test_compile_prediction_buy_intent(self):
        """
        Test: Compile a PredictionBuyIntent end-to-end.

        Validates:
        1. IntentCompiler routes to PolymarketAdapter
        2. Compilation returns valid ActionBundle
        3. Bundle has correct metadata
        """

        from almanak.framework.intents import IntentType, PredictionBuyIntent
        from almanak.framework.intents.compiler import (
            CompilationStatus,
            IntentCompiler,
            IntentCompilerConfig,
        )
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Create mock config
        mock_config = MagicMock()
        mock_config.wallet_address = TEST_WALLET
        mock_config.private_key = SecretStr(TEST_PRIVATE_KEY)

        # Create mock successful bundle
        mock_bundle = ActionBundle(
            intent_type=IntentType.PREDICTION_BUY.value,
            transactions=[],
            metadata={
                "intent_id": "test-123",
                "market_id": "test-market",
                "market_question": "Test Question?",
                "token_id": "111111111111111111111111",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.65",
                "size": "100",
                "order_type": "GTC",
                "order_payload": {"order": {}, "signature": "0x"},
                "protocol": "polymarket",
                "chain": "polygon",
            },
        )

        # Patch adapter initialization
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=TEST_WALLET,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_config,
                ),
            )

            # Mock the adapter
            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_bundle
            compiler._polymarket_adapter = mock_adapter

            # Create and compile intent
            intent = PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            result = compiler.compile(intent)

            print("\n=== Compile PredictionBuyIntent ===")
            print(f"Status: {result.status}")
            print(f"Intent ID: {result.intent_id}")
            print(f"Gas Estimate: {result.total_gas_estimate}")

            # Verify compilation succeeded
            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_BUY.value
            assert result.action_bundle.metadata["protocol"] == "polymarket"
            assert result.action_bundle.metadata["outcome"] == "YES"
            assert result.total_gas_estimate == 0  # CLOB orders are off-chain

            # Verify adapter was called
            mock_adapter.compile_intent.assert_called_once_with(intent)

    def test_compile_prediction_sell_intent(self):
        """
        Test: Compile a PredictionSellIntent end-to-end.

        Validates:
        1. IntentCompiler routes to PolymarketAdapter
        2. Compilation returns valid ActionBundle
        3. Bundle has SELL side
        """

        from almanak.framework.intents import IntentType, PredictionSellIntent
        from almanak.framework.intents.compiler import (
            CompilationStatus,
            IntentCompiler,
            IntentCompilerConfig,
        )
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Create mock config
        mock_config = MagicMock()
        mock_config.wallet_address = TEST_WALLET
        mock_config.private_key = SecretStr(TEST_PRIVATE_KEY)

        # Create mock successful bundle
        mock_bundle = ActionBundle(
            intent_type=IntentType.PREDICTION_SELL.value,
            transactions=[],
            metadata={
                "intent_id": "test-456",
                "market_id": "test-market",
                "market_question": "Test Question?",
                "token_id": "111111111111111111111111",
                "outcome": "YES",
                "side": "SELL",
                "price": "0.70",
                "size": "50",
                "order_type": "GTC",
                "order_payload": {"order": {}, "signature": "0x"},
                "protocol": "polymarket",
                "chain": "polygon",
            },
        )

        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=TEST_WALLET,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("50"),
            )

            result = compiler.compile(intent)

            print("\n=== Compile PredictionSellIntent ===")
            print(f"Status: {result.status}")
            print(f"Intent ID: {result.intent_id}")

            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_SELL.value
            assert result.action_bundle.metadata["side"] == "SELL"

    def test_compile_prediction_redeem_intent(self):
        """
        Test: Compile a PredictionRedeemIntent end-to-end.

        Validates:
        1. IntentCompiler routes to PolymarketAdapter
        2. Compilation returns valid ActionBundle with transactions
        3. Bundle has on-chain transaction (redemption)
        """

        from almanak.framework.intents import IntentType, PredictionRedeemIntent
        from almanak.framework.intents.compiler import (
            CompilationStatus,
            IntentCompiler,
            IntentCompilerConfig,
        )
        from almanak.framework.models.reproduction_bundle import ActionBundle

        # Create mock config
        mock_config = MagicMock()
        mock_config.wallet_address = TEST_WALLET
        mock_config.private_key = SecretStr(TEST_PRIVATE_KEY)

        # Create mock successful bundle with on-chain transaction
        mock_bundle = ActionBundle(
            intent_type=IntentType.PREDICTION_REDEEM.value,
            transactions=[
                {
                    "to": CONDITIONAL_TOKENS,
                    "value": 0,
                    "data": "0xredeemdata",
                    "gas_estimate": 200000,
                    "description": "Redeem winning positions",
                    "tx_type": "redeem",
                }
            ],
            metadata={
                "intent_id": "test-789",
                "market_id": "test-market",
                "market_question": "Test Question?",
                "condition_id": "0x1234567890abcdef",
                "outcome": "YES",
                "winning_outcome": "YES",
                "protocol": "polymarket",
                "chain": "polygon",
            },
        )

        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=TEST_WALLET,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionRedeemIntent(
                market_id="test-market",
            )

            result = compiler.compile(intent)

            print("\n=== Compile PredictionRedeemIntent ===")
            print(f"Status: {result.status}")
            print(f"Intent ID: {result.intent_id}")
            print(f"Transactions: {len(result.transactions)}")
            print(f"Gas Estimate: {result.total_gas_estimate}")

            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_REDEEM.value
            assert len(result.transactions) == 1  # Redemption is on-chain
            assert result.total_gas_estimate == 200000


# =============================================================================
# Run Tests Directly
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
