"""
Tests for Pendle Protocol SDK

These tests verify the Pendle SDK functions correctly on an Anvil fork.
Tests include:
- SDK initialization
- Transaction building for swaps
- Transaction building for liquidity operations
- Receipt parsing

To run these tests:
    pytest tests/connectors/pendle/test_pendle_sdk.py -v

For on-chain tests (requires Anvil):
    pytest tests/connectors/pendle/test_pendle_sdk.py -v -m onchain
"""

import os

import pytest
from web3 import Web3

from almanak.framework.connectors.pendle import (
    PENDLE_ADDRESSES,
    PendleAdapter,
    PendleEventType,
    PendleLPParams,
    PendleReceiptParser,
    PendleSDK,
    PendleSwapParams,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def rpc_url():
    """Get RPC URL for testing."""
    # Use Alchemy if available, otherwise localhost Anvil
    alchemy_key = os.environ.get("ALCHEMY_API_KEY")
    if alchemy_key:
        return f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    return "http://localhost:8545"


@pytest.fixture
def anvil_rpc():
    """Anvil RPC URL for forked testing."""
    return "http://localhost:8545"


@pytest.fixture
def sdk(rpc_url):
    """Create PendleSDK instance."""
    return PendleSDK(rpc_url=rpc_url, chain="arbitrum")


@pytest.fixture
def adapter(rpc_url):
    """Create PendleAdapter instance."""
    return PendleAdapter(rpc_url=rpc_url, chain="arbitrum")


@pytest.fixture
def parser():
    """Create PendleReceiptParser instance."""
    return PendleReceiptParser(chain="arbitrum")


@pytest.fixture
def test_wallet():
    """Test wallet address."""
    return "0x1234567890123456789012345678901234567890"


@pytest.fixture
def wsteth_market():
    """wstETH market address on Arbitrum."""
    return PENDLE_ADDRESSES["arbitrum"]["MARKET_WSTETH_26JUN2025"]


@pytest.fixture
def weth_address():
    """WETH address on Arbitrum."""
    return PENDLE_ADDRESSES["arbitrum"]["WETH"]


# =============================================================================
# SDK Initialization Tests
# =============================================================================


class TestSDKInitialization:
    """Test SDK initialization."""

    def test_sdk_initializes_for_arbitrum(self, rpc_url):
        """SDK should initialize correctly for Arbitrum."""
        sdk = PendleSDK(rpc_url=rpc_url, chain="arbitrum")

        assert sdk.chain == "arbitrum"
        assert sdk.router_address == PENDLE_ADDRESSES["arbitrum"]["ROUTER"]
        assert sdk.web3 is not None

    def test_sdk_initializes_for_ethereum(self, rpc_url):
        """SDK should initialize correctly for Ethereum."""
        # Use Ethereum RPC for this test
        eth_rpc = rpc_url.replace("arb-mainnet", "eth-mainnet")
        sdk = PendleSDK(rpc_url=eth_rpc, chain="ethereum")

        assert sdk.chain == "ethereum"
        assert sdk.router_address == PENDLE_ADDRESSES["ethereum"]["ROUTER"]

    def test_sdk_rejects_unsupported_chain(self, rpc_url):
        """SDK should reject unsupported chains."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            PendleSDK(rpc_url=rpc_url, chain="polygon")


# =============================================================================
# Transaction Building Tests
# =============================================================================


class TestSwapTransactionBuilding:
    """Test swap transaction building."""

    def test_build_swap_exact_token_for_pt(self, sdk, test_wallet, wsteth_market, weth_address):
        """Should build swap token -> PT transaction."""
        amount_in = 10**18  # 1 WETH
        min_pt_out = 10**18  # 1 PT minimum

        tx = sdk.build_swap_exact_token_for_pt(
            receiver=test_wallet,
            market=wsteth_market,
            token_in=weth_address,
            amount_in=amount_in,
            min_pt_out=min_pt_out,
            slippage_bps=50,
        )

        assert tx.to == sdk.router_address
        assert tx.value == 0  # WETH is ERC-20, no ETH value (only native ETH uses msg.value)
        assert tx.data.startswith("0x")
        assert tx.gas_estimate > 0
        assert "PT" in tx.description

    def test_build_swap_exact_pt_for_token(self, sdk, test_wallet, wsteth_market, weth_address):
        """Should build swap PT -> token transaction."""
        pt_amount = 10**18  # 1 PT
        min_token_out = 10**18  # 1 token minimum

        tx = sdk.build_swap_exact_pt_for_token(
            receiver=test_wallet,
            market=wsteth_market,
            pt_amount=pt_amount,
            token_out=weth_address,
            min_token_out=min_token_out,
            slippage_bps=50,
        )

        assert tx.to == sdk.router_address
        assert tx.value == 0  # No ETH for PT->token swaps
        assert tx.data.startswith("0x")
        assert tx.gas_estimate > 0

    def test_slippage_applied_correctly(self, sdk, test_wallet, wsteth_market, weth_address):
        """Slippage should be applied to minimum output."""
        min_pt_out = 1000  # 1000 units

        # With 100 bps (1%) slippage
        tx_with_slippage = sdk.build_swap_exact_token_for_pt(
            receiver=test_wallet,
            market=wsteth_market,
            token_in=weth_address,
            amount_in=10**18,
            min_pt_out=min_pt_out,
            slippage_bps=100,
        )

        # The min output should be 990 (1000 * 0.99)
        assert tx_with_slippage is not None
        assert "990" in tx_with_slippage.description


class TestLiquidityTransactionBuilding:
    """Test liquidity transaction building."""

    def test_build_add_liquidity_single_token(self, sdk, test_wallet, wsteth_market, weth_address):
        """Should build add liquidity transaction."""
        amount_in = 10**18  # 1 WETH
        min_lp_out = 10**17  # 0.1 LP minimum

        tx = sdk.build_add_liquidity_single_token(
            receiver=test_wallet,
            market=wsteth_market,
            token_in=weth_address,
            amount_in=amount_in,
            min_lp_out=min_lp_out,
            slippage_bps=50,
        )

        assert tx.to == sdk.router_address
        assert tx.data.startswith("0x")
        assert tx.gas_estimate > 0
        assert "liquidity" in tx.description.lower()

    def test_build_remove_liquidity_single_token(self, sdk, test_wallet, wsteth_market, weth_address):
        """Should build remove liquidity transaction."""
        lp_amount = 10**18  # 1 LP
        min_token_out = 10**17  # 0.1 token minimum

        tx = sdk.build_remove_liquidity_single_token(
            receiver=test_wallet,
            market=wsteth_market,
            lp_amount=lp_amount,
            token_out=weth_address,
            min_token_out=min_token_out,
            slippage_bps=50,
        )

        assert tx.to == sdk.router_address
        assert tx.value == 0  # No ETH for LP removal
        assert tx.data.startswith("0x")
        assert tx.gas_estimate > 0


class TestApprovalBuilding:
    """Test approval transaction building."""

    def test_build_approve_tx(self, sdk, weth_address):
        """Should build ERC20 approval transaction."""
        tx = sdk.build_approve_tx(token_address=weth_address)

        assert tx.to == weth_address
        assert tx.value == 0
        assert tx.data.startswith("0x095ea7b3")  # approve selector
        assert tx.gas_estimate > 0


# =============================================================================
# Adapter Tests
# =============================================================================


class TestPendleAdapter:
    """Test PendleAdapter functionality."""

    def test_adapter_supports_swap(self, adapter):
        """Adapter should support SWAP action type."""
        from almanak.core.enums import ActionType

        assert adapter.supports_action(ActionType.SWAP)

    def test_adapter_supports_lp_operations(self, adapter):
        """Adapter should support LP action types."""
        from almanak.core.enums import ActionType

        assert adapter.supports_action(ActionType.OPEN_LP_POSITION)
        assert adapter.supports_action(ActionType.CLOSE_LP_POSITION)

    def test_build_swap_via_adapter(self, adapter, test_wallet, wsteth_market, weth_address):
        """Should build swap using adapter params."""
        # PT-wstETH address on Arbitrum
        pt_wsteth_address = "0x1c27Ad8a19Ba026ADaBD615F6Bc77158130cfBE4"
        params = PendleSwapParams(
            market=wsteth_market,
            token_in=weth_address,
            token_out=pt_wsteth_address,
            amount_in=10**18,
            min_amount_out=10**18,
            receiver=test_wallet,
            swap_type="token_to_pt",
        )

        tx = adapter.build_swap(params)

        assert tx.to == adapter.get_router_address()
        assert tx.data.startswith("0x")

    def test_build_add_liquidity_via_adapter(self, adapter, test_wallet, wsteth_market, weth_address):
        """Should build add liquidity using adapter params."""
        params = PendleLPParams(
            market=wsteth_market,
            token=weth_address,
            amount=10**18,
            min_amount=10**17,
            receiver=test_wallet,
            operation="add",
        )

        tx = adapter.build_add_liquidity(params)

        assert tx.to == adapter.get_router_address()


# =============================================================================
# Receipt Parser Tests
# =============================================================================


class TestReceiptParser:
    """Test receipt parsing functionality."""

    def test_parser_initializes(self, parser):
        """Parser should initialize correctly."""
        assert parser.chain == "arbitrum"
        assert parser.registry is not None

    def test_parse_empty_receipt(self, parser):
        """Parser should handle empty receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 0

    def test_parse_failed_transaction(self, parser):
        """Parser should handle failed transaction."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        # Note: When there are no logs, the parser returns early
        # The error message is only set when there are logs but tx failed

    def test_parse_transfer_event(self, parser):
        """Parser should parse Transfer events."""
        # ERC20 Transfer event
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        from_addr = "0x" + "0" * 24 + "1234567890123456789012345678901234567890"
        to_addr = "0x" + "0" * 24 + "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
        amount_hex = "0x" + hex(10**18)[2:].zfill(64)

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "topics": [transfer_topic, from_addr, to_addr],
                    "data": amount_hex,
                    "logIndex": 0,
                    "address": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.transfer_events) == 1
        transfer = result.transfer_events[0]
        assert transfer.value == 10**18

    def test_event_type_mapping(self, parser):
        """Parser should correctly map event types."""
        assert parser.registry.get_event_type("Swap") == PendleEventType.SWAP
        assert parser.registry.get_event_type("Mint") == PendleEventType.MINT
        assert parser.registry.get_event_type("Burn") == PendleEventType.BURN
        assert parser.registry.get_event_type("Transfer") == PendleEventType.TRANSFER


# =============================================================================
# On-Chain Tests (require Anvil)
# =============================================================================


@pytest.mark.onchain
class TestOnChainExecution:
    """On-chain tests requiring Anvil fork.

    These tests execute real transactions on an Anvil fork.
    Run with: pytest tests/connectors/pendle/test_pendle_sdk.py -v -m onchain

    Prerequisites:
    - Anvil must be running with an Arbitrum fork
    - Test wallet must be funded with WETH
    """

    @pytest.fixture
    def web3(self, anvil_rpc):
        """Web3 instance connected to Anvil."""
        w3 = Web3(Web3.HTTPProvider(anvil_rpc))
        if not w3.is_connected():
            pytest.skip("Anvil not running")
        return w3

    @pytest.fixture
    def funded_wallet(self, web3):
        """Funded test wallet for on-chain tests."""
        # Use Anvil's default funded account
        accounts = web3.eth.accounts
        if not accounts:
            pytest.skip("No accounts available in Anvil")
        return accounts[0]

    def test_web3_connection(self, web3):
        """Verify Web3 connection to Anvil."""
        assert web3.is_connected()
        block = web3.eth.block_number
        assert block > 0

    def test_router_contract_exists(self, web3):
        """Verify Pendle Router contract exists on fork."""
        router_address = PENDLE_ADDRESSES["arbitrum"]["ROUTER"]
        code = web3.eth.get_code(router_address)
        assert len(code) > 0, "Router contract not found on fork"

    def test_market_contract_exists(self, web3, wsteth_market):
        """Verify Pendle market contract exists on fork."""
        code = web3.eth.get_code(wsteth_market)
        assert len(code) > 0, "Market contract not found on fork"


# =============================================================================
# Integration Tests
# =============================================================================


class TestEndToEndIntegration:
    """End-to-end integration tests."""

    def test_full_swap_flow(self, sdk, parser, test_wallet, wsteth_market, weth_address):
        """Test full swap flow: build -> parse receipt structure."""
        # 1. Build approval
        approve_tx = sdk.build_approve_tx(weth_address)
        assert approve_tx.to == weth_address

        # 2. Build swap
        swap_tx = sdk.build_swap_exact_token_for_pt(
            receiver=test_wallet,
            market=wsteth_market,
            token_in=weth_address,
            amount_in=10**18,
            min_pt_out=10**18,
        )
        assert swap_tx.to == sdk.router_address

        # 3. Verify receipt parser can handle expected event structure
        mock_receipt = {
            "transactionHash": "0x" + "ab" * 32,
            "blockNumber": 12345678,
            "status": 1,
            "logs": [],  # Empty for mock
        }
        result = parser.parse_receipt(mock_receipt)
        assert result.success is True

    def test_full_lp_flow(self, sdk, test_wallet, wsteth_market, weth_address):
        """Test full LP flow: add -> remove liquidity."""
        # 1. Build add liquidity
        add_tx = sdk.build_add_liquidity_single_token(
            receiver=test_wallet,
            market=wsteth_market,
            token_in=weth_address,
            amount_in=10**18,
            min_lp_out=10**17,
        )
        assert add_tx.to == sdk.router_address

        # 2. Build remove liquidity
        remove_tx = sdk.build_remove_liquidity_single_token(
            receiver=test_wallet,
            market=wsteth_market,
            lp_amount=10**17,
            token_out=weth_address,
            min_token_out=10**16,
        )
        assert remove_tx.to == sdk.router_address

        # 3. Verify transactions are different
        assert add_tx.data != remove_tx.data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
