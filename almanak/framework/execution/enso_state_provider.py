"""Enso State Provider for Cross-Chain Bridge Status Tracking.

This module provides the EnsoStateProvider class that implements the
OnChainStateProvider protocol for tracking Enso cross-chain swap completion.

Since Enso uses underlying bridges (Stargate, LayerZero) internally, we track
completion by polling the destination chain for token arrival rather than
querying a bridge-specific status API.

Example:
    from almanak.framework.execution.enso_state_provider import EnsoStateProvider

    provider = EnsoStateProvider(
        rpc_urls={"base": "http://...", "arbitrum": "http://..."},
        wallet_address="0x...",
    )

    # Check if bridge transfer completed
    status = await provider.get_bridge_transfer_status(
        bridge_name="enso",
        deposit_id="base:arbitrum:0xabc...:WETH:1000000000000000000",
    )
    # Returns: {"status": "completed", "destination_balance": ...}
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from web3 import Web3
from web3.exceptions import Web3Exception
from web3.types import HexStr

logger = logging.getLogger(__name__)


# Standard ERC20 ABI for balanceOf
ERC20_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }
]

# Common token addresses per chain
TOKEN_ADDRESSES: dict[str, dict[str, str]] = {
    "base": {
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "WETH": "0x4200000000000000000000000000000000000006",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "ethereum": {
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
    "optimism": {
        "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "WETH": "0x4200000000000000000000000000000000000006",
        "ETH": "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
    },
}


@dataclass
class BridgeTransferInfo:
    """Information about a pending bridge transfer.

    Attributes:
        source_chain: Source chain name
        destination_chain: Destination chain name
        source_tx_hash: Transaction hash on source chain
        token_symbol: Token being bridged
        expected_amount: Expected amount to receive (in wei)
        started_at: When the transfer was initiated
        initial_destination_balance: Balance on destination before transfer
    """

    source_chain: str
    destination_chain: str
    source_tx_hash: str
    token_symbol: str
    expected_amount: int
    started_at: datetime
    initial_destination_balance: int


class EnsoStateProvider:
    """State provider for tracking Enso cross-chain swap completion.

    This provider tracks bridge completion by monitoring the destination
    chain balance. When the balance increases by approximately the expected
    amount (accounting for slippage), the transfer is considered complete.

    Supports two modes:
    - Direct Web3: Uses rpc_urls to create Web3 instances (legacy)
    - Gateway-backed: Uses gateway_client.market.GetBalance for balance queries

    The deposit_id format for Enso transfers:
        "enso:{source_chain}:{dest_chain}:{tx_hash}:{token}:{expected_amount}:{initial_balance}"

    Example:
        provider = EnsoStateProvider(
            rpc_urls={"base": "http://...", "arbitrum": "http://..."},
            wallet_address="0x...",
        )

        # Register a pending transfer
        deposit_id = provider.register_bridge_transfer(
            source_chain="base",
            destination_chain="arbitrum",
            source_tx_hash="0x...",
            token_symbol="WETH",
            expected_amount=1000000000000000000,  # 1 WETH
        )

        # Poll for completion
        while True:
            status = await provider.get_bridge_transfer_status("enso", deposit_id)
            if status["status"] == "completed":
                break
            await asyncio.sleep(10)
    """

    def __init__(
        self,
        rpc_urls: dict[str, str],
        wallet_address: str,
        slippage_tolerance: float = 0.05,  # 5% slippage tolerance for completion check
        gateway_client: Any | None = None,
    ) -> None:
        """Initialize the Enso state provider.

        Args:
            rpc_urls: Mapping of chain name to RPC URL
            wallet_address: Wallet address to monitor
            slippage_tolerance: Tolerance for balance comparison (default 5%)
            gateway_client: Optional GatewayClient for gateway-backed balance queries
        """
        self._rpc_urls = rpc_urls
        self._wallet_address = Web3.to_checksum_address(wallet_address)
        self._slippage_tolerance = slippage_tolerance
        self._web3_instances: dict[str, Web3] = {}
        self._pending_transfers: dict[str, BridgeTransferInfo] = {}
        self._gateway_client = gateway_client

        mode = "gateway" if gateway_client else f"direct Web3 ({list(rpc_urls.keys())})"
        logger.info(f"EnsoStateProvider initialized mode={mode}, wallet={wallet_address[:10]}...")

    def _get_web3(self, chain: str) -> Web3:
        """Get or create Web3 instance for a chain.

        Args:
            chain: Chain name

        Returns:
            Web3 instance for the chain

        Raises:
            ValueError: If chain not configured
        """
        if chain not in self._web3_instances:
            if chain not in self._rpc_urls:
                raise ValueError(f"Chain {chain} not configured. Available: {list(self._rpc_urls.keys())}")
            self._web3_instances[chain] = Web3(Web3.HTTPProvider(self._rpc_urls[chain]))
        return self._web3_instances[chain]

    def _get_token_address(self, chain: str, token_symbol: str) -> str:
        """Get token address for a chain.

        Args:
            chain: Chain name
            token_symbol: Token symbol (e.g., "WETH", "USDC")

        Returns:
            Token contract address

        Raises:
            ValueError: If token not found for chain
        """
        chain_tokens = TOKEN_ADDRESSES.get(chain, {})
        if token_symbol not in chain_tokens:
            raise ValueError(f"Token {token_symbol} not found for chain {chain}")
        return chain_tokens[token_symbol]

    def _get_token_balance_via_gateway(self, chain: str, token_symbol: str) -> int:
        """Get token balance via gateway's MarketService.

        Args:
            chain: Chain name
            token_symbol: Token symbol

        Returns:
            Balance in wei (raw_balance from gateway)
        """
        from almanak.gateway.proto import gateway_pb2

        if self._gateway_client is None:
            raise RuntimeError("Gateway client is not available for balance query")
        response = self._gateway_client.market.GetBalance(
            gateway_pb2.BalanceRequest(
                token=token_symbol,
                chain=chain,
                wallet_address=str(self._wallet_address),
            ),
            timeout=15.0,
        )
        # Use raw_balance (wei) for bridge tracking precision
        if response.raw_balance:
            return int(response.raw_balance)
        # Fallback: convert human-readable balance back to wei
        if response.balance:
            decimals = 6 if token_symbol.upper() == "USDC" else 18
            return int(Decimal(response.balance) * Decimal(10**decimals))
        return 0

    def _get_token_balance(self, chain: str, token_symbol: str) -> int:
        """Get token balance on a chain.

        Uses gateway if available, otherwise falls back to direct Web3.

        Args:
            chain: Chain name
            token_symbol: Token symbol

        Returns:
            Balance in wei
        """
        # Use gateway if available (no direct Web3 needed)
        if self._gateway_client is not None:
            return self._get_token_balance_via_gateway(chain, token_symbol)

        w3 = self._get_web3(chain)
        token_address = self._get_token_address(chain, token_symbol)

        # Handle native ETH
        if token_address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
            return w3.eth.get_balance(self._wallet_address)

        # ERC20 balance
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token_address),
            abi=ERC20_BALANCE_ABI,
        )
        return contract.functions.balanceOf(self._wallet_address).call()

    def register_bridge_transfer(
        self,
        source_chain: str,
        destination_chain: str,
        source_tx_hash: str,
        token_symbol: str,
        expected_amount: int,
    ) -> str:
        """Register a pending bridge transfer for tracking.

        Call this after submitting a cross-chain swap to track its completion.

        Args:
            source_chain: Source chain name
            destination_chain: Destination chain name
            source_tx_hash: Transaction hash on source chain
            token_symbol: Token being received on destination
            expected_amount: Expected amount to receive (in wei)

        Returns:
            deposit_id for tracking the transfer
        """
        # Get current balance on destination chain
        initial_balance = self._get_token_balance(destination_chain, token_symbol)

        # Create deposit ID
        deposit_id = f"enso:{source_chain}:{destination_chain}:{source_tx_hash}:{token_symbol}:{expected_amount}:{initial_balance}"

        # Store transfer info
        self._pending_transfers[deposit_id] = BridgeTransferInfo(
            source_chain=source_chain,
            destination_chain=destination_chain,
            source_tx_hash=source_tx_hash,
            token_symbol=token_symbol,
            expected_amount=expected_amount,
            started_at=datetime.now(UTC),
            initial_destination_balance=initial_balance,
        )

        logger.info(
            f"Registered bridge transfer: {source_chain} -> {destination_chain}, "
            f"token={token_symbol}, expected={expected_amount}, "
            f"initial_balance={initial_balance}"
        )

        return deposit_id

    async def get_transaction_status(
        self,
        chain: str,
        tx_hash: str,
    ) -> dict[str, Any]:
        """Get transaction status from chain.

        Args:
            chain: Chain name
            tx_hash: Transaction hash

        Returns:
            Dictionary with status and details
        """
        try:
            w3 = self._get_web3(chain)
            receipt = w3.eth.get_transaction_receipt(HexStr(tx_hash))

            if receipt is None:
                return {"status": "pending", "tx_hash": tx_hash}

            if receipt["status"] == 1:
                return {
                    "status": "confirmed",
                    "tx_hash": tx_hash,
                    "block_number": receipt["blockNumber"],
                    "gas_used": receipt["gasUsed"],
                }
            else:
                return {
                    "status": "failed",
                    "tx_hash": tx_hash,
                    "block_number": receipt["blockNumber"],
                    "error": "Transaction reverted",
                }

        except Web3Exception as e:
            logger.warning(f"Error checking tx status for {tx_hash} on {chain}: {e}")
            return {"status": "unknown", "error": str(e)}

    async def get_bridge_transfer_status(
        self,
        bridge_name: str,
        deposit_id: str,
    ) -> dict[str, Any]:
        """Get bridge transfer status by checking destination balance.

        For Enso cross-chain swaps, we detect completion by monitoring the
        destination chain balance. When balance increases by approximately
        the expected amount, transfer is complete.

        Args:
            bridge_name: Bridge name (should be "enso")
            deposit_id: Deposit ID from register_bridge_transfer()

        Returns:
            Dictionary with status and details:
            - status: "pending", "completed", "failed", or "unknown"
            - destination_balance: Current balance on destination
            - balance_increase: How much balance increased
            - elapsed_seconds: Time since transfer started
        """
        # Parse deposit_id
        try:
            parts = deposit_id.split(":")
            if len(parts) != 7 or parts[0] != "enso":
                return {"status": "unknown", "error": f"Invalid deposit_id format: {deposit_id}"}

            _, source_chain, dest_chain, tx_hash, token_symbol, expected_amount_str, initial_balance_str = parts
            expected_amount = int(expected_amount_str)
            initial_balance = int(initial_balance_str)

        except (ValueError, IndexError) as e:
            return {"status": "unknown", "error": f"Failed to parse deposit_id: {e}"}

        # Get transfer info if registered
        transfer_info = self._pending_transfers.get(deposit_id)
        started_at = transfer_info.started_at if transfer_info else datetime.now(UTC)

        # Check current balance on destination chain
        try:
            current_balance = self._get_token_balance(dest_chain, token_symbol)
        except Exception as e:
            logger.warning(f"Error checking balance on {dest_chain}: {e}")
            return {"status": "unknown", "error": str(e)}

        balance_increase = current_balance - initial_balance
        elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()

        # Check if balance increased sufficiently
        # If expected_amount is provided and > 0, use slippage-adjusted threshold
        # Otherwise, just check for any meaningful increase (> dust threshold of 1000 wei)
        dust_threshold = 1000  # Minimum wei to consider as actual transfer

        if expected_amount > 0:
            min_expected = int(expected_amount * (1 - self._slippage_tolerance))
        else:
            # No expected amount - any meaningful increase counts
            min_expected = dust_threshold

        if balance_increase >= min_expected and balance_increase > dust_threshold:
            # Transfer completed!
            logger.info(
                f"Bridge transfer completed: {source_chain} -> {dest_chain}, "
                f"received={balance_increase}, expected={expected_amount}, "
                f"elapsed={elapsed_seconds:.1f}s"
            )

            # Clean up pending transfer
            if deposit_id in self._pending_transfers:
                del self._pending_transfers[deposit_id]

            return {
                "status": "completed",
                "destination_balance": current_balance,
                "balance_increase": balance_increase,
                "expected_amount": expected_amount,
                "elapsed_seconds": elapsed_seconds,
                "destination_tx": None,  # Enso doesn't provide this directly
            }

        # Still pending
        logger.debug(
            f"Bridge transfer pending: {source_chain} -> {dest_chain}, "
            f"increase={balance_increase}, expected={expected_amount}, "
            f"elapsed={elapsed_seconds:.1f}s"
        )

        return {
            "status": "pending",
            "destination_balance": current_balance,
            "balance_increase": balance_increase,
            "expected_amount": expected_amount,
            "elapsed_seconds": elapsed_seconds,
            "initial_balance": initial_balance,
        }

    async def get_balance(
        self,
        chain: str,
        token: str,
        address: str,
    ) -> Decimal:
        """Get token balance on chain.

        Args:
            chain: Chain name
            token: Token symbol or address
            address: Wallet address

        Returns:
            Balance as Decimal (in token units, not wei)
        """
        # Save current wallet, use provided address
        original_wallet = self._wallet_address
        self._wallet_address = Web3.to_checksum_address(address)

        try:
            balance_wei = self._get_token_balance(chain, token)
            # Assume 18 decimals for most tokens, 6 for USDC
            decimals = 6 if token.upper() == "USDC" else 18
            return Decimal(balance_wei) / Decimal(10**decimals)
        finally:
            self._wallet_address = original_wallet

    async def wait_for_bridge_completion(
        self,
        deposit_id: str,
        timeout_seconds: int = 300,
        poll_interval_seconds: int = 10,
    ) -> dict[str, Any]:
        """Wait for a bridge transfer to complete.

        Polls the bridge status until completion or timeout.

        Args:
            deposit_id: Deposit ID from register_bridge_transfer()
            timeout_seconds: Maximum time to wait (default 5 minutes)
            poll_interval_seconds: How often to poll (default 10 seconds)

        Returns:
            Final status dictionary

        Raises:
            TimeoutError: If transfer doesn't complete within timeout
        """
        start_time = datetime.now(UTC)

        while True:
            status = await self.get_bridge_transfer_status("enso", deposit_id)

            if status["status"] == "completed":
                return status

            if status["status"] == "failed":
                return status

            elapsed = (datetime.now(UTC) - start_time).total_seconds()
            if elapsed >= timeout_seconds:
                raise TimeoutError(f"Bridge transfer timed out after {timeout_seconds}s. Last status: {status}")

            logger.debug(f"Bridge pending, waiting {poll_interval_seconds}s... (elapsed: {elapsed:.0f}s)")
            await asyncio.sleep(poll_interval_seconds)


def is_cross_chain_intent(intent: Any) -> bool:
    """Check if an intent involves cross-chain bridging.

    Args:
        intent: Intent object to check

    Returns:
        True if intent has destination_chain different from source chain
    """
    dest_chain = getattr(intent, "destination_chain", None)
    src_chain = getattr(intent, "chain", None)
    return dest_chain is not None and dest_chain != src_chain


__all__ = [
    "EnsoStateProvider",
    "BridgeTransferInfo",
    "is_cross_chain_intent",
    "TOKEN_ADDRESSES",
]
