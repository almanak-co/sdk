"""
Pendle Protocol Adapter

This adapter maps the framework's ActionType to Pendle SDK operations.
It handles the translation between high-level intents and low-level
Pendle Router interactions.

Supported Actions:
- SWAP: Swap tokens to/from PT/YT
- OPEN_LP_POSITION: Add liquidity to Pendle markets
- CLOSE_LP_POSITION: Remove liquidity from Pendle markets
- WITHDRAW: Redeem PT/YT to underlying token

The adapter integrates with the Almanak intent system through the
standard adapter interface.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from almanak.core.enums import ActionType

from .sdk import (
    PENDLE_ADDRESSES,
    PENDLE_GAS_ESTIMATES,
    PendleActionType,
    PendleTransactionData,
    get_pendle_sdk,
)

if TYPE_CHECKING:
    from almanak.framework.data.pendle.api_client import PendleAPIClient
    from almanak.framework.data.pendle.on_chain_reader import PendleOnChainReader

logger = logging.getLogger(__name__)


# =============================================================================
# Pendle-specific action parameters
# =============================================================================


@dataclass
class PendleSwapParams:
    """Parameters for Pendle swap operations."""

    market: str
    token_in: str
    token_out: str
    amount_in: int
    min_amount_out: int
    receiver: str
    swap_type: str  # "token_to_pt", "pt_to_token", "token_to_yt", "yt_to_token"
    slippage_bps: int = 50
    token_mint_sy: str | None = None  # Token that mints SY (for yield-bearing token markets)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "market": self.market,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "min_amount_out": str(self.min_amount_out),
            "receiver": self.receiver,
            "swap_type": self.swap_type,
            "slippage_bps": self.slippage_bps,
        }
        if self.token_mint_sy:
            result["token_mint_sy"] = self.token_mint_sy
        return result


@dataclass
class PendleLPParams:
    """Parameters for Pendle liquidity operations."""

    market: str
    token: str
    amount: int
    min_amount: int
    receiver: str
    operation: str  # "add" or "remove"
    slippage_bps: int = 50

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market": self.market,
            "token": self.token,
            "amount": str(self.amount),
            "min_amount": str(self.min_amount),
            "receiver": self.receiver,
            "operation": self.operation,
            "slippage_bps": self.slippage_bps,
        }


@dataclass
class PendleRedeemParams:
    """Parameters for Pendle redemption operations."""

    yt_address: str
    py_amount: int
    token_out: str
    min_token_out: int
    receiver: str
    slippage_bps: int = 50

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "yt_address": self.yt_address,
            "py_amount": str(self.py_amount),
            "token_out": self.token_out,
            "min_token_out": str(self.min_token_out),
            "receiver": self.receiver,
            "slippage_bps": self.slippage_bps,
        }


# =============================================================================
# Pendle Adapter
# =============================================================================


class PendleAdapter:
    """
    Adapter for Pendle Protocol operations.

    This adapter translates between the framework's ActionType enum and
    Pendle's specific operations. It handles:
    - Token swaps to/from PT (Principal Token)
    - Token swaps to/from YT (Yield Token)
    - Liquidity provision (adding/removing)
    - PT/YT redemption at maturity

    Example:
        adapter = PendleAdapter(rpc_url="https://arb1.arbitrum.io/rpc", chain="arbitrum")

        # Build a swap transaction
        tx = adapter.build_swap(
            params=PendleSwapParams(
                market="0x...",
                token_in="0x...",
                token_out="0x...",
                amount_in=10**18,
                min_amount_out=10**18,
                receiver="0x...",
                swap_type="token_to_pt",
            )
        )
    """

    # Mapping from ActionType to Pendle action types
    ACTION_TYPE_MAP = {
        ActionType.SWAP: [
            PendleActionType.SWAP_EXACT_TOKEN_FOR_PT,
            PendleActionType.SWAP_EXACT_PT_FOR_TOKEN,
            PendleActionType.SWAP_EXACT_TOKEN_FOR_YT,
            PendleActionType.SWAP_EXACT_YT_FOR_TOKEN,
        ],
        ActionType.OPEN_LP_POSITION: [
            PendleActionType.ADD_LIQUIDITY_SINGLE_TOKEN,
            PendleActionType.ADD_LIQUIDITY_DUAL,
        ],
        ActionType.CLOSE_LP_POSITION: [
            PendleActionType.REMOVE_LIQUIDITY_SINGLE_TOKEN,
        ],
        ActionType.WITHDRAW: [
            PendleActionType.REDEEM_PY_TO_TOKEN,
            PendleActionType.REDEEM_SY_TO_TOKEN,
        ],
    }

    def __init__(
        self,
        rpc_url: str,
        chain: str = "arbitrum",
        wallet_address: str | None = None,
        api_client: "PendleAPIClient | None" = None,
        on_chain_reader: "PendleOnChainReader | None" = None,
    ):
        """
        Initialize the Pendle adapter.

        Args:
            rpc_url: RPC endpoint URL
            chain: Target chain (arbitrum, ethereum)
            wallet_address: Optional default wallet address for transactions
            api_client: Optional PendleAPIClient for REST API quotes
            on_chain_reader: Optional PendleOnChainReader for on-chain fallback
        """
        self.chain = chain
        self.wallet_address = wallet_address
        self.sdk = get_pendle_sdk(rpc_url, chain)
        self.addresses = PENDLE_ADDRESSES.get(chain, {})
        self._api_client = api_client
        self._on_chain_reader = on_chain_reader
        self._rpc_url = rpc_url

        logger.info(f"PendleAdapter initialized: chain={chain}")

    def supports_action(self, action_type: ActionType) -> bool:
        """Check if this adapter supports the given action type."""
        return action_type in self.ACTION_TYPE_MAP

    def get_supported_actions(self) -> list[ActionType]:
        """Get list of supported action types."""
        return list(self.ACTION_TYPE_MAP.keys())

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def build_swap(self, params: PendleSwapParams) -> PendleTransactionData:
        """
        Build a swap transaction based on the swap type.

        Args:
            params: Swap parameters including market, tokens, and amounts

        Returns:
            Transaction data ready for execution
        """
        swap_type = params.swap_type.lower()

        if swap_type == "token_to_pt":
            return self.sdk.build_swap_exact_token_for_pt(
                receiver=params.receiver,
                market=params.market,
                token_in=params.token_in,
                amount_in=params.amount_in,
                min_pt_out=params.min_amount_out,
                slippage_bps=params.slippage_bps,
                token_mint_sy=params.token_mint_sy,
            )
        elif swap_type == "pt_to_token":
            return self.sdk.build_swap_exact_pt_for_token(
                receiver=params.receiver,
                market=params.market,
                pt_amount=params.amount_in,
                token_out=params.token_out,
                min_token_out=params.min_amount_out,
                slippage_bps=params.slippage_bps,
                token_redeem_sy=params.token_mint_sy,
            )
        elif swap_type == "token_to_yt":
            return self.sdk.build_swap_exact_token_for_yt(
                receiver=params.receiver,
                market=params.market,
                token_in=params.token_in,
                amount_in=params.amount_in,
                min_yt_out=params.min_amount_out,
                slippage_bps=params.slippage_bps,
                token_mint_sy=params.token_mint_sy,
            )
        elif swap_type == "yt_to_token":
            return self.sdk.build_swap_exact_yt_for_token(
                receiver=params.receiver,
                market=params.market,
                yt_amount=params.amount_in,
                token_out=params.token_out,
                min_token_out=params.min_amount_out,
                slippage_bps=params.slippage_bps,
            )
        else:
            raise ValueError(f"Unsupported swap type: {swap_type}")

    def build_swap_token_to_pt(
        self,
        market: str,
        token_in: str,
        amount_in: int,
        min_pt_out: int,
        receiver: str,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a token -> PT swap transaction.

        This is a convenience method for the most common swap type.

        Args:
            market: Pendle market address
            token_in: Input token address
            amount_in: Amount of input token
            min_pt_out: Minimum PT to receive
            receiver: Address to receive PT
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Transaction data
        """
        return self.sdk.build_swap_exact_token_for_pt(
            receiver=receiver,
            market=market,
            token_in=token_in,
            amount_in=amount_in,
            min_pt_out=min_pt_out,
            slippage_bps=slippage_bps,
        )

    def build_swap_pt_to_token(
        self,
        market: str,
        pt_amount: int,
        token_out: str,
        min_token_out: int,
        receiver: str,
        slippage_bps: int = 50,
    ) -> PendleTransactionData:
        """
        Build a PT -> token swap transaction.

        Args:
            market: Pendle market address
            pt_amount: Amount of PT to swap
            token_out: Output token address
            min_token_out: Minimum token to receive
            receiver: Address to receive token
            slippage_bps: Slippage tolerance

        Returns:
            Transaction data
        """
        return self.sdk.build_swap_exact_pt_for_token(
            receiver=receiver,
            market=market,
            pt_amount=pt_amount,
            token_out=token_out,
            min_token_out=min_token_out,
            slippage_bps=slippage_bps,
        )

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def build_add_liquidity(self, params: PendleLPParams) -> PendleTransactionData:
        """
        Build an add liquidity transaction.

        Args:
            params: Liquidity parameters

        Returns:
            Transaction data
        """
        return self.sdk.build_add_liquidity_single_token(
            receiver=params.receiver,
            market=params.market,
            token_in=params.token,
            amount_in=params.amount,
            min_lp_out=params.min_amount,
            slippage_bps=params.slippage_bps,
        )

    def build_remove_liquidity(self, params: PendleLPParams) -> PendleTransactionData:
        """
        Build a remove liquidity transaction.

        Args:
            params: Liquidity parameters

        Returns:
            Transaction data
        """
        return self.sdk.build_remove_liquidity_single_token(
            receiver=params.receiver,
            market=params.market,
            lp_amount=params.amount,
            token_out=params.token,
            min_token_out=params.min_amount,
            slippage_bps=params.slippage_bps,
        )

    # =========================================================================
    # Redemption Operations
    # =========================================================================

    def build_redeem(self, params: PendleRedeemParams) -> PendleTransactionData:
        """
        Build a PT+YT redemption transaction.

        Args:
            params: Redemption parameters

        Returns:
            Transaction data
        """
        return self.sdk.build_redeem_py_to_token(
            receiver=params.receiver,
            yt_address=params.yt_address,
            py_amount=params.py_amount,
            token_out=params.token_out,
            min_token_out=params.min_token_out,
            slippage_bps=params.slippage_bps,
        )

    # =========================================================================
    # Approval Helpers
    # =========================================================================

    def build_approve(
        self,
        token_address: str,
        amount: int | None = None,
    ) -> PendleTransactionData:
        """
        Build an approval transaction for the Pendle Router.

        Args:
            token_address: Token to approve
            amount: Amount to approve (defaults to max)

        Returns:
            Transaction data
        """
        return self.sdk.build_approve_tx(
            token_address=token_address,
            spender=self.sdk.router_address,
            amount=amount if amount is not None else 2**256 - 1,
        )

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_router_address(self) -> str:
        """Get the Pendle Router address for this chain."""
        return self.sdk.router_address

    def get_gas_estimate(self, action: PendleActionType) -> int:
        """Get gas estimate for an action."""
        action_key = action.value.lower()
        return PENDLE_GAS_ESTIMATES.get(action_key, 500_000)

    def estimate_output(
        self,
        market: str,
        token_in: str,
        amount_in: int,
        swap_type: str,
        slippage_bps: int = 50,
    ) -> int:
        """
        Estimate output amount for a swap using a 3-tier cascade:
        1. Pendle REST API quote (most accurate)
        2. On-chain RouterStatic rate (good fallback)
        3. Conservative 1% haircut estimate (last resort, always logged as WARNING)

        Args:
            market: Market address
            token_in: Input token address
            amount_in: Input amount in wei
            swap_type: Type of swap ("token_to_pt", "pt_to_token", etc.)
            slippage_bps: Slippage tolerance in basis points

        Returns:
            Estimated output amount in wei
        """
        # Tier 1: Try Pendle API
        try:
            if self._api_client is None:
                from almanak.framework.data.pendle.api_client import PendleAPIClient

                self._api_client = PendleAPIClient(chain=self.chain)

            quote = self._api_client.get_swap_quote(
                market=market,
                token_in=token_in,
                amount_in=amount_in,
                swap_type=swap_type,
                slippage_bps=slippage_bps,
            )
            logger.info(f"Pendle API quote: {amount_in} -> {quote.amount_out} (impact={quote.price_impact_bps}bps)")
            return quote.amount_out
        except Exception as e:
            logger.info(f"Pendle API quote unavailable, trying on-chain fallback: {e}")

        # Tier 2: Try on-chain RouterStatic
        try:
            if self._on_chain_reader is None:
                from almanak.framework.data.pendle.on_chain_reader import PendleOnChainReader

                self._on_chain_reader = PendleOnChainReader(rpc_url=self._rpc_url, chain=self.chain)

            if swap_type == "token_to_pt":
                estimated = self._on_chain_reader.estimate_pt_output(market, amount_in)
            elif swap_type == "pt_to_token":
                from decimal import Decimal

                rate = self._on_chain_reader.get_pt_to_asset_rate(market)
                estimated = int(Decimal(str(amount_in)) * rate)
            else:
                # YT pricing is more complex than PT -- skip on-chain fallback
                # and fall through to conservative estimate below
                raise NotImplementedError("On-chain YT estimation not yet supported")

            logger.info(f"On-chain estimate: {amount_in} -> {estimated}")
            return estimated
        except Exception as e:
            logger.info(f"On-chain estimate unavailable, using conservative FALLBACK: {e}")

        # Tier 3: Conservative estimate (1% haircut from 1:1)
        conservative_estimate = int(amount_in * 9900 // 10000)
        logger.warning(
            f"FALLBACK: Using conservative 1% haircut estimate for Pendle {swap_type} "
            f"on market {market[:10]}...: {amount_in} -> {conservative_estimate}. "
            f"API and on-chain pricing unavailable."
        )
        return conservative_estimate


# =============================================================================
# Factory Function
# =============================================================================


def get_pendle_adapter(
    rpc_url: str,
    chain: str = "arbitrum",
    wallet_address: str | None = None,
) -> PendleAdapter:
    """Factory function to create a PendleAdapter instance."""
    return PendleAdapter(rpc_url, chain, wallet_address)


__all__ = [
    "PendleAdapter",
    "PendleSwapParams",
    "PendleLPParams",
    "PendleRedeemParams",
    "get_pendle_adapter",
]
