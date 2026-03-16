"""Uniswap V4 Adapter — compile SwapIntent to ActionBundle.

Follows the same pattern as UniswapV3Adapter but targets V4's
singleton PoolManager architecture.

Example:
    from almanak.framework.connectors.uniswap_v4.adapter import UniswapV4Adapter

    adapter = UniswapV4Adapter(chain="arbitrum")
    bundle = adapter.compile_swap_intent(intent, price_oracle)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.core.contracts import UNISWAP_V4
from almanak.framework.connectors.uniswap_v4.sdk import (
    SwapTransaction,
    UniswapV4SDK,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver
    from almanak.framework.intents.vocabulary import SwapIntent
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Config
# =============================================================================


@dataclass
class UniswapV4Config:
    """Configuration for UniswapV4Adapter.

    Attributes:
        chain: Chain name (e.g. "arbitrum").
        wallet_address: Wallet address for building transactions.
        rpc_url: Optional RPC URL for on-chain quotes.
        default_fee_tier: Default fee tier for swaps. Default 3000 (0.3%).
        default_slippage_bps: Default slippage in basis points. Default 50 (0.5%).
    """

    chain: str
    wallet_address: str = ""
    rpc_url: str | None = None
    default_fee_tier: int = 3000
    default_slippage_bps: int = 50


# =============================================================================
# Adapter
# =============================================================================


class UniswapV4Adapter:
    """Uniswap V4 swap adapter for intent compilation.

    Compiles SwapIntents into ActionBundles containing approve + swap
    transactions targeting the V4 swap router.

    Args:
        chain: Chain name.
        config: Optional UniswapV4Config. If not provided, chain is used.
        token_resolver: Optional TokenResolver for symbol -> address resolution.
    """

    def __init__(
        self,
        chain: str | None = None,
        config: UniswapV4Config | None = None,
        token_resolver: TokenResolver | None = None,
    ) -> None:
        if config is not None:
            self.chain = config.chain.lower()
            self.wallet_address = config.wallet_address
            self.rpc_url = config.rpc_url
            self.default_fee_tier = config.default_fee_tier
            self.default_slippage_bps = config.default_slippage_bps
        elif chain is not None:
            self.chain = chain.lower()
            self.wallet_address = ""
            self.rpc_url = None
            self.default_fee_tier = 3000
            self.default_slippage_bps = 50
        else:
            raise ValueError("Either chain or config must be provided")

        if self.chain not in UNISWAP_V4:
            raise ValueError(f"Uniswap V4 not supported on '{self.chain}'. Supported: {', '.join(UNISWAP_V4.keys())}")

        self.addresses = UNISWAP_V4[self.chain]
        self._sdk = UniswapV4SDK(chain=self.chain, rpc_url=self.rpc_url)
        self._token_resolver = token_resolver

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        fee_tier: int | None = None,
    ) -> SwapResult:
        """Build swap transactions for exact input amount.

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount_in: Input amount in human-readable units.
            slippage_bps: Slippage tolerance in bps. Default from config.
            fee_tier: Fee tier. Default from config.

        Returns:
            SwapResult with transactions list.
        """
        slippage_bps = slippage_bps or self.default_slippage_bps
        fee_tier = fee_tier or self.default_fee_tier

        # Resolve tokens
        token_in_addr, token_in_dec = self._resolve_token(token_in)
        token_out_addr, token_out_dec = self._resolve_token(token_out)

        # Convert to smallest units
        amount_in_raw = int(amount_in * Decimal(10**token_in_dec))

        # Get quote
        quote = self._sdk.get_quote_local(
            token_in=token_in_addr,
            token_out=token_out_addr,
            amount_in=amount_in_raw,
            fee_tier=fee_tier,
            token_in_decimals=token_in_dec,
            token_out_decimals=token_out_dec,
        )

        # Build transactions
        transactions: list[SwapTransaction] = []

        # Add approve if not native ETH
        if token_in_addr.lower() != "0x0000000000000000000000000000000000000000":
            approve_tx = self._sdk.build_approve_tx(
                token_address=token_in_addr,
                spender=self.addresses["v4_swap_router"],
                amount=amount_in_raw,
            )
            transactions.append(approve_tx)

        # Build swap tx
        if not self.wallet_address:
            raise ValueError(
                "wallet_address must be set before building swap transactions. "
                "Provide wallet_address via UniswapV4Config or set adapter.wallet_address."
            )

        # Build swap tx
        swap_tx = self._sdk.build_swap_tx(
            quote=quote,
            recipient=self.wallet_address,
            slippage_bps=slippage_bps,
        )
        transactions.append(swap_tx)

        amount_out_minimum = int(quote.amount_out * (10000 - slippage_bps) / 10000)

        return SwapResult(
            success=True,
            transactions=transactions,
            amount_in=amount_in_raw,
            amount_out_minimum=amount_out_minimum,
            gas_estimate=sum(tx.gas_estimate for tx in transactions),
        )

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle.

        This method integrates with the intent system to convert high-level
        swap intents into executable transaction bundles.

        Args:
            intent: The SwapIntent to compile.
            price_oracle: Optional price map for USD conversions.

        Returns:
            ActionBundle containing transactions for execution.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.models.reproduction_bundle import ActionBundle

        if price_oracle is None:
            price_oracle = {}

        # Determine swap amount
        if intent.amount is not None:
            if intent.amount == "all":
                raise ValueError(
                    "amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                )
            amount_in: Decimal = intent.amount  # type: ignore[assignment]
        elif intent.amount_usd is not None:
            from_price = price_oracle.get(intent.from_token.upper())
            if not from_price:
                raise ValueError(
                    f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd "
                    "to token amount. Ensure the price oracle includes this token."
                )
            amount_in = intent.amount_usd / from_price
        else:
            raise ValueError("Either amount or amount_usd must be specified")

        slippage_bps = int(intent.max_slippage * 10000)

        result = self.swap_exact_input(
            token_in=intent.from_token,
            token_out=intent.to_token,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
        )

        if not result.success:
            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[],
                metadata={
                    "error": result.error,
                    "intent_id": intent.intent_id,
                },
            )

        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx_to_dict(tx) for tx in result.transactions],
            metadata={
                "intent_id": intent.intent_id,
                "from_token": intent.from_token,
                "to_token": intent.to_token,
                "amount_in": str(result.amount_in),
                "amount_out_minimum": str(result.amount_out_minimum),
                "slippage_bps": slippage_bps,
                "chain": self.chain,
                "router": self.addresses["v4_swap_router"],
                "pool_manager": self.addresses["pool_manager"],
                "gas_estimate": result.gas_estimate,
                "protocol_version": "v4",
            },
        )

    def _resolve_token(self, token: str) -> tuple[str, int]:
        """Resolve token symbol to (address, decimals).

        Args:
            token: Token symbol (e.g. "USDC") or address.

        Returns:
            Tuple of (address, decimals).
        """
        # If already an address, try to get decimals
        if token.startswith("0x") and len(token) == 42:
            if self._token_resolver:
                resolved = self._token_resolver.resolve(token, self.chain)
                return resolved.address, resolved.decimals
            raise ValueError(
                f"Cannot resolve decimals for address '{token}' without a token_resolver. "
                "Provide a TokenResolver to the adapter."
            )

        # Resolve by symbol
        if self._token_resolver:
            resolved = self._token_resolver.resolve_for_swap(token, self.chain)
            return resolved.address, resolved.decimals

        # Fallback: use UNISWAP_V3_TOKENS registry for address
        from almanak.core.contracts import UNISWAP_V3_TOKENS

        chain_tokens = UNISWAP_V3_TOKENS.get(self.chain, {})
        address = chain_tokens.get(token.upper())
        if address:
            # Common decimals
            decimals_map = {
                "USDC": 6,
                "USDT": 6,
                "USDC.e": 6,
                "USDT.e": 6,
                "WBTC": 8,
                "WETH": 18,
                "ETH": 18,
                "DAI": 18,
                "LINK": 18,
                "UNI": 18,
                "WAVAX": 18,
                "WMATIC": 18,
                "WBNB": 18,
            }
            decimals = decimals_map.get(token.upper())
            if decimals is None:
                raise ValueError(
                    f"Cannot determine decimals for token '{token}' on {self.chain} without a token_resolver. "
                    "Provide a TokenResolver to the adapter."
                )
            return address, decimals

        raise ValueError(f"Cannot resolve token '{token}' on {self.chain}")


# =============================================================================
# Result types
# =============================================================================


@dataclass
class SwapResult:
    """Result of building swap transactions."""

    success: bool
    transactions: list[SwapTransaction]
    amount_in: int = 0
    amount_out_minimum: int = 0
    gas_estimate: int = 0
    error: str | None = None


def tx_to_dict(tx: SwapTransaction) -> dict[str, Any]:
    """Convert SwapTransaction to dict for ActionBundle."""
    return {
        "to": tx.to,
        "value": str(tx.value),
        "data": tx.data,
        "gas_estimate": tx.gas_estimate,
        "description": tx.description,
    }


__all__ = [
    "SwapResult",
    "UniswapV4Adapter",
    "UniswapV4Config",
]
