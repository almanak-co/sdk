"""Drift Protocol Adapter.

Converts PerpOpenIntent and PerpCloseIntent to executable Solana transactions
using raw instruction building (no driftpy dependency).

Drift is the #1 Solana perps DEX ($1.13B TVL). This adapter supports:
- Market orders for perp open/close
- Automatic user account initialization
- Remaining accounts resolution (oracles + active positions)

Example:
    from almanak.framework.connectors.drift import DriftAdapter, DriftConfig

    config = DriftConfig(
        wallet_address="your-solana-pubkey",
        rpc_url="https://api.mainnet-beta.solana.com",
    )
    adapter = DriftAdapter(config)

    intent = PerpOpenIntent(
        market="SOL-PERP",
        collateral_token="USDC",
        collateral_amount=Decimal("100"),
        size_usd=Decimal("500"),
        is_long=True,
        leverage=Decimal("5"),
        protocol="drift",
    )
    bundle = adapter.compile_perp_open_intent(intent)
"""

from __future__ import annotations

import base64
import logging
import os
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from solders.hash import Hash
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from ...intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent
from ...models.reproduction_bundle import ActionBundle
from ..jupiter.adapter import SolanaTransactionData
from .client import DriftDataClient
from .constants import (
    BASE_PRECISION,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    ORDER_TYPE_MARKET,
    PERP_MARKET_SYMBOL_TO_INDEX,
    PERP_MARKETS,
    PRICE_PRECISION,
)
from .exceptions import DriftMarketError, DriftValidationError
from .models import DriftConfig, OrderParams
from .sdk import DriftSDK

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


class DriftAdapter:
    """Adapter for Drift protocol integration with the Intent system.

    Converts PerpOpenIntent and PerpCloseIntent into ActionBundles
    containing serialized Solana VersionedTransactions.

    Key features:
    - Market orders only (MVP scope)
    - Automatic account initialization if needed
    - Oracle and remaining accounts resolution via RPC
    - Sub-account 0 only (default)
    """

    def __init__(
        self,
        config: DriftConfig,
        token_resolver: TokenResolverType | None = None,
    ) -> None:
        self.config = config
        self.wallet_address = config.wallet_address

        # Resolve RPC URL: config > env > empty
        rpc_url = config.rpc_url or os.environ.get("SOLANA_RPC_URL", "")
        self.sdk = DriftSDK(
            wallet_address=config.wallet_address,
            rpc_url=rpc_url,
            timeout=config.timeout,
        )
        self.client = DriftDataClient(
            base_url=config.data_api_base_url,
            timeout=config.timeout,
        )

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"DriftAdapter initialized for wallet={config.wallet_address[:8]}...")

    # =========================================================================
    # Intent Compilation
    # =========================================================================

    def compile_perp_open_intent(
        self,
        intent: PerpOpenIntent,
        price_oracle: Any = None,
    ) -> ActionBundle:
        """Compile a PerpOpenIntent to an ActionBundle.

        Args:
            intent: Perp open intent with market, size, direction, etc.
            price_oracle: Optional price oracle for USD conversions

        Returns:
            ActionBundle with serialized VersionedTransaction
        """
        try:
            # 1. Resolve market index
            market_index = self._resolve_market_index(intent.market)
            symbol = PERP_MARKETS.get(market_index, intent.market)

            # 2. Get oracle price for size calculation
            oracle_price = self._get_oracle_price(market_index, price_oracle)

            # 3. Calculate base_asset_amount from size_usd
            base_asset_amount = self._calculate_base_amount(intent.size_usd, oracle_price)

            # 4. Build order params with slippage protection
            direction = DIRECTION_LONG if intent.is_long else DIRECTION_SHORT
            # On Drift, market orders with a non-zero price act as a limit price cap.
            # For longs: worst acceptable price = oracle * (1 + max_slippage)
            # For shorts: worst acceptable price = oracle * (1 - max_slippage)
            slippage = getattr(intent, "max_slippage", Decimal("0.01"))
            if intent.is_long:
                worst_price = oracle_price * (Decimal("1") + slippage)
            else:
                worst_price = oracle_price * (Decimal("1") - slippage)
            # Convert to Drift price precision (1e6)
            price_limit = int(worst_price * Decimal(str(PRICE_PRECISION)))

            order_params = OrderParams(
                order_type=ORDER_TYPE_MARKET,
                direction=direction,
                base_asset_amount=base_asset_amount,
                market_index=market_index,
                price=max(price_limit, 0),  # Slippage-protected limit price
            )

            # 5. Get init instructions if needed
            init_ixs = self.sdk.get_init_instructions(self.config.sub_account_id)

            # 6. Build remaining accounts
            remaining_accounts = self.sdk.build_remaining_accounts(
                market_index=market_index,
                sub_account_id=self.config.sub_account_id,
            )

            # 7. Build place order instruction
            order_ix = self.sdk.build_place_perp_order_ix(
                order_params=order_params,
                remaining_accounts=remaining_accounts,
                sub_account_id=self.config.sub_account_id,
            )

            # 8. Combine instructions
            all_ixs = init_ixs + [order_ix]

            # 9. Build VersionedTransaction
            tx_data = self._build_transaction(all_ixs, tx_type="perp_open")

            # 10. Calculate collateral amount for metadata
            collateral_amount = intent.collateral_amount
            if isinstance(collateral_amount, str) and collateral_amount == "all":
                collateral_amount_str = "all"
            else:
                collateral_amount_str = str(collateral_amount)

            # 11. Return ActionBundle
            return ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "protocol": "drift",
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "action": "perp_open",
                    "market": symbol,
                    "market_index": market_index,
                    "direction": "long" if intent.is_long else "short",
                    "size_usd": str(intent.size_usd),
                    "base_asset_amount": str(base_asset_amount),
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": collateral_amount_str,
                    "leverage": str(intent.leverage),
                    "oracle_price": str(oracle_price) if oracle_price else "unknown",
                    "order_type": "market",
                    "needs_init": len(init_ixs) > 0,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Drift PERP_OPEN intent: {e}")
            return self._error_bundle(IntentType.PERP_OPEN, intent.intent_id, str(e))

    def compile_perp_close_intent(
        self,
        intent: PerpCloseIntent,
        price_oracle: Any = None,
    ) -> ActionBundle:
        """Compile a PerpCloseIntent to an ActionBundle.

        Args:
            intent: Perp close intent with market, direction, optional size
            price_oracle: Optional price oracle

        Returns:
            ActionBundle with serialized VersionedTransaction
        """
        try:
            # 1. Resolve market index
            market_index = self._resolve_market_index(intent.market)
            symbol = PERP_MARKETS.get(market_index, intent.market)

            # 2. Determine base_asset_amount to close and get oracle price
            oracle_price = self._get_oracle_price(market_index, price_oracle)
            if intent.size_usd is None:
                # Close full position — read from on-chain
                base_asset_amount = self._get_position_size(market_index)
            else:
                base_asset_amount = self._calculate_base_amount(intent.size_usd, oracle_price)

            # 3. Build order params (reduce_only for closing)
            # When closing: direction is OPPOSITE of position direction
            close_direction = DIRECTION_SHORT if intent.is_long else DIRECTION_LONG
            # Slippage protection: closing a long = selling (worst price below oracle),
            # closing a short = buying (worst price above oracle)
            slippage = getattr(intent, "max_slippage", Decimal("0.01"))
            if intent.is_long:
                # Closing long = selling, worst price is below oracle
                worst_price = oracle_price * (Decimal("1") - slippage)
            else:
                # Closing short = buying, worst price is above oracle
                worst_price = oracle_price * (Decimal("1") + slippage)
            price_limit = int(worst_price * Decimal(str(PRICE_PRECISION)))

            order_params = OrderParams(
                order_type=ORDER_TYPE_MARKET,
                direction=close_direction,
                base_asset_amount=base_asset_amount,
                market_index=market_index,
                price=max(price_limit, 0),
                reduce_only=True,
            )

            # 4. Build remaining accounts
            remaining_accounts = self.sdk.build_remaining_accounts(
                market_index=market_index,
                sub_account_id=self.config.sub_account_id,
            )

            # 5. Build instruction
            order_ix = self.sdk.build_place_perp_order_ix(
                order_params=order_params,
                remaining_accounts=remaining_accounts,
                sub_account_id=self.config.sub_account_id,
            )

            # 6. Build transaction
            tx_data = self._build_transaction([order_ix], tx_type="perp_close")

            return ActionBundle(
                intent_type=IntentType.PERP_CLOSE.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "protocol": "drift",
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "action": "perp_close",
                    "market": symbol,
                    "market_index": market_index,
                    "direction": "long" if intent.is_long else "short",
                    "size_usd": str(intent.size_usd) if intent.size_usd else "full",
                    "base_asset_amount": str(base_asset_amount),
                    "collateral_token": intent.collateral_token,
                    "order_type": "market",
                    "reduce_only": True,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Drift PERP_CLOSE intent: {e}")
            return self._error_bundle(IntentType.PERP_CLOSE, intent.intent_id, str(e))

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _resolve_market_index(self, market: str) -> int:
        """Resolve a market string to a Drift market index.

        Accepts:
        - "SOL-PERP" → 0
        - "SOL/USD" → 0 (convenience alias)
        - "SOL" → 0 (base asset only)
        - "0" → 0 (numeric index)

        Args:
            market: Market identifier string

        Returns:
            Market index

        Raises:
            DriftMarketError: If market cannot be resolved
        """
        market_upper = market.upper().strip()

        # Direct symbol match: "SOL-PERP"
        if market_upper in PERP_MARKET_SYMBOL_TO_INDEX:
            return PERP_MARKET_SYMBOL_TO_INDEX[market_upper]

        # Convenience: "SOL/USD" → "SOL-PERP"
        if "/" in market_upper:
            base = market_upper.split("/")[0].strip()
            perp_symbol = f"{base}-PERP"
            if perp_symbol in PERP_MARKET_SYMBOL_TO_INDEX:
                return PERP_MARKET_SYMBOL_TO_INDEX[perp_symbol]

        # Base asset only: "SOL" → "SOL-PERP"
        perp_symbol = f"{market_upper}-PERP"
        if perp_symbol in PERP_MARKET_SYMBOL_TO_INDEX:
            return PERP_MARKET_SYMBOL_TO_INDEX[perp_symbol]

        # Numeric index
        try:
            idx = int(market_upper)
            if idx in PERP_MARKETS:
                return idx
        except ValueError:
            pass

        raise DriftMarketError(
            f"Unknown Drift market: '{market}'. Valid markets: {', '.join(PERP_MARKETS.values())}",
            market=market,
        )

    def _get_oracle_price(
        self,
        market_index: int,
        price_oracle: Any = None,
    ) -> Decimal:
        """Get oracle price for a market.

        Tries: price_oracle → Drift Data API → fallback error.
        """
        # Try provided price oracle first (supports both dict and object with .get_price())
        if price_oracle:
            symbol = PERP_MARKETS.get(market_index, "")
            base_asset = symbol.replace("-PERP", "") if symbol else ""
            if base_asset:
                try:
                    if isinstance(price_oracle, dict):
                        price = price_oracle.get(base_asset)
                    else:
                        price = price_oracle.get_price(base_asset)
                    if price and price > 0:
                        return Decimal(str(price))
                except Exception:
                    pass

        # Try Drift Data API
        price = self.client.get_oracle_price(market_index)
        if price and price > 0:
            return price

        raise DriftValidationError(
            f"Could not get oracle price for market index {market_index}. "
            "Ensure the Drift Data API is reachable or provide a price oracle.",
            field="oracle_price",
        )

    def _calculate_base_amount(self, size_usd: Decimal, oracle_price: Decimal) -> int:
        """Calculate base_asset_amount from USD size and oracle price.

        base_asset_amount = (size_usd / oracle_price) * BASE_PRECISION

        Rounds down (truncates) to avoid overspending — standard convention
        for order amounts in DeFi.

        Args:
            size_usd: Position size in USD
            oracle_price: Current oracle price

        Returns:
            Base asset amount in Drift precision (1e9), rounded down
        """
        if oracle_price <= 0:
            raise DriftValidationError(
                "Oracle price must be positive",
                field="oracle_price",
                value=str(oracle_price),
            )

        base_amount = (size_usd / oracle_price) * Decimal(str(BASE_PRECISION))
        # Explicit truncation toward zero (round down) to avoid overspending
        return int(base_amount.to_integral_value(rounding="ROUND_DOWN"))

    def _get_position_size(self, market_index: int) -> int:
        """Get the current position size from on-chain data.

        Used for full position close (size_usd=None).

        Returns:
            Absolute base_asset_amount of the position
        """
        user_account = self.sdk.fetch_user_account(self.config.sub_account_id)
        if not user_account.exists:
            raise DriftValidationError(
                "No Drift user account found. Cannot close position.",
                field="user_account",
            )

        for pos in user_account.perp_positions:
            if pos.market_index == market_index and pos.is_active:
                return abs(pos.base_asset_amount)

        raise DriftValidationError(
            f"No active position found for market index {market_index}",
            field="market_index",
            value=str(market_index),
        )

    def _build_transaction(
        self,
        instructions: list,
        tx_type: str = "perp_open",
    ) -> SolanaTransactionData:
        """Build a VersionedTransaction from instructions and serialize to base64.

        Uses a placeholder blockhash — SolanaExecutionPlanner will replace
        it with a fresh one before signing and submission.
        """
        payer = Pubkey.from_string(self.wallet_address)
        placeholder_blockhash = Hash.default()

        msg = MessageV0.try_compile(
            payer=payer,
            instructions=instructions,
            address_lookup_table_accounts=[],
            recent_blockhash=placeholder_blockhash,
        )

        num_signers = msg.header.num_required_signatures
        tx = VersionedTransaction.populate(msg, [Signature.default()] * num_signers)
        serialized = base64.b64encode(bytes(tx)).decode("ascii")

        direction_str = "perp order"
        return SolanaTransactionData(
            serialized_transaction=serialized,
            chain_family="SOLANA",
            tx_type=tx_type,
            description=f"Drift {direction_str}",
        )

    def _error_bundle(self, intent_type: IntentType, intent_id: str, error: str) -> ActionBundle:
        """Create an error ActionBundle."""
        return ActionBundle(
            intent_type=intent_type.value,
            transactions=[],
            metadata={
                "error": error,
                "intent_id": intent_id,
                "protocol": "drift",
                "chain": "solana",
            },
        )
