"""Jupiter Protocol Adapter.

This module provides the JupiterAdapter class for converting SwapIntents
to executable Solana transactions using the Jupiter DEX aggregator.

Jupiter aggregates liquidity across Solana DEXs (Raydium, Orca, Meteora, etc.)
to find optimal swap routes. The adapter returns ActionBundles containing
base64-encoded VersionedTransactions ready for signing.

Key differences from EVM adapters (Enso):
- No approval transactions (Solana has no ERC-20 approve pattern)
- Transactions are serialized VersionedTransactions (base64), not calldata
- Token addresses are Solana mint addresses (Base58), not EVM addresses (hex)

Example:
    from almanak.framework.connectors.jupiter import JupiterAdapter, JupiterConfig

    config = JupiterConfig(wallet_address="your-solana-pubkey")
    adapter = JupiterAdapter(config)

    intent = SwapIntent(
        from_token="USDC",
        to_token="SOL",
        amount=Decimal("100"),
    )
    bundle = adapter.compile_swap_intent(intent)
"""

import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ...data.tokens.exceptions import TokenResolutionError
from ...intents.vocabulary import IntentType, SwapIntent
from ...models.reproduction_bundle import ActionBundle
from .client import JupiterClient, JupiterConfig

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)

# Default gas estimates (in compute units, not gas like EVM)
JUPITER_COMPUTE_ESTIMATES: dict[str, int] = {
    "swap": 400_000,
    "swap_multi_hop": 800_000,
}


@dataclass
class SolanaTransactionData:
    """Solana transaction data for ActionBundle.

    Attributes:
        serialized_transaction: Base64-encoded VersionedTransaction
        chain_family: Always "SOLANA"
        tx_type: Type of transaction (e.g., "swap")
        description: Human-readable description
        last_valid_block_height: Block height after which tx expires
        priority_fee_lamports: Priority fee included
    """

    serialized_transaction: str
    chain_family: str = "SOLANA"
    tx_type: str = "swap"
    description: str = ""
    last_valid_block_height: int = 0
    priority_fee_lamports: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for ActionBundle.transactions."""
        return {
            "serialized_transaction": self.serialized_transaction,
            "chain_family": self.chain_family,
            "tx_type": self.tx_type,
            "description": self.description,
            "last_valid_block_height": self.last_valid_block_height,
            "priority_fee_lamports": self.priority_fee_lamports,
        }


class JupiterAdapter:
    """Adapter for Jupiter protocol integration with the Intent system.

    Converts SwapIntents into ActionBundles containing serialized
    Solana VersionedTransactions from Jupiter's API.

    Unlike EVM adapters:
    - No approval transactions needed
    - Transactions contain base64-encoded VersionedTransactions
    - Route freshness is managed via get_fresh_swap_transaction()

    Example:
        config = JupiterConfig(wallet_address="your-solana-pubkey")
        adapter = JupiterAdapter(config)

        intent = SwapIntent(from_token="USDC", to_token="SOL", amount=Decimal("100"))
        bundle = adapter.compile_swap_intent(intent)
    """

    def __init__(
        self,
        config: JupiterConfig,
        price_provider: dict[str, Decimal] | None = None,
        allow_placeholder_prices: bool = False,
        token_resolver: "TokenResolverType | None" = None,
        rpc_url: str | None = None,
    ) -> None:
        """Initialize the Jupiter adapter.

        Args:
            config: Jupiter client configuration
            price_provider: Price oracle dict (token symbol -> USD price)
            allow_placeholder_prices: If True, allows running without prices (testing only)
            token_resolver: Optional TokenResolver instance
            rpc_url: Solana RPC URL for balance queries (resolving amount='all').
                Falls back to SOLANA_RPC_URL env var or public mainnet.
        """
        self._using_placeholders = price_provider is None
        if self._using_placeholders and not allow_placeholder_prices:
            raise ValueError(
                "JupiterAdapter requires price_provider for production use. "
                "Pass a dict mapping token symbols to USD prices "
                "or set allow_placeholder_prices=True for testing only."
            )

        self.config = config
        self.client = JupiterClient(config)
        self.wallet_address = config.wallet_address
        self._rpc_url = rpc_url or os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

        # Initialize token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price provider
        if self._using_placeholders:
            logger.warning("JupiterAdapter using PLACEHOLDER PRICES (testing only)")
            self._price_provider = self._get_placeholder_prices()
        else:
            self._price_provider = price_provider or {}

        logger.info(
            f"JupiterAdapter initialized for wallet={self.wallet_address[:8]}..., "
            f"using_placeholders={self._using_placeholders}"
        )

    def resolve_token_address(self, token: str) -> str:
        """Resolve token symbol or address to Solana mint address.

        Args:
            token: Token symbol (e.g., "USDC") or Solana mint address

        Returns:
            Solana mint address (Base58)

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        # If it looks like a Solana address (Base58, 32-44 chars, no 0x prefix)
        if not token.startswith("0x") and len(token) >= 32:
            return token
        try:
            resolved = self._token_resolver.resolve_for_swap(token, "solana")
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain="solana",
                reason=f"[JupiterAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def get_token_decimals(self, token: str) -> int:
        """Get token decimals using TokenResolver.

        Args:
            token: Token symbol or Solana mint address

        Returns:
            Token decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(token, "solana")
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain="solana",
                reason=f"[JupiterAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def compile_swap_intent(
        self,
        intent: SwapIntent,
        price_oracle: dict[str, Decimal] | None = None,
    ) -> ActionBundle:
        """Compile a SwapIntent to an ActionBundle using Jupiter.

        Args:
            intent: The SwapIntent to compile
            price_oracle: Optional price oracle for USD conversions

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            if price_oracle is None:
                price_oracle = self._price_provider

            # Resolve token mint addresses
            input_mint = self.resolve_token_address(intent.from_token)
            output_mint = self.resolve_token_address(intent.to_token)

            # Determine the swap amount in smallest units
            if intent.amount is not None:
                if intent.amount == "all":
                    # Resolve amount='all' by querying the wallet's SPL token balance
                    resolved = self._resolve_all_amount(input_mint)
                    if resolved is None:
                        return self._error_bundle(
                            intent,
                            f"amount='all' could not be resolved: no balance found for {intent.from_token} "
                            f"(mint={input_mint}) in wallet {self.wallet_address[:8]}...",
                        )
                    amount_in_smallest, decimals = resolved
                    if amount_in_smallest <= 0:
                        return self._error_bundle(
                            intent,
                            f"amount='all' resolved to 0 for {intent.from_token} — wallet has no balance",
                        )
                    amount_decimal = Decimal(amount_in_smallest) / Decimal(10**decimals)
                    logger.info(
                        "Resolved amount='all' for %s: %s (raw=%d, decimals=%d)",
                        intent.from_token,
                        amount_decimal,
                        amount_in_smallest,
                        decimals,
                    )
                else:
                    amount_decimal = intent.amount  # type: ignore[assignment]
                    decimals = self.get_token_decimals(intent.from_token)
                    amount_in_smallest = int(amount_decimal * Decimal(10**decimals))
            elif intent.amount_usd is not None:
                from_price = price_oracle.get(intent.from_token.upper())
                if not from_price:
                    return self._error_bundle(
                        intent,
                        f"Price unavailable for '{intent.from_token}' -- cannot convert amount_usd to token amount.",
                    )
                token_amount = intent.amount_usd / from_price
                decimals = self.get_token_decimals(intent.from_token)
                amount_in_smallest = int(token_amount * Decimal(10**decimals))
            else:
                return self._error_bundle(intent, "Either amount or amount_usd must be specified")

            # Convert slippage from decimal to basis points
            slippage_bps = int(intent.max_slippage * 10000)

            # Get quote from Jupiter
            quote = self.client.get_quote(
                input_mint=input_mint,
                output_mint=output_mint,
                amount=amount_in_smallest,
                slippage_bps=slippage_bps,
            )

            # Get swap transaction (with optional priority fee from intent)
            swap_tx = self.client.get_swap_transaction(
                quote=quote,
                user_public_key=self.wallet_address,
                priority_fee_level=getattr(intent, "priority_fee_level", None),
                priority_fee_max_lamports=getattr(intent, "priority_fee_max_lamports", None),
            )

            # Build Solana transaction data
            tx_data = SolanaTransactionData(
                serialized_transaction=swap_tx.swap_transaction,
                tx_type="swap",
                description=f"Swap {intent.from_token} -> {intent.to_token} via Jupiter",
                last_valid_block_height=swap_tx.last_valid_block_height,
                priority_fee_lamports=swap_tx.priority_fee_lamports,
            )

            logger.info(
                f"Compiled Jupiter swap: {intent.from_token} -> {intent.to_token}, "
                f"amount_in={amount_in_smallest}, amount_out={quote.out_amount}, "
                f"impact={quote.price_impact_pct}%"
            )

            return ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "from_token": intent.from_token,
                    "to_token": intent.to_token,
                    "input_mint": input_mint,
                    "output_mint": output_mint,
                    "amount_in": str(amount_in_smallest),
                    "amount_out": quote.out_amount,
                    "slippage_bps": slippage_bps,
                    "price_impact_pct": quote.price_impact_pct,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "jupiter",
                    "last_valid_block_height": swap_tx.last_valid_block_height,
                    # Parameters for refreshing route at execution time
                    "deferred_swap": True,
                    "route_params": {
                        "input_mint": input_mint,
                        "output_mint": output_mint,
                        "amount": amount_in_smallest,
                        "slippage_bps": slippage_bps,
                        "priority_fee_level": getattr(intent, "priority_fee_level", None),
                        "priority_fee_max_lamports": getattr(intent, "priority_fee_max_lamports", None),
                    },
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Jupiter swap intent: {e}")
            return self._error_bundle(intent, str(e))

    def get_fresh_swap_transaction(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch fresh swap transaction data immediately before execution.

        Jupiter routes expire quickly. This method fetches a fresh quote
        and transaction right before signing.

        Args:
            metadata: The metadata from a compiled ActionBundle containing route_params

        Returns:
            Fresh transaction data dict

        Raises:
            ValueError: If metadata doesn't contain route_params
        """
        route_params = metadata.get("route_params")
        if not route_params:
            raise ValueError(
                "metadata must contain 'route_params' for deferred swap. "
                "Ensure the bundle was compiled with compile_swap_intent()."
            )

        input_mint = route_params["input_mint"]
        output_mint = route_params["output_mint"]
        amount = route_params["amount"]
        slippage_bps = route_params["slippage_bps"]
        priority_fee_level = route_params.get("priority_fee_level")
        priority_fee_max_lamports = route_params.get("priority_fee_max_lamports")

        logger.info(
            f"Fetching fresh Jupiter route: {input_mint[:8]}... -> {output_mint[:8]}..., "
            f"amount={amount}, slippage={slippage_bps}bp"
        )

        # Fresh quote + swap transaction
        quote = self.client.get_quote(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=amount,
            slippage_bps=slippage_bps,
        )

        swap_tx = self.client.get_swap_transaction(
            quote=quote,
            user_public_key=self.wallet_address,
            priority_fee_level=priority_fee_level,
            priority_fee_max_lamports=priority_fee_max_lamports,
        )

        logger.info(f"Fresh Jupiter route: amount_out={quote.out_amount}, impact={quote.price_impact_pct}%")

        return {
            "serialized_transaction": swap_tx.swap_transaction,
            "chain_family": "SOLANA",
            "tx_type": "swap",
            "last_valid_block_height": swap_tx.last_valid_block_height,
            "priority_fee_lamports": swap_tx.priority_fee_lamports,
            "amount_out": quote.out_amount,
            "price_impact_pct": quote.price_impact_pct,
            "description": (f"Swap {metadata.get('from_token', '?')} -> {metadata.get('to_token', '?')} via Jupiter"),
        }

    def _resolve_all_amount(self, mint_address: str) -> tuple[int, int] | None:
        """Resolve amount='all' by querying the wallet's SPL token balance.

        Uses a lightweight RPC call to getTokenAccountsByOwner to find the
        wallet's balance for the given token mint.

        Args:
            mint_address: SPL token mint address (base58).

        Returns:
            Tuple of (raw_amount, decimals) or None if query fails.
        """
        try:
            import requests as req

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    self.wallet_address,
                    {"mint": mint_address},
                    {"encoding": "jsonParsed"},
                ],
            }
            resp = req.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                logger.warning("RPC error querying token balance: %s", data["error"])
                return None

            accounts = data.get("result", {}).get("value", [])
            if not accounts:
                logger.info(
                    "No token accounts found for mint %s in wallet %s", mint_address[:8], self.wallet_address[:8]
                )
                return 0, 0

            total_raw = 0
            decimals = 0
            for account in accounts:
                try:
                    parsed = account["account"]["data"]["parsed"]["info"]["tokenAmount"]
                    total_raw += int(parsed["amount"])
                    decimals = int(parsed["decimals"])
                except (KeyError, ValueError, TypeError):
                    continue

            return total_raw, decimals

        except Exception as e:
            logger.warning("Failed to resolve amount='all' via RPC: %s", e)
            return None

    def _error_bundle(self, intent: SwapIntent, error: str) -> ActionBundle:
        """Create an error ActionBundle."""
        return ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[],
            metadata={
                "error": error,
                "intent_id": intent.intent_id,
            },
        )

    @staticmethod
    def _get_placeholder_prices() -> dict[str, Decimal]:
        """Get placeholder prices for testing only."""
        return {
            "SOL": Decimal("150"),
            "WSOL": Decimal("150"),
            "USDC": Decimal("1"),
            "USDT": Decimal("1"),
            "JUP": Decimal("1"),
            "RAY": Decimal("2"),
            "ORCA": Decimal("0.5"),
            "BONK": Decimal("0.00002"),
            "WIF": Decimal("1.5"),
            "JTO": Decimal("3"),
            "PYTH": Decimal("0.4"),
            "MSOL": Decimal("170"),
            "JITOSOL": Decimal("170"),
        }
