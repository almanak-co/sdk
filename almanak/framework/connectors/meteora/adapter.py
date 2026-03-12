"""Meteora DLMM LP Protocol Adapter.

Converts LP intents (LPOpenIntent, LPCloseIntent) to executable Solana
transactions using the Meteora DLMM program.

Like Raydium CLMM, Meteora DLMM builds instructions locally using `solders`
and serializes them into VersionedTransactions. The SolanaExecutionPlanner
handles blockhash replacement and signing.

Key difference from Raydium:
- Uses discrete price bins (not continuous ticks)
- Positions are Keypair-based accounts (not NFTs)
- position_id = base58 pubkey of position account

Example:
    config = MeteoraConfig(wallet_address="your-solana-pubkey")
    adapter = MeteoraAdapter(config)

    intent = LPOpenIntent(
        pool="pool-address",
        amount0=Decimal("1"),
        amount1=Decimal("150"),
        range_lower=Decimal("100"),
        range_upper=Decimal("200"),
        protocol="meteora_dlmm",
    )
    bundle = adapter.compile_lp_open_intent(intent)
"""

from __future__ import annotations

import base64
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from solders.hash import Hash
from solders.message import MessageV0
from solders.transaction import VersionedTransaction

from ...intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent
from ...models.reproduction_bundle import ActionBundle
from ..jupiter.adapter import SolanaTransactionData
from .math import price_to_bin_id
from .sdk import MeteoraSDK

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


class MeteoraConfig:
    """Configuration for Meteora adapter.

    Attributes:
        wallet_address: Solana wallet public key (Base58).
        rpc_url: Solana RPC endpoint URL (for position queries).
        default_strategy_type: Default strategy type (6=SpotBalanced).
    """

    def __init__(
        self,
        wallet_address: str,
        rpc_url: str = "",
        default_strategy_type: int = 6,
    ) -> None:
        if not wallet_address:
            raise ValueError("wallet_address is required")
        self.wallet_address = wallet_address
        self.rpc_url = rpc_url
        self.default_strategy_type = default_strategy_type


class MeteoraAdapter:
    """Adapter for Meteora DLMM integration with the Intent system.

    Converts LP intents to ActionBundles containing serialized
    Solana VersionedTransactions built from Meteora DLMM instructions.

    Example:
        config = MeteoraConfig(wallet_address="your-solana-pubkey")
        adapter = MeteoraAdapter(config)

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool="pool-address",
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
        )
        bundle = adapter.compile_lp_open_intent(intent)
    """

    def __init__(
        self,
        config: MeteoraConfig,
        token_resolver: TokenResolverType | None = None,
    ) -> None:
        self.config = config
        self.sdk = MeteoraSDK(wallet_address=config.wallet_address)
        self.wallet_address = config.wallet_address

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Cache pool lookups
        self._pool_cache: dict[str, Any] = {}

        logger.info(f"MeteoraAdapter initialized for wallet={self.wallet_address[:8]}...")

    def _resolve_pool(self, pool_identifier: str) -> Any:
        """Resolve a pool identifier to a MeteoraPool.

        Supports:
        - Direct pool address (Base58)
        - "TOKEN_A/TOKEN_B" format (e.g., "SOL/USDC")

        Args:
            pool_identifier: Pool address or "TOKEN_A/TOKEN_B".

        Returns:
            MeteoraPool instance.
        """
        if pool_identifier in self._pool_cache:
            return self._pool_cache[pool_identifier]

        parts = pool_identifier.split("/")
        if len(parts) == 2:
            token_a, token_b = parts[0], parts[1]
            mint_a = self._resolve_mint(token_a)
            mint_b = self._resolve_mint(token_b)

            pool = self.sdk.find_pool(mint_a, mint_b)
            if not pool:
                raise ValueError(f"No Meteora DLMM pool found for {token_a}/{token_b}")
        else:
            pool = self.sdk.get_pool(pool_identifier)

        self._pool_cache[pool_identifier] = pool
        return pool

    def _resolve_mint(self, token: str) -> str:
        """Resolve a token symbol or address to a mint address."""
        if len(token) > 30:
            return token
        resolved = self._token_resolver.resolve(token, "solana")
        return resolved.address

    def compile_lp_open_intent(self, intent: LPOpenIntent) -> ActionBundle:
        """Compile an LPOpenIntent to an ActionBundle.

        Builds Meteora DLMM initializePosition + addLiquidityByStrategy,
        serializes into a VersionedTransaction, and wraps in an ActionBundle.

        Args:
            intent: The LPOpenIntent to compile.

        Returns:
            ActionBundle containing serialized Solana transaction(s).
        """
        try:
            pool = self._resolve_pool(intent.pool)

            # Convert amounts to smallest units
            amount_x = int(Decimal(str(intent.amount0)) * Decimal(10) ** pool.decimals_x)
            amount_y = int(Decimal(str(intent.amount1)) * Decimal(10) ** pool.decimals_y)

            # Convert price bounds to bin IDs
            lower_bin_id = price_to_bin_id(
                Decimal(str(intent.range_lower)),
                pool.bin_step,
                decimals_x=pool.decimals_x,
                decimals_y=pool.decimals_y,
            )
            upper_bin_id = price_to_bin_id(
                Decimal(str(intent.range_upper)),
                pool.bin_step,
                decimals_x=pool.decimals_x,
                decimals_y=pool.decimals_y,
            )

            # Ensure lower < upper
            if lower_bin_id > upper_bin_id:
                lower_bin_id, upper_bin_id = upper_bin_id, lower_bin_id

            # Build instructions
            ixs, position_kp, metadata = self.sdk.build_open_position_transaction(
                pool=pool,
                lower_bin_id=lower_bin_id,
                upper_bin_id=upper_bin_id,
                amount_x=amount_x,
                amount_y=amount_y,
                strategy_type=self.config.default_strategy_type,
            )

            # Serialize into VersionedTransaction
            serialized = self._serialize_transaction(ixs)

            tx_data = SolanaTransactionData(
                serialized_transaction=serialized,
                tx_type="lp_open",
                description=f"Open Meteora DLMM position: {pool.symbol_x}/{pool.symbol_y}",
            )

            logger.info(
                f"Compiled Meteora LP open: {pool.symbol_x}/{pool.symbol_y}, bins=[{lower_bin_id}, {upper_bin_id}]"
            )

            bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "pool": pool.address,
                    "token_a": pool.symbol_x,
                    "token_b": pool.symbol_y,
                    "mint_a": pool.mint_x,
                    "mint_b": pool.mint_y,
                    "lower_bin_id": metadata["lower_bin_id"],
                    "upper_bin_id": metadata["upper_bin_id"],
                    "width": metadata["width"],
                    "position_address": metadata["position_address"],
                    "active_bin_id": metadata["active_bin_id"],
                    "bin_step": metadata["bin_step"],
                    "amount_x": str(amount_x),
                    "amount_y": str(amount_y),
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "meteora_dlmm",
                    "action": "open_position",
                    "strategy_type": metadata["strategy_type"],
                },
                sensitive_data={
                    "additional_signers": [base64.b64encode(bytes(position_kp)).decode("ascii")],
                },
            )
            return bundle

        except Exception as e:
            logger.exception(f"Failed to compile Meteora LP open intent: {e}")
            return self._error_bundle(IntentType.LP_OPEN, intent.intent_id, str(e))

    def compile_lp_close_intent(self, intent: LPCloseIntent) -> ActionBundle:
        """Compile an LPCloseIntent to an ActionBundle.

        Removes all liquidity and closes the position.

        Args:
            intent: The LPCloseIntent to compile.

        Returns:
            ActionBundle containing serialized Solana transaction(s).
        """
        try:
            position_address = intent.position_id

            if not intent.pool:
                return self._error_bundle(
                    IntentType.LP_CLOSE,
                    intent.intent_id,
                    "pool address is required for Meteora LP close",
                )

            pool = self._resolve_pool(intent.pool)

            # Query on-chain position state
            if self.config.rpc_url:
                position = self.sdk.get_position_state(position_address, self.config.rpc_url)
                logger.info(f"Fetched on-chain position: bins=[{position.lower_bin_id}, {position.upper_bin_id}]")
            else:
                logger.warning(
                    "No rpc_url configured -- LP close will attempt with default bin range. "
                    "Set rpc_url in MeteoraConfig for proper LP close."
                )
                from .models import MeteoraPosition

                position = MeteoraPosition(
                    position_address=position_address,
                    lb_pair=pool.address,
                    lower_bin_id=0,
                    upper_bin_id=0,
                )

            # Build remove + close instructions
            ixs, metadata = self.sdk.build_close_position_transaction(
                pool=pool,
                position=position,
            )

            if not ixs:
                return self._error_bundle(
                    IntentType.LP_CLOSE,
                    intent.intent_id,
                    "No instructions generated for LP close",
                )

            serialized = self._serialize_transaction(ixs)

            tx_data = SolanaTransactionData(
                serialized_transaction=serialized,
                tx_type="lp_close",
                description=f"Close Meteora DLMM position: {position_address[:8]}...",
            )

            logger.info(f"Compiled Meteora LP close: position={position_address[:8]}...")

            return ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "pool": pool.address,
                    "position_address": position_address,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "meteora_dlmm",
                    "action": "close_position",
                    **metadata,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Meteora LP close intent: {e}")
            return self._error_bundle(IntentType.LP_CLOSE, intent.intent_id, str(e))

    def _serialize_transaction(self, ixs: list) -> str:
        """Serialize instructions into a base64-encoded VersionedTransaction."""
        from solders.pubkey import Pubkey
        from solders.signature import Signature

        placeholder_blockhash = Hash.default()
        payer = Pubkey.from_string(self.wallet_address)
        msg = MessageV0.try_compile(
            payer=payer,
            instructions=ixs,
            address_lookup_table_accounts=[],
            recent_blockhash=placeholder_blockhash,
        )
        num_signers = msg.header.num_required_signatures
        tx = VersionedTransaction.populate(msg, [Signature.default()] * num_signers)
        return base64.b64encode(bytes(tx)).decode("ascii")

    def _error_bundle(self, intent_type: IntentType, intent_id: str, error: str) -> ActionBundle:
        """Create an error ActionBundle."""
        return ActionBundle(
            intent_type=intent_type.value,
            transactions=[],
            metadata={
                "error": error,
                "intent_id": intent_id,
            },
        )
