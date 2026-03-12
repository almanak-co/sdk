"""Raydium CLMM LP Protocol Adapter.

Converts LP intents (LPOpenIntent, LPCloseIntent) to executable Solana
transactions using the Raydium CLMM program.

Unlike Jupiter/Kamino (which have REST APIs returning pre-built transactions),
Raydium CLMM requires building instructions locally using `solders`, then
serializing them into a VersionedTransaction. The SolanaExecutionPlanner
handles the serialized transaction format.

Example:
    config = RaydiumConfig(wallet_address="your-solana-pubkey")
    adapter = RaydiumAdapter(config)

    intent = LPOpenIntent(
        pool="SOL/USDC/60",
        amount0=Decimal("1"),
        amount1=Decimal("150"),
        range_lower=Decimal("100"),
        range_upper=Decimal("200"),
        protocol="raydium_clmm",
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
from .models import RaydiumPosition
from .sdk import RaydiumCLMMSDK

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


class RaydiumConfig:
    """Configuration for Raydium adapter.

    Attributes:
        wallet_address: Solana wallet public key (Base58).
        rpc_url: Solana RPC endpoint URL (for pool queries via RPC).
    """

    def __init__(self, wallet_address: str, rpc_url: str = "") -> None:
        if not wallet_address:
            raise ValueError("wallet_address is required")
        self.wallet_address = wallet_address
        self.rpc_url = rpc_url


class RaydiumAdapter:
    """Adapter for Raydium CLMM integration with the Intent system.

    Converts LP intents to ActionBundles containing serialized
    Solana VersionedTransactions built from Raydium CLMM instructions.

    Example:
        config = RaydiumConfig(wallet_address="your-solana-pubkey")
        adapter = RaydiumAdapter(config)

        intent = LPOpenIntent(
            pool="SOL/USDC/60",
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            protocol="raydium_clmm",
        )
        bundle = adapter.compile_lp_open_intent(intent)
    """

    def __init__(
        self,
        config: RaydiumConfig,
        token_resolver: TokenResolverType | None = None,
    ) -> None:
        self.config = config
        self.sdk = RaydiumCLMMSDK(wallet_address=config.wallet_address)
        self.wallet_address = config.wallet_address

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Cache pool lookups
        self._pool_cache: dict[str, Any] = {}

        logger.info(f"RaydiumAdapter initialized for wallet={self.wallet_address[:8]}...")

    def _resolve_pool(self, pool_identifier: str) -> Any:
        """Resolve a pool identifier to a RaydiumPool.

        Supports:
        - Direct pool address (Base58)
        - "TOKEN_A/TOKEN_B/TICK_SPACING" format (e.g., "SOL/USDC/60")

        Args:
            pool_identifier: Pool address or "TOKEN_A/TOKEN_B/TICK_SPACING".

        Returns:
            RaydiumPool instance.
        """
        if pool_identifier in self._pool_cache:
            return self._pool_cache[pool_identifier]

        parts = pool_identifier.split("/")
        if len(parts) == 3:
            # TOKEN_A/TOKEN_B/TICK_SPACING format
            token_a, token_b, tick_spacing = parts[0], parts[1], int(parts[2])

            # Resolve token symbols to mint addresses
            mint_a = self._resolve_mint(token_a)
            mint_b = self._resolve_mint(token_b)

            pool = self.sdk.find_pool_by_tokens(mint_a, mint_b, tick_spacing)
            if not pool:
                raise ValueError(f"No Raydium CLMM pool found for {token_a}/{token_b} with tick_spacing={tick_spacing}")
        else:
            # Direct pool address
            pool = self.sdk.get_pool_info(pool_identifier)

        self._pool_cache[pool_identifier] = pool
        return pool

    def _resolve_mint(self, token: str) -> str:
        """Resolve a token symbol or address to a mint address."""
        # If it looks like a Solana address (long base58), return as-is
        if len(token) > 30:
            return token

        # Resolve via token resolver
        resolved = self._token_resolver.resolve(token, "solana")
        return resolved.address

    def compile_lp_open_intent(self, intent: LPOpenIntent) -> ActionBundle:
        """Compile an LPOpenIntent to an ActionBundle.

        Builds Raydium CLMM openPosition instructions, serializes them
        into a VersionedTransaction, and wraps in an ActionBundle.

        Args:
            intent: The LPOpenIntent to compile.

        Returns:
            ActionBundle containing serialized Solana transaction(s).
        """
        try:
            # Resolve pool
            pool = self._resolve_pool(intent.pool)

            # Convert amounts to smallest units
            amount_a = int(Decimal(str(intent.amount0)) * Decimal(10) ** pool.decimals_a)
            amount_b = int(Decimal(str(intent.amount1)) * Decimal(10) ** pool.decimals_b)

            # Build instructions
            ixs, nft_mint_kp, metadata = self.sdk.build_open_position_transaction(
                pool=pool,
                price_lower=float(intent.range_lower),
                price_upper=float(intent.range_upper),
                amount_a=amount_a,
                amount_b=amount_b,
            )

            # Serialize instructions into a VersionedTransaction
            # Use a placeholder blockhash — the planner will refresh it
            placeholder_blockhash = Hash.default()
            from solders.pubkey import Pubkey

            payer = Pubkey.from_string(self.wallet_address)
            msg = MessageV0.try_compile(
                payer=payer,
                instructions=ixs,
                address_lookup_table_accounts=[],
                recent_blockhash=placeholder_blockhash,
            )
            # Use populate() with placeholder signatures — we don't have the
            # wallet keypair at compile time. The planner will re-sign at execution.
            from solders.signature import Signature

            num_signers = msg.header.num_required_signatures
            tx = VersionedTransaction.populate(msg, [Signature.default()] * num_signers)
            serialized = base64.b64encode(bytes(tx)).decode("ascii")

            tx_data = SolanaTransactionData(
                serialized_transaction=serialized,
                tx_type="lp_open",
                description=f"Open Raydium CLMM position: {pool.symbol_a}/{pool.symbol_b}",
            )

            logger.info(
                f"Compiled Raydium LP open: {pool.symbol_a}/{pool.symbol_b}, "
                f"ticks=[{metadata['tick_lower']}, {metadata['tick_upper']}]"
            )

            bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "pool": pool.address,
                    "token_a": pool.symbol_a,
                    "token_b": pool.symbol_b,
                    "mint_a": pool.mint_a,
                    "mint_b": pool.mint_b,
                    "tick_lower": metadata["tick_lower"],
                    "tick_upper": metadata["tick_upper"],
                    "liquidity": metadata["liquidity"],
                    "nft_mint": metadata["nft_mint"],
                    "amount_a": str(amount_a),
                    "amount_b": str(amount_b),
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "raydium_clmm",
                    "action": "open_position",
                },
                # Keypair material in sensitive_data -- excluded from serialization/logging
                sensitive_data={
                    "additional_signers": [base64.b64encode(bytes(nft_mint_kp)).decode("ascii")],
                },
            )
            return bundle

        except Exception as e:
            logger.exception(f"Failed to compile Raydium LP open intent: {e}")
            return self._error_bundle(IntentType.LP_OPEN, intent.intent_id, str(e))

    def compile_lp_close_intent(self, intent: LPCloseIntent) -> ActionBundle:
        """Compile an LPCloseIntent to an ActionBundle.

        Decreases all liquidity from the position and closes it.

        Args:
            intent: The LPCloseIntent to compile.

        Returns:
            ActionBundle containing serialized Solana transaction(s).
        """
        try:
            # The position_id is the NFT mint address
            nft_mint = intent.position_id

            # We need the pool info — try to resolve from intent.pool
            if not intent.pool:
                return self._error_bundle(
                    IntentType.LP_CLOSE,
                    intent.intent_id,
                    "pool address is required for Raydium LP close",
                )

            pool = self._resolve_pool(intent.pool)

            # Query on-chain position state for tick range and liquidity
            if self.config.rpc_url:
                position = self.sdk.get_position_state(nft_mint, self.config.rpc_url)
                logger.info(
                    f"Fetched on-chain position: liquidity={position.liquidity}, "
                    f"ticks=[{position.tick_lower}, {position.tick_upper}]"
                )
            else:
                logger.warning(
                    "No rpc_url configured — LP close will skip decreaseLiquidity. "
                    "Set rpc_url in RaydiumConfig for proper LP close."
                )
                position = RaydiumPosition(
                    nft_mint=nft_mint,
                    pool_address=pool.address,
                    tick_lower=0,
                    tick_upper=0,
                    liquidity=0,
                )

            # Build decrease + close instructions
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

            # Serialize into VersionedTransaction
            placeholder_blockhash = Hash.default()
            from solders.pubkey import Pubkey

            payer = Pubkey.from_string(self.wallet_address)
            msg = MessageV0.try_compile(
                payer=payer,
                instructions=ixs,
                address_lookup_table_accounts=[],
                recent_blockhash=placeholder_blockhash,
            )
            from solders.signature import Signature

            num_signers = msg.header.num_required_signatures
            tx = VersionedTransaction.populate(msg, [Signature.default()] * num_signers)
            serialized = base64.b64encode(bytes(tx)).decode("ascii")

            tx_data = SolanaTransactionData(
                serialized_transaction=serialized,
                tx_type="lp_close",
                description=f"Close Raydium CLMM position: {nft_mint[:8]}...",
            )

            logger.info(f"Compiled Raydium LP close: nft_mint={nft_mint[:8]}...")

            return ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "pool": pool.address,
                    "nft_mint": nft_mint,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "raydium_clmm",
                    "action": "close_position",
                    **metadata,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Raydium LP close intent: {e}")
            return self._error_bundle(IntentType.LP_CLOSE, intent.intent_id, str(e))

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
