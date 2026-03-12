"""Kamino Finance Lending Protocol Adapter.

This module provides the KaminoAdapter class for converting lending intents
(SupplyIntent, BorrowIntent, RepayIntent, WithdrawIntent) to executable
Solana transactions using the Kamino Finance REST API.

Kamino follows the same REST API pattern as Jupiter: the API returns
base64-encoded unsigned VersionedTransactions ready for signing.

Key differences from EVM lending adapters (Aave):
- No approval transactions (Solana has no ERC-20 approve pattern)
- Transactions are serialized VersionedTransactions (base64)
- Token addresses are Solana mint addresses (Base58)
- Amounts are in human-readable token units (e.g., "100.5" USDC), NOT smallest
  units. This differs from Jupiter (lamports) and Raydium (raw amounts).

Example:
    from almanak.framework.connectors.kamino import KaminoAdapter, KaminoConfig

    config = KaminoConfig(wallet_address="your-solana-pubkey")
    adapter = KaminoAdapter(config)

    intent = SupplyIntent(protocol="kamino", token="USDC", amount=Decimal("100"))
    bundle = adapter.compile_supply_intent(intent)
"""

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from ...intents.vocabulary import (
    BorrowIntent,
    IntentType,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from ...models.reproduction_bundle import ActionBundle
from ..jupiter.adapter import SolanaTransactionData
from .client import U64_MAX, KaminoClient, KaminoConfig

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


class KaminoAdapter:
    """Adapter for Kamino Finance integration with the Intent system.

    Converts lending intents (Supply, Borrow, Repay, Withdraw) into
    ActionBundles containing serialized Solana VersionedTransactions
    from Kamino's REST API.

    Example:
        config = KaminoConfig(wallet_address="your-solana-pubkey")
        adapter = KaminoAdapter(config)

        intent = SupplyIntent(protocol="kamino", token="USDC", amount=Decimal("100"))
        bundle = adapter.compile_supply_intent(intent)
    """

    def __init__(
        self,
        config: KaminoConfig,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the Kamino adapter.

        Args:
            config: Kamino client configuration
            token_resolver: Optional TokenResolver instance
        """
        self.config = config
        self.client = KaminoClient(config)
        self.wallet_address = config.wallet_address

        # Initialize token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Cache reserve lookups: (market, token_symbol) -> reserve_address
        self._reserve_cache: dict[tuple[str, str], str] = {}

        logger.info(f"KaminoAdapter initialized for wallet={self.wallet_address[:8]}...")

    def _resolve_reserve(self, token_symbol: str, market: str | None = None) -> str:
        """Resolve a token symbol to its Kamino reserve address.

        First checks cache, then queries the Kamino API.

        Args:
            token_symbol: Token symbol (e.g., "USDC", "SOL")
            market: Market address (defaults to config market)

        Returns:
            Reserve address (Base58)

        Raises:
            ValueError: If reserve not found for the token
        """
        market_addr = market or self.config.market
        cache_key = (market_addr, token_symbol.upper())

        if cache_key in self._reserve_cache:
            return self._reserve_cache[cache_key]

        reserve = self.client.find_reserve_by_token(token_symbol, market_addr)
        if not reserve:
            raise ValueError(
                f"No Kamino reserve found for token '{token_symbol}' in market {market_addr[:8]}... "
                f"Check that the token is supported in this market."
            )

        self._reserve_cache[cache_key] = reserve.address
        return reserve.address

    def compile_supply_intent(
        self,
        intent: SupplyIntent,
    ) -> ActionBundle:
        """Compile a SupplyIntent to an ActionBundle using Kamino.

        Args:
            intent: The SupplyIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            if intent.amount == "all":
                return self._error_bundle(
                    IntentType.SUPPLY,
                    intent.intent_id,
                    "amount='all' must be resolved before compilation.",
                )

            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            amount_str = str(amount_decimal)

            # Resolve token to Kamino reserve
            reserve_addr = self._resolve_reserve(intent.token)

            # Build deposit transaction via Kamino API
            tx_response = self.client.deposit(
                reserve=reserve_addr,
                amount=amount_str,
            )

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="deposit",
                description=f"Supply {amount_str} {intent.token} to Kamino",
            )

            logger.info(f"Compiled Kamino supply: {intent.token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "reserve": reserve_addr,
                    "market": self.config.market,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "kamino",
                    "action": "deposit",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Kamino supply intent: {e}")
            return self._error_bundle(IntentType.SUPPLY, intent.intent_id, str(e))

    def compile_borrow_intent(
        self,
        intent: BorrowIntent,
    ) -> ActionBundle:
        """Compile a BorrowIntent to an ActionBundle using Kamino.

        Note: Kamino's borrow endpoint only handles the borrow action.
        If collateral needs to be deposited first, the compiler should
        emit a SupplyIntent + BorrowIntent sequence.

        Args:
            intent: The BorrowIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            amount_str = str(intent.borrow_amount)

            # Resolve borrow token to Kamino reserve
            reserve_addr = self._resolve_reserve(intent.borrow_token)

            # Build borrow transaction via Kamino API
            tx_response = self.client.borrow(
                reserve=reserve_addr,
                amount=amount_str,
            )

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="borrow",
                description=f"Borrow {amount_str} {intent.borrow_token} from Kamino",
            )

            logger.info(f"Compiled Kamino borrow: {intent.borrow_token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "borrow_token": intent.borrow_token,
                    "borrow_amount": amount_str,
                    "reserve": reserve_addr,
                    "market": self.config.market,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "kamino",
                    "action": "borrow",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Kamino borrow intent: {e}")
            return self._error_bundle(IntentType.BORROW, intent.intent_id, str(e))

    def compile_repay_intent(
        self,
        intent: RepayIntent,
    ) -> ActionBundle:
        """Compile a RepayIntent to an ActionBundle using Kamino.

        Args:
            intent: The RepayIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            if intent.amount == "all":
                return self._error_bundle(
                    IntentType.REPAY,
                    intent.intent_id,
                    "amount='all' must be resolved before compilation.",
                )

            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            amount_str = str(amount_decimal)

            # Resolve token to Kamino reserve
            reserve_addr = self._resolve_reserve(intent.token)

            # Build repay transaction via Kamino API
            tx_response = self.client.repay(
                reserve=reserve_addr,
                amount=amount_str,
            )

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="repay",
                description=f"Repay {amount_str} {intent.token} on Kamino",
            )

            logger.info(f"Compiled Kamino repay: {intent.token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "reserve": reserve_addr,
                    "market": self.config.market,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "kamino",
                    "action": "repay",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Kamino repay intent: {e}")
            return self._error_bundle(IntentType.REPAY, intent.intent_id, str(e))

    def compile_withdraw_intent(
        self,
        intent: WithdrawIntent,
    ) -> ActionBundle:
        """Compile a WithdrawIntent to an ActionBundle using Kamino.

        Args:
            intent: The WithdrawIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            # Handle withdraw_all flag
            if intent.withdraw_all:
                amount_str = U64_MAX
            elif intent.amount == "all":
                return self._error_bundle(
                    IntentType.WITHDRAW,
                    intent.intent_id,
                    "amount='all' must be resolved before compilation.",
                )
            else:
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                amount_str = str(amount_decimal)

            # Resolve token to Kamino reserve
            reserve_addr = self._resolve_reserve(intent.token)

            # Build withdraw transaction via Kamino API
            tx_response = self.client.withdraw(
                reserve=reserve_addr,
                amount=amount_str,
            )

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="withdraw",
                description=f"Withdraw {intent.token} from Kamino"
                + (" (all)" if intent.withdraw_all else f" ({amount_str})"),
            )

            logger.info(
                f"Compiled Kamino withdraw: {intent.token}, amount={'all' if intent.withdraw_all else amount_str}"
            )

            return ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "withdraw_all": intent.withdraw_all,
                    "reserve": reserve_addr,
                    "market": self.config.market,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "kamino",
                    "action": "withdraw",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Kamino withdraw intent: {e}")
            return self._error_bundle(IntentType.WITHDRAW, intent.intent_id, str(e))

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
