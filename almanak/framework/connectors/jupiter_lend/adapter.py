"""Jupiter Lend Protocol Adapter.

This module provides the JupiterLendAdapter class for converting lending intents
(SupplyIntent, BorrowIntent, RepayIntent, WithdrawIntent) to executable
Solana transactions using the Jupiter Lend REST API.

Jupiter Lend follows the same REST API pattern as Kamino: the API returns
base64-encoded unsigned VersionedTransactions ready for signing.

Key differences from Kamino:
- Uses isolated vaults instead of a shared market with reserves
- No market parameter needed (vaults are self-contained)
- API endpoint structure is Jupiter-specific

Example:
    from almanak.framework.connectors.jupiter_lend import JupiterLendAdapter, JupiterLendConfig

    config = JupiterLendConfig(wallet_address="your-solana-pubkey")
    adapter = JupiterLendAdapter(config)

    intent = SupplyIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("100"))
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
from .client import U64_MAX, JupiterLendClient, JupiterLendConfig
from .models import JupiterLendTransactionResponse

if TYPE_CHECKING:
    from ...data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


def _normalize_decimal(value: Decimal, decimals: int | None = None) -> str:
    """Convert a Decimal to a plain numeric string (no scientific notation).

    If decimals is provided, rounds down (truncates) to that many decimal places
    to avoid overspending.
    """
    if decimals is not None:
        quantize_exp = Decimal(10) ** -decimals
        value = value.quantize(quantize_exp, rounding="ROUND_DOWN")
    return format(value, "f")


def _validate_transaction(tx_response: "JupiterLendTransactionResponse", action: str) -> None:
    """Validate that the API returned a non-empty transaction."""
    if not tx_response.transaction:
        raise ValueError(f"Jupiter Lend API returned empty transaction for {action}")


class JupiterLendAdapter:
    """Adapter for Jupiter Lend integration with the Intent system.

    Converts lending intents (Supply, Borrow, Repay, Withdraw) into
    ActionBundles containing serialized Solana VersionedTransactions
    from Jupiter Lend's REST API.

    Example:
        config = JupiterLendConfig(wallet_address="your-solana-pubkey")
        adapter = JupiterLendAdapter(config)

        intent = SupplyIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("100"))
        bundle = adapter.compile_supply_intent(intent)
    """

    def __init__(
        self,
        config: JupiterLendConfig,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the Jupiter Lend adapter.

        Args:
            config: Jupiter Lend client configuration
            token_resolver: Optional TokenResolver instance
        """
        self.config = config
        self.client = JupiterLendClient(config)
        self.wallet_address = config.wallet_address

        # Initialize token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from ...data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Cache vault lookups: token_symbol -> vault_address
        self._vault_cache: dict[str, str] = {}

        logger.info(f"JupiterLendAdapter initialized for wallet={self.wallet_address[:8]}...")

    def _resolve_vault(self, token_symbol: str) -> str:
        """Resolve a token symbol to its Jupiter Lend vault address.

        First checks cache, then queries the Jupiter Lend API.

        Args:
            token_symbol: Token symbol (e.g., "USDC", "SOL")

        Returns:
            Vault address (Base58)

        Raises:
            ValueError: If vault not found for the token
        """
        cache_key = token_symbol.upper()

        if cache_key in self._vault_cache:
            return self._vault_cache[cache_key]

        vault = self.client.find_vault_by_token(token_symbol)
        if not vault:
            raise ValueError(
                f"No Jupiter Lend vault found for token '{token_symbol}'. Check that the token is supported."
            )

        self._vault_cache[cache_key] = vault.address
        return vault.address

    def compile_supply_intent(
        self,
        intent: SupplyIntent,
    ) -> ActionBundle:
        """Compile a SupplyIntent to an ActionBundle using Jupiter Lend.

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

            # Resolve token decimals and round down to avoid overspending
            token_decimals = self._get_token_decimals(intent.token)
            amount_str = _normalize_decimal(amount_decimal, decimals=token_decimals)

            # Resolve token to Jupiter Lend vault
            vault_addr = self._resolve_vault(intent.token)

            # Build deposit transaction via Jupiter Lend API
            tx_response = self.client.deposit(
                vault=vault_addr,
                amount=amount_str,
            )
            _validate_transaction(tx_response, "deposit")

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="deposit",
                description=f"Supply {amount_str} {intent.token} to Jupiter Lend",
            )

            logger.info(f"Compiled Jupiter Lend supply: {intent.token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "vault": vault_addr,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "jupiter_lend",
                    "action": "deposit",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Jupiter Lend supply intent: {e}")
            return self._error_bundle(IntentType.SUPPLY, intent.intent_id, str(e))

    def compile_borrow_intent(
        self,
        intent: BorrowIntent,
    ) -> ActionBundle:
        """Compile a BorrowIntent to an ActionBundle using Jupiter Lend.

        Args:
            intent: The BorrowIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            token_decimals = self._get_token_decimals(intent.borrow_token)
            amount_str = _normalize_decimal(intent.borrow_amount, decimals=token_decimals)

            # Resolve borrow token to Jupiter Lend vault
            vault_addr = self._resolve_vault(intent.borrow_token)

            # Build borrow transaction via Jupiter Lend API
            tx_response = self.client.borrow(
                vault=vault_addr,
                amount=amount_str,
            )
            _validate_transaction(tx_response, "borrow")

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="borrow",
                description=f"Borrow {amount_str} {intent.borrow_token} from Jupiter Lend",
            )

            logger.info(f"Compiled Jupiter Lend borrow: {intent.borrow_token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "borrow_token": intent.borrow_token,
                    "borrow_amount": amount_str,
                    "vault": vault_addr,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "jupiter_lend",
                    "action": "borrow",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Jupiter Lend borrow intent: {e}")
            return self._error_bundle(IntentType.BORROW, intent.intent_id, str(e))

    def compile_repay_intent(
        self,
        intent: RepayIntent,
    ) -> ActionBundle:
        """Compile a RepayIntent to an ActionBundle using Jupiter Lend.

        Args:
            intent: The RepayIntent to compile

        Returns:
            ActionBundle containing a serialized Solana transaction
        """
        try:
            # Handle repay_full flag (repay entire outstanding debt)
            if intent.repay_full:
                amount_str = U64_MAX
            elif intent.amount == "all":
                return self._error_bundle(
                    IntentType.REPAY,
                    intent.intent_id,
                    "amount='all' must be resolved before compilation.",
                )
            else:
                amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
                token_decimals = self._get_token_decimals(intent.token)
                amount_str = _normalize_decimal(amount_decimal, decimals=token_decimals)

            # Resolve token to Jupiter Lend vault
            vault_addr = self._resolve_vault(intent.token)

            # Build repay transaction via Jupiter Lend API
            tx_response = self.client.repay(
                vault=vault_addr,
                amount=amount_str,
            )
            _validate_transaction(tx_response, "repay")

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="repay",
                description=f"Repay {intent.token} on Jupiter Lend"
                + (" (full)" if intent.repay_full else f" ({amount_str})"),
            )

            logger.info(f"Compiled Jupiter Lend repay: {intent.token}, amount={amount_str}")

            return ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "vault": vault_addr,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "jupiter_lend",
                    "action": "repay",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Jupiter Lend repay intent: {e}")
            return self._error_bundle(IntentType.REPAY, intent.intent_id, str(e))

    def compile_withdraw_intent(
        self,
        intent: WithdrawIntent,
    ) -> ActionBundle:
        """Compile a WithdrawIntent to an ActionBundle using Jupiter Lend.

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
                token_decimals = self._get_token_decimals(intent.token)
                amount_str = _normalize_decimal(amount_decimal, decimals=token_decimals)

            # Resolve token to Jupiter Lend vault
            vault_addr = self._resolve_vault(intent.token)

            # Build withdraw transaction via Jupiter Lend API
            tx_response = self.client.withdraw(
                vault=vault_addr,
                amount=amount_str,
            )
            _validate_transaction(tx_response, "withdraw")

            tx_data = SolanaTransactionData(
                serialized_transaction=tx_response.transaction,
                tx_type="withdraw",
                description=f"Withdraw {intent.token} from Jupiter Lend"
                + (" (all)" if intent.withdraw_all else f" ({amount_str})"),
            )

            logger.info(
                f"Compiled Jupiter Lend withdraw: {intent.token}, amount={'all' if intent.withdraw_all else amount_str}"
            )

            return ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx_data.to_dict()],
                metadata={
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "amount": amount_str,
                    "withdraw_all": intent.withdraw_all,
                    "vault": vault_addr,
                    "chain": "solana",
                    "chain_family": "SOLANA",
                    "protocol": "jupiter_lend",
                    "action": "withdraw",
                },
            )

        except Exception as e:
            logger.exception(f"Failed to compile Jupiter Lend withdraw intent: {e}")
            return self._error_bundle(IntentType.WITHDRAW, intent.intent_id, str(e))

    def _get_token_decimals(self, token_symbol: str) -> int:
        """Get token decimals from the resolver for amount rounding.

        Raises:
            TokenNotFoundError/TokenResolutionError: If decimals cannot be resolved.
                Compilation must fail explicitly rather than sending unrounded amounts.
        """
        return self._token_resolver.get_decimals("solana", token_symbol)

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
