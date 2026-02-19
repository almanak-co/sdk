"""Kraken adapter for intent compilation.

Converts stack-v2 intents into ActionBundles for CEX execution.
Integrates with the unified intent system using the `venue` parameter.

Example:
    from almanak.framework.connectors.kraken import KrakenAdapter
    from almanak.framework.intents.vocabulary import SwapIntent

    adapter = KrakenAdapter(config, sdk)

    # Compile a swap intent
    intent = SwapIntent(
        from_token="USDC",
        to_token="ETH",
        amount=Decimal("1000"),
        venue="kraken",
    )
    bundle = adapter.compile_intent(intent, context)
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

import structlog

from .exceptions import (
    KrakenChainNotSupportedError,
    KrakenInsufficientFundsError,
    KrakenMinimumOrderError,
    KrakenUnknownPairError,
)
from .models import CEXIdempotencyKey, CEXOperationType, KrakenConfig
from .receipt_resolver import ExecutionDetails, KrakenReceiptResolver
from .sdk import KrakenSDK
from .token_resolver import KrakenChainMapper, KrakenTokenResolver

logger = structlog.get_logger(__name__)


# =============================================================================
# Enums and Types
# =============================================================================


class VenueType(StrEnum):
    """Execution venue type."""

    DEX = "dex"
    CEX = "cex"


class ActionType(StrEnum):
    """CEX action types."""

    CEX_SWAP = "cex_swap"
    CEX_WITHDRAW = "cex_withdraw"
    CEX_DEPOSIT = "cex_deposit"


# =============================================================================
# Action and ActionBundle Models
# =============================================================================


@dataclass
class CEXAction:
    """A single CEX action to execute.

    Actions are the atomic units of work in CEX execution.
    """

    id: str  # Unique action identifier
    type: ActionType
    exchange: str  # "kraken"

    # Swap-specific
    asset_in: str | None = None
    asset_out: str | None = None
    amount_in: int | None = None  # Wei units
    decimals_in: int | None = None
    decimals_out: int | None = None

    # Withdraw-specific
    asset: str | None = None
    amount: int | None = None
    decimals: int | None = None
    chain: str | None = None
    to_address: str | None = None

    # Deposit-specific (tracking on-chain deposit)
    tx_hash: str | None = None
    from_chain: str | None = None

    # Idempotency
    userref: int | None = None  # For swaps

    # Metadata
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize for state persistence."""
        return {
            "id": self.id,
            "type": self.type.value,
            "exchange": self.exchange,
            "asset_in": self.asset_in,
            "asset_out": self.asset_out,
            "amount_in": self.amount_in,
            "decimals_in": self.decimals_in,
            "decimals_out": self.decimals_out,
            "asset": self.asset,
            "amount": self.amount,
            "decimals": self.decimals,
            "chain": self.chain,
            "to_address": self.to_address,
            "tx_hash": self.tx_hash,
            "from_chain": self.from_chain,
            "userref": self.userref,
            "metadata": self.metadata,
        }


@dataclass
class ActionBundle:
    """Bundle of actions to execute.

    For CEX operations, this typically contains a single action,
    but supports multiple for future batch operations.
    """

    actions: list[CEXAction]
    venue_type: VenueType
    exchange: str = "kraken"

    # Execution behavior
    continue_on_failure: bool = False

    # Metadata
    description: str = ""
    estimated_gas: int = 0  # Not applicable for CEX

    def to_dict(self) -> dict:
        """Serialize for state persistence."""
        return {
            "actions": [a.to_dict() for a in self.actions],
            "venue_type": self.venue_type.value,
            "exchange": self.exchange,
            "continue_on_failure": self.continue_on_failure,
            "description": self.description,
        }


# =============================================================================
# Execution Context
# =============================================================================


@dataclass
class ExecutionContext:
    """Context for CEX execution.

    Contains runtime information needed during execution.
    """

    chain: str = "ethereum"
    wallet_address: str = ""
    strategy_id: str = ""

    # Token decimals lookup
    token_decimals: dict[str, int] = field(default_factory=dict)

    def get_decimals(self, token: str, default: int = 18) -> int:
        """Get decimals for a token."""
        return self.token_decimals.get(token.upper(), default)


# =============================================================================
# Kraken Adapter
# =============================================================================


class KrakenAdapter:
    """Adapter for compiling intents into CEX ActionBundles.

    This adapter handles the translation from abstract intents
    (SwapIntent, WithdrawIntent, DepositIntent) into concrete
    CEX actions with validation.

    Example:
        adapter = KrakenAdapter(config, sdk)

        # Compile swap intent
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("1000"),
            venue="kraken",
        )
        bundle = adapter.compile_intent(intent, context)

        # Execute (handled by orchestrator)
        result = await adapter.execute_action(bundle.actions[0], context)
    """

    def __init__(
        self,
        config: KrakenConfig | None = None,
        sdk: KrakenSDK | None = None,
        token_resolver: KrakenTokenResolver | None = None,
        chain_mapper: KrakenChainMapper | None = None,
    ) -> None:
        """Initialize adapter.

        Args:
            config: Kraken configuration
            sdk: KrakenSDK instance. If not provided, will be created lazily.
            token_resolver: Custom token resolver
            chain_mapper: Custom chain mapper
        """
        self.config = config or KrakenConfig()
        self._sdk = sdk
        self.token_resolver = token_resolver or KrakenTokenResolver()
        self.chain_mapper = chain_mapper or KrakenChainMapper()
        self._receipt_resolver: KrakenReceiptResolver | None = None

    @property
    def sdk(self) -> KrakenSDK:
        """Get or create SDK instance."""
        if self._sdk is None:
            self._sdk = KrakenSDK(config=self.config)
        return self._sdk

    @property
    def receipt_resolver(self) -> KrakenReceiptResolver:
        """Get or create receipt resolver."""
        if self._receipt_resolver is None:
            self._receipt_resolver = KrakenReceiptResolver(self.sdk, self.config)
        return self._receipt_resolver

    # =========================================================================
    # Intent Compilation
    # =========================================================================

    def compile_intent(
        self,
        intent: Any,  # SwapIntent, WithdrawIntent, or DepositIntent
        context: ExecutionContext,
    ) -> ActionBundle:
        """Compile an intent into an ActionBundle.

        Args:
            intent: The intent to compile
            context: Execution context

        Returns:
            ActionBundle ready for execution

        Raises:
            ValueError: If intent type is not supported
        """
        intent_type = getattr(intent, "intent_type", None)

        if intent_type is None:
            # Try to infer from class name
            class_name = intent.__class__.__name__.lower()
            if "swap" in class_name:
                return self._compile_swap_intent(intent, context)
            elif "withdraw" in class_name:
                return self._compile_withdraw_intent(intent, context)
            elif "deposit" in class_name:
                return self._compile_deposit_intent(intent, context)
            else:
                raise ValueError(f"Unknown intent type: {intent.__class__.__name__}")

        intent_type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if intent_type_value.lower() == "swap":
            return self._compile_swap_intent(intent, context)
        elif intent_type_value.lower() == "withdraw":
            return self._compile_withdraw_intent(intent, context)
        elif intent_type_value.lower() == "deposit":
            return self._compile_deposit_intent(intent, context)
        else:
            raise ValueError(f"Unsupported intent type for CEX: {intent_type_value}")

    def _compile_swap_intent(
        self,
        intent: Any,
        context: ExecutionContext,
    ) -> ActionBundle:
        """Compile a swap intent into CEX ActionBundle."""
        import uuid

        from_token = getattr(intent, "from_token", None)
        to_token = getattr(intent, "to_token", None)
        amount = getattr(intent, "amount", None)
        chain = getattr(intent, "chain", None) or context.chain

        if not from_token or not to_token:
            raise ValueError("SwapIntent requires from_token and to_token")

        # Get decimals
        decimals_in = context.get_decimals(from_token)
        decimals_out = context.get_decimals(to_token)

        # Convert amount to wei
        if isinstance(amount, Decimal):
            amount_wei = int(amount * Decimal(10) ** decimals_in)
        elif isinstance(amount, int | float):
            amount_wei = int(Decimal(str(amount)) * Decimal(10) ** decimals_in)
        else:
            raise ValueError(f"Unsupported amount type: {type(amount)}")

        # Generate userref for idempotency
        userref = KrakenSDK.generate_userref()

        # Validate (checks market exists, minimums, balance)
        try:
            validated_amount = self.sdk.validate_swap_amount(
                asset_in=from_token,
                asset_out=to_token,
                amount_in=amount_wei,
                decimals_in=decimals_in,
                chain=chain,
            )
        except (KrakenMinimumOrderError, KrakenInsufficientFundsError, KrakenUnknownPairError):
            raise

        action = CEXAction(
            id=f"swap_{uuid.uuid4().hex[:8]}",
            type=ActionType.CEX_SWAP,
            exchange="kraken",
            asset_in=from_token,
            asset_out=to_token,
            amount_in=validated_amount,
            decimals_in=decimals_in,
            decimals_out=decimals_out,
            userref=userref,
            metadata={
                "chain": chain,
                "original_amount": amount_wei,
            },
        )

        return ActionBundle(
            actions=[action],
            venue_type=VenueType.CEX,
            exchange="kraken",
            description=f"Swap {from_token} -> {to_token} on Kraken",
        )

    def _compile_withdraw_intent(
        self,
        intent: Any,
        context: ExecutionContext,
    ) -> ActionBundle:
        """Compile a withdraw intent into CEX ActionBundle."""
        import uuid

        token = getattr(intent, "token", None)
        amount = getattr(intent, "amount", None)
        to_address = getattr(intent, "to_address", None)
        chain = getattr(intent, "chain", None)

        if not token:
            raise ValueError("WithdrawIntent requires token")
        if not to_address:
            raise ValueError("WithdrawIntent requires to_address")
        if not chain:
            raise ValueError("WithdrawIntent requires chain")

        # Validate chain is supported
        if chain.lower() not in self.chain_mapper.get_supported_chains():
            raise KrakenChainNotSupportedError(chain, "withdrawal")

        # Get decimals
        decimals = context.get_decimals(token)

        # Convert amount to wei
        if amount == "all":
            # Get balance and use it
            balance = self.sdk.get_balance(token, chain)
            amount_wei = int(balance.available * Decimal(10) ** decimals)
        elif isinstance(amount, Decimal):
            amount_wei = int(amount * Decimal(10) ** decimals)
        elif isinstance(amount, int | float):
            amount_wei = int(Decimal(str(amount)) * Decimal(10) ** decimals)
        else:
            raise ValueError(f"Unsupported amount type: {type(amount)}")

        action = CEXAction(
            id=f"withdraw_{uuid.uuid4().hex[:8]}",
            type=ActionType.CEX_WITHDRAW,
            exchange="kraken",
            asset=token,
            amount=amount_wei,
            decimals=decimals,
            chain=chain,
            to_address=to_address,
        )

        return ActionBundle(
            actions=[action],
            venue_type=VenueType.CEX,
            exchange="kraken",
            description=f"Withdraw {token} to {chain} on Kraken",
        )

    def _compile_deposit_intent(
        self,
        intent: Any,
        context: ExecutionContext,
    ) -> ActionBundle:
        """Compile a deposit tracking intent into CEX ActionBundle.

        Note: This doesn't initiate a deposit (that's on-chain),
        it tracks an existing deposit transaction.
        """
        import uuid

        token = getattr(intent, "token", None)
        amount = getattr(intent, "amount", None)
        tx_hash = getattr(intent, "tx_hash", None)
        chain = getattr(intent, "chain", None)

        if not token:
            raise ValueError("DepositIntent requires token")
        if not tx_hash:
            raise ValueError("DepositIntent requires tx_hash")
        if not chain:
            raise ValueError("DepositIntent requires chain")

        decimals = context.get_decimals(token)

        # Convert amount to wei
        if isinstance(amount, Decimal):
            amount_wei = int(amount * Decimal(10) ** decimals)
        elif isinstance(amount, int | float):
            amount_wei = int(Decimal(str(amount)) * Decimal(10) ** decimals)
        else:
            amount_wei = 0  # Will be determined from receipt

        action = CEXAction(
            id=f"deposit_{uuid.uuid4().hex[:8]}",
            type=ActionType.CEX_DEPOSIT,
            exchange="kraken",
            asset=token,
            amount=amount_wei,
            decimals=decimals,
            tx_hash=tx_hash,
            from_chain=chain,
        )

        return ActionBundle(
            actions=[action],
            venue_type=VenueType.CEX,
            exchange="kraken",
            description=f"Track {token} deposit from {chain} on Kraken",
        )

    # =========================================================================
    # Action Execution
    # =========================================================================

    async def execute_action(
        self,
        action: CEXAction,
        context: ExecutionContext,
    ) -> tuple[CEXIdempotencyKey, str]:
        """Execute a single CEX action.

        Args:
            action: The action to execute
            context: Execution context

        Returns:
            Tuple of (idempotency_key, result_id)
            - For swaps: result_id is txid
            - For withdrawals: result_id is refid
            - For deposits: result_id is tx_hash
        """
        if action.type == ActionType.CEX_SWAP:
            return await self._execute_swap(action, context)
        elif action.type == ActionType.CEX_WITHDRAW:
            return await self._execute_withdraw(action, context)
        elif action.type == ActionType.CEX_DEPOSIT:
            return await self._track_deposit(action, context)
        else:
            raise ValueError(f"Unknown action type: {action.type}")

    async def _execute_swap(
        self,
        action: CEXAction,
        context: ExecutionContext,
    ) -> tuple[CEXIdempotencyKey, str]:
        """Execute a swap action."""
        chain = action.metadata.get("chain", context.chain)

        # Create idempotency key BEFORE execution
        key = CEXIdempotencyKey(
            action_id=action.id,
            exchange="kraken",
            operation_type=CEXOperationType.SWAP,
            userref=action.userref,
        )

        logger.info(
            "Executing CEX swap",
            action_id=action.id,
            asset_in=action.asset_in,
            asset_out=action.asset_out,
            amount=action.amount_in,
            userref=action.userref,
        )

        # Validate required fields
        if action.asset_in is None or action.asset_out is None:
            raise ValueError("Swap action requires asset_in and asset_out")
        if action.amount_in is None or action.decimals_in is None:
            raise ValueError("Swap action requires amount_in and decimals_in")
        if action.userref is None:
            raise ValueError("Swap action requires userref")

        # Execute swap
        txid = self.sdk.swap(
            asset_in=action.asset_in,
            asset_out=action.asset_out,
            amount_in=action.amount_in,
            decimals_in=action.decimals_in,
            userref=action.userref,
            chain=chain,
        )

        # Update key with txid
        key.order_id = txid

        return key, txid

    async def _execute_withdraw(
        self,
        action: CEXAction,
        context: ExecutionContext,
    ) -> tuple[CEXIdempotencyKey, str]:
        """Execute a withdrawal action."""
        # Create idempotency key BEFORE execution
        key = CEXIdempotencyKey(
            action_id=action.id,
            exchange="kraken",
            operation_type=CEXOperationType.WITHDRAW,
        )

        logger.info(
            "Executing CEX withdrawal",
            action_id=action.id,
            asset=action.asset,
            chain=action.chain,
            amount=action.amount,
            to_address=action.to_address,
        )

        # Validate required fields
        if action.asset is None or action.chain is None:
            raise ValueError("Withdraw action requires asset and chain")
        if action.amount is None or action.decimals is None:
            raise ValueError("Withdraw action requires amount and decimals")
        if action.to_address is None:
            raise ValueError("Withdraw action requires to_address")

        # Execute withdrawal
        refid = self.sdk.withdraw(
            asset=action.asset,
            chain=action.chain,
            amount=action.amount,
            decimals=action.decimals,
            to_address=action.to_address,
        )

        # Update key with refid
        key.refid = refid

        return key, refid

    async def _track_deposit(
        self,
        action: CEXAction,
        context: ExecutionContext,
    ) -> tuple[CEXIdempotencyKey, str]:
        """Track a deposit (doesn't execute, just sets up tracking)."""
        key = CEXIdempotencyKey(
            action_id=action.id,
            exchange="kraken",
            operation_type=CEXOperationType.DEPOSIT,
            order_id=action.tx_hash,  # Use order_id for tx_hash
        )

        if action.tx_hash is None:
            raise ValueError("Deposit action requires tx_hash")

        logger.info(
            "Tracking CEX deposit",
            action_id=action.id,
            asset=action.asset,
            chain=action.from_chain,
            tx_hash=action.tx_hash,
        )

        return key, action.tx_hash

    # =========================================================================
    # Result Resolution
    # =========================================================================

    async def resolve_action(
        self,
        action: CEXAction,
        key: CEXIdempotencyKey,
        context: ExecutionContext,
    ) -> ExecutionDetails:
        """Wait for action completion and return result.

        Args:
            action: The action that was executed
            key: Idempotency key from execution
            context: Execution context

        Returns:
            ExecutionDetails with operation result
        """
        chain = action.metadata.get("chain", context.chain)

        if action.type == ActionType.CEX_SWAP:
            if key.order_id is None or key.userref is None:
                raise ValueError("Swap resolution requires order_id and userref on idempotency key")
            if action.asset_in is None or action.asset_out is None:
                raise ValueError("Swap resolution requires asset_in and asset_out")
            if action.decimals_in is None or action.decimals_out is None:
                raise ValueError("Swap resolution requires decimals_in and decimals_out")
            return await self.receipt_resolver.resolve_swap(
                txid=key.order_id,
                userref=key.userref,
                asset_in=action.asset_in,
                asset_out=action.asset_out,
                decimals_in=action.decimals_in,
                decimals_out=action.decimals_out,
                chain=chain,
                idempotency_key=key,
            )
        elif action.type == ActionType.CEX_WITHDRAW:
            if key.refid is None:
                raise ValueError("Withdrawal resolution requires refid on idempotency key")
            if action.asset is None or action.chain is None:
                raise ValueError("Withdrawal resolution requires asset and chain")
            if action.decimals is None or action.to_address is None or action.amount is None:
                raise ValueError("Withdrawal resolution requires decimals, to_address, and amount")
            return await self.receipt_resolver.resolve_withdrawal(
                refid=key.refid,
                asset=action.asset,
                chain=action.chain,
                decimals=action.decimals,
                to_address=action.to_address,
                amount=action.amount,
                idempotency_key=key,
            )
        elif action.type == ActionType.CEX_DEPOSIT:
            if action.tx_hash is None or action.asset is None or action.from_chain is None:
                raise ValueError("Deposit resolution requires tx_hash, asset, and from_chain")
            if action.decimals is None or action.amount is None:
                raise ValueError("Deposit resolution requires decimals and amount")
            return await self.receipt_resolver.resolve_deposit(
                tx_hash=action.tx_hash,
                asset=action.asset,
                chain=action.from_chain,
                decimals=action.decimals,
                amount=action.amount,
                idempotency_key=key,
            )
        else:
            raise ValueError(f"Unknown action type: {action.type}")


__all__ = [
    # Enums
    "VenueType",
    "ActionType",
    # Models
    "CEXAction",
    "ActionBundle",
    "ExecutionContext",
    # Adapter
    "KrakenAdapter",
]
