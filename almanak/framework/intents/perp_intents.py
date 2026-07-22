"""Perpetual futures intent classes.

Intent classes for perpetual futures operations: open and close positions.
These intents support protocols like GMX V2, Hyperliquid, Drift, etc.
"""

import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,  # noqa: F401  -- re-exported for backward compatibility
    OptionalSafeDecimal,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)

from .base import BaseIntent
from .intent_errors import InvalidProtocolParameterError
from .vocabulary import (
    IntentType,
)

# A well-formed bytes32 order key: ``0x`` + exactly 64 hex chars (no underscores).
_BYTES32_RE = re.compile(r"0x[0-9a-fA-F]{64}")

# A well-formed EVM address: ``0x`` + exactly 40 hex chars (no underscores).
_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def _capabilities_for(protocol_lower: str) -> dict[str, Any]:
    """Return the capability dict for ``protocol_lower`` via the connector registry.

    Function-local import: see ``lending_intents._capabilities_for`` for the
    full rationale. Same cold-boot circular-import constraint applies here.
    """
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

    return get_protocol_capabilities(protocol_lower)


def default_perp_withdraw_protocol() -> str:
    """Resolve the default PERP_WITHDRAW venue from the compiler registry.

    Self-containment (blueprint 22): the framework must NOT hardcode a connector
    folder name. Exactly one connector registers ``IntentType.PERP_WITHDRAW`` in
    its compiler manifest today (hyperliquid), so the default is that sole
    registered protocol — resolved from the registry, not a bare literal, so
    adding/renaming the venue needs no edit here. Function-local import + falling
    back to the sole registered perp venue keeps the vocabulary importable during
    cold-boot connector registration (same constraint as ``_capabilities_for``).
    """
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

    protocols = CompilerRegistry.protocols_for_intent(IntentType.PERP_WITHDRAW)
    return protocols[0] if protocols else ""


class PerpOpenIntent(BaseIntent):
    """Intent to open a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        collateral_amount: Amount of collateral in token terms, or "all" for previous step output
        size_usd: Position size in USD terms
        is_long: True for long position, False for short
        leverage: Target leverage for the position (protocol-specific limits apply)
        accept_venue_leverage: Explicit opt-in acknowledging that the target venue
            may be UNABLE to set the requested ``leverage`` on-venue, in which case
            the position opens at the account's existing per-asset leverage/margin
            mode (a risk divergence the connector cannot enforce). Defaults to
            ``False`` (fail-closed): a connector that cannot enforce leverage
            REJECTS a leverage-carrying open unless this is ``True``. Relevant only
            to venues without a set-leverage action — notably **Hyperliquid via
            CoreWriter** (VIB-5724); venues that DO set leverage on-venue (e.g.
            GMX V2) ignore this flag. When ``True`` the divergence is not silenced —
            it is recorded and warned (compile-time + post-fill venue-truth record).
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When collateral_amount="all", the perp open will use the entire output from
        the previous step in a sequence. This is useful for chaining operations like:
        swap -> perp_open.

        The leverage parameter is validated against protocol-specific limits:
        - GMX V2: Supports leverage from 1.1x to 100x
        - Hyperliquid: Supports leverage from 1x to 50x
    """

    market: str
    collateral_token: str
    collateral_amount: PydanticChainedAmount
    size_usd: SafeDecimal
    is_long: bool = True
    leverage: SafeDecimal = Field(default=Decimal("1"))
    # Fail-closed opt-in for venues that cannot set leverage on-venue (VIB-5724).
    # Default False: a connector unable to enforce ``leverage`` rejects the open
    # unless the strategy explicitly accepts the account-default venue leverage.
    accept_venue_leverage: bool = False
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_open_intent(self) -> "PerpOpenIntent":
        """Validate perp open parameters."""
        if isinstance(self.collateral_amount, Decimal) and self.collateral_amount <= 0:
            raise ValueError("collateral_amount must be positive")
        elif not isinstance(self.collateral_amount, Decimal) and self.collateral_amount != "all":
            raise ValueError("collateral_amount must be a positive Decimal or 'all'")
        if self.size_usd <= 0:
            raise ValueError("size_usd must be positive")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.leverage < 1:
            raise ValueError("leverage must be >= 1")
        # Validate leverage against protocol capabilities
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = _capabilities_for(protocol_lower)

        # Validate leverage if the protocol supports it
        if capabilities.get("supports_leverage", False):
            min_leverage = capabilities.get("min_leverage", Decimal("1"))
            max_leverage = capabilities.get("max_leverage", Decimal("100"))

            if self.leverage < min_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage must be at least {min_leverage}x for {self.protocol}",
                )
            if self.leverage > max_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage cannot exceed {max_leverage}x for {self.protocol}",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.collateral_amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_OPEN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.collateral_amount == "all":
            data["collateral_amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpOpenIntent":
        """Deserialize a dictionary to a PerpOpenIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpCloseIntent(BaseIntent):
    """Intent to close a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        is_long: Position direction
        size_usd: Amount to close in USD (None = close full position)
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        position_id: Optional venue-specific position identifier. Required for venues
            where (market, is_long, collateral_token) is insufficient to disambiguate
            an open position — in particular **PancakeSwap Perps (ApolloX)** which
            keys positions on a ``bytes32`` ``tradeHash``. Format is venue-specific:
              - ``pancakeswap_perps``: 0x-prefixed 32-byte hex (66 chars)
              - ``gmx_v2`` / ``hyperliquid`` / ``drift``: ignored (market+side suffices)
            Strategies obtain the ``tradeHash`` from the open receipt
            (``MarketPendingTrade`` / ``OpenMarketTrade`` events) via the
            ``ResultEnricher`` and persist it in their state.
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    market: str
    collateral_token: str
    is_long: bool
    size_usd: OptionalSafeDecimal = None
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    position_id: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_close_intent(self) -> "PerpCloseIntent":
        """Validate perp close parameters."""
        if self.size_usd is not None and self.size_usd <= 0:
            raise ValueError("size_usd must be positive if specified")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.position_id is not None:
            pid = self.position_id
            if not isinstance(pid, str) or not pid.startswith("0x"):
                raise ValueError("position_id must be a 0x-prefixed hex string")
            # bytes32 = 32 bytes = 64 hex chars + "0x" prefix = 66 chars total.
            # We accept any positive-length hex past the 0x prefix to keep the
            # field venue-agnostic; protocol-specific compilers do the strict
            # length check (e.g., PCS Perps requires exactly bytes32).
            try:
                int(pid, 16)
            except ValueError as e:
                raise ValueError(f"position_id must be valid hex: {e}") from e
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_CLOSE

    @property
    def close_full_position(self) -> bool:
        """Check if this intent is to close the full position."""
        return self.size_usd is None

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpCloseIntent":
        """Deserialize a dictionary to a PerpCloseIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpCancelIntent(BaseIntent):
    """Intent to cancel a pending (unfilled) perpetual order and recover its collateral.

    A pending GMX V2 order holds its committed collateral in the OrderVault but is
    **not a position** — no keeper executed it, and no ``position_registry`` row
    exists for it (the enumeration-blindness that stranded collateral in VIB-5116).
    Cancelling the order returns the committed ``initialCollateralDeltaAmount`` plus
    the unspent execution fee to the wallet (``cancellationReceiver`` defaults to the
    wallet). This is a pure risk-reducing, close-side action: **no collateral in, no
    side, no size** — it neither opens nor closes a position, so it carries no PnL.

    It is the *recovery* half of VIB-5116 (VIB-5568): teardown's residual discovery
    (``read_pending_orders`` → ``order_keys``) DETECTS the stranded order; this verb
    RECOVERS it, so teardown completeness passes instead of failing loud.

    Attributes:
        order_key: On-chain order key identifying the pending order to cancel. A
            GMX V2 order key is a ``bytes32`` value — a **strict** 0x-prefixed,
            exactly-66-char (0x + 64 hex) string. The strictness is fund-safety:
            the adapter left-pads the key to 32 bytes, so a truncated key would
            zero-pad into a *different* valid key and cancel the wrong order. It is
            obtained from the teardown residual-discovery read or the open receipt.
        protocol: Perpetuals protocol that owns the order (default "gmx_v2").
        chain: Optional target chain for execution (defaults to strategy's primary chain).
        intent_id: Unique identifier for this intent.
        created_at: Timestamp when the intent was created.
    """

    order_key: str
    protocol: str = "gmx_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_cancel_intent(self) -> "PerpCancelIntent":
        """Validate the order key is a well-formed ``bytes32`` (fail-closed).

        A malformed / truncated key is rejected rather than silently zero-padded by
        the adapter into a different valid order key (which would cancel — and refund
        — the wrong order). ``bytes32`` = 32 bytes = exactly 64 hex chars + the ``0x``
        prefix. We match with a strict regex rather than ``int(key, 16)`` because the
        latter accepts digit-separator underscores (``0x1234_5678``), which would then
        embed an invalid char into the calldata and only fail at signing.
        """
        key = self.order_key
        if not isinstance(key, str) or not _BYTES32_RE.fullmatch(key):
            raise ValueError("order_key must be a bytes32 value (0x + exactly 64 hex chars = 66 chars total)")
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_CANCEL_ORDER

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpCancelIntent":
        """Deserialize a dictionary to a PerpCancelIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpWithdrawIntent(BaseIntent):
    """Intent to withdraw funds from a perp venue's off-chain account back to L1.

    This is a **cash movement**, not a trade — it transfers the strategy's free
    (unencumbered) margin balance off the venue's off-chain ledger back to the
    on-chain wallet. It opens and closes NO position and carries NO PnL: the
    withdrawn amount is captured by the portfolio balance snapshot (the on-chain
    wallet credit), and the tx gets a ``transaction_ledger`` row via the normal
    commit pipeline — so it is visible without a phantom position / PnL event.

    On **Hyperliquid** (``protocol="hyperliquid"``, chain ``hyperevm``) the
    withdraw is a CoreWriter ``spotSend`` (action 6) of USDC (spot token index 0,
    weiDecimals **8** — NOT the 1e6 perp ntl scale) to the USDC **system address**
    (``0x2000…0000``). HyperCore detects the spot-send-to-system-address as a
    HyperCore→HyperEVM bridge and credits the SENDER's HyperEVM (L1) wallet with
    the linked ERC-20 (funds appear in ~seconds). This is the programmatic
    HyperCore→L1 withdraw a Safe (which cannot ECDSA-sign an L1 ``withdraw3``)
    uses to move parked HyperCore funds back on-chain (VIB-5615 / VIB-5617).

    HyperCore charges a small (~$1) withdraw fee, deducted from the credited
    amount by the venue. The fee is a measured venue deduction observable in the
    balance delta, not a PnL event — it is NOT synthesised into an accounting
    row (Empty ≠ Zero: an unmeasured fee is never fabricated as zero).

    Attributes:
        amount: Amount to withdraw in human token terms (e.g. ``Decimal("6.99")``
            USDC), or ``"all"`` to chain the previous step's output.
        asset: Token symbol to withdraw (default ``"USDC"`` — the only HyperCore
            bridge-linked token today).
        protocol: Perp venue that holds the funds (default ``"hyperliquid"``).
        chain: Target chain for execution (defaults to the strategy's primary
            chain; ``hyperevm`` for Hyperliquid).
        destination: Optional explicit L1 recipient. Defaults to the deployment
            wallet's own address — for the HyperCore bridge path the credited
            wallet is ALWAYS the sender (the spotSend originator), so a non-sender
            destination is a plain spot transfer, NOT a bridge; when set it must
            be a well-formed EVM address.
        intent_id: Unique identifier for this intent.
        created_at: Timestamp when the intent was created.
    """

    amount: PydanticChainedAmount
    asset: str = "USDC"
    # Default resolved from the compiler registry (blueprint 22 — no hardcoded
    # connector name in framework code); the sole PERP_WITHDRAW venue today.
    protocol: str = Field(default_factory=default_perp_withdraw_protocol)
    chain: str | None = None
    # Fail-closed sender-equality ASSERTION only. The HyperCore bridge credits the
    # spotSend originator, so a non-sender destination is rejected at compile time.
    # It is NEVER threaded into the encoder — build_usdc_withdraw_calldata hardcodes
    # the USDC system bridge address (which credits the sender), so a compromised /
    # mistaken destination can never redirect funds. Do not wire it into calldata.
    destination: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_withdraw_intent(self) -> "PerpWithdrawIntent":
        """Validate withdraw parameters (fail-closed)."""
        if isinstance(self.amount, Decimal):
            if not self.amount.is_finite():
                raise ValueError("amount must be a finite Decimal (not NaN/Infinity)")
            if self.amount <= 0:
                raise ValueError("amount must be positive")
        elif self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        if not self.asset or not self.asset.strip():
            raise ValueError("asset must be a non-empty token symbol")
        if self.destination is not None:
            dest = self.destination
            if not isinstance(dest, str) or not _ADDRESS_RE.fullmatch(dest):
                raise ValueError("destination must be a 0x-prefixed 20-byte EVM address (42 chars)")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_WITHDRAW

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpWithdrawIntent":
        """Deserialize a dictionary to a PerpWithdrawIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)
