"""Extracted Data Models for Result Enrichment.

This module defines the typed data classes used to represent extracted
data from transaction receipts. These models are populated by the
ResultEnricher component and attached to ExecutionResult.

The design follows "UX First, Safety Always" - providing strongly typed
data that strategy authors can access directly without manual parsing.

Example:
    result = await orchestrator.execute(intent)
    if result.swap_amounts:
        print(f"Swapped: {result.swap_amounts.amount_in_decimal}")
    if result.position_id:
        print(f"Position: {result.position_id}")
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class SwapAmounts:
    """Extracted swap execution data.

    Represents the token amounts exchanged in a swap transaction.
    All fields are immutable (frozen=True) for safety.

    Attributes:
        amount_in: Raw input amount (in token's smallest unit)
        amount_out: Raw output amount (in token's smallest unit)
        amount_in_decimal: Human-readable input amount
        amount_out_decimal: Human-readable output amount
        effective_price: Actual execution price (out/in)
        slippage_bps: Actual slippage in basis points (None if unknown)
        token_in: Input token address or symbol
        token_out: Output token address or symbol

    Example:
        if result.swap_amounts:
            price = result.swap_amounts.effective_price
            slippage = result.swap_amounts.slippage_bps
    """

    amount_in: int
    amount_out: int
    amount_in_decimal: Decimal
    amount_out_decimal: Decimal
    effective_price: Decimal | None = None
    slippage_bps: int | None = None
    token_in: str | None = None
    token_out: str | None = None

    # Aliases: amount_in_human / amount_out_human (VIB-295)
    # Strategy authors naturally reach for _human instead of _decimal.
    def __getattr__(self, name: str) -> Any:
        if name == "amount_in_human":
            return self.amount_in_decimal
        if name == "amount_out_human":
            return self.amount_out_decimal
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "amount_in_decimal": str(self.amount_in_decimal),
            "amount_out_decimal": str(self.amount_out_decimal),
            "effective_price": str(self.effective_price) if self.effective_price is not None else None,
            "slippage_bps": self.slippage_bps,
            "token_in": self.token_in,
            "token_out": self.token_out,
        }


@dataclass(frozen=True)
class LPCloseData:
    """Extracted LP close execution data.

    Represents the amounts collected when closing an LP position,
    including principal and fees.

    Attributes:
        amount0_collected: Total amount of token0 collected (principal + fees)
        amount1_collected: Total amount of token1 collected (principal + fees)
        fees0: Fees earned in token0 (if separately tracked)
        fees1: Fees earned in token1 (if separately tracked)
        liquidity_removed: Amount of liquidity removed (if available)
        additional_amounts: Amounts for coins beyond token0/token1 (e.g., Curve 3/4-coin pools).
            Maps coin index to raw amount: {2: 50000000, 3: 91000000000000000000}.
        additional_fees: Fees for coins beyond token0/token1.
            Maps coin index to fee amount: {2: 100000, 3: 0}.

    Example:
        if result.lp_close_data:
            total_0 = result.lp_close_data.amount0_collected
            fees_0 = result.lp_close_data.fees0
            # For 4-coin pools (e.g., Curve NG):
            all_amounts = result.lp_close_data.all_amounts  # [amt0, amt1, amt2, amt3]
    """

    amount0_collected: int
    amount1_collected: int
    fees0: int = 0
    fees1: int = 0
    liquidity_removed: int | None = None
    additional_amounts: dict[int, int] | None = None
    additional_fees: dict[int, int] | None = None

    @property
    def all_amounts(self) -> list[int]:
        """Return all coin amounts as a list, including additional coins."""
        result = [self.amount0_collected, self.amount1_collected]
        if self.additional_amounts:
            for i in sorted(self.additional_amounts):
                result.append(self.additional_amounts[i])
        return result

    @property
    def all_fees(self) -> list[int]:
        """Return all fee amounts as a list, including additional coins."""
        result = [self.fees0, self.fees1]
        if self.additional_fees:
            for i in sorted(self.additional_fees):
                result.append(self.additional_fees[i])
        return result

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: dict[str, Any] = {
            "amount0_collected": str(self.amount0_collected),
            "amount1_collected": str(self.amount1_collected),
            "fees0": str(self.fees0),
            "fees1": str(self.fees1),
            "liquidity_removed": str(self.liquidity_removed) if self.liquidity_removed else None,
        }
        if self.additional_amounts:
            d["additional_amounts"] = {str(k): str(v) for k, v in self.additional_amounts.items()}
        if self.additional_fees:
            d["additional_fees"] = {str(k): str(v) for k, v in self.additional_fees.items()}
        return d


@dataclass(frozen=True)
class LPOpenData:
    """Extracted LP open execution data.

    Represents the data extracted when opening a new LP position,
    including the position ID and range parameters.

    Attributes:
        position_id: NFT position ID (tokenId)
        tick_lower: Lower tick boundary of the position
        tick_upper: Upper tick boundary of the position
        liquidity: Amount of liquidity minted
        amount0: Actual amount of token0 deposited
        amount1: Actual amount of token1 deposited

    Example:
        if result.position_id:  # Core field
            # Access additional data via extracted_data
            lp_data = result.get_extracted("lp_open_data", LPOpenData)
            if lp_data:
                print(f"Range: {lp_data.tick_lower} - {lp_data.tick_upper}")
    """

    position_id: int
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: int | None = None
    amount0: int | None = None
    amount1: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": self.position_id,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "liquidity": str(self.liquidity) if self.liquidity else None,
            "amount0": str(self.amount0) if self.amount0 else None,
            "amount1": str(self.amount1) if self.amount1 else None,
        }


@dataclass(frozen=True)
class BorrowData:
    """Extracted borrow execution data.

    Represents the data from a borrow transaction on lending protocols.

    Attributes:
        borrow_amount: Amount borrowed (raw units)
        borrow_rate: Interest rate at time of borrow (if available)
        debt_token: Address or symbol of the debt token
        health_factor: Health factor after borrow (if available)
    """

    borrow_amount: int
    borrow_rate: Decimal | None = None
    debt_token: str | None = None
    health_factor: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "borrow_amount": str(self.borrow_amount),
            "borrow_rate": str(self.borrow_rate) if self.borrow_rate else None,
            "debt_token": self.debt_token,
            "health_factor": str(self.health_factor) if self.health_factor else None,
        }


@dataclass(frozen=True)
class SupplyData:
    """Extracted supply execution data.

    Represents the data from a supply transaction on lending protocols.

    Attributes:
        supply_amount: Amount supplied (raw units)
        a_token_received: Amount of aToken/receipt token received
        supply_rate: Supply APY at time of supply (if available)
    """

    supply_amount: int
    a_token_received: int | None = None
    supply_rate: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "supply_amount": str(self.supply_amount),
            "a_token_received": str(self.a_token_received) if self.a_token_received else None,
            "supply_rate": str(self.supply_rate) if self.supply_rate else None,
        }


@dataclass(frozen=True)
class PerpData:
    """Extracted perpetual position data.

    Represents the data from perpetual position operations.

    Attributes:
        position_id: Position identifier
        size_delta: Change in position size
        collateral: Collateral amount
        entry_price: Entry price (for opens)
        exit_price: Exit price (for closes)
        leverage: Position leverage
        realized_pnl: Realized PnL (for closes)
        fees_paid: Total fees paid
    """

    position_id: str | int | None = None
    size_delta: int | None = None
    collateral: int | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    leverage: Decimal | None = None
    realized_pnl: Decimal | None = None
    fees_paid: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": str(self.position_id) if self.position_id else None,
            "size_delta": str(self.size_delta) if self.size_delta else None,
            "collateral": str(self.collateral) if self.collateral else None,
            "entry_price": str(self.entry_price) if self.entry_price else None,
            "exit_price": str(self.exit_price) if self.exit_price else None,
            "leverage": str(self.leverage) if self.leverage else None,
            "realized_pnl": str(self.realized_pnl) if self.realized_pnl else None,
            "fees_paid": str(self.fees_paid) if self.fees_paid else None,
        }


@dataclass(frozen=True)
class StakeData:
    """Extracted staking execution data.

    Represents the data from staking/unstaking transactions.

    Attributes:
        stake_amount: Amount staked/unstaked
        shares_received: Shares/receipt tokens received (for stake)
        underlying_received: Underlying tokens received (for unstake)
        stake_token: Address or symbol of the stake token
    """

    stake_amount: int
    shares_received: int | None = None
    underlying_received: int | None = None
    stake_token: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stake_amount": str(self.stake_amount),
            "shares_received": str(self.shares_received) if self.shares_received else None,
            "underlying_received": str(self.underlying_received) if self.underlying_received else None,
            "stake_token": self.stake_token,
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "SwapAmounts",
    "LPCloseData",
    "LPOpenData",
    "BorrowData",
    "SupplyData",
    "PerpData",
    "StakeData",
]
