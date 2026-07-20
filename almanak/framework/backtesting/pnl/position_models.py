"""Position and fill data models for PnL backtesting.

Provides the core data models used during backtesting to represent
positions and trade fills:

- PositionType: Enum of position types (SPOT, LP, PERP_LONG, etc.)
- SimulatedPosition: A simulated position with protocol-specific fields
- SimulatedFill: Details of a simulated trade execution

These are self-contained data classes with no dependencies on the
SimulatedPortfolio or backtesting engine.

Extracted from pnl/portfolio.py for module size management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import (
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.pnl.data_provider import TokenRef, normalize_token_ref, token_ref_display


class PositionType(StrEnum):
    """Types of positions that can be held in a portfolio.

    Attributes:
        SPOT: Direct token holdings
        LP: Liquidity provider position (Uniswap V3 style)
        PERP_LONG: Perpetual futures long position
        PERP_SHORT: Perpetual futures short position
        SUPPLY: Lending protocol supply (aToken style)
        BORROW: Lending protocol borrow (debt)
    """

    SPOT = "SPOT"
    LP = "LP"
    PERP_LONG = "PERP_LONG"
    PERP_SHORT = "PERP_SHORT"
    SUPPLY = "SUPPLY"
    BORROW = "BORROW"


@dataclass
class SimulatedPosition:
    """A simulated position in the backtesting portfolio.

    This model represents a single position with all relevant fields
    for different position types (spot, LP, perps, lending).

    Core fields (all positions):
        position_type: Type of this position (SPOT, LP, etc.)
        protocol: Protocol name (uniswap_v3, gmx, aave_v3, etc.)
        tokens: List of token identities involved (e.g., ["ETH", "USDC"] or
            [("base", "0x...")])
        amounts: Dict of token identity -> amount held
        entry_price: Price at position entry (for base token in pair)
        entry_time: Timestamp when position was opened

    LP-specific fields (Uniswap V3 style):
        tick_lower: Lower tick boundary for concentrated liquidity
        tick_upper: Upper tick boundary for concentrated liquidity
        liquidity: LP liquidity in TRUE Uniswap V3 L-units (sqrt(k)).
            Never a USD notional — every valuation path feeds this into the
            V3 token-amount math, so producers must convert deposits via
            ImpermanentLossCalculator.liquidity_for_target_value (VIB-5096).
        fee_tier: Pool fee tier (0.01, 0.05, 0.3, 1.0 for Uniswap V3)
        fees_earned: Accumulated trading fees earned in USD
        accumulated_fees_usd: Total accumulated fees in USD (for detailed tracking)
        fees_token0: Accumulated fees in token0 units
        fees_token1: Accumulated fees in token1 units

    Perp-specific fields (GMX, Hyperliquid style):
        leverage: Leverage multiplier (e.g., 5 for 5x leverage)
        entry_funding_index: Funding rate index at position entry
        accumulated_funding: Accumulated funding payments (positive = received)
        collateral_usd: Collateral amount in USD
        notional_usd: Notional position size in USD
        cumulative_funding_paid: Total funding payments made by the position
        cumulative_funding_received: Total funding payments received by the position
        liquidation_price: Price at which position would be liquidated (for perps)

    Lending-specific fields (Aave style):
        apy_at_entry: APY at time of entry (for interest projection)
        interest_accrued: Accumulated interest (earned for SUPPLY, owed for BORROW)
        health_factor: Current health factor (for borrow positions)
    """

    # Core fields - required for all positions
    position_type: PositionType
    protocol: str
    tokens: list[TokenRef]
    amounts: dict[TokenRef, Decimal]
    entry_price: Decimal
    entry_time: datetime

    # Position ID for tracking
    position_id: str = ""

    # LP-specific fields
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: Decimal = Decimal("0")
    fee_tier: Decimal = Decimal("0")
    fees_earned: Decimal = Decimal("0")
    accumulated_fees_usd: Decimal = Decimal("0")
    fees_token0: Decimal = Decimal("0")
    fees_token1: Decimal = Decimal("0")
    fee_confidence: str | None = None
    """Confidence level of fee calculations ('high', 'medium', 'low').

    - high: Fees calculated using actual historical volume data from subgraph.
    - medium: Fees calculated using interpolated or estimated data.
    - low: Fees calculated using multiplier heuristic.
    Set during fee accrual based on the data source quality.
    """
    slippage_confidence: str | None = None
    """Confidence level of slippage calculations ('high', 'medium', 'low').

    - high: Slippage calculated using historical liquidity depth from subgraph.
    - medium: Slippage calculated using TWAP or estimated liquidity.
    - low: Slippage calculated using constant product fallback.
    Set during slippage calculation based on the data source quality.
    """

    # Perp-specific fields
    leverage: Decimal = Decimal("1")
    entry_funding_index: Decimal = Decimal("0")
    accumulated_funding: Decimal = Decimal("0")
    collateral_usd: Decimal = Decimal("0")
    notional_usd: Decimal = Decimal("0")
    cumulative_funding_paid: Decimal = Decimal("0")
    cumulative_funding_received: Decimal = Decimal("0")
    liquidation_price: Decimal | None = None
    is_liquidated: bool = False
    funding_confidence: str | None = None
    """Confidence level of funding rate calculations ('high', 'medium', 'low').

    - high: Funding calculated using actual historical rates from protocol API.
    - medium: Funding calculated using current/approximated rates (e.g., GMX current rate).
    - low: Funding calculated using fallback default rate.
    Set during funding payment application based on the data source quality.
    """
    funding_data_source: str | None = None
    """Description of the data source used for funding rate calculation.

    Examples: "hyperliquid_api", "gmx_api", "fallback:default_rate".
    """

    # Lending-specific fields
    apy_at_entry: Decimal = Decimal("0")
    interest_accrued: Decimal = Decimal("0")
    health_factor: Decimal | None = None
    apy_confidence: str | None = None
    """Confidence level of APY calculations ('high', 'medium', 'low').

    - high: APY calculated using actual historical rates from protocol subgraph.
    - medium: APY calculated using interpolated or estimated rates.
    - low: APY calculated using fallback default rate.
    Set during interest accrual based on the data source quality.
    """
    apy_data_source: str | None = None
    """Description of the data source used for APY calculation.

    Examples: "aave_v3_subgraph", "compound_v3_subgraph", "morpho_blue_subgraph",
              "spark_subgraph", "fallback:default_rate".
    """

    # Tracking fields
    last_updated: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Generate position_id if not provided."""
        if not self.position_id:
            token_str = "_".join(token_ref_display(token) for token in self.tokens)
            self.position_id = (
                f"{self.position_type.value}_{self.protocol}_{token_str}_{self.entry_time.timestamp():.0f}"
            )

    @property
    def is_lp(self) -> bool:
        """Check if this is an LP position."""
        return self.position_type == PositionType.LP

    @property
    def is_perp(self) -> bool:
        """Check if this is a perpetual position."""
        return self.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT)

    @property
    def is_lending(self) -> bool:
        """Check if this is a lending/borrowing position."""
        return self.position_type in (PositionType.SUPPLY, PositionType.BORROW)

    @property
    def is_spot(self) -> bool:
        """Check if this is a spot position."""
        return self.position_type == PositionType.SPOT

    @property
    def is_long(self) -> bool:
        """Check if this is a long position (spot, perp long, supply)."""
        return self.position_type in (
            PositionType.SPOT,
            PositionType.PERP_LONG,
            PositionType.SUPPLY,
        )

    @property
    def is_short(self) -> bool:
        """Check if this is a short position (perp short, borrow)."""
        return self.position_type in (PositionType.PERP_SHORT, PositionType.BORROW)

    @property
    def primary_token(self) -> TokenRef:
        """Get the primary token for this position."""
        return self.tokens[0] if self.tokens else ""

    @property
    def total_amount(self) -> Decimal:
        """Get total amount across all tokens (for single-token positions)."""
        return sum(self.amounts.values(), Decimal("0"))

    def get_amount(self, token: TokenRef) -> Decimal:
        """Get amount for a specific token.

        Args:
            token: Token identity to get amount for

        Returns:
            Amount held, or 0 if token not in position
        """
        return self.amounts.get(token, Decimal("0"))

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        result: dict[str, Any] = {
            "position_id": self.position_id,
            "position_type": self.position_type.value,
            "protocol": self.protocol,
            "tokens": [token_ref_display(token) for token in self.tokens],
            "amounts": {token_ref_display(k): str(v) for k, v in self.amounts.items()},
            "entry_price": str(self.entry_price),
            "entry_time": self.entry_time.isoformat(),
        }

        # Add LP-specific fields if relevant
        if self.is_lp:
            result["tick_lower"] = self.tick_lower
            result["tick_upper"] = self.tick_upper
            result["liquidity"] = str(self.liquidity)
            result["fee_tier"] = str(self.fee_tier)
            result["fees_earned"] = str(self.fees_earned)
            result["accumulated_fees_usd"] = str(self.accumulated_fees_usd)
            result["fees_token0"] = str(self.fees_token0)
            result["fees_token1"] = str(self.fees_token1)
            result["fee_confidence"] = self.fee_confidence
            result["slippage_confidence"] = self.slippage_confidence

        # Add perp-specific fields if relevant
        if self.is_perp:
            result["leverage"] = str(self.leverage)
            result["entry_funding_index"] = str(self.entry_funding_index)
            result["accumulated_funding"] = str(self.accumulated_funding)
            result["collateral_usd"] = str(self.collateral_usd)
            result["notional_usd"] = str(self.notional_usd)
            result["cumulative_funding_paid"] = str(self.cumulative_funding_paid)
            result["cumulative_funding_received"] = str(self.cumulative_funding_received)
            result["liquidation_price"] = str(self.liquidation_price) if self.liquidation_price is not None else None
            result["is_liquidated"] = self.is_liquidated
            result["funding_confidence"] = self.funding_confidence
            result["funding_data_source"] = self.funding_data_source

        # Add lending-specific fields if relevant
        if self.is_lending:
            result["apy_at_entry"] = str(self.apy_at_entry)
            result["interest_accrued"] = str(self.interest_accrued)
            result["health_factor"] = str(self.health_factor) if self.health_factor else None
            result["apy_confidence"] = self.apy_confidence
            result["apy_data_source"] = self.apy_data_source

        # Add tracking fields
        result["last_updated"] = self.last_updated.isoformat() if self.last_updated else None
        result["metadata"] = self.metadata

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SimulatedPosition":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized SimulatedPosition data

        Returns:
            SimulatedPosition instance
        """
        return cls(
            position_id=data.get("position_id", ""),
            position_type=PositionType(data["position_type"]),
            protocol=data["protocol"],
            tokens=[normalize_token_ref(token) for token in data["tokens"]],
            amounts={normalize_token_ref(k): Decimal(v) for k, v in data["amounts"].items()},
            entry_price=Decimal(data["entry_price"]),
            entry_time=datetime.fromisoformat(data["entry_time"]),
            # LP fields
            tick_lower=data.get("tick_lower"),
            tick_upper=data.get("tick_upper"),
            liquidity=Decimal(data.get("liquidity", "0")),
            fee_tier=Decimal(data.get("fee_tier", "0")),
            fees_earned=Decimal(data.get("fees_earned", "0")),
            accumulated_fees_usd=Decimal(data.get("accumulated_fees_usd", "0")),
            fees_token0=Decimal(data.get("fees_token0", "0")),
            fees_token1=Decimal(data.get("fees_token1", "0")),
            fee_confidence=data.get("fee_confidence"),
            slippage_confidence=data.get("slippage_confidence"),
            # Perp fields
            leverage=Decimal(data.get("leverage", "1")),
            entry_funding_index=Decimal(data.get("entry_funding_index", "0")),
            accumulated_funding=Decimal(data.get("accumulated_funding", "0")),
            collateral_usd=Decimal(data.get("collateral_usd", "0")),
            notional_usd=Decimal(data.get("notional_usd", "0")),
            cumulative_funding_paid=Decimal(data.get("cumulative_funding_paid", "0")),
            cumulative_funding_received=Decimal(data.get("cumulative_funding_received", "0")),
            liquidation_price=Decimal(data["liquidation_price"]) if data.get("liquidation_price") else None,
            is_liquidated=data.get("is_liquidated", False),
            funding_confidence=data.get("funding_confidence"),
            funding_data_source=data.get("funding_data_source"),
            # Lending fields
            apy_at_entry=Decimal(data.get("apy_at_entry", "0")),
            interest_accrued=Decimal(data.get("interest_accrued", "0")),
            health_factor=Decimal(data["health_factor"]) if data.get("health_factor") else None,
            apy_confidence=data.get("apy_confidence"),
            apy_data_source=data.get("apy_data_source"),
            # Tracking fields
            last_updated=datetime.fromisoformat(data["last_updated"]) if data.get("last_updated") else None,
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def spot(
        cls,
        token: TokenRef,
        amount: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        protocol: str = "spot",
    ) -> "SimulatedPosition":
        """Create a spot position.

        Args:
            token: Token symbol
            amount: Amount held
            entry_price: Price at entry
            entry_time: Time of entry
            protocol: Protocol name (default "spot")

        Returns:
            SimulatedPosition for spot holding
        """
        return cls(
            position_type=PositionType.SPOT,
            protocol=protocol,
            tokens=[token],
            amounts={token: amount},
            entry_price=entry_price,
            entry_time=entry_time,
        )

    @classmethod
    def lp(
        cls,
        token0: TokenRef,
        token1: TokenRef,
        amount0: Decimal,
        amount1: Decimal,
        liquidity: Decimal,
        tick_lower: int,
        tick_upper: int,
        fee_tier: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        protocol: str = "uniswap_v3",
    ) -> "SimulatedPosition":
        """Create an LP position.

        Args:
            token0: First token symbol
            token1: Second token symbol
            amount0: Amount of token0
            amount1: Amount of token1
            liquidity: LP liquidity in true V3 L-units (use
                ImpermanentLossCalculator.liquidity_for_target_value to
                convert a deposit value; never pass a USD notional)
            tick_lower: Lower tick boundary
            tick_upper: Upper tick boundary
            fee_tier: Pool fee tier
            entry_price: Price of token0 in token1 at entry
            entry_time: Time of entry
            protocol: Protocol name (default "uniswap_v3")

        Returns:
            SimulatedPosition for LP position
        """
        return cls(
            position_type=PositionType.LP,
            protocol=protocol,
            tokens=[token0, token1],
            amounts={token0: amount0, token1: amount1},
            entry_price=entry_price,
            entry_time=entry_time,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
            fee_tier=fee_tier,
        )

    @classmethod
    def perp_long(
        cls,
        token: TokenRef,
        collateral_usd: Decimal,
        leverage: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        entry_funding_index: Decimal = Decimal("0"),
        protocol: str = "gmx",
        maintenance_margin: Decimal | None = None,
    ) -> "SimulatedPosition":
        """Create a perpetual long position.

        Args:
            token: Token being longed
            collateral_usd: Collateral amount in USD
            leverage: Leverage multiplier
            entry_price: Entry price
            entry_time: Time of entry
            entry_funding_index: Funding rate index at entry
            protocol: Protocol name (default "gmx")
            maintenance_margin: Maintenance margin ratio (uses protocol default if None)

        Returns:
            SimulatedPosition for perp long
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.liquidation import (
            LiquidationCalculator,
        )

        notional = collateral_usd * leverage

        # Calculate liquidation price
        liq_calc = LiquidationCalculator()
        margin = (
            liq_calc.get_maintenance_margin_for_protocol(protocol) if maintenance_margin is None else maintenance_margin
        )
        liquidation_price = liq_calc.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=leverage,
            maintenance_margin=margin,
            is_long=True,
        )

        return cls(
            position_type=PositionType.PERP_LONG,
            protocol=protocol,
            tokens=[token],
            amounts={token: notional / entry_price},
            entry_price=entry_price,
            entry_time=entry_time,
            leverage=leverage,
            collateral_usd=collateral_usd,
            notional_usd=notional,
            entry_funding_index=entry_funding_index,
            liquidation_price=liquidation_price,
        )

    @classmethod
    def perp_short(
        cls,
        token: TokenRef,
        collateral_usd: Decimal,
        leverage: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        entry_funding_index: Decimal = Decimal("0"),
        protocol: str = "gmx",
        maintenance_margin: Decimal | None = None,
    ) -> "SimulatedPosition":
        """Create a perpetual short position.

        Args:
            token: Token being shorted
            collateral_usd: Collateral amount in USD
            leverage: Leverage multiplier
            entry_price: Entry price
            entry_time: Time of entry
            entry_funding_index: Funding rate index at entry
            protocol: Protocol name (default "gmx")
            maintenance_margin: Maintenance margin ratio (uses protocol default if None)

        Returns:
            SimulatedPosition for perp short
        """
        # Lazy import to avoid circular dependency
        from almanak.framework.backtesting.pnl.calculators.liquidation import (
            LiquidationCalculator,
        )

        notional = collateral_usd * leverage

        # Calculate liquidation price
        liq_calc = LiquidationCalculator()
        margin = (
            liq_calc.get_maintenance_margin_for_protocol(protocol) if maintenance_margin is None else maintenance_margin
        )
        liquidation_price = liq_calc.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=leverage,
            maintenance_margin=margin,
            is_long=False,
        )

        return cls(
            position_type=PositionType.PERP_SHORT,
            protocol=protocol,
            tokens=[token],
            amounts={token: notional / entry_price},
            entry_price=entry_price,
            entry_time=entry_time,
            leverage=leverage,
            collateral_usd=collateral_usd,
            notional_usd=notional,
            entry_funding_index=entry_funding_index,
            liquidation_price=liquidation_price,
        )

    @classmethod
    def supply(
        cls,
        token: TokenRef,
        amount: Decimal,
        apy: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        protocol: str = "aave_v3",
    ) -> "SimulatedPosition":
        """Create a lending supply position.

        Args:
            token: Token being supplied
            amount: Amount supplied
            apy: APY at time of supply
            entry_price: Token price at entry
            entry_time: Time of entry
            protocol: Protocol name (default "aave_v3")

        Returns:
            SimulatedPosition for supply position
        """
        return cls(
            position_type=PositionType.SUPPLY,
            protocol=protocol,
            tokens=[token],
            amounts={token: amount},
            entry_price=entry_price,
            entry_time=entry_time,
            apy_at_entry=apy,
        )

    @classmethod
    def borrow(
        cls,
        token: TokenRef,
        amount: Decimal,
        apy: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        health_factor: Decimal | None = None,
        protocol: str = "aave_v3",
    ) -> "SimulatedPosition":
        """Create a lending borrow position.

        Args:
            token: Token being borrowed
            amount: Amount borrowed
            apy: APY at time of borrow
            entry_price: Token price at entry
            entry_time: Time of entry
            health_factor: Initial health factor
            protocol: Protocol name (default "aave_v3")

        Returns:
            SimulatedPosition for borrow position
        """
        return cls(
            position_type=PositionType.BORROW,
            protocol=protocol,
            tokens=[token],
            amounts={token: amount},
            entry_price=entry_price,
            entry_time=entry_time,
            apy_at_entry=apy,
            health_factor=health_factor,
        )


@dataclass
class SimulatedFill:
    """Details of a simulated trade execution.

    This model captures the result of simulating an intent execution,
    including the actual fill price, fees, slippage, and position changes.

    Attributes:
        timestamp: When the fill occurred
        intent_type: Type of intent that was executed
        protocol: Protocol used (uniswap_v3, aave_v3, gmx, etc.)
        tokens: Tokens involved in the trade
        executed_price: Actual execution price (for swaps/perps)
        amount_usd: Notional amount of the trade in USD
        fee_usd: Protocol/exchange fee in USD
        slippage_usd: Slippage cost in USD
        gas_cost_usd: Gas cost in USD
        tokens_in: Dict of token -> amount received
        tokens_out: Dict of token -> amount paid/sent
        success: Whether the fill succeeded
        position_delta: Position to add/update (optional)
        position_close_id: ID of position being closed (optional)
        position_reduce_id: ID of position being partially reduced (optional;
            e.g. a partial lending WITHDRAW shrinks the matched SUPPLY
            position's principal instead of closing it)
        position_reduce_amounts: Token -> amount (token units) to remove from
            the reduced position's ``amounts``. Producers must size these to
            match the fill's inflow so the reduction is value-conserving.
        metadata: Additional fill-specific metadata
        gas_price_gwei: Gas price in gwei used for this trade (for gas cost analysis)
        estimated_mev_cost_usd: Estimated MEV (sandwich attack) cost in USD (None if MEV simulation disabled)
        delayed_at_end: Whether this fill was executed at simulation end from pending intents queue
    """

    timestamp: datetime
    intent_type: IntentType
    protocol: str
    tokens: list[TokenRef]
    executed_price: Decimal
    amount_usd: Decimal
    fee_usd: Decimal
    slippage_usd: Decimal
    gas_cost_usd: Decimal
    tokens_in: dict[TokenRef, Decimal]  # Tokens received
    tokens_out: dict[TokenRef, Decimal]  # Tokens paid/sent
    success: bool = True
    position_delta: SimulatedPosition | None = None
    position_close_id: str | None = None
    position_reduce_id: str | None = None
    #: LP position whose accrued fees an LP_COLLECT_FEES fill harvests.
    #: The position stays open; apply_fill pays the fees to the wallet (via
    #: tokens_in) and resets the position's uncollected-fee counters so a
    #: later close pays only fees accrued since (double-pay guard).
    position_collect_id: str | None = None
    position_reduce_amounts: dict[TokenRef, Decimal] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    gas_price_gwei: Decimal | None = None
    estimated_mev_cost_usd: Decimal | None = None
    delayed_at_end: bool = False

    @property
    def total_cost_usd(self) -> Decimal:
        """Get total execution cost (fee + slippage + gas + MEV)."""
        mev_cost = self.estimated_mev_cost_usd or Decimal("0")
        return self.fee_usd + self.slippage_usd + self.gas_cost_usd + mev_cost

    def to_trade_record(
        self,
        pnl_usd: Decimal | None = Decimal("0"),
        il_loss_usd: Decimal | None = None,
        fees_earned_usd: Decimal | None = None,
        net_lp_pnl_usd: Decimal | None = None,
        delayed_at_end: bool | None = None,
    ) -> TradeRecord:
        """Convert to a TradeRecord for backtest results.

        Args:
            pnl_usd: Realized PnL to record, or ``None`` when the trade
                realized no PnL (an opening / inventory-building trade). The
                metrics layer excludes ``None`` from win/loss stats so an
                unknown PnL is never miscounted as a loss (VIB-5083).
            il_loss_usd: Impermanent loss in USD (for LP positions, negative = loss)
            fees_earned_usd: Trading fees earned in USD (for LP positions)
            net_lp_pnl_usd: Net LP PnL = (Current Value + Fees) - Initial Value
            delayed_at_end: Override for delayed_at_end flag (uses self.delayed_at_end if None)

        Returns:
            TradeRecord instance
        """
        # Use fill's delayed_at_end if not overridden
        actual_delayed_at_end = delayed_at_end if delayed_at_end is not None else self.delayed_at_end
        # VIB-2916: surface the simulated position_id (open), the close-target
        # id, or the partial-reduce target id so on_intent_executed receives
        # the same id the engine actually tracks.
        position_id = (
            self.position_delta.position_id
            if self.position_delta
            else self.position_close_id or self.position_reduce_id or self.position_collect_id
        )
        # For SWAP fills the actual in/out token amounts are known from the
        # flows (tokens_out = paid = amount_in; tokens_in = received =
        # amount_out). Surface them so downstream consumers like
        # _build_swap_amounts have the realized amounts instead of None
        # (VIB-5083, CodeRabbit; repo learning).
        actual_amount_in: Decimal | None = None
        actual_amount_out: Decimal | None = None
        if self.intent_type == IntentType.SWAP:
            # Only populate the singular fields when each side has exactly one
            # positive leg; for a multi-leg SWAP next(iter(...)) would persist
            # an arbitrary amount that misrepresents the trade (VIB-5083,
            # CodeRabbit).
            paid = [a for a in self.tokens_out.values() if a > Decimal("0")]
            received = [a for a in self.tokens_in.values() if a > Decimal("0")]
            if len(paid) == 1:
                actual_amount_in = paid[0]
            if len(received) == 1:
                actual_amount_out = received[0]
        return TradeRecord(
            timestamp=self.timestamp,
            intent_type=self.intent_type,
            executed_price=self.executed_price,
            fee_usd=self.fee_usd,
            slippage_usd=self.slippage_usd,
            gas_cost_usd=self.gas_cost_usd,
            pnl_usd=pnl_usd,
            success=self.success,
            # Surface the portfolio's rejection reason on the record itself —
            # result.json serializes TradeRecord.error as rejection_reason, and
            # adapter-lane rejections would otherwise carry it in metadata only.
            error=None if self.success else (self.metadata or {}).get("failure_reason"),
            amount_usd=self.amount_usd,
            protocol=self.protocol,
            tokens=[token_ref_display(token) for token in self.tokens],
            metadata=self.metadata,
            actual_amount_in=actual_amount_in,
            actual_amount_out=actual_amount_out,
            il_loss_usd=il_loss_usd,
            fees_earned_usd=fees_earned_usd,
            net_lp_pnl_usd=net_lp_pnl_usd,
            gas_price_gwei=self.gas_price_gwei,
            estimated_mev_cost_usd=self.estimated_mev_cost_usd,
            delayed_at_end=actual_delayed_at_end,
            position_id=position_id,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        result: dict[str, Any] = {
            "timestamp": self.timestamp.isoformat(),
            "intent_type": self.intent_type.value,
            "protocol": self.protocol,
            "tokens": [token_ref_display(token) for token in self.tokens],
            "executed_price": str(self.executed_price),
            "amount_usd": str(self.amount_usd),
            "fee_usd": str(self.fee_usd),
            "slippage_usd": str(self.slippage_usd),
            "gas_cost_usd": str(self.gas_cost_usd),
            "tokens_in": {token_ref_display(k): str(v) for k, v in self.tokens_in.items()},
            "tokens_out": {token_ref_display(k): str(v) for k, v in self.tokens_out.items()},
            "success": self.success,
            "position_delta": self.position_delta.to_dict() if self.position_delta else None,
            "position_close_id": self.position_close_id,
            "position_reduce_id": self.position_reduce_id,
            "position_collect_id": self.position_collect_id,
            "position_reduce_amounts": {token_ref_display(k): str(v) for k, v in self.position_reduce_amounts.items()},
            "metadata": self.metadata,
            "gas_price_gwei": str(self.gas_price_gwei) if self.gas_price_gwei is not None else None,
            "estimated_mev_cost_usd": str(self.estimated_mev_cost_usd)
            if self.estimated_mev_cost_usd is not None
            else None,
        }
        return result
