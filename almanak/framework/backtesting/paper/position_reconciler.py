"""Position reconciliation for Paper Trader.

This module provides position tracking and reconciliation utilities that go beyond
simple token balance tracking. It maintains a record of open positions (LP, perp,
lending) and can reconcile them against on-chain state to detect discrepancies.

Key Features:
    - Track positions by type (LP, perp, lending) with full metadata
    - Reconcile tracked positions against on-chain query results
    - Log discrepancies between expected and actual state
    - Support for Uniswap V3 LP, GMX V2 perps, and Aave V3 lending positions

Example:
    from almanak.framework.backtesting.paper.position_reconciler import (
        PositionReconciler,
        TrackedPosition,
        PositionType,
    )

    # Create reconciler
    reconciler = PositionReconciler(chain="arbitrum")

    # Track positions from intents
    reconciler.track_lp_open(
        position_id="123",
        token0="WETH",
        token1="USDC",
        liquidity=1000000,
        tick_lower=-100,
        tick_upper=100,
    )

    # Reconcile against on-chain state
    discrepancies = await reconciler.reconcile(web3, wallet_address)
    for d in discrepancies:
        print(f"Discrepancy: {d}")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import ReconciliationEvent
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Data Classes
# =============================================================================


class PositionType(StrEnum):
    """Types of DeFi positions that can be tracked."""

    LP = "lp"  # Liquidity provider position (Uniswap V3, etc.)
    PERP_LONG = "perp_long"  # Perpetual long position (GMX, etc.)
    PERP_SHORT = "perp_short"  # Perpetual short position
    SUPPLY = "supply"  # Lending supply/collateral (Aave, etc.)
    BORROW = "borrow"  # Lending debt


class DiscrepancyType(StrEnum):
    """Types of position discrepancies."""

    MISSING_ON_CHAIN = "missing_on_chain"  # Tracked but not found on-chain
    MISSING_IN_TRACKER = "missing_in_tracker"  # On-chain but not tracked
    LIQUIDITY_MISMATCH = "liquidity_mismatch"  # LP liquidity differs
    SIZE_MISMATCH = "size_mismatch"  # Perp size differs
    AMOUNT_MISMATCH = "amount_mismatch"  # Lending amount differs
    TICK_RANGE_MISMATCH = "tick_range_mismatch"  # LP tick range differs


@dataclass
class TrackedPosition:
    """A position tracked by the Paper Trader.

    Represents a DeFi position (LP, perp, or lending) with all relevant metadata
    for reconciliation against on-chain state.

    Attributes:
        position_id: Unique identifier (token_id for LP, position_key for perp, etc.)
        position_type: Type of position (LP, PERP_LONG, PERP_SHORT, SUPPLY, BORROW)
        protocol: Protocol name (uniswap_v3, gmx_v2, aave_v3, etc.)
        opened_at: When the position was opened
        # LP-specific fields
        token0: First token in LP pair
        token1: Second token in LP pair
        liquidity: LP liquidity amount
        tick_lower: Lower tick bound (for concentrated liquidity)
        tick_upper: Upper tick bound (for concentrated liquidity)
        fee_tier: Pool fee tier (100, 500, 3000, 10000)
        # Perp-specific fields
        market: Perp market address
        collateral_token: Collateral token address
        size_in_usd: Position size in USD
        size_in_tokens: Position size in tokens
        collateral_amount: Collateral amount
        is_long: True for long, False for short
        # Lending-specific fields
        asset: Lending asset symbol
        asset_address: Lending asset address
        atoken_balance: Current aToken balance (supply)
        debt_balance: Current debt balance (borrow)
        # General fields
        metadata: Additional protocol-specific metadata
    """

    position_id: str
    position_type: PositionType
    protocol: str
    opened_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # LP-specific fields
    token0: str | None = None
    token1: str | None = None
    liquidity: int = 0
    tick_lower: int | None = None
    tick_upper: int | None = None
    fee_tier: int | None = None

    # Perp-specific fields
    market: str | None = None
    collateral_token: str | None = None
    size_in_usd: int = 0
    size_in_tokens: int = 0
    collateral_amount: int = 0
    is_long: bool = True

    # Lending-specific fields
    asset: str | None = None
    asset_address: str | None = None
    atoken_balance: int = 0
    debt_balance: int = 0

    # General
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "position_id": self.position_id,
            "position_type": self.position_type.value,
            "protocol": self.protocol,
            "opened_at": self.opened_at.isoformat(),
            "token0": self.token0,
            "token1": self.token1,
            "liquidity": str(self.liquidity),
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "fee_tier": self.fee_tier,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "is_long": self.is_long,
            "asset": self.asset,
            "asset_address": self.asset_address,
            "atoken_balance": str(self.atoken_balance),
            "debt_balance": str(self.debt_balance),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TrackedPosition":
        """Create from dictionary representation."""
        return cls(
            position_id=data["position_id"],
            position_type=PositionType(data["position_type"]),
            protocol=data["protocol"],
            opened_at=datetime.fromisoformat(data["opened_at"]),
            token0=data.get("token0"),
            token1=data.get("token1"),
            liquidity=int(data.get("liquidity", "0")),
            tick_lower=data.get("tick_lower"),
            tick_upper=data.get("tick_upper"),
            fee_tier=data.get("fee_tier"),
            market=data.get("market"),
            collateral_token=data.get("collateral_token"),
            size_in_usd=int(data.get("size_in_usd", "0")),
            size_in_tokens=int(data.get("size_in_tokens", "0")),
            collateral_amount=int(data.get("collateral_amount", "0")),
            is_long=data.get("is_long", True),
            asset=data.get("asset"),
            asset_address=data.get("asset_address"),
            atoken_balance=int(data.get("atoken_balance", "0")),
            debt_balance=int(data.get("debt_balance", "0")),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PositionDiscrepancy:
    """A discrepancy between tracked and on-chain position state.

    Attributes:
        discrepancy_type: Type of discrepancy detected
        position_type: Type of position (LP, perp, lending)
        position_id: Position identifier
        expected: Expected value (from tracker)
        actual: Actual value (from on-chain)
        message: Human-readable description
        timestamp: When the discrepancy was detected
    """

    discrepancy_type: DiscrepancyType
    position_type: PositionType
    position_id: str
    expected: Any
    actual: Any
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "discrepancy_type": self.discrepancy_type.value,
            "position_type": self.position_type.value,
            "position_id": self.position_id,
            "expected": str(self.expected) if self.expected is not None else None,
            "actual": str(self.actual) if self.actual is not None else None,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Position Reconciler
# =============================================================================


@dataclass
class PositionReconciler:
    """Tracks and reconciles positions for Paper Trader.

    This class extends beyond simple token balance tracking to maintain
    full position state (LP, perp, lending) and reconcile against on-chain
    data to detect discrepancies.

    Attributes:
        chain: Target blockchain (e.g., "arbitrum")
        positions: Dictionary of tracked positions by ID
        closed_positions: List of closed position IDs
        reconciliation_history: History of reconciliation results

    Example:
        reconciler = PositionReconciler(chain="arbitrum")

        # Track an LP position from intent execution
        reconciler.track_lp_open(
            position_id="12345",
            token0="0x82aF...",
            token1="0xaf88...",
            liquidity=1000000000000000000,
            tick_lower=-887220,
            tick_upper=887220,
            fee_tier=3000,
        )

        # Later, reconcile against on-chain state
        discrepancies = await reconciler.reconcile(web3, wallet_address)
    """

    chain: str = "arbitrum"
    positions: dict[str, TrackedPosition] = field(default_factory=dict)
    closed_positions: list[str] = field(default_factory=list)
    reconciliation_history: list[dict[str, Any]] = field(default_factory=list)

    # =========================================================================
    # Position Tracking Methods
    # =========================================================================

    def track_lp_open(
        self,
        position_id: str,
        token0: str,
        token1: str,
        liquidity: int,
        tick_lower: int,
        tick_upper: int,
        fee_tier: int = 3000,
        protocol: str = "uniswap_v3",
        metadata: dict[str, Any] | None = None,
    ) -> TrackedPosition:
        """Track a new LP position opened via Paper Trader.

        Args:
            position_id: Unique identifier (NFT token ID for Uniswap V3)
            token0: Token0 address
            token1: Token1 address
            liquidity: Liquidity amount
            tick_lower: Lower tick bound
            tick_upper: Upper tick bound
            fee_tier: Pool fee tier (default 3000 = 0.3%)
            protocol: Protocol name (default uniswap_v3)
            metadata: Additional metadata

        Returns:
            TrackedPosition object
        """
        position = TrackedPosition(
            position_id=position_id,
            position_type=PositionType.LP,
            protocol=protocol,
            token0=token0,
            token1=token1,
            liquidity=liquidity,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=fee_tier,
            metadata=metadata or {},
        )
        self.positions[position_id] = position
        logger.info(
            "Tracked LP position #%s: liquidity=%s, ticks=[%s, %s]",
            position_id,
            liquidity,
            tick_lower,
            tick_upper,
        )
        return position

    def track_lp_close(self, position_id: str) -> TrackedPosition | None:
        """Mark an LP position as closed.

        Args:
            position_id: Position identifier to close

        Returns:
            The closed TrackedPosition, or None if not found
        """
        if position_id in self.positions:
            position = self.positions.pop(position_id)
            self.closed_positions.append(position_id)
            logger.info("Closed LP position #%s", position_id)
            return position
        logger.warning("Attempted to close unknown LP position #%s", position_id)
        return None

    def track_perp_open(
        self,
        position_id: str,
        market: str,
        collateral_token: str,
        size_in_usd: int,
        size_in_tokens: int,
        collateral_amount: int,
        is_long: bool,
        protocol: str = "gmx_v2",
        metadata: dict[str, Any] | None = None,
    ) -> TrackedPosition:
        """Track a new perp position opened via Paper Trader.

        Args:
            position_id: Position key
            market: Market address
            collateral_token: Collateral token address
            size_in_usd: Position size in USD (30 decimals for GMX)
            size_in_tokens: Position size in tokens
            collateral_amount: Collateral amount
            is_long: True for long, False for short
            protocol: Protocol name (default gmx_v2)
            metadata: Additional metadata

        Returns:
            TrackedPosition object
        """
        position_type = PositionType.PERP_LONG if is_long else PositionType.PERP_SHORT
        position = TrackedPosition(
            position_id=position_id,
            position_type=position_type,
            protocol=protocol,
            market=market,
            collateral_token=collateral_token,
            size_in_usd=size_in_usd,
            size_in_tokens=size_in_tokens,
            collateral_amount=collateral_amount,
            is_long=is_long,
            metadata=metadata or {},
        )
        self.positions[position_id] = position
        direction = "LONG" if is_long else "SHORT"
        logger.info(
            "Tracked perp %s position: size=$%s",
            direction,
            size_in_usd / 10**30 if size_in_usd > 0 else 0,
        )
        return position

    def track_perp_close(self, position_id: str) -> TrackedPosition | None:
        """Mark a perp position as closed.

        Args:
            position_id: Position identifier to close

        Returns:
            The closed TrackedPosition, or None if not found
        """
        if position_id in self.positions:
            position = self.positions.pop(position_id)
            self.closed_positions.append(position_id)
            logger.info("Closed perp position %s...", position_id[:16])
            return position
        logger.warning("Attempted to close unknown perp position %s...", position_id[:16])
        return None

    def track_supply(
        self,
        asset: str,
        asset_address: str,
        amount: int,
        protocol: str = "aave_v3",
        metadata: dict[str, Any] | None = None,
    ) -> TrackedPosition:
        """Track a lending supply position.

        Args:
            asset: Asset symbol (e.g., "USDC")
            asset_address: Asset contract address
            amount: Supply amount in token's smallest unit
            protocol: Protocol name (default aave_v3)
            metadata: Additional metadata

        Returns:
            TrackedPosition object
        """
        position_id = f"{protocol}_{asset_address}_supply"
        position = TrackedPosition(
            position_id=position_id,
            position_type=PositionType.SUPPLY,
            protocol=protocol,
            asset=asset,
            asset_address=asset_address,
            atoken_balance=amount,
            metadata=metadata or {},
        )
        self.positions[position_id] = position
        logger.info("Tracked supply position: %s amount=%s", asset, amount)
        return position

    def track_borrow(
        self,
        asset: str,
        asset_address: str,
        amount: int,
        protocol: str = "aave_v3",
        metadata: dict[str, Any] | None = None,
    ) -> TrackedPosition:
        """Track a lending borrow position.

        Args:
            asset: Asset symbol (e.g., "USDC")
            asset_address: Asset contract address
            amount: Borrow amount in token's smallest unit
            protocol: Protocol name (default aave_v3)
            metadata: Additional metadata

        Returns:
            TrackedPosition object
        """
        position_id = f"{protocol}_{asset_address}_borrow"
        position = TrackedPosition(
            position_id=position_id,
            position_type=PositionType.BORROW,
            protocol=protocol,
            asset=asset,
            asset_address=asset_address,
            debt_balance=amount,
            metadata=metadata or {},
        )
        self.positions[position_id] = position
        logger.info("Tracked borrow position: %s amount=%s", asset, amount)
        return position

    def update_supply(self, asset_address: str, new_amount: int, protocol: str = "aave_v3") -> bool:
        """Update an existing supply position amount.

        Args:
            asset_address: Asset contract address
            new_amount: New supply amount
            protocol: Protocol name

        Returns:
            True if position was updated, False if not found
        """
        position_id = f"{protocol}_{asset_address}_supply"
        if position_id in self.positions:
            self.positions[position_id].atoken_balance = new_amount
            return True
        return False

    def update_borrow(self, asset_address: str, new_amount: int, protocol: str = "aave_v3") -> bool:
        """Update an existing borrow position amount.

        Args:
            asset_address: Asset contract address
            new_amount: New borrow amount
            protocol: Protocol name

        Returns:
            True if position was updated, False if not found
        """
        position_id = f"{protocol}_{asset_address}_borrow"
        if position_id in self.positions:
            self.positions[position_id].debt_balance = new_amount
            return True
        return False

    def close_lending_position(
        self, asset_address: str, position_type: PositionType, protocol: str = "aave_v3"
    ) -> TrackedPosition | None:
        """Close a lending position (supply or borrow).

        Args:
            asset_address: Asset contract address
            position_type: SUPPLY or BORROW
            protocol: Protocol name

        Returns:
            The closed TrackedPosition, or None if not found
        """
        suffix = "supply" if position_type == PositionType.SUPPLY else "borrow"
        position_id = f"{protocol}_{asset_address}_{suffix}"
        if position_id in self.positions:
            position = self.positions.pop(position_id)
            self.closed_positions.append(position_id)
            logger.info("Closed lending position: %s", position_id)
            return position
        return None

    # =========================================================================
    # Reconciliation Methods
    # =========================================================================

    async def reconcile(
        self,
        web3: Any,
        wallet_address: str,
        tolerance_percent: Decimal = Decimal("0.01"),
    ) -> list[PositionDiscrepancy]:
        """Reconcile tracked positions against on-chain state.

        Queries on-chain positions for all supported protocols and compares
        them against tracked positions, detecting and logging any discrepancies.

        Args:
            web3: Web3 instance connected to the target chain
            wallet_address: Wallet address to query positions for
            tolerance_percent: Tolerance for value comparisons (default 1%)

        Returns:
            List of PositionDiscrepancy objects for any detected issues
        """
        discrepancies: list[PositionDiscrepancy] = []
        timestamp = datetime.now(UTC)

        logger.info(
            "Starting position reconciliation for %s on %s",
            wallet_address[:10] + "...",
            self.chain,
        )

        # Reconcile LP positions (Uniswap V3)
        lp_discrepancies = await self._reconcile_lp_positions(web3, wallet_address, tolerance_percent)
        discrepancies.extend(lp_discrepancies)

        # Reconcile perp positions (GMX V2)
        perp_discrepancies = await self._reconcile_perp_positions(web3, wallet_address, tolerance_percent)
        discrepancies.extend(perp_discrepancies)

        # Reconcile lending positions (Aave V3)
        lending_discrepancies = await self._reconcile_lending_positions(web3, wallet_address, tolerance_percent)
        discrepancies.extend(lending_discrepancies)

        # Log summary
        if discrepancies:
            logger.warning(
                "Position reconciliation found %d discrepancies",
                len(discrepancies),
            )
            for d in discrepancies:
                logger.warning(
                    "  [%s] %s: %s",
                    d.discrepancy_type.value,
                    d.position_id[:16] + "..." if len(d.position_id) > 16 else d.position_id,
                    d.message,
                )
        else:
            logger.info("Position reconciliation completed: no discrepancies found")

        # Record in history
        self.reconciliation_history.append(
            {
                "timestamp": timestamp.isoformat(),
                "wallet_address": wallet_address,
                "discrepancy_count": len(discrepancies),
                "discrepancies": [d.to_dict() for d in discrepancies],
            }
        )

        return discrepancies

    async def _reconcile_lp_positions(
        self,
        web3: Any,
        wallet_address: str,
        tolerance_percent: Decimal,
    ) -> list[PositionDiscrepancy]:
        """Reconcile LP positions against on-chain Uniswap V3 state."""
        discrepancies: list[PositionDiscrepancy] = []

        # Import position querying
        from almanak.framework.backtesting.paper.position_queries import (
            query_uniswap_v3_positions,
        )

        # Get on-chain LP positions
        try:
            on_chain_positions = await query_uniswap_v3_positions(
                wallet=wallet_address,
                web3=web3,
                chain=self.chain,
            )
        except Exception as e:
            logger.error("Failed to query on-chain LP positions: %s", e)
            return discrepancies

        # Build lookup of on-chain positions by token ID
        on_chain_by_id = {str(pos.token_id): pos for pos in on_chain_positions}

        # Check tracked LP positions
        tracked_lp = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type == PositionType.LP and pos.protocol == "uniswap_v3"
        }

        for position_id, tracked in tracked_lp.items():
            on_chain = on_chain_by_id.get(position_id)

            if on_chain is None:
                # Position tracked but not found on-chain
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                        position_type=PositionType.LP,
                        position_id=position_id,
                        expected=tracked.liquidity,
                        actual=None,
                        message=f"LP position #{position_id} tracked but not found on-chain",
                    )
                )
                continue

            # Compare liquidity
            if not self._values_within_tolerance(tracked.liquidity, on_chain.liquidity, tolerance_percent):
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.LIQUIDITY_MISMATCH,
                        position_type=PositionType.LP,
                        position_id=position_id,
                        expected=tracked.liquidity,
                        actual=on_chain.liquidity,
                        message=(
                            f"LP #{position_id} liquidity mismatch: "
                            f"tracked={tracked.liquidity}, on-chain={on_chain.liquidity}"
                        ),
                    )
                )

            # Compare tick range
            if tracked.tick_lower != on_chain.tick_lower or tracked.tick_upper != on_chain.tick_upper:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.TICK_RANGE_MISMATCH,
                        position_type=PositionType.LP,
                        position_id=position_id,
                        expected=f"[{tracked.tick_lower}, {tracked.tick_upper}]",
                        actual=f"[{on_chain.tick_lower}, {on_chain.tick_upper}]",
                        message=(
                            f"LP #{position_id} tick range mismatch: "
                            f"tracked=[{tracked.tick_lower}, {tracked.tick_upper}], "
                            f"on-chain=[{on_chain.tick_lower}, {on_chain.tick_upper}]"
                        ),
                    )
                )

        # Check for on-chain positions not tracked
        for token_id, on_chain in on_chain_by_id.items():
            if token_id not in tracked_lp and on_chain.is_active:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
                        position_type=PositionType.LP,
                        position_id=token_id,
                        expected=None,
                        actual=on_chain.liquidity,
                        message=(f"LP #{token_id} found on-chain but not tracked (liquidity={on_chain.liquidity})"),
                    )
                )

        return discrepancies

    async def _reconcile_perp_positions(
        self,
        web3: Any,
        wallet_address: str,
        tolerance_percent: Decimal,
    ) -> list[PositionDiscrepancy]:
        """Reconcile perp positions against on-chain GMX V2 state."""
        discrepancies: list[PositionDiscrepancy] = []

        # Only check GMX positions on Arbitrum
        if self.chain != "arbitrum":
            return discrepancies

        # Import position querying
        from almanak.framework.backtesting.paper.position_queries import (
            query_gmx_positions,
        )

        # Get on-chain perp positions
        try:
            on_chain_positions = await query_gmx_positions(
                wallet=wallet_address,
                web3=web3,
                chain=self.chain,
            )
        except Exception as e:
            logger.error("Failed to query on-chain perp positions: %s", e)
            return discrepancies

        # Build lookup of on-chain positions by key
        on_chain_by_key = {pos.position_key: pos for pos in on_chain_positions}

        # Check tracked perp positions
        tracked_perps = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT) and pos.protocol == "gmx_v2"
        }

        for position_id, tracked in tracked_perps.items():
            on_chain = on_chain_by_key.get(position_id)

            if on_chain is None:
                # Position tracked but not found on-chain
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                        position_type=tracked.position_type,
                        position_id=position_id,
                        expected=tracked.size_in_usd,
                        actual=None,
                        message="Perp position tracked but not found on-chain",
                    )
                )
                continue

            # Compare size
            if not self._values_within_tolerance(tracked.size_in_usd, on_chain.size_in_usd, tolerance_percent):
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.SIZE_MISMATCH,
                        position_type=tracked.position_type,
                        position_id=position_id,
                        expected=tracked.size_in_usd,
                        actual=on_chain.size_in_usd,
                        message=(
                            f"Perp size mismatch: "
                            f"tracked=${tracked.size_in_usd / 10**30:.2f}, "
                            f"on-chain=${on_chain.size_in_usd / 10**30:.2f}"
                        ),
                    )
                )

        # Check for on-chain positions not tracked
        for position_key, on_chain in on_chain_by_key.items():
            if position_key not in tracked_perps and on_chain.is_active:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
                        position_type=(PositionType.PERP_LONG if on_chain.is_long else PositionType.PERP_SHORT),
                        position_id=position_key,
                        expected=None,
                        actual=on_chain.size_in_usd,
                        message=(
                            f"Perp position found on-chain but not tracked (size=${on_chain.size_in_usd / 10**30:.2f})"
                        ),
                    )
                )

        return discrepancies

    async def _reconcile_lending_positions(
        self,
        web3: Any,
        wallet_address: str,
        tolerance_percent: Decimal,
    ) -> list[PositionDiscrepancy]:
        """Reconcile lending positions against on-chain Aave V3 state."""
        discrepancies: list[PositionDiscrepancy] = []

        # Import position querying
        from almanak.framework.backtesting.paper.position_queries import (
            query_aave_positions,
        )

        # Get on-chain lending positions
        try:
            on_chain_positions = await query_aave_positions(
                wallet=wallet_address,
                web3=web3,
                chain=self.chain,
            )
        except Exception as e:
            logger.error("Failed to query on-chain lending positions: %s", e)
            return discrepancies

        # Build lookup by asset address for supply and borrow
        on_chain_supply = {pos.asset_address.lower(): pos for pos in on_chain_positions if pos.has_supply}
        on_chain_borrow = {pos.asset_address.lower(): pos for pos in on_chain_positions if pos.has_debt}

        # Check tracked supply positions
        tracked_supply = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type == PositionType.SUPPLY and pos.protocol == "aave_v3"
        }

        for position_id, tracked in tracked_supply.items():
            asset_addr = tracked.asset_address.lower() if tracked.asset_address else ""
            on_chain = on_chain_supply.get(asset_addr)

            if on_chain is None:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                        position_type=PositionType.SUPPLY,
                        position_id=position_id,
                        expected=tracked.atoken_balance,
                        actual=None,
                        message=f"Supply position {tracked.asset} not found on-chain",
                    )
                )
                continue

            if not self._values_within_tolerance(
                tracked.atoken_balance, on_chain.current_atoken_balance, tolerance_percent
            ):
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                        position_type=PositionType.SUPPLY,
                        position_id=position_id,
                        expected=tracked.atoken_balance,
                        actual=on_chain.current_atoken_balance,
                        message=(
                            f"Supply {tracked.asset} amount mismatch: "
                            f"tracked={tracked.atoken_balance}, "
                            f"on-chain={on_chain.current_atoken_balance}"
                        ),
                    )
                )

        # Check tracked borrow positions
        tracked_borrow = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type == PositionType.BORROW and pos.protocol == "aave_v3"
        }

        for position_id, tracked in tracked_borrow.items():
            asset_addr = tracked.asset_address.lower() if tracked.asset_address else ""
            on_chain = on_chain_borrow.get(asset_addr)

            if on_chain is None:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                        position_type=PositionType.BORROW,
                        position_id=position_id,
                        expected=tracked.debt_balance,
                        actual=None,
                        message=f"Borrow position {tracked.asset} not found on-chain",
                    )
                )
                continue

            if not self._values_within_tolerance(tracked.debt_balance, on_chain.total_debt, tolerance_percent):
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                        position_type=PositionType.BORROW,
                        position_id=position_id,
                        expected=tracked.debt_balance,
                        actual=on_chain.total_debt,
                        message=(
                            f"Borrow {tracked.asset} amount mismatch: "
                            f"tracked={tracked.debt_balance}, "
                            f"on-chain={on_chain.total_debt}"
                        ),
                    )
                )

        # Check for on-chain positions not tracked
        for asset_addr, on_chain in on_chain_supply.items():
            position_id = f"aave_v3_{asset_addr}_supply"
            if position_id not in tracked_supply:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
                        position_type=PositionType.SUPPLY,
                        position_id=position_id,
                        expected=None,
                        actual=on_chain.current_atoken_balance,
                        message=(
                            f"Supply {on_chain.asset} found on-chain but not tracked "
                            f"(balance={on_chain.current_atoken_balance})"
                        ),
                    )
                )

        for asset_addr, on_chain in on_chain_borrow.items():
            position_id = f"aave_v3_{asset_addr}_borrow"
            if position_id not in tracked_borrow:
                discrepancies.append(
                    PositionDiscrepancy(
                        discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
                        position_type=PositionType.BORROW,
                        position_id=position_id,
                        expected=None,
                        actual=on_chain.total_debt,
                        message=(
                            f"Borrow {on_chain.asset} found on-chain but not tracked (debt={on_chain.total_debt})"
                        ),
                    )
                )

        return discrepancies

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _values_within_tolerance(
        self,
        expected: int | float,
        actual: int | float,
        tolerance_percent: Decimal,
    ) -> bool:
        """Check if two values are within tolerance of each other.

        Args:
            expected: Expected value
            actual: Actual value
            tolerance_percent: Tolerance as decimal (e.g., 0.01 for 1%)

        Returns:
            True if values are within tolerance
        """
        if expected == 0 and actual == 0:
            return True
        if expected == 0:
            return False
        diff_percent = abs(Decimal(str(expected)) - Decimal(str(actual))) / Decimal(str(expected))
        return diff_percent <= tolerance_percent

    def get_position(self, position_id: str) -> TrackedPosition | None:
        """Get a tracked position by ID.

        Args:
            position_id: Position identifier

        Returns:
            TrackedPosition or None if not found
        """
        return self.positions.get(position_id)

    def get_positions_by_type(self, position_type: PositionType) -> list[TrackedPosition]:
        """Get all tracked positions of a given type.

        Args:
            position_type: Type of positions to retrieve

        Returns:
            List of TrackedPosition objects
        """
        return [pos for pos in self.positions.values() if pos.position_type == position_type]

    def get_all_positions(self) -> list[TrackedPosition]:
        """Get all tracked positions.

        Returns:
            List of all TrackedPosition objects
        """
        return list(self.positions.values())

    def clear(self) -> None:
        """Clear all tracked positions and history."""
        self.positions.clear()
        self.closed_positions.clear()
        self.reconciliation_history.clear()
        logger.info("Cleared all tracked positions")

    def to_dict(self) -> dict[str, Any]:
        """Serialize reconciler state to dictionary."""
        return {
            "chain": self.chain,
            "positions": {pid: pos.to_dict() for pid, pos in self.positions.items()},
            "closed_positions": self.closed_positions,
            "reconciliation_history": self.reconciliation_history,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PositionReconciler":
        """Deserialize reconciler state from dictionary."""
        reconciler = cls(chain=data.get("chain", "arbitrum"))
        reconciler.positions = {
            pid: TrackedPosition.from_dict(pos_data) for pid, pos_data in data.get("positions", {}).items()
        }
        reconciler.closed_positions = data.get("closed_positions", [])
        reconciler.reconciliation_history = data.get("reconciliation_history", [])
        return reconciler


# =============================================================================
# Position Comparison Function
# =============================================================================


def compare_positions(
    tracked: list["SimulatedPosition"],
    actual: list["SimulatedPosition"],
    tolerance_pct: Decimal = Decimal("0.01"),
) -> list["ReconciliationEvent"]:
    """Compare tracked positions against actual positions and detect discrepancies.

    This function compares two lists of SimulatedPosition objects and returns
    a list of ReconciliationEvent objects for any detected discrepancies in
    amounts, prices, or existence.

    Args:
        tracked: List of tracked (expected) SimulatedPosition objects
        actual: List of actual (on-chain/real) SimulatedPosition objects
        tolerance_pct: Tolerance percentage for numeric comparisons (default 1%)
            A discrepancy is only flagged if |expected - actual| / expected > tolerance_pct

    Returns:
        List of ReconciliationEvent objects describing each discrepancy found

    Discrepancy Types Detected:
        - MISSING_ACTUAL: Position exists in tracked but not in actual
        - MISSING_TRACKED: Position exists in actual but not in tracked
        - AMOUNT_MISMATCH: Token amounts differ beyond tolerance
        - PRICE_MISMATCH: Entry prices differ beyond tolerance
        - LIQUIDITY_MISMATCH: LP liquidity differs beyond tolerance (LP positions)
        - NOTIONAL_MISMATCH: Perp notional USD differs beyond tolerance (perp positions)

    Example:
        >>> from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition
        >>> from almanak.framework.backtesting.paper.position_reconciler import compare_positions
        >>> from decimal import Decimal
        >>> from datetime import datetime, UTC
        >>>
        >>> tracked = [SimulatedPosition.spot("ETH", Decimal("1.0"), Decimal("3000"), datetime.now(UTC))]
        >>> actual = [SimulatedPosition.spot("ETH", Decimal("0.95"), Decimal("3000"), datetime.now(UTC))]
        >>>
        >>> events = compare_positions(tracked, actual, tolerance_pct=Decimal("0.01"))
        >>> # Returns ReconciliationEvent for 5% amount discrepancy
    """
    # Import at runtime to avoid circular imports
    from almanak.framework.backtesting.models import ReconciliationEvent
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition  # noqa: F811

    events: list[ReconciliationEvent] = []
    timestamp = datetime.now(UTC)

    # Build lookup by position_id
    tracked_by_id: dict[str, SimulatedPosition] = {pos.position_id: pos for pos in tracked}
    actual_by_id: dict[str, SimulatedPosition] = {pos.position_id: pos for pos in actual}

    # Check for positions in tracked but not in actual (missing actual)
    for position_id, tracked_pos in tracked_by_id.items():
        if position_id not in actual_by_id:
            # Position exists in tracked but not in actual
            expected_value = tracked_pos.total_amount
            events.append(
                ReconciliationEvent(
                    timestamp=timestamp,
                    position_id=position_id,
                    expected=expected_value,
                    actual=Decimal("0"),
                    discrepancy=expected_value,
                    discrepancy_pct=Decimal("1.0"),  # 100% missing
                    field_name="existence",
                    auto_corrected=False,
                )
            )
            continue

        # Position exists in both - compare fields
        actual_pos = actual_by_id[position_id]

        # Compare token amounts
        all_tokens = set(tracked_pos.amounts.keys()) | set(actual_pos.amounts.keys())
        for token in all_tokens:
            tracked_amount = tracked_pos.get_amount(token)
            actual_amount = actual_pos.get_amount(token)

            if tracked_amount == actual_amount:
                continue

            discrepancy = abs(tracked_amount - actual_amount)

            # Calculate percentage (handle zero case)
            if tracked_amount != Decimal("0"):
                discrepancy_pct = discrepancy / tracked_amount
            elif actual_amount != Decimal("0"):
                discrepancy_pct = Decimal("1.0")  # 100% if expected was 0 but actual is not
            else:
                continue  # Both zero, no discrepancy

            # Only flag if exceeds tolerance
            if discrepancy_pct > tolerance_pct:
                events.append(
                    ReconciliationEvent(
                        timestamp=timestamp,
                        position_id=position_id,
                        expected=tracked_amount,
                        actual=actual_amount,
                        discrepancy=discrepancy,
                        discrepancy_pct=discrepancy_pct,
                        field_name=f"amount_{token}",
                        auto_corrected=False,
                    )
                )

        # Compare entry prices
        if tracked_pos.entry_price != actual_pos.entry_price:
            price_discrepancy = abs(tracked_pos.entry_price - actual_pos.entry_price)
            if tracked_pos.entry_price != Decimal("0"):
                price_pct = price_discrepancy / tracked_pos.entry_price
            elif actual_pos.entry_price != Decimal("0"):
                price_pct = Decimal("1.0")
            else:
                price_pct = Decimal("0")

            if price_pct > tolerance_pct:
                events.append(
                    ReconciliationEvent(
                        timestamp=timestamp,
                        position_id=position_id,
                        expected=tracked_pos.entry_price,
                        actual=actual_pos.entry_price,
                        discrepancy=price_discrepancy,
                        discrepancy_pct=price_pct,
                        field_name="entry_price",
                        auto_corrected=False,
                    )
                )

        # Compare LP-specific fields (liquidity)
        if tracked_pos.is_lp and actual_pos.is_lp:
            if tracked_pos.liquidity != actual_pos.liquidity:
                liq_discrepancy = abs(tracked_pos.liquidity - actual_pos.liquidity)
                if tracked_pos.liquidity != Decimal("0"):
                    liq_pct = liq_discrepancy / tracked_pos.liquidity
                elif actual_pos.liquidity != Decimal("0"):
                    liq_pct = Decimal("1.0")
                else:
                    liq_pct = Decimal("0")

                if liq_pct > tolerance_pct:
                    events.append(
                        ReconciliationEvent(
                            timestamp=timestamp,
                            position_id=position_id,
                            expected=tracked_pos.liquidity,
                            actual=actual_pos.liquidity,
                            discrepancy=liq_discrepancy,
                            discrepancy_pct=liq_pct,
                            field_name="liquidity",
                            auto_corrected=False,
                        )
                    )

        # Compare perp-specific fields (notional_usd)
        if tracked_pos.is_perp and actual_pos.is_perp:
            if tracked_pos.notional_usd != actual_pos.notional_usd:
                notional_discrepancy = abs(tracked_pos.notional_usd - actual_pos.notional_usd)
                if tracked_pos.notional_usd != Decimal("0"):
                    notional_pct = notional_discrepancy / tracked_pos.notional_usd
                elif actual_pos.notional_usd != Decimal("0"):
                    notional_pct = Decimal("1.0")
                else:
                    notional_pct = Decimal("0")

                if notional_pct > tolerance_pct:
                    events.append(
                        ReconciliationEvent(
                            timestamp=timestamp,
                            position_id=position_id,
                            expected=tracked_pos.notional_usd,
                            actual=actual_pos.notional_usd,
                            discrepancy=notional_discrepancy,
                            discrepancy_pct=notional_pct,
                            field_name="notional_usd",
                            auto_corrected=False,
                        )
                    )

        # Compare lending-specific fields (interest_accrued)
        if tracked_pos.is_lending and actual_pos.is_lending:
            if tracked_pos.interest_accrued != actual_pos.interest_accrued:
                interest_discrepancy = abs(tracked_pos.interest_accrued - actual_pos.interest_accrued)
                if tracked_pos.interest_accrued != Decimal("0"):
                    interest_pct = interest_discrepancy / tracked_pos.interest_accrued
                elif actual_pos.interest_accrued != Decimal("0"):
                    interest_pct = Decimal("1.0")
                else:
                    interest_pct = Decimal("0")

                if interest_pct > tolerance_pct:
                    events.append(
                        ReconciliationEvent(
                            timestamp=timestamp,
                            position_id=position_id,
                            expected=tracked_pos.interest_accrued,
                            actual=actual_pos.interest_accrued,
                            discrepancy=interest_discrepancy,
                            discrepancy_pct=interest_pct,
                            field_name="interest_accrued",
                            auto_corrected=False,
                        )
                    )

    # Check for positions in actual but not in tracked (missing tracked)
    for position_id, actual_pos in actual_by_id.items():
        if position_id not in tracked_by_id:
            # Position exists in actual but not in tracked
            actual_value = actual_pos.total_amount
            events.append(
                ReconciliationEvent(
                    timestamp=timestamp,
                    position_id=position_id,
                    expected=Decimal("0"),
                    actual=actual_value,
                    discrepancy=actual_value,
                    discrepancy_pct=Decimal("1.0"),  # 100% unexpected
                    field_name="existence",
                    auto_corrected=False,
                )
            )

    logger.debug(
        "Position comparison complete: %d tracked, %d actual, %d discrepancies found",
        len(tracked),
        len(actual),
        len(events),
    )

    return events


# =============================================================================
# Auto-Correct and Alerting Functions
# =============================================================================


def auto_correct_positions(
    tracked: list["SimulatedPosition"],
    actual: list["SimulatedPosition"],
    events: list["ReconciliationEvent"],
    alert_threshold_pct: Decimal = Decimal("0.05"),
) -> tuple[list["SimulatedPosition"], list["ReconciliationEvent"]]:
    """Auto-correct tracked positions to match actual on-chain state.

    This function updates tracked positions with values from actual positions
    when discrepancies are detected. It also marks ReconciliationEvents as
    auto_corrected=True for positions that were corrected.

    Args:
        tracked: List of tracked SimulatedPosition objects to update
        actual: List of actual SimulatedPosition objects (source of truth)
        events: List of ReconciliationEvent objects from compare_positions()
        alert_threshold_pct: Threshold for emitting warnings (default: 5%)

    Returns:
        Tuple of:
            - Updated tracked positions list (same objects, mutated in place)
            - Updated events list with auto_corrected=True for corrected positions

    Example:
        >>> events = compare_positions(tracked, actual)
        >>> corrected_tracked, updated_events = auto_correct_positions(
        ...     tracked, actual, events, alert_threshold_pct=Decimal("0.05")
        ... )
    """
    # Import at runtime to avoid circular imports
    from almanak.framework.backtesting.models import ReconciliationEvent
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition  # noqa: F811

    # Build lookup of actual positions by ID
    actual_by_id: dict[str, SimulatedPosition] = {pos.position_id: pos for pos in actual}
    tracked_by_id: dict[str, SimulatedPosition] = {pos.position_id: pos for pos in tracked}

    # Track which positions were corrected
    corrected_position_ids: set[str] = set()

    for event in events:
        position_id = event.position_id

        # Only correct if discrepancy exceeds threshold
        if event.discrepancy_pct < alert_threshold_pct:
            continue

        # Get both positions
        tracked_pos = tracked_by_id.get(position_id)
        actual_pos = actual_by_id.get(position_id)

        if tracked_pos is None and actual_pos is not None:
            # Position missing from tracked - add it
            tracked.append(actual_pos)
            tracked_by_id[position_id] = actual_pos
            corrected_position_ids.add(position_id)
            logger.info(
                "Auto-correct: Added missing position %s (actual=%s)",
                position_id,
                actual_pos.total_amount,
            )
        elif tracked_pos is not None and actual_pos is None:
            # Position missing from actual (should be removed from tracked)
            tracked.remove(tracked_pos)
            del tracked_by_id[position_id]
            corrected_position_ids.add(position_id)
            logger.info(
                "Auto-correct: Removed stale position %s (was=%s)",
                position_id,
                tracked_pos.total_amount,
            )
        elif tracked_pos is not None and actual_pos is not None:
            # Both exist - update tracked to match actual
            field_name = event.field_name

            if field_name.startswith("amount_"):
                # Update token amount
                token = field_name.replace("amount_", "")
                tracked_pos.amounts[token] = actual_pos.get_amount(token)
                corrected_position_ids.add(position_id)
                logger.info(
                    "Auto-correct: Updated %s amount_%s from %s to %s",
                    position_id,
                    token,
                    event.expected,
                    event.actual,
                )
            elif field_name == "entry_price":
                tracked_pos.entry_price = actual_pos.entry_price
                corrected_position_ids.add(position_id)
                logger.info(
                    "Auto-correct: Updated %s entry_price from %s to %s",
                    position_id,
                    event.expected,
                    event.actual,
                )
            elif field_name == "liquidity":
                tracked_pos.liquidity = actual_pos.liquidity
                corrected_position_ids.add(position_id)
                logger.info(
                    "Auto-correct: Updated %s liquidity from %s to %s",
                    position_id,
                    event.expected,
                    event.actual,
                )
            elif field_name == "notional_usd":
                tracked_pos.notional_usd = actual_pos.notional_usd
                corrected_position_ids.add(position_id)
                logger.info(
                    "Auto-correct: Updated %s notional_usd from %s to %s",
                    position_id,
                    event.expected,
                    event.actual,
                )
            elif field_name == "interest_accrued":
                tracked_pos.interest_accrued = actual_pos.interest_accrued
                corrected_position_ids.add(position_id)
                logger.info(
                    "Auto-correct: Updated %s interest_accrued from %s to %s",
                    position_id,
                    event.expected,
                    event.actual,
                )
            elif field_name == "existence":
                # Already handled above
                pass

    # Update events to mark which were auto-corrected
    updated_events: list[ReconciliationEvent] = []
    for event in events:
        if event.position_id in corrected_position_ids:
            # Create new event with auto_corrected=True
            updated_events.append(
                ReconciliationEvent(
                    timestamp=event.timestamp,
                    position_id=event.position_id,
                    expected=event.expected,
                    actual=event.actual,
                    discrepancy=event.discrepancy,
                    discrepancy_pct=event.discrepancy_pct,
                    field_name=event.field_name,
                    auto_corrected=True,
                )
            )
        else:
            updated_events.append(event)

    logger.info(
        "Auto-correction complete: %d positions corrected",
        len(corrected_position_ids),
    )

    return tracked, updated_events


@dataclass
class ReconciliationAlert:
    """Alert emitted when position reconciliation detects significant discrepancies.

    Attributes:
        timestamp: When the alert was created
        position_id: ID of the position with discrepancy
        field_name: Field that has the discrepancy
        expected: Expected value
        actual: Actual value
        discrepancy_pct: Percentage discrepancy
        severity: Alert severity level ("WARNING" or "CRITICAL")
        message: Human-readable alert message
        auto_corrected: Whether the position was auto-corrected
    """

    timestamp: datetime
    position_id: str
    field_name: str
    expected: Decimal
    actual: Decimal
    discrepancy_pct: Decimal
    severity: str = "WARNING"
    message: str = ""
    auto_corrected: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "position_id": self.position_id,
            "field_name": self.field_name,
            "expected": str(self.expected),
            "actual": str(self.actual),
            "discrepancy_pct": str(self.discrepancy_pct),
            "severity": self.severity,
            "message": self.message,
            "auto_corrected": self.auto_corrected,
        }


def emit_reconciliation_alerts(
    events: list["ReconciliationEvent"],
    alert_threshold_pct: Decimal = Decimal("0.05"),
    critical_threshold_pct: Decimal = Decimal("0.20"),
    strategy_id: str = "unknown",
) -> list[ReconciliationAlert]:
    """Emit alerts for reconciliation events that exceed thresholds.

    This function analyzes ReconciliationEvent objects and generates alerts
    when discrepancies exceed configurable thresholds. Alerts are logged
    and returned for further processing (e.g., sending to Slack/Telegram).

    Args:
        events: List of ReconciliationEvent objects from compare_positions()
        alert_threshold_pct: Threshold for WARNING alerts (default: 5%)
        critical_threshold_pct: Threshold for CRITICAL alerts (default: 20%)
        strategy_id: Strategy identifier for logging context

    Returns:
        List of ReconciliationAlert objects for events exceeding thresholds

    Example:
        >>> events = compare_positions(tracked, actual)
        >>> alerts = emit_reconciliation_alerts(
        ...     events,
        ...     alert_threshold_pct=Decimal("0.05"),
        ...     critical_threshold_pct=Decimal("0.20"),
        ...     strategy_id="my_strategy",
        ... )
        >>> for alert in alerts:
        ...     print(f"[{alert.severity}] {alert.message}")
    """
    alerts: list[ReconciliationAlert] = []
    timestamp = datetime.now(UTC)

    for event in events:
        # Skip events below alert threshold
        if event.discrepancy_pct < alert_threshold_pct:
            continue

        # Determine severity based on discrepancy magnitude
        if event.discrepancy_pct >= critical_threshold_pct:
            severity = "CRITICAL"
        else:
            severity = "WARNING"

        # Generate human-readable message
        pct_str = f"{float(event.discrepancy_pct) * 100:.1f}%"

        if event.field_name == "existence":
            if event.expected == Decimal("0"):
                message = f"Position {event.position_id} found on-chain but not tracked (value={event.actual})"
            else:
                message = f"Position {event.position_id} tracked but not found on-chain (expected={event.expected})"
        else:
            message = (
                f"Position {event.position_id} has {pct_str} discrepancy in {event.field_name}: "
                f"expected={event.expected}, actual={event.actual}"
            )

        alert = ReconciliationAlert(
            timestamp=timestamp,
            position_id=event.position_id,
            field_name=event.field_name,
            expected=event.expected,
            actual=event.actual,
            discrepancy_pct=event.discrepancy_pct,
            severity=severity,
            message=message,
            auto_corrected=event.auto_corrected,
        )
        alerts.append(alert)

        # Log the alert
        log_msg = f"[{strategy_id}] Reconciliation {severity}: {message}"
        if event.auto_corrected:
            log_msg += " (auto-corrected)"

        if severity == "CRITICAL":
            logger.error(log_msg)
        else:
            logger.warning(log_msg)

    if alerts:
        logger.info(
            "[%s] Reconciliation generated %d alerts (%d CRITICAL, %d WARNING)",
            strategy_id,
            len(alerts),
            sum(1 for a in alerts if a.severity == "CRITICAL"),
            sum(1 for a in alerts if a.severity == "WARNING"),
        )
    else:
        logger.debug(
            "[%s] Reconciliation completed with no alerts above threshold",
            strategy_id,
        )

    return alerts


def reconcile_and_correct(
    tracked: list["SimulatedPosition"],
    actual: list["SimulatedPosition"],
    auto_correct: bool = False,
    alert_threshold_pct: Decimal = Decimal("0.05"),
    critical_threshold_pct: Decimal = Decimal("0.20"),
    tolerance_pct: Decimal = Decimal("0.01"),
    strategy_id: str = "unknown",
) -> tuple[list["ReconciliationEvent"], list[ReconciliationAlert]]:
    """Full reconciliation workflow: compare, optionally correct, and emit alerts.

    This is the main entry point for position reconciliation. It combines
    position comparison, optional auto-correction, and alert emission into
    a single function call.

    Args:
        tracked: List of tracked SimulatedPosition objects
        actual: List of actual SimulatedPosition objects
        auto_correct: Whether to auto-correct tracked positions (default: False)
        alert_threshold_pct: Threshold for WARNING alerts (default: 5%)
        critical_threshold_pct: Threshold for CRITICAL alerts (default: 20%)
        tolerance_pct: Tolerance for numeric comparisons (default: 1%)
        strategy_id: Strategy identifier for logging context

    Returns:
        Tuple of:
            - List of ReconciliationEvent objects
            - List of ReconciliationAlert objects

    Example:
        >>> events, alerts = reconcile_and_correct(
        ...     tracked=my_tracked_positions,
        ...     actual=on_chain_positions,
        ...     auto_correct=True,
        ...     alert_threshold_pct=Decimal("0.05"),
        ...     strategy_id="momentum_v1",
        ... )
        >>> print(f"Found {len(events)} discrepancies, emitted {len(alerts)} alerts")
    """
    # Step 1: Compare positions
    events = compare_positions(tracked, actual, tolerance_pct=tolerance_pct)

    # Step 2: Optionally auto-correct
    if auto_correct and events:
        tracked, events = auto_correct_positions(tracked, actual, events, alert_threshold_pct=alert_threshold_pct)

    # Step 3: Emit alerts for significant discrepancies
    alerts = emit_reconciliation_alerts(
        events,
        alert_threshold_pct=alert_threshold_pct,
        critical_threshold_pct=critical_threshold_pct,
        strategy_id=strategy_id,
    )

    # Log reconciliation summary
    logger.info(
        "[%s] Reconciliation complete: %d tracked, %d actual, %d discrepancies, %d auto-corrected, %d alerts",
        strategy_id,
        len(tracked),
        len(actual),
        len(events),
        sum(1 for e in events if e.auto_corrected),
        len(alerts),
    )

    return events, alerts


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "PositionReconciler",
    "TrackedPosition",
    "PositionDiscrepancy",
    "PositionType",
    "DiscrepancyType",
    "compare_positions",
    "auto_correct_positions",
    "emit_reconciliation_alerts",
    "reconcile_and_correct",
    "ReconciliationAlert",
]
