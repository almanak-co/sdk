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

from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN

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

    chain: str = DEFAULT_CHAIN
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

        # Check tracked LP positions (filter to protocols the on-chain reader covers)
        from almanak.framework.backtesting.paper.position_queries import LP_RECONCILER_PROTOCOLS

        tracked_lp = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type == PositionType.LP and pos.protocol in LP_RECONCILER_PROTOCOLS
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

        # Check tracked perp positions (filter to protocols the on-chain reader covers)
        from almanak.framework.backtesting.paper.position_queries import PERP_RECONCILER_PROTOCOLS

        tracked_perps = {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT)
            and pos.protocol in PERP_RECONCILER_PROTOCOLS
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

        on_chain_positions = await self._query_aave_positions_safe(web3, wallet_address)
        if on_chain_positions is None:
            return discrepancies

        # Build lookup by asset address for supply and borrow
        on_chain_supply = {pos.asset_address.lower(): pos for pos in on_chain_positions if pos.has_supply}
        on_chain_borrow = {pos.asset_address.lower(): pos for pos in on_chain_positions if pos.has_debt}

        tracked_supply = self._tracked_lending_by_type(PositionType.SUPPLY)
        tracked_borrow = self._tracked_lending_by_type(PositionType.BORROW)

        for position_id, tracked in tracked_supply.items():
            asset_addr = tracked.asset_address.lower() if tracked.asset_address else ""
            discrepancy = self._check_supply_drift(
                position_id, tracked, on_chain_supply.get(asset_addr), tolerance_percent
            )
            if discrepancy is not None:
                discrepancies.append(discrepancy)

        for position_id, tracked in tracked_borrow.items():
            asset_addr = tracked.asset_address.lower() if tracked.asset_address else ""
            discrepancy = self._check_borrow_drift(
                position_id, tracked, on_chain_borrow.get(asset_addr), tolerance_percent
            )
            if discrepancy is not None:
                discrepancies.append(discrepancy)

        discrepancies.extend(self._collect_untracked_supply(on_chain_supply, tracked_supply))
        discrepancies.extend(self._collect_untracked_borrow(on_chain_borrow, tracked_borrow))

        return discrepancies

    async def _query_aave_positions_safe(
        self,
        web3: Any,
        wallet_address: str,
    ) -> list[Any] | None:
        """Query Aave V3 positions, logging and swallowing query failures."""
        from almanak.framework.backtesting.paper.position_queries import (
            query_aave_positions,
        )

        try:
            return await query_aave_positions(
                wallet=wallet_address,
                web3=web3,
                chain=self.chain,
            )
        except Exception as e:
            logger.error("Failed to query on-chain lending positions: %s", e)
            return None

    def _tracked_lending_by_type(self, position_type: PositionType) -> dict[str, "TrackedPosition"]:
        """Return tracked positions for the lending protocols the on-chain reader covers,
        filtered by supply/borrow type.

        The covered set is derived from ``LendingReadRegistry.default_protocol()`` so
        it stays in sync with the connector registry constant rather than being a
        separately maintained literal.
        """
        from almanak.framework.backtesting.paper.position_queries import LENDING_RECONCILER_PROTOCOLS

        return {
            pid: pos
            for pid, pos in self.positions.items()
            if pos.position_type == position_type and pos.protocol in LENDING_RECONCILER_PROTOCOLS
        }

    def _check_supply_drift(
        self,
        position_id: str,
        tracked: "TrackedPosition",
        on_chain: Any | None,
        tolerance_percent: Decimal,
    ) -> PositionDiscrepancy | None:
        """Compare a single tracked supply against its on-chain row."""
        if on_chain is None:
            return PositionDiscrepancy(
                discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                position_type=PositionType.SUPPLY,
                position_id=position_id,
                expected=tracked.atoken_balance,
                actual=None,
                message=f"Supply position {tracked.asset} not found on-chain",
            )

        if self._values_within_tolerance(tracked.atoken_balance, on_chain.current_atoken_balance, tolerance_percent):
            return None

        return PositionDiscrepancy(
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

    def _check_borrow_drift(
        self,
        position_id: str,
        tracked: "TrackedPosition",
        on_chain: Any | None,
        tolerance_percent: Decimal,
    ) -> PositionDiscrepancy | None:
        """Compare a single tracked borrow against its on-chain row."""
        if on_chain is None:
            return PositionDiscrepancy(
                discrepancy_type=DiscrepancyType.MISSING_ON_CHAIN,
                position_type=PositionType.BORROW,
                position_id=position_id,
                expected=tracked.debt_balance,
                actual=None,
                message=f"Borrow position {tracked.asset} not found on-chain",
            )

        if self._values_within_tolerance(tracked.debt_balance, on_chain.total_debt, tolerance_percent):
            return None

        return PositionDiscrepancy(
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

    @staticmethod
    def _tracked_asset_addresses(tracked: dict[str, "TrackedPosition"]) -> set[str]:
        """Return the set of normalized (lowercase) tracked asset addresses."""
        return {pos.asset_address.lower() for pos in tracked.values() if pos.asset_address}

    @staticmethod
    def _collect_untracked_supply(
        on_chain_supply: dict[str, Any],
        tracked_supply: dict[str, "TrackedPosition"],
    ) -> list[PositionDiscrepancy]:
        """Emit a discrepancy for every on-chain supply absent from the tracker.

        Match by normalized asset address rather than synthesised position id —
        a tracker entry keyed off a checksum address would otherwise look
        ``MISSING_IN_TRACKER`` against the lowercase on-chain key.
        """
        tracked_addrs = PositionReconciler._tracked_asset_addresses(tracked_supply)
        out: list[PositionDiscrepancy] = []
        for asset_addr, on_chain in on_chain_supply.items():
            if asset_addr.lower() in tracked_addrs:
                continue
            position_id = f"aave_v3_{asset_addr}_supply"
            out.append(
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
        return out

    @staticmethod
    def _collect_untracked_borrow(
        on_chain_borrow: dict[str, Any],
        tracked_borrow: dict[str, "TrackedPosition"],
    ) -> list[PositionDiscrepancy]:
        """Emit a discrepancy for every on-chain borrow absent from the tracker.

        See ``_collect_untracked_supply`` for the address-normalisation
        rationale — same shape, applied to borrow positions.
        """
        tracked_addrs = PositionReconciler._tracked_asset_addresses(tracked_borrow)
        out: list[PositionDiscrepancy] = []
        for asset_addr, on_chain in on_chain_borrow.items():
            if asset_addr.lower() in tracked_addrs:
                continue
            position_id = f"aave_v3_{asset_addr}_borrow"
            out.append(
                PositionDiscrepancy(
                    discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
                    position_type=PositionType.BORROW,
                    position_id=position_id,
                    expected=None,
                    actual=on_chain.total_debt,
                    message=(f"Borrow {on_chain.asset} found on-chain but not tracked (debt={on_chain.total_debt})"),
                )
            )
        return out

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
        reconciler = cls(chain=data.get("chain", LEGACY_SERIALIZED_CHAIN))
        reconciler.positions = {
            pid: TrackedPosition.from_dict(pos_data) for pid, pos_data in data.get("positions", {}).items()
        }
        reconciler.closed_positions = data.get("closed_positions", [])
        reconciler.reconciliation_history = data.get("reconciliation_history", [])
        return reconciler


# =============================================================================
# Position Comparison Function
# =============================================================================


def _position_lookup(positions: list["SimulatedPosition"]) -> dict[str, "SimulatedPosition"]:
    return {pos.position_id: pos for pos in positions}


def _ordered_amount_tokens(
    tracked_pos: "SimulatedPosition",
    actual_pos: "SimulatedPosition",
) -> list[str]:
    ordered_tokens: list[str] = []
    seen: set[str] = set()
    for token in [*tracked_pos.tokens, *actual_pos.tokens, *tracked_pos.amounts, *actual_pos.amounts]:
        if token in seen:
            continue
        seen.add(token)
        ordered_tokens.append(token)
    return ordered_tokens


def _discrepancy_pct(expected: Decimal, actual: Decimal) -> Decimal:
    if expected != Decimal("0"):
        return abs(expected - actual) / expected
    if actual != Decimal("0"):
        return Decimal("1.0")
    return Decimal("0")


def _field_event_if_needed(
    event_cls: type["ReconciliationEvent"],
    timestamp: datetime,
    position_id: str,
    field_name: str,
    expected: Decimal,
    actual: Decimal,
    tolerance_pct: Decimal,
) -> "ReconciliationEvent | None":
    if expected == actual:
        return None

    discrepancy = abs(expected - actual)
    discrepancy_pct = _discrepancy_pct(expected, actual)
    if discrepancy_pct <= tolerance_pct:
        return None

    return event_cls(
        timestamp=timestamp,
        position_id=position_id,
        expected=expected,
        actual=actual,
        discrepancy=discrepancy,
        discrepancy_pct=discrepancy_pct,
        field_name=field_name,
        auto_corrected=False,
    )


def _existence_event(
    event_cls: type["ReconciliationEvent"],
    timestamp: datetime,
    position_id: str,
    expected: Decimal,
    actual: Decimal,
) -> "ReconciliationEvent":
    discrepancy = expected if actual == Decimal("0") else actual
    return event_cls(
        timestamp=timestamp,
        position_id=position_id,
        expected=expected,
        actual=actual,
        discrepancy=discrepancy,
        discrepancy_pct=Decimal("1.0"),
        field_name="existence",
        auto_corrected=False,
    )


def _amount_mismatch_events(
    event_cls: type["ReconciliationEvent"],
    timestamp: datetime,
    tracked_pos: "SimulatedPosition",
    actual_pos: "SimulatedPosition",
    tolerance_pct: Decimal,
) -> list["ReconciliationEvent"]:
    events: list[ReconciliationEvent] = []
    for token in _ordered_amount_tokens(tracked_pos, actual_pos):
        event = _field_event_if_needed(
            event_cls,
            timestamp,
            tracked_pos.position_id,
            f"amount_{token}",
            tracked_pos.get_amount(token),
            actual_pos.get_amount(token),
            tolerance_pct,
        )
        if event is not None:
            events.append(event)
    return events


def _append_optional_event(
    events: list["ReconciliationEvent"],
    event: "ReconciliationEvent | None",
) -> None:
    if event is not None:
        events.append(event)


def _matching_position_events(
    event_cls: type["ReconciliationEvent"],
    timestamp: datetime,
    tracked_pos: "SimulatedPosition",
    actual_pos: "SimulatedPosition",
    tolerance_pct: Decimal,
) -> list["ReconciliationEvent"]:
    events = _amount_mismatch_events(event_cls, timestamp, tracked_pos, actual_pos, tolerance_pct)

    _append_optional_event(
        events,
        _field_event_if_needed(
            event_cls,
            timestamp,
            tracked_pos.position_id,
            "entry_price",
            tracked_pos.entry_price,
            actual_pos.entry_price,
            tolerance_pct,
        ),
    )

    if tracked_pos.is_lp and actual_pos.is_lp:
        _append_optional_event(
            events,
            _field_event_if_needed(
                event_cls,
                timestamp,
                tracked_pos.position_id,
                "liquidity",
                tracked_pos.liquidity,
                actual_pos.liquidity,
                tolerance_pct,
            ),
        )

    if tracked_pos.is_perp and actual_pos.is_perp:
        _append_optional_event(
            events,
            _field_event_if_needed(
                event_cls,
                timestamp,
                tracked_pos.position_id,
                "notional_usd",
                tracked_pos.notional_usd,
                actual_pos.notional_usd,
                tolerance_pct,
            ),
        )

    if tracked_pos.is_lending and actual_pos.is_lending:
        _append_optional_event(
            events,
            _field_event_if_needed(
                event_cls,
                timestamp,
                tracked_pos.position_id,
                "interest_accrued",
                tracked_pos.interest_accrued,
                actual_pos.interest_accrued,
                tolerance_pct,
            ),
        )

    return events


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

    events: list[ReconciliationEvent] = []
    timestamp = datetime.now(UTC)

    # Build lookup by position_id
    tracked_by_id = _position_lookup(tracked)
    actual_by_id = _position_lookup(actual)

    # Check for positions in tracked but not in actual (missing actual)
    for position_id, tracked_pos in tracked_by_id.items():
        if position_id not in actual_by_id:
            events.append(
                _existence_event(
                    ReconciliationEvent,
                    timestamp,
                    position_id,
                    expected=tracked_pos.total_amount,
                    actual=Decimal("0"),
                ),
            )
            continue

        # Position exists in both - compare fields
        events.extend(
            _matching_position_events(
                ReconciliationEvent,
                timestamp,
                tracked_pos,
                actual_by_id[position_id],
                tolerance_pct,
            )
        )

    # Check for positions in actual but not in tracked (missing tracked)
    for position_id, actual_pos in actual_by_id.items():
        if position_id not in tracked_by_id:
            events.append(
                _existence_event(
                    ReconciliationEvent,
                    timestamp,
                    position_id,
                    expected=Decimal("0"),
                    actual=actual_pos.total_amount,
                ),
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


def _should_auto_correct(event: "ReconciliationEvent", alert_threshold_pct: Decimal) -> bool:
    return event.discrepancy_pct >= alert_threshold_pct


def _add_missing_tracked_position(
    tracked: list["SimulatedPosition"],
    tracked_by_id: dict[str, "SimulatedPosition"],
    position_id: str,
    actual_pos: "SimulatedPosition",
) -> None:
    tracked.append(actual_pos)
    tracked_by_id[position_id] = actual_pos
    logger.info(
        "Auto-correct: Added missing position %s (actual=%s)",
        position_id,
        actual_pos.total_amount,
    )


def _remove_stale_tracked_position(
    tracked: list["SimulatedPosition"],
    tracked_by_id: dict[str, "SimulatedPosition"],
    position_id: str,
    tracked_pos: "SimulatedPosition",
) -> None:
    tracked.remove(tracked_pos)
    del tracked_by_id[position_id]
    logger.info(
        "Auto-correct: Removed stale position %s (was=%s)",
        position_id,
        tracked_pos.total_amount,
    )


def _update_tracked_field(
    tracked_pos: "SimulatedPosition",
    actual_pos: "SimulatedPosition",
    event: "ReconciliationEvent",
) -> bool:
    field_name = event.field_name
    position_id = event.position_id

    if field_name.startswith("amount_"):
        token = field_name.replace("amount_", "")
        tracked_pos.amounts[token] = actual_pos.get_amount(token)
        logger.info(
            "Auto-correct: Updated %s amount_%s from %s to %s",
            position_id,
            token,
            event.expected,
            event.actual,
        )
        return True

    if field_name == "entry_price":
        tracked_pos.entry_price = actual_pos.entry_price
    elif field_name == "liquidity":
        tracked_pos.liquidity = actual_pos.liquidity
    elif field_name == "notional_usd":
        tracked_pos.notional_usd = actual_pos.notional_usd
    elif field_name == "interest_accrued":
        tracked_pos.interest_accrued = actual_pos.interest_accrued
    else:
        return False

    logger.info(
        "Auto-correct: Updated %s %s from %s to %s",
        position_id,
        field_name,
        event.expected,
        event.actual,
    )
    return True


def _apply_auto_correction_event(
    tracked: list["SimulatedPosition"],
    tracked_by_id: dict[str, "SimulatedPosition"],
    actual_by_id: dict[str, "SimulatedPosition"],
    event: "ReconciliationEvent",
) -> bool:
    position_id = event.position_id
    tracked_pos = tracked_by_id.get(position_id)
    actual_pos = actual_by_id.get(position_id)

    if tracked_pos is None and actual_pos is not None:
        _add_missing_tracked_position(tracked, tracked_by_id, position_id, actual_pos)
        return True

    if tracked_pos is not None and actual_pos is None:
        _remove_stale_tracked_position(tracked, tracked_by_id, position_id, tracked_pos)
        return True

    if tracked_pos is not None and actual_pos is not None:
        return _update_tracked_field(tracked_pos, actual_pos, event)

    return False


def _mark_event_auto_corrected(
    event_cls: type["ReconciliationEvent"],
    event: "ReconciliationEvent",
) -> "ReconciliationEvent":
    return event_cls(
        timestamp=event.timestamp,
        position_id=event.position_id,
        expected=event.expected,
        actual=event.actual,
        discrepancy=event.discrepancy,
        discrepancy_pct=event.discrepancy_pct,
        field_name=event.field_name,
        auto_corrected=True,
    )


def _updated_auto_correction_events(
    event_cls: type["ReconciliationEvent"],
    events: list["ReconciliationEvent"],
    corrected_position_ids: set[str],
) -> list["ReconciliationEvent"]:
    return [
        _mark_event_auto_corrected(event_cls, event) if event.position_id in corrected_position_ids else event
        for event in events
    ]


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

    # Build lookup of actual positions by ID
    actual_by_id = _position_lookup(actual)
    tracked_by_id = _position_lookup(tracked)

    # Track which positions were corrected
    corrected_position_ids: set[str] = set()

    for event in events:
        # Only correct if discrepancy exceeds threshold
        if not _should_auto_correct(event, alert_threshold_pct):
            continue

        if _apply_auto_correction_event(tracked, tracked_by_id, actual_by_id, event):
            corrected_position_ids.add(event.position_id)

    # Update events to mark which were auto-corrected
    updated_events = _updated_auto_correction_events(ReconciliationEvent, events, corrected_position_ids)

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


def _alert_severity(discrepancy_pct: Decimal, critical_threshold_pct: Decimal) -> str:
    if discrepancy_pct >= critical_threshold_pct:
        return "CRITICAL"
    return "WARNING"


def _alert_message(event: "ReconciliationEvent") -> str:
    if event.field_name == "existence":
        if event.expected == Decimal("0"):
            return f"Position {event.position_id} found on-chain but not tracked (value={event.actual})"
        return f"Position {event.position_id} tracked but not found on-chain (expected={event.expected})"

    pct_str = f"{float(event.discrepancy_pct) * 100:.1f}%"
    return (
        f"Position {event.position_id} has {pct_str} discrepancy in {event.field_name}: "
        f"expected={event.expected}, actual={event.actual}"
    )


def _build_reconciliation_alert(
    timestamp: datetime,
    event: "ReconciliationEvent",
    critical_threshold_pct: Decimal,
) -> ReconciliationAlert:
    severity = _alert_severity(event.discrepancy_pct, critical_threshold_pct)
    return ReconciliationAlert(
        timestamp=timestamp,
        position_id=event.position_id,
        field_name=event.field_name,
        expected=event.expected,
        actual=event.actual,
        discrepancy_pct=event.discrepancy_pct,
        severity=severity,
        message=_alert_message(event),
        auto_corrected=event.auto_corrected,
    )


def _log_reconciliation_alert(deployment_id: str, alert: ReconciliationAlert) -> None:
    log_msg = f"[{deployment_id}] Reconciliation {alert.severity}: {alert.message}"
    if alert.auto_corrected:
        log_msg += " (auto-corrected)"

    if alert.severity == "CRITICAL":
        logger.error(log_msg)
    else:
        logger.warning(log_msg)


def _log_reconciliation_alert_summary(deployment_id: str, alerts: list[ReconciliationAlert]) -> None:
    if not alerts:
        logger.debug(
            "[%s] Reconciliation completed with no alerts above threshold",
            deployment_id,
        )
        return

    logger.info(
        "[%s] Reconciliation generated %d alerts (%d CRITICAL, %d WARNING)",
        deployment_id,
        len(alerts),
        sum(1 for a in alerts if a.severity == "CRITICAL"),
        sum(1 for a in alerts if a.severity == "WARNING"),
    )


def emit_reconciliation_alerts(
    events: list["ReconciliationEvent"],
    alert_threshold_pct: Decimal = Decimal("0.05"),
    critical_threshold_pct: Decimal = Decimal("0.20"),
    deployment_id: str = "unknown",
) -> list[ReconciliationAlert]:
    """Emit alerts for reconciliation events that exceed thresholds.

    This function analyzes ReconciliationEvent objects and generates alerts
    when discrepancies exceed configurable thresholds. Alerts are logged
    and returned for further processing (e.g., sending to Slack/Telegram).

    Args:
        events: List of ReconciliationEvent objects from compare_positions()
        alert_threshold_pct: Threshold for WARNING alerts (default: 5%)
        critical_threshold_pct: Threshold for CRITICAL alerts (default: 20%)
        deployment_id: Deployment identifier for logging context

    Returns:
        List of ReconciliationAlert objects for events exceeding thresholds

    Example:
        >>> events = compare_positions(tracked, actual)
        >>> alerts = emit_reconciliation_alerts(
        ...     events,
        ...     alert_threshold_pct=Decimal("0.05"),
        ...     critical_threshold_pct=Decimal("0.20"),
        ...     deployment_id="my_strategy",
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

        alert = _build_reconciliation_alert(timestamp, event, critical_threshold_pct)
        alerts.append(alert)
        _log_reconciliation_alert(deployment_id, alert)

    _log_reconciliation_alert_summary(deployment_id, alerts)

    return alerts


def reconcile_and_correct(
    tracked: list["SimulatedPosition"],
    actual: list["SimulatedPosition"],
    auto_correct: bool = False,
    alert_threshold_pct: Decimal = Decimal("0.05"),
    critical_threshold_pct: Decimal = Decimal("0.20"),
    tolerance_pct: Decimal = Decimal("0.01"),
    deployment_id: str = "unknown",
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
        deployment_id: Deployment identifier for logging context

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
        ...     deployment_id="momentum_v1",
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
        deployment_id=deployment_id,
    )

    # Log reconciliation summary
    logger.info(
        "[%s] Reconciliation complete: %d tracked, %d actual, %d discrepancies, %d auto-corrected, %d alerts",
        deployment_id,
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
