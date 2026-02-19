"""Position management with chain dimension support.

This module provides chain-aware position tracking for multi-chain strategies.
Positions are stored with an explicit chain field and can be queried/aggregated
across chains or filtered to a specific chain.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class PositionType(StrEnum):
    """Type of position."""

    TOKEN = "TOKEN"
    LP = "LP"
    BORROW = "BORROW"
    SUPPLY = "SUPPLY"
    PERP = "PERP"


@dataclass
class PositionRecord:
    """A single position record with chain dimension.

    This is the fundamental unit of position tracking. Every position
    must have an explicit chain field to support multi-chain strategies.

    Attributes:
        position_id: Unique identifier for this position
        chain: The blockchain this position is on (e.g., 'arbitrum', 'optimism')
        position_type: Type of position (TOKEN, LP, BORROW, SUPPLY, PERP)
        protocol: Protocol where position is held (e.g., 'aave_v3', 'uniswap_v3')
        token: Primary token symbol (or pool identifier for LP)
        amount: Amount of tokens or liquidity
        value_usd: Current USD value of the position
        created_at: When the position was opened
        updated_at: Last update timestamp
        metadata: Additional position-specific data (health factor, ranges, etc.)
    """

    position_id: str
    chain: str
    position_type: PositionType
    protocol: str | None
    token: str
    amount: Decimal
    value_usd: Decimal
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize position to dictionary."""
        return {
            "position_id": self.position_id,
            "chain": self.chain,
            "position_type": self.position_type.value,
            "protocol": self.protocol,
            "token": self.token,
            "amount": str(self.amount),
            "value_usd": str(self.value_usd),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PositionRecord":
        """Deserialize position from dictionary."""
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now(UTC)

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        elif updated_at is None:
            updated_at = datetime.now(UTC)

        return cls(
            position_id=data["position_id"],
            chain=data["chain"],
            position_type=PositionType(data["position_type"]),
            protocol=data.get("protocol"),
            token=data["token"],
            amount=Decimal(str(data["amount"])),
            value_usd=Decimal(str(data["value_usd"])),
            created_at=created_at,
            updated_at=updated_at,
            metadata=data.get("metadata", {}),
        )


class ChainNotFoundError(Exception):
    """Raised when accessing positions for an unconfigured chain."""

    def __init__(self, chain: str, configured_chains: list[str]) -> None:
        self.chain = chain
        self.configured_chains = configured_chains
        super().__init__(f"Chain '{chain}' not found. Configured chains: {configured_chains}")


class PositionManager:
    """Manages positions across multiple chains with chain dimension support.

    Provides methods to store, query, and aggregate positions across chains.
    Designed to work with StateData.state dictionary for persistence.

    Usage:
        # Create manager for a multi-chain strategy
        manager = PositionManager(chains=['arbitrum', 'optimism', 'base'])

        # Add positions
        manager.add_position(PositionRecord(
            position_id='pos-1',
            chain='arbitrum',
            position_type=PositionType.SUPPLY,
            protocol='aave_v3',
            token='WETH',
            amount=Decimal('1.5'),
            value_usd=Decimal('3000'),
        ))

        # Query positions
        all_positions = manager.positions  # All chains
        arb_positions = manager.positions_on('arbitrum')  # Single chain
        total_usd = manager.total_value_usd  # Aggregate value

    Attributes:
        chains: List of configured chain names
    """

    def __init__(
        self,
        chains: list[str],
        initial_positions: list[PositionRecord] | None = None,
    ) -> None:
        """Initialize position manager.

        Args:
            chains: List of chain names this manager handles
            initial_positions: Optional list of positions to pre-populate
        """
        self._chains = chains
        # Store positions indexed by chain, then by position_id
        self._positions: dict[str, dict[str, PositionRecord]] = {chain: {} for chain in chains}

        if initial_positions:
            for pos in initial_positions:
                self.add_position(pos)

    @property
    def chains(self) -> list[str]:
        """Get list of configured chains."""
        return list(self._chains)

    @property
    def positions(self) -> list[PositionRecord]:
        """Get all positions across all chains.

        Returns:
            List of all positions from all configured chains.
        """
        all_positions: list[PositionRecord] = []
        for chain_positions in self._positions.values():
            all_positions.extend(chain_positions.values())
        return all_positions

    def positions_on(self, chain: str) -> list[PositionRecord]:
        """Get positions on a specific chain.

        Args:
            chain: Chain name to filter by

        Returns:
            List of positions on the specified chain

        Raises:
            ChainNotFoundError: If chain is not configured
        """
        if chain not in self._positions:
            raise ChainNotFoundError(chain, self._chains)
        return list(self._positions[chain].values())

    @property
    def total_value_usd(self) -> Decimal:
        """Calculate total USD value across all chains.

        Returns:
            Sum of value_usd for all positions across all chains.
        """
        total = Decimal("0")
        for pos in self.positions:
            total += pos.value_usd
        return total

    def total_value_on(self, chain: str) -> Decimal:
        """Calculate total USD value on a specific chain.

        Args:
            chain: Chain name to calculate value for

        Returns:
            Sum of value_usd for positions on the specified chain

        Raises:
            ChainNotFoundError: If chain is not configured
        """
        if chain not in self._positions:
            raise ChainNotFoundError(chain, self._chains)

        total = Decimal("0")
        for pos in self._positions[chain].values():
            total += pos.value_usd
        return total

    def add_position(self, position: PositionRecord) -> None:
        """Add or update a position.

        If a position with the same position_id exists on the same chain,
        it will be replaced.

        Args:
            position: Position to add/update

        Raises:
            ChainNotFoundError: If position's chain is not configured
        """
        if position.chain not in self._positions:
            raise ChainNotFoundError(position.chain, self._chains)

        self._positions[position.chain][position.position_id] = position
        logger.debug(
            f"Added position {position.position_id} on {position.chain}: {position.token} = ${position.value_usd}"
        )

    def remove_position(self, position_id: str, chain: str) -> bool:
        """Remove a position by ID and chain.

        Args:
            position_id: Position identifier
            chain: Chain the position is on

        Returns:
            True if position was removed, False if not found

        Raises:
            ChainNotFoundError: If chain is not configured
        """
        if chain not in self._positions:
            raise ChainNotFoundError(chain, self._chains)

        if position_id in self._positions[chain]:
            del self._positions[chain][position_id]
            logger.debug(f"Removed position {position_id} from {chain}")
            return True
        return False

    def get_position(self, position_id: str, chain: str) -> PositionRecord | None:
        """Get a specific position by ID and chain.

        Args:
            position_id: Position identifier
            chain: Chain the position is on

        Returns:
            PositionRecord if found, None otherwise

        Raises:
            ChainNotFoundError: If chain is not configured
        """
        if chain not in self._positions:
            raise ChainNotFoundError(chain, self._chains)
        return self._positions[chain].get(position_id)

    def find_position(self, position_id: str) -> PositionRecord | None:
        """Find a position by ID across all chains.

        Args:
            position_id: Position identifier to search for

        Returns:
            PositionRecord if found, None otherwise
        """
        for chain_positions in self._positions.values():
            if position_id in chain_positions:
                return chain_positions[position_id]
        return None

    def positions_by_type(self, position_type: PositionType) -> list[PositionRecord]:
        """Get all positions of a specific type across all chains.

        Args:
            position_type: Type of position to filter by

        Returns:
            List of positions matching the type
        """
        return [pos for pos in self.positions if pos.position_type == position_type]

    def positions_by_protocol(self, protocol: str) -> list[PositionRecord]:
        """Get all positions for a specific protocol across all chains.

        Args:
            protocol: Protocol name to filter by

        Returns:
            List of positions on the protocol
        """
        return [pos for pos in self.positions if pos.protocol == protocol]

    def clear(self, chain: str | None = None) -> None:
        """Clear positions.

        Args:
            chain: If provided, clear only positions on this chain.
                  If None, clear all positions on all chains.

        Raises:
            ChainNotFoundError: If specified chain is not configured
        """
        if chain is not None:
            if chain not in self._positions:
                raise ChainNotFoundError(chain, self._chains)
            self._positions[chain].clear()
            logger.debug(f"Cleared all positions on {chain}")
        else:
            for chain_positions in self._positions.values():
                chain_positions.clear()
            logger.debug("Cleared all positions on all chains")

    def to_dict(self) -> dict[str, Any]:
        """Serialize all positions to dictionary for state storage.

        Returns:
            Dictionary with chain -> position_id -> position data structure
        """
        return {
            "chains": self._chains,
            "positions": {
                chain: {pos_id: pos.to_dict() for pos_id, pos in chain_positions.items()}
                for chain, chain_positions in self._positions.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PositionManager":
        """Deserialize position manager from dictionary.

        Args:
            data: Dictionary from to_dict()

        Returns:
            PositionManager with restored positions
        """
        chains = data.get("chains", [])
        manager = cls(chains=chains)

        positions_data = data.get("positions", {})
        for chain, chain_positions in positions_data.items():
            if chain not in manager._positions:
                continue
            for pos_data in chain_positions.values():
                try:
                    position = PositionRecord.from_dict(pos_data)
                    manager._positions[chain][position.position_id] = position
                except (KeyError, ValueError) as e:
                    logger.warning(f"Failed to deserialize position: {e}")

        return manager

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of positions across all chains.

        Returns:
            Dictionary with per-chain and total statistics
        """
        summary: dict[str, Any] = {
            "total_positions": len(self.positions),
            "total_value_usd": str(self.total_value_usd),
            "chains": {},
        }

        for chain in self._chains:
            chain_positions = self._positions[chain]
            summary["chains"][chain] = {
                "position_count": len(chain_positions),
                "value_usd": str(self.total_value_on(chain)),
                "positions": list(chain_positions.keys()),
            }

        return summary

    def __repr__(self) -> str:
        """String representation of manager."""
        pos_count = len(self.positions)
        return f"PositionManager(chains={self._chains}, positions={pos_count}, total_value_usd={self.total_value_usd})"
