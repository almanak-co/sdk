"""In-flight exposure tracking for cross-chain bridge transfers.

This module provides first-class state management for assets in transit during
cross-chain bridging operations. It tracks assets that have left the source chain
but have not yet been credited on the destination chain.

Key Features:
- Track in-flight assets as first-class state (not just risk context)
- Assets appear with 'bridging' status for portfolio visibility
- Risk limits consider current_exposure + in_flight_exposure
- Max total in-flight exposure is configurable and enforced
- Integration with RiskGuard for pre-execution validation

Example:
    from almanak.framework.state.in_flight import InFlightExposureTracker, InFlightAsset, InFlightStatus

    tracker = InFlightExposureTracker(chains=['arbitrum', 'optimism'])

    # Record a bridge deposit
    tracker.add_transfer(InFlightAsset(
        transfer_id='bridge-123',
        token='USDC',
        amount=Decimal('1000'),
        amount_usd=Decimal('1000'),
        from_chain='arbitrum',
        to_chain='optimism',
        bridge='across',
    ))

    # Check in-flight exposure
    total = tracker.total_in_flight_usd
    assets = tracker.in_flight_assets  # All assets with 'bridging' status

    # Update status when bridge completes
    tracker.mark_completed('bridge-123')
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class InFlightStatus(StrEnum):
    """Status of an in-flight bridge transfer.

    The status progresses through these states:
    - BRIDGING: Asset has left source chain, in transit
    - CONFIRMING: Destination transaction detected, waiting for confirmations
    - COMPLETED: Asset credited on destination chain
    - FAILED: Bridge transfer failed
    - EXPIRED: Bridge transfer timed out
    - REFUNDED: Asset was refunded to source chain
    """

    BRIDGING = "bridging"
    CONFIRMING = "confirming"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    REFUNDED = "refunded"


@dataclass
class InFlightAsset:
    """Represents an asset in transit during a cross-chain bridge transfer.

    This is a first-class state entity that tracks assets that have left
    the source chain but are not yet credited on the destination chain.

    Attributes:
        transfer_id: Unique identifier for this transfer
        token: Token symbol being transferred
        amount: Amount of tokens in transit
        amount_usd: USD value of the transfer
        from_chain: Source blockchain
        to_chain: Destination blockchain
        bridge: Bridge protocol used (e.g., 'across', 'stargate')
        status: Current status of the transfer
        source_tx_hash: Transaction hash on source chain
        destination_tx_hash: Transaction hash on destination chain (when credited)
        initiated_at: When the bridge deposit was initiated
        expected_completion: Estimated completion time
        updated_at: Last status update timestamp
        retry_count: Number of retry attempts
        metadata: Additional bridge-specific data
    """

    transfer_id: str
    token: str
    amount: Decimal
    amount_usd: Decimal
    from_chain: str
    to_chain: str
    bridge: str
    status: InFlightStatus = InFlightStatus.BRIDGING
    source_tx_hash: str | None = None
    destination_tx_hash: str | None = None
    initiated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expected_completion: datetime | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        """Check if this transfer is still in-flight (not terminal state)."""
        return self.status in (InFlightStatus.BRIDGING, InFlightStatus.CONFIRMING)

    @property
    def is_terminal(self) -> bool:
        """Check if this transfer has reached a terminal state."""
        return self.status in (
            InFlightStatus.COMPLETED,
            InFlightStatus.FAILED,
            InFlightStatus.EXPIRED,
            InFlightStatus.REFUNDED,
        )

    @property
    def elapsed_time(self) -> timedelta:
        """Calculate time elapsed since transfer was initiated."""
        return datetime.now(UTC) - self.initiated_at

    @property
    def is_overdue(self) -> bool:
        """Check if transfer has exceeded expected completion time."""
        if self.expected_completion is None:
            return False
        return datetime.now(UTC) > self.expected_completion

    def to_dict(self) -> dict[str, Any]:
        """Serialize asset to dictionary."""
        return {
            "transfer_id": self.transfer_id,
            "token": self.token,
            "amount": str(self.amount),
            "amount_usd": str(self.amount_usd),
            "from_chain": self.from_chain,
            "to_chain": self.to_chain,
            "bridge": self.bridge,
            "status": self.status.value,
            "source_tx_hash": self.source_tx_hash,
            "destination_tx_hash": self.destination_tx_hash,
            "initiated_at": self.initiated_at.isoformat(),
            "expected_completion": (self.expected_completion.isoformat() if self.expected_completion else None),
            "updated_at": self.updated_at.isoformat(),
            "retry_count": self.retry_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InFlightAsset":
        """Deserialize asset from dictionary."""
        initiated_at = data.get("initiated_at")
        if isinstance(initiated_at, str):
            initiated_at = datetime.fromisoformat(initiated_at)
        elif initiated_at is None:
            initiated_at = datetime.now(UTC)

        expected_completion = data.get("expected_completion")
        if isinstance(expected_completion, str):
            expected_completion = datetime.fromisoformat(expected_completion)

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        elif updated_at is None:
            updated_at = datetime.now(UTC)

        return cls(
            transfer_id=data["transfer_id"],
            token=data["token"],
            amount=Decimal(str(data["amount"])),
            amount_usd=Decimal(str(data["amount_usd"])),
            from_chain=data["from_chain"],
            to_chain=data["to_chain"],
            bridge=data["bridge"],
            status=InFlightStatus(data.get("status", "bridging")),
            source_tx_hash=data.get("source_tx_hash"),
            destination_tx_hash=data.get("destination_tx_hash"),
            initiated_at=initiated_at,
            expected_completion=expected_completion,
            updated_at=updated_at,
            retry_count=data.get("retry_count", 0),
            metadata=data.get("metadata", {}),
        )


class InFlightExposureError(Exception):
    """Base exception for in-flight exposure errors."""

    pass


class TransferNotFoundError(InFlightExposureError):
    """Raised when a transfer is not found."""

    def __init__(self, transfer_id: str) -> None:
        self.transfer_id = transfer_id
        super().__init__(f"Transfer '{transfer_id}' not found")


class InFlightLimitExceededError(InFlightExposureError):
    """Raised when in-flight exposure would exceed limits."""

    def __init__(
        self,
        current_usd: Decimal,
        new_usd: Decimal,
        limit_usd: Decimal,
    ) -> None:
        self.current_usd = current_usd
        self.new_usd = new_usd
        self.limit_usd = limit_usd
        super().__init__(
            f"In-flight limit exceeded: current ${current_usd} + new ${new_usd} "
            f"= ${current_usd + new_usd} > limit ${limit_usd}"
        )


@dataclass
class InFlightExposureConfig:
    """Configuration for in-flight exposure tracking.

    Attributes:
        max_total_in_flight_usd: Maximum total USD value allowed in-flight
        max_per_chain_in_flight_usd: Maximum per-chain in-flight exposure
        max_per_bridge_in_flight_usd: Maximum per-bridge in-flight exposure
        stale_transfer_hours: Hours after which transfers are considered stale
        auto_expire_hours: Hours after which to auto-expire stuck transfers
    """

    max_total_in_flight_usd: Decimal = field(default_factory=lambda: Decimal("100000"))
    max_per_chain_in_flight_usd: dict[str, Decimal] = field(default_factory=dict)
    max_per_bridge_in_flight_usd: dict[str, Decimal] = field(default_factory=dict)
    stale_transfer_hours: int = 1
    auto_expire_hours: int = 24

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to dictionary."""
        return {
            "max_total_in_flight_usd": str(self.max_total_in_flight_usd),
            "max_per_chain_in_flight_usd": {k: str(v) for k, v in self.max_per_chain_in_flight_usd.items()},
            "max_per_bridge_in_flight_usd": {k: str(v) for k, v in self.max_per_bridge_in_flight_usd.items()},
            "stale_transfer_hours": self.stale_transfer_hours,
            "auto_expire_hours": self.auto_expire_hours,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InFlightExposureConfig":
        """Deserialize configuration from dictionary."""
        return cls(
            max_total_in_flight_usd=Decimal(str(data.get("max_total_in_flight_usd", "100000"))),
            max_per_chain_in_flight_usd={
                k: Decimal(str(v)) for k, v in data.get("max_per_chain_in_flight_usd", {}).items()
            },
            max_per_bridge_in_flight_usd={
                k: Decimal(str(v)) for k, v in data.get("max_per_bridge_in_flight_usd", {}).items()
            },
            stale_transfer_hours=data.get("stale_transfer_hours", 1),
            auto_expire_hours=data.get("auto_expire_hours", 24),
        )


@dataclass
class InFlightSummary:
    """Summary of in-flight exposure for risk assessment.

    This is provided to RiskGuard for validation before executing
    new bridge transfers.

    Attributes:
        total_in_flight_usd: Total USD value currently in-flight
        active_transfer_count: Number of active (non-terminal) transfers
        per_chain_in_flight_usd: In-flight exposure by source chain
        per_bridge_in_flight_usd: In-flight exposure by bridge protocol
        oldest_transfer_age: Age of the oldest active transfer
        stale_transfer_count: Number of transfers exceeding expected time
    """

    total_in_flight_usd: Decimal
    active_transfer_count: int
    per_chain_in_flight_usd: dict[str, Decimal]
    per_bridge_in_flight_usd: dict[str, Decimal]
    oldest_transfer_age: timedelta | None
    stale_transfer_count: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize summary to dictionary."""
        return {
            "total_in_flight_usd": str(self.total_in_flight_usd),
            "active_transfer_count": self.active_transfer_count,
            "per_chain_in_flight_usd": {k: str(v) for k, v in self.per_chain_in_flight_usd.items()},
            "per_bridge_in_flight_usd": {k: str(v) for k, v in self.per_bridge_in_flight_usd.items()},
            "oldest_transfer_age_seconds": (
                self.oldest_transfer_age.total_seconds() if self.oldest_transfer_age else None
            ),
            "stale_transfer_count": self.stale_transfer_count,
        }


class InFlightExposureTracker:
    """Manages in-flight exposure as first-class state.

    Tracks assets that have left the source chain but are not yet
    credited on the destination chain. Integrates with RiskGuard
    for exposure validation before new bridge operations.

    Usage:
        tracker = InFlightExposureTracker(chains=['arbitrum', 'optimism'])

        # Record a new transfer
        tracker.add_transfer(InFlightAsset(...))

        # Get summary for risk validation
        summary = tracker.get_summary()

        # Check if new transfer would exceed limits
        can_bridge = tracker.can_add_exposure(Decimal('1000'))

        # Update status when bridge completes
        tracker.mark_completed('transfer-id', destination_tx='0x...')

    Attributes:
        chains: List of configured chain names
        config: In-flight exposure configuration
    """

    def __init__(
        self,
        chains: list[str],
        config: InFlightExposureConfig | None = None,
    ) -> None:
        """Initialize the in-flight exposure tracker.

        Args:
            chains: List of configured chain names
            config: Optional configuration for limits
        """
        self._chains = chains
        self._config = config or InFlightExposureConfig()
        self._transfers: dict[str, InFlightAsset] = {}
        logger.info(
            f"InFlightExposureTracker initialized for chains={chains}, "
            f"max_in_flight=${self._config.max_total_in_flight_usd}"
        )

    @property
    def chains(self) -> list[str]:
        """Get configured chains."""
        return list(self._chains)

    @property
    def config(self) -> InFlightExposureConfig:
        """Get configuration."""
        return self._config

    @property
    def in_flight_assets(self) -> list[InFlightAsset]:
        """Get all in-flight assets with 'bridging' status.

        Returns:
            List of all active (non-terminal) transfers.
        """
        return [t for t in self._transfers.values() if t.is_active]

    @property
    def all_transfers(self) -> list[InFlightAsset]:
        """Get all tracked transfers including completed ones.

        Returns:
            List of all transfers (active and terminal).
        """
        return list(self._transfers.values())

    @property
    def total_in_flight_usd(self) -> Decimal:
        """Calculate total USD value currently in-flight.

        Only includes active transfers (BRIDGING or CONFIRMING status).

        Returns:
            Total USD value of active in-flight transfers.
        """
        return sum(
            (t.amount_usd for t in self._transfers.values() if t.is_active),
            Decimal("0"),
        )

    def in_flight_on_chain(self, chain: str) -> Decimal:
        """Get in-flight exposure for transfers originating from a chain.

        Args:
            chain: Source chain name

        Returns:
            USD value of active transfers from this chain.
        """
        return sum(
            (t.amount_usd for t in self._transfers.values() if t.is_active and t.from_chain == chain),
            Decimal("0"),
        )

    def in_flight_via_bridge(self, bridge: str) -> Decimal:
        """Get in-flight exposure through a specific bridge.

        Args:
            bridge: Bridge protocol name

        Returns:
            USD value of active transfers through this bridge.
        """
        return sum(
            (t.amount_usd for t in self._transfers.values() if t.is_active and t.bridge == bridge),
            Decimal("0"),
        )

    def get_transfer(self, transfer_id: str) -> InFlightAsset | None:
        """Get a transfer by ID.

        Args:
            transfer_id: Unique transfer identifier

        Returns:
            InFlightAsset if found, None otherwise.
        """
        return self._transfers.get(transfer_id)

    def add_transfer(
        self,
        asset: InFlightAsset,
        enforce_limits: bool = True,
    ) -> None:
        """Add a new in-flight transfer.

        Args:
            asset: The in-flight asset to track
            enforce_limits: If True, raise error if limits exceeded

        Raises:
            InFlightLimitExceededError: If adding would exceed limits
        """
        if enforce_limits:
            current = self.total_in_flight_usd
            if current + asset.amount_usd > self._config.max_total_in_flight_usd:
                raise InFlightLimitExceededError(
                    current_usd=current,
                    new_usd=asset.amount_usd,
                    limit_usd=self._config.max_total_in_flight_usd,
                )

            # Check per-chain limits
            chain_limit = self._config.max_per_chain_in_flight_usd.get(asset.from_chain)
            if chain_limit:
                chain_current = self.in_flight_on_chain(asset.from_chain)
                if chain_current + asset.amount_usd > chain_limit:
                    raise InFlightLimitExceededError(
                        current_usd=chain_current,
                        new_usd=asset.amount_usd,
                        limit_usd=chain_limit,
                    )

            # Check per-bridge limits
            bridge_limit = self._config.max_per_bridge_in_flight_usd.get(asset.bridge)
            if bridge_limit:
                bridge_current = self.in_flight_via_bridge(asset.bridge)
                if bridge_current + asset.amount_usd > bridge_limit:
                    raise InFlightLimitExceededError(
                        current_usd=bridge_current,
                        new_usd=asset.amount_usd,
                        limit_usd=bridge_limit,
                    )

        self._transfers[asset.transfer_id] = asset
        logger.info(
            f"Added in-flight transfer {asset.transfer_id}: "
            f"${asset.amount_usd} {asset.token} from {asset.from_chain} "
            f"to {asset.to_chain} via {asset.bridge}"
        )

    def can_add_exposure(self, amount_usd: Decimal) -> bool:
        """Check if additional exposure can be added within limits.

        Args:
            amount_usd: USD value of potential new transfer

        Returns:
            True if adding would stay within limits.
        """
        return self.total_in_flight_usd + amount_usd <= self._config.max_total_in_flight_usd

    def update_status(
        self,
        transfer_id: str,
        status: InFlightStatus,
        destination_tx_hash: str | None = None,
    ) -> InFlightAsset:
        """Update the status of a transfer.

        Args:
            transfer_id: Transfer to update
            status: New status
            destination_tx_hash: Optional destination chain tx hash

        Returns:
            Updated InFlightAsset

        Raises:
            TransferNotFoundError: If transfer not found
        """
        asset = self._transfers.get(transfer_id)
        if not asset:
            raise TransferNotFoundError(transfer_id)

        old_status = asset.status
        asset.status = status
        asset.updated_at = datetime.now(UTC)

        if destination_tx_hash:
            asset.destination_tx_hash = destination_tx_hash

        logger.info(f"Transfer {transfer_id} status: {old_status.value} -> {status.value}")
        return asset

    def mark_confirming(
        self,
        transfer_id: str,
        destination_tx_hash: str,
    ) -> InFlightAsset:
        """Mark a transfer as confirming (detected on destination chain).

        Args:
            transfer_id: Transfer to update
            destination_tx_hash: Destination chain transaction hash

        Returns:
            Updated InFlightAsset
        """
        return self.update_status(
            transfer_id,
            InFlightStatus.CONFIRMING,
            destination_tx_hash=destination_tx_hash,
        )

    def mark_completed(
        self,
        transfer_id: str,
        destination_tx_hash: str | None = None,
    ) -> InFlightAsset:
        """Mark a transfer as completed (credited on destination chain).

        Args:
            transfer_id: Transfer to update
            destination_tx_hash: Optional destination chain tx hash

        Returns:
            Updated InFlightAsset
        """
        return self.update_status(
            transfer_id,
            InFlightStatus.COMPLETED,
            destination_tx_hash=destination_tx_hash,
        )

    def mark_failed(self, transfer_id: str) -> InFlightAsset:
        """Mark a transfer as failed.

        Args:
            transfer_id: Transfer to update

        Returns:
            Updated InFlightAsset
        """
        return self.update_status(transfer_id, InFlightStatus.FAILED)

    def mark_expired(self, transfer_id: str) -> InFlightAsset:
        """Mark a transfer as expired.

        Args:
            transfer_id: Transfer to update

        Returns:
            Updated InFlightAsset
        """
        return self.update_status(transfer_id, InFlightStatus.EXPIRED)

    def mark_refunded(self, transfer_id: str) -> InFlightAsset:
        """Mark a transfer as refunded to source chain.

        Args:
            transfer_id: Transfer to update

        Returns:
            Updated InFlightAsset
        """
        return self.update_status(transfer_id, InFlightStatus.REFUNDED)

    def remove_transfer(self, transfer_id: str) -> bool:
        """Remove a transfer from tracking.

        Only removes terminal transfers. Active transfers cannot be removed.

        Args:
            transfer_id: Transfer to remove

        Returns:
            True if removed, False if not found or still active.
        """
        asset = self._transfers.get(transfer_id)
        if not asset:
            return False

        if asset.is_active:
            logger.warning(f"Cannot remove active transfer {transfer_id} (status={asset.status.value})")
            return False

        del self._transfers[transfer_id]
        logger.debug(f"Removed transfer {transfer_id}")
        return True

    def get_stale_transfers(self) -> list[InFlightAsset]:
        """Get transfers that have exceeded expected completion time.

        Returns:
            List of overdue active transfers.
        """
        return [t for t in self._transfers.values() if t.is_active and t.is_overdue]

    def expire_old_transfers(self) -> list[InFlightAsset]:
        """Auto-expire transfers older than configured threshold.

        Returns:
            List of newly expired transfers.
        """
        cutoff = datetime.now(UTC) - timedelta(hours=self._config.auto_expire_hours)
        expired: list[InFlightAsset] = []

        for asset in self._transfers.values():
            if asset.is_active and asset.initiated_at < cutoff:
                self.mark_expired(asset.transfer_id)
                expired.append(asset)
                logger.warning(f"Auto-expired stale transfer {asset.transfer_id} (age: {asset.elapsed_time})")

        return expired

    def cleanup_terminal_transfers(self, max_age_hours: int = 168) -> int:
        """Remove old terminal transfers to prevent memory growth.

        Args:
            max_age_hours: Remove terminal transfers older than this (default 7 days)

        Returns:
            Number of transfers removed.
        """
        cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
        to_remove: list[str] = []

        for transfer_id, asset in self._transfers.items():
            if asset.is_terminal and asset.updated_at < cutoff:
                to_remove.append(transfer_id)

        for transfer_id in to_remove:
            del self._transfers[transfer_id]

        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old terminal transfers")

        return len(to_remove)

    def get_summary(self) -> InFlightSummary:
        """Get a summary of in-flight exposure for risk assessment.

        This is used by RiskGuard to validate new bridge transfers.

        Returns:
            InFlightSummary with current exposure metrics.
        """
        active = self.in_flight_assets

        # Calculate per-chain exposure
        per_chain: dict[str, Decimal] = {}
        for asset in active:
            per_chain[asset.from_chain] = per_chain.get(asset.from_chain, Decimal("0")) + asset.amount_usd

        # Calculate per-bridge exposure
        per_bridge: dict[str, Decimal] = {}
        for asset in active:
            per_bridge[asset.bridge] = per_bridge.get(asset.bridge, Decimal("0")) + asset.amount_usd

        # Find oldest transfer
        oldest_age: timedelta | None = None
        if active:
            oldest = min(active, key=lambda a: a.initiated_at)
            oldest_age = oldest.elapsed_time

        # Count stale transfers
        stale_count = len(self.get_stale_transfers())

        return InFlightSummary(
            total_in_flight_usd=self.total_in_flight_usd,
            active_transfer_count=len(active),
            per_chain_in_flight_usd=per_chain,
            per_bridge_in_flight_usd=per_bridge,
            oldest_transfer_age=oldest_age,
            stale_transfer_count=stale_count,
        )

    def get_transfers_from_chain(self, chain: str) -> list[InFlightAsset]:
        """Get all active transfers originating from a chain.

        Args:
            chain: Source chain name

        Returns:
            List of active transfers from this chain.
        """
        return [t for t in self._transfers.values() if t.is_active and t.from_chain == chain]

    def get_transfers_to_chain(self, chain: str) -> list[InFlightAsset]:
        """Get all active transfers destined for a chain.

        Args:
            chain: Destination chain name

        Returns:
            List of active transfers to this chain.
        """
        return [t for t in self._transfers.values() if t.is_active and t.to_chain == chain]

    def get_transfers_via_bridge(self, bridge: str) -> list[InFlightAsset]:
        """Get all active transfers through a specific bridge.

        Args:
            bridge: Bridge protocol name

        Returns:
            List of active transfers through this bridge.
        """
        return [t for t in self._transfers.values() if t.is_active and t.bridge == bridge]

    @staticmethod
    def generate_transfer_id() -> str:
        """Generate a unique transfer ID.

        Returns:
            UUID-based transfer identifier.
        """
        return f"ift-{uuid.uuid4().hex[:12]}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize tracker state to dictionary.

        Returns:
            Dictionary containing all tracker state.
        """
        return {
            "chains": self._chains,
            "config": self._config.to_dict(),
            "transfers": {tid: asset.to_dict() for tid, asset in self._transfers.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InFlightExposureTracker":
        """Deserialize tracker from dictionary.

        Args:
            data: Dictionary from to_dict()

        Returns:
            InFlightExposureTracker with restored state.
        """
        chains = data.get("chains", [])
        config_data = data.get("config", {})
        config = InFlightExposureConfig.from_dict(config_data)

        tracker = cls(chains=chains, config=config)

        transfers_data = data.get("transfers", {})
        for tid, asset_data in transfers_data.items():
            try:
                asset = InFlightAsset.from_dict(asset_data)
                tracker._transfers[tid] = asset
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to deserialize transfer {tid}: {e}")

        return tracker

    def __repr__(self) -> str:
        """String representation of tracker."""
        active_count = len(self.in_flight_assets)
        return (
            f"InFlightExposureTracker("
            f"chains={self._chains}, "
            f"active_transfers={active_count}, "
            f"total_in_flight_usd={self.total_in_flight_usd})"
        )


__all__ = [
    "InFlightStatus",
    "InFlightAsset",
    "InFlightExposureConfig",
    "InFlightSummary",
    "InFlightExposureTracker",
    "InFlightExposureError",
    "TransferNotFoundError",
    "InFlightLimitExceededError",
]
