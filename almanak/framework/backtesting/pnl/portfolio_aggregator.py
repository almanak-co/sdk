"""Portfolio aggregator for unified multi-protocol position tracking.

This module provides the PortfolioAggregator class which aggregates positions
across multiple protocols (DEX LP, lending, perps) into a unified view for
risk analysis and exposure calculation.

Classes:
    - PortfolioSnapshot: Point-in-time snapshot of portfolio state
    - UnifiedRiskScore: Result of unified risk assessment
    - CascadeRiskResult: Result of cascade risk analysis
    - CascadeRiskWarning: Warning for elevated cascade risk
    - PortfolioAggregator: Unified position tracking across protocols
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)

logger = logging.getLogger(__name__)


@dataclass
class UnifiedRiskScore:
    """Result of unified risk assessment across all positions.

    This dataclass captures the overall risk profile of a multi-protocol
    portfolio, combining health factors, leverage, and liquidation risk.

    Attributes:
        score: Overall risk score from 0 (no risk) to 1 (critical risk)
        min_health_factor: Minimum health factor across lending positions (None if no lending)
        max_leverage: Maximum effective leverage across perp protocols
        avg_leverage: Average leverage weighted by notional size
        positions_at_risk: Number of positions with elevated risk
        liquidation_risk_usd: Estimated USD at risk of liquidation
        risk_factors: Dict of individual risk components and their scores
    """

    score: Decimal
    min_health_factor: Decimal | None
    max_leverage: Decimal
    avg_leverage: Decimal
    positions_at_risk: int
    liquidation_risk_usd: Decimal
    risk_factors: dict[str, Decimal] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "score": str(self.score),
            "min_health_factor": str(self.min_health_factor) if self.min_health_factor is not None else None,
            "max_leverage": str(self.max_leverage),
            "avg_leverage": str(self.avg_leverage),
            "positions_at_risk": self.positions_at_risk,
            "liquidation_risk_usd": str(self.liquidation_risk_usd),
            "risk_factors": {k: str(v) for k, v in self.risk_factors.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UnifiedRiskScore":
        """Deserialize from dictionary."""
        return cls(
            score=Decimal(data["score"]),
            min_health_factor=Decimal(data["min_health_factor"]) if data.get("min_health_factor") else None,
            max_leverage=Decimal(data["max_leverage"]),
            avg_leverage=Decimal(data["avg_leverage"]),
            positions_at_risk=data["positions_at_risk"],
            liquidation_risk_usd=Decimal(data["liquidation_risk_usd"]),
            risk_factors={k: Decimal(v) for k, v in data.get("risk_factors", {}).items()},
        )


@dataclass
class CascadeRiskWarning:
    """Warning generated when cascade risk exceeds threshold.

    Cascade risk warnings indicate situations where liquidation of one position
    could trigger liquidations of other positions due to shared collateral.

    Attributes:
        severity: Warning severity level ("low", "medium", "high", "critical")
        message: Human-readable warning message
        affected_positions: List of position IDs that could be affected
        trigger_position_id: ID of the position whose liquidation would trigger cascade
        estimated_cascade_loss_usd: Estimated loss if cascade occurs
        collateral_at_risk_usd: Total collateral that could be affected
    """

    severity: str
    message: str
    affected_positions: list[str]
    trigger_position_id: str
    estimated_cascade_loss_usd: Decimal
    collateral_at_risk_usd: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "severity": self.severity,
            "message": self.message,
            "affected_positions": self.affected_positions,
            "trigger_position_id": self.trigger_position_id,
            "estimated_cascade_loss_usd": str(self.estimated_cascade_loss_usd),
            "collateral_at_risk_usd": str(self.collateral_at_risk_usd),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CascadeRiskWarning":
        """Deserialize from dictionary."""
        return cls(
            severity=data["severity"],
            message=data["message"],
            affected_positions=data.get("affected_positions", []),
            trigger_position_id=data["trigger_position_id"],
            estimated_cascade_loss_usd=Decimal(data["estimated_cascade_loss_usd"]),
            collateral_at_risk_usd=Decimal(data["collateral_at_risk_usd"]),
        )


@dataclass
class CascadeRiskResult:
    """Result of cascade risk analysis across all positions.

    This dataclass captures the cascade risk analysis for a portfolio,
    identifying situations where liquidation of one position could affect others.

    Cascade risk arises from:
    1. Shared collateral: Multiple positions using the same collateral pool
    2. Protocol correlation: Positions on the same protocol share risk
    3. Asset correlation: Positions with correlated assets move together
    4. Cross-margin: Perp positions where loss reduces available margin

    Attributes:
        risk_score: Overall cascade risk score from 0 (no risk) to 1 (critical)
        positions_with_shared_collateral: Count of positions sharing collateral
        cascade_chains: List of position chains that could cascade
        max_cascade_depth: Maximum depth of cascade chains
        total_collateral_at_risk_usd: Total collateral in shared pools
        estimated_cascade_loss_usd: Estimated loss if worst cascade occurs
        warnings: List of cascade risk warnings
        protocol_correlations: Risk score per protocol pair
    """

    risk_score: Decimal
    positions_with_shared_collateral: int
    cascade_chains: list[list[str]]
    max_cascade_depth: int
    total_collateral_at_risk_usd: Decimal
    estimated_cascade_loss_usd: Decimal
    warnings: list[CascadeRiskWarning] = field(default_factory=list)
    protocol_correlations: dict[str, Decimal] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "risk_score": str(self.risk_score),
            "positions_with_shared_collateral": self.positions_with_shared_collateral,
            "cascade_chains": self.cascade_chains,
            "max_cascade_depth": self.max_cascade_depth,
            "total_collateral_at_risk_usd": str(self.total_collateral_at_risk_usd),
            "estimated_cascade_loss_usd": str(self.estimated_cascade_loss_usd),
            "warnings": [w.to_dict() for w in self.warnings],
            "protocol_correlations": {k: str(v) for k, v in self.protocol_correlations.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CascadeRiskResult":
        """Deserialize from dictionary."""
        return cls(
            risk_score=Decimal(data["risk_score"]),
            positions_with_shared_collateral=data["positions_with_shared_collateral"],
            cascade_chains=data.get("cascade_chains", []),
            max_cascade_depth=data["max_cascade_depth"],
            total_collateral_at_risk_usd=Decimal(data["total_collateral_at_risk_usd"]),
            estimated_cascade_loss_usd=Decimal(data["estimated_cascade_loss_usd"]),
            warnings=[CascadeRiskWarning.from_dict(w) for w in data.get("warnings", [])],
            protocol_correlations={k: Decimal(v) for k, v in data.get("protocol_correlations", {}).items()},
        )


@dataclass
class PortfolioSnapshot:
    """Point-in-time snapshot of portfolio state for tick-by-tick export.

    This dataclass captures the complete portfolio state at a specific timestamp,
    enabling JSON export for analysis and debugging.

    Attributes:
        timestamp: When this snapshot was taken
        total_value_usd: Total portfolio value in USD
        positions: List of all positions (serialized)
        net_exposures: Net exposure per asset in units
        net_exposures_usd: Net exposure per asset in USD
        risk_score: Unified risk assessment at this point
        collateral_utilization: Overall collateral utilization ratio
        leverage_by_protocol: Effective leverage per protocol
        total_collateral_usd: Total collateral across all positions
        total_notional_usd: Total notional exposure across perp positions
        metadata: Additional context-specific data
    """

    timestamp: datetime
    total_value_usd: Decimal
    positions: list[dict[str, Any]]
    net_exposures: dict[str, Decimal]
    net_exposures_usd: dict[str, Decimal]
    risk_score: UnifiedRiskScore | None
    collateral_utilization: Decimal
    leverage_by_protocol: dict[str, Decimal]
    total_collateral_usd: Decimal
    total_notional_usd: Decimal
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "total_value_usd": str(self.total_value_usd),
            "positions": self.positions,
            "net_exposures": {k: str(v) for k, v in self.net_exposures.items()},
            "net_exposures_usd": {k: str(v) for k, v in self.net_exposures_usd.items()},
            "risk_score": self.risk_score.to_dict() if self.risk_score else None,
            "collateral_utilization": str(self.collateral_utilization),
            "leverage_by_protocol": {k: str(v) for k, v in self.leverage_by_protocol.items()},
            "total_collateral_usd": str(self.total_collateral_usd),
            "total_notional_usd": str(self.total_notional_usd),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PortfolioSnapshot":
        """Deserialize from dictionary."""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            total_value_usd=Decimal(data["total_value_usd"]),
            positions=data.get("positions", []),
            net_exposures={k: Decimal(v) for k, v in data.get("net_exposures", {}).items()},
            net_exposures_usd={k: Decimal(v) for k, v in data.get("net_exposures_usd", {}).items()},
            risk_score=UnifiedRiskScore.from_dict(data["risk_score"]) if data.get("risk_score") else None,
            collateral_utilization=Decimal(data.get("collateral_utilization", "0")),
            leverage_by_protocol={k: Decimal(v) for k, v in data.get("leverage_by_protocol", {}).items()},
            total_collateral_usd=Decimal(data.get("total_collateral_usd", "0")),
            total_notional_usd=Decimal(data.get("total_notional_usd", "0")),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PortfolioAggregator:
    """Unified position tracking across multiple protocols.

    This class aggregates positions from different protocols (Uniswap V3, Aave,
    GMX, etc.) into a unified view, enabling cross-protocol risk analysis and
    net exposure calculation.

    Attributes:
        positions: List of all tracked positions across protocols
        _positions_by_id: Internal dict for O(1) position lookup by ID
        _positions_by_protocol: Internal dict grouping positions by protocol
        _positions_by_type: Internal dict grouping positions by position type

    Example:
        aggregator = PortfolioAggregator()

        # Add positions from different protocols
        aggregator.add_position(lp_position)
        aggregator.add_position(perp_position)
        aggregator.add_position(lending_position)

        # Get all positions
        all_positions = aggregator.get_positions()

        # Filter by protocol
        aave_positions = aggregator.get_positions(protocol="aave_v3")

        # Filter by type
        lp_positions = aggregator.get_positions(position_type=PositionType.LP)
    """

    positions: list[SimulatedPosition] = field(default_factory=list)
    _positions_by_id: dict[str, SimulatedPosition] = field(default_factory=dict, repr=False)
    _positions_by_protocol: dict[str, list[SimulatedPosition]] = field(default_factory=dict, repr=False)
    _positions_by_type: dict[PositionType, list[SimulatedPosition]] = field(default_factory=dict, repr=False)

    def add_position(self, position: SimulatedPosition) -> None:
        """Add a position to the aggregator.

        Adds the position to the main list and updates all index dicts
        for efficient lookup.

        Args:
            position: The SimulatedPosition to add

        Raises:
            ValueError: If a position with the same ID already exists
        """
        if position.position_id in self._positions_by_id:
            raise ValueError(
                f"Position with ID '{position.position_id}' already exists. "
                "Use update_position() to modify existing positions."
            )

        # Add to main list
        self.positions.append(position)

        # Index by ID
        self._positions_by_id[position.position_id] = position

        # Index by protocol
        if position.protocol not in self._positions_by_protocol:
            self._positions_by_protocol[position.protocol] = []
        self._positions_by_protocol[position.protocol].append(position)

        # Index by type
        if position.position_type not in self._positions_by_type:
            self._positions_by_type[position.position_type] = []
        self._positions_by_type[position.position_type].append(position)

    def add_positions(self, positions: list[SimulatedPosition]) -> None:
        """Add multiple positions to the aggregator.

        Args:
            positions: List of SimulatedPositions to add
        """
        for position in positions:
            self.add_position(position)

    def get_position(self, position_id: str) -> SimulatedPosition | None:
        """Get a position by its ID.

        Args:
            position_id: The unique identifier of the position

        Returns:
            The position if found, None otherwise
        """
        return self._positions_by_id.get(position_id)

    def get_positions(
        self,
        protocol: str | None = None,
        position_type: PositionType | None = None,
        token: str | None = None,
    ) -> list[SimulatedPosition]:
        """Get positions with optional filtering.

        Filters are ANDed together - all specified filters must match.

        Args:
            protocol: Filter by protocol name (e.g., "uniswap_v3", "aave_v3")
            position_type: Filter by position type (e.g., PositionType.LP)
            token: Filter by token symbol (positions containing this token)

        Returns:
            List of positions matching all specified filters
        """
        # Start with the most restrictive filter for efficiency
        if protocol is not None and position_type is None and token is None:
            return list(self._positions_by_protocol.get(protocol, []))

        if position_type is not None and protocol is None and token is None:
            return list(self._positions_by_type.get(position_type, []))

        # For combined filters, start with smallest candidate set
        if protocol is not None:
            candidates = self._positions_by_protocol.get(protocol, [])
        elif position_type is not None:
            candidates = self._positions_by_type.get(position_type, [])
        else:
            candidates = self.positions

        result = []
        for pos in candidates:
            # Check protocol filter
            if protocol is not None and pos.protocol != protocol:
                continue
            # Check type filter
            if position_type is not None and pos.position_type != position_type:
                continue
            # Check token filter
            if token is not None and token not in pos.tokens:
                continue
            result.append(pos)

        return result

    def remove_position(self, position_id: str) -> SimulatedPosition | None:
        """Remove a position by its ID.

        Args:
            position_id: The unique identifier of the position to remove

        Returns:
            The removed position if found, None otherwise
        """
        position = self._positions_by_id.pop(position_id, None)
        if position is None:
            return None

        # Remove from main list
        self.positions.remove(position)

        # Remove from protocol index
        protocol_list = self._positions_by_protocol.get(position.protocol, [])
        if position in protocol_list:
            protocol_list.remove(position)
            if not protocol_list:
                del self._positions_by_protocol[position.protocol]

        # Remove from type index
        type_list = self._positions_by_type.get(position.position_type, [])
        if position in type_list:
            type_list.remove(position)
            if not type_list:
                del self._positions_by_type[position.position_type]

        return position

    def update_position(self, position: SimulatedPosition) -> None:
        """Update an existing position.

        Removes the old position (if exists) and adds the updated one.

        Args:
            position: The updated position (must have same position_id)
        """
        # Remove old version if exists
        self.remove_position(position.position_id)
        # Add updated version (use internal method to avoid duplicate check)
        self.positions.append(position)
        self._positions_by_id[position.position_id] = position

        if position.protocol not in self._positions_by_protocol:
            self._positions_by_protocol[position.protocol] = []
        self._positions_by_protocol[position.protocol].append(position)

        if position.position_type not in self._positions_by_type:
            self._positions_by_type[position.position_type] = []
        self._positions_by_type[position.position_type].append(position)

    def clear(self) -> None:
        """Remove all positions from the aggregator."""
        self.positions.clear()
        self._positions_by_id.clear()
        self._positions_by_protocol.clear()
        self._positions_by_type.clear()

    @property
    def position_count(self) -> int:
        """Get the total number of positions."""
        return len(self.positions)

    @property
    def protocols(self) -> list[str]:
        """Get list of all protocols with positions."""
        return list(self._positions_by_protocol.keys())

    @property
    def position_types(self) -> list[PositionType]:
        """Get list of all position types present."""
        return list(self._positions_by_type.keys())

    def get_protocol_counts(self) -> dict[str, int]:
        """Get count of positions per protocol.

        Returns:
            Dict mapping protocol name to position count
        """
        return {protocol: len(positions) for protocol, positions in self._positions_by_protocol.items()}

    def get_type_counts(self) -> dict[PositionType, int]:
        """Get count of positions per type.

        Returns:
            Dict mapping position type to position count
        """
        return {pos_type: len(positions) for pos_type, positions in self._positions_by_type.items()}

    def to_dict(self) -> dict[str, Any]:
        """Serialize the aggregator state to a dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        return {
            "positions": [pos.to_dict() for pos in self.positions],
            "position_count": self.position_count,
            "protocols": self.protocols,
            "position_types": [pt.value for pt in self.position_types],
            "protocol_counts": self.get_protocol_counts(),
            "type_counts": {pt.value: count for pt, count in self.get_type_counts().items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PortfolioAggregator":
        """Create a PortfolioAggregator from a dictionary.

        Args:
            data: Dictionary with serialized aggregator data

        Returns:
            New PortfolioAggregator instance with positions loaded
        """
        aggregator = cls()
        for pos_data in data.get("positions", []):
            position = SimulatedPosition.from_dict(pos_data)
            aggregator.add_position(position)
        return aggregator

    @classmethod
    def from_positions(cls, positions: list[SimulatedPosition]) -> "PortfolioAggregator":
        """Create a PortfolioAggregator from a list of positions.

        Args:
            positions: List of positions to add

        Returns:
            New PortfolioAggregator instance with positions added
        """
        aggregator = cls()
        aggregator.add_positions(positions)
        return aggregator

    def calculate_net_exposure(self, asset: str, prices: dict[str, Decimal] | None = None) -> Decimal:
        """Calculate net exposure for a specific asset across all positions.

        Net exposure is calculated as:
        - Long positions (SPOT, PERP_LONG, SUPPLY): add to exposure
        - Short positions (PERP_SHORT, BORROW): subtract from exposure

        For PERP positions, the exposure is based on notional_usd divided by price
        (or the collateral_usd * leverage if notional is not set).

        Args:
            asset: Token symbol to calculate exposure for (e.g., "ETH", "BTC")
            prices: Optional dict of token -> USD price for converting perp notional
                   to asset units. If not provided, perp positions use notional_usd directly.

        Returns:
            Net exposure in asset units. Positive = net long, negative = net short.
            For perp positions without prices, returns exposure in USD terms.
        """
        net_exposure = Decimal("0")

        for position in self.positions:
            if position.position_type == PositionType.SPOT:
                # Spot: direct holding
                net_exposure += position.get_amount(asset)

            elif position.position_type == PositionType.SUPPLY:
                # Supply: lending deposits count as long exposure
                net_exposure += position.get_amount(asset)

            elif position.position_type == PositionType.BORROW:
                # Borrow: borrowed amounts count as short exposure
                net_exposure -= position.get_amount(asset)

            elif position.position_type == PositionType.PERP_LONG:
                # Perp long: convert notional to asset units if price available
                if asset in position.tokens:
                    if position.notional_usd > Decimal("0"):
                        if prices and asset in prices and prices[asset] > Decimal("0"):
                            # Convert USD notional to asset units
                            net_exposure += position.notional_usd / prices[asset]
                        else:
                            # Fall back to using amounts if available
                            net_exposure += position.get_amount(asset)
                    else:
                        net_exposure += position.get_amount(asset)

            elif position.position_type == PositionType.PERP_SHORT:
                # Perp short: negative exposure
                if asset in position.tokens:
                    if position.notional_usd > Decimal("0"):
                        if prices and asset in prices and prices[asset] > Decimal("0"):
                            # Convert USD notional to asset units (negative for short)
                            net_exposure -= position.notional_usd / prices[asset]
                        else:
                            # Fall back to using amounts if available
                            net_exposure -= position.get_amount(asset)
                    else:
                        net_exposure -= position.get_amount(asset)

            elif position.position_type == PositionType.LP:
                # LP: Both tokens in the pair contribute to exposure
                # LP positions are considered neutral to slightly long
                # as they hold both tokens
                net_exposure += position.get_amount(asset)

        return net_exposure

    def calculate_net_exposure_usd(self, asset: str, prices: dict[str, Decimal]) -> Decimal:
        """Calculate net exposure in USD terms for a specific asset.

        This is a convenience method that multiplies the asset exposure by its price.

        Args:
            asset: Token symbol to calculate exposure for
            prices: Dict of token -> USD price

        Returns:
            Net exposure in USD terms
        """
        exposure = self.calculate_net_exposure(asset, prices)
        price = prices.get(asset, Decimal("0"))
        return exposure * price

    def get_all_assets(self) -> set[str]:
        """Get all unique assets across all positions.

        Returns:
            Set of all token symbols held in any position
        """
        assets: set[str] = set()
        for position in self.positions:
            assets.update(position.tokens)
        return assets

    def calculate_all_net_exposures(self, prices: dict[str, Decimal] | None = None) -> dict[str, Decimal]:
        """Calculate net exposure for all assets in the portfolio.

        Args:
            prices: Optional dict of token -> USD price for perp position conversion

        Returns:
            Dict mapping asset symbol to net exposure in asset units
        """
        exposures: dict[str, Decimal] = {}
        for asset in self.get_all_assets():
            exposures[asset] = self.calculate_net_exposure(asset, prices)
        return exposures

    def calculate_collateral_utilization(self) -> Decimal:
        """Calculate the overall collateral utilization ratio.

        Collateral utilization measures how much of the available collateral
        is being used to back leveraged positions. It's calculated as:

        utilization = total_notional_exposure / total_collateral

        Where:
        - total_notional_exposure: Sum of all perp notional values
        - total_collateral: Sum of all perp collateral + supply positions

        Returns:
            Collateral utilization as a decimal (e.g., 0.75 = 75% utilized).
            Returns Decimal("0") if there's no collateral.
            Returns Decimal("999") if there's exposure but no collateral (over-leveraged).
        """
        total_collateral = Decimal("0")
        total_notional = Decimal("0")

        for position in self.positions:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                # Perp positions contribute notional and collateral
                total_notional += position.notional_usd
                total_collateral += position.collateral_usd

            elif position.position_type == PositionType.SUPPLY:
                # Supply positions can be counted as available collateral
                # Note: In practice, only certain tokens may be collateral-eligible
                # This is a simplified model
                total_collateral += position.total_amount * position.entry_price

        # Handle edge cases
        if total_collateral <= Decimal("0"):
            if total_notional > Decimal("0"):
                # Exposure without collateral is dangerous
                return Decimal("999")
            return Decimal("0")

        return total_notional / total_collateral

    def calculate_leverage_by_protocol(self) -> dict[str, Decimal]:
        """Calculate effective leverage per protocol.

        For each protocol with perp positions, calculates:
        leverage = total_notional / total_collateral

        Returns:
            Dict mapping protocol name to effective leverage ratio
        """
        protocol_notional: dict[str, Decimal] = {}
        protocol_collateral: dict[str, Decimal] = {}

        for position in self.positions:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                protocol = position.protocol
                if protocol not in protocol_notional:
                    protocol_notional[protocol] = Decimal("0")
                    protocol_collateral[protocol] = Decimal("0")

                protocol_notional[protocol] += position.notional_usd
                protocol_collateral[protocol] += position.collateral_usd

        result: dict[str, Decimal] = {}
        for protocol in protocol_notional:
            collateral = protocol_collateral.get(protocol, Decimal("0"))
            if collateral > Decimal("0"):
                result[protocol] = protocol_notional[protocol] / collateral
            else:
                result[protocol] = Decimal("0")

        return result

    def get_total_collateral_usd(self) -> Decimal:
        """Get total collateral across all positions in USD.

        Returns:
            Total collateral value in USD
        """
        total = Decimal("0")
        for position in self.positions:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                total += position.collateral_usd
            elif position.position_type == PositionType.SUPPLY:
                # Supply positions at entry price
                total += position.total_amount * position.entry_price
        return total

    def get_total_notional_usd(self) -> Decimal:
        """Get total notional exposure across all perp positions in USD.

        Returns:
            Total notional value in USD
        """
        total = Decimal("0")
        for position in self.positions:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                total += position.notional_usd
        return total

    def calculate_unified_risk_score(
        self,
        prices: dict[str, Decimal] | None = None,
        health_factor_warning_threshold: Decimal = Decimal("1.5"),
        leverage_warning_threshold: Decimal = Decimal("5"),
        liquidation_proximity_threshold: Decimal = Decimal("0.1"),
    ) -> UnifiedRiskScore:
        """Calculate a unified risk score across all leveraged positions.

        The unified risk score combines multiple risk factors:
        1. Health factor risk (lending positions with low health factor)
        2. Leverage risk (perp positions with high leverage)
        3. Liquidation proximity risk (positions near liquidation price)
        4. Concentration risk (large positions relative to portfolio)

        The final score is a weighted average normalized to 0-1 range where:
        - 0.0 = No risk (no leveraged positions or very safe)
        - 0.0-0.3 = Low risk
        - 0.3-0.6 = Moderate risk
        - 0.6-0.8 = High risk
        - 0.8-1.0 = Critical risk

        Args:
            prices: Optional dict of token -> USD price for exposure calculations
            health_factor_warning_threshold: Health factor below which position is at risk (default 1.5)
            leverage_warning_threshold: Leverage above which position is at risk (default 5x)
            liquidation_proximity_threshold: % distance to liquidation below which is risky (default 10%)

        Returns:
            UnifiedRiskScore with overall score and individual risk components
        """
        risk_factors: dict[str, Decimal] = {}
        positions_at_risk = 0
        liquidation_risk_usd = Decimal("0")

        # Track health factors for lending positions
        min_health_factor: Decimal | None = None
        health_factor_risk = Decimal("0")
        lending_positions = self.get_positions(position_type=PositionType.BORROW)

        if lending_positions:
            health_factors = []
            for pos in lending_positions:
                if pos.health_factor is not None:
                    health_factors.append(pos.health_factor)
                    if pos.health_factor < health_factor_warning_threshold:
                        positions_at_risk += 1
                        # Estimate liquidation risk based on debt size
                        debt_usd = pos.total_amount * pos.entry_price
                        liquidation_risk_usd += debt_usd

            if health_factors:
                min_health_factor = min(health_factors)
                # Health factor risk: 0 at HF=2+, 1 at HF<=1
                if min_health_factor >= Decimal("2"):
                    health_factor_risk = Decimal("0")
                elif min_health_factor <= Decimal("1"):
                    health_factor_risk = Decimal("1")
                else:
                    # Linear interpolation between 1 and 2
                    health_factor_risk = Decimal("2") - min_health_factor

        risk_factors["health_factor_risk"] = health_factor_risk

        # Track leverage for perp positions
        leverage_values: list[tuple[Decimal, Decimal]] = []  # (leverage, notional)
        perp_positions = self.get_positions(position_type=PositionType.PERP_LONG) + self.get_positions(
            position_type=PositionType.PERP_SHORT
        )

        for pos in perp_positions:
            if pos.collateral_usd > Decimal("0"):
                effective_leverage = pos.notional_usd / pos.collateral_usd
                leverage_values.append((effective_leverage, pos.notional_usd))

                if effective_leverage > leverage_warning_threshold:
                    positions_at_risk += 1

                # Check liquidation proximity
                if pos.liquidation_price is not None and prices:
                    token = pos.primary_token
                    if token in prices and prices[token] > Decimal("0"):
                        current_price = prices[token]
                        if pos.position_type == PositionType.PERP_LONG:
                            # Long: at risk if price approaches liquidation from above
                            distance = (current_price - pos.liquidation_price) / current_price
                        else:
                            # Short: at risk if price approaches liquidation from below
                            distance = (pos.liquidation_price - current_price) / current_price

                        if distance < liquidation_proximity_threshold:
                            liquidation_risk_usd += pos.notional_usd

        # Calculate max and weighted average leverage
        max_leverage = Decimal("0")
        avg_leverage = Decimal("0")
        total_notional = Decimal("0")

        for leverage, notional in leverage_values:
            if leverage > max_leverage:
                max_leverage = leverage
            avg_leverage += leverage * notional
            total_notional += notional

        if total_notional > Decimal("0"):
            avg_leverage = avg_leverage / total_notional
        else:
            avg_leverage = Decimal("0")

        # Leverage risk: 0 at 1x, 0.5 at 5x, 1.0 at 10x+
        if max_leverage <= Decimal("1"):
            leverage_risk = Decimal("0")
        elif max_leverage >= Decimal("10"):
            leverage_risk = Decimal("1")
        else:
            leverage_risk = (max_leverage - Decimal("1")) / Decimal("9")

        risk_factors["leverage_risk"] = leverage_risk

        # Liquidation proximity risk (based on USD at risk)
        total_value = self.get_total_collateral_usd() + self.get_total_notional_usd()
        if total_value > Decimal("0"):
            liquidation_proximity_risk = min(liquidation_risk_usd / total_value, Decimal("1"))
        else:
            liquidation_proximity_risk = Decimal("0")

        risk_factors["liquidation_proximity_risk"] = liquidation_proximity_risk

        # Concentration risk (collateral utilization)
        collateral_util = self.calculate_collateral_utilization()
        if collateral_util >= Decimal("999"):  # Over-leveraged
            concentration_risk = Decimal("1")
        elif collateral_util >= Decimal("1"):  # Fully utilized
            concentration_risk = Decimal("0.8")
        elif collateral_util >= Decimal("0.8"):  # High utilization
            concentration_risk = Decimal("0.5") + (collateral_util - Decimal("0.8")) * Decimal("1.5")
        else:
            concentration_risk = collateral_util * Decimal("0.625")  # Scale 0-0.8 to 0-0.5

        risk_factors["concentration_risk"] = concentration_risk

        # Calculate weighted overall score
        # Weights: health_factor=0.35, leverage=0.25, liquidation_proximity=0.25, concentration=0.15
        overall_score = (
            health_factor_risk * Decimal("0.35")
            + leverage_risk * Decimal("0.25")
            + liquidation_proximity_risk * Decimal("0.25")
            + concentration_risk * Decimal("0.15")
        )

        # Ensure score is bounded 0-1
        overall_score = max(Decimal("0"), min(Decimal("1"), overall_score))

        return UnifiedRiskScore(
            score=overall_score,
            min_health_factor=min_health_factor,
            max_leverage=max_leverage,
            avg_leverage=avg_leverage,
            positions_at_risk=positions_at_risk,
            liquidation_risk_usd=liquidation_risk_usd,
            risk_factors=risk_factors,
        )

    def create_snapshot(
        self,
        timestamp: datetime,
        total_value_usd: Decimal,
        prices: dict[str, Decimal] | None = None,
        include_risk_score: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> PortfolioSnapshot:
        """Create a point-in-time snapshot of the portfolio state.

        This method captures the complete portfolio state at a specific timestamp,
        suitable for JSON export and tick-by-tick analysis.

        Args:
            timestamp: When this snapshot is being taken
            total_value_usd: Total portfolio value in USD at this timestamp
            prices: Optional dict of token -> USD price for exposure calculations
            include_risk_score: Whether to calculate and include risk score (default True)
            metadata: Additional context-specific data to include

        Returns:
            PortfolioSnapshot with complete portfolio state
        """
        prices = prices or {}

        # Calculate exposures
        net_exposures = self.calculate_all_net_exposures(prices)
        net_exposures_usd: dict[str, Decimal] = {}
        for asset, exposure in net_exposures.items():
            price = prices.get(asset, Decimal("0"))
            net_exposures_usd[asset] = exposure * price

        # Calculate risk score if requested
        risk_score = None
        if include_risk_score:
            risk_score = self.calculate_unified_risk_score(prices)

        return PortfolioSnapshot(
            timestamp=timestamp,
            total_value_usd=total_value_usd,
            positions=[pos.to_dict() for pos in self.positions],
            net_exposures=net_exposures,
            net_exposures_usd=net_exposures_usd,
            risk_score=risk_score,
            collateral_utilization=self.calculate_collateral_utilization(),
            leverage_by_protocol=self.calculate_leverage_by_protocol(),
            total_collateral_usd=self.get_total_collateral_usd(),
            total_notional_usd=self.get_total_notional_usd(),
            metadata=metadata or {},
        )

    def calculate_total_leverage(self, prices: dict[str, Decimal] | None = None) -> Decimal:
        """Calculate total portfolio leverage ratio.

        Total leverage is the ratio of total notional exposure to total equity,
        aggregated across all leveraged position types:
        - Perp positions: notional_usd / collateral_usd (direct leverage)
        - Lending positions: borrowed_usd / supplied_usd (effective leverage)
        - LP positions: 2x exposure due to holding both tokens (implicit leverage = 1)

        The formula is:
            total_leverage = total_notional_exposure / total_equity

        Where:
        - total_notional_exposure = sum of all position notional values
        - total_equity = sum of all collateral + supply positions - borrow positions

        Args:
            prices: Optional dict of token -> USD price for position valuation

        Returns:
            Total leverage ratio as Decimal. Returns Decimal("1") if no leverage
            (spot only portfolio). Returns Decimal("0") if no positions.
        """
        if not self.positions:
            return Decimal("0")

        total_notional = Decimal("0")
        total_equity = Decimal("0")

        for position in self.positions:
            position_value = self._get_position_value_usd(position, prices)

            if position.position_type == PositionType.SPOT:
                # Spot positions: no leverage, counts toward equity
                total_notional += position_value
                total_equity += position_value

            elif position.position_type == PositionType.SUPPLY:
                # Supply positions: counts as collateral/equity
                total_notional += position_value
                total_equity += position_value

            elif position.position_type == PositionType.BORROW:
                # Borrow positions: increases exposure, reduces equity
                total_notional += position_value
                total_equity -= position_value  # Debt reduces equity

            elif position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                # Perp positions: notional exposure with collateral
                total_notional += position.notional_usd
                total_equity += position.collateral_usd

            elif position.position_type == PositionType.LP:
                # LP positions: full position value as exposure
                total_notional += position_value
                total_equity += position_value

        # Handle edge cases
        if total_equity <= Decimal("0"):
            if total_notional > Decimal("0"):
                # Exposure without equity (e.g., fully borrowed) = max leverage
                return Decimal("999")
            return Decimal("0")

        return total_notional / total_equity

    def _get_position_value_usd(self, position: SimulatedPosition, prices: dict[str, Decimal] | None = None) -> Decimal:
        """Get the USD value of a position.

        Args:
            position: The position to value
            prices: Optional dict of token -> USD price

        Returns:
            Position value in USD
        """
        prices = prices or {}
        total_value = Decimal("0")

        for token in position.tokens:
            amount = position.get_amount(token)
            # Use provided price if available, otherwise fall back to entry_price
            price = prices.get(token, position.entry_price)
            total_value += amount * price

        # Fall back to entry price calculation if no specific prices available
        if total_value == Decimal("0"):
            total_value = position.total_amount * position.entry_price

        return total_value

    def calculate_net_delta(self, asset: str, prices: dict[str, Decimal] | None = None) -> Decimal:
        """Calculate net delta exposure for a specific asset.

        Delta measures directional exposure to an asset's price movement:
        - Positive delta: profit when asset price increases
        - Negative delta: profit when asset price decreases

        Delta is calculated by summing directional exposures across all positions:
        - SPOT: +1 delta per unit held
        - SUPPLY: +1 delta per unit supplied (benefit from appreciation)
        - BORROW: -1 delta per unit borrowed (debt grows with price)
        - PERP_LONG: +1 delta per unit of notional exposure
        - PERP_SHORT: -1 delta per unit of notional exposure
        - LP: ~0.5 delta per token (impermanent loss reduces effective delta)

        This method calculates delta in asset units. To get USD delta,
        multiply by the asset price.

        Args:
            asset: Token symbol to calculate delta for (e.g., "ETH", "BTC")
            prices: Optional dict of token -> USD price for perp position conversion

        Returns:
            Net delta in asset units. Positive = net long, negative = net short.
        """
        net_delta = Decimal("0")

        for position in self.positions:
            if position.position_type == PositionType.SPOT:
                # Spot: full delta
                net_delta += position.get_amount(asset)

            elif position.position_type == PositionType.SUPPLY:
                # Supply: full positive delta (benefit from appreciation)
                net_delta += position.get_amount(asset)

            elif position.position_type == PositionType.BORROW:
                # Borrow: negative delta (debt grows with price in fiat terms)
                net_delta -= position.get_amount(asset)

            elif position.position_type == PositionType.PERP_LONG:
                # Perp long: full positive delta
                if asset in position.tokens:
                    if position.notional_usd > Decimal("0"):
                        if prices and asset in prices and prices[asset] > Decimal("0"):
                            # Convert USD notional to asset units
                            net_delta += position.notional_usd / prices[asset]
                        else:
                            net_delta += position.get_amount(asset)
                    else:
                        net_delta += position.get_amount(asset)

            elif position.position_type == PositionType.PERP_SHORT:
                # Perp short: full negative delta
                if asset in position.tokens:
                    if position.notional_usd > Decimal("0"):
                        if prices and asset in prices and prices[asset] > Decimal("0"):
                            # Convert USD notional to asset units (negative for short)
                            net_delta -= position.notional_usd / prices[asset]
                        else:
                            net_delta -= position.get_amount(asset)
                    else:
                        net_delta -= position.get_amount(asset)

            elif position.position_type == PositionType.LP:
                # LP: reduced delta due to impermanent loss effect
                # IL causes LP positions to have ~0.5x delta exposure
                # as the position auto-rebalances against price movements
                lp_delta_multiplier = Decimal("0.5")
                net_delta += position.get_amount(asset) * lp_delta_multiplier

        return net_delta

    def calculate_all_net_deltas(self, prices: dict[str, Decimal] | None = None) -> dict[str, Decimal]:
        """Calculate net delta for all assets in the portfolio.

        Args:
            prices: Optional dict of token -> USD price for perp position conversion

        Returns:
            Dict mapping asset symbol to net delta in asset units
        """
        deltas: dict[str, Decimal] = {}
        for asset in self.get_all_assets():
            deltas[asset] = self.calculate_net_delta(asset, prices)
        return deltas

    def calculate_net_delta_usd(self, asset: str, prices: dict[str, Decimal]) -> Decimal:
        """Calculate net delta in USD terms for a specific asset.

        This is a convenience method that multiplies the asset delta by its price.

        Args:
            asset: Token symbol to calculate delta for
            prices: Dict of token -> USD price

        Returns:
            Net delta in USD terms
        """
        delta = self.calculate_net_delta(asset, prices)
        price = prices.get(asset, Decimal("0"))
        return delta * price

    def calculate_cascade_risk(
        self,
        prices: dict[str, Decimal] | None = None,
        cascade_threshold: Decimal = Decimal("0.3"),
        emit_warnings: bool = True,
    ) -> CascadeRiskResult:
        """Calculate liquidation cascade risk for shared collateral positions.

        Cascade risk measures the potential for a liquidation of one position
        to trigger liquidations of other positions. This can occur when:

        1. **Shared Collateral**: Multiple perp positions on the same protocol
           share a collateral pool. Liquidating one reduces available margin.

        2. **Protocol Correlation**: Positions on the same protocol may be
           affected by protocol-level events (e.g., depeg, exploit).

        3. **Asset Correlation**: Positions with the same underlying asset
           move together, amplifying directional risk.

        4. **Cross-Margin Effects**: Loss on one position reduces equity
           available to support other leveraged positions.

        The cascade risk is calculated by:
        1. Grouping positions by shared collateral pools (protocol + wallet)
        2. Identifying positions near liquidation that could trigger cascades
        3. Calculating the chain of affected positions if one liquidates
        4. Scoring based on depth and total value at risk

        Args:
            prices: Optional dict of token -> USD price for position valuation
            cascade_threshold: Threshold above which to emit warnings (default 0.3)
            emit_warnings: Whether to emit warnings for high cascade risk (default True)

        Returns:
            CascadeRiskResult with overall score, cascade chains, and warnings
        """
        prices = prices or {}
        warnings: list[CascadeRiskWarning] = []
        cascade_chains: list[list[str]] = []
        protocol_correlations: dict[str, Decimal] = {}

        # Group positions by protocol (shared collateral pool)
        positions_by_protocol: dict[str, list[SimulatedPosition]] = {}
        for position in self.positions:
            if position.protocol not in positions_by_protocol:
                positions_by_protocol[position.protocol] = []
            positions_by_protocol[position.protocol].append(position)

        # Calculate collateral at risk
        total_collateral_at_risk = Decimal("0")
        positions_with_shared_collateral = 0

        # Analyze each protocol for cascade risk
        for protocol, protocol_positions in positions_by_protocol.items():
            # Count leveraged positions (can cause cascades)
            leveraged_positions = [
                p
                for p in protocol_positions
                if p.position_type
                in (
                    PositionType.PERP_LONG,
                    PositionType.PERP_SHORT,
                    PositionType.BORROW,
                )
            ]

            if len(leveraged_positions) <= 1:
                # No cascade risk with single position
                continue

            # Multiple leveraged positions on same protocol = shared risk
            positions_with_shared_collateral += len(leveraged_positions)

            # Calculate total collateral in this pool
            pool_collateral = sum(
                (p.collateral_usd for p in leveraged_positions if p.collateral_usd > Decimal("0")), Decimal("0")
            )
            total_collateral_at_risk += pool_collateral

            # Find positions at risk of liquidation
            positions_at_risk = self._identify_positions_at_risk(leveraged_positions, prices)

            # Build cascade chains from at-risk positions
            for trigger_pos in positions_at_risk:
                chain = self._build_cascade_chain(trigger_pos, leveraged_positions, prices)
                if len(chain) > 1:
                    cascade_chains.append([p.position_id for p in chain])

                    # Calculate cascade loss
                    cascade_loss = self._estimate_cascade_loss(chain, prices)

                    # Generate warning if chain is significant
                    if cascade_loss > Decimal("0") and emit_warnings:
                        severity = self._get_cascade_severity(len(chain), cascade_loss, pool_collateral)
                        affected_ids = [p.position_id for p in chain if p != trigger_pos]

                        warning = CascadeRiskWarning(
                            severity=severity,
                            message=self._format_cascade_warning(trigger_pos, chain, cascade_loss),
                            affected_positions=affected_ids,
                            trigger_position_id=trigger_pos.position_id,
                            estimated_cascade_loss_usd=cascade_loss,
                            collateral_at_risk_usd=pool_collateral,
                        )
                        warnings.append(warning)

            # Calculate protocol correlation risk
            protocol_risk = self._calculate_protocol_correlation_risk(leveraged_positions)
            if protocol_risk > Decimal("0"):
                protocol_correlations[protocol] = protocol_risk

        # Calculate max cascade depth
        max_cascade_depth = max((len(chain) for chain in cascade_chains), default=0)

        # Estimate worst-case cascade loss
        estimated_cascade_loss = Decimal("0")
        if cascade_chains:
            # Find the chain with highest potential loss
            for chain_ids in cascade_chains:
                chain_positions_maybe = [
                    self._positions_by_id.get(pid) for pid in chain_ids if pid in self._positions_by_id
                ]
                chain_positions_filtered = [p for p in chain_positions_maybe if p is not None]
                loss = self._estimate_cascade_loss(chain_positions_filtered, prices)
                if loss > estimated_cascade_loss:
                    estimated_cascade_loss = loss

        # Calculate overall risk score
        risk_score = self._calculate_cascade_risk_score(
            positions_with_shared_collateral=positions_with_shared_collateral,
            max_cascade_depth=max_cascade_depth,
            total_collateral_at_risk=total_collateral_at_risk,
            estimated_cascade_loss=estimated_cascade_loss,
            protocol_correlations=protocol_correlations,
        )

        # Emit warnings if threshold exceeded
        if emit_warnings and risk_score >= cascade_threshold:
            self._emit_cascade_risk_warnings(risk_score, warnings)

        return CascadeRiskResult(
            risk_score=risk_score,
            positions_with_shared_collateral=positions_with_shared_collateral,
            cascade_chains=cascade_chains,
            max_cascade_depth=max_cascade_depth,
            total_collateral_at_risk_usd=total_collateral_at_risk,
            estimated_cascade_loss_usd=estimated_cascade_loss,
            warnings=warnings,
            protocol_correlations=protocol_correlations,
        )

    def _identify_positions_at_risk(
        self,
        positions: list[SimulatedPosition],
        prices: dict[str, Decimal],
    ) -> list[SimulatedPosition]:
        """Identify positions that are at risk of liquidation.

        A position is considered at risk if:
        - Perp: liquidation_price is within 20% of current price
        - Borrow: health_factor is below 1.3

        Args:
            positions: List of leveraged positions to analyze
            prices: Dict of token -> USD price

        Returns:
            List of positions at risk of liquidation
        """
        at_risk: list[SimulatedPosition] = []

        for position in positions:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                # Check perp liquidation proximity
                if position.liquidation_price is not None:
                    token = position.primary_token
                    if token in prices and prices[token] > Decimal("0"):
                        current_price = prices[token]
                        if position.position_type == PositionType.PERP_LONG:
                            # Long at risk if price within 20% of liquidation
                            distance = (current_price - position.liquidation_price) / current_price
                        else:
                            # Short at risk if price within 20% of liquidation
                            distance = (position.liquidation_price - current_price) / current_price

                        if distance < Decimal("0.2"):
                            at_risk.append(position)

            elif position.position_type == PositionType.BORROW:
                # Check lending health factor
                if position.health_factor is not None and position.health_factor < Decimal("1.3"):
                    at_risk.append(position)

        return at_risk

    def _build_cascade_chain(
        self,
        trigger_position: SimulatedPosition,
        pool_positions: list[SimulatedPosition],
        prices: dict[str, Decimal],
    ) -> list[SimulatedPosition]:
        """Build a chain of positions that would cascade from a trigger liquidation.

        When a position is liquidated:
        1. Its collateral is used to cover the debt/loss
        2. This reduces the total collateral pool available
        3. Other positions may then have insufficient margin
        4. They may also get liquidated, continuing the cascade

        Args:
            trigger_position: The position whose liquidation starts the cascade
            pool_positions: All positions in the same collateral pool
            prices: Dict of token -> USD price

        Returns:
            List of positions in the cascade chain, starting with trigger
        """
        chain: list[SimulatedPosition] = [trigger_position]
        remaining_positions = [p for p in pool_positions if p != trigger_position]

        # Simulate the cascade
        # Calculate collateral loss from trigger liquidation
        trigger_loss = trigger_position.collateral_usd

        # Calculate total available collateral after trigger
        remaining_collateral = sum(p.collateral_usd for p in remaining_positions)

        # Check if loss impacts other positions
        for position in remaining_positions:
            if position.collateral_usd <= Decimal("0"):
                continue

            # Calculate position's share of remaining collateral
            share = position.collateral_usd / (remaining_collateral or Decimal("1"))

            # Estimate impact of trigger loss on this position
            impact = trigger_loss * share

            # If impact reduces effective collateral below maintenance
            effective_collateral = position.collateral_usd - impact
            if position.notional_usd > Decimal("0"):
                effective_leverage = position.notional_usd / max(effective_collateral, Decimal("0.01"))
                # If leverage exceeds safe threshold, position cascades
                max_leverage = Decimal("20")  # Protocol max leverage
                if effective_leverage > max_leverage:
                    chain.append(position)
                    trigger_loss += position.collateral_usd

            # For borrow positions, check health factor impact
            elif position.position_type == PositionType.BORROW:
                if position.health_factor is not None:
                    # Reduced collateral reduces health factor
                    reduction_factor = (
                        effective_collateral / position.collateral_usd
                        if position.collateral_usd > Decimal("0")
                        else Decimal("0")
                    )
                    projected_hf = position.health_factor * reduction_factor
                    if projected_hf < Decimal("1.0"):
                        chain.append(position)
                        trigger_loss += position.collateral_usd

        return chain

    def _estimate_cascade_loss(
        self,
        chain: list[SimulatedPosition],
        prices: dict[str, Decimal],
    ) -> Decimal:
        """Estimate total loss from a cascade of liquidations.

        Loss includes:
        - Collateral seized in liquidations
        - Liquidation penalties (typically 5-10%)
        - Slippage from forced sales

        Args:
            chain: List of positions in the cascade
            prices: Dict of token -> USD price

        Returns:
            Estimated total loss in USD
        """
        total_loss = Decimal("0")
        liquidation_penalty = Decimal("0.05")  # 5% penalty

        for position in chain:
            if position.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                # Perp liquidation: lose collateral plus penalty
                loss = position.collateral_usd * (Decimal("1") + liquidation_penalty)
                total_loss += loss

            elif position.position_type == PositionType.BORROW:
                # Lending liquidation: collateral seized to cover debt
                # Estimate debt value
                debt_value = position.total_amount * position.entry_price
                # Close factor typically 50%
                close_factor = Decimal("0.5")
                seized = debt_value * close_factor * (Decimal("1") + liquidation_penalty)
                total_loss += seized

        return total_loss

    def _get_cascade_severity(
        self,
        chain_length: int,
        cascade_loss: Decimal,
        pool_collateral: Decimal,
    ) -> str:
        """Determine the severity level of a cascade warning.

        Args:
            chain_length: Number of positions in cascade chain
            cascade_loss: Estimated loss from cascade
            pool_collateral: Total collateral in the pool

        Returns:
            Severity level: "low", "medium", "high", or "critical"
        """
        # Calculate loss percentage
        if pool_collateral > Decimal("0"):
            loss_pct = cascade_loss / pool_collateral
        else:
            loss_pct = Decimal("0")

        if chain_length >= 4 or loss_pct >= Decimal("0.5"):
            return "critical"
        elif chain_length >= 3 or loss_pct >= Decimal("0.3"):
            return "high"
        elif chain_length >= 2 or loss_pct >= Decimal("0.15"):
            return "medium"
        else:
            return "low"

    def _format_cascade_warning(
        self,
        trigger_position: SimulatedPosition,
        chain: list[SimulatedPosition],
        cascade_loss: Decimal,
    ) -> str:
        """Format a human-readable cascade warning message.

        Args:
            trigger_position: The position that would trigger the cascade
            chain: All positions in the cascade chain
            cascade_loss: Estimated total loss

        Returns:
            Formatted warning message
        """
        return (
            f"Liquidation of {trigger_position.position_type.value} position "
            f"({trigger_position.position_id}) on {trigger_position.protocol} "
            f"could trigger cascade affecting {len(chain) - 1} other positions. "
            f"Estimated cascade loss: ${cascade_loss:,.2f}"
        )

    def _calculate_protocol_correlation_risk(
        self,
        positions: list[SimulatedPosition],
    ) -> Decimal:
        """Calculate correlation risk within a protocol.

        Positions on the same protocol share systemic risks:
        - Protocol-level bugs or exploits
        - Liquidity crises
        - Oracle failures

        Args:
            positions: List of positions on the same protocol

        Returns:
            Correlation risk score from 0 to 1
        """
        if len(positions) <= 1:
            return Decimal("0")

        # Calculate total value at risk
        total_value = Decimal("0")
        for pos in positions:
            if pos.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                total_value += pos.notional_usd
            else:
                total_value += pos.collateral_usd

        # Calculate concentration
        values = []
        for pos in positions:
            if pos.position_type in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
                values.append(pos.notional_usd)
            else:
                values.append(pos.collateral_usd)

        if not values or total_value <= Decimal("0"):
            return Decimal("0")

        # Higher concentration = higher correlation risk
        max_value = max(values)
        concentration = max_value / total_value

        # Scale to 0-1 based on number of positions and concentration
        position_factor = min(len(positions) / Decimal("5"), Decimal("1"))
        risk = concentration * position_factor

        return min(risk, Decimal("1"))

    def _calculate_cascade_risk_score(
        self,
        positions_with_shared_collateral: int,
        max_cascade_depth: int,
        total_collateral_at_risk: Decimal,
        estimated_cascade_loss: Decimal,
        protocol_correlations: dict[str, Decimal],
    ) -> Decimal:
        """Calculate overall cascade risk score.

        The score combines multiple factors:
        1. Number of positions sharing collateral
        2. Maximum cascade depth (how many positions can chain)
        3. Total collateral at risk
        4. Estimated loss relative to collateral
        5. Protocol correlation risk

        Args:
            positions_with_shared_collateral: Count of shared positions
            max_cascade_depth: Maximum cascade chain length
            total_collateral_at_risk: Total collateral in shared pools
            estimated_cascade_loss: Worst-case cascade loss
            protocol_correlations: Risk scores per protocol

        Returns:
            Overall cascade risk score from 0 to 1
        """
        # Base risk from shared positions
        # 0 at 1 position, 0.5 at 5 positions, 1.0 at 10+ positions
        if positions_with_shared_collateral <= 1:
            shared_risk = Decimal("0")
        elif positions_with_shared_collateral >= 10:
            shared_risk = Decimal("1")
        else:
            shared_risk = Decimal(positions_with_shared_collateral - 1) / Decimal("9")

        # Risk from cascade depth
        # 0 at depth 1, 0.5 at depth 3, 1.0 at depth 5+
        if max_cascade_depth <= 1:
            depth_risk = Decimal("0")
        elif max_cascade_depth >= 5:
            depth_risk = Decimal("1")
        else:
            depth_risk = Decimal(max_cascade_depth - 1) / Decimal("4")

        # Risk from potential loss
        if total_collateral_at_risk > Decimal("0"):
            loss_ratio = estimated_cascade_loss / total_collateral_at_risk
            loss_risk = min(loss_ratio, Decimal("1"))
        else:
            loss_risk = Decimal("0")

        # Average protocol correlation risk
        if protocol_correlations:
            avg_correlation = sum(protocol_correlations.values(), Decimal("0")) / len(protocol_correlations)
        else:
            avg_correlation = Decimal("0")

        # Weighted combination
        # Depth and loss are most important for cascade risk
        overall_score = (
            shared_risk * Decimal("0.15")
            + depth_risk * Decimal("0.35")
            + loss_risk * Decimal("0.35")
            + avg_correlation * Decimal("0.15")
        )

        return min(overall_score, Decimal("1"))

    def _emit_cascade_risk_warnings(
        self,
        risk_score: Decimal,
        warnings: list[CascadeRiskWarning],
    ) -> None:
        """Emit log warnings for elevated cascade risk.

        Args:
            risk_score: Overall cascade risk score
            warnings: List of specific warnings to emit
        """
        if risk_score >= Decimal("0.8"):
            level = logging.CRITICAL
            severity_text = "CRITICAL"
        elif risk_score >= Decimal("0.6"):
            level = logging.ERROR
            severity_text = "HIGH"
        elif risk_score >= Decimal("0.3"):
            level = logging.WARNING
            severity_text = "ELEVATED"
        else:
            return  # Below threshold

        logger.log(
            level,
            f"Cascade risk {severity_text}: score={risk_score:.2%}. {len(warnings)} potential cascade chains detected.",
        )

        # Emit individual warnings at appropriate level
        for warning in warnings:
            if warning.severity == "critical":
                logger.critical(warning.message)
            elif warning.severity == "high":
                logger.error(warning.message)
            elif warning.severity == "medium":
                logger.warning(warning.message)
            else:
                logger.info(warning.message)
