"""Bridge Selector for Optimal Bridge Selection.

This module provides the BridgeSelector class that evaluates available bridges
and selects the optimal one based on configurable criteria.

Selection Criteria:
- cost: Minimize total fees (gas + relayer + protocol fees)
- speed: Minimize completion time
- liquidity: Prefer bridges with deeper liquidity
- reliability: Prefer bridges with higher historical reliability

Example:
    from almanak.framework.connectors.bridges import BridgeSelector, AcrossBridgeAdapter, StargateBridgeAdapter

    selector = BridgeSelector([AcrossBridgeAdapter(), StargateBridgeAdapter()])

    # Select bridge optimizing for cost
    result = selector.select_bridge(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
        priority="cost",
    )

    if result.quote:
        print(f"Selected {result.bridge.name} with fee {result.quote.fee_amount}")
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from .base import (
    BridgeAdapter,
    BridgeError,
    BridgeQuote,
    BridgeQuoteError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class SelectionPriority(Enum):
    """Priority for bridge selection.

    Attributes:
        COST: Minimize total fees
        SPEED: Minimize completion time
        LIQUIDITY: Prefer deeper liquidity
        RELIABILITY: Prefer more reliable bridges
    """

    COST = "cost"
    SPEED = "speed"
    LIQUIDITY = "liquidity"
    RELIABILITY = "reliability"


# =============================================================================
# Exceptions
# =============================================================================


class BridgeSelectorError(BridgeError):
    """Base exception for bridge selector errors."""

    pass


class NoBridgeAvailableError(BridgeSelectorError):
    """Raised when no bridge can fulfill the request."""

    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BridgeScore:
    """Score for a bridge based on selection criteria.

    Attributes:
        bridge: The bridge adapter being scored
        quote: The quote from this bridge (if available)
        cost_score: Normalized cost score (0-1, lower is better)
        speed_score: Normalized speed score (0-1, lower is better)
        liquidity_score: Normalized liquidity score (0-1, lower is better)
        reliability_score: Normalized reliability score (0-1, lower is better)
        overall_score: Weighted overall score
        is_available: Whether bridge can fulfill request
        unavailable_reason: Reason if bridge unavailable
    """

    bridge: BridgeAdapter
    quote: BridgeQuote | None = None
    cost_score: float = 1.0
    speed_score: float = 1.0
    liquidity_score: float = 1.0
    reliability_score: float = 1.0
    overall_score: float = 1.0
    is_available: bool = False
    unavailable_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bridge_name": self.bridge.name,
            "quote": self.quote.to_dict() if self.quote else None,
            "cost_score": self.cost_score,
            "speed_score": self.speed_score,
            "liquidity_score": self.liquidity_score,
            "reliability_score": self.reliability_score,
            "overall_score": self.overall_score,
            "is_available": self.is_available,
            "unavailable_reason": self.unavailable_reason,
        }


@dataclass
class BridgeSelectionResult:
    """Result of bridge selection.

    Attributes:
        bridge: Selected bridge adapter (or None if no bridge available)
        quote: Quote from selected bridge
        scores: Scores for all evaluated bridges
        selection_reasoning: Human-readable explanation of selection
    """

    bridge: BridgeAdapter | None = None
    quote: BridgeQuote | None = None
    scores: list[BridgeScore] = field(default_factory=list)
    selection_reasoning: str = ""

    @property
    def is_success(self) -> bool:
        """Check if a bridge was successfully selected."""
        return self.bridge is not None and self.quote is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bridge_name": self.bridge.name if self.bridge else None,
            "quote": self.quote.to_dict() if self.quote else None,
            "scores": [s.to_dict() for s in self.scores],
            "selection_reasoning": self.selection_reasoning,
            "is_success": self.is_success,
        }


# =============================================================================
# Bridge Reliability Scores (configurable historical data)
# =============================================================================


# Default reliability scores (0-1, higher is better)
# Based on historical performance and trust
DEFAULT_RELIABILITY_SCORES: dict[str, float] = {
    "Across": 0.95,  # Well-established with UMA security
    "Stargate": 0.93,  # LayerZero backed, large TVL
}


# =============================================================================
# Bridge Selector
# =============================================================================


class BridgeSelector:
    """Selects optimal bridge for cross-chain transfers.

    The selector evaluates all registered bridge adapters and selects
    the best one based on configurable priority criteria.

    Attributes:
        bridges: List of registered bridge adapters
        reliability_scores: Historical reliability scores per bridge
        default_priority: Default selection priority

    Example:
        selector = BridgeSelector([
            AcrossBridgeAdapter(),
            StargateBridgeAdapter(),
        ])

        result = selector.select_bridge(
            token="USDC",
            amount=Decimal("1000"),
            from_chain="arbitrum",
            to_chain="optimism",
            priority="cost",
        )
    """

    def __init__(
        self,
        bridges: list[BridgeAdapter],
        reliability_scores: dict[str, float] | None = None,
        default_priority: SelectionPriority = SelectionPriority.COST,
    ):
        """Initialize the bridge selector.

        Args:
            bridges: List of bridge adapters to evaluate
            reliability_scores: Optional custom reliability scores per bridge name
            default_priority: Default selection priority
        """
        self.bridges = bridges
        self.reliability_scores = reliability_scores or DEFAULT_RELIABILITY_SCORES.copy()
        self.default_priority = default_priority

        logger.info(f"BridgeSelector initialized with {len(bridges)} bridges: {[b.name for b in bridges]}")

    def select_bridge(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        priority: str = "cost",
        max_slippage: Decimal = Decimal("0.005"),
    ) -> BridgeSelectionResult:
        """Select the optimal bridge for a transfer.

        Evaluates all registered bridges for the given route and returns
        the best one based on the priority criteria.

        Args:
            token: Token symbol to bridge (e.g., "ETH", "USDC")
            amount: Amount to bridge in token units
            from_chain: Source chain identifier
            to_chain: Destination chain identifier
            priority: Selection priority ("cost", "speed", "liquidity", "reliability")
            max_slippage: Maximum slippage tolerance

        Returns:
            BridgeSelectionResult with selected bridge and quote

        Raises:
            NoBridgeAvailableError: If no bridge can fulfill the request
        """
        # Parse priority
        try:
            selection_priority = SelectionPriority(priority.lower())
        except ValueError:
            logger.warning(f"Unknown priority '{priority}', falling back to '{self.default_priority.value}'")
            selection_priority = self.default_priority

        logger.info(
            f"Selecting bridge for {amount} {token} from {from_chain} to {to_chain} "
            f"with priority={selection_priority.value}"
        )

        # Evaluate all bridges
        scores = self._evaluate_bridges(
            token=token,
            amount=amount,
            from_chain=from_chain,
            to_chain=to_chain,
            max_slippage=max_slippage,
        )

        # Filter to available bridges
        available_scores = [s for s in scores if s.is_available]

        if not available_scores:
            # Build error message with reasons
            reasons = [f"{s.bridge.name}: {s.unavailable_reason}" for s in scores if s.unavailable_reason]
            error_msg = (
                f"No bridge available for {token} from {from_chain} to {to_chain}. Reasons: {'; '.join(reasons)}"
            )
            logger.error(error_msg)
            raise NoBridgeAvailableError(error_msg)

        # Calculate overall scores based on priority
        self._calculate_overall_scores(available_scores, selection_priority)

        # Sort by overall score (lower is better)
        available_scores.sort(key=lambda s: s.overall_score)

        # Select the best bridge
        best = available_scores[0]
        fallback = available_scores[1] if len(available_scores) > 1 else None

        # Build selection reasoning
        reasoning = self._build_reasoning(
            best=best,
            fallback=fallback,
            priority=selection_priority,
            all_scores=available_scores,
        )

        logger.info(f"Selected bridge: {best.bridge.name}")
        logger.debug(f"Selection reasoning: {reasoning}")

        return BridgeSelectionResult(
            bridge=best.bridge,
            quote=best.quote,
            scores=scores,
            selection_reasoning=reasoning,
        )

    def get_available_bridges(
        self,
        token: str,
        from_chain: str,
        to_chain: str,
    ) -> list[BridgeAdapter]:
        """Get list of bridges that support a route.

        Args:
            token: Token symbol
            from_chain: Source chain
            to_chain: Destination chain

        Returns:
            List of bridge adapters that support the route
        """
        available = []
        for bridge in self.bridges:
            is_valid, _ = bridge.validate_transfer(
                token=token,
                amount=Decimal("1"),  # Dummy amount for route check
                from_chain=from_chain,
                to_chain=to_chain,
            )
            if is_valid:
                available.append(bridge)
        return available

    def select_bridge_with_fallback(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        priority: str = "cost",
        max_slippage: Decimal = Decimal("0.005"),
        excluded_bridges: list[str] | None = None,
    ) -> BridgeSelectionResult:
        """Select bridge with automatic fallback if primary fails.

        If the primary selection fails for any reason, attempts to use
        the next best bridge as a fallback.

        Args:
            token: Token symbol to bridge
            amount: Amount to bridge
            from_chain: Source chain
            to_chain: Destination chain
            priority: Selection priority
            max_slippage: Maximum slippage tolerance
            excluded_bridges: List of bridge names to exclude from selection

        Returns:
            BridgeSelectionResult with selected bridge

        Raises:
            NoBridgeAvailableError: If no bridge (including fallbacks) available
        """
        excluded = {b.lower() for b in (excluded_bridges or [])}

        # Filter bridges
        active_bridges = [b for b in self.bridges if b.name.lower() not in excluded]

        if not active_bridges:
            raise NoBridgeAvailableError(f"No bridges available after excluding: {excluded_bridges}")

        # Create temporary selector with filtered bridges
        temp_selector = BridgeSelector(
            bridges=active_bridges,
            reliability_scores=self.reliability_scores,
            default_priority=self.default_priority,
        )

        try:
            return temp_selector.select_bridge(
                token=token,
                amount=amount,
                from_chain=from_chain,
                to_chain=to_chain,
                priority=priority,
                max_slippage=max_slippage,
            )
        except NoBridgeAvailableError:
            # Already tried all available bridges
            raise

    def _evaluate_bridges(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal,
    ) -> list[BridgeScore]:
        """Evaluate all bridges for a given route.

        Args:
            token: Token symbol
            amount: Amount to bridge
            from_chain: Source chain
            to_chain: Destination chain
            max_slippage: Maximum slippage

        Returns:
            List of BridgeScore for each bridge
        """
        scores: list[BridgeScore] = []

        for bridge in self.bridges:
            score = self._evaluate_single_bridge(
                bridge=bridge,
                token=token,
                amount=amount,
                from_chain=from_chain,
                to_chain=to_chain,
                max_slippage=max_slippage,
            )
            scores.append(score)

        return scores

    def _evaluate_single_bridge(
        self,
        bridge: BridgeAdapter,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal,
    ) -> BridgeScore:
        """Evaluate a single bridge for a transfer.

        Args:
            bridge: Bridge adapter to evaluate
            token: Token symbol
            amount: Amount to bridge
            from_chain: Source chain
            to_chain: Destination chain
            max_slippage: Maximum slippage

        Returns:
            BridgeScore with evaluation results
        """
        # First validate route support
        is_valid, error_msg = bridge.validate_transfer(
            token=token,
            amount=amount,
            from_chain=from_chain,
            to_chain=to_chain,
        )

        if not is_valid:
            logger.debug(f"Bridge {bridge.name} unavailable: {error_msg}")
            return BridgeScore(
                bridge=bridge,
                is_available=False,
                unavailable_reason=error_msg,
            )

        # Try to get a quote
        try:
            quote = bridge.get_quote(
                token=token,
                amount=amount,
                from_chain=from_chain,
                to_chain=to_chain,
                max_slippage=max_slippage,
            )

            # Quote expired?
            if quote.is_expired:
                return BridgeScore(
                    bridge=bridge,
                    is_available=False,
                    unavailable_reason="Quote expired immediately",
                )

            logger.debug(f"Bridge {bridge.name} quote: fee={quote.fee_amount}, time={quote.estimated_time_seconds}s")

            return BridgeScore(
                bridge=bridge,
                quote=quote,
                is_available=True,
            )

        except BridgeQuoteError as e:
            logger.debug(f"Bridge {bridge.name} quote error: {e}")
            return BridgeScore(
                bridge=bridge,
                is_available=False,
                unavailable_reason=f"Quote error: {str(e)}",
            )
        except Exception as e:
            logger.warning(f"Bridge {bridge.name} unexpected error: {e}")
            return BridgeScore(
                bridge=bridge,
                is_available=False,
                unavailable_reason=f"Unexpected error: {str(e)}",
            )

    def _calculate_overall_scores(
        self,
        scores: list[BridgeScore],
        priority: SelectionPriority,
    ) -> None:
        """Calculate overall scores based on priority.

        Normalizes individual scores and calculates weighted overall score.
        Modifies scores in place.

        Args:
            scores: List of BridgeScore to update
            priority: Selection priority
        """
        if not scores:
            return

        # Collect values for normalization
        fees = []
        times = []

        for score in scores:
            if score.quote:
                fees.append(float(score.quote.fee_amount))
                times.append(float(score.quote.estimated_time_seconds))

        if not fees:
            return

        # Normalize values (min-max scaling, lower is better)
        min_fee, max_fee = min(fees), max(fees)
        min_time, max_time = min(times), max(times)

        fee_range = max_fee - min_fee if max_fee > min_fee else 1.0
        time_range = max_time - min_time if max_time > min_time else 1.0

        for score in scores:
            if not score.quote:
                continue

            # Cost score (normalized fee)
            fee = float(score.quote.fee_amount)
            score.cost_score = (fee - min_fee) / fee_range if fee_range > 0 else 0.0

            # Speed score (normalized time)
            time_val = float(score.quote.estimated_time_seconds)
            score.speed_score = (time_val - min_time) / time_range if time_range > 0 else 0.0

            # Liquidity score (estimate based on output/input ratio)
            # Higher output ratio = better liquidity = lower score
            if score.quote.input_amount > 0:
                output_ratio = float(score.quote.output_amount / score.quote.input_amount)
                # Invert so higher ratio = lower score
                score.liquidity_score = 1.0 - min(output_ratio, 1.0)
            else:
                score.liquidity_score = 1.0

            # Reliability score (from configured scores)
            reliability = self.reliability_scores.get(score.bridge.name, 0.5)
            # Invert so higher reliability = lower score
            score.reliability_score = 1.0 - reliability

            # Calculate weighted overall score based on priority
            score.overall_score = self._calculate_weighted_score(score, priority)

    def _calculate_weighted_score(
        self,
        score: BridgeScore,
        priority: SelectionPriority,
    ) -> float:
        """Calculate weighted overall score.

        Weights depend on priority:
        - COST: cost=0.6, speed=0.2, liquidity=0.1, reliability=0.1
        - SPEED: speed=0.6, cost=0.2, reliability=0.1, liquidity=0.1
        - LIQUIDITY: liquidity=0.6, cost=0.2, reliability=0.1, speed=0.1
        - RELIABILITY: reliability=0.6, cost=0.2, liquidity=0.1, speed=0.1

        Args:
            score: BridgeScore with individual scores
            priority: Selection priority

        Returns:
            Weighted overall score
        """
        weights: dict[str, tuple[float, float, float, float]]
        weights = {
            # (cost, speed, liquidity, reliability)
            "cost": (0.6, 0.2, 0.1, 0.1),
            "speed": (0.2, 0.6, 0.1, 0.1),
            "liquidity": (0.2, 0.1, 0.6, 0.1),
            "reliability": (0.2, 0.1, 0.1, 0.6),
        }

        w = weights.get(priority.value, weights["cost"])

        return (
            w[0] * score.cost_score
            + w[1] * score.speed_score
            + w[2] * score.liquidity_score
            + w[3] * score.reliability_score
        )

    def _build_reasoning(
        self,
        best: BridgeScore,
        fallback: BridgeScore | None,
        priority: SelectionPriority,
        all_scores: list[BridgeScore],
    ) -> str:
        """Build human-readable selection reasoning.

        Args:
            best: Best scoring bridge
            fallback: Second best bridge (if any)
            priority: Selection priority used
            all_scores: All evaluated scores

        Returns:
            Human-readable reasoning string
        """
        parts = []

        # Primary selection
        parts.append(f"Selected {best.bridge.name} based on {priority.value} priority")

        if best.quote:
            parts.append(
                f"(fee: {best.quote.fee_amount} {best.quote.token}, time: {best.quote.estimated_time_seconds}s)"
            )

        # Score breakdown
        parts.append(
            f"Scores: cost={best.cost_score:.3f}, speed={best.speed_score:.3f}, "
            f"liquidity={best.liquidity_score:.3f}, reliability={best.reliability_score:.3f}, "
            f"overall={best.overall_score:.3f}"
        )

        # Fallback info
        if fallback:
            parts.append(f"Fallback: {fallback.bridge.name} (overall={fallback.overall_score:.3f})")

        # Comparison summary
        if len(all_scores) > 1:
            bridge_summary = ", ".join(
                f"{s.bridge.name}={s.overall_score:.3f}" for s in sorted(all_scores, key=lambda x: x.overall_score)
            )
            parts.append(f"All bridges ranked: {bridge_summary}")

        return ". ".join(parts)


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Main class
    "BridgeSelector",
    # Data classes
    "BridgeScore",
    "BridgeSelectionResult",
    # Enums
    "SelectionPriority",
    # Exceptions
    "BridgeSelectorError",
    "NoBridgeAvailableError",
    # Constants
    "DEFAULT_RELIABILITY_SCORES",
]
