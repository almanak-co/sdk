"""PnL Attribution calculators for portfolio analysis.

This module provides functions to attribute PnL to different categories:

- By Protocol: Attribute PnL to each protocol used (uniswap_v3, aave_v3, gmx, etc.)
- By Intent Type: Attribute PnL to each intent type (SWAP, LP_OPEN, BORROW, etc.)
- By Asset: Attribute PnL to each asset involved in trades

These breakdowns help identify which protocols, strategies, and assets are
driving returns and which are underperforming.

Example:
    from almanak.framework.backtesting.pnl.calculators.attribution import (
        AttributionCalculator,
        attribute_pnl_by_protocol,
        attribute_pnl_by_intent_type,
        attribute_pnl_by_asset,
        verify_attribution_totals,
    )

    # Using the calculator class
    calc = AttributionCalculator()

    # Attribute PnL by protocol
    by_protocol = calc.attribute_pnl_by_protocol(trades)
    # {"uniswap_v3": Decimal("100"), "aave_v3": Decimal("-50")}

    # Attribute PnL by intent type
    by_intent = calc.attribute_pnl_by_intent_type(trades)
    # {"SWAP": Decimal("75"), "LP_OPEN": Decimal("25")}

    # Attribute PnL by asset
    by_asset = calc.attribute_pnl_by_asset(trades)
    # {"ETH": Decimal("80"), "USDC": Decimal("20")}

    # Verify all attributions sum to total
    is_valid = verify_attribution_totals(
        trades=trades,
        pnl_by_protocol=by_protocol,
        pnl_by_intent_type=by_intent,
        pnl_by_asset=by_asset,
        tolerance=Decimal("0.01"),  # 1 cent tolerance for rounding
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import TradeRecord


@dataclass
class AttributionResult:
    """Result of a PnL attribution calculation.

    Attributes:
        attribution: Dictionary mapping category (protocol/intent/asset) to PnL
        total_pnl: Total PnL across all categories
        trade_count: Number of trades included in the attribution
        unattributed_pnl: PnL that couldn't be attributed (e.g., missing protocol)
    """

    attribution: dict[str, Decimal]
    total_pnl: Decimal = Decimal("0")
    trade_count: int = 0
    unattributed_pnl: Decimal = Decimal("0")

    def to_dict(self) -> dict[str, str | int | dict[str, str]]:
        """Serialize to dictionary."""
        return {
            "attribution": {k: str(v) for k, v in self.attribution.items()},
            "total_pnl": str(self.total_pnl),
            "trade_count": self.trade_count,
            "unattributed_pnl": str(self.unattributed_pnl),
        }


@dataclass
class AttributionCalculator:
    """Calculator for PnL attribution analysis.

    Provides methods to break down PnL by different categories:
    - Protocol: Which protocols generated/lost money
    - Intent Type: Which trade types generated/lost money
    - Asset: Which assets generated/lost money

    Attributes:
        use_net_pnl: If True, use net_pnl_usd (after costs) for attribution.
            If False, use pnl_usd (before costs). Default True.

    Example:
        calc = AttributionCalculator()

        # Get all attributions
        by_protocol = calc.attribute_pnl_by_protocol(trades)
        by_intent = calc.attribute_pnl_by_intent_type(trades)
        by_asset = calc.attribute_pnl_by_asset(trades)
    """

    use_net_pnl: bool = True

    def _get_trade_pnl(self, trade: "TradeRecord") -> Decimal:
        """Get the PnL value to use for a trade.

        Args:
            trade: TradeRecord to extract PnL from

        Returns:
            Net PnL if use_net_pnl is True, otherwise gross PnL
        """
        if self.use_net_pnl:
            return trade.net_pnl_usd
        return trade.pnl_usd

    def attribute_pnl_by_protocol(
        self,
        trades: list["TradeRecord"],
    ) -> dict[str, Decimal]:
        """Attribute PnL to each protocol used.

        Groups all trades by their protocol and sums the PnL for each.
        This helps identify which protocols are generating value and which
        are underperforming.

        Args:
            trades: List of TradeRecord objects from the backtest

        Returns:
            Dictionary mapping protocol name to total PnL.
            Example: {"uniswap_v3": Decimal("100"), "aave_v3": Decimal("-50")}

        Note:
            Trades with empty or missing protocol are grouped under "unknown".
        """
        attribution: dict[str, Decimal] = {}

        for trade in trades:
            # Skip failed trades
            if not trade.success:
                continue

            # Use "unknown" for trades without protocol
            protocol = trade.protocol if trade.protocol else "unknown"
            pnl = self._get_trade_pnl(trade)

            if protocol in attribution:
                attribution[protocol] += pnl
            else:
                attribution[protocol] = pnl

        return attribution

    def attribute_pnl_by_intent_type(
        self,
        trades: list["TradeRecord"],
    ) -> dict[str, Decimal]:
        """Attribute PnL to each intent type.

        Groups all trades by their intent type (SWAP, LP_OPEN, BORROW, etc.)
        and sums the PnL for each. This helps identify which trading strategies
        are most effective.

        Args:
            trades: List of TradeRecord objects from the backtest

        Returns:
            Dictionary mapping intent type to total PnL.
            Example: {"SWAP": Decimal("75"), "LP_OPEN": Decimal("25")}
        """
        attribution: dict[str, Decimal] = {}

        for trade in trades:
            # Skip failed trades
            if not trade.success:
                continue

            intent_type = trade.intent_type.value
            pnl = self._get_trade_pnl(trade)

            if intent_type in attribution:
                attribution[intent_type] += pnl
            else:
                attribution[intent_type] = pnl

        return attribution

    def attribute_pnl_by_asset(
        self,
        trades: list["TradeRecord"],
    ) -> dict[str, Decimal]:
        """Attribute PnL to each asset involved in trades.

        Distributes PnL across all assets involved in each trade. For trades
        involving multiple assets (e.g., swaps), the PnL is split equally
        among all assets. This helps identify which assets are driving returns.

        Args:
            trades: List of TradeRecord objects from the backtest

        Returns:
            Dictionary mapping asset symbol to total attributed PnL.
            Example: {"ETH": Decimal("80"), "USDC": Decimal("20")}

        Note:
            - For trades with multiple tokens, PnL is split equally
            - Trades with no tokens are grouped under "unknown"
            - Token symbols are normalized to uppercase
        """
        attribution: dict[str, Decimal] = {}

        for trade in trades:
            # Skip failed trades
            if not trade.success:
                continue

            pnl = self._get_trade_pnl(trade)
            tokens = trade.tokens

            if not tokens:
                # No tokens - attribute to unknown
                tokens = ["unknown"]

            # Split PnL equally among all tokens involved
            pnl_per_token = pnl / Decimal(str(len(tokens)))

            for token in tokens:
                # Normalize token symbol to uppercase
                token_normalized = token.upper() if token else "UNKNOWN"

                if token_normalized in attribution:
                    attribution[token_normalized] += pnl_per_token
                else:
                    attribution[token_normalized] = pnl_per_token

        return attribution

    def get_attribution_result(
        self,
        trades: list["TradeRecord"],
        attribution_type: str = "protocol",
    ) -> AttributionResult:
        """Get a complete attribution result with metadata.

        Args:
            trades: List of TradeRecord objects from the backtest
            attribution_type: Type of attribution ("protocol", "intent_type", or "asset")

        Returns:
            AttributionResult with attribution dict and metadata

        Raises:
            ValueError: If attribution_type is not recognized
        """
        if attribution_type == "protocol":
            attribution = self.attribute_pnl_by_protocol(trades)
        elif attribution_type == "intent_type":
            attribution = self.attribute_pnl_by_intent_type(trades)
        elif attribution_type == "asset":
            attribution = self.attribute_pnl_by_asset(trades)
        else:
            raise ValueError(
                f"Unknown attribution_type: {attribution_type}. Must be one of: protocol, intent_type, asset"
            )

        total_pnl = sum(attribution.values(), Decimal("0"))
        successful_trades = sum(1 for t in trades if t.success)

        return AttributionResult(
            attribution=attribution,
            total_pnl=total_pnl,
            trade_count=successful_trades,
            unattributed_pnl=Decimal("0"),  # All PnL is attributed
        )


def attribute_pnl_by_protocol(
    trades: list["TradeRecord"],
    use_net_pnl: bool = True,
) -> dict[str, Decimal]:
    """Attribute PnL by protocol (convenience function).

    See AttributionCalculator.attribute_pnl_by_protocol for full documentation.

    Args:
        trades: List of TradeRecord objects from the backtest
        use_net_pnl: If True, use net PnL after costs. Default True.

    Returns:
        Dictionary mapping protocol to total PnL
    """
    calc = AttributionCalculator(use_net_pnl=use_net_pnl)
    return calc.attribute_pnl_by_protocol(trades)


def attribute_pnl_by_intent_type(
    trades: list["TradeRecord"],
    use_net_pnl: bool = True,
) -> dict[str, Decimal]:
    """Attribute PnL by intent type (convenience function).

    See AttributionCalculator.attribute_pnl_by_intent_type for full documentation.

    Args:
        trades: List of TradeRecord objects from the backtest
        use_net_pnl: If True, use net PnL after costs. Default True.

    Returns:
        Dictionary mapping intent type to total PnL
    """
    calc = AttributionCalculator(use_net_pnl=use_net_pnl)
    return calc.attribute_pnl_by_intent_type(trades)


def attribute_pnl_by_asset(
    trades: list["TradeRecord"],
    use_net_pnl: bool = True,
) -> dict[str, Decimal]:
    """Attribute PnL by asset (convenience function).

    See AttributionCalculator.attribute_pnl_by_asset for full documentation.

    Args:
        trades: List of TradeRecord objects from the backtest
        use_net_pnl: If True, use net PnL after costs. Default True.

    Returns:
        Dictionary mapping asset to total attributed PnL
    """
    calc = AttributionCalculator(use_net_pnl=use_net_pnl)
    return calc.attribute_pnl_by_asset(trades)


def verify_attribution_totals(
    trades: list["TradeRecord"],
    pnl_by_protocol: dict[str, Decimal],
    pnl_by_intent_type: dict[str, Decimal],
    pnl_by_asset: dict[str, Decimal],
    tolerance: Decimal = Decimal("0.01"),
    use_net_pnl: bool = True,
) -> bool:
    """Verify that all attributions sum to the total PnL.

    This function checks that the sum of each attribution dictionary matches
    the expected total PnL from all successful trades. This validates that
    no PnL is lost or double-counted in the attribution process.

    Args:
        trades: List of TradeRecord objects from the backtest
        pnl_by_protocol: Attribution by protocol from attribute_pnl_by_protocol
        pnl_by_intent_type: Attribution by intent type from attribute_pnl_by_intent_type
        pnl_by_asset: Attribution by asset from attribute_pnl_by_asset
        tolerance: Maximum allowed difference between sums (default $0.01)
        use_net_pnl: Whether net PnL was used for attribution (must match)

    Returns:
        True if all attributions sum correctly within tolerance, False otherwise

    Example:
        is_valid = verify_attribution_totals(
            trades=trades,
            pnl_by_protocol=by_protocol,
            pnl_by_intent_type=by_intent,
            pnl_by_asset=by_asset,
        )
        if not is_valid:
            logger.warning("Attribution totals do not match!")
    """
    # Calculate expected total from trades
    expected_total = Decimal("0")
    for trade in trades:
        if trade.success:
            if use_net_pnl:
                expected_total += trade.net_pnl_usd
            else:
                expected_total += trade.pnl_usd

    # Sum each attribution
    protocol_total = sum(pnl_by_protocol.values(), Decimal("0"))
    intent_total = sum(pnl_by_intent_type.values(), Decimal("0"))
    asset_total = sum(pnl_by_asset.values(), Decimal("0"))

    # Check each against expected
    protocol_diff = abs(protocol_total - expected_total)
    intent_diff = abs(intent_total - expected_total)
    asset_diff = abs(asset_total - expected_total)

    return protocol_diff <= tolerance and intent_diff <= tolerance and asset_diff <= tolerance


def calculate_all_attributions(
    trades: list["TradeRecord"],
    use_net_pnl: bool = True,
) -> tuple[dict[str, Decimal], dict[str, Decimal], dict[str, Decimal]]:
    """Calculate all three attribution breakdowns at once.

    Convenience function that calculates PnL attribution by protocol,
    intent type, and asset in a single call.

    Args:
        trades: List of TradeRecord objects from the backtest
        use_net_pnl: If True, use net PnL after costs. Default True.

    Returns:
        Tuple of (pnl_by_protocol, pnl_by_intent_type, pnl_by_asset) dictionaries

    Example:
        by_protocol, by_intent, by_asset = calculate_all_attributions(trades)
    """
    calc = AttributionCalculator(use_net_pnl=use_net_pnl)
    return (
        calc.attribute_pnl_by_protocol(trades),
        calc.attribute_pnl_by_intent_type(trades),
        calc.attribute_pnl_by_asset(trades),
    )


__all__ = [
    "AttributionCalculator",
    "AttributionResult",
    "attribute_pnl_by_protocol",
    "attribute_pnl_by_intent_type",
    "attribute_pnl_by_asset",
    "verify_attribution_totals",
    "calculate_all_attributions",
]
