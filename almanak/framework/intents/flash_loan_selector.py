"""Flash-loan provider selector.

Scores a list of ``FlashLoanProvider`` candidates and picks the optimal
one based on the configured priority. The selector itself is protocol-
agnostic: it never names a specific provider. Each candidate is supplied
by its protocol connector via the abstract base in
``almanak.connectors._strategy_base.flash_loan_base``.

This is the cross-protocol routing analogue of
``almanak.framework.intents.bridge_selector.BridgeSelector``.

Example:
    from almanak.framework.intents.flash_loan_selector import FlashLoanSelector
    from almanak.connectors.aave_v3.flash_loan_provider import AaveFlashLoanProvider
    from almanak.connectors.balancer_v2.flash_loan_provider import BalancerFlashLoanProvider
    from almanak.connectors.morpho_blue.flash_loan_provider import MorphoFlashLoanProvider

    selector = FlashLoanSelector(
        chain="arbitrum",
        providers=[
            AaveFlashLoanProvider(),
            BalancerFlashLoanProvider(),
            MorphoFlashLoanProvider(),
        ],
    )
    result = selector.select_provider(token="USDC", amount=Decimal("1000000"))
"""

from __future__ import annotations

import logging
from decimal import Decimal

from almanak.connectors._strategy_base.flash_loan_base import (
    FlashLoanProvider,
    FlashLoanProviderInfo,
    FlashLoanSelectionResult,
    FlashLoanSelectorError,
    NoProviderAvailableError,
    SelectionPriority,
)

logger = logging.getLogger(__name__)


# Priority -> (fee, liquidity, reliability, gas) weights.
_PRIORITY_WEIGHTS: dict[SelectionPriority, tuple[float, float, float, float]] = {
    SelectionPriority.FEE: (0.6, 0.2, 0.1, 0.1),
    SelectionPriority.LIQUIDITY: (0.2, 0.6, 0.1, 0.1),
    SelectionPriority.RELIABILITY: (0.1, 0.1, 0.6, 0.2),
    SelectionPriority.GAS: (0.2, 0.1, 0.1, 0.6),
}


class FlashLoanSelector:
    """Pick the optimal flash-loan provider from a list of candidates.

    The selector is constructed with the concrete providers the
    orchestration layer wants to consider. It calls each provider's
    ``quote()`` polymorphically; no provider names are baked in.
    """

    def __init__(
        self,
        chain: str,
        providers: list[FlashLoanProvider],
        default_priority: SelectionPriority = SelectionPriority.FEE,
    ):
        """Initialize the selector.

        Args:
            chain: Target blockchain (e.g. ``"arbitrum"``).
            providers: Candidate providers to evaluate.
            default_priority: Default ranking priority.
        """
        self.chain = chain
        self.providers = providers
        self.default_priority = default_priority
        logger.info(
            "FlashLoanSelector initialized for chain=%s with %d providers: %s",
            chain,
            len(providers),
            [p.name for p in providers],
        )

    def select_provider(
        self,
        token: str,
        amount: Decimal,
        priority: str | SelectionPriority | None = None,
        min_liquidity_usd: int = 0,
    ) -> FlashLoanSelectionResult:
        """Select the optimal flash-loan provider for the request.

        Args:
            token: Token symbol (e.g. ``"USDC"``).
            amount: Flash-loan amount in token units.
            priority: Ranking priority — one of ``"fee" | "liquidity" |
                "reliability" | "gas"``. Falls back to the selector's
                default if unknown.
            min_liquidity_usd: Minimum required liquidity in USD. Quotes
                below this are marked unavailable.

        Returns:
            ``FlashLoanSelectionResult`` with the selected provider and
            full per-provider evaluation.

        Raises:
            NoProviderAvailableError: If no provider can serve the request.
        """
        selection_priority = self._resolve_priority(priority)
        logger.info(
            "Selecting flash loan provider for %s %s on %s with priority=%s",
            amount,
            token,
            self.chain,
            selection_priority.value,
        )

        quotes: list[FlashLoanProviderInfo] = []
        for p in self.providers:
            try:
                quotes.append(p.quote(self.chain, token, amount))
            except Exception as exc:  # noqa: BLE001
                # A buggy or transient provider implementation must not crash
                # the whole selection — surface it as unavailable and continue
                # evaluating the rest.
                logger.error(
                    "FlashLoanProvider %r raised during quote() for %s on %s: %s",
                    p.name,
                    token,
                    self.chain,
                    exc,
                    exc_info=True,
                )
                quotes.append(
                    FlashLoanProviderInfo(
                        provider=p.name,
                        is_available=False,
                        unavailable_reason=f"Provider raised during quote(): {exc}",
                    )
                )
        if min_liquidity_usd > 0:
            for q in quotes:
                if q.is_available and q.estimated_liquidity_usd < min_liquidity_usd:
                    q.is_available = False
                    q.unavailable_reason = (
                        f"Insufficient liquidity: {q.estimated_liquidity_usd:,} USD "
                        f"< required {min_liquidity_usd:,} USD"
                    )

        available = [q for q in quotes if q.is_available]
        if not available:
            reasons = [f"{q.provider}: {q.unavailable_reason}" for q in quotes if q.unavailable_reason]
            error_msg = f"No flash loan provider available for {token} on {self.chain}. Reasons: {'; '.join(reasons)}"
            logger.error(error_msg)
            raise NoProviderAvailableError(error_msg)

        self._calculate_scores(available, selection_priority)
        available.sort(key=lambda q: q.score)

        best = available[0]
        fallback = available[1] if len(available) > 1 else None
        reasoning = self._build_reasoning(best, fallback, selection_priority, available)
        logger.info("Selected flash loan provider: %s", best.provider)

        return FlashLoanSelectionResult(
            provider=best.provider,
            pool_address=best.pool_address,
            fee_bps=best.fee_bps,
            fee_amount=best.fee_amount,
            total_repay=amount + best.fee_amount,
            gas_estimate=best.gas_estimate,
            providers_evaluated=quotes,
            selection_reasoning=reasoning,
        )

    def get_provider_info(self, provider: str, token: str, amount: Decimal) -> FlashLoanProviderInfo:
        """Return the per-provider quote without ranking the others."""
        for candidate in self.providers:
            if candidate.name.lower() == provider.lower():
                return candidate.quote(self.chain, token, amount)
        return FlashLoanProviderInfo(
            provider=provider,
            is_available=False,
            unavailable_reason=f"Unknown provider: {provider}",
        )

    def is_token_supported(self, token: str, provider: str | None = None) -> bool:
        """Whether ``token`` is supported on ``self.chain`` by any (or one) provider."""
        if provider:
            target = provider.lower()
            return any(p.name.lower() == target and p.supports(self.chain, token) for p in self.providers)
        return any(p.supports(self.chain, token) for p in self.providers)

    def _resolve_priority(self, priority: str | SelectionPriority | None) -> SelectionPriority:
        if priority is None:
            return self.default_priority
        if isinstance(priority, SelectionPriority):
            return priority
        try:
            return SelectionPriority(priority.lower())
        except (AttributeError, ValueError):
            logger.warning("Unknown priority %r, using default %r", priority, self.default_priority.value)
            return self.default_priority

    @staticmethod
    def _calculate_scores(
        providers: list[FlashLoanProviderInfo],
        priority: SelectionPriority,
    ) -> None:
        """Mutate ``providers`` in place, setting ``score`` (lower is better)."""
        if not providers:
            return

        max_fee = max(p.fee_bps for p in providers) or 1
        max_liquidity = max(p.estimated_liquidity_usd for p in providers) or 1
        max_gas = max(p.gas_estimate for p in providers) or 1
        weights = _PRIORITY_WEIGHTS.get(priority, _PRIORITY_WEIGHTS[SelectionPriority.FEE])

        for p in providers:
            fee_score = p.fee_bps / max_fee if max_fee > 0 else 0
            liquidity_score = 1 - (p.estimated_liquidity_usd / max_liquidity)
            reliability_score = 1 - p.reliability_score
            gas_score = p.gas_estimate / max_gas if max_gas > 0 else 0
            p.score = (
                weights[0] * fee_score
                + weights[1] * liquidity_score
                + weights[2] * reliability_score
                + weights[3] * gas_score
            )

    @staticmethod
    def _build_reasoning(
        best: FlashLoanProviderInfo,
        fallback: FlashLoanProviderInfo | None,
        priority: SelectionPriority,
        all_providers: list[FlashLoanProviderInfo],
    ) -> str:
        parts = [f"Selected {best.provider} based on {priority.value} priority"]
        fee_desc = "zero" if best.fee_bps == 0 else f"{best.fee_bps} bps"
        parts.append(f"(fee: {fee_desc}, liquidity: ${best.estimated_liquidity_usd:,}, gas: {best.gas_estimate:,})")
        if fallback:
            fb_fee = "zero" if fallback.fee_bps == 0 else f"{fallback.fee_bps} bps"
            parts.append(f"Fallback: {fallback.provider} (fee: {fb_fee}, score: {fallback.score:.3f})")
        if len(all_providers) > 1:
            score_summary = ", ".join(
                f"{p.provider}={p.score:.3f}" for p in sorted(all_providers, key=lambda x: x.score)
            )
            parts.append(f"Scores: {score_summary}")
        return ". ".join(parts)


__all__ = [
    "FlashLoanProvider",
    "FlashLoanProviderInfo",
    "FlashLoanSelectionResult",
    "FlashLoanSelector",
    "FlashLoanSelectorError",
    "NoProviderAvailableError",
    "SelectionPriority",
]
