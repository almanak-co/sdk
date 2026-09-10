"""Measured oracle-versus-quote decisions for V4 swap compilation."""

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Literal

from almanak.framework.intents._compiler_helpers import PriceImpactDecision, check_price_impact


@dataclass(frozen=True)
class SwapPriceImpactEvidence:
    status: Literal["passed", "refused", "skipped"]
    reason: str
    quote_amount_raw: int
    oracle_estimate_raw: int | None = None
    price_ratio: str | None = None
    price_impact: str | None = None
    max_price_impact: str | None = None
    schema_version: int = 1

    def to_wire(self) -> dict[str, Any]:
        wire = asdict(self)
        wire["quote_amount_raw"] = str(self.quote_amount_raw)
        wire["oracle_estimate_raw"] = str(self.oracle_estimate_raw) if self.oracle_estimate_raw is not None else None
        return wire


def evaluate_swap_price_impact(
    *,
    quote_source: str,
    quoter_amount: int,
    amount_in: Decimal,
    token_out_dec: int,
    price_ratio: Decimal | None,
    max_price_impact: Decimal | None,
    config_max_price_impact: Decimal | None,
    using_placeholders: bool,
    managed_fork: bool,
) -> SwapPriceImpactEvidence:
    if quote_source != "onchain_quoter" or using_placeholders:
        return SwapPriceImpactEvidence("skipped", "offline_estimate", quoter_amount)
    if managed_fork:
        return SwapPriceImpactEvidence("skipped", "managed_fork_oracle_time_mismatch", quoter_amount)
    if price_ratio is None or not price_ratio.is_finite() or price_ratio <= 0:
        return SwapPriceImpactEvidence("refused", "oracle_unavailable", quoter_amount)
    limit = max_price_impact if max_price_impact is not None else config_max_price_impact
    limit = Decimal("0.05") if limit is None else limit
    if not limit.is_finite() or not Decimal("0") <= limit <= Decimal("1"):
        return SwapPriceImpactEvidence("refused", "invalid_impact_limit", quoter_amount)
    oracle_estimate = int(amount_in * price_ratio * Decimal(10**token_out_dec))
    if oracle_estimate <= 0:
        return SwapPriceImpactEvidence("refused", "oracle_estimate_below_raw_unit", quoter_amount)
    result = check_price_impact(
        oracle_estimate=oracle_estimate,
        quoter_amount=quoter_amount,
        intent_max_impact=limit,
        config_max_impact=limit,
        offline_mode=False,
        using_placeholders=False,
    )
    return SwapPriceImpactEvidence(
        status="passed" if result.decision is PriceImpactDecision.OK else "refused",
        reason=result.decision.value,
        quote_amount_raw=quoter_amount,
        oracle_estimate_raw=oracle_estimate,
        price_ratio=str(price_ratio),
        price_impact=str(result.price_impact) if result.price_impact is not None else None,
        max_price_impact=str(limit),
    )
