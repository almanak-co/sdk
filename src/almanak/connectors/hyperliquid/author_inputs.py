"""Unresolved HyperCore perp choices, naming one market and one fund account.

The author states the capital, the fixed cross leverage and the slippage of every `Ioc`
order, and for fork runs, optionally, the operator's HYPE reserve; the market's index,
size decimals and maximum leverage are resolved facts.
"""

from typing import Annotated, Literal

from pydantic import Field

from almanak.configuration.input_values import AmountInput, PositiveDuration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.hyperliquid.models import Allocation
from almanak.connectors.hyperliquid.resolution import ResolvedMarket
from almanak.gateway.evm import native_metadata
from almanak.triggers import HOLD_SECONDS_DESCRIPTION


class AllocationInput(PositionInput):
    connector: Literal["hyperliquid"] = Field(default="hyperliquid", exclude=True)
    capital: AmountInput
    leverage: Annotated[int, Field(strict=True, ge=1)]
    slippage_bps: Annotated[int, Field(strict=True, gt=0, le=1_000)]
    hold_seconds: PositiveDuration | None = Field(
        default=None, description=HOLD_SECONDS_DESCRIPTION
    )
    # HYPE the operator keeps for custody gas in fork runs; below it, no new exposure.
    minimum_native_balance: Quantity | None = None

    def resolve(self, name: str, market: ResolvedMarket) -> Allocation:
        """The allocation on the resolved market, its capital in USDC base units."""
        if self.capital.asset != market.usdc.identifier:
            raise ConfigurationError(
                "HyperCore capital is USDC on eip155:999", field="capital.asset"
            )
        if self.leverage > market.market.max_leverage:
            raise ConfigurationError(
                f"leverage exceeds the market's maximum of {market.market.max_leverage}",
                field="leverage",
            )
        result = self.model_dump(exclude={"market", "minimum_native_balance"})
        if self.minimum_native_balance is not None:
            _, native = native_metadata(market.selection.chain)
            result["minimum_native_balance"] = converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            )
        result.update(
            name=name,
            chain=market.block.chain,
            market=market.market,
            capital={
                "asset": self.capital.asset,
                "amount": converted(
                    self.capital.quantity, market.usdc.decimals, "capital.quantity"
                ),
            },
        )
        return Allocation.model_validate(result)
