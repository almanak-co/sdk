"""DEX Arbitrage Monitor on Base -- YAInnick Loop Iteration 20.

Monitors WETH/USDC price spread between Uniswap V3 and Aerodrome on Base.
Routes swaps to the selected DEX using SwapIntent with explicit protocol parameter.

This is the first multi-DEX strategy in yailoop -- exercises SwapIntent with
protocol-specific routing (uniswap_v3 vs aerodrome) within the same strategy class.
Tests that the compiler correctly dispatches to two different DEX connectors.

Key design note:
    True DEX quote comparison requires market.price_across_dexs(), but this method
    needs a _multi_dex_service configured by the runner (not currently wired up).
    This strategy surfaces that gap and falls back to oracle-based pricing.

Run:
    almanak strat run -d strategies/incubating/dex_arb_base --network anvil --once
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

# Supported DEX protocols on Base
SUPPORTED_PROTOCOLS = ("uniswap_v3", "aerodrome")


@dataclass
class DexArbConfig:
    """Configuration for DEX arbitrage monitor strategy."""

    # Swap parameters
    swap_amount_usdc: Decimal = Decimal("100")
    max_slippage: Decimal = Decimal("0.01")

    # Protocol selection
    # "uniswap_v3" or "aerodrome" to force a specific DEX
    # "auto" to use oracle-based selection (Uniswap V3 preferred by default)
    force_protocol: str = "uniswap_v3"

    # Direction
    # "sell_weth" -> swap WETH->USDC
    # "buy_weth"  -> swap USDC->WETH
    direction: str = "buy_weth"

    # Threshold: min spread in bps to execute (0 = always execute for testing)
    spread_threshold_bps: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.swap_amount_usdc, str | int | float):
            self.swap_amount_usdc = Decimal(str(self.swap_amount_usdc))
        if isinstance(self.max_slippage, str | int | float):
            self.max_slippage = Decimal(str(self.max_slippage))
        if isinstance(self.spread_threshold_bps, str | float):
            self.spread_threshold_bps = int(self.spread_threshold_bps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "swap_amount_usdc": str(self.swap_amount_usdc),
            "max_slippage": str(self.max_slippage),
            "force_protocol": self.force_protocol,
            "direction": self.direction,
            "spread_threshold_bps": self.spread_threshold_bps,
        }


@almanak_strategy(
    name="dex_arb_base",
    description="DEX arbitrage monitor on Base -- routes swaps to Uniswap V3 or Aerodrome",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["incubating", "swap", "arbitrage", "uniswap-v3", "aerodrome", "base", "multi-dex"],
    supported_chains=["base"],
    supported_protocols=["uniswap_v3", "aerodrome"],
    intent_types=["SWAP", "HOLD"],
)
class DexArbBaseStrategy(IntentStrategy[DexArbConfig]):
    """DEX Arbitrage Monitor on Base.

    Monitors WETH/USDC on both Uniswap V3 and Aerodrome on Base.
    Routes swaps to the configured DEX using SwapIntent with explicit protocol.

    Exercises the multi-DEX protocol routing path: the same strategy can route
    to two different connectors by setting intent.protocol.

    Configuration:
        swap_amount_usdc: USDC amount to swap (if direction=buy_weth)
        max_slippage: Maximum accepted slippage (0.01 = 1%)
        force_protocol: "uniswap_v3", "aerodrome", or "auto"
        direction: "buy_weth" (USDC->WETH) or "sell_weth" (WETH->USDC)
        spread_threshold_bps: Min spread in bps to trigger swap (0 = always)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.swap_amount_usdc = self.config.swap_amount_usdc
        self.max_slippage = self.config.max_slippage
        self.force_protocol = self.config.force_protocol.lower()
        self.direction = self.config.direction.lower()
        self.spread_threshold_bps = self.config.spread_threshold_bps

        # Validate protocol config
        if self.force_protocol not in (*SUPPORTED_PROTOCOLS, "auto"):
            logger.warning(
                f"Unknown force_protocol={self.force_protocol!r}, defaulting to uniswap_v3. "
                f"Valid: {SUPPORTED_PROTOCOLS} or 'auto'"
            )
            self.force_protocol = "uniswap_v3"

        logger.info(
            f"DexArbBaseStrategy initialized: "
            f"amount=${self.swap_amount_usdc} USDC, "
            f"protocol={self.force_protocol}, "
            f"direction={self.direction}, "
            f"threshold={self.spread_threshold_bps} bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Monitor DEX prices and route swap to best venue.

        Decision flow:
        1. Attempt multi-DEX price comparison via market.price_across_dexs()
        2. If unavailable, fall back to oracle price from market.price()
        3. Select protocol per force_protocol config
        4. Execute swap if spread exceeds threshold (or threshold == 0)
        """
        try:
            # ----------------------------------------------------------------
            # Step 1: Get oracle prices (always available)
            # ----------------------------------------------------------------
            try:
                weth_price = market.price("WETH")
                usdc_price = market.price("USDC")
                logger.info(f"Oracle prices: WETH=${weth_price:.2f}, USDC=${usdc_price:.6f}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Oracle price unavailable: {e}")
                weth_price = Decimal("2500")
                usdc_price = Decimal("1")

            # ----------------------------------------------------------------
            # Step 2: Attempt multi-DEX price comparison
            #
            # NOTE: This surfaces VIB-??? -- market.price_across_dexs() exists
            # in the MarketSnapshot API but requires _multi_dex_service to be
            # configured. The runner does NOT wire this up, so it always raises
            # ValueError("No multi-DEX service configured"). This is a gap.
            # ----------------------------------------------------------------
            spread_bps: int = 0
            selected_protocol: str = self._resolve_protocol()
            multi_dex_available = False

            try:
                amount_to_quote = (
                    self.swap_amount_usdc
                    if self.direction == "buy_weth"
                    else self.swap_amount_usdc / weth_price  # approx WETH amount
                )
                token_in = "USDC" if self.direction == "buy_weth" else "WETH"
                token_out = "WETH" if self.direction == "buy_weth" else "USDC"

                multi_result = market.price_across_dexs(token_in, token_out, amount_to_quote)
                multi_dex_available = True

                # Log all DEX quotes
                for dex_name, quote in multi_result.quotes.items():
                    logger.info(
                        f"  {dex_name}: {token_in} {amount_to_quote} -> {token_out} {quote.amount_out:.6f} "
                        f"(impact: {quote.price_impact_bps} bps)"
                    )

                spread_bps = multi_result.price_spread_bps
                logger.info(
                    f"Multi-DEX comparison: spread={spread_bps} bps, "
                    f"best={multi_result.best_quote.dex if multi_result.best_quote else 'N/A'}"
                )

                # If auto mode, use best DEX from comparison
                if self.force_protocol == "auto" and multi_result.best_quote:
                    selected_protocol = multi_result.best_quote.dex
                    logger.info(f"Auto-selected protocol: {selected_protocol}")

            except ValueError as e:
                logger.warning(
                    f"Multi-DEX price comparison unavailable: {e}. "
                    f"Falling back to oracle price + configured protocol={selected_protocol}. "
                    f"This is a framework gap -- market.price_across_dexs() needs _multi_dex_service configured."
                )
            except Exception as e:
                logger.warning(f"Unexpected error in multi-DEX comparison: {e}")

            # ----------------------------------------------------------------
            # Step 3: Check spread threshold
            # ----------------------------------------------------------------
            if self.spread_threshold_bps > 0 and not multi_dex_available:
                # Without multi-DEX data, can't verify spread -- proceed anyway
                logger.info(
                    f"Spread threshold={self.spread_threshold_bps} bps requested but "
                    f"multi-DEX comparison is unavailable. Proceeding with configured protocol."
                )

            if self.spread_threshold_bps > 0 and multi_dex_available and spread_bps < self.spread_threshold_bps:
                return Intent.hold(
                    reason=f"Spread {spread_bps} bps below threshold {self.spread_threshold_bps} bps -- waiting"
                )

            # ----------------------------------------------------------------
            # Step 4: Check balances
            # ----------------------------------------------------------------
            token_in = "USDC" if self.direction == "buy_weth" else "WETH"
            token_out = "WETH" if self.direction == "buy_weth" else "USDC"

            try:
                in_balance_result = market.balance(token_in)
                in_balance = (
                    in_balance_result.balance
                    if hasattr(in_balance_result, "balance")
                    else Decimal(str(in_balance_result))
                )

                swap_amount = self.swap_amount_usdc if token_in == "USDC" else self.swap_amount_usdc / weth_price
                if in_balance < swap_amount:
                    return Intent.hold(
                        reason=f"Insufficient {token_in}: {in_balance:.6f} < {swap_amount:.6f}"
                    )

                logger.info(f"Balance OK: {in_balance:.6f} {token_in} (need {swap_amount:.6f})")
            except (ValueError, KeyError):
                logger.warning("Balance check unavailable, proceeding with swap")
                swap_amount = self.swap_amount_usdc if token_in == "USDC" else self.swap_amount_usdc / weth_price

            # ----------------------------------------------------------------
            # Step 5: Execute swap on selected protocol
            # ----------------------------------------------------------------
            logger.info(
                f"Routing swap: {token_in} -> {token_out} via {selected_protocol} "
                f"(amount={swap_amount:.6f} {token_in}, slippage={self.max_slippage * 100:.1f}%, "
                f"multi_dex_available={multi_dex_available})"
            )

            return Intent.swap(
                from_token=token_in,
                to_token=token_out,
                amount=swap_amount,
                max_slippage=self.max_slippage,
                protocol=selected_protocol,
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _resolve_protocol(self) -> str:
        """Resolve the protocol to use for swap routing.

        Returns:
            Protocol name: "uniswap_v3" or "aerodrome"
        """
        if self.force_protocol in SUPPORTED_PROTOCOLS:
            return self.force_protocol

        # "auto" mode: default to uniswap_v3 without multi-DEX data
        logger.info("Protocol=auto, defaulting to uniswap_v3 (no multi-DEX service)")
        return "uniswap_v3"

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Log swap result."""
        if not success:
            logger.warning(f"Swap intent failed: {result}")
            return

        protocol = getattr(intent, "protocol", "unknown")
        logger.info(f"Swap executed successfully on {protocol}")

        if result and hasattr(result, "swap_amounts") and result.swap_amounts:
            amounts = result.swap_amounts
            logger.info(
                f"  Amount in:  {amounts.amount_in_decimal} {amounts.token_in}"
            )
            logger.info(
                f"  Amount out: {amounts.amount_out_decimal} {amounts.token_out}"
            )
