"""Vault-Wrapped Yield Rotation Strategy (Demo).

This demo shows how any IntentStrategy becomes vault-aware by adding a
``vault`` block to config.json. The framework handles valuation, settlement,
and epoch management transparently -- the strategy author only writes
trading logic.

How it works
------------
1. The ``vault`` config block tells the framework to manage an ERC-7540
   Lagoon vault around this strategy.
2. Before each ``decide()`` call, the framework checks whether a new
   settlement cycle is due (based on ``settlement_interval_minutes``).
3. If settlement is needed, the framework calls the default ``valuate()``
   method (sums token balances in USD), proposes a new total-assets value
   on-chain, and executes the deposit/redeem settlement.
4. The strategy never touches vault logic directly -- it just trades.

Trading logic
-------------
A simple RSI-based rotation between WETH and USDC:
- RSI below the oversold threshold  -> swap USDC to WETH (risk-on)
- RSI above the overbought threshold -> swap WETH to USDC (risk-off)
- Otherwise                          -> hold

Because this strategy only holds fungible tokens (no LP positions or
exotic derivatives), the default ``valuate()`` implementation -- which
sums ``balance.balance_usd`` for all known tokens -- works perfectly.
No custom ``valuate()`` override is needed.
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_vault_yield_rotator",
    description="Vault-wrapped yield rotation demo -- RSI-based swaps with transparent vault settlement",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "vault", "yield", "rsi", "erc-7540"],
    supported_chains=["ethereum", "arbitrum", "base"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class VaultYieldRotator(IntentStrategy):
    """RSI-based yield rotation strategy wrapped by a Lagoon ERC-7540 vault.

    The vault integration is entirely config-driven.  Adding or removing the
    ``vault`` block in config.json is the only change needed to enable or
    disable vault wrapping -- no code changes required.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        self.trade_size_usd = Decimal(str(get_config("trade_size_usd", "100")))
        self.rsi_period = int(get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(get_config("rsi_oversold", "35")))
        self.rsi_overbought = Decimal(str(get_config("rsi_overbought", "65")))
        self.max_slippage_bps = int(get_config("max_slippage_bps", 100))
        self.base_token = get_config("base_token", "WETH")
        self.quote_token = get_config("quote_token", "USDC")

        logger.info(
            f"VaultYieldRotator initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"pair={self.base_token}/{self.quote_token}, "
            f"RSI oversold={self.rsi_oversold} / overbought={self.rsi_overbought}"
        )

    # ------------------------------------------------------------------
    # decide() -- core trading logic
    # ------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Return a swap or hold intent based on RSI.

        The framework calls this method on every iteration.  If a vault
        settlement was needed, it already happened before this call.
        """
        try:
            base_price = market.price(self.base_token)

            try:
                rsi = market.rsi(self.base_token, period=self.rsi_period)
            except ValueError as exc:
                logger.warning(f"RSI unavailable: {exc}")
                return Intent.hold(reason="RSI data unavailable")

            try:
                quote_bal = market.balance(self.quote_token)
                base_bal = market.balance(self.base_token)
            except ValueError as exc:
                logger.warning(f"Balance unavailable: {exc}")
                return Intent.hold(reason="Balance data unavailable")

            slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

            # Oversold -> buy base token
            if rsi.value <= self.rsi_oversold:
                if quote_bal.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Oversold RSI={rsi.value:.1f} but insufficient {self.quote_token}"
                    )
                logger.info(f"BUY: RSI={rsi.value:.2f} < {self.rsi_oversold}")
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=slippage,
                    protocol="uniswap_v3",
                )

            # Overbought -> sell base token
            if rsi.value >= self.rsi_overbought:
                min_sell = self.trade_size_usd / base_price
                if base_bal.balance < min_sell:
                    return Intent.hold(
                        reason=f"Overbought RSI={rsi.value:.1f} but insufficient {self.base_token}"
                    )
                logger.info(f"SELL: RSI={rsi.value:.2f} > {self.rsi_overbought}")
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=slippage,
                    protocol="uniswap_v3",
                )

            # Neutral -> hold
            return Intent.hold(
                reason=f"RSI={rsi.value:.2f} in neutral zone [{self.rsi_oversold}-{self.rsi_overbought}]"
            )

        except Exception as exc:
            logger.exception(f"Error in decide(): {exc}")
            return Intent.hold(reason=f"Error: {exc}")
