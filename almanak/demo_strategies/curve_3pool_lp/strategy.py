"""
===============================================================================
TUTORIAL: Curve 3pool LP Strategy - 3-coin Stableswap Liquidity Provision
===============================================================================

This is a tutorial strategy demonstrating how to provide liquidity to Curve
Finance's canonical **3pool** (DAI/USDC/USDT) on Ethereum. Unlike every other
LP demo in this repo (uniswap_lp, traderjoe_lp), which manage a *2-token* pool,
3pool is a *3-coin* stableswap pool — so this demo genuinely exercises the
3-coin deposit path.

WHAT THIS STRATEGY DOES:
------------------------
1. Holds DAI + USDC + USDT in the wallet.
2. Deposits all three coins into Curve 3pool in a single LP_OPEN, funding one
   leg per pool coin index via the full per-coin allocation vector
   ``coin_amounts`` (DAI=idx0, USDC=idx1, USDT=idx2).
3. Receives a fungible 3Crv LP token (an ERC20, not an NFT).
4. Holds the position and is closed via the standard teardown signal
   (LP_CLOSE burns the 3Crv LP token, returning DAI + USDC + USDT).

WHY 3pool IS DIFFERENT (the point of this demo):
------------------------------------------------
- Uniswap V3 / TraderJoe V2 LP positions are concentrated, 2-token, and
  NFT/bin-based. 3pool is a flat-curve stableswap with THREE coins and a
  single fungible LP token.
- A real 3pool deposit funds all three legs. The legacy ``amount0``/``amount1``
  intent path can only express coins at indices 0 and 1 (USDT, idx 2, would be
  forced to zero). This demo instead uses ``coin_amounts`` — a length-``n_coins``
  vector that maps one amount to each pool coin index — so USDT (idx 2) is
  funded for real. See ``almanak/connectors/curve/compiler.py`` (``coin_amounts``
  branch) and ``Intent.lp_open(coin_amounts=...)``.

STABLESWAP EXPLAINED:
---------------------
Curve's StableSwap invariant blends the constant-sum (x+y+z=k) and
constant-product (x*y*z=k) curves. Near the 1:1:1 peg it behaves almost like
constant-sum, giving very low slippage between DAI/USDC/USDT — which is why
3pool is the base liquidity layer for most Curve metapools.

USAGE:
------
    # Copy the demo into the working directory and run on a managed Anvil fork
    almanak strat demo --name curve_3pool_lp
    cd curve_3pool_lp
    almanak strat run --network anvil

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import AnyIntent, Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================


@dataclass
class Curve3poolLPConfig:
    """Configuration for the Curve 3pool LP strategy.

    This dataclass loads all config fields from ``config.json``.
    """

    # Runtime config (used by CLI if no config.json)
    chain: str = "ethereum"
    network: str = "anvil"

    # Strategy-specific config.
    # ``pool`` is the Curve pool nickname registered in the curve adapter's
    # CURVE_POOLS registry. "3pool" resolves to the canonical Ethereum
    # DAI/USDC/USDT pool (0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7).
    pool: str = "3pool"

    # Per-coin deposit amounts (human units), one per pool coin index:
    #   idx0 = DAI (18 dec), idx1 = USDC (6 dec), idx2 = USDT (6 dec)
    amount_dai: Decimal = field(default_factory=lambda: Decimal("100"))
    amount_usdc: Decimal = field(default_factory=lambda: Decimal("100"))
    amount_usdt: Decimal = field(default_factory=lambda: Decimal("100"))

    # Minimum total inventory (USD) required to open a position. Below this we
    # HOLD rather than open a dust position.
    min_position_usd: Decimal = field(default_factory=lambda: Decimal("100"))

    # Force "open" or "close" for testing / sidecar single-iteration runs.
    force_action: str = ""

    def __post_init__(self):
        """Coerce string/int/float config values to Decimal.

        config.json may supply these as strings ("100") or JSON numbers
        (100 / 100.0); convert any non-Decimal via str() so later Decimal
        arithmetic in decide() never hits a float/int TypeError.
        """
        for attr in ("amount_dai", "amount_usdc", "amount_usdt", "min_position_usd"):
            value = getattr(self, attr)
            if value is not None and not isinstance(value, Decimal):
                setattr(self, attr, Decimal(str(value)))

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "chain": self.chain,
            "network": self.network,
            "pool": self.pool,
            "amount_dai": str(self.amount_dai),
            "amount_usdc": str(self.amount_usdc),
            "amount_usdt": str(self.amount_usdt),
            "min_position_usd": str(self.min_position_usd),
            "force_action": self.force_action,
        }

    def update(self, **kwargs: Any) -> Any:
        """Update configuration values."""

        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list = field(default_factory=list)

        updated = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)
        return UpdateResult(success=True, updated_fields=updated)


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    # Unique identifier - used to run via CLI
    name="demo_curve_3pool_lp",
    # Human-readable description
    description="Tutorial LP strategy - provides 3-coin liquidity to Curve 3pool (DAI/USDC/USDT) on Ethereum",
    # Semantic versioning
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "lp", "liquidity", "curve", "stableswap", "3pool", "ethereum"],
    # Curve 3pool is registered on Ethereum (DAI/USDC/USDT). Optimism/Polygon
    # 3pools use USDC.e instead of native USDC; Ethereum is the canonical, best-
    # tested deployment and is where all three coins are fundable on a managed
    # Anvil fork.
    supported_chains=["ethereum"],
    # Protocols this strategy interacts with
    supported_protocols=["curve"],
    # Types of intents this strategy may return
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="ethereum",
    quote_asset="USD",
)
class Curve3poolLPStrategy(IntentStrategy[Curve3poolLPConfig]):
    """A Curve 3pool stableswap LP strategy for educational purposes.

    This strategy demonstrates:
    - How to deposit into a 3-coin Curve pool using the full ``coin_amounts``
      allocation vector (one amount per pool coin index).
    - How fungible LP tokens (3Crv) differ from NFT positions.
    - How to close a fungible LP position on teardown.

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Curve pool nickname (default "3pool")
    - amount_dai / amount_usdc / amount_usdt: Per-coin deposit amounts
    - min_position_usd: Minimum total inventory (USD) to open a position
    - force_action: Force "open" or "close" for testing

    Example Config:
    ---------------
    {
        "pool": "3pool",
        "amount_dai": "100",
        "amount_usdc": "100",
        "amount_usdt": "100",
        "min_position_usd": "100",
        "force_action": ""
    }
    """

    # The three coins of Curve 3pool, in pool-coin-index order. This ordering
    # is load-bearing: ``coin_amounts[i]`` maps to pool coin index ``i``, so it
    # MUST match the registry order (DAI=0, USDC=1, USDT=2) in
    # ``almanak/connectors/curve/adapter.py`` CURVE_POOLS["ethereum"]["3pool"].
    COINS: tuple[str, ...] = ("DAI", "USDC", "USDT")

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """Initialize the LP strategy with configuration.

        The base class handles standard parameters:
        - self.config: Strategy configuration (Curve3poolLPConfig)
        - self.chain: Blockchain to operate on
        - self.wallet_address: Wallet for transactions
        """
        super().__init__(*args, **kwargs)

        # Pool nickname resolved by the Curve compiler against CURVE_POOLS.
        self.pool = self.config.pool

        # Per-coin deposit amounts, aligned to COINS / pool coin index.
        self.amount_dai = self.config.amount_dai
        self.amount_usdc = self.config.amount_usdc
        self.amount_usdt = self.config.amount_usdt

        # Minimum total inventory (USD) required to (re)open a position.
        # __post_init__ has already coerced this to a Decimal on the typed config.
        self.min_position_usd = self.config.min_position_usd

        # Force action for testing ("open" or "close").
        self.force_action = str(self.config.force_action).lower()

        # Internal state: whether we currently hold a 3Crv LP position. Curve
        # LP tokens are fungible (no NFT id), so we track presence as a flag
        # and let the compiler query the on-chain LP token balance at close
        # time (position_id = LP token address). This is robust across restarts.
        self._has_position: bool = False

        logger.info(
            "Curve3poolLPStrategy initialized: pool=%s, deposit=%s DAI + %s USDC + %s USDT, min_position_usd=%s",
            self.pool,
            self.amount_dai,
            self.amount_usdc,
            self.amount_usdt,
            self.min_position_usd,
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make an LP decision based on market conditions.

        Decision Flow:
        --------------
        1. If force_action is set, execute that action.
        2. If a position is already open, HOLD (teardown closes it).
        3. If no position and inventory is sufficient, open a 3-coin position.
        4. Otherwise HOLD.

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: LP_OPEN, LP_CLOSE, or HOLD
        """
        # =================================================================
        # STEP 1: Handle forced actions (for testing / sidecar runs)
        # =================================================================
        if self.force_action == "open":
            logger.info("Forced action: OPEN Curve 3pool position")
            return self._create_open_intent()

        if self.force_action == "close":
            logger.info("Forced action: CLOSE Curve 3pool position")
            return self._create_close_intent()

        # =================================================================
        # STEP 2: Position already open -> HOLD
        # =================================================================
        # 3pool is a passive stableswap LP: it earns trading fees over time and
        # has no rebalance lever, so once opened we simply hold until teardown.
        if self._has_position:
            return Intent.hold(reason=f"Curve 3pool position open on {self.pool}; holding for fees")

        # =================================================================
        # STEP 3: No position -> check inventory, then open 3-coin position
        # =================================================================
        # Verify we actually hold each coin before depositing. A 3pool deposit
        # that names an amount we don't hold would revert on-chain, so we gate
        # the open on real wallet balances (risk control: never open a position
        # we can't fund).
        # AttributeError guards the case where market.balance(coin) returns None
        # (token missing from the snapshot) — accessing .balance_usd would crash.
        try:
            balances_usd = {coin: Decimal(str(market.balance(coin).balance_usd)) for coin in self.COINS}
            balances_token = {coin: Decimal(str(market.balance(coin).balance)) for coin in self.COINS}
        except (ValueError, KeyError, AttributeError) as e:
            return Intent.hold(reason=f"Cannot read stablecoin balances: {e}")

        total_usd = sum(balances_usd.values())
        if total_usd < self.min_position_usd:
            return Intent.hold(
                reason=f"Total stablecoin inventory ${total_usd:.2f} below min_position_usd "
                f"${self.min_position_usd:.2f}"
            )

        # Risk control: only deposit what we actually hold per coin. We deposit
        # the configured amount for each coin, capped at ~99% of the wallet
        # balance (small buffer for rounding/dust) so the deposit never exceeds
        # inventory.
        deposits = {
            "DAI": min(self.amount_dai, balances_token["DAI"] * Decimal("0.99")),
            "USDC": min(self.amount_usdc, balances_token["USDC"] * Decimal("0.99")),
            "USDT": min(self.amount_usdt, balances_token["USDT"] * Decimal("0.99")),
        }

        # A genuine 3-coin deposit requires every leg to be positive. If any
        # coin is missing, HOLD rather than silently collapse to a 2-coin
        # deposit — that would defeat the purpose of this demo.
        missing = [coin for coin in self.COINS if deposits[coin] <= 0]
        if missing:
            return Intent.hold(reason=f"Missing inventory for 3-coin deposit: {missing}")

        logger.info("No position found - opening 3-coin Curve 3pool position")
        return self._create_open_intent(
            amount_dai=deposits["DAI"],
            amount_usdc=deposits["USDC"],
            amount_usdt=deposits["USDT"],
        )

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(
        self,
        amount_dai: Decimal | None = None,
        amount_usdc: Decimal | None = None,
        amount_usdt: Decimal | None = None,
    ) -> Intent:
        """Create an LP_OPEN intent depositing all three coins into 3pool.

        This is the heart of the demo: instead of the legacy ``amount0`` /
        ``amount1`` two-slot path (which can only fund coin indices 0 and 1),
        we build the full ``coin_amounts`` vector — one amount per pool coin
        index — so USDT (index 2) is funded for real. The Curve compiler maps
        ``coin_amounts[i]`` directly to pool coin index ``i``.

        Parameters:
            amount_dai/amount_usdc/amount_usdt: Optional per-coin deposit
                amounts; fall back to the configured amounts (force_action /
                initial open).

        Returns:
            LPOpenIntent ready for compilation.
        """
        amount_dai = self.amount_dai if amount_dai is None else amount_dai
        amount_usdc = self.amount_usdc if amount_usdc is None else amount_usdc
        amount_usdt = self.amount_usdt if amount_usdt is None else amount_usdt

        # Pool-coin-aligned allocation vector: [DAI(idx0), USDC(idx1), USDT(idx2)].
        coin_amounts = [amount_dai, amount_usdc, amount_usdt]

        logger.info(
            "💧 Curve LP_OPEN (3-coin): %s + %s + %s into %s",
            format_token_amount_human(amount_dai, "DAI"),
            format_token_amount_human(amount_usdc, "USDC"),
            format_token_amount_human(amount_usdt, "USDT"),
            self.pool,
        )

        return Intent.lp_open(
            pool=self.pool,
            coin_amounts=coin_amounts,
            protocol="curve",
        )

    def _create_close_intent(self) -> Intent:
        """Create an LP_CLOSE intent to burn the fungible 3Crv LP token.

        Curve LP positions are fungible ERC20 LP tokens. We pass the pool's LP
        token CONTRACT ADDRESS as ``position_id``; the Curve compiler then
        queries the wallet's on-chain LP balance and burns all of it,
        returning DAI + USDC + USDT proportionally. Resolving the burn amount
        at compile time (rather than tracking the minted amount in memory) is
        robust across process restarts.
        """
        lp_token = self._lp_token_address()

        logger.info("💧 Curve LP_CLOSE: burning 3Crv LP token %s", lp_token)

        return Intent.lp_close(
            position_id=lp_token,
            pool=self.pool,
            collect_fees=True,
            protocol="curve",
        )

    def _lp_token_address(self) -> str:
        """Resolve the 3Crv LP token contract address for this pool.

        Reads it from the Curve connector's CURVE_POOLS registry (no on-chain
        call). Falls back to the canonical Ethereum 3Crv token address if the
        pool nickname is not found, so teardown can always proceed.
        """
        from almanak.connectors.curve.adapter import CURVE_POOLS

        pool_data = CURVE_POOLS.get(self.chain, {}).get(self.pool)
        if pool_data and pool_data.get("lp_token"):
            return str(pool_data["lp_token"])
        # Canonical Ethereum 3Crv LP token.
        return "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Called after an intent is executed; track position presence."""
        if success and intent.intent_type.value == "LP_OPEN":
            self._has_position = True
            logger.info("Curve 3pool position opened successfully")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"Curve 3pool 3-coin position opened on {self.pool}",
                    deployment_id=self.deployment_id,
                    details={"pool": self.pool, "coins": list(self.COINS)},
                )
            )
        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Curve 3pool position closed successfully")
            self._has_position = False

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist position presence so teardown survives process restarts."""
        parent_get_state = getattr(super(), "get_persistent_state", None)
        state = parent_get_state() if callable(parent_get_state) else {}
        state["has_position"] = self._has_position
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore position presence saved by get_persistent_state()."""
        parent_load_state = getattr(super(), "load_persistent_state", None)
        if callable(parent_load_state):
            parent_load_state(state)
        if state and "has_position" in state:
            self._has_position = bool(state["has_position"])
            if self._has_position:
                logger.info("Restored Curve 3pool position presence from state")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "demo_curve_3pool_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "amount_dai": str(self.amount_dai),
                "amount_usdc": str(self.amount_usdc),
                "amount_usdt": str(self.amount_usdt),
            },
            "state": {
                "has_position": self._has_position,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._has_position:
            # Estimated value: the three coins are USD stablecoins (~$1 each),
            # so the deposited notional is a good estimate of position value.
            estimated_value = self.amount_dai + self.amount_usdc + self.amount_usdt

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._lp_token_address(),
                    chain=self.chain,
                    protocol="curve",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "coins": list(self.COINS),
                        "amount_dai": str(self.amount_dai),
                        "amount_usdc": str(self.amount_usdc),
                        "amount_usdt": str(self.amount_usdt),
                        "lp_token": self._lp_token_address(),
                    },
                )
            )

        total_value = sum(p.value_usd for p in positions)

        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[AnyIntent]:
        """Generate intents to close the 3pool LP position."""
        intents: list[AnyIntent] = []

        if self._has_position:
            logger.info("Generating teardown intent for Curve 3pool position (mode=%s)", mode.value)
            intents.append(self._create_close_intent())

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Curve3poolLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {Curve3poolLPStrategy.STRATEGY_NAME}")
    print(f"Version: {Curve3poolLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {Curve3poolLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {Curve3poolLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {Curve3poolLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {Curve3poolLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  almanak strat demo --name curve_3pool_lp && cd curve_3pool_lp")
    print("  almanak strat run --network anvil")
