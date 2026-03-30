"""
===============================================================================
Uniswap V4 Hook-Aware LP Strategy — Dynamic Fees and Hook Discovery
===============================================================================

Demonstrates hook-aware LP management on Uniswap V4. This strategy:

1. Discovers hook capabilities from the hook contract address (14-bit bitmask)
2. Encodes typed hookData via HookDataEncoder for dynamic fee hooks
3. Passes hookData through protocol_params when creating LP positions
4. Uses wider ranges for hooked pools (hooks may modify swap behavior)
5. Warns when empty hookData might cause on-chain reverts

KEY V4 HOOK CONCEPTS:
- Hook addresses encode capabilities in the last 14 bits (CREATE2 mining)
- HookFlags decodes which callbacks are active (beforeSwap, afterSwap, etc.)
- HookDataEncoder provides typed encoding for hook-specific parameters
- Empty hookData on hooked pools causes on-chain revert — must encode properly
- Pools are identified by PoolKey = (currency0, currency1, fee, tickSpacing, hooks)

SCOPE: Hooks support is reference-only. The V4 hooks ecosystem is still
maturing and most mainnet V4 pools use the zero-address (no hooks). This
strategy demonstrates the hooks API surface for when hooked pools are common.

===============================================================================
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.uniswap_v4.hooks import (
    DynamicFeeHookEncoder,
    EmptyHookDataEncoder,
    HookFlags,
    discover_pool,
    warn_empty_hook_data,
)
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


@dataclass
class UniswapV4HooksConfig:
    """Configuration for hook-aware V4 LP strategy.

    Attributes:
        pool: Pool identifier "TOKEN0/TOKEN1/FEE"
        hook_address: Hook contract address (zero address for hookless pools)
        range_width_pct: Total width of price range (wider for hooked pools)
        amount0: Amount of token0 to provide
        amount1: Amount of token1 to provide
        fee_hint: Optional fee override for dynamic fee hooks (null = let hook decide)
    """

    pool: str = "WETH/USDC/3000"
    hook_address: str = "0x0000000000000000000000000000000000000000"
    range_width_pct: Decimal = Decimal("0.30")
    amount0: Decimal = Decimal("0.01")
    amount1: Decimal = Decimal("30")
    fee_hint: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "hook_address": self.hook_address,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "fee_hint": self.fee_hint,
        }

    def update(self, **kwargs: Any) -> Any:
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


@almanak_strategy(
    name="demo_uniswap_v4_hooks",
    description="Hook-aware V4 LP — dynamic fee hooks, HookFlags discovery, typed hookData encoding",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lp", "hooks", "uniswap-v4", "ethereum", "v4"],
    supported_chains=["ethereum", "arbitrum", "base"],
    supported_protocols=["uniswap_v4"],
    intent_types=["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "HOLD"],
    default_chain="ethereum",
)
class UniswapV4HooksStrategy(IntentStrategy[UniswapV4HooksConfig]):
    """Hook-aware Uniswap V4 LP strategy (reference implementation).

    Demonstrates the hooks API surface for future use when V4 hooked pools
    are common on mainnet. Currently uses zero-address (no hooks) by default.

    Hooks API demonstrated:
    - Discover hook capabilities via HookFlags.from_address()
    - Select appropriate HookDataEncoder based on capabilities
    - Pass encoded hookData via protocol_params={"hook_data": ...}
    - Use wider ranges for hooked pools (hooks can modify swap behavior)
    - Warn when empty hookData might revert
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000

        self.hook_address = self.config.hook_address
        self.range_width_pct = Decimal(str(self.config.range_width_pct))
        self.amount0 = Decimal(str(self.config.amount0))
        self.amount1 = Decimal(str(self.config.amount1))
        self.fee_hint = self.config.fee_hint

        # -- Hook discovery --
        # Decode hook capabilities from the hook address's last 14 bits
        self.hook_flags = HookFlags.from_address(self.hook_address)

        # Select encoder based on hook capabilities
        if self.hook_flags.is_empty:
            self._encoder = EmptyHookDataEncoder()
        elif self.hook_flags.before_swap:
            # Dynamic fee hook: has beforeSwap callback
            self._encoder = DynamicFeeHookEncoder()
        else:
            # Unknown hook type — use empty encoder with warning
            self._encoder = EmptyHookDataEncoder()
            if not self.hook_flags.is_empty:
                logger.warning(
                    f"Hook at {self.hook_address} has capabilities {self.hook_flags.active_flags} "
                    "but no specialized encoder. Using empty hookData — may revert."
                )

        # Discover pool details
        self._pool_discovery = None

        self._current_position_id: str | None = None
        self._load_position_from_state()

        logger.info(
            f"UniswapV4HooksStrategy initialized: pool={self.pool}, "
            f"hook={self.hook_address[:10]}..., "
            f"capabilities={self.hook_flags.active_flags or 'none'}, "
            f"encoder={self._encoder.hook_name}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Hook-aware LP decision."""
        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            current_price = token0_price_usd / token1_price_usd
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # If we have a position, monitor it
        if self._current_position_id:
            return Intent.hold(
                reason=f"V4 hooked position {self._current_position_id} active — monitoring"
            )

        # Check balances
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)
            bal0 = token0_bal.balance if hasattr(token0_bal, "balance") else token0_bal
            bal1 = token1_bal.balance if hasattr(token1_bal, "balance") else token1_bal

            if bal0 < self.amount0:
                return Intent.hold(reason=f"Insufficient {self.token0_symbol}: {bal0} < {self.amount0}")
            if bal1 < self.amount1:
                return Intent.hold(reason=f"Insufficient {self.token1_symbol}: {bal1} < {self.amount1}")
        except (ValueError, KeyError):
            logger.warning("Could not verify balances, proceeding anyway")

        logger.info("No V4 hooked position found — opening new LP position")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="Opening new V4 hooked LP position",
                strategy_id=self.strategy_id,
                details={
                    "action": "opening_v4_hooked_position",
                    "hook": self.hook_address,
                    "capabilities": self.hook_flags.active_flags,
                },
            )
        )
        return self._create_open_intent(current_price)

    # =========================================================================
    # INTENT CREATION WITH HOOK DATA
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create LP_OPEN intent with encoded hookData.

        For hooked pools, the hookData is passed via protocol_params.
        The compiler forwards it to the PositionManager's modifyLiquidities call.
        """
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        # Encode hookData using the selected encoder
        hook_data = self._encoder.encode(fee_hint=self.fee_hint)

        # Safety check: warn if empty hookData on hooked pool
        warning = warn_empty_hook_data(self.hook_flags, hook_data)
        if warning:
            logger.warning(warning)

        logger.info(
            f"LP_OPEN (V4 hooked): {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}], "
            f"hook={self._encoder.hook_name}, hookData={len(hook_data)} bytes"
        )

        # protocol_params carries hook-specific data to the compiler
        # Key "hooks" matches what UniswapV4Adapter.compile_lp_open_intent() reads
        protocol_params = {
            "hooks": self.hook_address,
            "hook_data": hook_data.hex() if hook_data else "",
            "hook_capabilities": self.hook_flags.active_flags,
        }

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v4",
            protocol_params=protocol_params,
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        """Create LP_CLOSE intent with hookData for hooked pools."""
        hook_data = self._encoder.encode(fee_hint=self.fee_hint)

        logger.info(f"LP_CLOSE (V4 hooked): position={position_id}")
        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v4",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success and intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None
            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"V4 hooked LP position opened: position_id={position_id}")
                self._save_position_to_state(position_id)

                # Run pool discovery now that we have a real position
                self._run_pool_discovery()
            else:
                logger.warning("V4 hooked LP opened but could not extract position ID")

    def _run_pool_discovery(self) -> None:
        """Discover pool details and log hook information."""
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            token0 = resolver.resolve_for_swap(self.token0_symbol, self.chain)
            token1 = resolver.resolve_for_swap(self.token1_symbol, self.chain)

            self._pool_discovery = discover_pool(
                token0=token0.address,
                token1=token1.address,
                fee=self.fee_tier,
                hooks=self.hook_address,
            )

            logger.info(
                f"Pool discovery: id={self._pool_discovery.pool_id[:18]}..., "
                f"hook_capabilities={self._pool_discovery.hook_flags.active_flags}"
            )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Pool discovery failed (non-critical): {e}")

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def _load_position_from_state(self) -> None:
        state = self.get_persistent_state()
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])

    def _save_position_to_state(self, position_id: int) -> None:
        self._current_position_id = str(position_id)

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
            if "position_opened_at" not in state:
                state["position_opened_at"] = datetime.now(UTC).isoformat()
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        if self._current_position_id:
            try:
                snapshot = self.create_market_snapshot()
                t0_price = snapshot.price(self.token0_symbol)
                t1_price = snapshot.price(self.token1_symbol)
            except Exception:  # noqa: BLE001
                t0_price = Decimal("0")
                t1_price = Decimal("0")

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self._current_position_id,
                    chain=self.chain,
                    protocol="uniswap_v4",
                    value_usd=self.amount0 * t0_price + self.amount1 * t1_price,
                    details={
                        "pool": self.pool,
                        "hook_address": self.hook_address,
                        "hook_capabilities": self.hook_flags.active_flags,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_v4_hooks"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if not self._current_position_id:
            return []

        logger.info(
            f"V4 hooked teardown: closing position {self._current_position_id} (mode={mode.value})"
        )
        return [
            Intent.lp_close(
                position_id=self._current_position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v4",
            )
        ]

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"V4 hooked LP teardown completed. Recovered: ${recovered_usd:,.2f}")
            self._current_position_id = None
        else:
            logger.warning(f"V4 hooked LP teardown failed. Partial recovery: ${recovered_usd:,.2f}")
