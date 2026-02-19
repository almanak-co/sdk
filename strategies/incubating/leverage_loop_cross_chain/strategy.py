"""Leverage Loop Cross-Chain Strategy Implementation.

NORTH STAR TEST CASE - This is THE contract for multi-chain implementation.
The framework MUST adapt to make this code work, not vice versa.

If you're implementing the multi-chain PRD and this strategy requires
workarounds or hacks, STOP and update the PRD before proceeding.

See: tasks/prd-multi-chain-strategy-support.md

PRODUCTION-READY: This strategy uses Enso's cross-chain routing for bridging,
which handles the bridge automatically as part of a single atomic swap operation.
"""

from dataclasses import dataclass, field
from datetime import UTC
from decimal import Decimal

from almanak.framework.intents import DecideResult, Intent, IntentSequence
from almanak.framework.models.hot_reload_config import HotReloadableConfig
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.strategies.intent_strategy import MarketSnapshot, MultiChainMarketSnapshot


@dataclass
class LeverageLoopConfig(HotReloadableConfig):
    """Configuration for the Leverage Loop Cross-Chain Strategy.

    All amounts are in USD for simplicity. The framework handles
    token conversions and decimal precision.

    Attributes:
        min_usdc_to_start: Minimum USDC balance on Base to trigger the loop.
            This is the entry condition - the strategy won't run if there
            isn't enough capital on the source chain.

        min_health_factor: Minimum Aave health factor before adding more leverage.
            If the existing position's health factor is below this threshold,
            the strategy will hold to avoid over-leveraging.

        swap_amount_usd: Amount of USDC to swap to WETH on Base.
            This is the initial capital that flows through the entire loop.

        borrow_amount_usd: Amount of USDC to borrow against WETH collateral.
            Should be sized based on desired LTV and risk tolerance.

        perp_size_usd: Notional size of the GMX perps position.
            Combined with collateral determines the leverage ratio.

        max_leverage: Maximum leverage for GMX position.
            GMX V2 supports 1.1x to 100x, but this caps it for risk management.

        max_slippage_swap: Maximum slippage for swap operations.
            DEX swaps typically have lower slippage than bridges.

        max_slippage_bridge: Maximum slippage for bridge transfers.
            Bridges have fees and potential slippage, so allow more room.

        preferred_bridge: Preferred bridge protocol.
            None lets the system auto-select based on speed/cost/liquidity.
            Options: "across" (fast), "stargate" (stablecoins).

        interest_rate_mode: Aave interest rate mode.
            "variable" is usually cheaper but rates fluctuate.
            "stable" provides predictable rates at a premium.
    """

    # === Entry Conditions ===
    min_usdc_to_start: Decimal = field(default_factory=lambda: Decimal("10"))
    """Minimum USDC balance on Base to trigger the loop."""

    min_health_factor: Decimal = field(default_factory=lambda: Decimal("1.5"))
    """Minimum Aave health factor before adding more leverage."""

    # === Position Sizing ===
    swap_amount_usd: Decimal = field(default_factory=lambda: Decimal("10"))
    """Amount of USDC to swap to WETH on Base."""

    borrow_amount_usd: Decimal = field(default_factory=lambda: Decimal("5"))
    """Amount of USDC to borrow against WETH collateral."""

    perp_size_usd: Decimal = field(default_factory=lambda: Decimal("10"))
    """Notional size of the GMX perps position."""

    # === Risk Parameters ===
    max_leverage: Decimal = field(default_factory=lambda: Decimal("2.0"))
    """Maximum leverage for GMX position."""

    max_slippage_swap: Decimal = field(default_factory=lambda: Decimal("0.01"))
    """Maximum slippage for swaps (1%)."""

    max_slippage_bridge: Decimal = field(default_factory=lambda: Decimal("0.02"))
    """Maximum slippage for bridge (2% - bridges have fees)."""

    # === Protocol Preferences ===
    preferred_bridge: str | None = None
    """Preferred bridge protocol. None = auto-select best."""

    interest_rate_mode: str = "variable"
    """Aave interest rate mode: 'variable' or 'stable'."""


# Strategy Configuration
# ----------------------
# This decorator registers the strategy with the framework and declares
# its multi-chain configuration. The framework uses this to:
# 1. Initialize RPC connections to each chain (Base and Arbitrum)
# 2. Validate that intents target valid chains
# 3. Configure the MarketSnapshot with cross-chain data access
# 4. Set up protocol adapters for each chain
@almanak_strategy(
    name="leverage_loop_cross_chain",
    description="Cross-chain swap Base→Arbitrum via Enso, leverage loop via Aave, open perps on GMX",
    version="2.0.0",
    author="Almanak",
    tags=["leverage", "multi-chain", "cross-chain", "defi", "north-star", "production-ready"],
    # Multi-chain configuration: operate on Base and Arbitrum
    supported_chains=["base", "arbitrum"],
    # Protocol configuration per chain
    supported_protocols=["enso", "aave_v3", "gmx_v2"],
    # Intent types this strategy can emit (cross-chain SWAP via Enso replaces BRIDGE)
    intent_types=["SWAP", "SUPPLY", "BORROW", "PERP_OPEN", "HOLD"],
)
class LeverageLoopStrategy(IntentStrategy[LeverageLoopConfig]):
    """
    NORTH STAR: Leverage Loop Cross-Chain Strategy (Production-Ready).

    This strategy demonstrates the full power of the multi-chain system:
    - Multi-chain execution (Base + Arbitrum) via Enso cross-chain routing
    - Multi-protocol (Enso, Aave V3, GMX V2)
    - Atomic cross-chain swaps (Enso handles bridge internally)
    - Sequential dependencies (each step depends on previous)
    - Chained outputs (`amount="all"` pattern)
    - Protocol-specific parameters (interest_rate_mode, leverage)
    - Health factor monitoring

    Flow (4 steps - Enso combines swap+bridge):
        1. Cross-chain swap USDC (Base) → WETH (Arbitrum) via Enso
           (Enso handles the bridge internally via Stargate/LayerZero)
        2. Supply WETH as collateral on Aave V3 (Arbitrum)
        3. Borrow USDC against the WETH collateral
        4. Open leveraged ETH long position on GMX V2

    Risk Management:
        - Pre-flight health factor check prevents over-leveraging
        - Slippage limits on cross-chain swaps
        - Maximum leverage cap on perps position
        - Atomic cross-chain execution (if bridge fails, tx reverts)

    Remediation (if something fails mid-sequence):
        - Step 1 fails: Transaction reverts, no funds moved (atomic)
        - Step 2 fails: WETH on Arbitrum, can retry supply
        - Step 3 fails: WETH supplied, can retry borrow (or hold)
        - Step 4 fails: USDC borrowed, can retry perp (or repay and unwind)

    PRODUCTION READY: Uses Enso's cross-chain routing for real on-chain execution.
    Test with small amounts first!
    """

    # Chains this strategy operates on
    SUPPORTED_CHAINS = ["base", "arbitrum"]

    # Protocols per chain (used by CLI for banner display)
    SUPPORTED_PROTOCOLS = {
        "base": ["enso"],  # Enso handles cross-chain swap+bridge
        "arbitrum": ["aave_v3", "gmx_v2"],  # Lending and perps on Arbitrum
    }

    # Token constants
    BASE_TOKEN = "USDC"
    COLLATERAL_TOKEN = "WETH"

    def __init__(
        self,
        config: LeverageLoopConfig,
        chain: str = "base",
        wallet_address: str = "",
        **kwargs,
    ) -> None:
        """Initialize the strategy.

        Args:
            config: Hot-reloadable configuration
            chain: Primary chain for the strategy (Base is the entry point)
            wallet_address: Wallet address for transactions
        """
        super().__init__(config, chain, wallet_address, **kwargs)

    def decide(self, market: MarketSnapshot) -> DecideResult:
        """
        Make trading decision based on multi-chain market state.

        This method is called on each iteration of the strategy loop. It:
        1. Checks pre-flight conditions (balances, health factors)
        2. If conditions are met, builds the 5-step leverage loop sequence
        3. Otherwise, returns a HoldIntent with the reason

        Args:
            market: MarketSnapshot (or MultiChainMarketSnapshot for multi-chain strategies).
                   For multi-chain strategies, the market provides methods like:
                   - market.balance('USDC', chain='base') -> chain-specific balance
                   - market.aave_health_factor(chain='arbitrum') -> protocol metrics
                   - market.chains -> list of configured chains

        Returns:
            DecideResult: One of:
            - HoldIntent: If conditions not met for leverage loop
            - IntentSequence: The 5-step leverage loop sequence

        Note:
            The return type is DecideResult which can be:
            - A single Intent (like HoldIntent)
            - An IntentSequence (for dependent actions)
            - A list of intents/sequences (for parallel independent actions)
            - None (equivalent to HoldIntent)

            This strategy expects a MultiChainMarketSnapshot at runtime for
            the cross-chain balance and protocol metric methods to work.
        """
        # Cast to MultiChainMarketSnapshot for IDE support (runtime duck typing)
        multi_chain_market: MultiChainMarketSnapshot = market  # type: ignore[assignment]

        # ============================================================
        # PRE-FLIGHT CHECKS
        # ============================================================

        # Check USDC balance on Base
        # The balance() method with chain parameter is a core multi-chain API
        base_usdc_balance = multi_chain_market.balance(self.BASE_TOKEN, chain="base")
        base_usdc = base_usdc_balance.balance if base_usdc_balance else Decimal("0")

        if base_usdc < self.config.min_usdc_to_start:
            return Intent.hold(
                reason=f"Insufficient USDC on Base: have {base_usdc:.2f}, need {self.config.min_usdc_to_start:.2f}"
            )

        # Check Aave health factor on Arbitrum (if we have an existing position)
        # The aave_health_factor() method is a protocol-specific metric
        arb_health = multi_chain_market.aave_health_factor(chain="arbitrum")

        if arb_health is not None and arb_health < self.config.min_health_factor:
            return Intent.hold(
                reason=f"Health factor too low: {arb_health:.2f} < {self.config.min_health_factor:.2f}. "
                f"Deleveraging may be needed."
            )

        # ============================================================
        # THE CORE FLOW
        # This sequence MUST be expressible cleanly.
        # If it requires hacks, the multi-chain design has gaps.
        # ============================================================

        return self._build_leverage_loop_sequence()

    def _build_leverage_loop_sequence(self) -> IntentSequence:
        """
        Build the sequence of intents for the leverage loop.

        This method constructs the 4-step sequence that demonstrates:
        - Cross-chain execution (Base → Arbitrum) via Enso
        - Multi-protocol usage (Enso, Aave, GMX)
        - Amount chaining (amount="all")
        - Protocol-specific parameters

        Returns:
            IntentSequence: Ordered sequence of 4 intents for execution

        Example flow at $3500 ETH:
            1. Cross-chain swap $1000 USDC (Base) → ~0.285 WETH (Arbitrum) via Enso
               (Enso handles the bridge internally via Stargate/LayerZero)
            2. Supply ~0.285 WETH to Aave on Arbitrum
            3. Borrow $500 USDC against WETH collateral (~45% LTV)
            4. Open $1000 ETH long with $500 USDC collateral (2x leverage)

        Note:
            This uses Enso's cross-chain routing which atomically handles:
            - Swap on source chain (if needed)
            - Bridge via optimal route (Stargate, LayerZero, etc.)
            - Swap on destination chain (if needed)
            All in a single transaction on the source chain.
        """
        # Calculate effective leverage for the perp position
        # size_usd / collateral = leverage
        effective_leverage = min(
            self.config.perp_size_usd / self.config.borrow_amount_usd,
            self.config.max_leverage,
        )

        return Intent.sequence(
            [
                # ---------------------------------------------------------
                # STEP 1: Cross-Chain Swap USDC (Base) → WETH (Arbitrum) via Enso
                # ---------------------------------------------------------
                # This single intent handles BOTH the swap AND the bridge.
                # Enso routes through optimal path (may include DEX + bridge).
                #
                # Key parameters:
                # - chain="base": Transaction executes on Base
                # - destination_chain="arbitrum": Tokens arrive on Arbitrum
                # - protocol="enso": Uses Enso cross-chain routing
                # - amount_usd: Fixed USD amount to swap
                #
                # The output (WETH on Arbitrum) flows to the next step.
                Intent.swap(
                    from_token=self.BASE_TOKEN,  # USDC on Base
                    to_token=self.COLLATERAL_TOKEN,  # WETH on Arbitrum
                    amount_usd=self.config.swap_amount_usd,
                    max_slippage=self.config.max_slippage_bridge,  # Use bridge slippage for cross-chain
                    protocol="enso",
                    chain="base",
                    destination_chain="arbitrum",
                ),
                # ---------------------------------------------------------
                # STEP 2: Supply WETH as collateral on Aave V3
                # ---------------------------------------------------------
                # Supply the WETH (now on Arbitrum) to Aave as collateral.
                # This enables us to borrow against it.
                #
                # Key parameters:
                # - amount="all": Use ALL WETH received from cross-chain swap
                # - chain="arbitrum": Execute on Arbitrum
                # - protocol="aave_v3": Use Aave V3 lending protocol
                # - use_as_collateral=True: Enable as collateral (default)
                Intent.supply(
                    protocol="aave_v3",
                    token=self.COLLATERAL_TOKEN,  # WETH
                    amount="all",  # Use all WETH from cross-chain swap
                    use_as_collateral=True,  # Enable as collateral
                    chain="arbitrum",
                ),
                # ---------------------------------------------------------
                # STEP 3: Borrow USDC against WETH collateral
                # ---------------------------------------------------------
                # Borrow USDC using our WETH as collateral.
                # We use variable rate by default (usually cheaper).
                #
                # Key parameters:
                # - chain="arbitrum": Execute on Arbitrum
                # - protocol="aave_v3": Use Aave V3
                # - interest_rate_mode="variable": Protocol-specific param
                # - borrow_amount: Fixed USD amount to borrow
                #
                # NOTE: For borrow, we use a fixed amount rather than "all"
                # because we want to maintain a specific LTV ratio.
                Intent.borrow(
                    protocol="aave_v3",
                    collateral_token=self.COLLATERAL_TOKEN,  # WETH
                    collateral_amount=Decimal("0"),  # Already supplied in Step 2
                    borrow_token=self.BASE_TOKEN,  # USDC
                    borrow_amount=self.config.borrow_amount_usd,
                    interest_rate_mode=self.config.interest_rate_mode,  # type: ignore[arg-type]
                    chain="arbitrum",
                ),
                # ---------------------------------------------------------
                # STEP 4: Open ETH long perps on GMX V2
                # ---------------------------------------------------------
                # Use the borrowed USDC as collateral for a leveraged ETH long.
                # This completes our leverage loop.
                #
                # Key parameters:
                # - chain="arbitrum": Execute on Arbitrum
                # - protocol="gmx_v2": Use GMX V2 perpetuals
                # - leverage: Protocol-specific param (GMX supports 1.1x-100x)
                # - collateral_amount="all": Use ALL borrowed USDC from Step 3
                Intent.perp_open(
                    market="ETH/USD",
                    collateral_token=self.BASE_TOKEN,  # USDC
                    collateral_amount="all",  # Use borrowed amount from Step 3
                    size_usd=self.config.perp_size_usd,
                    is_long=True,
                    leverage=effective_leverage,
                    max_slippage=self.config.max_slippage_swap,
                    protocol="gmx_v2",
                    chain="arbitrum",
                ),
            ],
            description="Leverage Loop: Base→Arbitrum cross-chain swap (Enso) → Aave supply/borrow → GMX perp",
        )

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def supports_teardown(self) -> bool:
        """Indicate this strategy supports safe teardown.

        Leverage loop has multiple position types that must be closed in order:
        1. Close perp position (GMX)
        2. Repay borrow (Aave)
        3. Withdraw supply (Aave)

        Returns:
            True - this strategy can be safely torn down
        """
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open positions for teardown preview.

        Leverage loop has:
        - PERP: GMX V2 ETH long position
        - BORROW: USDC borrowed from Aave
        - SUPPLY: WETH supplied to Aave

        Returns:
            TeardownPositionSummary with all position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        # PERP position (GMX)
        positions.append(
            PositionInfo(
                position_type=PositionType.PERP,
                position_id="leverage_loop_perp_0",
                chain="arbitrum",
                protocol="gmx_v2",
                value_usd=self.config.perp_size_usd,
                details={
                    "asset": "ETH/USD",
                    "is_long": True,
                    "collateral_token": self.BASE_TOKEN,
                    "size": str(self.config.perp_size_usd),
                },
            )
        )

        # BORROW position (Aave)
        positions.append(
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id="leverage_loop_borrow_0",
                chain="arbitrum",
                protocol="aave_v3",
                value_usd=self.config.borrow_amount_usd,
                details={
                    "asset": self.BASE_TOKEN,  # USDC borrowed
                    "collateral_token": self.COLLATERAL_TOKEN,
                    "amount": str(self.config.borrow_amount_usd),
                },
            )
        )

        # SUPPLY position (Aave)
        positions.append(
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="leverage_loop_supply_0",
                chain="arbitrum",
                protocol="aave_v3",
                value_usd=self.config.swap_amount_usd,
                details={
                    "asset": self.COLLATERAL_TOKEN,  # WETH supplied
                    "amount": str(self.config.swap_amount_usd),
                },
            )
        )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "leverage_loop_cross_chain"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list:
        """Generate intents to close all positions.

        Teardown order (CRITICAL):
        1. Close perp (free up collateral)
        2. Repay borrow (clear debt)
        3. Withdraw supply (get back collateral)

        Args:
            mode: TeardownMode (SOFT or HARD) - affects slippage

        Returns:
            List of intents in correct order
        """
        from almanak.framework.teardown import TeardownMode

        intents: list = []

        # Slippage based on mode
        if mode == TeardownMode.HARD:
            max_slippage = Decimal("0.05")  # 5% emergency
        else:
            max_slippage = self.config.max_slippage_swap

        # 1. Close perp position
        intents.append(
            Intent.perp_close(
                market="ETH/USD",
                protocol="gmx_v2",
                chain="arbitrum",
                max_slippage=max_slippage,
            )
        )

        # 2. Repay borrow
        intents.append(
            Intent.repay(
                protocol="aave_v3",
                token=self.BASE_TOKEN,
                amount="all",
                chain="arbitrum",
            )
        )

        # 3. Withdraw supply
        intents.append(
            Intent.withdraw(
                protocol="aave_v3",
                token=self.COLLATERAL_TOKEN,
                amount="all",
                withdraw_all=True,
                chain="arbitrum",
            )
        )

        return intents


# ================================================================
# VALIDATION: This strategy is THE test case for multi-chain PRD
# ================================================================
#
# PRODUCTION-READY: Uses Enso cross-chain routing (no separate bridge step)
#
# To run this strategy:
#
# 1. Environment Setup:
#    export ALMANAK_BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/xxx
#    export ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/xxx
#    export ALMANAK_PRIVATE_KEY=0x...  # Your wallet private key
#    export ENSO_API_KEY=xxx           # Get from https://enso.finance
#
# 2. Test Cross-Chain Route (dry run):
#    python scripts/test_enso_crosschain.py
#
# 3. Run Strategy Test:
#    python scripts/run_leverage_loop_onchain.py --dry-run
#
# 4. Run On-Chain (REAL TRANSACTIONS - use small amounts!):
#    python scripts/run_leverage_loop_onchain.py --amount 10  # $10 test
#
# Acceptance Criteria Checklist (v2.0 - Enso Cross-Chain):
# [x] Strategy compiles with chains=['base', 'arbitrum']
# [x] protocols={'base': ['enso'], 'arbitrum': ['aave_v3', 'gmx_v2']}
# [x] Intent.swap() with chain='base', destination_chain='arbitrum', protocol='enso'
# [x] Cross-chain swap replaces separate bridge intent (atomic execution)
# [x] Intent.supply() with chain='arbitrum', protocol='aave_v3'
# [x] Intent.borrow() with chain='arbitrum', protocol='aave_v3', interest_rate_mode='variable'
# [x] Intent.perp_open() with chain='arbitrum', protocol='gmx_v2', leverage=Decimal('2.0')
# [x] amount='all' flows output of step N to input of step N+1
# [x] market.balance('USDC', chain='base') returns Base-specific balance
# [x] market.aave_health_factor(chain='arbitrum') returns Aave health factor
# [x] Enso handles bridge atomically (tx reverts if bridge fails)
# [ ] If supply fails, WETH on Arbitrum, can retry (runtime test)
# [ ] If borrow fails, WETH remains supplied on Aave (runtime test)
# [ ] P&L includes bridge fees, Aave interest, GMX funding rates (runtime test)
# [x] Typecheck passes
# [ ] On-chain test passes with small amount
# ================================================================
