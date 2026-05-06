"""
===============================================================================
TUTORIAL: Lido Staker Strategy - Stake ETH for Liquid Staking Derivatives
===============================================================================

This tutorial strategy demonstrates how to stake ETH with Lido to receive
liquid staking derivatives (stETH or wstETH).

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors ETH balance in the wallet
2. Reserves a configurable amount of ETH for gas fees (default: 0.01 ETH)
3. When (ETH balance - gas_reserve) >= min_stake_amount: Stakes ETH with Lido
4. Receives stETH (rebasing) or wstETH (non-rebasing, wrapped)
5. Holds when already staked or insufficient balance

WHAT IS LIDO?
-------------
Lido is the largest liquid staking protocol for Ethereum:
- Stake ETH without running a validator (32 ETH minimum for solo staking)
- Receive stETH (liquid staking token) representing your staked ETH
- stETH earns ~4% APY from Ethereum staking rewards
- stETH is liquid - trade, use as collateral, LP in DeFi

stETH vs wstETH:
- stETH: Rebasing token - balance increases daily with staking rewards
- wstETH: Non-rebasing wrapper - share value increases, balance constant
- wstETH is better for DeFi (most protocols prefer non-rebasing tokens)

RISKS:
------
- Smart Contract Risk: Lido protocol bugs, exploits
- Slashing Risk: If Lido validators are slashed, stakers may lose funds
- Liquidity Risk: stETH may trade at discount to ETH
- Withdrawal Queue: Unstaking takes 3-5 days through Lido withdrawal queue

USAGE:
------
    # Run once
    python -m src.cli.run --strategy demo_lido_staker --once

    # Run continuously
    python -m src.cli.run --strategy demo_lido_staker

    # Test on Anvil
    python strategies/demo/lido_staker/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from decimal import Decimal
from typing import Any

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging
logger = logging.getLogger(__name__)

# Dust threshold for teardown verification, in stETH/wstETH native units.
# stETH is a rebasing token: after a full unwind swap, the wallet's reported
# balance can show residual amounts from rebase share/amount conversion that
# never lands exactly on zero (typically a few wei to ~1e-12 stETH). A bare
# `> 0` check trips on this and reports the position as still open. 0.0001
# stETH (≈$0.40 at typical ETH prices) is well below any real position and
# many orders of magnitude above any rebase rounding artifact. VIB-3739.
_DUST_THRESHOLD = Decimal("0.0001")


# =============================================================================
# STRATEGY METADATA
# =============================================================================


@almanak_strategy(
    # Unique identifier for CLI (matches the directory name)
    name="lido_staker",
    # Description
    description="Tutorial strategy - stake ETH with Lido for liquid staking yield",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "staking", "lido", "liquid-staking", "eth"],
    # Supported chains (Lido is on Ethereum mainnet)
    supported_chains=["ethereum"],
    # Default chain (used when chain is not specified in config)
    default_chain="ethereum",
    # Protocols used (lido for staking, uniswap_v3 for the teardown exit swap)
    supported_protocols=["lido", "uniswap_v3"],
    # Intent types this strategy may emit
    # STAKE: Stake ETH to receive stETH/wstETH
    # SWAP: Teardown path to exit stETH/wstETH -> ETH
    # HOLD: No action
    intent_types=["STAKE", "SWAP", "HOLD"],
)
class LidoStakerStrategy(IntentStrategy):
    """
    Lido staking strategy for educational purposes.

    This strategy demonstrates:
    - How to stake ETH with Lido
    - How to choose between stETH and wstETH
    - How to track staking state

    Configuration Parameters (from config.yaml or config.json):
    -----------------------------------------------------------
    - min_stake_amount: Minimum ETH balance to trigger staking (default: "0.1")
    - gas_reserve: ETH reserved for gas fees, never staked (default: "0.01")
    - receive_wrapped: Whether to receive wstETH instead of stETH (default: True)
    - force_action: Force "stake" for testing

    Example Config:
    ---------------
    {
        "min_stake_amount": "0.1",
        "gas_reserve": "0.01",
        "receive_wrapped": true,
        "force_action": ""
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the staking strategy.

        Extracts configuration and sets up internal state for tracking
        the staking process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration (via StrategyBase.get_config)
        # =====================================================================

        self.min_stake_amount = Decimal(str(self.get_config("min_stake_amount", "0.1")))

        # ETH reserved for gas fees (never staked)
        self.gas_reserve = Decimal(str(self.get_config("gas_reserve", "0.01")))
        if self.gas_reserve < 0:
            raise ValueError("gas_reserve must be >= 0")

        # Whether to receive wstETH (wrapped, non-rebasing) instead of stETH
        self.receive_wrapped = self.get_config("receive_wrapped", True)

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "") or "").lower()

        # Internal state tracking
        self._staked = False
        self._staked_amount = Decimal("0")

        logger.info(
            f"LidoStakerStrategy initialized: min_stake={self.min_stake_amount} ETH, "
            f"gas_reserve={self.gas_reserve} ETH, receive_wrapped={self.receive_wrapped}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a staking decision based on wallet balance and configuration.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If already staked, hold
        3. If (ETH balance - gas_reserve) >= min_stake_amount, stake
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: STAKE or HOLD
        """
        # =================================================================
        # STEP 1: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "stake":
            logger.info("Forced action: STAKE ETH")
            return self._create_stake_intent(self.min_stake_amount)

        # =================================================================
        # STEP 2: Check if already staked
        # =================================================================

        if self._staked:
            output_token = "wstETH" if self.receive_wrapped else "stETH"
            return Intent.hold(reason=f"Already staked {self._staked_amount} ETH -> {output_token}")

        # =================================================================
        # STEP 3: Check ETH balance
        # =================================================================

        try:
            eth_balance = market.balance("ETH")
            # Extract balance value from TokenBalance object if needed
            balance_value = eth_balance.balance if hasattr(eth_balance, "balance") else eth_balance
            logger.debug(f"ETH balance: {balance_value}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get ETH balance: {e}")
            return Intent.hold(reason=f"Could not fetch ETH balance: {e}")

        # =================================================================
        # STEP 4: Stake if sufficient balance
        # =================================================================

        # Reserve ETH for gas fees - never stake the entire balance
        stake_amount = max(Decimal("0"), balance_value - self.gas_reserve)
        if stake_amount >= self.min_stake_amount:
            logger.info(
                f"ETH balance ({balance_value}) - gas_reserve ({self.gas_reserve}) = "
                f"{stake_amount} >= min_stake ({self.min_stake_amount}), staking"
            )
            return self._create_stake_intent(stake_amount)

        # =================================================================
        # STEP 5: Insufficient balance - hold
        # =================================================================

        return Intent.hold(
            reason=f"Insufficient stakeable ETH: {balance_value} - {self.gas_reserve} gas reserve = {stake_amount} < {self.min_stake_amount}"
        )

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_stake_intent(self, amount: Decimal) -> Intent:
        """
        Create a STAKE intent to deposit ETH with Lido.

        Staking flow:
        1. ETH is sent to Lido's staking contract
        2. You receive stETH (1:1 ratio initially)
        3. If receive_wrapped=True, stETH is wrapped to wstETH

        Parameters:
            amount: Amount of ETH to stake

        Returns:
            StakeIntent ready for compilation
        """
        output_token = "wstETH" if self.receive_wrapped else "stETH"
        logger.info(f"STAKE intent: {format_token_amount_human(amount, 'ETH')} -> {output_token}")

        return Intent.stake(
            protocol="lido",
            token_in="ETH",
            amount=amount,
            receive_wrapped=self.receive_wrapped,
            chain="ethereum",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track staking status.
        """
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success and intent_type_val == "STAKE":
            self._staked = True
            # Prefer the amount from the receipt (LidoReceiptParser populates
            # `stake_amount`); fall back to the intent's requested amount.
            amount = None
            extracted = getattr(result, "extracted_data", None)
            if isinstance(extracted, dict):
                amount = extracted.get("stake_amount")
            if amount is None and hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                amount = intent.amount
            if amount is not None:
                self._staked_amount = Decimal(str(amount))
            output_token = "wstETH" if self.receive_wrapped else "stETH"
            logger.info(f"Staking successful: {self._staked_amount} ETH -> {output_token}")
        elif success and intent_type_val == "SWAP":
            # Teardown exit swap succeeded — clear staked state so decide()
            # doesn't keep returning HOLD("Already staked") in later iterations.
            self._staked = False
            self._staked_amount = Decimal("0")
            logger.info("Teardown SWAP succeeded: cleared staked state")
        elif not success:
            logger.warning(f"{intent_type_val} failed")

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": self.STRATEGY_NAME,
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "min_stake_amount": str(self.min_stake_amount),
                "gas_reserve": str(self.gas_reserve),
                "receive_wrapped": self.receive_wrapped,
            },
            "state": {
                "staked": self._staked,
                "staked_amount": str(self._staked_amount),
            },
        }

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery.

        This allows the strategy to resume from where it left off.
        """
        return {
            "staked": self._staked,
            "staked_amount": str(self._staked_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup.

        Called when resuming a strategy after crash/restart.
        """
        if "staked" in state:
            self._staked = state["staked"]
            logger.info(f"Restored staked state: {self._staked}")

        if "staked_amount" in state:
            self._staked_amount = Decimal(str(state["staked_amount"]))
            logger.info(f"Restored staked_amount: {self._staked_amount}")

    def get_open_positions(self):
        """Return open stETH/wstETH positions for teardown.

        Lido withdrawals go through a 3-5 day queue, so teardown exits by
        swapping stETH/wstETH back to ETH on Uniswap V3 (deep liquidity).
        Queries on-chain balance when possible, falling back to tracked state.
        """
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        output_token = "wstETH" if self.receive_wrapped else "stETH"

        # Prefer on-chain balance over cached state (CLAUDE.md teardown contract).
        balance_amount = Decimal("0")
        balance_usd = Decimal("0")
        try:
            market = self.create_market_snapshot()
            balance = market.balance(output_token)
            balance_amount = balance.balance if hasattr(balance, "balance") else Decimal(str(balance))
            balance_usd = getattr(balance, "balance_usd", None) or Decimal("0")
        except Exception as exc:
            logger.warning(f"Unable to query on-chain {output_token} balance for teardown: {exc!r}")
            if self._staked and self._staked_amount > 0:
                balance_amount = self._staked_amount

        positions: list[PositionInfo] = []
        if balance_amount > _DUST_THRESHOLD:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"lido-{output_token.lower()}",
                    chain=self.chain,
                    protocol="lido",
                    value_usd=balance_usd,
                    details={"asset": output_token, "source": "lido_stake"},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id or self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None):
        """Exit staked ETH via Uniswap V3 swap when a position exists.

        Direct Lido unstake has a 3-5 day queue, so teardown uses a spot swap
        back to ETH. HARD mode allows wider slippage to guarantee exit. If the
        wallet holds no stETH/wstETH, returns `[]` so teardown isn't blocked
        by a zero-balance swap.
        """
        from almanak.framework.teardown import TeardownMode

        output_token = "wstETH" if self.receive_wrapped else "stETH"

        # Only emit a swap when the wallet actually holds the staking token.
        # Prefer on-chain balance; fall back to tracked state if unavailable.
        has_balance = False
        try:
            snapshot = market or self.create_market_snapshot()
            balance = snapshot.balance(output_token)
            amount = balance.balance if hasattr(balance, "balance") else Decimal(str(balance))
            has_balance = amount > _DUST_THRESHOLD
        except Exception as exc:
            logger.warning(
                f"Unable to query on-chain {output_token} balance for teardown intents: {exc!r}"
            )
            has_balance = self._staked and self._staked_amount > _DUST_THRESHOLD

        if not has_balance:
            logger.info("No staking position detected — no teardown intents needed")
            return []

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [
            Intent.swap(
                from_token=output_token,
                to_token="ETH",
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v3",
                chain="ethereum",
            )
        ]


