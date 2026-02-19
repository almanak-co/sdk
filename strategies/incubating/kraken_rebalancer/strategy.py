"""
===============================================================================
TUTORIAL: Kraken CEX Rebalancer - Deposit, Swap, and Withdraw
===============================================================================

This tutorial strategy demonstrates how to integrate with the Kraken centralized
exchange (CEX) for trading operations. It showcases the complete lifecycle of
CEX-DeFi interaction:

1. DEPOSIT: Transfer USDC from on-chain wallet to Kraken
2. SWAP: Execute a USDC -> ETH trade on Kraken's orderbook
3. WITHDRAW: Transfer ETH from Kraken back to on-chain wallet

WHY USE A CEX?
--------------
- Better liquidity for large trades
- Lower fees than DEX swaps
- No slippage on limit orders
- Access to fiat on/off ramps

PREREQUISITES:
--------------
1. Kraken account with API access enabled
2. Whitelisted withdrawal address on Kraken (for security)
3. USDC balance on-chain to deposit
4. Environment variables:
   - KRAKEN_API_KEY: Your Kraken API key
   - KRAKEN_API_SECRET: Your Kraken API secret

SUPPORTED CHAINS FOR DEPOSIT/WITHDRAWAL:
----------------------------------------
- Arbitrum (recommended - low fees)
- Optimism
- Ethereum

USAGE:
------
    # Set environment variables
    export KRAKEN_API_KEY="your_api_key"
    export KRAKEN_API_SECRET="your_api_secret"

    # Run the strategy
    python -m strategies.demo.kraken_rebalancer.strategy

===============================================================================
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.connectors.kraken import (
    KrakenAdapter,
    KrakenConfig,
    KrakenCredentials,
    KrakenSDK,
)
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CEX INTENT MODELS
# =============================================================================
# These are simple intent-like models for CEX operations.
# The KrakenAdapter reads attributes from these to compile actions.


@dataclass
class CEXDepositIntent:
    """Intent to track a deposit to Kraken.

    Note: The actual deposit is an on-chain transfer to Kraken's deposit address.
    This intent tracks when that deposit is credited to your Kraken account.
    """

    token: str
    amount: Decimal
    chain: str
    tx_hash: str  # On-chain tx hash of the deposit


@dataclass
class CEXSwapIntent:
    """Intent to swap tokens on Kraken CEX."""

    from_token: str
    to_token: str
    amount: Decimal  # Amount of from_token to swap
    chain: str = "arbitrum"  # For token resolution


@dataclass
class CEXWithdrawIntent:
    """Intent to withdraw tokens from Kraken to on-chain wallet."""

    token: str
    amount: Decimal | str  # Decimal amount or "all"
    chain: str
    to_address: str


# =============================================================================
# STRATEGY STATE
# =============================================================================


class RebalancerState(str, Enum):
    """State machine states for the rebalancer."""

    IDLE = "idle"
    DEPOSITING = "depositing"
    DEPOSIT_PENDING = "deposit_pending"
    SWAPPING = "swapping"
    SWAP_PENDING = "swap_pending"
    WITHDRAWING = "withdrawing"
    WITHDRAW_PENDING = "withdraw_pending"
    COMPLETE = "complete"
    ERROR = "error"


# =============================================================================
# STRATEGY IMPLEMENTATION
# =============================================================================


@almanak_strategy(
    name="demo_kraken_rebalancer",
    description="Tutorial strategy - deposit, swap, and withdraw using Kraken CEX",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "cex", "kraken", "rebalancer"],
    supported_chains=["arbitrum", "optimism", "ethereum"],
    supported_protocols=["kraken"],
    intent_types=["CEX_DEPOSIT", "CEX_SWAP", "CEX_WITHDRAW", "HOLD"],
)
class KrakenRebalancerStrategy(IntentStrategy):
    """
    Kraken CEX Rebalancer Strategy.

    Demonstrates the complete CEX integration lifecycle:
    1. Deposit USDC from on-chain to Kraken
    2. Swap USDC to ETH on Kraken
    3. Withdraw ETH from Kraken to on-chain

    Configuration Parameters (from config.json):
    --------------------------------------------
    - swap_amount_usd: Amount in USD to swap (default: 10)
    - chain: Chain for deposits/withdrawals (default: "arbitrum")
    - from_token: Token to deposit and swap from (default: "USDC")
    - to_token: Token to swap to and withdraw (default: "ETH")
    - skip_deposit: Skip deposit step, use existing CEX balance (default: False)
    - skip_withdraw: Skip withdraw step, keep funds on CEX (default: False)

    Example Config:
    ---------------
    {
        "swap_amount_usd": 10,
        "chain": "arbitrum",
        "from_token": "USDC",
        "to_token": "ETH",
        "skip_deposit": false,
        "skip_withdraw": false
    }
    """

    def __init__(self, *args, **kwargs):
        """Initialize the Kraken rebalancer strategy."""
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Trading configuration
        self.swap_amount_usd = Decimal(str(get_config("swap_amount_usd", "10")))
        self.trade_chain = get_config("chain", "arbitrum")
        self.from_token = get_config("from_token", "USDC")
        self.to_token = get_config("to_token", "ETH")

        # Flow control
        self.skip_deposit = get_config("skip_deposit", False)
        self.skip_withdraw = get_config("skip_withdraw", False)

        # Force specific action for testing
        self.force_action = str(get_config("force_action", "")).lower()

        # =====================================================================
        # Initialize Kraken components
        # =====================================================================

        try:
            self.credentials = KrakenCredentials.from_env()
            self.kraken_config = KrakenConfig(credentials=self.credentials)
            self.sdk = KrakenSDK(credentials=self.credentials)
            self.adapter = KrakenAdapter(config=self.kraken_config, sdk=self.sdk)
            self._kraken_available = True
            logger.info("Kraken SDK initialized successfully")
        except Exception as e:
            logger.warning(f"Kraken SDK initialization failed: {e}")
            logger.warning("Strategy will run in simulation mode")
            self._kraken_available = False
            self.sdk = None
            self.adapter = None

        # =====================================================================
        # Internal state
        # =====================================================================

        self._state = RebalancerState.IDLE
        self._deposit_tx_hash: str | None = None
        self._swap_txid: str | None = None
        self._withdraw_refid: str | None = None
        self._eth_amount: Decimal = Decimal("0")

        logger.info(
            f"KrakenRebalancerStrategy initialized: "
            f"swap_amount=${self.swap_amount_usd}, "
            f"chain={self.trade_chain}, "
            f"{self.from_token} -> {self.to_token}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a trading decision based on current state.

        Decision Flow:
        1. IDLE -> Start deposit (or skip to swap if skip_deposit)
        2. DEPOSIT_PENDING -> Wait for deposit confirmation
        3. SWAPPING -> Execute swap on Kraken
        4. SWAP_PENDING -> Wait for swap completion
        5. WITHDRAWING -> Initiate withdrawal
        6. WITHDRAW_PENDING -> Wait for withdrawal completion
        7. COMPLETE -> Done

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent or None (CEX operations return None and handle internally)
        """
        try:
            # =================================================================
            # Handle forced actions (for testing)
            # =================================================================

            if self.force_action == "deposit":
                return self._handle_deposit(market)
            elif self.force_action == "swap":
                return self._handle_swap(market)
            elif self.force_action == "withdraw":
                return self._handle_withdraw(market)

            # =================================================================
            # State machine
            # =================================================================

            if self._state == RebalancerState.IDLE:
                if self.skip_deposit:
                    logger.info("Skipping deposit, moving to swap")
                    self._state = RebalancerState.SWAPPING
                    return self._handle_swap(market)
                else:
                    return self._handle_deposit(market)

            elif self._state == RebalancerState.DEPOSIT_PENDING:
                return self._check_deposit_status()

            elif self._state == RebalancerState.SWAPPING:
                return self._handle_swap(market)

            elif self._state == RebalancerState.SWAP_PENDING:
                return self._check_swap_status()

            elif self._state == RebalancerState.WITHDRAWING:
                return self._handle_withdraw(market)

            elif self._state == RebalancerState.WITHDRAW_PENDING:
                return self._check_withdraw_status()

            elif self._state == RebalancerState.COMPLETE:
                return Intent.hold(reason="Rebalancing complete")

            elif self._state == RebalancerState.ERROR:
                return Intent.hold(reason="Strategy in error state - manual intervention required")

            return Intent.hold(reason=f"Unknown state: {self._state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            self._state = RebalancerState.ERROR
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # DEPOSIT HANDLING
    # =========================================================================

    def _handle_deposit(self, market: MarketSnapshot) -> Intent | None:
        """Handle the deposit phase.

        To deposit to Kraken:
        1. Get Kraken's deposit address for the chain
        2. Send an on-chain transfer to that address
        3. Wait for Kraken to credit the deposit

        For this demo, we return a SWAP intent to transfer on-chain,
        but in production you'd use the execution system.
        """
        logger.info("=== DEPOSIT PHASE ===")
        logger.info(f"Depositing {self.swap_amount_usd} {self.from_token} to Kraken")

        if not self._kraken_available:
            logger.info("[SIMULATION] Would deposit to Kraken")
            self._state = RebalancerState.SWAPPING
            return Intent.hold(reason="[Simulation] Deposit simulated, moving to swap")

        try:
            # Get Kraken deposit address
            deposit_addresses = self.sdk.get_deposit_addresses(
                asset=self.from_token,
                chain=self.trade_chain,
            )

            if not deposit_addresses:
                logger.error(f"No deposit address found for {self.from_token} on {self.trade_chain}")
                self._state = RebalancerState.ERROR
                return Intent.hold(reason="No deposit address available")

            deposit_address = list(deposit_addresses)[0]
            logger.info(f"Kraken deposit address: {deposit_address}")

            # Log the deposit intent
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description=f"Initiating deposit of {self.swap_amount_usd} {self.from_token} to Kraken",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "deposit",
                        "token": self.from_token,
                        "amount": str(self.swap_amount_usd),
                        "chain": self.trade_chain,
                        "deposit_address": deposit_address,
                    },
                )
            )

            # Return a swap intent to transfer to Kraken's deposit address
            # In a real scenario, you'd use an ERC20 transfer
            # For demo purposes, we'll simulate completion
            logger.info(
                f"To deposit: Send {self.swap_amount_usd} {self.from_token} to {deposit_address} on {self.trade_chain}"
            )

            # Move to swap phase (in production, would wait for deposit confirmation)
            self._state = RebalancerState.SWAPPING
            return Intent.hold(reason=f"Deposit address: {deposit_address}. Moving to swap phase.")

        except Exception as e:
            logger.error(f"Deposit failed: {e}")
            self._state = RebalancerState.ERROR
            return Intent.hold(reason=f"Deposit error: {e}")

    def _check_deposit_status(self) -> Intent | None:
        """Check if deposit has been credited."""
        if not self._deposit_tx_hash:
            self._state = RebalancerState.SWAPPING
            return None

        status = self.sdk.get_deposit_status(
            tx_hash=self._deposit_tx_hash,
            asset=self.from_token,
            chain=self.trade_chain,
        )

        if status == "success":
            logger.info("Deposit confirmed!")
            self._state = RebalancerState.SWAPPING
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Deposit of {self.swap_amount_usd} {self.from_token} confirmed",
                    strategy_id=self.strategy_id,
                    details={"tx_hash": self._deposit_tx_hash},
                )
            )
        elif status == "failed":
            logger.error("Deposit failed!")
            self._state = RebalancerState.ERROR
        else:
            logger.info(f"Deposit pending... status: {status}")

        return Intent.hold(reason=f"Deposit status: {status}")

    # =========================================================================
    # SWAP HANDLING
    # =========================================================================

    def _handle_swap(self, market: MarketSnapshot) -> Intent | None:
        """Handle the swap phase on Kraken.

        Executes a market order to swap USDC -> ETH.
        """
        logger.info("=== SWAP PHASE ===")
        logger.info(f"Swapping {self.swap_amount_usd} {self.from_token} -> {self.to_token}")

        if not self._kraken_available:
            logger.info("[SIMULATION] Would execute swap on Kraken")
            # Simulate ETH amount (assume $3400/ETH)
            self._eth_amount = self.swap_amount_usd / Decimal("3400")
            self._state = RebalancerState.WITHDRAWING if not self.skip_withdraw else RebalancerState.COMPLETE
            return Intent.hold(reason=f"[Simulation] Swap simulated, received ~{self._eth_amount:.6f} {self.to_token}")

        try:
            # Check Kraken balance first
            balance = self.sdk.get_balance(self.from_token, self.trade_chain)
            logger.info(f"Kraken {self.from_token} balance: {balance.available}")

            if balance.available < self.swap_amount_usd:
                logger.warning(f"Insufficient Kraken balance: {balance.available} < {self.swap_amount_usd}")
                return Intent.hold(
                    reason=f"Insufficient Kraken balance. Have: {balance.available}, Need: {self.swap_amount_usd}"
                )

            # Get current ETH price for logging
            try:
                eth_price = market.price(self.to_token)
                expected_eth = self.swap_amount_usd / eth_price
                logger.info(f"Expected {self.to_token}: ~{expected_eth:.6f} @ ${eth_price:.2f}")
            except Exception:
                expected_eth = self.swap_amount_usd / Decimal("3400")
                logger.info(f"Expected {self.to_token}: ~{expected_eth:.6f} (estimated)")

            # Generate userref for idempotency
            userref = KrakenSDK.generate_userref()

            # Execute the swap
            logger.info(f"Executing swap with userref: {userref}")

            # USDC has 6 decimals
            amount_wei = int(self.swap_amount_usd * Decimal("1000000"))

            txid = self.sdk.swap(
                asset_in=self.from_token,
                asset_out=self.to_token,
                amount_in=amount_wei,
                decimals_in=6,  # USDC decimals
                userref=userref,
                chain=self.trade_chain,
            )

            self._swap_txid = txid
            self._state = RebalancerState.SWAP_PENDING

            logger.info(f"Swap order placed! txid: {txid}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Swap order placed: {self.swap_amount_usd} {self.from_token} -> {self.to_token}",
                    strategy_id=self.strategy_id,
                    details={"txid": txid, "userref": userref},
                )
            )

            return Intent.hold(reason=f"Swap order placed, txid: {txid}")

        except Exception as e:
            logger.error(f"Swap failed: {e}")
            self._state = RebalancerState.ERROR
            return Intent.hold(reason=f"Swap error: {e}")

    def _check_swap_status(self) -> Intent | None:
        """Check if swap has completed."""
        if not self._swap_txid:
            self._state = RebalancerState.ERROR
            return Intent.hold(reason="No swap txid found")

        status = self.sdk.get_swap_status(self._swap_txid, userref=None)

        if status == "success":
            logger.info("Swap completed!")

            # Get the result to know how much ETH we received
            try:
                result = self.sdk.get_swap_result(
                    txid=self._swap_txid,
                    userref=None,
                    asset_in=self.from_token,
                    asset_out=self.to_token,
                    decimals_in=6,
                    decimals_out=18,
                    chain=self.trade_chain,
                )
                self._eth_amount = Decimal(result["amount_out"]) / Decimal("1e18")
                logger.info(f"Received: {self._eth_amount:.6f} {self.to_token}")
            except Exception as e:
                logger.warning(f"Could not get swap result: {e}")
                # Estimate based on current balance
                balance = self.sdk.get_balance(self.to_token, self.trade_chain)
                self._eth_amount = balance.available

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Swap completed: received {self._eth_amount:.6f} {self.to_token}",
                    strategy_id=self.strategy_id,
                    details={"txid": self._swap_txid, "amount": str(self._eth_amount)},
                )
            )

            if self.skip_withdraw:
                self._state = RebalancerState.COMPLETE
            else:
                self._state = RebalancerState.WITHDRAWING

        elif status == "failed":
            logger.error("Swap failed!")
            self._state = RebalancerState.ERROR

        elif status == "cancelled":
            logger.error("Swap was cancelled!")
            self._state = RebalancerState.ERROR

        else:
            logger.info(f"Swap pending... status: {status}")

        return Intent.hold(reason=f"Swap status: {status}")

    # =========================================================================
    # WITHDRAW HANDLING
    # =========================================================================

    def _handle_withdraw(self, market: MarketSnapshot) -> Intent | None:
        """Handle the withdrawal phase.

        Withdraws ETH from Kraken to the on-chain wallet.
        The wallet address must be whitelisted on Kraken.
        """
        logger.info("=== WITHDRAW PHASE ===")
        logger.info(f"Withdrawing {self._eth_amount:.6f} {self.to_token} to {self.trade_chain}")

        if not self._kraken_available:
            logger.info("[SIMULATION] Would withdraw from Kraken")
            self._state = RebalancerState.COMPLETE
            return Intent.hold(reason="[Simulation] Withdrawal simulated, complete!")

        if not self.wallet_address:
            logger.error("No wallet address configured for withdrawal")
            self._state = RebalancerState.ERROR
            return Intent.hold(reason="No wallet address for withdrawal")

        try:
            # Check if wallet is whitelisted
            whitelisted = self.sdk.get_withdrawal_addresses(
                asset=self.to_token,
                chain=self.trade_chain,
            )

            if self.wallet_address not in whitelisted:
                logger.error(f"Wallet {self.wallet_address} is not whitelisted for {self.to_token} on {self.trade_chain}")
                logger.info(f"Whitelisted addresses: {whitelisted}")
                self._state = RebalancerState.ERROR
                return Intent.hold(
                    reason=f"Wallet not whitelisted. Please add {self.wallet_address} to Kraken withdrawal whitelist."
                )

            # Get balance to withdraw
            balance = self.sdk.get_balance(self.to_token, self.trade_chain)
            withdraw_amount = balance.available

            if withdraw_amount <= 0:
                logger.warning("No balance to withdraw")
                self._state = RebalancerState.COMPLETE
                return Intent.hold(reason="No balance to withdraw, complete!")

            logger.info(f"Withdrawing {withdraw_amount} {self.to_token} to {self.wallet_address}")

            # ETH has 18 decimals
            amount_wei = int(withdraw_amount * Decimal("1e18"))

            refid = self.sdk.withdraw(
                asset=self.to_token,
                chain=self.trade_chain,
                amount=amount_wei,
                decimals=18,
                to_address=self.wallet_address,
            )

            self._withdraw_refid = refid
            self._state = RebalancerState.WITHDRAW_PENDING

            logger.info(f"Withdrawal initiated! refid: {refid}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Withdrawal initiated: {withdraw_amount} {self.to_token} to {self.wallet_address}",
                    strategy_id=self.strategy_id,
                    details={"refid": refid, "chain": self.chain},
                )
            )

            return Intent.hold(reason=f"Withdrawal initiated, refid: {refid}")

        except Exception as e:
            logger.error(f"Withdrawal failed: {e}")
            self._state = RebalancerState.ERROR
            return Intent.hold(reason=f"Withdrawal error: {e}")

    def _check_withdraw_status(self) -> Intent | None:
        """Check if withdrawal has completed."""
        if not self._withdraw_refid:
            self._state = RebalancerState.ERROR
            return Intent.hold(reason="No withdrawal refid found")

        status = self.sdk.get_withdrawal_status(
            asset=self.to_token,
            chain=self.trade_chain,
            refid=self._withdraw_refid,
        )

        if status == "success":
            logger.info("Withdrawal completed!")

            # Get the on-chain tx hash
            tx_hash = self.sdk.get_withdrawal_tx_hash(
                asset=self.to_token,
                chain=self.trade_chain,
                refid=self._withdraw_refid,
            )

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Withdrawal completed: {self._eth_amount:.6f} {self.to_token}",
                    strategy_id=self.strategy_id,
                    details={"refid": self._withdraw_refid, "tx_hash": tx_hash},
                )
            )

            self._state = RebalancerState.COMPLETE
            logger.info("=== REBALANCING COMPLETE ===")
            logger.info(f"On-chain tx: {tx_hash}")

        elif status == "failed":
            logger.error("Withdrawal failed!")
            self._state = RebalancerState.ERROR

        else:
            logger.info(f"Withdrawal pending... status: {status}")

        return Intent.hold(reason=f"Withdrawal status: {status}")

    # =========================================================================
    # STATUS & STATE PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_kraken_rebalancer",
            "chain": self.trade_chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "kraken_available": self._kraken_available,
            "config": {
                "swap_amount_usd": str(self.swap_amount_usd),
                "from_token": self.from_token,
                "to_token": self.to_token,
                "skip_deposit": self.skip_deposit,
                "skip_withdraw": self.skip_withdraw,
            },
            "state": {
                "current_state": self._state.value,
                "deposit_tx_hash": self._deposit_tx_hash,
                "swap_txid": self._swap_txid,
                "withdraw_refid": self._withdraw_refid,
                "eth_amount": str(self._eth_amount),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery."""
        return {
            "state": self._state.value,
            "deposit_tx_hash": self._deposit_tx_hash,
            "swap_txid": self._swap_txid,
            "withdraw_refid": self._withdraw_refid,
            "eth_amount": str(self._eth_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup."""
        if "state" in state:
            self._state = RebalancerState(state["state"])
            logger.info(f"Restored state: {self._state}")

        if "deposit_tx_hash" in state:
            self._deposit_tx_hash = state["deposit_tx_hash"]

        if "swap_txid" in state:
            self._swap_txid = state["swap_txid"]

        if "withdraw_refid" in state:
            self._withdraw_refid = state["withdraw_refid"]

        if "eth_amount" in state:
            self._eth_amount = Decimal(str(state["eth_amount"]))


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================

if __name__ == "__main__":
    import os

    metadata = KrakenRebalancerStrategy.STRATEGY_METADATA

    print("=" * 70)
    print("KrakenRebalancerStrategy - Demo Strategy")
    print("=" * 70)
    print(f"\nStrategy Name: {KrakenRebalancerStrategy.STRATEGY_NAME}")
    print(f"Version: {metadata.version}")
    print(f"Supported Chains: {metadata.supported_chains}")
    print(f"Supported Protocols: {metadata.supported_protocols}")
    print(f"\nDescription: {metadata.description}")

    print("\n" + "-" * 70)
    print("ENVIRONMENT CHECK")
    print("-" * 70)

    api_key = os.environ.get("KRAKEN_API_KEY", "")
    api_secret = os.environ.get("KRAKEN_API_SECRET", "")

    if api_key and api_secret:
        print(f"KRAKEN_API_KEY: {'*' * 8}...{api_key[-4:]}")
        print(f"KRAKEN_API_SECRET: {'*' * 8}...{api_secret[-4:]}")
        print("\nKraken credentials found! Strategy will execute real trades.")
    else:
        print("KRAKEN_API_KEY: Not set")
        print("KRAKEN_API_SECRET: Not set")
        print("\nNo Kraken credentials found. Strategy will run in simulation mode.")

    print("\n" + "-" * 70)
    print("USAGE")
    print("-" * 70)
    print("""
To run this strategy:

1. Set up Kraken API credentials:
   export KRAKEN_API_KEY="your_api_key"
   export KRAKEN_API_SECRET="your_api_secret"

2. Whitelist your wallet address on Kraken:
   - Go to Kraken -> Funding -> Withdraw
   - Add your wallet address to the withdrawal whitelist
   - Wait for the verification period (24-72 hours)

3. Run the strategy:
   python -m strategies.demo.kraken_rebalancer.strategy

4. Or with custom config:
   python -m src.cli.run --strategy demo_kraken_rebalancer --config '{
       "swap_amount_usd": 10,
       "chain": "arbitrum",
       "skip_deposit": true
   }'
""")
