"""LiFi Cross-Chain Yield Rotator Strategy.

==============================================================================
CONCEPT
==============================================================================

A multi-chain strategy that supplies USDC to Aave V3 on whichever chain
currently offers the highest supply APY. When the rate differential between
chains exceeds a threshold, it rotates capital:

  withdraw from current chain -> bridge via LiFi -> supply on better chain

This pushes the SDK to its limits:
1. Multi-chain orchestration across Arbitrum + Base
2. IntentSequence with bridge waiting (withdraw -> bridge -> supply)
3. Cross-chain state management (track which chain capital is deployed on)
4. Complex teardown (unwind from whichever chain, bridge back to origin)

==============================================================================
TESTING ON ANVIL
==============================================================================

Full cross-chain flow cannot be tested on a single Anvil fork. For Anvil
testing, use `force_action: "supply"` which exercises the Aave V3 supply
path on a single chain. The cross-chain rotation logic is present but
requires mainnet or multi-fork setup to test fully.

==============================================================================
"""

import json
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="lifi_yield_rotator",
    description="Cross-chain yield rotator: supplies USDC to highest-APY Aave V3 deployment, bridges via LiFi",
    version="1.0.0",
    author="Almanak",
    tags=["incubating", "yield", "cross-chain", "lifi", "aave", "bridge", "rotation"],
    supported_chains=["arbitrum", "base"],
    supported_protocols=["aave_v3", "lifi"],
    intent_types=["SUPPLY", "WITHDRAW", "BRIDGE", "HOLD"],
)
class LiFiYieldRotatorStrategy(IntentStrategy):
    """Cross-chain yield rotator using LiFi bridges and Aave V3 lending.

    STATE MACHINE:
        idle -> supplying -> deployed -> (check rates) -> withdrawing -> bridging -> supplying -> deployed
                                                      |
                                                      v (teardown)
                                                  withdrawing -> done

    CONFIGURATION (from config.json):
        supply_token (str): Token to supply (e.g., "USDC")
        supply_amount (str): Amount to supply in token units
        protocol (str): Lending protocol (default "aave_v3")
        rate_threshold_bps (int): Min APY difference to trigger rotation (basis points)
        min_rotation_interval_hours (int): Min hours between rotations
        chains (list): Chains to rotate between (e.g., ["arbitrum", "base"])
        default_chain (str): Initial deployment chain
        max_bridge_slippage_pct (float): Max bridge slippage percentage
        force_action (str|None): Force "supply", "withdraw", or "rotate" for testing
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(self.config, dict):
            config_dict = self.config
        elif hasattr(self.config, "__dict__"):
            config_dict = {k: v for k, v in self.config.__dict__.items() if not k.startswith("_")}
        else:
            config_dict = {}

        # Core parameters
        self.supply_token = config_dict.get("supply_token", "USDC")
        self.supply_amount = Decimal(str(config_dict.get("supply_amount", "5")))
        self.lending_protocol = config_dict.get("protocol", "aave_v3")

        # Rotation parameters
        self.rate_threshold_bps = int(config_dict.get("rate_threshold_bps", 100))
        self.min_rotation_interval_hours = int(config_dict.get("min_rotation_interval_hours", 24))

        # Chain configuration
        self.rotation_chains = config_dict.get("chains", ["arbitrum", "base"])
        if len(self.rotation_chains) != 2:
            raise ValueError("rotation_chains must contain exactly two chains")
        self.default_chain = config_dict.get("default_chain", "arbitrum")

        # Slippage
        self.max_bridge_slippage_pct = float(config_dict.get("max_bridge_slippage_pct", 0.5))
        self.max_swap_slippage_pct = float(config_dict.get("max_swap_slippage_pct", 1.0))

        # Force action for testing
        self.force_action = config_dict.get("force_action", None)

        # Internal state
        self._state = "idle"  # idle, deployed, withdrawing, bridging, supplying
        self._current_chain = self.default_chain
        self._deployed_amount = Decimal("0")
        self._rotation_count = 0
        self._last_rotation_time = 0.0

        logger.info(
            f"LiFiYieldRotatorStrategy initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"protocol={self.lending_protocol}, "
            f"chains={self.rotation_chains}, "
            f"threshold={self.rate_threshold_bps}bps"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to supply, rotate, or hold.

        DECISION FLOW:
        1. If force_action set, execute that action
        2. If idle, supply to default chain
        3. If deployed, check rates and rotate if beneficial
        4. Otherwise, hold
        """
        try:
            # Handle forced actions (for Anvil testing)
            if self.force_action:
                logger.info(f"Force action requested: {self.force_action}")
                if self.force_action == "supply":
                    return self._create_supply_intent(self._current_chain)
                elif self.force_action == "withdraw":
                    return self._create_withdraw_intent(self._current_chain)
                elif self.force_action == "rotate":
                    target = self._get_other_chain(self._current_chain)
                    return self._create_rotation_sequence(self._current_chain, target)
                else:
                    logger.warning(f"Unknown force_action: {self.force_action}")

            # State machine
            if self._state == "idle":
                logger.info(f"State: idle -> supplying on {self._current_chain}")
                self._state = "supplying"
                return self._create_supply_intent(self._current_chain)

            elif self._state == "deployed":
                # Check if rotation is beneficial
                best_chain, should_rotate = self._evaluate_rotation(market)
                if should_rotate and best_chain != self._current_chain:
                    logger.info(
                        f"Rotation triggered: {self._current_chain} -> {best_chain} "
                        f"(threshold={self.rate_threshold_bps}bps)"
                    )
                    self._state = "withdrawing"
                    return self._create_rotation_sequence(self._current_chain, best_chain)
                else:
                    return Intent.hold(
                        reason=f"Deployed on {self._current_chain}, no rotation needed"
                    )

            else:
                return Intent.hold(reason=f"Waiting for state transition (current: {self._state})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # RATE EVALUATION
    # =========================================================================

    # Aave V3 Pool addresses (for getReserveData queries)
    AAVE_V3_POOL = {
        "ethereum": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "arbitrum": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "optimism": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "base": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
        "polygon": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "avalanche": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    }

    # getReserveData(address) selector
    GET_RESERVE_DATA_SELECTOR = "0x35ea6a75"

    # 1 ray = 1e27 (Aave's fixed-point unit for rates)
    RAY = Decimal("1000000000000000000000000000")

    def _evaluate_rotation(self, market: MarketSnapshot) -> tuple[str, bool]:
        """Evaluate whether rotation to another chain is beneficial.

        Queries Aave V3 supply APY on each configured chain via gateway RPC,
        then compares against the current chain's rate. Triggers rotation
        when the differential exceeds rate_threshold_bps.

        Returns:
            (best_chain, should_rotate) tuple
        """
        # Check minimum rotation interval
        elapsed_hours = (time.time() - self._last_rotation_time) / 3600
        if elapsed_hours < self.min_rotation_interval_hours:
            logger.debug(
                f"Rotation cooldown: {elapsed_hours:.1f}h < {self.min_rotation_interval_hours}h"
            )
            return self._current_chain, False

        # Query supply rates on all chains
        rates: dict[str, Decimal] = {}
        for chain in self.rotation_chains:
            try:
                rate = self._query_aave_supply_rate(chain)
                if rate is not None:
                    rates[chain] = rate
                    logger.info(f"Aave V3 supply APY on {chain}: {rate:.4f}%")
            except Exception as e:
                logger.warning(f"Failed to query rate on {chain}: {e}")

        if len(rates) < 2:
            logger.debug(f"Only got rates for {len(rates)} chain(s), cannot compare")
            return self._current_chain, False

        # Find best chain
        best_chain = max(rates, key=rates.get)
        current_rate = rates.get(self._current_chain, Decimal("0"))
        best_rate = rates[best_chain]

        # Check if differential exceeds threshold (rates are in %, threshold in bps)
        diff_bps = (best_rate - current_rate) * Decimal("100")  # % -> bps
        logger.info(
            f"Rate comparison: current={self._current_chain} {current_rate:.4f}%, "
            f"best={best_chain} {best_rate:.4f}%, diff={diff_bps:.1f}bps "
            f"(threshold={self.rate_threshold_bps}bps)"
        )

        should_rotate = diff_bps >= self.rate_threshold_bps and best_chain != self._current_chain
        return best_chain, should_rotate

    def _query_aave_supply_rate(self, chain: str) -> Decimal | None:
        """Query current Aave V3 supply APY for supply_token on a given chain.

        Calls getReserveData(address) on the Aave V3 Pool contract via gateway RPC.
        The currentLiquidityRate field (index 2) gives the supply rate in ray units.

        Returns:
            Supply APY as a percentage (e.g., 3.5 means 3.5%), or None on failure.
        """
        from almanak.framework.data.tokens import get_token_resolver
        from almanak.gateway.proto import gateway_pb2

        gateway_client = self._get_gateway_client()
        if gateway_client is None:
            logger.debug("No gateway client available for rate query")
            return None

        pool_address = self.AAVE_V3_POOL.get(chain)
        if not pool_address:
            logger.debug(f"No Aave V3 pool address for chain: {chain}")
            return None

        # Resolve token address on this chain
        resolver = get_token_resolver()
        try:
            token = resolver.resolve(self.supply_token, chain)
        except Exception as e:
            logger.debug(f"Cannot resolve {self.supply_token} on {chain}: {e}")
            return None

        # Build calldata: getReserveData(address)
        # Pad address to 32 bytes (remove 0x prefix, left-pad with zeros)
        padded_address = token.address[2:].lower().zfill(64)
        calldata = self.GET_RESERVE_DATA_SELECTOR + padded_address

        params = json.dumps([{"to": pool_address, "data": calldata}, "latest"])

        response = gateway_client.rpc.Call(
            gateway_pb2.RpcRequest(
                chain=chain,
                method="eth_call",
                params=params,
                id=f"aave-rate-{chain}-{self.supply_token}",
            ),
            timeout=10.0,
        )

        if not response.success:
            logger.warning(f"eth_call failed for getReserveData on {chain}: {response.error}")
            return None

        # Parse response: currentLiquidityRate is field index 2 in the return struct
        # Each field is 32 bytes (64 hex chars). Strip 0x prefix, skip first 2 fields.
        # Gateway RPC returns result as JSON-encoded string, so decode first.
        hex_data = json.loads(response.result) if response.result else None
        if not hex_data or hex_data == "0x":
            logger.warning(f"getReserveData returned empty result on {chain}")
            return None
        if hex_data.startswith("0x"):
            hex_data = hex_data[2:]

        if len(hex_data) < 192:  # Need at least 3 fields (3 * 64 chars)
            logger.warning(f"getReserveData response too short on {chain}: {len(hex_data)} chars")
            return None

        # Field 2 (offset 128-192) = currentLiquidityRate in ray
        liquidity_rate_hex = hex_data[128:192]
        liquidity_rate_ray = Decimal(int(liquidity_rate_hex, 16))

        # Convert ray to percentage: rate_ray / 1e27 * 100
        supply_apy_pct = (liquidity_rate_ray / self.RAY) * Decimal("100")
        return supply_apy_pct

    def _get_gateway_client(self) -> Any:
        """Get the gateway client for on-chain queries, if available."""
        compiler = getattr(self, "_compiler", None)
        if compiler is not None:
            client = getattr(compiler, "_gateway_client", None)
            if client is not None:
                return client
        return None

    def _get_other_chain(self, current: str) -> str:
        """Get the other chain in the rotation pair."""
        for chain in self.rotation_chains:
            if chain != current:
                return chain
        return current

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_supply_intent(self, chain: str) -> Intent:
        """Create a supply intent for Aave V3."""
        logger.info(
            f"SUPPLY: {format_usd(self.supply_amount)} {self.supply_token} to {self.lending_protocol} on {chain}"
        )

        return Intent.supply(
            protocol=self.lending_protocol,
            token=self.supply_token,
            amount=self.supply_amount,
            use_as_collateral=False,  # Pure yield, not collateral
            chain=chain,
        )

    def _create_withdraw_intent(self, chain: str) -> Intent:
        """Create a withdraw intent from Aave V3."""
        logger.info(
            f"WITHDRAW: {self.supply_token} from {self.lending_protocol} on {chain}"
        )

        return Intent.withdraw(
            protocol=self.lending_protocol,
            token=self.supply_token,
            amount=self._deployed_amount if self._deployed_amount > 0 else self.supply_amount,
            withdraw_all=True,
            chain=chain,
        )

    def _create_rotation_sequence(self, from_chain: str, to_chain: str) -> Intent:
        """Create an IntentSequence for cross-chain rotation.

        Sequence: withdraw -> bridge -> supply
        """
        logger.info(f"ROTATE: {from_chain} -> {to_chain} via LiFi bridge")

        max_bridge_slippage = Decimal(str(self.max_bridge_slippage_pct)) / Decimal("100")

        return Intent.sequence([
            # Step 1: Withdraw from current chain's Aave V3
            Intent.withdraw(
                protocol=self.lending_protocol,
                token=self.supply_token,
                amount=self._deployed_amount if self._deployed_amount > 0 else self.supply_amount,
                withdraw_all=True,
                chain=from_chain,
            ),
            # Step 2: Bridge to target chain via LiFi
            Intent.bridge(
                token=self.supply_token,
                amount="all",  # Use output from withdraw
                from_chain=from_chain,
                to_chain=to_chain,
                max_slippage=max_bridge_slippage,
            ),
            # Step 3: Supply on target chain's Aave V3
            Intent.supply(
                protocol=self.lending_protocol,
                token=self.supply_token,
                amount="all",  # Use output from bridge
                use_as_collateral=False,
                chain=to_chain,
            ),
        ])

    # =========================================================================
    # CALLBACKS
    # =========================================================================

    def on_intent_executed(self, intent, success: bool, result):
        """Update state after intent execution."""
        if not success:
            logger.warning("Intent failed, resetting state to idle")
            self._state = "idle"
            return

        intent_type = intent.intent_type.value if hasattr(intent, "intent_type") else ""

        if intent_type == "SUPPLY":
            self._state = "deployed"
            self._deployed_amount = self.supply_amount
            logger.info(
                f"Supply successful. Deployed {format_usd(self._deployed_amount)} "
                f"{self.supply_token} on {self._current_chain}"
            )

        elif intent_type == "WITHDRAW":
            self._deployed_amount = Decimal("0")
            self._state = "idle"
            logger.info(f"Withdraw successful from {self._current_chain}")

        elif intent_type == "SEQUENCE":
            # Full rotation completed
            old_chain = self._current_chain
            self._current_chain = self._get_other_chain(old_chain)
            self._state = "deployed"
            self._deployed_amount = self.supply_amount
            self._rotation_count += 1
            self._last_rotation_time = time.time()
            logger.info(
                f"Rotation complete: {old_chain} -> {self._current_chain} "
                f"(rotation #{self._rotation_count})"
            )

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "lifi_yield_rotator",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "...",
            "config": {
                "supply_token": self.supply_token,
                "supply_amount": str(self.supply_amount),
                "protocol": self.lending_protocol,
                "chains": self.rotation_chains,
                "rate_threshold_bps": self.rate_threshold_bps,
            },
            "state": {
                "state": self._state,
                "current_chain": self._current_chain,
                "deployed_amount": str(self._deployed_amount),
                "rotation_count": self._rotation_count,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()

        if isinstance(self.config, dict):
            config_dict = self.config
        elif hasattr(self.config, "to_dict"):
            config_dict = self.config.to_dict()
        else:
            config_dict = {}

        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "current_chain": self._current_chain,
            "deployed_amount": str(self._deployed_amount),
            "rotation_count": self._rotation_count,
            "last_rotation_time": self._last_rotation_time,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "current_chain" in state:
            self._current_chain = state["current_chain"]
        if "deployed_amount" in state:
            self._deployed_amount = Decimal(str(state["deployed_amount"]))
        if "rotation_count" in state:
            self._rotation_count = int(state["rotation_count"])
        if "last_rotation_time" in state:
            self._last_rotation_time = float(state["last_rotation_time"])
        logger.info(
            f"Restored state: {self._state}, chain={self._current_chain}, "
            f"deployed={self._deployed_amount}, rotations={self._rotation_count}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._deployed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-{self._current_chain}",
                    chain=self._current_chain,
                    protocol=self.lending_protocol,
                    value_usd=self._deployed_amount,
                    details={
                        "asset": self.supply_token,
                        "amount": str(self._deployed_amount),
                        "rotation_count": self._rotation_count,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "lifi_yield_rotator"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        if self._deployed_amount <= 0:
            return intents

        # Withdraw from current chain
        intents.append(
            Intent.withdraw(
                protocol=self.lending_protocol,
                token=self.supply_token,
                amount=self._deployed_amount,
                withdraw_all=True,
                chain=self._current_chain,
            )
        )

        # If deployed on a non-origin chain, bridge back
        if self._current_chain != self.default_chain:
            max_slippage = Decimal("0.01") if mode == TeardownMode.HARD else Decimal(str(self.max_bridge_slippage_pct)) / Decimal("100")
            intents.append(
                Intent.bridge(
                    token=self.supply_token,
                    amount="all",
                    from_chain=self._current_chain,
                    to_chain=self.default_chain,
                    max_slippage=max_slippage,
                )
            )

        return intents


if __name__ == "__main__":
    print("LiFiYieldRotatorStrategy loaded successfully!")
    print(f"Metadata: {LiFiYieldRotatorStrategy.STRATEGY_METADATA}")
