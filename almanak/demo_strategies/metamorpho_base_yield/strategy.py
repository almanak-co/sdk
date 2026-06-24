"""MetaMorpho Base Yield Strategy — yield-floor-gated ERC4626 vault rotation.

Deposits USDC into the Moonwell Flagship USDC MetaMorpho vault on Base for
optimized lending yield across Morpho Blue markets (curated by Gauntlet). It is
the reference for archetype #10 (ERC4626 vault yield) and exists to demonstrate
the one thing a yield-vault strategy MUST do that a deposit-and-forget tutorial
does not: **time entry and exit on the live yield**, not just deposit and wait
for teardown.

Strategy Logic (a real yield-timing loop, symmetric in/out):
1. IDLE → DEPOSIT only when the live supply APY is at or above ``min_apy_floor``
   (read via ``market.lending_rate``); the yield must justify the position
   before capital enters. (Entry is NOT blocked when the APY read is merely
   unavailable — only when it is readable AND below the floor.)
2. DEPOSITED → HOLD while APY ≥ floor, auto-compounding idle USDC every
   ``compound_interval_hours``; **REDEEM (exit) when APY falls below the floor
   for ``exit_confirm_checks`` consecutive checks** (hysteresis — a single
   transient dip does not churn the position). Also redeems on teardown.
3. After an APY-floor exit the strategy returns to IDLE and only re-enters once
   the APY recovers above the floor — so it rotates capital in and out with the
   yield rather than sitting in a vault whose rate has collapsed.

Design rules honoured (the golden promotion gate):
- The exit is gated on a real economic signal (live APY vs a configured floor),
  not a fixed timer; state commits only in ``on_intent_executed`` after a fill.
- Data-unavailable reads degrade to HOLD (exit only on a *readable* sub-floor
  rate — never churn out on a missing read); any other exception propagates.
- No direct network egress — APY via ``lending_rate`` (gateway), value via the
  framework.

Note: a share-price *drawdown* guard (exit on a vault loss / de-peg event) needs
a correct ERC4626 vault NAV read, which is currently blocked by the vault-NAV
units bug (VIB-5392); it is a deliberate follow-up, not implemented here.

Target vault:
  Moonwell Flagship USDC (0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca)
  - Curated by Gauntlet / Moonwell; allocates across Morpho Blue USDC markets.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="metamorpho_base_yield",
    description="MetaMorpho USDC yield on Base via Moonwell Flagship vault",
    version="1.0.0",
    author="Almanak",
    tags=["vault", "metamorpho", "erc4626", "yield", "lending", "base"],
    supported_chains=["base"],
    default_chain="base",
    supported_protocols=["metamorpho"],
    intent_types=["VAULT_DEPOSIT", "VAULT_REDEEM", "HOLD"],
    quote_asset="USD",
)
class MetaMorphoBaseYield(IntentStrategy):
    """MetaMorpho yield strategy depositing USDC into Moonwell Flagship vault on Base.

    State Machine:
        idle -> depositing -> deposited -> compounding -> deposited -> redeeming -> idle
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.vault_address = self.get_config(
            "vault_address",
            "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca",
        )
        self.deposit_token = self.get_config("deposit_token", "USDC")
        self.deposit_amount = Decimal(str(self.get_config("deposit_amount", "1000")))
        self.min_deposit_usd = Decimal(str(self.get_config("min_deposit_usd", "100")))
        self.max_vault_allocation_pct = int(self.get_config("max_vault_allocation_pct", 80))
        self.compound_interval_hours = int(self.get_config("compound_interval_hours", 24))

        # Yield-floor gate: enter only when the live supply APY >= floor, exit
        # when it stays below the floor for exit_confirm_checks consecutive ticks.
        self.min_apy_floor = Decimal(str(self.get_config("min_apy_floor", "3.0")))  # percent
        self.exit_confirm_checks = int(self.get_config("exit_confirm_checks", 2))
        self.rate_protocol = str(self.get_config("rate_protocol", "morpho_blue"))
        self.rate_token = str(self.get_config("rate_token", self.deposit_token))

        if self.min_apy_floor < 0:
            raise ValueError("min_apy_floor must be >= 0")
        if self.exit_confirm_checks < 1:
            raise ValueError("exit_confirm_checks must be >= 1")

        # State
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._total_deposited = Decimal("0")
        self._deposit_shares = Decimal("0")
        self._deposit_timestamp: datetime | None = None
        self._last_compound_time: datetime | None = None
        self._redeem_assets = Decimal("0")
        self._total_yield_earned = Decimal("0")
        self._epochs_completed = 0
        self._compounds_completed = 0
        self._current_apy: Decimal | None = None  # last readable supply APY (percent)
        self._below_floor_count = 0  # consecutive sub-floor reads (exit hysteresis)

        logger.info(
            "MetaMorphoBaseYield initialized: vault=%s, deposit=%s %s, "
            "max_alloc=%d%%, compound_interval=%dh",
            self.vault_address[:10],
            self.deposit_amount,
            self.deposit_token,
            self.max_vault_allocation_pct,
            self.compound_interval_hours,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the vault yield state machine.

        Data-unavailable reads degrade to HOLD inside the per-state handlers;
        any other exception propagates (no blanket ``except -> hold`` masking a
        real bug).
        """
        if self._state == "idle":
            return self._handle_idle(market)
        if self._state == "deposited":
            return self._handle_deposited(market)
        if self._state in ("depositing", "redeeming", "compounding"):
            revert_to = self._previous_stable_state
            logger.warning(
                "Stuck in transitional state '%s' -- reverting to '%s'",
                self._state, revert_to,
            )
            self._state = revert_to
            return Intent.hold(reason=f"Reverted from stuck state to '{revert_to}'")
        return Intent.hold(reason=f"Unknown state: {self._state}")

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """Evaluate whether to deposit into the vault."""
        try:
            balance_info = market.balance(self.deposit_token)
            available = balance_info.balance
            available_usd = balance_info.balance_usd
        except (ValueError, KeyError) as e:
            logger.warning("Could not check %s balance: %s", self.deposit_token, e)
            return Intent.hold(reason=f"Balance unavailable: {e}")

        if available_usd < self.min_deposit_usd:
            return Intent.hold(
                reason=f"Insufficient {self.deposit_token}: ${available_usd:.2f} < ${self.min_deposit_usd}"
            )

        # Yield-floor entry gate: only enter when the live supply APY justifies
        # the position. A merely *unavailable* read does not block entry (the
        # deposit is low-risk and we should not NULL the strategy on a flaky
        # feed); only a readable, sub-floor APY holds capital out.
        apy = self._read_apy(market)
        if apy is not None and apy < self.min_apy_floor:
            return Intent.hold(
                reason=f"APY {apy:.2f}% below floor {self.min_apy_floor:.2f}% — staying out of the vault"
            )

        # Respect max allocation cap
        max_deposit = available * Decimal(self.max_vault_allocation_pct) / Decimal("100")
        deposit_amount = min(self.deposit_amount, max_deposit)

        if deposit_amount < Decimal("1"):
            return Intent.hold(reason="Deposit amount too small after allocation cap")

        logger.info(
            "DEPOSIT: %s %s into Moonwell vault on Base (available: %s, alloc cap: %d%%)",
            deposit_amount, self.deposit_token, available, self.max_vault_allocation_pct,
        )

        self._previous_stable_state = self._state
        self._state = "depositing"
        self._deposit_timestamp = datetime.now(UTC)

        return Intent.vault_deposit(
            protocol="metamorpho",
            vault_address=self.vault_address,
            amount=deposit_amount,
            deposit_token=self.deposit_token,
            chain=self.chain,
        )

    def _handle_deposited(self, market: MarketSnapshot) -> Intent:
        """Monitor yield: exit on a sustained sub-floor APY, else compound/hold."""
        # Yield-floor EXIT gate (checked before compound/hold). Exit only on a
        # *readable* sub-floor APY, and only after exit_confirm_checks consecutive
        # sub-floor reads (hysteresis — a single transient dip must not churn the
        # position). A missing read holds (never exit on unavailable data).
        apy = self._read_apy(market)
        if apy is None:
            # Data-unavailable: HOLD without compounding. Adding capital (a fresh
            # VAULT_DEPOSIT) without a readable yield signal would violate the
            # golden-demo "degrade to HOLD on unavailable data" gate.
            return Intent.hold(
                reason=f"APY unavailable; holding vault position without compounding "
                f"(floor {self.min_apy_floor:.2f}%)"
            )
        if apy < self.min_apy_floor:
            self._below_floor_count += 1
            if self._below_floor_count >= self.exit_confirm_checks:
                logger.info(
                    "EXIT: APY %.2f%% < floor %.2f%% for %d checks — redeeming vault position",
                    apy, self.min_apy_floor, self._below_floor_count,
                )
                self._previous_stable_state = self._state
                self._state = "redeeming"
                return self._create_redeem_intent()
            return Intent.hold(
                reason=f"APY {apy:.2f}% below floor {self.min_apy_floor:.2f}% "
                f"({self._below_floor_count}/{self.exit_confirm_checks} confirms before exit)"
            )
        # APY healthy — reset the exit counter.
        self._below_floor_count = 0

        # Check if we should compound idle USDC back into the vault
        if self._should_compound():
            try:
                balance_info = market.balance(self.deposit_token)
                idle_usdc = balance_info.balance
                idle_usd = balance_info.balance_usd
            except (ValueError, KeyError):
                idle_usd = Decimal("0")
                idle_usdc = Decimal("0")

            if idle_usd >= self.min_deposit_usd:
                compound_amount = min(idle_usdc, self.deposit_amount)
                logger.info(
                    "COMPOUND: Re-depositing %s idle %s into vault (compound #%d)",
                    compound_amount, self.deposit_token, self._compounds_completed + 1,
                )
                self._previous_stable_state = self._state
                self._state = "compounding"
                return Intent.vault_deposit(
                    protocol="metamorpho",
                    vault_address=self.vault_address,
                    amount=compound_amount,
                    deposit_token=self.deposit_token,
                    chain=self.chain,
                )

        logger.info(
            "HOLD: Vault position active (shares=%s, total_deposited=%s %s, "
            "epoch=%d, compounds=%d)",
            self._deposit_shares, self._total_deposited, self.deposit_token,
            self._epochs_completed, self._compounds_completed,
        )
        apy_str = f"{self._current_apy:.2f}%" if self._current_apy is not None else "n/a"
        return Intent.hold(
            reason=f"Vault active, earning yield (APY {apy_str} >= floor {self.min_apy_floor:.2f}%, "
            f"epoch {self._epochs_completed})"
        )

    def _read_apy(self, market: MarketSnapshot) -> Decimal | None:
        """Live supply APY (percent) for the vault's underlying market, or None if unavailable.

        Reads via the gateway-backed ``lending_rate`` (off-chain subgraph data,
        so it works on Anvil forks too). The morpho_blue USDC supply rate is the
        yield proxy for this Morpho-Blue-backed vault. Caches the last good read
        on ``_current_apy`` for status/dashboard. Returns None on any
        data-unavailable error (caller treats None as "hold, don't churn").
        """
        try:
            rate = market.lending_rate(self.rate_protocol, self.rate_token, "supply", chain=self.chain)
        except ValueError as e:  # genuinely-unavailable rate -> None (lending_rate's contract is ValueError)
            logger.warning("APY read unavailable (%s %s): %s", self.rate_protocol, self.rate_token, e)
            return None
        # A malformed/non-finite rate is a bug, not "unavailable" — let it propagate
        # rather than silently degrading to a capital-adding deposit.
        apy = Decimal(str(rate.apy_percent))
        if not apy.is_finite():
            raise ValueError(f"APY read returned non-finite value: {rate.apy_percent!r}")
        self._current_apy = apy
        return apy

    def _create_redeem_intent(self) -> Intent:
        """Redeem the full vault position (shares='all')."""
        return Intent.vault_redeem(
            protocol="metamorpho",
            vault_address=self.vault_address,
            shares="all",
            deposit_token=self.deposit_token,
            chain=self.chain,
        )

    def _should_compound(self) -> bool:
        """Check if enough time has passed since last compound."""
        if self._last_compound_time is None:
            if self._deposit_timestamp is None:
                return False
            reference_time = self._deposit_timestamp
        else:
            reference_time = self._last_compound_time

        elapsed_hours = (datetime.now(UTC) - reference_time).total_seconds() / 3600
        return elapsed_hours >= self.compound_interval_hours

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "VAULT_DEPOSIT":
                new_assets = Decimal("0")
                new_shares = Decimal("0")
                if hasattr(result, "extracted_data") and result.extracted_data:
                    deposit_data = result.extracted_data.get("deposit_data")
                    if deposit_data:
                        new_assets = Decimal(str(deposit_data.get("assets", 0)))
                        new_shares = Decimal(str(deposit_data.get("shares", 0)))

                if self._state == "compounding":
                    self._compounds_completed += 1
                    self._last_compound_time = datetime.now(UTC)
                    self._state = "deposited"
                    logger.info(
                        "Compound #%d confirmed: +%s assets, +%s shares",
                        self._compounds_completed, new_assets, new_shares,
                    )
                else:
                    self._state = "deposited"
                    self._epochs_completed += 1
                    logger.info("VAULT_DEPOSIT successful -> state=deposited")

                self._total_deposited += new_assets
                self._deposit_shares += new_shares

            elif intent_type == "VAULT_REDEEM":
                if hasattr(result, "extracted_data") and result.extracted_data:
                    redeem_data = result.extracted_data.get("redeem_data")
                    if redeem_data:
                        self._redeem_assets = Decimal(str(redeem_data.get("assets_received", 0)))
                        yield_earned = self._redeem_assets - self._total_deposited
                        if yield_earned > 0:
                            self._total_yield_earned += yield_earned
                        logger.info(
                            "Redeem confirmed: received=%s, yield=%s, total_yield=%s",
                            self._redeem_assets, yield_earned, self._total_yield_earned,
                        )
                self._state = "idle"
                self._total_deposited = Decimal("0")
                self._deposit_shares = Decimal("0")
                self._below_floor_count = 0  # reset exit hysteresis; re-entry needs APY back above floor
                # Clear the compound timers so a later re-entry starts a fresh
                # compound_interval_hours epoch instead of compounding immediately
                # off a stale _last_compound_time.
                self._deposit_timestamp = None
                self._last_compound_time = None
                logger.info("VAULT_REDEEM successful -> state=idle")
        else:
            revert_to = self._previous_stable_state
            logger.warning(
                "%s failed in state '%s' -- reverting to '%s'",
                intent_type, self._state, revert_to,
            )
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "metamorpho_base_yield",
            "chain": self.chain,
            "state": self._state,
            "vault_address": self.vault_address,
            "deposit_token": self.deposit_token,
            "total_deposited": str(self._total_deposited),
            "deposit_shares": str(self._deposit_shares),
            "total_yield_earned": str(self._total_yield_earned),
            "epochs_completed": self._epochs_completed,
            "compounds_completed": self._compounds_completed,
            "current_apy": str(self._current_apy) if self._current_apy is not None else None,
            "min_apy_floor": str(self.min_apy_floor),
            "below_floor_count": self._below_floor_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist state for crash recovery."""
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "total_deposited": str(self._total_deposited),
            "deposit_shares": str(self._deposit_shares),
            "deposit_timestamp": self._deposit_timestamp.isoformat() if self._deposit_timestamp else None,
            "last_compound_time": self._last_compound_time.isoformat() if self._last_compound_time else None,
            "redeem_assets": str(self._redeem_assets),
            "total_yield_earned": str(self._total_yield_earned),
            "epochs_completed": self._epochs_completed,
            "compounds_completed": self._compounds_completed,
            "current_apy": str(self._current_apy) if self._current_apy is not None else None,
            "below_floor_count": self._below_floor_count,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore persisted state on startup."""
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "total_deposited" in state:
            self._total_deposited = Decimal(str(state["total_deposited"]))
        if "deposit_shares" in state:
            self._deposit_shares = Decimal(str(state["deposit_shares"]))
        if state.get("deposit_timestamp"):
            self._deposit_timestamp = datetime.fromisoformat(state["deposit_timestamp"])
        if state.get("last_compound_time"):
            self._last_compound_time = datetime.fromisoformat(state["last_compound_time"])
        if "redeem_assets" in state:
            self._redeem_assets = Decimal(str(state["redeem_assets"]))
        if "total_yield_earned" in state:
            self._total_yield_earned = Decimal(str(state["total_yield_earned"]))
        if "epochs_completed" in state:
            self._epochs_completed = int(state["epochs_completed"])
        if "compounds_completed" in state:
            self._compounds_completed = int(state["compounds_completed"])
        if "below_floor_count" in state:
            self._below_floor_count = int(state["below_floor_count"])
        if state.get("current_apy"):
            self._current_apy = Decimal(str(state["current_apy"]))
        logger.info(
            "Restored state: %s (epoch %d, compounds %d)",
            self._state, self._epochs_completed, self._compounds_completed,
        )

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._state in ("deposited", "compounding", "redeeming") and self._deposit_shares > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"metamorpho-base-{self.vault_address[:16]}",
                    chain=self.chain,
                    protocol="metamorpho",
                    value_usd=self._total_deposited,
                    details={
                        "vault_address": self.vault_address,
                        "deposit_token": self.deposit_token,
                        "shares": str(self._deposit_shares),
                        "total_deposited": str(self._total_deposited),
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        intents = []
        if self._state in ("deposited", "compounding", "redeeming") and self._deposit_shares > 0:
            intents.append(
                Intent.vault_redeem(
                    protocol="metamorpho",
                    vault_address=self.vault_address,
                    shares="all",
                    deposit_token=self.deposit_token,
                    chain=self.chain,
                )
            )
        return intents
