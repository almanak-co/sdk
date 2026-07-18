"""Configuration and state types for vault integration."""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator
from web3 import Web3

from almanak.core.models.config import VaultVersion


class SettlementPhase(Enum):
    """Phases of the vault settlement cycle.

    The base flow settles deposits:
    ``IDLE -> PROPOSING -> PROPOSED -> SETTLING -> SETTLED -> IDLE``.

    Lagoon v0.5.0 consumes a valuation proposal on each settle call
    (``updateNewTotalAssets`` is single-use, reset to ``type(uint256).max``).
    Because ``settleDeposit`` already spends the first proposal, settling any
    remaining redeem shares requires a *fresh* proposal. The redeem leg therefore
    has its own resumable sub-phases:
    ``... -> PROPOSING_REDEEM -> PROPOSED_REDEEM -> SETTLING_REDEEM -> SETTLED``.
    """

    IDLE = "idle"
    PROPOSING = "proposing"
    PROPOSED = "proposed"
    SETTLING = "settling"
    SETTLED = "settled"
    # Redeem leg (second proposal) -- Lagoon v0.5.0 single-use-proposal recovery.
    PROPOSING_REDEEM = "proposing_redeem"
    PROPOSED_REDEEM = "proposed_redeem"
    SETTLING_REDEEM = "settling_redeem"


class VaultReleasePhase(Enum):
    """Phases of the teardown vault-release sequence (VIB-5667).

    Persisted so the release is crash-resumable: on restart the manager reads the
    on-chain ``state()`` and the persisted phase and resumes from the interrupted
    point without re-issuing a completed leg.

    ``NOT_STARTED -> PROPOSED -> CLOSING_INITIATED -> CLOSED -> DEPOSITORS_RELEASED``

    - PROPOSED: ``updateNewTotalAssets`` landed (vault still Open).
    - CLOSING_INITIATED: ``initiateClosing`` landed (vault Closing).
    - CLOSED: ``close`` landed (vault Closed; all depositor capital claimable).
    - DEPOSITORS_RELEASED: manager's own residual shares redeemed (terminal).
    """

    NOT_STARTED = "not_started"
    PROPOSED = "release_proposed"
    CLOSING_INITIATED = "closing_initiated"
    CLOSED = "closed"
    DEPOSITORS_RELEASED = "depositors_released"


class VaultAction(Enum):
    """Actions the vault lifecycle manager can take."""

    HOLD = "hold"
    SETTLE = "settle"
    RESUME_SETTLE = "resume_settle"


_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


class VaultConfig(BaseModel):
    """Configuration for vault integration, parsed from config.json."""

    vault_address: str
    valuator_address: str

    @field_validator("vault_address", "valuator_address")
    @classmethod
    def validate_eth_address(cls, v: str) -> str:
        if not _ETH_ADDRESS_RE.match(v):
            raise ValueError(f"Invalid Ethereum address: {v!r} (must be 0x + 40 hex chars)")
        return Web3.to_checksum_address(v)

    underlying_token: str
    version: VaultVersion = Field(default=VaultVersion.V0_5_0)
    settlement_interval_minutes: int = Field(default=60)
    min_valuation_change_down_bps: int = Field(default=500)
    max_valuation_change_up_bps: int = Field(default=1000)
    auto_settle_redeems: bool = Field(default=True)
    redeem_failure_fatal: bool = Field(default=True)
    # VIB-5667: on teardown, transition the vault Open->Closing->Closed so ALL
    # depositors (including a deposit-only user who never requested redemption)
    # can redeem their capital. Default ON — NOT releasing strands depositors.
    # The escape hatch (False) is only for handing an open vault to a successor
    # manager. Irreversible (Open->Closed is one-way), so default-true + explicit
    # is the correct safety posture.
    release_on_teardown: bool = Field(default=True)

    # --- Share-backed AUM invariant (VIB-5672, vault ship-gate #1) ---
    # The vault Safe must hold ONLY share-backed AUM: capital that flowed through
    # requestDeposit -> settle (Option A, ratified). ``valuate()`` sums the whole
    # Safe, so any non-depositor capital commingled there (a manager seed, working
    # capital) inflates the proposed NAV, mis-prices every depositor's shares, and
    # mints fee-shares against phantom AUM (an irreversible fund-loss path). The
    # settlement-time guard refuses to propose a NAV that materially exceeds the
    # share-backed base = on-chain ``totalAssets`` + pending deposit assets.
    #
    # NAV legitimately grows with strategy PnL between settlements, so the guard
    # targets UNEXPLAINED excess only. The tolerance is generous enough to never
    # fire on plausible inter-settlement PnL, while the 100x commingling case in
    # the VIB-5667 E2E (200k seed alongside 2k of depositor capital) always fires.
    nav_share_backed_tolerance_bps: int = Field(
        default=500,
        ge=0,
        description=(
            "Relative tolerance (basis points) for the share-backed AUM invariant. "
            "Proposed NAV may exceed on-chain totalAssets + pending deposits by up to "
            "this fraction before the guard fires; sized to absorb legitimate "
            "inter-settlement PnL (default 500 bps = 5%)."
        ),
    )
    nav_share_backed_abs_floor: int = Field(
        default=0,
        ge=0,
        description=(
            "Absolute floor (RAW underlying token units) added on top of the relative "
            "tolerance for the share-backed AUM invariant. Cushions dust / rounding on "
            "a small share-backed base. Decimal-dependent (e.g. 10 USDC = 10_000_000)."
        ),
    )


@dataclass
class VaultState:
    """Runtime state for vault lifecycle management."""

    last_valuation_time: datetime | None = None
    last_total_assets: int = 0
    last_proposed_total_assets: int = 0
    last_pending_deposits: int = 0
    last_settlement_epoch: int = 0
    settlement_phase: SettlementPhase = SettlementPhase.IDLE
    initialized: bool = False
    settlement_nonce: int = 0  # Incrementing counter to disambiguate same-value settlements
    # VIB-5667: teardown vault-release phase (crash-resumable). Independent of the
    # settlement phase — release only runs during teardown, after position closure.
    release_phase: VaultReleasePhase = VaultReleasePhase.NOT_STARTED
    release_final_nav: int = 0  # Final NAV proposed for close() (underlying units)


@dataclass
class SettlementResult:
    """Result of a vault settlement cycle."""

    success: bool
    deposits_received: int = 0
    redemptions_processed: int = 0
    new_total_assets: int = 0
    shares_minted: int = 0
    shares_burned: int = 0
    fee_shares_minted: int = 0
    epoch_id: int = 0
    # VIB-5666 — True when any settlement tx's commit/accounting write degraded
    # (loud ERROR + deferred-write log). The on-chain settlement still succeeded;
    # this only signals the books did not fully tie for this cycle and an operator
    # / reconcile pass should replay the deferred writes. Never blocks settlement.
    accounting_degraded: bool = False


@dataclass
class ReleaseResult:
    """Result of a teardown vault-release sequence (VIB-5667).

    ``released`` is True when the vault reached ``Closed`` (depositor capital is
    claimable) OR was already Closed on entry. ``skipped`` is True when release
    was disabled or not applicable (no vault state to release). ``degraded`` marks
    a genuine shortfall / preflight failure that could NOT force a safe close —
    the operator must intervene; teardown still continues to reduce on-chain risk.
    """

    released: bool = False
    skipped: bool = False
    degraded: bool = False
    final_state: str = ""
    final_nav: int = 0
    manager_shares_redeemed: int = 0
    reason: str = ""
