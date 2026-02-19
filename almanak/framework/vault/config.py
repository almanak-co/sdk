"""Configuration and state types for vault integration."""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator
from web3 import Web3

from almanak.core.models.config import VaultVersion


class SettlementPhase(Enum):
    """Phases of the vault settlement cycle."""

    IDLE = "idle"
    PROPOSING = "proposing"
    PROPOSED = "proposed"
    SETTLING = "settling"
    SETTLED = "settled"


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
