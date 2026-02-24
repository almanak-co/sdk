"""Copy trading models: strict config schema + runtime signal records."""

from __future__ import annotations

import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

from pydantic import Field, ValidationError, field_validator, model_validator

from almanak.framework.models.base import AlmanakImmutableModel, OptionalSafeDecimal, SafeDecimal

_HEX_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class CopyTradingConfigError(ValueError):
    """Raised when strict copy-trading config validation fails."""

    def __init__(self, message: str, errors: list[Any] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


class SizingMode(StrEnum):
    """How to size copy trades relative to the leader."""

    FIXED_USD = "fixed_usd"
    PROPORTION_OF_LEADER = "proportion_of_leader"
    PROPORTION_OF_EQUITY = "proportion_of_equity"


class CopyMode(StrEnum):
    """Copy operation mode."""

    LIVE = "live"
    SHADOW = "shadow"
    REPLAY = "replay"


class SubmissionMode(StrEnum):
    """How approved copy trades should be submitted."""

    PUBLIC = "public"
    PRIVATE = "private"
    AUTO = "auto"


class LeaderConfig(AlmanakImmutableModel):
    """Leader wallet configuration."""

    address: str
    label: str | None = None
    chain: str | None = None
    weight: SafeDecimal = Decimal("1")
    max_notional_usd: OptionalSafeDecimal = None

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        if not _HEX_ADDRESS_RE.match(value):
            raise ValueError("Leader address must be a 0x-prefixed 40-byte hex address")
        return value

    @field_validator("chain")
    @classmethod
    def normalize_chain(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.lower().strip()

    @field_validator("weight")
    @classmethod
    def validate_weight(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("Leader weight must be > 0")
        return value


class MonitoringConfig(AlmanakImmutableModel):
    """Signal ingestion and freshness controls."""

    confirmation_depth: int = Field(default=1, ge=0, le=64)
    poll_interval_seconds: int = Field(default=12, ge=1, le=3600)
    lookback_blocks: int = Field(default=50, ge=1, le=100_000)
    max_signal_age_seconds: int = Field(default=300, ge=1, le=86_400)
    max_leader_lag_blocks: int = Field(default=2, ge=0, le=128)


class ActionPolicyConfig(AlmanakImmutableModel):
    """Action-level filters and trade bounds."""

    enabled: bool = True
    action_types: list[str] = Field(default_factory=list)
    protocols: list[str] = Field(default_factory=list)
    tokens: list[str] = Field(default_factory=list)
    min_usd_value: OptionalSafeDecimal = None
    max_usd_value: OptionalSafeDecimal = None

    @field_validator("action_types")
    @classmethod
    def normalize_action_types(cls, values: list[str]) -> list[str]:
        return [v.strip().upper() for v in values if v and v.strip()]

    @field_validator("protocols")
    @classmethod
    def normalize_protocols(cls, values: list[str]) -> list[str]:
        return [v.strip().lower() for v in values if v and v.strip()]

    @field_validator("tokens")
    @classmethod
    def normalize_tokens(cls, values: list[str]) -> list[str]:
        return [v.strip().upper() for v in values if v and v.strip()]

    @field_validator("min_usd_value", "max_usd_value")
    @classmethod
    def validate_optional_positive(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value < 0:
            raise ValueError("USD bounds must be >= 0")
        return value

    @model_validator(mode="after")
    def validate_bounds(self) -> ActionPolicyConfig:
        if self.min_usd_value is not None and self.max_usd_value is not None:
            if self.min_usd_value > self.max_usd_value:
                raise ValueError("min_usd_value cannot exceed max_usd_value")
        return self


class SizingConfig(AlmanakImmutableModel):
    """Sizing mode and parameters."""

    mode: SizingMode = SizingMode.FIXED_USD
    fixed_usd: SafeDecimal = Decimal("100")
    percentage_of_leader: SafeDecimal = Decimal("0.1")
    percentage_of_equity: SafeDecimal = Decimal("0.02")

    @field_validator("mode", mode="before")
    @classmethod
    def normalize_mode(cls, value: SizingMode | str) -> SizingMode:
        if isinstance(value, SizingMode):
            return value
        return SizingMode(str(value).strip().lower())

    @field_validator("fixed_usd")
    @classmethod
    def validate_fixed_usd(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("fixed_usd must be >= 0")
        return value

    @field_validator("percentage_of_leader", "percentage_of_equity")
    @classmethod
    def validate_percentage(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("percentage values must be >= 0")
        return value


class RiskConfig(AlmanakImmutableModel):
    """Risk guardrails for copy execution."""

    max_trade_usd: SafeDecimal = Decimal("1000")
    min_trade_usd: SafeDecimal = Decimal("10")
    max_daily_notional_usd: SafeDecimal = Decimal("10000")
    max_open_positions: int = Field(default=10, ge=0, le=10_000)
    max_slippage: SafeDecimal = Decimal("0.01")
    max_price_deviation_bps: int = Field(default=150, ge=0, le=10_000)

    @model_validator(mode="after")
    def validate_trade_bounds(self) -> RiskConfig:
        if self.min_trade_usd > self.max_trade_usd:
            raise ValueError("min_trade_usd cannot exceed max_trade_usd")
        return self


class ExecutionPolicyConfig(AlmanakImmutableModel):
    """Execution and operational mode configuration."""

    submission_mode: SubmissionMode = SubmissionMode.AUTO
    copy_mode: CopyMode = CopyMode.LIVE
    strict: bool = False
    shadow: bool = False
    replay_file: str | None = None

    @field_validator("submission_mode", mode="before")
    @classmethod
    def normalize_submission_mode(cls, value: SubmissionMode | str) -> SubmissionMode:
        if isinstance(value, SubmissionMode):
            return value
        return SubmissionMode(str(value).strip().lower())

    @field_validator("copy_mode", mode="before")
    @classmethod
    def normalize_copy_mode(cls, value: CopyMode | str) -> CopyMode:
        if isinstance(value, CopyMode):
            return value
        return CopyMode(str(value).strip().lower())

    @model_validator(mode="after")
    def normalize_shadow_mode(self) -> ExecutionPolicyConfig:
        if self.shadow and self.copy_mode == CopyMode.LIVE:
            return self.model_copy(update={"copy_mode": CopyMode.SHADOW})
        return self


class CopyTradingConfigV2(AlmanakImmutableModel):
    """Strict copy-trading schema for institutional workflows."""

    version: int = 2
    leaders: list[LeaderConfig] = Field(default_factory=list)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    global_policy: ActionPolicyConfig = Field(default_factory=ActionPolicyConfig)
    action_policies: dict[str, ActionPolicyConfig] = Field(default_factory=dict)
    sizing: SizingConfig = Field(default_factory=SizingConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution_policy: ExecutionPolicyConfig = Field(default_factory=ExecutionPolicyConfig)

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_shape(cls, raw: Any) -> Any:
        if not isinstance(raw, dict):
            raise ValueError("copy_trading config must be a dictionary")

        data = dict(raw)

        # Legacy alias: filters -> global_policy (remove filters to avoid extra="forbid" rejection)
        if "global_policy" not in data and "filters" in data:
            data["global_policy"] = data.pop("filters")
        elif "filters" in data:
            # global_policy already present; drop stale filters key
            data.pop("filters")

        # Legacy leader shape: ["0x..."] -> [{"address": "0x..."}]
        leaders = data.get("leaders")
        if isinstance(leaders, list):
            normalized_leaders: list[dict[str, Any]] = []
            for leader in leaders:
                if isinstance(leader, str):
                    normalized_leaders.append({"address": leader})
                elif isinstance(leader, dict):
                    normalized_leaders.append(dict(leader))
            data["leaders"] = normalized_leaders

        # Legacy scalar controls on top-level -> move into execution_policy, then remove
        # to avoid extra="forbid" rejection on the top-level model.
        _LEGACY_EP_KEYS = ("submission_mode", "copy_mode", "strict", "shadow", "replay_file")
        if "execution_policy" not in data:
            data["execution_policy"] = {}
        if isinstance(data["execution_policy"], dict):
            ep = dict(data["execution_policy"])
            if "submission_mode" in data and "submission_mode" not in ep:
                ep["submission_mode"] = data["submission_mode"]
            if "copy_mode" in data and "copy_mode" not in ep:
                ep["copy_mode"] = data["copy_mode"]
            if "strict" in data and "strict" not in ep:
                ep["strict"] = bool(data["strict"])
            if "shadow" in data and "shadow" not in ep:
                ep["shadow"] = bool(data["shadow"])
            if "replay_file" in data and "replay_file" not in ep:
                ep["replay_file"] = data["replay_file"]
            data["execution_policy"] = ep
        for key in _LEGACY_EP_KEYS:
            data.pop(key, None)
        # Also remove other known non-schema keys that configs may carry
        data.pop("copy_strict", None)

        # Ensure at least SWAP policy is derivable from global policy
        if "action_policies" not in data or not data["action_policies"]:
            gp = data.get("global_policy", {})
            data["action_policies"] = {"SWAP": gp}

        return data

    @field_validator("action_policies")
    @classmethod
    def normalize_action_policy_keys(cls, value: dict[str, ActionPolicyConfig]) -> dict[str, ActionPolicyConfig]:
        return {k.upper(): v for k, v in value.items()}

    @model_validator(mode="after")
    def ensure_default_action_policy(self) -> CopyTradingConfigV2:
        if not self.action_policies:
            return self.model_copy(update={"action_policies": {"SWAP": self.global_policy}})
        return self

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> CopyTradingConfigV2:
        """Parse and strictly validate copy_trading config."""
        try:
            return cls.model_validate(config)
        except ValidationError as exc:
            raise CopyTradingConfigError("Invalid copy_trading configuration", errors=exc.errors()) from exc

    def get_leader_weight(self, address: str) -> Decimal | None:
        """Return configured leader weight (case-insensitive)."""
        addr_lower = address.lower()
        for leader in self.leaders:
            if leader.address.lower() == addr_lower:
                return leader.weight
        return None

    def get_leader_cap(self, address: str) -> Decimal | None:
        """Return optional per-leader notional cap (USD)."""
        addr_lower = address.lower()
        for leader in self.leaders:
            if leader.address.lower() == addr_lower:
                return leader.max_notional_usd
        return None

    def to_legacy(self) -> dict[str, Any]:
        """Render config in legacy dict shape for backward compatibility."""
        return {
            "version": self.version,
            "leaders": [leader.model_dump(mode="python") for leader in self.leaders],
            "monitoring": self.monitoring.model_dump(mode="python"),
            "filters": self.global_policy.model_dump(mode="python"),
            "sizing": self.sizing.model_dump(mode="python"),
            "risk": self.risk.model_dump(mode="python"),
            "execution_policy": self.execution_policy.model_dump(mode="python"),
            "action_policies": {k: v.model_dump(mode="python") for k, v in self.action_policies.items()},
        }


@dataclass(frozen=True)
class SwapPayload:
    """Normalized SWAP-specific payload."""

    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    effective_price: Decimal | None = None
    slippage_bps: int | None = None


@dataclass(frozen=True)
class LPPayload:
    """Normalized LP action payload."""

    pool: str | None = None
    position_id: str | None = None
    amount0: Decimal | None = None
    amount1: Decimal | None = None
    range_lower: Decimal | None = None
    range_upper: Decimal | None = None
    close_fraction: Decimal | None = None


@dataclass(frozen=True)
class LendingPayload:
    """Normalized lending action payload."""

    token: str | None = None
    amount: Decimal | None = None
    collateral_token: str | None = None
    borrow_token: str | None = None
    market_id: str | None = None
    use_as_collateral: bool | None = None


@dataclass(frozen=True)
class PerpPayload:
    """Normalized perpetuals action payload."""

    market: str | None = None
    collateral_token: str | None = None
    collateral_amount: Decimal | None = None
    size_usd: Decimal | None = None
    is_long: bool | None = None
    leverage: Decimal | None = None
    position_id: str | None = None


@dataclass(frozen=True)
class LeaderEvent:
    """An on-chain event from a monitored leader wallet."""

    chain: str
    block_number: int
    tx_hash: str
    log_index: int
    timestamp: int
    from_address: str
    to_address: str
    receipt: dict
    block_hash: str | None = None
    tx_index: int | None = None
    tx_type: str | None = None
    gas_price_wei: int | None = None

    @property
    def event_id(self) -> str:
        return f"{self.chain}:{self.tx_hash}:{self.log_index}"


@dataclass(frozen=True)
class CopySignal:
    """A decoded, actionable signal derived from a LeaderEvent."""

    event_id: str
    action_type: str
    protocol: str
    chain: str
    tokens: list[str]
    amounts: dict[str, Decimal]
    amounts_usd: dict[str, Decimal]
    metadata: dict
    leader_address: str
    block_number: int
    timestamp: int
    signal_id: str | None = None
    leader_tx_hash: str | None = None
    leader_block: int | None = None
    detected_at: int = 0
    age_seconds: int = 0
    action_payload: SwapPayload | LPPayload | LendingPayload | PerpPayload | dict[str, Any] | None = None
    capability_flags: dict[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.signal_id is None:
            object.__setattr__(self, "signal_id", self.event_id)
        if self.leader_tx_hash is None:
            object.__setattr__(self, "leader_tx_hash", self.event_id.split(":")[1] if ":" in self.event_id else None)
        if self.leader_block is None:
            object.__setattr__(self, "leader_block", self.block_number)
        if self.detected_at <= 0:
            now = int(time.time())
            object.__setattr__(self, "detected_at", now)
            if self.age_seconds <= 0:
                object.__setattr__(self, "age_seconds", max(0, now - self.timestamp))


@dataclass
class CopyDecision:
    """A decision on whether to execute or skip a CopySignal."""

    signal: CopySignal
    action: str  # 'execute' or 'skip'
    skip_reason: str | None = None
    decision_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    policy_results: dict[str, Any] = field(default_factory=dict)
    skip_reason_code: str | None = None
    risk_snapshot: dict[str, Any] = field(default_factory=dict)


@dataclass
class CopyExecutionRecord:
    """Record of a copy trade execution attempt."""

    event_id: str
    intent_id: str | None = None
    status: str = "skipped"  # 'executed', 'skipped', 'failed'
    skip_reason: str | None = None
    tx_hashes: list[str] | None = None
    timestamp: int = 0
    signal_id: str | None = None
    intent_ids: list[str] | None = None
    submission_mode: SubmissionMode | str | None = None
    leader_follower_lag_ms: int | None = None
    price_deviation_bps: int | None = None
    status_code: str | None = None


@dataclass
class CopyTradingConfig:
    """Legacy-compatible wrapper for copy trading config."""

    leaders: list[dict]
    monitoring: dict
    filters: dict
    sizing: dict
    risk: dict
    execution_policy: dict = field(default_factory=dict)
    action_policies: dict = field(default_factory=dict)
    version: int = 1

    @classmethod
    def from_config(cls, config: dict) -> CopyTradingConfig:
        """Parse copy_trading config with strict V2 validation + legacy shape output."""
        strict_requested = bool(
            config.get("strict") or config.get("execution_policy", {}).get("strict") or config.get("copy_strict", False)
        )

        try:
            v2 = CopyTradingConfigV2.from_config(config)
        except CopyTradingConfigError as exc:
            if strict_requested:
                raise
            # Lenient fallback (legacy behavior)
            logger.warning(
                "V2 copy trading config validation failed, falling back to legacy mode. "
                'Common causes: float values in Decimal fields (use strings like "0.1" instead of 0.1), '
                "or unknown keys. Error: %s",
                exc,
            )
            leaders = config.get("leaders", [])
            monitoring = {
                "confirmation_depth": 1,
                "poll_interval_seconds": 12,
                "lookback_blocks": 50,
                "max_signal_age_seconds": 300,
            }
            monitoring.update(config.get("monitoring", {}))
            filters = config.get("filters", {})
            sizing = {
                "mode": "fixed_usd",
                "fixed_usd": 100,
                "percentage_of_leader": 0.1,
                "percentage_of_equity": 0.02,
            }
            sizing.update(config.get("sizing", {}))
            risk = {
                "max_trade_usd": 1000,
                "min_trade_usd": 10,
                "max_daily_notional_usd": 10000,
                "max_open_positions": 10,
                "max_slippage": 0.01,
                "max_price_deviation_bps": 150,
            }
            risk.update(config.get("risk", {}))
            return cls(
                leaders=leaders,
                monitoring=monitoring,
                filters=filters,
                sizing=sizing,
                risk=risk,
                execution_policy=config.get("execution_policy", {}),
                action_policies=config.get("action_policies", {}),
                version=int(config.get("version", 1)),
            )

        legacy = v2.to_legacy()
        filters_value = legacy["filters"]
        if "filters" not in config and "global_policy" not in config:
            filters_value = {}

        action_policies_value = legacy["action_policies"]
        if "action_policies" not in config:
            action_policies_value = {}

        return cls(
            leaders=legacy["leaders"],
            monitoring=legacy["monitoring"],
            filters=filters_value,
            sizing=legacy["sizing"],
            risk=legacy["risk"],
            execution_policy=legacy["execution_policy"],
            action_policies=action_policies_value,
            version=int(legacy["version"]),
        )

    def get_leader_weight(self, address: str) -> Decimal | None:
        """Get configured weight for a leader address (case-insensitive)."""
        addr_lower = address.lower()
        for leader in self.leaders:
            if str(leader.get("address", "")).lower() == addr_lower:
                weight = leader.get("weight")
                if weight is not None:
                    return Decimal(str(weight))
        return None
