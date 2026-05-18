"""S2 repro: emit two LP_OPEN intents into the same pool with no
registry_handle on either, on consecutive iterations.

Expected behaviour TODAY (no preflight):
  iter 1 — LP_OPEN -> compile -> sign -> submit -> NFT minted -> registry write OK
  iter 2 — LP_OPEN -> compile -> sign -> submit -> NFT minted ON-CHAIN -> registry write FAILS
           with RegistryAutoCollisionError(semantic_grouping_key="arbitrum:<pool_addr>")
           because both rows share the same auto-mode group key and partial unique
           index ix_registry_auto_mode rejects the second insert.

Expected behaviour AFTER the proposed preflight (see
docs/internal/S2-LP-Registry-Preflight-Proposal.md):
  iter 2 — LP_OPEN -> _phase_registry_preflight raises BEFORE _phase_sign.
           No second NFT is minted. ExecutionResult.success == False with
           error_phase == VALIDATION.

This strategy is the negative-path twin of the demo `uniswap_lp` strategy.
It deliberately omits `registry_handle` on the LP_OPEN factory call. Do not
copy this pattern into a demo.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
)

logger = logging.getLogger(__name__)


@dataclass
class DoubleLpOpenReproConfig:
    pool: str = "WETH/USDC/500"
    range_width_pct: Decimal = Decimal("0.20")
    amount0: Decimal = Decimal("0.001")
    amount1: Decimal = Decimal("3")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
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
    name="double_lp_open_repro",
    description="S2 repro — two LP_OPEN intents into the same pool, both with registry_handle=None",
    version="0.1.0",
    author="Almanak (internal)",
    tags=["repro", "internal", "registry-collision", "s2"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="arbitrum",
)
class DoubleLpOpenReproStrategy(IntentStrategy[DoubleLpOpenReproConfig]):
    """Emit two LP_OPENs into the same WETH/USDC/500 pool then HOLD."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.pool: str = self.config.pool
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0]
        self.token1_symbol = pool_parts[1]
        self.range_width_pct = Decimal(str(self.config.range_width_pct))
        self.amount0 = Decimal(str(self.config.amount0))
        self.amount1 = Decimal(str(self.config.amount1))

        # In-process counter — survives across iterations within one Anvil run.
        # No persistence on purpose; restarting the strategy resets the repro.
        self._opens_emitted: int = 0
        self._position_ids: list[str] = []

        logger.info(
            "DoubleLpOpenReproStrategy initialized: pool=%s amounts=%s %s + %s %s",
            self.pool,
            self.amount0,
            self.token0_symbol,
            self.amount1,
            self.token1_symbol,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._opens_emitted >= 2:
            return Intent.hold(reason="repro complete — two LP_OPENs emitted; awaiting teardown signal")

        try:
            token0_usd = market.price(self.token0_symbol)
            token1_usd = market.price(self.token1_symbol)
            current_price = token0_usd / token1_usd
        except (ValueError, KeyError) as exc:
            return Intent.hold(reason=f"price unavailable: {exc}")

        half = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half)
        range_upper = current_price * (Decimal("1") + half)

        self._opens_emitted += 1
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Emitting LP_OPEN #{self._opens_emitted} (no registry_handle)",
                strategy_id=self.strategy_id,
                details={"open_seq": self._opens_emitted, "pool": self.pool},
            )
        )
        logger.info(
            "S2 repro emitting LP_OPEN #%d on %s (registry_handle=None)",
            self._opens_emitted,
            self.pool,
        )

        # The bug surface: registry_handle is intentionally NOT passed.
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            chain="arbitrum",
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        if not success:
            return
        raw = getattr(intent, "intent_type", None)
        # IntentType is a plain Enum (not StrEnum), so str compare needs .value.
        intent_type = raw.value if hasattr(raw, "value") else raw
        if intent_type != "LP_OPEN":
            return
        position_id = self._extract_position_id(result)
        if position_id:
            self._position_ids.append(position_id)
            logger.info("S2 repro recorded position_id=%s (total=%d)", position_id, len(self._position_ids))

    @staticmethod
    def _extract_position_id(result: Any) -> str | None:
        for path in (("position_id",), ("data", "position_id"), ("metadata", "position_id")):
            cur: Any = result
            for key in path:
                if cur is None:
                    break
                cur = cur.get(key) if isinstance(cur, dict) else getattr(cur, key, None)
            # Uniswap V3 NFT IDs come through as int; coerce to str.
            if isinstance(cur, str | int) and cur != "":
                return str(cur)
        return None

    def get_open_positions(self) -> TeardownPositionSummary:
        positions = [
            PositionInfo(
                position_type=PositionType.LP,
                position_id=pid,
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("0"),
                details={"pool": self.pool},
            )
            for pid in self._position_ids
        ]
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(
        self,
        mode: TeardownMode,
        market: MarketSnapshot,
    ) -> list[Intent]:
        intents: list[Intent] = []
        for position_id in self._position_ids:
            intents.append(
                Intent.lp_close(
                    position_id=position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                    chain="arbitrum",
                )
            )
        return intents
