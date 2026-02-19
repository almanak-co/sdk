"""Intent builder for mapping normalized CopySignal objects to Almanak intents."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent, IntentSequence
from almanak.framework.services.copy_sizer import CopySizer
from almanak.framework.services.copy_trading_models import (
    CopySignal,
    CopyTradingConfigV2,
    LendingPayload,
    LPPayload,
    PerpPayload,
    SwapPayload,
)


@dataclass(frozen=True)
class CopyIntentBuildResult:
    """Result of mapping a copy signal to one or more intents."""

    intent: Any | None
    reason_code: str | None = None
    details: dict[str, Any] | None = None


class CopyIntentBuilder:
    """Translate `CopySignal` into typed Almanak intents."""

    def __init__(
        self,
        config: CopyTradingConfigV2,
        sizer: CopySizer | None = None,
    ) -> None:
        self._config = config
        self._sizer = sizer

    def build(self, signal: CopySignal) -> CopyIntentBuildResult:
        """Build an intent for a normalized copy signal.

        Returns a structured result with an explicit skip reason when mapping
        cannot be performed safely.
        """
        action = signal.action_type.upper()

        if action == "SWAP":
            return self._build_swap(signal)
        if action == "LP_OPEN":
            return self._build_lp_open(signal)
        if action == "LP_CLOSE":
            return self._build_lp_close(signal)
        if action == "SUPPLY":
            return self._build_supply(signal)
        if action == "WITHDRAW":
            return self._build_withdraw(signal)
        if action == "BORROW":
            return self._build_borrow(signal)
        if action == "REPAY":
            return self._build_repay(signal)
        if action == "PERP_OPEN":
            return self._build_perp_open(signal)
        if action == "PERP_CLOSE":
            return self._build_perp_close(signal)

        return CopyIntentBuildResult(
            intent=None,
            reason_code="unsupported_action_type",
            details={"action_type": signal.action_type},
        )

    def _desired_notional_usd(self, signal: CopySignal) -> Decimal | None:
        if self._sizer is None:
            return self._config.sizing.fixed_usd

        leader_weight = self._config.get_leader_weight(signal.leader_address)
        return self._sizer.compute_size(signal, leader_weight=leader_weight)

    def _leader_notional_usd(self, signal: CopySignal) -> Decimal:
        if signal.amounts_usd:
            return max((abs(v) for v in signal.amounts_usd.values()), default=Decimal("0"))

        if isinstance(signal.action_payload, PerpPayload) and signal.action_payload.size_usd is not None:
            return abs(signal.action_payload.size_usd)

        meta = signal.metadata.get("notional_usd")
        if meta is not None:
            return abs(Decimal(str(meta)))

        return Decimal("0")

    def _sizing_scale(self, signal: CopySignal) -> tuple[Decimal, Decimal | None]:
        desired = self._desired_notional_usd(signal)
        if desired is None or desired <= 0:
            return Decimal("0"), desired

        leader = self._leader_notional_usd(signal)
        if leader <= 0:
            return Decimal("1"), desired

        return desired / leader, desired

    def _build_swap(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, SwapPayload) else None

        token_in = payload.token_in if payload is not None else (signal.tokens[0] if len(signal.tokens) > 0 else None)
        token_out = payload.token_out if payload is not None else (signal.tokens[1] if len(signal.tokens) > 1 else None)
        if token_in is None or token_out is None:
            return CopyIntentBuildResult(intent=None, reason_code="swap_tokens_missing")

        amount_usd = self._desired_notional_usd(signal)
        if amount_usd is None or amount_usd <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="swap_size_invalid")

        intent = Intent.swap(
            from_token=token_in,
            to_token=token_out,
            amount_usd=amount_usd,
            max_slippage=self._config.risk.max_slippage,
            protocol=signal.protocol,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent, details={"amount_usd": str(amount_usd)})

    def _build_lp_open(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LPPayload) else None
        if payload is None:
            return CopyIntentBuildResult(intent=None, reason_code="lp_payload_missing")

        if payload.pool is None:
            return CopyIntentBuildResult(intent=None, reason_code="lp_pool_missing")
        if payload.range_lower is None or payload.range_upper is None:
            return CopyIntentBuildResult(intent=None, reason_code="lp_range_missing")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="lp_size_invalid")

        amount0 = (payload.amount0 or Decimal("0")) * scale
        amount1 = (payload.amount1 or Decimal("0")) * scale
        if amount0 <= 0 and amount1 <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="lp_amount_missing")

        intent = Intent.lp_open(
            pool=payload.pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=payload.range_lower,
            range_upper=payload.range_upper,
            protocol=signal.protocol,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_lp_close(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LPPayload) else None
        position_id = payload.position_id if payload is not None else None
        if position_id is None:
            position_id = signal.metadata.get("position_id")
        if position_id is None:
            return CopyIntentBuildResult(intent=None, reason_code="lp_position_missing")

        intent = Intent.lp_close(
            position_id=str(position_id),
            pool=payload.pool if payload is not None else None,
            protocol=signal.protocol,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_supply(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LendingPayload) else None
        if payload is None or payload.token is None or payload.amount is None:
            return CopyIntentBuildResult(intent=None, reason_code="supply_payload_incomplete")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="supply_size_invalid")

        intent = Intent.supply(
            protocol=signal.protocol,
            token=payload.token,
            amount=payload.amount * scale,
            use_as_collateral=True if payload.use_as_collateral is None else payload.use_as_collateral,
            market_id=payload.market_id,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_withdraw(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LendingPayload) else None
        if payload is None or payload.token is None or payload.amount is None:
            return CopyIntentBuildResult(intent=None, reason_code="withdraw_payload_incomplete")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="withdraw_size_invalid")

        intent = Intent.withdraw(
            protocol=signal.protocol,
            token=payload.token,
            amount=payload.amount * scale,
            withdraw_all=False,
            market_id=payload.market_id,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_borrow(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LendingPayload) else None
        if (
            payload is None
            or payload.borrow_token is None
            or payload.collateral_token is None
            or payload.amount is None
        ):
            return CopyIntentBuildResult(intent=None, reason_code="borrow_payload_incomplete")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="borrow_size_invalid")

        collateral_amount = payload.amount * scale
        borrow_amount = payload.amount * scale

        intent = Intent.borrow(
            protocol=signal.protocol,
            collateral_token=payload.collateral_token,
            collateral_amount=collateral_amount,
            borrow_token=payload.borrow_token,
            borrow_amount=borrow_amount,
            market_id=payload.market_id,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_repay(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, LendingPayload) else None
        if payload is None or payload.borrow_token is None or payload.amount is None:
            return CopyIntentBuildResult(intent=None, reason_code="repay_payload_incomplete")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="repay_size_invalid")

        intent = Intent.repay(
            protocol=signal.protocol,
            token=payload.borrow_token,
            amount=payload.amount * scale,
            repay_full=False,
            market_id=payload.market_id,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_perp_open(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, PerpPayload) else None
        if (
            payload is None
            or payload.market is None
            or payload.collateral_token is None
            or payload.collateral_amount is None
            or payload.size_usd is None
        ):
            return CopyIntentBuildResult(intent=None, reason_code="perp_open_payload_incomplete")

        scale, _ = self._sizing_scale(signal)
        if scale <= 0:
            return CopyIntentBuildResult(intent=None, reason_code="perp_open_size_invalid")

        intent = Intent.perp_open(
            market=payload.market,
            collateral_token=payload.collateral_token,
            collateral_amount=payload.collateral_amount * scale,
            size_usd=payload.size_usd * scale,
            is_long=True if payload.is_long is None else payload.is_long,
            leverage=Decimal("1") if payload.leverage is None else payload.leverage,
            max_slippage=self._config.risk.max_slippage,
            protocol=signal.protocol,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def _build_perp_close(self, signal: CopySignal) -> CopyIntentBuildResult:
        payload = signal.action_payload if isinstance(signal.action_payload, PerpPayload) else None
        if payload is None or payload.market is None or payload.collateral_token is None:
            return CopyIntentBuildResult(intent=None, reason_code="perp_close_payload_incomplete")

        intent = Intent.perp_close(
            market=payload.market,
            collateral_token=payload.collateral_token,
            is_long=True if payload.is_long is None else payload.is_long,
            size_usd=payload.size_usd,
            max_slippage=self._config.risk.max_slippage,
            protocol=signal.protocol,
            chain=signal.chain,
        )
        return CopyIntentBuildResult(intent=intent)

    def build_sequence(self, signals: list[CopySignal]) -> CopyIntentBuildResult:
        """Build a deterministic intent sequence from a batch of signals."""
        intents: list[Any] = []
        for signal in sorted(signals, key=lambda s: (s.leader_block or s.block_number, s.signal_id or s.event_id)):
            result = self.build(signal)
            if result.intent is not None:
                intents.append(result.intent)

        if not intents:
            return CopyIntentBuildResult(intent=None, reason_code="no_actionable_signals")

        if len(intents) == 1:
            return CopyIntentBuildResult(intent=intents[0])

        return CopyIntentBuildResult(intent=IntentSequence(intents))
