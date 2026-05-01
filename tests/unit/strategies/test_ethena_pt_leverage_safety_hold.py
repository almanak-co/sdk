"""Regression test for VIB-3815 (QA April31 NEW-3) — ethena_pt_leverage HOLD on safety decline.

QA-PostFixesApril31-Tests.md NEW-3: when ``build_pt_leverage_loop`` raises a
``ValueError`` because its safety checks fail (PT maturity below
MIN_DAYS_TO_MATURITY, projected health factor below the floor), the strategy
previously let the exception propagate, causing the runner to classify the
iteration as ``STRATEGY_ERROR`` and pollute pass-rate metrics.

The fix translates that ValueError to ``Intent.hold(reason=...)`` in
``_handle_idle`` BEFORE the state transition, mirroring the contract
established by VIB-3744 / VIB-3749 / VIB-3754: operator-visible safety
declines are HOLD, not STRATEGY_ERROR. STRATEGY_ERROR is reserved for true
exceptions (uncaught code paths, framework misuse).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import HoldIntent
from strategies.incubating.ethena_pt_leverage.strategy import EthenaPTLeverageStrategy

_PT_MOD = "strategies.incubating.ethena_pt_leverage.strategy"


def _build_strategy() -> EthenaPTLeverageStrategy:
    cfg = {
        "chain": "ethereum",
        "wallet_address": "0x" + "aa" * 20,
        "morpho_market_id": "0x" + "11" * 32,
        "pt_token": "PT-sUSDe-29MAY2026",
        "pt_token_address": "0x" + "22" * 20,
        "pendle_market": "0x" + "33" * 20,
        "borrow_token": "USDC",
        "target_leverage": "3.0",
        "lltv": "0.915",
        "min_health_factor": "1.3",
        "max_slippage": "0.01",
        "exit_days_before_maturity": 7,
    }
    return EthenaPTLeverageStrategy(
        config=cfg,
        chain="ethereum",
        wallet_address="0x" + "aa" * 20,
    )


def _market_with_balance(usdc_balance: Decimal) -> MagicMock:
    market = MagicMock()
    # Strategy queries balance via _get_balance(market, "USDC"). Stubbing the
    # internal helper is cleaner than reverse-engineering the MarketSnapshot
    # API surface.
    return market


class TestEthenaPTLeverageSafetyHold:
    def test_pt_maturity_safety_check_returns_hold_not_strategy_error(self):
        """Headline VIB-3815 case: ``build_pt_leverage_loop`` raising
        ``ValueError("PT expires in 6 days, minimum is 7 days")`` must be
        caught and translated to HOLD instead of bubbling up as
        STRATEGY_ERROR. State must NOT advance to "entering" since the entry
        was refused."""
        s = _build_strategy()
        market = _market_with_balance(Decimal("500"))

        with (
            patch.object(s, "_get_balance", return_value=Decimal("500")),
            patch(
                f"{_PT_MOD}.build_pt_leverage_loop",
                side_effect=ValueError(
                    "PT leverage loop safety check failed: "
                    "PT expires in 6 days, minimum is 7 days. Choose a PT with later maturity."
                ),
            ),
        ):
            intent = s.decide(market)

        assert isinstance(intent, HoldIntent), (
            f"Expected HoldIntent, got {type(intent).__name__} — safety-check "
            f"declines must NOT bubble up as STRATEGY_ERROR"
        )
        assert "safety check" in (intent.reason or "").lower()
        assert "PT expires in 6 days" in (intent.reason or "")
        # Critical: state must remain idle since entry was refused. Advancing
        # to "entering" would leave the state machine waiting for a flash loan
        # confirmation that never comes.
        assert s._phase == "idle"

    def test_safety_check_pass_proceeds_to_entry(self):
        """When the safety check passes, the strategy advances to "entering"
        and returns the flash loan intent."""
        s = _build_strategy()
        market = _market_with_balance(Decimal("500"))
        flash_intent = MagicMock()
        flash_intent.intent_type = MagicMock()
        flash_intent.intent_type.value = "FLASH_LOAN"

        with (
            patch.object(s, "_get_balance", return_value=Decimal("500")),
            patch(f"{_PT_MOD}.build_pt_leverage_loop", return_value=flash_intent),
        ):
            intent = s.decide(market)

        assert intent is flash_intent
        assert s._phase == "entering"

    def test_insufficient_balance_returns_hold_unchanged(self):
        """The pre-existing insufficient-balance HOLD path must remain
        unaffected by the new safety-check catch — guard against accidental
        regression on the other early HOLD branch."""
        s = _build_strategy()
        market = _market_with_balance(Decimal("50"))

        with patch.object(s, "_get_balance", return_value=Decimal("50")):
            intent = s.decide(market)

        assert isinstance(intent, HoldIntent)
        assert "Insufficient" in (intent.reason or "")
        assert s._phase == "idle"

    def test_unrelated_value_error_propagates(self):
        """CodeRabbit feedback on PR #1987: the safety-decline catch must NOT
        mask unrelated ``ValueError``s (timeline construction bugs, dataclass
        misuse, etc.) — those are real STRATEGY_ERRORs and must propagate."""
        s = _build_strategy()
        market = _market_with_balance(Decimal("500"))

        with (
            patch.object(s, "_get_balance", return_value=Decimal("500")),
            patch(
                f"{_PT_MOD}.build_pt_leverage_loop",
                side_effect=ValueError("timeline event construction failed: bad enum"),
            ),
            pytest.raises(ValueError, match="timeline event construction"),
        ):
            s.decide(market)
        # State must NOT have advanced — the exception bypassed the success path.
        assert s._phase == "idle"
