"""V3 LP close tolerances remain outside the swap teardown ladder."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.framework.intents.vocabulary import Intent
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager, ExecutionAttempt
from almanak.framework.teardown.slippage_policy import fixed_teardown_slippage
from almanak.framework.teardown.teardown_manager import TeardownManager


@pytest.mark.parametrize(
    ("protocol", "max_slippage", "expected"),
    [
        ("uniswap_v3", None, Decimal("0.99")),
        ("sushiswap_v3", None, Decimal("0.99")),
        ("pancakeswap_v3", None, Decimal("0.99")),
        ("agni_finance", None, Decimal("0.99")),
        ("uniswap_v3", Decimal("0.005"), Decimal("0.005")),
    ],
)
def test_v3_lp_close_resolves_its_own_teardown_tolerance(
    protocol: str, max_slippage: Decimal | None, expected: Decimal
) -> None:
    intent = Intent.lp_close(position_id="7", protocol=protocol, max_slippage=max_slippage)

    assert fixed_teardown_slippage(intent) == expected


@pytest.mark.parametrize(
    "intent",
    [
        SimpleNamespace(protocol="uniswap_v3", intent_type="LP_CLOSE"),
        {"protocol": "uniswap_v3", "type": "LP_CLOSE"},
        {"protocol": "uniswap_v3", "type": "IntentType.LP_CLOSE"},
        {"protocol": "uniswap_v3", "intent_type": "LP_CLOSE"},
    ],
)
def test_v3_lp_close_fixed_tolerance_accepts_resumed_intent_shapes(intent: object) -> None:
    assert fixed_teardown_slippage(intent) == Decimal("0.99")


def test_v3_lp_close_fixed_tolerance_accepts_the_persisted_serialized_shape() -> None:
    serialized = Intent.lp_close(position_id="7", protocol="uniswap_v3").serialize()

    assert serialized["type"] == "LP_CLOSE"
    assert fixed_teardown_slippage(serialized) == Decimal("0.99")


def test_teardown_ladder_does_not_replace_an_undeclared_v3_close_tolerance() -> None:
    intent = Intent.lp_close(position_id="7", protocol="uniswap_v3")

    cloned = TeardownManager()._clone_intent_with_slippage(intent, Decimal("0.02"))

    assert cloned is intent
    assert cloned.max_slippage is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "intent",
    [
        Intent.lp_close(position_id="7", protocol="uniswap_v3"),
        {"protocol": "uniswap_v3", "type": "LP_CLOSE"},
    ],
)
async def test_v3_lp_close_retries_its_fixed_default_instead_of_the_swap_ladder(intent: object) -> None:
    execute = AsyncMock(
        return_value=ExecutionAttempt(success=True, slippage_used=Decimal("0.99"), actual_slippage=Decimal("0.99"))
    )

    result = await EscalatingSlippageManager().execute_with_escalation(
        intent=intent,
        position_value=Decimal("100"),
        execute_func=execute,
    )

    assert result.success
    assert result.final_slippage == Decimal("0.99")
    execute.assert_awaited_once_with(intent, Decimal("0.99"))
