"""The Lido exit declares the asset its swap connector actually delivers."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.demo_strategies.lido_staker.strategy import LidoStakerStrategy
from almanak.framework.runner.reconciliation import BalanceSnapshot, build_reconciliation_report
from almanak.framework.teardown import TeardownMode


@pytest.mark.parametrize("wrapped,token", [(True, "wstETH"), (False, "stETH")])
@pytest.mark.parametrize("mode", [TeardownMode.SOFT, TeardownMode.HARD])
def test_teardown_declares_wrapped_output_and_reconciles_receipt(wrapped, token, mode):
    strategy = LidoStakerStrategy(
        chain="ethereum", wallet_address="0x" + "1" * 40, config={"receive_wrapped": wrapped}
    )
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("0.4"))
    intents = strategy.generate_teardown_intents(mode=mode, market=market)
    assert len(intents) == 1
    intent = intents[0]
    assert intent.from_token == token
    assert intent.to_token == "WETH"
    assert intent.protocol == "uniswap_v3"
    assert intent.amount == "all"
    assert intent.max_slippage == (Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005"))
    market.balance.assert_called_once_with(token)
    pre = BalanceSnapshot(datetime.now(UTC), {token: Decimal("0.4"), "WETH": Decimal("0"), "ETH": Decimal("10")})
    post = BalanceSnapshot(datetime.now(UTC), {token: Decimal("0"), "WETH": Decimal("0.5"), "ETH": Decimal("9.999")})
    execution = SimpleNamespace(
        swap_amounts=SimpleNamespace(amount_in_decimal=Decimal("0.4"), amount_out_decimal=Decimal("0.5"))
    )
    report = build_reconciliation_report(pre=pre, post=post, intent=intent, execution_result=execution)
    assert report.enforced is True
    assert report.incident is False
    assert report.mismatches == []
    wrong_asset = intent.model_copy(update={"to_token": "ETH"})
    wrong_report = build_reconciliation_report(pre=pre, post=post, intent=wrong_asset, execution_result=execution)
    assert wrong_report.incident is True
    assert [m.token for m in wrong_report.mismatches] == ["ETH"]


def test_empty_lido_balance_emits_no_swap():
    strategy = LidoStakerStrategy(chain="ethereum", wallet_address="0x" + "1" * 40, config={})
    market = MagicMock()
    market.balance.return_value = SimpleNamespace(balance=Decimal("0"))
    assert strategy.generate_teardown_intents(market=market) == []
