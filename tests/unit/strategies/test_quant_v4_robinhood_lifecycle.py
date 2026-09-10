"""Exercise the Robinhood fixture against the public SDK result contracts."""

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData, SwapAmounts
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.teardown import TeardownMode
from strategies.experiments.quant_v4_robinhood_lifecycle.strategy import QuantV4FullKeyStrategy

CONFIG = Path(__file__).resolve().parents[3] / "strategies/experiments/quant_v4_robinhood_lifecycle/config.json"


def strategy():
    return QuantV4FullKeyStrategy(
        config=json.loads(CONFIG.read_text()), chain="robinhood", wallet_address="0x" + "12" * 20
    )


@pytest.fixture(params=["local", "gateway"])
def result_factory(request):
    def build(**data):
        if request.param == "local":
            return ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE, **data)
        tx_hash = "0x" + "ab" * 32
        return GatewayExecutionResult(
            success=True,
            tx_hashes=[tx_hash],
            total_gas_used=1,
            receipts=[
                {
                    "transactionHash": tx_hash,
                    "blockHash": "0x" + "cd" * 32,
                    "blockNumber": 1,
                    "status": 1,
                    "gasUsed": 1,
                    "effectiveGasPrice": 1,
                    "logs": [],
                }
            ],
            execution_id="test-execution",
            **data,
        )

    return build


def entered(result_factory):
    subject = strategy()
    market = Mock(spec=["price", "balance"])
    market.price.side_effect = lambda token: Decimal("1") if token == subject.tokens[0] else Decimal("0.001")
    market.balance.side_effect = lambda token: SimpleNamespace(
        balance=Decimal("3.15") if token == subject.tokens[0] else Decimal("0")
    )
    entry = subject.decide(market)
    assert entry.amount == Decimal("1.55")
    subject.on_intent_executed(
        entry,
        True,
        result_factory(
            swap_amounts=SwapAmounts(
                amount_in=1550000,
                amount_out=1500 * 10**18,
                amount_in_decimal=Decimal("1.55"),
                amount_out_decimal=Decimal("1500"),
                token_in="USDG",
                token_out="CME",
                token_in_address=subject.tokens[0],
                token_out_address=subject.tokens[1],
            )
        ),
    )
    return subject, market


def minted(subject):
    return LPOpenData(
        position_id=7,
        tick_lower=349253,
        tick_upper=351430,
        liquidity=123,
        amount0=1450000,
        amount1=1400 * 10**18,
        pool_address=subject.key.pool_id,
        currency0=subject.tokens[0],
        currency1=subject.tokens[1],
    )


@pytest.mark.parametrize("deposit0", [0, 1450000])
def test_open_hold_resume_and_signaled_full_close_use_real_result_api(result_factory, deposit0):
    subject, market = entered(result_factory)
    opened = subject.decide(market)
    result = result_factory(position_id=7, extracted_data={"lp_open_data": replace(minted(subject), amount0=deposit0)})
    assert result.success
    assert not hasattr(result, "lp_open_data")
    subject.on_intent_executed(opened, True, result)
    resumed = strategy()
    resumed.load_persistent_state(subject.get_persistent_state())
    assert resumed.phase == "lp_open"
    assert resumed.inventory == [1550000 - deposit0, 100 * 10**18]
    for _ in range(3):
        assert resumed.decide(market).intent_type.value == "HOLD"
    close, sale = resumed.generate_teardown_intents(TeardownMode.SOFT)
    assert close.position_id == "7"
    assert sale.amount == "all"
    resumed.on_intent_executed(
        close,
        True,
        result_factory(
            lp_close_data=LPCloseData(
                amount0_collected=deposit0,
                amount1_collected=1400 * 10**18,
                liquidity_removed=123,
                pool_address=resumed.key.pool_id,
                currency0=resumed.tokens[0],
                currency1=resumed.tokens[1],
            )
        ),
    )
    assert resumed.phase == "lp_closed"
    assert resumed.inventory == [1550000, 1500 * 10**18]
    resumed.on_intent_executed(
        sale,
        True,
        result_factory(
            swap_amounts=SwapAmounts(
                amount_in=1500 * 10**18,
                amount_out=1500000,
                amount_in_decimal=Decimal("1500"),
                amount_out_decimal=Decimal("1.5"),
                token_in="CME",
                token_out="USDG",
                token_in_address=resumed.tokens[1],
                token_out_address=resumed.tokens[0],
            )
        ),
    )
    resumed.on_teardown_completed(True, Decimal("3.05"))
    assert resumed.phase == "done"
    assert resumed.inventory == [3050000, 0]
    assert resumed.position_id is None
    assert resumed.get_open_positions().positions == []


@pytest.mark.parametrize("invalid", ["missing", "wrong_pool", "missing_liquidity"])
def test_unverified_mint_retains_nft_and_prevents_another_open(result_factory, invalid):
    subject, market = entered(result_factory)
    opened = subject.decide(market)
    data = minted(subject)
    if invalid == "wrong_pool":
        data = replace(data, pool_address="0x" + "ff" * 32)
    elif invalid == "missing_liquidity":
        data = replace(data, liquidity=None)
    result = result_factory(position_id=7, extracted_data={} if invalid == "missing" else {"lp_open_data": data})
    with pytest.raises(ValueError):
        subject.on_intent_executed(opened, True, result)
    assert subject.position_id == "7"
    assert subject.error
    assert subject.decide(market).intent_type.value == "HOLD"
    (close,) = subject.generate_teardown_intents(TeardownMode.SOFT)
    assert close.position_id == "7"


@pytest.mark.parametrize("field", ["token_in_address", "token_out_address"])
@pytest.mark.parametrize("address", [None, "0x" + "ef" * 20])
def test_same_symbol_receipt_requires_exact_addresses_before_inventory_mutation(result_factory, field, address):
    subject, market = entered(result_factory)
    subject.phase = "entry_pending"
    before = list(subject.inventory)
    data = SwapAmounts(
        amount_in=1,
        amount_out=1,
        amount_in_decimal=Decimal("0.000001"),
        amount_out_decimal=Decimal("0.000000000000000001"),
        token_in="USDG",
        token_out="CME",
        token_in_address=subject.tokens[0],
        token_out_address=subject.tokens[1],
    )
    data = replace(data, **{field: address})
    with pytest.raises(ValueError, match="asset identities are missing or mismatched"):
        subject.on_intent_executed(subject._swap(0, Decimal("0.000001")), True, result_factory(swap_amounts=data))
    assert subject.inventory == before
    assert subject.phase == "entry_pending"
    assert subject.error
    assert subject.decide(market).intent_type.value == "HOLD"
