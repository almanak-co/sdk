"""Exchange units and execution scope do not inherit perpetual position identity."""

import pytest
from pydantic import TypeAdapter

from almanak.connectors.gmx_v2.deployments import verify_order_executors
from almanak.connectors.gmx_v2.exchange_domain import (
    Close,
    Decrease,
    ExchangeContracts,
    ExchangeIntent,
    ExchangeMarket,
    ExchangeTarget,
    Open,
    Swap,
)
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_gmx_positions import target


def exchange_target():
    position = target()
    contracts = ExchangeContracts(
        **{field: f"0x{i:040x}" for i, field in enumerate(ExchangeContracts.model_fields, 10)}
    )
    market = ExchangeMarket(**(position.market.model_dump() | {"contracts": contracts}))
    return ExchangeTarget(
        allocation=position.allocation,
        account=position.account,
        fund=position.fund,
        market=market,
        funding_asset=market.long_asset,
    )


def test_exchange_target_has_only_market_currencies_and_aggregate_share_scope():
    value = exchange_target()
    assert value.assets == (value.market.long_asset, value.market.short_asset)
    renamed = value.model_copy(update={"allocation": "Second"})
    assert renamed.liquidity_scope == value.liquidity_scope
    assert "side" not in value.model_dump()
    assert "collateral_asset" not in value.model_dump()
    with pytest.raises(ValueError, match="environment"):
        ExchangeTarget.model_validate(
            value.model_dump()
            | {
                "fund": target().fund.model_copy(
                    update={
                        "chain": target().fund.chain.model_copy(
                            update={"environment": "anvil:other"}
                        )
                    }
                )
            }
        )


def test_shared_swap_intent_retains_deadline_and_exact_asset_direction():
    value = exchange_target()
    intent = Swap(
        target=value,
        amount=AssetAmount(asset=value.assets[0], amount=100),
        minimum_output=AssetAmount(asset=value.assets[1], amount=1),
        deadline=123,
    )
    assert TypeAdapter(ExchangeIntent).validate_json(intent.model_dump_json()) == intent
    with pytest.raises(ValueError, match="two different target assets"):
        Swap.model_validate(intent.model_dump() | {"minimum_output": intent.amount})
    with pytest.raises(ValueError, match="two different target assets"):
        Swap.model_validate(
            intent.model_dump()
            | {"amount": AssetAmount(asset=value.market.share_asset, amount=100)}
        )
    with pytest.raises(ValueError):
        Swap.model_validate(intent.model_dump() | {"target": target()})


def test_shared_liquidity_intents_keep_fungible_share_quantity_and_two_outputs():
    value = exchange_target()
    amounts = [
        AssetAmount(asset=asset, amount=amount)
        for asset, amount in zip(value.assets, (10, 0), strict=True)
    ]
    opened = Open(target=value, maximum0=amounts[0], maximum1=amounts[1], deadline=100)
    assert TypeAdapter(ExchangeIntent).validate_json(opened.model_dump_json()) == opened
    decreased = Decrease(
        target=value, minimum0=amounts[0], minimum1=amounts[1], liquidity=7, deadline=100
    )
    assert decreased.liquidity == 7
    closed = Close(target=value, minimum0=amounts[0], minimum1=amounts[1], deadline=100)
    assert TypeAdapter(ExchangeIntent).validate_json(closed.model_dump_json()) == closed
    with pytest.raises(ValueError, match="currency order"):
        Open(target=value, maximum0=amounts[1], maximum1=amounts[0], deadline=100)


def test_exchange_contracts_require_distinct_explicit_roles():
    contracts = exchange_target().market.contracts
    for field in ("swap_order_executor", "deposit_handler", "withdrawal_vault"):
        dumped = contracts.model_dump()
        del dumped[field]
        with pytest.raises(ValueError, match="Field required"):
            ExchangeContracts.model_validate(dumped)
    with pytest.raises(ValueError, match="distinct nonzero"):
        ExchangeContracts.model_validate(
            contracts.model_dump() | {"deposit_vault": contracts.order_vault}
        )


@pytest.mark.parametrize("changed", ["swapOrderExecutor()", "swapHandler()"])
def test_compiled_order_handler_must_select_the_configured_swap_executor(changed):
    market = exchange_target().market

    class Reader:
        swapped = False

        def call(self, address, getter, block, *, outputs):
            assert address == market.contracts.order_handler
            selected = {
                "increaseOrderExecutor()": market.contracts.increase_order_executor,
                "decreaseOrderExecutor()": market.contracts.decrease_order_executor,
                "swapOrderExecutor()": market.contracts.swap_order_executor,
                "swapHandler()": market.contracts.swap_handler,
            }[getter]
            if self.swapped and getter == changed:
                selected = market.contracts.increase_order_executor
            return (selected,)

    reader = Reader()
    verify_order_executors(reader, market, None)
    reader.swapped = True
    with pytest.raises(DataError, match="different compiled executor"):
        verify_order_executors(reader, market, None)
