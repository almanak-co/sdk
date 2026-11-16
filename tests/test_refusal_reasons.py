"""Policy refusals carry the values an author needs to choose workable limits."""

from types import SimpleNamespace

import pytest

from almanak.caip import AssetType
from almanak.connectors.refusals import below_entry_minimum
from almanak.gateway.ethereum import DataError
from almanak.gateway.production_fees import validate_fee_limits
from almanak.models import AssetAmount

ETH = AssetType("eip155:1/slip44:60")


def policy(**changes):
    values = {
        "asset": ETH,
        "max_gas_limit": 500_000,
        "max_gas_price_wei": 2_000_000_000,
        "max_transaction_fee": AssetAmount(asset=ETH, amount=10**15),
        "gas_limit_buffer_bps": 2000,
    }
    return SimpleNamespace(**{**values, **changes})


def refusal(call):
    with pytest.raises(DataError) as refused:
        call()
    return refused.value.kind, str(refused.value)


def test_gas_limit_refusal_names_the_buffered_estimate_and_the_limit():
    assert refusal(lambda: validate_fee_limits(policy(), 600_000, 10**9)) == (
        "gas_limit",
        "estimated gas exceeds the configured gas limit: 600000 gas including the 2000 bps "
        "gas_limit_buffer_bps, max_gas_limit is 500000",
    )


def test_gas_price_refusal_names_the_quote_and_the_ceiling():
    assert refusal(lambda: validate_fee_limits(policy(), 100_000, 3_000_000_000)) == (
        "gas_price",
        "market gas quote exceeds the configured price ceiling: quoted 3000000000 wei, "
        "max_gas_price_wei is 2000000000",
    )


def test_fee_ceiling_refusal_names_the_maximum_cost_and_the_ceiling():
    assert refusal(lambda: validate_fee_limits(policy(), 500_000, 2 * 10**9, 7)) == (
        "gas_fee_quote",
        "transaction maximum cost exceeds its fee ceiling: 500000 gas at 2000000000 wei "
        "plus 7 L1 data and operator fees costs up to 1000000000000007, max_transaction_fee "
        f"is 1000000000000000 {ETH} base units",
    )
    # Outside OP Stack networks there is no additional exposure to name.
    kind, message = refusal(
        lambda: validate_fee_limits(policy(max_gas_limit=600_000), 500_001, 2 * 10**9)
    )
    assert kind == "gas_fee_quote" and "L1 data" not in message


def test_entry_minimum_refusal_names_the_balance_and_the_minimum():
    error = below_entry_minimum(AssetAmount(asset=ETH, amount=10), 25)
    assert error.kind == "gas_budget"
    assert str(error) == (
        f"gas payer is below the configured entry minimum: it holds 10 {ETH}, "
        "minimum_native_balance is 25 (base units)"
    )
    assert "an unobserved balance" in str(below_entry_minimum(None, 25))
