"""Atomic swap escrow keeps fee units and admission expiry distinct from keeper fields."""

import pytest
from eth_abi import decode, encode

from almanak.connectors.gmx_v2.candidate_permissions import CREATE_ORDER, ORDER_PARAMS
from almanak.connectors.gmx_v2.exchange_abi import swap_data, validate_simulation
from almanak.connectors.gmx_v2.exchange_operations import CreateSwap
from almanak.models import ZERO_ADDRESS, AssetAmount
from tests.test_gmx_exchange_domain import exchange_target


def swap_operation(reverse=False):
    target = exchange_target()
    assets = target.assets[::-1] if reverse else target.assets
    return CreateSwap(
        target=target,
        amount=AssetAmount(asset=assets[0], amount=100),
        minimum_output=AssetAmount(asset=assets[1], amount=7),
        execution_fee=AssetAmount(asset=target.market.wrapped_native, amount=3),
        admission_deadline=1234,
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_swap_multicall_correlates_every_escrow_transfer_with_one_request(reverse):
    from eth_utils import keccak

    operation = swap_operation(reverse)
    raw = swap_data(operation)
    assert raw[:4] == keccak(text="multicall(bytes[])")[:4]
    calls = decode(("bytes[]",), raw[4:])[0]
    assert len(calls) == (3 if reverse else 2)
    for call, amount in zip(calls[:-1], operation.inputs, strict=True):
        assert call[:4] == keccak(text="sendTokens(address,address,uint256)")[:4]
        assert decode(("address", "address", "uint256"), call[4:100]) == (
            amount.asset.reference,
            operation.target.market.contracts.order_vault,
            amount.amount,
        )
        assert not any(call[100:])
        assert len(call) == len(calls[-1])
    assert calls[-1][:4] == keccak(text=CREATE_ORDER)[:4]
    addresses, numbers, *rest = decode((ORDER_PARAMS,), calls[-1][4:])[0]
    assert addresses[4] == ZERO_ADDRESS
    assert addresses[5] == operation.amount.asset.reference
    assert addresses[6] == (operation.target.market.share_asset.reference,)
    assert numbers == (0, 0, 0, 0, 3, 0, 7, 0)
    assert rest == [0, 0, False, False, False, bytes(32), ()]
    expired = operation.model_copy(update={"admission_deadline": 1})
    assert swap_data(expired) == raw


@pytest.mark.parametrize("reverse", [False, True])
def test_simulated_swap_must_return_exactly_one_key_after_empty_transfer_results(reverse):
    operation = swap_operation(reverse)
    values = (b"",) * len(operation.inputs) + (b"\x12" * 32,)
    validate_simulation(operation, "0x" + encode(("bytes[]",), (values,)).hex())
    for invalid in (values[:-1], (*values[:-1], bytes(32)), (b"\x01", *values[1:])):
        with pytest.raises(ValueError, match="empty transfers and one request key"):
            validate_simulation(operation, "0x" + encode(("bytes[]",), (invalid,)).hex())
    with pytest.raises(ValueError, match="noncanonical"):
        validate_simulation(operation, "0x" + (encode(("bytes[]",), (values,)) + bytes(32)).hex())
