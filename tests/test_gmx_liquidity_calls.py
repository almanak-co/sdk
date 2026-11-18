"""GM calls retain exact approval spenders, vault transfers, request kinds and cancellation keys."""

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.gmx_v2.exchange_abi import call
from almanak.connectors.gmx_v2.liquidity_abi import CREATE_DEPOSIT, CREATE_WITHDRAWAL
from almanak.connectors.gmx_v2.liquidity_operations import CancelLiquidityRequest
from tests.gmx_native_runtime import gm_completed as gm_completed


def test_actual_gm_operations_encode_fund_scoped_calls(completed):
    for outcome in completed.outcomes:
        if outcome.kind != "gmx_transaction_outcome":
            continue
        operation = outcome.operation
        encoded = call(operation)
        assert encoded.fund == operation.target.fund and encoded.value == 0
        if operation.kind == "gmx_exchange_approval":
            assert encoded.to == operation.amount.asset.reference
            spender, amount = decode(("address", "uint256"), bytes.fromhex(encoded.data[10:]))
            assert (spender, amount) == (
                operation.target.market.contracts.token_router,
                operation.amount.amount,
            )
        else:
            assert encoded.to == operation.target.market.contracts.exchange_router
            transfers = decode(("bytes[]",), bytes.fromhex(encoded.data[10:]))[0]
            signature = CREATE_DEPOSIT if operation.request_kind == "deposit" else CREATE_WITHDRAWAL
            assert transfers[-1][:4] == keccak(text=signature)[:4]
            assert len(transfers) == len(operation.inputs) + 1


def test_actual_gm_request_keys_encode_exact_cancellation_handlers(completed):
    for request in completed.requests:
        operation = CancelLiquidityRequest(
            target=completed.config.target(request.allocation),
            request_kind=request.kind,
            request_key=request.key,
        )
        encoded = call(operation)
        prefix = keccak(text="cancel" + request.kind.title() + "(bytes32)")[:4]
        assert encoded.data == "0x" + prefix.hex() + request.key[2:]
        assert encoded.to == operation.target.market.contracts.exchange_router
