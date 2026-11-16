"""GM deposit and withdrawal calls escrow exact tokens before creating one request."""

from almanak.connectors.gmx_v2.candidate_permissions import calldata
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit
from almanak.models import ZERO_ADDRESS

DEPOSIT_PARAMS = (
    "((address,address,address,address,address,address,address[],address[]),"
    "uint256,bool,uint256,uint256,bytes32[])"
)
WITHDRAWAL_PARAMS = (
    "((address,address,address,address,address[],address[]),"
    "uint256,uint256,bool,uint256,uint256,bytes32[])"
)
CREATE_DEPOSIT = "createDeposit(" + DEPOSIT_PARAMS + ")"
CREATE_WITHDRAWAL = "createWithdrawal(" + WITHDRAWAL_PARAMS + ")"
# Roles requires both OR branches and all bytes-array entries to share a type tree.
# Empty-route deposits occupy 18 ABI words; withdrawals occupy 17 before padding.
REQUEST_BYTES = 4 + 18 * 32


def request_data(operation):
    target = operation.target
    common = (target.fund.address, ZERO_ADDRESS, ZERO_ADDRESS, target.market.share_asset.reference)
    if isinstance(operation, CreateDeposit):
        addresses = (
            *common,
            target.market.long_asset.reference,
            target.market.short_asset.reference,
            (),
            (),
        )
        params = (
            addresses,
            operation.minimum_market_tokens.amount,
            False,
            operation.execution_fee.amount,
            0,
            (),
        )
        return calldata(CREATE_DEPOSIT, (DEPOSIT_PARAMS,), (params,))
    params = (
        (*common, (), ()),
        *(value.amount for value in operation.minimum_outputs.root),
        False,
        operation.execution_fee.amount,
        0,
        (),
    )
    return calldata(CREATE_WITHDRAWAL, (WITHDRAWAL_PARAMS,), (params,))


def vault(target, kind):
    contracts = target.market.contracts
    return {"deposit": contracts.deposit_vault, "withdrawal": contracts.withdrawal_vault}[kind]


def create_data(operation):
    request = request_data(operation).ljust(REQUEST_BYTES, b"\0")
    transfers = tuple(
        calldata(
            "sendTokens(address,address,uint256)",
            ("address", "address", "uint256"),
            (value.asset.reference, vault(operation.target, operation.request_kind), value.amount),
        ).ljust(len(request), b"\0")
        for value in operation.inputs
    )
    return calldata("multicall(bytes[])", ("bytes[]",), ((*transfers, request),))


def cancel_data(operation):
    signature = {"deposit": "cancelDeposit(bytes32)", "withdrawal": "cancelWithdrawal(bytes32)"}[
        operation.request_kind
    ]
    return calldata(signature, ("bytes32",), (bytes.fromhex(operation.request_key[2:]),))
