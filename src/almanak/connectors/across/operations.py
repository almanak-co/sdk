"""The approval, the deposit and the revocation of one bridge, and their calldata."""

from typing import Annotated, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.across.domain import Target, Uint32
from almanak.connectors.across.quote import Quote
from almanak.gateway.accounts import ContractCall
from almanak.gateway.evm import chain_number
from almanak.models import AssetAmount, Model

DEPOSIT = (
    "deposit(bytes32,bytes32,bytes32,bytes32,uint256,uint256,uint256,"
    "bytes32,uint32,uint32,uint32,bytes)"
)
DEPOSIT_TYPES = (
    "bytes32",
    "bytes32",
    "bytes32",
    "bytes32",
    "uint256",
    "uint256",
    "uint256",
    "bytes32",
    "uint32",
    "uint32",
    "uint32",
    "bytes",
)
APPROVE_TYPES = ("address", "uint256")


class Operation(Model):
    target: Target

    @property
    def account(self):
        return self.target.account

    @property
    def allocation(self):
        return self.target.allocation


class FundedOperation(Operation):
    amount: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        if self.amount.asset != self.target.asset or not 0 < self.amount.amount < 2**256:
            raise ValueError("an Across step requires a positive amount of the sent asset")
        return self


class ApproveOperation(FundedOperation):
    kind: Literal["approve"] = "approve"


class DepositOperation(FundedOperation):
    kind: Literal["deposit"] = "deposit"
    # The venue's quote this deposit signs. Its terms are the deposit's own calldata.
    quote: Quote
    # The SpokePool reverts a deposit mined later than its quote timestamp plus this
    # buffer, read from the SpokePool at the planning block: the calldata's own deadline.
    quote_time_buffer: Uint32

    @model_validator(mode="after")
    def quoted(self):
        if self.quote.input_amount != self.amount.amount:
            raise ValueError("an Across deposit signs the quote of its own amount")
        return self

    @property
    def deadline(self) -> int:
        """Unix seconds of the last block timestamp the SpokePool mines this deposit at."""
        return self.quote.timestamp + self.quote_time_buffer


class RevokeOperation(Operation):
    kind: Literal["revoke"] = "revoke"
    # Whether the revocation ends its decision. One that clears an approval an earlier
    # decision left standing does not: the approval and deposit of this decision follow.
    final: bool


AcrossOperation = Annotated[
    ApproveOperation | DepositOperation | RevokeOperation, Field(discriminator="kind")
]


def word(address: str) -> bytes:
    """An EVM address as the left-padded bytes32 the SpokePool takes."""
    return bytes(12) + bytes.fromhex(address[2:])


def deposit_arguments(operation: DepositOperation) -> tuple:
    target, quote = operation.target, operation.quote
    route = target.route
    return (
        word(target.fund.address),
        word(target.destination_fund.address),
        word(route.asset.reference),
        word(route.destination_asset.reference),
        operation.amount.amount,
        quote.output_amount,
        chain_number(route.destination_chain),
        word(quote.exclusive_relayer),
        quote.timestamp,
        quote.fill_deadline,
        quote.exclusivity_parameter,
        b"",
    )


def encoded(signature: str, types, values) -> str:
    return "0x" + (keccak(text=signature)[:4] + encode(types, values)).hex()


def plan_call(operation: AcrossOperation) -> ContractCall:
    target = operation.target
    if isinstance(operation, DepositOperation):
        to = target.route.spoke_pool
        data = encoded(DEPOSIT, DEPOSIT_TYPES, deposit_arguments(operation))
    else:
        amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        to = target.asset.reference
        data = encoded("approve(address,uint256)", APPROVE_TYPES, (target.route.spoke_pool, amount))
    # The sent asset is always an ERC-20: a deposit never carries native value.
    return ContractCall(fund=target.fund, to=to, value=0, data=data)
