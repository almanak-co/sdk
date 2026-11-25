"""The approval, the deposit and the revocation of one bridge, and their calldata."""

from typing import Annotated, Literal

from eth_abi import encode
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.across.domain import Target, Uint32, Uint256
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
    # Wei the deposit's transaction carries. A native route's deposit carries exactly its
    # amount, which the SpokePool wraps; any other deposit carries none. Stated on the
    # operation, never derived at signing: with the wrapped token as input and no value
    # the SpokePool would pull that token by allowance instead, another transaction than
    # the one admitted.
    value: Uint256 = Field(default=0, exclude_if=lambda v: not v)

    @model_validator(mode="after")
    def quoted(self):
        if self.quote.input_amount != self.amount.amount:
            raise ValueError("an Across deposit signs the quote of its own amount")
        return self

    @model_validator(mode="after")
    def valued(self):
        if self.value != native_value(self.target, self.amount.amount):
            raise ValueError(
                "a native Across deposit carries exactly its amount as value, and a "
                "deposit of a token carries none"
            )
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


def native_value(target: Target, amount: int) -> int:
    """Wei a deposit of `amount` along the target's route must carry."""
    return amount if target.route.native else 0


def word(address: str) -> bytes:
    """An EVM address as the left-padded bytes32 the SpokePool takes."""
    return bytes(12) + bytes.fromhex(address[2:])


def deposit_arguments(operation: DepositOperation) -> tuple:
    target, quote = operation.target, operation.quote
    route = target.route
    return (
        word(target.fund.address),
        word(target.destination_fund.address),
        word(route.input_token),
        word(route.output_token),
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
        data = encoded(DEPOSIT, DEPOSIT_TYPES, deposit_arguments(operation))
        # Only a native route's deposit carries value: exactly its amount, held above.
        return ContractCall(
            fund=target.fund, to=target.route.spoke_pool, value=operation.value, data=data
        )
    if target.route.native:
        raise ValueError("a native Across route gives the SpokePool no approval")
    amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
    data = encoded("approve(address,uint256)", APPROVE_TYPES, (target.route.spoke_pool, amount))
    return ContractCall(fund=target.fund, to=target.asset.reference, value=0, data=data)
