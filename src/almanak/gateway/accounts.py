"""Unsigned account routing. Call preparation does not establish authorization."""

from typing import Annotated

from eth_abi import decode, encode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak
from pydantic import Field

from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.reverts import MAX_REVERT_BYTES
from almanak.models import (
    Account,
    AccountBinding,
    Address,
    Eoa,
    Model,
    SafeRoles,
    StrategyAccounts,
    UInt,
)

HexData = Annotated[str, Field(pattern=r"^0x([0-9a-f]{2})*$")]
Wei = Annotated[UInt, Field(lt=2**256)]


class ContractCall(Model):
    fund: Account
    to: Address
    value: Wei
    data: HexData


class PreparedCall(Model):
    binding: AccountBinding
    call: ContractCall
    to: Address
    value: Wei
    data: HexData

    @property
    def submitter(self) -> Account:
        return self.binding.profile.submitter


def prepare_call(accounts: StrategyAccounts, account_name: str, call: ContractCall) -> PreparedCall:
    chain_number(call.fund.chain.chain_id)
    binding = accounts.binding(account_name)
    profile = binding.profile
    if call.fund != profile.fund:
        raise ValueError("call fund does not match the selected account/chain/environment")
    if isinstance(profile, Eoa):
        to, value, data = call.to, call.value, call.data
    elif isinstance(profile, SafeRoles):
        selector = keccak(text="execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)")[
            :4
        ]
        # Native value is paid by the Safe. CALL and shouldRevert preserve inner failure.
        payload = encode(
            ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
            (
                call.to,
                call.value,
                bytes.fromhex(call.data[2:]),
                0,
                bytes.fromhex(profile.role_key[2:]),
                True,
            ),
        )
        to, value, data = profile.roles_module, 0, "0x" + (selector + payload).hex()
    else:
        raise ValueError("simulation accounts cannot prepare executable transactions")
    return PreparedCall(binding=binding, call=call, to=to, value=value, data=data)


def protocol_simulation(execution: PreparedCall) -> dict:
    data = execution.data
    if isinstance(execution.binding.profile, SafeRoles):
        # Roles 2.1.0 applies the same authorization and allowance consumption
        # through this entry point, while exposing the inner protocol return data.
        selector = keccak(
            text="execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)"
        )[:4]
        data = "0x" + selector.hex() + data[10:]
    return {
        "from": execution.submitter.address,
        "to": execution.to,
        "value": hex(execution.value),
        "data": data,
        "gasPrice": "0x0",
    }


def revert_diagnosis(execution: PreparedCall) -> dict | None:
    """The protocol simulation with shouldRevert cleared, which returns the inner revert data.

    With shouldRevert set, Roles replaces a failed inner call's reason with
    ModuleTransactionFailed(). This read only explains a simulation that already reverted;
    it never authorizes anything.
    """
    if not isinstance(execution.binding.profile, SafeRoles):
        return None
    tx = protocol_simulation(execution)
    # shouldRevert is the sixth static head word after the 4-byte selector.
    head = 10 + 5 * 64
    tx["data"] = tx["data"][:head] + "0" * 64 + tx["data"][head + 64 :]
    return tx


def inner_revert(result: object) -> bytes | None:
    """The inner call's revert data from a non-reverting Roles simulation, if it failed."""
    try:
        raw = bytes.fromhex(result[2:]) if isinstance(result, str) else b""
        success, returned = decode(("bool", "bytes"), raw)
    except (ValueError, DecodingError):
        return None
    return None if success or len(returned) > MAX_REVERT_BYTES else returned


def protocol_result(execution: PreparedCall, result: str) -> str:
    if not isinstance(execution.binding.profile, SafeRoles):
        return result
    try:
        raw = bytes.fromhex(result[2:])
        success, returned = decode(("bool", "bytes"), raw)
    except (ValueError, DecodingError):
        raise DataError("simulation", "Safe/Roles returned malformed simulation data") from None
    if not success or encode(("bool", "bytes"), (success, returned)) != raw:
        raise DataError("simulation", "Safe/Roles did not return a successful protocol result")
    return "0x" + returned.hex()


def require_plain_eoa(rpc, address):
    """Delegated code can execute when a protocol sends native funds to the account."""
    if rpc("eth_getCode", [address, "latest"]) != "0x":
        raise DataError(
            "authority", "EOA fund must have empty code; delegated accounts are unsupported"
        )
