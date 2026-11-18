"""CoreWriter actions and the HyperEVM calls of the custody path (blueprint 30, section 4).

A Safe reaches HyperCore through `CoreWriter.sendRawAction(bytes)`, whose argument is a
version byte, a three-byte action ID and the ABI-encoded fields. HyperCore applies the
action after the EVM block or drops it with no trace (T7), so a call here only queues
work; custody requests (`custody_requests.py`) establish its effect.

Units differ by path, as the mainnet canary measured: a CoreDepositWallet deposit and an
action 7 `ntl` are USDC base units (6 decimals); an action 13 `wei` is HyperCore units,
8 decimals for USDC (`weiDecimals`); ledger entries are decimal strings.
"""

from decimal import Decimal

from eth_abi import decode, encode

from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.gateway.safe_fixture import calldata
from almanak.models import Address, Model, address

CORE_WRITER = "0x3333333333333333333333333333333333333333"
USDC_CONTRACT = "0xb88339cb7199b77e23db6e890353e22632ba630f"
# HyperCore credits a HyperEVM transfer to a token's system address (0x20 followed by
# the token index) to its sender; USDC is token 0, crediting through CoreDepositWallet.
USDC_SYSTEM = "0x2000000000000000000000000000000000000000"
HYPE_SYSTEM = "0x2222222222222222222222222222222222222222"
USDC_TOKEN = 0
# HYPE's spot token index, which pays each withdrawal's HyperEVM system transaction.
HYPE_TOKEN = 150
SPOT_DEX = 2**32 - 1
PERP_DEX = 0
VERSION = 1
SEND_RAW_ACTION = "sendRawAction(bytes)"
RAW_ACTION_EVENT = "RawAction(address,bytes)"

USD_CLASS_TRANSFER = 7
ADD_API_WALLET = 9
SEND_ASSET = 13
SET_ABSTRACTION = 16
LAYOUTS = {
    USD_CLASS_TRANSFER: ("uint64", "bool"),
    ADD_API_WALLET: ("address", "string"),
    SEND_ASSET: ("address", "address", "uint32", "uint32", "uint64", "uint64"),
    SET_ABSTRACTION: ("address", "uint8"),
}
# Action 16's standard mode: separate spot and perp balances. `userAbstraction` reads it
# back as `disabled`.
DISABLED = 1

USDC_DECIMALS = 6
CORE_USDC_DECIMALS = 8
CORE_PER_EVM_USDC = 10 ** (CORE_USDC_DECIMALS - USDC_DECIMALS)


def header(action: int, version: int = VERSION) -> bytes:
    return bytes([version]) + action.to_bytes(3, "big")


def payload(action: int, values) -> bytes:
    return header(action) + encode(LAYOUTS[action], values)


def decoded(raw: bytes) -> tuple[int, tuple] | None:
    """The action ID and fields of a version-1 payload this module encodes, else None."""
    if len(raw) < 4 or raw[0] != VERSION:
        return None
    action = int.from_bytes(raw[1:4], "big")
    if action not in LAYOUTS:
        return None
    try:
        return action, decode(LAYOUTS[action], raw[4:])
    except Exception:
        return None


class CoreCall(Model):
    """One HyperEVM call the operator makes as the Safe, with what it asks of HyperCore."""

    to: Address
    data: str
    note: str


def raw_action(action: int, values, note: str) -> CoreCall:
    data = calldata(SEND_RAW_ACTION, ("bytes",), (payload(action, values),))
    return CoreCall(to=CORE_WRITER, data=data, note=note)


def deposit_calls(amount: int) -> tuple[CoreCall, ...]:
    """Approve, deposit to spot and revoke: the Safe credits only its own HyperCore account.

    Only spot deposits are planned: the canary recorded their ledger entries, which a
    perp deposit's attribution would need first.
    """
    if amount <= 0:
        raise ValueError("a deposit needs a positive USDC amount")
    return approval(amount), deposit(amount), approval(0)


def approval(amount: int) -> CoreCall:
    """The USDC allowance of CoreDepositWallet; zero revokes the residual of a deposit."""
    if amount < 0:
        raise ValueError("an approval is a nonnegative USDC amount")
    note = (
        "approve CoreDepositWallet for the deposit"
        if amount
        else "revoke the residual CoreDepositWallet allowance"
    )
    data = calldata(
        "approve(address,uint256)", ("address", "uint256"), (CORE_DEPOSIT_WALLET, amount)
    )
    return CoreCall(to=USDC_CONTRACT, data=data, note=note)


def deposit(amount: int) -> CoreCall:
    """CoreDepositWallet `deposit` to spot: it pulls from its caller and credits the caller."""
    if amount <= 0:
        raise ValueError("a deposit needs a positive USDC amount")
    data = calldata("deposit(uint256,uint32)", ("uint256", "uint32"), (amount, SPOT_DEX))
    return CoreCall(
        to=CORE_DEPOSIT_WALLET, data=data, note="deposit USDC to the Safe's HyperCore account"
    )


def class_transfer(usdc: int, to_perp: bool) -> CoreCall:
    if usdc <= 0:
        raise ValueError("a class transfer needs a positive USDC amount")
    side = "spot to perp" if to_perp else "perp to spot"
    return raw_action(USD_CLASS_TRANSFER, (usdc, to_perp), f"action 7: {side}")


def withdrawal(usdc: int) -> CoreCall:
    """Action 13 of USDC to its system address, which HyperCore pays to the Safe on HyperEVM."""
    if usdc <= 0:
        raise ValueError("a withdrawal needs a positive USDC amount")
    values = (USDC_SYSTEM, "0x" + "00" * 20, SPOT_DEX, SPOT_DEX, USDC_TOKEN, core_units(usdc))
    return raw_action(SEND_ASSET, values, "action 13: spot USDC to the Safe on HyperEVM")


def standard_mode(safe: str) -> CoreCall:
    return raw_action(SET_ABSTRACTION, (address(safe), DISABLED), "action 16: standard mode")


def agent_approval(agent: str, label: str) -> CoreCall:
    return raw_action(ADD_API_WALLET, (address(agent), label), "action 9: approve agent")


def core_units(usdc: int) -> int:
    return usdc * CORE_PER_EVM_USDC


def usdc_text(usdc: int) -> Decimal:
    """A USDC base-unit amount as the decimal a ledger entry writes."""
    return Decimal(usdc).scaleb(-USDC_DECIMALS)
