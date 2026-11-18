"""Generated Roles grants of the Safe's HyperCore custody path (blueprint 30, section 4.3).

The operator may approve CoreDepositWallet, deposit the Safe's own USDC and send four
CoreWriter actions as the Safe: 7 (USD class transfer, header only, since it moves
USDC within the Safe's own account), 13 (USDC to its system address, which credits the
Safe on HyperEVM), 16 (standard mode for the Safe) and 9 (a fresh agent under one of
the deployment's two names, D9). No grant covers `depositFor`, so the operator credits
no other HyperCore account, nor any other CoreWriter action.

Roles 2.1 accepts an `Or` across actions only when every branch has the first branch's
type tree or is a plain `Dynamic` beside a decoded first branch. Action 13 is the one
decoded branch; the others pin payload bytes with `Bitmask` conditions over the
`bytes` argument, at the offsets of the standard ABI encoding.
"""

from eth_abi import encode

from almanak.connectors.hyperliquid.agents import EXPIRY_DIGITS, AgentNames
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.corewriter import (
    ADD_API_WALLET,
    CORE_WRITER,
    PERP_DEX,
    SEND_ASSET,
    SEND_RAW_ACTION,
    SET_ABSTRACTION,
    SPOT_DEX,
    USD_CLASS_TRANSFER,
    USDC_CONTRACT,
    USDC_SYSTEM,
    USDC_TOKEN,
    header,
    payload,
)
from almanak.gateway.roles_conditions import (
    Condition,
    FunctionGrant,
    any_amount,
    either,
    equal,
    every,
    matches,
    nested_header,
    pinned,
    positive,
)
from almanak.models import ZERO_ADDRESS, address

# Action 9's payload: the header, the agent word (12 zero bytes, then the address), the
# name's offset word, its length word, then the name. The offset and length words are
# pinned in full, so the name HyperCore decodes is the one the grant inspected, and the
# label is one of the two names, ` valid_until ` and 13 digits.
AGENT_WORD = 4
OFFSET_WORD = AGENT_WORD + 32
LENGTH_WORD = OFFSET_WORD + 32
NAME = LENGTH_WORD + 32


def custody_grants(safe: str, names: AgentNames) -> tuple[FunctionGrant, ...]:
    safe = address(safe)
    return (
        FunctionGrant(
            target=USDC_CONTRACT,
            signature="approve(address,uint256)",
            condition=matches(equal("address", CORE_DEPOSIT_WALLET), any_amount()),
            explanation=("hypercore: approve only CoreDepositWallet, any amount, or revoke.",),
        ),
        FunctionGrant(
            target=CORE_DEPOSIT_WALLET,
            signature="deposit(uint256,uint32)",
            condition=matches(
                positive(), either(equal("uint32", PERP_DEX), equal("uint32", SPOT_DEX))
            ),
            explanation=(
                "hypercore: deposit pulls from and credits its caller, so the Safe credits "
                "only its own HyperCore account, spot or the first perp dex.",
            ),
        ),
        FunctionGrant(
            target=CORE_WRITER,
            signature=SEND_RAW_ACTION,
            condition=matches(
                either(
                    withdrawal_branch(),
                    class_transfer_branch(),
                    standard_mode_branch(safe),
                    agent_branch(names),
                )
            ),
            explanation=(
                "hypercore: action 13 sends only spot USDC to its system address, which "
                "pays the Safe on HyperEVM.",
                "hypercore: action 7 moves USDC between the Safe's own spot and perp balances.",
                "hypercore: action 16 sets only the Safe's own standard mode.",
                "hypercore: action 9 approves any address, only under the deployment's two "
                "agent names with an expiry; the empty-name main agent is refused (D9).",
            ),
        ),
    )


def withdrawal_branch() -> Condition:
    return nested_header(
        header(SEND_ASSET),
        equal("address", USDC_SYSTEM),
        equal("address", ZERO_ADDRESS),
        equal("uint32", SPOT_DEX),
        equal("uint32", SPOT_DEX),
        equal("uint64", USDC_TOKEN),
        positive(),
    )


def class_transfer_branch() -> Condition:
    return every(*pinned(0, header(USD_CLASS_TRANSFER)))


def standard_mode_branch(safe: str) -> Condition:
    # The whole 68-byte payload: header, the Safe as `user` and abstraction 1.
    return every(*pinned(0, payload(SET_ABSTRACTION, (safe, 1))))


def agent_branch(names: AgentNames) -> Condition:
    words = encode(("uint256", "uint256"), (32 * 2, names.label_length))
    return every(
        *pinned(0, header(ADD_API_WALLET) + bytes(12)),
        *pinned(OFFSET_WORD, words),
        either(*(every(*pinned(NAME, names.prefix(name))) for name in names.both)),
        *expiry_digits(NAME + len(names.prefix(names.first))),
    )


def expiry_digits(offset: int) -> tuple[Condition, ...]:
    """Each of the label's 13 expiry bytes an ASCII digit, so HyperCore parses the
    `valid_until` suffix: a label whose suffix does not parse would be a third name with
    no expiry. A bitmask tests equality under a mask, so a digit (0x30 to 0x39) is a
    high nibble of 3, then bit 3 clear (0 to 7) or bits 3 to 1 equal to 100 (8 or 9)."""
    nibbles = pinned(offset, b"\x30" * EXPIRY_DIGITS, b"\xf0" * EXPIRY_DIGITS)
    low = tuple(
        either(
            *pinned(offset + index, b"\x00", b"\x08"),
            *pinned(offset + index, b"\x08", b"\x0e"),
        )
        for index in range(EXPIRY_DIGITS)
    )
    return (*nibbles, *low)
