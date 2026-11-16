"""Hyperliquid L1 action signing (blueprint 30, sections 5.1, 5.4 and 9.8).

An agent signs an action as Hyperliquid's phantom agent: the keccak of the action's
msgpack bytes, its nonce and its `expiresAfter` is the `connectionId` of an EIP-712
`Agent` message in the `Exchange` domain with chain ID 1337, source `a` for mainnet.
The signature authorizes exactly that action at that nonce until that time.

The gateway signs only the actions of phases 3 and 4, each built from closed fields in
the key order the venue hashes: `order` with one limit order (`Ioc`, `Gtc` or `Alo`) or
one reduce-only limit trigger order, always with a cloid; `cancel` of one order by its
oid, never `cancelByCloid`, which HyperCore resolves to the newest order carrying the
cloid only when it executes (T5); `scheduleCancel`, set or cleared; `updateLeverage`,
cross or isolated; `updateIsolatedMargin`; and `noop`. It never signs with a vault
address: an agent's action applies to the account that approved it, the Safe, when
signed without one (mainnet canary). The official SDK's fixtures in
`tests/fixtures/hyperliquid-signing.json` fix every byte; the fork model's verifier is a
second implementation held to the same fixtures.
"""

import json
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

import msgpack
from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.gateway.hypercore_shapes import Cloid, Millis
from almanak.models import Model

# The phantom-agent domain is fixed by the venue, for every network.
DOMAIN_TYPE = b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
AGENT_TYPE = b"Agent(string source,bytes32 connectionId)"
DOMAIN_SEPARATOR = keccak(
    keccak(DOMAIN_TYPE)
    + keccak(b"Exchange")
    + keccak(b"1")
    + (1337).to_bytes(32, "big")
    + bytes(32)
)
# Testnet signs with source "b". Verification uses mainnet forks only, so only mainnet's
# source is ever signed.
MAINNET = "a"
# HyperCore accepts a nonce within two days before and one day after its block time.
NONCE_PAST_MS = 2 * 24 * 3600 * 1000
NONCE_FUTURE_MS = 24 * 3600 * 1000
# A wire decimal has at most eight fraction digits, as the official SDK writes it.
WIRE_PLACES = 8

Asset = Annotated[int, Field(strict=True, ge=0, lt=10_000)]
Leverage = Annotated[int, Field(strict=True, ge=1, le=100)]


def wire_decimal(value: Decimal) -> str:
    """A positive amount as the venue hashes it: plain digits, no trailing zeros."""
    if not value.is_finite() or value <= 0:
        raise ValueError("a wire amount is a positive finite decimal")
    exponent = value.normalize().as_tuple().exponent
    if isinstance(exponent, int) and -exponent > WIRE_PLACES:
        raise ValueError(f"a wire amount has at most {WIRE_PLACES} fraction digits")
    return format(value.normalize(), "f")


Tif = Literal["Ioc", "Gtc", "Alo"]
Tpsl = Literal["tp", "sl"]
# A margin change in micro-USDC, as the venue hashes it: positive adds, negative removes.
Ntli = Annotated[int, Field(strict=True, gt=-(2**63), lt=2**63)]


class LimitOrder(Model):
    """One limit order: an `Ioc` entry or reduce-only exit (phase 3), or a resting `Gtc`
    or post-only `Alo` order (phase 4)."""

    asset: Asset
    is_buy: bool
    price: Decimal
    size: Decimal
    reduce_only: bool
    tif: Tif = "Ioc"
    cloid: Cloid

    def wire(self) -> dict:
        return {
            "a": self.asset,
            "b": self.is_buy,
            "p": wire_decimal(self.price),
            "s": wire_decimal(self.size),
            "r": self.reduce_only,
            "t": {"limit": {"tif": self.tif}},
            "c": self.cloid,
        }


class TriggerOrder(Model):
    """A reduce-only take-profit or stop-loss limit order, placed by HyperCore when the
    mark reaches its trigger price. Only limit triggers are signed: a market trigger
    executes at a price HyperCore chooses, which the gateway cannot bound."""

    asset: Asset
    is_buy: bool
    price: Decimal
    size: Decimal
    trigger_price: Decimal
    tpsl: Tpsl
    cloid: Cloid

    @property
    def reduce_only(self) -> bool:
        return True

    def wire(self) -> dict:
        trigger = {"isMarket": False, "triggerPx": wire_decimal(self.trigger_price)}
        return {
            "a": self.asset,
            "b": self.is_buy,
            "p": wire_decimal(self.price),
            "s": wire_decimal(self.size),
            "r": True,
            "t": {"trigger": {**trigger, "tpsl": self.tpsl}},
            "c": self.cloid,
        }


class OrderAction(Model):
    type: Literal["order"] = "order"
    order: LimitOrder | TriggerOrder

    def wire(self) -> dict:
        return {"type": "order", "orders": [self.order.wire()], "grouping": "na"}


class Cancel(Model):
    """A cancel of one order by its oid."""

    type: Literal["cancel"] = "cancel"
    asset: Asset
    oid: Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]

    def wire(self) -> dict:
        return {"type": "cancel", "cancels": [{"a": self.asset, "o": self.oid}]}


class ScheduleCancel(Model):
    """The dead man's switch: cancel every open order of the account at `time`, or clear
    the scheduled cancel when `time` is absent."""

    type: Literal["scheduleCancel"] = "scheduleCancel"
    time: Millis | None = None

    def wire(self) -> dict:
        return {"type": "scheduleCancel"} | ({} if self.time is None else {"time": self.time})


class UpdateIsolatedMargin(Model):
    """Add (positive) or remove (negative) micro-USDC of an isolated position's margin.
    `isBuy` has no effect until hedge mode exists, so it is always true, as the official
    SDK writes it."""

    type: Literal["updateIsolatedMargin"] = "updateIsolatedMargin"
    asset: Asset
    ntli: Ntli

    @model_validator(mode="after")
    def nonzero(self):
        if self.ntli == 0:
            raise ValueError("a margin change moves a nonzero amount")
        return self

    def wire(self) -> dict:
        return {
            "type": "updateIsolatedMargin",
            "asset": self.asset,
            "isBuy": True,
            "ntli": self.ntli,
        }


class UpdateLeverage(Model):
    type: Literal["updateLeverage"] = "updateLeverage"
    asset: Asset
    is_cross: bool = True
    leverage: Leverage

    def wire(self) -> dict:
        return {
            "type": "updateLeverage",
            "asset": self.asset,
            "isCross": self.is_cross,
            "leverage": self.leverage,
        }


class Noop(Model):
    type: Literal["noop"] = "noop"

    def wire(self) -> dict:
        return {"type": "noop"}


Action = Annotated[
    OrderAction | Cancel | ScheduleCancel | UpdateLeverage | UpdateIsolatedMargin | Noop,
    Field(discriminator="type"),
]
ActionModel = OrderAction | Cancel | ScheduleCancel | UpdateLeverage | UpdateIsolatedMargin | Noop


def decimal_text(value) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("a wire amount is a decimal string")
    try:
        return Decimal(value)
    except InvalidOperation:
        raise ValueError("a wire amount is a decimal string") from None


def mapping(value, message: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(message)
    return value


def single(wire: dict, key: str) -> dict:
    """The one object of an action's list field: the gateway signs one order or cancel."""
    items = wire.get(key)
    if not isinstance(items, list) or len(items) != 1:
        raise ValueError(f"an action carries one {key} entry")
    return mapping(items[0], f"an action carries one {key} object")


def parsed_order(wire: dict) -> OrderAction:
    order = single(wire, "orders")
    kind = mapping(order.get("t"), "an order names its type")
    fields = {
        "asset": order.get("a"),
        "is_buy": order.get("b"),
        "price": decimal_text(order.get("p")),
        "size": decimal_text(order.get("s")),
        "cloid": order.get("c"),
    }
    if "trigger" in kind:
        trigger = mapping(kind["trigger"], "a trigger order names its trigger")
        price = decimal_text(trigger.get("triggerPx"))
        fields |= {"trigger_price": price, "tpsl": trigger.get("tpsl")}
        return OrderAction(order=TriggerOrder.model_validate(fields))
    limit = mapping(kind.get("limit"), "an order is a limit or trigger order")
    fields |= {"reduce_only": order.get("r"), "tif": limit.get("tif")}
    return OrderAction(order=LimitOrder.model_validate(fields))


def parsed_cancel(wire: dict) -> Cancel:
    cancel = single(wire, "cancels")
    return Cancel.model_validate({"asset": cancel.get("a"), "oid": cancel.get("o")})


def parsed_schedule(wire: dict) -> ScheduleCancel:
    return ScheduleCancel.model_validate({"time": wire.get("time")})


def parsed_leverage(wire: dict) -> UpdateLeverage:
    fields = {"asset": wire.get("asset"), "is_cross": wire.get("isCross")}
    return UpdateLeverage.model_validate(fields | {"leverage": wire.get("leverage")})


def parsed_margin(wire: dict) -> UpdateIsolatedMargin:
    return UpdateIsolatedMargin.model_validate(
        {"asset": wire.get("asset"), "ntli": wire.get("ntli")}
    )


PARSERS: dict[str, Callable[[dict], ActionModel]] = {
    "order": parsed_order,
    "cancel": parsed_cancel,
    "scheduleCancel": parsed_schedule,
    "updateLeverage": parsed_leverage,
    "updateIsolatedMargin": parsed_margin,
    "noop": lambda wire: Noop(),
}


def parsed(wire: dict) -> ActionModel:
    """The admitted action whose wire form is exactly `wire`, keys in order, or a refusal.

    A remote signer and the gateway each rebuild the bytes they hash from closed fields,
    so a wire dict that differs from its own rebuild in any key, value or order is not
    an admitted action.
    """
    parse = PARSERS.get(str(wire.get("type"))) if isinstance(wire, dict) else None
    if parse is None:
        raise ValueError("the action is not one the gateway signs")
    action = parse(wire)
    if json.dumps(action.wire()) != json.dumps(wire):
        raise ValueError("the action differs from its admitted wire form")
    return action


def action_hash(wire: dict, nonce: int, expires_after: int | None) -> bytes:
    """The phantom agent's `connectionId`: msgpack action, nonce, no vault, expiry."""
    data = msgpack.packb(wire) + nonce.to_bytes(8, "big") + b"\x00"
    if expires_after is not None:
        data += b"\x00" + expires_after.to_bytes(8, "big")
    return keccak(data)


def agent_digest(connection_id: bytes) -> bytes:
    """The EIP-712 digest of the mainnet phantom `Agent` message."""
    struct = keccak(keccak(AGENT_TYPE) + keccak(MAINNET.encode()) + connection_id)
    return keccak(b"\x19\x01" + DOMAIN_SEPARATOR + struct)


def digest(wire: dict, nonce: int, expires_after: int | None) -> bytes:
    return agent_digest(action_hash(wire, nonce, expires_after))


class Signature(Model):
    # The venue's own form: minimal hex integers and v of 27 or 28.
    r: Annotated[str, Field(pattern=r"^0x[0-9a-f]{1,64}$")]
    s: Annotated[str, Field(pattern=r"^0x[0-9a-f]{1,64}$")]
    v: Literal[27, 28]

    @property
    def vrs(self) -> tuple[int, int, int]:
        return self.v, int(self.r, 16), int(self.s, 16)


def sign(key: bytes | str, wire: dict, nonce: int, expires_after: int | None) -> Signature:
    # Imported here: a strategy's decision path decodes these models and never signs.
    from eth_account import Account

    signed = Account.unsafe_sign_hash(digest(wire, nonce, expires_after), key)
    return Signature(r=hex(signed.r), s=hex(signed.s), v=signed.v)


def recover(wire: dict, nonce: int, expires_after: int | None, signature: Signature) -> str:
    from eth_account import Account

    return Account._recover_hash(digest(wire, nonce, expires_after), vrs=signature.vrs).lower()


class ActionRequest(Model):
    """What a signer is asked to sign: the action, its nonce and its expiry."""

    action: Action
    nonce: Millis
    expires_after: Millis

    def wire(self) -> dict:
        return self.action.wire()

    def connection_id(self) -> bytes:
        return action_hash(self.wire(), self.nonce, self.expires_after)


class SignedAction(Model):
    """A signed `/exchange` request. Its body is recorded before it is ever posted, and a
    resend posts exactly these bytes."""

    request: ActionRequest
    agent: Annotated[str, Field(pattern=r"^0x[0-9a-f]{40}$")]
    signature: Signature

    def body(self) -> bytes:
        payload = {
            "action": self.request.wire(),
            "nonce": self.request.nonce,
            "signature": self.signature.model_dump(),
            "vaultAddress": None,
            "expiresAfter": self.request.expires_after,
        }
        return json.dumps(payload, separators=(",", ":")).encode()

    def action_hash(self) -> str:
        return "0x" + self.request.connection_id().hex()


def verified(request: ActionRequest, agent: str, signature: Signature) -> SignedAction:
    """The signed request, refused unless the signature recovers to the expected agent."""
    agent = agent.lower()
    if recover(request.wire(), request.nonce, request.expires_after, signature) != agent:
        raise ValueError("the signer returned a signature of another key or action")
    return SignedAction(request=request, agent=agent, signature=signature)


def require_nonce_window(nonce: int, observed: int) -> None:
    """Refuse to sign a nonce HyperCore would refuse: outside two days before and one day
    after its own time, which the latest venue observation places."""
    if not observed - NONCE_PAST_MS < nonce < observed + NONCE_FUTURE_MS:
        raise ValueError("the signing clock is outside HyperCore's nonce window")
