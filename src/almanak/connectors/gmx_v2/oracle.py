"""Signed stream decoding preserves source and time without asserting signature validity."""

from typing import Literal, Self

from eth_abi import decode, encode
from pydantic import model_validator

from almanak.caip import AssetType
from almanak.gateway.accounts import HexData
from almanak.models import Address, Hash32, Model, UInt

ENVELOPE_TYPES = ("bytes32[3]", "bytes", "bytes32[]", "bytes32[]", "bytes32")
REPORT_TYPES = (
    "bytes32",
    "uint32",
    "uint32",
    "uint192",
    "uint192",
    "uint32",
    "int192",
    "int192",
    "int192",
)
REPORT_FIELDS = (
    "feed_id",
    "valid_from",
    "observed_at",
    "native_verification_fee",
    "link_verification_fee",
    "expires_at",
    "price",
    "bid",
    "ask",
)


class StreamReport(Model):
    asset: AssetType
    provider: Address
    price_unit: Literal["feed_report_integer"] = "feed_report_integer"
    feed_id: Hash32
    valid_from: UInt
    observed_at: UInt
    expires_at: UInt
    native_verification_fee: UInt
    link_verification_fee: UInt
    price: int
    bid: int
    ask: int
    signed_payload: HexData

    @model_validator(mode="after")
    def validity(self) -> Self:
        if not self.valid_from <= self.observed_at <= self.expires_at:
            raise ValueError("signed stream report has inconsistent validity timestamps")
        if not 0 < self.bid <= self.ask or self.price <= 0:
            raise ValueError("signed stream report requires positive ordered price bounds")
        fields = report_fields(bytes.fromhex(self.signed_payload[2:]))
        if any(getattr(self, name) != value for name, value in fields.items()):
            raise ValueError("decoded stream report fields differ from the retained signed payload")
        return self


def report_fields(payload: bytes) -> dict:
    envelope = decode(ENVELOPE_TYPES, payload)
    if encode(ENVELOPE_TYPES, envelope) != payload:
        raise ValueError("signed stream payload must retain its canonical ABI envelope")
    _, report, rs, ss, _ = envelope
    if not rs or len(rs) != len(ss):
        raise ValueError("signed stream payload requires matching signature vectors")
    fields = decode(REPORT_TYPES, report)
    if encode(REPORT_TYPES, fields) != report:
        raise ValueError("stream report has a different schema or trailing data")
    return dict(zip(REPORT_FIELDS, ("0x" + fields[0].hex(), *fields[1:]), strict=True))


def decode_report(asset: AssetType, provider: str, payload: bytes) -> StreamReport:
    return StreamReport(
        asset=asset,
        provider=provider,
        signed_payload="0x" + payload.hex(),
        **report_fields(payload),
    )
