"""Coherent, implementation-pinned scaled-token reference observations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from typing import Any

from web3 import AsyncWeb3
from web3.types import RPCEndpoint

from almanak.integrations.bstocks.catalog import MAX_CLOCK_SKEW_SECONDS, MAX_CONTRACT_AGE_SECONDS, TokenReferenceProfile

BEACON_SLOT = int.from_bytes(AsyncWeb3.keccak(text="eip1967.proxy.beacon"), "big") - 1
SCALE = Decimal(10**18)


def _scaled(value: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 160
        return Decimal(value) / SCALE


@dataclass(frozen=True)
class MultiplierObservation:
    multiplier: Decimal
    block_number: int
    block_hash: str
    block_timestamp: int
    read_at: int
    scheduled_multiplier: Decimal | None
    effective_at: int
    beacon: str
    implementation: str


def _word(raw: bytes) -> int:
    if len(raw) != 32:
        raise ValueError("multiplier_malformed_contract_word")
    return int.from_bytes(raw, "big")


def _address(raw: bytes) -> str:
    if len(raw) != 32 or any(raw[:12]):
        raise ValueError("multiplier_malformed_contract_address")
    return "0x" + raw[-20:].hex()


def contract_age_reason(observation: MultiplierObservation, now: int) -> str | None:
    for stamp in (observation.block_timestamp, observation.read_at):
        if stamp <= 0 or stamp > now + MAX_CLOCK_SKEW_SECONDS:
            return "multiplier_timestamp_invalid"
        if now - stamp > MAX_CONTRACT_AGE_SECONDS:
            return "multiplier_observation_stale"
    return None


async def read_multiplier(w3: AsyncWeb3, profile: TokenReferenceProfile) -> MultiplierObservation:
    """Read all state at one block and reject a reorg before returning it."""
    async with asyncio.timeout(10):
        if await w3.eth.chain_id != profile.chain_id:
            raise ValueError("multiplier_chain_mismatch")
        block = await w3.eth.get_block("latest")
        number, timestamp = int(block["number"]), int(block["timestamp"])
        block_hash = "0x" + bytes(block["hash"]).hex()
        block_ref: Any = {"blockHash": block_hash, "requireCanonical": True}
        token = w3.to_checksum_address(profile.address)
        beacon = w3.to_checksum_address(profile.beacon)

        async def rpc_word(method: str, params: list[Any]) -> bytes:
            # Web3's ENS formatter does not support EIP-1898 block objects.
            # Use the same owned transport with explicit addresses and words.
            response = await w3.provider.make_request(RPCEndpoint(method), params)
            if response.get("error") is not None:
                raise ValueError("multiplier_rpc_error")
            raw = response.get("result")
            if not isinstance(raw, str) or not raw.startswith("0x") or len(raw) != 66:
                raise ValueError("multiplier_malformed_contract_word")
            try:
                return bytes.fromhex(raw[2:])
            except ValueError as exc:
                raise ValueError("multiplier_malformed_contract_word") from exc

        async def call(address: str, signature: str) -> bytes:
            selector = "0x" + AsyncWeb3.keccak(text=signature)[:4].hex()
            return await rpc_word("eth_call", [{"to": address, "data": selector}, block_ref])

        raw_beacon, raw_impl, raw_multiplier, raw_next, raw_effective, raw_decimals = await asyncio.gather(
            rpc_word("eth_getStorageAt", [token, hex(BEACON_SLOT), block_ref]),
            call(beacon, "implementation()"),
            call(token, "uiMultiplier()"),
            call(token, "newUIMultiplier()"),
            call(token, "effectiveAt()"),
            call(token, "decimals()"),
        )
        if _address(bytes(raw_beacon)) != profile.beacon or _address(raw_impl) != profile.implementation:
            raise ValueError("multiplier_implementation_mismatch")
        if _word(raw_decimals) != profile.decimals:
            raise ValueError("multiplier_decimals_mismatch")
        multiplier, next_multiplier, effective = _word(raw_multiplier), _word(raw_next), _word(raw_effective)
        if multiplier <= 0 or next_multiplier <= 0:
            raise ValueError("multiplier_non_positive")
        if effective == 0 and next_multiplier != multiplier:
            raise ValueError("multiplier_schedule_incoherent")
        if effective <= timestamp and next_multiplier != multiplier:
            raise ValueError("multiplier_activation_incoherent")
        final_block = await w3.eth.get_block(number)
        if bytes(final_block["hash"]) != bytes(block["hash"]):
            raise ValueError("multiplier_block_reorganized")
        observed = MultiplierObservation(
            multiplier=_scaled(multiplier),
            block_number=number,
            block_hash=block_hash,
            block_timestamp=timestamp,
            read_at=int(datetime.now(UTC).timestamp()),
            scheduled_multiplier=_scaled(next_multiplier) if effective > timestamp else None,
            effective_at=effective,
            beacon=profile.beacon,
            implementation=profile.implementation,
        )
        if reason := contract_age_reason(observed, observed.read_at):
            raise ValueError(reason)
        return observed


class AdjustmentCoherence:
    """Do not combine a pre-observation stock quote with an unobserved adjustment."""

    def __init__(self) -> None:
        self._seen: dict[tuple[str, str], tuple[MultiplierObservation, int]] = {}

    def observe(self, profile: TokenReferenceProfile, state: MultiplierObservation, quote_timestamp: int) -> str | None:
        key = (profile.chain, profile.address)
        previous = self._seen.get(key)
        verified_since = state.block_timestamp
        if previous is not None:
            prior, verified_since = previous
            if state.block_number < prior.block_number or state.block_timestamp < prior.block_timestamp:
                return "multiplier_observation_out_of_order"
            if state.block_number == prior.block_number and (
                state.block_hash != prior.block_hash
                or state.multiplier != prior.multiplier
                or state.block_timestamp != prior.block_timestamp
                or state.scheduled_multiplier != prior.scheduled_multiplier
                or state.effective_at != prior.effective_at
            ):
                return "multiplier_observation_conflict"
            if prior.multiplier != state.multiplier:
                verified_since = state.block_timestamp
        self._seen[key] = (state, verified_since)
        if state.scheduled_multiplier is not None:
            return "multiplier_adjustment_pending"
        if quote_timestamp <= max(verified_since, state.effective_at):
            return "reference_adjustment_alignment_unproven"
        return None


def compose_reference(
    underlying: Any,
    profile: TokenReferenceProfile,
    state: MultiplierObservation,
    coherence: AdjustmentCoherence,
) -> Any:
    """Preserve source freshness; the contract observation supplies no stock ticks."""
    from almanak.gateway.proto import gateway_pb2

    result = gateway_pb2.ReferencePriceResponse()
    result.CopyFrom(underlying)
    result.instrument = profile.symbol
    result.token_address = profile.address
    result.basis = gateway_pb2.REFERENCE_PRICE_BASIS_RAW_TOKEN
    now = int(datetime.now(UTC).timestamp())
    result.composition.CopyFrom(
        gateway_pb2.ReferencePriceComposition(
            underlying_instrument=profile.underlying,
            underlying_price=underlying.price,
            underlying_source=underlying.source,
            underlying_observed_at=underlying.observed_at,
            multiplier=str(state.multiplier),
            multiplier_block_number=state.block_number,
            multiplier_block_hash=state.block_hash,
            multiplier_block_timestamp=state.block_timestamp,
            multiplier_read_at=state.read_at,
            scheduled_multiplier=str(state.scheduled_multiplier) if state.scheduled_multiplier is not None else "",
            multiplier_effective_at=state.effective_at,
            beacon_address=state.beacon,
            implementation_address=state.implementation,
            composed_at=now,
        )
    )
    reason = contract_age_reason(state, now) or coherence.observe(profile, state, underlying.observed_at)
    if reason:
        result.price = ""
        result.availability = gateway_pb2.REFERENCE_PRICE_AVAILABILITY_UNMEASURED
        result.reason = reason
        result.stale = True
        return result
    with localcontext() as ctx:
        ctx.prec = 160
        result.price = str(Decimal(underlying.price) * state.multiplier)
    result.source = f"composition:scaled-token:{profile.address}:{underlying.source}"
    return result
