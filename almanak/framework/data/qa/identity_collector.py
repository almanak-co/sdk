"""Gateway-backed collector for production-derived Data identity requirements.

The collector never owns declarations and never calls a chain RPC directly. It
pins one block through the SDK gateway, captures the exact JSON-RPC witnesses,
and writes content-addressed artifacts that the independent sealer can replay.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from web3 import Web3

from almanak.framework.data.qa.production_identity import (
    DirectChainlinkFeedObservation,
    DirectChainlinkFeedRequirement,
    Observation,
    ObservationProvenance,
    Requirement,
    TokenObservation,
    TokenRequirement,
    V3PoolObservation,
    derive_production_requirements,
    requirements_digest,
)
from almanak.gateway.proto import gateway_pb2

DECIMALS_SELECTOR = "0x313ce567"
DESCRIPTION_SELECTOR = "0x7284e416"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"


class GatewayIdentityClient(Protocol):
    """Smallest gateway surface required by the collector."""

    @property
    def rpc(self) -> Any: ...

    def block_number(self, chain: str, *, timeout: float | None = None) -> int | None: ...

    def eth_call(
        self,
        chain: str,
        to: str,
        data: str,
        block: int | str | None = None,
        *,
        raise_on_error: bool = False,
    ) -> str | None: ...


def _canonical_bytes(payload: object) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_canonical_bytes(payload))
    os.replace(temporary, path)


def _decode_uint(raw: str, label: str) -> int:
    if not isinstance(raw, str) or not raw.startswith("0x"):
        raise ValueError(f"{label} returned a non-hex result")
    value = int(raw, 16)
    if value < 0:
        raise ValueError(f"{label} returned a negative value")
    return value


def _decode_address(raw: str, label: str) -> str:
    if not isinstance(raw, str) or not raw.startswith("0x") or len(raw) < 42:
        raise ValueError(f"{label} returned an invalid ABI address")
    return "0x" + raw[-40:].lower()


#: web3's ABI codec, reached without a provider. CLAUDE.md permits web3 for
#: "ABI / checksum / encoding utilities" and the data-layer import allowlist
#: already carries `web3` on exactly that basis; `eth_abi` is not on it, and
#: widening a deliberately-ratcheted boundary allowlist to reach the same
#: codec web3 already exposes would be the wrong trade. Constructing `Web3()`
#: with no provider opens no socket — the forbidden shape is instantiating a
#: URL-pointed transport provider, not a bare instance. (Naming that provider
#: class here in prose would itself trip the boundary scan, which is
#: substring-based rather than AST-aware.)
_ABI_CODEC = Web3().codec


def _decode_string(raw: str, label: str) -> str:
    try:
        value = _ABI_CODEC.decode(["string"], bytes.fromhex(raw.removeprefix("0x")))[0]
    except Exception as exc:
        raise ValueError(f"{label} returned an invalid ABI string: {exc}") from exc
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} returned an empty string")
    return value


def _normalize_pair(description: str) -> str:
    pair = "/".join(part.strip().upper() for part in description.split("/"))
    if pair.count("/") != 1:
        raise ValueError(f"Chainlink description is not a BASE/QUOTE pair: {description!r}")
    return pair


def _feed_kind(pair: str) -> str:
    quote = pair.rsplit("/", 1)[1]
    return quote.lower() if quote in {"USD", "ETH"} else "reference"


def _pinned_block(client: GatewayIdentityClient, chain: str) -> tuple[int, str]:
    number = client.block_number(chain)
    if number is None:
        raise ValueError(f"Gateway could not measure the {chain} block head")
    response = client.rpc.Call(
        gateway_pb2.RpcRequest(
            chain=chain,
            method="eth_getBlockByNumber",
            params=json.dumps([hex(number), False]),
            id="data_identity_pinned_block",
        )
    )
    if not response.success:
        raise ValueError(f"Gateway could not read pinned block {number}: {response.error}")
    try:
        block = json.loads(response.result)
        block_hash = str(block["hash"]).lower()
        observed_number = int(str(block["number"]), 16)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"Gateway returned a malformed pinned block: {exc}") from exc
    if observed_number != number:
        raise ValueError(f"Pinned block number mismatch: requested {number}, observed {observed_number}")
    return number, block_hash


def _call(
    client: GatewayIdentityClient,
    requirement: Requirement,
    selector: str,
    block_number: int,
    calls: list[dict[str, object]],
) -> str:
    result = client.eth_call(
        requirement.chain,
        requirement.address,
        selector,
        block=block_number,
        raise_on_error=True,
    )
    if result is None:
        raise ValueError(f"Gateway returned no result for {requirement.requirement_id} {selector}")
    calls.append(
        {
            "method": "eth_call",
            "chain": requirement.chain,
            "to": requirement.address,
            "data": selector,
            "block": hex(block_number),
            "result": result,
        }
    )
    return result


def _observed_values(
    client: GatewayIdentityClient,
    requirement: Requirement,
    block_number: int,
    calls: list[dict[str, object]],
) -> dict[str, object]:
    if isinstance(requirement, TokenRequirement):
        selectors = (DECIMALS_SELECTOR,)
    elif isinstance(requirement, DirectChainlinkFeedRequirement):
        selectors = (DESCRIPTION_SELECTOR, DECIMALS_SELECTOR)
    else:
        selectors = (TOKEN0_SELECTOR, TOKEN1_SELECTOR, FEE_SELECTOR)
    for selector in selectors:
        _call(client, requirement, selector, block_number, calls)
    return decode_identity_calls(requirement, calls)


def decode_identity_calls(requirement: Requirement, calls: list[dict[str, object]]) -> dict[str, object]:
    """Independently derive identity fields from exact raw RPC return bytes."""
    if isinstance(requirement, TokenRequirement):
        expected = (DECIMALS_SELECTOR,)
    elif isinstance(requirement, DirectChainlinkFeedRequirement):
        expected = (DESCRIPTION_SELECTOR, DECIMALS_SELECTOR)
    else:
        expected = (TOKEN0_SELECTOR, TOKEN1_SELECTOR, FEE_SELECTOR)
    if tuple(call.get("data") for call in calls) != expected:
        raise ValueError(f"Unexpected identity call recipe for {requirement.requirement_id}")
    results = [str(call.get("result") or "") for call in calls]
    if isinstance(requirement, TokenRequirement):
        return {"decimals": _decode_uint(results[0], "decimals")}
    if isinstance(requirement, DirectChainlinkFeedRequirement):
        pair = _normalize_pair(_decode_string(results[0], "description"))
        return {
            "pair": pair,
            "decimals": _decode_uint(results[1], "decimals"),
            "feed_kind": _feed_kind(pair),
        }
    return {
        "token_pair": [_decode_address(results[0], "token0"), _decode_address(results[1], "token1")],
        "fee_tier": _decode_uint(results[2], "fee"),
    }


def _observation(
    requirement: Requirement,
    values: dict[str, object],
    provenance: ObservationProvenance,
) -> Observation:
    common = {
        "requirement_id": requirement.requirement_id,
        "chain": requirement.chain,
        "address": requirement.address,
        "provenance": provenance,
    }
    if isinstance(requirement, TokenRequirement):
        return TokenObservation(decimals=int(values["decimals"]), **common)
    if isinstance(requirement, DirectChainlinkFeedRequirement):
        return DirectChainlinkFeedObservation(
            provider="chainlink",
            pair=str(values["pair"]),
            decimals=int(values["decimals"]),
            feed_kind=str(values["feed_kind"]),
            **common,
        )
    pair = values["token_pair"]
    if not isinstance(pair, list):
        raise TypeError("Collected V3 token pair must be a list")
    return V3PoolObservation(
        protocol=requirement.protocol,
        token_pair=(str(pair[0]), str(pair[1])),
        fee_tier=int(values["fee_tier"]),
        **common,
    )


def collect_identity_bundle(
    client: GatewayIdentityClient,
    *,
    chain: str,
    output: Path,
    requirement_ids: Iterable[str] | None = None,
    captured_at: datetime | None = None,
) -> Path:
    """Capture one exact chain scope into a replayable, content-addressed bundle."""
    chain = chain.strip().lower()
    production = tuple(requirement for requirement in derive_production_requirements() if requirement.chain == chain)
    by_id = {requirement.requirement_id: requirement for requirement in production}
    if requirement_ids is None:
        selected = production
    else:
        requested = tuple(sorted(set(requirement_ids)))
        unknown = sorted(set(requested) - set(by_id))
        if unknown:
            raise ValueError(f"Unknown production identity requirement(s): {', '.join(unknown)}")
        selected = tuple(by_id[requirement_id] for requirement_id in requested)
    if not selected:
        raise ValueError(f"No production identity requirements selected for {chain}")

    block_number, block_hash = _pinned_block(client, chain)
    observed_at = (captured_at or datetime.now(UTC)).astimezone(UTC)
    observations: list[Observation] = []
    raw_dir = output / "raw"
    for requirement in selected:
        calls: list[dict[str, object]] = []
        values = _observed_values(client, requirement, block_number, calls)
        raw = {
            "schema_version": 1,
            "requirement": requirement.to_canonical_dict(),
            "pinned_block": {"number": block_number, "hash": block_hash},
            "calls": calls,
            "observed": values,
        }
        encoded = _canonical_bytes(raw)
        digest = hashlib.sha256(encoded).hexdigest()
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{digest}.json").write_bytes(encoded)
        provenance = ObservationProvenance(
            collector="gateway_rpc",
            captured_at=observed_at,
            block_number=block_number,
            block_hash=block_hash,
            artifact_sha256=digest,
        )
        observations.append(_observation(requirement, values, provenance))

    digest = requirements_digest(selected)
    _atomic_json(
        output / "requirements.json",
        {
            "schema_version": 1,
            "scope": {"chain": chain, "complete_chain_inventory": len(selected) == len(production)},
            "requirements_sha256": digest,
            "requirements": [requirement.to_canonical_dict() for requirement in selected],
        },
    )
    _atomic_json(
        output / "observations.json",
        {
            "schema_version": 1,
            "chain": chain,
            "requirements_sha256": digest,
            "pinned_block": {"number": block_number, "hash": block_hash},
            "captured_at": observed_at.isoformat().replace("+00:00", "Z"),
            "observations": [observation.to_canonical_dict() for observation in observations],
        },
    )
    return output


__all__ = ["GatewayIdentityClient", "collect_identity_bundle", "decode_identity_calls"]
