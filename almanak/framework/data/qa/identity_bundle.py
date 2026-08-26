"""Independent replay and validation of a Data identity evidence bundle."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from almanak.framework.data.qa.identity_collector import decode_identity_calls
from almanak.framework.data.qa.production_identity import (
    DirectChainlinkFeedObservation,
    Observation,
    ObservationSetEvaluation,
    Requirement,
    TokenObservation,
    V3PoolObservation,
    derive_production_requirements,
    evaluate_observation_set,
    observation_from_dict,
    requirement_from_dict,
    requirements_digest,
)


@dataclass(frozen=True)
class ValidatedIdentityBundle:
    chain: str
    requirements: tuple[Requirement, ...]
    observations: tuple[Observation, ...]
    evaluation: ObservationSetEvaluation
    requirements_sha256: str
    captured_at: datetime
    complete_chain_inventory: bool


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read identity artifact {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Identity artifact {path.name} must contain an object")
    return value


def _observation_values(observation: Observation) -> dict[str, object]:
    if isinstance(observation, TokenObservation):
        return {"decimals": observation.decimals}
    if isinstance(observation, DirectChainlinkFeedObservation):
        return {
            "pair": observation.pair,
            "decimals": observation.decimals,
            "feed_kind": observation.feed_kind,
        }
    if isinstance(observation, V3PoolObservation):
        return {"token_pair": list(observation.token_pair), "fee_tier": observation.fee_tier}
    raise TypeError(f"Unsupported identity observation {type(observation).__name__}")


def _validate_raw_artifact(bundle: Path, requirement: Requirement, observation: Observation) -> None:
    digest = observation.provenance.artifact_sha256
    path = bundle / "raw" / f"{digest}.json"
    if not path.is_file():
        raise ValueError(f"Raw gateway artifact missing for {requirement.requirement_id}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ValueError(f"Raw gateway artifact digest mismatch for {requirement.requirement_id}")
    raw = _load_object(path)
    if raw.get("requirement") != requirement.to_canonical_dict():
        raise ValueError(f"Raw gateway artifact requirement mismatch for {requirement.requirement_id}")
    pinned = raw.get("pinned_block")
    if not isinstance(pinned, dict):
        raise ValueError(f"Raw gateway artifact lacks pinned block for {requirement.requirement_id}")
    if pinned.get("number") != observation.provenance.block_number:
        raise ValueError(f"Raw gateway artifact block number mismatch for {requirement.requirement_id}")
    if str(pinned.get("hash") or "").lower() != observation.provenance.block_hash:
        raise ValueError(f"Raw gateway artifact block hash mismatch for {requirement.requirement_id}")
    if raw.get("observed") != _observation_values(observation):
        raise ValueError(f"Observation is not entailed by raw gateway artifact for {requirement.requirement_id}")
    calls = raw.get("calls")
    if not isinstance(calls, list) or not calls:
        raise ValueError(f"Raw gateway artifact has no RPC calls for {requirement.requirement_id}")
    for call in calls:
        if not isinstance(call, dict):
            raise ValueError(f"Malformed gateway call for {requirement.requirement_id}")
        expected = {
            "method": "eth_call",
            "chain": requirement.chain,
            "to": requirement.address,
            "block": hex(observation.provenance.block_number),
        }
        if any(call.get(key) != value for key, value in expected.items()):
            raise ValueError(f"Unbound gateway call for {requirement.requirement_id}")
        if not isinstance(call.get("data"), str) or not isinstance(call.get("result"), str):
            raise ValueError(f"Gateway call lacks request/result bytes for {requirement.requirement_id}")
    if decode_identity_calls(requirement, calls) != _observation_values(observation):
        raise ValueError(f"Raw gateway return bytes do not entail observation for {requirement.requirement_id}")


def _parse_rows(
    requirements_payload: dict[str, object], observations_payload: dict[str, object]
) -> tuple[tuple[Requirement, ...], tuple[Observation, ...], str]:
    raw_requirements = requirements_payload.get("requirements")
    raw_observations = observations_payload.get("observations")
    if not isinstance(raw_requirements, list) or not raw_requirements:
        raise ValueError("requirements.json requires a non-empty requirements array")
    if not isinstance(raw_observations, list) or not raw_observations:
        raise ValueError("observations.json requires a non-empty observations array")
    requirements = tuple(requirement_from_dict(row) for row in raw_requirements if isinstance(row, dict))
    observations = tuple(observation_from_dict(row) for row in raw_observations if isinstance(row, dict))
    if len(requirements) != len(raw_requirements) or len(observations) != len(raw_observations):
        raise ValueError("Identity requirement and observation rows must be objects")
    digest = requirements_digest(requirements)
    if requirements_payload.get("requirements_sha256") != digest:
        raise ValueError("requirements.json digest does not match its exact declarations")
    if observations_payload.get("requirements_sha256") != digest:
        raise ValueError("observations.json is not bound to the exact requirements digest")
    return requirements, observations, digest


def _validate_scope(
    requirements_payload: dict[str, object],
    observations_payload: dict[str, object],
    requirements: tuple[Requirement, ...],
    observations: tuple[Observation, ...],
) -> tuple[str, bool]:
    scope = requirements_payload.get("scope")
    if not isinstance(scope, dict) or not isinstance(scope.get("chain"), str):
        raise ValueError("requirements.json requires an exact chain scope")
    chain = str(scope["chain"]).lower()
    if any(requirement.chain != chain for requirement in requirements):
        raise ValueError("Identity bundle mixes requirement chains")
    if any(observation.chain != chain for observation in observations):
        raise ValueError("Identity bundle mixes observation chains")
    if observations_payload.get("chain") != chain:
        raise ValueError("observations.json chain does not match requirement scope")
    production = {item.requirement_id: item for item in derive_production_requirements() if item.chain == chain}
    selected = {item.requirement_id: item for item in requirements}
    if len(selected) != len(requirements):
        raise ValueError("Duplicate identity requirements are forbidden")
    unknown = sorted(set(selected) - set(production))
    if unknown:
        raise ValueError(f"Bundle contains non-production requirements: {', '.join(unknown)}")
    mutated = sorted(key for key, value in selected.items() if value != production[key])
    if mutated:
        raise ValueError(f"Bundle requirements drift from production: {', '.join(mutated)}")
    complete = scope.get("complete_chain_inventory") is True
    if complete and set(selected) != set(production):
        raise ValueError("Bundle claims complete chain inventory but omits production requirements")
    return chain, complete


def _validated_capture_time(
    observations_payload: dict[str, object],
    observations: tuple[Observation, ...],
    *,
    sealed_at: datetime | None,
    max_capture_age: timedelta,
) -> datetime:
    captured_values = {observation.provenance.captured_at for observation in observations}
    blocks = {(observation.provenance.block_number, observation.provenance.block_hash) for observation in observations}
    if len(captured_values) != 1 or len(blocks) != 1:
        raise ValueError("Every identity observation must share one capture time and pinned block")
    captured_at = next(iter(captured_values))
    declared_capture = datetime.fromisoformat(str(observations_payload.get("captured_at") or "").replace("Z", "+00:00"))
    # Known limitation: fromisoformat accepts a naive string, and astimezone(UTC)
    # then reads it in the host zone. Every producer writes datetime.now(UTC), so
    # naive values are not reachable today.
    if declared_capture.astimezone(UTC) != captured_at:
        raise ValueError("Observation capture timestamp mismatch")
    now = (sealed_at or datetime.now(UTC)).astimezone(UTC)
    if captured_at > now + timedelta(minutes=5):
        raise ValueError("Identity evidence is future-dated")
    if now - captured_at > max_capture_age:
        raise ValueError("Identity evidence is too old to seal")
    return captured_at


def validate_identity_bundle(
    bundle: Path,
    *,
    sealed_at: datetime | None = None,
    max_capture_age: timedelta = timedelta(hours=24),
) -> ValidatedIdentityBundle:
    """Re-derive declarations and replay every exact observation fail-closed."""
    requirements_payload = _load_object(bundle / "requirements.json")
    observations_payload = _load_object(bundle / "observations.json")
    requirements, observations, digest = _parse_rows(requirements_payload, observations_payload)
    chain, complete = _validate_scope(requirements_payload, observations_payload, requirements, observations)
    captured_at = _validated_capture_time(
        observations_payload,
        observations,
        sealed_at=sealed_at,
        max_capture_age=max_capture_age,
    )

    by_observation = {observation.requirement_id: observation for observation in observations}
    for requirement in requirements:
        observation = by_observation.get(requirement.requirement_id)
        if observation is not None:
            _validate_raw_artifact(bundle, requirement, observation)
    referenced = {f"{observation.provenance.artifact_sha256}.json" for observation in observations}
    actual = {path.name for path in (bundle / "raw").glob("*.json")}
    if actual != referenced:
        raise ValueError("Raw gateway artifact set is not bijective with observations")

    evaluation = evaluate_observation_set(requirements, observations)
    return ValidatedIdentityBundle(
        chain=chain,
        requirements=requirements,
        observations=observations,
        evaluation=evaluation,
        requirements_sha256=digest,
        captured_at=captured_at,
        complete_chain_inventory=complete,
    )


__all__ = ["ValidatedIdentityBundle", "validate_identity_bundle"]
