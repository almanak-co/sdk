"""Block-anchored exact venue verifier for Aerodrome Slipstream LP pools."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from almanak.connectors._strategy_base.solidly_pool_abi import SOLIDLY_FACTORY_SELECTOR
from almanak.connectors._strategy_base.v3_pool_abi import V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    BaseVenueVerifier,
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VenueVerificationGateway,
    VenueVerificationRequest,
    VenueVerificationResult,
    build_verified_venue_binding,
)

from .addresses import SlipstreamDeployment, slipstream_deployment_for_factory
from .pool_validation import encode_aerodrome_cl_get_pool

VERIFIER_CONTRACT_VERSION = "aerodrome_slipstream_exact_pool.v1"
BINDING_POLICY_VERSION = 1

_TICK_SPACING_SELECTOR = "0xd0c93a7c"
_ZERO_ADDRESS = "0x" + "0" * 40
_ABI_WORD_BYTES = 32
_ABI_ADDRESS_PADDING_BYTES = 12
_ABI_INT24_BYTES = 3
_MIN_INT24 = -(1 << 23)
_MAX_INT24 = (1 << 23) - 1


class _SlipstreamAbiDecodeError(ValueError):
    """A successfully returned value has an invalid ABI shape."""


def _address_ref(role: VenueTargetRole, address: str) -> VenueTargetRef:
    return VenueTargetRef(
        role=role,
        reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
        reference=address.lower(),
    )


def _decode_address(raw: bytes) -> str:
    if type(raw) is not bytes or len(raw) != _ABI_WORD_BYTES:
        raise _SlipstreamAbiDecodeError("address result must be exactly one 32-byte ABI word")
    if raw[:_ABI_ADDRESS_PADDING_BYTES] != b"\x00" * _ABI_ADDRESS_PADDING_BYTES:
        raise _SlipstreamAbiDecodeError("address result has non-zero ABI padding")
    return "0x" + raw[_ABI_ADDRESS_PADDING_BYTES:].hex()


def _decode_int24(raw: bytes) -> int:
    if type(raw) is not bytes or len(raw) != _ABI_WORD_BYTES:
        raise _SlipstreamAbiDecodeError("int24 result must be exactly one 32-byte ABI word")
    encoded = int.from_bytes(raw[-_ABI_INT24_BYTES:], "big")
    value = encoded - (1 << 24) if encoded & (1 << 23) else encoded
    padding = b"\xff" if value < 0 else b"\x00"
    if raw[:-_ABI_INT24_BYTES] != padding * (_ABI_WORD_BYTES - _ABI_INT24_BYTES):
        raise _SlipstreamAbiDecodeError("int24 result has invalid ABI sign extension")
    if value < _MIN_INT24 or value > _MAX_INT24:
        raise _SlipstreamAbiDecodeError("int24 result is out of range")
    return value


def _failure(
    state: VenueBindingFailureState,
    reason: VenueBindingFailureReason,
    detail: str,
    *,
    evidence: VenueVerificationEvidence | None = None,
) -> VenueBindingFailure:
    return VenueBindingFailure(state=state, reason_code=reason, detail=detail, evidence=evidence)


@dataclass(frozen=True)
class _SlipstreamRequestInputs:
    pool_ref: VenueTargetRef
    requested_assets: tuple[str, ...]
    requested_spacing: str | None


@dataclass(frozen=True)
class _SlipstreamPoolObservation:
    token0: str
    token1: str
    tick_spacing: int
    factory: str


@dataclass(frozen=True)
class _SlipstreamGenerationObservation:
    deployment: SlipstreamDeployment
    canonical_pool: str
    factory_ref: VenueTargetRef
    operational_refs: tuple[VenueTargetRef, ...]
    code_by_ref: dict[VenueTargetRef, bytes]


class SlipstreamVenueVerifier(BaseVenueVerifier):
    """Verify an exact Slipstream pool and its factory/NPM generation."""

    @staticmethod
    def _validate_request(request: VenueVerificationRequest) -> VenueBindingFailure | _SlipstreamRequestInputs:
        if request.protocol != "aerodrome_slipstream" or request.primitive is not Primitive.LP:
            return _failure(
                VenueBindingFailureState.UNSUPPORTED,
                VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                "Slipstream exact-venue verification supports only aerodrome_slipstream LP",
            )
        if len(request.requested_refs) != 1 or request.requested_refs[0].role is not VenueTargetRole.POOL:
            return _failure(
                VenueBindingFailureState.UNSUPPORTED,
                VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                "Slipstream exact-venue verification requires exactly one pool reference",
            )
        return _SlipstreamRequestInputs(
            pool_ref=request.requested_refs[0],
            requested_assets=tuple(asset.asset_reference.lower() for asset in request.ordered_assets),
            requested_spacing=next(
                (component.value for component in request.binding_components if component.name == "tick_spacing"),
                None,
            ),
        )

    @staticmethod
    def _read_pool_observation(
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        pool_ref: VenueTargetRef,
        observed_block: int,
    ) -> _SlipstreamPoolObservation:
        return _SlipstreamPoolObservation(
            token0=_decode_address(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(V3_TOKEN0_SELECTOR[2:]),
                    block_number=observed_block,
                )
            ),
            token1=_decode_address(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(V3_TOKEN1_SELECTOR[2:]),
                    block_number=observed_block,
                )
            ),
            tick_spacing=_decode_int24(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(_TICK_SPACING_SELECTOR[2:]),
                    block_number=observed_block,
                )
            ),
            factory=_decode_address(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(SOLIDLY_FACTORY_SELECTOR[2:]),
                    block_number=observed_block,
                )
            ),
        )

    @staticmethod
    def _pool_facts(observation: _SlipstreamPoolObservation, pool_ref: VenueTargetRef) -> tuple[VenueObservedFact, ...]:
        return (
            VenueObservedFact(name="token0", value=observation.token0, target_ref=pool_ref),
            VenueObservedFact(name="token1", value=observation.token1, target_ref=pool_ref),
            VenueObservedFact(name="tick_spacing", value=str(observation.tick_spacing), target_ref=pool_ref),
            VenueObservedFact(name="factory", value=observation.factory, target_ref=pool_ref),
        )

    @staticmethod
    def _build_evidence(
        request: VenueVerificationRequest,
        pool_ref: VenueTargetRef,
        observed_block: int,
        block_hash: str,
        observation: _SlipstreamPoolObservation,
        generation: _SlipstreamGenerationObservation | None = None,
    ) -> VenueVerificationEvidence:
        facts = list(SlipstreamVenueVerifier._pool_facts(observation, pool_ref))
        if generation is not None:
            facts.extend(
                [
                    VenueObservedFact(
                        name="factory_pool", value=generation.canonical_pool, target_ref=generation.factory_ref
                    ),
                    VenueObservedFact(name="deployment_generation", value=generation.deployment.generation),
                ]
            )
            facts.extend(
                VenueObservedFact(
                    name="deployed_code_sha256",
                    value=hashlib.sha256(code).hexdigest(),
                    target_ref=ref,
                )
                for ref, code in generation.code_by_ref.items()
            )
        return VenueVerificationEvidence(
            chain=request.chain,
            verifier_ref="almanak.connectors.aerodrome.venue_verifier:SlipstreamVenueVerifier",
            verifier_contract_version=VERIFIER_CONTRACT_VERSION,
            block_number=observed_block,
            block_hash=block_hash,
            observed_facts=tuple(sorted(facts, key=lambda fact: fact.sort_key)),
        )

    @staticmethod
    def _match_observation(
        request: VenueVerificationRequest,
        inputs: _SlipstreamRequestInputs,
        observation: _SlipstreamPoolObservation,
        generation: _SlipstreamGenerationObservation | None,
        evidence: VenueVerificationEvidence,
    ) -> VenueBindingFailure | None:
        pool_ref = inputs.pool_ref
        if observation.token0 == _ZERO_ADDRESS or observation.token1 == _ZERO_ADDRESS or observation.tick_spacing <= 0:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.TARGET_MISMATCH,
                f"pool {pool_ref.reference} returned an invalid Slipstream tuple",
                evidence=evidence,
            )
        if (observation.token0, observation.token1) != inputs.requested_assets:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.ASSET_MISMATCH,
                f"pool assets {(observation.token0, observation.token1)!r} do not match requested order {inputs.requested_assets!r}",
                evidence=evidence,
            )
        if inputs.requested_spacing != str(observation.tick_spacing):
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.COMPONENT_MISMATCH,
                f"pool tick spacing {observation.tick_spacing} does not match requested {inputs.requested_spacing!r}",
                evidence=evidence,
            )
        assert generation is not None
        if generation.canonical_pool != pool_ref.reference:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.FACTORY_MISMATCH,
                f"factory returned {generation.canonical_pool}, not requested pool {pool_ref.reference}",
                evidence=evidence,
            )
        missing_code = [ref.reference for ref, code in generation.code_by_ref.items() if not code]
        if missing_code:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.TARGET_MISMATCH,
                f"verified Slipstream targets have no deployed code: {missing_code!r}",
                evidence=evidence,
            )
        return None

    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        *,
        block_number: int | None = None,
    ) -> VenueVerificationResult:
        inputs = self._validate_request(request)
        if isinstance(inputs, VenueBindingFailure):
            return inputs
        pool_ref = inputs.pool_ref
        try:
            observed_block = block_number if block_number is not None else gateway.block_number(chain=request.chain)
            if type(observed_block) is not int or observed_block <= 0:
                raise ValueError("venue verification requires a positive integer block number")
            block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
            observation = self._read_pool_observation(request, gateway, pool_ref, observed_block)
            if observation.tick_spacing <= 0:
                closing_block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
                if closing_block_hash != block_hash:
                    return _failure(
                        VenueBindingFailureState.UNAVAILABLE,
                        VenueBindingFailureReason.STALE_EVIDENCE,
                        f"Block {observed_block} changed while verifying Slipstream pool {pool_ref.reference}",
                    )
                evidence = self._build_evidence(request, pool_ref, observed_block, block_hash, observation)
                return _failure(
                    VenueBindingFailureState.MISMATCHED,
                    VenueBindingFailureReason.TARGET_MISMATCH,
                    f"pool {pool_ref.reference} returned an invalid Slipstream tuple",
                    evidence=evidence,
                )
            deployment = slipstream_deployment_for_factory(request.chain, observation.factory)
            if deployment is None:
                closing_block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
                if closing_block_hash != block_hash:
                    return _failure(
                        VenueBindingFailureState.UNAVAILABLE,
                        VenueBindingFailureReason.STALE_EVIDENCE,
                        f"Block {observed_block} changed while verifying Slipstream pool {pool_ref.reference}",
                    )
                evidence = self._build_evidence(request, pool_ref, observed_block, block_hash, observation)
                return _failure(
                    VenueBindingFailureState.MISMATCHED,
                    VenueBindingFailureReason.FACTORY_MISMATCH,
                    f"pool {pool_ref.reference} reports unreviewed Slipstream factory {observation.factory}",
                    evidence=evidence,
                )
            factory_ref = _address_ref(VenueTargetRole.FACTORY, deployment.factory)
            position_manager_ref = _address_ref(VenueTargetRole.POSITION_MANAGER, deployment.position_manager)
            canonical_pool = _decode_address(
                gateway.read(
                    chain=request.chain,
                    target=factory_ref,
                    payload=bytes.fromhex(
                        encode_aerodrome_cl_get_pool(observation.token0, observation.token1, observation.tick_spacing)[
                            2:
                        ]
                    ),
                    block_number=observed_block,
                )
            )
            operational_refs = tuple(sorted((factory_ref, position_manager_ref), key=lambda ref: ref.sort_key))
            code_by_ref = {
                ref: gateway.code(chain=request.chain, target=ref, block_number=observed_block)
                for ref in operational_refs
            }
            closing_block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
            generation = _SlipstreamGenerationObservation(
                deployment, canonical_pool, factory_ref, operational_refs, code_by_ref
            )
        except _SlipstreamAbiDecodeError as exc:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.COMPONENT_MISMATCH,
                f"Malformed Slipstream ABI observation for pool {pool_ref.reference}: {exc}",
            )
        except Exception as exc:
            return _failure(
                VenueBindingFailureState.UNAVAILABLE,
                VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
                f"Cannot verify exact Slipstream pool {pool_ref.reference} on {request.chain}: {exc}",
            )

        if closing_block_hash != block_hash:
            return _failure(
                VenueBindingFailureState.UNAVAILABLE,
                VenueBindingFailureReason.STALE_EVIDENCE,
                f"Block {observed_block} changed while verifying Slipstream pool {pool_ref.reference}",
            )

        evidence = self._build_evidence(request, pool_ref, observed_block, block_hash, observation, generation)
        mismatch = self._match_observation(request, inputs, observation, generation, evidence)
        if mismatch is not None:
            return mismatch

        return build_verified_venue_binding(
            chain=request.chain,
            protocol=request.protocol,
            primitive=request.primitive,
            identity_refs=request.requested_refs,
            binding_components=request.binding_components,
            ordered_assets=request.ordered_assets,
            binding_policy_version=request.binding_policy_version,
            operational_refs=generation.operational_refs,
            evidence=evidence,
        )


__all__ = ["BINDING_POLICY_VERSION", "VERIFIER_CONTRACT_VERSION", "SlipstreamVenueVerifier"]
