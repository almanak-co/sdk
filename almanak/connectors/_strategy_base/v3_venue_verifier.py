"""Block-anchored exact venue verifier for canonical V3 AMM pools."""

from __future__ import annotations

import hashlib

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.v3_pool_abi import (
    V3_FEE_SELECTOR,
    V3_TOKEN0_SELECTOR,
    V3_TOKEN1_SELECTOR,
    encode_v3_get_pool,
)
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

VERIFIER_CONTRACT_VERSION = "v3_exact_pool.v1"
BINDING_POLICY_VERSION = 1
_MAX_SANE_FEE = 1_000_000
_ZERO_ADDRESS = "0x" + "0" * 40


def _address_ref(role: VenueTargetRole, address: str) -> VenueTargetRef:
    return VenueTargetRef(
        role=role,
        reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
        reference=address.lower(),
    )


def _decode_address(raw: bytes) -> str:
    if len(raw) < 32:
        raise ValueError("address result is shorter than one ABI word")
    return "0x" + raw[-20:].hex()


def _evidence(
    *,
    request: VenueVerificationRequest,
    block_number: int,
    block_hash: str,
    observed_facts: list[VenueObservedFact],
) -> VenueVerificationEvidence:
    return VenueVerificationEvidence(
        chain=request.chain,
        verifier_ref="almanak.connectors._strategy_base.v3_venue_verifier:V3VenueVerifier",
        verifier_contract_version=VERIFIER_CONTRACT_VERSION,
        block_number=block_number,
        block_hash=block_hash,
        observed_facts=tuple(sorted(observed_facts, key=lambda fact: fact.sort_key)),
    )


def _result_from_observation(
    *,
    request: VenueVerificationRequest,
    operational_refs: tuple[VenueTargetRef, ...],
    token0: str,
    token1: str,
    fee: int,
    canonical_pool: str,
    code_by_ref: dict[VenueTargetRef, bytes],
    evidence: VenueVerificationEvidence,
) -> VenueVerificationResult:
    pool_ref = request.requested_refs[0]
    requested_assets = tuple(asset.asset_reference.lower() for asset in request.ordered_assets)
    requested_fee = next((component.value for component in request.binding_components if component.name == "fee"), None)
    if token0 == _ZERO_ADDRESS or token1 == _ZERO_ADDRESS or fee <= 0 or fee > _MAX_SANE_FEE:
        return VenueBindingFailure(
            state=VenueBindingFailureState.MISMATCHED,
            reason_code=VenueBindingFailureReason.TARGET_MISMATCH,
            detail=f"{pool_ref.reference} did not return a valid V3 token/fee tuple",
            evidence=evidence,
        )
    if (token0, token1) != requested_assets:
        return VenueBindingFailure(
            state=VenueBindingFailureState.MISMATCHED,
            reason_code=VenueBindingFailureReason.ASSET_MISMATCH,
            detail=f"pool assets {(token0, token1)!r} do not match requested order {requested_assets!r}",
            evidence=evidence,
        )
    if requested_fee != str(fee):
        return VenueBindingFailure(
            state=VenueBindingFailureState.MISMATCHED,
            reason_code=VenueBindingFailureReason.COMPONENT_MISMATCH,
            detail=f"pool fee {fee} does not match requested fee {requested_fee!r}",
            evidence=evidence,
        )
    if canonical_pool != pool_ref.reference:
        return VenueBindingFailure(
            state=VenueBindingFailureState.MISMATCHED,
            reason_code=VenueBindingFailureReason.FACTORY_MISMATCH,
            detail=f"factory returned {canonical_pool}, not requested pool {pool_ref.reference}",
            evidence=evidence,
        )
    empty_code = [ref.reference for ref, code in code_by_ref.items() if not code]
    if empty_code:
        return VenueBindingFailure(
            state=VenueBindingFailureState.MISMATCHED,
            reason_code=VenueBindingFailureReason.TARGET_MISMATCH,
            detail=f"verified operational targets have no deployed code: {empty_code!r}",
            evidence=evidence,
        )

    return build_verified_venue_binding(
        chain=request.chain,
        protocol=request.protocol,
        primitive=request.primitive,
        identity_refs=request.requested_refs,
        binding_components=request.binding_components,
        ordered_assets=request.ordered_assets,
        binding_policy_version=request.binding_policy_version,
        operational_refs=operational_refs,
        evidence=evidence,
    )


class V3VenueVerifier(BaseVenueVerifier):
    """Verify a requested pool against its connector-owned factory at one block."""

    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        *,
        block_number: int | None = None,
    ) -> VenueVerificationResult:
        if request.primitive not in (Primitive.SWAP, Primitive.LP):
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNSUPPORTED,
                reason_code=VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                detail=f"V3 exact-pool verification does not support {request.primitive.value}",
            )
        if len(request.requested_refs) != 1 or request.requested_refs[0].role is not VenueTargetRole.POOL:
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNSUPPORTED,
                reason_code=VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                detail="V3 exact-pool verification requires exactly one pool reference",
            )

        factory_address = AddressRegistry.resolve_contract_address(request.protocol, request.chain, "factory")
        router_address = AddressRegistry.resolve_contract_address(request.protocol, request.chain, "swap_router")
        position_manager_address = AddressRegistry.resolve_contract_address(
            request.protocol, request.chain, ("position_manager", "nft")
        )
        required_operational = (
            (VenueTargetRole.FACTORY, factory_address),
            (VenueTargetRole.ROUTER, router_address)
            if request.primitive is Primitive.SWAP
            else (VenueTargetRole.POSITION_MANAGER, position_manager_address),
        )
        if any(not address for _, address in required_operational):
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNSUPPORTED,
                reason_code=VenueBindingFailureReason.UNSUPPORTED_CHAIN,
                detail=f"{request.protocol} has no complete exact-venue address table on {request.chain}",
            )

        pool_ref = request.requested_refs[0]
        operational_refs = tuple(
            sorted(
                (_address_ref(role, str(address)) for role, address in required_operational),
                key=lambda ref: ref.sort_key,
            )
        )
        try:
            observed_block = block_number if block_number is not None else gateway.block_number(chain=request.chain)
            if type(observed_block) is not int or observed_block <= 0:
                raise ValueError("venue verification requires a positive integer block number")
            block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
            token0 = _decode_address(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(V3_TOKEN0_SELECTOR[2:]),
                    block_number=observed_block,
                )
            )
            token1 = _decode_address(
                gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=bytes.fromhex(V3_TOKEN1_SELECTOR[2:]),
                    block_number=observed_block,
                )
            )
            fee_raw = gateway.read(
                chain=request.chain,
                target=pool_ref,
                payload=bytes.fromhex(V3_FEE_SELECTOR[2:]),
                block_number=observed_block,
            )
            if len(fee_raw) < 32:
                raise ValueError("fee result is shorter than one ABI word")
            fee = int.from_bytes(fee_raw[:32], "big")
            factory_ref = next(ref for ref in operational_refs if ref.role is VenueTargetRole.FACTORY)
            canonical_pool = _decode_address(
                gateway.read(
                    chain=request.chain,
                    target=factory_ref,
                    payload=bytes.fromhex(encode_v3_get_pool(token0, token1, fee)[2:]),
                    block_number=observed_block,
                )
            )
            code_by_ref = {
                ref: gateway.code(
                    chain=request.chain,
                    target=ref,
                    block_number=observed_block,
                )
                for ref in operational_refs
            }
            closing_block_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
        except Exception as exc:
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNAVAILABLE,
                reason_code=VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
                detail=f"Cannot verify exact V3 pool {pool_ref.reference} on {request.chain}: {exc}",
            )
        if closing_block_hash != block_hash:
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNAVAILABLE,
                reason_code=VenueBindingFailureReason.STALE_EVIDENCE,
                detail=(
                    f"Block {observed_block} hash changed while verifying exact V3 pool "
                    f"{pool_ref.reference} on {request.chain}"
                ),
            )

        observed_facts = [
            VenueObservedFact(name="token0", value=token0, target_ref=pool_ref),
            VenueObservedFact(name="token1", value=token1, target_ref=pool_ref),
            VenueObservedFact(name="fee", value=str(fee), target_ref=pool_ref),
            VenueObservedFact(name="factory_pool", value=canonical_pool, target_ref=factory_ref),
        ]
        observed_facts.extend(
            VenueObservedFact(
                name="deployed_code_sha256",
                value=hashlib.sha256(code).hexdigest(),
                target_ref=ref,
            )
            for ref, code in code_by_ref.items()
        )
        evidence = _evidence(
            request=request,
            block_number=observed_block,
            block_hash=block_hash,
            observed_facts=observed_facts,
        )

        return _result_from_observation(
            request=request,
            operational_refs=operational_refs,
            token0=token0,
            token1=token1,
            fee=fee,
            canonical_pool=canonical_pool,
            code_by_ref=code_by_ref,
            evidence=evidence,
        )


__all__ = ["BINDING_POLICY_VERSION", "VERIFIER_CONTRACT_VERSION", "V3VenueVerifier"]
