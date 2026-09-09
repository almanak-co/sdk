"""Gateway-mediated, block-anchored full-key verification for the V4 singleton."""

from __future__ import annotations

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors._strategy_base.v4_pool_abi import V4_ZERO_ADDRESS, encode_get_slot0
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    BaseVenueVerifier,
    VenueBindingComponent,
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

from .addresses import UNISWAP_V4
from .pool_key import PoolKey

VERIFIER_REF = "almanak.connectors.uniswap_v4.venue_verifier:V4VenueVerifier"
CONTRACT_VERSION = "v4_exact_pool.v1"
COMPONENT_NAMES = ("currency0", "currency1", "fee", "hooks", "pool_manager", "tick_spacing")


def address_ref(role: VenueTargetRole, address: str) -> VenueTargetRef:
    return VenueTargetRef(role, VenueReferenceNamespace.EVM_ADDRESS, address.lower())


def verification_request(chain: str, key: PoolKey, primitive: Primitive = Primitive.SWAP) -> VenueVerificationRequest:
    values = {**key.to_wire(), "pool_manager": UNISWAP_V4[chain]["pool_manager"].lower()}
    return VenueVerificationRequest(
        chain=chain,
        protocol="uniswap_v4",
        primitive=primitive,
        requested_refs=(VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_BYTES32, key.pool_id),),
        ordered_assets=tuple(
            AssetIdentity.native(chain)
            if asset == V4_ZERO_ADDRESS
            else AssetIdentity(chain, AssetNamespace.ERC20, asset)
            for asset in (key.currency0, key.currency1)
        ),
        binding_components=tuple(VenueBindingComponent(name, str(values[name])) for name in COMPONENT_NAMES),
        binding_policy_version=1,
    )


class V4VenueVerifier(BaseVenueVerifier):
    """Hash the complete key and prove its initialized state in the configured manager."""

    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        *,
        block_number: int | None = None,
    ) -> VenueVerificationResult:
        try:
            components = {component.name: component.value for component in request.binding_components}
            key = PoolKey.from_wire(
                {
                    "currency0": components["currency0"],
                    "currency1": components["currency1"],
                    "fee": int(components["fee"]),
                    "tick_spacing": int(components["tick_spacing"]),
                    "hooks": components["hooks"],
                }
            )
            expected = verification_request(request.chain, key, request.primitive)
            if request != expected or request.primitive not in (Primitive.SWAP, Primitive.LP):
                raise ValueError("Full key, assets, manager, primitive or pool ID does not match the request")
        except (ValueError, KeyError, TypeError) as exc:
            return VenueBindingFailure(
                state=VenueBindingFailureState.MISMATCHED,
                reason_code=VenueBindingFailureReason.COMPONENT_MISMATCH,
                detail=str(exc),
            )

        addresses = UNISWAP_V4[request.chain]
        manager = address_ref(VenueTargetRole.FACTORY, addresses["pool_manager"])
        state_view = address_ref(VenueTargetRole.PERMISSION_TARGET, addresses["state_view"])
        route = address_ref(
            VenueTargetRole.ROUTER if request.primitive is Primitive.SWAP else VenueTargetRole.POSITION_MANAGER,
            addresses["universal_router"] if request.primitive is Primitive.SWAP else addresses["position_manager"],
        )
        operational = tuple(sorted((manager, route), key=lambda ref: ref.sort_key))
        try:
            block = gateway.block_number(chain=request.chain) if block_number is None else block_number
            block_hash = gateway.block_hash(chain=request.chain, block_number=block)
            # StateView is immutable periphery; verify it actually reads the selected manager.
            manager_bytes = gateway.read(
                chain=request.chain,
                target=state_view,
                payload=keccak(text="poolManager()")[:4],
                block_number=block,
            )
            if decode(["address"], manager_bytes)[0].lower() != manager.reference:
                raise ValueError("StateView belongs to a different PoolManager")
            raw = gateway.read(
                chain=request.chain,
                target=state_view,
                payload=bytes.fromhex(encode_get_slot0(key.pool_id)[2:]),
                block_number=block,
            )
            price, tick, protocol_fee, stored_fee = decode(["uint160", "int24", "uint24", "uint24"], raw)
            if price == 0:
                raise ValueError("Selected full V4 PoolKey is not initialized")
            code_facts = []
            for target in (*operational, state_view):
                code = gateway.code(chain=request.chain, target=target, block_number=block)
                if not code:
                    raise ValueError(f"Operational target {target.reference} has no deployed code")
                code_facts.append(VenueObservedFact("code_hash", "0x" + keccak(code).hex(), target))
            if gateway.block_hash(chain=request.chain, block_number=block) != block_hash:
                raise ValueError("Block hash changed during V4 venue verification")
        except Exception as exc:
            return VenueBindingFailure(
                state=VenueBindingFailureState.UNAVAILABLE,
                reason_code=VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
                detail=f"Cannot verify initialized V4 pool {key.pool_id}: {exc}",
            )
        facts = code_facts + [
            VenueObservedFact("sqrt_price_x96", str(price)),
            VenueObservedFact("tick", str(tick)),
            VenueObservedFact("stored_lp_fee", str(stored_fee)),
            VenueObservedFact("protocol_fee", str(protocol_fee)),
        ]
        return build_verified_venue_binding(
            chain=request.chain,
            protocol=request.protocol,
            primitive=request.primitive,
            identity_refs=request.requested_refs,
            binding_components=request.binding_components,
            ordered_assets=request.ordered_assets,
            binding_policy_version=request.binding_policy_version,
            operational_refs=operational,
            evidence=VenueVerificationEvidence(
                chain=request.chain,
                verifier_ref=VERIFIER_REF,
                verifier_contract_version=CONTRACT_VERSION,
                block_number=block,
                block_hash=block_hash,
                observed_facts=tuple(sorted(facts, key=lambda fact: fact.sort_key)),
            ),
        )
