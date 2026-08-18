"""Block-anchored exact-venue verification for Curve pools.

The existing MetaRegistry resolver remains the candidate-discovery and
compatibility projection.  This verifier is the authority used before exact
new-risk calldata is compiled: it repeats every identity-defining read at one
gateway-observed block, sandwiches the observation with the block hash, and
returns only the shared :class:`VerifiedVenueBinding` contract or a closed
failure.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from eth_utils.abi import function_signature_to_4byte_selector
from web3.exceptions import ContractLogicError

from almanak.connectors._strategy_base.rpc import looks_like_revert
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
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

VERIFIER_CONTRACT_VERSION = "curve_exact_pool.v1"
BINDING_POLICY_VERSION = 1
VERIFIER_REF = "almanak.connectors.curve.venue_verifier:CurveVenueVerifier"

ADDRESS_PROVIDER = "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98"
META_REGISTRY_ADDRESS_ID = 7
MAX_COINS = 8
MAX_COIN_DECIMALS = 36
ZERO_ADDRESS = "0x" + "0" * 40

COMPONENT_NAMES = (
    "base_pool",
    "base_pool_coin_references",
    "coin_decimals",
    "coin_indices",
    "coin_references",
    "is_metapool",
    "lp_token",
    "n_coins",
    "pool_type",
)


class CurveBindingLike(Protocol):
    @property
    def chain(self) -> str: ...

    @property
    def pool_address(self) -> str: ...

    @property
    def coin_addresses(self) -> tuple[str, ...]: ...

    @property
    def coin_decimals(self) -> tuple[int, ...]: ...

    @property
    def lp_token(self) -> str: ...

    @property
    def pool_type(self) -> str: ...

    @property
    def is_metapool(self) -> bool: ...

    @property
    def base_pool(self) -> str | None: ...

    @property
    def base_pool_coin_addresses(self) -> tuple[str, ...] | None: ...


@dataclass(frozen=True, slots=True)
class _CurveObservation:
    block_number: int
    block_hash: str
    address_provider_ref: VenueTargetRef
    meta_registry_ref: VenueTargetRef
    coin_references: tuple[str, ...]
    coin_decimals: tuple[int, ...]
    lp_token: str
    pool_type: str
    is_metapool: bool
    base_pool: str | None
    base_pool_coin_references: tuple[str, ...] | None
    pool_code: bytes
    address_provider_code: bytes
    registry_code: bytes


class _CurveObservationRejected(Exception):
    def __init__(self, state: VenueBindingFailureState, reason: VenueBindingFailureReason, detail: str) -> None:
        super().__init__(detail)
        self.state = state
        self.reason = reason


class _CurveObservedInvalid(ValueError):
    """A successfully returned on-chain value violates the Curve contract."""


def _selector(signature: str) -> bytes:
    return function_signature_to_4byte_selector(signature)


GET_ADDRESS = _selector("get_address(uint256)")
GET_N_COINS = _selector("get_n_coins(address)")
GET_COINS = _selector("get_coins(address)")
GET_DECIMALS = _selector("get_decimals(address)")
GET_LP_TOKEN = _selector("get_lp_token(address)")
IS_META = _selector("is_meta(address)")
GET_BASE_POOL = _selector("get_base_pool(address)")
GET_UNDERLYING_COINS = _selector("get_underlying_coins(address)")
GAMMA = _selector("gamma()")


def _address_ref(role: VenueTargetRole, address: str) -> VenueTargetRef:
    return VenueTargetRef(
        role=role,
        reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
        reference=address.lower(),
    )


def _encode_uint(value: int) -> bytes:
    if type(value) is not int or value < 0:
        raise ValueError("ABI uint argument must be a non-negative integer")
    return value.to_bytes(32, "big")


def _encode_address(address: str) -> bytes:
    ref = _address_ref(VenueTargetRole.POOL, address)
    return bytes.fromhex(ref.reference[2:]).rjust(32, b"\x00")


def _word(raw: bytes, index: int = 0) -> bytes:
    if type(raw) is not bytes or len(raw) % 32 != 0 or len(raw) < (index + 1) * 32:
        raise _CurveObservedInvalid("Curve ABI result must contain complete 32-byte words")
    return raw[index * 32 : (index + 1) * 32]


def _decode_uint(raw: bytes, index: int = 0) -> int:
    return int.from_bytes(_word(raw, index), "big")


def _decode_scalar_uint(raw: bytes) -> int:
    if type(raw) is not bytes or len(raw) != 32:
        raise _CurveObservedInvalid("Curve ABI scalar uint result must contain exactly one word")
    return _decode_uint(raw)


def _decode_bool(raw: bytes) -> bool:
    if type(raw) is not bytes or len(raw) != 32:
        raise _CurveObservedInvalid("Curve ABI bool result must contain exactly one word")
    value = _decode_uint(raw)
    if value not in (0, 1):
        raise _CurveObservedInvalid("Curve ABI bool result must be 0 or 1")
    return bool(value)


def _decode_address(raw: bytes, index: int = 0) -> str:
    word = _word(raw, index)
    if word[:12] != b"\x00" * 12:
        raise _CurveObservedInvalid("Curve ABI address result has non-zero padding")
    return "0x" + word[12:].hex()


def _decode_scalar_address(raw: bytes) -> str:
    if type(raw) is not bytes or len(raw) != 32:
        raise _CurveObservedInvalid("Curve ABI scalar address result must contain exactly one word")
    return _decode_address(raw)


def _decode_fixed_addresses(raw: bytes) -> tuple[str, ...]:
    if type(raw) is not bytes or len(raw) != MAX_COINS * 32:
        raise _CurveObservedInvalid("Curve MetaRegistry address vector must contain exactly eight words")
    return tuple(_decode_address(raw, index) for index in range(MAX_COINS))


def _decode_fixed_uints(raw: bytes) -> tuple[int, ...]:
    if type(raw) is not bytes or len(raw) != MAX_COINS * 32:
        raise _CurveObservedInvalid("Curve MetaRegistry uint vector must contain exactly eight words")
    return tuple(_decode_uint(raw, index) for index in range(MAX_COINS))


def _component_map(request: VenueVerificationRequest) -> Mapping[str, str]:
    return {component.name: component.value for component in request.binding_components}


def _join(values: tuple[object, ...]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _pool_type_components(
    *,
    coin_references: tuple[str, ...],
    coin_decimals: tuple[int, ...],
    lp_token: str,
    pool_type: str,
    is_metapool: bool,
    base_pool: str | None,
    base_pool_coin_references: tuple[str, ...] | None,
) -> tuple[VenueBindingComponent, ...]:
    values = {
        "base_pool": base_pool or "none",
        "base_pool_coin_references": _join(base_pool_coin_references or ()),
        "coin_decimals": _join(tuple(coin_decimals)),
        "coin_indices": _join(tuple(range(len(coin_references)))),
        "coin_references": _join(tuple(address.lower() for address in coin_references)),
        "is_metapool": "true" if is_metapool else "false",
        "lp_token": lp_token.lower(),
        "n_coins": str(len(coin_references)),
        "pool_type": pool_type,
    }
    return tuple(VenueBindingComponent(name=name, value=values[name]) for name in COMPONENT_NAMES)


def curve_asset_identity(chain: str, address: str) -> AssetIdentity:
    """Convert one measured Curve coin reference without losing native identity."""
    if address.lower() == NATIVE_SENTINEL.lower():
        return AssetIdentity.native(chain)
    return AssetIdentity(chain, AssetNamespace.ERC20, address.lower())


def curve_verification_request(binding: CurveBindingLike, primitive: Primitive) -> VenueVerificationRequest:
    """Project the legacy deployment binding into the shared verifier request."""
    coin_addresses = tuple(str(value).lower() for value in binding.coin_addresses)
    coin_decimals = tuple(int(value) for value in binding.coin_decimals)
    base_pool = binding.base_pool
    base_pool_coins = binding.base_pool_coin_addresses
    pool_address = str(binding.pool_address).lower()
    return VenueVerificationRequest(
        chain=str(binding.chain),
        protocol="curve",
        primitive=primitive,
        requested_refs=(_address_ref(VenueTargetRole.POOL, pool_address),),
        ordered_assets=tuple(curve_asset_identity(str(binding.chain), address) for address in coin_addresses),
        binding_components=_pool_type_components(
            coin_references=coin_addresses,
            coin_decimals=coin_decimals,
            lp_token=str(binding.lp_token),
            pool_type=str(binding.pool_type),
            is_metapool=bool(binding.is_metapool),
            base_pool=None if base_pool is None else str(base_pool).lower(),
            base_pool_coin_references=(
                None if base_pool_coins is None else tuple(str(value).lower() for value in base_pool_coins)
            ),
        ),
        binding_policy_version=BINDING_POLICY_VERSION,
    )


def _failure(
    state: VenueBindingFailureState,
    reason: VenueBindingFailureReason,
    detail: str,
    *,
    evidence: VenueVerificationEvidence | None = None,
) -> VenueBindingFailure:
    return VenueBindingFailure(state=state, reason_code=reason, detail=detail, evidence=evidence)


class CurveVenueVerifier(BaseVenueVerifier):
    """Verify Curve MetaRegistry identity and exact ordered pool facts at one block."""

    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        *,
        block_number: int | None = None,
    ) -> VenueVerificationResult:
        invalid = self._validate_request(request)
        if invalid is not None:
            return invalid

        pool_ref = request.requested_refs[0]
        try:
            observation = self._observe(request, gateway, pool_ref, block_number)
        except _CurveObservationRejected as exc:
            return _failure(exc.state, exc.reason, str(exc))
        except _CurveObservedInvalid as exc:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.COMPONENT_MISMATCH,
                f"Measured Curve venue data is invalid for {pool_ref.reference}: {exc}",
            )
        except Exception as exc:
            return _failure(
                VenueBindingFailureState.UNAVAILABLE,
                VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
                f"Cannot verify exact Curve pool {pool_ref.reference} on {request.chain}: {exc}",
            )

        if observation is None:
            return _failure(
                VenueBindingFailureState.UNAVAILABLE,
                VenueBindingFailureReason.STALE_EVIDENCE,
                f"Block identity changed while verifying Curve pool {pool_ref.reference}",
            )

        observed_components = _pool_type_components(
            coin_references=observation.coin_references,
            coin_decimals=observation.coin_decimals,
            lp_token=observation.lp_token,
            pool_type=observation.pool_type,
            is_metapool=observation.is_metapool,
            base_pool=observation.base_pool,
            base_pool_coin_references=observation.base_pool_coin_references,
        )
        observed_assets = tuple(curve_asset_identity(request.chain, address) for address in observation.coin_references)
        evidence = self._evidence(request, pool_ref, observation, observed_components)

        if observed_assets != request.ordered_assets:
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.ASSET_MISMATCH,
                f"Curve ordered assets {observed_assets!r} do not match the requested venue order",
                evidence=evidence,
            )
        if observed_components != request.binding_components:
            expected = _component_map(request)
            actual = {component.name: component.value for component in observed_components}
            return _failure(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.COMPONENT_MISMATCH,
                f"Curve venue components differ: expected={expected!r}, observed={actual!r}",
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
            operational_refs=tuple(
                sorted(
                    (observation.address_provider_ref, observation.meta_registry_ref),
                    key=lambda ref: ref.sort_key,
                )
            ),
            evidence=evidence,
        )

    @staticmethod
    def _validate_request(request: VenueVerificationRequest) -> VenueBindingFailure | None:
        if request.protocol != "curve":
            return _failure(
                VenueBindingFailureState.UNSUPPORTED,
                VenueBindingFailureReason.UNSUPPORTED_PROTOCOL,
                f"Curve verifier cannot verify protocol {request.protocol!r}",
            )
        if request.primitive is not Primitive.SWAP:
            return _failure(
                VenueBindingFailureState.UNSUPPORTED,
                VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                f"Curve exact-pool verification does not support {request.primitive.value}",
            )
        if len(request.requested_refs) != 1 or request.requested_refs[0].role is not VenueTargetRole.POOL:
            return _failure(
                VenueBindingFailureState.UNSUPPORTED,
                VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                "Curve exact-pool verification requires exactly one pool reference",
            )
        return None

    def _observe(
        self,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        pool_ref: VenueTargetRef,
        block_number: int | None,
    ) -> _CurveObservation | None:
        address_provider_ref = _address_ref(VenueTargetRole.FACTORY, ADDRESS_PROVIDER)
        pool_arg = _encode_address(pool_ref.reference)
        observed_block = block_number if block_number is not None else gateway.block_number(chain=request.chain)
        if type(observed_block) is not int or observed_block <= 0:
            raise ValueError("venue verification requires a positive integer block number")
        opening_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)

        def read(target: VenueTargetRef, selector: bytes, argument: bytes = b"") -> bytes:
            return gateway.read(
                chain=request.chain,
                target=target,
                payload=selector + argument,
                block_number=observed_block,
            )

        meta_registry = _decode_scalar_address(
            read(address_provider_ref, GET_ADDRESS, _encode_uint(META_REGISTRY_ADDRESS_ID))
        )
        if meta_registry == ZERO_ADDRESS:
            raise _CurveObservationRejected(
                VenueBindingFailureState.MISMATCHED,
                VenueBindingFailureReason.FACTORY_MISMATCH,
                f"Curve AddressProvider returned no MetaRegistry on {request.chain}",
            )
        meta_registry_ref = _address_ref(VenueTargetRole.FACTORY, meta_registry)
        n_coins = _decode_scalar_uint(read(meta_registry_ref, GET_N_COINS, pool_arg))
        if n_coins < 2 or n_coins > MAX_COINS:
            raise _CurveObservedInvalid(f"Curve MetaRegistry returned implausible n_coins={n_coins}")
        coin_references = _decode_fixed_addresses(read(meta_registry_ref, GET_COINS, pool_arg))[:n_coins]
        coin_decimals = _decode_fixed_uints(read(meta_registry_ref, GET_DECIMALS, pool_arg))[:n_coins]
        self._validate_primary_coins(coin_references, coin_decimals)
        lp_token = _decode_scalar_address(read(meta_registry_ref, GET_LP_TOKEN, pool_arg))
        if lp_token == ZERO_ADDRESS:
            raise _CurveObservedInvalid("Curve MetaRegistry returned a zero LP token")
        is_metapool = _decode_bool(read(meta_registry_ref, IS_META, pool_arg))
        underlying = _decode_fixed_addresses(read(meta_registry_ref, GET_UNDERLYING_COINS, pool_arg))
        base_pool, base_pool_coins = self._base_pool_identity(
            is_metapool=is_metapool,
            underlying=underlying,
            coin_references=coin_references,
            read=read,
            meta_registry_ref=meta_registry_ref,
            pool_arg=pool_arg,
        )
        pool_type = self._pool_type(
            request=request,
            gateway=gateway,
            pool_ref=pool_ref,
            meta_registry_ref=meta_registry_ref,
            pool_arg=pool_arg,
            block_number=observed_block,
            n_coins=n_coins,
        )
        address_provider_code = gateway.code(
            chain=request.chain,
            target=address_provider_ref,
            block_number=observed_block,
        )
        pool_code = gateway.code(chain=request.chain, target=pool_ref, block_number=observed_block)
        registry_code = gateway.code(chain=request.chain, target=meta_registry_ref, block_number=observed_block)
        if not address_provider_code or not pool_code or not registry_code:
            raise _CurveObservedInvalid(
                "Curve AddressProvider, pool, or MetaRegistry has no deployed code at the verification block"
            )
        closing_hash = gateway.block_hash(chain=request.chain, block_number=observed_block)
        if closing_hash != opening_hash:
            return None
        return _CurveObservation(
            block_number=observed_block,
            block_hash=opening_hash,
            address_provider_ref=address_provider_ref,
            meta_registry_ref=meta_registry_ref,
            coin_references=coin_references,
            coin_decimals=coin_decimals,
            lp_token=lp_token,
            pool_type=pool_type,
            is_metapool=is_metapool,
            base_pool=base_pool,
            base_pool_coin_references=base_pool_coins,
            pool_code=pool_code,
            address_provider_code=address_provider_code,
            registry_code=registry_code,
        )

    @staticmethod
    def _validate_primary_coins(coin_references: tuple[str, ...], coin_decimals: tuple[int, ...]) -> None:
        if any(address == ZERO_ADDRESS for address in coin_references):
            raise _CurveObservedInvalid("Curve MetaRegistry returned a zero primary coin reference")
        if any(decimals > MAX_COIN_DECIMALS for decimals in coin_decimals):
            raise _CurveObservedInvalid(f"Curve MetaRegistry returned implausible decimals {coin_decimals!r}")

    @staticmethod
    def _base_pool_identity(
        *,
        is_metapool: bool,
        underlying: tuple[str, ...],
        coin_references: tuple[str, ...],
        read,
        meta_registry_ref: VenueTargetRef,
        pool_arg: bytes,
    ) -> tuple[str | None, tuple[str, ...] | None]:
        if not is_metapool:
            measured_underlying = tuple(underlying[: len(coin_references)])
            if (
                any(address != ZERO_ADDRESS for address in measured_underlying)
                and measured_underlying != coin_references
            ):
                raise _CurveObservationRejected(
                    VenueBindingFailureState.UNSUPPORTED,
                    VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
                    "Curve wrapped-lending pool identity is unsupported by the exact plain-pool contract",
                )
            return None, None
        base_pool = _decode_scalar_address(read(meta_registry_ref, GET_BASE_POOL, pool_arg))
        if base_pool == ZERO_ADDRESS:
            raise _CurveObservedInvalid("Curve metapool returned a zero base pool")
        first_zero = next(
            (index for index, address in enumerate(underlying) if address == ZERO_ADDRESS), len(underlying)
        )
        leading = underlying[:first_zero]
        if any(address != ZERO_ADDRESS for address in underlying[first_zero:]):
            raise _CurveObservedInvalid("Curve metapool underlying coins are not a contiguous prefix")
        if len(leading) <= 1:
            raise _CurveObservedInvalid("Curve metapool returned no ordered base-pool coins")
        if leading[0] != coin_references[0]:
            raise _CurveObservedInvalid(
                "Curve metapool underlying primary coin does not match the pool's first primary coin"
            )
        return base_pool, leading[1:]

    @staticmethod
    def _evidence(
        request: VenueVerificationRequest,
        pool_ref: VenueTargetRef,
        observation: _CurveObservation,
        observed_components: tuple[VenueBindingComponent, ...],
    ) -> VenueVerificationEvidence:
        observed_facts = [
            VenueObservedFact(
                name="deployed_code_sha256",
                value=hashlib.sha256(observation.address_provider_code).hexdigest(),
                target_ref=observation.address_provider_ref,
            ),
            VenueObservedFact(
                name="meta_registry",
                value=observation.meta_registry_ref.reference,
                target_ref=observation.meta_registry_ref,
            ),
            VenueObservedFact(
                name="deployed_code_sha256",
                value=hashlib.sha256(observation.registry_code).hexdigest(),
                target_ref=observation.meta_registry_ref,
            ),
            VenueObservedFact(
                name="deployed_code_sha256",
                value=hashlib.sha256(observation.pool_code).hexdigest(),
                target_ref=pool_ref,
            ),
        ]
        observed_facts.extend(
            VenueObservedFact(name=component.name, value=component.value, target_ref=pool_ref)
            for component in observed_components
        )
        return VenueVerificationEvidence(
            chain=request.chain,
            verifier_ref=VERIFIER_REF,
            verifier_contract_version=VERIFIER_CONTRACT_VERSION,
            block_number=observation.block_number,
            block_hash=observation.block_hash,
            observed_facts=tuple(sorted(observed_facts, key=lambda fact: fact.sort_key)),
        )

    @staticmethod
    def _pool_type(
        *,
        request: VenueVerificationRequest,
        gateway: VenueVerificationGateway,
        pool_ref: VenueTargetRef,
        meta_registry_ref: VenueTargetRef,
        pool_arg: bytes,
        block_number: int,
        n_coins: int,
    ) -> str:
        def gamma() -> bytes | None:
            try:
                return gateway.read(
                    chain=request.chain,
                    target=pool_ref,
                    payload=GAMMA,
                    block_number=block_number,
                )
            except ContractLogicError:
                return None
            except ValueError as exc:
                if not looks_like_revert(str(exc)):
                    raise
                return None

        def confirm_transport() -> None:
            raw = gateway.read(
                chain=request.chain,
                target=meta_registry_ref,
                payload=GET_N_COINS + pool_arg,
                block_number=block_number,
            )
            if _decode_scalar_uint(raw) != n_coins:
                raise _CurveObservedInvalid("Curve MetaRegistry identity drifted during gamma classification")

        raw = gamma()
        if not raw:
            confirm_transport()
            raw = gamma()
            if not raw:
                confirm_transport()
        if not raw:
            return "stableswap"
        _decode_scalar_uint(raw)
        return "tricrypto" if n_coins == 3 else "cryptoswap"


__all__ = [
    "BINDING_POLICY_VERSION",
    "COMPONENT_NAMES",
    "VERIFIER_CONTRACT_VERSION",
    "CurveVenueVerifier",
    "curve_asset_identity",
    "curve_verification_request",
]
