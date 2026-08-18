"""Contract and mutation controls for the Curve exact-venue verifier."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from almanak.connectors._strategy_base.venue_verifier_registry import VenueVerifierRegistry
from almanak.connectors.curve.compiler import _verify_exact_curve_swap_binding
from almanak.connectors.curve.venue_verifier import (
    ADDRESS_PROVIDER,
    GAMMA,
    GET_ADDRESS,
    GET_BASE_POOL,
    GET_COINS,
    GET_DECIMALS,
    GET_LP_TOKEN,
    GET_N_COINS,
    GET_UNDERLYING_COINS,
    IS_META,
    ZERO_ADDRESS,
    CurveVenueVerifier,
    curve_verification_request,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueTargetRole,
    VerifiedVenueBinding,
)

POOL = "0x1111111111111111111111111111111111111111"
META_REGISTRY = "0x2222222222222222222222222222222222222222"
LP_TOKEN = "0x3333333333333333333333333333333333333333"
DAI = "0x4444444444444444444444444444444444444444"
USDC = "0x5555555555555555555555555555555555555555"
META_COIN = "0x6666666666666666666666666666666666666666"
BASE_POOL = "0x7777777777777777777777777777777777777777"
BASE_LP = "0x8888888888888888888888888888888888888888"
WRONG_META_COIN = "0x9999999999999999999999999999999999999999"
BLOCK_HASH = "0x" + "ab" * 32


def _word(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _address(address: str) -> bytes:
    return bytes.fromhex(address[2:]).rjust(32, b"\x00")


def _address_vector(*addresses: str) -> bytes:
    return b"".join(_address(address) for address in addresses) + b"\x00" * (32 * (8 - len(addresses)))


def _uint_vector(*values: int) -> bytes:
    return b"".join(_word(value) for value in values) + b"\x00" * (32 * (8 - len(values)))


class FakeCurveGateway:
    def __init__(self) -> None:
        self.coin_addresses: tuple[str, ...] = (DAI, USDC)
        self.coin_decimals: tuple[int, ...] = (18, 6)
        self.underlying_addresses: tuple[str, ...] = self.coin_addresses
        self.meta_registry = META_REGISTRY
        self.dirty_registry_padding = False
        self.oversized_registry_result = False
        self.oversized_meta_selector: bytes | None = None
        self.oversized_n_coins_result = False
        self.pool_error: Exception | None = ValueError("execution reverted")
        self.is_metapool = False
        self.base_pool = BASE_POOL
        self.missing_code_target: str | None = None
        self.hashes = [BLOCK_HASH, BLOCK_HASH]
        self.calls: list[tuple[str, bytes, int | None]] = []

    def block_number(self, *, chain: str) -> int:
        assert chain == "ethereum"
        return 20_000_000

    def block_hash(self, *, chain: str, block_number: int) -> str:
        assert chain == "ethereum"
        assert block_number == 20_000_000
        return self.hashes.pop(0)

    def code(self, *, chain: str, target, block_number: int | None = None) -> bytes:  # noqa: ANN001
        assert chain == "ethereum"
        assert block_number == 20_000_000
        assert target.reference.lower() in {ADDRESS_PROVIDER.lower(), POOL.lower(), META_REGISTRY.lower()}
        return b"" if target.reference == self.missing_code_target else b"\x60\x00"

    def read(self, *, chain: str, target, payload: bytes, block_number: int | None = None) -> bytes:  # noqa: ANN001
        assert chain == "ethereum"
        assert block_number == 20_000_000
        self.calls.append((target.reference, payload, block_number))
        selector = payload[:4]
        if target.reference.lower() == ADDRESS_PROVIDER.lower() and selector == GET_ADDRESS:
            encoded = _address(self.meta_registry)
            if self.dirty_registry_padding:
                return b"\x01" + encoded[1:]
            return encoded + (_word(1) if self.oversized_registry_result else b"")
        if target.reference == META_REGISTRY:
            if selector == GET_N_COINS:
                result = _word(2)
                return result + (_word(2) if self.oversized_n_coins_result else b"")
            if selector == GET_COINS:
                return _address_vector(*self.coin_addresses)
            if selector == GET_DECIMALS:
                return _uint_vector(*self.coin_decimals)
            if selector == GET_LP_TOKEN:
                result = _address(LP_TOKEN)
                return result + (_word(1) if self.oversized_meta_selector == selector else b"")
            if selector == IS_META:
                result = _word(int(self.is_metapool))
                return result + (_word(1) if self.oversized_meta_selector == selector else b"")
            if selector == GET_UNDERLYING_COINS:
                return _address_vector(*self.underlying_addresses)
            if selector == GET_BASE_POOL:
                if not self.is_metapool:
                    raise AssertionError("plain pool must not request a base pool")
                return _address(self.base_pool)
        if target.reference == POOL:
            if selector != GAMMA:
                raise AssertionError((target.reference, payload.hex()))
            if self.pool_error is not None:
                raise self.pool_error
            return b""
        raise AssertionError((target.reference, payload.hex()))


@dataclass(frozen=True)
class _Binding:
    chain: str = "ethereum"
    pool_address: str = POOL
    coin_addresses: tuple[str, ...] = (DAI, USDC)
    coin_decimals: tuple[int, ...] = (18, 6)
    lp_token: str = LP_TOKEN
    pool_type: str = "stableswap"
    is_metapool: bool = False
    base_pool: str | None = None
    base_pool_coin_addresses: tuple[str, ...] | None = None


def _binding() -> _Binding:
    return _Binding()


def _metapool_binding() -> _Binding:
    return _Binding(
        coin_addresses=(META_COIN, BASE_LP),
        coin_decimals=(18, 18),
        is_metapool=True,
        base_pool=BASE_POOL,
        base_pool_coin_addresses=(DAI, USDC),
    )


def _metapool_gateway() -> FakeCurveGateway:
    gateway = FakeCurveGateway()
    gateway.is_metapool = True
    gateway.coin_addresses = (META_COIN, BASE_LP)
    gateway.coin_decimals = (18, 18)
    gateway.underlying_addresses = (META_COIN, DAI, USDC)
    return gateway


def test_curve_verifier_builds_one_block_anchored_shared_binding() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()

    result = VenueVerifierRegistry().validate_result(request, CurveVenueVerifier().verify_venue(request, gateway))

    assert type(result) is VerifiedVenueBinding
    assert result.binding.identity_refs[0].reference == POOL
    assert tuple(asset.asset_reference for asset in result.binding.ordered_assets) == (DAI, USDC)
    assert result.operational_refs[0].role is VenueTargetRole.FACTORY
    assert result.operational_refs[0].reference == META_REGISTRY
    assert result.evidence.block_number == 20_000_000
    assert result.evidence.block_hash == BLOCK_HASH
    assert result.binding.binding_hash == "18c1b5f853f34dc04dcee0d4e85a53387456a8119c007bde1865578c24f1cd4d"
    assert {block for _, _, block in gateway.calls} == {20_000_000}


def test_curve_verifier_rejects_same_count_ordered_asset_substitution() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.coin_addresses = (USDC, DAI)
    gateway.underlying_addresses = gateway.coin_addresses

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.ASSET_MISMATCH


def test_curve_verifier_rejects_component_drift() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.coin_decimals = (18, 18)

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH


def test_curve_verifier_rejects_mid_observation_reorg() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.hashes[-1] = "0x" + "cd" * 32

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.STALE_EVIDENCE


def test_curve_verifier_rejects_noncanonical_abi_address_padding() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.dirty_registry_padding = True

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
    assert "non-zero padding" in result.detail


def test_curve_verifier_rejects_oversized_scalar_address_result() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.oversized_registry_result = True

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
    assert "exactly one word" in result.detail


def test_curve_verifier_rejects_oversized_scalar_bool_result() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.oversized_meta_selector = IS_META

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
    assert "exactly one word" in result.detail


def test_curve_verifier_rejects_oversized_n_coins_result() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.oversized_n_coins_result = True

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
    assert "exactly one word" in result.detail


def test_curve_verifier_binds_canonical_metapool_underlying_identity() -> None:
    request = curve_verification_request(_metapool_binding(), Primitive.SWAP)

    result = CurveVenueVerifier().verify_venue(request, _metapool_gateway())

    assert type(result) is VerifiedVenueBinding
    components = {component.name: component.value for component in result.binding.binding_components}
    assert components["base_pool"] == BASE_POOL
    assert components["base_pool_coin_references"] == f"{DAI},{USDC}"


def test_curve_verifier_rejects_metapool_wrong_primary_underlying_coin() -> None:
    request = curve_verification_request(_metapool_binding(), Primitive.SWAP)
    gateway = _metapool_gateway()
    gateway.underlying_addresses = (WRONG_META_COIN, DAI, USDC)

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert "primary coin" in result.detail


def test_curve_verifier_rejects_metapool_underlying_vector_gap() -> None:
    request = curve_verification_request(_metapool_binding(), Primitive.SWAP)
    gateway = _metapool_gateway()
    gateway.underlying_addresses = (META_COIN, ZERO_ADDRESS, DAI, USDC)

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert "contiguous prefix" in result.detail


def test_curve_verifier_classifies_gamma_absence_reverts_and_transport() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)

    gateway = FakeCurveGateway()
    gateway.pool_error = TimeoutError("pool-target timeout")
    result = CurveVenueVerifier().verify_venue(request, gateway)
    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE
    assert "pool-target timeout" in result.detail

    gateway = FakeCurveGateway()
    gateway.pool_error = ValueError("Gateway eth_call transport error for pool on ethereum: UNAVAILABLE")
    result = CurveVenueVerifier().verify_venue(request, gateway)
    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE

    for pool_error in (
        ValueError("execution reverted"),
        None,
        ValueError('Gateway eth_call error for pool on ethereum: {"code": 3, "message": "execution error"}'),
    ):
        gateway = FakeCurveGateway()
        gateway.pool_error = pool_error
        result = CurveVenueVerifier().verify_venue(request, gateway)
        assert type(result) is VerifiedVenueBinding
        components = {component.name: component.value for component in result.binding.binding_components}
        assert components["pool_type"] == "stableswap"
        assert sum(payload == GAMMA for target, payload, _ in gateway.calls if target == POOL) == 2
        assert sum(payload.startswith(GET_N_COINS) for target, payload, _ in gateway.calls if target == META_REGISTRY) == 3


def test_curve_verifier_treats_missing_pool_code_as_measured_mismatch() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.missing_code_target = POOL

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
    assert "no deployed code" in result.detail


def test_curve_verifier_rejects_missing_meta_registry_as_measured_mismatch() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.meta_registry = "0x" + "0" * 40

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.FACTORY_MISMATCH


def test_curve_verifier_does_not_misclassify_wrapped_lending_pool() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    gateway = FakeCurveGateway()
    gateway.underlying_addresses = (USDC, DAI)

    result = CurveVenueVerifier().verify_venue(request, gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNSUPPORTED
    assert result.reason_code is VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE


def test_curve_verifier_request_component_schema_is_manifest_owned() -> None:
    request = curve_verification_request(_binding(), Primitive.SWAP)
    malformed = replace(request, binding_components=request.binding_components[:-1])

    try:
        VenueVerifierRegistry().validate_result(
            malformed,
            CurveVenueVerifier().verify_venue(malformed, FakeCurveGateway()),
        )
    except ValueError as exc:
        assert "component schema" in str(exc)
    else:  # pragma: no cover - mutation guard
        raise AssertionError("manifest schema drift must fail closed")


def test_curve_verifier_does_not_certify_lp_before_abi_generation_is_bound() -> None:
    request = curve_verification_request(_binding(), Primitive.LP)

    result = CurveVenueVerifier().verify_venue(request, FakeCurveGateway())

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNSUPPORTED
    assert result.reason_code is VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE


def _pool_data() -> dict[str, object]:
    return {
        "address": POOL,
        "coins": ["DAI", "USDC"],
        "coin_addresses": [DAI, USDC],
        "coin_decimals": [18, 6],
        "lp_token": LP_TOKEN,
        "n_coins": 2,
        "pool_type": "stableswap",
        "is_metapool": False,
    }


def _intent() -> SwapIntent:
    return SwapIntent(
        from_token=DAI,
        to_token=USDC,
        amount=Decimal("1"),
        swap_params={"pool": POOL},
        protocol="curve",
        chain="ethereum",
    )


def _context(factory) -> Any:  # noqa: ANN001
    return SimpleNamespace(
        chain="ethereum",
        permission_discovery=False,
        venue_verification_gateway_factory=factory,
    )


def test_compiler_boundary_accepts_only_the_fresh_shared_binding() -> None:
    result = _verify_exact_curve_swap_binding(
        ctx=_context(FakeCurveGateway),
        intent=_intent(),
        pool_data=_pool_data(),
    )

    assert type(result) is VerifiedVenueBinding
    assert result.binding.binding_hash == "18c1b5f853f34dc04dcee0d4e85a53387456a8119c007bde1865578c24f1cd4d"


def test_compiler_boundary_refuses_measured_identity_mismatch() -> None:
    gateway = FakeCurveGateway()
    gateway.coin_addresses = (USDC, DAI)
    gateway.underlying_addresses = gateway.coin_addresses

    result = _verify_exact_curve_swap_binding(
        ctx=_context(lambda: gateway),
        intent=_intent(),
        pool_data=_pool_data(),
    )

    assert type(result) is CompilationResult
    assert result.status is CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert "asset_mismatch" in str(result.error)


def test_unavailable_verifier_blocks_exact_swap(caplog) -> None:  # noqa: ANN001
    def unavailable():
        raise ConnectionError("gateway unavailable")

    with caplog.at_level("ERROR", logger="almanak.connectors.curve.compiler"):
        blocked = _verify_exact_curve_swap_binding(
            ctx=_context(unavailable),
            intent=_intent(),
            pool_data=_pool_data(),
        )

    assert type(blocked) is CompilationResult
    assert blocked.status is CompilationStatus.FAILED
    assert blocked.is_safety_refusal is True
    assert blocked.action_bundle is None
    assert blocked.transactions == []
    assert "Exact Curve venue verification transport is unavailable" in caplog.text


def test_malformed_candidate_identity_is_a_logged_safety_refusal(caplog) -> None:  # noqa: ANN001
    malformed = _pool_data()
    malformed["coin_decimals"] = []

    with caplog.at_level("ERROR", logger="almanak.connectors.curve.compiler"):
        blocked = _verify_exact_curve_swap_binding(
            ctx=_context(FakeCurveGateway),
            intent=_intent(),
            pool_data=malformed,
        )

    assert type(blocked) is CompilationResult
    assert blocked.status is CompilationStatus.FAILED
    assert blocked.is_safety_refusal is True
    assert blocked.action_bundle is None
    assert blocked.transactions == []
    assert "Exact Curve venue identity is invalid before verification" in caplog.text
