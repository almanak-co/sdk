"""Exact, generation-aware Aerodrome Slipstream venue verification."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from almanak.connectors._strategy_base.solidly_pool_abi import SOLIDLY_FACTORY_SELECTOR
from almanak.connectors._strategy_base.v3_pool_abi import V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_lp_deployments
from almanak.connectors.aerodrome.venue_verifier import SlipstreamVenueVerifier
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    VenueBindingComponent,
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationRequest,
    VerifiedVenueBinding,
)

POOL = "0x1111111111111111111111111111111111111111"
TOKEN0 = "0x2222222222222222222222222222222222222222"
TOKEN1 = "0x3333333333333333333333333333333333333333"
BLOCK = 31_000_000
BLOCK_HASH = "0x" + "ab" * 32
TICK_SPACING = 100
TICK_SPACING_SELECTOR = "0xd0c93a7c"


def _word_address(address: str) -> bytes:
    return bytes.fromhex("00" * 12 + address[2:])


def _word_int24(value: int) -> bytes:
    encoded = value if value >= 0 else (1 << 24) + value
    prefix = b"\xff" if value < 0 else b"\x00"
    return prefix * 29 + encoded.to_bytes(3, "big")


@dataclass
class _Gateway:
    deployment: SlipstreamDeployment
    factory_pool: str = POOL
    changing_hash: bool = False
    empty_code_roles: frozenset[VenueTargetRole] = frozenset()
    response_overrides: dict[tuple[VenueTargetRole, str], bytes] = field(default_factory=dict)
    calls: list[tuple[str, VenueTargetRole, int | None]] = field(default_factory=list)
    hash_reads: int = 0

    def read(self, *, chain, target, payload, block_number=None):
        self.calls.append(("read", target.role, block_number))
        selector = "0x" + payload[:4].hex()
        override = self.response_overrides.get((target.role, selector))
        if override is not None:
            return override
        if target.role is VenueTargetRole.POOL and selector == V3_TOKEN0_SELECTOR:
            return _word_address(TOKEN0)
        if target.role is VenueTargetRole.POOL and selector == V3_TOKEN1_SELECTOR:
            return _word_address(TOKEN1)
        if target.role is VenueTargetRole.POOL and selector == TICK_SPACING_SELECTOR:
            return _word_int24(TICK_SPACING)
        if target.role is VenueTargetRole.POOL and selector == SOLIDLY_FACTORY_SELECTOR:
            return _word_address(self.deployment.factory)
        if target.role is VenueTargetRole.FACTORY:
            assert target.reference == self.deployment.factory.lower()
            return _word_address(self.factory_pool)
        raise AssertionError((target, selector))

    def code(self, *, chain, target, block_number=None):
        self.calls.append(("code", target.role, block_number))
        return b"" if target.role in self.empty_code_roles else b"\x60\x00"

    def block_number(self, *, chain):
        assert chain == "base"
        return BLOCK

    def block_hash(self, *, chain, block_number):
        assert (chain, block_number) == ("base", BLOCK)
        self.hash_reads += 1
        if self.changing_hash and self.hash_reads > 1:
            return "0x" + "cd" * 32
        return BLOCK_HASH


def _request() -> VenueVerificationRequest:
    return VenueVerificationRequest(
        chain="base",
        protocol="aerodrome_slipstream",
        primitive=Primitive.LP,
        requested_refs=(VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_ADDRESS, POOL),),
        ordered_assets=(
            AssetIdentity("base", AssetNamespace.ERC20, TOKEN0),
            AssetIdentity("base", AssetNamespace.ERC20, TOKEN1),
        ),
        binding_components=(VenueBindingComponent("tick_spacing", str(TICK_SPACING)),),
        binding_policy_version=1,
    )


@pytest.mark.parametrize("deployment", slipstream_lp_deployments("base"), ids=lambda value: value.generation)
def test_verifier_binds_factory_generation_to_its_exact_position_manager(
    deployment: SlipstreamDeployment,
) -> None:
    gateway = _Gateway(deployment)

    result = SlipstreamVenueVerifier().verify_venue(_request(), gateway)

    assert type(result) is VerifiedVenueBinding
    assert result.binding.protocol == "aerodrome_slipstream"
    assert result.binding.binding_components == (VenueBindingComponent("tick_spacing", "100"),)
    assert {ref.role: ref.reference for ref in result.operational_refs} == {
        VenueTargetRole.FACTORY: deployment.factory.lower(),
        VenueTargetRole.POSITION_MANAGER: deployment.position_manager.lower(),
    }
    assert result.evidence.block_number == BLOCK
    assert result.evidence.block_hash == BLOCK_HASH
    assert {
        (fact.name, fact.value) for fact in result.evidence.observed_facts if fact.name == "deployment_generation"
    } == {("deployment_generation", deployment.generation)}
    assert all(block_number == BLOCK for _, _, block_number in gateway.calls)


def test_cross_factory_pool_is_a_measured_mismatch() -> None:
    deployment = slipstream_lp_deployments("base")[0]
    result = SlipstreamVenueVerifier().verify_venue(
        _request(),
        _Gateway(deployment, factory_pool="0x4444444444444444444444444444444444444444"),
    )

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.FACTORY_MISMATCH
    assert result.evidence is not None


def test_pool_reported_unreviewed_factory_is_a_measured_mismatch() -> None:
    deployment = slipstream_lp_deployments("base")[0]
    gateway = _Gateway(
        deployment,
        response_overrides={
            (VenueTargetRole.POOL, SOLIDLY_FACTORY_SELECTOR): _word_address(
                "0x4444444444444444444444444444444444444444"
            ),
        },
    )

    result = SlipstreamVenueVerifier().verify_venue(_request(), gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.FACTORY_MISMATCH
    assert result.evidence is not None
    assert gateway.hash_reads == 2
    assert all(role is VenueTargetRole.POOL for _, role, _ in gateway.calls)


@pytest.mark.parametrize("tick_spacing", (-1, 0))
def test_non_positive_tick_spacing_refuses_before_factory_read(tick_spacing: int) -> None:
    deployment = slipstream_lp_deployments("base")[0]
    gateway = _Gateway(
        deployment,
        response_overrides={
            (VenueTargetRole.POOL, TICK_SPACING_SELECTOR): _word_int24(tick_spacing),
        },
    )

    result = SlipstreamVenueVerifier().verify_venue(_request(), gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.TARGET_MISMATCH
    assert result.evidence is not None
    assert gateway.calls
    assert all(role is VenueTargetRole.POOL for _, role, _ in gateway.calls)


def test_reorg_and_missing_generation_target_fail_closed() -> None:
    deployment = slipstream_lp_deployments("base")[0]
    stale = SlipstreamVenueVerifier().verify_venue(_request(), _Gateway(deployment, changing_hash=True))
    missing_manager = SlipstreamVenueVerifier().verify_venue(
        _request(),
        _Gateway(deployment, empty_code_roles=frozenset({VenueTargetRole.POSITION_MANAGER})),
    )

    assert type(stale) is VenueBindingFailure
    assert stale.reason_code is VenueBindingFailureReason.STALE_EVIDENCE
    assert type(missing_manager) is VenueBindingFailure
    assert missing_manager.reason_code is VenueBindingFailureReason.TARGET_MISMATCH


@pytest.mark.parametrize(
    "selector, malformed_word",
    (
        (V3_TOKEN0_SELECTOR, b"\x00" * 31),
        (V3_TOKEN1_SELECTOR, b"\x01" + b"\x00" * 31),
        (TICK_SPACING_SELECTOR, b"\x01" + b"\x00" * 28 + TICK_SPACING.to_bytes(3, "big")),
        (SOLIDLY_FACTORY_SELECTOR, b"\x00" * 33),
    ),
)
def test_malformed_pool_observations_are_typed_unavailable(selector: str, malformed_word: bytes) -> None:
    deployment = slipstream_lp_deployments("base")[0]
    result = SlipstreamVenueVerifier().verify_venue(
        _request(),
        _Gateway(
            deployment,
            response_overrides={(VenueTargetRole.POOL, selector): malformed_word},
        ),
    )

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.MISMATCHED
    assert result.reason_code is VenueBindingFailureReason.COMPONENT_MISMATCH
