"""Exact, block-pinned V3 venue verification and manifest ownership."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.v3_pool_abi import V3_FEE_SELECTOR, V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.connectors._strategy_base.v3_venue_verifier import V3VenueVerifier
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
BLOCK = 123_456
BLOCK_HASH = "0x" + "ab" * 32


def _word_address(address: str) -> bytes:
    return bytes.fromhex("00" * 12 + address[2:])


@dataclass
class _Gateway:
    factory_pool: str = POOL
    fail: bool = False
    empty_code_roles: frozenset[VenueTargetRole] = frozenset()
    reads: list[tuple[str, VenueTargetRole, int | None]] = field(default_factory=list)
    fail_head: bool = False
    changing_hash: bool = False
    hash_reads: int = 0
    response_overrides: dict[tuple[VenueTargetRole, str], bytes] = field(default_factory=dict)

    def read(self, *, chain, target, payload, block_number=None):
        self.reads.append((chain, target.role, block_number))
        if self.fail:
            raise ValueError("upstream unavailable")
        selector = "0x" + payload[:4].hex()
        override = self.response_overrides.get((target.role, selector))
        if override is not None:
            return override
        if target.role is VenueTargetRole.POOL and selector == V3_TOKEN0_SELECTOR:
            return _word_address(TOKEN0)
        if target.role is VenueTargetRole.POOL and selector == V3_TOKEN1_SELECTOR:
            return _word_address(TOKEN1)
        if target.role is VenueTargetRole.POOL and selector == V3_FEE_SELECTOR:
            return (500).to_bytes(32, "big")
        if target.role is VenueTargetRole.FACTORY:
            return _word_address(self.factory_pool)
        raise AssertionError((target, selector))

    def code(self, *, chain, target, block_number=None):
        self.reads.append((chain, target.role, block_number))
        return b"" if target.role in self.empty_code_roles else b"\x60\x00"

    def block_number(self, *, chain):
        assert chain == "arbitrum"
        if self.fail_head:
            raise ValueError("head unavailable")
        return BLOCK

    def block_hash(self, *, chain, block_number):
        assert (chain, block_number) == ("arbitrum", BLOCK)
        self.hash_reads += 1
        if self.changing_hash and self.hash_reads > 1:
            return "0x" + "cd" * 32
        return BLOCK_HASH


def _request(*, protocol: str = "uniswap_v3", primitive: Primitive = Primitive.SWAP) -> VenueVerificationRequest:
    return VenueVerificationRequest(
        chain="arbitrum",
        protocol=protocol,
        primitive=primitive,
        requested_refs=(VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_ADDRESS, POOL),),
        ordered_assets=(
            AssetIdentity("arbitrum", AssetNamespace.ERC20, TOKEN0),
            AssetIdentity("arbitrum", AssetNamespace.ERC20, TOKEN1),
        ),
        binding_components=(VenueBindingComponent("fee", "500"),),
        binding_policy_version=1,
    )


@pytest.mark.parametrize("protocol", ["uniswap_v3", "pancakeswap_v3"])
@pytest.mark.parametrize("primitive", [Primitive.SWAP, Primitive.LP])
def test_declared_v3_verifier_produces_one_block_pinned_exact_binding(protocol: str, primitive: Primitive) -> None:
    gateway = _Gateway()
    result = V3VenueVerifier().verify_venue(_request(protocol=protocol, primitive=primitive), gateway)

    assert type(result) is VerifiedVenueBinding
    assert result.binding.identity_refs[0].reference == POOL
    assert result.binding.ordered_assets == _request(protocol=protocol, primitive=primitive).ordered_assets
    assert result.binding.binding_components == (VenueBindingComponent("fee", "500"),)
    assert result.evidence.block_number == BLOCK
    assert result.evidence.block_hash == BLOCK_HASH
    assert {fact["name"] for fact in result.evidence.to_wire()["observedFacts"]} == {
        "deployed_code_sha256",
        "factory_pool",
        "fee",
        "token0",
        "token1",
    }
    assert all(read[0] == "arbitrum" and read[2] == BLOCK for read in gateway.reads)
    assert {ref.role for ref in result.operational_refs} == {
        VenueTargetRole.FACTORY,
        VenueTargetRole.ROUTER if primitive is Primitive.SWAP else VenueTargetRole.POSITION_MANAGER,
    }


def test_factory_substitution_is_a_closed_mismatch() -> None:
    result = V3VenueVerifier().verify_venue(
        _request(),
        _Gateway(factory_pool="0x4444444444444444444444444444444444444444"),
    )

    assert type(result) is VenueBindingFailure
    assert result.reason_code is VenueBindingFailureReason.FACTORY_MISMATCH
    assert result.evidence is not None


def test_gateway_failure_and_missing_operational_code_fail_closed() -> None:
    unavailable = V3VenueVerifier().verify_venue(_request(), _Gateway(fail=True))
    missing_code = V3VenueVerifier().verify_venue(
        _request(),
        _Gateway(empty_code_roles=frozenset({VenueTargetRole.ROUTER})),
    )

    assert type(unavailable) is VenueBindingFailure
    assert unavailable.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE
    assert type(missing_code) is VenueBindingFailure
    assert missing_code.reason_code is VenueBindingFailureReason.TARGET_MISMATCH


@pytest.mark.parametrize(
    "malformed_word",
    (
        b"\x00" * 31,
        b"\x00" * 33,
        b"\x00" * 64,
        b"\x01" + b"\x00" * 11 + bytes.fromhex(TOKEN0[2:]),
    ),
    ids=("short", "oversized", "concatenated", "dirty-padding"),
)
def test_malformed_address_words_fail_closed(malformed_word: bytes) -> None:
    gateway = _Gateway(
        response_overrides={(VenueTargetRole.POOL, V3_TOKEN0_SELECTOR): malformed_word},
    )

    result = V3VenueVerifier().verify_venue(_request(), gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE


@pytest.mark.parametrize(
    "malformed_word",
    (
        b"\x00" * 31,
        b"\x00" * 33,
        b"\x00" * 64,
        b"\x01" + b"\x00" * 28 + (500).to_bytes(3, "big"),
    ),
    ids=("short", "oversized", "concatenated", "dirty-padding"),
)
def test_malformed_fee_words_fail_closed(malformed_word: bytes) -> None:
    gateway = _Gateway(
        response_overrides={(VenueTargetRole.POOL, V3_FEE_SELECTOR): malformed_word},
    )

    result = V3VenueVerifier().verify_venue(_request(), gateway)

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE
    assert all(role is not VenueTargetRole.FACTORY for _, role, _ in gateway.reads)


def test_head_block_failure_is_a_closed_unavailable_result() -> None:
    result = V3VenueVerifier().verify_venue(_request(), _Gateway(fail_head=True))

    assert type(result) is VenueBindingFailure
    assert result.reason_code is VenueBindingFailureReason.GATEWAY_UNAVAILABLE


def test_block_reorg_during_observation_is_closed_stale_evidence() -> None:
    result = V3VenueVerifier().verify_venue(_request(), _Gateway(changing_hash=True))

    assert type(result) is VenueBindingFailure
    assert result.state is VenueBindingFailureState.UNAVAILABLE
    assert result.reason_code is VenueBindingFailureReason.STALE_EVIDENCE


def test_manifests_declare_only_the_reviewed_exact_venue_protocols() -> None:
    declared = {
        declaration.protocol: declaration
        for connector in CONNECTOR_REGISTRY.with_venue_verifiers()
        for declaration in connector.venue_verifiers
    }

    assert set(declared) == {"aerodrome_slipstream", "curve", "pancakeswap_v3", "uniswap_v3"}
    assert declared["uniswap_v3"].chains == (
        "arbitrum",
        "avalanche",
        "base",
        "bsc",
        "ethereum",
        "optimism",
        "polygon",
    )
    assert declared["pancakeswap_v3"].chains == ("arbitrum", "base", "bsc", "ethereum")
    assert all(declared[protocol].component_names == ("fee",) for protocol in ("pancakeswap_v3", "uniswap_v3"))
    assert declared["curve"].chains == ("arbitrum", "base", "ethereum", "optimism", "polygon")
    assert declared["curve"].component_names == (
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
    assert declared["aerodrome_slipstream"].chains == ("base",)
    assert declared["aerodrome_slipstream"].primitives == (Primitive.LP,)
    assert declared["aerodrome_slipstream"].component_names == ("tick_spacing",)
    assert all(
        declared[protocol].component_names == ("fee",)
        for protocol in ("pancakeswap_v3", "uniswap_v3")
    )
    assert AddressRegistry.resolve_contract_address("uniswap_v3", "arbitrum", "factory")
