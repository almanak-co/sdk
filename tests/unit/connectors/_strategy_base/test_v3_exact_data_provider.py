from __future__ import annotations

import json
from decimal import Decimal, localcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.exact_venue_data_registry import ExactVenueDataProviderRegistry
from almanak.connectors._strategy_base.v3_pool_abi import V3_FEE_SELECTOR, V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    GatewayBlockIdentity,
    QuoteParameters,
    TwapParameters,
    VenueBindingComponent,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VerifiedVenueBinding,
    build_verified_venue_binding,
    observe_exact_venue_data,
)

BLOCK_HASH = "0x" + "44" * 32
POOL = "0x1111111111111111111111111111111111111111"
TOKEN0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
OBSERVE_SELECTOR = bytes.fromhex("883bdbfd")
DECIMALS_SELECTOR = bytes.fromhex("313ce567")


def _word(value: int, *, signed: bool = False) -> bytes:
    return value.to_bytes(32, "big", signed=signed)


def _address_word(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


def _observe_response(tick_start: int = 0, tick_end: int = 300) -> bytes:
    return b"".join(
        (
            _word(64),
            _word(160),
            _word(2),
            _word(tick_start, signed=True),
            _word(tick_end, signed=True),
            _word(2),
            _word(0),
            _word(1),
        )
    )


def _verified(protocol: str = "uniswap_v3") -> VerifiedVenueBinding:
    pool = VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_ADDRESS, POOL)
    router = VenueTargetRef(
        VenueTargetRole.ROUTER,
        VenueReferenceNamespace.EVM_ADDRESS,
        "0x2222222222222222222222222222222222222222",
    )
    return build_verified_venue_binding(
        chain="base",
        protocol=protocol,
        primitive=Primitive.SWAP,
        identity_refs=(pool,),
        binding_components=(VenueBindingComponent("fee", "500"),),
        ordered_assets=(
            AssetIdentity("base", AssetNamespace.ERC20, TOKEN0),
            AssetIdentity("base", AssetNamespace.ERC20, TOKEN1),
        ),
        binding_policy_version=1,
        operational_refs=(router,),
        evidence=VenueVerificationEvidence(
            chain="base",
            verifier_ref="almanak.connectors._strategy_base.v3_venue_verifier:V3VenueVerifier",
            verifier_contract_version="v3_exact_pool.v1",
            block_number=20,
            block_hash=BLOCK_HASH,
            observed_facts=(VenueObservedFact("router", router.reference, router),),
        ),
    )


def _twap_request(protocol: str = "uniswap_v3", *, reverse: bool = False) -> ExactVenueFeatureRequest:
    return ExactVenueFeatureRequest(
        verified_binding=_verified(protocol),
        parameters=TwapParameters(1, 0, 300, 20) if reverse else TwapParameters(0, 1, 300, 20),
        feature_contract_version="exact_twap.v1",
    )


class FakeGateway:
    def __init__(self) -> None:
        self.reads: list[tuple[str, str, bytes, int]] = []
        self.block_reads = 0
        self.block_hash = BLOCK_HASH
        self.token0 = TOKEN0
        self.fee = 500
        self.observe_response = _observe_response()

    def read(self, *, chain: str, target_address: str, payload: bytes, block_number: int) -> bytes:
        self.reads.append((chain, target_address, payload, block_number))
        if target_address == POOL and payload == bytes.fromhex(V3_TOKEN0_SELECTOR[2:]):
            return _address_word(self.token0)
        if target_address == POOL and payload == bytes.fromhex(V3_TOKEN1_SELECTOR[2:]):
            return _address_word(TOKEN1)
        if target_address == POOL and payload == bytes.fromhex(V3_FEE_SELECTOR[2:]):
            return _word(self.fee)
        if target_address in (TOKEN0, TOKEN1) and payload == DECIMALS_SELECTOR:
            return _word(18)
        if target_address == POOL and payload.startswith(OBSERVE_SELECTOR):
            return self.observe_response
        raise AssertionError("provider attempted an undeclared or fallback read")

    def block_identity(self, *, chain: str, block_number: int) -> GatewayBlockIdentity:
        assert (chain, block_number) == ("base", 20)
        self.block_reads += 1
        return GatewayBlockIdentity(number=20, block_hash=self.block_hash, timestamp=1_766_000_000)


def test_public_sdk_facade_executes_the_connector_declared_provider() -> None:
    raw_gateway = FakeGateway()
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0

    def eth_call(*, chain: str, to: str, data: str, block: int, raise_on_error: bool) -> str:
        assert raise_on_error is True
        raw = raw_gateway.read(
            chain=chain,
            target_address=to,
            payload=bytes.fromhex(data[2:]),
            block_number=block,
        )
        return "0x" + raw.hex()

    client.eth_call.side_effect = eth_call
    client.rpc.Call.return_value = SimpleNamespace(
        success=True,
        result=json.dumps(
            {
                "number": hex(20),
                "hash": BLOCK_HASH,
                "timestamp": hex(1_766_000_000),
            }
        ),
        error="",
    )

    result = observe_exact_venue_data(_twap_request(), client)

    assert type(result) is ExactVenueObservation
    assert result.value == Decimal("1.0001")
    assert client.eth_call.call_count == 6
    assert client.rpc.Call.call_count == 2


def test_public_sdk_facade_normalizes_an_unavailable_gateway() -> None:
    client = MagicMock()
    client.is_connected = False

    result = observe_exact_venue_data(_twap_request(), client)

    assert type(result) is VenueDataFailure
    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.PROVIDER_UNAVAILABLE
    client.eth_call.assert_not_called()


def test_public_sdk_facade_normalizes_a_connected_mid_call_failure() -> None:
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0
    client.rpc.Call.return_value = SimpleNamespace(
        success=True,
        result=json.dumps(
            {
                "number": hex(20),
                "hash": BLOCK_HASH,
                "timestamp": hex(1_766_000_000),
            }
        ),
        error="",
    )
    client.eth_call.side_effect = ConnectionError("gateway unavailable")

    result = observe_exact_venue_data(_twap_request(), client)

    assert type(result) is VenueDataFailure
    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.TRANSPORT_UNAVAILABLE
    assert client.eth_call.call_count == 1
    assert client.rpc.Call.call_count == 1


def test_public_sdk_facade_normalizes_registry_import_failure(monkeypatch, caplog) -> None:
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0

    import builtins
    import sys

    monkeypatch.delitem(
        sys.modules,
        "almanak.connectors._strategy_base.exact_venue_data_registry",
        raising=False,
    )
    real_import = builtins.__import__

    def _fail_registry(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "almanak.connectors._strategy_base.exact_venue_data_registry":
            raise ModuleNotFoundError("exact venue data registry cannot be imported")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fail_registry)

    with caplog.at_level("ERROR"):
        result = observe_exact_venue_data(_twap_request(), client)

    assert type(result) is VenueDataFailure
    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.PROVIDER_UNAVAILABLE
    assert "ModuleNotFoundError" in result.detail
    assert "Exact venue data provider is unavailable" in caplog.text
    client.eth_call.assert_not_called()
    client.rpc.Call.assert_not_called()


def test_public_sdk_facade_normalizes_provider_discovery_failure(monkeypatch, caplog) -> None:
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0

    def _fail_discovery(*_args, **_kwargs):
        raise ModuleNotFoundError("declared exact-data provider is missing")

    monkeypatch.setattr(ExactVenueDataProviderRegistry, "observe", _fail_discovery)

    with caplog.at_level("ERROR"):
        result = observe_exact_venue_data(_twap_request(), client)

    assert type(result) is VenueDataFailure
    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.PROVIDER_UNAVAILABLE
    assert "ModuleNotFoundError" in result.detail
    assert "Exact venue data provider is unavailable" in caplog.text
    client.eth_call.assert_not_called()
    client.rpc.Call.assert_not_called()


@pytest.mark.parametrize("protocol", ["uniswap_v3", "pancakeswap_v3"])
def test_registry_measures_only_the_verified_pool_through_generic_gateway_reads(protocol: str) -> None:
    request = _twap_request(protocol)
    gateway = FakeGateway()

    result = ExactVenueDataProviderRegistry().observe(request, gateway)

    assert type(result) is ExactVenueObservation
    assert result.value == Decimal("1.0001")
    assert result.binding_hash == request.binding_hash
    assert result.feature_identity == request.feature_identity
    assert result.anchor.block_number == 20
    assert result.anchor.block_hash == BLOCK_HASH
    assert result.provenance.source == "gateway_rpc.eth_call.v3_observe"
    assert gateway.block_reads == 2
    assert {target for _, target, _, _ in gateway.reads} == {POOL, TOKEN0, TOKEN1}
    assert all(chain == "base" and block == 20 for chain, _, _, block in gateway.reads)


def test_reverse_asset_order_inverts_the_same_exact_pool_measurement() -> None:
    forward = ExactVenueDataProviderRegistry().observe(_twap_request(), FakeGateway())
    reverse = ExactVenueDataProviderRegistry().observe(_twap_request(reverse=True), FakeGateway())
    assert type(forward) is ExactVenueObservation
    assert type(reverse) is ExactVenueObservation
    assert forward.value * reverse.value == Decimal(1)


def test_negative_fractional_tick_uses_v3_oracle_floor_semantics() -> None:
    gateway = FakeGateway()
    gateway.observe_response = _observe_response(tick_end=-301)
    result = ExactVenueDataProviderRegistry().observe(_twap_request(), gateway)
    assert type(result) is ExactVenueObservation
    with localcontext() as context:
        context.prec = 80
        assert result.value == Decimal("1.0001") ** -2


@pytest.mark.parametrize("mismatch", ["token0", "fee"])
def test_pool_identity_mismatch_is_typed_and_never_substituted(mismatch: str) -> None:
    gateway = FakeGateway()
    if mismatch == "token0":
        gateway.token0 = "0xcccccccccccccccccccccccccccccccccccccccc"
    else:
        gateway.fee = 3_000
    result = ExactVenueDataProviderRegistry().observe(_twap_request(), gateway)
    assert type(result) is VenueDataFailure
    assert result.state is VenueDataFailureState.MISMATCHED
    assert result.reason_code is VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH


@pytest.mark.parametrize(
    "malformed",
    [b"", bytes(255), bytes(257), _word(32) + _observe_response()[32:]],
)
def test_malformed_observe_boundary_is_a_typed_failure(malformed: bytes) -> None:
    gateway = FakeGateway()
    gateway.observe_response = malformed
    result = ExactVenueDataProviderRegistry().observe(_twap_request(), gateway)
    assert type(result) is VenueDataFailure
    assert result.reason_code is VenueDataFailureReason.TRANSPORT_UNAVAILABLE


def test_reorged_block_is_typed_unavailable_before_any_pool_read() -> None:
    gateway = FakeGateway()
    gateway.block_hash = "0x" + "55" * 32
    result = ExactVenueDataProviderRegistry().observe(_twap_request(), gateway)
    assert type(result) is VenueDataFailure
    assert result.reason_code is VenueDataFailureReason.REORGED_OBSERVATION
    assert gateway.reads == []


def test_mid_observation_reorg_discards_the_measurement() -> None:
    class ReorgGateway(FakeGateway):
        def block_identity(self, *, chain: str, block_number: int) -> GatewayBlockIdentity:
            identity = super().block_identity(chain=chain, block_number=block_number)
            if self.block_reads == 2:
                return GatewayBlockIdentity(
                    number=identity.number,
                    block_hash="0x" + "55" * 32,
                    timestamp=identity.timestamp,
                )
            return identity

    gateway = ReorgGateway()
    result = ExactVenueDataProviderRegistry().observe(_twap_request(), gateway)
    assert type(result) is VenueDataFailure
    assert result.reason_code is VenueDataFailureReason.REORGED_OBSERVATION
    assert gateway.reads


def test_unsupported_feature_protocol_and_contract_never_touch_a_fallback() -> None:
    quote_request = ExactVenueFeatureRequest(
        verified_binding=_verified(),
        parameters=QuoteParameters(0, 1, 1_000_000, 20),
        feature_contract_version="exact_quote.v1",
    )
    wrong_version = ExactVenueFeatureRequest(
        verified_binding=_verified(),
        parameters=TwapParameters(0, 1, 300, 20),
        feature_contract_version="exact_twap.v2",
    )
    gateway = FakeGateway()
    results = (
        ExactVenueDataProviderRegistry().observe(quote_request, gateway),
        ExactVenueDataProviderRegistry().observe(_twap_request("sushiswap_v3"), gateway),
        ExactVenueDataProviderRegistry().observe(wrong_version, gateway),
    )

    assert all(type(result) is VenueDataFailure for result in results)
    assert [result.reason_code for result in results if type(result) is VenueDataFailure] == [
        VenueDataFailureReason.UNSUPPORTED_FEATURE,
        VenueDataFailureReason.UNSUPPORTED_PROTOCOL,
        VenueDataFailureReason.UNSUPPORTED_FEATURE,
    ]
    assert gateway.reads == []
    assert gateway.block_reads == 0
