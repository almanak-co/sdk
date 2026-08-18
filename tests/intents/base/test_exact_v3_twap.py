"""Real-fork exact TWAP observations for the reviewed Base V3 protocols."""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.v3_pool_validation import validate_v3_pool
from almanak.connectors._strategy_base.venue_verifier_registry import VenueVerifierRegistry
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    TwapParameters,
    VenueBindingComponent,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationRequest,
    VerifiedVenueBinding,
    observe_exact_venue_data,
)
from almanak.gateway.core.settings import GatewaySettings
from tests.conftest_gateway import AnvilFixture, GatewayServerThread, find_free_port
from tests.intents.conftest import CHAIN_CONFIGS

CHAIN_NAME = "base"


@pytest.fixture(scope="module")
def base_gateway_client(anvil_base: AnvilFixture) -> Generator[GatewayClient, None, None]:
    """Run one insecure local gateway routed only to the managed Base fork."""
    server = None
    client = None
    try:
        port = find_free_port()
        server = GatewayServerThread(
            GatewaySettings(
                grpc_port=port,
                grpc_host="127.0.0.1",
                network="anvil",
                metrics_enabled=False,
                audit_enabled=False,
                allow_insecure=True,
                standalone=True,
            ),
            anvil_ports={"base": anvil_base.port},
        )
        server.start()
        client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=port))
        client.connect()
        assert client.health_check()
        yield client
    finally:
        primary_error = sys.exc_info()[1]
        cleanup_error: BaseException | None = None
        try:
            if client is not None:
                client.disconnect()
        except BaseException as exc:
            cleanup_error = exc
        finally:
            try:
                if server is not None:
                    server.stop()
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


@pytest.mark.parametrize(
    ("failure_phase", "cleanup_failure"),
    (("start", None), ("connect", None), ("health", None), ("connect", "disconnect"), ("health", "stop")),
)
@pytest.mark.intent(IntentType.SWAP)  # noqa: layers -- data-only gateway lifecycle control.
def test_base_gateway_client_cleans_up_setup_failures(
    failure_phase: str,
    cleanup_failure: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Data-only gateway lifecycle controls cover setup and cleanup failures."""

    class FakeServer:
        def __init__(self, *args, **kwargs):
            self.start_calls = 0
            self.stop_calls = 0

        def start(self) -> None:
            self.start_calls += 1
            if failure_phase == "start":
                raise RuntimeError("start failed")

        def stop(self) -> None:
            self.stop_calls += 1
            if cleanup_failure == "stop":
                raise RuntimeError("stop failed")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.connect_calls = 0
            self.health_calls = 0
            self.disconnect_calls = 0

        def connect(self) -> None:
            self.connect_calls += 1
            if failure_phase == "connect":
                raise RuntimeError("connect failed")

        def health_check(self) -> bool:
            self.health_calls += 1
            if failure_phase == "health":
                raise RuntimeError("health failed")
            return True

        def disconnect(self) -> None:
            self.disconnect_calls += 1
            if cleanup_failure == "disconnect":
                raise RuntimeError("disconnect failed")

    server = FakeServer()
    client = FakeClient()
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "GatewayServerThread", lambda *a, **k: server)
    monkeypatch.setattr(module, "GatewayClient", lambda *a, **k: client)
    monkeypatch.setattr(module, "find_free_port", lambda: 12345)

    expected_error = f"{failure_phase} failed"
    with pytest.raises(RuntimeError, match=expected_error):
        next(base_gateway_client.__wrapped__(type("Anvil", (), {"port": 8545})()))
    assert server.start_calls == 1
    assert server.stop_calls == 1
    assert client.connect_calls == (0 if failure_phase == "start" else 1)
    assert client.health_calls == (1 if failure_phase == "health" else 0)
    assert client.disconnect_calls == (0 if failure_phase == "start" else 1)


@pytest.mark.base
@pytest.mark.intent(IntentType.SWAP)  # noqa: layers -- data-only observation has no intent execution layers.
@pytest.mark.parametrize(
    ("protocol", "fee_tier"),
    (("pancakeswap_v3", 100), ("uniswap_v3", 3000)),
)
def test_verified_exact_v3_twap_is_measured_from_the_named_pool(
    protocol: str,
    fee_tier: int,
    web3: Web3,
    anvil_eth_call_adapter,
    base_gateway_client: GatewayClient,
) -> None:
    """A verified pool yields a block-bound, request-bound positive TWAP."""
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    ordered_addresses = tuple(sorted((tokens["USDC"].lower(), tokens["WETH"].lower()), key=lambda item: int(item, 16)))
    pool = validate_v3_pool(
        CHAIN_NAME,
        protocol,
        ordered_addresses[0],
        ordered_addresses[1],
        fee_tier,
        web3.provider.endpoint_uri,  # type: ignore[attr-defined]
    )
    assert pool.exists is True and pool.pool_address

    verification_request = VenueVerificationRequest(
        chain=CHAIN_NAME,
        protocol=protocol,
        primitive=Primitive.SWAP,
        requested_refs=(
            VenueTargetRef(
                role=VenueTargetRole.POOL,
                reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
                reference=pool.pool_address.lower(),
            ),
        ),
        ordered_assets=tuple(AssetIdentity(CHAIN_NAME, AssetNamespace.ERC20, address) for address in ordered_addresses),
        binding_components=(VenueBindingComponent("fee", str(fee_tier)),),
        binding_policy_version=1,
    )
    fork_block = int(os.environ["ANVIL_FORK_BLOCK_BASE"])
    verifier_registry = VenueVerifierRegistry()
    verifier = verifier_registry.load_class(protocol)()
    verified = verifier_registry.validate_result(
        verification_request,
        verifier.verify_venue(verification_request, anvil_eth_call_adapter, block_number=fork_block),
    )
    assert type(verified) is VerifiedVenueBinding

    request = ExactVenueFeatureRequest(
        verified_binding=verified,
        parameters=TwapParameters(0, 1, 300, fork_block),
        feature_contract_version="exact_twap.v1",
    )
    result = observe_exact_venue_data(request, base_gateway_client)

    assert type(result) is ExactVenueObservation
    assert type(result.value) is Decimal and result.value.is_finite() and result.value > 0
    assert result.binding_hash == verified.binding.binding_hash
    assert result.feature_identity == request.feature_identity
    assert result.anchor.block_number == fork_block
    assert result.anchor.block_hash == verified.evidence.block_hash
    assert result.provenance.source == "gateway_rpc.eth_call.v3_observe"
    print(
        f"EXACT_TWAP_OK protocol={protocol} pool={pool.pool_address.lower()} "
        f"block={fork_block} binding={result.binding_hash} twap={result.value}"
    )
