"""Gateway-bound transport adapters for exact venue verification."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.venues import VenueReferenceNamespace, VenueTargetRef, VenueTargetRole
from almanak.framework.venues.gateway import (
    GatewayClientExactVenueDataGateway,
    GatewayClientVenueVerificationGateway,
)
from almanak.gateway.services.venue_verification_gateway import GatewayRpcVenueVerificationGateway

TARGET = VenueTargetRef(
    VenueTargetRole.POOL,
    VenueReferenceNamespace.EVM_ADDRESS,
    "0x1111111111111111111111111111111111111111",
)


def test_gateway_client_adapter_pins_every_rpc_to_the_requested_chain_and_block() -> None:
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0
    client.eth_call.return_value = "0x1234"
    client.block_number.return_value = 123

    def rpc_call(request, *, timeout):
        params = json.loads(request.params)
        if request.method == "eth_getCode":
            assert params == [TARGET.reference, "0x7b"]
            result = "0x6000"
        else:
            assert request.method == "eth_getBlockByNumber"
            assert params == ["0x7b", False]
            result = {"hash": "0x" + "ab" * 32}
        return SimpleNamespace(success=True, result=json.dumps(result), error="")

    client.rpc.Call.side_effect = rpc_call
    gateway = GatewayClientVenueVerificationGateway(client)

    assert gateway.read(chain="arbitrum", target=TARGET, payload=b"\x12\x34", block_number=123) == b"\x12\x34"
    assert gateway.code(chain="arbitrum", target=TARGET, block_number=123) == b"\x60\x00"
    assert gateway.block_number(chain="arbitrum") == 123
    assert gateway.block_hash(chain="arbitrum", block_number=123) == "0x" + "ab" * 32
    client.eth_call.assert_called_once_with(
        chain="arbitrum",
        to=TARGET.reference,
        data="0x1234",
        block=123,
        raise_on_error=True,
    )


def test_exact_data_gateway_adapter_uses_generic_pinned_reads_and_strict_block_identity() -> None:
    client = MagicMock()
    client.is_connected = True
    client.config.timeout = 12.0
    client.eth_call.return_value = "0x1234"
    client.rpc.Call.return_value = SimpleNamespace(
        success=True,
        result=json.dumps(
            {
                "number": "0x7b",
                "hash": "0x" + "ab" * 32,
                "timestamp": hex(1_766_000_000),
            }
        ),
        error="",
    )
    gateway = GatewayClientExactVenueDataGateway(client)

    assert (
        gateway.read(
            chain="base",
            target_address=TARGET.reference,
            payload=b"\x12\x34",
            block_number=123,
        )
        == b"\x12\x34"
    )
    block = gateway.block_identity(chain="base", block_number=123)

    assert block.number == 123
    assert block.block_hash == "0x" + "ab" * 32
    assert block.timestamp == 1_766_000_000
    client.eth_call.assert_called_once_with(
        chain="base",
        to=TARGET.reference,
        data="0x1234",
        block=123,
        raise_on_error=True,
    )


def test_gateway_internal_adapter_rejects_cross_chain_labelling(monkeypatch: pytest.MonkeyPatch) -> None:
    web3 = MagicMock()
    monkeypatch.setattr(
        "almanak.gateway.services.venue_verification_gateway.get_cached_web3",
        lambda chain, network: web3,
    )
    gateway = GatewayRpcVenueVerificationGateway(chain="arbitrum", network=Network.ANVIL)

    with pytest.raises(ValueError, match="bound to 'arbitrum'"):
        gateway.read(chain="base", target=TARGET, payload=b"\x12\x34", block_number=123)
    web3.eth.call.assert_not_called()


def test_gateway_internal_adapter_pins_reads_and_code_to_the_supplied_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web3 = MagicMock()
    web3.to_checksum_address.return_value = TARGET.reference
    web3.eth.call.return_value = b"\x12\x34"
    web3.eth.get_code.return_value = b"\x60\x00"
    monkeypatch.setattr(
        "almanak.gateway.services.venue_verification_gateway.get_cached_web3",
        lambda chain, network: web3,
    )
    gateway = GatewayRpcVenueVerificationGateway(chain="arbitrum", network=Network.ANVIL)

    assert gateway.read(chain="arbitrum", target=TARGET, payload=b"\x12\x34", block_number=123) == b"\x12\x34"
    assert gateway.code(chain="arbitrum", target=TARGET, block_number=123) == b"\x60\x00"
    web3.eth.call.assert_called_once_with(
        {"to": TARGET.reference, "data": b"\x12\x34"},
        block_identifier=123,
    )
    web3.eth.get_code.assert_called_once_with(TARGET.reference, block_identifier=123)


@pytest.mark.parametrize("bad_head", [0, -1, True, "123"])
def test_gateway_internal_adapter_rejects_non_positive_or_untyped_head(
    monkeypatch: pytest.MonkeyPatch,
    bad_head: object,
) -> None:
    web3 = MagicMock()
    web3.eth.block_number = bad_head
    monkeypatch.setattr(
        "almanak.gateway.services.venue_verification_gateway.get_cached_web3",
        lambda chain, network: web3,
    )

    with pytest.raises(ValueError, match="positive head block"):
        GatewayRpcVenueVerificationGateway(chain="arbitrum", network=Network.ANVIL).block_number(chain="arbitrum")


def test_gateway_internal_adapter_rejects_missing_block_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    web3 = MagicMock()
    web3.eth.get_block.return_value = {"number": 123}
    monkeypatch.setattr(
        "almanak.gateway.services.venue_verification_gateway.get_cached_web3",
        lambda chain, network: web3,
    )

    with pytest.raises(ValueError, match="did not return block 123 hash"):
        GatewayRpcVenueVerificationGateway(chain="arbitrum", network=Network.ANVIL).block_hash(
            chain="arbitrum", block_number=123
        )


@pytest.mark.parametrize("bad_network", ["anvil", 1, True])
def test_gateway_internal_adapter_requires_typed_network(bad_network: object) -> None:
    with pytest.raises(TypeError, match="exact Network"):
        GatewayRpcVenueVerificationGateway(chain="arbitrum", network=bad_network)  # type: ignore[arg-type]
