"""Gateway-client adapter for connector-owned venue verification."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from almanak.gateway.proto import gateway_pb2

from .types import VenueReferenceNamespace, VenueTargetRef

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


class GatewayClientVenueVerificationGateway:
    """Route exact-venue observations through an SDK ``GatewayClient``."""

    def __init__(self, client: GatewayClient) -> None:
        if not getattr(client, "is_connected", False):
            raise ValueError("venue verification requires a connected gateway client")
        self._client = client

    @staticmethod
    def _evm_target(target: VenueTargetRef) -> str:
        if target.reference_namespace is not VenueReferenceNamespace.EVM_ADDRESS:
            raise ValueError("EVM venue verification requires an EVM address target")
        return target.reference

    def read(
        self,
        *,
        chain: str,
        target: VenueTargetRef,
        payload: bytes,
        block_number: int | None = None,
    ) -> bytes:
        result = self._client.eth_call(
            chain=chain,
            to=self._evm_target(target),
            data="0x" + payload.hex(),
            block=block_number,
            raise_on_error=True,
        )
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError(f"venue read returned no data for {target.reference} on {chain}")
        return bytes.fromhex(result[2:])

    def _rpc(self, *, chain: str, method: str, params: list[Any]) -> Any:
        response = self._client.rpc.Call(
            gateway_pb2.RpcRequest(chain=chain, method=method, params=json.dumps(params)),
            timeout=self._client.config.timeout,
        )
        if not response.success or not response.result:
            raise ValueError(f"gateway {method} failed on {chain}: {response.error or 'empty result'}")
        return json.loads(response.result)

    def code(
        self,
        *,
        chain: str,
        target: VenueTargetRef,
        block_number: int | None = None,
    ) -> bytes:
        block = "latest" if block_number is None else hex(block_number)
        raw = self._rpc(chain=chain, method="eth_getCode", params=[self._evm_target(target), block])
        if not isinstance(raw, str) or not raw.startswith("0x"):
            raise ValueError(f"eth_getCode returned malformed data for {target.reference} on {chain}")
        return bytes.fromhex(raw[2:])

    def block_number(self, *, chain: str) -> int:
        block_number = self._client.block_number(chain)
        if type(block_number) is not int or block_number <= 0:
            raise ValueError(f"gateway did not return a positive head block for {chain}")
        return block_number

    def block_hash(self, *, chain: str, block_number: int) -> str:
        block = self._rpc(chain=chain, method="eth_getBlockByNumber", params=[hex(block_number), False])
        block_hash = block.get("hash") if isinstance(block, dict) else None
        if not isinstance(block_hash, str):
            raise ValueError(f"gateway did not return block {block_number} hash on {chain}")
        return block_hash.lower()


__all__ = ["GatewayClientVenueVerificationGateway"]
