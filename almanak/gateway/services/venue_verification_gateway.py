"""Gateway-internal exact-venue observation adapter."""

from __future__ import annotations

from typing import Any

from almanak.core.rpc_network import Network
from almanak.framework.venues import VenueReferenceNamespace, VenueTargetRef
from almanak.gateway.utils.rpc_provider import get_cached_web3


class GatewayRpcVenueVerificationGateway:
    """Bind verifier reads to one canonical gateway chain and network.

    The adapter is constructed inside the gateway from the same typed
    ``(chain, network)`` selection used by its other RPC services. Every call
    rechecks the chain, so a verifier cannot accidentally use one chain's
    client while labelling evidence as another.
    """

    def __init__(self, *, chain: str, network: Network) -> None:
        if type(chain) is not str or not chain.strip():
            raise ValueError("venue verification chain must be non-empty")
        if type(network) is not Network:
            raise TypeError("venue verification network must be an exact Network")
        self._chain = chain
        self._web3 = get_cached_web3(chain, network)

    def _require_chain(self, chain: str) -> None:
        if chain != self._chain:
            raise ValueError(f"venue verification adapter is bound to {self._chain!r}, not {chain!r}")

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
        self._require_chain(chain)
        block: int | str = "latest" if block_number is None else block_number
        result = self._web3.eth.call(
            {"to": self._web3.to_checksum_address(self._evm_target(target)), "data": payload},
            block_identifier=block,
        )
        return bytes(result)

    def code(
        self,
        *,
        chain: str,
        target: VenueTargetRef,
        block_number: int | None = None,
    ) -> bytes:
        self._require_chain(chain)
        block: int | str = "latest" if block_number is None else block_number
        return bytes(
            self._web3.eth.get_code(
                self._web3.to_checksum_address(self._evm_target(target)),
                block_identifier=block,
            )
        )

    def block_number(self, *, chain: str) -> int:
        self._require_chain(chain)
        block_number = self._web3.eth.block_number
        if type(block_number) is not int or block_number <= 0:
            raise ValueError(f"gateway did not return a positive head block for {chain}")
        return block_number

    def block_hash(self, *, chain: str, block_number: int) -> str:
        self._require_chain(chain)
        block: Any = self._web3.eth.get_block(block_number)
        block_hash = block.get("hash")
        if block_hash is None:
            raise ValueError(f"gateway did not return block {block_number} hash on {chain}")
        return "0x" + bytes(block_hash).hex().lower()


__all__ = ["GatewayRpcVenueVerificationGateway"]
