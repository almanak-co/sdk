"""Direct JSON-RPC reads for cross-checks, independent of the SDK's own reader.

A cross-check asks the same endpoint the same question at the same block through
plain `eth_call`, so a disagreement is the SDK's decoding or resolution, not a
second provider's view of the chain.
"""

from typing import Any

import httpx
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway.providers import PUBLIC_RPC

RPC_TIMEOUT_SECONDS = 20.0


class RpcUnavailable(Exception):
    """The endpoint did not answer the direct read; the check is blocked, not failed."""


def endpoint_url(endpoint: str, chain: str) -> str | None:
    """The named endpoint's URL for the chain, or None where it serves no such chain."""
    return PUBLIC_RPC.get(endpoint, {}).get(ChainId(chain))


def asset_address(asset: str) -> str:
    """The contract address of a CAIP-19 ERC-20 asset."""
    namespace, _, reference = asset.rpartition("/")[2].partition(":")
    if namespace != "erc20":
        raise ValueError(f"not an ERC-20 asset: {asset}")
    return reference


class Rpc:
    def __init__(self, url: str | None, client: httpx.Client | None = None):
        self.url = url
        self.client = client or httpx.Client(timeout=RPC_TIMEOUT_SECONDS)

    def request(self, method: str, params: list[Any]) -> Any:
        if self.url is None:
            raise RpcUnavailable(f"{method}: the endpoint does not serve this chain")
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            response = self.client.post(self.url, json=body)
            response.raise_for_status()
            document = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RpcUnavailable(f"{method}: {type(exc).__name__}") from None
        if not isinstance(document, dict) or document.get("result") is None:
            raise RpcUnavailable(f"{method}: no result")
        return document["result"]

    def call(
        self,
        to: str,
        signature: str,
        block: int,
        outputs: tuple[str, ...],
        args: tuple[Any, ...] = (),
    ) -> tuple[Any, ...]:
        """`eth_call` of `name(type,...)` at a block number, decoded as `outputs`."""
        types = [item for item in signature[signature.index("(") + 1 : -1].split(",") if item]
        data = keccak(text=signature)[:4] + encode(types, args)
        result = self.request("eth_call", [{"to": to, "data": "0x" + data.hex()}, hex(block)])
        try:
            return decode(outputs, bytes.fromhex(result[2:]))
        except Exception:
            # An empty or short answer is what a pruned or rate-limited node returns.
            raise RpcUnavailable(f"eth_call {signature}: undecodable result") from None

    def balance(self, account: str, block: int) -> int:
        return int(self.request("eth_getBalance", [account, hex(block)]), 16)

    def block_hash(self, block: int) -> str:
        header = self.request("eth_getBlockByNumber", [hex(block), False])
        return str(header["hash"])
