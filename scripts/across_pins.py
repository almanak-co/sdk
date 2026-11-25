"""Read the facts an Across SpokePool pin rests on, for every network the SDK defines.

Read-only. For each network it asks the venue's quote API which SpokePool a route from
that chain uses, a candidate only, and then reads the chain through the operator's
provider: the proxy's EIP-1967 implementation, the implementation's code hash, whether
that code holds every selector and event this connector uses, the views it reads,
whether its wrapped native token is the network definition's, and the first block at
which the proxy ran this implementation. A pin is written by hand into
`almanak.connectors.across.domain` from this output, after review; nothing here edits
code.

    uv run python scripts/across_pins.py --env-file .env
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.connectors.across.operations import DEPOSIT as DEPOSIT_SIGNATURE
from almanak.connectors.across.receipts import FUNDS_DEPOSITED_TOPIC
from almanak.connectors.across.settlement_reader import FILLED_RELAY_TOPIC, REFUND_ROOT_TOPIC
from almanak.gateway.providers import ALCHEMY_NETWORKS, AlchemyProvider, provider_endpoint

API = "https://app.across.to/api"
IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
VIEWS = (
    "depositQuoteTimeBuffer()",
    "fillDeadlineBuffer()",
    "chainId()",
    "pausedDeposits()",
    "wrappedNativeToken()",
)
ARGUMENT_VIEWS = {
    "fillStatuses(bytes32)": "00" * 32,
    "getRelayerRefund(address,address)": "00" * 64,
}
# Amounts tried for the candidate quote: tokens of 6 and of 18 decimals.
AMOUNTS = (10**8, 10**20)


def selector(signature: str) -> str:
    return keccak(text=signature)[:4].hex()


def required_code() -> dict[str, str]:
    """Every selector and event topic the connector relies on, as hex found in the code."""
    return {
        DEPOSIT_SIGNATURE: selector(DEPOSIT_SIGNATURE),
        "FundsDeposited": FUNDS_DEPOSITED_TOPIC[2:],
        "FilledRelay": FILLED_RELAY_TOPIC[2:],
        "ExecutedRelayerRefundRoot": REFUND_ROOT_TOPIC[2:],
        **{name: selector(name) for name in (*VIEWS, *ARGUMENT_VIEWS)},
    }


def missing_code(code: str) -> list[str]:
    """The required selectors and topics the implementation's runtime code lacks."""
    return [name for name, pattern in required_code().items() if pattern not in code.lower()]


class Node:
    def __init__(self, endpoint: str):
        self.endpoint, self.client = endpoint, httpx.Client(timeout=30)

    def rpc(self, method: str, params: list):
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        answer = self.client.post(self.endpoint, json=body).json()
        if "error" in answer:
            raise RuntimeError(f"{method}: {str(answer['error'].get('message'))[:120]}")
        return answer["result"]

    def view(self, to: str, signature: str, arguments: str = "", block: str = "latest") -> int:
        data = "0x" + selector(signature) + arguments
        return int(self.rpc("eth_call", [{"to": to, "data": data}, block]), 16)

    def implementation(self, proxy: str, block: str = "latest") -> str:
        return "0x" + self.rpc("eth_getStorageAt", [proxy, IMPLEMENTATION_SLOT, block])[-40:]


def candidates() -> dict[int, str]:
    """The SpokePool the venue names per origin chain. A candidate, verified on chain."""
    rows = httpx.get(f"{API}/available-routes", timeout=60).json()
    found: dict[int, str] = {}
    for row in rows:
        origin = row["originChainId"]
        if origin in found or ChainId(f"eip155:{origin}") not in ALCHEMY_NETWORKS:
            continue
        pool = quoted_pool(row)
        if pool:
            found[origin] = pool
    return found


def quoted_pool(row: dict) -> str | None:
    query = {
        "inputToken": row["originToken"],
        "outputToken": row["destinationToken"],
        "originChainId": row["originChainId"],
        "destinationChainId": row["destinationChainId"],
        # Only to learn the pool address: no deposit of unmatched decimals is ever built.
        "allowUnmatchedDecimals": "true",
    }
    for amount in AMOUNTS:
        answer = httpx.get(f"{API}/suggested-fees", params=query | {"amount": amount}, timeout=30)
        if answer.status_code == 200:
            return answer.json().get("spokePoolAddress", "").lower() or None
    return None


def installed(node: Node, proxy: str, implementation: str) -> dict:
    """The first block whose implementation slot holds `implementation`, by bisection."""
    head, low = int(node.rpc("eth_blockNumber", []), 16), 1
    while head - low > 1:
        middle = (low + head) // 2
        if node.implementation(proxy, hex(middle)) == implementation:
            head = middle
        else:
            low = middle
    stamp = int(node.rpc("eth_getBlockByNumber", [hex(head), False])["timestamp"], 16)
    return {"block": head, "at": datetime.fromtimestamp(stamp, UTC).isoformat()}


def block_seconds(node: Node, span: int = 2000) -> float:
    head = node.rpc("eth_getBlockByNumber", ["latest", False])
    number = int(head["number"], 16)
    earlier = node.rpc("eth_getBlockByNumber", [hex(number - span), False])
    return (int(head["timestamp"], 16) - int(earlier["timestamp"], 16)) / span


def views(node: Node, proxy: str) -> dict:
    read = {name: node.view(proxy, name) for name in VIEWS}
    read |= {name: node.view(proxy, name, body) for name, body in ARGUMENT_VIEWS.items()}
    return read


def wrapped_native_matches(chain: int, read: dict) -> bool:
    """Whether the pool wraps the asset the SDK refuses to send as wrapped native."""
    wrapped = evm_network(ChainId(f"eip155:{chain}")).wrapped_native.reference
    return f"0x{read['wrappedNativeToken()']:040x}" == wrapped


def facts(node: Node, chain: int, proxy: str) -> dict:
    implementation = node.implementation(proxy)
    code = node.rpc("eth_getCode", [implementation, "latest"])
    read = views(node, proxy)
    return {
        "chain": f"eip155:{chain}",
        "address": proxy,
        "implementation": implementation,
        "implementation_code_hash": "0x" + keccak(hexstr=code).hex(),
        "missing": missing_code(code),
        "chain_id_matches": read["chainId()"] == chain,
        "wrapped_native_matches": wrapped_native_matches(chain, read),
        "views": read,
        "installed": installed(node, proxy, implementation),
        "block_seconds": round(block_seconds(node), 3),
    }


def chain_facts(chain: int, proxy: str, env_file: Path) -> dict:
    network = ALCHEMY_NETWORKS[ChainId(f"eip155:{chain}")]
    provider = AlchemyProvider(kind="alchemy", network=network, api_key_env="ALCHEMY_API_KEY")
    try:
        return facts(Node(provider_endpoint(provider, env_file)), chain, proxy)
    except (RuntimeError, httpx.HTTPError, KeyError, ValueError) as error:
        # Never the endpoint: it carries the provider key.
        return {"chain": f"eip155:{chain}", "address": proxy, "error": type(error).__name__}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args(argv)
    rows = [chain_facts(chain, pool, args.env_file) for chain, pool in sorted(candidates().items())]
    json.dump(rows, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
