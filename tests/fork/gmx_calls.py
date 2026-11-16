"""GMX calldata from the retained deployment ABIs, and receipt polling, for fork fixtures."""

import gzip
import json
import time
from pathlib import Path

from eth_abi import encode
from eth_utils import keccak

ROOT = Path(__file__).parents[2]
ARTIFACTS = (
    ROOT
    / "docs/internal/reports/evidence/2026-09-07-rollout/gmx-discovery/source/deployments/arbitrum"
)


def artifact(name):
    return json.loads(gzip.decompress((ARTIFACTS / f"{name}.json.gz").read_bytes()))


def abi_type(item):
    if item["type"].startswith("tuple"):
        return "(" + ",".join(abi_type(c) for c in item["components"]) + ")" + item["type"][5:]
    return item["type"]


def call(name, function, args):
    fn = next(
        a for a in artifact(name)["abi"] if a.get("name") == function and a["type"] == "function"
    )
    types = [abi_type(i) for i in fn["inputs"]]
    return keccak(text=function + "(" + ",".join(types) + ")")[:4] + encode(types, args)


def receipt_for(rpc, tx_hash):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        receipt = rpc("eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            return receipt
        time.sleep(0.05)
    raise TimeoutError("fixture receipt remains unknown")
