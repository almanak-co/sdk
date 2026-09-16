"""Recorder-owned token identity observations for scoped Intent proofs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from web3 import Web3

from qa_lab.asset_scenarios import validate_scope


def capture_asset_identity(web3: Any, receipt: dict[str, Any], plan_path: str, nodeid: str) -> dict[str, Any]:
    plan = json.loads(Path(plan_path).read_text())
    scope = validate_scope(plan["asset_scope"])
    if scope["proof_node"] != nodeid:
        raise ValueError("Asset observations must belong to the planned proof node")
    block = receipt.get("blockNumber", receipt.get("block_number"))
    if block is None:
        raise ValueError("Asset identity reads require a receipt block")
    header = web3.eth.get_block(block)
    calls = []
    for asset in scope["contract"]["assets"]:
        address = Web3.to_checksum_address(asset["address"])
        raw = web3.eth.call({"to": address, "data": "0x313ce567"}, block_identifier=block)
        calls.append({"to": address.lower(), "data": "0x313ce567", "result": Web3.to_hex(raw)})
    if web3.eth.get_block(block)["hash"] != header["hash"]:
        raise ValueError("Asset identity witness block changed during observation")
    return {
        "schema_version": 1,
        "scope_lookup_key": scope["lookup_key"],
        "contract_sha256": scope["contract_sha256"],
        "run_id": plan["run_id"],
        "sdk_commit": plan["sdk"]["commit"],
        "block_number": int(header["number"]),
        "block_hash": Web3.to_hex(header["hash"]),
        "calls": calls,
    }
