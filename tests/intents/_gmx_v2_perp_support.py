"""Shared real-fork support for chain-symmetric GMX V2 intent tests."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.gmx_v2.addresses import GMX_V2_TOKENS
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.gateway.proto import gateway_pb2


@dataclass(frozen=True)
class PriceService:
    prices_by_token: dict[str, Decimal]

    def GetPrice(self, request: Any) -> gateway_pb2.PriceResponse:  # noqa: N802
        price = self.prices_by_token.get(str(request.token).lower())
        if price is None:
            raise AssertionError(f"No measured test price for GMX oracle token {request.token}")
        return gateway_pb2.PriceResponse(price=str(price), source="intent-test-price-oracle", stale=False)


@dataclass(frozen=True)
class RpcService:
    web3: Web3

    def Call(self, request: Any, timeout: float | None = None) -> gateway_pb2.RpcResponse:  # noqa: N802
        del timeout
        response = self.web3.provider.make_request(request.method, json.loads(request.params))
        error = response.get("error")
        if error is not None:
            return gateway_pb2.RpcResponse(id=request.id, error=json.dumps(error), success=False)
        return gateway_pb2.RpcResponse(id=request.id, result=json.dumps(response.get("result")), success=True)


class AnvilGateway:
    """Gateway-shaped read adapter that routes every RPC to the test fork."""

    config = None

    def __init__(self, web3: Web3, chain: str, prices: dict[str, Decimal]) -> None:
        prices_by_token = {
            address.lower(): prices["WETH" if symbol == "WETH.e" else symbol]
            for symbol, address in GMX_V2_TOKENS[chain].items()
            if ("WETH" if symbol == "WETH.e" else symbol) in prices
        }
        self.chain = chain
        self.web3 = web3
        self.rpc = RpcService(web3)
        self.market = PriceService(prices_by_token)

    def eth_call(self, *, chain: str, to: str, data: str, block: int | str | None = None) -> str:
        assert chain == self.chain
        result = self.web3.eth.call(
            {"to": Web3.to_checksum_address(to), "data": data},
            block_identifier=block if block is not None else "latest",
        )
        return Web3.to_hex(result)


def build_compiler(
    *, chain: str, wallet: str, orchestrator: ExecutionOrchestrator, prices: dict[str, Decimal]
) -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        wallet_address=wallet,
        price_oracle=prices,
        rpc_url=orchestrator.rpc_url,
    )


def receipt_dict(execution: Any, transaction_index: int = -1) -> dict[str, Any]:
    result = execution.transaction_results[transaction_index]
    assert result.receipt is not None
    receipt = result.receipt.to_dict()
    status = receipt.get("status")
    status_int = int(status, 16) if isinstance(status, str) else int(status)
    assert status_int == 1
    return receipt


def assert_recent_fork(web3: Web3) -> None:
    fork_age_seconds = int(time.time()) - int(web3.eth.get_block("latest")["timestamp"])
    assert abs(fork_age_seconds) <= 7 * 24 * 60 * 60, (
        f"GMX intent proof requires a fork no older than seven days; age={fork_age_seconds}s"
    )


def advance_past_cancel_age(web3: Web3) -> None:
    """Advance chain time deterministically past GMX's account-cancel gate."""
    # GMX currently gates account cancellation at 300s. Keep a generous,
    # deterministic buffer so timestamp rounding and manifest-application blocks
    # cannot put the actual cancel back on the governance boundary.
    increased = web3.provider.make_request("evm_increaseTime", [600])
    assert "error" not in increased, increased
    mined = web3.provider.make_request("evm_mine", [])
    assert "error" not in mined, mined


__all__ = [
    "AnvilGateway",
    "advance_past_cancel_age",
    "assert_recent_fork",
    "build_compiler",
    "receipt_dict",
]
