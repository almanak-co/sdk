"""Shared real-fork support for chain-symmetric GMX V2 intent tests."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents.compiler import IntentCompiler
from almanak.gateway.proto import gateway_pb2
from tests.support.gmx_v2 import GMX_V2_TOKENS

# The wrapped / bridged spellings of one asset. The GMX market label names the
# UNWRAPPED base symbol ("ETH/USD" -> "ETH") while the price fixture is keyed by
# the chain's own token symbols ("WETH" on Arbitrum, "WETH.e" on Avalanche), so
# the two have to be reconciled somewhere. Ordered most-canonical first.
_ASSET_SPELLINGS: dict[str, tuple[str, ...]] = {
    "ETH": ("ETH", "WETH", "WETH.e"),
    "BTC": ("BTC", "WBTC", "BTC.b"),
    "AVAX": ("AVAX", "WAVAX"),
}
_SPELLING_TO_BASE: dict[str, str] = {
    spelling.upper(): base for base, spellings in _ASSET_SPELLINGS.items() for spelling in spellings
}
_EVENT_LOG_DATA_TYPE = (
    "(((string,address)[],(string,address[])[]),((string,uint256)[],(string,uint256[])[]),"
    "((string,int256)[],(string,int256[])[]),((string,bool)[],(string,bool[])[]),"
    "((string,bytes32)[],(string,bytes32[])[]),((string,bytes)[],(string,bytes[])[]),"
    "((string,string)[],(string,string[])[]))"
)
_EVENT_EMITTER_SIGNATURE_BY_TOPIC_COUNT = {
    2: Web3.to_hex(Web3.keccak(text=f"EventLog(address,string,string,{_EVENT_LOG_DATA_TYPE})")).lower(),
    3: Web3.to_hex(Web3.keccak(text=f"EventLog1(address,string,string,bytes32,{_EVENT_LOG_DATA_TYPE})")).lower(),
    4: Web3.to_hex(
        Web3.keccak(text=f"EventLog2(address,string,string,bytes32,bytes32,{_EVENT_LOG_DATA_TYPE})")
    ).lower(),
}


def gmx_oracle_price_map(chain: str, prices: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """Price map keyed by BOTH address and symbol, as the gateway resolves both.

    The managed-Anvil executor prices a market's COLLATERAL tokens by address
    and its INDEX token by the market's base symbol — a GMX synthetic index
    token has no contract to answer for itself (ALM-3108). A stub keyed only by
    address answers the first leg and fails the second, which is not a shape the
    real gateway has: ``MarketService.GetPrice`` accepts either.

    Every key is lowercased so the caller can look up whatever it was handed.
    """
    by_key: dict[str, Decimal] = {}
    for symbol, price in prices.items():
        by_key.setdefault(symbol.lower(), price)
        base = _SPELLING_TO_BASE.get(symbol.upper())
        if base is not None:
            by_key.setdefault(base.lower(), price)
    for symbol, address in GMX_V2_TOKENS[chain].items():
        price = _price_for_symbol(prices, symbol)
        if price is not None:
            by_key[address.lower()] = price
    return by_key


def _price_for_symbol(prices: Mapping[str, Decimal], symbol: str) -> Decimal | None:
    """This token's price under any spelling the fixture may have used."""
    if symbol in prices:
        return prices[symbol]
    base = _SPELLING_TO_BASE.get(symbol.upper())
    if base is None:
        return None
    return next((prices[spelling] for spelling in _ASSET_SPELLINGS[base] if spelling in prices), None)


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
        self.chain = chain
        self.web3 = web3
        self.rpc = RpcService(web3)
        self.market = PriceService(gmx_oracle_price_map(chain, prices))

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
    from tests.unit.connectors.gmx_v2.market_fixtures import fake_dynamic_gateway

    return IntentCompiler(
        # Opens demand CURRENT venue listing (require_listed): resolution is
        # served by the fixture gateway while every chain read proxies to the
        # REAL fork rpc — the faithful hybrid for 4-layer intent tests.
        gateway_client=fake_dynamic_gateway(chain, rpc_url=orchestrator.rpc_url),
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


def assert_gmx_event_key(
    receipt: Mapping[str, Any],
    *,
    chain: str,
    event_name: str,
    key: str,
) -> dict[str, Any]:
    """Prove an exact GMX EventEmitter name/key pair from raw receipt topics.

    This intentionally does not use ``GMXv2ReceiptParser`` or its topic table:
    it is the independent witness against which parser output is reconciled.
    """
    emitter = Web3.to_checksum_address(GMX_V2[chain]["event_emitter"])
    event_name_topic = Web3.to_hex(Web3.keccak(text=event_name)).lower()
    expected_key = key.lower() if isinstance(key, str) else Web3.to_hex(key).lower()
    for log in receipt.get("logs", []):
        address = log.get("address")
        topics = [
            topic.lower() if isinstance(topic, str) else Web3.to_hex(topic).lower() for topic in log.get("topics", [])
        ]
        matched_key = next((topic for topic in topics[2:] if topic == expected_key), None)
        if (
            isinstance(address, str)
            and address.lower() == emitter.lower()
            and len(topics) >= 3
            and topics[0] == _EVENT_EMITTER_SIGNATURE_BY_TOPIC_COUNT.get(len(topics))
            and topics[1] == event_name_topic
            and matched_key is not None
        ):
            return {
                "kind": "raw_gmx_event_key",
                "event_emitter": emitter,
                "event_name": event_name,
                "event_name_topic": event_name_topic,
                "matched_event_name_topic": topics[1],
                "key": expected_key,
                "matched_key": matched_key,
                "log_index": log.get("log_index", log.get("logIndex")),
            }
    raise AssertionError(f"Raw receipt has no {event_name} emitted by {emitter} for exact key {expected_key}")


def assert_gmx_event_name(receipt: Mapping[str, Any], *, chain: str, event_name: str) -> dict[str, Any]:
    """Prove an exact GMX EventEmitter event name from raw receipt topics."""
    emitter = Web3.to_checksum_address(GMX_V2[chain]["event_emitter"])
    event_name_topic = Web3.to_hex(Web3.keccak(text=event_name)).lower()
    for log in receipt.get("logs", []):
        address = log.get("address")
        topics = [
            topic.lower() if isinstance(topic, str) else Web3.to_hex(topic).lower() for topic in log.get("topics", [])
        ]
        if (
            isinstance(address, str)
            and address.lower() == emitter.lower()
            and len(topics) >= 2
            and topics[0] == _EVENT_EMITTER_SIGNATURE_BY_TOPIC_COUNT.get(len(topics))
            and topics[1] == event_name_topic
        ):
            return {
                "kind": "raw_gmx_event_name",
                "event_emitter": emitter,
                "event_name": event_name,
                "event_name_topic": event_name_topic,
                "matched_event_name_topic": topics[1],
                "indexed_topics": topics[2:],
                "log_index": log.get("log_index", log.get("logIndex")),
            }
    raise AssertionError(f"Raw receipt has no {event_name} emitted by {emitter}")


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
    "gmx_oracle_price_map",
    "advance_past_cancel_age",
    "assert_recent_fork",
    "assert_gmx_event_key",
    "assert_gmx_event_name",
    "build_compiler",
    "receipt_dict",
]
