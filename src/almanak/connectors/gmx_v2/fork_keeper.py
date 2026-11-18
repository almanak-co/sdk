"""Fork-only settlement of accepted GMX requests with authentic keeper reports.

GMX executes an accepted order, deposit or withdrawal only when one of its keepers
submits signed Chainlink Data Streams reports, and a fork runs no keeper. A fork test
replays a real one: it reads a keeper execution mined on the origin chain after the
request was created, impersonates that keeper, which already holds the role at the fork
origin, and submits its unchanged signed payloads to the fork's own handler. GMX then
verifies signatures, report times, acceptable price and triggers as it does on chain.

Nothing is synthesized: no price, signature, provider or role. A request without an
eligible report in its window stays pending, and the failure names why.
"""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.fresh_reports import FreshReports, Limits
from almanak.connectors.gmx_v2.oracle import StreamReport
from almanak.connectors.gmx_v2.oracle_reader import key
from almanak.connectors.gmx_v2.report_timeline import Timeline
from almanak.connectors.interfaces import ForkNodes
from almanak.gateway.block_time import first_block_at
from almanak.gateway.ethereum import DataError

LIMITS = Limits(
    page_blocks=1000,
    maximum_blocks=10000,
    maximum_transactions=100,
    maximum_logs=1000,
    wait_seconds=300,
    poll_seconds=2,
)
HANDLERS = {
    "order": "order_handler",
    "deposit": "deposit_handler",
    "withdrawal": "withdrawal_handler",
}
RECEIPT_SECONDS = 180
SETTLEMENTS = {"Executed": "executed", "Cancelled": "cancelled", "Frozen": "frozen"}
# Errors GMX cancels a request with when its prices or outputs miss the request's bounds.
CANCELLATIONS = {
    "0x" + keccak(text=signature)[:4].hex(): signature.split("(")[0]
    for signature in (
        "OrderNotFulfillableAtAcceptablePrice(uint256,uint256)",
        "PriceImpactLargerThanOrderSize(int256,uint256)",
        "InsufficientSwapOutputAmount(uint256,uint256)",
        "InsufficientOutputAmount(uint256,uint256)",
        "MinMarketTokens(uint256,uint256)",
    )
}


def settlement(mined, target, request) -> dict:
    """What the keeper's mined transaction did to `request`, with a cancellation's reason.

    The keeper transaction succeeds whether GMX executed the request or cancelled it,
    so its status alone does not say the request settled as asked.
    """
    for log in mined.get("logs", ()):
        try:
            event = decode_event(log, emitter=target.market.contracts.event_emitter)
        except ValueError:
            continue
        if event is None or not event.topics or event.topics[0] != request.key:
            continue
        status = SETTLEMENTS.get(event.name.removeprefix(request.kind.title()))
        if status == "cancelled":
            return {"request_status": status, **cancellation(event)}
        if status is not None:
            return {"request_status": status}
    return {}


def cancellation(event) -> dict:
    try:
        reason, data = event.value("string", "reason"), event.value("bytes", "reasonBytes")
    except ValueError:
        return {}
    return {"reason": reason or CANCELLATIONS.get(data[:10], data), "reason_bytes": data}


def execute(rpc, target, request, selected):
    """Submit one keeper execution of `request` on the fork with a selected source row."""
    reports = tuple(
        routed(rpc, target.market.contracts, StreamReport.model_validate_json(json.dumps(r)))
        for r in selected["reports"]
    )
    keeper = selected["oracle_source"]["from"]
    timestamp = max(r.observed_at for r in reports) + 1
    latest = int(rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    if latest >= timestamp:
        raise DataError("unavailable", "fork time has passed the selected reports")
    handler = getattr(target.market.contracts, HANDLERS[request.kind], None)
    if handler is None:
        raise DataError("unsupported", f"market has no {request.kind} handler")
    priced = fees(rpc)
    rpc("anvil_impersonateAccount", [keeper])
    rpc("anvil_setBalance", [keeper, hex(10**20)])
    rpc("evm_setNextBlockTimestamp", [timestamp])
    data = calldata(request.kind, request.key, source.oracle_parameters(reports))
    transaction = {
        "from": keeper,
        "to": handler,
        "data": data,
        "gas": hex(20_000_000),
        **priced,
    }
    try:
        return receipt(rpc, rpc("eth_sendTransaction", [transaction]))
    finally:
        rpc("anvil_stopImpersonatingAccount", [keeper])


def fees(rpc):
    """Explicit EIP-1559 fees: a node's defaults can pair a tip above the fee cap."""
    base = int(rpc("eth_getBlockByNumber", ["latest", False]).get("baseFeePerGas", "0x0"), 16)
    return {"maxFeePerGas": hex(2 * base + 1), "maxPriorityFeePerGas": "0x0"}


def calldata(kind, request_key, parameters):
    signature = "execute" + kind.title() + "(" + ",".join(source.PARAMETERS) + ")"
    arguments = encode(source.PARAMETERS, (bytes.fromhex(request_key[2:]), parameters))
    return "0x" + (keccak(text=signature)[:4] + arguments).hex()


def routed(rpc, contracts, report):
    """Submit an authentic signed payload through the provider the target Oracle is configured for.

    A retained report names the provider it was captured against. The Oracle admits a
    token's report only from its own configured provider, and the payload's signature
    does not cover that address.
    """
    token = report.asset.reference
    slot = key("ORACLE_PROVIDER_FOR_TOKEN", ("address", "address"), (contracts.oracle, token))
    data = keccak(text="getAddress(bytes32)")[:4] + slot
    raw = rpc("eth_call", [{"to": contracts.data_store, "data": "0x" + data.hex()}, "latest"])
    provider = "0x" + raw[-40:]
    # An unset key reads as zero; the keeper call would then revert without naming why.
    if not int(provider, 16):
        raise DataError("unsupported", f"target Oracle has no configured provider for {token}")
    return report.model_copy(update={"provider": provider})


def receipt(rpc, transaction_hash):
    deadline = time.monotonic() + RECEIPT_SECONDS
    while time.monotonic() < deadline:
        mined = rpc("eth_getTransactionReceipt", [transaction_hash])
        if mined is not None:
            return mined
        time.sleep(0.05)
    raise DataError("unavailable", "fork keeper receipt remains unknown")


def mined_block(rpc, mined):
    return rpc("eth_getBlockByNumber", [mined["blockNumber"], False])


def start_block(reader, timestamp):
    """Where a request's reports can begin; a fork ahead of the chain head waits at the head."""
    head = reader.pin_block("latest")
    return head if head.timestamp < timestamp else first_block_at(reader, timestamp, head=head)


@dataclass
class Keeper:
    """Settles every accepted request of a fork run once, from the origin chain's keepers.

    `forks` gives each chain's fork RPC, origin block and origin-chain reader;
    `project` selects the GMX state of a composed deployment, None when it has no GMX.
    """

    forks: ForkNodes
    output: Path
    project: Callable[[Any], Any] = staticmethod(lambda state: state)
    settled: set = field(default_factory=set)
    sources: dict = field(default_factory=dict)
    timelines: dict = field(default_factory=dict)

    def align(self, state, seconds: int) -> int:
        """Fork seconds to advance, at least `seconds`, so the next decision lands shortly
        before a usable keeper publication for every market the strategy trades."""
        state = self.project(state)
        if state is None:
            return seconds
        markets: dict = {}
        for allocation in state.config.allocations:
            market = state.config.target(allocation.name).market
            markets.setdefault(market.share_asset, market)
        if not markets:
            return seconds
        chain = next(iter(markets.values())).chain.chain_id
        now = int(self.forks.rpc(chain)("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
        target = now + seconds
        for _ in range(len(markets) * 4):
            aligned = target
            for market in markets.values():
                start = self.timeline(market).align(aligned)
                if start is None:
                    return seconds
                aligned = start
            if aligned == target:
                return target - now
            target = aligned
        return target - now

    def timeline(self, market):
        if market.share_asset not in self.timelines:
            reports = self.source(market)
            adjustments = {b.asset.reference: b.timestamp_adjustment for b in reports.destination}
            self.timelines[market.share_asset] = Timeline(
                reports.reader, reports.market, adjustments
            )
        return self.timelines[market.share_asset]

    def settle(self, state) -> tuple[dict, ...]:
        state = self.project(state)
        if state is None:
            return ()
        return tuple(
            self.settle_request(state, request)
            for request in state.requests
            if request.status == "accepted" and request.key not in self.settled
        )

    def settle_request(self, state, request):
        # One attempt per request: a source search that found nothing would only repeat.
        self.settled.add(request.key)
        target = state.config.target(request.allocation)
        chain = target.market.chain.chain_id
        rpc = self.forks.rpc(chain)
        reports = self.source(target.market)
        created = request.events[0].identity.block.timestamp
        now = int(rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
        reports.seek(start_block(self.forks.reader(chain), created))
        selected = reports.acquire(request, not_before=now)
        mined = execute(rpc, target, request, selected)
        row = {
            "request": request.key,
            "kind": request.kind,
            "chain": str(chain),
            "keeper": selected["oracle_source"]["from"],
            "source_transaction": selected["oracle_source"]["hash"],
            "source_block": selected["publication_block"]["number"],
            "report_times": sorted(r["observed_at"] for r in selected["reports"]),
            "transaction": mined["transactionHash"],
            "status": "mined" if mined["status"] == "0x1" else "reverted",
            "advanced_seconds": int(mined_block(rpc, mined)["timestamp"], 16) - now,
        }
        if mined["status"] == "0x1":
            row.update(settlement(mined, target, request))
        else:
            trace = rpc(
                "debug_traceTransaction", [mined["transactionHash"], {"tracer": "callTracer"}]
            )
            row["revert"] = trace.get("error") or trace.get("revertReason")
        return row

    def source(self, market):
        chain = market.chain.chain_id
        if market.share_asset not in self.sources:
            name = f"gmx-report-source-{len(self.sources) + 1}.json"
            origin = self.forks.origin(chain)
            # The fork routes the market to its local environment; its contracts, and the
            # keeper publications that source its reports, are the origin chain's.
            origin_market = market.model_copy(update={"chain": origin.chain})
            self.sources[market.share_asset] = FreshReports(
                self.forks.reader(chain),
                origin_market,
                origin,
                source.selections_for(origin_market),
                LIMITS,
                self.output / name,
            )
        return self.sources[market.share_asset]
