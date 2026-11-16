"""Drive a V3 pool's hourly closes on an identified fork with real swaps and fork time.

A crossing strategy (the Base RSI reference buys on a downward RSI-14 cross of 30) never
acts on a pinned fork, whose market does not move. This driver moves the configured
pool's own price with exact-input swaps from a funded fork account, and moves fork time
across hourly boundaries, so the strategy's price series, read by the SDK from that pool's
state at the fork's own boundary blocks, carries the move. The strategy, its price series
code and its observations are unchanged; nothing is injected.

Every swap and time step is retained in `market-drive.json` beside the journal, which the
supervised lane copies into the evidence and the case record.
"""

import json
import time
from datetime import UTC, datetime

from eth_abi import decode

from almanak.connectors.v3_pool_abi import calldata
from almanak.state import Journal

# A fork-only account with no key: the node impersonates it and funds it with native ETH.
DRIVER = "0x000000000000000000000000000000000000d71e"
# One close per step, each this fraction below the previous one; four 1% steps take the
# retained Base series (RSI-14 61.8 at the pinned anchor) to RSI-14 near 22 to 24.
HOURLY_DROP = 0.01
STEPS = 4
# Automine leaves the head at the last driven block until the next decision, while the
# next mined block takes the current time. Shortly before that decision one empty block
# moves the head on by REFRESH_STEP fork seconds, so the decision plans its swap deadline
# from a fresh head and the head stays inside the last driven hour.
LEAD_SECONDS = 60
REFRESH_STEP = 60
# The last driven boundary becomes available `availability_lag_seconds` (300) after it.
AVAILABLE_AFTER = 400
HOUR = 3600
WAIT_SECONDS = 1800
MAX_UINT = 2**256 - 1
WRAP_WEI = 500_000 * 10**18
SWAP_PARAMS = "(address,address,uint24,address,uint256,uint256,uint160)"
RECORD = "market-drive.json"


def drive_oversold_cross(node, journal, running, allocation="position"):
    """After the strategy's first scored decision, drive the closes down before its next."""
    path = journal.path.parent / RECORD
    if path.is_file():
        return  # driven once already; later calls (teardown) leave the market alone
    state = await_first_score(journal, running)
    target = state.config.target(allocation)
    capital = state.config.allocation(allocation).capital.asset
    record = drive(node, target, capital)
    record["first_score"] = state.memory.values.get("last_rsi")
    record["next_due_at"] = state.schedule.next_due_at.isoformat()
    path.write_text(json.dumps(record, indent=2) + "\n")
    pause_until(state.schedule.next_due_at.timestamp() - LEAD_SECONDS)
    head = latest(node.rpc)["timestamp"]
    record["steps"].append({**advance(node.rpc, head + REFRESH_STEP), "purpose": "fresh_head"})
    path.write_text(json.dumps(record, indent=2) + "\n")


def await_first_score(journal: Journal, running):
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        state = journal.load()
        if state.memory.values.get("last_rsi") is not None and state.schedule is not None:
            return state
        if running.poll() is not None:
            raise AssertionError("the coordinator exited before the strategy's first score")
        time.sleep(1)
    raise AssertionError("the strategy recorded no first score to drive a cross from")


def pause_until(epoch):
    while (remaining := epoch - datetime.now(UTC).timestamp()) > 0:
        time.sleep(min(remaining, 30))


def drive(node, target, capital):
    pool = target.pool
    tokens = (pool.currency0.reference, pool.currency1.reference)
    # Sell the priced asset (the currency that is not the capital) into the pool.
    sell = 0 if tokens[1] == capital.reference else 1
    rpc = node.rpc
    node._validate_origin()
    rpc("anvil_setBalance", [DRIVER, hex(WRAP_WEI * 2)])
    rpc("anvil_impersonateAccount", [DRIVER])
    try:
        head = latest(rpc)
        clock = head["timestamp"] + 1
        steps = fund(rpc, tokens[sell], target.contracts.router, clock)
        start = sqrt_price(rpc, pool.pool)
        boundary = (clock + 2) // HOUR * HOUR + HOUR
        steps.extend(hourly_sales(rpc, target, tokens, sell, start, boundary))
        # The last sale closes the boundary STEPS - 1 hours after the first.
        steps.append(advance(rpc, boundary + (STEPS - 1) * HOUR + AVAILABLE_AFTER))
    finally:
        rpc("anvil_stopImpersonatingAccount", [DRIVER])
    return {
        "driven": True,
        "account": DRIVER,
        "pool": pool.pool,
        "sold": tokens[sell],
        "hourly_drop": HOURLY_DROP,
        "steps_count": STEPS,
        "start_sqrt_price": str(start),
        "fork_head_before": head,
        "steps": steps,
    }


def fund(rpc, token, router, clock):
    """Wrap fork ETH into the priced asset and approve the router to sell it."""
    if token != "0x4200000000000000000000000000000000000006":
        raise AssertionError("the driver funds only a wrapped-native priced asset")
    return [
        send(rpc, token, calldata("deposit()", (), ()), clock, WRAP_WEI),
        send(rpc, token, approve(router), clock + 1),
    ]


def hourly_sales(rpc, target, tokens, sell, start, boundary):
    """One exact-input sale per hour, each closing its hour one step below the last."""
    steps = []
    for step in range(1, STEPS + 1):
        factor = (1 - HOURLY_DROP) ** (step / 2)
        # Selling currency0 lowers sqrtPrice; selling currency1 raises it.
        limit = int(start * factor) if sell == 0 else int(start / factor)
        swap_time = max(latest(rpc)["timestamp"] + 1, boundary - HOUR + 60)
        before = sqrt_price(rpc, target.pool.pool)
        data = swap(tokens, sell, target.pool.fee, MAX_UINT >> 128, limit)
        entry = send(rpc, target.contracts.router, data, swap_time)
        after = sqrt_price(rpc, target.pool.pool)
        assert after == limit, "the swap did not reach its price limit"
        steps.append(
            {
                **entry,
                "kind": "swap",
                "sqrt_price_before": str(before),
                "sqrt_price_after": str(after),
                "sqrt_price_limit": str(limit),
                "closes_boundary": boundary,
            }
        )
        boundary += HOUR
    return steps


def latest(rpc):
    block = rpc("eth_getBlockByNumber", ["latest", False])
    return {"number": int(block["number"], 16), "timestamp": int(block["timestamp"], 16)}


def sqrt_price(rpc, pool):
    raw = rpc("eth_call", [{"to": pool, "data": "0x3850c7bd"}, "latest"])
    return decode(("uint160",), bytes.fromhex(raw[2:66]))[0]


def approve(spender):
    return calldata("approve(address,uint256)", ("address", "uint256"), (spender, MAX_UINT))


def swap(tokens, sell, fee, amount, limit):
    params = (tokens[sell], tokens[1 - sell], fee, DRIVER, amount, 0, limit)
    return calldata(f"exactInputSingle({SWAP_PARAMS})", (SWAP_PARAMS,), (params,))


def send(rpc, to, data, timestamp, value=0):
    call = {"from": DRIVER, "to": to, "data": "0x" + data.hex(), "value": hex(value)}
    gas = int(rpc("eth_estimateGas", [call]), 16) + 100_000
    # A legacy price above the fork's base fee; the node's default tip can exceed it.
    price = 2 * int(rpc("eth_gasPrice"), 16) + 1
    rpc("evm_setNextBlockTimestamp", [timestamp])
    sent = rpc("eth_sendTransaction", [{**call, "gas": hex(gas), "gasPrice": hex(price)}])
    receipt = rpc.wait_receipt(sent)
    block = rpc("eth_getBlockByNumber", [receipt["blockNumber"], False])
    return {
        "kind": "transaction",
        "to": to,
        "selector": "0x" + data[:4].hex(),
        "tx_hash": receipt["transactionHash"],
        "block": int(receipt["blockNumber"], 16),
        "timestamp": int(block["timestamp"], 16),
        "gas_used": int(receipt["gasUsed"], 16),
    }


def advance(rpc, timestamp):
    before = latest(rpc)
    rpc("evm_setNextBlockTimestamp", [timestamp])
    rpc("evm_mine", [])
    after = latest(rpc)
    assert after["timestamp"] == timestamp
    return {"kind": "time", "from": before, "to": after}
