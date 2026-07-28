"""GMX V2 keeper-settlement correlation (VIB-3872 WI-2).

Correlates a submitted order's ``OrderCreated.key`` to its keeper-settlement
events and produces a protocol-neutral :class:`PerpSettlementVerdict` per watched
order. A GMX order is submitted in OUR ``createOrder`` tx; a separate ORDER_KEEPER
transaction later runs ``executeOrder``, emitting — in that one keeper tx —
``OrderExecuted`` **plus** ``PositionIncrease``/``PositionDecrease`` and
``PositionFeesCollected``. So the correlation is:

    order_key --(eth_getLogs on EventEmitter, indexed by key)--> keeper tx hash
             --(eth_getTransactionReceipt)--> keeper receipt
             --(extract_perp_fill, WI-1)--> measured PerpFillData

Every read goes through the strategy's own gateway
(``get_gateway_web3``) — no direct RPC, no new gateway proto (gateway-boundary
rule). ``eth_getLogs`` and ``eth_getTransactionReceipt`` are both on the gateway
allowlist. The read pattern mirrors ``accounting/capital_flows.py`` (VIB-5866).

Fail-closed / Empty≠Zero throughout: a read that cannot be measured NEVER
fabricates a terminal venue outcome. Horizon expiry books ``UNMEASURED`` with a
reason, never a fabricated ``CANCELLED`` (ratified Q2).
"""

from __future__ import annotations

import logging
from typing import Any, cast

from eth_typing import HexStr
from web3 import Web3

from almanak.connectors._strategy_base.runner_hook_registry import (
    PerpSettlementState,
    PerpSettlementVerdict,
    PerpSettlementWatchEntry,
)
from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.connectors.gmx_v2.receipt_parser import TOPIC_TO_EVENT, GMXv2ReceiptParser
from almanak.connectors.gmx_v2.teardown_reads import read_pending_orders
from almanak.framework.web3.gateway_provider import get_gateway_web3

logger = logging.getLogger(__name__)

# GMX order-outcome event names indexed by the order key at topic[2].
_ORDER_OUTCOME_EVENTS: dict[str, PerpSettlementState] = {
    "OrderExecuted": PerpSettlementState.EXECUTED,
    "OrderCancelled": PerpSettlementState.CANCELLED,
    "OrderFrozen": PerpSettlementState.FROZEN,
}

# Connector-owned watch horizon (reuses the AsyncSettlementPolicy shape). GMX
# keeper settlement is normally seconds; this is the generous non-blocking upper
# bound past which an un-settled order books UNMEASURED-expired (never a
# fabricated CANCELLED — ratified Q2).
PERP_SETTLEMENT_TIMEOUT_SECONDS = 1800
PERP_SETTLEMENT_POLL_INTERVAL_SECONDS = 5


def resolve_event_emitter(chain: str) -> str | None:
    """Resolve the central GMX EventEmitter address for ``chain`` (or ``None``)."""
    return GMX_V2.get(str(chain or "").lower(), {}).get("event_emitter")


def _normalize_order_key(order_key: str) -> str | None:
    """Return a lowercased ``0x``+64-hex order key, or ``None`` if malformed."""
    text = str(order_key or "").lower()
    if not text.startswith("0x"):
        text = "0x" + text
    body = text[2:]
    if len(body) != 64:
        return None
    try:
        int(body, 16)
    except ValueError:
        return None
    return text


def _horizon_expired(entry: PerpSettlementWatchEntry, timeout_seconds: int) -> bool:
    """True iff the reconciler-supplied elapsed clock has passed the watch horizon.

    ``seconds_since_submission is None`` means the reconciler could not evaluate
    elapsed time yet → NOT expired (never expire an entry we cannot age).
    """
    elapsed = entry.seconds_since_submission
    return isinstance(elapsed, int) and elapsed >= timeout_seconds


def _log_event_name(log: Any) -> str | None:
    topics = log.get("topics") or []
    if len(topics) < 2:
        return None
    topic1 = topics[1]
    topic1 = "0x" + topic1.hex() if isinstance(topic1, bytes | bytearray) else str(topic1)
    return TOPIC_TO_EVENT.get(topic1.lower())


def _tx_hash_hex(value: Any) -> str | None:
    if isinstance(value, bytes | bytearray):
        return "0x" + value.hex()
    text = str(value or "")
    if not text:
        return None
    return text if text.startswith("0x") else "0x" + text


def _pending_order_keys(pending: Any) -> set[str]:
    keys = {str(key).lower() for key in pending.order_keys}
    keys.update(str(order.order_key).lower() for order in pending.orders if getattr(order, "order_key", None))
    return keys


def resolve_perp_settlements(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    watch_entries: tuple[PerpSettlementWatchEntry, ...],
    timeout_seconds: int = PERP_SETTLEMENT_TIMEOUT_SECONDS,
) -> tuple[PerpSettlementVerdict, ...]:
    """Resolve one settlement verdict per watched order (order-independent)."""
    if not watch_entries:
        return ()

    emitter = resolve_event_emitter(chain)
    web3: Web3 | None = None
    setup_error: str | None = None
    if emitter is None:
        setup_error = f"no GMX EventEmitter registered for chain {chain!r}"
    elif gateway_client is None:
        setup_error = "no gateway client to read GMX keeper settlement"
    else:
        try:
            web3 = get_gateway_web3(gateway_client, chain)
        except Exception as exc:  # noqa: BLE001 — unmeasured setup ⇒ fail-closed verdicts
            setup_error = f"gateway web3 handle unavailable: {exc}"

    # Pass chain so the parser can scale executionPrice → USD-per-token entry/exit
    # price via the market's index-token decimals (VIB-6110).
    parser = GMXv2ReceiptParser(chain=chain)
    # The account pending-order set is identical for every entry in this batch, so
    # read it AT MOST ONCE per invocation — lazily, only if some entry lacks an
    # outcome log. Every no-outcome entry then evaluates the same cached result,
    # preserving exact per-entry semantics (a failed/truncated read still yields
    # each entry's own UNMEASURED verdict).
    pending_cache = _PendingOrdersCache(gateway_client=gateway_client, chain=chain, wallet_address=wallet_address)
    verdicts: list[PerpSettlementVerdict] = []
    for entry in watch_entries:
        if web3 is None or emitter is None:
            verdicts.append(_setup_unmeasured(entry, setup_error or "gateway read unavailable", timeout_seconds))
            continue
        verdicts.append(
            _resolve_one(
                web3=web3,
                emitter=emitter,
                parser=parser,
                pending_cache=pending_cache,
                entry=entry,
                timeout_seconds=timeout_seconds,
            )
        )
    return tuple(verdicts)


class _PendingOrdersCache:
    """Reads the account's pending-order set at most once per settlement batch.

    N watched orders on one deployment share one pending-order set; without this
    the reconciler would issue N identical gateway reads per tick. The read is
    lazy (skipped entirely when every entry already has a terminal outcome log).
    """

    def __init__(self, *, gateway_client: Any, chain: str, wallet_address: str) -> None:
        self._gateway_client = gateway_client
        self._chain = chain
        self._wallet_address = wallet_address
        self._loaded = False
        self._value: Any = None

    def get(self) -> Any:
        if not self._loaded:
            self._value = read_pending_orders(self._gateway_client, self._chain, self._wallet_address)
            self._loaded = True
        return self._value


def _setup_unmeasured(entry: PerpSettlementWatchEntry, reason: str, timeout_seconds: int) -> PerpSettlementVerdict:
    return PerpSettlementVerdict(
        order_key=entry.order_key,
        state=PerpSettlementState.UNMEASURED,
        terminal=_horizon_expired(entry, timeout_seconds),
        unavailable_reason=reason,
    )


def _resolve_one(
    *,
    web3: Web3,
    emitter: str,
    parser: GMXv2ReceiptParser,
    pending_cache: _PendingOrdersCache,
    entry: PerpSettlementWatchEntry,
    timeout_seconds: int,
) -> PerpSettlementVerdict:
    order_key = _normalize_order_key(entry.order_key)
    if order_key is None:
        return PerpSettlementVerdict(
            order_key=entry.order_key,
            state=PerpSettlementState.UNMEASURED,
            terminal=True,
            unavailable_reason=f"watch entry carried a malformed order key: {entry.order_key!r}",
        )

    # 1. One indexed-topic scan for every order-outcome event keyed by this order.
    try:
        logs = web3.eth.get_logs(
            {
                "address": Web3.to_checksum_address(emitter),
                "fromBlock": int(entry.submission_block),
                "toBlock": "latest",
                "topics": [None, None, order_key],
            }
        )
    except Exception as exc:  # noqa: BLE001 — read failure ⇒ unmeasured, never fabricated
        logger.debug("GMX keeper-event eth_getLogs failed for %s: %s", order_key, exc, exc_info=True)
        return _unresolved_after_read_failure(entry, timeout_seconds, reason=f"keeper-event eth_getLogs failed: {exc}")

    outcome_log = _first_outcome_log(logs)
    if outcome_log is not None:
        return _verdict_from_outcome(
            web3=web3,
            parser=parser,
            entry=entry,
            order_key=order_key,
            outcome=outcome_log,
            timeout_seconds=timeout_seconds,
        )

    # 2. No terminal outcome yet — distinguish still-pending vs gone-uncorrelated
    #    vs horizon-expired, all measured against the account's pending-order set
    #    (read once per batch via the shared cache).
    return _verdict_without_outcome(
        pending=pending_cache.get(),
        entry=entry,
        order_key=order_key,
        timeout_seconds=timeout_seconds,
    )


def _first_outcome_log(logs: Any) -> tuple[str, Any] | None:
    """Return ``(event_name, log)`` for the first order-outcome event, preferring
    a definitive terminal outcome. Executed wins over cancelled/frozen if both
    somehow appear (an executed order is the strongest measured outcome)."""
    found: dict[str, Any] = {}
    for log in logs:
        name = _log_event_name(log)
        if name in _ORDER_OUTCOME_EVENTS and name not in found:
            found[name] = log
    for name in ("OrderExecuted", "OrderCancelled", "OrderFrozen"):
        if name in found:
            return name, found[name]
    return None


def _verdict_from_outcome(
    *,
    web3: Web3,
    parser: GMXv2ReceiptParser,
    entry: PerpSettlementWatchEntry,
    order_key: str,
    outcome: tuple[str, Any],
    timeout_seconds: int,
) -> PerpSettlementVerdict:
    name, log = outcome
    state = _ORDER_OUTCOME_EVENTS[name]
    keeper_tx = _tx_hash_hex(log.get("transactionHash"))
    if keeper_tx is None:
        return PerpSettlementVerdict(
            order_key=entry.order_key,
            state=PerpSettlementState.UNMEASURED,
            terminal=True,
            unavailable_reason=f"{name} log carried no transaction hash to fetch the keeper receipt",
        )

    try:
        receipt = web3.eth.get_transaction_receipt(HexStr(keeper_tx))
    except Exception as exc:  # noqa: BLE001 — receipt read failed ⇒ unmeasured (retry next tick)
        logger.debug("GMX keeper receipt fetch failed for %s: %s", keeper_tx, exc, exc_info=True)
        return PerpSettlementVerdict(
            order_key=entry.order_key,
            state=PerpSettlementState.UNMEASURED,
            terminal=False,
            unavailable_reason=f"{name} observed but keeper receipt {keeper_tx} was unreadable: {exc}",
        )

    # VIB-6110: correlate to the WATCHED order. A keeper tx may batch several
    # orders across markets; an uncorrelated extract would return a sibling
    # order's market, direction and execution price for this settlement.
    fill = parser.extract_perp_fill(cast("dict[str, Any]", receipt), order_key=order_key)

    if state is PerpSettlementState.EXECUTED:
        if fill is None:
            # OrderExecuted without a decodable position event should not happen
            # for a perp increase/decrease; fail-closed rather than assert a fill.
            #
            # This branch must respect the watch horizon (VIB-6110). Correlating
            # the extraction to the watched order key tightened the condition that
            # reaches here: previously ANY decodable position event satisfied it,
            # now only an orderKey-matching one does. A GMX payload-key rename, an
            # ADL/liquidation-shaped decrease, or a forged-emitter receipt would
            # otherwise pin the entry non-terminal forever — one eth_getLogs plus
            # one eth_getTransactionReceipt per entry per tick, with the
            # PERP_SETTLEMENT row never booked. Expire like every other unmeasured
            # branch in this module so it eventually books a terminal UNMEASURED
            # carrying the reason string below.
            return PerpSettlementVerdict(
                order_key=entry.order_key,
                state=PerpSettlementState.UNMEASURED,
                terminal=_horizon_expired(entry, timeout_seconds),
                unavailable_reason=(
                    f"OrderExecuted observed in {keeper_tx} but no PositionIncrease/Decrease "
                    f"for order {order_key} was decodable — fill economics unmeasured"
                ),
            )
        return PerpSettlementVerdict(
            order_key=entry.order_key,
            state=PerpSettlementState.EXECUTED,
            terminal=True,
            fill_data=fill,
            keeper_tx_hash=keeper_tx,
        )

    # CANCELLED / FROZEN: fill_data only when the venue tx yielded measurable
    # money movement (e.g. a collateral-returning PositionDecrease); otherwise
    # None. Fill-economics fields on such a fill stay None per Empty≠Zero.
    return PerpSettlementVerdict(
        order_key=entry.order_key,
        state=state,
        terminal=True,
        fill_data=fill,
        keeper_tx_hash=keeper_tx,
    )


def _verdict_without_outcome(
    *,
    pending: Any,
    entry: PerpSettlementWatchEntry,
    order_key: str,
    timeout_seconds: int,
) -> PerpSettlementVerdict:
    if not pending.ok:
        return _unresolved_after_read_failure(
            entry,
            timeout_seconds,
            reason=f"pending-order read was unmeasured: {pending.error or 'unknown'}",
        )

    still_pending = order_key in _pending_order_keys(pending)
    if still_pending:
        if _horizon_expired(entry, timeout_seconds):
            return PerpSettlementVerdict(
                order_key=entry.order_key,
                state=PerpSettlementState.UNMEASURED,
                terminal=True,
                unavailable_reason="watch horizon expired before keeper events were observed",
            )
        return PerpSettlementVerdict(
            order_key=entry.order_key,
            state=PerpSettlementState.PENDING,
            terminal=False,
        )

    # Order is not in the pending set. If the pending read was truncated we cannot
    # prove absence — treat as unmeasured, not "gone".
    if getattr(pending, "truncated", False):
        return _unresolved_after_read_failure(
            entry,
            timeout_seconds,
            reason="pending-order set was truncated; order absence was not measurable",
        )

    return PerpSettlementVerdict(
        order_key=entry.order_key,
        state=PerpSettlementState.NOT_FOUND_UNCORRELATED,
        terminal=True,
        unavailable_reason=(
            "order left the pending set with no correlated keeper outcome event in "
            "[submission_block, latest] — economics could not be attributed"
        ),
    )


def _unresolved_after_read_failure(
    entry: PerpSettlementWatchEntry, timeout_seconds: int, *, reason: str
) -> PerpSettlementVerdict:
    """An unmeasured read: terminal only when the watch horizon has expired
    (loud, book it) — otherwise non-terminal so the reconciler retries."""
    return PerpSettlementVerdict(
        order_key=entry.order_key,
        state=PerpSettlementState.UNMEASURED,
        terminal=_horizon_expired(entry, timeout_seconds),
        unavailable_reason=reason,
    )


__all__ = [
    "PERP_SETTLEMENT_POLL_INTERVAL_SECONDS",
    "PERP_SETTLEMENT_TIMEOUT_SECONDS",
    "resolve_event_emitter",
    "resolve_perp_settlements",
]
