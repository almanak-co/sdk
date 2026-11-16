"""External GMX liquidation scans, receipts and reconciliation on synthetic evidence.

The retained-evidence modules replay a captured Arbitrum liquidation; these cases pin
the same refusals offline with invented logs, traces and stand-in runtime state.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import (
    liquidation_binding,
    liquidation_reader,
    liquidation_receipts,
    liquidation_reconciliation,
)
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.liquidation_types import LiquidationOutcome, scope
from almanak.connectors.gmx_v2.position_reader import position_key
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests import gmx_synthetic_frames as frames
from tests.gmx_event_logs import event_log, word

TARGET = frames.perpetual_target()
MARKET = TARGET.market
EMITTER = MARKET.contracts.event_emitter
EXECUTOR = MARKET.contracts.decrease_order_executor
HANDLER = liquidation_receipts.handler(TARGET)
FUND = TARGET.fund.address
ACCOUNT = word(FUND)
POSITION = "0x" + position_key(TARGET).hex()
ORDER = "0x" + "0d" * 32
SIZE = 5 * 10**30
TOKENS = 10**18


def block(number, tag="ab"):
    return frames.block(MARKET, number=number, timestamp=frames.TIMESTAMP + number).model_copy(
        update={"hash": "0x" + tag * 32}
    )


class Reader:
    """Canonical blocks by number, with optional replaced hashes and a moving head."""

    def __init__(self, head=200, replaced=(), responses=None):
        self.chain = MARKET.chain
        self.head, self.replaced = head, set(replaced)
        self.responses = responses or {}
        self.calls = []

    def pin_block(self, number):
        if number == "latest":
            number = self.head
        return block(number, "cd" if number in self.replaced else "ab")

    def _rpc(self, method, args, allow_none=False):
        self.calls.append((method, args))
        value = self.responses[method]
        return value(args) if callable(value) else value


def position_event(name="PositionDecrease", *, number=10, index=0, order_type=7, **changes):
    values = {
        "address": {"account": FUND},
        "bytes32": {"positionKey": POSITION, "orderKey": ORDER},
        "uint256": {"orderType": order_type},
    }
    fields = {"topics": (ACCOUNT,), "transaction": "0x" + f"{number:02x}{index:02x}" * 16}
    fields |= changes
    return event_log(
        name,
        emitter=EMITTER,
        sender=EXECUTOR,
        values=values,
        number=number,
        index=index,
        **fields,
    )


def decoded(log):
    return decode_event(log, emitter=EMITTER)


# Scan pages


def test_logs_orders_events_and_queries_only_the_fund_position_events():
    rows = [position_event(number=12, index=1), position_event("PositionIncrease", number=11)]
    reader = Reader(responses={"eth_getLogs": rows})
    events = liquidation_reader.logs(reader, TARGET, block(10), block(20))
    assert [(e.block_number, e.name) for e in events] == [
        (11, "PositionIncrease"),
        (12, "PositionDecrease"),
    ]
    query = reader.calls[0][1][0]
    assert query["topics"][2] == ACCOUNT and query["fromBlock"] == hex(10)


@pytest.mark.parametrize(
    "row",
    [
        position_event("OrderExecuted"),
        position_event(topics=(word(ZERO_ADDRESS),)),
        position_event(number=21),
        event_log(
            "PositionDecrease",
            emitter=EMITTER,
            sender=EXECUTOR,
            topics=(ACCOUNT,),
            values={"address": {"account": ZERO_ADDRESS}},
            number=12,
        ),
        {**position_event(), "address": ZERO_ADDRESS},
    ],
    ids=["name", "topic", "range", "account", "emitter"],
)
def test_logs_refuses_events_outside_the_query(row):
    reader = Reader(responses={"eth_getLogs": [row]})
    with pytest.raises(DataError, match="outside its query"):
        liquidation_reader.logs(reader, TARGET, block(10), block(20))


def test_logs_refuses_duplicate_event_identities():
    row = position_event(number=12)
    reader = Reader(responses={"eth_getLogs": [row, row]})
    with pytest.raises(DataError, match="duplicate event identities"):
        liquidation_reader.logs(reader, TARGET, block(10), block(20))


def outcome(target=TARGET, *, opened=None, tx="0x" + "01" * 32):
    """A retained outcome; `opened` makes it the owned opening transition at that block."""
    return SimpleNamespace(
        tx_hash=tx,
        before=SimpleNamespace(owned=SimpleNamespace(position=None if opened else object())),
        after=SimpleNamespace(
            target=target,
            owned=SimpleNamespace(position=object()),
            block=block(opened or 5),
        ),
    )


def scan_state(outcomes=(), scans=(), span=10, requests=()):
    allocation = SimpleNamespace(maximum_event_block_span=span)
    return SimpleNamespace(
        outcomes=tuple(outcomes),
        external_scans=tuple(scans),
        requests=tuple(requests),
        config=SimpleNamespace(allocation=lambda name: allocation),
        owned=(),
        pending=None,
    )


def test_bounds_starts_at_the_owned_opening_and_follows_retained_pages():
    reader = Reader(head=200)
    state = scan_state([outcome(opened=50)])
    assert liquidation_reader.bounds(reader, state, TARGET, None) == (block(50), block(59))
    page = SimpleNamespace(target=TARGET, end=block(59))
    later = scan_state([outcome(opened=50)], [page])
    assert liquidation_reader.bounds(reader, later, TARGET, None) == (block(60), block(69))
    assert liquidation_reader.bounds(Reader(head=55), later, TARGET, None) is None
    assert liquidation_reader.bounds(Reader(head=62), later, TARGET, None) == (block(60), block(62))


def test_bounds_refuses_missing_opening_changed_cursor_and_reorganization():
    with pytest.raises(DataError, match="opening evidence"):
        liquidation_reader.bounds(Reader(), scan_state([outcome()]), TARGET, None)
    state = scan_state([outcome(opened=50)])
    moved = SimpleNamespace(start=block(51), end=block(55))
    with pytest.raises(DataError, match="retained bounded cursor"):
        liquidation_reader.bounds(Reader(), state, TARGET, moved)
    wide = SimpleNamespace(start=block(50), end=block(60))
    with pytest.raises(DataError, match="retained bounded cursor"):
        liquidation_reader.bounds(Reader(), state, TARGET, wide)
    reference = SimpleNamespace(start=block(50), end=block(55))
    assert liquidation_reader.bounds(Reader(), state, TARGET, reference) == (block(50), block(55))
    with pytest.raises(DataError, match="no longer canonical"):
        liquidation_reader.bounds(Reader(replaced={55}), state, TARGET, reference)


def test_selected_liquidation_ignores_recorded_and_unrelated_events():
    recorded = decoded(position_event(number=12))
    state = scan_state([outcome(tx=recorded.transaction_hash)])
    assert liquidation_reader.selected_liquidation(state, TARGET, (recorded,)) == (None, False)
    unrelated = decoded(
        event_log(
            "PositionDecrease",
            emitter=EMITTER,
            sender=EXECUTOR,
            topics=(ACCOUNT,),
            values={"bytes32": {"positionKey": "0x" + "00" * 32}},
            number=12,
        )
    )
    assert liquidation_reader.selected_liquidation(scan_state(), TARGET, (unrelated,)) == (
        None,
        False,
    )


def test_selected_liquidation_returns_the_first_unrecorded_liquidation():
    first, later = decoded(position_event(number=12)), decoded(position_event(number=13))
    assert liquidation_reader.selected_liquidation(scan_state(), TARGET, (first, later)) == (
        first,
        False,
    )


def test_selected_liquidation_waits_for_an_unresolved_sdk_request():
    change = decoded(position_event(number=12, order_type=4))
    request = SimpleNamespace(unresolved=True, key=ORDER, fund=TARGET.fund)
    state = scan_state(requests=[request])
    assert liquidation_reader.selected_liquidation(state, TARGET, (change,)) == (None, True)
    settled = scan_state(requests=[SimpleNamespace(unresolved=False, key=ORDER, fund=TARGET.fund)])
    with pytest.raises(DataError, match="without retained SDK request authority"):
        liquidation_reader.selected_liquidation(settled, TARGET, (change,))
    increase = decoded(position_event("PositionIncrease", number=12))
    with pytest.raises(DataError, match="without retained SDK request authority"):
        liquidation_reader.selected_liquidation(scan_state(), TARGET, (increase,))


def test_selected_liquidation_refuses_another_change_in_the_same_block():
    first = decoded(position_event(number=12))
    same = decoded(position_event(number=12, index=4, order_type=4))
    with pytest.raises(DataError, match="shares a block"):
        liquidation_reader.selected_liquidation(scan_state(), TARGET, (first, same))


# Binding: page rotation and audit


def owned(target=TARGET, position=True):
    return Ownership.model_construct(
        target=target, inventory=(), position=object() if position else None
    )


def test_reconcile_rotates_after_the_last_scanned_owned_target(monkeypatch):
    other = TARGET.model_copy(update={"allocation": "second"})
    seen = []

    def page(backend, state, target, reference):
        seen.append(target.allocation)
        return "page" if target == TARGET else None

    monkeypatch.setattr(liquidation_reader, "page", page)
    state = scan_state(scans=[SimpleNamespace(target=TARGET)])
    state.owned = (owned(), owned(other), owned(position=False), SimpleNamespace(target=TARGET))
    assert liquidation_binding.reconcile(None, state) == "page"
    assert seen == ["second", TARGET.allocation]
    seen.clear()
    state.owned = (owned(other),)
    assert liquidation_binding.reconcile(None, state) is None


def test_reconcile_with_a_reference_scans_only_an_owned_reference_target(monkeypatch):
    monkeypatch.setattr(liquidation_reader, "page", lambda b, s, t, r: (t, r))
    state = scan_state()
    state.owned = (owned(),)
    reference = SimpleNamespace(target=TARGET)
    assert liquidation_binding.reconcile(None, state, reference=reference) == (TARGET, reference)
    other = SimpleNamespace(target=TARGET.model_copy(update={"allocation": "second"}))
    with pytest.raises(DataError, match="does not concern an owned position"):
        liquidation_binding.reconcile(None, state, reference=other)


def retained_page(events=("event",), result=None, chain=MARKET.chain):
    return SimpleNamespace(
        scope=SimpleNamespace(chain=chain),
        start=block(10),
        end=block(20),
        target=TARGET,
        events=events,
        outcome=result,
    )


def test_audit_rechecks_canonical_bounds_events_and_outcome(monkeypatch):
    monkeypatch.setattr(liquidation_reader, "logs", lambda reader, target, start, end: ("event",))
    receipt, trace = {"status": "0x1"}, {"type": "CALL"}
    result = SimpleNamespace(tx_hash="0x" + "01" * 32, receipt=receipt, trace=trace)
    reader = Reader(
        responses={"eth_getTransactionReceipt": receipt, "debug_traceTransaction": trace}
    )
    elsewhere = retained_page(chain=frames.perpetual_target("avalanche").market.chain)
    state = SimpleNamespace(external_scans=(elsewhere, retained_page(result=result)))
    liquidation_binding.audit(SimpleNamespace(reader=reader), state)
    assert [method for method, _ in reader.calls] == [
        "eth_getTransactionReceipt",
        "debug_traceTransaction",
    ]
    changed = Reader(responses={"eth_getTransactionReceipt": receipt, "debug_traceTransaction": {}})
    with pytest.raises(DataError, match="receipt or trace has changed"):
        liquidation_binding.audit(SimpleNamespace(reader=changed), state)


def test_audit_refuses_reorganized_bounds_or_events(monkeypatch):
    monkeypatch.setattr(liquidation_reader, "logs", lambda reader, target, start, end: ())
    state = SimpleNamespace(external_scans=(retained_page(),))
    with pytest.raises(DataError, match="bounds are no longer canonical"):
        liquidation_binding.audit(SimpleNamespace(reader=Reader(replaced={20})), state)
    with pytest.raises(DataError, match="events are no longer canonical"):
        liquidation_binding.audit(SimpleNamespace(reader=Reader()), state)


# Receipts


def order_event(name, index, **changes):
    values = {
        "address": {
            "account": FUND,
            "receiver": FUND,
            "cancellationReceiver": FUND,
            "callbackContract": ZERO_ADDRESS,
            "uiFeeReceiver": ZERO_ADDRESS,
            "market": MARKET.share_asset.reference,
            "initialCollateralToken": TARGET.collateral_asset.reference,
        },
        "bytes32": {"key": ORDER},
        "uint256": {
            "orderType": 7,
            "executionFee": 0,
            "initialCollateralDeltaAmount": 0,
            "sizeDeltaUsd": SIZE,
            "decreasePositionSwapType": 1,
        },
        "bool": {"isLong": TARGET.side == "long", "shouldUnwrapNativeToken": True},
    }
    fields = {"topics": (ORDER, ACCOUNT), "arrays": {"address": {"swapPath": ()}}} | changes
    return event_log(name, emitter=EMITTER, sender=HANDLER, values=values, index=index, **fields)


def closed_event(index=2, **changes):
    values = {
        "address": {
            "account": FUND,
            "market": MARKET.share_asset.reference,
            "collateralToken": TARGET.collateral_asset.reference,
        },
        "bytes32": {"positionKey": POSITION, "orderKey": ORDER},
        "bool": {"isLong": TARGET.side == "long"},
        "uint256": {
            "orderType": 7,
            "sizeInUsd": 0,
            "sizeInTokens": 0,
            "collateralAmount": 0,
            "sizeDeltaUsd": SIZE,
            "sizeDeltaInTokens": TOKENS,
        },
    }
    fields = {"topics": (ACCOUNT,), "block_hash": block(20).hash, "number": 20} | changes
    return event_log(
        "PositionDecrease", emitter=EMITTER, sender=EXECUTOR, values=values, index=index, **fields
    )


PREIMAGE = SimpleNamespace(
    owned=SimpleNamespace(
        position=SimpleNamespace(size_usd_e30=SIZE, index_amount=SimpleNamespace(amount=TOKENS))
    )
)


def test_validate_events_returns_the_ordered_liquidation_events():
    receipt = {
        "logs": [order_event("OrderCreated", 1), closed_event(), order_event("OrderExecuted", 3)]
    }
    created, executed, decreased = liquidation_receipts.validate_events(receipt, TARGET, PREIMAGE)
    assert (created.name, executed.name, decreased.name) == (
        "OrderCreated",
        "OrderExecuted",
        "PositionDecrease",
    )
    assert decreased.value("bytes32", "orderKey") == ORDER


@pytest.mark.parametrize(
    "logs, message",
    [
        (
            [
                order_event("OrderCreated", 1, topics=("0x" + "0e" * 32, ACCOUNT)),
                closed_event(),
                order_event("OrderExecuted", 3),
            ],
            "differ from fund and order key",
        ),
        (
            [
                order_event("OrderCreated", 1),
                closed_event(index=4),
                order_event("OrderExecuted", 3),
            ],
            "ambiguous execution order",
        ),
        (
            [
                order_event("OrderCreated", 1, arrays={"address": {"swapPath": (FUND,)}}),
                closed_event(),
                order_event("OrderExecuted", 3, arrays={"address": {"swapPath": (FUND,)}}),
            ],
            "external swap path",
        ),
    ],
    ids=["key", "order", "swap-path"],
)
def test_validate_events_refuses_mismatched_liquidation_events(logs, message):
    with pytest.raises(DataError, match=message):
        liquidation_receipts.validate_events({"logs": logs}, TARGET, PREIMAGE)


def call(sender, receiver, value=0, kind="CALL", calls=(), error=None):
    frame = {
        "type": kind,
        "from": sender,
        "to": receiver,
        "value": hex(value),
        "calls": list(calls),
    }
    if error:
        frame["error"] = error
    return frame


SHARE = MARKET.share_asset.reference
CALLER = "0x" + "c1" * 20


def receipt_for(**changes):
    return {"status": "0x1", "from": CALLER, "to": HANDLER} | changes


def test_native_received_sums_successful_market_transfers_to_the_fund():
    trace = call(
        CALLER,
        HANDLER,
        calls=[
            call(HANDLER, SHARE, calls=[call(SHARE, FUND, 7)]),
            call(SHARE, FUND, 5),
            call(SHARE, FUND, 100, error="execution reverted"),
            call(SHARE, CALLER, 9),
            call(SHARE, FUND, 11, kind="STATICCALL"),
        ],
    )
    assert liquidation_receipts.native_received(trace, receipt_for(), TARGET) == 12


@pytest.mark.parametrize(
    "trace, receipt",
    [
        (call(CALLER, HANDLER, calls=[call(HANDLER, SHARE)]), receipt_for(status="0x0")),
        (call(CALLER, HANDLER, calls=[call(HANDLER, SHARE)], error="out of gas"), receipt_for()),
        (call(CALLER, HANDLER, kind="DELEGATECALL", calls=[call(HANDLER, SHARE)]), receipt_for()),
        (call(CALLER, HANDLER), receipt_for()),
        (call(CALLER, SHARE, calls=[call(HANDLER, SHARE)]), receipt_for()),
        (call(FUND, HANDLER, calls=[call(HANDLER, SHARE)]), receipt_for()),
        (call(CALLER, HANDLER, calls=[call(HANDLER, SHARE)]), receipt_for(to=SHARE)),
    ],
    ids=["status", "error", "type", "no-calls", "to", "from", "receipt-to"],
)
def test_native_received_requires_the_successful_handler_call(trace, receipt):
    with pytest.raises(DataError, match="successful handler call trace"):
        liquidation_receipts.native_received(trace, receipt, TARGET)


def test_native_calls_refuse_fund_debits_and_unexpected_counterparties():
    with pytest.raises(DataError, match="debited fund native inventory"):
        liquidation_receipts.native_calls(call(FUND, SHARE, 1), TARGET)
    with pytest.raises(DataError, match="lacks its market counterparty"):
        liquidation_receipts.native_calls(call(CALLER, FUND, 1), TARGET)
    with pytest.raises(DataError, match="lacks its market counterparty"):
        liquidation_receipts.native_calls(call(SHARE, FUND, 1, kind="CREATE"), TARGET)
    assert liquidation_receipts.native_calls(call(FUND, SHARE, 0), TARGET) == 0


# Reconciliation


def test_validate_native_measurement_requires_exact_receipt_attribution():
    native = frames.exchange_target().fund.chain.chain_id
    from almanak.gateway.evm import native_asset
    from almanak.models import AssetAmount

    asset = native_asset(native)
    other = AssetAmount(asset=TARGET.collateral_asset, amount=1)
    before = SimpleNamespace(wallets=(other, AssetAmount(asset=asset, amount=10)))
    after = SimpleNamespace(wallets=(other, AssetAmount(asset=asset, amount=15)))
    liquidation_reconciliation.validate_native_measurement(before, after, 5)
    with pytest.raises(DataError, match="exact receipt-call attribution"):
        liquidation_reconciliation.validate_native_measurement(before, after, 4)


class Snapshot(SimpleNamespace):
    def model_copy(self, update):
        return Snapshot(**{**vars(self), **update})


def reconciliation_state(position=True, opened=10):
    held = owned(position=position)
    before = Snapshot(
        target=TARGET, owned=held, block=block(opened), allowance=3, wallets=(), effective_at=None
    )
    return SimpleNamespace(
        owned=(held,),
        outcomes=(SimpleNamespace(after=before),),
    ), before


@pytest.fixture
def stubbed(monkeypatch):
    """Replace each separately tested receipt, snapshot and accounting step."""
    event = decoded(closed_event())
    created = decoded(order_event("OrderCreated", 1))
    calls = []
    monkeypatch.setattr(liquidation_reconciliation, "verify_handler", lambda r, t, b: HANDLER)
    monkeypatch.setattr(
        liquidation_receipts, "validate_events", lambda r, t, b: (created, "executed", event)
    )
    monkeypatch.setattr(liquidation_receipts, "fee_events", lambda r, t, k: ("fees", k))
    monkeypatch.setattr(liquidation_receipts, "native_received", lambda tr, r, t: 5)
    monkeypatch.setattr(
        liquidation_reconciliation, "validate_native_measurement", lambda *a: calls.append(a)
    )
    monkeypatch.setattr(liquidation_reconciliation, "cash_effects", lambda *a: ("cash",))
    monkeypatch.setattr(
        liquidation_receipts, "accrue_funding", lambda b, a, r: (("claim",), ("funding issue",))
    )
    monkeypatch.setattr(
        liquidation_reconciliation,
        "changed_ownership",
        lambda before, assets, _: Snapshot(assets=assets),
    )
    monkeypatch.setattr(
        liquidation_reconciliation, "validate_wallets", lambda *a: ("wallet issue",)
    )
    monkeypatch.setattr(liquidation_reconciliation, "LiquidationOutcome", SimpleNamespace)
    after = Snapshot(position=None, allowance=3, effective_at="later", owned=None)
    monkeypatch.setattr(liquidation_reconciliation, "snapshot", lambda *a: after)
    return SimpleNamespace(event=event, after=after, calls=calls)


def backend(receipt, trace=None):
    return SimpleNamespace(
        reader=Reader(
            responses={"eth_getTransactionReceipt": receipt, "debug_traceTransaction": trace or {}}
        )
    )


def test_reconcile_attributes_a_proved_liquidation_to_the_owned_position(stubbed):
    state, before = reconciliation_state()
    receipt = {"blockHash": block(20).hash}
    result = liquidation_reconciliation.reconcile(
        backend(receipt), state, TARGET, stubbed.event, block(20)
    )
    assert (
        result.identity.reference == f"{stubbed.event.transaction_hash}:{stubbed.event.log_index}"
    )
    assert result.identity.scope == scope(TARGET)
    assert result.before is before and result.handler == HANDLER
    assert result.after.owned.claimable_funding == ("claim",)
    assert result.assets == ("cash",)
    assert result.accounting_issues == ("funding issue", "wallet issue")
    assert stubbed.calls == [(before, stubbed.after, 5)]


def test_reconcile_refuses_a_missing_or_stale_financial_preimage(stubbed):
    state, _ = reconciliation_state(position=False)
    with pytest.raises(DataError, match="owned financial preimage"):
        liquidation_reconciliation.reconcile(backend({}), state, TARGET, stubbed.event, block(20))
    state, _ = reconciliation_state(opened=20)
    with pytest.raises(DataError, match="cannot precede its financial reference"):
        liquidation_reconciliation.reconcile(backend({}), state, TARGET, stubbed.event, block(20))


def test_reconcile_refuses_noncanonical_receipts_and_other_events(stubbed):
    state, _ = reconciliation_state()
    for receipt in (None, {"blockHash": block(20, "cd").hash}):
        with pytest.raises(DataError, match="absent from canonical history"):
            liquidation_reconciliation.reconcile(
                backend(receipt), state, TARGET, stubbed.event, block(20)
            )
    other = decoded(closed_event(index=5))
    with pytest.raises(DataError, match="differs from its canonical receipt"):
        liquidation_reconciliation.reconcile(
            backend({"blockHash": block(20).hash}), state, TARGET, other, block(20)
        )


def test_reconcile_refuses_a_partial_close_or_changed_allowance(stubbed):
    state, _ = reconciliation_state()
    receipt = {"blockHash": block(20).hash}
    for change in ({"position": object()}, {"allowance": 4}):
        for key, value in change.items():
            setattr(stubbed.after, key, value)
        with pytest.raises(DataError, match="did not fully close"):
            liquidation_reconciliation.reconcile(
                backend(receipt), state, TARGET, stubbed.event, block(20)
            )
        stubbed.after.position, stubbed.after.allowance = None, 3


# Outcome identity


def liquidation(**changes):
    at = block(20)
    receipt = {
        "transactionHash": "0x" + "01" * 32,
        "blockHash": at.hash,
        "blockNumber": hex(at.number),
        "status": "0x1",
    }
    asset = SimpleNamespace(key=SimpleNamespace(account=TARGET.account, chain=TARGET.fund.chain))
    values = {
        "identity": SimpleNamespace(connector=ConnectorId("gmx_v2"), scope=scope(TARGET)),
        "target": TARGET,
        "before": SimpleNamespace(target=TARGET),
        "after": SimpleNamespace(target=TARGET, effective_at="at", block=at),
        "effective_at": "at",
        "tx_hash": receipt["transactionHash"],
        "receipt": receipt,
        "assets": (asset,),
    }
    return SimpleNamespace(**(values | changes))


def test_liquidation_outcome_admits_its_exact_scope_and_receipt():
    value = liquidation()
    assert LiquidationOutcome.identity_scope(value) is value


@pytest.mark.parametrize(
    "changes",
    [
        {"identity": SimpleNamespace(connector=ConnectorId("uniswap_v4"), scope=scope(TARGET))},
        {"before": SimpleNamespace(target=frames.perpetual_target("avalanche"))},
        {"effective_at": "other"},
    ],
    ids=["connector", "before", "time"],
)
def test_liquidation_outcome_refuses_another_scope(changes):
    with pytest.raises(ValueError, match="exact fund, chain or time"):
        LiquidationOutcome.identity_scope(liquidation(**changes))


def test_liquidation_outcome_refuses_another_receipt_or_cash_scope():
    value = liquidation()
    with pytest.raises(ValueError, match="canonical successful receipt"):
        LiquidationOutcome.identity_scope(liquidation(receipt=value.receipt | {"status": "0x0"}))
    other = SimpleNamespace(key=SimpleNamespace(account="other", chain=TARGET.fund.chain))
    with pytest.raises(ValueError, match="affected fund scope"):
        LiquidationOutcome.identity_scope(liquidation(assets=(other,)))
