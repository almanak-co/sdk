"""Settle transfers in flight from canonical pages: arrival where it lands, refund where it left.

A page is committed only when it settles a transfer, ends an arrival scan past the fill
deadline, anchors a scan's first block, or is full. A shorter empty page is read again
on the next tick, so waiting grows no history.
"""

from datetime import UTC, datetime
from itertools import combinations

from almanak.connectors.across import settlement_reader as chain
from almanak.connectors.across.domain import spoke_pool
from almanak.connectors.across.operations import word
from almanak.connectors.across.relay import node_number, parse_reference, relay_hash
from almanak.connectors.across.settlement_types import (
    CONNECTOR,
    FILL_TYPES,
    ArrivalOutcome,
    RefundOutcome,
    Scan,
    arrival_scope,
    refund_scope,
)
from almanak.effects import WalletAsset
from almanak.external_effects import ExternalIdentity
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount

SPAN = chain.MAXIMUM_EVENT_BLOCK_SPAN
# One refund can pay several transfers. Attribution enumerates their subsets, so it is bounded.
MAXIMUM_REFUND_CANDIDATES = 12


def seconds(instant) -> int:
    return int(instant.timestamp())


def pages_of(state, side, target, transfer=None):
    return [
        page
        for page in state.external_scans
        if page.side == side and page.target == target and page.transfer == transfer
    ]


def next_bounds(reader, last, anchor: int):
    """The next page to read: after the last one, or from the anchor's own block."""
    head = reader.pin_block("latest")
    if head.timestamp <= anchor:
        return None
    start = None
    if last is None or last.timestamp < anchor:
        # Bisected once per scan: the page it starts is committed at once and retains it.
        start = chain.block_at_or_before(reader, anchor, head)
    if start is None or (last is not None and start.number <= last.number):
        if last.number + 1 > head.number:
            return None
        start = reader.pin_block(last.number + 1)
    return start, reader.pin_block(min(start.number + SPAN - 1, head.number))


def bounds(reader, pages, anchor: int, reference):
    """The page's first and last block, and whether it starts its scan or jumps ahead.

    A page continues the one before it, or starts at the last block not after `anchor`:
    the quote's timestamp for an arrival, the earliest open fill deadline for a refund.
    """
    last = pages[-1].end if pages else None
    if reference is not None:
        interval = chain.canonical(reader, reference.start), chain.canonical(reader, reference.end)
    else:
        interval = next_bounds(reader, last, anchor)
    if interval is None:
        return None
    start, end = interval
    continues = last is not None and start.number == last.number + 1
    if not continues and not (
        (last is None or start.number > last.number) and chain.anchored(reader, start, anchor)
    ):
        raise chain.integrity("Across scan changed its retained bounded cursor")
    if end.number - start.number >= SPAN:
        raise chain.integrity("Across scan page exceeds its bounded span")
    return start, end, not continues


def committable(page, first: bool) -> bool:
    full = page.end.number - page.start.number + 1 == SPAN
    return page.outcome is not None or page.expired or first or full


def waiting_arrivals(state, reader_chain):
    """Transfers whose fill this chain may still show: open ones, and refunded ones until
    their arrival scan has passed the fill deadline, so a fill beside a refund is seen."""
    for transfer in state.transfers:
        if transfer.connector != CONNECTOR or transfer.destination.chain != reader_chain:
            continue
        target = state.config.target(transfer.allocation)
        pages = pages_of(state, "arrival", target, transfer.reference)
        if any(page.outcome is not None or page.expired for page in pages):
            continue
        if transfer.open or transfer.resolution.kind == "refunded":
            yield transfer, target, pages


def matching_fill(rows, relay: str, destination):
    matches = []
    for row in rows:
        fill = chain.decoded_fill(row)
        if fill["relay"] is not None and relay_hash(fill["relay"], destination) == relay:
            matches.append((row, fill))
    if len(matches) > 1:
        raise chain.integrity("Across filled one relay twice")
    return matches[0] if matches else None


def measured_arrival(reader, target, row, fill) -> tuple[dict, int]:
    """The fill's receipt and the amount it paid the destination fund, held to the event."""
    fund = target.destination_fund.address
    if fill["updated_recipient"] != word(fund):
        raise chain.integrity("an Across fill paid another recipient than the destination fund")
    receipt = chain.receipt_of(reader, row)
    paid = chain.paid_to(receipt, target.destination_asset.reference, fund, chain.position(row)[1])
    if paid is None or paid != fill["updated_output"]:
        raise DataError(
            "accounting",
            f"Across fill {row['transactionHash']} paid the fund {paid}, "
            f"not the {fill['updated_output']} its event states",
        )
    return receipt, paid


def arrival_outcome(reader, transfer, target, row, fill, block):
    if not transfer.open:
        raise chain.integrity(
            f"Across transfer {transfer.reference} was refunded and is also filled in "
            f"{row['transactionHash']}; both are retained"
        )
    receipt, paid = measured_arrival(reader, target, row, fill)
    asset = target.destination_asset
    return ArrivalOutcome(
        identity=ExternalIdentity(
            connector=CONNECTOR,
            scope=arrival_scope(target),
            reference=f"fill:{row['transactionHash']}:{chain.position(row)[1]}",
        ),
        effective_at=datetime.fromtimestamp(block.timestamp, UTC),
        target=target,
        block=block,
        tx_hash=row["transactionHash"],
        receipt=receipt,
        transfer=transfer.reference,
        received=AssetAmount(asset=asset, amount=paid),
        offered=transfer.offered,
        fill_type=FILL_TYPES[fill["fill_type"]],
        log_index=chain.position(row)[1],
    )


def require_status(reader, pool, relay, page):
    """The SpokePool's own fill status, as a cross-check of what the page's logs show."""
    filled = chain.fill_status(reader, pool, relay, page.end) == chain.FILLED
    if page.outcome is not None and not filled:
        raise chain.integrity("an Across fill event has no filled status behind it")
    if page.expired and filled:
        raise chain.integrity("an Across relay is filled outside its scanned pages")


def arrival_page(backend, state, transfer, target, pages, reference=None):
    reader, deposit = backend.reader, parse_reference(transfer.reference)
    interval = bounds(reader, pages, deposit.quoted_at, reference)
    if interval is None:
        return None
    start, end, first = interval
    pool = spoke_pool(target.route.destination_chain).address
    origin = node_number(target.fund.chain)
    events = chain.fill_logs(reader, pool, origin, deposit.deposit_id, start, end)
    found = matching_fill(events, deposit.relay_hash, node_number(target.destination_fund.chain))
    outcome = None
    if found is not None:
        row, fill = found
        if reference is None and chain.position(row)[0] < end.number:
            end = reader.pin_block(chain.position(row)[0])
            events = chain.fill_logs(reader, pool, origin, deposit.deposit_id, start, end)
        outcome = arrival_outcome(reader, transfer, target, row, fill, end)
    page = Scan(
        connector=CONNECTOR,
        scope=arrival_scope(target),
        side="arrival",
        target=target,
        transfer=transfer.reference,
        start=start,
        end=end,
        events=events,
        expired=outcome is None and end.timestamp > seconds(transfer.fill_deadline),
        outcome=outcome,
    )
    return checked(
        page, first, reference, lambda: require_status(reader, pool, deposit.relay_hash, page)
    )


def checked(page, first, reference, cross_check):
    if not committable(page, first):
        if reference is not None:
            raise chain.integrity("Across scan page is neither full nor settled")
        return None
    cross_check()
    return page


def expired_open(state, wallet: WalletAsset, timestamp: int):
    return [
        transfer
        for transfer in state.transfers
        if transfer.connector == CONNECTOR
        and transfer.open
        and transfer.source == wallet
        and seconds(transfer.fill_deadline) < timestamp
    ]


def attributed(total: int, own, others):
    """The transfers a refund of `total` pays: the one subset of the wallet asset's expired
    open transfers whose sent amounts sum to it.

    A refund returns each transfer's whole sent amount. Own transfers are returned;
    another allocation's are None, left to that allocation's scan. No subset, or more
    than one, is refused: the refund cannot be told apart.
    """
    everyone = (*own, *others)
    if len(everyone) > MAXIMUM_REFUND_CANDIDATES:
        raise DataError("accounting", "too many expired Across transfers to attribute a refund")
    subsets = [
        subset
        for size in range(1, len(everyone) + 1)
        for subset in combinations(everyone, size)
        if sum(t.sent.amount for t in subset) == total
    ]
    if len(subsets) != 1:
        raise DataError(
            "accounting",
            f"an Across refund of {total} matches {len(subsets)} sets of expired transfers "
            "and cannot be attributed",
        )
    mine = [t for t in subsets[0] if t in own]
    if mine and len(mine) != len(subsets[0]):
        raise DataError("accounting", "an Across refund pays transfers of two allocations at once")
    return tuple(mine) or None


def refund_total(receipt, target) -> int | None:
    """What the transaction's refund leaves paid the fund; None when it executed none for it."""
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    entries = chain.refund_entries(receipt, pool, token, fund)
    if not entries:
        return None
    if sorted(entries) != sorted(chain.refund_payments(receipt, token, pool, fund)):
        raise DataError(
            "accounting",
            f"Across refund {receipt['transactionHash']} did not pay the fund what its leaf names",
        )
    return sum(entries)


def source_wallet(target) -> WalletAsset:
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=target.asset)


def refund_found(reader, state, target, events):
    """The first refund in the page that pays this allocation's transfers."""
    seen = set()
    for row in events:
        if row["transactionHash"] in seen:
            continue
        seen.add(row["transactionHash"])
        receipt = chain.receipt_of(reader, row)
        total = refund_total(receipt, target)
        if total is None:
            continue
        block = reader.pin_block(chain.position(row)[0])
        expired = expired_open(state, source_wallet(target), block.timestamp)
        own = [t for t in expired if t.allocation == target.allocation]
        paid = attributed(total, own, [t for t in expired if t not in own])
        if paid is not None:
            return row, receipt, block, total, paid
    return None


def refund_outcome(target, row, receipt, block, total, paid):
    return RefundOutcome(
        identity=ExternalIdentity(
            connector=CONNECTOR,
            scope=refund_scope(target),
            reference=f"refund:{row['transactionHash']}",
        ),
        effective_at=datetime.fromtimestamp(block.timestamp, UTC),
        target=target,
        block=block,
        tx_hash=row["transactionHash"],
        receipt=receipt,
        transfers=tuple(t.reference for t in paid),
        returned=AssetAmount(asset=target.asset, amount=total),
    )


def require_single(events, row):
    """A page ends at its refund's block; a second refund there would be skipped."""
    block = chain.position(row)[0]
    others = {
        e["transactionHash"]
        for e in events
        if chain.position(e)[0] == block and e["transactionHash"] != row["transactionHash"]
    }
    if others:
        raise DataError("ordering", "two Across refund payments to the fund share one block")


def require_undeferred(reader, target, block):
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    held = chain.deferred_refund(reader, pool, token, fund, block)
    if held:
        raise DataError(
            "refund_deferred",
            f"the Across SpokePool holds {held} of {target.asset} for the fund because a refund "
            "transfer failed; it is unresolved until the fund claims it",
        )


def waiting_refunds(state, reader_chain):
    for allocation in state.config.positions:
        target = state.config.target(allocation.name)
        if target.fund.chain != reader_chain:
            continue
        waiting = [
            t
            for t in state.transfers
            if t.connector == CONNECTOR and t.open and t.allocation == allocation.name
        ]
        if waiting:
            yield target, min(seconds(t.fill_deadline) for t in waiting)


def refund_page(backend, state, target, earliest: int, reference=None):
    reader = backend.reader
    interval = bounds(reader, pages_of(state, "refund", target), earliest, reference)
    if interval is None:
        return None
    start, end, first = interval
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    events = chain.refund_logs(reader, token, pool, fund, start, end)
    found = refund_found(reader, state, target, events)
    outcome = None
    if found is not None:
        row, receipt, block, total, paid = found
        if reference is None and block.number < end.number:
            end, events = block, chain.refund_logs(reader, token, pool, fund, start, block)
        require_single(events, row)
        outcome = refund_outcome(target, row, receipt, block, total, paid)
    page = Scan(
        connector=CONNECTOR,
        scope=refund_scope(target),
        side="refund",
        target=target,
        start=start,
        end=end,
        events=events,
        outcome=outcome,
    )
    return checked(page, first, reference, lambda: require_undeferred(reader, target, page.end))


def referenced(backend, state, reference):
    """Re-derive exactly the retained page, for the gateway's independent verification."""
    reader_chain = backend.reader.chain
    if reference.side == "arrival":
        for transfer, target, pages in waiting_arrivals(state, reader_chain):
            if (target, transfer.reference) == (reference.target, reference.transfer):
                return arrival_page(backend, state, transfer, target, pages, reference)
    else:
        for target, earliest in waiting_refunds(state, reader_chain):
            if target == reference.target:
                return refund_page(backend, state, target, earliest, reference)
    raise DataError("ownership", "Across external page concerns no transfer in flight")


def reconcile(backend, state, *, reference=None):
    if reference is not None:
        return referenced(backend, state, reference)
    reader_chain = backend.reader.chain
    for transfer, target, pages in waiting_arrivals(state, reader_chain):
        page = arrival_page(backend, state, transfer, target, pages)
        if page is not None:
            return page
    for target, earliest in waiting_refunds(state, reader_chain):
        page = refund_page(backend, state, target, earliest)
        if page is not None:
            return page
    return None


def reread(reader, page):
    target = page.target
    if page.side == "refund":
        pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
        return chain.refund_logs(reader, token, pool, fund, page.start, page.end)
    deposit = parse_reference(page.transfer)
    pool = spoke_pool(target.route.destination_chain).address
    origin = node_number(target.fund.chain)
    return chain.fill_logs(reader, pool, origin, deposit.deposit_id, page.start, page.end)


def audit(backend, state):
    """Retained pages of this chain must still be canonical: bounds, logs and receipts."""
    reader = backend.reader
    for page in state.external_scans:
        if page.scope.chain != reader.chain:
            continue
        chain.canonical(reader, page.start)
        chain.canonical(reader, page.end)
        if reread(reader, page) != page.events:
            raise DataError("reorganization", "Across retained scan events are no longer canonical")
        if page.outcome is not None:
            receipt = reader._rpc(
                "eth_getTransactionReceipt", [page.outcome.tx_hash], allow_none=True
            )
            if receipt != page.outcome.receipt:
                raise DataError("reorganization", "Across retained settlement receipt has changed")


class ExternalEffects:
    page_type = Scan
    reconcile = staticmethod(reconcile)
    audit = staticmethod(audit)

    @staticmethod
    def unresolved(state) -> bool:
        return any(transfer.open for transfer in state.transfers)

    @staticmethod
    def fork_keeper(forks, directory):
        from almanak.connectors.across.fork_settler import Settler

        return Settler(forks, directory)
