"""Settle transfers in flight from canonical pages: arrival where it lands, refund where it left.

A page is committed only when it settles a transfer, holds an unattributed refund
payment, ends an arrival scan past the fill deadline, anchors a scan's first block, or
is full. A shorter empty page is read again on the next tick. Full empty pages are
retained and audited, so a scan that waits grows by one page per span of blocks.
"""

from datetime import UTC, datetime
from itertools import combinations

from almanak.connectors.across import settlement_reader as chain
from almanak.connectors.across.operations import word
from almanak.connectors.across.relay import node_number, parse_reference, relay_hash
from almanak.connectors.across.settlement_types import (
    CONNECTOR,
    FILL_TYPES,
    ArrivalOutcome,
    Companion,
    RefundOutcome,
    Scan,
    Unsolicited,
    arrival_scope,
    refund_scope,
)
from almanak.external_effects import ExternalIdentity
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount

SPAN = chain.MAXIMUM_EVENT_BLOCK_SPAN
# One refund can pay several transfers. Attribution enumerates their subsets, so it is bounded.
MAXIMUM_REFUND_CANDIDATES = 12
# How many sets the joint assignment of one block's payments may try before it falls back
# to taking the payments one by one.
JOINT_SEARCH_STEPS = 10_000


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
    ended = page.outcome is not None or bool(page.unsolicited)
    return ended or page.expired or first or full


def waiting_arrivals(state, reader_chain):
    """Transfers whose fill this chain may still show: those whose arrival scan has
    neither found a fill nor ended past the fill deadline."""
    for transfer in state.transfers:
        if transfer.connector != CONNECTOR or transfer.destination.chain != reader_chain:
            continue
        target = state.config.target(transfer.allocation)
        pages = pages_of(state, "arrival", target, transfer.reference)
        if any(page.outcome is not None or page.expired for page in pages):
            continue
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
    payer = row["address"].lower() if FILL_TYPES[fill["fill_type"]] == "slow" else None
    token, after = target.destination_asset.reference, chain.position(row)[1]
    paid = chain.paid_to(receipt, token, fund, after, payer)
    if paid is None or paid != fill["updated_output"]:
        raise DataError(
            "accounting",
            f"Across fill {row['transactionHash']} paid the fund {paid}, "
            f"not the {fill['updated_output']} its event states",
        )
    return receipt, paid


def arrival_outcome(reader, transfer, target, row, fill, block):
    if not transfer.open or transfer.unfilled:
        raise chain.integrity(
            f"Across transfer {transfer.reference} was proven unfilled or refunded and is "
            f"also filled in {row['transactionHash']}; both are retained"
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
    # The pool the transfer was sent to, whatever the pin in code names today.
    pool = target.route.destination_pool.address
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


def candidates(state, target, timestamp: int):
    """The transfers a refund mined at `timestamp` may pay: this allocation's, proven
    unfilled by the destination chain, past their fill deadline. Oldest deadline first,
    then by reference."""
    waiting = [
        transfer
        for transfer in state.transfers
        if transfer.connector == CONNECTOR
        and transfer.open
        and transfer.unfilled
        and transfer.allocation == target.allocation
        and seconds(transfer.fill_deadline) < timestamp
    ]
    waiting.sort(key=lambda transfer: (transfer.fill_deadline, transfer.reference))
    return waiting


def refund_payment(receipt, target) -> tuple[int, int, bool] | None:
    """What the transaction's refund leaves name for the fund, what the SpokePool paid
    it there, and whether the two agree; None when it executed no leaf for the fund."""
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    entries = chain.refund_entries(receipt, pool, token, fund)
    if not entries:
        return None
    payments = chain.refund_payments(receipt, token, pool, fund)
    # Equal as lists, not only in sum: each leaf entry is one payment of its amount.
    return sum(entries), sum(payments), sorted(entries) == sorted(payments)


def leaf_payments(reader, target, events):
    """Every refund-leaf payment to the fund in the first block that holds one, in the
    block's own log order, with that block."""
    found, block, seen = [], None, set()
    for row in sorted(events, key=chain.position):
        if row["transactionHash"] in seen or (
            block is not None and chain.position(row)[0] != block.number
        ):
            continue
        seen.add(row["transactionHash"])
        receipt = chain.receipt_of(reader, row)
        amounts = refund_payment(receipt, target)
        if amounts is None:
            continue
        block = block or reader.pin_block(chain.position(row)[0])
        found.append((row, receipt, *amounts))
    return found, block


def unattributed(target, row, named: int, paid: int, exact: bool) -> Unsolicited:
    leaf = None if exact else AssetAmount(asset=target.asset, amount=named)
    return Unsolicited(
        tx_hash=row["transactionHash"],
        amount=AssetAmount(asset=target.asset, amount=paid),
        leaf_amount=leaf,
    )


def matching_sets(total: int, waiting, free):
    """Index sets of the free transfers whose sent amounts sum to `total`, oldest first.

    Only the oldest few free transfers are considered, because their subsets are
    enumerated. A transfer another payment of the block took is not free, so a younger
    one moves into the window.
    """
    window = free[:MAXIMUM_REFUND_CANDIDATES]
    return sorted(
        subset
        for size in range(1, len(window) + 1)
        for subset in combinations(window, size)
        if sum(waiting[index].sent.amount for index in subset) == total
    )


def joint(totals, waiting, free, budget):
    """Disjoint sets of waiting transfers, one per total, or None when there are none.

    The first complete assignment in the tie rule's order: the first payment takes its
    oldest possible set, the next the oldest of what that leaves, and so on. The search
    is bounded: past its budget of steps it gives up, as if there were none.
    """
    if not totals:
        return ()
    for subset in matching_sets(totals[0], waiting, free):
        budget[0] -= 1
        if budget[0] < 0:
            return None
        rest = joint(totals[1:], waiting, tuple(i for i in free if i not in subset), budget)
        if rest is not None:
            return (subset, *rest)
    return None


def one_by_one(totals, waiting, free):
    """Each payment in log order takes its oldest set of what the earlier ones left."""
    taken = []
    for total in totals:
        matches = matching_sets(total, waiting, free)
        taken.append(matches[0] if matches else ())
        free = tuple(i for i in free if i not in taken[-1])
    return taken


def assigned(totals, waiting):
    """One set of waiting transfers per payment total, empty where none is taken.

    Payments that fit some set on their own are assigned together, so that none of
    them is left without one where a complete assignment exists; a stranger's payment
    that fits nothing does not enter that search. The others then take, in log order,
    what is left. Without a complete assignment every payment is taken one by one.
    """
    everything = tuple(range(len(waiting)))
    fitting = [n for n, total in enumerate(totals) if matching_sets(total, waiting, everything)]
    together = joint([totals[n] for n in fitting], waiting, everything, [JOINT_SEARCH_STEPS])
    if together is None:
        return one_by_one(totals, waiting, everything)
    sets = dict(zip(fitting, together, strict=True))
    used = {index for subset in together for index in subset}
    rest = [n for n in range(len(totals)) if n not in sets]
    left = tuple(i for i in everything if i not in used)
    sets.update(zip(rest, one_by_one([totals[n] for n in rest], waiting, left), strict=True))
    return [sets[n] for n in range(len(totals))]


def settled(state, target, block, payments):
    """Attribute the block's refund payments to disjoint sets of waiting transfers.

    All payments of a block are known at once, so they are assigned together, under
    `assigned`. A payment that fits no set is attributed to nothing, and one the
    SpokePool paid in another amount than its leaf names is never attributed.
    """
    waiting = candidates(state, target, block.timestamp)
    sets = iter(assigned([paid for _, _, _, paid, agrees in payments if agrees], waiting))
    refunds, left = [], []
    for row, receipt, named, paid, agrees in payments:
        taken = tuple(waiting[index] for index in next(sets)) if agrees else ()
        if not taken:
            left.append(unattributed(target, row, named, paid, agrees))
            continue
        refunds.append((row, receipt, paid, taken))
    return refunds, tuple(left)


def refund_outcome(target, block, refunds):
    (row, receipt, _, _), others = refunds[0], refunds[1:]
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
        transfers=tuple(t.reference for _, _, _, taken in refunds for t in taken),
        returned=AssetAmount(asset=target.asset, amount=sum(paid for _, _, paid, _ in refunds)),
        companions=tuple(
            Companion(tx_hash=other["transactionHash"], receipt=other_receipt)
            for other, other_receipt, _, _ in others
        ),
    )


def require_undeferred(reader, target, block):
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    held = chain.deferred_refund(reader, pool, token, fund, block)
    if held:
        raise DataError(
            "refund_deferred",
            f"the Across SpokePool holds {held} of {target.asset} for the fund because a refund "
            "transfer failed; it is unresolved until the fund claims it",
        )


def in_flight(state, target):
    return [
        transfer
        for transfer in state.transfers
        if transfer.connector == CONNECTOR
        and transfer.open
        and transfer.allocation == target.allocation
    ]


def waiting_refunds(state, reader_chain):
    """Allocations with a transfer the destination chain has proven unfilled, and the
    earliest fill deadline among those: a refund is read for nothing else."""
    for allocation in state.config.positions:
        target = state.config.target(allocation.name)
        if target.fund.chain != reader_chain:
            continue
        waiting = [transfer for transfer in in_flight(state, target) if transfer.unfilled]
        if waiting:
            yield target, min(seconds(transfer.fill_deadline) for transfer in waiting)


def held_back(state, target, end) -> bool:
    """Whether the page would pass the fill deadline of a transfer not yet proven
    unfilled. Its refund could lie there and would be read as unattributed, so the
    refund scan waits for the destination chain's proof or its arrival."""
    return any(
        not transfer.unfilled and seconds(transfer.fill_deadline) <= end.timestamp
        for transfer in in_flight(state, target)
    )


def refund_page(backend, state, target, earliest: int, reference=None):
    reader = backend.reader
    interval = bounds(reader, pages_of(state, "refund", target), earliest, reference)
    if interval is None:
        return None
    start, end, first = interval
    pool, token, fund = target.route.spoke_pool, target.asset.reference, target.fund.address
    events = chain.refund_logs(reader, token, pool, fund, start, end)
    payments, block = leaf_payments(reader, target, events)
    outcome, left = None, ()
    if block is not None:
        if block.number < end.number:
            # A page ends at its first refund-leaf payment: a wider one would let the
            # scan continue past refunds it never read.
            if reference is not None:
                raise chain.integrity("Across refund page runs past its first refund payment")
            end, events = block, chain.refund_logs(reader, token, pool, fund, start, block)
        refunds, left = settled(state, target, block, payments)
        outcome = refund_outcome(target, block, refunds) if refunds else None
    if held_back(state, target, end):
        if reference is not None:
            raise chain.integrity("Across refund page passes an unproven fill deadline")
        return None
    page = Scan(
        connector=CONNECTOR,
        scope=refund_scope(target),
        side="refund",
        target=target,
        start=start,
        end=end,
        events=events,
        unsolicited=left,
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
    pool = target.route.destination_pool.address
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
            for companion in getattr(page.outcome, "companions", ()):
                chain.require_retained(reader, companion.tx_hash, companion.receipt)


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
