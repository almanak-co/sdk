"""HyperCore fills, funding, liquidations and ledger entries as the connector's external
effects (blueprint 30, sections 6 and 13.1).

Each page reconciles the range from the book's cursor to the time of a fresh
`clearinghouseState` read (`effects.reconcile`): every effect is applied once by its own
key, and the range is proven complete and balanced before the cursor moves, or the
deployment halts with the pages as evidence. A page commits only when it applies new
effects, so an idle account reads its growing range again rather than growing the
checkpoint.

Fills of the strategy's own attempts, a pending one's included, change the book's owned
exposure through these pages; the attempt's own resolution is its venue outcome. The
gateway verifies a proposed page by reading its fills, funding and ledger ranges again:
history at past times does not change, so the same range answers the same entries. The
account read that bounds the range cannot be read again as of its time; it is checked
for consistency with those entries, which is all it decides.
"""

from almanak.connectors.hyperliquid import lifecycle
from almanak.connectors.hyperliquid.effects import Pages, require_unchanged
from almanak.connectors.hyperliquid.effects import reconcile as book_reconcile
from almanak.connectors.hyperliquid.positions import holding
from almanak.connectors.hyperliquid.state import EffectsOutcome, Scan, effective, scope
from almanak.connectors.identifiers import ConnectorId
from almanak.external_effects import ExternalIdentity
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    ClearinghouseState,
    InfoRequest,
    UserFillsByTime,
    UserFunding,
    UserNonFundingLedgerUpdates,
    typed,
)

HYPERLIQUID = ConnectorId("hyperliquid")


def reconcile(backend, state, *, reference=None):
    """The next page of effects, or None when the account shows none since the cursor."""
    pages = (
        fresh_pages(backend.venue, state.book)
        if reference is None
        else verified(backend.venue, state, reference)
    )
    if pages is None:
        return None
    book = book_reconcile(state.book, pages, lifecycle.identities(state))
    new = book.effects[len(state.book.effects) :]
    if not new:
        if reference is not None:
            raise ValueError("a HyperCore page applies effects the book has not applied")
        return None
    (owned,) = state.owned
    # The holding's price is part of the proof: fills whose chaining orders price it
    # differently halt here with their pages, as fills leaving different exposure do.
    holding(book, owned.target.market.coin)
    end = typed(pages.account).time
    target = owned.target
    outcome = EffectsOutcome(
        identity=ExternalIdentity(
            connector=HYPERLIQUID, scope=scope(target), reference=f"{state.book.cursor}-{end}"
        ),
        effective_at=effective(end),
        target=target,
        pages=pages,
        effects=new,
    )
    return Scan(connector=HYPERLIQUID, scope=scope(target), target=target, outcome=outcome)


def fresh_pages(venue, book) -> Pages | None:
    account = venue.fetch(InfoRequest(info=ClearinghouseState(user=book.safe)))
    end = typed(account).time
    if end < book.cursor:
        # Nothing happened since the last range; the account must still match the book.
        require_unchanged(book, account)
        return None
    ranges = {"user": book.safe, "start_time": book.cursor, "end_time": end}
    return Pages(
        account=account,
        fills=venue.fetch(InfoRequest(info=UserFillsByTime(**ranges))),
        funding=venue.fetch(InfoRequest(info=UserFunding(**ranges))),
        ledger=venue.fetch(InfoRequest(info=UserNonFundingLedgerUpdates(**ranges))),
    )


def verified(venue, state, reference) -> Pages:
    """A proposed page's own pages, each of its ranges read again and answered the same."""
    (owned,) = state.owned
    if reference.outcome is None or reference.target != owned.target:
        raise ValueError("a HyperCore page reconciles the deployment's own Safe")
    pages = reference.outcome.pages
    for page in (pages.fills, pages.funding, pages.ledger):
        if venue.fetch(page.request).result != page.result:
            raise DataError(
                "source_mismatch",
                f"a HyperCore {page.request.info.type} page differs from its range read again",
            )
    return pages


def audit(backend, state) -> None:
    """Nothing to re-prove: HyperCore pages carry no block a reorganization could remove,
    and each was verified against its ranges read again when it committed."""
    return None


class ExternalEffects:
    page_type = Scan
    reconcile = staticmethod(reconcile)
    audit = staticmethod(audit)
