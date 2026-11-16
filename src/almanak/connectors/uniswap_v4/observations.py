"""Pinned reads and explicit decision quotes for the retained economic inventory."""

from almanak.connectors.dex_residual import entry_or_reason
from almanak.connectors.uniswap_v4 import reader as reads
from almanak.connectors.uniswap_v4.hook_terms import observed_terms
from almanak.connectors.uniswap_v4.math import entry_split
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.state import Allowance, Ownership, Snapshot
from almanak.connectors.v3_pool_observations import rebalance_amount
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def initial_owned(config, name):
    allocation, target = config.allocation(name), config.target(name)
    capital = allocation.capital
    inventory0, inventory1 = (
        AssetAmount(asset=held, amount=capital.amount if capital.asset == held else 0)
        for held in target.assets
    )
    return Ownership(target=target, inventory0=inventory0, inventory1=inventory1)


def ownership(config, name, reference):
    if reference is None:
        return initial_owned(config, name)
    if isinstance(reference, Ownership):
        return reference
    return reference.owned


def allowances(reader, target, block):
    result = []
    for asset in target.assets:
        result.append(
            Allowance(
                asset=asset,
                spender=target.contracts.permit2,
                amount=reads.allowance(reader, target, asset, block),
            )
        )
        for spender in (target.contracts.router, target.contracts.position_manager):
            amount, expiration, nonce = reads.permit_allowance(
                reader, target, asset, spender, block
            )
            result.append(
                Allowance(
                    asset=asset,
                    spender=spender,
                    amount=amount,
                    expiration=expiration,
                    nonce=nonce,
                )
            )
    return tuple(result)


def position_evidence(reader, owned, block):
    if owned.position is None:
        return {}
    liquidity, selected = reads.position(
        reader, owned.target, owned.position.token_id, block, expected=owned.position.range
    )
    result = {"position_liquidity": liquidity}
    try:
        result["fee_growth"] = reads.fee_growth(reader, owned.target, block, selected)
        _, result["position_growth"] = reads.position_growth(
            reader,
            owned.target,
            owned.position.token_id,
            block,
            selected,
        )
    except DataError as exc:
        # Ownership and exit authority remain independently verifiable when a fee
        # observation fails. The missing measurement is retained in reports.
        result["fee_error"] = f"fee growth unavailable: {exc.kind}"
    return result


def decision_quotes(reader, config, owned, pool, *, entry=True):
    return quotes_for(
        config,
        owned,
        pool,
        lambda amount: reads.quote(reader, owned.target, amount, pool.block),
        entry=entry,
    )


def entry_budget(allocation, owned, funding):
    """One entry slice of capital, capped by the funding inventory the allocation holds.

    Entries repeat: the slice is offered whenever it is above zero, before and after
    other entries and exits. `entry_complete` only records the first exposure. A
    liquidity allocation's entry prepares a mint, which an owned position refuses, so it
    is offered only while no position is held.
    """
    if owned.target.liquidity_enabled and owned.position is not None:
        return 0
    return min(funding.amount, allocation.capital.amount * allocation.entry_swap_bps // 10000)


def entry_amount(owned, pool, funding, budget, quote):
    """The swap part of an entry: all of it for trading, the mint split for liquidity."""
    if not budget or not owned.target.liquidity_enabled:
        return budget
    lower, upper = reads.sqrt_bounds(owned.target, requested=planned_range(owned, pool))
    return entry_split(
        budget,
        pool.sqrt_price_x96,
        lower,
        upper,
        lambda value: quote(AssetAmount(asset=funding.asset, amount=value)).output.amount,
        funding0=funding.asset == owned.target.assets[0],
    )


def quotes_for(config, owned, pool, quote, *, entry=True):
    """Decision quotes; `entry=False` skips the entry quote an exit or teardown cannot use.

    A liquidity entry sizes its swap by a search of Quoter calls, so skipping it also
    skips that search.
    """
    allocation = config.allocation(owned.target.allocation)
    inventory = (owned.inventory0, owned.inventory1)
    funding = next(v for v in inventory if v.asset == allocation.capital.asset)
    counter = next(v for v in inventory if v.asset != allocation.capital.asset)
    result = (
        entry_or_reason(lambda: entry_quote(allocation, owned, pool, funding, quote))
        if entry
        else {}
    )
    if owned.position is None and counter.amount:
        measured = quote(counter)
        result.update(offered("exit_quote", measured) or {"exit_residual": measured})
    if owned.position is None and owned.target.band is not None:
        rebalance = rebalance_amount(owned, pool)
        if rebalance is not None:
            result.update(offered("rebalance_quote", quote(rebalance)))
    return result


def entry_quote(allocation, owned, pool, funding, quote):
    budget = entry_budget(allocation, owned, funding)
    amount = entry_amount(owned, pool, funding, budget, quote)
    if not amount:
        return {}
    return offered("entry_quote", quote(AssetAmount(asset=funding.asset, amount=amount)))


def offered(key, measured):
    """A quote a swap can follow; a zero-output entry or rebalance quote offers none."""
    return {key: measured} if measured.output.amount else {}


def residual_measurement(owned, quote):
    """The exit residual after an exit leg, or nothing when none is measured.

    An unavailable quote leaves the counter obligation visible instead of failing
    the observation that records an already mined leg.
    """
    counter = next(
        v for v in (owned.inventory0, owned.inventory1) if v.asset == owned.target.counter_currency
    )
    if owned.position is not None or not counter.amount:
        return {}
    try:
        measured = quote(counter)
    except DataError:
        return {}
    return {} if measured.output.amount else {"exit_residual": measured}


def planned_range(owned, pool):
    """A banded entry sizes against the position it would open now; fixed ranges need none."""
    if owned.target.band is None:
        return None
    return reads.centered_range(owned.target, pool.tick)


def snapshot(backend, name, block, reference=None, *, quotes=True, residual=False):
    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned = ownership(config, name, reference)
    if isinstance(reference, Snapshot):
        quotes = reference.quotes_included
    if owned.target != target:
        raise ValueError("observation reference belongs to another target")
    profile = config.accounts.binding(target.account).profile
    pool = read_pool(reader, target.pool, block)
    from almanak.configuration.metadata import verify

    verify(config, pool.currency0)
    verify(config, pool.currency1)
    native = int(
        reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(
            asset=target.assets[0],
            amount=reads.wallet_balance(reader, target, target.assets[0], block),
        ),
        wallet1=AssetAmount(
            asset=target.assets[1],
            amount=reads.wallet_balance(reader, target, target.assets[1], block),
        ),
        allowances=allowances(reader, target, block),
        native_balance=AssetAmount(asset=config.allocation(name).native_asset, amount=native),
        payer=profile.submitter,
        quotes_included=quotes,
        **position_evidence(reader, owned, block),
        **observed_terms(reader, target.pool, block, live=True),
        **snapshot_quotes(reader, config, owned, pool, quotes, residual),
    )


def snapshot_quotes(reader, config, owned, pool, quotes, residual):
    if quotes:
        return decision_quotes(reader, config, owned, pool)
    if residual:
        return residual_measurement(
            owned, lambda amount: reads.quote(reader, owned.target, amount, pool.block)
        )
    return {}
