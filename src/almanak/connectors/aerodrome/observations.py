"""Observe fund cash, LP claims, pooled balances and separate gas payer funds."""

from datetime import UTC, datetime

from pydantic import Field, model_validator

from almanak.configuration.metadata import verify
from almanak.connectors.aerodrome import reader as reads
from almanak.connectors.aerodrome.deployments import require_direct_fund, verify_router
from almanak.connectors.aerodrome.domain import PoolObservation
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.positions import Allowance, Ownership, Position, Quote
from almanak.connectors.dex_residual import (
    classified,
    counter_of,
    entry_or_reason,
    residual_measurement,
    validate_entry_error,
    validate_quotes,
)
from almanak.gateway.ethereum import DataError
from almanak.models import Account, AssetAmount, Model


class Snapshot(Model):
    target: Target
    pool: PoolObservation
    owned: Ownership
    wallet0: AssetAmount
    wallet1: AssetAmount
    allowances: tuple[Allowance, ...]
    native_balance: AssetAmount | None
    payer: Account | None
    position: Position | None
    entry_quote: Quote | None = None
    exit_quote: Quote | None = None
    # Counter inventory whose exact-size exit quote returned nothing: measured residual
    # dust, absent unless measured so other snapshots keep their shape.
    exit_residual: Quote | None = Field(default=None, exclude_if=lambda v: v is None)
    # Why no entry quote is offered although an entry slice remains, when the market
    # cannot fill it at this block; absent otherwise so other snapshots keep their shape.
    entry_quote_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    quotes_included: bool = True
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def block(self):
        return self.pool.block

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def residuals(self):
        """Measured zero-output exit quotes of residual counter dust."""
        return () if self.exit_residual is None else (self.exit_residual,)

    @property
    def needs_cleanup(self):
        return any(allowance.amount for allowance in self.allowances)

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.pool.source != self.target.pool:
            raise ValueError("snapshot must retain its configured target and ownership")
        if (self.wallet0.asset, self.wallet1.asset) != self.target.assets:
            raise ValueError("wallet observations must identify the pool currencies in order")
        if self.payer is not None and self.payer.chain != self.block.chain:
            raise ValueError("payer must retain the observed execution chain")
        if self.position is not None:
            self.position.validate_target(self.target)
        self.validate_inputs()
        return self

    def validate_inputs(self):
        expected = (*self.target.assets, self.target.pool.shares_asset)
        if tuple((a.asset, a.spender) for a in self.allowances) != tuple(
            (asset, self.target.contracts.router) for asset in expected
        ):
            raise ValueError("allowances must identify both currencies, LP shares and exact router")
        self.validate_quote_scope()

    def validate_quote_scope(self):
        for quote in (self.entry_quote, self.exit_quote, self.exit_residual):
            if quote is not None and (quote.source, quote.block) != (self.target, self.block):
                raise ValueError("quotes must retain the observed market and block")
        validate_quotes(
            self.owned, (self.entry_quote, self.exit_quote), self.exit_quote, self.exit_residual
        )
        validate_entry_error(self.entry_quote, self.entry_quote_error)


def initial_owned(config, name):
    allocation = config.allocation(name)
    target = config.target(name)
    counter = 0 if allocation.adopt is None else allocation.adopt.counter
    values = tuple(
        AssetAmount(
            asset=asset,
            amount=allocation.capital.amount if asset == allocation.capital.asset else counter,
        )
        for asset in target.assets
    )
    return Ownership(target=target, inventory0=values[0], inventory1=values[1])


def funded_owned(config, name):
    """Initial ownership of a backtest: funded capital, never an adoption."""
    from almanak.operator_deployment import refuse_adoption

    refuse_adoption((config.allocation(name),))
    return initial_owned(config, name)


def ownership(config, name, reference):
    if reference is None:
        return initial_owned(config, name)
    return reference if isinstance(reference, Ownership) else reference.owned


def position(reader, target, pool, *, signed=False):
    """The fund's stored pool position at the pool's block; None when it holds none.

    Shares and fee claims belong to the whole account. A trading allocation owns none,
    so it cannot keep the fund's own apart from its inventory, and nothing is planned
    or signed for a fund that holds any: that observation is refused.

    `signed` observes for a step whose bytes are already signed: the block it was
    mined in, or the head it is sent at when it is sent whatever the fund holds
    (`validation.sent_past_shares`). Anyone can send the fund shares, and what it
    holds there cannot recall the bytes or undo the step. Refusing would hold an
    approval, which carries no deadline, back for good. The trading allocation still
    owns none, so the observation carries no position, the step is sent, and its
    reconciliation reports the holding as an accounting issue
    (`reconciliation.unowned_holdings`).
    """
    value = stored_position(reader, target, pool)
    if value.empty:
        return None
    if target.liquidity_enabled:
        return value
    if signed:
        return None
    raise DataError(
        "ownership",
        f"the fund holds LP shares or fee claims of pool {target.pool.pool}, "
        "which a trading-only allocation cannot own",
    )


def stored_position(reader, target, pool):
    shares = reads.balance(reader, target.pool.shares_asset, target.fund.address, pool.block)
    checkpoints = tuple(
        reads.fee_checkpoint(reader, target.pool.pool, pool.block, target.fund.address, index)
        for index in (0, 1)
    )
    return Position(
        shares=AssetAmount(asset=target.pool.shares_asset, amount=shares),
        fees0=AssetAmount(asset=target.assets[0], amount=checkpoints[0][1]),
        fees1=AssetAmount(asset=target.assets[1], amount=checkpoints[1][1]),
        index0=checkpoints[0][0],
        index1=checkpoints[1][0],
    )


def allowances(reader, target, block):
    return tuple(
        Allowance(
            asset=asset,
            spender=target.contracts.router,
            amount=reader.call(
                asset.reference,
                "allowance(address,address)",
                block,
                inputs=("address", "address"),
                args=(target.fund.address, target.contracts.router),
            )[0],
        )
        for asset in (*target.assets, target.pool.shares_asset)
    )


def measured_quote(reader, target, block, amount):
    other = next(asset for asset in target.assets if asset != amount.asset)
    result = reads.quote(reader, target.pool, target.contracts, block, amount, other)
    return Quote(
        source=target,
        block=block,
        amount_in=amount,
        amount_out=AssetAmount(asset=other, amount=result),
        calculation="contract_router",
    )


def quotes_for(config, owned, quote, *, entry=True):
    """Decision quotes; `entry=False` skips the entry quote an exit or teardown cannot use."""
    allocation = config.allocation(owned.target.allocation)
    if allocation.adopt is not None:
        # An adopting allocation trades nothing. With no quote in its observations the
        # gateway's swap check has nothing to admit a swap along.
        return {}
    amount = entry_amount(allocation, owned) if entry else None
    result = (
        {} if amount is None else entry_or_reason(lambda: classified("entry_quote", quote(amount)))
    )
    counter = exit_amount(owned)
    if counter is not None:
        result.update(classified("exit_quote", quote(counter)))
    return result


def entry_amount(allocation, owned):
    """One entry slice, offered whenever funding inventory remains (blueprint 27 section 7).

    `entry_complete` records first exposure for accounting; it does not stop re-entry.
    """
    first = owned.target.funding_currency == owned.target.assets[0]
    funding = owned.inventory0 if first else owned.inventory1
    amount = min(funding.amount, allocation.capital.amount * allocation.entry_swap_bps // 10000)
    return AssetAmount(asset=funding.asset, amount=amount) if amount else None


def exit_amount(owned):
    counter = counter_of(owned)
    return counter if owned.position is None and counter.amount else None


def snapshot(backend, name, block, reference=None, *, quotes=True, residual=False, signed=False):
    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned = ownership(config, name, reference)
    if owned.target != target:
        raise ValueError("snapshot reference differs from configured Aerodrome target")
    if isinstance(reference, Snapshot):
        quotes = reference.quotes_included
    pool = reads.read_pool(reader, target.pool, block)
    verify_router(reader, target.pool, target.contracts, block)
    require_direct_fund(reader, target, block)
    for asset in (pool.currency0, pool.currency1, pool.shares):
        verify(config, asset)
    profile = config.accounts.binding(target.account).profile
    native = int(
        reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    wallets = tuple(
        AssetAmount(asset=asset, amount=reads.balance(reader, asset, target.fund.address, block))
        for asset in target.assets
    )
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=wallets[0],
        wallet1=wallets[1],
        allowances=allowances(reader, target, block),
        native_balance=AssetAmount(asset=config.allocation(name).native_asset, amount=native),
        payer=profile.submitter,
        position=position(reader, target, pool, signed=signed),
        quotes_included=quotes,
        **snapshot_quotes(
            config,
            owned,
            lambda amount: measured_quote(reader, target, block, amount),
            quotes,
            residual,
        ),
    )


def snapshot_quotes(config, owned, quote, quotes, residual):
    if quotes:
        return quotes_for(config, owned, quote)
    return residual_measurement(owned, quote) if residual else {}
