"""Pinned pool, fund inventory, LP ownership and distinct submitter evidence."""

from datetime import UTC, datetime

from pydantic import Field, model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.configuration.metadata import verify
from almanak.connectors.curve import reader as reads
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.domain import PoolObservation
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.positions import Allowance, Ownership, Position, Quote
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.dex_residual import entry_or_reason, validate_entry_error
from almanak.models import Account, AssetAmount, Model


class Snapshot(Model):
    target: Target
    pool: PoolObservation | CryptoObservation | TriCryptoObservation
    owned: Ownership
    wallets: AssetAmounts
    allowances: tuple[Allowance, ...]
    native_balance: AssetAmount | None
    payer: Account | None
    position: Position | None
    entry_quote: Quote | None = None
    exit_quotes: tuple[Quote, ...] = ()
    # Non-funding inventory whose exact-size exit quote returned nothing: measured
    # residual dust, absent unless measured so other snapshots keep their shape.
    exit_residuals: tuple[Quote, ...] = Field(default=(), exclude_if=lambda v: not v)
    # Why no entry quote is offered although an entry slice remains, when the market
    # cannot fill it at this block; absent otherwise so other snapshots keep their shape.
    entry_quote_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    # LP shares of the pool that the fund holds beside a target without liquidity
    # authority. They are outside the allocation and never owned; absent unless held,
    # so other snapshots keep their shape. They refuse work that is not yet signed
    # (`ownership.refuse_unowned_shares`) and are an accounting issue on an outcome.
    unowned_shares: AssetAmount | None = Field(default=None, exclude_if=lambda v: v is None)
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
        """Measured zero-output exit quotes of residual non-funding dust."""
        return self.exit_residuals

    @property
    def needs_cleanup(self):
        return any(value.amount for value in self.allowances)

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.pool.source != self.target.pool:
            raise ValueError("Curve snapshot must retain its configured target and ownership")
        if isinstance(self.pool, (CryptoObservation, TriCryptoObservation)) and (
            self.pool.accounting.receiver == self.target.fund.address
        ):
            raise ValueError("Crypto fee receiver must be distinct from the strategy fund")
        self.wallets.in_order(self.target.assets)
        if self.payer is not None and self.payer.chain != self.block.chain:
            raise ValueError("Curve submitter must retain the observed execution chain")
        if self.position is not None:
            self.position.validate_target(self.target)
        self.validate_unowned()
        expected = tuple((asset, self.target.pool.pool) for asset in self.target.assets)
        if tuple((value.asset, value.spender) for value in self.allowances) != expected:
            raise ValueError("Curve allowances must retain ordered pool coins and their spender")
        self.validate_quotes()
        return self

    def validate_unowned(self):
        held = self.unowned_shares
        if held is None:
            return
        if self.target.liquidity_enabled or held.asset != self.target.pool.shares_asset:
            raise ValueError("unowned Curve LP shares belong beside a trading-only target")
        if not 0 < held.amount < 2**256:
            raise ValueError("unowned Curve LP shares are recorded only when held")

    def validate_quotes(self):
        offered = (
            self.exit_quotes if self.entry_quote is None else (self.entry_quote, *self.exit_quotes)
        )
        self.validate_scope((*offered, *self.exit_residuals))
        self.validate_exits((*self.exit_quotes, *self.exit_residuals))
        if any(not quote.amount_out.amount for quote in offered):
            raise ValueError("a Curve decision quote requires a positive measured output")
        self.validate_residuals()
        validate_entry_error(self.entry_quote, self.entry_quote_error)

    def validate_scope(self, quotes):
        if any((q.source, q.block) != (self.target, self.block) for q in quotes):
            raise ValueError("Curve quotes must retain the snapshot's pool and block")

    def validate_exits(self, exits):
        if len({quote.amount_in.asset for quote in exits}) != len(exits):
            raise ValueError("Curve exit quotes cannot repeat a funding input")
        if any(quote.amount_out.asset != self.target.funding_asset for quote in exits):
            raise ValueError("Curve exit quotes must return to the configured funding asset")

    def validate_residuals(self):
        if self.exit_residuals and self.owned.position is not None:
            raise ValueError("Curve exit residuals require closed liquidity")
        if any(whole_zero(quote, self.owned) is False for quote in self.exit_residuals):
            raise ValueError("a Curve exit residual measures one whole balance with no output")


def whole_zero(quote, owned):
    """A residual measures one whole owned balance and returns nothing."""
    return not quote.amount_out.amount and quote.amount_in.amount == owned.amount(
        quote.amount_in.asset
    )


def held_counters(owned):
    """Positive balances of every coin other than the funding asset."""
    funding = owned.target.funding_asset
    return tuple(v for v in owned.inventory.root if v.asset != funding and v.amount)


def unsettled(owned, residuals):
    """Non-funding inventory not exactly covered by a zero-output exit measurement."""
    measured = [quote.amount_in for quote in residuals]
    return tuple(value for value in held_counters(owned) if value not in measured)


def initial_owned(config, name):
    return Ownership(target=config.target(name), inventory=config.allocation(name).capital)


def ownership(config, name, reference):
    if reference is None:
        return initial_owned(config, name)
    return reference if isinstance(reference, Ownership) else reference.owned


def token_balance(reader, asset, owner, block):
    return reader.call(
        asset.reference, "balanceOf(address)", block, inputs=("address",), args=(owner,)
    )[0]


def held_shares(reader, target, block):
    """The fund's balance of the pool's LP token: the position of a target with liquidity
    authority, and unowned shares beside one without. Each is absent at zero."""
    value = reads.fund_shares(reader, target.pool, target.fund, block)
    if value == 0:
        return {"position": None}
    shares = AssetAmount(asset=target.pool.shares_asset, amount=value)
    if target.liquidity_enabled:
        return {"position": Position(shares=shares)}
    return {"position": None, "unowned_shares": shares}


def allowances(reader, target, block):
    return tuple(
        Allowance(
            asset=asset,
            spender=target.pool.pool,
            amount=reader.call(
                asset.reference,
                "allowance(address,address)",
                block,
                inputs=("address", "address"),
                args=(target.fund.address, target.pool.pool),
            )[0],
        )
        for asset in target.assets
    )


def measured_quote(reader, target, block, amount, output_asset):
    result = reads.quote(reader, target.pool, block, amount, output_asset)
    return Quote(
        source=target,
        block=block,
        amount_in=amount,
        amount_out=AssetAmount(asset=output_asset, amount=result),
        calculation="contract_pool",
    )


def quotes_for(config, owned, quote, *, entry=True):
    """Decision quotes; `entry=False` skips the entry quote an exit or teardown cannot use."""
    allocation = config.allocation(owned.target.allocation)
    if allocation.adopt:
        # An adopting allocation trades nothing. With no quote in its observations the
        # gateway's swap check has nothing to admit a swap along.
        return {}
    result = entry_or_reason(lambda: entry_measurement(allocation, owned, quote)) if entry else {}
    if owned.position is None:
        result.update(exit_split(owned, quote))
    return result


def entry_measurement(allocation, owned, quote):
    """The entry quote, absent when the entry amount or its measured output is zero.

    Entries repeat: one slice of capital, capped by the funding inventory, is offered
    before and after other entries and exits. `entry_complete` only records the first
    exposure.
    """
    target = owned.target
    capital = next(v.amount for v in allocation.capital.root if v.asset == target.funding_asset)
    amount = min(owned.amount(target.funding_asset), capital * allocation.entry_swap_bps // 10000)
    if not amount:
        return {}
    measured = quote(AssetAmount(asset=target.funding_asset, amount=amount), target.trading_asset)
    return {"entry_quote": measured} if measured.amount_out.amount else {}


def exit_split(owned, quote):
    """Positive exit quotes are offered; zero-output ones are kept as residuals."""
    offered: list[Quote] = []
    residuals: list[Quote] = []
    for value in held_counters(owned):
        measured = quote(value, owned.target.funding_asset)
        (offered if measured.amount_out.amount else residuals).append(measured)
    result = {"exit_quotes": tuple(offered)}
    if residuals:
        result["exit_residuals"] = tuple(residuals)
    return result


def measured_or_none(quote, value, asset):
    from almanak.gateway.ethereum import DataError

    try:
        return quote(value, asset)
    except DataError:
        return None


def residual_measurement(owned, quote):
    """Exit residuals after an exit leg; an unavailable quote leaves its balance unmeasured."""
    if owned.position is not None:
        return {}
    residuals = tuple(q for q in available_exits(owned, quote) if not q.amount_out.amount)
    return {"exit_residuals": residuals} if residuals else {}


def available_exits(owned, quote):
    funding = owned.target.funding_asset
    measured = (measured_or_none(quote, value, funding) for value in held_counters(owned))
    return tuple(q for q in measured if q is not None)


def snapshot(backend, name, block, reference=None, *, quotes=True, residual=False):
    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned = ownership(config, name, reference)
    if owned.target != target:
        raise ValueError("snapshot reference differs from configured Curve target")
    if isinstance(reference, Snapshot):
        quotes = reference.quotes_included
    pool = reads.read_pool(reader, target.pool, block)
    for asset in (*pool.currencies, pool.shares):
        verify(config, asset)
    profile = config.accounts.binding(target.account).profile
    native = int(
        reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    wallets = AssetAmounts(
        tuple(
            AssetAmount(
                asset=asset, amount=token_balance(reader, asset, target.fund.address, block)
            )
            for asset in target.assets
        )
    )
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallets=wallets,
        allowances=allowances(reader, target, block),
        native_balance=AssetAmount(asset=config.allocation(name).native_asset, amount=native),
        payer=profile.submitter,
        quotes_included=quotes,
        **held_shares(reader, target, block),
        **snapshot_quotes(
            config,
            owned,
            lambda amount, output: measured_quote(reader, target, block, amount, output),
            quotes,
            residual,
        ),
    )


def snapshot_quotes(config, owned, quote, quotes, residual):
    if quotes:
        return quotes_for(config, owned, quote)
    return residual_measurement(owned, quote) if residual else {}
