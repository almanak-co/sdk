"""Retained V3 observations separate fund inventory, NFT obligations and payer funds."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import ModuleType

from pydantic import Field, model_validator

from almanak.connectors.dex_residual import (
    classified,
    entry_or_reason,
    residual_measurement,
    validate_entry_error,
    validate_quotes,
)
from almanak.connectors.v3_pool_domain import ConcentratedObservation
from almanak.connectors.v3_pool_models import Target
from almanak.connectors.v3_pool_positions import (
    Allowance,
    Ownership,
    Position,
    Quote,
    SimulatedPosition,
)
from almanak.models import Account, AssetAmount, Model


class Snapshot[T: Target, P: ConcentratedObservation, W: Ownership, Q: Quote](Model):
    target: T
    pool: P
    owned: W
    wallet0: AssetAmount
    wallet1: AssetAmount
    allowances: tuple[Allowance, ...]
    native_balance: AssetAmount | None
    payer: Account | None
    position: Position | SimulatedPosition | None
    entry_quote: Q | None = None
    exit_quote: Q | None = None
    # Absent unless the allocation is banded, so fixed-range snapshots and reports
    # keep the shape they had before banded liquidity existed.
    rebalance_quote: Q | None = Field(default=None, exclude_if=lambda v: v is None)
    # Counter inventory whose exact-size exit quote consumed its whole input and
    # returned nothing. It is measured residual dust, never a quote a swap can follow,
    # and it is absent unless measured so other snapshots keep their shape.
    exit_residual: Q | None = Field(default=None, exclude_if=lambda v: v is None)
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
            raise ValueError("snapshot must identify its exact configured target and ownership")
        if (self.wallet0.asset, self.wallet1.asset) != self.target.assets:
            raise ValueError("wallet observations must identify the pool currencies in order")
        self.validate_environment()
        self.validate_payer()
        self.validate_allowances()
        self.validate_quotes()
        return self

    def validate_environment(self):
        if not self.simulated and any(
            isinstance(p, SimulatedPosition) for p in (self.position, self.owned.position)
        ):
            raise ValueError("simulated liquidity cannot establish an on-chain observation")

    def validate_payer(self):
        if self.payer is not None and self.payer.chain != self.block.chain:
            raise ValueError("payer must retain the observed execution chain")

    def validate_allowances(self):
        expected = tuple(
            (a, s)
            for a in self.target.assets
            for s in (self.target.contracts.router, self.target.contracts.position_manager)
        )
        if tuple((a.asset, a.spender) for a in self.allowances) != expected:
            raise ValueError("allowance observations must cover each exact currency and spender")

    def validate_quotes(self):
        offered = (self.entry_quote, self.exit_quote, self.rebalance_quote)
        for value in (*offered, self.exit_residual):
            if value is not None and (value.source, value.block) != (self.target, self.block):
                raise ValueError("decision quotes must retain the observed market and block")
        validate_quotes(self.owned, offered, self.exit_quote, self.exit_residual)
        validate_entry_error(self.entry_quote, self.entry_quote_error)


def quotes_for(config, owned, quote, pool=None, *, entry=True):
    """Decision quotes; `entry=False` skips the entry quote an exit or teardown cannot use."""
    allocation = config.allocation(owned.target.allocation)
    result = entry_quotes(allocation, owned, quote) if entry else {}
    requests = {"exit_quote": exit_amount(owned), "rebalance_quote": rebalance_request(owned, pool)}
    for key, amount in requests.items():
        if amount is not None:
            result.update(classified(key, quote(amount)))
    return result


def entry_quotes(allocation, owned, quote):
    amount = entry_amount(allocation, owned)
    if amount is None:
        return {}
    return entry_or_reason(lambda: classified("entry_quote", quote(amount)))


def inventory_of(owned, asset):
    return next(value for value in (owned.inventory0, owned.inventory1) if value.asset == asset)


def entry_amount(allocation, owned):
    """One entry slice, offered whenever funding inventory remains (blueprint 27 section 7).

    `entry_complete` records first exposure for accounting; it does not stop re-entry.
    """
    funding = inventory_of(owned, owned.target.funding_currency)
    amount = min(funding.amount, slice_amount(allocation))
    return AssetAmount(asset=funding.asset, amount=amount) if amount else None


def slice_amount(allocation):
    return allocation.capital.amount * allocation.entry_swap_bps // 10000


def exit_amount(owned):
    counter = inventory_of(owned, owned.target.counter_currency)
    return counter if owned.position is None and counter.amount else None


def rebalance_request(owned, pool):
    if owned.position is not None or owned.target.band is None or pool is None:
        return None
    return rebalance_amount(owned, pool)


def rebalance_amount(owned, pool):
    """The single swap that leaves inventory in the ratio a centred position consumes.

    Solves for the input that makes both inventories deplete together at the current
    price, so the following addition wastes neither asset; the quoted output replaces
    the spot price before the strategy decides.
    """
    from fractions import Fraction

    from almanak.connectors.concentrated_math import amounts, sqrt_at_tick

    target = owned.target
    selected = target.band.centered(pool.tick, target.pool.tick_spacing)
    price = pool.sqrt_price_x96
    unit0, unit1 = amounts(2**96, price, sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper))
    held0, held1 = owned.inventory0.amount, owned.inventory1.amount
    if unit1 == 0:
        amount, asset = held1, owned.inventory1.asset
    elif unit0 == 0:
        amount, asset = held0, owned.inventory0.asset
    else:
        # A swap of x token0 at spot P yields x*P token1; the ratio condition
        # (held0 - x) / (held1 + x*P) == unit0 / unit1 solves to the expression below.
        spot = Fraction(price * price, 2**192)
        surplus0 = Fraction(held0 * unit1 - held1 * unit0)
        if surplus0 > 0:
            amount, asset = int(surplus0 / (unit1 + unit0 * spot)), owned.inventory0.asset
        else:
            amount, asset = int(-surplus0 / (unit0 + unit1 / spot)), owned.inventory1.asset
    amount = min(amount, held0 if asset == owned.inventory0.asset else held1)
    return AssetAmount(asset=asset, amount=amount) if amount > 0 else None


@dataclass(frozen=True)
class Observations:
    snapshot_type: type[Snapshot]
    ownership_type: type[Ownership]
    quote_type: type[Quote]
    reads: ModuleType
    verify_periphery: Callable

    def initial_owned(self, config, name):
        allocation = config.allocation(name)
        values = tuple(
            AssetAmount(
                asset=asset,
                amount=allocation.capital.amount if asset == allocation.capital.asset else 0,
            )
            for asset in (allocation.pool.currency0, allocation.pool.currency1)
        )
        return self.ownership_type(
            target=config.target(name), inventory0=values[0], inventory1=values[1]
        )

    def ownership(self, config, name, reference):
        if reference is None:
            return self.initial_owned(config, name)
        return reference if isinstance(reference, self.ownership_type) else reference.owned

    def allowances(self, reader, target, block):
        return tuple(
            Allowance(
                asset=asset,
                spender=spender,
                amount=self.reads.allowance(reader, target, asset, spender, block),
            )
            for asset in target.assets
            for spender in (target.contracts.router, target.contracts.position_manager)
        )

    def decision_quotes(self, reader, config, owned, pool):
        return quotes_for(
            config,
            owned,
            lambda amount: self.measured_quote(reader, owned.target, pool.block, amount),
            pool,
        )

    def measured_quote(self, reader, target, block, amount):
        other = next(asset for asset in target.assets if asset != amount.asset)
        output, price, ticks, _ = self.reads.quote(
            reader, target.pool, target.contracts, block, amount, other
        )
        return self.quote_type(
            source=target,
            block=block,
            amount_in=amount,
            amount_out=AssetAmount(asset=other, amount=output),
            sqrt_price_after=price,
            initialized_ticks_crossed=ticks,
            calculation="contract_quoter",
        )

    def snapshot(self, backend, name, block, reference=None, *, quotes=True, residual=False):
        config, reader = backend.config, backend.reader
        target = config.target(name)
        owned = self.ownership(config, name, reference)
        if owned.target != target:
            raise ValueError("snapshot reference differs from the configured V3 target")
        if isinstance(reference, self.snapshot_type):
            quotes = reference.quotes_included
        pool = self.reads.read_pool(reader, target.pool, block)
        self.verify_periphery(reader, target.pool, target.contracts, block)
        from almanak.configuration.metadata import verify

        verify(config, pool.currency0)
        verify(config, pool.currency1)
        profile = config.accounts.binding(target.account).profile
        native = int(
            reader._rpc(
                "eth_getBalance",
                [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
            ),
            16,
        )
        wallets = self.wallet_balances(reader, target, block)
        position = (
            self.reads.nft_position(
                reader, target, owned.position.nft, block, expected=owned.position.range
            )
            if owned.position is not None
            else None
        )
        return self.snapshot_type(
            target=target,
            pool=pool,
            owned=owned,
            wallet0=wallets[0],
            wallet1=wallets[1],
            allowances=self.allowances(reader, target, block),
            native_balance=AssetAmount(asset=config.allocation(name).native_asset, amount=native),
            payer=profile.submitter,
            position=position,
            quotes_included=quotes,
            **self.snapshot_quotes(reader, config, owned, pool, quotes, residual),
        )

    def snapshot_quotes(self, reader, config, owned, pool, quotes, residual):
        if quotes:
            return self.decision_quotes(reader, config, owned, pool)
        if residual:
            return residual_measurement(
                owned, lambda amount: self.measured_quote(reader, owned.target, pool.block, amount)
            )
        return {}

    def wallet_balances(self, reader, target, block):
        return tuple(
            AssetAmount(asset=asset, amount=self.reads.wallet_balance(reader, target, asset, block))
            for asset in target.assets
        )
