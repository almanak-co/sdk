"""Pinned periphery quotes and exact position reads through trusted transport."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import Digest
from almanak.backtest.types import UnixTime
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.math import sqrt_at_tick
from almanak.connectors.uniswap_v4.models import Range
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, Block, Model, UInt


class QuoterSource(Model):
    kind: Literal["archival_quoter"] = "archival_quoter"


class EconomicQuoteSource(Model):
    kind: Literal["economic_pool_calculation"] = "economic_pool_calculation"
    model: Literal["uniswap-v4-economic-integer-fixed-growth-v1"] = (
        "uniswap-v4-economic-integer-fixed-growth-v1"
    )
    simulation_time: UnixTime
    evidence: Annotated[tuple[Digest, ...], Field(min_length=1)]


class Quote(Model):
    block: Block
    amount: AssetAmount
    output: AssetAmount
    quoter_gas: UInt | None
    source: Annotated[QuoterSource | EconomicQuoteSource, Field(discriminator="kind")] = (
        QuoterSource()
    )

    @model_validator(mode="after")
    def provenance(self):
        if isinstance(self.source, QuoterSource) != (self.quoter_gas is not None):
            raise ValueError("only archival Quoter results include measured Quoter gas")
        if (
            self.amount.asset.chain != self.block.chain.chain_id
            or self.output.asset.chain != self.amount.asset.chain
        ):
            raise ValueError("quote amounts must retain their source chain")
        if (
            isinstance(self.source, EconomicQuoteSource)
            and self.source.simulation_time < self.block.timestamp
        ):
            raise ValueError("economic quote cannot use a future pool observation")
        return self


def quote(reader, target, amount, block):
    if amount.asset not in target.assets or not 0 < amount.amount < 2**128:
        raise ValueError("quote requires a positive uint128 input in the selected pool")
    direction = amount.asset == target.assets[0]
    output = target.assets[1 if direction else 0]
    result, gas = reader.call(
        target.contracts.quoter,
        f"quoteExactInputSingle({abi.QUOTE_PARAMS})",
        block,
        inputs=(abi.QUOTE_PARAMS,),
        args=((abi.pool_key(target.pool), direction, amount.amount, b""),),
        outputs=("uint256", "uint256"),
    )
    # The V4 Quoter reverts when liquidity cannot fill the whole input, so a zero
    # result consumed it all: a measured zero output, admitted only as exit residual.
    if result >= 2**128:
        raise DataError("quote", "Uniswap quote output exceeds uint128")
    return Quote(
        block=block, amount=amount, output=AssetAmount(asset=output, amount=result), quoter_gas=gas
    )


def wallet_balance(reader, target, asset, block):
    return reader.call(
        asset.reference,
        "balanceOf(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )[0]


def allowance(reader, target, asset, block):
    return reader.call(
        asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, target.contracts.permit2),
    )[0]


def permit_allowance(reader, target, asset, spender, block):
    return reader.call(
        target.contracts.permit2,
        "allowance(address,address,address)",
        block,
        inputs=("address", "address", "address"),
        args=(target.fund.address, asset.reference, spender),
        outputs=("uint160", "uint48", "uint48"),
    )


def signed_tick(value):
    tick = value & (2**24 - 1)
    return tick - 2**24 if tick >= 2**23 else tick


def position(reader, target, token_id, block, *, expected=None):
    """The retained NFT's liquidity and verified range; `expected` is the retained range."""
    owner = reader.call(
        target.contracts.position_manager,
        "ownerOf(uint256)",
        block,
        inputs=("uint256",),
        args=(token_id,),
        outputs=("address",),
    )[0]
    key, info = reader.call(
        target.contracts.position_manager,
        "getPoolAndPositionInfo(uint256)",
        block,
        inputs=("uint256",),
        args=(token_id,),
        outputs=(abi.POOL_KEY, "uint256"),
    )
    ticks = signed_tick(info >> 8), signed_tick(info >> 32)
    if not target.liquidity_enabled or (
        owner != target.fund.address or key != abi.pool_key(target.pool)
    ):
        raise DataError("ownership", "NFT owner, pool or range differs from the retained target")
    selected = verified_range(target, ticks, expected)
    liquidity = reader.call(
        target.contracts.position_manager,
        "getPositionLiquidity(uint256)",
        block,
        inputs=("uint256",),
        args=(token_id,),
        outputs=("uint128",),
    )[0]
    return liquidity, selected


def verified_range(target, ticks, expected):
    """A banded NFT retains its verified range; a fixed-range NFT must match its target."""
    if target.band is None:
        if ticks != (target.range.lower, target.range.upper):
            raise DataError("ownership", "NFT range differs from the retained fixed target range")
        return None
    try:
        selected = concrete_range(target, requested=Range(lower=ticks[0], upper=ticks[1]))
    except ValueError as exc:
        raise DataError("ownership", f"retained NFT range is outside the band: {exc}") from None
    if expected is not None and selected != expected:
        raise DataError("ownership", "retained NFT range differs from the retained position range")
    return selected


def fee_growth(reader, target, block, selected=None):
    bounds = position_range(target, selected)
    return reader.call(
        target.pool.state_view,
        "getFeeGrowthInside(bytes32,int24,int24)",
        block,
        inputs=("bytes32", "int24", "int24"),
        args=(bytes.fromhex(pool_id(target.pool)[2:]), bounds.lower, bounds.upper),
        outputs=("uint256", "uint256"),
    )


def position_growth(reader, target, token_id, block, selected=None):
    bounds = position_range(target, selected)
    liquidity, growth0, growth1 = reader.call(
        target.pool.state_view,
        "getPositionInfo(bytes32,address,int24,int24,bytes32)",
        block,
        inputs=("bytes32", "address", "int24", "int24", "bytes32"),
        args=(
            bytes.fromhex(pool_id(target.pool)[2:]),
            target.contracts.position_manager,
            bounds.lower,
            bounds.upper,
            token_id.to_bytes(32, "big"),
        ),
        outputs=("uint128", "uint256", "uint256"),
    )
    return liquidity, (growth0, growth1)


def position_range(target, selected):
    """The concrete ticks of a retained position: its own range or the fixed target range."""
    if selected is not None:
        return concrete_range(target, requested=selected)
    if target.range is None:
        raise ValueError("liquidity math requires a fixed range or a retained position range")
    return target.range


def concrete_range(target, *, position=None, requested=None):
    """The ticks a liquidity operation acts on: the fixed range or the band's selection."""
    if target.band is None:
        if target.range is None:
            raise ValueError("liquidity math requires an explicit range")
        if requested is not None and (requested.lower, requested.upper) != (
            target.range.lower,
            target.range.upper,
        ):
            raise ValueError("fixed-range liquidity cannot select another tick range")
        return target.range
    if position is not None:
        if position.range is None:
            raise ValueError("banded liquidity requires the retained position range")
        return position.range
    if requested is None:
        raise ValueError("banded liquidity requires an explicitly selected tick range")
    selected = target.band.validate_range(requested, target.pool.tick_spacing)
    return Range(lower=selected.lower, upper=selected.upper)


def centered_range(target, tick):
    """The band position centred on the observed tick, clamped inside the band's limits."""
    selected = target.band.centered(tick, target.pool.tick_spacing)
    return Range(lower=selected.lower, upper=selected.upper)


def sqrt_bounds(target, *, position=None, requested=None):
    selected = concrete_range(target, position=position, requested=requested)
    return sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
