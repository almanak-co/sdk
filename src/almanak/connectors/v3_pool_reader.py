"""Verified V3 position-manager and ERC-20 observations after revision admission."""

from almanak.gateway.ethereum import DataError


def nft_position(reader, target, nft, block, *, expected=None):
    return keyed_nft_position(
        reader, target, nft, block, "uint24", target.pool.fee, expected=expected
    )


def keyed_nft_position(reader, target, nft, block, key_type, key_value, *, expected=None):
    from almanak.caip import AssetType
    from almanak.connectors.v3_pool_positions import Position

    manager = AssetType(f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}")
    if nft.asset_type != manager or not target.liquidity_enabled:
        raise DataError("ownership", "NFT must identify the configured V3 manager and range")
    token = int(nft.token_id)
    (owner,) = reader.call(
        target.contracts.position_manager,
        "ownerOf(uint256)",
        block,
        inputs=("uint256",),
        args=(token,),
        outputs=("address",),
    )
    if owner != target.fund.address:
        raise DataError("ownership", "retained V3 NFT is not owned by the named fund")
    result = reader.call(
        target.contracts.position_manager,
        "positions(uint256)",
        block,
        inputs=("uint256",),
        args=(token,),
        outputs=(
            "uint96",
            "address",
            "address",
            "address",
            key_type,
            "int24",
            "int24",
            "uint128",
            "uint256",
            "uint256",
            "uint128",
            "uint128",
        ),
    )
    if result[2:5] != (target.pool.currency0.reference, target.pool.currency1.reference, key_value):
        raise DataError("ownership", "retained NFT differs from the configured V3 pool or range")
    return Position(
        nft=nft,
        liquidity=result[7],
        owed0=result[10],
        owed1=result[11],
        fee_growth=(result[8], result[9]),
        range=verified_range(target, result[5:7], expected),
    )


def verified_range(target, ticks, expected):
    """A banded NFT retains its verified range; a fixed-range NFT must match its target."""
    from almanak.connectors.v3_pool_domain import TickRange

    if target.band is None:
        if ticks != (target.range.lower, target.range.upper):
            raise DataError("ownership", "retained NFT differs from the configured V3 range")
        return None
    try:
        selected = target.band.validate_range(
            TickRange(lower=ticks[0], upper=ticks[1]), target.pool.tick_spacing
        )
    except ValueError as exc:
        raise DataError("ownership", f"retained NFT range is outside the band: {exc}") from None
    if expected is not None and selected != expected:
        raise DataError("ownership", "retained NFT range differs from the retained position range")
    return selected


def wallet_balance(reader, target, asset, block):
    return reader.call(
        asset.reference,
        "balanceOf(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )[0]


def allowance(reader, target, asset, spender, block):
    return reader.call(
        asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, spender),
    )[0]


def consumed_quote(result, source, amount, label):
    """A QuoterV2 exact-input result whose zero output consumed the whole input.

    With a zero price limit the Quoter swaps toward MIN_SQRT_RATIO + 1 or
    MAX_SQRT_RATIO - 1 and stops there when liquidity runs out, returning a partial
    fill. A zero output with the price still inside that limit consumed the whole
    input, so it measures residual dust rather than missing liquidity.
    """
    from almanak.connectors.concentrated_math import sqrt_at_tick

    zero_for_one = amount.asset == source.currency0
    limit = sqrt_at_tick(-887272) + 1 if zero_for_one else sqrt_at_tick(887272) - 1
    if not result[0] and result[1] == limit:
        raise DataError("liquidity", f"{label} Quoter returned no output for requested input")
    return result
