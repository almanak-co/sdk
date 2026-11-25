"""Read-only protocol observations at a pinned block.

The parser has already validated `--block` into a block number, `finalized` or
`latest`; the reader pins that selector to one concrete header and every read
names its hash, so all values in one observation come from the same block.
HyperCore observations are the exception: its API has no block to pin, so they
name the time each answer was received.
"""

import json

from almanak import ax_bridge, ax_discovery, ax_hypercore, ax_lending_markets, ax_portfolio
from almanak.balances import parse_balance_request, read_balance
from almanak.caip import AssetType, ChainId
from almanak.cli_arguments import (
    CONNECTOR_INLINE_FLAGS,
    CONNECTOR_INLINE_OPTIONS,
    INLINE_FLAGS,
    LENDING_SELECTOR_FLAGS,
)
from almanak.connectors.aerodrome.position_request import (
    PositionRequest as AerodromePositionRequest,
)
from almanak.connectors.curve.position_request import PositionRequest as CurvePositionRequest
from almanak.connectors.discovery import pool_connectors
from almanak.connectors.gmx_v2.market_request import GmxMarketRequest, read_gmx_market
from almanak.connectors.gmx_v2.position_request import GmxPositionRequest, read_gmx_position
from almanak.connectors.lending_lookup import (
    read_lending_position,
    read_lending_reserve,
    selection,
)
from almanak.connectors.lending_risk import RiskQuery
from almanak.connectors.uniswap_v4.custody import custody_asset
from almanak.connectors.uniswap_v4.domain import PoolPriceRequest
from almanak.connectors.uniswap_v4.hook_terms import describe
from almanak.connectors.uniswap_v4.networks import roles_for
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.position_request import (
    PositionRequest as KeyedPositionRequest,
)
from almanak.connectors.v3_pool_position_request import PositionRequest, read_liquidity_position
from almanak.connectors.v3_pool_request import (
    RESOLVED_POOL_CONNECTORS,
    PoolRequest,
    read_pool_state,
    read_resolved_pool,
    support_level,
)
from almanak.gateway.auth import read_token
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.evm import native_asset
from almanak.gateway.hypercore_info import GatewayInfo, HyperCoreInfo
from almanak.gateway.observation_gateway import gateway_reader
from almanak.gateway.onchain_lookup import GatewayLookups, VendorLookups
from almanak.gateway.rabby_portfolio import RabbyPortfolio
from almanak.gateway.read_endpoints import public_reader, verified_reader
from almanak.models import ZERO_ADDRESS, Account, Chain, Model

# Request model per (product, connector). The parser's --connector choices in
# cli_arguments.py must cover exactly these pairs; tests assert they match.
# Lending requests are each connector's own market selection (lending_lookup).
CONNECTOR_REQUESTS: dict[tuple[str, str], type[Model]] = {
    ("ax-pool-price", "uniswap_v4"): PoolPriceRequest,
    ("ax-perp-position", "gmx_v2"): GmxPositionRequest,
    ("ax-perp-markets", "gmx_v2"): GmxMarketRequest,
    # Every concentrated or resolved pool connector takes the same pool request.
    **{("ax-pool", c): PoolRequest for c in pool_connectors("concentrated", "resolved")},
    # A position is an NFT of a V3-style position manager.
    **{("ax-liquidity-position", c): PositionRequest for c in pool_connectors("concentrated")},
    # A Curve or an Aerodrome position is an account's balance of the pool's LP token.
    ("ax-liquidity-position", "curve"): CurvePositionRequest,
    ("ax-liquidity-position", "aerodrome"): AerodromePositionRequest,
    # A Uniswap V4 position is an NFT whose pool is named by its key.
    ("ax-liquidity-position", "uniswap_v4"): KeyedPositionRequest,
}


def command(args) -> int:
    handlers = {
        "ax-pool-price": pool_price,
        "ax-lending-reserve": lending_reserve,
        "ax-lending-position": lending_position,
        "ax-balance": balance,
        "ax-perp-position": perp_position,
        "ax-perp-markets": perp_markets,
        "ax-pool": pool,
        "ax-liquidity-position": liquidity_position,
        "ax-resolve": token_resolve,
        "ax-dex-pools": pool_discovery,
        "ax-lending-markets": lending_markets,
        "ax-hypercore-markets": hypercore_markets,
        "ax-hypercore-account": hypercore_account,
        "ax-portfolio": portfolio,
        "ax-bridge-status": ax_bridge.status,
    }
    print(json.dumps(handlers[args.product](args), indent=2))
    return 0


def pool_price(args):
    request = parse_request(args)
    chain = live_chain(request.source.chain)
    with observation_reader(args, chain.chain_id) as reader:
        block = reader.pin_block(args.block)
        pool = read_pool(reader, request.source, block)
        price = pool.spot_price(request)
        return {
            "product": "pool_spot_price",
            "request": request.model_dump(mode="json"),
            "price": {
                "numerator": str(price.numerator),
                "denominator": str(price.denominator),
            },
            "observation": pool.model_dump(mode="json"),
            **describe(reader, request.source, block),
        }


def lending_reserve(args):
    request = lending_request(args)
    with observation_reader(args, request.chain) as reader:
        block = reader.pin_block(args.block)
        return read_lending_reserve(reader, request, block, risk_query(args))


def risk_query(args) -> RiskQuery:
    return RiskQuery(tuple(args.borrow_fraction or ()), args.health_floor)


def lending_position(args):
    request = lending_request(args)
    account = Account(chain=live_chain(mainnet(request.chain)), address=args.account)
    with observation_reader(args, request.chain) as reader:
        return read_lending_position(reader, request, account, reader.pin_block(args.block))


def lending_request(args):
    if args.config is not None:
        values = json.loads(args.config.read_text())
    else:
        values = {"chain": args.chain}
        values.update(
            (name, getattr(args, name))
            for name in LENDING_SELECTOR_FLAGS
            if getattr(args, name) is not None
        )
    return selection(args.connector, values)


def balance(args):
    requests = [parse_balance_request(args.asset, value, None) for value in args.account]
    asset = requests[0][0]
    with observation_reader(args, asset.chain) as reader:
        # Pin once so every account is read at the same block, not the head
        # each read would otherwise resolve for a `latest` selector.
        block = reader.pin_block(args.block)
        observations = [
            read_balance(reader, asset, account, block.number) for _, account in requests
        ]
    if any(observation.block != block for observation in observations):
        raise ValueError("balance reads moved off their pinned block")
    return {
        "product": args.product,
        "observations": [observation.model_dump(mode="json") for observation in observations],
    }


def perp_position(args):
    request = parse_request(args)
    chain = live_chain(request.market.chain)
    account = Account(chain=chain, address=args.account)
    with observation_reader(args, chain.chain_id) as reader:
        block = reader.pin_block(args.block)
        return read_gmx_position(reader, request, account, block)


def perp_markets(args):
    request = parse_request(args)
    chain = live_chain(mainnet(request.chain))
    with observation_reader(args, chain.chain_id) as reader:
        block = reader.pin_block(args.block)
        return read_gmx_market(reader, request, block)


def pool(args):
    request = parse_request(args)
    chain = live_chain(mainnet(request.chain))
    support_level(args.connector, request.chain)
    resolved_only = args.connector in RESOLVED_POOL_CONNECTORS
    if resolved_only and args.depth_bps is not None:
        raise ValueError("--depth-bps is available for V3-style concentrated-liquidity pools")
    with observation_reader(args, chain.chain_id) as reader:
        block = reader.pin_block(args.block)
        if resolved_only:
            return read_resolved_pool(reader, args.connector, request, block)
        return read_pool_state(reader, args.connector, request, block, args.depth_bps)


def liquidity_position(args):
    request = parse_request(args)
    chain = live_chain(mainnet(request.chain))
    support_level(args.connector, request.chain)
    with observation_reader(args, chain.chain_id) as reader:
        block = reader.pin_block(args.block)
        if not isinstance(request, PositionRequest):
            return account_position_read(args.connector)(reader, request, block)
        return read_liquidity_position(reader, args.connector, request, block)


def account_position_read(connector: str):
    """The position read of a connector whose position is an account's LP token balance:
    its own `position_request.read_liquidity_position(reader, request, block)`."""
    from importlib import import_module

    return import_module(f"almanak.connectors.{connector}.position_request").read_liquidity_position


def token_resolve(args):
    chain = live_chain(mainnet(ChainId(args.chain))).chain_id
    with lookup_source(args) as lookups, observation_reader(args, chain) as reader:
        block = reader.pin_block(args.block)
        return ax_discovery.resolve(lookups, reader, block, args.query, args.limit)


def pool_discovery(args):
    chain = live_chain(mainnet(ChainId(args.chain))).chain_id
    query = ax_discovery.PoolQuery(
        token=pool_token_on(chain, args.token, "--token"),
        quote=None if args.quote is None else pool_token_on(chain, args.quote, "--quote"),
        connector=args.connector_filter,
        limit=args.limit,
        depth_bps=args.depth_bps,
        offset=args.offset,
    )
    with lookup_source(args) as lookups, observation_reader(args, chain) as reader:
        block = reader.pin_block(args.block)
        return ax_discovery.dex_pools(lookups, reader, block, query)


def lending_markets(args):
    chain = live_chain(mainnet(ChainId(args.chain))).chain_id
    query = ax_lending_markets.MarketQuery(
        connector=args.connector,
        asset=None if args.asset is None else erc20_on(chain, args.asset, "--asset"),
        collateral=None
        if args.collateral is None
        else erc20_on(chain, args.collateral, "--collateral"),
        limit=args.limit,
        offset=args.offset,
        include_unlisted=args.include_unlisted,
    )
    with lookup_source(args) as lookups, observation_reader(args, chain) as reader:
        block = reader.pin_block(args.block)
        return ax_lending_markets.lending_markets(reader, lookups, block, query)


def hypercore_markets(args):
    with info_source(args) as info:
        return ax_hypercore.markets(
            info, args.query, args.limit, args.offset, args.include_delisted
        )


def hypercore_account(args):
    with info_source(args) as info:
        return ax_hypercore.accounts(info, args.account)


def portfolio(args):
    with portfolio_source() as source:
        return ax_portfolio.portfolio(source, args.account, args.include_unlisted)


def portfolio_source() -> RabbyPortfolio:
    # The vendor needs no key and no chain endpoint, so this read takes no transport.
    return RabbyPortfolio()


def asset_on(chain: ChainId, value: str, flag: str) -> AssetType:
    asset = AssetType(value)
    if asset.chain != chain:
        raise ValueError(f"{flag} must be an asset on {chain}")
    return asset


def erc20_on(chain: ChainId, value: str, flag: str) -> AssetType:
    # Pools and lending markets are recorded by token contract; the wrapped
    # native token stands for the native asset there.
    asset = asset_on(chain, value, flag)
    if asset.namespace != "erc20":
        raise ValueError(f"{flag} must be an ERC-20 asset; use the wrapped native token")
    return asset


def pool_token_on(chain: ChainId, value: str, flag: str) -> AssetType:
    # Pools are filtered by the token a fund holds: the native coin asks for its wrapped
    # token, whose filter matches both wrapped and native-coin pools.
    asset = asset_on(chain, value, flag)
    if asset.namespace == "slip44" and asset == native_asset(chain):
        return custody_asset(asset)
    return erc20_on(chain, value, flag)


def parse_request(args):
    try:
        model = CONNECTOR_REQUESTS[(args.product, args.connector)]
    except KeyError:
        raise ValueError(
            f"unsupported connector '{args.connector}' for ax {args.ax_command}"
        ) from None
    if args.config is not None:
        return model.model_validate_json(args.config.read_text())
    # Inline flags validate through the same JSON path as a --config file, so
    # both forms share one contract and one error shape.
    return model.model_validate_json(json.dumps(inline_request(args)))


def inline_request(args) -> dict:
    if args.product == "ax-pool":
        return {"chain": args.chain, "pool": args.pool}
    if args.product == "ax-perp-markets":
        return {"chain": args.chain, "query": args.query}
    if args.product == "ax-liquidity-position":
        # An NFT position is named by its token ID, an LP token balance by its account.
        shape = (args.product, args.connector)
        names = CONNECTOR_INLINE_FLAGS.get(shape, INLINE_FLAGS[args.product])
        optional = CONNECTOR_INLINE_OPTIONS.get(shape, ())
        given = [name for name in optional if getattr(args, name) is not None]
        return {name: getattr(args, name) for name in (*names, *given)}
    return pool_price_request(args)


def pool_price_request(args) -> dict:
    """A pool key on the reviewed deployment, hookless unless --hooks names a reviewed hook."""
    roles = roles_for(ChainId(args.chain))
    base = args.base or args.currency0
    # CAIP-19 lowercases ERC-20 references, so compare assets, not the typed strings.
    first = AssetType(base) == AssetType(args.currency0)
    return {
        "source": {
            "kind": "uniswap_v4",
            "chain": {"chain_id": args.chain, "environment": "mainnet"},
            "pool_manager": roles.pool_manager,
            "state_view": roles.state_view,
            "currency0": args.currency0,
            "currency1": args.currency1,
            "fee": args.fee,
            "tick_spacing": args.tick_spacing,
            "hooks": args.hooks or ZERO_ADDRESS,
        },
        "base": base,
        "quote": args.currency1 if first else args.currency0,
    }


def lookup_source(args):
    # The same transport as the reads: the operator's env file, or the read
    # gateway, which keeps the vendor key in its own process.
    if args.env_file is not None:
        return VendorLookups(args.env_file)
    if public_rpc(args) is not None:
        return VendorLookups(None)
    return GatewayLookups(args.gateway_url, read_token(args.gateway_auth_file))


def info_source(args):
    # The same transport as the reads. Hyperliquid's API needs no key, so an env file
    # only selects querying it from this process instead of through the gateway.
    if args.env_file is not None or public_rpc(args) is not None:
        return HyperCoreInfo()
    return GatewayInfo(args.gateway_url, read_token(args.gateway_auth_file))


def observation_reader(args, chain: ChainId) -> EthereumReader:
    # A session without a provider key reads through the observation
    # gateway, which keeps the credential in its own process.
    if args.env_file is not None:
        return verified_reader(chain, args.env_file)
    name = public_rpc(args)
    if name is not None:
        return public_reader(chain, name)
    if args.gateway_auth_file is None:
        raise ValueError("--gateway-url requires --gateway-auth-file")
    return gateway_reader(chain, args.gateway_url, args.gateway_auth_file)


def public_rpc(args) -> str | None:
    return getattr(args, "public_rpc", None)


def mainnet(chain_id: ChainId) -> Chain:
    return Chain(chain_id=chain_id, environment="mainnet")


def live_chain(chain):
    # The read transport serves live mainnet endpoints only, directly or through
    # a read-only observation gateway; fork and backtest observations flow
    # through their own gateways, never this CLI.
    if chain.environment != "mainnet":
        raise ValueError("ax reads live endpoints; the request chain environment must be mainnet")
    return chain
