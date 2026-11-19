"""Cross-checks of `almanak ax` read output against direct reads at the same block.

Each check takes the command's JSON output and a direct RPC client for the same
endpoint, and returns one line per disagreement. Raising `RpcUnavailable` blocks
the case instead of failing it: the endpoint, not the SDK, did not answer.
"""

from collections.abc import Callable, Iterable
from typing import Any

from eth_abi import encode
from eth_utils import keccak

from scripts.live_qa.rpc import Rpc, RpcUnavailable, asset_address

Output = dict[str, Any]
Check = Callable[[Output, Rpc], list[str]]

ZERO_ADDRESS = "0x" + "00" * 20
# Refusal prefixes that name the endpoint, as `error: message` reasons carry them.
ENDPOINT_REASONS = ("rpc:", "http:", "transport:", "unavailable:", "stale:")


def differences(pairs: Iterable[tuple[str, Any, Any]]) -> list[str]:
    return [
        f"{label}: CLI {reported!r}, direct read {direct!r}"
        for label, reported, direct in pairs
        if reported != direct
    ]


def same_block(observation: Output, rpc: Rpc) -> tuple[str, Any, Any]:
    block = observation["block"]
    return ("block hash", block["hash"], rpc.block_hash(block["number"]))


def native_balance(output: Output, rpc: Rpc) -> list[str]:
    pairs = []
    for observation in output["observations"]:
        number = observation["block"]["number"]
        account = observation["account"]["address"]
        pairs.append(("balance", int(observation["amount"]), rpc.balance(account, number)))
        pairs.append(same_block(observation, rpc))
    return differences(pairs)


def erc20_balance(output: Output, rpc: Rpc) -> list[str]:
    pairs = []
    for observation in output["observations"]:
        number = observation["block"]["number"]
        token = asset_address(observation["asset"])
        account = observation["account"]["address"]
        (held,) = rpc.call(token, "balanceOf(address)", number, ("uint256",), (account,))
        (decimals,) = rpc.call(token, "decimals()", number, ("uint8",))
        pairs.append(("balance", int(observation["amount"]), held))
        pairs.append(("decimals", observation["decimals"], decimals))
        pairs.append(same_block(observation, rpc))
    return differences(pairs)


def v3_pool(output: Output, rpc: Rpc) -> list[str]:
    observation = output["observation"]
    pool, number = output["resolution"]["pool"], observation["block"]["number"]
    slot0 = rpc.call(pool, "slot0()", number, ("uint160", "int24"))
    (liquidity,) = rpc.call(pool, "liquidity()", number, ("uint128",))
    (token0,) = rpc.call(pool, "token0()", number, ("address",))
    (fee,) = rpc.call(pool, "fee()", number, ("uint24",))
    return differences(
        (
            ("sqrt_price_x96", observation["sqrt_price_x96"], slot0[0]),
            ("tick", observation["tick"], slot0[1]),
            ("liquidity", observation["liquidity"], liquidity),
            ("currency0", asset_address(observation["currency0"]["identifier"]), token0.lower()),
            ("fee", output["resolution"]["fee"], fee),
            same_block(observation, rpc),
        )
    )


def v4_pool_id(source: Output) -> bytes:
    """A Uniswap V4 pool ID: the hash of its ABI-encoded key."""
    key = (
        asset_address(source["currency0"]),
        asset_address(source["currency1"]),
        source["fee"],
        source["tick_spacing"],
        source["hooks"],
    )
    return keccak(encode(("address", "address", "uint24", "int24", "address"), key))


def v4_pool_price(output: Output, rpc: Rpc) -> list[str]:
    observation = output["observation"]
    source, number = observation["source"], observation["block"]["number"]
    slot0 = rpc.call(
        source["state_view"],
        "getSlot0(bytes32)",
        number,
        ("uint160", "int24", "uint24", "uint24"),
        (v4_pool_id(source),),
    )
    return differences(
        (
            ("sqrt_price_x96", observation["sqrt_price_x96"], slot0[0]),
            ("tick", observation["tick"], slot0[1]),
            ("lp_fee", observation["lp_fee"], slot0[3]),
            same_block(observation, rpc),
        )
    )


def reserve_decimals(output: Output, rpc: Rpc) -> list[str]:
    """Any lending reserve: the resolved asset's own `decimals()`."""
    resolution, observation = output["resolution"], output["observation"]
    number = observation["block"]["number"]
    (decimals,) = rpc.call(asset_address(resolution["asset"]), "decimals()", number, ("uint8",))
    return differences(
        (("decimals", resolution["decimals"], decimals), same_block(observation, rpc))
    )


def pooled_reserve(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V3 or SparkLend reserve: the aToken the Pool itself records."""
    resolution, number = output["resolution"], output["observation"]["block"]["number"]
    # aTokenAddress is the ninth static word of the Pool's reserve data.
    words = rpc.call(
        resolution["pool"],
        "getReserveData(address)",
        number,
        ("uint256",) * 8 + ("address",),
        (asset_address(resolution["asset"]),),
    )
    recorded = asset_address(resolution["a_token"])
    return differences((("a_token", recorded, words[8].lower()),)) + reserve_decimals(output, rpc)


def comet_reserve(output: Output, rpc: Rpc) -> list[str]:
    resolution, number = output["resolution"], output["observation"]["block"]["number"]
    (base,) = rpc.call(resolution["comet"], "baseToken()", number, ("address",))
    recorded = asset_address(resolution["base_asset"])
    return differences((("base_asset", recorded, base.lower()),)) + reserve_decimals(output, rpc)


def morpho_reserve(output: Output, rpc: Rpc) -> list[str]:
    resolution, number = output["resolution"], output["observation"]["block"]["number"]
    params = rpc.call(
        resolution["core"],
        "idToMarketParams(bytes32)",
        number,
        ("address", "address", "address", "address", "uint256"),
        (bytes.fromhex(resolution["market_id"][2:]),),
    )
    return differences(
        (
            ("loan_asset", asset_address(resolution["loan_asset"]), params[0].lower()),
            ("collateral", asset_address(resolution["collateral_asset"]), params[1].lower()),
            ("oracle", resolution["oracle"], params[2].lower()),
            ("lltv", resolution["lltv"], params[4]),
        )
    ) + reserve_decimals(output, rpc)


def aave_v4_position(output: Output, rpc: Rpc) -> list[str]:
    observation, selection = output["observation"], output["resolution"]["selection"]
    number = observation["reserve"]["block"]["number"]
    arguments = (selection["reserve_id"], observation["account"]["address"])
    (supplied,) = rpc.call(
        selection["spoke"],
        "getUserSuppliedAssets(uint256,address)",
        number,
        ("uint256",),
        arguments,
    )
    return differences((("supplied_assets", observation["supplied_assets"], supplied),))


def gmx_market(share: str, symbol: str) -> Check:
    """A GMX market lookup must name this GM share, and its index token this symbol."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        observation = output["observation"]
        (onchain,) = rpc.call(
            asset_address(observation["index"]), "symbol()", observation["block"], ("string",)
        )
        return differences(
            (
                ("share", asset_address(observation["share"]), share),
                ("index_symbol", observation["index_symbol"], symbol),
                ("index symbol()", observation["index_symbol"], onchain),
            )
        )

    return check


def readable_markets(output: Output) -> list[Output]:
    """The listing's markets, once nothing in it went unread because of the endpoint."""
    unreadable = output["source"].get("unreadable") or []
    reasons = [item.get("reason", "") for item in (*unreadable, *output["markets"])]
    blocked = [reason for reason in reasons if reason.startswith(ENDPOINT_REASONS)]
    if blocked:
        raise RpcUnavailable(f"{len(blocked)} entries unreadable, first: {blocked[0]}")
    return list(output["markets"])


def listed_markets(output: Output, rpc: Rpc) -> list[str]:
    """A listing of reviewed deployments: every reported market is verified."""
    markets = readable_markets(output)
    if not markets:
        return ["the listing reports no markets"]
    return [
        f"market {index} is {market['status']}: {market.get('reason', '')}"
        for index, market in enumerate(markets)
        if market["status"] != "verified"
    ]


def some_verified_markets(output: Output, rpc: Rpc) -> list[str]:
    """A listing of permissionless markets: the connector may refuse some, never all."""
    if any(market["status"] == "verified" for market in readable_markets(output)):
        return []
    return ["the listing reports no verified market"]


def pool_reserve_listing(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V3 or SparkLend listing: as many reserves as the Pool's own list."""
    source = output["source"]
    (reserves,) = rpc.call(
        source["contract"], "getReservesList()", output["block"]["number"], ("address[]",)
    )
    counted = differences((("listed reserves", source["listed"], len(reserves)),))
    return counted + listed_markets(output, rpc)


def hypercore_market(coin: str) -> Check:
    def check(output: Output, rpc: Rpc) -> list[str]:
        names = [market["target"]["coin"] for market in output["markets"]]
        return differences((("markets", names, [coin]),))

    return check
