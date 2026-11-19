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


# The leading static words of a Spoke's reserve record.
SPOKE_RESERVE = ("address", "address", "uint16", "uint8")


def spoke_reserve(rpc: Rpc, spoke: str, reserve_id: int, block: int) -> tuple[Any, ...]:
    """The underlying token, Hub, Hub asset ID and decimals a Spoke records for a reserve."""
    return rpc.call(spoke, "getReserve(uint256)", block, SPOKE_RESERVE, (reserve_id,))


def aave_v4_reserve(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V4 reserve: what the Spoke itself records under the selected reserve ID."""
    resolution, number = output["resolution"], output["observation"]["block"]["number"]
    selection = resolution["selection"]
    underlying, hub, hub_asset_id, decimals = spoke_reserve(
        rpc, selection["spoke"], selection["reserve_id"], number
    )
    return differences(
        (
            ("asset", asset_address(resolution["asset"]), underlying.lower()),
            ("hub", resolution["hub"], hub.lower()),
            ("hub_asset_id", resolution["hub_asset_id"], hub_asset_id),
            ("spoke decimals", resolution["decimals"], decimals),
        )
    ) + reserve_decimals(output, rpc)


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


def empty_spokes(output: Output, rpc: Rpc) -> list[str]:
    """Spokes the listing calls unreadable although the endpoint answered: they revert.

    A Spoke without reserves reverts `getReserveCount()`. Any other unreadable entry,
    and any market refused for an endpoint reason, blocks the case instead.
    """
    unreadable = output["source"].get("unreadable") or []
    block = output["block"]["number"]
    empty = [
        item["spoke"]
        for item in unreadable
        if "spoke" in item and rpc.reverts(item["spoke"], "getReserveCount()", block)
    ]
    unanswered = [item.get("reason", "") for item in unreadable if item.get("spoke") not in empty]
    unanswered += [
        market["reason"]
        for market in output["markets"]
        if market.get("reason", "").startswith(ENDPOINT_REASONS)
    ]
    if unanswered:
        raise RpcUnavailable(f"{len(unanswered)} entries unreadable, first: {unanswered[0]}")
    return empty


def unverified(markets: list[Output]) -> list[str]:
    if not markets:
        return ["the listing reports no markets"]
    return [
        f"market {index} is {market['status']}: {market.get('reason', '')}"
        for index, market in enumerate(markets)
        if market["status"] != "verified"
    ]


def listed_markets(output: Output, rpc: Rpc) -> list[str]:
    """A listing of reviewed deployments: nothing unreadable, every market verified."""
    empty = empty_spokes(output, rpc)
    misread = (
        [
            f"{len(empty)} Spokes are reported unreadable, but they revert getReserveCount()"
            f" on a direct read, as a Spoke without reserves does: {', '.join(empty)}"
        ]
        if empty
        else []
    )
    return misread + unverified(output["markets"])


def aave_v4_listing(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V4 listing: each market is the reserve its Spoke records under that ID."""
    block = output["block"]["number"]
    pairs = []
    for index, market in enumerate(output["markets"]):
        underlying, hub, _, _ = spoke_reserve(rpc, market["spoke"], market["reserve_id"], block)
        asset = asset_address(market["asset"]["asset"])
        pairs.append((f"market {index} asset", asset, underlying.lower()))
        pairs.append((f"market {index} hub", market["hub"], hub.lower()))
    return differences(pairs) + listed_markets(output, rpc)


def morpho_listing(core: str) -> Check:
    """A Morpho Blue listing: the connector may refuse some markets, never all, and each
    verified market's loan asset is the one the core records under its ID."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        empty_spokes(output, rpc)
        verified = [market for market in output["markets"] if market["status"] == "verified"]
        if not verified:
            return ["the listing reports no verified market"]
        pairs = []
        for market in verified:
            (loan,) = rpc.call(
                core,
                "idToMarketParams(bytes32)",
                output["block"]["number"],
                ("address",),
                (bytes.fromhex(market["market_id"][2:]),),
            )
            reported = asset_address(market["loan_asset"]["asset"])
            pairs.append((f"loan asset of {market['market_id'][:10]}", reported, loan.lower()))
        return differences(pairs)

    return check


def pool_reserve_listing(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V3 or SparkLend listing against the Pool's own reserve list: the same
    count, and every reported market an asset on that list."""
    source = output["source"]
    (reserves,) = rpc.call(
        source["contract"], "getReservesList()", output["block"]["number"], ("address[]",)
    )
    onchain = {reserve.lower() for reserve in reserves}
    counted = differences((("listed reserves", source["listed"], len(reserves)),))
    strangers = [
        f"market {index} asset {market['asset']['asset']} is not on the Pool's reserve list"
        for index, market in enumerate(output["markets"])
        if asset_address(market["asset"]["asset"]) not in onchain
    ]
    return counted + strangers + listed_markets(output, rpc)


def hypercore_market(coin: str) -> Check:
    def check(output: Output, rpc: Rpc) -> list[str]:
        names = [market["target"]["coin"] for market in output["markets"]]
        return differences((("markets", names, [coin]),))

    return check
