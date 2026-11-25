"""Cross-checks of `almanak ax` read output against direct reads at the same block.

Each check takes the command's JSON output and a direct RPC client for the same
endpoint, and returns one line per disagreement. Raising `RpcUnavailable` blocks
the case instead of failing it: the endpoint, not the SDK, did not answer.
"""

from collections.abc import Callable, Iterable, Sequence
from typing import Any

from eth_abi import encode
from eth_utils import keccak

from almanak.caip import AssetType
from scripts.live_qa.rpc import Rpc, RpcUnavailable, asset_address

Output = dict[str, Any]
Check = Callable[[Output, Rpc], list[str]]

ZERO_ADDRESS = "0x" + "00" * 20
# An EVM chain's native balance is counted in units of 10**-18 of its coin.
EVM_NATIVE_DECIMALS = 18
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


NO_OBSERVATIONS = ["the command reported no observations, so no balance was compared"]


def lowered(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def not_requested(pairs: Iterable[tuple[str, Any, Any]]) -> list[str]:
    """Where what the command reports it read differs from what the case requested.

    A check whose direct read follows the reported identity would agree with a correct
    answer about the wrong market, so identity is held to the request before any read.
    Addresses and asset references compare in any letter case.
    """
    return [
        f"{label}: CLI {reported!r}, requested {requested!r}"
        for label, reported, requested in pairs
        if lowered(reported) != lowered(requested)
    ]


def other_identity(observation: Output, asset: AssetType, account: str) -> list[str]:
    """Where an observation names another asset or account than the case requested.

    A direct read of what the command reported would agree with a correct balance of
    the wrong account, so identity is held to the case before any read.
    """
    return not_requested(
        (
            ("asset", observation["asset"], str(asset)),
            ("account", observation["account"]["address"], account),
        )
    )


def unrequested(output: Output, asset: AssetType, account: str) -> list[str]:
    """Why the output's observations cannot be compared: none, or another identity."""
    observations = output["observations"]
    if not observations:
        return NO_OBSERVATIONS
    return [found for item in observations for found in other_identity(item, asset, account)]


def native_balance(asset: AssetType, account: str) -> Check:
    """A native balance of the requested account, held to `eth_getBalance` and 18 decimals."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        if refused := unrequested(output, asset, account):
            return refused
        pairs = []
        for observation in output["observations"]:
            number = observation["block"]["number"]
            pairs.append(("balance", int(observation["amount"]), rpc.balance(account, number)))
            pairs.append(("decimals", observation["decimals"], EVM_NATIVE_DECIMALS))
            pairs.append(same_block(observation, rpc))
        return differences(pairs)

    return check


def erc20_balance(asset: AssetType, account: str) -> Check:
    """An ERC-20 balance of the requested account, held to `balanceOf` and `decimals()`."""
    token = asset_address(str(asset))

    def check(output: Output, rpc: Rpc) -> list[str]:
        if refused := unrequested(output, asset, account):
            return refused
        pairs = []
        for observation in output["observations"]:
            number = observation["block"]["number"]
            (held,) = rpc.call(token, "balanceOf(address)", number, ("uint256",), (account,))
            (decimals,) = rpc.call(token, "decimals()", number, ("uint8",))
            pairs.append(("balance", int(observation["amount"]), held))
            pairs.append(("decimals", observation["decimals"], decimals))
            pairs.append(same_block(observation, rpc))
        return differences(pairs)

    return check


def v3_pool(pool: str) -> Check:
    """A Uniswap V3 pool read of the requested pool, held to the pool's own state."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        if other := not_requested((("pool", output["resolution"]["pool"], pool),)):
            return other
        observation = output["observation"]
        number = observation["block"]["number"]
        slot0 = rpc.call(pool, "slot0()", number, ("uint160", "int24"))
        (liquidity,) = rpc.call(pool, "liquidity()", number, ("uint128",))
        (token0,) = rpc.call(pool, "token0()", number, ("address",))
        (fee,) = rpc.call(pool, "fee()", number, ("uint24",))
        token0_reported = asset_address(observation["currency0"]["identifier"])
        return differences(
            (
                ("sqrt_price_x96", observation["sqrt_price_x96"], slot0[0]),
                ("tick", observation["tick"], slot0[1]),
                ("liquidity", observation["liquidity"], liquidity),
                ("currency0", token0_reported, token0.lower()),
                ("fee", output["resolution"]["fee"], fee),
                same_block(observation, rpc),
            )
        )

    return check


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


def v4_pool_price(
    currency0: str, currency1: str, fee: int, tick_spacing: int, hooks: str = ZERO_ADDRESS
) -> Check:
    """A Uniswap V4 price of the requested pool key, held to the StateView's `getSlot0`.

    The pool ID is hashed from the key the case requested, so a price of another pool
    cannot agree. `hooks` is the zero address for a hookless pool, as the command
    reads a key given without `--hooks`.
    """
    key = {
        "currency0": currency0,
        "currency1": currency1,
        "fee": fee,
        "tick_spacing": tick_spacing,
        "hooks": hooks,
    }

    def check(output: Output, rpc: Rpc) -> list[str]:
        observation = output["observation"]
        source, number = observation["source"], observation["block"]["number"]
        if other := not_requested((name, source[name], value) for name, value in key.items()):
            return other
        slot0 = rpc.call(
            source["state_view"],
            "getSlot0(bytes32)",
            number,
            ("uint160", "int24", "uint24", "uint24"),
            (v4_pool_id(key),),
        )
        return differences(
            (
                ("sqrt_price_x96", observation["sqrt_price_x96"], slot0[0]),
                ("tick", observation["tick"], slot0[1]),
                ("lp_fee", observation["lp_fee"], slot0[3]),
                same_block(observation, rpc),
            )
        )

    return check


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


def aave_v4_reserve(spoke: str, reserve_id: int) -> Check:
    """An Aave V4 reserve: what the requested Spoke records under the requested reserve ID."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        resolution, number = output["resolution"], output["observation"]["block"]["number"]
        selection = resolution["selection"]
        if other := not_requested(
            (
                ("spoke", selection["spoke"], spoke),
                ("reserve_id", selection["reserve_id"], reserve_id),
            )
        ):
            return other
        underlying, hub, hub_asset_id, decimals = spoke_reserve(rpc, spoke, reserve_id, number)
        return differences(
            (
                ("asset", asset_address(resolution["asset"]), underlying.lower()),
                ("hub", resolution["hub"], hub.lower()),
                ("hub_asset_id", resolution["hub_asset_id"], hub_asset_id),
                ("spoke decimals", resolution["decimals"], decimals),
            )
        ) + reserve_decimals(output, rpc)

    return check


def pooled_reserve(pool: str, asset: str) -> Check:
    """An Aave V3 or SparkLend reserve of the requested asset in the connector's reviewed
    Pool: the aToken that Pool itself records. The request names no Pool, so the check
    holds the command to the Pool the connector pins on the chain."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        resolution, number = output["resolution"], output["observation"]["block"]["number"]
        if other := not_requested(
            (("pool", resolution["pool"], pool), ("asset", resolution["asset"], asset))
        ):
            return other
        # aTokenAddress is the ninth static word of the Pool's reserve data.
        words = rpc.call(
            pool,
            "getReserveData(address)",
            number,
            ("uint256",) * 8 + ("address",),
            (asset_address(asset),),
        )
        recorded = asset_address(resolution["a_token"])
        a_token = differences((("a_token", recorded, words[8].lower()),))
        return a_token + reserve_decimals(output, rpc)

    return check


def comet_reserve(comet: str, asset: str) -> Check:
    """A Compound V3 reserve of the requested Comet and asset: the Comet's own base token."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        resolution, number = output["resolution"], output["observation"]["block"]["number"]
        if other := not_requested(
            (("comet", resolution["comet"], comet), ("asset", resolution["asset"], asset))
        ):
            return other
        (base,) = rpc.call(comet, "baseToken()", number, ("address",))
        recorded = asset_address(resolution["base_asset"])
        base_asset = differences((("base_asset", recorded, base.lower()),))
        return base_asset + reserve_decimals(output, rpc)

    return check


def morpho_reserve(core: str, market_id: str) -> Check:
    """A Morpho Blue reserve of the requested market: what the core records under its ID."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        resolution, number = output["resolution"], output["observation"]["block"]["number"]
        if other := not_requested(
            (("core", resolution["core"], core), ("market_id", resolution["market_id"], market_id))
        ):
            return other
        params = rpc.call(
            core,
            "idToMarketParams(bytes32)",
            number,
            ("address", "address", "address", "address", "uint256"),
            (bytes.fromhex(market_id[2:]),),
        )
        return differences(
            (
                ("loan_asset", asset_address(resolution["loan_asset"]), params[0].lower()),
                ("collateral", asset_address(resolution["collateral_asset"]), params[1].lower()),
                ("oracle", resolution["oracle"], params[2].lower()),
                ("lltv", resolution["lltv"], params[4]),
            )
        ) + reserve_decimals(output, rpc)

    return check


def aave_v4_position(spoke: str, reserve_id: int, account: str) -> Check:
    """An Aave V4 position of the requested account in the requested Spoke and reserve.

    The direct read asks about what the case requested, not what the command reported,
    and a reported identity that differs fails before it. A zero position passes when
    the Spoke agrees.
    """

    def check(output: Output, rpc: Rpc) -> list[str]:
        observation, selection = output["observation"], output["resolution"]["selection"]
        if other := not_requested(
            (
                ("spoke", selection["spoke"], spoke),
                ("reserve_id", selection["reserve_id"], reserve_id),
                ("account", observation["account"]["address"], account),
            )
        ):
            return other
        number = observation["reserve"]["block"]["number"]
        (supplied,) = rpc.call(
            spoke,
            "getUserSuppliedAssets(uint256,address)",
            number,
            ("uint256",),
            (reserve_id, account),
        )
        return differences((("supplied_assets", observation["supplied_assets"], supplied),))

    return check


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


# The keys of an unreadable entry that are its refusal; the others name the entry.
ENTRY_STATE = ("status", "reason")


def entry_name(item: Output) -> str:
    return ", ".join(f"{key} {value}" for key, value in item.items() if key not in ENTRY_STATE)


def endpoint_refused(item: Output) -> bool:
    return str(item.get("reason", "")).startswith(ENDPOINT_REASONS)


def entry_findings(output: Output, rpc: Rpc) -> tuple[list[str], list[str]]:
    """Findings on what the listing calls unreadable, and the reasons it left unanswered.

    A Spoke without reserves reverts `getReserveCount()`, so calling it unreadable is a
    misread. An entry refused for a reason that does not name the endpoint is a finding
    too. An entry or a market refused for an endpoint reason is no finding; it is
    returned so that `unless_answered` can block a case that found nothing else.
    """
    unreadable = output["source"].get("unreadable") or []
    block = output["block"]["number"]
    empty = [
        item["spoke"]
        for item in unreadable
        if "spoke" in item and rpc.reverts(item["spoke"], "getReserveCount()", block)
    ]
    others = [item for item in unreadable if item.get("spoke") not in empty]
    misread = (
        [
            f"{len(empty)} Spokes are reported unreadable, but they revert getReserveCount()"
            f" on a direct read, as a Spoke without reserves does: {', '.join(empty)}"
        ]
        if empty
        else []
    )
    answered = [
        f"{entry_name(item)} is unreadable: {item.get('reason', '')}"
        for item in others
        if not endpoint_refused(item)
    ]
    unanswered = [
        item["reason"] for item in [*others, *output["markets"]] if endpoint_refused(item)
    ]
    return misread + answered, unanswered


def unless_answered(findings: list[str], unanswered: list[str]) -> list[str]:
    """The findings, or a blocked case where there are none and an entry went unanswered.

    An unanswered entry cannot excuse a finding the endpoint did answer, so a case with
    both fails. A case with only unanswered entries blocks, and the run retries it.
    """
    if unanswered and not findings:
        raise RpcUnavailable(f"{len(unanswered)} entries unreadable, first: {unanswered[0]}")
    return findings


def unverified(markets: list[Output]) -> list[str]:
    """Markets that are not verified for a reason that does not name the endpoint."""
    return [
        f"market {index} is {market['status']}: {market.get('reason', '')}"
        for index, market in enumerate(markets)
        if market["status"] != "verified" and not endpoint_refused(market)
    ]


def listed_markets(output: Output, rpc: Rpc, found: Sequence[str] = ()) -> list[str]:
    """A listing of reviewed deployments: nothing unreadable, every market verified.

    `found` holds what a connector's own check already found in the same listing. An
    empty listing fails, unless an entry the endpoint did not answer may hold the
    markets: then the case blocks and the run retries it.
    """
    findings, unanswered = entry_findings(output, rpc)
    markets = output["markets"]
    empty = [] if markets or unanswered else ["the listing reports no markets"]
    return unless_answered([*found, *findings, *empty, *unverified(markets)], unanswered)


def aave_v4_listing(output: Output, rpc: Rpc) -> list[str]:
    """An Aave V4 listing: each market is the reserve its Spoke records under that ID."""
    block = output["block"]["number"]
    pairs = []
    for index, market in enumerate(output["markets"]):
        underlying, hub, _, _ = spoke_reserve(rpc, market["spoke"], market["reserve_id"], block)
        asset = asset_address(market["asset"]["asset"])
        pairs.append((f"market {index} asset", asset, underlying.lower()))
        pairs.append((f"market {index} hub", market["hub"], hub.lower()))
    return listed_markets(output, rpc, differences(pairs))


def morpho_listing(core: str, loan_asset: str) -> Check:
    """A Morpho Blue listing filtered by a loan asset: the connector may refuse some
    markets, never all, and each verified market lends the requested asset, as the
    core itself records under the market's ID."""
    requested = asset_address(loan_asset)

    def check(output: Output, rpc: Rpc) -> list[str]:
        findings, unanswered = entry_findings(output, rpc)
        markets = output["markets"]
        verified = [market for market in markets if market["status"] == "verified"]
        if not verified:
            # Markets the endpoint did not answer may be the verified ones.
            missing = (
                []
                if any(map(endpoint_refused, markets))
                else ["the listing reports no verified market"]
            )
            return unless_answered([*findings, *missing], unanswered)
        pairs = []
        for market in verified:
            name = f"loan asset of {market['market_id'][:10]}"
            reported = asset_address(market["loan_asset"]["asset"])
            if reported != requested:
                findings.append(f"{name}: CLI {reported!r}, requested {requested!r}")
                continue
            (loan,) = rpc.call(
                core,
                "idToMarketParams(bytes32)",
                output["block"]["number"],
                ("address",),
                (bytes.fromhex(market["market_id"][2:]),),
            )
            pairs.append((name, reported, loan.lower()))
        return unless_answered(findings + differences(pairs), unanswered)

    return check


def pool_reserve_listing(limit: int) -> Check:
    """An unfiltered Aave V3 or SparkLend listing from its first reserve, held to the
    Pool's own reserve list: the same count, every reported market a distinct asset on
    that list, and as many markets as the limit allows."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        source = output["source"]
        (reserves,) = rpc.call(
            source["contract"], "getReservesList()", output["block"]["number"], ("address[]",)
        )
        onchain = {reserve.lower() for reserve in reserves}
        counted = differences((("listed reserves", source["listed"], len(reserves)),))
        assets = [asset_address(market["asset"]["asset"]) for market in output["markets"]]
        repeated = [
            f"asset {asset} is reported {assets.count(asset)} times"
            for asset in sorted(set(assets))
            if assets.count(asset) > 1
        ]
        expected = min(limit, len(reserves))
        short = (
            [
                f"the listing reports {len(set(assets))} distinct markets, where a limit of"
                f" {limit} on {len(reserves)} reserves gives {expected}"
            ]
            if len(set(assets)) != expected
            else []
        )
        strangers = [
            f"market {index} asset {asset} is not on the Pool's reserve list"
            for index, asset in enumerate(assets)
            if asset not in onchain
        ]
        return listed_markets(output, rpc, counted + repeated + short + strangers)

    return check


def hypercore_market(coin: str) -> Check:
    def check(output: Output, rpc: Rpc) -> list[str]:
        names = [market["target"]["coin"] for market in output["markets"]]
        return differences((("markets", names, [coin]),))

    return check
