"""Cross-checks of `almanak ax` read output against direct reads at the same block.

Each check takes the command's JSON output and a direct RPC client for the same
endpoint, and returns one line per disagreement. Raising `RpcUnavailable` blocks
the case instead of failing it: the endpoint, not the SDK, did not answer.
"""

from collections.abc import Callable, Iterable, Sequence
from functools import partial
from typing import Any

from eth_abi import encode
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from scripts.live_qa.rpc import Rpc, RpcReverted, RpcUnavailable, asset_address

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


def on_chain(chain: ChainId, address: str) -> str:
    """The CAIP-19 reference of an ERC-20 contract a direct read returned on the case's chain."""
    return str(AssetType(f"{chain}/erc20:{address}"))


def reported_asset(value: Any) -> str:
    """A CAIP-19 reference the command reported, validated and in canonical form.

    A reported asset is compared whole, chain included: the right contract address
    under another chain is another asset. A value that is no CAIP-19 reference raises
    ValueError, which fails the case as output the check cannot read.
    """
    return str(AssetType(value))


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


def v3_pool(chain: ChainId, pool: str) -> Check:
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
        token0_reported = reported_asset(observation["currency0"]["identifier"])
        return differences(
            (
                ("sqrt_price_x96", observation["sqrt_price_x96"], slot0[0]),
                ("tick", observation["tick"], slot0[1]),
                ("liquidity", observation["liquidity"], liquidity),
                ("currency0", token0_reported, on_chain(chain, token0)),
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


def then_decimals(found: list[str], output: Output, rpc: Rpc) -> list[str]:
    """A reserve's established mismatches, or its decimals check where there are none.

    A mismatch the endpoint already answered is a finding whatever a later read does,
    so it is returned before the decimals read: an unanswered read cannot turn it into
    a blocked case. Matching reserve data still gets the decimals check.
    """
    return found or reserve_decimals(output, rpc)


# The leading static words of a Spoke's reserve record.
SPOKE_RESERVE = ("address", "address", "uint16", "uint8")


def spoke_reserve(rpc: Rpc, spoke: str, reserve_id: int, block: int) -> tuple[Any, ...]:
    """The underlying token, Hub, Hub asset ID and decimals a Spoke records for a reserve."""
    return rpc.call(spoke, "getReserve(uint256)", block, SPOKE_RESERVE, (reserve_id,))


def aave_v4_reserve(chain: ChainId, spoke: str, reserve_id: int) -> Check:
    """An Aave V4 reserve: what the requested Spoke records under the requested reserve ID,
    its underlying asset an asset of the requested chain."""

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
        found = differences(
            (
                ("asset", reported_asset(resolution["asset"]), on_chain(chain, underlying)),
                ("hub", resolution["hub"], hub.lower()),
                ("hub_asset_id", resolution["hub_asset_id"], hub_asset_id),
                ("spoke decimals", resolution["decimals"], decimals),
            )
        )
        return then_decimals(found, output, rpc)

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
        chain = AssetType(asset).chain
        recorded = reported_asset(resolution["a_token"])
        a_token = differences((("a_token", recorded, on_chain(chain, words[8])),))
        return then_decimals(a_token, output, rpc)

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
        recorded, held = (
            reported_asset(resolution["base_asset"]),
            on_chain(AssetType(asset).chain, base),
        )
        base_asset = differences((("base_asset", recorded, held),))
        return then_decimals(base_asset, output, rpc)

    return check


def morpho_reserve(chain: ChainId, core: str, market_id: str) -> Check:
    """A Morpho Blue reserve of the loan leg of the requested market: what the core records
    under its ID, its assets assets of the requested chain. The reserve's own asset is the
    loan asset, so the decimals read is of the token the core lends."""

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
        loan, collateral = on_chain(chain, params[0]), on_chain(chain, params[1])
        found = differences(
            (
                ("asset", reported_asset(resolution["asset"]), loan),
                ("loan_asset", reported_asset(resolution["loan_asset"]), loan),
                ("collateral", reported_asset(resolution["collateral_asset"]), collateral),
                ("oracle", resolution["oracle"], params[2].lower()),
                ("lltv", resolution["lltv"], params[4]),
            )
        )
        return then_decimals(found, output, rpc)

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


def gmx_market(chain: ChainId, share: str, symbol: str) -> Check:
    """A GMX market lookup must name this GM share on the requested chain, and its index
    token this symbol.

    What the lookup reports is held to the request before the index's `symbol()` read,
    so a lookup that names another asset fails even when that read goes unanswered.
    """

    def check(output: Output, rpc: Rpc) -> list[str]:
        observation = output["observation"]
        # Asset references compare in any letter case; a symbol compares exactly.
        if other := not_requested(
            (
                ("share", reported_asset(observation["share"]), on_chain(chain, share)),
                ("index chain", str(AssetType(observation["index"]).chain), str(chain)),
            )
        ) + differences((("index_symbol", observation["index_symbol"], symbol),)):
            return other
        (onchain,) = rpc.call(
            asset_address(observation["index"]), "symbol()", observation["block"], ("string",)
        )
        return differences((("index symbol()", observation["index_symbol"], onchain),))

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
    returned so that `unless_answered` can block a case that found nothing else, and so
    is a direct `getReserveCount()` read the endpoint did not answer.
    """
    unreadable = output["source"].get("unreadable") or []
    block = output["block"]["number"]
    empty, unread = [], []
    for item in unreadable:
        if "spoke" not in item:
            continue
        # An unanswered direct read is kept, not raised, so that it cannot hide a
        # misread of another Spoke or a mismatch the caller already found.
        try:
            if rpc.reverts(item["spoke"], "getReserveCount()", block):
                empty.append(item["spoke"])
        except RpcUnavailable as exc:
            unread.append(f"spoke {item['spoke']} getReserveCount(): {exc}")
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
    return misread + answered, unanswered + unread


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


def listed_markets(
    output: Output, rpc: Rpc, found: Sequence[str] = (), unread: Sequence[str] = ()
) -> list[str]:
    """A listing of reviewed deployments: nothing unreadable, every market verified.

    `found` holds what a connector's own check already found in the same listing, and
    `unread` the direct reads of its markets the endpoint did not answer. An empty
    listing fails, unless an entry the endpoint did not answer may hold the markets:
    then the case blocks and the run retries it.
    """
    findings, unanswered = entry_findings(output, rpc)
    markets = output["markets"]
    empty = [] if markets or unanswered else ["the listing reports no markets"]
    return unless_answered(
        [*found, *findings, *empty, *unverified(markets)], [*unanswered, *unread]
    )


def answered(read: Callable[[], tuple[Any, ...]], name: str, unread: list[str]) -> tuple[Any, ...]:
    """A market's direct read, or an empty tuple where the endpoint did not answer it.

    The unanswered read is kept in `unread` rather than raised, so that it cannot hide
    a mismatch the endpoint answered for another market of the same listing. A revert
    is an answer and still raises.
    """
    try:
        return read()
    except RpcReverted:
        raise
    except RpcUnavailable as exc:
        unread.append(f"{name}: {exc}")
        return ()


def other_chain(index: int, asset: str, chain: ChainId) -> list[str]:
    """A finding where a reported market asset is an asset of another chain than the case's.

    It needs no direct read, so it is kept where the read that would compare the whole
    asset went unanswered.
    """
    reported = AssetType(asset).chain
    if str(reported) == str(chain):
        return []
    return [f"market {index} asset {asset} is not an asset of {chain}"]


def aave_v4_listing(chain: ChainId) -> Check:
    """An Aave V4 listing: each market is the reserve its Spoke records under that ID, its
    asset an asset of the requested chain."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        block = output["block"]["number"]
        pairs, elsewhere = [], []
        unread: list[str] = []
        for index, market in enumerate(output["markets"]):
            # Validated before the read, so that output the check cannot read fails and an
            # asset of another chain is a finding whether or not the Spoke answers.
            asset = reported_asset(market["asset"]["asset"])
            spoke, reserve_id = market["spoke"], market["reserve_id"]
            read = partial(spoke_reserve, rpc, spoke, reserve_id, block)
            record = answered(read, f"market {index}", unread)
            if not record:
                elsewhere += other_chain(index, asset, chain)
                continue
            underlying, hub, _, _ = record
            pairs.append((f"market {index} asset", asset, on_chain(chain, underlying)))
            pairs.append((f"market {index} hub", market["hub"], hub.lower()))
        return listed_markets(output, rpc, [*differences(pairs), *elsewhere], unread)

    return check


def morpho_listing(core: str, loan_asset: str) -> Check:
    """A Morpho Blue listing filtered by a loan asset: the connector may refuse some
    markets, never all, and each verified market lends the requested asset, as the
    core itself records under the market's ID."""
    requested = str(AssetType(loan_asset))
    chain = AssetType(loan_asset).chain

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
        pairs, block = [], output["block"]["number"]
        for market in verified:
            name = f"loan asset of {market['market_id'][:10]}"
            reported = reported_asset(market["loan_asset"]["asset"])
            if reported != requested:
                findings.append(f"{name}: CLI {reported!r}, requested {requested!r}")
                continue
            market_id = bytes.fromhex(market["market_id"][2:])
            read = partial(
                rpc.call, core, "idToMarketParams(bytes32)", block, ("address",), (market_id,)
            )
            params = answered(read, name, unanswered)
            if params:
                pairs.append((name, reported, on_chain(chain, params[0])))
        return unless_answered(findings + differences(pairs), unanswered)

    return check


def pool_reserve_listing(chain: ChainId, limit: int) -> Check:
    """An unfiltered Aave V3 or SparkLend listing from its first reserve, held to the
    Pool's own reserve list: the same count, every reported market a distinct asset of
    the requested chain on that list, and as many markets as the limit allows."""

    def check(output: Output, rpc: Rpc) -> list[str]:
        source = output["source"]
        assets = [reported_asset(market["asset"]["asset"]) for market in output["markets"]]
        repeated = [
            f"asset {asset} is reported {assets.count(asset)} times"
            for asset in sorted(set(assets))
            if assets.count(asset) > 1
        ]
        unread: list[str] = []
        read = partial(
            rpc.call,
            source["contract"],
            "getReservesList()",
            output["block"]["number"],
            ("address[]",),
        )
        listed = answered(read, "getReservesList()", unread)
        if not listed:
            # What needs no reserve list is still compared, so an unanswered list cannot
            # turn a repeated asset or an asset of another chain into a blocked case.
            elsewhere = [
                found
                for index, asset in enumerate(assets)
                for found in other_chain(index, asset, chain)
            ]
            return listed_markets(output, rpc, repeated + elsewhere, unread)
        (reserves,) = listed
        onchain = {on_chain(chain, reserve) for reserve in reserves}
        counted = differences((("listed reserves", source["listed"], len(reserves)),))
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
