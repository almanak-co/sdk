"""ax command handlers wire parser output to readers at a pinned block."""

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak import ax
from almanak.balances import BalanceObservation
from almanak.caip import AssetType, ChainId
from almanak.cli_arguments import LENDING_SELECTOR_FLAGS
from almanak.connectors.gmx_v2.domain import Contracts, Market
from almanak.connectors.gmx_v2.position_request import GmxPositionRequest
from almanak.connectors.lending_risk import RiskQuery
from almanak.models import Account, Block, Chain
from tests.read_at_support import read_at

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples" / "ethereum"
CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
BLOCK = Block(chain=CHAIN, number=25908472, hash="0x" + "ab" * 32, timestamp=1_700_000_000)
OWNER = "0x" + "11" * 20


def abi(*values, types):
    return {"result": "0x" + encode(types, values).hex()}


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


def args(product, connector, config, **extra):
    return SimpleNamespace(
        product=product,
        ax_command=product.removeprefix("ax-"),
        connector=connector,
        config=config,
        block=25908472,
        env_file=Path("operator.env"),
        account=extra.pop("account", None),
        **extra,
    )


@pytest.fixture
def live_reader(monkeypatch, rpc_factory):
    def install(reader):
        monkeypatch.setattr(ax, "verified_reader", lambda chain, env_file: reader)
        return reader

    return install


def test_pool_price_end_to_end(live_reader, rpc_factory, capsys):
    reader, _ = rpc_factory()
    live_reader(reader)
    result = ax.command(args("ax-pool-price", "uniswap_v4", EXAMPLES / "uniswap-v4-weth-usdc.json"))
    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["product"] == "pool_spot_price"
    assert output["observation"]["block"]["number"] == 25908472
    assert int(output["price"]["numerator"]) > 0
    assert output["request"]["source"]["chain"]["chain_id"] == "eip155:1"


def lending_args(product, connector, config=None, **inline):
    values = dict.fromkeys(LENDING_SELECTOR_FLAGS)
    values.update(inline)
    return args(product, connector, config, chain=values.pop("chain", None), **values)


class Pinned:
    chain = CHAIN

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def pin_block(self, selector):
        assert selector == 25908472
        return BLOCK


def test_lending_reserve_builds_the_selection_from_inline_flags(live_reader, monkeypatch):
    seen = {}

    def read(reader, request, block, query):
        seen.update(request=request, block=block, query=query)
        return {"product": "lending_reserve"}

    monkeypatch.setattr(ax, "read_lending_reserve", read)
    live_reader(Pinned())
    asset = "eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
    invocation = lending_args(
        "ax-lending-reserve",
        "aave_v3",
        chain="eip155:1",
        asset=asset,
        borrow_fraction=[Decimal("0.3"), Decimal("0.5")],
        health_floor=Decimal("1.6"),
    )
    stated = read_at(monkeypatch, 25908472, BLOCK)
    assert ax.lending_reserve(invocation) == {"product": "lending_reserve", "read_at": stated}
    assert seen["request"].connector == "aave_v3"
    assert str(seen["request"].asset) == asset
    assert seen["request"].pool is None
    assert seen["block"] == BLOCK
    assert seen["query"] == RiskQuery((Decimal("0.3"), Decimal("0.5")), Decimal("1.6"))
    invocation.borrow_fraction = invocation.health_floor = None
    ax.lending_reserve(invocation)
    assert seen["query"] == RiskQuery()


def test_lending_position_reads_a_file_selection_for_the_account(
    live_reader, monkeypatch, tmp_path
):
    seen = {}
    config = tmp_path / "market.json"
    selection = {"chain": "eip155:1", "spoke": "0x" + "94" * 20, "reserve_id": 7}
    config.write_text(json.dumps(selection))

    def read(reader, request, account, block):
        seen.update(request=request, account=account, block=block)
        return {"product": "lending_position"}

    monkeypatch.setattr(ax, "read_lending_position", read)
    live_reader(Pinned())
    invocation = lending_args("ax-lending-position", "aave_v4", config, account=OWNER)
    stated = read_at(monkeypatch, 25908472, BLOCK)
    assert ax.lending_position(invocation) == {"product": "lending_position", "read_at": stated}
    assert (seen["request"].spoke, seen["request"].reserve_id) == (selection["spoke"], 7)
    assert seen["account"] == Account(chain=CHAIN, address=OWNER)
    assert seen["block"] == BLOCK


class Selected(Pinned):
    def pin_block(self, selector):
        return BLOCK


@pytest.mark.parametrize("selector", ["finalized", "latest", 25908472])
def test_an_empty_lending_position_names_the_selector_that_chose_its_block(
    live_reader, monkeypatch, selector
):
    # A position opened after the pinned block reads as empty, with exit 0 and no error.
    empty = {"product": "lending_position", "observation": {"supplied": "0", "borrowed": "0"}}
    monkeypatch.setattr(ax, "read_lending_position", lambda *read: dict(empty))
    live_reader(Selected())
    invocation = lending_args("ax-lending-position", "aave_v3", chain="eip155:1", account=OWNER)
    invocation.asset = "eip155:1/erc20:" + "0x" + "a0" * 20
    invocation.block = selector
    stated = read_at(monkeypatch, selector, BLOCK, age=900)
    assert ax.lending_position(invocation) == {**empty, "read_at": stated}


@pytest.mark.parametrize("selector", ["finalized", "latest"])
def test_a_perp_position_names_the_selector_that_chose_its_block(monkeypatch, tmp_path, selector):
    config = tmp_path / "position.json"
    config.write_text(gmx_request_json())
    read = {"product": "gmx_perp_position", "observation": {"size": "0"}}
    monkeypatch.setattr(ax, "read_gmx_position", lambda *stated: dict(read))
    monkeypatch.setattr(ax, "verified_reader", lambda chain, env_file: Selected())
    invocation = args("ax-perp-position", "gmx_v2", config, account="0x" + "55" * 20)
    invocation.block = selector
    stated = read_at(monkeypatch, selector, BLOCK)
    assert ax.perp_position(invocation) == {**read, "read_at": stated}


def test_lending_selection_naming_another_connector_is_refused(tmp_path):
    config = tmp_path / "market.json"
    config.write_text(json.dumps({"connector": "sparklend", "chain": "eip155:1"}))
    with pytest.raises(ValueError, match="names connector 'sparklend'"):
        ax.lending_request(lending_args("ax-lending-reserve", "aave_v3", config))


def test_balance_passes_asset_account_and_block_through(live_reader, monkeypatch):
    seen = {}
    observation = BalanceObservation(
        asset=AssetType("eip155:1/slip44:60"),
        account=Account(chain=CHAIN, address=OWNER),
        block=BLOCK,
        amount="1",
        denomination="native",
        decimals=18,
    )

    other = "0x" + "22" * 20
    seen = []

    def read(reader, asset, account, block):
        seen.append((asset, account, block))
        return observation.model_copy(update={"account": account})

    monkeypatch.setattr(ax, "read_balance", read)
    live_reader(Pinned())
    invocation = SimpleNamespace(
        product="ax-balance",
        ax_command="balance",
        asset="eip155:1/slip44:60",
        account=[OWNER, other],
        block=25908472,
        env_file=Path("operator.env"),
    )
    result = ax.balance(invocation)
    # Both accounts read at the one concrete block the selector pinned.
    assert [(asset, account.address, block) for asset, account, block in seen] == [
        (observation.asset, OWNER, BLOCK.number),
        (observation.asset, other, BLOCK.number),
    ]
    assert [row["account"]["address"] for row in result["observations"]] == [OWNER, other]


def test_balance_refuses_reads_that_leave_the_pinned_block(live_reader, monkeypatch):
    moved = Block(chain=CHAIN, number=BLOCK.number, hash="0x" + "cd" * 32, timestamp=1)

    def read(reader, asset, account, block):
        return BalanceObservation(
            asset=asset,
            account=account,
            block=moved,
            amount="1",
            denomination="native",
            decimals=18,
        )

    monkeypatch.setattr(ax, "read_balance", read)
    live_reader(Pinned())
    invocation = SimpleNamespace(
        product="ax-balance", asset="eip155:1/slip44:60", account=[OWNER], block=25908472
    )
    invocation.env_file = Path("operator.env")
    with pytest.raises(ValueError, match="pinned block"):
        ax.balance(invocation)


class Tagged(Pinned):
    def pin_block(self, selector):
        self.asked = selector
        return BLOCK


@pytest.mark.parametrize("selector", ["finalized", "latest"])
def test_a_balance_read_states_the_block_it_asked_for_and_its_age(
    live_reader, monkeypatch, capsys, selector
):
    def read(reader, asset, account, block):
        return BalanceObservation(
            asset=asset,
            account=account,
            block=BLOCK,
            amount="1",
            denomination="native",
            decimals=18,
        )

    monkeypatch.setattr(ax, "read_balance", read)
    monkeypatch.setattr(ax, "now", lambda: BLOCK.timestamp + 780)
    reader = live_reader(Tagged())
    invocation = SimpleNamespace(
        product="ax-balance",
        asset="eip155:1/slip44:60",
        account=[OWNER],
        block=selector,
        env_file=Path("operator.env"),
    )
    assert ax.command(invocation) == 0
    output = json.loads(capsys.readouterr().out)
    assert reader.asked == selector
    assert output["read_at"] == {
        "block": selector,
        "number": BLOCK.number,
        "timestamp": BLOCK.timestamp,
        "age_seconds": 780,
    }
    # The observation's own block is unchanged beside it.
    assert output["observations"][0]["block"]["number"] == BLOCK.number


def test_a_numbered_read_states_its_number_and_a_clock_behind_the_block_is_not_zero(monkeypatch):
    monkeypatch.setattr(ax, "now", lambda: BLOCK.timestamp - 3)
    stated = ax.read_at(SimpleNamespace(block=BLOCK.number), BLOCK)["read_at"]
    # A stated number is reported as given, and a negative age as it is.
    assert stated == {
        "block": BLOCK.number,
        "number": BLOCK.number,
        "timestamp": BLOCK.timestamp,
        "age_seconds": -3,
    }


class Named(Pinned):
    def pin_block(self, selector):
        return BLOCK


@pytest.mark.parametrize(
    ("handler", "read", "product"),
    [
        (ax.lending_reserve, "read_lending_reserve", "ax-lending-reserve"),
        (ax.perp_markets, "read_gmx_market", "ax-perp-markets"),
    ],
)
def test_reads_that_hold_no_position_state_the_same_read_at(
    live_reader, monkeypatch, handler, read, product
):
    monkeypatch.setattr(ax, read, lambda *stated: {"product": product})
    monkeypatch.setattr(ax, "lending_request", lambda args: SimpleNamespace(chain=CHAIN.chain_id))
    monkeypatch.setattr(ax, "parse_request", lambda args: SimpleNamespace(chain=CHAIN.chain_id))
    live_reader(Named())
    invocation = SimpleNamespace(
        product=product,
        block="finalized",
        env_file=Path("operator.env"),
        borrow_fraction=None,
        health_floor=None,
    )
    stated = read_at(monkeypatch, "finalized", BLOCK, age=780)
    assert handler(invocation) == {"product": product, "read_at": stated}


def test_the_command_clock_is_whole_seconds(monkeypatch):
    monkeypatch.setattr(ax.time, "time", lambda: 12.9)
    assert ax.now() == 12


def gmx_request_json():
    chain = Chain(chain_id=ChainId("eip155:42161"), environment="mainnet")
    reference = str(chain.chain_id)
    assets = tuple(AssetType(f"{reference}/erc20:0x{i:040x}") for i in range(1, 5))
    fields = Contracts.model_fields
    contracts = Contracts(**{field: f"0x{i:040x}" for i, field in enumerate(fields, 10)})
    market = Market(
        chain=chain,
        share_asset=assets[0],
        index_asset=assets[1],
        long_asset=assets[1],
        short_asset=assets[2],
        wrapped_native=assets[1],
        contracts=contracts,
    )
    request = GmxPositionRequest(market=market, collateral_asset=market.long_asset, side="long")
    return request.model_dump_json()


def test_perp_position_passes_request_account_and_block_through(live_reader, monkeypatch, tmp_path):
    config = tmp_path / "gmx.json"
    config.write_text(gmx_request_json())
    block = Block(
        chain=Chain(chain_id=ChainId("eip155:42161"), environment="mainnet"),
        number=123,
        hash="0x" + "cd" * 32,
        timestamp=1_700_000_000,
    )
    seen = {}

    class Reader:
        def pin_block(self, selector):
            seen["selector"] = selector
            return block

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

    def read(reader, request, account, pinned):
        seen.update(request=request, account=account, pinned=pinned)
        return {"product": "gmx_perp_position"}

    monkeypatch.setattr(ax, "read_gmx_position", read)
    monkeypatch.setattr(ax, "verified_reader", lambda chain, env_file: Reader())
    stated = read_at(monkeypatch, 25908472, block)
    result = ax.perp_position(args("ax-perp-position", "gmx_v2", config, account="0x" + "55" * 20))
    assert result == {"product": "gmx_perp_position", "read_at": stated}
    assert seen["selector"] == 25908472
    assert seen["pinned"] == block
    assert seen["account"].address == "0x" + "55" * 20
    assert seen["request"].market.chain.chain_id == ChainId("eip155:42161")


def test_unsupported_connector_pairs_fail_closed(tmp_path):
    config = tmp_path / "source.json"
    config.write_text("{}")
    invocation = args("ax-pool-price", "aave_v4", config)
    with pytest.raises(ValueError, match="unsupported connector"):
        ax.parse_request(invocation)


BASE_WETH = "eip155:8453/erc20:0x4200000000000000000000000000000000000006"
BASE_USDC = "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


class Seam:
    def __init__(self, block):
        self.block = block

    def pin_block(self, selector):
        assert selector == "finalized"
        return self.block

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


# What a stand-in discovery read returns, and the block every such read states.
LISTED = {"product": "listed"}
DISCOVERED_AT = {
    "block": "finalized",
    "number": 36_000_000,
    "timestamp": 1_790_000_000,
    "age_seconds": 7,
}


@pytest.fixture
def discovery_seams(monkeypatch):
    block = Block(
        chain=Chain(chain_id=ChainId("eip155:8453"), environment="mainnet"),
        number=36_000_000,
        hash="0x" + "cd" * 32,
        timestamp=1_790_000_000,
    )
    seen = {}
    assert read_at(monkeypatch, "finalized", block) == DISCOVERED_AT
    monkeypatch.setattr(ax, "lookup_source", lambda args: Seam(None))
    monkeypatch.setattr(ax, "observation_reader", lambda args, chain: Seam(block))
    monkeypatch.setattr(
        ax.ax_discovery,
        "resolve",
        lambda *call: seen.setdefault("resolve", call[2:]) and dict(LISTED),
    )
    monkeypatch.setattr(
        ax.ax_discovery,
        "dex_pools",
        lambda *call: seen.setdefault("pools", call[2:]) and dict(LISTED),
    )
    monkeypatch.setattr(
        ax.ax_lending_markets,
        "lending_markets",
        lambda *call: seen.__setitem__("markets", call[1:]) or dict(LISTED),
    )
    return block, seen


def discovery_args(**values):
    return SimpleNamespace(chain="eip155:8453", block="finalized", limit=10, **values)


def test_discovery_handlers_pin_one_block_and_pass_the_validated_query(discovery_seams):
    block, seen = discovery_seams
    assert ax.token_resolve(discovery_args(query="cbBTC")) == {**LISTED, "read_at": DISCOVERED_AT}
    assert seen["resolve"] == (block, "cbBTC", 10)
    pools = discovery_args(
        token=BASE_WETH, quote=BASE_USDC, connector_filter=None, depth_bps=None, offset=20
    )
    assert ax.pool_discovery(pools) == {**LISTED, "read_at": DISCOVERED_AT}
    pinned, query = seen["pools"]
    assert pinned == block
    assert (str(query.token), str(query.quote), query.offset) == (BASE_WETH, BASE_USDC, 20)


def test_lending_markets_pin_one_block_and_refuse_other_chains(discovery_seams):
    block, seen = discovery_seams
    markets = discovery_args(
        connector="morpho_blue",
        asset=BASE_USDC,
        collateral=BASE_WETH,
        offset=10,
        include_unlisted=True,
    )
    assert ax.lending_markets(markets) == {**LISTED, "read_at": DISCOVERED_AT}
    lookups, pinned, query = seen["markets"]
    # The lookup transport is opened beside the reader, as for the other discovery reads.
    assert pinned == block and isinstance(lookups, Seam) and lookups.block is None
    assert (query.connector, str(query.asset), str(query.collateral), query.offset) == (
        "morpho_blue",
        BASE_USDC,
        BASE_WETH,
        10,
    )
    assert query.include_unlisted is True
    listing = discovery_args(
        connector="aave_v3", asset=None, collateral=None, offset=0, include_unlisted=False
    )
    assert ax.lending_markets(listing) == {**LISTED, "read_at": DISCOVERED_AT}
    assert seen["markets"][2].asset is None
    other = discovery_args(
        connector="aave_v3",
        asset="eip155:1/erc20:0x" + "aa" * 20,
        collateral=None,
        offset=0,
        include_unlisted=False,
    )
    with pytest.raises(ValueError, match="--asset must be an asset on eip155:8453"):
        ax.lending_markets(other)


def test_discovery_refuses_assets_of_another_chain_and_other_coins(discovery_seams):
    wrong_chain = discovery_args(
        token="eip155:1/erc20:0x" + "aa" * 20,
        quote=None,
        connector_filter=None,
        depth_bps=None,
        offset=0,
    )
    with pytest.raises(ValueError, match="--token must be an asset on eip155:8453"):
        ax.pool_discovery(wrong_chain)
    other_coin = discovery_args(
        token=BASE_USDC,
        quote="eip155:8453/slip44:0",
        connector_filter=None,
        depth_bps=None,
        offset=0,
    )
    with pytest.raises(ValueError, match="--quote must be an ERC-20 asset; use the wrapped native"):
        ax.pool_discovery(other_coin)


def test_discovery_asks_for_the_wrapped_token_of_the_native_coin(discovery_seams):
    _, seen = discovery_seams
    # The native coin asks for the wrapped token, whose filter matches native-coin pools.
    for token, quote in (
        ("eip155:8453/slip44:60", None),
        ("eip155:8453/erc20:0x" + "aa" * 20, "eip155:8453/slip44:60"),
    ):
        args = discovery_args(
            token=token, quote=quote, connector_filter=None, depth_bps=None, offset=0
        )
        assert ax.pool_discovery(args) == {**LISTED, "read_at": DISCOVERED_AT}
        _, query = seen.pop("pools")
        asked = query.token if quote is None else query.quote
        assert str(asked) == BASE_WETH


@pytest.mark.parametrize(
    ("native", "wrapped"),
    [
        ("eip155:1/slip44:60", "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
        ("eip155:56/slip44:714", "eip155:56/erc20:0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
        ("eip155:137/slip44:966", "eip155:137/erc20:0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
        (
            "eip155:43114/slip44:9005",
            "eip155:43114/erc20:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        ),
    ],
)
def test_each_chain_native_coin_asks_for_its_own_wrapped_token(native, wrapped):
    chain = ChainId(native.split("/")[0])
    assert str(ax.pool_token_on(chain, native, "--token")) == wrapped
    # Another chain's native coin is not this chain's.
    with pytest.raises(ValueError):
        ax.pool_token_on(ChainId("eip155:8453"), native, "--token")


def test_inline_pool_price_key_uses_the_reviewed_deployment():
    inline = SimpleNamespace(
        product="ax-pool-price",
        ax_command="pool-price",
        connector="uniswap_v4",
        config=None,
        chain="eip155:8453",
        currency0="eip155:8453/slip44:60",
        currency1=BASE_USDC,
        fee=500,
        tick_spacing=10,
        base=BASE_USDC,
        hooks=None,
    )
    request = ax.parse_request(inline)
    assert request.source.pool_manager == ax.roles_for(ChainId("eip155:8453")).pool_manager
    assert (str(request.base), str(request.quote)) == (BASE_USDC, "eip155:8453/slip44:60")
    markets = SimpleNamespace(
        product="ax-perp-markets",
        ax_command="perp-markets",
        connector="gmx_v2",
        config=None,
        chain="eip155:42161",
        query="BTC/USD",
    )
    assert ax.parse_request(markets).query == "BTC/USD"


def test_inline_pool_price_key_takes_a_reviewed_hook_and_the_connector_admits_it():
    from tests.uniswap_hook_support import PONS_HOOK, PRIORS, USDG

    inline = SimpleNamespace(
        product="ax-pool-price",
        ax_command="pool-price",
        connector="uniswap_v4",
        config=None,
        chain="eip155:4663",
        currency0=f"eip155:4663/erc20:{USDG}",
        currency1=f"eip155:4663/erc20:{PRIORS}",
        fee=0,
        tick_spacing=200,
        base=None,
        hooks=PONS_HOOK,
    )
    assert ax.parse_request(inline).source.hooks == PONS_HOOK
    unknown = "0x" + "12" * 20
    with pytest.raises(ValueError, match="has no reviewed profile on eip155:4663"):
        ax.parse_request(SimpleNamespace(**(vars(inline) | {"hooks": unknown})))
    with pytest.raises(ValueError, match="requires a static fee"):
        ax.parse_request(SimpleNamespace(**(vars(inline) | {"fee": 8388608})))


def test_lookups_use_the_same_transport_as_reads(monkeypatch, tmp_path):
    token = tmp_path / "token"
    token.write_text("ab" * 32)
    token.chmod(0o600)
    env = tmp_path / "operator.env"
    env.write_text("COINGECKO_API_KEY=key\n")
    direct = ax.lookup_source(SimpleNamespace(env_file=env))
    assert isinstance(direct, ax.VendorLookups) and direct.env_file == env
    direct.close()
    hosted = ax.lookup_source(
        SimpleNamespace(env_file=None, gateway_url="http://127.0.0.1:8300", gateway_auth_file=token)
    )
    assert isinstance(hosted, ax.GatewayLookups) and hosted.token == "ab" * 32
    hosted.close()


def test_a_checksummed_base_equal_to_currency0_prices_currency0():
    # CAIP-19 lowercases the address; the same asset typed checksummed is still currency0.
    checksummed = "eip155:8453/erc20:0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    inline = SimpleNamespace(
        product="ax-pool-price",
        ax_command="pool-price",
        connector="uniswap_v4",
        config=None,
        chain="eip155:8453",
        currency0=BASE_USDC,
        currency1="eip155:8453/erc20:0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        fee=500,
        tick_spacing=10,
        base=checksummed,
        hooks=None,
    )
    request = ax.parse_request(inline)
    assert str(request.base) == BASE_USDC
    assert str(request.quote) == "eip155:8453/erc20:0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"
