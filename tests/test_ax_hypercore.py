"""`ax hypercore-markets` and `ax hypercore-account` from recorded mainnet answers."""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak import ax, ax_hypercore
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors.hyperliquid import assets
from almanak.connectors.hyperliquid.markets import find_market, perp_markets
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import GatewayInfo, HyperCoreInfo
from almanak.gateway.hypercore_shapes import RESPONSES, SpotToken
from tests.hypercore_recordings import SUBJECTS, RecordedInfo, answer

GATEWAY = ["--gateway-url", "http://127.0.0.1:8300", "--gateway-auth-file", "/tmp/token"]


def parse(argv):
    return parse_validated(build_parser(), argv)


def markets_of(meta, contexts):
    return perp_markets(RESPONSES["metaAndAssetCtxs"].validate_json(json.dumps([meta, contexts])))


def recorded_markets():
    return markets_of(*answer("metaAndAssetCtxs"))


def test_one_market_reports_its_target_asset_index_and_margin_tiers():
    report = ax_hypercore.markets(RecordedInfo(), "BTC", 20, 0, False)
    assert report["product"] == "hypercore_perp_markets"
    assert report["venue"] == {"chain": "eip155:999", "venue": "hypercore", "dex": ""}
    assert report["source"]["query"] == "metaAndAssetCtxs" and report["matched"] == 1
    (btc,) = report["markets"]
    assert btc["target"] == {"dex": "", "coin": "BTC"} and btc["asset"] == 0
    assert btc["meta"] == {"name": "BTC", "szDecimals": 5, "maxLeverage": 40, "marginTableId": 56}
    assert btc["margin_tiers"][0] == {"lowerBound": "0.0", "maxLeverage": 40}
    assert btc["context"]["markPx"] == answer("metaAndAssetCtxs")[1][0]["markPx"]


def test_a_market_is_found_by_its_name_or_apart_from_letter_case():
    markets = recorded_markets()
    assert find_market(markets, "kPEPE").meta.name == "kPEPE"
    assert find_market(markets, "kpepe").meta.name == "kPEPE"
    assert find_market(markets, "eth").asset == 1
    with pytest.raises(DataError) as missing:
        find_market(markets, "NOPE")
    assert missing.value.kind == "not_found"


def test_a_market_named_twice_is_inconsistent_and_a_case_clash_ambiguous():
    meta, contexts = answer("metaAndAssetCtxs")
    meta["universe"][1]["name"] = "BTC"
    with pytest.raises(DataError) as twice:
        markets_of(meta, contexts)
    assert twice.value.kind == "inconsistent"
    meta["universe"][1]["name"] = "Abc"
    meta["universe"][2]["name"] = "ABC"
    markets = markets_of(meta, contexts)
    with pytest.raises(DataError) as clash:
        find_market(markets, "abc")
    assert clash.value.kind == "ambiguous"


def test_an_unlisted_margin_table_is_left_unknown():
    markets = recorded_markets()
    listed = {identifier for identifier, _ in answer("metaAndAssetCtxs")[0]["marginTables"]}
    unlisted = [m for m in markets if m.meta.marginTableId not in listed]
    assert unlisted and all(m.margin_tiers is None for m in unlisted)


def test_listings_rank_tradable_markets_by_the_venues_own_volume():
    markets = recorded_markets()
    tradable = [m for m in markets if not m.meta.isDelisted]
    report = ax_hypercore.markets(RecordedInfo(), None, 5, 0, False)
    assert report["matched"] == len(tradable) < len(markets)
    volumes = [Decimal(m["context"]["dayNtlVlm"]) for m in report["markets"]]
    assert len(volumes) == 5 and volumes == sorted(volumes, reverse=True)
    paged = ax_hypercore.markets(RecordedInfo(), None, 5, 5, False)
    assert Decimal(paged["markets"][0]["context"]["dayNtlVlm"]) <= volumes[-1]
    everything = ax_hypercore.markets(RecordedInfo(), None, 50, 0, True)
    assert everything["matched"] == len(markets)


def test_an_account_reports_its_role_mode_perp_state_and_identified_balances():
    info = RecordedInfo()
    report = ax_hypercore.accounts(info, [SUBJECTS["leader"], SUBJECTS["fresh"]])
    assert report["product"] == "hypercore_account"
    assert [s["query"] for s in report["sources"]] == ["spotMeta", "meta"]
    leader, fresh = report["accounts"]
    assert leader["role"] == {"role": "user"} and leader["abstraction"] == "default"
    assert leader["perp"]["state"] == "as_reported"
    recorded = answer("clearinghouseState.leader")["marginSummary"]["accountValue"]
    assert leader["perp"]["margin_summary"]["accountValue"] == recorded
    balances = {b["token"]["name"]: b for b in leader["spot"]}
    assert balances["USDC"]["asset"] == str(assets.USDC)
    usdc = next(b for b in answer("spotClearinghouseState.leader")["balances"] if b["token"] == 0)
    assert balances["USDC"]["total"] == usdc["total"]
    assert balances["HYPE"]["asset"] == "eip155:999/slip44:2457"
    assert (
        balances["PURR"]["asset"] == "eip155:999/erc20:0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
    )
    # A token HyperCore links to no HyperEVM contract has no identity.
    assert balances["MUNCH"]["asset"] is None
    assert [s["query"] for s in leader["sources"]] == [
        "userRole",
        "userAbstraction",
        "clearinghouseState",
        "spotClearinghouseState",
        "openOrders",
        "extraAgents",
    ]
    assert leader["open_orders"] == answer("openOrders.leader")
    assert leader["agents"] == answer("extraAgents.leader")
    assert fresh["role"] == {"role": "missing"} and fresh["spot"] == []
    assert fresh["perp"]["positions"] == [] and fresh["open_orders"] == []


def test_positions_carry_their_asset_index():
    child = SUBJECTS["hlp_child"]
    info = RecordedInfo()
    perp = info.by_request
    # The child vault's other account queries were not recorded; reuse the leader's.
    for label in ("userRole", "userAbstraction", "spotClearinghouseState", "extraAgents"):
        request = next(r for r, name in perp.items() if name == f"{label}.leader")
        moved = request.model_copy(update={"info": request.info.model_copy(update={"user": child})})
        perp[moved] = f"{label}.leader"
    (report,) = ax_hypercore.accounts(info, [child])["accounts"]
    positions = {p["position"]["coin"]: p for p in report["perp"]["positions"]}
    assert positions["BTC"]["asset"] == 0 and positions["ETH"]["asset"] == 1


def test_a_unified_account_marks_its_perp_state_not_meaningful():
    (report,) = ax_hypercore.accounts(RecordedInfo(), [SUBJECTS["unified"]])["accounts"]
    assert report["abstraction"] == "unifiedAccount"
    assert report["perp"]["state"] == "not_meaningful"
    # Its spot state carries the field only unified accounts answer.
    assert "tokenToAvailableAfterMaintenance" in answer("spotClearinghouseState.unified")
    assert report["spot"] and all(b["token"]["name"] for b in report["spot"])


def test_open_orders_keep_their_cloids_and_agents_their_expiries():
    (report,) = ax_hypercore.accounts(RecordedInfo(), [SUBJECTS["cloid_trader"]])["accounts"]
    assert report["abstraction"] == "disabled" and report["perp"]["state"] == "as_reported"
    assert report["open_orders"] and all(order["cloid"] for order in report["open_orders"])
    assert report["agents"] and all(agent["validUntil"] for agent in report["agents"])


def test_a_balance_of_a_token_spot_meta_lacks_is_inconsistent():
    balances = answer("spotClearinghouseState.leader")
    balances["balances"][0]["token"] = 99_999
    info = RecordedInfo({"spotClearinghouseState.leader": balances})
    with pytest.raises(DataError) as refused:
        ax_hypercore.accounts(info, [SUBJECTS["leader"]])
    assert refused.value.kind == "inconsistent"


def test_accounts_must_be_evm_addresses():
    with pytest.raises(ValueError, match="20-byte EVM address"):
        ax_hypercore.accounts(RecordedInfo(), ["0x12"])


def token(**changes) -> SpotToken:
    usdc = next(t for t in answer("spotMeta")["tokens"] if t["index"] == 0)
    return SpotToken.model_validate({**usdc, **changes})


def test_pinned_spot_identities_refuse_a_changed_link():
    wallet = {"address": assets.CORE_DEPOSIT_WALLET, "evm_extra_wei_decimals": -2}
    other = {"address": "0x" + "12" * 20, "evm_extra_wei_decimals": 0}
    assert assets.spot_asset(token()) == assets.USDC
    for refused in (
        token(evmContract=other),
        token(evmContract=None),
        token(tokenId=assets.HYPE_TOKEN_ID, evmContract=other),
        token(tokenId="0x" + "34" * 16, evmContract=wallet),
    ):
        with pytest.raises(DataError) as mismatch:
            assets.spot_asset(refused)
        assert mismatch.value.kind == "identity"
    assert str(assets.spot_asset(token(tokenId="0x" + "34" * 16, evmContract=other))) == (
        "eip155:999/erc20:0x" + "12" * 20
    )
    assert assets.spot_asset(token(tokenId="0x" + "34" * 16, evmContract=None)) is None


def test_hypercore_commands_parse_without_a_block():
    markets = parse(["ax", "hypercore-markets", *GATEWAY])
    assert (markets.product, markets.query, markets.limit, markets.offset) == (
        "ax-hypercore-markets",
        None,
        20,
        0,
    )
    assert markets.include_delisted is False
    one = parse(["ax", "hypercore-markets", "--query", "BTC", "--env-file", "/tmp/env"])
    assert one.query == "BTC"
    account = parse(
        ["ax", "hypercore-account", "--account", SUBJECTS["leader"]]
        + ["--account", SUBJECTS["fresh"], *GATEWAY]
    )
    assert account.account == [SUBJECTS["leader"], SUBJECTS["fresh"]]
    for argv in (
        ["ax", "hypercore-markets", "--limit", "51", *GATEWAY],
        ["ax", "hypercore-markets", "--offset", "501", *GATEWAY],
        ["ax", "hypercore-markets", "--block", "latest", *GATEWAY],
        ["ax", "hypercore-account", *GATEWAY],
    ):
        with pytest.raises(SystemExit):
            parse(argv)


def test_hypercore_commands_take_the_read_transport(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("ab" * 32)
    token_file.chmod(0o600)
    direct = ax.info_source(SimpleNamespace(env_file=tmp_path / "operator.env"))
    relayed = ax.info_source(
        SimpleNamespace(
            env_file=None, gateway_url="http://127.0.0.1:8300", gateway_auth_file=token_file
        )
    )
    assert isinstance(direct, HyperCoreInfo) and isinstance(relayed, GatewayInfo)
    direct.close()
    relayed.close()
    monkeypatch.setattr(ax, "info_source", lambda args: RecordedInfo())
    args = parse(["ax", "hypercore-markets", "--query", "ETH", *GATEWAY])
    assert ax.hypercore_markets(args)["markets"][0]["asset"] == 1
    args = parse(["ax", "hypercore-account", "--account", SUBJECTS["fresh"], *GATEWAY])
    assert ax.hypercore_account(args)["accounts"][0]["role"] == {"role": "missing"}
