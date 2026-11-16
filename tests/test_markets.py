"""`almanak markets`: what each market resolves to, for the user to check before approving."""

import json
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import almanak.ax
from almanak import markets as m
from almanak.cli import main

BASE = "eip155:8453"
USDC = f"{BASE}/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
CBBTC = f"{BASE}/erc20:0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
WETH = f"{BASE}/erc20:0x4200000000000000000000000000000000000006"
POOL = "0xfbb6eed8e7aa03b138556eedaf5d271a5e1e43ef"
SELECTION = {"connector": "x", "chain": BASE, "pool": "0x" + "11" * 20}
BLOCK = {"number": 1, "hash": "0x" + "22" * 32}


def test_a_pool_is_its_contract_and_its_currencies():
    resolution = {
        "selection": SELECTION,
        "pool": POOL,
        "currencies": [USDC, CBBTC],
        "fee": 500,
        "block": 53043061,
    }
    assert m.identity("uniswap_v3", BASE, resolution)["contracts"] == [
        {"role": "pool", "address": POOL},
        {"role": "currencies", "address": USDC.split(":")[-1], "asset": USDC},
        {"role": "currencies", "address": CBBTC.split(":")[-1], "asset": CBBTC},
    ]


def test_a_reserve_names_each_token_and_contract_once_by_its_field():
    resolution = {
        "selection": SELECTION,
        "core": "0x" + "bb" * 20,
        "market_id": "0x" + "91" * 32,
        "loan_asset": USDC,
        "collateral_asset": CBBTC,
        "oracle": "0x" + "66" * 20,
        "asset": USDC,
        "lltv": 860000000000000000,
    }
    contracts = m.identity("morpho_blue", BASE, resolution)["contracts"]
    assert [c["role"] for c in contracts] == [
        "core",
        "market_id",
        "loan_asset",
        "collateral_asset",
        "oracle",
    ]


def test_nested_assets_take_their_parents_name_and_listed_contracts_are_left_out():
    resolution = {
        "selection": SELECTION,
        "block": BLOCK,
        "market": {"address": "0x" + "70" * 20},
        "index": {"chain": BASE, "identifier": WETH, "decimals": 18},
        "identities": [{"name": "reader", "address": "0x" + "ee" * 20}],
    }
    contracts = m.identity("gmx_v2", BASE, resolution)["contracts"]
    assert [(c["role"], c["address"]) for c in contracts] == [
        ("market", "0x" + "70" * 20),
        ("index", WETH.split(":")[-1]),
    ]


def test_symbols_join_their_tokens_and_two_names_for_one_market_share_an_entry():
    identity = m.identity("uniswap_v3", BASE, {"pool": POOL, "currencies": [USDC, CBBTC]})
    assets = {USDC: {"symbol": "USDC", "decimals": 6}, CBBTC: {"decimals": 8}}
    assert m.named({"pool": identity, "pool_again": identity}, assets) == [
        {
            "name": "pool, pool_again",
            "connector": "uniswap_v3",
            "chain": BASE,
            "contracts": [
                {"role": "pool", "address": POOL},
                {
                    "role": "currencies",
                    "address": USDC.split(":")[-1],
                    "asset": USDC,
                    "symbol": "USDC",
                },
                {"role": "currencies", "address": CBBTC.split(":")[-1], "asset": CBBTC},
            ],
        }
    ]


def test_the_command_writes_each_market_and_lists_what_it_could_not_read(
    tmp_path, monkeypatch, capsys
):
    pins = []

    class Reader:
        def pin_block(self, block):
            pins.append(block)
            return SimpleNamespace(number=1, hash="0x" + "22" * 32)

    @contextmanager
    def reader(args, chain):
        yield Reader()

    def resolution(binding, reader, head, market):
        if market["pool"] == "bad":
            raise ValueError("no such pool")
        return {"pool": POOL, "currencies": [USDC, CBBTC]}

    token = tmp_path / "token"
    token.write_text("t")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", str(token))
    monkeypatch.setattr(almanak.ax, "observation_reader", reader)
    monkeypatch.setattr(m, "resolution", resolution)
    monkeypatch.setattr(m, "asset_meta", lambda r, h, a: {"symbol": "USDC"} if a == USDC else {})
    config = tmp_path / "config.yaml"
    markets = {
        "pool": {"connector": "uniswap_v3", "chain": BASE, "pool": POOL},
        "broken": {"connector": "uniswap_v3", "chain": BASE, "pool": "bad"},
        "other": {"connector": "unknown", "chain": BASE},
    }
    config.write_text(json.dumps({"strategy": {"allocations": {}}, "markets": markets}))
    out = tmp_path / ".almanak" / "markets.json"
    argv = ["almanak", "markets", "--config", str(config), "--out", str(out)]
    monkeypatch.setattr(sys, "argv", argv)
    assert main() == 0
    capsys.readouterr()
    written = json.loads(out.read_text())
    assert [entry["name"] for entry in written["markets"]] == ["pool"]
    assert written["assets"] == {USDC: {"symbol": "USDC"}}
    assert sorted(written["skipped"]) == ["broken", "other"]
    # Both markets on the chain resolve at one pinned block, which the record names.
    assert len(pins) == 1
    assert written["blocks"] == {BASE: {"number": 1, "hash": "0x" + "22" * 32}}


def test_the_command_takes_the_read_gateway_from_the_session(tmp_path, monkeypatch):
    from almanak.cli_arguments import build_parser, parse_validated

    token = tmp_path / "token"
    token.write_text("t")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", str(token))
    args = parse_validated(build_parser(), ["markets"])
    assert (args.gateway_url, args.gateway_auth_file) == ("http://127.0.0.1:1", token)


def test_every_reference_market_resolves_through_its_connectors_author_binding():
    # The resolver `almanak check` uses; a connector bound there is covered here too.
    from pathlib import Path

    import yaml

    seen = set()
    for config in sorted(Path("strategies").glob("*/config.yaml")):
        for name, market in (yaml.safe_load(config.read_text()).get("markets") or {}).items():
            binding = m.bound(market["connector"])
            assert binding is not None, f"{config}: {name}"
            binding.selection.model_validate_json(json.dumps(market))
            seen.add(market["connector"])
    assert {"uniswap_v4", "gmx_v2", "curve", "morpho_blue", "aerodrome"} <= seen
    assert m.bound("unknown") is None


def test_a_market_resolves_with_its_bindings_selection_and_reports_its_summary():
    from dataclasses import replace

    market = {
        "connector": "uniswap_v4",
        "chain": "eip155:42161",
        "currency0": "eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "currency1": "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "fee": 500,
        "tick_spacing": 10,
    }
    calls = []

    class Resolved:
        def summary(self):
            return {"pool_id": "0x" + "ab" * 32, "currencies": [market["currency0"]]}

    def resolve(reader, selection, block):
        calls.append((reader, selection, block))
        return Resolved()

    binding = replace(m.bound("uniswap_v4"), resolve=resolve)
    assert m.resolution(binding, "reader", BLOCK, market) == Resolved().summary()
    [(reader, selection, block)] = calls
    assert (reader, block) == ("reader", BLOCK)
    assert type(selection).__module__ == "almanak.connectors.uniswap_v4.resolution"
    assert (selection.fee, selection.tick_spacing) == (500, 10)


def test_a_contract_in_several_roles_is_listed_once_with_each():
    weth = f"{BASE}/erc20:0x4200000000000000000000000000000000000006"
    resolution = {
        "share": f"{BASE}/erc20:0x" + "70" * 20,
        "index": weth,
        "long": weth,
        "short": USDC,
        "wrapped_native": weth,
    }
    contracts = m.identity("gmx_v2", BASE, resolution)["contracts"]
    assert [(c["role"], c["address"]) for c in contracts] == [
        ("share", "0x" + "70" * 20),
        ("index, long, wrapped_native", weth.split(":")[-1]),
        ("short", USDC.split(":")[-1]),
    ]


def test_an_entry_that_is_not_a_market_is_skipped_and_the_others_still_resolve(
    tmp_path, monkeypatch
):
    @contextmanager
    def reader(args, chain):
        class Reader:
            def pin_block(self, block):
                return SimpleNamespace(number=7, hash="0x" + "33" * 32)

        yield Reader()

    token = tmp_path / "token"
    token.write_text("t")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", str(token))
    monkeypatch.setattr(almanak.ax, "observation_reader", reader)
    monkeypatch.setattr(m, "resolution", lambda b, r, h, market: {"pool": POOL})
    monkeypatch.setattr(m, "asset_meta", lambda r, h, a: {})
    config = tmp_path / "config.yaml"
    config.write_text(
        "markets:\n  broken:\n  pool: {connector: uniswap_v3, chain: '"
        + BASE
        + "', pool: '"
        + POOL
        + "'}\n",
        encoding="utf-8",
    )
    from almanak.cli_arguments import build_parser, parse_validated

    record = m.resolve(parse_validated(build_parser(), ["markets", "--config", str(config)]))
    assert [entry["name"] for entry in record["markets"]] == ["pool"]
    assert list(record["skipped"]) == ["broken"]
