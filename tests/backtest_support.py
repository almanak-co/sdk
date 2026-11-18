"""Synthetic archive transport for process proofs using the actual Aave strategy."""

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.aave_v4.history import Market

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/aave-v4-historical-market.json"
REFERENCE = ROOT / "strategies/aave_v4_weth_supply"
# The unconverted reference bytes, frozen for the harness loader paths.
LEGACY = ROOT / "tests/fixtures/author/aave_v4_weth_supply/legacy"
# Proxy admission reads for the same Spokes and Hubs, captured independently.
AUTHORITY_READS = ROOT / "tests/fixtures/aave-v4-economic-authority-reads.json"


class Archive:
    def __init__(self):
        fixture = json.loads(FIXTURE.read_text())
        self.markets = [Market.model_validate_json(json.dumps(m)) for m in fixture["markets"]]
        self.start = self.markets[0].reserve.block.timestamp
        self.responses = {}
        self.calls = []
        rows = (*fixture["requests"], *json.loads(AUTHORITY_READS.read_text()))
        for evidence in rows:
            request = evidence["request"]
            if request["method"] in ("eth_call", "eth_getCode", "eth_getStorageAt"):
                self.responses[self.key(request["method"], request["params"])] = evidence["result"]

    def key(self, method, params):
        return json.dumps([method, params[:-1]], sort_keys=True)

    def result(self, method, params):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return "0x1"
        if method == "eth_getBlockByNumber":
            number = 1000 if params[0] == "finalized" else int(params[0], 16)
            return {
                "number": hex(number),
                "hash": "0x" + f"{number:064x}",
                "timestamp": hex(self.start + number * 12),
            }
        if method == "eth_call":
            converted = self.preview(params[0])
            if converted is not None:
                return converted
        return self.responses[self.key(method, params)]

    def preview(self, call):
        methods = {
            keccak(text="previewAddByAssets(uint256,uint256)")[:4].hex(): "supplied_shares",
            keccak(text="previewRemoveByShares(uint256,uint256)")[:4].hex(): "supplied_assets",
        }
        selector = call["data"][2:10]
        market = next((m for m in self.markets if m.reserve.source.hub == call["to"]), None)
        if market is None:
            return None
        if selector == HUB_ASSET:
            # The captured fixture predates market resolution; the Hub's asset view
            # is synthesized from the same verified reserve observation.
            asset = market.reserve.asset
            values = (asset.identifier.reference, asset.decimals)
            return "0x" + encode(("address", "uint8"), values).hex()
        if selector not in methods:
            return None
        _, amount = decode(("uint256", "uint256"), bytes.fromhex(call["data"][10:]))
        return "0x" + encode(("uint256",), (getattr(market, methods[selector])(amount),)).hex()


def archive_server(archive):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))

            def respond(item):
                result = archive.result(item["method"], item["params"])
                return {"jsonrpc": "2.0", "id": item["id"], "result": result}

            payload = (
                [respond(item) for item in request]
                if isinstance(request, list)
                else respond(request)
            )
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def author_config(directory, archive, server):
    """Harness-root copies of the frozen unconverted reference over the archive."""
    raw = yaml.safe_load((LEGACY / "config.yaml").read_text())
    raw["strategy"]["debts"] = {}
    backtest = yaml.safe_load((LEGACY / "backtest.yaml").read_text())
    backtest["data"]["history"][0]["provider"] = {"kind": "rpc_url", "url_env": "ARCHIVE_TEST_RPC"}
    backtest["window"].update(start=iso(archive.start + 1), end=iso(archive.start + 901))
    backtest["execution"]["decision_interval"] = "60s"
    config = directory / "config.yaml"
    config.write_text(yaml.safe_dump(raw))
    (directory / "backtest.yaml").write_text(yaml.safe_dump(backtest))
    env_file = directory / "providers.env"
    env_file.write_text(f"ARCHIVE_TEST_RPC=http://127.0.0.1:{server.server_port}\n")
    return config, env_file


HUB_ASSET = keccak(text="getAssetUnderlyingAndDecimals(uint256)")[:4].hex()


def converted_config(directory, archive, server, settings="backtest.yaml"):
    """Public-root copies of the reference; the operator file owns the archive provider.

    The archive holds the two WETH reserves only, so the USDC market, the debts
    and the USDC inventory are dropped from the converted copies.
    """
    from almanak.configuration.yaml import load
    from tests.test_author_resolution import frozen_cadence

    raw = frozen_cadence(load(REFERENCE / "config.yaml").value)
    raw["strategy"].pop("debts")
    raw["markets"].pop("main_usdc_market")
    kept = {
        m["asset"]
        for n, m in load(LEGACY / "config.yaml").value["markets"].items()
        if n in raw["markets"]
    }
    kept.add("eip155:1/slip44:60")
    backtest = yaml.safe_load((REFERENCE / settings).read_text())
    for fund in backtest["inventory"]["funds"].values():
        fund["balances"] = [b for b in fund["balances"] if b["asset"] in kept]
    backtest["window"].update(start=iso(archive.start + 1), end=iso(archive.start + 901))
    backtest["execution"]["decision_interval"] = "60s"
    config = directory / "config.yaml"
    config.write_text(yaml.safe_dump(raw, sort_keys=False))
    (directory / settings).write_text(yaml.safe_dump(backtest, sort_keys=False))
    operator = directory / "operator.yaml"
    operator.write_text(
        yaml.safe_dump(
            {
                "modes": ["backtest"],
                "networks": [
                    {
                        "chain": "eip155:1",
                        "provider": {"kind": "rpc_url", "url_env": "ARCHIVE_TEST_RPC"},
                    }
                ],
                "vendors": [
                    {
                        "name": "market_prices",
                        "provider": {
                            "kind": "coingecko",
                            "api": "demo",
                            "api_key_env": "COINGECKO_API_KEY",
                        },
                    }
                ],
                "accounts": {"bindings": []},
            }
        )
    )
    env_file = directory / "providers.env"
    env_file.write_text(f"ARCHIVE_TEST_RPC=http://127.0.0.1:{server.server_port}\n")
    return config, operator, env_file


def iso(value):
    return datetime.fromtimestamp(value, UTC).isoformat()
