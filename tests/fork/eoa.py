"""Explicit EOA-only regression configuration; discovered lifecycles never use this."""

import json
from pathlib import Path


def configuration():
    root = Path(__file__).parents[2]
    raw = json.loads((root / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text())
    raw["forks"][0]["block"] = "latest"
    raw["allocations"] = [a for a in raw["allocations"] if a["account"] == "lending_0"]
    raw["funds"] = [
        {
            "account": "lending_0",
            "foundry_account_index": 0,
            "native_balance": {"asset": "eip155:1/slip44:60", "amount": 5 * 10**18},
            "token_balance": 4 * 10**16,
            "funding": "wrap_native",
        }
    ]
    raw["operators"] = []
    raw["debts"] = []
    return raw
