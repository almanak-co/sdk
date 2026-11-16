# /// script
# requires-python = ">=3.12"
# dependencies = ["hyperliquid-python-sdk==0.24.0"]
#
# [tool.uv]
# exclude-newer = "2026-10-01T00:00:00Z"
# ///
"""Generate the Hyperliquid L1 signing fixtures with the official SDK (blueprint 30, 9.8).

The official `hyperliquid-python-sdk`, pinned to 0.24.0, hashes and signs a fixed set of
actions with a public Foundry test key. The SDK's gateway signer and the fork model's
verifier must each reproduce every byte. The official SDK
runs only here, in its own isolated environment, never in the runtime:

    uv run --script scripts/generate_hyperliquid_signing_fixtures.py \
        tests/fixtures/hyperliquid-signing.json
"""

import json
import sys
from importlib.metadata import version

from eth_account import Account
from eth_account.messages import _hash_eip191_message, encode_typed_data
from hyperliquid.utils.signing import (
    action_hash,
    construct_phantom_agent,
    l1_payload,
    order_request_to_order_wire,
    order_wires_to_order_action,
    sign_l1_action,
)
from hyperliquid.utils.types import Cloid

# Foundry's first public test key.
KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
NONCE = 1793836800123
EXPIRES = NONCE + 30_000


def order(asset, is_buy, px, sz, reduce_only, tif, cloid=None):
    request = {
        "coin": "",
        "is_buy": is_buy,
        "sz": sz,
        "limit_px": px,
        "order_type": {"limit": {"tif": tif}},
        "reduce_only": reduce_only,
    }
    if cloid is not None:
        request["cloid"] = Cloid.from_str(cloid)
    return order_request_to_order_wire(request, asset)


def trigger(asset, is_buy, px, sz, trigger_px, tpsl="sl", is_market=True, cloid=None):
    request = {
        "coin": "",
        "is_buy": is_buy,
        "sz": sz,
        "limit_px": px,
        "order_type": {"trigger": {"triggerPx": trigger_px, "isMarket": is_market, "tpsl": tpsl}},
        "reduce_only": True,
    }
    if cloid is not None:
        request["cloid"] = Cloid.from_str(cloid)
    return order_request_to_order_wire(request, asset)


def cases():
    cloid = "0x" + "5a" * 16
    return [
        # The phase 3 actions: an Ioc entry, a reduce-only Ioc exit, leverage and noop.
        (
            "ioc_entry_with_cloid",
            order_wires_to_order_action([order(0, True, 108250.0, 0.0012, False, "Ioc", cloid)]),
            EXPIRES,
        ),
        (
            "ioc_reduce_only_exit",
            order_wires_to_order_action(
                [order(1, False, 3790.5, 0.25, True, "Ioc", "0x0000000000000000000000000000002a")]
            ),
            EXPIRES,
        ),
        (
            "update_leverage_cross",
            {"type": "updateLeverage", "asset": 0, "isCross": True, "leverage": 5},
            EXPIRES,
        ),
        ("noop", {"type": "noop"}, EXPIRES),
        # Without expiresAfter, which every runtime action carries.
        ("noop_without_expiry", {"type": "noop"}, None),
        (
            "update_leverage_isolated_without_expiry",
            {"type": "updateLeverage", "asset": 159, "isCross": False, "leverage": 3},
            None,
        ),
        # Number strings: trailing zeros normalize away, integers carry no point.
        (
            "trailing_zeros_without_cloid",
            order_wires_to_order_action([order(5, True, 1.10, 10.0, False, "Gtc")]),
            None,
        ),
        (
            "small_price_and_size",
            order_wires_to_order_action([order(7, False, 0.000123, 1234.5, False, "Alo")]),
            EXPIRES,
        ),
        # Several orders in one action, and a trigger order (phase 4).
        (
            "two_orders",
            order_wires_to_order_action(
                [
                    order(0, True, 100000.0, 0.001, False, "Ioc", "0x" + "01" * 16),
                    order(0, False, 120000.0, 0.001, True, "Ioc", "0x" + "02" * 16),
                ]
            ),
            EXPIRES,
        ),
        (
            "stop_loss_trigger",
            order_wires_to_order_action([trigger(0, False, 90000.0, 0.001, 95000.0)]),
            EXPIRES,
        ),
        # Cancels, isolated margin and the dead man's switch (phase 4).
        ("cancel_by_oid", {"type": "cancel", "cancels": [{"a": 0, "o": 123456789012}]}, EXPIRES),
        (
            "cancel_by_cloid",
            {"type": "cancelByCloid", "cancels": [{"asset": 0, "cloid": cloid}]},
            EXPIRES,
        ),
        (
            "update_isolated_margin_add",
            {"type": "updateIsolatedMargin", "asset": 0, "isBuy": True, "ntli": 12_500_000},
            EXPIRES,
        ),
        (
            "update_isolated_margin_remove",
            {"type": "updateIsolatedMargin", "asset": 0, "isBuy": True, "ntli": -1_000_000},
            None,
        ),
        ("schedule_cancel_set", {"type": "scheduleCancel", "time": NONCE + 60_000}, EXPIRES),
        ("schedule_cancel_clear", {"type": "scheduleCancel"}, EXPIRES),
        # The phase 4 orders the gateway signs: resting Gtc and post-only Alo limits and
        # reduce-only limit TP/SL triggers, each with its cloid; isolated leverage.
        (
            "gtc_resting_entry_with_cloid",
            order_wires_to_order_action(
                [order(0, True, 104500.0, 0.002, False, "Gtc", "0x" + "03" * 16)]
            ),
            EXPIRES,
        ),
        (
            "alo_reduce_only_with_cloid",
            order_wires_to_order_action(
                [order(0, False, 112000.0, 0.002, True, "Alo", "0x" + "04" * 16)]
            ),
            EXPIRES,
        ),
        (
            "take_profit_limit_trigger_with_cloid",
            order_wires_to_order_action(
                [trigger(0, False, 115000.0, 0.002, 116000.0, "tp", False, "0x" + "05" * 16)]
            ),
            EXPIRES,
        ),
        (
            "stop_loss_limit_trigger_with_cloid",
            order_wires_to_order_action(
                [trigger(0, False, 94525.0, 0.002, 95000.0, "sl", False, "0x" + "06" * 16)]
            ),
            EXPIRES,
        ),
        (
            "update_leverage_isolated",
            {"type": "updateLeverage", "asset": 0, "isCross": False, "leverage": 10},
            EXPIRES,
        ),
    ]


def fixture(wallet, name, action, expires):
    import msgpack

    digest = action_hash(action, None, NONCE, expires)
    message = encode_typed_data(full_message=l1_payload(construct_phantom_agent(digest, True)))
    signature = sign_l1_action(wallet, action, None, NONCE, expires, True)
    return {
        "name": name,
        "action": action,
        "nonce": NONCE,
        "expires_after": expires,
        "msgpack": msgpack.packb(action).hex(),
        "action_hash": "0x" + digest.hex(),
        "digest": "0x" + _hash_eip191_message(message).hex(),
        "signature": signature,
    }


def main(path: str) -> None:
    wallet = Account.from_key(KEY)
    document = {
        "generator": f"hyperliquid-python-sdk=={version('hyperliquid-python-sdk')}",
        "network": "mainnet",
        "key": KEY,
        # Checksummed, as the venue's SDK writes it; verification compares addresses
        # without regard to case.
        "signer": wallet.address,
        "fixtures": [fixture(wallet, *case) for case in cases()],
    }
    with open(path, "w") as handle:
        json.dump(document, handle, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main(sys.argv[1])
