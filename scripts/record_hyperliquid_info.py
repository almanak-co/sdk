"""Record HyperCore `/info` answers for public accounts as an offline fixture.

The closed response shapes in `almanak/gateway/hypercore_shapes.py` are fixed by
these recordings, and the HyperCore tests replay them. Every recorded request is
built from the whitelist's own query models and sent as `body()` sends it; the
answers are stored as received, without the shape check they exist to fix.

The subjects are public mainnet accounts: the HLP vault, one of its child vaults
(which trades), the vault leader, one of the leader's agents, Circle's
CoreDepositWallet and an address that has never touched HyperCore. Two order IDs
come from the child vault's own recorded answers: one from its fills and one from
its open orders. One
backstop liquidation on 1 November 2026 is recorded from both sides: the HLP
child vault that took the positions over and the account it liquidated. One
public account in unified account mode shows the spot state that mode answers,
and one whose orders and agents carry client order IDs and expiries shows those.
One Safe that walked the custody path on mainnet records that path itself: its
activation, class transfers, sends and withdrawal in the ledger, its agents, and
fills that carry the cloids its agent set. Its committed recordings are anonymized:
addresses, hashes, order and trade IDs and nonces are synthetic.

Reads only. Nothing here signs or sends an action.
"""

import argparse
import gzip
import json
import time
from pathlib import Path

import httpx

from almanak.gateway.hypercore_info import (
    API,
    ActiveAssetData,
    ClearinghouseState,
    ExtraAgents,
    InfoRequest,
    L2Book,
    Meta,
    MetaAndAssetCtxs,
    OpenOrders,
    OrderStatus,
    SpotClearinghouseState,
    SpotMeta,
    UserAbstraction,
    UserFillsByTime,
    UserFunding,
    UserNonFundingLedgerUpdates,
    UserRateLimit,
    UserRole,
    body,
    received_now,
)

SUBJECTS = {
    "hlp_parent": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
    "hlp_child": "0x010461c14e146ac35fe42271bdc1134ee31c703a",
    "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
    "agent": "0xeeb5fb0d32a403733ebbf6e1c694f9253b37cf4c",
    "core_deposit_wallet": "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24",
    "fresh": "0x5c5368732f2a5b11506ef99305b68dbcf7c989dd",
    "hlp_liquidator": "0xb0a55f13d22f66e6d495ac98113841b2326e9540",
    "liquidated": "0x97ef6bf5944e451d371196ed564bedc8d970f170",
    "unified": "0xdf9ea6ec3b7109935ccb4fb267e15ac1fb077ab1",
    "cloid_trader": "0xf5d81a135f756ca16544e53c20fc20643ec3ad53",
    # A placeholder: the committed recordings for this subject are anonymized. Its
    # addresses, transaction hashes, order and trade IDs and nonces are synthetic, and
    # everything else is as received. Recording it again needs a real account that has
    # walked the custody path, and the same replacement before the result is committed.
    "canary_safe": "0xca00000000000000000000000000000000000001",
}
# That account's whole history: activation to withdrawal, in milliseconds.
CANARY = (1793777800000, 1793779500000)
# The block time of that liquidation, in milliseconds.
LIQUIDATION = 1793507570816
MINUTE = 60_000
HOUR = 60 * MINUTE
DAY = 24 * HOUR
# Stays well inside the API's 1200 weight per minute for this script's ~650 weight.
PAUSE = 1.0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    with httpx.Client(timeout=30) as client:
        recordings = record(client)
    fixture = {"api": API, "recordings": recordings}
    args.output.write_bytes(gzip.compress(json.dumps(fixture, sort_keys=True).encode(), mtime=0))
    print(json.dumps({"output": str(args.output), "recordings": len(recordings)}))
    return 0


def record(client: httpx.Client) -> list[dict]:
    now = received_now()
    boundary = now // HOUR * HOUR
    s = SUBJECTS
    queries = [
        ("meta", Meta()),
        ("metaAndAssetCtxs", MetaAndAssetCtxs()),
        ("spotMeta", SpotMeta()),
        ("l2Book.BTC", L2Book(coin="BTC")),
        ("l2Book.delisted", L2Book(coin="MATIC")),
        *(
            (f"clearinghouseState.{name}", ClearinghouseState(user=s[name]))
            for name in ("hlp_child", "hlp_parent", "leader", "fresh", "unified", "cloid_trader")
        ),
        *(
            (f"spotClearinghouseState.{name}", SpotClearinghouseState(user=s[name]))
            for name in ("leader", "core_deposit_wallet", "fresh", "unified", "cloid_trader")
        ),
        *(
            (f"activeAssetData.{name}", ActiveAssetData(user=s[name], coin=coin))
            for name, coin in (("leader", "BTC"), ("fresh", "BTC"), ("hlp_child", "BTC"))
        ),
        *(
            (f"openOrders.{name}", OpenOrders(user=s[name]))
            for name in ("hlp_child", "leader", "fresh", "unified", "cloid_trader")
        ),
        *(
            (f"extraAgents.{name}", ExtraAgents(user=s[name]))
            for name in ("leader", "fresh", "unified", "cloid_trader")
        ),
        *(
            (f"userRole.{name}", UserRole(user=s[name]))
            for name in (
                "hlp_parent",
                "leader",
                "agent",
                "core_deposit_wallet",
                "fresh",
                "unified",
                "cloid_trader",
            )
        ),
        *(
            (f"userAbstraction.{name}", UserAbstraction(user=s[name]))
            for name in ("hlp_parent", "leader", "fresh", "unified", "cloid_trader")
        ),
        *((f"userRateLimit.{name}", UserRateLimit(user=s[name])) for name in ("leader", "fresh")),
        (
            "userFillsByTime.hlp_child",
            UserFillsByTime(user=s["hlp_child"], start_time=now - 30_000, end_time=now),
        ),
        (
            "userFillsByTime.fresh",
            UserFillsByTime(user=s["fresh"], start_time=now - DAY, end_time=now),
        ),
        (
            "userFillsByTime.hlp_liquidator",
            UserFillsByTime(
                user=s["hlp_liquidator"],
                start_time=LIQUIDATION - MINUTE,
                end_time=LIQUIDATION + MINUTE,
            ),
        ),
        (
            "userFillsByTime.liquidated",
            UserFillsByTime(
                user=s["liquidated"], start_time=LIQUIDATION - MINUTE, end_time=LIQUIDATION + MINUTE
            ),
        ),
        (
            "userFillsByTime.canary_safe",
            UserFillsByTime(user=s["canary_safe"], start_time=CANARY[0], end_time=CANARY[1]),
        ),
        (
            "userNonFundingLedgerUpdates.canary_safe",
            UserNonFundingLedgerUpdates(
                user=s["canary_safe"], start_time=CANARY[0], end_time=CANARY[1]
            ),
        ),
        ("extraAgents.canary_safe", ExtraAgents(user=s["canary_safe"])),
        (
            "userFunding.hlp_child.hour",
            UserFunding(
                user=s["hlp_child"], start_time=boundary - MINUTE, end_time=boundary + MINUTE
            ),
        ),
        (
            "userFunding.hlp_child.page",
            UserFunding(user=s["hlp_child"], start_time=now - 2 * DAY, end_time=now),
        ),
        (
            "userNonFundingLedgerUpdates.hlp_parent",
            UserNonFundingLedgerUpdates(user=s["hlp_parent"], start_time=now - DAY, end_time=now),
        ),
        (
            "userNonFundingLedgerUpdates.hlp_parent.page",
            UserNonFundingLedgerUpdates(
                user=s["hlp_parent"], start_time=now - 30 * DAY, end_time=now
            ),
        ),
        (
            "userNonFundingLedgerUpdates.leader",
            UserNonFundingLedgerUpdates(user=s["leader"], start_time=now - 365 * DAY, end_time=now),
        ),
        (
            "userNonFundingLedgerUpdates.liquidated",
            UserNonFundingLedgerUpdates(
                user=s["liquidated"], start_time=LIQUIDATION - HOUR, end_time=LIQUIDATION + HOUR
            ),
        ),
        (
            "userNonFundingLedgerUpdates.fresh",
            UserNonFundingLedgerUpdates(user=s["fresh"], start_time=now - DAY, end_time=now),
        ),
    ]
    recordings = [captured(client, label, query) for label, query in queries]
    answers = {row["label"]: row["result"] for row in recordings}
    fills, opened = answers["userFillsByTime.hlp_child"], answers["openOrders.hlp_child"]
    orders = [
        ("orderStatus.filled", fills[0]["oid"] if fills else None),
        ("orderStatus.open", opened[0]["oid"] if opened else None),
        ("orderStatus.unknown_oid", 1),
        ("orderStatus.unknown_cloid", "0x" + "00" * 15 + "01"),
    ]
    for label, oid in orders:
        if oid is not None:
            recordings.append(captured(client, label, OrderStatus(user=s["hlp_child"], oid=oid)))
    return recordings


def captured(client: httpx.Client, label: str, query) -> dict:
    response = client.post(API, json=body(query), headers={"Content-Type": "application/json"})
    received = received_now()
    response.raise_for_status()
    time.sleep(PAUSE)
    request = InfoRequest(info=query).model_dump(mode="json")
    return {"label": label, "request": request, "received_at": received, "result": response.json()}


if __name__ == "__main__":
    raise SystemExit(main())
