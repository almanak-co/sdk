"""`ax portfolio` from vendor answers shaped like Rabby's backend's."""

import json
from decimal import Decimal

import httpx
import pytest

from almanak import ax, ax_portfolio
from almanak.cli_arguments import build_parser, parse_validated
from almanak.gateway.ethereum import DataError
from almanak.gateway.rabby_portfolio import PATHS, RabbyPortfolio

ACCOUNT = "0x" + "11" * 20
OTHER = "0x" + "22" * 20
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
POOL = "0x" + "33" * 20


def token(**fields) -> dict:
    return {
        "id": "eth",
        "chain": "eth",
        "name": "ETH",
        "symbol": "ETH",
        "optimized_symbol": "ETH",
        "decimals": 18,
        "price": Decimal("2000.5"),
        "is_core": True,
        "is_verified": True,
        "is_scam": False,
        "is_suspicious": False,
        "amount": Decimal("0.5"),
        "raw_amount_str": "500000000000000001",
    } | fields


def leg(**fields) -> dict:
    # A position leg carries the vendor's decimal amount and no base units.
    row = token(id=USDC, name="USD Coin", symbol="USDC", optimized_symbol="USDC", decimals=6)
    del row["raw_amount_str"]
    return row | {"price": Decimal("1.0009"), "amount": Decimal("12.019914")} | fields


def lending(**detail) -> dict:
    return {
        "id": "aave4",
        "chain": "eth",
        "name": "Aave V4",
        "site_url": "https://pro.aave.com/",
        "portfolio_item_list": [
            {
                "name": "Lending",
                "stats": {
                    "asset_usd_value": Decimal("12.0307"),
                    "debt_usd_value": 0,
                    "net_usd_value": Decimal("12.0307"),
                },
                "detail": {"supply_token_list": [leg()], "health_rate": 10000} | detail,
                "pool": {"id": POOL},
                "update_at": Decimal("1795151846.0"),
            }
        ],
    }


class Answers:
    """A vendor that answers each listing from fixed rows and records what it was asked."""

    def __init__(self, tokens=(), positions=()):
        self.rows = {"tokens": list(tokens), "positions": list(positions)}
        self.asked = []

    def fetch(self, listing, account):
        self.asked.append((listing, account))
        return self.rows[listing], 1795000000000 + len(self.asked)


def parse(argv):
    return parse_validated(build_parser(), argv)


def one(source, include_unlisted=False) -> dict:
    (account,) = ax_portfolio.portfolio(source, [ACCOUNT], include_unlisted)["accounts"]
    return account


def test_the_report_names_the_vendor_and_claims_no_verification():
    source = Answers([token()], [lending()])
    report = ax_portfolio.portfolio(source, [ACCOUNT], False)
    assert report["product"] == "portfolio"
    assert report["source"]["provider"] == "rabby" and report["source"]["verified"] is False
    assert source.asked == [("tokens", ACCOUNT), ("positions", ACCOUNT)]
    account = report["accounts"][0]
    assert account["account"] == ACCOUNT
    assert account["received_at"] == {"tokens": 1795000000001, "positions": 1795000000002}
    assert json.loads(json.dumps(report)) == report


def test_a_wallet_token_keeps_its_base_units_and_its_caip_references():
    (eth,) = one(Answers([token()]))["tokens"]
    assert (eth["chain"], eth["asset"]) == ("eip155:1", "eip155:1/slip44:60")
    assert (eth["vendor_chain"], eth["vendor_token"]) == ("eth", "eth")
    # The amount is the base units scaled, not the vendor's rounded decimal.
    assert eth["raw_amount"] == "500000000000000001"
    assert eth["amount"] == "0.500000000000000001"
    assert (eth["price_usd"], eth["value_usd"]) == ("2000.5", "1000.25")


def test_an_erc20_is_referenced_by_its_contract_and_an_unknown_chain_by_nothing():
    rows = [
        token(id=USDC.upper().replace("0X", "0x"), symbol="USDC", optimized_symbol=None),
        token(id="0x" + "44" * 20, chain="citrea"),
        token(id="not-an-address", chain="base"),
    ]
    by_chain = {row["vendor_chain"]: row for row in one(Answers(rows))["tokens"]}
    assert by_chain["eth"]["asset"] == f"eip155:1/erc20:{USDC}"
    assert by_chain["eth"]["symbol"] == "USDC"
    assert (by_chain["citrea"]["chain"], by_chain["citrea"]["asset"]) == (None, None)
    assert (by_chain["base"]["chain"], by_chain["base"]["asset"]) == ("eip155:8453", None)


def test_unlisted_tokens_are_counted_and_left_out_of_the_total():
    rows = [token(), token(id="0x" + "55" * 20, is_core=False, is_scam=True)]
    listed = one(Answers(rows))
    assert len(listed["tokens"]) == 1 and listed["tokens_unlisted"] == 1
    assert listed["totals"]["tokens_usd"] == "1000.25"
    every = one(Answers(rows), include_unlisted=True)
    assert every["tokens_unlisted"] == 0 and every["totals"]["tokens_usd"] == "2000.50"
    assert [row["vendor_flags"]["scam"] for row in every["tokens"]] == [False, True]


def test_a_missing_price_is_absent_and_leaves_the_totals_absent():
    account = one(Answers([token(), token(id=USDC, price=None)], [lending()]))
    priced, unpriced = account["tokens"]
    assert priced["value_usd"] == "1000.25"
    assert (unpriced["price_usd"], unpriced["value_usd"]) == (None, None)
    assert account["totals"] == {
        "tokens_usd": None,
        "positions_usd": "12.03",
        "total_usd": None,
    }


def test_an_empty_account_totals_zero():
    account = one(Answers())
    assert account["tokens"] == [] and account["positions"] == []
    assert account["totals"] == {
        "tokens_usd": "0.00",
        "positions_usd": "0.00",
        "total_usd": "0.00",
    }


def test_a_position_reports_the_vendors_figures_and_each_leg_by_name():
    borrowed = leg(amount=Decimal("3"))
    protocol = lending(borrow_token_list=[borrowed], reward_token_list=[], health_rate="1.82")
    (position,) = one(Answers([], [protocol]))["positions"]
    assert position["chain"] == "eip155:1" and position["kind"] == "Lending"
    assert position["protocol"] == {
        "vendor_id": "aave4",
        "name": "Aave V4",
        "site_url": "https://pro.aave.com/",
    }
    assert position["vendor_pool"] == POOL and position["updated_at"] == 1795151846
    assert (position["asset_usd"], position["debt_usd"], position["net_usd"]) == (
        "12.03",
        "0.00",
        "12.03",
    )
    assert position["health_rate"] == "1.82"
    assert sorted(position["legs"]) == ["borrow", "reward", "supply"]
    (supplied,) = position["legs"]["supply"]
    assert (supplied["amount"], supplied["raw_amount"]) == ("12.019914", None)
    assert supplied["asset"] == f"eip155:1/erc20:{USDC}"
    assert position["legs"]["borrow"][0]["amount"] == "3"


def test_positions_and_tokens_are_ordered_by_value_with_unvalued_rows_last():
    small = lending()
    small["portfolio_item_list"][0]["stats"]["net_usd_value"] = Decimal("1")
    unvalued = lending()
    del unvalued["portfolio_item_list"][0]["stats"]
    account = one(
        Answers(
            [token(price=None), token(amount=None, raw_amount_str="1"), token()],
            [unvalued, small, lending()],
        )
    )
    assert [row["value_usd"] for row in account["tokens"]] == ["1000.25", "0.00", None]
    assert [row["net_usd"] for row in account["positions"]] == ["12.03", "1.00", None]
    assert account["totals"]["positions_usd"] is None


def test_a_large_balance_keeps_every_digit_and_an_absurd_number_is_absent():
    raw = "1234567890123456789012345678901234567"
    big = token(id=USDC, raw_amount_str=raw, price=Decimal("0.5"))
    (row,) = one(Answers([big]))["tokens"]
    assert row["amount"] == "1234567890123456789.012345678901234567"
    assert row["value_usd"] == "617283945061728394.51"
    widest = token(raw_amount_str="9" * 78, decimals=0, price=Decimal("1e60"))
    assert one(Answers([widest]))["tokens"][0]["value_usd"] == "9" * 78 + "0" * 60 + ".00"
    absurd = lending()
    absurd["portfolio_item_list"][0]["stats"]["net_usd_value"] = Decimal("1e400")
    absurd["portfolio_item_list"][0]["update_at"] = Decimal("1e999999")
    account = one(Answers([token(price=Decimal("1e400"))], [absurd]))
    assert account["tokens"][0]["price_usd"] is None
    assert account["positions"][0]["net_usd"] is None
    assert account["positions"][0]["updated_at"] is None
    assert (
        one(Answers([token(amount=Decimal("1e-7"), raw_amount_str=None)]))["tokens"][0]["amount"]
        == "0.0000001"
    )


@pytest.mark.parametrize("price", [0, Decimal("0.0"), -5, None, "free"])
def test_a_price_that_is_zero_or_negative_is_no_price(price):
    account = one(Answers([token(price=price)]))
    assert account["tokens"][0]["price_usd"] is None
    assert account["tokens"][0]["value_usd"] is None
    assert account["totals"]["tokens_usd"] is None


@pytest.mark.parametrize(
    ("tokens", "positions"),
    [
        ([token(amount=Decimal("-5"), raw_amount_str=None)], []),
        ([token(amount=Decimal("1e999999"), raw_amount_str=None)], []),
        ([token(amount=None, raw_amount_str="9" * 79)], []),
        ([token(amount=None, raw_amount_str="-5")], []),
        ([token(chain=None)], []),
        ([token(id="")], []),
        ([token(amount=None, raw_amount_str=None)], []),
        ([token(amount=float("nan"), raw_amount_str=None)], []),
        ([], [{"id": "aave4", "chain": "eth"}]),
        ([], [lending() | {"chain": None}]),
        ([], [lending(supply_token_list=["USDC"])]),
    ],
)
def test_a_row_that_states_no_identity_or_amount_refuses_the_report(tokens, positions):
    with pytest.raises(DataError) as refused:
        one(Answers(tokens, positions))
    assert refused.value.kind == "malformed"


def test_accounts_are_validated_deduplicated_and_bounded():
    source = Answers()
    shouted = "0x" + ACCOUNT[2:].upper()
    report = ax_portfolio.portfolio(source, [ACCOUNT, OTHER, shouted] + [ACCOUNT] * 4, False)
    assert [row["account"] for row in report["accounts"]] == [ACCOUNT, OTHER]
    assert len(source.asked) == 4
    with pytest.raises(ValueError, match="20-byte EVM address"):
        ax_portfolio.portfolio(source, ["0x1234"], False)
    many = [f"0x{n:040x}" for n in range(1, 7)]
    with pytest.raises(ValueError, match="at most 5 accounts"):
        ax_portfolio.portfolio(source, many, False)


def vendor(handler) -> RabbyPortfolio:
    return RabbyPortfolio(transport=httpx.MockTransport(handler), clock=lambda: 7)


def test_the_client_asks_one_fixed_path_per_listing_and_keeps_decimals():
    seen = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.url.path, dict(request.url.params)))
        return httpx.Response(200, text='[{"amount": 0.1, "price": 1750.77}]')

    with vendor(answer) as source:
        rows, received = source.fetch("tokens", ACCOUNT.upper().replace("0X", "0x"))
        source.fetch("positions", ACCOUNT)
    assert rows == [{"amount": Decimal("0.1"), "price": Decimal("1750.77")}] and received == 7
    assert seen == [
        ("api.rabby.io", PATHS["tokens"], {"id": ACCOUNT}),
        ("api.rabby.io", PATHS["positions"], {"id": ACCOUNT}),
    ]


@pytest.mark.parametrize(
    ("response", "kind"),
    [
        (httpx.Response(429, json={"message": "too many requests"}), "rate_limit"),
        (httpx.Response(404, text="<title>404 Not Found</title>"), "unavailable"),
        (httpx.Response(200, text="<html>"), "malformed"),
        (httpx.Response(200, json={"message": "moved"}), "malformed"),
        (httpx.Response(200, json=["eth"]), "malformed"),
    ],
)
def test_the_client_names_each_refusal_and_substitutes_nothing(response, kind):
    with vendor(lambda request: response) as source, pytest.raises(DataError) as refused:
        source.fetch("tokens", ACCOUNT)
    assert refused.value.kind == kind and "rabby: tokens" in str(refused.value)


def test_a_failed_request_is_unavailable_and_a_bad_account_is_never_sent():
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route")

    with vendor(unreachable) as source:
        with pytest.raises(DataError) as refused:
            source.fetch("positions", ACCOUNT)
        assert refused.value.kind == "unavailable"
        with pytest.raises(ValueError, match="20-byte EVM address"):
            source.fetch("positions", "vitalik.eth")


def test_the_command_takes_accounts_and_no_transport_or_block(monkeypatch):
    monkeypatch.delenv("ALMANAK_AX_GATEWAY_URL", raising=False)
    monkeypatch.delenv("ALMANAK_AX_GATEWAY_AUTH_FILE", raising=False)
    args = parse(["ax", "portfolio", "--account", ACCOUNT, "--account", OTHER])
    assert (args.product, args.account, args.include_unlisted) == (
        "ax-portfolio",
        [ACCOUNT, OTHER],
        False,
    )
    assert parse(["ax", "portfolio", "--account", ACCOUNT, "--include-unlisted"]).include_unlisted
    for argv in (
        ["ax", "portfolio"],
        ["ax", "portfolio", "--account", ACCOUNT, "--block", "latest"],
        ["ax", "portfolio", "--account", ACCOUNT, "--public-rpc", "publicnode"],
        ["ax", "portfolio", "--account", ACCOUNT, "--env-file", "/tmp/env"],
    ):
        with pytest.raises(SystemExit):
            parse(argv)
    # Every other read still needs its transport.
    with pytest.raises(SystemExit):
        parse(["ax", "hypercore-account", "--account", ACCOUNT])


def test_the_handler_reads_through_the_vendor_source(monkeypatch, capsys):
    source = ax.portfolio_source()
    assert isinstance(source, RabbyPortfolio)
    source.close()

    class Source(Answers):
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

    monkeypatch.setattr(ax, "portfolio_source", lambda: Source([token()], [lending()]))
    assert ax.command(parse(["ax", "portfolio", "--account", ACCOUNT])) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["accounts"][0]["totals"]["total_usd"] == "1012.28"
