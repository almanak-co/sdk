"""The account overview for `almanak ax portfolio`.

Every figure is the vendor's: balances, protocol positions, prices and health
rates are what Rabby's backend reports, read at no block and verified by nothing
here. The overview is for display. Before an action, the market is named and read
on chain by the observation commands, and no row of this report stands in for
that read.

A value the vendor omits stays absent. A total is stated only when every row it
sums has a value, so a missing price never counts as zero.
"""

from decimal import Decimal, InvalidOperation

from almanak.caip import AssetType
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.gateway.rabby_portfolio import CHAINS, SOURCE
from almanak.models import address

# One command reads at most this many accounts: each costs two vendor requests, and
# the vendor throttles an address of ours after a few dozen.
MAX_ACCOUNTS = 5
CENT = Decimal("0.01")
TOKEN_LIST = "_token_list"


def portfolio(source, accounts: list[str], include_unlisted: bool) -> dict:
    if len(accounts) > MAX_ACCOUNTS:
        raise ValueError(f"ax portfolio reads at most {MAX_ACCOUNTS} accounts in one command")
    return {
        "product": "portfolio",
        "source": {
            "provider": SOURCE,
            "verified": False,
            "note": (
                "Every figure is the vendor's, read at no block. Read a market on chain"
                " before acting on it."
            ),
        },
        "accounts": [
            account_report(source, address(account), include_unlisted)
            for account in dict.fromkeys(accounts)
        ],
    }


def account_report(source, account: str, include_unlisted: bool) -> dict:
    token_rows, tokens_at = source.fetch("tokens", account)
    position_rows, positions_at = source.fetch("positions", account)
    held = [token_report(row) for row in token_rows]
    listed = [token for token in held if include_unlisted or token["vendor_flags"]["core"]]
    positions = [
        position_report(protocol, item) for protocol in position_rows for item in items(protocol)
    ]
    tokens_usd = total([token["value_usd"] for token in listed])
    positions_usd = total([position["net_usd"] for position in positions])
    return {
        "account": account,
        "received_at": {"tokens": tokens_at, "positions": positions_at},
        "totals": {
            "tokens_usd": tokens_usd,
            "positions_usd": positions_usd,
            "total_usd": total([tokens_usd, positions_usd]),
        },
        "tokens": sorted(listed, key=lambda token: rank(token["value_usd"]), reverse=True),
        # Tokens the vendor does not list as core, left out of the rows and the totals.
        "tokens_unlisted": len(held) - len(listed),
        "positions": sorted(
            positions, key=lambda position: rank(position["net_usd"]), reverse=True
        ),
    }


def token_report(row: dict) -> dict:
    """One wallet balance or one leg of a position, as the vendor states it."""
    amount = token_amount(row)
    price = decimal(row.get("price"))
    return {
        **identity(row),
        "symbol": text(row.get("optimized_symbol")) or text(row.get("symbol")),
        "name": text(row.get("name")),
        "decimals": row.get("decimals") if type(row.get("decimals")) is int else None,
        "amount": plain(amount),
        # Base units, where the vendor states them; a position leg carries none.
        "raw_amount": text(row.get("raw_amount_str")),
        "price_usd": plain(price),
        "value_usd": None if price is None else cents(amount * price),
        "vendor_flags": {
            "core": row.get("is_core") is True,
            "verified": row.get("is_verified") is True,
            "scam": row.get("is_scam") is True,
            "suspicious": row.get("is_suspicious") is True,
        },
    }


def identity(row: dict) -> dict:
    """The vendor's chain label and token ID, and the CAIP references they imply.

    The references are derived from the vendor's answer, not verified on chain. A
    chain without a verified network definition has neither.
    """
    label, token = text(row.get("chain")), text(row.get("id"))
    if label is None or token is None:
        raise DataError("malformed", f"{SOURCE}: a token row names no chain or no token")
    chain = CHAINS.get(label)
    return {
        "chain": caip(chain),
        "asset": None if chain is None else caip(asset(chain, label, token)),
        "vendor_chain": label,
        "vendor_token": token,
    }


def asset(chain, label: str, token: str) -> AssetType | None:
    # The vendor names a chain's native coin by the chain's own label.
    if token == label:
        return native_asset(chain)
    try:
        return AssetType(f"{chain}/erc20:{address(token)}")
    except ValueError:
        return None


def token_amount(row: dict) -> Decimal:
    raw, decimals = row.get("raw_amount_str"), row.get("decimals")
    if isinstance(raw, str) and raw.isdecimal() and type(decimals) is int and 0 <= decimals <= 77:
        return Decimal(raw).scaleb(-decimals)
    amount = decimal(row.get("amount"))
    if amount is None:
        raise DataError("malformed", f"{SOURCE}: a token row states no amount")
    return amount


def items(protocol: dict) -> list[dict]:
    listed = protocol.get("portfolio_item_list")
    if not isinstance(listed, list) or not all(isinstance(item, dict) for item in listed):
        raise DataError("malformed", f"{SOURCE}: a protocol row lists no positions")
    return listed


def position_report(protocol: dict, item: dict) -> dict:
    label = text(protocol.get("chain"))
    if label is None or text(protocol.get("id")) is None:
        raise DataError("malformed", f"{SOURCE}: a protocol row names no chain or no protocol")
    detail, stats, pool = (mapping(item.get(name)) for name in ("detail", "stats", "pool"))
    return {
        "chain": caip(CHAINS.get(label)),
        "vendor_chain": label,
        "protocol": {
            "vendor_id": text(protocol.get("id")),
            "name": text(protocol.get("name")),
            "site_url": text(protocol.get("site_url")),
        },
        # The vendor's own label for the position, such as Lending or Rewards.
        "kind": text(item.get("name")),
        "vendor_pool": text(pool.get("id")),
        "asset_usd": cents(decimal(stats.get("asset_usd_value"))),
        "debt_usd": cents(decimal(stats.get("debt_usd_value"))),
        "net_usd": cents(decimal(stats.get("net_usd_value"))),
        # The vendor's figure. A health floor is checked against the venue's own
        # read (`ax lending-position`), never against this.
        "health_rate": plain(decimal(detail.get("health_rate"))),
        "legs": legs(detail),
        "updated_at": seconds(item.get("update_at")),
    }


def legs(detail: dict) -> dict:
    """Each token list of the vendor's detail under its own name: supply, borrow, reward."""
    found = {}
    for key, value in detail.items():
        if not key.endswith(TOKEN_LIST) or not isinstance(value, list):
            continue
        if not all(isinstance(row, dict) for row in value):
            raise DataError("malformed", f"{SOURCE}: a position leg is not a token row")
        found[key.removesuffix(TOKEN_LIST)] = [token_report(row) for row in value]
    return found


def mapping(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def total(values: list[str | None]) -> str | None:
    if any(value is None for value in values):
        return None
    return cents(sum((Decimal(value) for value in values if value is not None), Decimal(0)))


def rank(value: str | None) -> Decimal:
    return Decimal("-Infinity") if value is None else Decimal(value)


def decimal(value: object) -> Decimal | None:
    """A vendor number as a decimal; absent or not a finite number stays absent."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal | str):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def cents(value: Decimal | None) -> str | None:
    return None if value is None else str(value.quantize(CENT))


def plain(value: Decimal | None) -> str | None:
    return None if value is None else format(value.normalize() if value else Decimal(0), "f")


def seconds(value: object) -> int | None:
    number = decimal(value)
    return None if number is None else int(number)


def caip(reference: object) -> str | None:
    return None if reference is None else str(reference)


def text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
