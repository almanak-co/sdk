"""The account overview for `almanak ax portfolio`.

Every figure is the vendor's: balances, protocol positions, prices and health
rates are what Rabby's backend reports, read at no block and verified by nothing
here. The overview is for display. Before an action, the market is named and read
on chain by the observation commands, and no row of this report stands in for
that read.

Wallet tokens are read chain by chain, for the chains the vendor's summary does
not value at zero. A chain past the bound is named as unread, never dropped. A
chain the vendor values at zero is not read, so a token the vendor cannot price
there is not seen.

A value the vendor omits stays absent. A total is stated only when every row it
sums has a value and every chain to read was read, so a missing price on a chain
read never counts as zero.

The vendor's text is written by others: a token's symbol and name are whatever its
deployer chose, and anyone can send a token to an account. A report is read by
agents, so no string of the vendor's leaves it unbounded. A symbol, a name and a
label are short runs of letters, digits and a few marks, and an identifier is
ASCII; a value that is not is absent, never shortened or rewritten. The vendor's
link fields are not reported, and no symbol, name or label can hold a URL: none
takes a colon or two slashes in a row. A bare domain name is still a short name.
"""

import string
import unicodedata
from decimal import Context, Decimal, InvalidOperation

from almanak.caip import AssetType
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.gateway.rabby_portfolio import CHAINS, SOURCE
from almanak.models import address

# One command reads at most this many accounts, and for each the tokens of at most
# this many chains: an account costs two vendor requests and one more per chain, and
# the vendor throttles an address of ours after a few dozen.
MAX_ACCOUNTS = 2
MAX_CHAINS = 8
MAX_HEX_DIGITS = 64
CENT = Decimal("0.01")
# A vendor number further from one than this many decimal places is not a balance, a
# price or a time; it is read as absent. Arithmetic keeps every digit of what is left.
MAX_EXPONENT = 60
HEX = "0123456789abcdefABCDEF"
# The bounds of the vendor's text, in characters. A symbol has no space. The bounds
# are chosen, not measured from the vendor: a longer or stranger value is absent.
MAX_SYMBOL = 24
MAX_NAME = 48
MAX_IDENTIFIER = 128
SYMBOL_MARKS = "._-+/$*"
NAME_MARKS = SYMBOL_MARKS + " ()&',#"
IDENTIFIER = frozenset(string.ascii_letters + string.digits + "._-:")
EXACT = Context(prec=4 * MAX_EXPONENT)
TOKEN_LIST = "_token_list"


def portfolio(source, accounts: list[str], include_unlisted: bool) -> dict:
    unique = list(dict.fromkeys(address(account) for account in accounts))
    if len(unique) > MAX_ACCOUNTS:
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
        "accounts": [account_report(source, account, include_unlisted) for account in unique],
    }


def account_report(source, account: str, include_unlisted: bool) -> dict:
    chain_rows, chains_at = source.fetch("chains", account)
    valued = funded(chain_rows)
    token_rows, tokens_at = [], None
    for label, _ in valued[:MAX_CHAINS]:
        found, received = source.fetch("tokens", account, label)
        if any(row.get("chain") != label for row in found):
            raise DataError("malformed", f"{SOURCE}: tokens: a row is on another chain")
        token_rows += found
        # The oldest answer among the chains read.
        tokens_at = received if tokens_at is None else min(tokens_at, received)
    unread = [unread_chain(label, value) for label, value in valued[MAX_CHAINS:]]
    position_rows, positions_at = source.fetch("positions", account)
    held = [token_report(row) for row in token_rows]
    listed = [token for token in held if include_unlisted or token["vendor_flags"]["core"]]
    positions = [
        position_report(protocol, item) for protocol in position_rows for item in items(protocol)
    ]
    # Tokens on a chain that was not read are not measured, so there is no total.
    tokens_usd = None if unread else total([token["value_usd"] for token in listed])
    positions_usd = total([position["net_usd"] for position in positions])
    return {
        "account": account,
        "received_at": {"chains": chains_at, "tokens": tokens_at, "positions": positions_at},
        "totals": {
            "tokens_usd": tokens_usd,
            "positions_usd": positions_usd,
            "total_usd": total([tokens_usd, positions_usd]),
        },
        "tokens": sorted(listed, key=lambda token: rank(token["value_usd"]), reverse=True),
        # Tokens the vendor does not list as core, left out of the rows and the totals.
        # Counted on the chains read; a chain the vendor values at zero is not read.
        "tokens_unlisted": len(held) - len(listed),
        "chains_read": [label for label, _ in valued[:MAX_CHAINS]],
        "chains_unread": unread,
        "positions": sorted(
            positions, key=lambda position: rank(position["net_usd"]), reverse=True
        ),
    }


def funded(chain_rows: list[dict]) -> list[tuple[str, Decimal | None]]:
    """The chains to read, largest value first: every one the vendor's summary does
    not value at zero. A chain whose value cannot be read is not known to be empty,
    so it is kept, after the valued ones."""
    valued: dict[str, Decimal | None] = {}
    for row in chain_rows:
        label, value = identifier(row.get("id")), decimal(row.get("usd_value"))
        if label is None or label in valued:
            raise DataError("malformed", f"{SOURCE}: a chain row names no chain or repeats one")
        valued[label] = value
    kept = [(label, value) for label, value in valued.items() if value != 0]
    return sorted(kept, key=lambda chain: rank(chain[1]), reverse=True)


def unread_chain(label: str, value: Decimal | None) -> dict:
    # The vendor's value for the whole chain, tokens and positions together.
    return {"chain": caip(CHAINS.get(label)), "vendor_chain": label, "value_usd": cents(value)}


def token_report(row: dict) -> dict:
    """One wallet balance or one leg of a position, as the vendor states it."""
    amount = token_amount(row)
    price = positive(decimal(row.get("price")))
    return {
        **identity(row),
        "symbol": symbol(row.get("optimized_symbol")) or symbol(row.get("symbol")),
        "name": name(row.get("name")),
        "decimals": row.get("decimals") if type(row.get("decimals")) is int else None,
        "amount": plain(amount),
        # Base units, where the vendor states them; a position leg carries none.
        "raw_amount": base_units(row),
        "price_usd": plain(price),
        "value_usd": None if price is None else cents(EXACT.multiply(amount, price)),
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
    label, token = identifier(row.get("chain")), identifier(row.get("id"))
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
    """The balance from its base units where the vendor states them, exactly."""
    raw, decimals = base_units(row), row.get("decimals")
    amount = decimal(row.get("amount"))
    if raw is not None and type(decimals) is int and 0 <= decimals <= MAX_EXPONENT:
        amount = Decimal(raw).scaleb(-decimals, EXACT)
    if amount is None or amount < 0:
        raise DataError("malformed", f"{SOURCE}: a token row states no amount")
    return amount


def base_units(row: dict) -> str | None:
    """The balance in base units, from the vendor's hexadecimal statement of it."""
    stated = row.get("raw_amount_hex_str")
    if not isinstance(stated, str) or not stated.isascii() or not stated.startswith("0x"):
        return None
    digits = stated[2:]
    if not digits or len(digits) > MAX_HEX_DIGITS or set(digits) - set(HEX):
        return None
    return str(int(digits, 16))


def positive(price: Decimal | None) -> Decimal | None:
    # The vendor writes zero for a token it has no price for; that is not a price.
    return price if price is not None and price > 0 else None


def items(protocol: dict) -> list[dict]:
    listed = protocol.get("portfolio_item_list")
    if not isinstance(listed, list) or not all(isinstance(item, dict) for item in listed):
        raise DataError("malformed", f"{SOURCE}: a protocol row lists no positions")
    return listed


def position_report(protocol: dict, item: dict) -> dict:
    label, vendor_id = identifier(protocol.get("chain")), identifier(protocol.get("id"))
    if label is None or vendor_id is None:
        raise DataError("malformed", f"{SOURCE}: a protocol row names no chain or no protocol")
    detail, stats, pool = (mapping(item.get(name)) for name in ("detail", "stats", "pool"))
    return {
        "chain": caip(CHAINS.get(label)),
        "vendor_chain": label,
        "protocol": {
            "vendor_id": vendor_id,
            "name": name(protocol.get("name")),
        },
        # The vendor's own label for the position, such as Lending or Rewards.
        "kind": name(item.get("name")),
        "vendor_pool": identifier(pool.get("id")),
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
        leg = identifier(key.removesuffix(TOKEN_LIST))
        if leg is None:
            raise DataError("malformed", f"{SOURCE}: a position leg has no readable name")
        if not all(isinstance(row, dict) for row in value):
            raise DataError("malformed", f"{SOURCE}: a position leg is not a token row")
        found[leg] = [token_report(row) for row in value]
    return found


def mapping(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def total(values: list[str | None]) -> str | None:
    if any(value is None for value in values):
        return None
    summed = Decimal(0)
    for value in values:
        summed = EXACT.add(summed, Decimal(value or 0))
    return cents(summed)


def rank(value: Decimal | str | None) -> Decimal:
    return Decimal("-Infinity") if value is None else Decimal(value)


def decimal(value: object) -> Decimal | None:
    """A vendor number as a decimal; absent, not finite or out of bounds stays absent."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal | str):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    if not number.is_finite():
        return None
    return number if number == 0 or abs(number.adjusted()) <= MAX_EXPONENT else None


def cents(value: Decimal | None) -> str | None:
    return None if value is None else str(value.quantize(CENT, context=EXACT))


def plain(value: Decimal | None) -> str | None:
    """Every digit of the value in positional notation, without trailing zeros."""
    if value is None:
        return None
    digits = format(value, "f")
    return (digits.rstrip("0").rstrip(".") if "." in digits else digits) or "0"


def seconds(value: object) -> int | None:
    number = decimal(value)
    return None if number is None else int(number)


def caip(reference: object) -> str | None:
    return None if reference is None else str(reference)


def symbol(value: object) -> str | None:
    return display(value, MAX_SYMBOL, SYMBOL_MARKS)


def name(value: object) -> str | None:
    return display(value, MAX_NAME, NAME_MARKS)


def display(value: object, limit: int, marks: str) -> str | None:
    """The vendor's symbol, name or label, or absent when it is not a short one.

    Letters, digits and currency signs of any script are kept, with the marks named.
    Control and format characters, line breaks and every other sign make the value
    absent, and so does a length past the limit: a cut sentence is still a sentence.
    """
    if not isinstance(value, str) or not 0 < len(value) <= limit or value != value.strip():
        return None
    # A pair such as USDC/WETH keeps its slash; two in a row begin an address.
    if "//" in value:
        return None
    return value if all(shown(character, marks) for character in value) else None


def shown(character: str, marks: str) -> bool:
    category = unicodedata.category(character)
    return character in marks or category[0] in "LN" or category == "Sc"


def identifier(value: object) -> str | None:
    """The vendor's own ID of a chain, a token, a protocol or a pool, or absent."""
    if not isinstance(value, str) or not 0 < len(value) <= MAX_IDENTIFIER:
        return None
    return value if set(value) <= IDENTIFIER else None
