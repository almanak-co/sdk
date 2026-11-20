"""An account's tokens and protocol positions from Rabby's backend.

Rabby's backend is the accepted default source for the `almanak ax portfolio`
overview only. It needs no key. It is the wallet's own backend, not a documented
public API: it can change, throttle or refuse without notice, and it sees every
address asked about. Its figures are a vendor's and are
reported as such; nothing read here becomes identity, a price input or the basis
of an action.

The whitelist is two fixed paths, each answering for every chain the vendor
indexes. A refusal, a throttle and an answer that is not a list of rows are each
reported as what they are, and nothing is substituted.
"""

import json
import time
from decimal import Decimal
from typing import Literal

import httpx

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import address

API = "https://api.rabby.io"
SOURCE = "rabby"

Listing = Literal["tokens", "positions"]
# Wallet balances come from the vendor's cache of the account; protocol positions
# carry each protocol's detail. Neither takes a chain: one call covers them all.
PATHS: dict[Listing, str] = {
    "tokens": "/v1/user/cache_token_list",
    "positions": "/v1/user/complex_protocol_list",
}

# The vendor's chain labels for the chains with a verified network definition. A
# row on any other chain keeps the vendor's label and has no CAIP identity.
CHAINS: dict[str, ChainId] = {
    "eth": ChainId("eip155:1"),
    "op": ChainId("eip155:10"),
    "bsc": ChainId("eip155:56"),
    "matic": ChainId("eip155:137"),
    "hyper": ChainId("eip155:999"),
    "hood": ChainId("eip155:4663"),
    "base": ChainId("eip155:8453"),
    "arb": ChainId("eip155:42161"),
    "avax": ChainId("eip155:43114"),
}


def received_now() -> int:
    return time.time_ns() // 1_000_000


def rows(listing: Listing, response: httpx.Response) -> list[dict]:
    """The vendor's rows, or the category of its refusal."""
    if response.status_code == 429:
        raise DataError(
            "rate_limit",
            f"{SOURCE}: {listing}: the vendor throttled this address of ours; a throttle"
            " has lasted longer than ten minutes",
            code=429,
        )
    if response.status_code != 200:
        raise DataError("unavailable", f"{SOURCE}: {listing}: HTTP {response.status_code}")
    try:
        # Amounts and prices stay the decimals the vendor sent, never binary floats.
        body = json.loads(response.text, parse_float=Decimal)
    except ValueError:
        raise DataError("malformed", f"{SOURCE}: {listing}: response is not JSON") from None
    if not isinstance(body, list) or not all(isinstance(row, dict) for row in body):
        raise DataError("malformed", f"{SOURCE}: {listing}: response is not a list of rows")
    return body


class RabbyPortfolio:
    """Answers the two whitelisted listings for one account."""

    def __init__(self, *, transport: httpx.BaseTransport | None = None, clock=received_now):
        self.client = httpx.Client(base_url=API, timeout=30, transport=transport)
        self.clock = clock

    def fetch(self, listing: Listing, account: str) -> tuple[list[dict], int]:
        """The rows of one listing and the time, in milliseconds, they were received."""
        try:
            response = self.client.get(PATHS[listing], params={"id": address(account)})
        except httpx.HTTPError:
            raise DataError("unavailable", f"{SOURCE}: {listing}: request failed") from None
        return rows(listing, response), self.clock()

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()
