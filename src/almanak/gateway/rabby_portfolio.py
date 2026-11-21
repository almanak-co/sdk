"""An account's tokens and protocol positions from Rabby's backend.

Rabby's backend is the accepted default source for the `almanak ax portfolio`
overview only. It needs no key. It is the wallet's own backend, not a documented
public API: it can change, throttle or refuse without notice, and it sees every
address asked about. Its figures are a vendor's and are
reported as such; nothing read here becomes identity, a price input or the basis
of an action.

The whitelist is three fixed paths: the chains the account has value on, the
tokens it holds on one chain, and its protocol positions on every chain. The
vendor also serves a stored token list for every chain at once, which keeps
balances from before an account's latest transactions until something refreshes
it; it is not read. A refusal, a throttle and an answer of another shape are each
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

Listing = Literal["chains", "tokens", "positions"]
# The chain summary and the protocol positions each cover every chain in one call.
# Wallet tokens are asked one chain at a time, in the vendor's own chain label.
PATHS: dict[Listing, str] = {
    "chains": "/v1/user/total_balance",
    "tokens": "/v1/user/token_list",
    "positions": "/v1/user/complex_protocol_list",
}
CHAIN_LIST = "chain_list"

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
    body = answer(listing, response)
    if listing == "chains":
        # The summary is an object; its rows are the chains it lists.
        body = body.get(CHAIN_LIST) if isinstance(body, dict) else None
    if not isinstance(body, list) or not all(isinstance(row, dict) for row in body):
        raise DataError("malformed", f"{SOURCE}: {listing}: response is not a list of rows")
    return body


def answer(listing: Listing, response: httpx.Response) -> object:
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
        return json.loads(response.text, parse_float=Decimal)
    except ValueError:
        raise DataError("malformed", f"{SOURCE}: {listing}: response is not JSON") from None


class RabbyPortfolio:
    """Answers the three whitelisted listings for one account."""

    def __init__(self, *, transport: httpx.BaseTransport | None = None, clock=received_now):
        self.client = httpx.Client(base_url=API, timeout=30, transport=transport)
        self.clock = clock

    def fetch(
        self, listing: Listing, account: str, chain: str | None = None
    ) -> tuple[list[dict], int]:
        """The rows of one listing and the time, in milliseconds, they were received.

        `chain` is the vendor's chain label, stated for the token listing only.
        """
        if (listing == "tokens") != (chain is not None):
            raise ValueError("the token listing, and only it, is asked for one chain")
        params = {"id": address(account)}
        if chain is not None:
            # Every token of the chain, so the report can count the ones it leaves out.
            params |= {"chain_id": chain, "is_all": "true"}
        try:
            response = self.client.get(PATHS[listing], params=params)
        except httpx.HTTPError:
            raise DataError("unavailable", f"{SOURCE}: {listing}: request failed") from None
        return rows(listing, response), self.clock()

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()
