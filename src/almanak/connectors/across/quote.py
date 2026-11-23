"""The venue's own quote of one deposit, and the checks it must pass before signing.

The SpokePool checks no price: the output amount is whatever the deposit states. The
gateway asks Across for an exact-size quote and refuses when its fee exceeds the
author's ceiling, so a false quote costs at most that ceiling, or a delay and a refund.
Nothing here is on-chain enforcement, and no other source is ever substituted.
"""

import json
from typing import Any

import httpx

from almanak.connectors.across.domain import Target, Uint32, Uint256
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.models import ZERO_ADDRESS, Address, Model

API = "https://app.across.to/api/suggested-fees"
SOURCE = "across_api"
TIMEOUT_SECONDS = 15.0
# A deposit mined with less time than this before its fill deadline gives a relayer too
# little time to fill it; it would wait for the refund instead.
MINIMUM_FILL_SECONDS = 600
# The SpokePool reads an exclusivity parameter up to one year as an offset from the
# block that mines the deposit, and a larger one as a timestamp.
MAX_EXCLUSIVITY_OFFSET = 31_536_000
FIELDS = (
    "outputAmount",
    "timestamp",
    "fillDeadline",
    "exclusiveRelayer",
    "exclusivityDeadline",
    "spokePoolAddress",
    "isAmountTooLow",
)


class Quote(Model):
    """The quoted deposit terms, with the venue's answer retained as it was received."""

    source: str
    input_amount: Uint256
    output_amount: Uint256
    timestamp: Uint32
    fill_deadline: Uint32
    exclusive_relayer: Address
    exclusivity_parameter: Uint32
    spoke_pool: Address
    amount_too_low: bool
    response: dict[str, Any]


def request(target: Target, amount: int) -> dict[str, str]:
    route = target.route
    return {
        "inputToken": route.asset.reference,
        "outputToken": route.destination_asset.reference,
        "originChainId": str(chain_number(route.chain.chain_id)),
        "destinationChainId": str(chain_number(route.destination_chain.chain_id)),
        "amount": str(amount),
        "recipient": target.destination_fund.address,
    }


def answer(response: httpx.Response) -> dict:
    """The venue's answer, or the category of its refusal."""
    if response.status_code == 429:
        raise DataError("rate_limited", f"{SOURCE}: the venue throttled the quote request")
    if response.status_code >= 400:
        raise DataError(
            "quote_refused", f"{SOURCE}: the venue refused the quote ({response.status_code})"
        )
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        body = None
    if not isinstance(body, dict):
        raise DataError("malformed", f"{SOURCE}: the quote is not a JSON object")
    return body


def unsigned(body: dict, field: str) -> int:
    value = body[field]
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise DataError("malformed", f"{SOURCE}: {field} is not an integer")
    try:
        number = int(value)
    except ValueError:
        raise DataError("malformed", f"{SOURCE}: {field} is not an integer") from None
    if number < 0:
        raise DataError("malformed", f"{SOURCE}: {field} is negative")
    return number


def parse(body: dict, amount: int) -> Quote:
    missing = [field for field in FIELDS if field not in body]
    if missing:
        raise DataError("malformed", f"{SOURCE}: the quote lacks {', '.join(missing)}")
    if not isinstance(body["isAmountTooLow"], bool):
        raise DataError("malformed", f"{SOURCE}: isAmountTooLow is not a boolean")
    try:
        return Quote(
            source=SOURCE,
            input_amount=amount,
            output_amount=unsigned(body, "outputAmount"),
            timestamp=unsigned(body, "timestamp"),
            fill_deadline=unsigned(body, "fillDeadline"),
            exclusive_relayer=body["exclusiveRelayer"],
            exclusivity_parameter=unsigned(body, "exclusivityDeadline"),
            spoke_pool=body["spokePoolAddress"],
            amount_too_low=body["isAmountTooLow"],
            response=body,
        )
    except ValueError as error:
        raise DataError("malformed", f"{SOURCE}: the quote has an invalid field") from error


def headers(api_key: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


def fetch(target: Target, amount: int, *, api_key: str | None = None, client=None) -> Quote:
    """Ask the venue once. A failure is reported as what it is; nothing is retried here."""
    owned = client is None
    client = client or httpx.Client(timeout=TIMEOUT_SECONDS, trust_env=False)
    try:
        response = client.get(API, params=request(target, amount), headers=headers(api_key))
    except httpx.HTTPError as error:
        # The exception text can carry the request, so only its type is reported.
        raise DataError(
            "quote_unavailable", f"{SOURCE}: no answer from the venue ({type(error).__name__})"
        ) from None
    finally:
        if owned:
            client.close()
    return parse(answer(response), amount)


def fee_ceiling(amount: int, max_fee_bps: int) -> int:
    """The largest fee the author accepts for `amount`, rounded down."""
    return amount * max_fee_bps // 10_000


def exclusivity_end(quote: Quote, block_timestamp: int) -> int:
    """When exclusivity ends for a deposit mined at `block_timestamp`; zero means none."""
    parameter = quote.exclusivity_parameter
    if parameter == 0:
        return 0
    return block_timestamp + parameter if parameter <= MAX_EXCLUSIVITY_OFFSET else parameter


def refusals(quote: Quote, target: Target, amount: int, pool, block_timestamp: int):
    """Every reason the quote cannot be signed, as (kind, message) pairs."""
    route = target.route
    if quote.source != route.quote or quote.input_amount != amount:
        yield "quote_mismatch", "the quote answers another source or amount"
    if quote.amount_too_low or quote.output_amount == 0:
        yield "amount_too_low", "the venue reports the amount as too low to bridge"
    if quote.output_amount > amount:
        yield "quote_mismatch", "the quoted output exceeds the amount sent"
    elif amount - quote.output_amount > fee_ceiling(amount, route.max_fee_bps):
        yield (
            "fee_above_ceiling",
            f"the quoted bridge fee {amount - quote.output_amount} exceeds the ceiling "
            f"{fee_ceiling(amount, route.max_fee_bps)} ({route.max_fee_bps} bps of {amount})",
        )
    if quote.spoke_pool != route.spoke_pool:
        yield "quote_mismatch", "the quote names another SpokePool than the pinned one"
    yield from timing_refusals(quote, pool, block_timestamp)


def timing_refusals(quote: Quote, pool, block_timestamp: int):
    if quote.timestamp > block_timestamp:
        yield "quote_time", "the quote timestamp is ahead of the pinned block"
    elif block_timestamp - quote.timestamp > pool.quote_time_buffer:
        yield "quote_time", "the quote timestamp is older than the SpokePool's quote buffer"
    if quote.fill_deadline > block_timestamp + pool.fill_deadline_buffer:
        yield "fill_deadline", "the fill deadline is beyond the SpokePool's deadline buffer"
    if quote.fill_deadline < block_timestamp + MINIMUM_FILL_SECONDS:
        yield "fill_deadline", "the fill deadline leaves a relayer too little time to fill"
    if exclusivity_end(quote, block_timestamp) > quote.fill_deadline:
        yield "exclusivity", "relayer exclusivity ends after the fill deadline"
    if quote.exclusivity_parameter and quote.exclusive_relayer == ZERO_ADDRESS:
        yield "exclusivity", "exclusivity names no relayer"


def admit(quote: Quote, target: Target, amount: int, pool, block_timestamp: int) -> None:
    """Refuse the first reason the quote cannot be signed."""
    for kind, message in refusals(quote, target, amount, pool, block_timestamp):
        raise DataError(kind, f"{SOURCE}: {message}")
