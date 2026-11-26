"""The venue's own quote of one deposit, and the checks it must pass before signing.

The SpokePool checks no price: the output amount is whatever the deposit states. The
gateway asks Across for an exact-size quote and refuses when its fee exceeds the
author's ceiling, so a false quote costs at most that ceiling, or a delay and a refund.
Nothing here is on-chain enforcement, and no other source is ever substituted.
"""

import json
import os
import re
from typing import Annotated, Literal, Protocol

import httpx
from pydantic import Field

from almanak.connectors.across.domain import Decimals, Target, Uint32, Uint256, spoke_pool
from almanak.gateway.environments import is_fork
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.models import ZERO_ADDRESS, Address, Model

API = "https://app.across.to/api/suggested-fees"
SOURCE: Literal["across_api"] = "across_api"
# A quote an identified fork's gateway constructed itself. It is admitted on a fork only
# and is never evidence of what the venue would have quoted.
FORK_SOURCE: Literal["across_fork_constructed"] = "across_fork_constructed"
Source = Literal["across_api", "across_fork_constructed"]
# The name a gateway declares its quote transport under.
QUOTE_VENUE = "across-quote"
# An optional operator key for the venue's quote API. It stays in the gateway's
# environment and never reaches author files, logs or retained evidence.
API_KEY_ENV = "ALMANAK_ACROSS_API_KEY"
TIMEOUT_SECONDS = 15.0
# A deposit mined with less time than this before its fill deadline gives a relayer too
# little time to fill it; it would wait for the refund instead.
MINIMUM_FILL_SECONDS = 600
# The SpokePool reads an exclusivity parameter up to one year as an offset from the
# block that mines the deposit, and a larger one as a timestamp.
MAX_EXCLUSIVITY_OFFSET = 31_536_000
# The venue's answer is about 1.4 kB. A larger body is refused before it is parsed, so the
# copy every plan, pending attempt and outcome retains stays bounded.
MAX_ANSWER_BYTES = 16_384
# At most the 78 digits of a uint256, so a long digit string never reaches integer conversion.
# The code the venue answers, with HTTP 400, for an amount below what its fees allow.
TOO_LOW_CODE = "AMOUNT_TOO_LOW"
DIGITS = re.compile(r"[0-9]{1,78}", re.ASCII)
FIELDS = (
    "outputAmount",
    "timestamp",
    "fillDeadline",
    "exclusiveRelayer",
    "exclusivityDeadline",
    "spokePoolAddress",
    "destinationSpokePoolAddress",
    "inputToken",
    "outputToken",
    "isAmountTooLow",
)


class Quote(Model):
    """The quoted deposit terms, with the venue's answer retained in canonical form."""

    source: Source
    input_amount: Uint256
    output_amount: Uint256
    timestamp: Uint32
    fill_deadline: Uint32
    exclusive_relayer: Address
    exclusivity_parameter: Uint32
    spoke_pool: Address
    destination_spoke_pool: Address
    # The tokens the venue says it quoted, with the decimals it holds for them.
    input_token: Address
    input_decimals: Decimals
    output_token: Address
    output_decimals: Decimals
    amount_too_low: bool
    # The whole answer as canonical JSON text: sorted keys, no whitespace, finite numbers.
    # Text survives a checkpoint round trip byte for byte, which a decoded tree does not.
    response: Annotated[str, Field(min_length=2, max_length=MAX_ANSWER_BYTES)]


def request(target: Target, amount: int) -> dict[str, str]:
    route = target.route
    return {
        # The venue quotes token addresses and has no native parameter: a native route
        # asks with the wrapped native token of each chain.
        "inputToken": route.input_token,
        "outputToken": route.output_token,
        "originChainId": str(chain_number(route.chain.chain_id)),
        "destinationChainId": str(chain_number(route.destination_chain)),
        "amount": str(amount),
        "recipient": target.destination_fund.address,
    }


def answer(response: httpx.Response) -> dict:
    """The venue's answer, or the category of its refusal."""
    if response.status_code == 429:
        raise DataError("rate_limited", f"{SOURCE}: the venue throttled the quote request")
    if too_low(response):
        raise DataError(
            "amount_too_low",
            f"{SOURCE}: the venue refuses the amount as too low relative to its fees; the "
            "minimum is the venue's own and changes with them",
        )
    if response.status_code >= 400:
        raise DataError(
            "quote_refused", f"{SOURCE}: the venue refused the quote ({response.status_code})"
        )
    if response.status_code != 200:
        raise DataError(
            "quote_refused", f"{SOURCE}: the venue gave no quote ({response.status_code})"
        )
    if len(response.content) > MAX_ANSWER_BYTES:
        raise DataError("malformed", f"{SOURCE}: the quote exceeds {MAX_ANSWER_BYTES} bytes")
    try:
        body = json.loads(response.content, parse_constant=non_finite)
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise DataError("malformed", f"{SOURCE}: the quote is not a JSON object")
    return body


def too_low(response: httpx.Response) -> bool:
    """Whether the venue refused the request with its own `AMOUNT_TOO_LOW` code.

    Only the code is read, from a bounded body; the venue's message is not repeated.
    """
    if response.status_code != 400 or len(response.content) > MAX_ANSWER_BYTES:
        return False
    try:
        body = json.loads(response.content)
    except ValueError:
        return False
    return isinstance(body, dict) and body.get("code") == TOO_LOW_CODE


def non_finite(name: str):
    raise ValueError(f"non-finite number {name}")


def canonical(body: dict) -> str:
    """The answer as bounded canonical JSON text; a non-finite number has no such form."""
    try:
        text = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise DataError("malformed", f"{SOURCE}: the quote has no canonical JSON form") from None
    if len(text.encode()) > MAX_ANSWER_BYTES:
        raise DataError("malformed", f"{SOURCE}: the quote exceeds {MAX_ANSWER_BYTES} bytes")
    return text


def unsigned(body: dict, field: str) -> int:
    """A non-negative integer the venue sends as a number or as ASCII decimal digits."""
    value = body.get(field)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and DIGITS.fullmatch(value):
        return int(value)
    raise DataError("malformed", f"{SOURCE}: {field} is not a non-negative integer")


def token(body: dict, field: str) -> tuple[str, int]:
    """The address and decimals of a token the venue says it quoted."""
    value = body[field]
    if not isinstance(value, dict) or not isinstance(value.get("address"), str):
        raise DataError("malformed", f"{SOURCE}: {field} names no token address")
    return value["address"], unsigned(value, "decimals")


def parse(body: dict, amount: int, source: Source = SOURCE) -> Quote:
    missing = [field for field in FIELDS if field not in body]
    if missing:
        raise DataError("malformed", f"{SOURCE}: the quote lacks {', '.join(missing)}")
    if not isinstance(body["isAmountTooLow"], bool):
        raise DataError("malformed", f"{SOURCE}: isAmountTooLow is not a boolean")
    response = canonical(body)
    (input_token, input_decimals), (output_token, output_decimals) = (
        token(body, "inputToken"),
        token(body, "outputToken"),
    )
    try:
        return Quote(
            source=source,
            input_amount=amount,
            output_amount=unsigned(body, "outputAmount"),
            timestamp=unsigned(body, "timestamp"),
            fill_deadline=unsigned(body, "fillDeadline"),
            exclusive_relayer=body["exclusiveRelayer"],
            exclusivity_parameter=unsigned(body, "exclusivityDeadline"),
            spoke_pool=body["spokePoolAddress"],
            destination_spoke_pool=body["destinationSpokePoolAddress"],
            input_token=input_token,
            input_decimals=input_decimals,
            output_token=output_token,
            output_decimals=output_decimals,
            amount_too_low=body["isAmountTooLow"],
            response=response,
        )
    except ValueError as error:
        raise DataError("malformed", f"{SOURCE}: the quote has an invalid field") from error


def headers(api_key: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}


class Transport(Protocol):
    """Where a gateway gets the answer to one quote request, and the source it labels."""

    source: Source

    def get(self, target: Target, amount: int, block_timestamp: int) -> httpx.Response: ...


class HttpTransport:
    """The venue's own API over HTTPS. The pinned block plays no part in its request."""

    source: Source = SOURCE

    def __init__(self, *, api_key: str | None = None, client=None):
        self.api_key = api_key
        self.client = client

    def get(self, target: Target, amount: int, block_timestamp: int) -> httpx.Response:
        key = self.api_key or os.environ.get(API_KEY_ENV) or None
        client = self.client or httpx.Client(timeout=TIMEOUT_SECONDS, trust_env=False)
        try:
            return client.get(API, params=request(target, amount), headers=headers(key))
        except httpx.HTTPError as error:
            # The exception text can carry the request, so only its type is reported.
            raise DataError(
                "quote_unavailable", f"{SOURCE}: no answer from the venue ({type(error).__name__})"
            ) from None
        finally:
            if self.client is None:
                client.close()


def fetch(target: Target, amount: int, transport: Transport, block_timestamp: int) -> Quote:
    """Ask the gateway's one transport once. A failure is reported as what it is; nothing
    is retried here, and every transport's answer takes the same size cap and parser."""
    response = transport.get(target, amount, block_timestamp)
    return parse(answer(response), amount, transport.source)


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
    yield from source_refusals(quote, target)
    if quote.input_amount != amount:
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
    yield from route_refusals(quote, route)
    yield from timing_refusals(quote, pool, block_timestamp)


def source_refusals(quote: Quote, target: Target):
    """A constructed quote is admitted on an identified fork only; any other quote must
    come from the source the route names."""
    if quote.source == FORK_SOURCE:
        if not is_fork(target.fund.chain):
            yield (
                "quote_source",
                "a fork-constructed quote is admitted on an identified fork only, and "
                f"{target.fund.chain.chain_id} is not one",
            )
    elif quote.source != target.route.quote:
        yield "quote_mismatch", "the quote answers another source or amount"


def route_refusals(quote: Quote, route):
    """The venue must have quoted this route's own contracts and tokens.

    The fee ceiling compares base units of the sent and the received asset, which means
    nothing unless both have the same decimals.
    """
    if quote.spoke_pool != route.spoke_pool:
        yield "quote_mismatch", "the quote names another SpokePool than the pinned one"
    if quote.destination_spoke_pool != spoke_pool(route.destination_chain).address:
        yield "quote_mismatch", "the quote names another destination SpokePool than the pinned one"
    if (quote.input_token, quote.output_token) != (route.input_token, route.output_token):
        yield "quote_mismatch", "the quote names other tokens than the route's"
    if quote.input_decimals != route.decimals:
        yield "quote_mismatch", "the quote holds other decimals for the sent asset than its chain"
    if quote.output_decimals != route.decimals:
        yield "decimals_differ", "the sent and the received asset have different decimals"


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
