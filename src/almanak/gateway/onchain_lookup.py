"""Vendor discovery lookups for `almanak ax`.

CoinGecko on-chain lookups, defined here, name candidate pools and tokens with
the liquidity and volume the vendor reports; Morpho's market lookup is in
`morpho_lookup.py`. They are authoring discovery only: never asset identity, a
strategy observation or a backtest input. `ax` verifies a candidate through the
connector's own resolver at a pinned block before it calls it verified.

Each request is a closed shape; the URL is built here from validated fields, so
a caller cannot reach any other vendor path. The key is read from the operator
env file, in this process or in the read gateway, and never leaves it.
"""

import json
import threading
from pathlib import Path
from typing import Annotated, Literal

import httpx
from dotenv import dotenv_values
from pydantic import Field, JsonValue

from almanak.caip import ChainId
from almanak.gateway.auth import loopback_endpoint
from almanak.gateway.ethereum import DataError
from almanak.gateway.morpho_lookup import MorphoApi, MorphoMarkets
from almanak.gateway.rate_limits import ProviderLimit
from almanak.models import Address, Model

API = "https://pro-api.coingecko.com/api/v3/onchain"
KEY_VARIABLE = "COINGECKO_API_KEY"

# CoinGecko on-chain network IDs for the chains with a verified network definition.
NETWORKS: dict[ChainId, str] = {
    ChainId("eip155:1"): "eth",
    ChainId("eip155:10"): "optimism",
    ChainId("eip155:56"): "bsc",
    ChainId("eip155:137"): "polygon_pos",
    ChainId("eip155:999"): "hyperevm",
    ChainId("eip155:4663"): "robinhood",
    ChainId("eip155:8453"): "base",
    ChainId("eip155:42161"): "arbitrum",
    ChainId("eip155:43114"): "avax",
}

# The vendor answers one page of 20 rows; ten pages bound one lookup.
Page = Annotated[int, Field(strict=True, ge=1, le=10)]
# Symbols and names as a person types them, including the Unicode letters and currency
# signs some tokens use (USD₮0); the vendor's own search matches them.
Query = Annotated[str, Field(pattern=r"^[\p{L}\p{N}][\p{L}\p{N}\p{Sc} ._+-]{0,63}$")]
# The vendor's token batch limit.
MAX_TOKENS = 30


class TokenPools(Model):
    kind: Literal["token_pools"] = "token_pools"
    chain: ChainId
    token: Address
    page: Page = 1


class PoolSearch(Model):
    kind: Literal["pool_search"] = "pool_search"
    chain: ChainId
    query: Query
    page: Page = 1


class Tokens(Model):
    kind: Literal["tokens"] = "tokens"
    chain: ChainId
    tokens: Annotated[tuple[Address, ...], Field(min_length=1, max_length=MAX_TOKENS)]


Lookup = Annotated[TokenPools | PoolSearch | Tokens | MorphoMarkets, Field(discriminator="kind")]


class LookupRequest(Model):
    lookup: Lookup


class LookupResult(Model):
    request: LookupRequest
    # The vendor document as received; its rows are parsed and verified by the caller.
    result: JsonValue


def network(chain: ChainId) -> str:
    try:
        return NETWORKS[chain]
    except KeyError:
        # A DataError keeps its reason through the read gateway; a ValueError would
        # become its generic invalid-request refusal.
        raise DataError(
            "unsupported", f"no CoinGecko on-chain network is configured for {chain}"
        ) from None


def endpoint(lookup) -> tuple[str, dict[str, str]]:
    """The fixed vendor path and query parameters for one validated lookup."""
    name = network(lookup.chain)
    if isinstance(lookup, TokenPools):
        return f"/networks/{name}/tokens/{lookup.token}/pools", {"page": str(lookup.page)}
    if isinstance(lookup, PoolSearch):
        return "/search/pools", {"query": lookup.query, "network": name, "page": str(lookup.page)}
    if isinstance(lookup, Tokens):
        return f"/networks/{name}/tokens/multi/{','.join(lookup.tokens)}", {}
    raise ValueError(f"{lookup.kind} is not a CoinGecko on-chain lookup")


def document(response: httpx.Response) -> JsonValue:
    """The vendor body, or the category of its refusal.

    A token the vendor has not indexed answers 404. That is the vendor's missing
    data, not a measured empty result, so it is marked rather than returned as
    an empty list.
    """
    if response.status_code == 404:
        return {"data": [], "indexed": False}
    if response.status_code == 429:
        raise DataError("rate_limit", "coingecko_onchain: vendor rate limit")
    if response.status_code != 200:
        raise DataError("unavailable", f"coingecko_onchain: HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        raise DataError("malformed", "coingecko_onchain: response is not JSON") from None
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        raise DataError("malformed", "coingecko_onchain: response has no data rows")
    return body


class OnchainLookups:
    """Answers lookups with the operator's own key; the read gateway holds one of these."""

    def __init__(self, env_file: Path):
        key = dotenv_values(env_file, interpolate=False).get(KEY_VARIABLE)
        if not key:
            raise ValueError(f"{KEY_VARIABLE} is required in the selected env file")
        self.headers = {"x-cg-pro-api-key": key, "Accept": "application/json"}
        self.limit = ProviderLimit("coingecko")
        self.client = httpx.Client(timeout=30)

    def fetch(self, request: LookupRequest) -> LookupResult:
        path, params = endpoint(request.lookup)
        self.limit.admit()
        try:
            response = self.client.get(API + path, params=params, headers=self.headers)
        except httpx.HTTPError:
            raise DataError("unavailable", "coingecko_onchain: request failed") from None
        return LookupResult(request=request, result=document(response))

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


class VendorLookups:
    """Answers each lookup with its own vendor; the read gateway holds one of these.

    The CoinGecko key is read on the first CoinGecko lookup, so a process without
    one still answers Morpho lookups, and its absence is reported by name: the
    generic invalid-request refusal would hide that this process, not the caller,
    lacks the credential.
    """

    def __init__(self, env_file: Path | None):
        # None is a command with no credentials at all: lookups that need no key answer.
        self.env_file = env_file
        self.coingecko: OnchainLookups | None = None
        self.morpho: MorphoApi | None = None
        self.lock = threading.Lock()

    def fetch(self, request: LookupRequest) -> LookupResult:
        lookup = request.lookup
        if isinstance(lookup, MorphoMarkets):
            return LookupResult(request=request, result=self.morpho_api().fetch(lookup))
        return self.coingecko_api().fetch(request)

    def coingecko_api(self) -> OnchainLookups:
        with self.lock:
            if self.env_file is None:
                raise DataError(
                    "unavailable",
                    "lookup: this lookup uses a vendor key; give --env-file or the read gateway",
                )
            if self.coingecko is None:
                try:
                    self.coingecko = OnchainLookups(self.env_file)
                except ValueError as error:
                    raise DataError("unavailable", f"lookup: {error}") from None
            return self.coingecko

    def morpho_api(self) -> MorphoApi:
        with self.lock:
            if self.morpho is None:
                try:
                    self.morpho = MorphoApi()
                except ValueError as error:
                    raise DataError("unavailable", f"lookup: {error}") from None
            return self.morpho

    def close(self) -> None:
        for client in (self.coingecko, self.morpho):
            if client is not None:
                client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


class GatewayLookups:
    """Sends lookups to a read gateway, which holds the key. No credential is present here."""

    def __init__(self, endpoint_url: str, token: str):
        self.endpoint = loopback_endpoint(endpoint_url)
        self.token = token
        self.client = httpx.Client(timeout=httpx.Timeout(15.0, read=60.0))

    def fetch(self, request: LookupRequest) -> LookupResult:
        try:
            response = self.client.post(
                self.endpoint + "/lookup",
                content=request.model_dump_json(),
                headers={
                    "Authorization": "Bearer " + self.token,
                    "Content-Type": "application/json",
                },
            )
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            raise DataError("gateway_unavailable", "lookup: read gateway request failed") from None
        if not isinstance(payload, dict):
            raise DataError("malformed", "lookup: invalid gateway response")
        if response.status_code != 200:
            kind = payload.get("error", "gateway_error")
            raise DataError(str(kind), str(payload.get("message", "lookup: gateway refused")))
        return answered(request, payload.get("result"))

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.close()


def answered(request: LookupRequest, result: object) -> LookupResult:
    """The gateway's answer, refused unless it names the lookup that was sent."""
    try:
        observed = LookupResult.model_validate_json(json.dumps(result))
    except (TypeError, ValueError):
        raise DataError("malformed", "lookup: invalid gateway response") from None
    if observed.request != request:
        raise DataError("source_mismatch", "lookup: gateway answered a different lookup")
    return observed
