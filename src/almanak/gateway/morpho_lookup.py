"""Morpho Blue market discovery lookups for `almanak ax`.

Morpho's public API indexes every Blue market with the supply it holds, in the
loan asset and in USD, Morpho's own `listed` curation and its warnings. The API
answers from its current index, not from the block `ax` pins. Its
answers are authoring discovery only: never identity, a strategy observation or
a backtest input. `ax` resolves each candidate through the connector at a
pinned block before it calls it verified.

The request is a closed shape. The GraphQL document is fixed here and only the
validated fields travel as its variables, so a caller cannot ask the API
anything else. The API needs no key.
"""

from typing import Annotated, Literal

import httpx
from pydantic import Field, JsonValue, field_validator

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.rate_limits import ProviderLimit
from almanak.models import Address, Model

API = "https://api.morpho.org/graphql"

# The chains the API indexes among those with a reviewed Morpho core; the API
# refuses any other chain.
API_CHAINS = frozenset(
    ChainId(chain)
    for chain in (
        "eip155:1",
        "eip155:10",
        "eip155:137",
        "eip155:999",
        "eip155:4663",
        "eip155:8453",
        "eip155:42161",
    )
)

# Supplied markets of one loan asset, most supplied first. The loan asset is
# fixed, so its own amounts order the markets without depending on a USD price.
# A null filter variable leaves that filter out.
QUERY = """
query Markets(
  $chain: Int!
  $loan: [String!]
  $collateral: [String!]
  $listed: Boolean
  $first: Int!
  $skip: Int!
) {
  markets(
    first: $first
    skip: $skip
    orderBy: SupplyAssets
    orderDirection: Desc
    where: {
      chainId_in: [$chain]
      loanAssetAddress_in: $loan
      collateralAssetAddress_in: $collateral
      listed: $listed
      supplyAssets_gte: 1
    }
  ) {
    pageInfo { countTotal }
    items {
      marketId
      listed
      lltv
      loanAsset { address decimals }
      collateralAsset { address }
      state { supplyAssets borrowAssets supplyAssetsUsd borrowAssetsUsd supplyApy }
      warnings { type }
    }
  }
}
"""


class MorphoMarkets(Model):
    kind: Literal["morpho_markets"] = "morpho_markets"
    chain: ChainId
    loan: Address
    collateral: Address | None = None
    # Morpho's own curation. Anyone can create a market, and an unlisted one can
    # show supply looped against worthless collateral, so listings default to it.
    listed_only: bool = True
    first: Annotated[int, Field(strict=True, ge=1, le=100)] = 20
    skip: Annotated[int, Field(strict=True, ge=0, le=1000)] = 0

    @field_validator("chain")
    @classmethod
    def indexed(cls, chain: ChainId) -> ChainId:
        if chain not in API_CHAINS:
            raise ValueError(f"the Morpho API does not index {chain}")
        return chain


def variables(lookup: MorphoMarkets) -> dict:
    return {
        "chain": int(lookup.chain.reference),
        "loan": [lookup.loan],
        "collateral": None if lookup.collateral is None else [lookup.collateral],
        "listed": True if lookup.listed_only else None,
        "first": lookup.first,
        "skip": lookup.skip,
    }


def document(response: httpx.Response) -> JsonValue:
    """The API's market page, or the category of its refusal."""
    if response.status_code == 429:
        raise DataError("rate_limit", "morpho_api: vendor rate limit")
    if response.status_code != 200:
        # The API answers a document that fails its validation with 400 and its errors.
        reason = rejected(response) if response.status_code == 400 else None
        refused = "" if reason is None else f", the API refused the market query: {reason}"
        raise DataError("unavailable", f"morpho_api: HTTP {response.status_code}{refused}")
    try:
        body = response.json()
    except ValueError:
        raise DataError("malformed", "morpho_api: response is not JSON") from None
    if not isinstance(body, dict):
        raise DataError("unavailable", "morpho_api: the API refused the market query")
    if body.get("errors"):
        # The document is fixed, so a refused query means the API changed or failed.
        reason = refusal_reason(body["errors"])
        raise DataError("unavailable", f"morpho_api: the API refused the market query: {reason}")
    data = body.get("data")
    markets = data.get("markets") if isinstance(data, dict) else None
    if not isinstance(markets, dict) or not isinstance(markets.get("items"), list):
        raise DataError("malformed", "morpho_api: response has no market page")
    return markets


def rejected(response: httpx.Response) -> str | None:
    try:
        body = response.json()
    except ValueError:
        return None
    errors = body.get("errors") if isinstance(body, dict) else None
    return refusal_reason(errors) if errors else None


def refusal_reason(errors: object) -> str:
    first = errors[0] if isinstance(errors, list) and errors else None
    message = first.get("message") if isinstance(first, dict) else None
    return str(message)[:160] if message else "no reason given"


class MorphoApi:
    """Answers Morpho market lookups; the read gateway holds one of these."""

    def __init__(self):
        self.limit = ProviderLimit("morpho")
        self.client = httpx.Client(timeout=30)

    def fetch(self, lookup: MorphoMarkets) -> JsonValue:
        self.limit.admit()
        try:
            response = self.client.post(
                API,
                json={"query": QUERY, "variables": variables(lookup)},
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError:
            raise DataError("unavailable", "morpho_api: request failed") from None
        return document(response)

    def close(self) -> None:
        self.client.close()
