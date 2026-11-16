"""Verified CoinGecko contract discovery retained alongside historical evidence."""

import hashlib
import json
from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.types import Name
from almanak.caip import AssetType
from almanak.models import Model

CoinId = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]*$")]


def require_contract(asset):
    if asset.chain.namespace != "eip155" or asset.namespace != "erc20":
        raise ValueError(
            "CoinGecko discovery supports EVM ERC-20 assets; supply an explicit coin_id"
        )


def platform_for(asset, response):
    require_contract(asset)
    rows = json.loads(response)
    if not isinstance(rows, list):
        raise ValueError("CoinGecko platform response must be a list")
    matches = [
        row.get("id")
        for row in rows
        if isinstance(row, dict)
        and type(row.get("chain_identifier")) is int
        and row["chain_identifier"] == int(asset.chain.reference)
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise ValueError("CoinGecko discovery requires one matching chain platform")
    # Platform identifiers are inserted into fixed-host API paths.
    from pydantic import TypeAdapter

    return TypeAdapter(CoinId).validate_python(matches[0])


def coin_for(asset, platform, response):
    row = json.loads(response)
    if not isinstance(row, dict) or not isinstance(row.get("platforms"), dict):
        raise ValueError("CoinGecko discovery lacks contract metadata")
    address = row["platforms"].get(platform)
    if not isinstance(address, str) or address.lower() != asset.reference:
        raise ValueError("CoinGecko discovery contract does not match the requested asset")
    from pydantic import TypeAdapter

    return TypeAdapter(CoinId).validate_python(row.get("id"))


class CoinMapping(Model):
    asset: AssetType
    platform: CoinId
    coin_id: CoinId


class Discovery(CoinMapping):
    platforms_response: str
    coin_response: str
    retrieved_at: float

    @model_validator(mode="after")
    def verified(self):
        if self.platform != platform_for(self.asset, self.platforms_response):
            raise ValueError("retained CoinGecko platform differs from lookup evidence")
        if self.coin_id != coin_for(self.asset, self.platform, self.coin_response):
            raise ValueError("retained CoinGecko coin differs from lookup evidence")
        return self


class LookupRequest(Model):
    provider: Name
    asset: AssetType
    path: str

    @property
    def key(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()
