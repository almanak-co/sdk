"""Operator settings used for read bootstrap and independent execution checks."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.pricing import VendorBinding
from almanak.caip import ChainId
from almanak.connectors.support import RunMode
from almanak.gateway.operator_execution import ProductionAuthority
from almanak.gateway.operator_fixture import ForkFixtureInputs
from almanak.gateway.providers import (
    ALCHEMY_NETWORKS,
    AlchemyProvider,
    EnvName,
    Provider,
    UrlProvider,
)
from almanak.models import Model, SimulationAccount, StrategyAccounts

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


def content_digest(value: Model) -> str:
    encoded = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


class AlchemyAccess(Model):
    kind: Literal["alchemy"]
    api_key_env: EnvName


ReadProvider = Annotated[AlchemyAccess | UrlProvider, Field(discriminator="kind")]


class OperatorNetwork(Model):
    chain: ChainId
    provider: ReadProvider

    def binding(self) -> Provider:
        if isinstance(self.provider, UrlProvider):
            return self.provider
        try:
            network = ALCHEMY_NETWORKS[self.chain]
        except KeyError:
            raise ValueError(f"no Alchemy network binding for {self.chain}") from None
        return AlchemyProvider(
            kind="alchemy", network=network, api_key_env=self.provider.api_key_env
        )

    @model_validator(mode="after")
    def supported_provider(self):
        self.binding()
        return self


class OperatorConfig(Model):
    """Configuration alone never admits a signer, spending or fixture installation."""

    schema_version: Literal["operator-read-config-v1", "operator-gateway-config-v1"] = (
        "operator-gateway-config-v1"
    )
    modes: Annotated[tuple[RunMode, ...], Field(min_length=1)]
    networks: Annotated[tuple[OperatorNetwork, ...], Field(min_length=1)]
    vendors: tuple[VendorBinding, ...] = Field(default=(), exclude_if=lambda value: not value)
    accounts: StrategyAccounts
    production: ProductionAuthority | None = Field(default=None, exclude_if=lambda v: v is None)
    fork: ForkFixtureInputs | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        chains = {network.chain for network in self.networks}
        if len(chains) != len(self.networks) or len(set(self.modes)) != len(self.modes):
            raise ValueError("operator networks and modes must be unique")
        if len({vendor.name for vendor in self.vendors}) != len(self.vendors):
            raise ValueError("operator vendor bindings must have unique names")
        for binding in self.accounts.bindings:
            profile = binding.profile
            if (
                isinstance(profile, SimulationAccount)
                or profile.fund.chain.environment != "mainnet"
            ):
                raise ValueError("bootstrap account bindings require origin EOA or Safe profiles")
            if profile.fund.chain.chain_id not in chains:
                raise ValueError(f"operator account {binding.name} has no provider binding")
        if self.production is not None or self.fork is not None:
            if self.schema_version == "operator-read-config-v1":
                raise ValueError("retained read-only operator codecs cannot declare execution")
        if self.production is not None:
            if RunMode.PRODUCTION not in self.modes:
                raise ValueError("production authority requires explicitly enabled production mode")
            self.production.validate_operator(self)
        if self.fork is not None:
            if RunMode.FORK not in self.modes:
                raise ValueError("fork fixture requires explicitly enabled fork mode")
            self.fork.validate_accounts(self.accounts)
        return self

    @property
    def digest(self):
        return content_digest(self)

    def network(self, chain: ChainId) -> OperatorNetwork:
        for network in self.networks:
            if network.chain == chain:
                return network
        raise ValueError(f"operator configuration has no provider binding for {chain}")


def read_operator_config(path: Path) -> OperatorConfig:
    from almanak.configuration.yaml import load

    return OperatorConfig.model_validate_json(json.dumps(load(path).value))
