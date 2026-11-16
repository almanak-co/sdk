"""Generated read context and frozen anchors preceding parameter construction."""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.chains import CheckpointReference
from almanak.configuration.anchors import AnchorSelection as AnchorSelection
from almanak.configuration.authoring import MarketSelection
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import (
    Digest,
    Name,
    OperatorConfig,
    content_digest,
)
from almanak.models import Account, AccountBinding, Eoa, Model, StrategyAccounts


def simulation_address(name: str, chain: str) -> str:
    digest = hashlib.sha256(f"almanak-simulation:{chain}:{name}".encode())
    return "0x" + digest.hexdigest()[-40:]


def simulation_binding(name: str, chain: ChainId) -> AccountBinding:
    """A hypothetical fund identity; it never carries signing or installed authority."""
    fund = Account.model_validate(
        {
            "chain": {"chain_id": chain, "environment": "mainnet"},
            "address": simulation_address(name, str(chain)),
        }
    )
    return AccountBinding(name=name, profile=Eoa(kind="eoa", fund=fund))


class AccountRequirement(Model):
    name: Name
    chain: ChainId


class NamedMarket(Model):
    name: Name
    selection: MarketSelection


class ReadContext(Model):
    """Runtime artifact fixed before I/O; never a runnable deployment or author file."""

    schema_version: Literal["configuration-read-context-v1"] = "configuration-read-context-v1"
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    authored_digest: Digest
    operator_digest: Digest
    mode: RunMode
    accounts: Annotated[tuple[AccountRequirement, ...], Field(min_length=1)]
    markets: Annotated[tuple[NamedMarket, ...], Field(min_length=1)]
    anchors: Annotated[tuple[AnchorSelection, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def scope(self):
        for label, values in (("account", self.accounts), ("market", self.markets)):
            if len({item.name for item in values}) != len(values):
                raise ValueError(f"read context {label} names must be unique")
        chains = {item.chain for item in self.anchors}
        if len(chains) != len(self.anchors):
            raise ValueError("read context requires one anchor per chain")
        if {item.chain for item in self.accounts} != chains:
            raise ValueError("read context account and anchor chains must match")
        if not {item.selection.chain for item in self.markets} <= chains:
            raise ValueError("read context market has no account or anchor on its chain")
        return self

    @property
    def digest(self):
        return content_digest(self)

    def match_operator(self, operator: OperatorConfig, mode: RunMode) -> StrategyAccounts:
        if self.operator_digest != operator.digest or self.mode != mode:
            raise ValueError(
                "read context differs from the selected operator configuration or mode"
            )
        if mode not in operator.modes:
            raise ValueError("operator configuration does not enable the requested mode")
        bindings = []
        for requirement in self.accounts:
            operator.network(requirement.chain)
            if mode == RunMode.BACKTEST:
                bindings.append(simulation_binding(requirement.name, requirement.chain))
                continue
            try:
                binding = operator.accounts.binding(requirement.name)
            except KeyError:
                raise ValueError(
                    f"operator account binding {requirement.name} is missing"
                ) from None
            if binding.profile.fund.chain.chain_id != requirement.chain:
                raise ValueError(f"operator account {requirement.name} belongs to another chain")
            bindings.append(binding)
        for market in self.markets:
            require_market_support(market, mode)
        return StrategyAccounts(bindings=tuple(bindings))


def require_market_support(market: NamedMarket, mode: RunMode):
    """Each named market's own connector must declare its chain and the requested mode."""
    from almanak.connectors.composition import connector_from_wire

    connector = connector_from_wire({"connector": str(market.selection.connector)})
    support = connector.support_for(market.selection.chain)
    if mode not in support.modes:
        raise ValueError(f"market {market.name} has no declared support for this chain/mode")


class ResolutionAnchors(Model):
    context_digest: Digest
    checkpoints: Annotated[tuple[CheckpointReference, ...], Field(min_length=1)]

    def require(self, context: ReadContext):
        if self.context_digest != context.digest:
            raise ValueError("resolution anchors belong to another read context")
        if len(self.checkpoints) != len(context.anchors):
            raise ValueError("resolution requires every selected chain anchor")
        for selection, checkpoint in zip(context.anchors, self.checkpoints, strict=True):
            selection.require(checkpoint)

    def checkpoint(self, chain: ChainId) -> CheckpointReference:
        for checkpoint in self.checkpoints:
            if checkpoint.chain.chain_id == chain:
                return checkpoint
        raise ValueError(f"resolution has no anchor for {chain}")
