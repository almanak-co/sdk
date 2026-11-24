"""Read-only phase of the existing gateway, before executable parameters exist."""

from pathlib import Path
from typing import Any, Literal, cast

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.chains import CheckpointReference
from almanak.configuration.authoring import ResolvedMarketFacts, binding_for
from almanak.configuration.resolution import ReadContext, ResolutionAnchors
from almanak.connectors.support import RunMode
from almanak.gateway.ethereum import BUSY_WAIT_LIMIT, DataError
from almanak.gateway.operator_config import Digest, Name, OperatorConfig, content_digest
from almanak.gateway.production_config import GasPolicy
from almanak.gateway.providers import provider_endpoint
from almanak.gateway.resolution_evm import (
    ResolutionRead,
    ResolutionReader,
    asset_symbols,
    evm_block,
    resolve_anchor,
)
from almanak.models import Chain, Model, StrategyAccounts

FIELDS = {
    "resolution-context": {"context_digest"},
    "resolution-anchor": {"context_digest", "chain"},
    "resolution-markets": {"context_digest", "anchors"},
    "resolution-audit": {"context_digest", "anchors"},
    "resolution-gas-policy": {"context_digest", "policies"},
}


class BootstrapRequest(Model):
    context_digest: Digest
    chain: ChainId | None = None
    anchors: ResolutionAnchors | None = None
    policies: tuple[GasPolicy, ...] | None = None


class ReadBinding(Model):
    context_digest: Digest
    operator_digest: Digest
    mode: RunMode
    accounts: StrategyAccounts
    authority: Literal["read_only"] = "read_only"

    def require(self, context: ReadContext):
        if (self.context_digest, self.operator_digest, self.mode) != (
            context.digest,
            context.operator_digest,
            context.mode,
        ):
            raise ValueError("gateway read binding differs from the provisional context")
        expected = tuple((a.name, a.chain) for a in context.accounts)
        actual = tuple((a.name, a.profile.fund.chain.chain_id) for a in self.accounts.bindings)
        if expected != actual or any(
            a.profile.fund.chain.environment != "mainnet" for a in self.accounts.bindings
        ):
            raise ValueError("gateway read binding differs from the requested account scopes")


class MarketFacts(Model):
    name: Name
    resolved: ResolvedMarketFacts
    evidence: tuple[ResolutionRead, ...]
    # Display names read beside the facts. They are outside the verified facts digest.
    symbols: dict[AssetType, str] = Field(default_factory=dict, exclude_if=lambda v: not v)
    # Reads of a two-chain market's second chain, at that chain's anchor. Omitted otherwise.
    destination_evidence: tuple[ResolutionRead, ...] = Field(default=(), exclude_if=lambda v: not v)


class ResolvedMarket(Model):
    name: Name
    resolved: ResolvedMarketFacts


class ResolvedFacts(Model):
    """The verified facts alone; retrieval times and context identity are excluded."""

    checkpoints: tuple[CheckpointReference, ...]
    markets: tuple[ResolvedMarket, ...]


class ResolutionResult(Model):
    schema_version: Literal["configuration-resolution-v1"] = "configuration-resolution-v1"
    status: Literal["facts_resolved"] = "facts_resolved"
    binding: ReadBinding
    anchors: ResolutionAnchors
    markets: tuple[MarketFacts, ...]

    @property
    def digest(self):
        return content_digest(self)

    @property
    def facts(self) -> ResolvedFacts:
        return ResolvedFacts(
            checkpoints=self.anchors.checkpoints,
            markets=tuple(ResolvedMarket(name=m.name, resolved=m.resolved) for m in self.markets),
        )

    @property
    def facts_digest(self):
        return content_digest(self.facts)

    @model_validator(mode="after")
    def scope(self):
        if self.binding.context_digest != self.anchors.context_digest:
            raise ValueError("resolved facts and anchors have different contexts")
        if not self.markets or len({m.name for m in self.markets}) != len(self.markets):
            raise ValueError("resolved facts require unique named markets")
        for market in self.markets:
            block = evm_block(self.anchors.checkpoint(market.resolved.selection.chain))
            if market.resolved.block != block or not market.evidence:
                raise ValueError("resolved market requires evidence at its exact anchor")
        return self

    def require(self, context: ReadContext, binding: ReadBinding):
        binding.require(context)
        self.anchors.require(context)
        if self.binding != binding:
            raise ValueError("resolved facts have different operator bindings")
        expected = tuple((market.name, market.selection) for market in context.markets)
        actual = tuple((market.name, market.resolved.selection) for market in self.markets)
        if expected != actual:
            raise ValueError("resolved facts differ from the exact authored market set")


class BootstrapService:
    request_type = BootstrapRequest

    def __init__(
        self, context: ReadContext, operator: OperatorConfig, mode: RunMode, env_file: Path
    ):
        accounts = context.match_operator(operator, mode)
        self.context, self.operator, self.env_file = context, operator, env_file
        self.binding = ReadBinding(
            context_digest=context.digest,
            operator_digest=operator.digest,
            mode=mode,
            accounts=accounts,
        )

    def reader(self, chain: ChainId) -> ResolutionReader:
        network = self.operator.network(chain)
        endpoint = provider_endpoint(network.binding(), self.env_file)
        # Resolution reads are idempotent observations: a provider's throttle or a block
        # it serves late is waited out, as for any observation reader, rather than
        # failing the run the resolution starts. A fresh anchor at `latest` also pins the
        # network's observation depth behind the head, where its providers serve pinned
        # state (on HyperEVM, Alchemy runs reads pinned near its head at head + 1).
        return ResolutionReader(
            Chain(chain_id=chain, environment="mainnet"),
            endpoint,
            retry_budget=BUSY_WAIT_LIMIT,
            observation=True,
        )

    def dispatch(self, method: str, request: BootstrapRequest):
        if method not in FIELDS or request.model_fields_set != FIELDS[method]:
            raise ValueError("bootstrap request requires the exact operation fields")
        if request.context_digest != self.context.digest:
            raise ValueError("bootstrap request belongs to another provisional run")
        if method == "resolution-context":
            return self.binding
        if method == "resolution-gas-policy":
            from almanak.gateway.policy_resolution import check_policy

            if request.policies is None:
                raise ValueError("gas compatibility requires explicit author policies")
            return check_policy(self.context, self.operator, request.policies)
        if method == "resolution-anchor":
            selection = next((a for a in self.context.anchors if a.chain == request.chain), None)
            if selection is None:
                raise ValueError("bootstrap anchor chain is outside the fixed context")
            with self.reader(selection.chain) as reader:
                return resolve_anchor(reader, selection)
        if request.anchors is None:
            raise ValueError("market resolution requires retained anchors")
        request.anchors.require(self.context)
        if method == "resolution-audit":
            for anchor in request.anchors.checkpoints:
                with self.reader(anchor.chain.chain_id) as reader:
                    block = evm_block(anchor)
                    if reader.pin_block(block.number) != block:
                        raise DataError(
                            "block_mismatch", "retained resolution anchor is invalidated"
                        )
            return request.anchors
        return self.resolve(request.anchors)

    def resolve(self, anchors: ResolutionAnchors) -> ResolutionResult:
        facts = {}
        for anchor in anchors.checkpoints:
            with self.reader(anchor.chain.chain_id) as reader:
                block = evm_block(anchor)
                cache: dict[str, tuple[Model, tuple[ResolutionRead, ...], dict]] = {}
                for market in self.context.markets:
                    if market.selection.chain == anchor.chain.chain_id:
                        facts[market.name] = resolve_named_market(reader, market, block, cache)
        for anchor in anchors.checkpoints:
            crossing = [
                market
                for market in self.context.markets
                if getattr(market.selection, "destination_chain", None) == anchor.chain.chain_id
            ]
            if not crossing:
                continue
            with self.reader(anchor.chain.chain_id) as reader:
                for market in crossing:
                    facts[market.name] = complete_named_market(
                        reader, facts[market.name], evm_block(anchor)
                    )
        result = ResolutionResult(
            binding=self.binding,
            anchors=anchors,
            markets=tuple(facts[m.name] for m in self.context.markets),
        )
        result.require(self.context, self.binding)
        return result


def resolve_named_market(reader, market, block, cache) -> MarketFacts:
    """Resolve one market through its connector; equal targets share one read set."""
    binding = binding_for(market.selection.connector)
    target = market.selection.model_copy(update={"assertions": None})
    key = content_digest(target)
    if key not in cache:
        start = len(reader.evidence)
        resolved = admitted(binding, binding.resolve(reader, target, block))
        assets = cast(ResolvedMarketFacts, resolved).assets
        symbols = asset_symbols(reader, assets, block)
        cache[key] = (resolved, tuple(reader.evidence[start:]), symbols)
    resolved, evidence, symbols = cache[key]
    # The authored assertions are replayed against the shared verified facts.
    replayed = binding.resolved.model_validate(
        {**resolved.model_dump(), "selection": market.selection}
    )
    return MarketFacts(
        name=market.name,
        resolved=cast(ResolvedMarketFacts, replayed),
        evidence=evidence,
        symbols=symbols,
    )


def admitted(binding, resolved):
    """Freshly resolved facts, held to the connector's reviewed code where it states any."""
    if binding.admit is not None:
        binding.admit(resolved)
    return resolved


def complete_named_market(reader, facts: MarketFacts, block) -> MarketFacts:
    """Complete a two-chain market on its second chain and retain those reads apart."""
    binding = binding_for(facts.resolved.selection.connector)
    if binding.complete is None:
        raise ValueError("market names a destination chain its connector does not resolve")
    start = len(reader.evidence)
    resolved = admitted(binding, binding.complete(reader, facts.resolved, block))
    symbols = asset_symbols(reader, (cast(Any, resolved).destination,), block)
    return MarketFacts(
        name=facts.name,
        resolved=cast(ResolvedMarketFacts, resolved),
        evidence=facts.evidence,
        symbols={**facts.symbols, **symbols},
        destination_evidence=tuple(reader.evidence[start:]),
    )


def serve(service: BootstrapService, token_file: Path, listen):
    import json

    from almanak.gateway.service import http_server

    server = http_server(service, token_file, listen, fields=FIELDS)
    try:
        print(
            json.dumps(
                {"event": "gateway_ready", "port": server.server_port, "authority": "read_only"}
            ),
            flush=True,
        )
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
