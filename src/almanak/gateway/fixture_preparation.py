"""Compile operator fixture inputs using existing, verified Anvil mechanics."""

from typing import Annotated, Literal

from pydantic import Field

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.configuration.resolution import ReadContext, ResolutionAnchors
from almanak.connectors.support import RunMode
from almanak.gateway.evm import erc20_address
from almanak.gateway.fork_config import (
    ForkAssetFunding,
    ForkEoaFund,
    ForkOperator,
    ForkSafeFund,
    ForkTarget,
    Name,
)
from almanak.gateway.operator_config import Digest, OperatorConfig, content_digest
from almanak.gateway.operator_fixture import EoaFixtureFund, ForkFixtureInputs, SafeFixtureFund
from almanak.gateway.resolution_evm import evm_block
from almanak.models import AssetAmount, Model, UInt

# The default Foundry fund has delegated code on the pinned Ethereum origin.
# Reuse the EOA proof range; each selected address still requires empty code.
EOA_INDEX_START = 941_167
OWNER_INDEX_START = 1_000_000_000


class FixtureKey(Model):
    chain: ChainId
    role: Literal["fund", "operator", "owner"]
    name: Name
    foundry_account_index: Annotated[UInt, Field(lt=2**31)]


class FixturePlan(Model):
    schema_version: Literal["anvil-fixture-plan-v1"] = "anvil-fixture-plan-v1"
    authority: Literal["unprovisioned"] = "unprovisioned"
    context_digest: Digest
    operator_digest: Digest
    fixture_digest: Digest
    anchors: ResolutionAnchors
    forks: tuple[ForkTarget, ...]
    funds: tuple[ForkEoaFund | ForkSafeFund, ...]
    operators: tuple[ForkOperator, ...]
    keys: tuple[FixtureKey, ...]

    @property
    def digest(self):
        return content_digest(self)

    def require(self, context: ReadContext, operator: OperatorConfig, anchors: ResolutionAnchors):
        if self != compile_fixture(context, operator, anchors):
            raise ValueError(
                "retained fixture differs from its operator inputs or resolved anchors"
            )


def compile_fixture(
    context: ReadContext, operator: OperatorConfig, anchors: ResolutionAnchors
) -> FixturePlan:
    context.match_operator(operator, RunMode.FORK)
    anchors.require(context)
    fixture = operator.fork
    if fixture is None:
        raise ValueError("operator configuration requires explicit fork fixture inputs")
    if fixture.anchors != context.anchors:
        raise ValueError("operator fixture anchor selection differs from the read context")
    if {f.account for f in fixture.funds} != {a.name for a in context.accounts}:
        raise ValueError("operator fixture must cover exactly the requested accounts")
    keys = fixture_keys(fixture)
    indices = {(key.role, key.name): key.foundry_account_index for key in keys}
    funds = tuple(
        compile_fund(fund, indices)
        for fund in sorted(fixture.funds, key=lambda f: (str(fixture.chain(f)), f.account))
    )
    operators = tuple(
        ForkOperator(
            name=item.name,
            native_balance=item.native_balance,
            foundry_account_index=indices["operator", item.name],
        )
        for item in sorted(
            fixture.operators, key=lambda o: (str(o.native_balance.asset.chain), o.name)
        )
    )
    return FixturePlan(
        context_digest=context.digest,
        operator_digest=operator.digest,
        fixture_digest=content_digest(fixture),
        anchors=anchors,
        forks=tuple(
            ForkTarget(
                chain=anchor.chain,
                provider=operator.network(anchor.chain.chain_id).binding(),
                block=evm_block(anchor),
            )
            for anchor in anchors.checkpoints
        ),
        funds=funds,
        operators=operators,
        keys=keys,
    )


def compile_fund(fund, indices):
    assets = tuple(compile_funding(balance) for balance in fund.assets)
    if isinstance(fund, SafeFixtureFund):
        return ForkSafeFund(account=fund.account, operator=fund.operator, assets=assets)
    return ForkEoaFund(
        account=fund.account,
        native_balance=fund.native_balance,
        assets=assets,
        foundry_account_index=indices["fund", fund.account],
    )


def compile_funding(balance: AssetAmount) -> ForkAssetFunding:
    if balance.asset == evm_network(balance.asset.chain).wrapped_native:
        method: Literal["wrap_native", "local_storage"] = "wrap_native"
    else:
        erc20_address(balance.asset, balance.asset.chain)
        method = "local_storage"
    return ForkAssetFunding(balance=balance, method=method)


def fixture_keys(fixture: ForkFixtureInputs) -> tuple[FixtureKey, ...]:
    keys: list[FixtureKey] = []
    for anchor in sorted(fixture.anchors, key=lambda a: str(a.chain)):
        funds = sorted(
            (f for f in fixture.funds if fixture.chain(f) == anchor.chain), key=lambda f: f.account
        )
        names: list[tuple[Literal["fund", "operator"], str]] = [
            ("fund", f.account) for f in funds if isinstance(f, EoaFixtureFund)
        ]
        names.extend(
            ("operator", o.name)
            for o in fixture.operators
            if o.native_balance.asset.chain == anchor.chain
        )
        if len(names) >= OWNER_INDEX_START - EOA_INDEX_START:
            raise ValueError("fixture key count exceeds the reserved owner range")
        keys.extend(
            FixtureKey(
                chain=anchor.chain,
                role=role,
                name=name,
                foundry_account_index=index + (EOA_INDEX_START if role == "fund" else 0),
            )
            for index, (role, name) in enumerate(sorted(names))
        )
        keys.extend(
            FixtureKey(
                chain=anchor.chain,
                role="owner",
                name=fund.account,
                foundry_account_index=OWNER_INDEX_START + index,
            )
            for index, fund in enumerate(funds)
            if isinstance(fund, SafeFixtureFund)
        )
    return tuple(keys)
