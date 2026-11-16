"""Comet fund ownership retains signed base principal and actual collateral balances."""

from datetime import UTC, datetime

from pydantic import model_validator

from almanak.connectors.compound_v3.domain import (
    AccountObservation,
    Principal,
    Target,
    Uint128,
    Uint256,
)
from almanak.connectors.compound_v3.reader import read_account, read_market
from almanak.gateway.evm import native_asset
from almanak.lending_intents import OwnedCash
from almanak.models import Account, Asset, AssetAmount, Model


def target_asset(target: Target, account: AccountObservation) -> Asset:
    market = account.market
    if target.fund != account.fund or target.reserve.market != market.source:
        raise ValueError("Compound target differs from its observed fund or market")
    if target.reserve.is_base:
        return market.base_asset
    for collateral in market.collaterals:
        if collateral.asset.identifier == target.asset:
            if collateral.price_feed != target.reserve.price_feed:
                raise ValueError("Compound collateral differs from its configured oracle feed")
            return collateral.asset
    raise ValueError("Compound target is outside the observed market collateral list")


class Snapshot(Model):
    target: Target
    account: AccountObservation
    wallet_balance: Uint256
    allowance: Uint256
    native_balance: AssetAmount
    payer: Account
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def block(self):
        return self.account.market.block

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def asset(self):
        return target_asset(self.target, self.account)

    @model_validator(mode="after")
    def scope(self):
        target_asset(self.target, self.account)
        if self.payer.chain != self.block.chain or self.native_balance.asset != native_asset(
            self.block.chain.chain_id
        ):
            raise ValueError("Compound snapshot gas evidence differs from its payer chain")
        return self


class Ownership(Model):
    target: Target
    principal: Principal
    collateral: Uint128
    inventory: OwnedCash = None

    @model_validator(mode="after")
    def scope(self):
        if (self.target.reserve.is_base and self.collateral) or (
            not self.target.reserve.is_base and self.principal
        ):
            raise ValueError("Compound ownership cannot mix base principal and collateral units")
        return self

    @property
    def empty(self):
        return self.principal == 0 and self.collateral == 0


def owned(snapshot: Snapshot) -> Ownership:
    target, account = snapshot.target, snapshot.account
    if target.reserve.is_base:
        return Ownership(target=target, principal=account.principal, collateral=0)
    quantity = next(c.balance for c in account.collaterals if c.asset == target.asset)
    # Absorption can clear membership bits without clearing excluded collateral balances.
    return Ownership(target=target, principal=0, collateral=quantity)


def snapshot(backend, name, block):
    from almanak.configuration.metadata import verify

    target = backend.config.target(name)
    profile = backend.config.accounts.binding(target.account).profile
    reader = backend.reader
    market = read_market(reader, target.reserve.market, block)
    account = read_account(reader, market, target.fund)
    verify(backend.config, target_asset(target, account))
    wallet = reader.call(
        target.asset.reference,
        "balanceOf(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )[0]
    allowance = reader.call(
        target.asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, market.source.comet),
    )[0]
    native = int(
        reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    return Snapshot(
        target=target,
        account=account,
        wallet_balance=wallet,
        allowance=allowance,
        native_balance=AssetAmount(asset=native_asset(block.chain.chain_id), amount=native),
        payer=profile.submitter,
    )
