"""GMX fund inventories, aggregate position and submitter balances are separate observations."""

from datetime import UTC, datetime

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.order_reader import PendingOrder, read_order
from almanak.connectors.gmx_v2.position_reader import read_position
from almanak.connectors.gmx_v2.positions import Ownership, Position
from almanak.connectors.gmx_v2.requests import Request
from almanak.models import Account, AssetAmount, Block, Model, UInt


class Snapshot(Model):
    target: Target
    block: Block
    owned: Ownership
    wallets: tuple[AssetAmount, ...]
    position: Position | None
    allowance: UInt
    native_balance: AssetAmount
    claimable_funding: tuple[AssetAmount, ...]
    payer: Account
    request: Request | None = None
    pending_order: PendingOrder | None = None

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def needs_cleanup(self):
        return self.allowance != 0 or any(value.amount for value in self.claimable_funding)

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.block.chain != self.target.fund.chain:
            raise ValueError("GMX snapshot must retain its exact target and execution scope")
        if self.payer.chain != self.block.chain:
            raise ValueError("GMX submitter observation must retain the execution chain")
        if tuple(a.asset for a in self.wallets) != tuple(a.asset for a in self.owned.inventory):
            raise ValueError("GMX wallet observations must cover every owned asset in order")
        return self

    @model_validator(mode="after")
    def funding_scope(self):
        expected = (self.target.market.long_asset, self.target.market.short_asset)
        if tuple(value.asset for value in self.claimable_funding) != expected:
            raise ValueError("GMX funding observations must measure both market assets in order")
        return self


def initial_owned(config, name):
    if isinstance(config.target(name), ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_observations import (
            initial_owned as initial_exchange,
        )

        return initial_exchange(config, name)
    allocation = config.allocation(name)
    other = next(
        asset
        for asset in (allocation.market.long_asset, allocation.market.short_asset)
        if asset != allocation.capital.asset
    )
    return Ownership(
        target=config.target(name),
        inventory=(
            allocation.capital,
            AssetAmount(asset=other, amount=0),
            AssetAmount(asset=allocation.native_asset, amount=0),
        ),
    )


def wallet_balance(reader, fund, asset, block):
    if asset.namespace == "slip44":
        amount = int(
            reader._rpc(
                "eth_getBalance",
                [fund.address, {"blockHash": block.hash, "requireCanonical": True}],
            ),
            16,
        )
    else:
        amount = reader.call(
            asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(fund.address,),
            outputs=("uint256",),
        )[0]
    return AssetAmount(asset=asset, amount=amount)


def snapshot(backend, name, block, reference=None, *, request=None):
    if isinstance(backend.config.target(name), ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_observations import snapshot as exchange_snapshot

        return exchange_snapshot(backend, name, block, reference, request=request)
    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned, request = snapshot_reference(config, name, target, reference, request)
    verify_assets(config, reader, target, block)
    observed = read_position(reader, target, block)
    payer = config.accounts.binding(target.account).profile.submitter
    native = config.allocation(name).native_asset
    allowance = input_allowance(reader, target, block)
    return build_snapshot(reader, target, owned, observed, payer, block, request, allowance, native)


def snapshot_reference(config, name, target, reference, request):
    owned = initial_owned(config, name) if reference is None else ownership(reference)
    if owned.target != target:
        raise ValueError("GMX snapshot reference differs from its configured target")
    if isinstance(reference, Snapshot) and request is None:
        request = reference.request
    return owned, request


def input_allowance(reader, target, block):
    return reader.call(
        target.market.wrapped_native.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, target.market.contracts.token_router),
        outputs=("uint256",),
    )[0]


def build_snapshot(reader, target, owned, observed, payer, block, request, allowance, native):
    return Snapshot(
        target=target,
        block=block,
        owned=owned,
        position=observed.position,
        allowance=allowance,
        wallets=tuple(wallet_balance(reader, target.fund, a.asset, block) for a in owned.inventory),
        native_balance=wallet_balance(reader, payer, native, block),
        payer=payer,
        claimable_funding=claimable_funding(reader, target, block),
        request=request,
        pending_order=read_order(reader, target, request, block) if request is not None else None,
    )


def ownership(reference):
    return reference if isinstance(reference, Ownership) else reference.owned


def claimable_funding(reader, target, block):
    from almanak.connectors.gmx_v2.oracle_reader import stored

    return tuple(
        AssetAmount(
            asset=asset,
            amount=stored(
                reader,
                block,
                target.market.contracts.data_store,
                "Uint",
                "CLAIMABLE_FUNDING_AMOUNT",
                ("address", "address", "address"),
                (target.market.share_asset.reference, asset.reference, target.fund.address),
            ),
        )
        for asset in (target.market.long_asset, target.market.short_asset)
    )


def verify_assets(config, reader, target, block):
    from almanak.configuration.metadata import verify
    from almanak.models import Asset

    assets = {target.market.index_asset, target.market.long_asset, target.market.short_asset}
    if isinstance(target, ExchangeTarget):
        assets.add(target.market.share_asset)
    for asset in assets:
        decimals = reader.call(asset.reference, "decimals()", block, outputs=("uint8",))[0]
        verify(config, Asset(chain=block.chain, identifier=asset, decimals=decimals))
