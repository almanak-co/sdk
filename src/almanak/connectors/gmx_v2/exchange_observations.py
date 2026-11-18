"""Exchange snapshots measure fungible shares and every token allowance explicitly."""

from datetime import UTC, datetime

from pydantic import model_validator

from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.connectors.gmx_v2.liquidity_reader import PendingLiquidity, read_liquidity
from almanak.connectors.gmx_v2.order_reader import PendingOrder, read_order
from almanak.connectors.gmx_v2.requests import Request
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount, Block, Model


def inventory_assets(target):
    return (*input_assets(target.market), native_asset(target.fund.chain.chain_id))


def allowance_assets(target):
    return (*input_assets(target.market), target.market.share_asset)


class ExchangeOwnership(Model):
    target: ExchangeTarget
    inventory: tuple[AssetAmount, ...]
    market_tokens: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        if tuple(value.asset for value in self.inventory) != inventory_assets(self.target):
            raise ValueError(
                "GMX exchange inventory must cover configured inputs and native refunds in order"
            )
        if self.market_tokens.asset != self.target.market.share_asset:
            raise ValueError("GMX exchange owned shares require the configured GM asset")
        return self


class ExchangeSnapshot(Model):
    target: ExchangeTarget
    block: Block
    owned: ExchangeOwnership
    wallets: tuple[AssetAmount, ...]
    market_tokens: AssetAmount
    allowances: tuple[AssetAmount, ...]
    native_balance: AssetAmount
    payer: Account
    request: Request | None = None
    pending_order: PendingOrder | None = None
    pending_liquidity: PendingLiquidity | None = None

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def needs_cleanup(self):
        return any(value.amount for value in self.allowances)

    @property
    def request_pending(self):
        return self.request is not None and self.request.unresolved

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.block.chain != self.target.fund.chain:
            raise ValueError(
                "GMX exchange snapshot must retain its exact owned target and environment"
            )
        if self.payer.chain != self.block.chain:
            raise ValueError("GMX exchange submitter must retain its execution environment")
        if tuple(value.asset for value in self.wallets) != inventory_assets(self.target):
            raise ValueError("GMX exchange wallet observations must cover its owned input assets")
        if tuple(value.asset for value in self.allowances) != allowance_assets(self.target):
            raise ValueError(
                "GMX exchange allowance observations must measure every input and GM asset"
            )
        if self.market_tokens.asset != self.target.market.share_asset:
            raise ValueError("GMX exchange observed shares require the configured GM asset")
        if self.native_balance.asset != native_asset(self.block.chain.chain_id):
            raise ValueError("GMX exchange payer balance requires its chain native asset")
        return self


def initial_owned(config, name):
    allocation, target = config.allocation(name), config.target(name)
    return ExchangeOwnership(
        target=target,
        inventory=(
            *allocation.initial_inventory.root,
            AssetAmount(asset=allocation.native_asset, amount=0),
        ),
        market_tokens=AssetAmount(asset=target.market.share_asset, amount=0),
    )


def snapshot(backend, name, block, reference=None, *, request=None):
    from almanak.connectors.gmx_v2.observations import verify_assets

    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned = (
        initial_owned(config, name)
        if reference is None
        else (reference if isinstance(reference, ExchangeOwnership) else reference.owned)
    )
    request = snapshot_request(owned, target, reference, request)
    verify_assets(config, reader, target, block)
    payer = config.accounts.binding(target.account).profile.submitter
    return build_snapshot(config, reader, target, owned, payer, block, request, name)


def snapshot_request(owned, target, reference, request):
    if owned.target != target:
        raise ValueError("GMX exchange snapshot reference differs from its configured target")
    return (
        reference.request
        if isinstance(reference, ExchangeSnapshot) and request is None
        else request
    )


def exchange_allowances(reader, target, block):
    return tuple(
        AssetAmount(
            asset=asset,
            amount=reader.call(
                asset.reference,
                "allowance(address,address)",
                block,
                inputs=("address", "address"),
                args=(target.fund.address, target.market.contracts.token_router),
                outputs=("uint256",),
            )[0],
        )
        for asset in allowance_assets(target)
    )


def build_snapshot(config, reader, target, owned, payer, block, request, name):
    from almanak.connectors.gmx_v2.observations import wallet_balance

    allowances = exchange_allowances(reader, target, block)
    return ExchangeSnapshot(
        target=target,
        block=block,
        owned=owned,
        wallets=exchange_wallets(reader, target, block),
        market_tokens=wallet_balance(reader, target.fund, target.market.share_asset, block),
        allowances=allowances,
        native_balance=wallet_balance(reader, payer, config.allocation(name).native_asset, block),
        payer=payer,
        request=request,
        pending_order=read_order(reader, target, request, block)
        if request is not None and request.kind == "order"
        else None,
        pending_liquidity=read_liquidity(reader, target, request, block)
        if request is not None and request.kind != "order"
        else None,
    )


def exchange_wallets(reader, target, block):
    from almanak.connectors.gmx_v2.observations import wallet_balance

    return tuple(
        wallet_balance(reader, target.fund, asset, block) for asset in inventory_assets(target)
    )
