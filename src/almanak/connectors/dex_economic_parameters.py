"""Bind exchange author parameters to explicit hypothetical fund and payer inventory."""

from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import (
    AssetDelta,
    PositionKey,
    WalletAsset,
)
from almanak.models import AccountBinding, StrategyAccounts


def wallet(target, asset):
    return WalletAsset(account=target.account, chain=target.fund.chain, asset=asset)


def payer_wallet(settings, target, asset):
    profile = next(f.profile for f in settings.funds if f.name == target.account)
    if profile.submitter == profile.fund:
        return wallet(target, asset)
    payer = settings.payer(target.account)
    return WalletAsset(account=payer.name, chain=payer.fund.chain, asset=asset)


@dataclass(frozen=True)
class ParametersBinding:
    connector: ConnectorId
    parameters_type: type
    position_type: type
    state_type: type
    initial_owned: Callable
    position_record_type: type
    portfolio_type: type
    effects_type: type
    change_type: type
    # The liquidity model a connector requires for banded allocations; None refuses them.
    band_liquidity_model: str | None = None

    def parameters(self, author, settings, interval):
        if (
            any(a.liquidity_enabled for a in author.allocations)
            and settings.liquidity_model is None
        ):
            raise ValueError("LP allocations require an explicit liquidity model")
        self.validate_bands(author, settings)
        return self.parameters_type(
            decision_trigger=interval.effective,
            asset_metadata=author.asset_metadata,
            parameters=author.parameters,
            allocations=author.allocations,
            accounts=StrategyAccounts(
                bindings=tuple(
                    AccountBinding(name=f.name, profile=f.profile) for f in settings.funds
                )
            ),
        )

    def validate_bands(self, author, settings):
        banded = any(getattr(a, "band", None) is not None for a in author.allocations)
        if self.band_liquidity_model is None:
            if banded:
                raise ValueError(
                    "banded liquidity allocations are not supported in economic backtests yet"
                )
            return
        selected = settings.liquidity_model == self.band_liquidity_model
        if banded and not selected:
            raise ValueError(
                f"banded liquidity allocations require liquidity_model {self.band_liquidity_model}"
            )
        if selected and not banded:
            raise ValueError("bracketed fee growth requires a banded liquidity allocation")

    def position_key(self, target):
        return PositionKey(
            connector=self.connector,
            chain=target.fund.chain,
            account=target.account,
            reference=f"allocation:{target.allocation}:{target.pool.pool}",
        )

    def initial(self, settings, config, interval):
        balances = tuple(b for f in (*settings.funds, *settings.payers) for b in f.inventory())
        positions = tuple(
            self.position_record_type(
                key=self.position_key(config.target(a.name)),
                payload=self.position_type(
                    target=config.target(a.name), owned=self.initial_owned(config, a.name)
                ),
            )
            for a in config.allocations
        )
        portfolio = self.portfolio_type(balances=balances, positions=positions)
        for allocation in config.allocations:
            target = config.target(allocation.name)
            capital = allocation.capital
            values = capital.root if isinstance(capital, AssetAmounts) else (capital,)
            if any(
                portfolio.balance(wallet(target, value.asset)) < value.amount for value in values
            ):
                raise ValueError("initial inventory cannot cover configured exchange capital")
            for asset in target.assets:
                portfolio.balance(wallet(target, asset))
            portfolio.balance(payer_wallet(settings, target, allocation.native_asset))
        return self.state_type(
            run_id=uuid4().hex,
            time=settings.schedule.start,
            schedule=settings.schedule,
            interval=interval,
            portfolio=portfolio,
        )

    def transition_effects(self, settings, before, payload, deltas, native_asset, gas):
        changes = tuple(
            AssetDelta(key=wallet(payload.target, asset), amount=delta)
            for asset, delta in zip(payload.target.assets, deltas, strict=True)
        )
        return self.effects_type(
            assets=(
                *changes,
                *(
                    (
                        AssetDelta(
                            key=payer_wallet(settings, payload.target, native_asset), amount=-gas
                        ),
                    )
                    if gas
                    else ()
                ),
            ),
            positions=(
                self.change_type(
                    before=before,
                    after=self.position_record_type(key=before.key, payload=payload),
                ),
            ),
        )
