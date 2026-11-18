"""Concrete connector binds the shared durable exchange economic driver."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors import dex_economic_model as family
from almanak.connectors.curve import (
    authoring,
    economic_data,
    economic_parameters,
    economic_simulation,
    economic_state,
)
from almanak.connectors.curve.authority import admit, validate_capabilities
from almanak.connectors.curve.economic_validation import validate
from almanak.connectors.curve.lifecycle import has_obligations as remaining
from almanak.connectors.curve.lifecycle import increases_exposure
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.observations import quotes_for
from almanak.connectors.curve.positions import Allowance
from almanak.connectors.curve.receipts import effects
from almanak.connectors.curve.state import Decision, exit_complete
from almanak.connectors.dex_economic_parameters import wallet
from almanak.dex_intents import RevokeExchange
from almanak.effects import Effects
from almanak.models import AssetAmount


def allowances(target):
    return tuple(
        Allowance(asset=asset, spender=target.pool.pool, amount=0) for asset in target.assets
    )


class EconomicModel(family.EconomicModel):
    admit = staticmethod(admit)
    validate = staticmethod(validate)
    quotes_for = staticmethod(quotes_for)
    exit_complete = staticmethod(exit_complete)
    allowances = staticmethod(allowances)
    remaining = staticmethod(remaining)
    increases_exposure = staticmethod(increases_exposure)
    data = economic_data
    state_types = economic_state
    parameters = economic_parameters
    simulation = economic_simulation
    authoring = authoring
    decision_type = Decision
    revoke_type = RevokeExchange[Target]
    effects_type = Effects[economic_state.EconomicPosition]
    validate_capabilities = staticmethod(validate_capabilities)

    @property
    def model_name(self):
        return economic_data.model_identity(self.config)[0]

    def exit_leg(self, config, observed):
        allocation = config.allocation(observed.allocation)
        if observed.position is not None:
            return self.authoring.close_liquidity(allocation, observed)
        if observed.exit_quotes:
            return self.authoring.swap(allocation, observed, observed.exit_quotes[0])
        return self.revoke_type(target=observed.target)

    def snapshot_quote(self, record, payload, time):
        return lambda amount, output: self.simulation.measured_quote_at(
            record, payload.owned, amount, output, time
        )

    def snapshot_assets(self, state, payload, record):
        from almanak.configuration.metadata import verify

        for asset in (*record.payload.pool.currencies, record.payload.pool.shares):
            verify(self.config, asset)
        target = payload.target
        return {
            "wallets": AssetAmounts(
                tuple(
                    AssetAmount(asset=asset, amount=state.portfolio.balance(wallet(target, asset)))
                    for asset in target.assets
                )
            )
        }

    def result_measurements(self, target, deltas, fees, delta):
        result = effects(target, deltas, delta)
        return {"asset_changes": result.asset_changes, "share_change": result.share_change}
