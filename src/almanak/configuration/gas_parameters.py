"""Author gas economics and the frozen-preparation link shared by every connector.

Every connector's parameter type carries the author's gas policies and
`strategy.parameters` knobs, and every executable configuration carries the
digest of the preparation that froze it, so the gateway, the supervisor and the
journal check one contract regardless of the protocol.
"""

from typing import Annotated, Any, cast

from pydantic import Field, model_validator

from almanak.configuration.strategy_parameters import (
    ParametersField,
    StrategyParameters,
    unset,
)
from almanak.gateway.production_config import (
    ForkGasExecution,
    GasPolicy,
    UnenforcedGas,
    execution_gas,
    validate_gas_payers,
)
from almanak.models import Model


class AuthorGas(Model):
    gas: tuple[GasPolicy, ...] = Field(default=(), exclude_if=lambda value: not value)
    parameters: ParametersField = Field(default_factory=StrategyParameters, exclude_if=unset)

    @model_validator(mode="after")
    def gas_scope(self):
        if self.gas:
            owner = cast(Any, self)
            validate_gas_payers(self.gas, owner.accounts, owner.positions)
        return self


class PreparedConfig(Model):
    fork_gas: ForkGasExecution | None = Field(default=None, exclude_if=lambda value: value is None)
    unenforced_gas: UnenforcedGas | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    preparation_digest: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def prepared_scope(self):
        owner = cast(Any, self)
        admitted = execution_gas(owner)
        if admitted is not None and owner.unenforced_gas is not None:
            raise ValueError("unenforced author gas excludes admitted execution policy")
        retained = admitted or owner.unenforced_gas
        if owner.gas and (retained is None or owner.gas != retained.gas):
            raise ValueError("author gas policy requires matching admitted execution policy")
        fork = owner.fork_gas or owner.unenforced_gas
        if fork is not None:
            if owner.gas != fork.gas:
                raise ValueError("fork gas policy must equal the author gas policy")
            if (
                owner.production is not None
                or not owner.preparation_digest
                or not getattr(owner, "fork_bindings", ())
            ):
                raise ValueError("fork gas policy requires an identified prepared fork")
        return self
