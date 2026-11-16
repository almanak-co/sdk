"""Decision and memory contracts shared by economic families and run modes."""

from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field, JsonValue, PrivateAttr, model_validator

from almanak.models import Model, UInt
from almanak.price_series import PriceSeries
from almanak.triggers import TeardownEvent, TriggerEvent
from almanak.work import ChainAvailability, ObservationScope


class InvalidConfig(ValueError):
    """Raise from `validate_config` to refuse the configuration with this message.

    The run reports it as a final `invalid_config` refusal, never as a crash, and
    from any function in strategy.py that `validate_config` calls.
    """


class Memory(Model):
    # Non-finite floats become JSON constants instead of null, so persisting refuses them
    # rather than storing a value the strategy never wrote. Accepted memory is unaffected.
    model_config = ConfigDict(ser_json_inf_nan="constants")

    version: Annotated[UInt, Field(gt=0)] = 1
    values: dict[str, JsonValue] = Field(default_factory=dict)
    schema_id: str | None = Field(default=None, exclude_if=lambda v: v is None)

    def updated(self, **values):
        # Field values as they are: a python-mode dump turns a Fraction into a str that the
        # strict model then refuses.
        current = {name: getattr(self, name) for name in type(self).model_fields}
        return type(self).model_validate({**current, **values})


class StrategyState(Memory):
    """Subclass as State inside strategy.py; typed fields are persisted by the worker."""


class DecisionContext[Config, Position, Observation](Model):
    config: Config
    memory: Memory
    owned: tuple[Position, ...]
    observations: tuple[Observation, ...]
    availability: tuple[ChainAvailability, ...] = Field(default=(), exclude_if=lambda v: not v)
    # One series per strategy price the strategy declares, as of the decision cutoff.
    prices: tuple[PriceSeries, ...] = Field(default=(), exclude_if=lambda v: not v)
    trigger: TriggerEvent
    _accessed: set[ObservationScope] = PrivateAttr(default_factory=set)

    @model_validator(mode="after")
    def no_series_in_teardown(self):
        # Teardown never waits on a price read, so a teardown decision sees no series in
        # any run mode: a backtest that delivered one would pass a strategy that fails
        # live. Single strategy prices of Aave V4 economic backtests are not series.
        if isinstance(self.trigger, TeardownEvent):
            kept = tuple(item for item in self.prices if not isinstance(item, PriceSeries))
            # The model is frozen; this runs inside its own validation, before any use.
            object.__setattr__(self, "prices", kept)
        return self

    @property
    def teardown(self) -> bool:
        return isinstance(self.trigger, TeardownEvent)

    @property
    def actions(self):
        from almanak.connectors.composition import connector_for

        return connector_for(self.config).codec.actions(self)

    def observation(self, name):
        for item in self.observations:
            observed: Any = item
            if observed.allocation == name:
                target = observed.target
                self._accessed.add(
                    ObservationScope(
                        chain=target.fund.chain, account=target.account, allocation=name
                    )
                )
                return item
        return None

    def require_observation(self, name):
        item = self.observation(name)
        if item is None:
            raise ValueError(f"{name}: required observation is unavailable")
        return item

    def price(self, asset):
        """The declared series for one asset; a strategy declares at most one per asset."""
        matches = [series for series in self.prices if series.binding.asset == asset]
        if len(matches) != 1:
            raise ValueError(f"{asset}: no single declared strategy price series")
        return matches[0]

    def position(self, name):
        for item in self.owned:
            position: Any = item
            if position.target.allocation == name:
                return item
        raise ValueError(f"{name}: no retained owned position")

    def decision(self, intent=None, *, memory=None, reason=None):
        from almanak.connectors.composition import connector_for

        scopes = tuple(sorted(self._accessed, key=lambda scope: scope.model_dump_json()))
        return connector_for(self.config).codec.decision(
            memory=self.memory if memory is None else memory,
            intent=intent,
            required_observations=scopes if intent is not None else (),
            reason=reason,
        )


class StrategyDecision[Intent](Model):
    memory: Memory
    intent: Intent | None = None
    required_observations: tuple[ObservationScope, ...] = Field(
        default=(), exclude_if=lambda v: not v
    )
    # Why the strategy holds or takes this intent, in its own words. Only a test run's
    # coordinator logs it; the runtime never acts on it and the checkpoint never keeps it.
    reason: str | None = Field(default=None, exclude_if=lambda v: v is None)


class OutcomeContext[Config, Result](Model):
    config: Config
    memory: Memory
    result: Result


class SimulationEvidence(Model):
    kind: Literal["simulation"] = "simulation"
    execution_id: str
    model: str
    inputs: tuple[str, ...]
