"""Named economic declarations bind to trusted configured targets at admission."""

from typing import Annotated

from pydantic import Field

from almanak.models import Model

AllocationName = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class DeclaredCapabilities(Model):
    allocations: tuple[AllocationName, ...]

    def validate_config(self, config):
        expected = tuple(a.name for a in config.positions)
        if len(set(self.allocations)) != len(self.allocations) or set(self.allocations) != set(
            expected
        ):
            raise ValueError("capabilities must declare every configured allocation exactly once")


def declared(config):
    result = DeclaredCapabilities(allocations=tuple(a.name for a in config.positions))
    result.validate_config(config)
    from almanak.configuration.parameter_codec import AuthorBase

    return result if isinstance(config, AuthorBase) else bind(config, result)


def bind(config, declaration):
    from almanak.connectors.composition import connector_for

    declaration.validate_config(config)
    return connector_for(config).codec.capability.declared(config)
